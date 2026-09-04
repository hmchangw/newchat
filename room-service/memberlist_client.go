//go:generate mockgen -source=memberlist_client.go -destination=mock_memberlist_client_test.go -package=main

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// MemberListClient fetches room members from a remote site's member.list endpoint.
// limit caps the response size at the wire layer so a misconfigured or oversized
// remote room cannot exhaust the caller's memory; pass maxRoomSize+1 to detect
// "source channel too large" without the local cap check ever seeing more than
// that many members.
type MemberListClient interface {
	ListMembers(ctx context.Context, requester string, ch model.ChannelRef, limit int) ([]model.RoomMember, error)
}

// natsMemberListClient is a NATS-backed implementation of MemberListClient.
type natsMemberListClient struct {
	nc      *nats.Conn
	timeout time.Duration
	metrics requestRecorder
}

type requestRecorder interface {
	Request(context.Context, natsmetrics.RPCMethod, time.Duration, error)
}

type memberListClientOption func(*natsMemberListClient)

func withMemberListRequestRecorder(metrics requestRecorder) memberListClientOption {
	return func(c *natsMemberListClient) { c.metrics = metrics }
}

func withMemberListMetrics(metrics natsmetrics.Publisher) memberListClientOption {
	return withMemberListRequestRecorder(metrics)
}

// NewNATSMemberListClient creates a NATS-backed MemberListClient. Returns the
// concrete type so future struct-only methods don't require widening the
// MemberListClient interface ("accept interfaces, return structs").
func NewNATSMemberListClient(nc *nats.Conn, timeout time.Duration, opts ...memberListClientOption) *natsMemberListClient {
	c := &natsMemberListClient{nc: nc, timeout: timeout}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ListMembers fetches members from a remote or same-site room via NATS request.
func (c *natsMemberListClient) ListMembers(ctx context.Context, requester string, ch model.ChannelRef, limit int) (members []model.RoomMember, resultErr error) {
	req := model.ListRoomMembersRequest{}
	if limit > 0 {
		req.Limit = &limit
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal member.list body: %w", err)
	}

	// c.timeout is validated as > 0 at config-load time (see room-service/main.go).
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// natsutil.NewMsg forwards the X-Request-ID from ctx for trace correlation;
	// the remote member.list endpoint mints one (RequestID middleware) if absent.
	out := natsutil.NewMsg(reqCtx, subject.MemberList(requester, ch.RoomID, ch.SiteID), body)
	started := time.Now()
	defer func() {
		if c.metrics == nil {
			return
		}
		// A remote "not a room member" is a complete answer from a healthy peer:
		// the request/reply exchange worked. rpc.client.call.duration's
		// error.type is a transport-shaped enum, so counting a business
		// rejection there lands it in other_error and leaves the family
		// non-zero at baseline.
		// GetMessageReadMeta already excludes its CodeNotFound the same way.
		outcome := resultErr
		if errors.Is(outcome, errNotRoomMember) {
			outcome = nil
		}
		c.metrics.Request(ctx, natsmetrics.MethodListMembers, time.Since(started), outcome)
	}()
	reply, err := c.nc.RequestMsgWithContext(reqCtx, out)
	if err != nil {
		return nil, fmt.Errorf("member.list request to %s: %w", ch.SiteID, err)
	}

	if ee, ok := errcode.Parse(reply.Data); ok {
		// Map the remote not-member reason back onto the local sentinel so callers
		// can use errors.Is(err, errNotRoomMember) uniformly regardless of which
		// site the source channel lives on. Other remote errors are reconstructed
		// as a typed *errcode.Error preserving the remote code/message/reason.
		//
		// Mixed-version rollout: a legacy remote that replies without a "code"
		// still parses (only "error" is required) but yields Code=="" and no
		// reason, so the not-member remap simply does not fire until both sides
		// are upgraded — an acceptable degradation, not a bug. Tasks 20.5/20.16:
		// errcode.New now panics on a non-canonical Code OR empty Message, so a
		// legacy/non-canonical envelope falls back to errcode.Internal here and
		// emits a single warn so SREs can spot legacy peers.
		if ee.Reason == errcode.RoomNotMember {
			return nil, errNotRoomMember
		}
		if !ee.Code.Valid() || ee.Message == "" {
			slog.WarnContext(ctx, "legacy peer emitted non-canonical errcode",
				"code", string(ee.Code), "message", ee.Message, "site", ch.SiteID)
			msg := ee.Message
			if msg == "" {
				msg = "remote site returned an error"
			}
			return nil, errcode.Internal(msg)
		}
		opts := []errcode.Option{errcode.WithReason(ee.Reason)}
		if len(ee.Metadata) > 0 {
			kv := make([]string, 0, 2*len(ee.Metadata))
			for k, v := range ee.Metadata {
				kv = append(kv, k, v)
			}
			opts = append(opts, errcode.WithMetadata(kv...))
		}
		return nil, errcode.New(ee.Code, ee.Message, opts...)
	}

	var resp model.ListRoomMembersResponse
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal member.list reply: %w", err)
	}
	return resp.Members, nil
}
