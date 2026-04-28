//go:generate mockgen -source=memberlist_client.go -destination=mock_memberlist_client_test.go -package=main

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/model"
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
}

// NewNATSMemberListClient creates a NATS-backed MemberListClient. Returns the
// concrete type so future struct-only methods don't require widening the
// MemberListClient interface ("accept interfaces, return structs").
func NewNATSMemberListClient(nc *nats.Conn, timeout time.Duration) *natsMemberListClient {
	return &natsMemberListClient{nc: nc, timeout: timeout}
}

// ListMembers fetches members from a remote or same-site room via NATS request.
func (c *natsMemberListClient) ListMembers(ctx context.Context, requester string, ch model.ChannelRef, limit int) ([]model.RoomMember, error) {
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

	out := &nats.Msg{
		Subject: subject.MemberList(requester, ch.RoomID, ch.SiteID),
		Data:    body,
		Header:  nats.Header{},
	}
	reply, err := c.nc.RequestMsgWithContext(reqCtx, out)
	if err != nil {
		return nil, fmt.Errorf("member.list request to %s: %w", ch.SiteID, err)
	}

	if errResp, ok := natsutil.TryParseError(reply.Data); ok {
		// Map the remote sentinel string back onto the local sentinel so callers
		// can use errors.Is(err, errNotRoomMember) uniformly regardless of which
		// site the source channel lives on. Other remote errors are passed
		// through via the "remote member.list:" prefix sanitizeError whitelists.
		if errResp.Error == errNotRoomMember.Error() {
			return nil, errNotRoomMember
		}
		return nil, fmt.Errorf("remote member.list: %s", errResp.Error)
	}

	var resp model.ListRoomMembersResponse
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal member.list reply: %w", err)
	}
	return resp.Members, nil
}
