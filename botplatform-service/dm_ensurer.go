package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/subject"
)

// dmEnsurer materializes a DM room at BP's site on first-DM subscription miss, then returns
// so the caller can forward the actual send; kept as an interface so tests substitute a fake.
type dmEnsurer interface {
	Ensure(ctx context.Context, sess *session.Session, targetUserID string) (roomID string, err error)
}

type natsDMEnsurer struct {
	nc      natsRequester
	siteID  string
	timeout time.Duration
}

func newNATSDMEnsurer(nc natsRequester, siteID string, timeout time.Duration) *natsDMEnsurer {
	return &natsDMEnsurer{nc: nc, siteID: siteID, timeout: timeout}
}

func (e *natsDMEnsurer) Ensure(ctx context.Context, sess *session.Session, targetUserID string) (string, error) {
	if sess == nil {
		return "", errcode.Internal("dm ensurer: missing session")
	}
	body, err := json.Marshal(model.BotDMEnsureRequest{TargetUserID: targetUserID})
	if err != nil {
		return "", fmt.Errorf("marshal dm ensure req: %w", err)
	}
	identity := model.BotIdentity{ID: sess.UserID, Account: sess.Account, SiteID: sess.SiteID}
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("marshal identity: %w", err)
	}
	msg := natsutil.NewMsg(ctx, subject.ServerBotRoomDMEnsure(e.siteID), body)
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	msg.Header.Set(model.HeaderBotIdentity, string(identityJSON))

	reqCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	reply, err := e.nc.RequestMsgWithContext(reqCtx, msg)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
			return "", errcode.Unavailable("dm ensure timeout",
				errcode.WithReason(errcode.BotHandlerTimeout),
				errcode.WithCause(err))
		}
		return "", errcode.Internal("dm ensure request", errcode.WithCause(err))
	}
	if ee, ok := errcode.Parse(reply.Data); ok {
		return "", errcode.New(ee.Code, ee.Message,
			errcode.WithReason(ee.Reason),
			errcode.WithCause(fmt.Errorf("dm ensure reply: %s", ee.Message)))
	}
	var resp model.BotDMEnsureResponse
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		return "", errcode.Internal("decode dm ensure reply", errcode.WithCause(err))
	}
	return resp.RoomID, nil
}
