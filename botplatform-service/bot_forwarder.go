package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/subject"
)

// natsRequester is the narrow outbound NATS surface botForwarder calls.
type natsRequester interface {
	RequestMsgWithContext(ctx context.Context, msg *nats.Msg) (*nats.Msg, error)
}

// botForwarder issues req/reply from BP to bot-message-handler and generates the messageID + createdAt headers bot-message-handler uses verbatim.
type botForwarder struct {
	nc           natsRequester
	timeout      time.Duration
	newMessageID func() string
	nowUTC       func() time.Time
}

func newBotForwarder(nc natsRequester, timeout time.Duration) *botForwarder {
	return &botForwarder{
		nc:           nc,
		timeout:      timeout,
		newMessageID: idgen.GenerateMessageID,
		nowUTC:       func() time.Time { return time.Now().UTC() },
	}
}

// sendRoom forwards a send-in-room request; NATS timeout maps to 503 handler_timeout.
func (f *botForwarder) sendRoom(ctx context.Context, sess *session.Session, siteID, roomID string, body []byte) (*model.Message, error) {
	subj := subject.ServerBotMsgRoomSend(siteID, roomID)
	return f.forward(ctx, sess, subj, body)
}

// sendDM forwards a send-DM request.
func (f *botForwarder) sendDM(ctx context.Context, sess *session.Session, siteID, targetUserID string, body []byte) (*model.Message, error) {
	subj := subject.ServerBotDMSend(siteID, targetUserID)
	return f.forward(ctx, sess, subj, body)
}

// forwardRoomMgmt uses a 15s timeout (vs 3s for msg flow) to accommodate multi-member fan-out.
// Returns raw JSON so BP can pass bot-room-service's envelope through unchanged.
func (f *botForwarder) forwardRoomMgmt(ctx context.Context, sess *session.Session, subj string, body []byte) ([]byte, error) {
	if sess == nil {
		return nil, errcode.Internal("bot forwarder: missing session")
	}

	identity := model.BotIdentity{ID: sess.UserID, Account: sess.Account, SiteID: sess.SiteID}
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		return nil, fmt.Errorf("marshal identity: %w", err)
	}

	msg := natsutil.NewMsg(ctx, subj, body)
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	msg.Header.Set(model.HeaderBotIdentity, string(identityJSON))
	// No X-Bot-Message-ID / X-Bot-Created-At: room mgmt has no messageID.

	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	reply, err := f.nc.RequestMsgWithContext(reqCtx, msg)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
			return nil, errcode.Unavailable("bot room-service timeout",
				errcode.WithReason(errcode.BotHandlerTimeout),
				errcode.WithCause(err))
		}
		return nil, errcode.Internal("bot room-service request", errcode.WithCause(err))
	}
	if ee, ok := errcode.Parse(reply.Data); ok {
		return nil, errcode.New(ee.Code, ee.Message,
			errcode.WithReason(ee.Reason),
			errcode.WithCause(fmt.Errorf("bot-room-service reply: %s", ee.Message)))
	}
	return reply.Data, nil
}

func (f *botForwarder) createRoom(ctx context.Context, sess *session.Session, siteID string, body []byte) ([]byte, error) {
	return f.forwardRoomMgmt(ctx, sess, subject.ServerBotRoomCreate(siteID), body)
}

func (f *botForwarder) addMembers(ctx context.Context, sess *session.Session, siteID, roomID string, body []byte) ([]byte, error) {
	return f.forwardRoomMgmt(ctx, sess, subject.ServerBotRoomMemberAdd(siteID, roomID), body)
}

func (f *botForwarder) removeMembers(ctx context.Context, sess *session.Session, siteID, roomID string, body []byte) ([]byte, error) {
	return f.forwardRoomMgmt(ctx, sess, subject.ServerBotRoomMemberRemove(siteID, roomID), body)
}

func (f *botForwarder) forward(ctx context.Context, sess *session.Session, subj string, body []byte) (*model.Message, error) {
	if sess == nil {
		return nil, errcode.Internal("bot forwarder: missing session")
	}

	messageID := f.newMessageID()
	createdAtMs := f.nowUTC().UnixMilli()

	identity := model.BotIdentity{
		ID:      sess.UserID,
		Account: sess.Account,
		SiteID:  sess.SiteID,
	}
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		return nil, fmt.Errorf("marshal identity: %w", err)
	}

	msg := natsutil.NewMsg(ctx, subj, body)
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	msg.Header.Set(model.HeaderBotIdentity, string(identityJSON))
	msg.Header.Set(model.HeaderBotMessageID, messageID)
	msg.Header.Set(model.HeaderBotCreatedAt, strconv.FormatInt(createdAtMs, 10))

	reqCtx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()
	reply, err := f.nc.RequestMsgWithContext(reqCtx, msg)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
			return nil, errcode.Unavailable("bot handler timeout",
				errcode.WithReason(errcode.BotHandlerTimeout),
				errcode.WithCause(err))
		}
		return nil, errcode.Internal("bot handler request", errcode.WithCause(err))
	}

	// Errcode envelope: reconstruct as a typed error so BP's boundary classify+log fires.
	if ee, ok := errcode.Parse(reply.Data); ok {
		return nil, errcode.New(ee.Code, ee.Message,
			errcode.WithReason(ee.Reason),
			errcode.WithCause(fmt.Errorf("bot-message-handler reply: %s", ee.Message)))
	}

	var resp model.BotSendResponse
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		return nil, errcode.Internal("unmarshal bot handler reply", errcode.WithCause(err))
	}
	return &resp.Message, nil
}

var _ natsRequester = (*nats.Conn)(nil)
