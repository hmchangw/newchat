package main

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

// fakeStore is the hand-rolled Store double; unset function fields panic loudly.
type fakeStore struct {
	FindSubscriptionFn func(ctx context.Context, roomID, userID string) (*Subscription, error)
	FindRoomFn         func(ctx context.Context, roomID string) (*Room, error)
	ListMemberIDsFn    func(ctx context.Context, roomID string) ([]string, error)
	FindUserFn         func(ctx context.Context, userID string) (*model.User, error)
}

func (f *fakeStore) FindSubscription(ctx context.Context, roomID, userID string) (*Subscription, error) {
	return f.FindSubscriptionFn(ctx, roomID, userID)
}
func (f *fakeStore) FindRoom(ctx context.Context, roomID string) (*Room, error) {
	return f.FindRoomFn(ctx, roomID)
}
func (f *fakeStore) ListMemberIDs(ctx context.Context, roomID string) ([]string, error) {
	if f.ListMemberIDsFn == nil {
		return nil, nil
	}
	return f.ListMemberIDsFn(ctx, roomID)
}
func (f *fakeStore) FindUser(ctx context.Context, userID string) (*model.User, error) {
	return f.FindUserFn(ctx, userID)
}

type fakePublisher struct {
	calls     int32
	lastSubj  string
	lastData  []byte
	lastMsgID string
	err       error
}

func (f *fakePublisher) PublishWithMsgID(_ context.Context, subj string, data []byte, msgID string) (*jetstream.PubAck, error) {
	atomic.AddInt32(&f.calls, 1)
	f.lastSubj = subj
	f.lastData = append([]byte(nil), data...)
	f.lastMsgID = msgID
	return &jetstream.PubAck{Stream: "BOT_MESSAGES_CANONICAL_test", Sequence: 1}, f.err
}

// newCtx builds a *natsrouter.Context bypassing the router and NATS bus.
func newCtx(t *testing.T, roomID string, headers nats.Header, body any) *natsrouter.Context {
	t.Helper()
	c := natsrouter.NewContext(map[string]string{"roomID": roomID})
	msg := &nats.Msg{
		Subject: "chat.server.bot.request.room.site-a." + roomID + ".msg.send",
		Header:  headers,
	}
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		msg.Data = data
	}
	c.Msg = msg
	return c
}

func validHeaders(t *testing.T, identity BotIdentity, messageID string, createdAtMs int64) nats.Header { //nolint:gocritic // hugeParam: BotIdentity by value is fine for a test helper
	t.Helper()
	identityJSON, err := json.Marshal(identity)
	require.NoError(t, err)
	h := nats.Header{}
	h.Set(model.HeaderBotIdentity, string(identityJSON))
	h.Set(model.HeaderBotMessageID, messageID)
	h.Set(model.HeaderBotCreatedAt, strconv.FormatInt(createdAtMs, 10))
	return h
}

func botIdent() BotIdentity {
	return BotIdentity{
		ID: "bot-1", Account: "myapp.bot", SiteID: "site-a",
		EngName: "Payroll Bot", AppID: "app-123", AppName: "PayrollBot",
	}
}

func TestHandleSendRoom_HappyPath(t *testing.T) {
	store := &fakeStore{
		FindSubscriptionFn: func(_ context.Context, _, _ string) (*Subscription, error) {
			return &Subscription{RoomID: "r1", UserID: "bot-1", SiteID: "site-a"}, nil
		},
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", Name: "deployments", SiteID: "site-a"}, nil
		},
	}
	pub := &fakePublisher{}
	h := newHandler(store, pub, "site-a")

	msgID := idgen.GenerateMessageID()
	created := time.Now().UnixMilli()
	c := newCtx(t, "r1", validHeaders(t, botIdent(), msgID, created), nil)

	resp, err := h.handleSendRoom(c, BotSendRoomRequest{Content: "hi"})
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Equal(t, msgID, resp.Message.ID)
	assert.Equal(t, "r1", resp.Message.RoomID)
	assert.Equal(t, "bot-1", resp.Message.UserID)
	assert.Equal(t, "myapp.bot", resp.Message.UserAccount)
	assert.Equal(t, "Payroll Bot", resp.Message.UserDisplayName, "displayName derives from EngName")
	assert.Equal(t, "hi", resp.Message.Content)
	assert.Equal(t, time.UnixMilli(created).UTC(), resp.Message.CreatedAt)

	assert.Equal(t, int32(1), atomic.LoadInt32(&pub.calls))
	assert.Equal(t, subject.BotCanonicalCreated("site-a"), pub.lastSubj)
	assert.Equal(t, msgID, pub.lastMsgID, "MsgID must be the BP-generated messageID (JS-layer dedup)")

	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal(pub.lastData, &evt))
	assert.Equal(t, model.EventCreated, evt.Event)
	assert.Equal(t, "site-a", evt.SiteID)
	assert.Equal(t, resp.Message.ID, evt.Message.ID)
	assert.Equal(t, created, evt.Timestamp)
}

func TestHandleSendRoom_SubscriptionMissing(t *testing.T) {
	store := &fakeStore{
		FindSubscriptionFn: func(_ context.Context, _, _ string) (*Subscription, error) {
			return nil, ErrNotFound
		},
	}
	pub := &fakePublisher{}
	h := newHandler(store, pub, "site-a")

	c := newCtx(t, "r1", validHeaders(t, botIdent(), idgen.GenerateMessageID(), 1), nil)
	_, err := h.handleSendRoom(c, BotSendRoomRequest{Content: "hi"})
	requireErrcode(t, err, errcode.CodeForbidden, "not_a_room_member")
	assert.Equal(t, int32(0), atomic.LoadInt32(&pub.calls), "no publish when auth fails")
}

func TestHandleSendRoom_RoomMissing(t *testing.T) {
	store := &fakeStore{
		FindSubscriptionFn: func(_ context.Context, _, _ string) (*Subscription, error) {
			return &Subscription{RoomID: "r1", UserID: "bot-1", SiteID: "site-a"}, nil
		},
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) { return nil, ErrNotFound },
	}
	h := newHandler(store, &fakePublisher{}, "site-a")

	c := newCtx(t, "r1", validHeaders(t, botIdent(), idgen.GenerateMessageID(), 1), nil)
	_, err := h.handleSendRoom(c, BotSendRoomRequest{Content: "hi"})
	requireErrcode(t, err, errcode.CodeNotFound, "room_not_found")
}

func TestHandleSendRoom_ContentValidation(t *testing.T) {
	tests := []struct {
		name    string
		content string
		reason  string
	}{
		{"empty content", "", "content_invalid"},
		{"oversize content", strings.Repeat("a", 20*1024+1), "content_invalid"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{
				FindSubscriptionFn: func(_ context.Context, _, _ string) (*Subscription, error) {
					return &Subscription{RoomID: "r1", UserID: "bot-1", SiteID: "site-a"}, nil
				},
				FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
					return &Room{ID: "r1", Type: "c", SiteID: "site-a"}, nil
				},
			}
			h := newHandler(store, &fakePublisher{}, "site-a")
			c := newCtx(t, "r1", validHeaders(t, botIdent(), idgen.GenerateMessageID(), 1), nil)
			_, err := h.handleSendRoom(c, BotSendRoomRequest{Content: tc.content})
			requireErrcode(t, err, errcode.CodeBadRequest, tc.reason)
		})
	}
}

func TestHandleSendRoom_HeaderValidation(t *testing.T) {
	store := &fakeStore{
		FindSubscriptionFn: func(_ context.Context, _, _ string) (*Subscription, error) {
			return &Subscription{RoomID: "r1", UserID: "bot-1", SiteID: "site-a"}, nil
		},
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", SiteID: "site-a"}, nil
		},
	}
	h := newHandler(store, &fakePublisher{}, "site-a")

	tests := []struct {
		name    string
		headers nats.Header
	}{
		{"missing identity", func() nats.Header {
			h := nats.Header{}
			h.Set(model.HeaderBotMessageID, idgen.GenerateMessageID())
			h.Set(model.HeaderBotCreatedAt, "1")
			return h
		}()},
		{"malformed identity", func() nats.Header {
			h := nats.Header{}
			h.Set(model.HeaderBotIdentity, "{not json")
			h.Set(model.HeaderBotMessageID, idgen.GenerateMessageID())
			h.Set(model.HeaderBotCreatedAt, "1")
			return h
		}()},
		{"missing messageID", func() nats.Header {
			ib, _ := json.Marshal(botIdent())
			h := nats.Header{}
			h.Set(model.HeaderBotIdentity, string(ib))
			h.Set(model.HeaderBotCreatedAt, "1")
			return h
		}()},
		{"malformed createdAt", func() nats.Header {
			ib, _ := json.Marshal(botIdent())
			h := nats.Header{}
			h.Set(model.HeaderBotIdentity, string(ib))
			h.Set(model.HeaderBotMessageID, idgen.GenerateMessageID())
			h.Set(model.HeaderBotCreatedAt, "not-a-number")
			return h
		}()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newCtx(t, "r1", tc.headers, nil)
			_, err := h.handleSendRoom(c, BotSendRoomRequest{Content: "hi"})
			requireErrcode(t, err, errcode.CodeBadRequest, "invalid_header")
		})
	}
}

func TestHandleSendRoom_MentionCanonicalization(t *testing.T) {
	store := &fakeStore{
		FindSubscriptionFn: func(_ context.Context, _, _ string) (*Subscription, error) {
			return &Subscription{RoomID: "r1", UserID: "bot-1", SiteID: "site-a"}, nil
		},
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", SiteID: "site-a"}, nil
		},
		ListMemberIDsFn: func(_ context.Context, _ string) ([]string, error) {
			return []string{"bot-1", "user-99"}, nil
		},
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "alice.true", SiteID: "site-a", EngName: "Alice True"}, nil
		},
	}
	h := newHandler(store, &fakePublisher{}, "site-a")

	c := newCtx(t, "r1", validHeaders(t, botIdent(), idgen.GenerateMessageID(), 1), nil)
	req := BotSendRoomRequest{
		Content: "hi",
		Mentions: []model.Participant{{
			UserID:      "user-99",
			Account:     "FAKE-ACCOUNT",
			DisplayName: "Impersonator",
		}},
	}
	resp, err := h.handleSendRoom(c, req)
	require.NoError(t, err)
	require.Len(t, resp.Message.Mentions, 1)
	got := resp.Message.Mentions[0]
	assert.Equal(t, "user-99", got.UserID)
	assert.Equal(t, "alice.true", got.Account, "server-authoritative account must overwrite client-supplied value")
	assert.NotEqual(t, "Impersonator", got.DisplayName)
}

func TestHandleSendRoom_MentionNonMemberRejected(t *testing.T) {
	store := &fakeStore{
		FindSubscriptionFn: func(_ context.Context, _, _ string) (*Subscription, error) {
			return &Subscription{RoomID: "r1", UserID: "bot-1", SiteID: "site-a"}, nil
		},
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", SiteID: "site-a"}, nil
		},
		ListMemberIDsFn: func(_ context.Context, _ string) ([]string, error) {
			return []string{"bot-1"}, nil
		},
	}
	h := newHandler(store, &fakePublisher{}, "site-a")

	c := newCtx(t, "r1", validHeaders(t, botIdent(), idgen.GenerateMessageID(), 1), nil)
	req := BotSendRoomRequest{
		Content:  "hi",
		Mentions: []model.Participant{{UserID: "user-99"}},
	}
	_, err := h.handleSendRoom(c, req)
	requireErrcode(t, err, errcode.CodeBadRequest, "mention_invalid")
}

func TestHandleSendRoom_ThreadReplyFieldsCopied(t *testing.T) {
	store := &fakeStore{
		FindSubscriptionFn: func(_ context.Context, _, _ string) (*Subscription, error) {
			return &Subscription{RoomID: "r1", UserID: "bot-1", SiteID: "site-a"}, nil
		},
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", SiteID: "site-a"}, nil
		},
	}
	h := newHandler(store, &fakePublisher{}, "site-a")

	parentCreated := time.Now().UTC()
	c := newCtx(t, "r1", validHeaders(t, botIdent(), idgen.GenerateMessageID(), 1), nil)
	req := BotSendRoomRequest{
		Content:                      "reply",
		ThreadParentMessageID:        "parent-msg",
		ThreadParentMessageCreatedAt: &parentCreated,
		TShow:                        true,
	}
	resp, err := h.handleSendRoom(c, req)
	require.NoError(t, err)
	assert.Equal(t, "parent-msg", resp.Message.ThreadParentMessageID)
	assert.Equal(t, parentCreated, *resp.Message.ThreadParentMessageCreatedAt)
	assert.True(t, resp.Message.TShow)
}

func TestHandleSendRoom_TShowIgnoredOnNonThread(t *testing.T) {
	store := &fakeStore{
		FindSubscriptionFn: func(_ context.Context, _, _ string) (*Subscription, error) {
			return &Subscription{RoomID: "r1", UserID: "bot-1", SiteID: "site-a"}, nil
		},
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", SiteID: "site-a"}, nil
		},
	}
	h := newHandler(store, &fakePublisher{}, "site-a")
	c := newCtx(t, "r1", validHeaders(t, botIdent(), idgen.GenerateMessageID(), 1), nil)
	resp, err := h.handleSendRoom(c, BotSendRoomRequest{Content: "hi", TShow: true})
	require.NoError(t, err)
	assert.False(t, resp.Message.TShow, "tshow must be false when there is no thread parent")
}

func TestHandleSendRoom_PublishError(t *testing.T) {
	store := &fakeStore{
		FindSubscriptionFn: func(_ context.Context, _, _ string) (*Subscription, error) {
			return &Subscription{RoomID: "r1", UserID: "bot-1", SiteID: "site-a"}, nil
		},
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", SiteID: "site-a"}, nil
		},
	}
	pub := &fakePublisher{err: errors.New("nats down")}
	h := newHandler(store, pub, "site-a")

	c := newCtx(t, "r1", validHeaders(t, botIdent(), idgen.GenerateMessageID(), 1), nil)
	_, err := h.handleSendRoom(c, BotSendRoomRequest{Content: "hi"})
	require.Error(t, err)
	var ec *errcode.Error
	require.True(t, errors.As(err, &ec))
	assert.Equal(t, errcode.CodeInternal, ec.Code)
}

// requireErrcode asserts err is a *errcode.Error with the given category and reason.
func requireErrcode(t *testing.T, err error, wantCode errcode.Code, wantReason string) {
	t.Helper()
	require.Error(t, err)
	var ec *errcode.Error
	require.Truef(t, errors.As(err, &ec), "want *errcode.Error, got %T (%v)", err, err)
	assert.Equal(t, wantCode, ec.Code)
	assert.Equal(t, wantReason, string(ec.Reason))
}
