package main

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
)

type fakeRequester struct {
	lastMsg *nats.Msg
	reply   func(msg *nats.Msg) (*nats.Msg, error)
}

func (f *fakeRequester) RequestMsgWithContext(_ context.Context, msg *nats.Msg) (*nats.Msg, error) {
	f.lastMsg = msg
	if f.reply == nil {
		return nil, nats.ErrTimeout
	}
	return f.reply(msg)
}

func botSess() *session.Session {
	return &session.Session{
		UserID: "bot-user-id", Account: "myapp.bot", SiteID: "site-a",
	}
}

func TestBotForwarder_SendRoom_HappyPath(t *testing.T) {
	msgID := "01970a4f8c2d7c9aabc9"
	created := time.Unix(1700000000, 0).UTC()

	rq := &fakeRequester{
		reply: func(msg *nats.Msg) (*nats.Msg, error) {
			assert.Equal(t, "chat.server.bot.request.room.site-a.r1.msg.send", msg.Subject)
			assert.Equal(t, msgID, msg.Header.Get(model.HeaderBotMessageID))
			assert.Equal(t, strconv.FormatInt(created.UnixMilli(), 10), msg.Header.Get(model.HeaderBotCreatedAt))

			var id model.BotIdentity
			require.NoError(t, json.Unmarshal([]byte(msg.Header.Get(model.HeaderBotIdentity)), &id))
			assert.Equal(t, "bot-user-id", id.ID)
			assert.Equal(t, "myapp.bot", id.Account)
			assert.Equal(t, "site-a", id.SiteID)

			reply := model.BotSendResponse{Message: model.Message{
				ID: msgID, RoomID: "r1", UserID: "bot-user-id", UserAccount: "myapp.bot",
				Content: "hi", CreatedAt: created,
			}}
			data, _ := json.Marshal(reply)
			return &nats.Msg{Data: data}, nil
		},
	}

	f := newBotForwarder(rq, 3*time.Second)
	f.newMessageID = func() string { return msgID }
	f.nowUTC = func() time.Time { return created }

	m, err := f.sendRoom(context.Background(), botSess(), "site-a", "r1", []byte(`{"content":"hi"}`))
	require.NoError(t, err)
	require.NotNil(t, m)
	assert.Equal(t, msgID, m.ID)
	assert.Equal(t, "hi", m.Content)
}

func TestBotForwarder_SendRoom_ErrcodeReply(t *testing.T) {
	rq := &fakeRequester{
		reply: func(_ *nats.Msg) (*nats.Msg, error) {
			envelope := []byte(`{"code":"forbidden","error":"not a member","reason":"not_a_room_member"}`)
			return &nats.Msg{Data: envelope}, nil
		},
	}
	f := newBotForwarder(rq, 3*time.Second)

	_, err := f.sendRoom(context.Background(), botSess(), "site-a", "r1", []byte(`{"content":"hi"}`))
	require.Error(t, err)
	var ec *errcode.Error
	require.True(t, errors.As(err, &ec))
	assert.Equal(t, errcode.CodeForbidden, ec.Code)
	assert.Equal(t, "not_a_room_member", string(ec.Reason))
}

func TestBotForwarder_SendRoom_TimeoutIsUnavailable(t *testing.T) {
	rq := &fakeRequester{
		reply: func(_ *nats.Msg) (*nats.Msg, error) { return nil, nats.ErrTimeout },
	}
	f := newBotForwarder(rq, 50*time.Millisecond)

	_, err := f.sendRoom(context.Background(), botSess(), "site-a", "r1", []byte(`{"content":"hi"}`))
	require.Error(t, err)
	var ec *errcode.Error
	require.True(t, errors.As(err, &ec))
	assert.Equal(t, errcode.CodeUnavailable, ec.Code)
	assert.Equal(t, "handler_timeout", string(ec.Reason))
}

func TestBotForwarder_SendDM_UsesDMSubject(t *testing.T) {
	captured := ""
	rq := &fakeRequester{
		reply: func(msg *nats.Msg) (*nats.Msg, error) {
			captured = msg.Subject
			data, _ := json.Marshal(model.BotSendResponse{Message: model.Message{ID: "m1"}})
			return &nats.Msg{Data: data}, nil
		},
	}
	f := newBotForwarder(rq, time.Second)
	_, err := f.sendDM(context.Background(), botSess(), "site-a", "target-user", []byte(`{"content":"hi"}`))
	require.NoError(t, err)
	assert.Equal(t, "chat.server.bot.request.dm.site-a.target-user.msg.send", captured)
}

func TestBotForwarder_MissingSessionIs500(t *testing.T) {
	f := newBotForwarder(&fakeRequester{}, time.Second)
	_, err := f.sendRoom(context.Background(), nil, "site-a", "r1", []byte(`{}`))
	require.Error(t, err)
	var ec *errcode.Error
	require.True(t, errors.As(err, &ec))
	assert.Equal(t, errcode.CodeInternal, ec.Code)
}
