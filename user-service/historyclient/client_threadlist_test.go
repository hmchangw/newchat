package historyclient

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// respondThreadList subscribes the site's thread-list subject with a canned raw
// reply, capturing the request body the client put on the wire.
func respondThreadList(t *testing.T, nc *o11ynats.Conn, siteID string, reply []byte) *[]byte {
	t.Helper()
	got := new([]byte)
	sub, err := nc.Subscribe(context.Background(), subject.ThreadSubscriptionList(siteID), func(_ context.Context, m *nats.Msg) {
		*got = append([]byte(nil), m.Data...)
		_ = m.Respond(reply)
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return got
}

func TestClient_GetThreadList(t *testing.T) {
	t.Run("happy path — request is marshaled and reply decodes", func(t *testing.T) {
		nc := startTestNATS(t)

		cursorAt := int64(1700000000000)
		reply, err := json.Marshal(model.ThreadSubscriptionListResponse{
			Items: []model.ThreadListItem{
				{SiteID: "site-a", RoomID: "r1", ThreadRoomID: "t1", LastMsgAt: 42, TCount: 3, Unread: true},
				{SiteID: "site-a", RoomID: "r2", ThreadRoomID: "t2", LastMsgAt: 41},
			},
			HasMore: true,
		})
		require.NoError(t, err)
		gotBody := respondThreadList(t, nc, "site-a", reply)

		out, err := New(nc).GetThreadList(context.Background(), "site-a", model.ThreadSubscriptionListRequest{
			Account:            "alice",
			CursorLastMsgAt:    &cursorAt,
			CursorThreadRoomID: "t9",
			Limit:              25,
		})
		require.NoError(t, err)

		var sent model.ThreadSubscriptionListRequest
		require.NoError(t, json.Unmarshal(*gotBody, &sent))
		assert.Equal(t, "alice", sent.Account)
		require.NotNil(t, sent.CursorLastMsgAt)
		assert.Equal(t, cursorAt, *sent.CursorLastMsgAt)
		assert.Equal(t, "t9", sent.CursorThreadRoomID)
		assert.Equal(t, 25, sent.Limit)

		require.Len(t, out.Items, 2)
		assert.Equal(t, "t1", out.Items[0].ThreadRoomID)
		assert.Equal(t, int64(42), out.Items[0].LastMsgAt)
		assert.Equal(t, 3, out.Items[0].TCount)
		assert.True(t, out.Items[0].Unread)
		assert.Equal(t, "t2", out.Items[1].ThreadRoomID)
		assert.True(t, out.HasMore)
	})

	t.Run("zero-value request — nil cursor is omitted from the wire body", func(t *testing.T) {
		nc := startTestNATS(t)

		reply, err := json.Marshal(model.ThreadSubscriptionListResponse{})
		require.NoError(t, err)
		gotBody := respondThreadList(t, nc, "site-a", reply)

		out, err := New(nc).GetThreadList(context.Background(), "site-a", model.ThreadSubscriptionListRequest{})
		require.NoError(t, err)
		assert.Empty(t, out.Items)
		assert.False(t, out.HasMore)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(*gotBody, &raw))
		assert.NotContains(t, raw, "cursorLastMsgAt")
		assert.NotContains(t, raw, "cursorThreadRoomId")
	})

	t.Run("empty page — empty items decode to an empty, non-error response", func(t *testing.T) {
		nc := startTestNATS(t)
		respondThreadList(t, nc, "site-a", []byte(`{"items":[],"hasMore":false}`))

		out, err := New(nc).GetThreadList(context.Background(), "site-a", model.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
		require.NoError(t, err)
		assert.Empty(t, out.Items)
		assert.False(t, out.HasMore)
	})

	t.Run("errcode reply — remote classification is relayed via errcode.Parse", func(t *testing.T) {
		nc := startTestNATS(t)
		envelope, err := json.Marshal(errcode.NotFound("thread not found"))
		require.NoError(t, err)
		respondThreadList(t, nc, "site-a", envelope)

		out, err := New(nc).GetThreadList(context.Background(), "site-a", model.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
		require.Error(t, err)
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, errcode.CodeNotFound, e.Code)
		assert.Equal(t, model.ThreadSubscriptionListResponse{}, out)
	})

	t.Run("malformed reply — non-JSON body surfaces a decode error", func(t *testing.T) {
		nc := startTestNATS(t)
		respondThreadList(t, nc, "site-a", []byte("not json at all"))

		out, err := New(nc).GetThreadList(context.Background(), "site-a", model.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
		require.Error(t, err)
		assert.ErrorContains(t, err, "decode thread-list response")
		assert.Equal(t, model.ThreadSubscriptionListResponse{}, out)
	})

	t.Run("type-mismatched reply — wrong JSON shape surfaces a decode error", func(t *testing.T) {
		nc := startTestNATS(t)
		respondThreadList(t, nc, "site-a", []byte(`{"items":"nope"}`))

		_, err := New(nc).GetThreadList(context.Background(), "site-a", model.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
		require.Error(t, err)
		assert.ErrorContains(t, err, "decode thread-list response")
	})

	t.Run("no responder — request failure is classified unavailable", func(t *testing.T) {
		nc := startTestNATS(t)
		// Intentionally no subscriber on the thread-list subject.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		out, err := New(nc).GetThreadList(ctx, "site-a", model.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
		require.Error(t, err)
		assert.ErrorContains(t, err, "thread-list rpc")
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, errcode.CodeUnavailable, e.Code)
		assert.Equal(t, model.ThreadSubscriptionListResponse{}, out)
	})

	t.Run("wrong site — a responder on another site's subject is not reached", func(t *testing.T) {
		nc := startTestNATS(t)
		reply, err := json.Marshal(model.ThreadSubscriptionListResponse{HasMore: true})
		require.NoError(t, err)
		respondThreadList(t, nc, "site-b", reply)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err = New(nc).GetThreadList(ctx, "site-a", model.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
		require.Error(t, err)
		assert.ErrorContains(t, err, "thread-list rpc")
	})
}
