package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestHistoryLastMessageFetcher_FetchLastMessage(t *testing.T) {
	const (
		roomID = "room-1"
		siteID = "site-a"
	)
	survivorAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

	t.Run("happy path — decodes the surviving preview", func(t *testing.T) {
		nc := startTestNATS(t)

		resp := model.LastRoomMessageResponse{LastMessage: &model.LastMessagePreview{
			MessageID:       "m-last",
			SenderAccount:   "bob",
			SenderName:      "Bob Chen",
			Msg:             "still here",
			CreatedAt:       survivorAt,
			AttachmentCount: 2,
		}}
		_, err := nc.Subscribe(context.Background(), subject.MsgRoomLast(siteID), func(_ context.Context, m *nats.Msg) {
			var req model.LastRoomMessageRequest
			require.NoError(t, json.Unmarshal(m.Data, &req))
			assert.Equal(t, roomID, req.RoomID)
			data, _ := json.Marshal(resp)
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		fetcher := newHistoryLastMessageFetcher(nc, siteID)
		got, err := fetcher.FetchLastMessage(context.Background(), roomID)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "m-last", got.MessageID)
		assert.Equal(t, "bob", got.SenderAccount)
		assert.Equal(t, "Bob Chen", got.SenderName)
		assert.Equal(t, "still here", got.Msg)
		assert.Equal(t, 2, got.AttachmentCount)
		assert.Equal(t, survivorAt, got.CreatedAt.UTC())
	})

	t.Run("room with no surviving message — nil preview, nil error", func(t *testing.T) {
		nc := startTestNATS(t)

		_, err := nc.Subscribe(context.Background(), subject.MsgRoomLast(siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(model.LastRoomMessageResponse{})
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		fetcher := newHistoryLastMessageFetcher(nc, siteID)
		got, err := fetcher.FetchLastMessage(context.Background(), roomID)
		require.NoError(t, err)
		assert.Nil(t, got, "empty response must map to a nil preview, not an error")
	})

	t.Run("history returns errcode error envelope — returns typed error", func(t *testing.T) {
		nc := startTestNATS(t)

		_, err := nc.Subscribe(context.Background(), subject.MsgRoomLast(siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(errcode.NotFound("room not found"))
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		fetcher := newHistoryLastMessageFetcher(nc, siteID)
		got, err := fetcher.FetchLastMessage(context.Background(), roomID)
		require.Error(t, err)
		assert.Nil(t, got)
		var ee *errcode.Error
		require.ErrorAs(t, err, &ee)
		assert.Equal(t, errcode.CodeNotFound, ee.Code)
	})

	t.Run("no responder — returns error", func(t *testing.T) {
		nc := startTestNATS(t)
		// Intentionally no subscriber: nc.Request must fail with "no responders".

		fetcher := newHistoryLastMessageFetcher(nc, siteID)
		got, err := fetcher.FetchLastMessage(context.Background(), roomID)
		require.Error(t, err)
		assert.Nil(t, got)
	})

	t.Run("malformed response body — returns unmarshal error", func(t *testing.T) {
		nc := startTestNATS(t)

		_, err := nc.Subscribe(context.Background(), subject.MsgRoomLast(siteID), func(_ context.Context, m *nats.Msg) {
			_ = m.Respond([]byte("not json"))
		})
		require.NoError(t, err)

		fetcher := newHistoryLastMessageFetcher(nc, siteID)
		got, err := fetcher.FetchLastMessage(context.Background(), roomID)
		require.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "unmarshal last room message")
	})
}

// Decode failures must never embed reply fragments in the error: for
// plaintext rooms the reply body IS message content, and the wrapped error is
// logged at the jsretry boundary.
func TestHistoryLastMessageFetcher_MalformedReply_ErrorOmitsPayload(t *testing.T) {
	nc := startTestNATS(t)
	siteID := "site-a"

	_, err := nc.Subscribe(context.Background(), subject.MsgRoomLast(siteID), func(_ context.Context, m *nats.Msg) {
		_ = m.Respond([]byte(`{"lastMessage": {"msg": "SECRET-CONTENT"`)) // truncated JSON carrying content
	})
	require.NoError(t, err)

	fetcher := newHistoryLastMessageFetcher(nc, siteID)
	got, err := fetcher.FetchLastMessage(context.Background(), "r1")
	require.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "unmarshal last room message")
	assert.NotContains(t, err.Error(), "SECRET-CONTENT", "decode errors must not quote the reply body")
}
