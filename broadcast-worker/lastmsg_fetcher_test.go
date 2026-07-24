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

// fetchTestBefore is the walk ceiling passed by fetcher tests that don't assert on it.
var fetchTestBefore = time.Date(2026, 7, 1, 12, 30, 0, 0, time.UTC)

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
		got, _, err := fetcher.FetchLastMessage(context.Background(), roomID, fetchTestBefore)
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
		got, _, err := fetcher.FetchLastMessage(context.Background(), roomID, fetchTestBefore)
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
		got, _, err := fetcher.FetchLastMessage(context.Background(), roomID, fetchTestBefore)
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
		got, _, err := fetcher.FetchLastMessage(context.Background(), roomID, fetchTestBefore)
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
		got, _, err := fetcher.FetchLastMessage(context.Background(), roomID, fetchTestBefore)
		require.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "unmarshal last room message")
	})
}

// Decode failures must not embed reply fragments (message content for plaintext
// rooms) in the error, which is logged at the jsretry boundary.
func TestHistoryLastMessageFetcher_MalformedReply_ErrorOmitsPayload(t *testing.T) {
	nc := startTestNATS(t)
	siteID := "site-a"

	_, err := nc.Subscribe(context.Background(), subject.MsgRoomLast(siteID), func(_ context.Context, m *nats.Msg) {
		_ = m.Respond([]byte(`{"lastMessage": {"msg": "SECRET-CONTENT"`)) // truncated JSON carrying content
	})
	require.NoError(t, err)

	fetcher := newHistoryLastMessageFetcher(nc, siteID)
	got, _, err := fetcher.FetchLastMessage(context.Background(), "r1", fetchTestBefore)
	require.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "unmarshal last room message")
	assert.NotContains(t, err.Error(), "SECRET-CONTENT", "decode errors must not quote the reply body")
}

// The request carries the caller's walk ceiling, and the reply's pointer rides
// back alongside the preview.
func TestHistoryLastMessageFetcher_SendsBeforeAndParsesPointer(t *testing.T) {
	nc := startTestNATS(t)
	siteID := "site-a"
	sysAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

	var gotReq model.LastRoomMessageRequest
	_, err := nc.Subscribe(context.Background(), subject.MsgRoomLast(siteID), func(_ context.Context, m *nats.Msg) {
		_ = json.Unmarshal(m.Data, &gotReq)
		resp, _ := json.Marshal(model.LastRoomMessageResponse{
			LastMessage: &model.LastMessagePreview{MessageID: "m-user", SenderAccount: "alice", Msg: "hi", CreatedAt: sysAt.Add(-time.Minute)},
			Pointer:     &model.LastMessagePointer{MessageID: "m-sys", CreatedAt: sysAt},
		})
		_ = m.Respond(resp)
	})
	require.NoError(t, err)

	fetcher := newHistoryLastMessageFetcher(nc, siteID)
	preview, pointer, err := fetcher.FetchLastMessage(context.Background(), "r1", fetchTestBefore)
	require.NoError(t, err)
	assert.Equal(t, fetchTestBefore.UnixMilli(), gotReq.Before, "the delete-event time must ride the request as the walk ceiling")
	require.NotNil(t, preview)
	assert.Equal(t, "m-user", preview.MessageID)
	require.NotNil(t, pointer)
	assert.Equal(t, "m-sys", pointer.MessageID)
}

// Rolling deploy: a pre-pointer server replies without a pointer — the
// fetcher derives it from the preview so the caller always has one.
func TestHistoryLastMessageFetcher_DerivesPointerFromLegacyReply(t *testing.T) {
	nc := startTestNATS(t)
	siteID := "site-a"
	at := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

	_, err := nc.Subscribe(context.Background(), subject.MsgRoomLast(siteID), func(_ context.Context, m *nats.Msg) {
		resp, _ := json.Marshal(model.LastRoomMessageResponse{
			LastMessage: &model.LastMessagePreview{MessageID: "m-prev", SenderAccount: "bob", Msg: "hi", CreatedAt: at},
		})
		_ = m.Respond(resp)
	})
	require.NoError(t, err)

	fetcher := newHistoryLastMessageFetcher(nc, siteID)
	preview, pointer, err := fetcher.FetchLastMessage(context.Background(), "r1", fetchTestBefore)
	require.NoError(t, err)
	require.NotNil(t, preview)
	require.NotNil(t, pointer, "pointer must be derived from a legacy preview-only reply")
	assert.Equal(t, "m-prev", pointer.MessageID)
	assert.True(t, pointer.CreatedAt.Equal(at))
}
