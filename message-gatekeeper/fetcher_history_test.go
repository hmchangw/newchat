package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/subject"
)

func startTestNATS(t *testing.T) *o11ynats.Conn {
	t.Helper()
	opts := &natsserver.Options{Port: -1}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := o11ynats.Connect(context.Background(), ns.ClientURL(), noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

func TestHistoryParentFetcher_FetchQuotedParent(t *testing.T) {
	const (
		account   = "alice"
		roomID    = "room-1"
		siteID    = "site-a"
		messageID = "parent-msg-uuid"
		baseURL   = "http://localhost:3000"
	)
	parentCreatedAt := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	threadParentCreatedAt := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)

	t.Run("happy path — returns projected snapshot with thread context and messageLink", func(t *testing.T) {
		nc := startTestNATS(t)

		parent := cassandra.Message{
			MessageID:             messageID,
			RoomID:                roomID,
			Sender:                cassandra.Participant{ID: "u-bob", Account: "bob", EngName: "Bob Chen"},
			CreatedAt:             parentCreatedAt,
			Msg:                   "a reply inside thread T",
			Mentions:              []cassandra.Participant{{ID: "u-carol", Account: "carol", EngName: "Carol Lee"}},
			ThreadParentID:        "thread-parent-uuid",
			ThreadParentCreatedAt: &threadParentCreatedAt,
		}

		// Stand up a stub responder on the exact subject the fetcher should publish on.
		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(parent)
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		fetcher := newHistoryParentFetcher(nc, baseURL)
		got, err := fetcher.FetchQuotedParent(context.Background(), account, roomID, siteID, messageID)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, messageID, got.MessageID)
		assert.Equal(t, roomID, got.RoomID)
		assert.Equal(t, "a reply inside thread T", got.Msg)
		assert.Equal(t, "bob", got.Sender.Account)
		assert.Equal(t, parentCreatedAt, got.CreatedAt.UTC())
		require.Len(t, got.Mentions, 1)
		assert.Equal(t, "carol", got.Mentions[0].Account)
		assert.Equal(t, baseURL+"/"+roomID+"/"+messageID, got.MessageLink)
		assert.Equal(t, "thread-parent-uuid", got.ThreadParentID)
		require.NotNil(t, got.ThreadParentCreatedAt)
		assert.Equal(t, threadParentCreatedAt, got.ThreadParentCreatedAt.UTC())
	})

	t.Run("captures the parent's decoded attachments into the snapshot", func(t *testing.T) {
		nc := startTestNATS(t)
		parent := cassandra.Message{
			MessageID:          messageID,
			RoomID:             roomID,
			Sender:             cassandra.Participant{ID: "u-bob", Account: "bob"},
			CreatedAt:          parentCreatedAt,
			Msg:                "has an attachment",
			DecodedAttachments: []cassandra.Attachment{{ID: "f1", Title: "a.png", Type: "file"}},
		}
		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(parent)
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		fetcher := newHistoryParentFetcher(nc, baseURL)
		got, err := fetcher.FetchQuotedParent(context.Background(), account, roomID, siteID, messageID)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Len(t, got.DecodedAttachments, 1)
		assert.Equal(t, "f1", got.DecodedAttachments[0].ID)
	})

	t.Run("history returns errcode error envelope — returns error", func(t *testing.T) {
		nc := startTestNATS(t)

		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(errcode.NotFound("message not found"))
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		fetcher := newHistoryParentFetcher(nc, baseURL)
		got, err := fetcher.FetchQuotedParent(context.Background(), account, roomID, siteID, messageID)
		require.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "message not found")
	})

	t.Run("no responder — returns error", func(t *testing.T) {
		nc := startTestNATS(t)
		// Intentionally no subscriber: nc.Request must fail with "no responders".

		fetcher := newHistoryParentFetcher(nc, baseURL)
		got, err := fetcher.FetchQuotedParent(context.Background(), account, roomID, siteID, messageID)
		require.Error(t, err)
		assert.Nil(t, got)
	})
}

func TestHistoryParentFetcher_FetchForwardedSource(t *testing.T) {
	const (
		account   = "alice"
		srcRoomID = "r-src"
		siteID    = "site-a"
		messageID = "01970a4f8c2d7c9aSRCM"
		baseURL   = "https://chat.example.com"
	)

	t.Run("success projects snapshot fields and reject signals", func(t *testing.T) {
		nc := startTestNATS(t)
		fetcher := newHistoryParentFetcher(nc, baseURL)

		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, srcRoomID, siteID), func(_ context.Context, m *nats.Msg) {
			// history-service replies with the full cassandra.Message JSON;
			// include reject-signal fields to prove the projection surfaces them.
			_ = m.Respond([]byte(`{
				"roomId":"r-src",
				"sender":{"id":"u5","account":"eve"},
				"createdAt":"2026-08-01T09:00:00Z",
				"msg":"original body",
				"mentions":[{"id":"u2","account":"bob"}],
				"threadParentId":"01970a4f8c2d7c9aTHRD",
				"deleted":false,
				"attachments":[{"id":"f1","title":"a.png","type":"file"}],
				"card":{"template":"approval"},
				"forwardedMessage":{"messageId":"m-deeper"}
			}`))
		})
		require.NoError(t, err)

		got, err := fetcher.FetchForwardedSource(context.Background(), account, srcRoomID, siteID, messageID)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "r-src", got.RoomID)
		assert.Equal(t, "eve", got.Sender.Account)
		assert.Equal(t, "original body", got.Msg)
		assert.Len(t, got.Mentions, 1)
		assert.Equal(t, "01970a4f8c2d7c9aTHRD", got.ThreadParentID)
		assert.NotEmpty(t, got.Attachments, "attachment presence must survive the projection")
		assert.NotEmpty(t, got.Card, "card presence must survive the projection")
		assert.NotEmpty(t, got.ForwardedMessage, "chain presence must survive the projection")
	})

	t.Run("errcode envelope propagates typed error", func(t *testing.T) {
		nc := startTestNATS(t)
		fetcher := newHistoryParentFetcher(nc, baseURL)

		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, srcRoomID, siteID), func(_ context.Context, m *nats.Msg) {
			_ = m.Respond([]byte(`{"code":"not_found","error":"message not found"}`))
		})
		require.NoError(t, err)

		got, err := fetcher.FetchForwardedSource(context.Background(), account, srcRoomID, siteID, messageID)
		assert.Nil(t, got)
		var ee *errcode.Error
		require.ErrorAs(t, err, &ee)
		assert.Equal(t, errcode.CodeNotFound, ee.Code)
	})

	t.Run("no responder wraps transport error", func(t *testing.T) {
		nc := startTestNATS(t)
		fetcher := newHistoryParentFetcher(nc, baseURL)

		got, err := fetcher.FetchForwardedSource(context.Background(), account, "r-nobody", siteID, messageID)
		assert.Nil(t, got)
		require.Error(t, err)
		var ee *errcode.Error
		assert.False(t, errors.As(err, &ee), "transport failure must stay a bare error")
	})
}
