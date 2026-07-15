package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
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

func TestHistoryParentFetcher_FetchParent(t *testing.T) {
	const (
		account   = "alice"
		roomID    = "room-1"
		siteID    = "site-a"
		messageID = "parent-msg-uuid"
	)
	parentCreatedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("happy path — projects the parent's author and createdAt", func(t *testing.T) {
		nc := startTestNATS(t)

		parent := cassandra.Message{
			MessageID: messageID,
			RoomID:    roomID,
			Sender:    cassandra.Participant{ID: "u-carol", Account: "carol", EngName: "Carol Lee"},
			CreatedAt: parentCreatedAt,
			Msg:       "the thread's root message",
		}
		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(parent)
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		fetcher := newHistoryParentFetcher(nc)
		got, err := fetcher.FetchParent(context.Background(), account, roomID, siteID, messageID)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "carol", got.SenderAccount)
		assert.Equal(t, parentCreatedAt, got.CreatedAt.UTC())
	})

	t.Run("history returns errcode error envelope — returns typed error", func(t *testing.T) {
		nc := startTestNATS(t)

		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(errcode.NotFound("message not found"))
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		fetcher := newHistoryParentFetcher(nc)
		got, err := fetcher.FetchParent(context.Background(), account, roomID, siteID, messageID)
		require.Error(t, err)
		assert.Nil(t, got)
		var ee *errcode.Error
		require.ErrorAs(t, err, &ee)
		assert.Equal(t, errcode.CodeNotFound, ee.Code)
	})

	t.Run("no responder — returns error", func(t *testing.T) {
		nc := startTestNATS(t)
		// Intentionally no subscriber: nc.Request must fail with "no responders".

		fetcher := newHistoryParentFetcher(nc)
		got, err := fetcher.FetchParent(context.Background(), account, roomID, siteID, messageID)
		require.Error(t, err)
		assert.Nil(t, got)
	})

	t.Run("malformed response body — returns unmarshal error", func(t *testing.T) {
		nc := startTestNATS(t)

		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			_ = m.Respond([]byte("not json"))
		})
		require.NoError(t, err)

		fetcher := newHistoryParentFetcher(nc)
		got, err := fetcher.FetchParent(context.Background(), account, roomID, siteID, messageID)
		require.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "unmarshal parent message")
	})
}
