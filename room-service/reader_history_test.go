package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/subject"
)

func startOtelNATS(t *testing.T) *o11ynats.Conn {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := o11ynats.Connect(context.Background(), ns.ClientURL(), noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

func TestHistoryMessageReader_GetMessageReadMeta(t *testing.T) {
	const (
		account   = "alice"
		roomID    = "room-1"
		siteID    = "site-a"
		messageID = "msg-uuid"
	)
	createdAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	t.Run("happy path returns room, createdAt, sender from history-service", func(t *testing.T) {
		nc := startOtelNATS(t)
		msg := cassandra.Message{
			MessageID: messageID,
			RoomID:    roomID,
			CreatedAt: createdAt,
			Sender:    cassandra.Participant{ID: "u-alice", Account: account},
		}
		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(msg)
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		r := newHistoryMessageReader(nc, siteID)
		meta, found, err := r.GetMessageReadMeta(context.Background(), account, roomID, messageID)

		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, roomID, meta.RoomID)
		assert.Equal(t, createdAt, meta.CreatedAt.UTC())
		assert.Equal(t, account, meta.Sender)
		assert.False(t, meta.ThreadOnly)
	})

	// Regression (#440): a message with a reaction still decodes. The reply carries
	// a non-empty reactions object (Reactions.MarshalJSON), which the old full
	// cassandra.Message decode rejected (struct-keyed map, no UnmarshalJSON).
	t.Run("message with reactions still decodes", func(t *testing.T) {
		nc := startOtelNATS(t)
		msg := cassandra.Message{
			MessageID: messageID,
			RoomID:    roomID,
			CreatedAt: createdAt,
			Sender:    cassandra.Participant{ID: "u-alice", Account: account},
			Reactions: cassandra.Reactions{
				{Emoji: "smile", UserAccount: "bob"}: {Account: "bob", ReactedAt: createdAt},
			},
		}
		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(msg)
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		r := newHistoryMessageReader(nc, siteID)
		meta, found, err := r.GetMessageReadMeta(context.Background(), account, roomID, messageID)

		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, roomID, meta.RoomID)
		assert.Equal(t, createdAt, meta.CreatedAt.UTC())
		assert.Equal(t, account, meta.Sender)
	})

	// #443: a thread-only reply (threadParentId set, not tshow) is flagged ThreadOnly
	// with its threadRoomId; a tshow reply is not (it's in the channel).
	t.Run("thread-only reply is flagged ThreadOnly with threadRoomId", func(t *testing.T) {
		nc := startOtelNATS(t)
		msg := cassandra.Message{
			MessageID: messageID, RoomID: roomID, CreatedAt: createdAt,
			Sender:         cassandra.Participant{ID: "u-alice", Account: account},
			ThreadParentID: "parent-1", ThreadRoomID: "thread-room-1", TShow: false,
		}
		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(msg)
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		r := newHistoryMessageReader(nc, siteID)
		meta, found, err := r.GetMessageReadMeta(context.Background(), account, roomID, messageID)
		require.NoError(t, err)
		assert.True(t, found)
		assert.True(t, meta.ThreadOnly)
		assert.Equal(t, "thread-room-1", meta.ThreadRoomID)
	})

	t.Run("tshow reply is not ThreadOnly", func(t *testing.T) {
		nc := startOtelNATS(t)
		msg := cassandra.Message{
			MessageID: messageID, RoomID: roomID, CreatedAt: createdAt,
			Sender:         cassandra.Participant{ID: "u-alice", Account: account},
			ThreadParentID: "parent-1", ThreadRoomID: "thread-room-1", TShow: true,
		}
		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(msg)
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		r := newHistoryMessageReader(nc, siteID)
		meta, found, err := r.GetMessageReadMeta(context.Background(), account, roomID, messageID)
		require.NoError(t, err)
		assert.True(t, found)
		assert.False(t, meta.ThreadOnly)
	})

	t.Run("history NotFound maps to found=false with no error", func(t *testing.T) {
		nc := startOtelNATS(t)
		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(errcode.NotFound("message not found"))
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		r := newHistoryMessageReader(nc, siteID)
		_, found, err := r.GetMessageReadMeta(context.Background(), account, roomID, messageID)

		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("other history errcode is propagated", func(t *testing.T) {
		nc := startOtelNATS(t)
		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(errcode.Forbidden("message is outside access window",
				errcode.WithReason(errcode.MessageOutsideAccessWindow)))
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		r := newHistoryMessageReader(nc, siteID)
		_, found, err := r.GetMessageReadMeta(context.Background(), account, roomID, messageID)

		assert.False(t, found)
		require.Error(t, err)
		assert.True(t, errcode.HasReason(err, errcode.MessageOutsideAccessWindow))
	})

	t.Run("malformed reply surfaces an unmarshal error", func(t *testing.T) {
		nc := startOtelNATS(t)
		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			_ = m.Respond([]byte("not json"))
		})
		require.NoError(t, err)

		r := newHistoryMessageReader(nc, siteID)
		_, found, err := r.GetMessageReadMeta(context.Background(), account, roomID, messageID)

		assert.False(t, found)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("no responder degrades to unavailable", func(t *testing.T) {
		nc := startOtelNATS(t) // no subscriber registered

		r := newHistoryMessageReader(nc, siteID)
		_, found, err := r.GetMessageReadMeta(context.Background(), account, roomID, messageID)

		assert.False(t, found)
		require.Error(t, err)
		assert.True(t, errcode.HasReason(err, errcode.RoomReadReceiptsUnavailable))
	})
}
