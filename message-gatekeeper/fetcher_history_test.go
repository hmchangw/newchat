package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/subject"
)

func startTestNATS(t *testing.T) *otelnats.Conn {
	t.Helper()
	opts := &natsserver.Options{Port: -1}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := otelnats.Connect(ns.ClientURL())
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
		_, err := nc.Subscribe(subject.MsgGet(account, roomID, siteID), func(m otelnats.Msg) {
			data, _ := json.Marshal(parent)
			_ = m.Msg.Respond(data)
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
		_, err := nc.Subscribe(subject.MsgGet(account, roomID, siteID), func(m otelnats.Msg) {
			data, _ := json.Marshal(parent)
			_ = m.Msg.Respond(data)
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

		_, err := nc.Subscribe(subject.MsgGet(account, roomID, siteID), func(m otelnats.Msg) {
			data, _ := json.Marshal(errcode.NotFound("message not found"))
			_ = m.Msg.Respond(data)
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
