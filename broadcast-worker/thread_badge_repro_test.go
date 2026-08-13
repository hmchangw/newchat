//go:build integration

// Live end-to-end repro of the thread-metadata badge lane over a real
// (embedded) NATS server: message-worker's exact badge publish →
// broadcast-worker's queue subscription → thread_metadata_updated on the
// client-facing subjects. Mirrors the wiring in broadcast-worker/main.go
// (same wildcard subject + queue group) and the exact payload built by
// message-worker's publishThreadReplyEvent.
package main

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// coreNATSPublisher adapts a plain *nats.Conn to the Publisher interface,
// standing in for main.go's o11ynats-wrapped natsPublisher.
type coreNATSPublisher struct{ nc *nats.Conn }

func (p *coreNATSPublisher) Publish(_ context.Context, subj string, data []byte) error {
	return p.nc.Publish(subj, data)
}

func startEmbeddedNATS(t *testing.T) *natsserver.Server {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1})
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(10*time.Second), "embedded nats-server never became ready")
	t.Cleanup(srv.Shutdown)
	return srv
}

// publishBadgeAsMessageWorker publishes the EventThreadReplyAdded badge event
// byte-for-byte the way message-worker's publishThreadReplyEvent does.
func publishBadgeAsMessageWorker(t *testing.T, nc *nats.Conn, siteID, roomID, replyID, parentID string, tcount int) {
	t.Helper()
	tlm := time.Now().UTC()
	evt := model.MessageEvent{
		Event: model.EventThreadReplyAdded,
		Message: model.Message{
			ID:                    replyID,
			RoomID:                roomID,
			ThreadParentMessageID: parentID,
		},
		SiteID:             siteID,
		Timestamp:          time.Now().UTC().UnixMilli(),
		NewTCount:          &tcount,
		NewThreadLastMsgAt: &tlm,
	}
	data, err := sonic.Marshal(evt)
	require.NoError(t, err)
	require.NoError(t, nc.Publish(subject.ServerBroadcastThreadTCount(siteID), data))
	require.NoError(t, nc.Flush())
}

func expectThreadMetadata(t *testing.T, sub *nats.Subscription, wantRoom string, wantTcount int) {
	t.Helper()
	msg, err := sub.NextMsg(3 * time.Second)
	require.NoError(t, err, "no event arrived on %s", sub.Subject)
	var got model.ThreadMetadataUpdatedEvent
	require.NoError(t, sonic.Unmarshal(msg.Data, &got))
	assert.Equal(t, model.RoomEventThreadMetadataUpdated, got.Type)
	assert.Equal(t, wantRoom, got.RoomID)
	assert.Equal(t, wantTcount, got.NewTCount)
	assert.Equal(t, model.ThreadActionReplyAdded, got.Action)
}

func TestThreadBadgeLane_EndToEnd_OverRealNATS(t *testing.T) {
	const siteID = "site-local"
	srv := startEmbeddedNATS(t)

	connect := func(name string) *nats.Conn {
		nc, err := nats.Connect(srv.ClientURL(), nats.Name(name))
		require.NoError(t, err)
		t.Cleanup(nc.Close)
		return nc
	}
	workerNC := connect("broadcast-worker")
	msgWorkerNC := connect("message-worker")
	clientNC := connect("frontend")

	sameSite := false
	dmRoom := &model.Room{ID: "dm-1", Type: model.RoomTypeDM, SiteID: siteID,
		Accounts: []string{"alice", "bob"}}
	chanRoom := &model.Room{ID: "chan-1", Type: model.RoomTypeChannel, SiteID: siteID,
		CrossSite: &sameSite}

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "dm-1").Return(dmRoom, nil).AnyTimes()
	store.EXPECT().GetRoom(gomock.Any(), "chan-1").Return(chanRoom, nil).AnyTimes()

	// RouteDual mirrors docker-local (ROOM_SUBJECT_MODE=dual).
	handler := NewHandler(store, nil, &coreNATSPublisher{nc: workerNC}, nil, nil, false, subject.RouteDual)

	// Same subject + queue group as broadcast-worker/main.go.
	_, err := workerNC.QueueSubscribe(subject.ServerBroadcastWildcard(siteID), "broadcast-worker",
		func(msg *nats.Msg) { handler.HandleServerBroadcast(context.Background(), msg.Data) })
	require.NoError(t, err)
	require.NoError(t, workerNC.Flush())

	// Frontend-side subscriptions (see chat-frontend subjects.ts).
	aliceSub, err := clientNC.SubscribeSync("chat.user.alice.event.room")
	require.NoError(t, err)
	bobSub, err := clientNC.SubscribeSync("chat.user.bob.event.room")
	require.NoError(t, err)
	chanLocalSub, err := clientNC.SubscribeSync("chat.local.room.chan-1.event")
	require.NoError(t, err)
	chanGlobalSub, err := clientNC.SubscribeSync("chat.room.chan-1.event")
	require.NoError(t, err)
	require.NoError(t, clientNC.Flush())

	t.Run("DM room fans out to both member user subjects", func(t *testing.T) {
		publishBadgeAsMessageWorker(t, msgWorkerNC, siteID, "dm-1", "m-reply-1", "m-parent-1", 3)
		expectThreadMetadata(t, aliceSub, "dm-1", 3)
		expectThreadMetadata(t, bobSub, "dm-1", 3)
	})

	t.Run("channel room publishes to local and global room subjects (dual mode)", func(t *testing.T) {
		publishBadgeAsMessageWorker(t, msgWorkerNC, siteID, "chan-1", "m-reply-2", "m-parent-2", 7)
		expectThreadMetadata(t, chanLocalSub, "chan-1", 7)
		expectThreadMetadata(t, chanGlobalSub, "chan-1", 7)
	})
}
