//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/userstore"
)

// seedSameSiteRoom inserts a room whose CrossSite is explicitly false. The flag
// must be set, not left nil: a nil CrossSite is the fail-safe that routes global
// anyway, and a test built on it would pass without the routing change.
func seedSameSiteRoom(t *testing.T, ctx context.Context, roomID string) (Store, userstore.UserStore, *roomkeystore.VersionedKeyPair) {
	t.Helper()
	db := setupMongo(t)
	sameSite := false
	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: roomID, Name: "general", Type: model.RoomTypeChannel, UserCount: 2,
		SiteID: "site-a", CrossSite: &sameSite,
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: roomID},
		model.Subscription{ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: roomID},
	})
	require.NoError(t, err)
	seedUsers(t, db)

	return NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"),
			db.Collection("thread_rooms"), db.Collection("users"), nil, 0, 0, nil),
		userstore.NewMongoStore(db.Collection("users")),
		testRoomKey(t)
}

func connectBuddyNATS(t *testing.T, url string) *o11ynats.Conn {
	t.Helper()
	conn, err := natsutil.Connect(context.Background(), url, "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.NoError(t, err)
	t.Cleanup(func() { conn.NatsConn().Close() })
	return conn
}

func channelMessageEvent(t *testing.T, roomID, msgID string) []byte {
	t.Helper()
	data, err := json.Marshal(model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: msgID, RoomID: roomID, UserID: "u1", UserAccount: "alice",
			Content: "hello", CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
		},
	})
	require.NoError(t, err)
	return data
}

// The end-to-end property this whole plan exists for: a subscriber on the BUDDY
// cluster receives a SAME-SITE room's message when it came through the failover
// lane. Without lane-aware routing the publish goes to chat.local.room.>, which
// is filtered at the leaf node and never crosses a gateway — the client hears
// nothing, and nothing anywhere logs an error.
func TestFailoverLane_SameSiteRoomReachesRemoteSubscriber(t *testing.T) {
	_, buddyURL := testutil.NATSPair(t)
	ctx := context.Background()

	store, us, key := seedSameSiteRoom(t, ctx, "r1")
	buddy := connectBuddyNATS(t, buddyURL)

	// The displaced client: connected to the buddy, subscribed to the global
	// root, as a client in failover mode is.
	client, err := nats.Connect(buddyURL)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	globalSub, err := client.SubscribeSync(subject.RoomEvent("r1", true))
	require.NoError(t, err)
	localSub, err := client.SubscribeSync(subject.RoomEvent("r1", false))
	require.NoError(t, err)
	require.NoError(t, client.Flush())

	// The failover-lane handler: publishes on the buddy, resolver pinned to the
	// failover lane. Configured mode is local, which is the case that breaks.
	h := NewHandler(store, us, corePublisher(natsutil.CorePublishFunc(buddy, natsmetrics.Publisher{})), &fakeRoomKeyProvider{pair: key},
		defaultParentFetcher, true,
		subject.NewLaneRouter(subject.RouteLocal, subject.LaneFailover, nil,
			subject.DefaultFailoverRevertGrace))

	require.NoError(t, h.HandleMessage(ctx, channelMessageEvent(t, "r1", "m1")))

	msg, err := globalSub.NextMsg(10 * time.Second)
	require.NoError(t, err,
		"a same-site room's message must reach a remote subscriber on the failover lane")
	roomEvt, decoded := decryptClientMessage(t, msg.Data, key)
	assert.Equal(t, "site-a", roomEvt.SiteID)
	require.NotNil(t, decoded)
	assert.Equal(t, "m1", decoded.ID)

	// Forcing global means global only — the local root must carry nothing.
	_, err = localSub.NextMsg(500 * time.Millisecond)
	assert.ErrorIs(t, err, nats.ErrTimeout)
}

// The control that proves the test above is load-bearing: the same message on
// the home lane in local mode goes to the local root, which the remote
// subscriber never sees. This is the silent failure the plan closes.
func TestHomeLane_SameSiteRoomStaysOnTheLocalRoot(t *testing.T) {
	_, buddyURL := testutil.NATSPair(t)
	ctx := context.Background()

	store, us, key := seedSameSiteRoom(t, ctx, "r2")
	buddy := connectBuddyNATS(t, buddyURL)

	client, err := nats.Connect(buddyURL)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	globalSub, err := client.SubscribeSync(subject.RoomEvent("r2", true))
	require.NoError(t, err)
	localSub, err := client.SubscribeSync(subject.RoomEvent("r2", false))
	require.NoError(t, err)
	require.NoError(t, client.Flush())

	h := NewHandler(store, us, corePublisher(natsutil.CorePublishFunc(buddy, natsmetrics.Publisher{})), &fakeRoomKeyProvider{pair: key},
		defaultParentFetcher, true,
		subject.NewLaneRouter(subject.RouteLocal, subject.LaneHome, nil,
			subject.DefaultFailoverRevertGrace))

	require.NoError(t, h.HandleMessage(ctx, channelMessageEvent(t, "r2", "m2")))

	_, err = localSub.NextMsg(10 * time.Second)
	require.NoError(t, err, "home-lane local-mode routing is unchanged")

	_, err = globalSub.NextMsg(500 * time.Millisecond)
	assert.ErrorIs(t, err, nats.ErrTimeout)
}

// Recovery: inside the revert grace window the home lane carries BOTH roots, so
// a client that has not yet reverted keeps receiving while one that has also
// does.
func TestHomeLane_DualPublishesDuringTheRevertGraceWindow(t *testing.T) {
	_, buddyURL := testutil.NATSPair(t)
	ctx := context.Background()

	store, us, key := seedSameSiteRoom(t, ctx, "r3")
	buddy := connectBuddyNATS(t, buddyURL)

	client, err := nats.Connect(buddyURL)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	globalSub, err := client.SubscribeSync(subject.RoomEvent("r3", true))
	require.NoError(t, err)
	localSub, err := client.SubscribeSync(subject.RoomEvent("r3", false))
	require.NoError(t, err)
	require.NoError(t, client.Flush())

	restored := time.Now().UTC().Add(-time.Minute)
	h := NewHandler(store, us, corePublisher(natsutil.CorePublishFunc(buddy, natsmetrics.Publisher{})), &fakeRoomKeyProvider{pair: key},
		defaultParentFetcher, true,
		subject.NewLaneRouter(subject.RouteLocal, subject.LaneHome,
			func() time.Time { return restored }, subject.DefaultFailoverRevertGrace))

	require.NoError(t, h.HandleMessage(ctx, channelMessageEvent(t, "r3", "m3")))

	_, err = localSub.NextMsg(10 * time.Second)
	require.NoError(t, err, "stragglers still on the local root must keep receiving")
	_, err = globalSub.NextMsg(10 * time.Second)
	require.NoError(t, err, "clients that reverted must receive too")
}
