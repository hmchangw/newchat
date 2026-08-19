//go:build integration

package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/outbox"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// connectFailoverJS dials url and returns its JetStream context, cleaning up on
// test end.
func connectFailoverJS(t *testing.T, url string) (*o11ynats.Conn, o11ynats.JetStream) {
	t.Helper()
	conn, err := natsutil.Connect(context.Background(), url, "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.NoError(t, err)
	t.Cleanup(func() { conn.NatsConn().Close() })

	js, err := conn.JetStream()
	require.NoError(t, err)
	return conn, js
}

// memberAddedEnvelope builds the InboxEvent envelope a peer would forward for a
// member_added, so both lanes carry byte-identical payloads.
func memberAddedEnvelope(t *testing.T, roomID, account string) []byte {
	t.Helper()
	now := time.Now().UTC().UnixMilli()
	change := model.MemberAddEvent{
		Type: "member_added", RoomID: roomID, Accounts: []string{account}, SiteID: "site-b",
		JoinedAt: now, Timestamp: now,
	}
	changeData, err := json.Marshal(change)
	require.NoError(t, err)

	evt := model.InboxEvent{
		Type: "member_added", SiteID: "site-b", DestSiteID: "site-a", Payload: changeData,
	}
	evtData, err := json.Marshal(evt)
	require.NoError(t, err)
	return evtData
}

// A federation event redirected to the buddy-hosted failover lane must land in
// the SAME database as one delivered through the primary lane. The whole
// correctness argument for the design is that the down site's own worker does
// the work against the down site's own store — so this asserts both lanes
// converge on one Mongo, not merely that each lane drains.
func TestFailoverLane_BothLanesApplyToSameStore(t *testing.T) {
	homeURL, buddyURL := testutil.NATSPair(t)
	db := setupMongo(t)
	ctx := context.Background()

	handler := newIntegrationHandler(t, db)
	mustInsertUser(t, db, &model.User{ID: "home-user", Account: "home-user", SiteID: "site-b"})
	mustInsertUser(t, db, &model.User{ID: "failover-user", Account: "failover-user", SiteID: "site-b"})

	_, homeJS := connectFailoverJS(t, homeURL)
	_, buddyJS := connectFailoverJS(t, buddyURL)

	primary := stream.Inbox("site-a")
	_, err := homeJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: primary.Name, Subjects: primary.Subjects,
	})
	require.NoError(t, err)

	cfg := config{SiteID: "site-a", MaxWorkers: 4,
		Buddy: natsutil.BuddyConfig{SiteID: "site-b", NatsURL: buddyURL}}
	cfg.Bootstrap.Enabled = true

	homeCons, err := homeJS.CreateOrUpdateConsumer(ctx, primary.Name,
		buildConsumerConfig(cfg.Consumer, cfg.SiteID))
	require.NoError(t, err)

	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup

	homeLane, err := startInboxLane(ctx, homeCons, &cfg, handler, sem, &wg)
	require.NoError(t, err)
	t.Cleanup(homeLane.Stop)

	// startFailoverLane creates the standby stream itself under Bootstrap.Enabled,
	// which is the dev path — production verifies and asserts placement instead.
	binder := &natsutil.FailoverBinder{
		Service: "inbox-worker", SiteID: cfg.SiteID, Buddy: cfg.Buddy,
		Bootstrap: cfg.Bootstrap.Enabled, MaxWorkers: cfg.MaxWorkers, Sem: sem, WG: &wg,
	}
	buddyLane, err := startFailoverLane(ctx, buddyJS, &cfg, handler, binder, sem, &wg)
	require.NoError(t, err)
	t.Cleanup(buddyLane.Stop)

	_, err = homeJS.Publish(ctx, subject.InboxExternal("site-a", model.InboxMemberAdded),
		memberAddedEnvelope(t, "room-home", "home-user"))
	require.NoError(t, err)

	_, err = buddyJS.Publish(ctx, subject.FailoverInboxExternal("site-a", model.InboxMemberAdded),
		memberAddedEnvelope(t, "room-failover", "failover-user"))
	require.NoError(t, err)

	subscriptionExists := func(roomID string) bool {
		err := db.Collection("subscriptions").
			FindOne(ctx, bson.M{"roomId": roomID}).Err()
		return err == nil
	}

	require.Eventually(t, func() bool {
		return subscriptionExists("room-home") && subscriptionExists("room-failover")
	}, 20*time.Second, 200*time.Millisecond,
		"both the home lane and the buddy failover lane must drain into the same store")
}

// Pins which error a JetStream publish returns when no stream captures the
// subject. outbox.ForwardWithFailover redirects ONLY on this class — a timeout
// is ambiguous and must park instead — so if neither candidate matches, the
// redirect is inert and the whole inbound-failover path silently never fires.
func TestPublishToMissingStream_ReturnsNoResponders(t *testing.T) {
	homeURL, _ := testutil.NATSPair(t)
	_, js := connectFailoverJS(t, homeURL)

	_, err := js.Publish(context.Background(),
		subject.InboxExternal("nonexistent-site", model.InboxMemberAdded), []byte(`{}`))

	require.Error(t, err)
	t.Logf("publish-to-missing-stream error: %v (%T)", err, err)
	assert.True(t,
		outbox.IsNoResponders(err),
		"publish to an uncaptured subject must be an unambiguous no-responders, got %v (%T)", err, err)
}
