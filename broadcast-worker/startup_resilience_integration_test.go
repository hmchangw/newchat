//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/userstore"
)

// TestStartup_ReachesServingStateWithMongoUnreachable walks the service's real
// Mongo boot sequence with MongoDB down: connect, build the store, run the
// index step. Before this change each of those steps killed the process; all
// three must now complete so the worker can go on to consume from JetStream.
func TestStartup_ReachesServingStateWithMongoUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Gate 1: connect must not fail.
	client, err := mongoutil.Connect(ctx, testutil.UnreachableMongoURI(t), "", "", mongoutil.WithLazyConnect())
	require.NoError(t, err, "service must connect lazily while MongoDB is unreachable")
	t.Cleanup(func() { mongoutil.Disconnect(context.Background(), client) })

	db := client.Database("broadcast_worker_startup_test")
	store := NewMongoStore(
		db.Collection("rooms"),
		db.Collection("subscriptions"),
		db.Collection("thread_rooms"),
		nil, // no Valkey L2 tier
		time.Minute,
	)
	require.NotNil(t, store, "store must be constructible with MongoDB unreachable")

	// Gate 2: an unreachable MongoDB is transient, so the index step must warn
	// and report success, not hand the caller a reason to exit.
	require.NoError(t,
		mongoutil.EnsureIndexes(ctx, mongoutil.Step("broadcast-worker store", store.EnsureIndexes)),
		"failed index creation must not stop startup")

	// Serving state: the store answers requests with an error rather than
	// hanging, so the consumer loop can Nak and retry instead of wedging.
	opCtx, opCancel := context.WithTimeout(ctx, 5*time.Second)
	defer opCancel()
	start := time.Now()
	_, err = store.GetRoom(opCtx, "room-1")
	elapsed := time.Since(start)

	assert.Error(t, err, "reads must fail while MongoDB is unreachable")
	assert.Less(t, elapsed, 8*time.Second, "reads must fail bounded, not hang")
}

// TestStartup_ConsumerRedeliversUntilMongoRecovers is the half that booting
// during an outage is for. Reaching a serving state is worth nothing if the
// consumer then mishandles the events it pulls, and the two ways to get that
// wrong are both silent: Ack a message the store never processed (the event is
// gone for good), or hot-loop Naks that burn MaxDeliver before MongoDB returns
// (same outcome, slower). So this drives the worker's real consumer — the
// production consumer config, broadcastProcessor, consumeLoop — against a
// MongoDB that is down and then comes back, with no restart in between.
func TestStartup_ConsumerRedeliversUntilMongoRecovers(t *testing.T) {
	const siteID = "site-outage"
	ctx := context.Background()

	// Seed through a direct connection: the room has to exist for the post-
	// recovery delivery to succeed, and the worker's own client cannot write it
	// while the outage is on.
	seed := testutil.MongoDB(t, "broadcast_worker_outage_test")
	_, err := seed.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r1", Name: "general", Type: model.RoomTypeChannel, UserCount: 2, SiteID: siteID,
	})
	require.NoError(t, err)
	_, err = seed.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1"},
		model.Subscription{ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1"},
	})
	require.NoError(t, err)
	seedUsers(t, seed)

	// The worker reaches the same database through an endpoint that is down.
	outage := testutil.NewMongoOutage(t, testutil.MongoURI(t))
	client, err := mongoutil.Connect(ctx, outage.URI(), "", "", mongoutil.WithLazyConnect())
	require.NoError(t, err, "worker must boot while MongoDB is unreachable")
	t.Cleanup(func() { mongoutil.Disconnect(context.Background(), client) })

	db := client.Database(seed.Name())
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	pub := &recordingPublisher{}
	handler := NewHandler(store, userstore.NewMongoStore(db.Collection("users")), pub, nil,
		defaultParentFetcher, false, subject.RouteGlobal)

	js, cons, iter, subj := startCanonicalConsumer(t, siteID)

	// Count deliveries around the real processor rather than replacing it, so
	// the Ack/Nak decision under test stays broadcastProcessor's.
	var deliveries atomic.Int32
	process := broadcastProcessor(handler)
	counted := func(msgCtx context.Context, msg jetstream.Msg) {
		deliveries.Add(1)
		process(msgCtx, msg)
	}

	// The production composition: natsmetrics.Consume driving
	// guardedProcessor(broadcastProcessor(…)), same as main.go wires.
	metrics, _ := newTestBroadcastMetrics(t)
	consumer := metrics.Consumer(natsmetrics.ConsumerConfig{
		ServiceName: "broadcast-worker", Site: siteID,
		Stream: stream.MessagesCanonical(siteID).Name, Consumer: "broadcast-worker",
	})
	consumer.LoopStarted(ctx)

	var wg sync.WaitGroup
	go natsmetrics.Consume(ctx, iter, consumer, 4, consumerMaxDeliver, &wg,
		func(msg jetstream.Msg) natsmetrics.EventType { return natsmetrics.EventTypeFromSubject(msg.Subject()) },
		guardedProcessor(counted))

	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: siteID,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice", Content: "hello",
			CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
		},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	_, err = js.Publish(ctx, subj, data)
	require.NoError(t, err)

	// While MongoDB is down the event must come back. A second delivery proves
	// the store failure was Nak'd rather than Acked away.
	require.Eventually(t, func() bool { return deliveries.Load() >= 2 }, 30*time.Second, 100*time.Millisecond,
		"the event was never redelivered — a failed Mongo read was Acked, losing the event")
	assert.Empty(t, pub.getRecords(), "nothing may be broadcast from a read that failed")

	// The retries are spaced, not a hot loop: a Nak() without delay would burn
	// the consumer's MaxDeliver within milliseconds of the first failure.
	assert.Less(t, deliveries.Load(), int32(consumerMaxDeliver),
		"retries are spaced by jsretry backoff, so MaxDeliver must not be exhausted this early")

	outage.Restore(t)

	// Same process, same consumer: the next redelivery must now succeed.
	require.Eventually(t, func() bool { return len(pub.getRecords()) > 0 }, 60*time.Second, 200*time.Millisecond,
		"the event was never processed after MongoDB returned")

	records := pub.getRecords()
	require.Len(t, records, 1, "the recovered delivery must broadcast exactly once")
	assert.Equal(t, subject.RoomEvent("r1", true), records[0].subject)

	// And it is Acked: an unacked success would sit in ack-pending until AckWait
	// expired and then broadcast the same event a second time.
	require.Eventually(t, func() bool {
		info, infoErr := cons.Info(ctx)
		return infoErr == nil && info.NumAckPending == 0 && info.NumPending == 0
	}, 10*time.Second, 200*time.Millisecond, "the processed event was never Acked")
}

// Consumer settings for the outage test. AckWait must exceed the time one
// delivery spends failing against an unreachable MongoDB, or JetStream
// redelivers underneath a handler that is still running and the delivery count
// stops meaning what this test reads it to mean.
const (
	consumerAckWait    = 15 * time.Second
	consumerMaxDeliver = 10
)

// startCanonicalConsumer builds the worker's real durable consumer over
// MESSAGES-CANONICAL on the shared test NATS, returning it both directly (for
// ack-state assertions) and as a Consume iterator, plus the subject to publish
// canonical events on.
func startCanonicalConsumer(t *testing.T, siteID string) (jetstream.JetStream, jetstream.Consumer, natsmetrics.Iterator, string) {
	t.Helper()

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	sc := stream.MessagesCanonical(siteID)
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{Name: sc.Name, Subjects: sc.Subjects})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), sc.Name) })

	cons, err := js.CreateOrUpdateConsumer(context.Background(), sc.Name, buildConsumerConfig(stream.ConsumerSettings{
		AckWait:       consumerAckWait,
		MaxDeliver:    consumerMaxDeliver,
		MaxWaiting:    512,
		MaxAckPending: 1000,
	}, "broadcast-worker", sc.Subjects[0]))
	require.NoError(t, err)

	iter, err := cons.Messages()
	require.NoError(t, err)
	t.Cleanup(iter.Stop)

	return js, cons, plainIterAdapter{inner: iter}, fmt.Sprintf("chat.msg.canonical.%s.created", siteID)
}
