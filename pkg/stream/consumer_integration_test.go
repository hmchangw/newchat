//go:build integration

package stream_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// newJetStream returns a JetStream context against the shared test server.
func newJetStream(t *testing.T) (jetstream.JetStream, context.Context) {
	t.Helper()

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return js, ctx
}

// newStream creates a per-test stream and registers its deletion.
func newStream(t *testing.T, js jetstream.JetStream, ctx context.Context, name, subject string) {
	t.Helper()

	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     name,
		Subjects: []string{subject},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), name) })
}

// nats-server overwrites AckWait with BackOff[0] (server/consumer.go:677-682).
// Deriving the schedule from AckWait makes the overwrite a no-op — this proves
// it against a real server rather than against the struct we send, which is the
// only way to catch it: three services shipped a hardcoded BackOff starting at
// 1s and silently ran a 1-second AckWait in production.
func TestDurableConsumerDefaults_ServerHonoursDerivedSchedule(t *testing.T) {
	js, ctx := newJetStream(t)
	newStream(t, js, ctx, "BACKOFF-TEST", "backoff.test.>")

	cc := stream.DurableConsumerDefaults(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 6, MaxWaiting: 512, MaxAckPending: 1000,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	})
	cc.Durable = "backoff-test-consumer"

	cons, err := js.CreateOrUpdateConsumer(ctx, "BACKOFF-TEST", cc)
	require.NoError(t, err, "the server must accept the derived schedule")

	got := cons.CachedInfo().Config
	assert.Equal(t, []time.Duration{
		30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute,
	}, got.BackOff)
	assert.Equal(t, 30*time.Second, got.AckWait,
		"server-side AckWait must survive the BackOff[0] overwrite")
	assert.Equal(t, 6, got.MaxDeliver)
}

// A hardcoded schedule whose head differs from AckWait is exactly the bug the
// derivation prevents: the server silently discards the declared AckWait.
func TestServerOverwritesAckWaitWithBackOffHead(t *testing.T) {
	js, ctx := newJetStream(t)
	newStream(t, js, ctx, "BACKOFF-OVERWRITE-TEST", "backoff.overwrite.>")

	cons, err := js.CreateOrUpdateConsumer(ctx, "BACKOFF-OVERWRITE-TEST", jetstream.ConsumerConfig{
		Durable:       "backoff-overwrite-consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    6,
		BackOff:       []time.Duration{time.Second, 5 * time.Second, 30 * time.Second},
	})
	require.NoError(t, err)

	assert.Equal(t, time.Second, cons.CachedInfo().Config.AckWait,
		"the declared 30s AckWait is discarded in favour of BackOff[0]")
}

// len(BackOff) > MaxDeliver is JSConsumerMaxDeliverBackoffError at create time
// (server/consumer.go:807) — the clamp in backOffSchedule is what prevents it.
func TestDurableConsumerDefaults_ClampKeepsTheServerHappy(t *testing.T) {
	js, ctx := newJetStream(t)
	newStream(t, js, ctx, "BACKOFF-CLAMP-TEST", "backoff.clamp.>")

	// 9 steps against MaxDeliver=3 — without the clamp the server rejects this.
	cc := stream.DurableConsumerDefaults(stream.ConsumerSettings{
		AckWait: time.Second, MaxDeliver: 3, MaxWaiting: 512, MaxAckPending: 100,
		BackOffSteps: 9, BackOffFactor: 2, BackOffMax: time.Minute,
	})
	cc.Durable = "backoff-clamp-consumer"

	cons, err := js.CreateOrUpdateConsumer(ctx, "BACKOFF-CLAMP-TEST", cc)
	require.NoError(t, err)
	assert.Len(t, cons.CachedInfo().Config.BackOff, 3)
}

// The unclamped schedule the previous test would have sent is rejected outright,
// which is why the clamp exists rather than trusting operators to size it.
func TestServerRejectsBackOffLongerThanMaxDeliver(t *testing.T) {
	js, ctx := newJetStream(t)
	newStream(t, js, ctx, "BACKOFF-REJECT-TEST", "backoff.reject.>")

	_, err := js.CreateOrUpdateConsumer(ctx, "BACKOFF-REJECT-TEST", jetstream.ConsumerConfig{
		Durable:       "backoff-reject-consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       time.Second,
		MaxDeliver:    3,
		BackOff:       []time.Duration{1, 2, 3, 4, 5},
	})
	require.Error(t, err, "a schedule longer than MaxDeliver must be rejected")
}

// An existing durable must accept the new schedule on update, not just create —
// every service in the repo wires its consumer with CreateOrUpdateConsumer, so a
// deploy updates durables in place rather than recreating them.
func TestDurableConsumerDefaults_UpdatesAnExistingDurable(t *testing.T) {
	js, ctx := newJetStream(t)
	newStream(t, js, ctx, "BACKOFF-UPDATE-TEST", "backoff.update.>")

	// Stand up the pre-change shape: flat AckWait, no BackOff.
	before := stream.DurableConsumerDefaults(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 5, MaxWaiting: 512, MaxAckPending: 1000,
	})
	before.Durable = "backoff-update-consumer"
	cons, err := js.CreateOrUpdateConsumer(ctx, "BACKOFF-UPDATE-TEST", before)
	require.NoError(t, err)
	require.Empty(t, cons.CachedInfo().Config.BackOff)

	after := stream.DurableConsumerDefaults(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 6, MaxWaiting: 512, MaxAckPending: 1000,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	})
	after.Durable = "backoff-update-consumer"
	updated, err := js.CreateOrUpdateConsumer(ctx, "BACKOFF-UPDATE-TEST", after)
	require.NoError(t, err, "the schedule must apply to an existing durable, not just a fresh one")

	got := updated.CachedInfo().Config
	assert.Len(t, got.BackOff, 5)
	assert.Equal(t, 30*time.Second, got.AckWait)
	assert.Equal(t, 6, got.MaxDeliver)
}

// CONSUMER_BACKOFF_STEPS=0 is the documented off-switch: it must produce a
// consumer the server treats exactly as the pre-change flat-AckWait shape.
func TestDurableConsumerDefaults_OffSwitchProducesFlatAckWait(t *testing.T) {
	js, ctx := newJetStream(t)
	newStream(t, js, ctx, "BACKOFF-OFF-TEST", "backoff.off.>")

	cc := stream.DurableConsumerDefaults(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 6, MaxWaiting: 512, MaxAckPending: 1000,
		BackOffSteps: 0, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	})
	cc.Durable = "backoff-off-consumer"

	cons, err := js.CreateOrUpdateConsumer(ctx, "BACKOFF-OFF-TEST", cc)
	require.NoError(t, err)

	got := cons.CachedInfo().Config
	assert.Empty(t, got.BackOff)
	assert.Equal(t, 30*time.Second, got.AckWait)
}
