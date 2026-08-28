package natsmetrics

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeliveryAttemptFromContext_AgainstRealJetStream pins the one contract that
// every short-retry-budget caller depends on: after a NAK, a real broker reports
// the redelivery as attempt 2, and Track/Context carry that into the handler's
// context.
//
// The worker-level tests (broadcast-worker and message-worker's
// TestConsume_UnresolvableThreadParent_*) are built on this. If they fail but
// this passes, the fault is in the worker wiring. If this fails too, the fault
// is in the environment or the nats.go/nats-server pair — not in the handlers —
// and the failure below says which.
func TestDeliveryAttemptFromContext_AgainstRealJetStream(t *testing.T) {
	opts := &natsserver.Options{Port: -1, JetStream: true, StoreDir: t.TempDir()}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second),
		"embedded nats server did not start — this machine cannot run the in-process JetStream tests")
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: "ATTEMPTS", Subjects: []string{"attempts.>"}})
	require.NoError(t, err)

	// AckWait far exceeds the test window, so a redelivery can only come from the
	// NAK below — never from an ack timeout.
	cons, err := js.CreateOrUpdateConsumer(ctx, "ATTEMPTS", jetstream.ConsumerConfig{
		Durable: "attempts", AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: 30 * time.Second, MaxDeliver: 3,
	})
	require.NoError(t, err)

	iter, err := cons.Messages()
	require.NoError(t, err)
	t.Cleanup(iter.Stop)

	m, _ := newTestMetrics(t)
	consumer := m.Consumer(ConsumerConfig{Site: "s1", Stream: "ATTEMPTS", Consumer: "attempts"})
	consumer.LoopStarted(ctx)

	_, err = js.Publish(ctx, "attempts.one", []byte("payload"))
	require.NoError(t, err)

	seen := make(chan observation, 4)

	go func() {
		for {
			msg, nerr := iter.Next()
			if nerr != nil {
				return
			}
			tracked := consumer.Track(context.Background(), msg, EventCreated, 3)
			hctx := tracked.Context(context.Background())
			attempt, ok := DeliveryAttemptFromContext(hctx)
			seen <- observation{attempt: attempt, visible: ok}
			if attempt < 2 {
				_ = tracked.NakWithDelay(50 * time.Millisecond)
				continue
			}
			_ = tracked.Ack()
		}
	}()

	first := requireObservation(t, seen, "first delivery")
	require.True(t, first.visible,
		"the handler context carried no delivery count on the FIRST delivery — Track could not read the message metadata, "+
			"so every short-retry budget (parentResolveAttempts) is dead and messages retry to MaxDeliver instead")
	assert.Equal(t, uint64(1), first.attempt, "first delivery must report attempt 1")

	second := requireObservation(t, seen, "redelivery after NAK")
	require.True(t, second.visible, "the handler context carried no delivery count on the REDELIVERY")
	assert.Equal(t, uint64(2), second.attempt,
		"the broker must report a NAK'd redelivery as attempt 2; without this no retry budget can ever be exhausted")
}

func requireObservation(t *testing.T, seen <-chan observation, what string) observation {
	t.Helper()
	select {
	case o := <-seen:
		return o
	case <-time.After(15 * time.Second):
		t.Fatalf("no %s arrived within 15s — the embedded broker is not delivering (or not redelivering after a NAK)", what)
		return observation{}
	}
}

type observation struct {
	attempt uint64
	visible bool
}
