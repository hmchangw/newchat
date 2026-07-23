package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

// plainIterAdapter adapts the standard nats.go jetstream iterator (Next returns
// (Msg, error)) to consumeLoop's messageIterator (which expects the
// trace-carrying (ctx, Msg, error) shape of the o11y/nats facade). The
// production wrapper is a thin tracing layer over the same iterator; the panic
// recovery under test is identical regardless of which one feeds the loop, and
// the standard client's Stop() is race-safe.
type plainIterAdapter struct{ inner jetstream.MessagesContext }

func (a plainIterAdapter) Next(opts ...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	msg, err := a.inner.Next(opts...)
	if err != nil {
		return nil, nil, err
	}
	return context.Background(), msg, nil
}

// startEmbeddedCanonicalConsumer spins up an in-process JetStream server (no
// Docker) with the MESSAGES_CANONICAL stream and a broadcast-worker-style
// durable consumer, returning the JetStream handle, a consumeLoop-compatible
// iterator, and the subject to publish canonical messages on.
func startEmbeddedCanonicalConsumer(t *testing.T, siteID string) (jetstream.JetStream, messageIterator, string) {
	t.Helper()
	opts := &natsserver.Options{Port: -1, JetStream: true, StoreDir: t.TempDir()}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	sc := stream.MessagesCanonical(siteID)
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{Name: sc.Name, Subjects: sc.Subjects})
	require.NoError(t, err)

	// Short AckWait so a message that is NOT acked (left pending or Nak'd)
	// visibly redelivers within the test window. jobguard Acks the poison
	// message, so it must NOT redeliver.
	cc := buildConsumerConfig(stream.ConsumerSettings{
		AckWait:       time.Second,
		MaxDeliver:    10,
		MaxWaiting:    512,
		MaxAckPending: 1000,
	}, "broadcast-worker", sc.Subjects[0])
	cons, err := js.CreateOrUpdateConsumer(context.Background(), sc.Name, cc)
	require.NoError(t, err)

	iter, err := cons.Messages()
	require.NoError(t, err)
	t.Cleanup(iter.Stop)

	return js, plainIterAdapter{inner: iter}, "chat.msg.canonical." + siteID + ".created"
}

// TestConsumeLoop_PoisonMessageDoesNotBlockStream is the regression test for the
// missing panic recovery. A handler panic on the first ("poison") message must
// not crash the worker or wedge the consumer: a good message published behind it
// must still be processed, and the poison message must be Acked (poison drop)
// rather than redelivered — a redelivery loop is what crash-loops a real worker.
//
// Run against the pre-fix loop (process invoked without jobguard.Run), the panic
// terminates the test binary — that is the "before" failure this test guards.
func TestConsumeLoop_PoisonMessageDoesNotBlockStream(t *testing.T) {
	js, iter, subj := startEmbeddedCanonicalConsumer(t, "site-test")

	var poisonCalls atomic.Int32
	good := make(chan struct{}, 1)

	// process panics on the poison message (standing in for a handler panic such
	// as an errcode option misuse or a nil deref) and signals on the good one.
	// It never reaches Ack on the poison path — jobguard must recover and Ack it.
	// No require/t.Fatal here: this runs on a consumeLoop goroutine, where
	// FailNow is illegal.
	process := func(_ context.Context, msg jetstream.Msg) {
		if string(msg.Data()) == "poison" {
			poisonCalls.Add(1)
			panic("boom: simulated handler panic on poison message")
		}
		_ = msg.Ack()
		select {
		case good <- struct{}{}:
		default:
		}
	}

	var wg sync.WaitGroup
	go consumeLoop(iter, process, 4, &wg)

	// Poison FIRST, good behind it.
	_, err := js.Publish(context.Background(), subj, []byte("poison"))
	require.NoError(t, err)
	_, err = js.Publish(context.Background(), subj, []byte("good"))
	require.NoError(t, err)

	select {
	case <-good:
	case <-time.After(10 * time.Second):
		t.Fatal("good message was never processed — a panic on the poison message crashed or wedged the consumer")
	}

	// With a 1s AckWait, a non-Acked poison message would redeliver and bump the
	// counter past 1 inside this window. Acked (poison drop) => stays at 1.
	require.Never(t, func() bool { return poisonCalls.Load() > 1 }, 3*time.Second, 200*time.Millisecond,
		"poison message was redelivered — it was not Acked as a poison drop, which crash-loops a real worker")
	require.Equal(t, int32(1), poisonCalls.Load(), "poison handler must run exactly once")
}
