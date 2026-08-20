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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/stream"
)

// plainIterAdapter adapts the standard nats.go jetstream iterator (Next returns
// (Msg, error)) to natsmetrics.Iterator (which expects the trace-carrying
// (ctx, Msg, error) shape of the o11y/nats facade). The production wrapper is a
// thin tracing layer over the same iterator; the behaviour under test is
// identical regardless of which one feeds the loop, and the standard client's
// Stop() is race-safe.
type plainIterAdapter struct{ inner jetstream.MessagesContext }

func (a plainIterAdapter) Next(opts ...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	msg, err := a.inner.Next(opts...)
	if err != nil {
		return nil, nil, err
	}
	return context.Background(), msg, nil
}

// startEmbeddedCanonicalConsumer spins up an in-process JetStream server (no
// Docker) with the MESSAGES-CANONICAL stream and a broadcast-worker-style
// durable consumer, returning the JetStream handle, the iterator, the subject
// to publish canonical messages on, and the durable's MaxDeliver.
func startEmbeddedCanonicalConsumer(t *testing.T, siteID string) (jetstream.JetStream, natsmetrics.Iterator, string, int) {
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
	settings := stream.ConsumerSettings{
		AckWait:       time.Second,
		MaxDeliver:    10,
		MaxWaiting:    512,
		MaxAckPending: 1000,
	}
	cc := buildConsumerConfig(settings, "broadcast-worker", sc.Subjects[0])
	cons, err := js.CreateOrUpdateConsumer(context.Background(), sc.Name, cc)
	require.NoError(t, err)

	iter, err := cons.Messages()
	require.NoError(t, err)
	t.Cleanup(iter.Stop)

	return js, plainIterAdapter{inner: iter}, "chat.msg.canonical." + siteID + ".created", settings.MaxDeliver
}

// TestConsume_PoisonMessageDoesNotBlockStream is the regression test for the
// missing panic recovery, driven through the production composition
// (natsmetrics.Consume + guardedProcessor). A handler panic on the first
// ("poison") message must not crash the worker or wedge the consumer: a good
// message published behind it must still be processed, and the poison message
// must be Acked (poison drop) rather than redelivered — a redelivery loop is
// what crash-loops a real worker.
func TestConsume_PoisonMessageDoesNotBlockStream(t *testing.T) {
	js, iter, subj, maxDeliver := startEmbeddedCanonicalConsumer(t, "site-test")

	var poisonCalls atomic.Int32
	good := make(chan struct{}, 1)

	// process panics on the poison message (standing in for a handler panic such
	// as an errcode option misuse or a nil deref) and signals on the good one.
	// It never reaches Ack on the poison path — jobguard must recover and Ack it.
	// No require/t.Fatal here: this runs on a Consume goroutine, where FailNow
	// is illegal.
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

	m, _ := newTestBroadcastMetrics(t)
	consumer := m.Consumer(natsmetrics.ConsumerConfig{
		Site:   "site-test",
		Stream: "MESSAGES-CANONICAL-site-test", Consumer: "broadcast-worker",
	})
	consumer.LoopStarted(context.Background())

	var wg sync.WaitGroup
	go natsmetrics.Consume(context.Background(), iter, consumer, 4, maxDeliver, &wg,
		func(msg jetstream.Msg) natsmetrics.EventType { return natsmetrics.EventTypeFromSubject(msg.Subject()) },
		guardedProcessor(process))

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

func newTestBroadcastMetrics(t *testing.T) (*natsmetrics.Metrics, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return natsmetrics.NewFromProvider(mp), reader
}

// TestConsume_MetricsAgainstRealJetStream is the metrics contract's integration
// check: run real deliveries through a real durable and assert the exact
// counter deltas, the loop gauge, and that no identifier leaked into a label.
func TestConsume_MetricsAgainstRealJetStream(t *testing.T) {
	js, iter, subj, maxDeliver := startEmbeddedCanonicalConsumer(t, "site-metrics")
	m, reader := newTestBroadcastMetrics(t)
	consumer := m.Consumer(natsmetrics.ConsumerConfig{
		Site:   "site-metrics",
		Stream: "MESSAGES-CANONICAL-site-metrics", Consumer: "broadcast-worker",
	})
	consumer.LoopStarted(context.Background())

	redelivered := make(chan struct{}, 1)
	acked := make(chan struct{}, 2)
	var naks atomic.Int32

	process := func(_ context.Context, msg *natsmetrics.Message) {
		switch string(msg.Data()) {
		case "ok":
			_ = msg.Ack()
			acked <- struct{}{}
		case "transient":
			// Nak once, then Ack on the redelivery so the run terminates.
			if naks.Add(1) == 1 {
				_ = msg.Nak()
				return
			}
			_ = msg.Ack()
			select {
			case redelivered <- struct{}{}:
			default:
			}
			acked <- struct{}{}
		case "poison":
			msg.MarkTerminal(context.Background(), natsmetrics.TerminalInvalidPayload)
			_ = msg.Ack()
			acked <- struct{}{}
		}
	}

	var wg sync.WaitGroup
	go natsmetrics.Consume(context.Background(), iter, consumer, 4, maxDeliver, &wg,
		func(msg jetstream.Msg) natsmetrics.EventType { return natsmetrics.EventTypeFromSubject(msg.Subject()) },
		process)

	for _, body := range []string{"ok", "transient", "poison"} {
		_, err := js.Publish(context.Background(), subj, []byte(body))
		require.NoError(t, err)
	}

	// Three Acks total: ok, poison, and transient after its redelivery.
	for range 3 {
		select {
		case <-acked:
		case <-time.After(15 * time.Second):
			t.Fatal("timed out waiting for deliveries to settle")
		}
	}
	select {
	case <-redelivered:
	case <-time.After(5 * time.Second):
		t.Fatal("transient message never redelivered")
	}

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	assert.Equal(t, int64(1), sumOf(t, rm, "chat.nats.consumer.loop.up", nil), "loop must be up")
	assert.Equal(t, int64(3), sumOf(t, rm, "chat.nats.consumer.messages", map[string]string{"outcome": "ack", "event_type": "created"}))
	// The nak above is the redelivery signal. A nakked delivery is what makes
	// JetStream redeliver, and this counter already carries the event type, so
	// "which kind of message keeps failing" is answered here; "how many times it
	// was redelivered" is the broker's jetstream_consumer_num_redelivered.
	assert.Equal(t, int64(1), sumOf(t, rm, "chat.nats.consumer.messages", map[string]string{"outcome": "nak", "event_type": "created"}))
	assert.Equal(t, int64(1), sumOf(t, rm, "chat.nats.terminal.failures", map[string]string{"reason": "invalid_payload"}))

	// The stream, subject and site carry a site id and a message body, but no
	// message id, room id, account or subject may reach a label.
	allowed := map[string]bool{
		"site": true, "stream": true,
		"consumer": true, "event_type": true, "outcome": true, "reason": true,
	}
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			for _, key := range attributeKeys(metric) {
				assert.True(t, allowed[key], "unexpected label %q on %s", key, metric.Name)
			}
		}
	}
}

func attributeKeys(m metricdata.Metrics) []string {
	var keys []string
	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		for _, dp := range data.DataPoints {
			for _, kv := range dp.Attributes.ToSlice() {
				keys = append(keys, string(kv.Key))
			}
		}
	case metricdata.Histogram[float64]:
		for _, dp := range data.DataPoints {
			for _, kv := range dp.Attributes.ToSlice() {
				keys = append(keys, string(kv.Key))
			}
		}
	}
	return keys
}

// sumOf totals every data point of name whose attributes contain want.
func sumOf(t *testing.T, rm metricdata.ResourceMetrics, name string, want map[string]string) int64 {
	t.Helper()
	var total int64
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			sum, ok := metric.Data.(metricdata.Sum[int64])
			require.True(t, ok, name)
			for _, dp := range sum.DataPoints {
				got := map[string]string{}
				for _, kv := range dp.Attributes.ToSlice() {
					got[string(kv.Key)] = kv.Value.AsString()
				}
				matches := true
				for k, v := range want {
					if got[k] != v {
						matches = false
						break
					}
				}
				if matches {
					total += dp.Value
				}
			}
		}
	}
	return total
}
