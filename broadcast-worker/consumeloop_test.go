package main

import (
	"context"
	"encoding/json"
	"fmt"
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

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
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
	// Short AckWait so a message that is NOT acked (left pending or Nak'd)
	// visibly redelivers within the test window. jobguard Acks the poison
	// message, so it must NOT redeliver.
	js, iter, _, subj, maxDeliver := startEmbeddedCanonicalConsumerWith(t, siteID, stream.ConsumerSettings{
		AckWait:       time.Second,
		MaxDeliver:    10,
		MaxWaiting:    512,
		MaxAckPending: 1000,
	})
	return js, iter, subj, maxDeliver
}

// startEmbeddedCanonicalConsumerWith is startEmbeddedCanonicalConsumer with
// caller-chosen consumer settings, and also returns the consumer handle so a
// test can read server-side state such as NumAckPending.
func startEmbeddedCanonicalConsumerWith(t *testing.T, siteID string, settings stream.ConsumerSettings) (jetstream.JetStream, natsmetrics.Iterator, jetstream.Consumer, string, int) {
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

	cc := buildConsumerConfig(settings, "broadcast-worker", sc.Subjects[0])
	cons, err := js.CreateOrUpdateConsumer(context.Background(), sc.Name, cc)
	require.NoError(t, err)

	iter, err := cons.Messages()
	require.NoError(t, err)
	t.Cleanup(iter.Stop)

	return js, plainIterAdapter{inner: iter}, cons, "chat.msg.canonical." + siteID + ".created", settings.MaxDeliver
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
	assert.Equal(t, int64(1), sumOf(t, rm, "chat.nats.consumer.messages", map[string]string{"outcome": "nak", "event_type": "created"}))
	assert.Equal(t, int64(1), sumOf(t, rm, "chat.nats.consumer.redeliveries", map[string]string{"event_type": "created"}))
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

// ackPendingStore is a hand-written Store for the ack-pending regression test.
// gomock is avoided deliberately: these calls run on Consume goroutines, where
// a controller's t.Fatalf would be an illegal FailNow off the test goroutine.
type ackPendingStore struct{}

func (ackPendingStore) GetRoomMeta(context.Context, string) (roommetacache.Meta, error) {
	return roommetacache.Meta{ID: "room-1", Type: model.RoomTypeChannel, SiteID: "site-ackpending", UserCount: 2}, nil
}

func (ackPendingStore) GetThreadFollowers(context.Context, string) (map[string]struct{}, error) {
	return map[string]struct{}{"bob": {}}, nil
}

func (ackPendingStore) GetRoom(context.Context, string) (*model.Room, error) { return nil, nil }
func (ackPendingStore) ListSubscriptions(context.Context, string) ([]model.Subscription, error) {
	return nil, nil
}
func (ackPendingStore) UpdateRoomLastMessage(context.Context, roomLastMessage) error { return nil }
func (ackPendingStore) SetSubscriptionMentions(context.Context, string, []string, time.Time) error {
	return nil
}
func (ackPendingStore) GetHistorySharedSince(context.Context, string, []string) (map[string]*time.Time, error) {
	return map[string]*time.Time{}, nil
}
func (ackPendingStore) AdvanceSubscriptionLastSeen(context.Context, string, string, time.Time) error {
	return nil
}

type ackPendingUsers struct{}

func (ackPendingUsers) FindUserByID(context.Context, string) (*model.User, error) { return nil, nil }
func (ackPendingUsers) FindUserByAccount(context.Context, string) (*model.User, error) {
	return nil, nil
}
func (ackPendingUsers) FindUsersByAccounts(context.Context, []string) ([]model.User, error) {
	return nil, nil
}

// perParentFetcher resolves realParentID and reports every other parent as
// not_found, the way history-service answers for a parent that never existed.
type perParentFetcher struct{ realParentID string }

func (f perParentFetcher) FetchParent(_ context.Context, _, _, _, messageID string) (*ParentMessageInfo, error) {
	if messageID == f.realParentID {
		return &ParentMessageInfo{SenderAccount: "carol", CreatedAt: time.Now().UTC().Add(-time.Hour)}, nil
	}
	return nil, errcode.NotFound("message not found")
}

// TestConsume_UnresolvableThreadParent_DoesNotExhaustAckPending is the
// regression test for the ack-pending stall.
//
// A reply whose thread parent does not exist fails identically on every
// delivery. While that failure was classified transient, each such message
// NAK'd and held its ack-pending slot for the whole MaxDeliver budget — so a
// burst larger than MaxAckPending filled the consumer's budget and JetStream
// stopped delivering ANYTHING, healthy messages included. Now the failure is
// classified permanent and Ack-dropped on the first delivery, so the budget is
// released immediately.
//
// The test publishes 3x MaxAckPending unresolvable replies and then one
// resolvable one. The good message is the assertion: it sits behind the whole
// burst in stream order, so it can only be delivered if the burst released its
// slots.
func TestConsume_UnresolvableThreadParent_DoesNotExhaustAckPending(t *testing.T) {
	const (
		maxAckPending = 4
		burst         = 3 * maxAckPending
		realParentID  = "real-parent"
	)

	// AckWait is long on purpose: nothing in this test may redeliver because it
	// timed out. Every redelivery must come from a NAK, so a stall here proves
	// the classification and nothing else.
	js, iter, cons, subj, maxDeliver := startEmbeddedCanonicalConsumerWith(t, "site-ackpending", stream.ConsumerSettings{
		AckWait:       30 * time.Second,
		MaxDeliver:    6,
		MaxWaiting:    512,
		MaxAckPending: maxAckPending,
	})

	h := NewHandler(ackPendingStore{}, ackPendingUsers{}, &mockPublisher{}, nil,
		perParentFetcher{realParentID: realParentID}, false, subject.RouteGlobal)

	good := make(chan struct{}, 1)
	var settled atomic.Int32
	// The production composition: the same Settle call and backoff schedule
	// main.go's consume loop uses.
	process := func(ctx context.Context, msg jetstream.Msg) {
		err := h.HandleMessage(ctx, msg.Data())
		jsretry.Settle(ctx, msg, jsretry.LowLatencyBackoff, err)
		settled.Add(1)
		if err == nil {
			select {
			case good <- struct{}{}:
			default:
			}
		}
	}

	m, _ := newTestBroadcastMetrics(t)
	consumer := m.Consumer(natsmetrics.ConsumerConfig{
		Site:   "site-ackpending",
		Stream: "MESSAGES-CANONICAL-site-ackpending", Consumer: "broadcast-worker",
	})
	consumer.LoopStarted(context.Background())

	var wg sync.WaitGroup
	go natsmetrics.Consume(context.Background(), iter, consumer, maxAckPending, maxDeliver, &wg,
		func(msg jetstream.Msg) natsmetrics.EventType { return natsmetrics.EventTypeFromSubject(msg.Subject()) },
		guardedProcessor(process))

	publishReply := func(id, parentID string) {
		t.Helper()
		evt := model.MessageEvent{
			Event: model.EventCreated, SiteID: "site-ackpending", Timestamp: time.Now().UTC().UnixMilli(),
			Message: model.Message{
				ID: id, RoomID: "room-1", UserID: "u-alice", UserAccount: "alice",
				Content: "a thread reply", CreatedAt: time.Now().UTC(),
				ThreadParentMessageID: parentID,
				TShow:                 false,
			},
		}
		data, err := json.Marshal(evt)
		require.NoError(t, err)
		_, err = js.Publish(context.Background(), subj, data)
		require.NoError(t, err)
	}

	for i := range burst {
		publishReply(fmt.Sprintf("ghost-reply-%d", i), fmt.Sprintf("ghost-parent-%d", i))
	}
	// Published last, so it is behind the entire burst in stream order.
	publishReply("good-reply", realParentID)

	select {
	case <-good:
	case <-time.After(15 * time.Second):
		info, err := cons.Info(context.Background())
		pending := -1
		if err == nil {
			pending = info.NumAckPending
		}
		t.Fatalf("the resolvable reply was never delivered: %d unresolvable replies wedged the consumer "+
			"(num_ack_pending=%d, cap=%d, settled=%d)", burst, pending, maxAckPending, settled.Load())
	}

	// The burst must also have released every slot, not merely enough of them
	// for one message to squeeze past.
	require.Eventually(t, func() bool {
		info, err := cons.Info(context.Background())
		return err == nil && info.NumAckPending == 0
	}, 10*time.Second, 50*time.Millisecond, "unresolvable replies must not hold ack-pending slots")

	assert.Equal(t, int32(burst+1), settled.Load(),
		"every message is settled exactly once — an unresolvable reply must not be redelivered")
}
