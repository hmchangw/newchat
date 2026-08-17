//go:build probe

// Package-local diagnostic probe (build tag `probe`, excluded from `make test`).
// Reproduces the JetStream redelivery / ack-pending behaviour reported against
// search-sync-worker against an in-process nats-server.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/stream"
)

// startNATS boots an embedded JetStream server and returns a connected client.
func startNATS(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	opts := &server.Options{Port: -1, JetStream: true, StoreDir: t.TempDir()}
	srv, err := server.NewServer(opts)
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(10*time.Second), "nats server did not start")
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return nc, js
}

func newStream(t *testing.T, js jetstream.JetStream, name, subj string) jetstream.Stream {
	t.Helper()
	s, err := js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     name,
		Subjects: []string{subj},
	})
	require.NoError(t, err)
	return s
}

// --- Probe 1: is AckWait the "maximum event processing time"? ---------------

// TestProbe_AckWaitIsTheProcessingBudget holds a message longer than AckWait
// without acking and observes the server redelivering it while the first
// delivery is still "in flight" — the redelivery/ack-pending shape reported.
func TestProbe_AckWaitIsTheProcessingBudget(t *testing.T) {
	_, js := startNATS(t)
	s := newStream(t, js, "P1", "p1.subj")

	cons, err := s.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:       "probe",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       1 * time.Second,
		MaxDeliver:    10,
		MaxAckPending: 1000,
	})
	require.NoError(t, err)

	_, err = js.Publish(context.Background(), "p1.subj", []byte(`{"n":1}`))
	require.NoError(t, err)

	batch, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	require.NoError(t, err)
	var first jetstream.Msg
	for m := range batch.Messages() {
		first = m
	}
	require.NotNil(t, first)
	md, err := first.Metadata()
	require.NoError(t, err)
	t.Logf("delivery #1 seq=%d numDelivered=%d", md.Sequence.Stream, md.NumDelivered)

	// Simulate "processing" that outlives AckWait without acking.
	time.Sleep(2500 * time.Millisecond)

	info, err := cons.Info(context.Background())
	require.NoError(t, err)
	t.Logf("while still processing: numAckPending=%d numRedelivered=%d",
		info.NumAckPending, info.NumRedelivered)

	batch2, err := cons.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	require.NoError(t, err)
	var second jetstream.Msg
	for m := range batch2.Messages() {
		second = m
	}
	require.NotNil(t, second, "message was NOT redelivered — hypothesis wrong")
	md2, err := second.Metadata()
	require.NoError(t, err)
	t.Logf("delivery #2 seq=%d numDelivered=%d", md2.Sequence.Stream, md2.NumDelivered)
	require.Equal(t, uint64(2), md2.NumDelivered)

	// The late ack from delivery #1 still lands, but the duplicate work already happened.
	require.NoError(t, first.Ack())
}

// TestProbe_InProgressExtendsTheBudget shows msg.InProgress() resetting the
// redelivery timer, so long processing does NOT trigger redelivery.
func TestProbe_InProgressExtendsTheBudget(t *testing.T) {
	_, js := startNATS(t)
	s := newStream(t, js, "P2", "p2.subj")

	cons, err := s.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:    "probe",
		AckPolicy:  jetstream.AckExplicitPolicy,
		AckWait:    1 * time.Second,
		MaxDeliver: 10,
	})
	require.NoError(t, err)
	_, err = js.Publish(context.Background(), "p2.subj", []byte(`{"n":1}`))
	require.NoError(t, err)

	batch, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	require.NoError(t, err)
	var first jetstream.Msg
	for m := range batch.Messages() {
		first = m
	}
	require.NotNil(t, first)

	// Heartbeat every 400ms across a 2.5s "processing" window.
	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(400 * time.Millisecond)
		require.NoError(t, first.InProgress())
	}
	require.NoError(t, first.Ack())

	info, err := cons.Info(context.Background())
	require.NoError(t, err)
	t.Logf("with InProgress heartbeat: numRedelivered=%d numAckPending=%d",
		info.NumRedelivered, info.NumAckPending)
	require.Zero(t, info.NumRedelivered, "InProgress should have prevented redelivery")
}

// --- Probe 2: does Nak() honour the consumer BackOff? -----------------------

// TestProbe_NakIgnoresBackOff replays search-sync-worker's exact consumer
// config (BackOff 1s/5s/30s, MaxDeliver 5) and naks each delivery, counting
// how fast MaxDeliver is exhausted.
func TestProbe_NakIgnoresBackOff(t *testing.T) {
	_, js := startNATS(t)
	s := newStream(t, js, "P3", "p3.subj")

	cc := buildConsumerConfig(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 5, MaxWaiting: 512, MaxAckPending: 1000,
	}, fakeCollection{name: "probe"}, "site-a")
	cc.FilterSubjects = nil
	cons, err := s.CreateOrUpdateConsumer(context.Background(), cc)
	require.NoError(t, err)
	t.Logf("consumer config: AckWait=%s MaxDeliver=%d BackOff=%v", cc.AckWait, cc.MaxDeliver, cc.BackOff)

	_, err = js.Publish(context.Background(), "p3.subj", []byte(`{"n":1}`))
	require.NoError(t, err)

	start := time.Now()
	deliveries := 0
	for i := 0; i < 8; i++ {
		batch, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
		require.NoError(t, err)
		got := false
		for m := range batch.Messages() {
			got = true
			deliveries++
			md, _ := m.Metadata()
			t.Logf("delivery #%d at t+%s (numDelivered=%d)", deliveries, time.Since(start).Round(time.Millisecond), md.NumDelivered)
			require.NoError(t, m.Nak())
		}
		if !got {
			break
		}
	}
	t.Logf("Nak(): %d deliveries in %s", deliveries, time.Since(start).Round(time.Millisecond))
	require.Equal(t, 5, deliveries, "MaxDeliver should be exhausted")
	require.Less(t, time.Since(start), 5*time.Second,
		"BackOff (1s/5s/30s) was NOT applied — plain Nak redelivers instantly")
}

// TestProbe_NakWithDelayHonoursBackOff is the control: an explicit delay does
// space redeliveries out.
func TestProbe_NakWithDelayHonoursBackOff(t *testing.T) {
	_, js := startNATS(t)
	s := newStream(t, js, "P4", "p4.subj")

	cons, err := s.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:    "probe",
		AckPolicy:  jetstream.AckExplicitPolicy,
		AckWait:    30 * time.Second,
		MaxDeliver: 3,
		BackOff:    []time.Duration{time.Second, 2 * time.Second},
	})
	require.NoError(t, err)
	_, err = js.Publish(context.Background(), "p4.subj", []byte(`{"n":1}`))
	require.NoError(t, err)

	start := time.Now()
	deliveries := 0
	for i := 0; i < 5; i++ {
		batch, err := cons.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
		require.NoError(t, err)
		got := false
		for m := range batch.Messages() {
			got = true
			deliveries++
			t.Logf("delivery #%d at t+%s", deliveries, time.Since(start).Round(100*time.Millisecond))
			require.NoError(t, m.NakWithDelay(time.Second))
		}
		if !got {
			break
		}
	}
	t.Logf("NakWithDelay(1s): %d deliveries in %s", deliveries, time.Since(start).Round(time.Millisecond))
	require.GreaterOrEqual(t, time.Since(start), 2*time.Second)
}

// --- Probe 3: is the acked "build action failed" message really gone? -------

// TestProbe_AckedPoisonIsNotRedelivered confirms the reported reasoning: a
// message acked on an unmarshal failure is NOT what JetStream is redelivering.
func TestProbe_AckedPoisonIsNotRedelivered(t *testing.T) {
	_, js := startNATS(t)
	s := newStream(t, js, "P5", "p5.subj")

	cons, err := s.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:    "probe",
		AckPolicy:  jetstream.AckExplicitPolicy,
		AckWait:    time.Second,
		MaxDeliver: 5,
	})
	require.NoError(t, err)

	// Not valid JSON for any collection — BuildAction fails, handler Acks.
	_, err = js.Publish(context.Background(), "p5.subj", []byte(`{"this":`))
	require.NoError(t, err)

	h := NewHandler(&probeStore{}, newSpotlightCollection("spot-v1", true), 10)
	batch, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	require.NoError(t, err)
	for m := range batch.Messages() {
		h.AddWithContext(context.Background(), m)
	}
	require.Zero(t, h.MessageCount(), "poison must not be buffered")

	time.Sleep(2500 * time.Millisecond) // > 2x AckWait
	info, err := cons.Info(context.Background())
	require.NoError(t, err)
	t.Logf("after ack-on-unmarshal-failure: numAckPending=%d numRedelivered=%d numPending=%d",
		info.NumAckPending, info.NumRedelivered, info.NumPending)
	require.Zero(t, info.NumAckPending)
	require.Zero(t, info.NumRedelivered)
}

// --- Probe 4: the real runConsumer loop under a slow search backend ---------

// TestProbe_SlowBulkCausesRedeliveryPileup runs the production runConsumer loop
// against embedded NATS with a store whose Bulk call outlives AckWait, and
// counts duplicate ES work caused by the resulting redeliveries.
func TestProbe_SlowBulkCausesRedeliveryPileup(t *testing.T) {
	_, js := startNATS(t)
	s := newStream(t, js, "P6", "p6.subj")

	cons, err := s.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:       "probe",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Second, // scaled-down stand-in for the 30s default
		MaxDeliver:    50,
		MaxAckPending: 1000,
	})
	require.NoError(t, err)

	const published = 20
	for i := 0; i < published; i++ {
		_, err = js.Publish(context.Background(), "p6.subj", memberAddedEnvelope(fmt.Sprintf("r%d", i)))
		require.NoError(t, err)
	}

	// Every flush takes 3x AckWait, so each batch's acks land long after the
	// server has already re-queued them for delivery.
	store := &probeStore{delay: 3 * time.Second}
	h := NewHandler(store, newSpotlightCollection("spot-v1", true), 5)

	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	go runConsumer(context.Background(), o11yLikeAdapter{cons}, h, 5, 5, time.Second, stopCh, doneCh)

	time.Sleep(20 * time.Second)
	close(stopCh)
	select {
	case <-doneCh:
	case <-time.After(20 * time.Second):
		t.Fatal("consumer loop did not stop")
	}

	total := int(store.actions.Load())
	t.Logf("published=%d bulkCalls=%d actionsSubmitted=%d distinctDocs=%d duplicates=%d",
		published, store.calls.Load(), total, store.distinct(), total-store.distinct())
	require.Greater(t, total, store.distinct(),
		"expected duplicate ES work from messages redelivered while their flush was still in flight")
}

// TestProbe_NakStormOnBulkFailure runs the production loop against a store that
// always fails, and counts how many deliveries JetStream burns and how fast.
func TestProbe_NakStormOnBulkFailure(t *testing.T) {
	_, js := startNATS(t)
	s := newStream(t, js, "P7", "p7.subj")

	cc := buildConsumerConfig(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 5, MaxWaiting: 512, MaxAckPending: 1000,
	}, fakeCollection{name: "probe"}, "site-a")
	cc.FilterSubjects = nil
	cons, err := s.CreateOrUpdateConsumer(context.Background(), cc)
	require.NoError(t, err)

	const published = 10
	for i := 0; i < published; i++ {
		_, err = js.Publish(context.Background(), "p7.subj", memberAddedEnvelope(fmt.Sprintf("r%d", i)))
		require.NoError(t, err)
	}

	store := &probeStore{failBulk: true} // stands in for an ES blip / 5xx
	h := NewHandler(store, newSpotlightCollection("spot-v1", true), 500)

	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	start := time.Now()
	go runConsumer(context.Background(), o11yLikeAdapter{cons}, h, 100, 500, time.Second, stopCh, doneCh)

	// Poll until the stream is fully drained (every message exhausted MaxDeliver).
	var drainedAt time.Duration
	for i := 0; i < 160; i++ {
		time.Sleep(50 * time.Millisecond)
		in, err := cons.Info(context.Background())
		if err == nil && in.NumPending == 0 && in.NumAckPending == 0 && store.actions.Load() >= published*int64(cc.MaxDeliver) {
			drainedAt = time.Since(start)
			break
		}
	}
	close(stopCh)
	select {
	case <-doneCh:
	case <-time.After(15 * time.Second):
		t.Fatal("consumer loop did not stop")
	}
	t.Logf("all %d messages exhausted MaxDeliver=%d after %s", published, cc.MaxDeliver, drainedAt.Round(time.Millisecond))

	info, err := cons.Info(context.Background())
	require.NoError(t, err)
	t.Logf("after %s of a failing ES: deliveryAttempts=%d numPending=%d numAckPending=%d",
		time.Since(start).Round(time.Second), store.actions.Load(), info.NumPending, info.NumAckPending)
	t.Logf("stream still holds %d msgs, but the consumer has burned MaxDeliver=%d on all of them",
		published, cc.MaxDeliver)
	require.GreaterOrEqual(t, int(store.actions.Load()), published*cc.MaxDeliver,
		"expected MaxDeliver attempts per message")
	require.Zero(t, info.NumPending, "all messages exhausted MaxDeliver — permanently dropped, no DLQ")
}

// TestProbe_UpdateConflictNaksForever shows a 409 on a scripted user-room
// update being classified as a hard failure, so the message is nakked and
// redelivered until MaxDeliver is exhausted.
func TestProbe_UpdateConflictNaksForever(t *testing.T) {
	_, js := startNATS(t)
	s := newStream(t, js, "P8", "p8.subj")

	cc := buildConsumerConfig(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 5, MaxWaiting: 512, MaxAckPending: 1000,
	}, fakeCollection{name: "probe"}, "site-a")
	cc.FilterSubjects = nil
	cons, err := s.CreateOrUpdateConsumer(context.Background(), cc)
	require.NoError(t, err)

	_, err = js.Publish(context.Background(), "p8.subj", memberAddedEnvelope("r1"))
	require.NoError(t, err)

	// ES returns 409 version_conflict_engine_exception — what concurrent pods
	// updating the same per-account doc produce without retry_on_conflict.
	store := &probeStore{itemStatus: 409, itemErrorType: "version_conflict_engine_exception"}
	h := NewHandler(store, newUserRoomCollection("user-room-v1", true), 500)

	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	go runConsumer(context.Background(), o11yLikeAdapter{cons}, h, 10, 10, time.Second, stopCh, doneCh)
	time.Sleep(5 * time.Second)
	close(stopCh)
	<-doneCh

	info, err := cons.Info(context.Background())
	require.NoError(t, err)
	t.Logf("409 on scripted update: attempts=%d numPending=%d numAckPending=%d",
		store.actions.Load(), info.NumPending, info.NumAckPending)
	require.EqualValues(t, cc.MaxDeliver, store.actions.Load(), "message retried until MaxDeliver")
}

// --- Probe 5: ack accounting when Flush panics ------------------------------

// TestProbe_FlushPanicLosesAckAccounting shows Handler.Flush taking ownership of
// the pending buffer before the store call, so a panic inside Bulk leaves those
// messages neither acked nor nakked — they only move again after AckWait.
func TestProbe_FlushPanicLosesAckAccounting(t *testing.T) {
	store := &probeStore{panicOnBulk: true}
	h := NewHandler(store, newSpotlightCollection("spot-v1", true), 10)

	msg := &recordingMsg{subject: "p7.subj", data: memberAddedEnvelope("r1")}
	h.AddWithContext(context.Background(), msg)
	require.Equal(t, 1, h.MessageCount())

	panicked := func() (p bool) {
		defer func() {
			if r := recover(); r != nil {
				p = true
			}
		}()
		h.Flush(context.Background())
		return false
	}()

	require.True(t, panicked, "Bulk panic should propagate to the jobguard wrapper")
	t.Logf("after panicking Flush: acks=%d naks=%d buffered=%d", msg.acks.Load(), msg.naks.Load(), h.MessageCount())
	require.Zero(t, msg.acks.Load(), "message was not acked")
	require.Zero(t, msg.naks.Load(), "message was not nakked either — ack accounting lost")
	require.Zero(t, h.MessageCount(), "and it is no longer buffered, so no retry can ack it")
}

// --- probe doubles ----------------------------------------------------------

type probeStore struct {
	delay         time.Duration
	panicOnBulk   bool
	failBulk      bool
	itemStatus    int
	itemErrorType string
	calls         atomic.Int64
	actions       atomic.Int64
	mu            sync.Mutex
	seen          map[string]struct{}
}

func (s *probeStore) distinct() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

func (s *probeStore) Bulk(ctx context.Context, actions []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
	if s.panicOnBulk {
		panic("probe: bulk exploded")
	}
	s.calls.Add(1)
	s.actions.Add(int64(len(actions)))
	if s.failBulk {
		return nil, fmt.Errorf("probe: elasticsearch unavailable")
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		s.seen = make(map[string]struct{})
	}
	for _, a := range actions {
		s.seen[a.DocID] = struct{}{}
	}
	out := make([]searchengine.BulkResult, len(actions))
	for i := range out {
		out[i] = searchengine.BulkResult{Status: 200}
		if s.itemStatus != 0 {
			out[i] = searchengine.BulkResult{Status: s.itemStatus, ErrorType: s.itemErrorType, Error: "probe"}
		}
	}
	return out, nil
}

// o11yLikeAdapter adapts a raw consumer to msgFetcher without the o11y facade.
type o11yLikeAdapter struct{ c jetstream.Consumer }

func (a o11yLikeAdapter) Fetch(ctx context.Context, n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
	b, err := a.c.Fetch(n, opts...)
	if err != nil {
		return nil, err
	}
	return rawBatch{ctx: ctx, b: b}, nil
}

// recordingMsg is a jetstream.Msg stub that counts Ack/Nak calls.
type recordingMsg struct {
	jetstream.Msg
	subject string
	data    []byte
	acks    atomic.Int64
	naks    atomic.Int64
}

func (m *recordingMsg) Subject() string      { return m.subject }
func (m *recordingMsg) Data() []byte         { return m.data }
func (m *recordingMsg) Headers() nats.Header { return nats.Header{} }
func (m *recordingMsg) Ack() error           { m.acks.Add(1); return nil }
func (m *recordingMsg) Nak() error           { m.naks.Add(1); return nil }

// memberAddedEnvelope builds a well-formed INBOX envelope the way every Go
// publisher does — via model.InboxEvent, so Payload lands base64-encoded.
func memberAddedEnvelope(roomID string) []byte {
	inner, _ := json.Marshal(model.InboxMemberEvent{
		RoomID: roomID, RoomName: "R", SiteID: "site-a",
		Accounts: []string{"alice"}, Timestamp: time.Now().UnixMilli(),
	})
	b, _ := json.Marshal(model.InboxEvent{
		Type: model.InboxMemberAdded, SiteID: "site-a",
		Payload: inner, Timestamp: time.Now().UnixMilli(),
	})
	return b
}
