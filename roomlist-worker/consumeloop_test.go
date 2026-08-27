package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// fakeJetstreamMsg implements the full jetstream.Msg interface (a superset of
// jsretry.Msg) so consumeLoop can be driven without a live NATS connection.
type fakeJetstreamMsg struct {
	subject string
	data    []byte
	headers nats.Header

	acked bool
	naked bool
}

func (m *fakeJetstreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: 1}, nil
}
func (m *fakeJetstreamMsg) Data() []byte         { return m.data }
func (m *fakeJetstreamMsg) Headers() nats.Header { return m.headers }
func (m *fakeJetstreamMsg) Subject() string      { return m.subject }
func (m *fakeJetstreamMsg) Reply() string        { return "" }
func (m *fakeJetstreamMsg) Ack() error           { m.acked = true; return nil }
func (m *fakeJetstreamMsg) DoubleAck(context.Context) error {
	m.acked = true
	return nil
}
func (m *fakeJetstreamMsg) Nak() error                       { m.naked = true; return nil }
func (m *fakeJetstreamMsg) NakWithDelay(time.Duration) error { m.naked = true; return nil }
func (m *fakeJetstreamMsg) InProgress() error                { return nil }
func (m *fakeJetstreamMsg) Term() error                      { return nil }
func (m *fakeJetstreamMsg) TermWithReason(string) error      { return nil }

var errFakeIterDone = errors.New("fake iterator exhausted")

// fakeIterator feeds a fixed sequence of messages then returns errFakeIterDone,
// satisfying messageIterator without a live NATS consumer.
type fakeIterator struct {
	msgs []jetstream.Msg
	i    int
}

func (f *fakeIterator) Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	if f.i >= len(f.msgs) {
		return nil, nil, errFakeIterDone
	}
	m := f.msgs[f.i]
	f.i++
	return context.Background(), m, nil
}

// wellFormedEventBytes marshals a minimal but valid MESSAGES-CANONICAL event
// whose deriveIntents output is non-empty (RoomID set, EventCreated).
func wellFormedEventBytes(t *testing.T) []byte {
	t.Helper()
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID:          "m1",
			RoomID:      "r1",
			UserAccount: "alice",
			Content:     "hello",
			CreatedAt:   time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC),
		},
	}
	b, err := json.Marshal(evt)
	require.NoError(t, err)
	return b
}

// TestConsumeLoop_MalformedPayloadSettledImmediatelyNeverJoinsBatch is the
// regression test for the malformed-payload path: it can never parse on
// redelivery, so it must be settled right away rather than routed into the
// flusher's batch, where it would sit un-settled until AckWait expires.
func TestConsumeLoop_MalformedPayloadSettledImmediatelyNeverJoinsBatch(t *testing.T) {
	store := &stubStore{}
	f := newFlusher(store)
	bad := &fakeJetstreamMsg{subject: "chat.msg.canonical.site-a.created", data: []byte("not valid json"), headers: nats.Header{}}
	iter := &fakeIterator{msgs: []jetstream.Msg{bad}}

	var wg sync.WaitGroup
	wg.Add(1)
	consumeLoop(iter, f, &wg, &consumeState{})

	assert.True(t, bad.acked, "a malformed payload must be settled (Acked as permanent) immediately")
	assert.False(t, bad.naked)
	assert.True(t, f.pending.empty(), "a malformed payload must never be added to the flusher's pending batch")
}

// TestConsumeLoop_WellFormedPayloadReachesFlusherHeldUntilFlush is the
// wiring-level ack-after-write contract: the derived intents must reach the
// flusher's pending batch, and the message must stay un-settled until a flush
// actually writes it.
func TestConsumeLoop_WellFormedPayloadReachesFlusherHeldUntilFlush(t *testing.T) {
	store := &stubStore{}
	f := newFlusher(store)
	good := &fakeJetstreamMsg{subject: "chat.msg.canonical.site-a.created", data: wellFormedEventBytes(t), headers: nats.Header{}}
	iter := &fakeIterator{msgs: []jetstream.Msg{good}}

	var wg sync.WaitGroup
	wg.Add(1)
	consumeLoop(iter, f, &wg, &consumeState{})

	assert.False(t, good.acked, "a well-formed message must stay un-settled until its batch is flushed")
	assert.False(t, good.naked)
	require.False(t, f.pending.empty(), "a well-formed message's intents must reach the flusher's pending batch")
	assert.Equal(t, "m1", f.pending.rooms["r1"].msgID, "the derived room-pointer intent must be present in the pending batch")

	f.Flush(context.Background())
	assert.True(t, good.acked, "once the batch is flushed and written, the held message must be settled")
}

// panickyHeadersMsg embeds fakeJetstreamMsg but panics from Headers(), which
// consumeLoop calls from inside the jobguard.Guard-wrapped closure — a stand-in
// for any deterministic panic in the derive/add path (e.g. a malformed-but-
// parseable event tripping a nil deref downstream).
type panickyHeadersMsg struct {
	fakeJetstreamMsg
}

func (m *panickyHeadersMsg) Headers() nats.Header {
	panic("boom: simulated handler panic")
}

// TestConsumeLoop_PanicInGuardedPathDoesNotKillLoopOrCrashMessage proves
// jobguard.Guard does two things for roomlist-worker specifically: the
// panicking message is left un-acked (so JetStream redelivers it — the batch
// writes are replay-safe, unlike broadcast-worker's Run which Acks-drops on
// panic), and the consume loop itself survives to process the next message.
// Without this guard the goroutine — and with it the whole process, since an
// unrecovered panic in a goroutine is fatal — would die with the message still
// un-acked, and with MaxDeliver=-1 the redelivery would hit the same panic on
// every restart forever.
func TestConsumeLoop_PanicInGuardedPathDoesNotKillLoopOrCrashMessage(t *testing.T) {
	store := &stubStore{}
	f := newFlusher(store)
	bad := &panickyHeadersMsg{fakeJetstreamMsg{subject: "chat.msg.canonical.site-a.created", data: wellFormedEventBytes(t)}}
	good := &fakeJetstreamMsg{subject: "chat.msg.canonical.site-a.created", data: wellFormedEventBytes(t), headers: nats.Header{}}
	iter := &fakeIterator{msgs: []jetstream.Msg{bad, good}}

	var wg sync.WaitGroup
	wg.Add(1)
	require.NotPanics(t, func() { consumeLoop(iter, f, &wg, &consumeState{}) },
		"a panic in one message's handling must not escape the consume loop")

	assert.False(t, bad.acked, "a message whose handling panicked must stay un-acked so JetStream redelivers it")
	assert.False(t, bad.naked)
	require.False(t, f.pending.empty(), "the loop must keep processing messages after recovering from a panic")
	assert.Equal(t, "m1", f.pending.rooms["r1"].msgID, "the message after the panicking one must still reach the batch")
}

// TestConsumeLoop_ReturnsWhenIteratorErrors verifies the loop calls wg.Done()
// once the iterator is exhausted, so a caller's wg.Wait() (as shutdown does)
// actually returns instead of hanging until the drain timeout.
func TestConsumeLoop_ReturnsWhenIteratorErrors(t *testing.T) {
	store := &stubStore{}
	f := newFlusher(store)
	iter := &fakeIterator{}

	var wg sync.WaitGroup
	wg.Add(1)
	consumeLoop(iter, f, &wg, &consumeState{})

	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("wg.Wait() did not return — consumeLoop did not call wg.Done() on iterator error")
	}
}

// The consume loop is the only thing this worker does. If it stops — for any
// reason other than a shutdown that is tearing the process down anyway — the
// pod must stop reporting ready, or Kubernetes keeps a silently-dead worker in
// rotation while room pointers, lastSeenAt and mention badges go unwritten with
// no signal at all. /readyz previously probed only the NATS connection, which
// stays healthy in exactly this failure.
func TestConsumeLoop_ExitFailsReadiness(t *testing.T) {
	var state consumeState
	probe := state.Check().Probe

	require.NoError(t, probe(context.Background()),
		"a loop that has not exited must report ready")

	var wg sync.WaitGroup
	wg.Add(1)
	consumeLoop(&fakeIterator{}, newFlusher(&stubStore{}), &wg, &state)
	wg.Wait()

	err := probe(context.Background())
	require.Error(t, err, "an exited consume loop must fail readiness")
	assert.Contains(t, err.Error(), errFakeIterDone.Error(),
		"the probe must carry the reason the loop stopped")
}

// A loop still draining messages is healthy — the probe must not fail merely
// because work is in flight.
func TestConsumeLoop_ReadyWhileConsuming(t *testing.T) {
	var state consumeState
	assert.NoError(t, state.Check().Probe(context.Background()))
}

// Failing readiness is not recovery for a queue worker. Nothing routes traffic
// to it, and liveness always returns 200 by design, so a 503 on /readyz leaves
// a dead consumer running indefinitely — visible to an alert, but never
// restarted. The loop therefore asks the process to terminate so the supervisor
// replaces it, which is the only actor that can actually recover a stopped
// iterator.
func TestConsumeLoop_UnexpectedExitRequestsProcessRestart(t *testing.T) {
	restarted := make(chan struct{}, 1)
	state := consumeState{onUnexpectedStop: func() { restarted <- struct{}{} }}

	var wg sync.WaitGroup
	wg.Add(1)
	consumeLoop(&fakeIterator{}, newFlusher(&stubStore{}), &wg, &state)
	wg.Wait()

	select {
	case <-restarted:
	default:
		t.Fatal("an unexpected consume-loop exit must request process termination, not merely fail readiness")
	}
}

// The deliberate iter.Stop() during shutdown must not be mistaken for a failure
// — the process is already going down, and signalling it again would log a
// spurious error on every clean stop.
func TestConsumeLoop_ShutdownExitDoesNotRequestRestart(t *testing.T) {
	restarted := make(chan struct{}, 1)
	state := consumeState{onUnexpectedStop: func() { restarted <- struct{}{} }}
	state.beginShutdown()

	var wg sync.WaitGroup
	wg.Add(1)
	consumeLoop(&fakeIterator{}, newFlusher(&stubStore{}), &wg, &state)
	wg.Wait()

	select {
	case <-restarted:
		t.Fatal("a shutdown-initiated stop must not request termination")
	default:
	}
	require.Error(t, state.Check().Probe(context.Background()),
		"readiness must still report not-ready while shutting down, so the pod drains")
}
