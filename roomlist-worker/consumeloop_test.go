package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/loopguard"
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
	f := newFlusher(store, 0, 0)
	bad := &fakeJetstreamMsg{subject: "chat.msg.canonical.site-a.created", data: []byte("not valid json"), headers: nats.Header{}}
	iter := &fakeIterator{msgs: []jetstream.Msg{bad}}

	var wg sync.WaitGroup
	wg.Add(1)
	consumeLoop(iter, f, &wg, loopguard.New("consume-loop", nil))

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
	f := newFlusher(store, 0, 0)
	good := &fakeJetstreamMsg{subject: "chat.msg.canonical.site-a.created", data: wellFormedEventBytes(t), headers: nats.Header{}}
	iter := &fakeIterator{msgs: []jetstream.Msg{good}}

	var wg sync.WaitGroup
	wg.Add(1)
	consumeLoop(iter, f, &wg, loopguard.New("consume-loop", nil))

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
	f := newFlusher(store, 0, 0)
	bad := &panickyHeadersMsg{fakeJetstreamMsg{subject: "chat.msg.canonical.site-a.created", data: wellFormedEventBytes(t)}}
	good := &fakeJetstreamMsg{subject: "chat.msg.canonical.site-a.created", data: wellFormedEventBytes(t), headers: nats.Header{}}
	iter := &fakeIterator{msgs: []jetstream.Msg{bad, good}}

	var wg sync.WaitGroup
	wg.Add(1)
	require.NotPanics(t, func() { consumeLoop(iter, f, &wg, loopguard.New("consume-loop", nil)) },
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
	f := newFlusher(store, 0, 0)
	iter := &fakeIterator{}

	var wg sync.WaitGroup
	wg.Add(1)
	consumeLoop(iter, f, &wg, loopguard.New("consume-loop", nil))

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
	state := loopguard.New("consume-loop", nil)
	probe := state.Check().Probe

	require.NoError(t, probe(context.Background()),
		"a loop that has not exited must report ready")

	var wg sync.WaitGroup
	wg.Add(1)
	consumeLoop(&fakeIterator{}, newFlusher(&stubStore{}, 0, 0), &wg, state)
	wg.Wait()

	err := probe(context.Background())
	require.Error(t, err, "an exited consume loop must fail readiness")
	assert.Contains(t, err.Error(), errFakeIterDone.Error(),
		"the probe must carry the reason the loop stopped")
}

// A loop still draining messages is healthy — the probe must not fail merely
// because work is in flight.
func TestConsumeLoop_ReadyWhileConsuming(t *testing.T) {
	state := loopguard.New("consume-loop", nil)
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
	state := loopguard.New("consume-loop", func() { restarted <- struct{}{} })

	var wg sync.WaitGroup
	wg.Add(1)
	consumeLoop(&fakeIterator{}, newFlusher(&stubStore{}, 0, 0), &wg, state)
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
	state := loopguard.New("consume-loop", func() { restarted <- struct{}{} })
	state.BeginShutdown()

	var wg sync.WaitGroup
	wg.Add(1)
	consumeLoop(&fakeIterator{}, newFlusher(&stubStore{}, 0, 0), &wg, state)
	wg.Wait()

	select {
	case <-restarted:
		t.Fatal("a shutdown-initiated stop must not request termination")
	default:
	}
	require.Error(t, state.Check().Probe(context.Background()),
		"readiness must still report not-ready while shutting down, so the pod drains")
}

// capturingHandler records the level and message of everything logged, so a
// test can assert HOW a stop was reported rather than merely that it was.
// Only those two fields are kept: a slog.Record is 288 bytes, and retaining
// whole ones costs a copy per record for nothing the assertions read.
type capturingHandler struct {
	mu    sync.Mutex
	lines []loggedLine
}

type loggedLine struct {
	level slog.Level
	msg   string
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

// The by-value slog.Record is fixed by the slog.Handler interface.
//
//nolint:gocritic // hugeParam: signature is dictated by slog.Handler
func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lines = append(h.lines, loggedLine{level: r.Level, msg: r.Message})
	return nil
}
func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) levelFor(substr string) (slog.Level, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, l := range h.lines {
		if strings.Contains(l.msg, substr) {
			return l.level, true
		}
	}
	return 0, false
}

// captureLogs installs a recording default logger for one test and restores the
// previous one afterwards, so the level assertion cannot leak between tests.
func captureLogs(t *testing.T) *capturingHandler {
	t.Helper()
	h := &capturingHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return h
}

// A graceful stop is the normal end of every pod's life: iter.Stop() during
// shutdown makes Next return, and reporting that at ERROR trips error-rate
// alerting on every single deploy. The loop hands the exit to the guard, which
// reads the intent BeginShutdown recorded to pick the level.
func TestConsumeLoop_GracefulStopIsNotLoggedAsAnError(t *testing.T) {
	logs := captureLogs(t)
	state := loopguard.New("consume-loop", nil)
	state.BeginShutdown()
	iter := &fakeIterator{} // exhausted immediately: Next returns errFakeIterDone
	var wg sync.WaitGroup
	wg.Add(1)

	consumeLoop(iter, newFlusher(&stubStore{}, 0, 0), &wg, state)
	wg.Wait()

	lvl, found := logs.levelFor("consume loop stopped")
	require.True(t, found, "the stop must still be reported")
	assert.Equal(t, slog.LevelInfo, lvl, "an intended shutdown must not report at ERROR")
}

// The unexpected stop keeps ERROR: nothing else observes the loop dying, and
// this is the line that says room-list state has silently stopped being written.
func TestConsumeLoop_UnexpectedStopStaysAnError(t *testing.T) {
	logs := captureLogs(t)
	state := loopguard.New("consume-loop", func() {})
	iter := &fakeIterator{} // exhausted immediately: Next returns errFakeIterDone
	var wg sync.WaitGroup
	wg.Add(1)

	consumeLoop(iter, newFlusher(&stubStore{}, 0, 0), &wg, state)
	wg.Wait()

	lvl, found := logs.levelFor("consume loop stopped")
	require.True(t, found)
	assert.Equal(t, slog.LevelError, lvl, "an unexpected stop must stay at ERROR")
}

// mentionEventBytes builds a canonical event whose content mentions each of the
// supplied accounts, so deriveIntents produces one mention intent per account.
func mentionEventBytes(t *testing.T, id string, mentions ...string) []byte {
	t.Helper()
	content := "hi"
	for _, m := range mentions {
		content += " @" + m
	}
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: id, RoomID: "r1", UserAccount: "alice", Content: content,
			CreatedAt: time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC),
		},
	}
	b, err := json.Marshal(evt)
	require.NoError(t, err)
	return b
}

// The end-to-end guarantee behind the budget: no ticker runs in this test, so a
// write reaching the store proves the consume loop drained on the budget alone.
// Without that, mentions accumulate for a whole flush interval no matter how
// large they grow.
func TestConsumeLoop_DrainsEarlyWhenMentionsCrossTheBudget(t *testing.T) {
	store := &stubStore{}
	f := newFlusher(store, 2, time.Second)
	iter := &fakeIterator{msgs: []jetstream.Msg{
		&fakeJetstreamMsg{
			subject: "chat.msg.canonical.site-a.created",
			data:    mentionEventBytes(t, "m1", "bob", "carol"),
			headers: nats.Header{},
		},
	}}
	state := loopguard.New("consume-loop", func() {})
	var wg sync.WaitGroup
	wg.Add(1)

	consumeLoop(iter, f, &wg, state)
	wg.Wait()

	assert.NotEmpty(t, store.mentions,
		"the batch must drain on the budget rather than wait for a ticker that has not run")
}
