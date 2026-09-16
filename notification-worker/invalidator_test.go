package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// fakeMemberMsg is a minimal jetstream.Msg double. It embeds the interface (nil)
// so it satisfies the type without implementing methods the invalidator never calls.
type fakeMemberMsg struct {
	jetstream.Msg
	data  []byte
	acked atomic.Bool
}

func (m *fakeMemberMsg) Data() []byte { return m.data }
func (m *fakeMemberMsg) Ack() error   { m.acked.Store(true); return nil }

func memberEventMsg(t *testing.T, roomID string) *fakeMemberMsg {
	t.Helper()
	data, err := sonic.Marshal(model.CanonicalMemberEvent{RoomID: roomID})
	require.NoError(t, err)
	return &fakeMemberMsg{data: data}
}

// scriptedIter yields the queued messages, then blocks in Next until Stop is
// called — the shape of a live consumer that goes quiet during shutdown.
type scriptedIter struct {
	mu      sync.Mutex
	msgs    []jetstream.Msg
	stopped chan struct{}
	stopOne sync.Once
}

func newScriptedIter(msgs ...jetstream.Msg) *scriptedIter {
	return &scriptedIter{msgs: msgs, stopped: make(chan struct{})}
}

func (s *scriptedIter) Next(_ ...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	s.mu.Lock()
	if len(s.msgs) > 0 {
		msg := s.msgs[0]
		s.msgs = s.msgs[1:]
		s.mu.Unlock()
		return context.Background(), msg, nil
	}
	s.mu.Unlock()
	<-s.stopped
	return nil, nil, jetstream.ErrMsgIteratorClosed
}

func (s *scriptedIter) Stop()  { s.stopOne.Do(func() { close(s.stopped) }) }
func (s *scriptedIter) Drain() {}

// drained reports whether every scripted message has been handed out, i.e. the
// reader never blocked partway through.
func (s *scriptedIter) drained() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs) == 0
}

// floodIter returns a fresh message on every Next until Stop, so a reader
// goroutine is essentially always mid-iteration when shutdown lands.
type floodIter struct {
	roomID   string
	stopped  chan struct{}
	stopOne  sync.Once
	produced atomic.Int64
}

func newFloodIter(roomID string) *floodIter {
	return &floodIter{roomID: roomID, stopped: make(chan struct{})}
}

func (f *floodIter) Next(_ ...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	select {
	case <-f.stopped:
		return nil, nil, jetstream.ErrMsgIteratorClosed
	default:
	}
	f.produced.Add(1)
	data, _ := sonic.Marshal(model.CanonicalMemberEvent{RoomID: f.roomID})
	return context.Background(), &fakeMemberMsg{data: data}, nil
}

func (f *floodIter) Stop()  { f.stopOne.Do(func() { close(f.stopped) }) }
func (f *floodIter) Drain() {}

// TestRoomInvalidator_StopUnderContinuousTrafficDoesNotPanic is the regression
// pin for the shutdown crash. The reader is the only sender on the queue, so it
// must be the one that closes it: closing from the shutdown goroutine raced a
// reader parked between Next() returning and its send, panicking the pod with
// "send on closed channel". Run under -race, repeated so the window is hit.
func TestRoomInvalidator_StopUnderContinuousTrafficDoesNotPanic(t *testing.T) {
	for i := 0; i < 50; i++ {
		iter := newFloodIter("r1")
		inv := newRoomInvalidator(context.Background(), iter, func(context.Context, string) {}, 1)

		// Wait until the reader is actually mid-flight, so Stop lands while it is
		// producing at full rate rather than before it is even scheduled.
		require.Eventually(t, func() bool { return iter.produced.Load() > 0 },
			2*time.Second, time.Microsecond, "reader never started producing")

		require.NotPanics(t, func() {
			require.NoError(t, inv.Stop(context.Background()))
		})
	}
}

// TestRoomInvalidator_StopWaitsForBothGoroutines pins the shutdown handshake:
// once Stop returns, neither the reader nor the drain worker is still running.
func TestRoomInvalidator_StopWaitsForBothGoroutines(t *testing.T) {
	iter := newFloodIter("r1")
	var active atomic.Int64
	inv := newRoomInvalidator(context.Background(), iter, func(context.Context, string) {
		active.Add(1)
		defer active.Add(-1)
	}, 8)
	require.Eventually(t, func() bool { return iter.produced.Load() > 0 },
		2*time.Second, time.Microsecond, "reader never started producing")

	require.NoError(t, inv.Stop(context.Background()))

	assert.Zero(t, active.Load(), "no invalidate call may still be running after Stop")
	select {
	case <-inv.readerDone:
	default:
		t.Fatal("reader goroutine still running after Stop returned")
	}
	select {
	case <-inv.workerDone:
	default:
		t.Fatal("drain worker still running after Stop returned")
	}
}

// TestRoomInvalidator_DrainsQueuedRoomsBeforeStopReturns: work already accepted
// into the queue is completed, not discarded, on a graceful stop.
func TestRoomInvalidator_DrainsQueuedRoomsBeforeStopReturns(t *testing.T) {
	iter := newScriptedIter(
		memberEventMsg(t, "r1"),
		memberEventMsg(t, "r2"),
		memberEventMsg(t, "r3"),
	)

	var mu sync.Mutex
	var got []string
	inv := newRoomInvalidator(context.Background(), iter, func(_ context.Context, roomID string) {
		mu.Lock()
		got = append(got, roomID)
		mu.Unlock()
	}, 16)

	require.NoError(t, inv.Stop(context.Background()))

	mu.Lock()
	defer mu.Unlock()
	assert.ElementsMatch(t, []string{"r1", "r2", "r3"}, got, "queued invalidations must drain before Stop returns")
}

// TestRoomInvalidator_DropsWhenQueueFull preserves the existing back-pressure
// policy: a slow Valkey must never block NATS dispatch, and TTL reconciles the drop.
func TestRoomInvalidator_DropsWhenQueueFull(t *testing.T) {
	release := make(chan struct{})
	iter := newScriptedIter(
		memberEventMsg(t, "r1"),
		memberEventMsg(t, "r2"),
		memberEventMsg(t, "r3"),
		memberEventMsg(t, "r4"),
	)

	var handled atomic.Int64
	inv := newRoomInvalidator(context.Background(), iter, func(context.Context, string) {
		<-release // wedge the worker so the queue backs up
		handled.Add(1)
	}, 1)

	// The reader must not block: it drops once the queue is full.
	require.Eventually(t, func() bool { return iter.drained() }, 2*time.Second, 5*time.Millisecond,
		"reader must keep consuming the iterator instead of blocking on a full queue")

	close(release)
	require.NoError(t, inv.Stop(context.Background()))
	assert.LessOrEqual(t, handled.Load(), int64(4))
}

// TestRoomInvalidator_MalformedEventIsAckedAndSkipped: an undecodable event is
// poison — Ack it rather than redelivering forever, and keep consuming.
func TestRoomInvalidator_MalformedEventIsAckedAndSkipped(t *testing.T) {
	bad := &fakeMemberMsg{data: []byte("{not json")}
	good := memberEventMsg(t, "r9")
	iter := newScriptedIter(bad, good)

	var mu sync.Mutex
	var got []string
	inv := newRoomInvalidator(context.Background(), iter, func(_ context.Context, roomID string) {
		mu.Lock()
		got = append(got, roomID)
		mu.Unlock()
	}, 8)

	require.NoError(t, inv.Stop(context.Background()))

	assert.True(t, bad.acked.Load(), "malformed event must be acked, not left to redeliver")
	assert.True(t, good.acked.Load(), "the event after a malformed one must still be processed")
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"r9"}, got)
}

// TestRoomInvalidator_EmptyRoomIDIsIgnored: a decodable event with no room id
// is acked but never enqueued.
func TestRoomInvalidator_EmptyRoomIDIsIgnored(t *testing.T) {
	empty := memberEventMsg(t, "")
	iter := newScriptedIter(empty)

	var calls atomic.Int64
	inv := newRoomInvalidator(context.Background(), iter, func(context.Context, string) {
		calls.Add(1)
	}, 8)

	require.NoError(t, inv.Stop(context.Background()))
	assert.True(t, empty.acked.Load())
	assert.Zero(t, calls.Load(), "an empty roomId must not trigger an invalidation")
}

// TestRoomInvalidator_StopIsIdempotent: shutdown may run a step more than once;
// a second Stop must neither panic nor block.
func TestRoomInvalidator_StopIsIdempotent(t *testing.T) {
	iter := newScriptedIter(memberEventMsg(t, "r1"))
	inv := newRoomInvalidator(context.Background(), iter, func(context.Context, string) {}, 4)

	require.NoError(t, inv.Stop(context.Background()))
	require.NotPanics(t, func() {
		require.NoError(t, inv.Stop(context.Background()))
	})
}

// TestRoomInvalidator_StopCancelsWedgedWorkerOnDeadline: if the drain worker is
// stuck in an in-flight Valkey call past the step deadline, Stop cancels its
// context to free it and still returns rather than hanging shutdown.
func TestRoomInvalidator_StopCancelsWedgedWorkerOnDeadline(t *testing.T) {
	iter := newScriptedIter(memberEventMsg(t, "r1"))

	entered := make(chan struct{})
	var once sync.Once
	inv := newRoomInvalidator(context.Background(), iter, func(ctx context.Context, _ string) {
		once.Do(func() { close(entered) })
		<-ctx.Done() // only the invalidator's cancel can free this
	}, 4)

	<-entered
	stepCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- inv.Stop(stepCtx) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop hung on a wedged worker instead of cancelling it")
	}
}
