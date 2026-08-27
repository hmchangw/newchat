package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/errcode"
)

// stubStore records the calls the flusher makes and can fail chosen stages
// independently. ctx.Err() is checked first so tests can prove Run's final
// flush truly uses a fresh context rather than the already-cancelled one.
type stubStore struct {
	mu       sync.Mutex
	order    []string
	rooms    map[string]roomLastMsgUpdate
	lastSeen map[subKey]time.Time
	mentions map[subKey]time.Time
	failWith map[string]error // stage name ("rooms"/"lastSeen"/"mentions") -> error to return
	// sawDeadline records whether the write context carried one, which is what
	// keeps a wedged Mongo write from stalling the flusher past AckWait.
	sawDeadline bool
}

func (s *stubStore) deadlineSeen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sawDeadline
}

func (s *stubStore) BulkUpdateRoomLastMessage(ctx context.Context, u map[string]roomLastMsgUpdate) error {
	s.mu.Lock()
	_, s.sawDeadline = ctx.Deadline()
	s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	s.order = append(s.order, "rooms")
	s.rooms = u
	return s.failWith["rooms"]
}

func (s *stubStore) BulkAdvanceLastSeen(ctx context.Context, u map[subKey]time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.order = append(s.order, "lastSeen")
	s.lastSeen = u
	return s.failWith["lastSeen"]
}

func (s *stubStore) BulkSetMentions(ctx context.Context, u map[subKey]time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.order = append(s.order, "mentions")
	s.mentions = u
	return s.failWith["mentions"]
}

// permanentWriteErr builds a mongo.BulkWriteException carrying one WriteError
// per code given, for tests that need a document-rejection-shaped error.
func permanentWriteErr(codes ...int) error {
	writeErrs := make([]mongo.BulkWriteError, len(codes))
	for i, c := range codes {
		writeErrs[i] = mongo.BulkWriteError{WriteError: mongo.WriteError{Code: c}}
	}
	return mongo.BulkWriteException{WriteErrors: writeErrs}
}

// allStagesIntents is the intent set that populates all three write stages, so
// a flush exercises rooms, lastSeen and mentions. Most flush tests need exactly
// this and vary only in how the store is made to fail.
func allStagesIntents(at time.Time) writeIntents {
	return writeIntents{
		RoomID: "r1", LastMsgID: "m1", LastMsgAt: at,
		SenderAccount: "alice", SenderSeenAt: at,
		MentionAccounts: []string{"bob"}, MentionAt: at,
	}
}

func TestFlusher_AcksHeldMessagesAfterSuccessfulWrite(t *testing.T) {
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	store := &stubStore{}
	f := newFlusher(store)
	m := &fakeMsg{}

	f.add(allStagesIntents(at), held(m))
	f.Flush(context.Background())

	assert.True(t, m.acked)
	assert.False(t, m.naked)
	assert.Equal(t, []string{"rooms", "lastSeen", "mentions"}, store.order,
		"lastSeenAt must be written before mentions so a self-mention does not badge the sender")
}

func TestFlusher_NaksHeldMessagesOnTransientWriteFailure(t *testing.T) {
	store := &stubStore{failWith: map[string]error{"rooms": errors.New("connection refused")}}
	f := newFlusher(store)
	m := &fakeMsg{}

	f.add(writeIntents{RoomID: "r1", LastMsgID: "m1", LastMsgAt: time.Now().UTC()}, held(m))
	f.Flush(context.Background())

	assert.True(t, m.naked, "a transient failure must retry, not drop")
	assert.False(t, m.acked)
	assert.Greater(t, m.nakDelay, time.Duration(0))
}

func TestFlusher_AcksHeldMessagesOnServerRejectedWrite(t *testing.T) {
	store := &stubStore{failWith: map[string]error{"mentions": permanentWriteErr(9)}}
	f := newFlusher(store)
	m := &fakeMsg{}

	f.add(writeIntents{RoomID: "r1", MentionAccounts: []string{"bob"}, MentionAt: time.Now().UTC()}, held(m))
	f.Flush(context.Background())

	assert.True(t, m.acked, "a rejected document never succeeds on retry; it must be dropped, not looped")
	assert.False(t, m.naked)
}

func TestFlusher_TransientFailureStopsRemainingStages(t *testing.T) {
	store := &stubStore{failWith: map[string]error{"rooms": errors.New("connection refused")}}
	f := newFlusher(store)

	f.add(allStagesIntents(time.Now().UTC()), held(&fakeMsg{}))
	f.Flush(context.Background())

	assert.Equal(t, []string{"rooms"}, store.order,
		"a transient failure means the whole batch redelivers, so later stages must not run against a half-failed flush")
}

func TestFlusher_PermanentFailureInStage1StillRunsRemainingStages(t *testing.T) {
	store := &stubStore{failWith: map[string]error{"rooms": permanentWriteErr(9)}}
	f := newFlusher(store)
	m := &fakeMsg{}

	at := time.Now().UTC()
	f.add(allStagesIntents(at), held(m))
	f.Flush(context.Background())

	assert.Equal(t, []string{"rooms", "lastSeen", "mentions"}, store.order,
		"a rejected document never redelivers, so the later stages' writes are the only chance to land them")
	assert.NotEmpty(t, store.lastSeen, "lastSeenAt advance must still be written despite the rooms rejection")
	assert.NotEmpty(t, store.mentions, "mention badge must still be written despite the rooms rejection")
	assert.True(t, m.acked, "a permanent failure drops the message; there is nothing left to retry")
	assert.False(t, m.naked)
}

func TestFlusher_PermanentThenTransientNaksAndSkipsThirdStage(t *testing.T) {
	store := &stubStore{failWith: map[string]error{
		"rooms":    permanentWriteErr(9),
		"lastSeen": errors.New("connection refused"),
	}}
	f := newFlusher(store)
	m := &fakeMsg{}

	at := time.Now().UTC()
	f.add(allStagesIntents(at), held(m))
	f.Flush(context.Background())

	assert.Equal(t, []string{"rooms", "lastSeen"}, store.order,
		"stage 2's transient failure must stop stage 3 even though stage 1 was permanent")
	assert.True(t, m.naked, "a transient failure anywhere in the batch means the whole batch retries")
	assert.False(t, m.acked)
}

func TestFlusher_JoinedPermanentErrorsAcrossStagesStillClassifyPermanent(t *testing.T) {
	store := &stubStore{failWith: map[string]error{
		"rooms":    permanentWriteErr(9),
		"mentions": permanentWriteErr(11000),
	}}
	f := newFlusher(store)
	m := &fakeMsg{}

	at := time.Now().UTC()
	f.add(allStagesIntents(at), held(m))
	f.Flush(context.Background())

	assert.Equal(t, []string{"rooms", "lastSeen", "mentions"}, store.order,
		"stage 2 succeeded so stage 3 must still run even though stage 1 was permanent")
	assert.True(t, m.acked, "errors.Join of two permanent errors must still classify as permanent overall")
	assert.False(t, m.naked)
}

func TestFlusher_SettlesAllHeldMessagesInBatch(t *testing.T) {
	store := &stubStore{}
	f := newFlusher(store)
	m1 := &fakeMsg{}
	m2 := &fakeMsg{}

	f.add(writeIntents{RoomID: "r1", LastMsgID: "m1", LastMsgAt: time.Now().UTC()}, held(m1))
	f.add(writeIntents{RoomID: "r1", LastMsgID: "m2", LastMsgAt: time.Now().UTC()}, held(m2))
	f.Flush(context.Background())

	assert.True(t, m1.acked, "the first held message in the batch must be settled")
	assert.True(t, m2.acked, "the second held message in the batch must also be settled, not just held[0]")
}

func TestFlusher_AcksNoOpMessagesWithoutWriting(t *testing.T) {
	store := &stubStore{}
	f := newFlusher(store)
	m := &fakeMsg{}

	f.add(writeIntents{}, held(m))
	f.Flush(context.Background())

	assert.True(t, m.acked)
	require.Empty(t, store.rooms)
	require.Empty(t, store.lastSeen)
	require.Empty(t, store.mentions)
}

func TestFlusher_FlushOnEmptyBatchIsNoOp(t *testing.T) {
	store := &stubStore{}
	newFlusher(store).Flush(context.Background())
	assert.Empty(t, store.order)
}

func TestFlusher_RunFlushesOnCancellation(t *testing.T) {
	store := &stubStore{}
	f := newFlusher(store)
	m := &fakeMsg{}
	f.add(writeIntents{RoomID: "r1", LastMsgID: "m1", LastMsgAt: time.Now().UTC()}, held(m))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.Run(ctx, time.Hour, 10*time.Second); close(done) }()
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	assert.True(t, m.acked, "the final flush must land buffered work during shutdown, using a fresh (non-cancelled) context")
}

// panicStore's BulkUpdateRoomLastMessage panics unconditionally, standing in
// for any deterministic panic reached from Flush's write path (e.g. a
// malformed BulkWrite response).
type panicStore struct{}

func (panicStore) BulkUpdateRoomLastMessage(context.Context, map[string]roomLastMsgUpdate) error {
	panic("boom: simulated write panic")
}
func (panicStore) BulkAdvanceLastSeen(context.Context, map[subKey]time.Time) error { return nil }
func (panicStore) BulkSetMentions(context.Context, map[subKey]time.Time) error     { return nil }

// TestFlusher_RunRecoversPanicAndStillReturns proves flush.go's Run is
// jobguard-guarded end to end: a panic inside Flush (reached via Run's final-
// flush-on-shutdown branch) does not escape Run, and the held message that
// triggered the panic is left un-acked rather than lost — replay-safety means
// a redelivery of the same batch is harmless once the poison root cause is
// fixed. Without the guard, the unrecovered panic would kill the goroutine —
// and with it the whole process, since panics in goroutines are fatal — and
// with MaxDeliver=-1 the redelivered message would hit the same panic forever.
func TestFlusher_RunRecoversPanicAndStillReturns(t *testing.T) {
	f := newFlusher(panicStore{})
	m := &fakeMsg{}
	f.add(writeIntents{RoomID: "r1", LastMsgID: "m1", LastMsgAt: time.Now().UTC()}, held(m))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	// interval = time.Hour so only the ctx.Done() final-flush branch can ever
	// fire before we cancel — deterministic, no ticker race, no time.Sleep.
	// (An unguarded panic here would crash the whole test binary, not just
	// fail this assertion — the select below only proves the guarded case.)
	go func() { f.Run(ctx, time.Hour, 10*time.Second); close(done) }()
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation despite a panicking final flush")
	}
	assert.False(t, m.acked, "a panicking flush must leave the held message un-acked so it redelivers")
	assert.False(t, m.naked)
}

func TestClassifyFlushErr(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantPermanent bool
		wantNil       bool
	}{
		{name: "nil stays nil", err: nil, wantNil: true},
		{
			name: "plain connection error is transient",
			err:  errors.New("connection refused"),
		},
		{
			name:          "document rejection is permanent",
			err:           permanentWriteErr(9),
			wantPermanent: true,
		},
		{
			name: "write-concern failure is transient",
			err: mongo.BulkWriteException{
				WriteErrors:       []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 9}}},
				WriteConcernError: &mongo.WriteConcernError{Code: 64},
			},
		},
		{
			name: "retryable label wins over the document rejection",
			err: mongo.BulkWriteException{
				WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 9}}},
				Labels:      []string{"RetryableWriteError"},
			},
		},
		{
			name: "wrapped bulk exception is still classified",
			err: fmt.Errorf("bulk set subscription mentions (1 subscriptions): %w", mongo.BulkWriteException{
				WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 9}}},
			}),
			wantPermanent: true,
		},
		{
			name: "mixing an allow-listed code with an unrecognised/transient code is transient",
			err:  permanentWriteErr(9, 112), // 112 = WriteConflict, not on the allow-list
		},
		{
			name:          "every code allow-listed is permanent",
			err:           permanentWriteErr(9, 11000), // FailedToParse + DuplicateKey
			wantPermanent: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyFlushErr(tc.err)
			if tc.wantNil {
				assert.NoError(t, got)
				return
			}
			require.Error(t, got)
			_, isPermanent := errcode.IsPermanent(got)
			assert.Equal(t, tc.wantPermanent, isPermanent)
		})
	}
}

// Run drives Flush SYNCHRONOUSLY, so a write that never returns stops every
// later flush too — the batch stops draining, held messages stay un-acked past
// AckWait, and JetStream redelivers them into a Mongo that is already
// struggling. Only the shutdown branch used to bound its flush; the periodic
// one inherited the worker's long-lived context and could wedge forever.
func TestFlusher_RunBoundsEachPeriodicFlush(t *testing.T) {
	store := &stubStore{}
	f := newFlusher(store)
	f.add(writeIntents{RoomID: "r1", LastMsgID: "m1", LastMsgAt: time.Now().UTC()}, held(&fakeMsg{}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { f.Run(ctx, time.Millisecond, time.Second); close(done) }()

	require.Eventually(t, store.deadlineSeen, 2*time.Second, 5*time.Millisecond,
		"the periodic flush must bound the write context, not hand it the worker's")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// A flush that outruns its bound must not hold the batch: it has to fail so the
// held messages are Nak'd and redelivered rather than sitting un-acked while
// the flusher is stuck behind them.
func TestFlusher_RunWedgedFlushGivesUpAndKeepsFlushing(t *testing.T) {
	store := &blockingStore{released: make(chan struct{})}
	f := newFlusher(store)
	f.add(writeIntents{RoomID: "r1", LastMsgID: "m1", LastMsgAt: time.Now().UTC()}, held(&fakeMsg{}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { f.Run(ctx, time.Millisecond, 20*time.Millisecond); close(done) }()

	// Keep feeding intents: a flush swaps the batch out, so without new work the
	// later ticks would find it empty and return before reaching the store —
	// proving nothing about whether the loop is still turning.
	feeding := make(chan struct{})
	go func() {
		for {
			select {
			case <-feeding:
				return
			default:
				f.add(writeIntents{RoomID: "r1", LastMsgID: "m", LastMsgAt: time.Now().UTC()}, held(&fakeMsg{}))
				time.Sleep(time.Millisecond)
			}
		}
	}()

	require.Eventually(t, func() bool { return store.calls() >= 2 }, 3*time.Second, 5*time.Millisecond,
		"a wedged flush must time out and let the next flush run, not block the loop forever")

	close(feeding)
	close(store.released)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// blockingStore's first write blocks until its context expires, standing in for
// a Mongo that has stopped answering rather than refusing.
type blockingStore struct {
	mu       sync.Mutex
	n        int
	released chan struct{}
}

func (s *blockingStore) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

func (s *blockingStore) BulkUpdateRoomLastMessage(ctx context.Context, _ map[string]roomLastMsgUpdate) error {
	s.mu.Lock()
	s.n++
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.released:
		return nil
	}
}
func (s *blockingStore) BulkAdvanceLastSeen(context.Context, map[subKey]time.Time) error { return nil }
func (s *blockingStore) BulkSetMentions(context.Context, map[subKey]time.Time) error     { return nil }

// The mentions map is the one write map MaxAckPending does not bound: it grows
// with mentioned accounts per message, not with messages. mention.Parse caps
// neither the token count nor its input beyond the 20KB content limit, so a
// single maximum-size message yields thousands of accounts and a slow-flush
// window can hold MaxAckPending times that — enough that BulkSetMentions cannot
// finish inside FLUSH_TIMEOUT, which under MaxDeliver=-1 is a Nak/rebuild
// livelock with a green readiness probe. Draining on a budget bounds it without
// dropping badges, which a per-message cap would do silently.
func TestFlusher_AddSignalsAnEarlyFlushWhenMentionsCrossTheBudget(t *testing.T) {
	f := newFlusher(&stubStore{}, withEarlyFlush(3, time.Second))
	at := time.Now().UTC()

	crossed := f.add(writeIntents{RoomID: "r1", MentionAccounts: []string{"a", "b"}, MentionAt: at}, held(&fakeMsg{}))
	assert.False(t, crossed, "two intents are under a budget of three")

	crossed = f.add(writeIntents{RoomID: "r1", MentionAccounts: []string{"c"}, MentionAt: at}, held(&fakeMsg{}))
	assert.True(t, crossed, "crossing the budget must not wait for the ticker")
}

// Coalescing is what makes the budget a memory bound rather than a message
// count: the same account mentioned repeatedly in one room is one map entry.
func TestFlusher_AddCountsDistinctMentionsNotMessages(t *testing.T) {
	f := newFlusher(&stubStore{}, withEarlyFlush(2, time.Second))
	at := time.Now().UTC()

	for range 5 {
		if f.add(writeIntents{RoomID: "r1", MentionAccounts: []string{"a"}, MentionAt: at}, held(&fakeMsg{})) {
			t.Fatal("one distinct account must not cross a budget of two however often it is mentioned")
		}
	}
}

// Without a configured budget the ticker stays the only trigger, so every
// existing construction of a flusher keeps its current behaviour.
func TestFlusher_AddNeverSignalsWithoutABudget(t *testing.T) {
	f := newFlusher(&stubStore{})
	at := time.Now().UTC()

	crossed := f.add(writeIntents{RoomID: "r1", MentionAccounts: []string{"a", "b", "c"}, MentionAt: at}, held(&fakeMsg{}))

	assert.False(t, crossed, "an unset budget must not change when a flusher drains")
}
