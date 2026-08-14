package main

import (
	"context"
	"errors"
	"fmt"
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
	order    []string
	rooms    map[string]roomLastMsgUpdate
	lastSeen map[subKey]time.Time
	mentions map[subKey]time.Time
	failWith map[string]error // stage name ("rooms"/"lastSeen"/"mentions") -> error to return
}

func (s *stubStore) BulkUpdateRoomLastMessage(ctx context.Context, u map[string]roomLastMsgUpdate) error {
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
	go func() { f.Run(ctx, time.Hour, 5*time.Second); close(done) }()
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
	go func() { f.Run(ctx, time.Hour, 5*time.Second); close(done) }()
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
