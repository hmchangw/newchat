package failure

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingBufferedJournal struct {
	mu          sync.Mutex
	events      []Event
	written     chan struct{}
	syncs       int
	compactions int
	closed      bool
	appendErr   error
	syncErr     error
	compactErr  error
	closeErr    error
	configured  bool
	upgrade     bool
}

func newRecordingBufferedJournal() *recordingBufferedJournal {
	return &recordingBufferedJournal{written: make(chan struct{}, 16)}
}

func (j *recordingBufferedJournal) Replay() ([]Event, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]Event(nil), j.events...), nil
}

func (j *recordingBufferedJournal) Append(event *Event) error {
	if err := j.AppendBuffered(event); err != nil {
		return err
	}
	return j.Sync()
}

func (j *recordingBufferedJournal) AppendBuffered(event *Event) error {
	if j.appendErr != nil {
		return j.appendErr
	}
	j.mu.Lock()
	j.events = append(j.events, *event)
	j.mu.Unlock()
	j.written <- struct{}{}
	return nil
}

func (j *recordingBufferedJournal) Sync() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.syncs++
	return j.syncErr
}

func (j *recordingBufferedJournal) Compact([]Event) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.compactions++
	return j.compactErr
}

func (j *recordingBufferedJournal) Size() int64 { return 0 }

func (j *recordingBufferedJournal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.closed = true
	return j.closeErr
}

func (j *recordingBufferedJournal) ConfigureObserverContract(ObserverContract, []Operation) error {
	j.configured = true
	return nil
}

func (j *recordingBufferedJournal) NeedsUpgrade() bool { return j.upgrade }

func (j *recordingBufferedJournal) syncCount() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.syncs
}

type flushRecord struct {
	batch int
	err   error
}

type recordingFlushRecorder struct {
	mu      sync.Mutex
	records []flushRecord
}

func (r *recordingFlushRecorder) RecordWALFlush(_ time.Duration, batchSize int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, flushRecord{batch: batchSize, err: err})
}

func TestFailureGroupCommit_BatchesConcurrentDurableIntents(t *testing.T) {
	inner := newRecordingBufferedJournal()
	recorder := &recordingFlushRecorder{}
	journal := NewGroupCommit(inner, 100*time.Millisecond, 256, recorder)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	results := make(chan error, 2)
	for _, id := range []string{"operation-1", "operation-2"} {
		id := id
		go func() {
			results <- journal.Append(&Event{Type: EventStarted, Operation: &Operation{ID: id}})
		}()
		awaitGroupWrite(t, inner.written)
	}

	require.NoError(t, <-results)
	require.NoError(t, <-results)
	assert.Equal(t, 1, inner.syncCount())
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	require.Len(t, recorder.records, 1)
	assert.Equal(t, 2, recorder.records[0].batch)
}

func TestFailureGroupCommit_NonBarrierReturnsBeforeCloseFlush(t *testing.T) {
	inner := newRecordingBufferedJournal()
	journal := NewGroupCommit(inner, time.Hour, 256)

	require.NoError(t, journal.Append(&Event{Type: EventObserved}))
	assert.Zero(t, inner.syncCount())
	require.NoError(t, journal.Close())
	assert.Equal(t, 1, inner.syncCount())
}

func TestFailureGroupCommit_InvalidationWaitsForDurability(t *testing.T) {
	inner := newRecordingBufferedJournal()
	journal := NewGroupCommit(inner, time.Hour, 1)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })

	require.NoError(t, journal.Append(&Event{Type: EventInvalidated}))
	assert.Equal(t, 1, inner.syncCount())
}

func TestFailureGroupCommit_SyncFailureIsSticky(t *testing.T) {
	inner := newRecordingBufferedJournal()
	inner.syncErr = assert.AnError
	recorder := &recordingFlushRecorder{}
	journal := NewGroupCommit(inner, time.Hour, 1, recorder)
	t.Cleanup(func() { _ = journal.Close() })

	err := journal.Append(&Event{Type: EventStarted})
	require.ErrorIs(t, err, assert.AnError)
	err = journal.Append(&Event{Type: EventObserved})
	require.ErrorIs(t, err, assert.AnError)
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	require.Len(t, recorder.records, 1)
	assert.ErrorIs(t, recorder.records[0].err, assert.AnError)
}

func TestFailureGroupCommit_AppendFailureReleasesWaitingBarriers(t *testing.T) {
	inner := newRecordingBufferedJournal()
	journal := NewGroupCommit(inner, time.Hour, 256)
	t.Cleanup(func() { _ = journal.Close() })
	first := make(chan error, 1)
	go func() { first <- journal.Append(&Event{Type: EventStarted}) }()
	awaitGroupWrite(t, inner.written)
	inner.appendErr = assert.AnError

	err := journal.Append(&Event{Type: EventObserved})
	require.ErrorIs(t, err, assert.AnError)
	require.ErrorIs(t, <-first, assert.AnError)
}

func TestFailureGroupCommit_CompactFlushesDirtyEvents(t *testing.T) {
	inner := newRecordingBufferedJournal()
	journal := NewGroupCommit(inner, time.Hour, 256)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	require.NoError(t, journal.Append(&Event{Type: EventObserved}))

	require.NoError(t, journal.Compact([]Event{{Type: EventCheckpoint}}))
	assert.Equal(t, 1, inner.syncCount())
	assert.Equal(t, 1, inner.compactions)
}

func TestFailureGroupCommit_RejectsNilAndClosedRequests(t *testing.T) {
	inner := newRecordingBufferedJournal()
	journal := NewGroupCommit(inner, time.Hour, 256)
	require.Error(t, journal.Append(nil))
	require.NoError(t, journal.Close())
	require.NoError(t, journal.Close())
	assert.Error(t, journal.Append(&Event{Type: EventObserved}))
	assert.Error(t, journal.Compact(nil))
}

func TestFailureGroupCommit_ForwardsOptionalJournalContracts(t *testing.T) {
	inner := newRecordingBufferedJournal()
	inner.upgrade = true
	journal := NewGroupCommit(inner, time.Hour, 256)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })

	assert.True(t, journal.NeedsUpgrade())
	require.NoError(t, journal.ConfigureObserverContract(ObserverContract{}, nil))
	assert.True(t, inner.configured)
}

type streamingBufferedJournal struct {
	*recordingBufferedJournal
	streamed int
}

func (j *streamingBufferedJournal) ReplayEach(emit func(*Event) error) error {
	j.streamed++
	for index := range j.events {
		if err := emit(&j.events[index]); err != nil {
			return err
		}
	}
	return nil
}

func TestFailureGroupCommit_ReplayEachStreamsWhenAvailable(t *testing.T) {
	inner := &streamingBufferedJournal{recordingBufferedJournal: newRecordingBufferedJournal()}
	inner.events = []Event{{Type: EventCheckpoint}}
	journal := NewGroupCommit(inner, time.Hour, 256)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })

	seen := 0
	require.NoError(t, journal.ReplayEach(func(*Event) error { seen++; return nil }))
	assert.Equal(t, 1, inner.streamed)
	assert.Equal(t, 1, seen)
}

func TestFailureGroupCommit_ReplayEachFallsBackToBuffering(t *testing.T) {
	inner := newRecordingBufferedJournal()
	inner.events = []Event{{Type: EventCheckpoint}}
	journal := NewGroupCommit(inner, time.Hour, 256)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })

	seen := 0
	require.NoError(t, journal.ReplayEach(func(*Event) error { seen++; return nil }))
	assert.Equal(t, 1, seen)

	err := journal.ReplayEach(func(*Event) error { return errors.New("apply") })
	require.ErrorContains(t, err, "apply group-commit")
}

func awaitGroupWrite(t *testing.T, written <-chan struct{}) {
	t.Helper()
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for buffered journal write")
	}
}
