package main

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeSoakReplayJournal lays down a journal holding `finalized` operations that
// resolved cleanly plus `active` that did not, i.e. a run whose evidence has
// mostly been retired. Only the active ones survive recovery.
func writeSoakReplayJournal(t *testing.T, path string, finalized, active int) *fileFailureWAL {
	t.Helper()
	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	require.NoError(t, wal.ConfigureObserverContract(newFailureObserverContract(false, false), nil))

	now := time.Unix(1000, 0).UTC()
	write := func(id string, finalize bool) {
		operation := testFailureOperation(id, now)
		operation.SchemaVersion = failureWALSchemaVersion
		operation.RunID = "soak-replay-probe"
		operation.LifecycleState = failureOperationJournaled
		operation.OperationType = failureOperationMessageCreate
		operation.Targets = map[string]string{"roomId": "room-000001", "messageId": id}
		operation.Attributes = map[string]string{"account": "user-000001"}
		operation.Effects = messageCreateExpectedEffectsForObservers(false, false, 0, "")
		events := []failureLedgerEvent{
			{SchemaVersion: failureWALSchemaVersion, Type: failureLedgerEventStarted, Operation: operation, At: now},
			{SchemaVersion: failureWALSchemaVersion, Type: failureLedgerEventActivated, OperationID: id, At: now},
		}
		if finalize {
			for _, observer := range operation.Expected {
				events = append(events, failureLedgerEvent{
					SchemaVersion: failureWALSchemaVersion, Type: failureLedgerEventObserved,
					OperationID: id, Observer: observer,
					Observation: failureObservationGood, At: now,
				})
			}
			events = append(events, failureLedgerEvent{
				SchemaVersion: failureWALSchemaVersion, Type: failureLedgerEventFinalized,
				OperationID: id, Result: failureResultGood, At: now,
			})
		}
		for i := range events {
			require.NoError(t, wal.AppendBuffered(&events[i]))
		}
	}
	for i := range finalized {
		write(fmt.Sprintf("done-%08d", i), true)
	}
	for i := range active {
		write(fmt.Sprintf("live-%08d", i), false)
	}
	require.NoError(t, wal.Close())

	reopened, err := openFailureWAL(path)
	require.NoError(t, err)
	return reopened
}

func soakHeapAllocMB() float64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return float64(stats.HeapAlloc) / (1 << 20)
}

// recordingFailureJournal reports which recovery path the ledger took. Measuring
// the heap after recovery cannot tell them apart — the buffered slice is dead by
// then and the GC has already reclaimed it — so the property has to be pinned at
// the call, not at the allocator.
type recordingFailureJournal struct {
	events         []failureLedgerEvent
	bufferedReplay int
	streamedReplay int
}

func (j *recordingFailureJournal) Replay() ([]failureLedgerEvent, error) {
	j.bufferedReplay++
	return j.events, nil
}

func (j *recordingFailureJournal) ReplayEach(emit func(*failureLedgerEvent) error) error {
	j.streamedReplay++
	for i := range j.events {
		if err := emit(&j.events[i]); err != nil {
			return err
		}
	}
	return nil
}

func (j *recordingFailureJournal) Append(*failureLedgerEvent) error         { return nil }
func (j *recordingFailureJournal) AppendBuffered(*failureLedgerEvent) error { return nil }
func (j *recordingFailureJournal) Sync() error                              { return nil }
func (j *recordingFailureJournal) Compact([]failureLedgerEvent) error       { return nil }
func (j *recordingFailureJournal) Size() int64                              { return 0 }
func (j *recordingFailureJournal) Close() error                             { return nil }

// A journal that has been running for hours is mostly retired evidence, and
// reading all of it into one slice is what turns a restart into an OOM — worse
// on every restart, because the journal only grows. Recovery must take the
// streaming path whenever the journal offers one.
func TestFailureLedger_RecoveryStreamsInsteadOfBufferingTheJournal(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	journal := &recordingFailureJournal{events: []failureLedgerEvent{
		{Type: failureLedgerEventStarted, Operation: testFailureOperation("op-1", now), At: now},
		{Type: failureLedgerEventActivated, OperationID: "op-1", At: now},
	}}

	ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 10, Journal: journal})
	require.NoError(t, err)

	assert.Equal(t, 1, journal.streamedReplay)
	assert.Equal(t, 0, journal.bufferedReplay,
		"recovery must not fall back to buffering when the journal streams")
	assert.Len(t, ledger.active, 1, "streaming must rebuild the same state")
}

// A journal that cannot stream still has to recover, or an older on-disk format
// would crash-loop the pod instead of degrading.
func TestFailureLedger_RecoveryFallsBackToBufferingWhenTheJournalCannotStream(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	journal := &bufferOnlyFailureJournal{events: []failureLedgerEvent{
		{Type: failureLedgerEventStarted, Operation: testFailureOperation("op-1", now), At: now},
	}}

	ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 10, Journal: journal})

	require.NoError(t, err)
	assert.Equal(t, 1, journal.replays)
	assert.Len(t, ledger.active, 1)
}

type bufferOnlyFailureJournal struct {
	events  []failureLedgerEvent
	replays int
}

func (j *bufferOnlyFailureJournal) Replay() ([]failureLedgerEvent, error) {
	j.replays++
	return j.events, nil
}
func (j *bufferOnlyFailureJournal) Append(*failureLedgerEvent) error         { return nil }
func (j *bufferOnlyFailureJournal) AppendBuffered(*failureLedgerEvent) error { return nil }
func (j *bufferOnlyFailureJournal) Sync() error                              { return nil }
func (j *bufferOnlyFailureJournal) Compact([]failureLedgerEvent) error       { return nil }
func (j *bufferOnlyFailureJournal) Size() int64                              { return 0 }
func (j *bufferOnlyFailureJournal) Close() error                             { return nil }

// The streaming reader must hold nothing it has already handed over: that is the
// whole point, and it is the difference between recovery costing the surviving
// operations and costing the file. Both figures are taken with the buffered
// slice deliberately alive, so the comparison is real rather than a GC artefact.
func TestFileFailureWAL_StreamedReplayRetainsNothingItHasEmitted(t *testing.T) {
	wal := writeSoakReplayJournal(t, t.TempDir()+"/retain.wal", 60000, 0)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })

	base := soakHeapAllocMB()
	buffered, err := wal.Replay()
	require.NoError(t, err)
	bufferedCost := soakHeapAllocMB() - base
	require.NotEmpty(t, buffered)
	runtime.KeepAlive(buffered)

	base = soakHeapAllocMB()
	streamed := 0
	require.NoError(t, wal.ReplayEach(func(*failureLedgerEvent) error {
		streamed++
		return nil
	}))
	streamedCost := soakHeapAllocMB() - base

	assert.Equal(t, len(buffered), streamed, "both readers must see the same records")
	assert.Greater(t, bufferedCost, 50.0,
		"the journal must be big enough for the comparison to mean anything")
	assert.Less(t, streamedCost, bufferedCost/10,
		"streaming held %.1fMB against buffering's %.1fMB", streamedCost, bufferedCost)
}

// The streaming path is only worth anything if the journal the soak actually
// builds uses it. A wrapper that forgets to forward it silently reinstates the
// buffering replay, so this pins the forwarding rather than the interface.
func TestFailureJournal_ProductionChainForwardsStreamingToTheWAL(t *testing.T) {
	inner := &recordingFailureJournal{events: []failureLedgerEvent{
		{Type: failureLedgerEventActivated, OperationID: "op-1"},
	}}
	chain := newFailureJournalMetrics(
		newFailureJournalGroupCommit(inner, time.Millisecond, 8), nil,
	)

	seen := 0
	require.NoError(t, chain.ReplayEach(func(*failureLedgerEvent) error {
		seen++
		return nil
	}))

	assert.Equal(t, 1, inner.streamedReplay, "the wrappers must forward, not buffer")
	assert.Equal(t, 0, inner.bufferedReplay)
	assert.Equal(t, 1, seen)
}

// A journal that predates the streaming reader still has to recover, or an
// older on-disk format would crash-loop the pod instead of degrading.
func TestFailureJournal_WrappersBufferWhenTheInnerJournalCannotStream(t *testing.T) {
	inner := &bufferOnlyFailureJournal{events: []failureLedgerEvent{
		{Type: failureLedgerEventActivated, OperationID: "op-1"},
	}}
	chain := newFailureJournalMetrics(
		newFailureJournalGroupCommit(inner, time.Millisecond, 8), nil,
	)

	seen := 0
	require.NoError(t, chain.ReplayEach(func(*failureLedgerEvent) error {
		seen++
		return nil
	}))

	assert.Equal(t, 1, inner.replays)
	assert.Equal(t, 1, seen)
}

func TestFileFailureWAL_ReplayEachYieldsEveryRecordInOrder(t *testing.T) {
	wal := writeSoakReplayJournal(t, t.TempDir()+"/order.wal", 3, 2)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })

	var streamed []string
	require.NoError(t, wal.ReplayEach(func(event *failureLedgerEvent) error {
		streamed = append(streamed, event.Type)
		return nil
	}))

	buffered, err := wal.Replay()
	require.NoError(t, err)
	types := make([]string, 0, len(buffered))
	for i := range buffered {
		types = append(types, buffered[i].Type)
	}
	assert.Equal(t, types, streamed)
}

func TestFileFailureWAL_ReplayEachStopsOnTheFirstApplyError(t *testing.T) {
	wal := writeSoakReplayJournal(t, t.TempDir()+"/stop.wal", 3, 0)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })

	seen := 0
	err := wal.ReplayEach(func(*failureLedgerEvent) error {
		seen++
		return fmt.Errorf("boom")
	})

	require.Error(t, err)
	assert.Equal(t, 1, seen)
}
