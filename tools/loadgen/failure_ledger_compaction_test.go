package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type compactionCountingJournal struct {
	events   []failureLedgerEvent
	appends  int
	compacts int
	size     int64
}

func (j *compactionCountingJournal) Replay() ([]failureLedgerEvent, error) { return j.events, nil }

func (j *compactionCountingJournal) ReplayEach(emit func(*failureLedgerEvent) error) error {
	for i := range j.events {
		if err := emit(&j.events[i]); err != nil {
			return err
		}
	}
	return nil
}

func (j *compactionCountingJournal) Append(*failureLedgerEvent) error {
	j.appends++
	j.size += 512
	return nil
}

func (j *compactionCountingJournal) Compact(events []failureLedgerEvent) error {
	j.compacts++
	j.size = int64(len(events)) * 512
	return nil
}

func (j *compactionCountingJournal) Size() int64  { return j.size }
func (j *compactionCountingJournal) Close() error { return nil }

func compactionTestEvents(t *testing.T, operations int) []failureLedgerEvent {
	t.Helper()
	now := time.Unix(1000, 0).UTC()
	events := make([]failureLedgerEvent, 0, operations*2)
	for i := range operations {
		id := fmt.Sprintf("op-%05d", i)
		events = append(events,
			failureLedgerEvent{Type: failureLedgerEventStarted, Operation: testFailureOperation(id, now), At: now},
			failureLedgerEvent{Type: failureLedgerEventActivated, OperationID: id, At: now},
		)
	}
	return events
}

// The journal a restarting run inherits is mostly retired evidence, but nothing
// used to reclaim it until 10,000 more operations finalized — and a pod that
// keeps dying never gets there, so the file only ever grew. Recovery reclaims it
// once, up front, which is what stops the growth from compounding across
// restarts.
func TestFailureLedger_CompactsOnceAfterRecoveringAJournal(t *testing.T) {
	journal := &compactionCountingJournal{events: compactionTestEvents(t, 50), size: 1 << 20}

	ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 1000, Journal: journal})
	require.NoError(t, err)

	assert.Equal(t, 1, journal.compacts, "recovery must reclaim the inherited journal")
	assert.Len(t, ledger.active, 50)
	assert.Less(t, journal.Size(), int64(1<<20),
		"compaction must actually shrink the file it rewrote")
}

func TestFailureLedger_DoesNotCompactAnEmptyJournalOnRecovery(t *testing.T) {
	journal := &compactionCountingJournal{}

	_, err := newFailureLedger(&failureLedgerConfig{Capacity: 1000, Journal: journal})
	require.NoError(t, err)

	assert.Equal(t, 0, journal.compacts, "there is nothing to reclaim in a fresh journal")
}

// Waiting for a finalize count means a run whose operations all outlive their
// deadline never compacts at all. Size is the bound that actually matters, so
// it triggers on its own.
func TestFailureLedger_CompactsWhenTheJournalOutgrowsItsBudget(t *testing.T) {
	journal := &compactionCountingJournal{}
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 1000, Journal: journal,
		CompactEvery: 1000000, MaxJournalBytes: 4096,
	})
	require.NoError(t, err)
	now := time.Unix(2000, 0).UTC()

	for i := range 40 {
		operation := testFailureOperation(fmt.Sprintf("live-%03d", i), now)
		require.NoError(t, ledger.Start(operation))
	}

	assert.NoError(t, ledger.MaybeCompact(now))
	assert.Positive(t, journal.compacts,
		"a journal past its byte budget must be reclaimed without waiting for finalizations")
}

func TestFailureLedger_LeavesTheJournalAloneWhileUnderBudget(t *testing.T) {
	journal := &compactionCountingJournal{}
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 1000, Journal: journal,
		CompactEvery: 1000000, MaxJournalBytes: 1 << 20,
	})
	require.NoError(t, err)
	now := time.Unix(2000, 0).UTC()
	require.NoError(t, ledger.Start(testFailureOperation("live-000", now)))
	before := journal.compacts

	require.NoError(t, ledger.MaybeCompact(now))

	assert.Equal(t, before, journal.compacts)
}

// An operation between claiming its slot and having its start record written is
// not in the active set a compaction rewrites from, so compacting now would
// erase it. The budget check must not override that.
func TestFailureLedger_DefersCompactionWhileAnOperationIsMidStart(t *testing.T) {
	journal := &compactionCountingJournal{}
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 1000, Journal: journal,
		CompactEvery: 1000000, MaxJournalBytes: 1,
	})
	require.NoError(t, err)
	ledger.starting["in-flight"] = struct{}{}
	before := journal.compacts

	require.NoError(t, ledger.MaybeCompact(time.Unix(2000, 0).UTC()))

	assert.Equal(t, before, journal.compacts,
		"compacting now would drop the operation whose start record is still in flight")
}
