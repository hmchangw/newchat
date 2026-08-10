package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mkResult(id string, st CheckState) CheckResult {
	return CheckResult{ID: id, Collection: "c", Op: "insert", DocID: "d" + id, State: st}
}

func TestResultsStore_UpsertAndRecentOrder(t *testing.T) {
	s := newResultsStore(3, 10, nil)
	s.Upsert(mkResult("a", StatePending))
	s.Upsert(mkResult("b", StatePending))
	got := s.Recent()
	require.Len(t, got, 2)
	assert.Equal(t, "b", got[0].ID) // newest first

	// update in place keeps position, changes state
	s.Upsert(mkResult("a", StateMatched))
	got = s.Recent()
	require.Len(t, got, 2)
	assert.Equal(t, StateMatched, got[1].State)
}

func TestResultsStore_RecentCapEvicts(t *testing.T) {
	s := newResultsStore(2, 10, nil)
	for i := 0; i < 4; i++ {
		s.Upsert(mkResult(fmt.Sprint(i), StateMatched))
	}
	got := s.Recent()
	require.Len(t, got, 2)
	assert.Equal(t, "3", got[0].ID)
	assert.Equal(t, "2", got[1].ID)
}

func TestResultsStore_FailuresAndEviction(t *testing.T) {
	s := newResultsStore(10, 2, nil)
	s.Upsert(mkResult("x", StateFailed))
	s.Upsert(mkResult("y", StateFailed))
	s.Upsert(mkResult("z", StateFailed))
	f := s.Failures()
	require.Len(t, f, 2)
	assert.Equal(t, "z", f[0].ID)
	_, _, c := s.Snapshot()
	assert.Equal(t, uint64(3), c.Failed)
	assert.Equal(t, uint64(1), c.Evicted)
}

func TestResultsStore_CountersOncePerCheck(t *testing.T) {
	s := newResultsStore(10, 10, nil)
	r := mkResult("a", StatePending)
	s.Upsert(r)
	r.State = StateMatched
	s.Upsert(r)
	s.Upsert(r) // duplicate terminal upsert must not double-count
	_, _, c := s.Snapshot()
	assert.Equal(t, uint64(1), c.Checked)
	assert.Equal(t, uint64(1), c.Matched)
}

func TestResultsStore_OnUpdateFires(t *testing.T) {
	var events []CheckResult
	s := newResultsStore(10, 10, func(r CheckResult) { events = append(events, r) })
	s.Upsert(mkResult("a", StatePending))
	s.Upsert(mkResult("a", StateMatched))
	require.Len(t, events, 2)
	assert.Equal(t, StateMatched, events[1].State)
}
