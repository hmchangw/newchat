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

func TestResultsStore_CountedMapPruningSymmetric(t *testing.T) {
	// Test that counted map is pruned on both eviction sides.
	// With recentCap=2, failedCap=2, upsert three distinct StateFailed rows.
	// After the third, counted should have exactly 2 entries (the ones still in failures).
	s := newResultsStore(2, 2, nil)
	s.Upsert(mkResult("x", StateFailed))
	s.Upsert(mkResult("y", StateFailed))
	s.Upsert(mkResult("z", StateFailed))

	// Lock to inspect internal state
	s.mu.Lock()
	defer s.mu.Unlock()

	// At this point:
	// - recent has the 2 newest: z, y
	// - failures has the 2 newest: z, y
	// - x was evicted from both
	// - counted should have only y, z (not x)
	assert.Len(t, s.counted, 2, "counted map should have exactly 2 entries after third failure evicts oldest")
	assert.True(t, s.counted["y"], "y should be in counted")
	assert.True(t, s.counted["z"], "z should be in counted")
	assert.False(t, s.counted["x"], "x should not be in counted after eviction from both lists")
}

func TestResultsStore_DeepCopyTargets(t *testing.T) {
	// Test that Upsert deep-copies Targets and Diffs slices.
	// Mutating the original should not affect stored values.
	s := newResultsStore(10, 10, nil)

	original := CheckResult{
		ID:         "test",
		Collection: "c",
		Op:         "insert",
		DocID:      "d",
		State:      StateMatched,
		Targets: []TargetResult{
			{
				Alias:   "a",
				Matched: false,
				Diffs: []FieldDiff{
					{SourcePath: "p", DestField: "df", Want: "v1", Got: "v2"},
				},
			},
		},
	}

	s.Upsert(original)

	// Mutate the original's slices
	original.Targets[0].Matched = true
	original.Targets[0].Diffs[0].SourcePath = "changed"

	// Verify Recent() still shows un-mutated values
	recent := s.Recent()
	require.Len(t, recent, 1)
	assert.False(t, recent[0].Targets[0].Matched, "stored Matched should not be mutated")
	assert.Equal(t, "p", recent[0].Targets[0].Diffs[0].SourcePath, "stored SourcePath should not be changed")
}
