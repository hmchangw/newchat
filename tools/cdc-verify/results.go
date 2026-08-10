package main

import (
	"sync"
)

// deepCopyResult creates a deep copy of a CheckResult with fresh backing
// arrays for Targets and Diffs slices. CheckResult is passed by value
// throughout this package (Upsert, Snapshot) to match the JSON-serializable
// value semantics of the type; switching to a pointer here alone would be
// inconsistent.
//
//nolint:gocritic // CheckResult is passed by value throughout this package
func deepCopyResult(r CheckResult) CheckResult {
	result := r
	if len(r.Targets) > 0 {
		result.Targets = make([]TargetResult, len(r.Targets))
		for i := range r.Targets {
			result.Targets[i] = r.Targets[i]
			if len(r.Targets[i].Diffs) > 0 {
				result.Targets[i].Diffs = make([]FieldDiff, len(r.Targets[i].Diffs))
				copy(result.Targets[i].Diffs, r.Targets[i].Diffs)
			}
		}
	}
	return result
}

type CheckState string

const (
	StatePending    CheckState = "pending"
	StateMatched    CheckState = "matched"
	StateFailed     CheckState = "failed"
	StateSkipped    CheckState = "skipped"
	StateSuperseded CheckState = "superseded"
)

// TargetResult is one sub-check's live state.
type TargetResult struct {
	Alias     string      `json:"alias"`
	Matched   bool        `json:"matched"`
	LastCause string      `json:"lastCause,omitempty"` // "", "mismatch", "dest-missing", "resolver-miss: u", "ambiguous-key", "lookup-error: ...", "source-missing"
	Diffs     []FieldDiff `json:"diffs,omitempty"`     // populated on final failure only
}

// CheckResult is one table row. A copy is what leaves the store — callers
// never see the live pointer.
type CheckResult struct {
	ID          string         `json:"id"` // idgen.GenerateID()
	Collection  string         `json:"collection"`
	Op          string         `json:"op"`
	DocID       string         `json:"docId"`
	State       CheckState     `json:"state"`
	SkipReason  string         `json:"skipReason,omitempty"`
	Targets     []TargetResult `json:"targets,omitempty"`
	Attempts    int            `json:"attempts"`
	StartedAtMs int64          `json:"startedAtMs"`
	EndedAtMs   int64          `json:"endedAtMs,omitempty"`
}

type Counters struct {
	Checked    uint64 `json:"checked"`
	Matched    uint64 `json:"matched"`
	Failed     uint64 `json:"failed"`
	Skipped    uint64 `json:"skipped"`
	Superseded uint64 `json:"superseded"`
	Evicted    uint64 `json:"evicted"` // failures dropped by FAILED_CAP
}

type resultsStore struct {
	mu        sync.Mutex
	recent    []CheckResult // newest at index 0
	failures  []CheckResult // newest at index 0
	counters  Counters
	counted   map[string]bool // check IDs whose terminal state was tallied
	recentCap int
	failedCap int
	onUpdate  func(CheckResult)
}

func newResultsStore(recentCap, failedCap int, onUpdate func(CheckResult)) *resultsStore {
	return &resultsStore{
		counted:   map[string]bool{},
		recentCap: recentCap,
		failedCap: failedCap,
		onUpdate:  onUpdate,
	}
}

//nolint:gocritic // value receiver matches the package-wide CheckResult convention
func (s *resultsStore) Upsert(r CheckResult) {
	// Deep-copy the incoming result to own its slices and protect from caller mutations
	r = deepCopyResult(r)

	s.mu.Lock()
	idx := -1
	for i := range s.recent {
		if s.recent[i].ID == r.ID {
			idx = i
			break
		}
	}
	if idx >= 0 {
		s.recent[idx] = r
	} else {
		s.recent = append([]CheckResult{r}, s.recent...)
		if len(s.recent) > s.recentCap {
			evicted := s.recent[len(s.recent)-1]
			s.recent = s.recent[:s.recentCap]
			delete(s.counted, evictedIDIfUncounted(s.failures, evicted))
		}
	}
	if isTerminal(r.State) && !s.counted[r.ID] {
		s.counted[r.ID] = true
		s.counters.Checked++
		switch r.State {
		case StateMatched:
			s.counters.Matched++
		case StateFailed:
			s.counters.Failed++
			s.failures = append([]CheckResult{r}, s.failures...)
			if len(s.failures) > s.failedCap {
				evictedFromFailures := s.failures[s.failedCap]
				s.failures = s.failures[:s.failedCap]
				s.counters.Evicted++
				// Symmetric pruning: remove from counted if also not in recent
				delete(s.counted, evictedIDIfUncounted(s.recent, evictedFromFailures))
			}
		case StateSkipped:
			s.counters.Skipped++
		case StateSuperseded:
			s.counters.Superseded++
		default:
			// StatePending is not terminal and never reaches this switch
			// (guarded by isTerminal above); no counter to bump.
		}
	}
	cb := s.onUpdate
	s.mu.Unlock()
	if cb != nil {
		cb(r)
	}
}

func isTerminal(st CheckState) bool {
	return st == StateMatched || st == StateFailed || st == StateSkipped || st == StateSuperseded
}

// evictedIDIfUncounted checks if the evicted ID is still present in the "other" list.
// If found in the other list, returns "" (no delete); if not found, returns the ID for deletion.
// Called at both eviction sites:
// - recent-eviction: check if ID is still in failures (passed as others)
// - failures-eviction: check if ID is still in recent (passed as others)
// An ID evicted from both windows can never be upserted again (checks are single-writer),
// so its dedup entry is dropped.
//
//nolint:gocritic // value receiver matches the package-wide CheckResult convention
func evictedIDIfUncounted(others []CheckResult, r CheckResult) string {
	for i := range others {
		if others[i].ID == r.ID {
			return "" // still referenced; deleting "" is a no-op
		}
	}
	return r.ID
}

func (s *resultsStore) Recent() []CheckResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]CheckResult, len(s.recent))
	copy(out, s.recent)
	return out
}

func (s *resultsStore) Failures() []CheckResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]CheckResult, len(s.failures))
	copy(out, s.failures)
	return out
}

func (s *resultsStore) Snapshot() ([]CheckResult, []CheckResult, Counters) {
	s.mu.Lock()
	defer s.mu.Unlock()
	recent := make([]CheckResult, len(s.recent))
	copy(recent, s.recent)
	failures := make([]CheckResult, len(s.failures))
	copy(failures, s.failures)
	return recent, failures, s.counters
}
