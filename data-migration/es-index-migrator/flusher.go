package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hmchangw/chat/pkg/searchengine"
)

// flusher buffers ES bulk actions and flushes them in batches. It is not
// safe for concurrent use by multiple goroutines without external
// synchronization — see runner.go's flusher-per-worker-pool usage.
//
// Note on the batchSize-overshoot race: if multiple callers observed
// len(buffered) < batchSize before either flushed, the buffer can briefly
// exceed batchSize before the next Add call's own check triggers a flush.
// This is intentional and harmless given the caller (runner.go) uses one
// flusher per collection with WORKER_CONCURRENCY workers feeding it under
// a mutex — the overshoot is bounded by concurrency, not unbounded.
type flusher struct {
	store       ESStore
	batchSize   int
	buffered    []searchengine.BulkAction
	failedCount int
}

func newFlusher(store ESStore, batchSize int) *flusher {
	return &flusher{store: store, batchSize: batchSize, buffered: make([]searchengine.BulkAction, 0, batchSize)}
}

// Add buffers action, auto-flushing when the buffer reaches batchSize.
// A zero-value action (searchengine.BulkAction{}) is silently ignored —
// callers like buildUserRoomAction return one to signal "skip this row"
// (e.g. a bot subscription) without needing a separate sentinel type.
//
//nolint:gocritic // hugeParam is acceptable: we store the action by value anyway
func (f *flusher) Add(ctx context.Context, action searchengine.BulkAction) error {
	if action.Action == "" {
		return nil
	}
	f.buffered = append(f.buffered, action)
	if len(f.buffered) >= f.batchSize {
		return f.Flush(ctx)
	}
	return nil
}

// Flush sends every buffered action as one ES _bulk request and clears the
// buffer. A per-item failure is logged and counted (FailedCount) but does
// not itself make Flush return an error — the caller decides at the end of
// the run whether any failures occurred. Flush only returns an error for a
// request-level failure (the whole _bulk call failed) or a result-count
// mismatch, both of which mean every buffered action's outcome is unknown.
func (f *flusher) Flush(ctx context.Context) error {
	if len(f.buffered) == 0 {
		return nil
	}
	actions := f.buffered
	f.buffered = make([]searchengine.BulkAction, 0, f.batchSize)

	results, err := f.store.Bulk(ctx, actions)
	if err != nil {
		f.failedCount += len(actions)
		return fmt.Errorf("bulk flush %d actions: %w", len(actions), err)
	}
	if len(results) != len(actions) {
		f.failedCount += len(actions)
		return fmt.Errorf("bulk flush: expected %d results, got %d", len(actions), len(results))
	}

	for i, result := range results {
		if searchengine.IsBulkItemSuccess(actions[i].Action, result) {
			continue
		}
		f.failedCount++
		slog.Error("bulk item failed",
			"status", result.Status, "error", result.Error, "docID", actions[i].DocID, "index", actions[i].Index)
	}
	return nil
}

// FailedCount returns the running total of bulk items that failed across
// every Flush call so far (including ones counted when Flush itself
// returned a request-level error).
func (f *flusher) FailedCount() int { return f.failedCount }
