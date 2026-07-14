package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

// cacheLoadTimeout bounds a single full scan of the hr_employee collection.
const cacheLoadTimeout = time.Minute

// directoryCache is the in-memory account → employee directory. The backing
// hr_employee collection is rewritten wholesale by a daily HR cron, so the
// whole map is swapped wholesale by Load (at startup and on the periodic
// refresh) — entries have no TTL.
//
// The snapshot lives in an atomic.Pointer: reads (Get/Ready) happen all day and
// are a single lock-free load of an immutable map, while the once-a-day refresh
// swaps in a freshly built map with one atomic store. The map is never mutated
// in place, so readers never need a lock.
type directoryCache struct {
	entries atomic.Pointer[map[string]employee]
}

func newDirectoryCache() *directoryCache {
	return &directoryCache{}
}

// Get returns the directory entry for account.
func (c *directoryCache) Get(account string) (employee, bool) {
	m := c.entries.Load()
	if m == nil {
		return employee{}, false
	}
	e, ok := (*m)[account]
	return e, ok
}

// Ready reports whether the cache holds directory data — the /readyz signal.
// An empty directory is not ready: the portal cannot resolve any account.
func (c *directoryCache) Ready() bool {
	m := c.entries.Load()
	return m != nil && len(*m) > 0
}

// Load reads the full employee directory and swaps it in. An empty snapshot
// after a prior successful load (the HR cron may be mid-rewrite) is rejected;
// on any error the previous entries keep serving and the refresh loop retries.
func (c *directoryCache) Load(ctx context.Context, store DirectoryStore) error {
	ctx, cancel := context.WithTimeout(ctx, cacheLoadTimeout)
	defer cancel()
	emps, err := store.ListEmployees(ctx)
	if err != nil {
		return fmt.Errorf("list employee directory: %w", err)
	}
	if c.Ready() && len(emps) == 0 {
		return fmt.Errorf("refresh employee directory: empty snapshot after a successful load")
	}
	n := c.replace(emps)
	slog.Info("directory cache loaded", "rows", len(emps), "entries", n)
	return nil
}

// replace builds a snapshot keyed by account and swaps it in atomically. A
// duplicate account is skipped with a warning rather than rejecting the whole
// snapshot, so one malformed HR row cannot stall the directory; the first
// occurrence wins (deterministic, independent of Mongo cursor order). Returns
// the number of accounts published.
func (c *directoryCache) replace(emps []employee) int {
	entries := make(map[string]employee, len(emps))
	for _, e := range emps {
		if _, dup := entries[e.Account]; dup {
			slog.Warn("duplicate account in directory, skipping", "account", e.Account)
			continue
		}
		entries[e.Account] = e
	}
	c.entries.Store(&entries)
	return len(entries)
}

// RefreshLoop populates the cache immediately, then refreshes it every
// refreshEvery. A failed attempt is retried after retryEvery instead, so a
// transient Mongo failure does not leave the portal stale until the next
// scheduled refresh. Returns when ctx is cancelled.
func (c *directoryCache) RefreshLoop(ctx context.Context, store DirectoryStore, refreshEvery, retryEvery time.Duration) {
	for {
		wait := refreshEvery
		if err := c.Load(ctx, store); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("refresh directory cache", "error", err)
			wait = retryEvery
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
