package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// cacheLoadTimeout bounds a single full scan of the cards collection.
const cacheLoadTimeout = time.Minute

// refreshAtLayout parses TCARD_CACHE_REFRESH_AT: a daily wall-clock time with
// an explicit UTC offset, e.g. "08:00+08:00".
const refreshAtLayout = "15:04Z07:00"

// parseRefreshAt parses the daily refresh schedule. Only the clock time and
// zone offset of the returned value are meaningful.
func parseRefreshAt(raw string) (time.Time, error) {
	at, err := time.Parse(refreshAtLayout, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse daily refresh time (want %q, e.g. 08:00+08:00): %w", refreshAtLayout, err)
	}
	return at, nil
}

// nextDailyRefresh returns the first occurrence of at's wall-clock time (in
// at's zone) strictly after now.
func nextDailyRefresh(now, at time.Time) time.Time {
	local := now.In(at.Location())
	next := time.Date(local.Year(), local.Month(), local.Day(), at.Hour(), at.Minute(), 0, 0, at.Location())
	if !next.After(now) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

// cardKey identifies a cached template by (path, cardVersion). A struct key
// avoids the delimiter-collision risk a concatenated string key would carry.
type cardKey struct {
	path        string
	cardVersion string
}

// cardCache is the in-memory (path, cardVersion) → template snapshot behind an
// atomic.Pointer: reads are lock-free; writers (Load/Add) serialize on writeMu.
type cardCache struct {
	entries atomic.Pointer[map[cardKey]json.RawMessage]
	writeMu sync.Mutex
}

func newCardCache() *cardCache {
	return &cardCache{}
}

// Get returns the template document for (path, cardVersion).
func (c *cardCache) Get(path, cardVersion string) (json.RawMessage, bool) {
	m := c.entries.Load()
	if m == nil {
		return nil, false
	}
	tmpl, ok := (*m)[cardKey{path: path, cardVersion: cardVersion}]
	return tmpl, ok
}

// Add copy-on-writes one card into the snapshot so a freshly registered card is
// servable at once; serialized with Load via writeMu. No-op until the first Load.
func (c *cardCache) Add(cd card) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	m := c.entries.Load()
	if m == nil {
		return
	}
	next := make(map[cardKey]json.RawMessage, len(*m)+1)
	for k, v := range *m {
		next[k] = v
	}
	next[cardKey{path: cd.Path, cardVersion: cd.CardVersion}] = cd.Template
	c.entries.Store(&next)
}

// Ready reports whether at least one load has succeeded (the /readyz signal).
// An empty snapshot IS ready: zero cards is a legitimate "synced to Mongo" state.
func (c *cardCache) Ready() bool {
	return c.entries.Load() != nil
}

// Load reads the full cards collection and swaps it in (writeMu held across the
// read so a concurrent Add isn't clobbered by a stale scan). Returns the count.
func (c *cardCache) Load(ctx context.Context, store CardStore) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, cacheLoadTimeout)
	defer cancel()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	cards, err := store.ListCards(ctx)
	if err != nil {
		return 0, fmt.Errorf("list cards: %w", err)
	}
	n := c.replace(cards)
	slog.Info("card cache loaded", "rows", len(cards), "entries", n)
	return n, nil
}

// replace swaps in a fresh (path, cardVersion)-keyed snapshot, skipping a
// duplicate key with a warning (first wins). Returns the number published.
func (c *cardCache) replace(cards []card) int {
	entries := make(map[cardKey]json.RawMessage, len(cards))
	for _, card := range cards {
		key := cardKey{path: card.Path, cardVersion: card.CardVersion}
		if _, dup := entries[key]; dup {
			slog.Warn("duplicate (path, cardVersion) in cards collection, skipping",
				"path", card.Path, "cardVersion", card.CardVersion)
			continue
		}
		entries[key] = card.Template
	}
	c.entries.Store(&entries)
	return len(entries)
}

// RefreshLoop loads at startup then daily at at's wall-clock time, retrying
// after retryEvery on failure. Returns when ctx is cancelled.
func (c *cardCache) RefreshLoop(ctx context.Context, store CardStore, at time.Time, retryEvery time.Duration) {
	for {
		var wait time.Duration
		if _, err := c.Load(ctx, store); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("refresh card cache", "error", err)
			wait = retryEvery
		} else {
			wait = time.Until(nextDailyRefresh(time.Now(), at))
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

// listResult is the outcome of scanning the snapshot for one prefix's direct
// children (design spec §4: generic over depth, entries are full paths).
type listResult struct {
	cards     []string // "path@version", one per cached version, sorted
	folders   []string // full folder paths, deduped, sorted
	exactPath bool     // prefix names a cached card path exactly (no version)
	found     bool     // at least one card or folder matched under prefix
}

// List returns prefix's direct children ("" = root; pre-trimmed of slashes):
// one lock-free O(total entries) snapshot scan — fine for a bounded catalog.
func (c *cardCache) List(prefix string) listResult {
	res := listResult{cards: []string{}, folders: []string{}}
	m := c.entries.Load()
	if m == nil {
		return res
	}

	type cardEntry struct{ path, version string }
	var cards []cardEntry
	folderSet := make(map[string]struct{})
	needle := prefix + "/"
	for k := range *m {
		if k.path == prefix {
			res.exactPath = true
			continue
		}
		rest := k.path
		if prefix != "" {
			if !strings.HasPrefix(k.path, needle) {
				continue
			}
			rest = k.path[len(needle):]
		}
		if i := strings.Index(rest, "/"); i >= 0 {
			folder := rest[:i]
			if prefix != "" {
				folder = prefix + "/" + folder
			}
			folderSet[folder] = struct{}{}
		} else {
			cards = append(cards, cardEntry{path: k.path, version: k.cardVersion})
		}
	}

	// Sort by path, then semver; non-semver versions (out-of-band writes only)
	// sort after semver ones, lexicographic among themselves — a total order.
	sort.Slice(cards, func(i, j int) bool {
		if cards[i].path != cards[j].path {
			return cards[i].path < cards[j].path
		}
		vi, oki := parseSemver(cards[i].version)
		vj, okj := parseSemver(cards[j].version)
		if oki && okj {
			return vj.greater(vi)
		}
		if oki != okj {
			return oki
		}
		return cards[i].version < cards[j].version
	})
	res.cards = make([]string, 0, len(cards))
	for _, e := range cards {
		res.cards = append(res.cards, e.path+"@"+e.version)
	}
	for f := range folderSet {
		res.folders = append(res.folders, f)
	}
	sort.Strings(res.folders)
	res.found = len(res.cards) > 0 || len(res.folders) > 0
	return res
}
