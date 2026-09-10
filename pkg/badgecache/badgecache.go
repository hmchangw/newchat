// Package badgecache is a per-user Valkey SET of unread room IDs backing the
// badge count, plus a freshness marker (MarkerKey): marker present means the
// set is accurate and a missing/empty set means zero unread. Fail-open — the
// contents are always recoverable from Mongo via Reseed.
package badgecache

import (
	"context"
	"hash/fnv"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hmchangw/chat/pkg/cachekeys"
)

// DefaultMaxCount caps every count BumpBatch/Seed return (the UI renders the
// cap as "9+"). Override via New's maxCount (BADGE_COUNT_CAP).
const DefaultMaxCount = 10

// jitterPercent is the maximum fraction of the marker TTL shaved off by
// markerTTLSeconds (via a per-account, deterministic offset), so accounts
// stamped in the same instant expire spread across a window instead of all
// at once.
const jitterPercent = 20

// bumpScript SADDs one room and refreshes the SET's TTL only. The marker's TTL
// is deliberately untouched: a bump is not a verification against Mongo, so it
// must not extend the staleness countdown. Miss (-1, no writes) when the marker
// is absent — a surviving set without a marker is unverified and the caller must
// recompute. KEYS=[set, marker], ARGV=[roomID, setTtlSec]; returns SCARD on hit.
var bumpScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 0 then
  return -1
end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
return redis.call('SCARD', KEYS[1])
`)

// seedScript replaces the set from Mongo-derived IDs and stamps the marker.
// Replace (not union): seed runs on a miss, where any surviving set is
// unverified and must not contaminate a freshly computed answer.
// KEYS=[set, marker], ARGV=[setTtlSec, markerTtlSec, roomIDs...]; returns SCARD.
var seedScript = redis.NewScript(`
redis.call('DEL', KEYS[1])
if #ARGV > 2 then
  redis.call('SADD', KEYS[1], unpack(ARGV, 3))
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
return redis.call('SCARD', KEYS[1])
`)

// reseedScript replaces the set wholesale (DEL, then SADD if any IDs) and
// stamps the marker either way — an empty reseed records "fresh, zero unread".
// KEYS=[set, marker], ARGV=[setTtlSec, markerTtlSec, roomIDs...].
var reseedScript = redis.NewScript(`
redis.call('DEL', KEYS[1])
if #ARGV > 2 then
  redis.call('SADD', KEYS[1], unpack(ARGV, 3))
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
return 1
`)

// Cache is the Valkey-backed unread-room-id set, one SET per account.
type Cache struct {
	rdb       redis.UniversalClient
	ttl       time.Duration
	markerTTL time.Duration
	maxCount  int
}

// Option customizes a Cache at construction.
type Option func(*Cache)

// WithMarkerTTL gives the freshness marker its own lifetime, shorter than the
// set's. Because only Seed/Reseed (which compute from Mongo) stamp the marker,
// its TTL is the maximum time the set can go unverified — i.e. the badge's
// maximum staleness. Non-positive, or longer than the set TTL, falls back to
// the set TTL: a marker outliving its set would read as a fresh zero. The
// value given here is an upper bound, not the exact stamped TTL: each stamp
// applies a downward-only, per-account offset (see markerTTLSeconds) so
// accounts seeded in the same burst don't all expire — and recompute from
// Mongo — together. The offset is derived deterministically from the account,
// not drawn randomly, so a given account's expiry stays stable across reseeds.
func WithMarkerTTL(d time.Duration) Option {
	return func(c *Cache) {
		if d <= 0 || d > c.ttl {
			slog.Warn("badgecache marker TTL out of range, using set TTL", "marker_ttl", d, "set_ttl", c.ttl)
			return
		}
		c.markerTTL = d
	}
}

// New builds a Cache. ttl bounds how long a set survives without a refresh
// (missed clears self-heal); maxCount caps returned counts, non-positive
// falls back to DefaultMaxCount so a misconfig can't zero every badge.
// Without WithMarkerTTL the marker shares ttl.
func New(rdb redis.UniversalClient, ttl time.Duration, maxCount int, opts ...Option) *Cache {
	if maxCount <= 0 {
		maxCount = DefaultMaxCount
	}
	c := &Cache{rdb: rdb, ttl: ttl, markerTTL: ttl, maxCount: maxCount}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Key returns the hash-tagged Valkey key for account's unread-room set. The
// {account} hash tag keeps every multi-key/scripted op for one account on a
// single cluster slot.
func Key(account string) string {
	return cachekeys.BadgeSet(account)
}

// MarkerKey returns the freshness-marker key: marker present ⇒ the set is
// accurate (missing/empty set = zero unread). Same {account} hash tag as Key
// so scripts and multi-key DELs stay on one slot.
func MarkerKey(account string) string {
	return cachekeys.BadgeFresh(account)
}

// scriptKeys is the KEYS array every badge script takes: the set and its
// freshness marker, always addressed together.
func scriptKeys(account string) []string {
	return []string{Key(account), MarkerKey(account)}
}

// BumpBatch adds roomID to many accounts' unread sets in one pipeline. Hit is
// determined solely by marker presence — a surviving set without a marker is
// unverified and counts as a miss. The result maps each hit account to its
// capped post-add size; misses and errors are absent (callers seed those from
// Mongo). NOSCRIPT commands are re-run as a second pipelined EVAL pass.
func (c *Cache) BumpBatch(ctx context.Context, accounts []string, roomID string) map[string]int {
	counts := make(map[string]int, len(accounts))
	if len(accounts) == 0 {
		return counts
	}
	ttl := ttlSeconds(c.ttl)
	cmds := make([]*redis.Cmd, len(accounts))
	if _, err := c.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, account := range accounts {
			cmds[i] = bumpScript.EvalSha(ctx, pipe, scriptKeys(account), roomID, ttl)
		}
		return nil
	}); err != nil {
		// Logged once; per-command errors below decide each account's outcome.
		slog.WarnContext(ctx, "badgecache bump batch failed", "room_id", roomID, "accounts", len(accounts), "error", err)
	}
	var retry []int // indexes whose EVALSHA hit NOSCRIPT
	for i, account := range accounts {
		n, err := cmds[i].Int64()
		if err != nil {
			if redis.HasErrorPrefix(err, "NOSCRIPT") {
				retry = append(retry, i)
			}
			continue
		}
		if n < 0 {
			continue // miss — marker absent, set (if any) is unverified
		}
		counts[account] = c.capCount(n)
	}
	if len(retry) == 0 {
		return counts
	}
	// Second pipelined pass with the full script source.
	retryCmds := make([]*redis.Cmd, len(retry))
	if _, err := c.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for j, i := range retry {
			retryCmds[j] = bumpScript.Eval(ctx, pipe, scriptKeys(accounts[i]), roomID, ttl)
		}
		return nil
	}); err != nil {
		slog.WarnContext(ctx, "badgecache bump batch retry failed", "room_id", roomID, "accounts", len(retry), "error", err)
	}
	for j, i := range retry {
		if n, err := retryCmds[j].Int64(); err == nil && n >= 0 {
			counts[accounts[i]] = c.capCount(n)
		}
	}
	return counts
}

// Seed replaces account's set with roomIDs plus triggerRoomID (deduplicated,
// empty skipped) and returns the capped size. ok=false on any Valkey error
// (fail-open).
func (c *Cache) Seed(ctx context.Context, account string, roomIDs []string, triggerRoomID string) (count int, ok bool) {
	argv := seedArgs(ttlSeconds(c.ttl), c.markerTTLSeconds(account), roomIDs, triggerRoomID)
	n, err := seedScript.Run(ctx, c.rdb, scriptKeys(account), argv...).Int64()
	if err != nil {
		slog.WarnContext(ctx, "badgecache seed failed", "account", account, "error", err)
		return 0, false
	}
	return c.capCount(n), true
}

// ClearRoom removes roomID from account's set. Exact removal — the freshness
// marker survives. Fail-open on Valkey errors.
func (c *Cache) ClearRoom(ctx context.Context, account, roomID string) {
	if err := c.rdb.SRem(ctx, Key(account), roomID).Err(); err != nil {
		slog.WarnContext(ctx, "badgecache clear room failed", "account", account, "room_id", roomID, "error", err)
	}
}

// ClearAll deletes account's set and freshness marker (same slot, one DEL) —
// wholesale invalidation for reads/unmutes; the next count recomputes from
// Mongo. Fail-open on Valkey errors.
func (c *Cache) ClearAll(ctx context.Context, account string) {
	if err := c.rdb.Del(ctx, Key(account), MarkerKey(account)).Err(); err != nil {
		slog.WarnContext(ctx, "badgecache clear all failed", "account", account, "error", err)
	}
}

// Count returns the cached unread-room count, uncapped. fresh=false (marker
// absent or Valkey error) means the caller must recompute from Mongo;
// fresh=true with n=0 is the legitimate all-read state.
func (c *Cache) Count(ctx context.Context, account string) (n int, fresh bool) {
	var existsCmd, scardCmd *redis.IntCmd
	if _, err := c.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		existsCmd = pipe.Exists(ctx, MarkerKey(account))
		scardCmd = pipe.SCard(ctx, Key(account))
		return nil
	}); err != nil {
		slog.WarnContext(ctx, "badgecache count failed", "account", account, "error", err)
		return 0, false
	}
	if existsCmd.Val() == 0 {
		return 0, false
	}
	return int(scardCmd.Val()), true
}

// Reseed replaces account's set wholesale with roomIDs and stamps the marker
// — the Mongo-driven reconciliation path. Empty roomIDs records fresh zero
// unread. Fail-open on Valkey errors.
func (c *Cache) Reseed(ctx context.Context, account string, roomIDs []string) {
	argv := make([]interface{}, 0, len(roomIDs)+2)
	argv = append(argv, ttlSeconds(c.ttl), c.markerTTLSeconds(account))
	for _, id := range roomIDs {
		argv = append(argv, id)
	}
	if err := reseedScript.Run(ctx, c.rdb, scriptKeys(account), argv...).Err(); err != nil {
		slog.WarnContext(ctx, "badgecache reseed failed", "account", account, "error", err)
	}
}

// seedArgs builds the seedScript ARGV: set ttl seconds, marker ttl seconds,
// then roomIDs deduplicated with triggerRoomID appended (skipped when empty).
func seedArgs(ttlSec, markerTTLSec int64, roomIDs []string, triggerRoomID string) []interface{} {
	seen := make(map[string]struct{}, len(roomIDs)+1)
	argv := make([]interface{}, 0, len(roomIDs)+3)
	argv = append(argv, ttlSec, markerTTLSec)
	add := func(id string) {
		if id == "" {
			return
		}
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		argv = append(argv, id)
	}
	for _, id := range roomIDs {
		add(id)
	}
	add(triggerRoomID)
	return argv
}

// ttlSeconds converts ttl to whole seconds for Lua's EXPIRE.
func ttlSeconds(ttl time.Duration) int64 {
	return int64(ttl / time.Second)
}

// markerTTLSeconds returns account's marker TTL in whole seconds, with a
// per-account offset of up to jitterPercent shaved off. Accounts stamped in
// the same instant therefore expire spread across a window instead of all at
// once, which keeps the Mongo recompute they trigger from arriving as one
// stampede. The offset is derived from the account rather than drawn randomly:
// the requirement is spread across accounts, not unpredictability, and a
// stable offset also keeps an account's expiry from moving between reseeds.
// Downward-only, so the result never exceeds markerTTL and the
// markerTTL <= ttl invariant holds without re-checking it. Never returns 0 —
// a zero EX aborts the script mid-execution, leaving the set rewritten but
// unmarked and the cache permanently missing. That floor is enforced here
// rather than left to config validation: a sub-second markerTTL truncates to
// zero seconds, and this package is shared, so it cannot assume every caller
// validates the way user-service does.
func (c *Cache) markerTTLSeconds(account string) int64 {
	sec := ttlSeconds(c.markerTTL)
	if sec <= 1 {
		return 1
	}
	span := sec*jitterPercent/100 + 1
	h := fnv.New32a()
	_, _ = h.Write([]byte(account)) // hash.Hash.Write never returns an error
	if out := sec - int64(h.Sum32())%span; out >= 1 {
		return out
	}
	return 1
}

// capCount bounds n at the configured cap, matching the badge UI's "9+" display.
func (c *Cache) capCount(n int64) int {
	if n > int64(c.maxCount) {
		return c.maxCount
	}
	return int(n)
}
