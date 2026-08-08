// Package badgecache provides a per-user Valkey SET of unread room IDs used to
// materialize the thread-unread badge: a best-effort, fail-open accelerator
// whose contents are always recoverable from MongoDB (the source of truth) via
// Reseed, so a cache miss, eviction, or Valkey error degrades the badge rather
// than corrupting it.
package badgecache

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// maxCount is the cap applied in Go to every count BumpBatch/Seed return — the
// badge UI never needs to distinguish "10 unread rooms" from "50 unread
// rooms", and capping keeps SCARD's cost bounded from the caller's view.
const maxCount = 10

// bumpScript adds a single room to an existing unread set and refreshes its
// TTL. It is a miss (no writes at all) when the key does not exist yet — the
// caller must Seed/Reseed first. KEYS[1]=set key. ARGV[1]=roomID,
// ARGV[2]=ttlSeconds. Returns -1 on miss, else SCARD after SADD+EXPIRE.
var bumpScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
return redis.call('SCARD', KEYS[1])
`)

// seedScript creates (or extends) an unread set from a batch of room IDs and
// refreshes its TTL. KEYS[1]=set key. ARGV[1]=ttlSeconds, ARGV[2..]=roomIDs
// (may be absent). Returns SCARD after SADD+EXPIRE.
var seedScript = redis.NewScript(`
if #ARGV > 1 then
  redis.call('SADD', KEYS[1], unpack(ARGV, 2))
end
redis.call('EXPIRE', KEYS[1], ARGV[1])
return redis.call('SCARD', KEYS[1])
`)

// reseedScript replaces an unread set wholesale: delete, then (if any room IDs
// were given) recreate from scratch and refresh the TTL. KEYS[1]=set key.
// ARGV[1]=ttlSeconds, ARGV[2..]=roomIDs (may be absent, in which case the key
// is simply left deleted).
var reseedScript = redis.NewScript(`
redis.call('DEL', KEYS[1])
if #ARGV > 1 then
  redis.call('SADD', KEYS[1], unpack(ARGV, 2))
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return 1
`)

// Cache is the Valkey-backed unread-room-id set, one SET per account.
type Cache struct {
	rdb redis.UniversalClient
	ttl time.Duration
}

// New builds a Cache. ttl bounds how long an account's unread-room set
// survives without a BumpBatch/Seed/Reseed refresh, so a crashed/missed clear
// self-heals rather than sticking forever.
func New(rdb redis.UniversalClient, ttl time.Duration) *Cache {
	return &Cache{rdb: rdb, ttl: ttl}
}

// Key returns the hash-tagged Valkey key for account's unread-room set. The
// {account} hash tag keeps every multi-key/scripted op for one account on a
// single cluster slot.
func Key(account string) string {
	return "badge:{" + account + "}"
}

// BumpBatch adds one triggering roomID to many accounts' unread sets (TTL
// refreshed) in a single pipeline — grouped ~1 round trip per cluster node
// instead of one per account. The result maps each account that HIT (key
// existed; SADD+EXPIRE applied) to its post-add set size capped at 10;
// accounts that missed (no key yet) or errored are simply absent — fail-open,
// so callers seed those from the source of truth. NOSCRIPT failures (empty
// script cache on a fresh node) are re-run as one pipelined EVAL pass, which
// also re-caches the script's SHA for subsequent batches.
func (c *Cache) BumpBatch(ctx context.Context, accounts []string, roomID string) map[string]int {
	counts := make(map[string]int, len(accounts))
	if len(accounts) == 0 {
		return counts
	}
	ttl := ttlSeconds(c.ttl)
	cmds := make([]*redis.Cmd, len(accounts))
	if _, err := c.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, account := range accounts {
			cmds[i] = bumpScript.EvalSha(ctx, pipe, []string{Key(account)}, roomID, ttl)
		}
		return nil
	}); err != nil {
		// Logged once for visibility; the per-command errors below decide each
		// account's outcome individually.
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
			continue // miss — the key does not exist yet
		}
		counts[account] = capCount(n)
	}
	if len(retry) == 0 {
		return counts
	}
	// Second pipelined pass with the full script source — bounded extra rounds
	// per node instead of one round trip per affected account.
	retryCmds := make([]*redis.Cmd, len(retry))
	if _, err := c.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for j, i := range retry {
			retryCmds[j] = bumpScript.Eval(ctx, pipe, []string{Key(accounts[i])}, roomID, ttl)
		}
		return nil
	}); err != nil {
		slog.WarnContext(ctx, "badgecache bump batch retry failed", "room_id", roomID, "accounts", len(retry), "error", err)
	}
	for j, i := range retry {
		if n, err := retryCmds[j].Int64(); err == nil && n >= 0 {
			counts[accounts[i]] = capCount(n)
		}
	}
	return counts
}

// Seed creates (or extends) account's unread set from roomIDs plus
// triggerRoomID (deduplicated; an empty triggerRoomID is skipped), refreshes
// the TTL, and returns the resulting size capped at 10. ok=false on any
// Valkey error (fail-open, after one warn log).
func (c *Cache) Seed(ctx context.Context, account string, roomIDs []string, triggerRoomID string) (count int, ok bool) {
	argv := seedArgs(c.ttl, roomIDs, triggerRoomID)
	n, err := seedScript.Run(ctx, c.rdb, []string{Key(account)}, argv...).Int64()
	if err != nil {
		slog.WarnContext(ctx, "badgecache seed failed", "account", account, "error", err)
		return 0, false
	}
	return capCount(n), true
}

// ClearRoom removes roomID from account's unread set. Fail-open: a missing
// key or any Valkey error is a silent no-op after one warn log.
func (c *Cache) ClearRoom(ctx context.Context, account, roomID string) {
	if err := c.rdb.SRem(ctx, Key(account), roomID).Err(); err != nil {
		slog.WarnContext(ctx, "badgecache clear room failed", "account", account, "room_id", roomID, "error", err)
	}
}

// ClearAll deletes account's entire unread set. Fail-open: a missing key or
// any Valkey error is a silent no-op after one warn log.
func (c *Cache) ClearAll(ctx context.Context, account string) {
	if err := c.rdb.Del(ctx, Key(account)).Err(); err != nil {
		slog.WarnContext(ctx, "badgecache clear all failed", "account", account, "error", err)
	}
}

// Reseed replaces account's unread set wholesale with roomIDs (delete, then
// recreate and refresh the TTL) — the reconciliation path driven by Mongo,
// the source of truth. An empty roomIDs just deletes the key. Fail-open: any
// Valkey error is a silent no-op after one warn log.
func (c *Cache) Reseed(ctx context.Context, account string, roomIDs []string) {
	argv := make([]interface{}, 0, len(roomIDs)+1)
	argv = append(argv, ttlSeconds(c.ttl))
	for _, id := range roomIDs {
		argv = append(argv, id)
	}
	if err := reseedScript.Run(ctx, c.rdb, []string{Key(account)}, argv...).Err(); err != nil {
		slog.WarnContext(ctx, "badgecache reseed failed", "account", account, "error", err)
	}
}

// seedArgs builds the seedScript ARGV: ttl seconds, then roomIDs deduplicated
// with triggerRoomID appended (triggerRoomID skipped when empty).
func seedArgs(ttl time.Duration, roomIDs []string, triggerRoomID string) []interface{} {
	seen := make(map[string]struct{}, len(roomIDs)+1)
	argv := make([]interface{}, 0, len(roomIDs)+2)
	argv = append(argv, ttlSeconds(ttl))
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

// capCount bounds n at maxCount, matching the badge UI's "10+" display cap.
func capCount(n int64) int {
	if n > maxCount {
		return maxCount
	}
	return int(n)
}
