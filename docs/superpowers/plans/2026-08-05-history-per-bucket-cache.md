# History Per-Bucket Message Cache — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cache history-service's Cassandra message reads **per sealed bucket** inside the `cassrepo` walker, so the multi-bucket walk that dominates sparse-room reads is computed once per `(room, bucket)` and reused across every read, user, and page size. The cache boundary is the mutability boundary: the **current** bucket is always read live, every **sealed** bucket (strictly older than `sizer.Of(now)`) is cached, so ordinary message flow (new messages land only in the current bucket) invalidates nothing cached.

This is the "per-bucket" architecture from the appendix of `2026-08-04-history-load-message-cache.md`; read that appendix first for the rationale and the comparison against the page-cache decorator. **This plan replaces the Phase 1 page-cache decorator** (`history-service/internal/msgpagecache`) for the two DESC read methods — per-bucket subsumes it (it even caches the sealed portion of a *hot-anchored* read, which the decorator skipped) while keeping the tip live, so Task 7 retires the decorator.

## Why per-bucket helps more reads than the decorator

A single walk mixes buckets. `GetMessagesBefore(before=now)` starts in the hot current bucket (live) and then advances into ~N sealed buckets. The decorator classified the whole read "hot" (anchor in current bucket) and cached nothing. Per-bucket caches every sealed bucket the walk crosses regardless of where the anchor sits, so:

- **"Latest 20 across 6 months"** (anchor recent): 1 live query for the current bucket + ~59 sealed-bucket cache hits, instead of ~60 live queries — and it stays that way after each new message (the sealed buckets are untouched).
- **Any scroll-back / different page size / different `before`**: reuses the same cached buckets it walks; only genuinely-new older buckets are queried.

## Scope

- **Cached:** the two DESC (`LoadHistory`) reads — `GetMessagesBefore`, `GetMessagesBetweenDesc`.
- **Not cached (stay live):** the ASC/forward reads `GetMessagesAfter`, `GetAllMessagesAsc` (`LoadNextMessages`), because they round-trip a `Cursor` whose intra-bucket resume needs an offset scheme for cached buckets — deferred. Also uncached: `GetMessageByID`, `GetMessagesByIDs`, pinned reads, thread reads.

**Why the DESC methods need no cursor change:** `LoadHistory` is timestamp-anchored and **discards** `fillPage`'s `NextCursor`/`HasNext` (`LoadHistoryResponse` has neither field); the client continues by re-sending `before = oldest returned message's createdAt`. A page that fills mid-bucket is continued by the next timestamp-anchored call re-walking from `sizer.Of(before)` with `created_at < before`. So a cached-bucket walk step that returns only `remaining` of a fuller bucket is correct for `LoadHistory` even though the cursor `fillPage` emits is a boundary cursor — the cursor is never read. **This is why the cached fetcher is wired ONLY to the DESC methods** (Task 4): the ASC methods, whose cursor IS consumed, keep the live fetcher untouched.

## Design decisions (locked)

- **Seam:** inside `cassrepo`. `Repository` gains an optional `*bucketcache.Cache`; the two DESC methods route sealed buckets through it. Absent cache ⇒ identical behavior to today.
- **Cache unit / key:** `hist:{roomID}:bkt:{bucketStartMs}` (hash-tagged on `{roomID}`). Value: **gob**-encoded `[]models.Message` for the whole `(room, bucket)` partition in clustering order (`created_at DESC, message_id DESC`), stored in the **same sealed form Cassandra holds** — `enc_payload`/`enc_meta` intact, not decrypted. Caching the decrypted rows would put a plaintext copy of message bodies in Valkey, outside the at-rest protection `enc_payload` exists to provide; this way the cache inherits the at-rest posture of the table it mirrors. Readers decrypt only the rows they serve, after the bounds and limit have been applied — those read `created_at`, which stays plaintext as a clustering column, so a 20-row page out of a 2000-row bucket pays for 20 decrypts. The Cassandra round-trip and the bucket walk are still saved; only the per-row decrypt is not. gob (not JSON) for the same reason as the decorator — `Reactions` is a struct-keyed, marshal-only map. Decode-per-hit gives copy-safety against the service layer's in-place redaction.
- **Tiers:** L1 (`expirable.LRU[string, []byte]`, `cachemetrics.For("history_bucket","l1")`) + L2 (Valkey, `"l2"`). Fail-open throughout.
- **Sealed test:** `bucket < sizer.Of(r.now())` (a `now` field on `Repository`, default `time.Now().UTC`, overridable in tests).
- **Size guard:** the full-bucket load runs `… LIMIT maxRows+1`. If it returns `> maxRows`, the bucket is **oversized** → don't cache, fall back to the normal bounded live query for that walk step. Dense buckets (already a cheap 1-query full page) are thus declined; value concentrates on sparse buckets, which are tiny.
- **Invalidation:** **no generation counter** — one key per bucket, so a plain `DEL`. New messages need no bust (current bucket never cached). Edit/delete/react/pin bust exactly `sizer.Of(msg.CreatedAt)`; a delete that recomputes the thread parent's tcount also busts `sizer.Of(*msg.ThreadParentCreatedAt)`. The bumping instance also drops its own L1 entry immediately; other replicas reconcile within L1 TTL (same TTL-bounded cross-replica window as `pkg/roommetacache`).
- **Cross-site/federated edits** (applied by `message-worker`, not history-service): backstopped by the L2 TTL; optional canonical-event consumer as later hardening.

**Tech Stack:** Go 1.25, `pkg/valkeyutil`, `github.com/hashicorp/golang-lru/v2/expirable`, `golang.org/x/sync/singleflight`, `pkg/cachemetrics`, `pkg/msgbucket`, `encoding/gob`, `gocql`, `pkg/testutil` (Cassandra + Valkey), testify.

**Docs note — no `docs/client-api.md` change:** internal caching; no client-facing schema/error/event change.

---

## File Structure

**New files:**
- `history-service/internal/bucketcache/bucketcache.go` — `Cache` (`Get`/`Put`/`Bust`), key builder, gob codec, L1+L2, fail-open.
- `history-service/internal/bucketcache/bucketcache_test.go` — unit tests (fake `valkeyutil.Client`).
- `history-service/internal/bucketcache/integration_test.go` — real-Valkey tests + `TestMain`.
- `history-service/internal/cassrepo/sealedbucket.go` — full-bucket loader (`loadSealedBucket`) + in-memory bounds helpers.
- `history-service/internal/cassrepo/sealedbucket_test.go` — unit tests for the bounds helpers.

**Modified files:**
- `history-service/internal/cassrepo/walker.go` — refactor `fillPage` to take a `bucketFetcher` in place of `queryFn`+`scan`.
- `history-service/internal/cassrepo/messages_by_room.go` — adapt the four methods to build live fetchers; wrap the two DESC fetchers with the cache path.
- `history-service/internal/cassrepo/repository.go` — add `bucketCache`, `now` fields + `Option`s (`WithBucketCache`, `withClock` for tests).
- `history-service/internal/cassrepo/write.go`, `pin.go`, `reactions.go` — bust calls after each successful mutation (6 sites, +1 parent-bucket bust in delete).
- `history-service/internal/config/config.go` — `HISTORY_BUCKET_CACHE_*` knobs (+ reuse `VALKEY_ADDRS`/`VALKEY_PASSWORD` from the decorator config).
- `history-service/cmd/main.go` — build the `bucketcache.Cache`, pass it to `NewRepository`; **remove** the `msgpagecache` decorator wiring.
- `history-service/deploy/docker-compose.yml` — Valkey env for local dev (already added by the decorator plan; keep).

**Retired (Task 7):** `history-service/internal/msgpagecache/` and its config/wiring.

**No-cycle check:** `bucketcache` imports `cassrepo`'s `models`, `valkeyutil`, `msgbucket`, `cachemetrics`; `cassrepo` imports `bucketcache`. `bucketcache` must NOT import `cassrepo` (only `models`, which is a leaf) — the `[]models.Message` value type lives in `history-service/internal/models`, so `bucketcache` depends on `models`, not on `cassrepo`. `cassrepo` → `bucketcache` is the only direction.

---

## Task 1: bucketcache package (L1+L2, unit-tested)

**Files:** create `bucketcache.go` + `bucketcache_test.go`.

- [ ] **Step 1: failing unit tests.** Mirror the decorator's `fakeValkey` (in-memory map + per-op error hooks + call log). Cover:
  - `Key(roomID, bucket)` = `"hist:{"+roomID+"}:bkt:"+bucket`.
  - miss → `Get` returns `(nil,false)`; `Put` then `Get` returns a **distinct** slice from one a prior caller mutated (copy-safety).
  - L2 hit populates L1 (two `Cache` instances sharing one fake Valkey: instance-2 `Get` serves without the value ever being `Put` on instance-2).
  - `Bust` DELs the L2 key and removes the local L1 entry; a subsequent `Get` misses.
  - fail-open: `Get`/`Put`/`Bust` swallow Valkey errors (return miss / no error / no panic); nil client ⇒ `Get` miss, `Put`/`Bust` no-op.
  - `NewCache` rejects non-positive size/ttl.

- [ ] **Step 2:** `make test SERVICE=history-service/internal/bucketcache` → FAIL (undefined).

- [ ] **Step 3: implement.** Sketch:

```go
package bucketcache

// Cache is an L1(LRU+TTL) + L2(Valkey) store of whole sealed message buckets,
// keyed by (roomID, bucket). Values are gob-encoded []models.Message; Get
// decodes a fresh slice per call (copy-safe). Every Valkey op is fail-open.
type Cache struct {
	valkey valkeyutil.Client
	l1     *lru.LRU[string, []byte]
	ttl    time.Duration
	l1Rec  cachemetrics.Recorder
	l2Rec  cachemetrics.Recorder
}

func NewCache(valkey valkeyutil.Client, l1Size int, ttl time.Duration) (*Cache, error) { /* validate */ }

func Key(roomID string, bucket int64) string {
	return "hist:{" + roomID + "}:bkt:" + strconv.FormatInt(bucket, 10)
}

// Get returns the cached bucket (freshly decoded) and true on hit; (nil,false)
// on miss or any fail-open degradation.
func (c *Cache) Get(ctx context.Context, roomID string, bucket int64) ([]models.Message, bool) { /* L1→L2, decode fresh */ }

// Put caches msgs (the complete bucket) under (roomID, bucket). Best-effort.
func (c *Cache) Put(ctx context.Context, roomID string, bucket int64, msgs []models.Message) { /* encode; l1.Add; l2 SET ttl */ }

// Bust removes a bucket from L2 (DEL) and this instance's L1. Best-effort;
// other replicas' L1 reconcile within ttl.
func (c *Cache) Bust(ctx context.Context, roomID string, bucket int64) { /* nil-safe; l2 Del; l1.Remove */ }
```
Encode/decode: `gob` of `[]models.Message` (same pattern as `msgpagecache.encodePage`). Reuse the fail-open logging shape from `msgpagecache`.

- [ ] **Step 4:** tests green. **Step 5:** commit.

---

## Task 2: full-bucket loader + in-memory bounds (cassrepo)

**Files:** create `sealedbucket.go` + `sealedbucket_test.go`.

- [ ] **Step 1: failing unit tests** for the pure bounds helper `sliceBounded(rows []models.Message, before, since *time.Time, limit int) []models.Message` (no Cassandra needed):
  - rows are `created_at DESC`; `before` (exclusive upper) drops the leading rows with `created_at >= *before`; `since` (exclusive lower) drops the trailing rows with `created_at <= *since`; `limit` caps the result; nil bound = unbounded on that side; equal-timestamp boundary rows are excluded (strict `<`/`>`, matching the CQL `created_at < ?` / `> ?`).

- [ ] **Step 2:** run → FAIL.

- [ ] **Step 3: implement.**

```go
// loadSealedBucket reads the COMPLETE (room, bucket) partition in clustering
// order for population into bucketcache. Rows come back UNDECRYPTED so the
// cache stores Cassandra's sealed form and never holds plaintext bodies; the
// read path decrypts what it serves. It bounds the read with LIMIT maxRows+1:
// a result of exactly maxRows+1 means the bucket is oversized — return
// (nil, true, nil) so the caller declines to cache it and falls back to a
// bounded live query for that walk step.
func (r *Repository) loadSealedBucket(ctx context.Context, roomID string, bucket int64, maxRows int) (msgs []models.Message, oversized bool, err error) {
	q := r.session.Query(
		messageByRoomQuery+` WHERE room_id = ? AND bucket = ? ORDER BY created_at DESC LIMIT ?`,
		roomID, bucket, maxRows+1,
	).WithContext(ctx)
	iter := q.Iter()
	rows, scanErr := scanRawMessagesUpTo(iter, maxRows+1) // scan WITHOUT decrypting — cache stores the sealed form
	if err := iter.Close(); err != nil { return nil, false, fmt.Errorf("load sealed bucket %d: %w", bucket, err) }
	if scanErr != nil { return nil, false, fmt.Errorf("load sealed bucket %d: %w", bucket, scanErr) }
	if len(rows) > maxRows { return nil, true, nil } // oversized → don't cache
	return rows, false, nil
}

// sliceBounded applies the DESC in-memory equivalents of `created_at < before`
// (first bucket) and `created_at > since` (floor bucket), then caps at limit.
func sliceBounded(rows []models.Message, before, since *time.Time, limit int) []models.Message { /* binary-search bounds on the DESC slice */ }
```

- [ ] **Step 4:** tests green. **Step 5:** `make lint`. **Step 6:** commit.

> Integration coverage for `loadSealedBucket` against real Cassandra (including the oversized path) lands in Task 7.

---

## Task 3: refactor `fillPage` to a `bucketFetcher` (behavior-preserving)

**Files:** modify `walker.go`, `messages_by_room.go`. **This task changes no behavior** — it restructures the per-bucket step so Task 4 can swap in the cache. Existing walker/read tests must stay green unchanged.

- [ ] **Step 1: introduce the fetcher type** in `walker.go`:

```go
// bucketFetcher returns up to `remaining` rows for one bucket, honoring the
// per-call bounds, plus the gocql page state to resume this bucket (nil when
// the bucket is exhausted or was served whole). firstBucket lets the fetcher
// apply the upper-bound predicate only at the top of the walk; initialPageState
// is the resume state for the first bucket (always nil on the LoadHistory path).
type bucketFetcher func(ctx context.Context, bucket int64, firstBucket bool, remaining int, initialPageState []byte) (rows []models.Message, nextPageState []byte, err error)
```

- [ ] **Step 2: rewrite `fillPage`** to call `fetch(...)` where it currently builds the query + iterates + scans, keeping the accumulation / floor / maxBuckets / cursor-emission logic **identical**. It drops the `queryFn`/`scan` params in favor of one `fetch bucketFetcher`. The generic `[T any]` collapses to `models.Message` (the only instantiation), simplifying the signature.

- [ ] **Step 3: give each read method a live fetcher.** Extract the existing per-method `queryFn` switch + `scanMessagesUpTo` into a `liveFetcher(...)` closure that runs the query with `PageSize(remaining)` + optional `PageState`, scans up to `remaining`, and returns `iter.PageState()`. The four methods now call `fillPage(..., liveFetcher(...))`. Behavior is byte-for-byte the same.

- [ ] **Step 4:** `make test SERVICE=history-service` — **all existing cassrepo unit tests pass unchanged**. If any fail, the refactor diverged; fix before continuing.
- [ ] **Step 5:** `make test-integration SERVICE=history-service` (cassrepo) still green. **Step 6:** `make lint`. **Step 7:** commit.

---

## Task 4: route sealed DESC buckets through the cache

**Files:** modify `messages_by_room.go` (the two DESC methods only), `repository.go`.

- [ ] **Step 1:** add fields + option to `Repository` (`repository.go`):

```go
type Repository struct {
	session    *gocql.Session
	bucket     msgbucket.Sizer
	maxBuckets int
	cipher     atrest.Cipher
	bucketCache *bucketcache.Cache // nil disables per-bucket caching
	maxCacheRows int               // size guard; 0 when cache disabled
	now         func() time.Time   // overridable clock for the sealed test
}

type Option func(*Repository)
func WithBucketCache(c *bucketcache.Cache, maxRows int) Option { return func(r *Repository){ r.bucketCache = c; r.maxCacheRows = maxRows } }
func withClock(f func() time.Time) Option { return func(r *Repository){ r.now = f } } // test-only

func NewRepository(session *gocql.Session, bucket msgbucket.Sizer, maxBuckets int, cipher atrest.Cipher, opts ...Option) *Repository {
	r := &Repository{session: session, bucket: bucket, maxBuckets: maxBuckets, cipher: cipher, now: func() time.Time { return time.Now().UTC() }}
	for _, o := range opts { o(r) }
	return r
}
```

- [ ] **Step 2: a caching fetcher** wrapping the live one, used only by the two DESC methods:

```go
// cachedDescFetcher serves sealed buckets from bucketcache (whole bucket +
// in-memory bounds), current/hot buckets and oversized buckets from the live
// fetcher. before/since capture the walk's row predicates; since is nil for
// GetMessagesBefore.
func (r *Repository) cachedDescFetcher(roomID string, before time.Time, since *time.Time, floorBucket int64, live bucketFetcher) bucketFetcher {
	currentBucket := r.bucket.Of(r.now())
	return func(ctx context.Context, bucket int64, firstBucket bool, remaining int, ips []byte) ([]models.Message, []byte, error) {
		if r.bucketCache == nil || bucket >= currentBucket {
			return live(ctx, bucket, firstBucket, remaining, ips) // hot/disabled → live
		}
		full, ok := r.bucketCache.Get(ctx, roomID, bucket)
		if !ok {
			loaded, oversized, err := r.loadSealedBucket(ctx, roomID, bucket, r.maxCacheRows)
			if err != nil { return nil, nil, err }
			if oversized { return live(ctx, bucket, firstBucket, remaining, ips) } // decline to cache
			r.bucketCache.Put(ctx, roomID, bucket, loaded)
			full = loaded
		}
		// In-memory bounds: upper (before) only on the first bucket; lower (since)
		// only on the floor bucket — mirrors the live queryFn switch.
		var upper *time.Time
		if firstBucket { upper = &before }
		var lower *time.Time
		if since != nil && bucket == floorBucket { lower = since }
		rows := sliceBounded(full, upper, lower, remaining)
		return rows, nil, nil // nil pageState: LoadHistory re-anchors by timestamp
	}
}
```

- [ ] **Step 3: wire it into the two DESC methods.** In `GetMessagesBefore`, replace the `fillPage(..., liveFetcher)` call with `fillPage(..., r.cachedDescFetcher(roomID, before, nil, floorBucket, liveFetcher))`. In `GetMessagesBetweenDesc`, pass `&since` as the lower bound. `GetMessagesAfter` / `GetAllMessagesAsc` keep the plain `liveFetcher` (unchanged).

- [ ] **Step 4: unit tests** (`messages_by_room_test.go`, no Cassandra) with a fake `bucketcache.Cache` behind an interface OR a real `*bucketcache.Cache` over a fake Valkey, plus a stub gocql layer is impractical — so cover the *routing/bounds* logic at the `cachedDescFetcher` seam by testing `sliceBounded` (Task 2) and the sealed/hot branch selection via `withClock`. Full read-path behavior is covered by Task 7 integration tests.
- [ ] **Step 5:** `make test SERVICE=history-service && make lint`. **Step 6:** commit.

---

## Task 5: write-site bucket invalidation

**Files:** modify `write.go` (`:234`, `:290`), `pin.go` (`:65`, `:85`), `reactions.go` (`:30`, `:54`).

- [ ] **Step 1: helper** on `Repository`:

```go
func (r *Repository) bustBucket(ctx context.Context, roomID string, createdAt time.Time) {
	if r.bucketCache == nil { return }
	r.bucketCache.Bust(ctx, roomID, r.bucket.Of(createdAt))
}
```

- [ ] **Step 2: failing tests** — inject a spy cache (record `(roomID, bucket)` busts) via `WithBucketCache`; assert each mutation busts `r.bucket.Of(msg.CreatedAt)` on success and **not** on the failed/`applied=false` path; a read does not bust.

- [ ] **Step 3: add the calls** after each successful mutation:
  - `UpdateMessageContent` (write.go:234): after the edit batch succeeds → `r.bustBucket(ctx, msg.RoomID, msg.CreatedAt)`.
  - `SoftDeleteMessage` (write.go:290): only when the LWT `applied`; bust `msg.CreatedAt`, **and** when the parent tcount was recomputed (`newTcount != nil` and `msg.ThreadParentCreatedAt != nil`) also `r.bustBucket(ctx, msg.RoomID, *msg.ThreadParentCreatedAt)` (the parent row lives in the same room, a different bucket).
  - `PinMessage`/`UnpinMessage` (pin.go), `AddReaction`/`RemoveReaction` (reactions.go): bust `msg.CreatedAt`.

- [ ] **Step 4:** `make test SERVICE=history-service && make lint`. **Step 5:** commit.

---

## Task 6: config + main wiring; retire the decorator

**Files:** modify `config.go`, `cmd/main.go`; remove `msgpagecache` wiring.

- [ ] **Step 1: config** — add (reuse `VALKEY_ADDRS`/`VALKEY_PASSWORD` already added for the decorator):

```go
	BucketCacheL1Size int           `env:"HISTORY_BUCKET_CACHE_L1_SIZE" envDefault:"20000"`
	BucketCacheTTL    time.Duration `env:"HISTORY_BUCKET_CACHE_TTL"      envDefault:"10m"`
	BucketCacheMaxRows int          `env:"HISTORY_BUCKET_CACHE_MAX_ROWS" envDefault:"2000"`
```
Extend `validate` to reject negatives / non-positive `MaxRows` when the cache is enabled.

- [ ] **Step 2: wire in `main.go`** — after the Valkey connect (from the decorator plan), build the cache and pass it to `NewRepository`; **delete** the `msgpagecache` decorator block and the `WithPageCacheBuster` wiring:

```go
	var bucketCacheOpt []cassrepo.Option
	if msgValkey != nil && cfg.BucketCacheL1Size > 0 && cfg.BucketCacheTTL > 0 {
		bc, err := bucketcache.NewCache(msgValkey, cfg.BucketCacheL1Size, cfg.BucketCacheTTL)
		if err != nil { slog.Error("init bucket cache failed", "error", err); os.Exit(1) }
		bucketCacheOpt = append(bucketCacheOpt, cassrepo.WithBucketCache(bc, cfg.BucketCacheMaxRows))
		slog.Info("per-bucket cache enabled", "l1Size", cfg.BucketCacheL1Size, "ttl", cfg.BucketCacheTTL, "maxRows", cfg.BucketCacheMaxRows)
	}
	cassRepo := cassrepo.NewRepository(cassSession, bucketSizer, cfg.MessageReadMaxBuckets, cipher, bucketCacheOpt...)
	// msgReader is now the raw cassRepo (no decorator); msgWriter unchanged.
```
Remove the `service.WithPageCacheBuster` option and the `pageBuster` variable. (`service.HistoryService.pageCacheBuster` + the 8 `bustPageCache` call sites from the decorator plan become dead — delete them too, or leave the nil-safe hook in place if you want to keep the diff small. Recommended: delete, since invalidation now lives in `cassrepo`.)

- [ ] **Step 3:** `make build SERVICE=history-service && make test SERVICE=history-service && make lint`. **Step 4:** commit.

---

## Task 7: retire msgpagecache + integration tests + verify + push

- [ ] **Step 1: delete** `history-service/internal/msgpagecache/` (package + tests) and its config knobs (`HISTORY_MSG_CACHE_*`, `HISTORY_MSG_GEN_TTL`) now that nothing references them. `grep -rn msgpagecache history-service` must come back empty.

- [ ] **Step 2: integration tests** (`bucketcache/integration_test.go` with real Valkey; `cassrepo/*_integration_test.go` with real Cassandra + Valkey). Assert:
  1. **Reuse across reads** — seed a sparse room spanning several sealed buckets; first `GetMessagesBefore` populates the buckets; a second identical read serves them from cache after the Cassandra rows are deleted.
  2. **Hot bucket stays live** — a `before=now` read's current-bucket rows reflect a post-cache Cassandra write (no `hist:{room}:bkt:{currentBucket}` key is ever written).
  3. **Bust one bucket** — edit a message in bucket B; only `hist:{room}:bkt:{B}` is invalidated; a read reflects the edit while other buckets stay cached.
  4. **Size guard** — a bucket with `> MaxRows` rows is not cached (no key), and the read still returns correct results via the live fallback.
  5. **Reactions round-trip** — a reacted message survives the gob→Valkey→gob path (the reason JSON was rejected).
  6. **Cross-page-size reuse** — a `limit=20` then `limit=50` read of the same room reuse the same bucket entries (assert Cassandra query count via a counting session wrapper, or assert buckets present in Valkey).

- [ ] **Step 3: full verification.**
```
make test
make test-integration SERVICE=history-service/internal/bucketcache
make test-integration SERVICE=history-service
make lint
make sast
```
Expected: all green; SAST clean (no new `#nosec`).

- [ ] **Step 4: commit + push.**
```bash
git push -u origin claude/load-history-cache-feasibility-m1c9c9
```

---

## Self-Review

**Design coverage:**
- Cache = mutability boundary: current bucket always live, sealed buckets cached → `cachedDescFetcher` sealed/hot branch (Task 4). ✓
- Caches the sealed portion of hot-anchored reads (decorator's blind spot) → per-bucket fetcher runs per bucket regardless of anchor (Task 4). ✓
- No cursor-format change for DESC → cached fetcher returns nil pageState; `LoadHistory` re-anchors by timestamp; cached fetcher wired ONLY to DESC methods (Task 4). ✓
- gob codec + decode-per-hit for reactions correctness + copy-safety (Task 1, integration Task 7). ✓
- No generation counter: one key per bucket → plain `DEL` (Task 1/5); new messages need no bust (Task 5 rationale). ✓
- Size guard via `LIMIT maxRows+1` + live fallback (Task 2/4). ✓
- Behavior-preserving `fillPage` refactor isolates risk from the ASC/cursor path (Task 3, existing tests unchanged). ✓
- Retire the page-cache decorator so there is one caching layer (Task 6/7). ✓

**Risk notes:** (1) Task 3 is the highest-risk change (touches the shared walker); it is explicitly behavior-preserving and gated on existing tests passing unchanged before Task 4 adds new behavior. (2) The cached DESC fetcher emits a boundary cursor when a bucket fills mid-way; this is safe **only** because `LoadHistory` discards the cursor — enforced by wiring the cached fetcher solely to `GetMessagesBefore`/`GetMessagesBetweenDesc`. If per-bucket is ever extended to the ASC/forward path, it needs a real offset-resume cursor (separate design). (3) Cross-replica L1 staleness after a bust is bounded by `HISTORY_BUCKET_CACHE_TTL` (same trade-off as `pkg/roommetacache`); acceptable because sealed-bucket mutations (edits/deletes of old messages) are rare. (4) `bucketcache` must depend only on `models`, never `cassrepo`, to keep the import direction `cassrepo → bucketcache` acyclic.
