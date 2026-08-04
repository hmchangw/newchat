# History Load-Message Cache (Cassandra page reads, L1+L2 Valkey) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a cache for `history-service`'s **Cassandra message-page reads** — the load-history / scroll-back hot path — which is the one part of the read path that is currently uncached (only the Mongo lookups behind it are cached today, in `history-service/internal/readcache`). The cache serves the repeated, cross-user "scroll up into history" workload from memory/Valkey instead of re-walking buckets and re-decrypting rows in Cassandra.

## Why this is feasible (grounded in the current code)

- **The read seam is clean.** `history-service/internal/service/service.go:19` defines `MessageReader`; it is injected into `HistoryService.msgReader` (`service.go:124`, wired at `cmd/main.go:135`/`:188` as `*cassrepo.Repository`). A caching decorator that implements `MessageReader` drops in without touching handler logic.
- **The expensive work is cacheable.** Reads walk one Cassandra query per 72h bucket (`internal/cassrepo/walker.go` `fillPage`) and decrypt each row at-rest (`messages_by_room.go` `scanMessagesUpTo` → `decryptIfNeeded`). A cache of the returned page skips both the bucket queries and the per-row Vault/AES decrypt.
- **Sealed buckets are immutable.** `messages_by_room` is partitioned by `(room_id, bucket)` where `bucket = floor(created_at/windowMs)*windowMs` (`pkg/msgbucket`). Any bucket strictly older than the current one receives no new rows; it changes only via edit/delete/pin/react mutations, which this service performs itself (`MessageWriter` lives in `history-service`, `service.go:31`) — so the write-site can invalidate synchronously, exactly like `pkg/roommetacache.BustMeta`.
- **All the infrastructure already exists:** `pkg/valkeyutil` (cluster client + `GetJSON`/`SetJSONWithTTL`/`IncrEx`/`Disconnect`), `pkg/cachemetrics`, and the `internal/readcache` LRU+TTL+singleflight pattern to mirror.

## Architecture

A **read-through decorator** (`internal/msgpagecache.Reader`) wraps `*cassrepo.Repository` and satisfies `service.MessageReader`. It caches the four paginated page-read methods (`GetMessagesBefore`, `GetMessagesBetweenDesc`, `GetMessagesAfter`, `GetAllMessagesAsc`); all other `MessageReader` methods pass straight through to the wrapped repo.

**Sealed-only caching.** For each call the decorator computes whether the entire bucket walk stays strictly below the **current** bucket (`sizer.Of(now)`):
- `GetMessagesBefore` / `GetMessagesBetweenDesc` (DESC): cacheable iff `sizer.Of(before) < currentBucket`.
- `GetMessagesAfter` / `GetAllMessagesAsc` (ASC): cacheable iff `sizer.Of(ceiling) < currentBucket`.

The "latest page" (`before = now`, which touches the hot current bucket) is **never cached** — it is the volatile request and clients keep it fresh from real-time delivery anyway. The high-value, repeated, **cross-user** requests — infinite scroll into history, which arrive with a server-generated `Cursor` anchored in older buckets — are fully sealed and cached. Two users independently scrolling the same room receive identical deterministic `NextCursor` values at each step, so their page-N requests carry identical cache keys → shared hits.

**Two tiers, mirroring `roommetacache`:**
- **L1** — in-process `expirable.LRU` keyed by the compound cache key, storing the **JSON-encoded** `cassrepo.Page[models.Message]` as `[]byte` (see the mutation hazard below), singleflight-deduped, `cachemetrics.For("history_msg_page", "l1")`.
- **L2** — Valkey string key holding the same JSON, read-through on L1 miss, repopulated with a TTL. Shared across replicas, survives restarts. Fail-open: any Valkey error logs at warn and degrades to the live Cassandra read.

**Key + generation-based invalidation.** Because the cache key is compound (method + bounds + cursor + pageSize) rather than one key per room, individual entries cannot be enumerated to delete on a write. Instead every key is namespaced by a **per-room generation counter** held in Valkey (`hist:{roomID}:gen`, hash-tagged on `{roomID}`):

```
key = "hist:{" + roomID + "}:pg:" + gen + ":" + method + ":" + boundsFingerprint + ":" + pageSize
```

- On any mutation the write-site calls `Bump(ctx, roomID)` → `IncrEx(hist:{roomID}:gen)`. The next read reads the new gen, so every previously-cached page for that room is instantly orphaned (and expires by TTL).
- Reads resolve the gen via `Gen(ctx, roomID)` — a Valkey `GET`, itself fronted by a **short-TTL L1** (default 1s) so a burst of pages for one room shares one gen lookup. A missing gen key reads as gen `0`.
- The gen L1 TTL is the *only* residual-staleness window for a same-site mutation (see below), and it is sub-request-latency small.

**Write-site bump call sites** (all in `history-service`, after the successful Cassandra mutation):
- edit — `internal/service/messages.go:517` (`UpdateMessageContent`)
- delete — `internal/service/messages.go:598` (`SoftDeleteMessage`, only when `applied`)
- pin / unpin — `internal/service/pin.go:120` / `:164`
- react add / remove — `internal/service/reactions.go:87` / `:76`
- migration edit / delete — `internal/service/migration.go:49` / `:112`

### Accepted residual staleness (TTL-backstopped, by design)

- **Gen L1 window (same-site mutations):** after a bump, a replica whose gen-L1 entry is still warm serves the old gen (hence stale pages) for up to `HISTORY_MSG_GEN_TTL` (default 1s). Bounded, tiny.
- **Cross-site / federated mutations:** an edit/delete that originates at a *remote* site is applied to this site's Cassandra by `message-worker`, **not** by `history-service`, so the write-site bump never fires here. These go stale by at most the page TTL (`HISTORY_MSG_CACHE_TTL`, default 30s). Optional hardening (out of scope for the first cut, noted in Task 7): a durable `history-service-cachebust` queue-group consumer on `MESSAGES-CANONICAL-{siteID}` (`chat.msg.canonical.{siteID}.{updated,deleted,pinned,unpinned,reacted,created}`, `pkg/subject` builders exist) that calls `Bump` on the event's `roomID`, closing the gap without waiting for TTL.
- **`created` into a sealed bucket:** a normal new message lands in the current (uncached) bucket, but a federation replay or backdated write can insert into a sealed bucket. The write-site bump does not cover cross-site `created`; the page TTL backstops it (and the optional consumer closes it).

### The mutation-in-place hazard (must-honor correctness constraint)

`LoadHistory` mutates the reader's returned slice **in place** after the read: `redactUnavailableQuotes(page.Data, accessSince)` and `setDecodedAttachments(c, page.Data)` (`internal/service/messages.go:94-95`), and the surrounding/next paths do the same. If the decorator returned a cached `[]models.Message` by reference, the first caller's redaction would corrupt every subsequent cached hit. **Therefore the cache stores and returns JSON-encoded bytes and decodes a fresh `Page` on every hit** (L1 and L2 both). This also makes L1 and L2 byte-identical and copy-safe for free. Encode/decode uses `encoding/json` (history-service is not on the sonic hot-path list in CLAUDE.md).

Caching at the `MessageReader` seam is also **pre-redaction and pre-attachment-decode** (those run in the service layer *after* the reader returns), so a cached page is shareable across users regardless of their per-user `accessSince` redaction — the redaction is re-applied per request on the decoded copy.

### Cross-user key correctness

- The `accessSince == nil` path (full-history rooms) uses `GetMessagesBefore` → fully shareable across all members.
- The `accessSince != nil` path uses `GetMessagesBetweenDesc(roomID, *accessSince, before)`; `accessSince` (when history was shared with a user) is part of the bounds fingerprint, so users sharing a join time share a key and users with different windows get correctly-distinct keys.

**Tech Stack:** Go 1.25, `pkg/valkeyutil` (go-redis/v9 cluster wrapper), `github.com/hashicorp/golang-lru/v2/expirable`, `golang.org/x/sync/singleflight`, `pkg/cachemetrics`, `pkg/msgbucket`, `encoding/json`, `pkg/testutil` (testcontainers Valkey + Cassandra), testify, `go.uber.org/mock`.

**Docs note — no `docs/client-api.md` change required:** this is an internal caching change behind the `chat.user.…msg.history` / `.next` / `.surrounding` handlers. No client-facing request/response schema, field, error case, or event changes — only the *source* of the message page (cache vs Cassandra). The client-facing-handler doc rule in CLAUDE.md §5 is therefore not triggered.

---

## File Structure

**New files:**
- `history-service/internal/msgpagecache/msgpagecache.go` — `Key`, generation `Gen`/`Bump`, `Reader` decorator (read-through over the four page methods; pass-through for the rest), JSON codec, config-driven construction. The only genuinely new logic.
- `history-service/internal/msgpagecache/msgpagecache_test.go` — unit tests (fake `valkeyutil.Client` + a stub `MessageReader`; no containers).
- `history-service/internal/msgpagecache/integration_test.go` — integration tests (real Valkey + Cassandra) + this package's `TestMain`.

**Modified files:**
- `history-service/internal/config/config.go` — add `ValkeyAddrs`, `ValkeyPassword`, `MsgCacheTTL`, `MsgCacheL1Size`, `MsgGenTTL`; extend `validate`.
- `history-service/cmd/main.go` — connect an optional `valkeyutil.Client`, wrap `cassRepo` with `msgpagecache.NewReader(...)` as the `msgReader` seam (writer stays the raw repo), give `HistoryService` the bumper, add a Valkey shutdown hook.
- `history-service/internal/service/service.go` — add a `bump func(ctx, roomID)` (or a small `PageCacheBuster` interface, nil-safe) field to `HistoryService` + constructor `Option`.
- `history-service/internal/service/messages.go`, `pin.go`, `reactions.go`, `migration.go` — one `s.bustPageCache(ctx, roomID)` call after each successful mutation (8 sites).
- `history-service/internal/service/*_test.go` — assert bump fires on mutation, and does not on read-only / failed-mutation paths.
- `history-service/deploy/docker-compose.yml` — add `VALKEY_ADDRS` / `HISTORY_MSG_CACHE_TTL` for local dev.

**No-cycle check:** `msgpagecache` imports `cassrepo`, `models`, `valkeyutil`, `msgbucket`, `cachemetrics` — none import `msgpagecache`. `service` importing `msgpagecache` is unnecessary (it depends only on the `MessageReader` interface and a nil-safe buster func), keeping the decorator out of the service package entirely.

---

## Task 1: msgpagecache core — key, generation, decorator (unit-tested)

**Files:**
- Create: `history-service/internal/msgpagecache/msgpagecache.go`
- Test: `history-service/internal/msgpagecache/msgpagecache_test.go`

- [ ] **Step 1: Write the failing unit tests**

Cover: (a) `Key` shape + `{roomID}` hash tag + gen namespacing; (b) sealed-detection — a `before` in the current bucket bypasses the cache (delegates, never touches Valkey), a `before` in an older bucket is cached; (c) L1 hit returns a **distinct slice** from a prior caller that mutated its result (the in-place-mutation guard); (d) L2 hit repopulates L1; (e) `Bump` increments the room gen and the next read misses; (f) fail-open — a Valkey `Get`/`Set` error still returns the live Cassandra page; (g) pass-through methods (`GetMessageByID`, `GetMessagesByIDs`, pinned, thread) never consult the cache.

Use a `fakeValkey` (in-memory `map`, injectable `getErr`/`setErr`/`incrErr`) and a `stubReader` implementing `service.MessageReader` that records calls and returns canned pages. Assert `stubReader` call counts to prove hits skip Cassandra.

```go
package msgpagecache

// fakeValkey: in-memory valkeyutil.Client (Get/Set/SetNX/IncrEx/Del/Close),
//   with per-op error hooks and a call log.
// stubReader: records (method, roomID, bounds) and returns a fixed Page.

func TestKey_HashTagAndGenNamespace(t *testing.T) {
	k := pageKey("r1", 7, "before", "b=..|f=..", 20)
	assert.True(t, strings.HasPrefix(k, "hist:{r1}:pg:7:before:"))
}

func TestReader_CurrentBucket_BypassesCache(t *testing.T) {
	// before == now (current bucket): must delegate and never GET/SET Valkey.
}

func TestReader_SealedBucket_CachesAndServesCopy(t *testing.T) {
	// 1st call: miss → stubReader called once, page cached.
	// caller mutates the returned slice in place (simulate redaction).
	// 2nd call: hit → stubReader NOT called again, returned slice is pristine.
}

func TestReader_Bump_InvalidatesRoom(t *testing.T) { /* gen++ → next read misses */ }
func TestReader_FailOpen_OnValkeyError(t *testing.T) { /* getErr set → live page still returned */ }
func TestReader_PassThroughMethods_NeverCache(t *testing.T) { /* GetMessageByID etc. */ }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=history-service/internal/msgpagecache`
Expected: FAIL — `undefined: pageKey`, `undefined: NewReader`, etc.

- [ ] **Step 3: Implement `msgpagecache.go`**

Sketch (fill in per the tests). `Reader` embeds the wrapped `MessageReader` so pass-through methods need no boilerplate; only the four page methods are overridden.

```go
package msgpagecache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// now is a package var so tests can pin the clock; production uses time.Now.
var now = func() time.Time { return time.Now().UTC() }

func genKey(roomID string) string  { return "hist:{" + roomID + "}:gen" }
func pageKey(roomID string, gen uint64, method, bounds string, pageSize int) string {
	return "hist:{" + roomID + "}:pg:" + strconv.FormatUint(gen, 10) + ":" + method + ":" + bounds + ":" + strconv.Itoa(pageSize)
}

// Reader decorates a service.MessageReader with an L1+L2 cache for the four
// sealed-bucket page reads. All other methods pass through via the embed.
type Reader struct {
	service.MessageReader
	valkey   valkeyutil.Client
	sizer    msgbucket.Sizer
	l1       *lru.LRU[string, []byte] // encoded Page bytes; decode per hit (copy-safe)
	genL1    *lru.LRU[string, uint64]
	sf       singleflight.Group
	ttl      time.Duration
	metrics  cachemetrics.Recorder // reuse the readcache Recorder shape
}

func NewReader(inner service.MessageReader, valkey valkeyutil.Client, sizer msgbucket.Sizer, l1Size int, ttl, genTTL time.Duration) (*Reader, error) { /* validate + build LRUs */ }

func (r *Reader) currentBucket() int64 { return r.sizer.Of(now()) }

// boundsFingerprint canonicalizes the per-method bounds + cursor into the key.
func boundsBefore(before, floor time.Time, pr cassrepo.PageRequest) string { /* unixmilli + cursor.Encode() */ }
// ... boundsBetween, boundsAfter, boundsAllAsc

func (r *Reader) GetMessagesBefore(ctx context.Context, roomID string, before, floor time.Time, pr cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
	if r.valkey == nil || r.sizer.Of(before) >= r.currentBucket() {
		return r.MessageReader.GetMessagesBefore(ctx, roomID, before, floor, pr) // hot/disabled → live
	}
	return r.readThrough(ctx, roomID, "before", boundsBefore(before, floor, pr), pr.PageSize,
		func(ctx context.Context) (cassrepo.Page[models.Message], error) {
			return r.MessageReader.GetMessagesBefore(ctx, roomID, before, floor, pr)
		})
}
// GetMessagesBetweenDesc / GetMessagesAfter / GetAllMessagesAsc: identical shape,
// ASC pair gates on r.sizer.Of(ceiling) >= currentBucket().

// readThrough: gen := r.gen(ctx, roomID); key := pageKey(...); GET L1 → GET L2 → load+SET.
//   Decode a FRESH Page from bytes on every return (copy-safe). Fail-open on any Valkey error.

// gen: GET genL1 → GET Valkey genKey (ErrCacheMiss ⇒ 0) → cache in genL1 with genTTL.

// Bump: r.valkey.IncrEx(ctx, genKey(roomID), 0) best-effort; log+swallow on error.
func (r *Reader) Bump(ctx context.Context, roomID string) { /* nil-safe, fail-open */ }
```

Key implementation notes to honor in code:
- **Copy-safety:** store `[]byte` in L1; on every hit `json.Unmarshal` into a new `cassrepo.Page[models.Message]`. Never hand back a shared slice.
- **Singleflight** the `load+SET` on the compound key so a room's first scroll doesn't stampede Cassandra.
- **Fail-open everywhere:** nil client, `Get`/`Set`/`IncrEx` errors → log `warn` + serve/So the live path; only the Cassandra result governs the returned error.
- **`Bump` uses `IncrEx(key, 0)`** — zero TTL means the gen counter never expires (a per-room monotonic namespace), matching `valkeyutil` semantics ("zero ttl stores without expiry").

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=history-service/internal/msgpagecache`
Expected: PASS.

- [ ] **Step 5: Lint + commit**

```bash
make lint
git add history-service/internal/msgpagecache/msgpagecache.go history-service/internal/msgpagecache/msgpagecache_test.go
git commit -m "feat(history-service): add L1+L2 message-page cache decorator

Sealed-bucket read-through over MessageReader with per-room generation
invalidation; stores encoded pages so in-place redaction can't corrupt hits."
```

---

## Task 2: Config — Valkey + cache knobs

**Files:**
- Modify: `history-service/internal/config/config.go`

- [ ] **Step 1: Add fields to `Config`** (after the preview-cache block, `config.go:90`):

```go
	// Message-page cache (Cassandra sealed-bucket reads). L2 is Valkey; when
	// ValkeyAddrs is empty the whole cache is disabled and reads go direct to
	// Cassandra. Only sealed buckets (strictly older than the current one) are
	// cached; the hot "latest" page is always live.
	ValkeyAddrs    []string      `env:"VALKEY_ADDRS"            envSeparator:","`
	ValkeyPassword string        `env:"VALKEY_PASSWORD"         envDefault:""`
	MsgCacheTTL    time.Duration `env:"HISTORY_MSG_CACHE_TTL"   envDefault:"30s"`
	MsgCacheL1Size int           `env:"HISTORY_MSG_CACHE_L1_SIZE" envDefault:"50000"`
	MsgGenTTL      time.Duration `env:"HISTORY_MSG_GEN_TTL"     envDefault:"1s"`
```

- [ ] **Step 2: Extend `validate`** (reject negatives, mirroring the existing cache-knob checks at `config.go:115`):

```go
	if cfg.MsgCacheTTL < 0 {
		return fmt.Errorf("HISTORY_MSG_CACHE_TTL must be >= 0, got %s", cfg.MsgCacheTTL)
	}
	if cfg.MsgCacheL1Size < 0 {
		return fmt.Errorf("HISTORY_MSG_CACHE_L1_SIZE must be >= 0, got %d", cfg.MsgCacheL1Size)
	}
	if cfg.MsgGenTTL < 0 {
		return fmt.Errorf("HISTORY_MSG_GEN_TTL must be >= 0, got %s", cfg.MsgGenTTL)
	}
```

- [ ] **Step 3: Add a config test** for the new defaults + a negative-TTL rejection (mirror existing config tests if present, else a small table test). Run `make test SERVICE=history-service/internal/config`.

- [ ] **Step 4: Commit**

```bash
git add history-service/internal/config/config.go history-service/internal/config/*_test.go
git commit -m "feat(history-service): config for message-page cache (Valkey + TTLs)"
```

---

## Task 3: Service — nil-safe page-cache buster hook

**Files:**
- Modify: `history-service/internal/service/service.go` (struct + constructor Option), and add the 8 bump call sites (Task 4 tests them).

- [ ] **Step 1: Add the buster field + Option**

In `HistoryService` (`service.go:123`) add:

```go
	// pageCacheBuster invalidates the message-page cache for a room after a
	// mutation. nil disables invalidation (cache off, or purely TTL-backstopped).
	pageCacheBuster PageCacheBuster
```

Define the interface + a nil-safe helper (new small block in `service.go`, near the other interfaces):

```go
// PageCacheBuster bumps a room's message-page cache generation after a
// mutation. Satisfied by *msgpagecache.Reader (via Bump).
type PageCacheBuster interface {
	Bump(ctx context.Context, roomID string)
}

func (s *HistoryService) bustPageCache(ctx context.Context, roomID string) {
	if s.pageCacheBuster == nil {
		return
	}
	s.pageCacheBuster.Bump(ctx, roomID)
}
```

Add a constructor `Option` (follow the existing `WithPreviewCache` option pattern):

```go
func WithPageCacheBuster(b PageCacheBuster) Option {
	return func(s *HistoryService) { s.pageCacheBuster = b }
}
```

- [ ] **Step 2: `make generate SERVICE=history-service`** if the mock set needs the new interface (only if a test mocks `PageCacheBuster`; the concrete `*Reader` is used in wiring). Then `make test SERVICE=history-service` to confirm it still builds.

- [ ] **Step 3: Commit**

```bash
git add history-service/internal/service/service.go
git commit -m "feat(history-service): nil-safe page-cache buster hook on HistoryService"
```

---

## Task 4: Wire the 8 write-site bumps (TDD)

**Files:**
- Modify: `messages.go` (edit `:517`, delete `:598`), `pin.go` (`:120`, `:164`), `reactions.go` (`:87`, `:76`), `migration.go` (`:49`, `:112`)
- Test: the corresponding `*_test.go`

- [ ] **Step 1: Write failing tests** — inject a spy `PageCacheBuster` (records `roomID`s) via `WithPageCacheBuster` and assert:
  - edit success → one bump for the room; edit that fails the writer → **no** bump.
  - delete: bump **only when `applied == true`** (a lost LWT race must not bump).
  - pin, unpin, react-add, react-remove → bump on success.
  - migration edit/delete → bump on success.
  - a **read-only** handler (`LoadHistory`) → **no** bump.

Run: `make test SERVICE=history-service` → FAIL (no bump calls yet).

- [ ] **Step 2: Add the calls** immediately after each successful mutation. Examples:

`messages.go:517` (edit):
```go
	if err := s.msgWriter.UpdateMessageContent(c, msg, req.NewMsg, editedAt); err != nil {
		return nil, fmt.Errorf("update message content: %w", err)
	}
	s.bustPageCache(c, roomID)
```

`messages.go:598` (delete — guard on `applied`):
```go
	actualDeletedAt, applied, newTcount, newThreadLastMsgAt, err := s.msgWriter.SoftDeleteMessage(c, msg, deletedAt)
	if err != nil {
		return nil, fmt.Errorf("soft delete message: %w", err)
	}
	if applied {
		s.bustPageCache(c, roomID)
	}
```

`pin.go:120`/`:164`, `reactions.go:87`/`:76`, `migration.go:49`/`:112`: add `s.bustPageCache(c, <roomID>)` right after the writer call returns nil. Use the `roomID` already in scope at each handler (confirm the variable name per site; pin/react resolve it from the request/subject like the other handlers).

- [ ] **Step 3: Run tests to green + lint**

Run: `make test SERVICE=history-service && make lint`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add history-service/internal/service/messages.go history-service/internal/service/pin.go history-service/internal/service/reactions.go history-service/internal/service/migration.go history-service/internal/service/*_test.go
git commit -m "feat(history-service): bust message-page cache on edit/delete/pin/react"
```

---

## Task 5: Wire the cache in main

**Files:**
- Modify: `history-service/cmd/main.go`

- [ ] **Step 1: Connect an optional Valkey client** (after the Cassandra connect block, `main.go:118`):

```go
	var msgValkey valkeyutil.Client
	if len(cfg.ValkeyAddrs) > 0 {
		msgValkey, err = valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword,
			valkeyutil.WithObservability(sdk), valkeyutil.WithRequireParentSpan(true))
		if err != nil {
			slog.Error("valkey connect (message-page cache) failed", "error", err)
			os.Exit(1)
		}
	}
```

- [ ] **Step 2: Wrap the reader seam** — after `cassRepo := cassrepo.NewRepository(...)` (`main.go:135`), build the decorator and use it as `msgReader` while `msgWriter` stays the raw repo. Since `service.New` currently takes one `msgs` value for both roles (`main.go:188`, `service.go:153-154`), split them: pass the caching reader for the reader arg and keep the raw `cassRepo` for the writer arg (adjust `service.New`'s signature to take a `MessageReader` and a `MessageWriter`, or add a `WithMessageReader` Option — the Option keeps the change local).

```go
	var msgReader service.MessageReader = cassRepo
	var pageBuster service.PageCacheBuster
	if msgValkey != nil && cfg.MsgCacheL1Size > 0 && cfg.MsgCacheTTL > 0 {
		cr, err := msgpagecache.NewReader(cassRepo, msgValkey, bucketSizer, cfg.MsgCacheL1Size, cfg.MsgCacheTTL, cfg.MsgGenTTL)
		if err != nil {
			slog.Error("init message-page cache failed", "error", err)
			os.Exit(1)
		}
		msgReader = cr
		pageBuster = cr
		slog.Info("message-page cache enabled", "l1Size", cfg.MsgCacheL1Size, "ttl", cfg.MsgCacheTTL, "genTTL", cfg.MsgGenTTL)
	}
```

Then thread `msgReader` into the reader slot and append `service.WithPageCacheBuster(pageBuster)` (nil-safe) to `opts` before `service.New(...)`.

- [ ] **Step 3: Shutdown hook** — add to the `shutdown.Wait` list (`main.go:227`), after the Cassandra close:

```go
		func(ctx context.Context) error { valkeyutil.Disconnect(msgValkey); return nil },
```

Add `"github.com/hmchangw/chat/history-service/internal/msgpagecache"` and `"github.com/hmchangw/chat/pkg/valkeyutil"` to the imports. `valkeyutil.Disconnect` is nil-safe, so the hook is fine when Valkey is unconfigured.

- [ ] **Step 4: Build + unit tests + lint**

Run: `make build SERVICE=history-service && make test SERVICE=history-service && make lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add history-service/cmd/main.go history-service/internal/service/service.go
git commit -m "feat(history-service): wire L1+L2 message-page cache into main"
```

---

## Task 6: Integration tests (real Valkey + Cassandra)

**Files:**
- Create: `history-service/internal/msgpagecache/integration_test.go`

- [ ] **Step 1: Write the tests** (`//go:build integration`, `package msgpagecache`, `func TestMain(m *testing.M) { testutil.RunTests(m) }`). Use `testutil.SharedValkeyCluster(t)` (+ `t.Cleanup(FlushValkey)`) and `testutil.CassandraKeyspace(t, "msgpagecache")`; construct a real `*cassrepo.Repository` over the keyspace and seed sealed-bucket rows (created_at older than one bucket window).

Assert:
1. **Miss → populate → serve from L2:** first `GetMessagesBefore` (sealed `before`) reads Cassandra; drop the Cassandra rows; second identical call still returns them (served from L2).
2. **Bump invalidates:** after `Bump(ctx, roomID)`, the next read misses and reflects fresh Cassandra state.
3. **Hot page bypasses:** `before = now` never populates a Valkey key (assert no `hist:{room}:pg:*` key exists).
4. **Copy-safety:** mutate the first result's slice; a second cached read returns pristine data.
5. **Fail-open:** point the client at a dead addr (or inject a wrapper that errors) → reads still succeed from Cassandra.

- [ ] **Step 2: Run**

Run: `make test-integration SERVICE=history-service/internal/msgpagecache`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add history-service/internal/msgpagecache/integration_test.go
git commit -m "test(history-service): integration coverage for message-page cache"
```

---

## Task 7: Local dev wiring, docs, verification, push

**Files:**
- Modify: `history-service/deploy/docker-compose.yml`

- [ ] **Step 1: Add Valkey to the compose** — read `history-service/deploy/docker-compose.yml`; in the history-service `environment:` block add `VALKEY_ADDRS` / `HISTORY_MSG_CACHE_TTL` (and a `valkey`/Valkey-cluster service + `depends_on` if none is present, mirroring a service whose compose already runs Valkey, e.g. `room-worker/deploy/docker-compose.yml`).

- [ ] **Step 2: (Optional hardening, note only — do NOT implement unless scoped in)** If cross-site edit staleness beyond `HISTORY_MSG_CACHE_TTL` is unacceptable, add a durable queue-group consumer `history-service-cachebust` on `MESSAGES-CANONICAL-{siteID}` filtering `chat.msg.canonical.{siteID}.{updated,deleted,pinned,unpinned,reacted,created}` that unmarshals the event and calls `pageBuster.Bump(ctx, roomID)`. Sequential `cons.Consume` is fine (low volume relative to reads). This closes the federated-write gap; the first cut relies on the write-site bump + TTL.

- [ ] **Step 3: Full verification**

Run:
```bash
make test
make test-integration SERVICE=history-service/internal/msgpagecache
make test-integration SERVICE=history-service
make lint
make sast
```
Expected: PASS across the board; SAST clean (no new `InsecureSkipVerify`, unchecked conversions, or subprocess calls, so no `#nosec` needed).

- [ ] **Step 4: Commit + push**

```bash
git add history-service/deploy/docker-compose.yml
git commit -m "chore(deploy): enable message-page cache Valkey for history-service local dev"
git push -u origin claude/load-history-cache-feasibility-m1c9c9
```

---

## Self-Review

**Design coverage:**
- Caches the one uncached hot path (Cassandra page reads) at the `MessageReader` seam → decorator (Task 1), wired without touching handler logic (Task 5). ✓
- Sealed-only: hot "latest" page always live; scroll-back cached and cross-user shareable → `sizer.Of(before/ceiling) < currentBucket` gate (Task 1). ✓
- In-place-mutation hazard (`redactUnavailableQuotes`/`setDecodedAttachments`) → cache stores encoded bytes, decodes a fresh Page per hit; L1==L2 format (Task 1, tested copy-safety Tasks 1 & 6). ✓
- L1+L2 mirroring `roommetacache`, fail-open, singleflight, cachemetrics → Task 1. ✓
- Invalidation for a compound-keyed cache → per-room generation counter, write-site `Bump` at all 8 mutation sites (Tasks 3-4); delete guarded on `applied`. ✓
- Residual staleness bounded + documented (gen-L1 window; cross-site via TTL; optional consumer) → Architecture + Task 7 note. ✓
- Config-gated and default-off when `VALKEY_ADDRS` unset → Task 2 + main guard (Task 5). ✓
- No client-api.md change (internal-only) → stated up front. ✓

**Type/signature consistency:** `NewReader(inner service.MessageReader, valkey valkeyutil.Client, sizer msgbucket.Sizer, l1Size int, ttl, genTTL time.Duration) (*Reader, error)`; `Reader` embeds `service.MessageReader` and overrides the four page methods with identical `cassrepo.Page[models.Message]` return types matching the interface at `service.go:20-23`; `Bump(ctx, roomID)` satisfies `service.PageCacheBuster`; `WithPageCacheBuster` mirrors the existing `WithPreviewCache` Option. Valkey usage (`ConnectCluster`, `GetJSON`/`SetJSONWithTTL` or raw `Get`/`Set`, `IncrEx(key, 0)`, `Disconnect`) matches `pkg/valkeyutil/valkey.go`.

**Placeholder scan:** Task 1's implementation is a sketch (marked as such) to be completed test-first; every other task shows concrete code/edits with exact file:line anchors and an expected command result. Compose edits (Task 7) instruct reading the file first because indentation/service-name vary.

**Risk notes:** (1) `service.New` currently binds one value to both reader and writer — Task 5 splits them via an Option to inject the caching reader while the writer stays raw (both still `*cassrepo.Repository` underneath, so `MessageWriter` is unaffected). (2) The gen counter is unbounded-lifetime by design (a per-room namespace, not data) — negligible key growth, one small key per active room. (3) A very large sealed bucket produces a large cached page, but pages are `pageSize`-bounded (≤100 rows), so entry size is bounded regardless of bucket density.
