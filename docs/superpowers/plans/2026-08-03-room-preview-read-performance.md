# Room-list Preview Read Performance (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the local-site `subscription.list` fast by removing the per-room Cassandra over-walk on history-service's `rooms.get` path and caching the resolved preview — with zero changes to the message write path.

**Architecture:** Two independent read-side wins in history-service, plus instrumentation. (1a) `roomLastPreviewMessage` currently asks `GetMessagesBefore` for a fixed 50-row page but uses only the first eligible row, so `fillPage` over-walks older/empty buckets to fill 50; switch to a tiny first page that grows geometrically only when the newest rows are ineligible. (1b) Front the per-room preview resolve with a short-TTL LRU+singleflight cache reusing the existing `readcache` machinery. (1c) Add a `rooms.get` batch-size metric; the cache hit/miss metric comes free from `cachemetrics`. Phase 2 (denormalization) is explicitly out of scope — see the design spec.

**Tech Stack:** Go 1.25, gocql (Cassandra), `go.uber.org/mock` + `stretchr/testify`, OpenTelemetry metrics via `pkg/cachemetrics` and `go.opentelemetry.io/otel`.

**Design spec:** `docs/superpowers/specs/2026-08-03-room-preview-read-performance-design.md` (Phase 1).

## Global Constraints

- Go 1.25. Use `make` targets only — never raw `go` commands. Key: `make test SERVICE=history-service`, `make lint`, `make test-integration SERVICE=history-service`.
- TDD: write the failing test first, confirm it fails, implement minimally, confirm green, commit.
- Minimum 80% coverage on touched packages; all tests run under `-race` (the Makefile handles it).
- Errors wrapped with context (`fmt.Errorf("…: %w", err)`); structured `log/slog` only; no `fmt.Println`.
- No client-facing wire change in Phase 1 → **no `docs/client-api.md` edit**.
- No store-interface change → **no `make generate`** needed.
- `roomLastPreviewMessage` is shared by the `rooms.get` read path AND `previewAfterMutation` (edit/delete). Task 1 changes it; the full history-service unit suite must stay green.
- Each commit message ends with the two trailers this session requires:
  `Co-Authored-By: Claude <noreply@anthropic.com>` and the `Claude-Session:` line.

---

## File Structure

- `history-service/internal/service/rooms.go` — Task 1 (walk fix in `roomLastPreviewMessage`, new walk constants) + Task 3 (`resolvePreview` helper, `RoomsGet` uses it) + Task 4 (batch-size record).
- `history-service/internal/service/rooms_test.go` — Tasks 1, 3 unit tests.
- `history-service/internal/service/service.go` — Task 3 (`PreviewCache` interface, `Option`, `WithPreviewCache`, field, variadic `New`).
- `history-service/internal/service/metrics.go` — Task 4 (new file: batch-size histogram).
- `history-service/internal/readcache/readcache.go` — Task 2 (`PreviewCache` type).
- `history-service/internal/readcache/readcache_test.go` — Task 2 tests.
- `history-service/internal/config/config.go` — Task 3 (`PreviewCacheSize`/`PreviewCacheTTL` + validate guards).
- `history-service/cmd/main.go` — Task 3 (construct cache, pass `WithPreviewCache`).

---

## Task 1: Fix the preview-walk over-fetch

Replace the fixed 50-row × 5-page walk in `roomLastPreviewMessage` with a small-first-page geometric escalation. Common case (newest message eligible) becomes a single Cassandra query; the ≤250-message ineligible-skip budget is preserved. Exhaustion is detected via the walker's `page.HasNext` (authoritative terminal signal), which keeps the existing single-page tests green.

**Files:**
- Modify: `history-service/internal/service/rooms.go:16-21` (constants), `:71-114` (`roomLastPreviewMessage`)
- Test: `history-service/internal/service/rooms_test.go`

**Interfaces:**
- Consumes: `s.msgReader.GetMessagesBefore(ctx, roomID, before, floor, cassrepo.PageRequest) (cassrepo.Page[models.Message], error)`; `cassrepo.PageRequest{PageSize int}`; `cassrepo.Page[T]{Data []T; HasNext bool}`; `makePage(msgs []models.Message, hasNext bool) cassrepo.Page[models.Message]` (test helper, `messages_test.go:130`).
- Produces: `roomLastPreviewMessage(ctx, roomID, now) (models.PreviewMessage, bool)` — signature unchanged.

- [ ] **Step 1: Write the failing test — eligible newest resolves in a single query**

Add to `rooms_test.go`. This asserts the first `GetMessagesBefore` is issued with `PageSize == 1` and that exactly one read happens when the newest message is eligible:

```go
func TestHistoryService_RoomsGet_EligibleNewest_SingleQuery(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	var sizes []int
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ time.Time, _ time.Time, pr cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
			sizes = append(sizes, pr.PageSize)
			return makePage([]models.Message{
				{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt, Sender: models.Participant{Account: "alice"}},
			}, false), nil
		}).Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
	assert.Equal(t, []int{1}, sizes, "first (and only) walk page must be size 1")
}
```

Add the imports `"context"` and `"github.com/hmchangw/chat/history-service/internal/cassrepo"` to `rooms_test.go` if not already present.

- [ ] **Step 2: Run the test — confirm it fails**

Run: `make test SERVICE=history-service`
Expected: FAIL — current code requests `PageSize == 50`, so `sizes` is `[50]`, not `[1]`.

- [ ] **Step 3: Implement the walk fix**

In `rooms.go`, replace the walk constants (`:16-21`):

```go
const (
	maxRoomsGetBatch       = 100 // mirrors maxGetByIDsBatchSize
	maxRoomsGetConcurrency = 16  // mirrors cassrepo.maxConcurrentIDReads

	// Preview walk: fetch a tiny first page so the common case (newest message
	// eligible) costs one Cassandra query instead of over-walking older buckets
	// to fill a 50-row page whose extra rows are unused. Grow geometrically only
	// to skip a run of ineligible (deleted/system) messages; lastMsgWalkMaxScan
	// preserves the previous 50×5 = 250 ineligible-skip budget before giving up.
	lastMsgWalkFirstPage = 1
	lastMsgWalkGrowth    = 8
	lastMsgWalkMaxScan   = 250
)
```

Replace `roomLastPreviewMessage` (`:71-114`) with:

```go
func (s *HistoryService) roomLastPreviewMessage(ctx context.Context, roomID string, now time.Time) (models.PreviewMessage, bool) {
	lastMsgAt, createdAt, err := s.resolveRoomTimesOrError(ctx, roomID, nil, now)
	if err != nil {
		slog.WarnContext(ctx, "rooms.get room degraded", "room_id", roomID,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		return models.PreviewMessage{}, false
	}

	ceiling, floor := s.walkBounds(lastMsgAt, createdAt, now)
	before := ceiling.Add(time.Millisecond)

	pageSize := lastMsgWalkFirstPage
	scanned := 0
	for scanned < lastMsgWalkMaxScan {
		if remaining := lastMsgWalkMaxScan - scanned; pageSize > remaining {
			pageSize = remaining
		}
		page, err := s.msgReader.GetMessagesBefore(ctx, roomID, before, floor, cassrepo.PageRequest{PageSize: pageSize})
		if err != nil {
			slog.WarnContext(ctx, "rooms.get latest-message read degraded", "room_id", roomID,
				"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
			return models.PreviewMessage{}, false
		}
		if len(page.Data) == 0 {
			return models.PreviewMessage{}, false // room empty or floor reached
		}
		for i := range page.Data {
			m := page.Data[i]
			// System and deleted messages aren't representative room content — skip to the
			// previous eligible message. Quoted replies ARE eligible (normal user content).
			if m.Deleted || pkgmodel.IsSystemMessageType(m.Type) {
				continue
			}
			return s.toPreviewMessage(ctx, &m), true
		}
		// Whole page ineligible. HasNext=false means the walk reached a terminal
		// state (floor/empty) — stop. Otherwise grow the page and continue strictly
		// before the oldest one seen.
		scanned += len(page.Data)
		if !page.HasNext {
			return models.PreviewMessage{}, false
		}
		before = page.Data[len(page.Data)-1].CreatedAt
		pageSize *= lastMsgWalkGrowth
	}
	return models.PreviewMessage{}, false // ineligible tail longer than the scan budget
}
```

Add `"github.com/hmchangw/chat/history-service/internal/cassrepo"` to `rooms.go`'s imports. Remove the now-unused `parsePageRequest` call that was in this function (the helper stays in `utils.go` for other callers).

- [ ] **Step 4: Run tests — confirm the new test and all existing rooms tests pass**

Run: `make test SERVICE=history-service`
Expected: PASS. The existing `SkipsDeletedTail`, `SkipsSystemTail`, `AllDeletedOmitted`, `EmptyRoomOmitted`, `LatestMessage`, `FullContent` tests stay green — they return single pages with `hasNext=false`, so the new `!page.HasNext` exhaustion check behaves identically.

- [ ] **Step 5: Write the failing test — geometric escalation across ineligible pages**

```go
func TestHistoryService_RoomsGet_EscalatesPastIneligibleTail(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	var sizes []int
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ time.Time, _ time.Time, pr cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
			sizes = append(sizes, pr.PageSize)
			if pr.PageSize == 1 {
				// Newest is deleted; more remains (HasNext=true) → escalate.
				return makePage([]models.Message{
					{MessageID: "d1", RoomID: "r1", Deleted: true, CreatedAt: roomLastMsgAt},
				}, true), nil
			}
			// Next (larger) page reaches the eligible survivor.
			return makePage([]models.Message{
				{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-time.Minute), Sender: models.Participant{Account: "alice"}},
			}, false), nil
		}).Times(2)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
	assert.Equal(t, []int{1, 8}, sizes, "page size grows 1 → 8 on an ineligible first page")
}
```

- [ ] **Step 6: Run tests — confirm pass**

Run: `make test SERVICE=history-service`
Expected: PASS (implementation from Step 3 already supports escalation).

- [ ] **Step 7: Lint and commit**

Run: `make lint`
Then:
```bash
git add history-service/internal/service/rooms.go history-service/internal/service/rooms_test.go
git commit -m "perf(history-service): single-query preview walk with geometric escalation"
```

---

## Task 2: `readcache.PreviewCache`

Add a positives-only LRU+TTL+singleflight cache for the resolved per-room preview, reusing the existing `ttlCache` primitive and `cachemetrics` (which yields hit/miss/error metrics for free).

**Files:**
- Modify: `history-service/internal/readcache/readcache.go` (append the new type)
- Test: `history-service/internal/readcache/readcache_test.go`

**Interfaces:**
- Consumes: unexported `ttlCache[V]`, `newTTLCache[V](size, ttl, cachemetrics.Recorder)`, `cachemetrics.For(cache, tier string) Recorder`, `pkgmodel.PreviewMessage` — all already in the package.
- Produces: `NewPreviewCache(size int, ttl time.Duration) (*PreviewCache, error)`; `(*PreviewCache).Get(ctx context.Context, roomID string, load func(context.Context) (pkgmodel.PreviewMessage, bool, error)) (pkgmodel.PreviewMessage, bool, error)`.

- [ ] **Step 1: Write the failing test — positive cached, negative not cached**

Add to `readcache_test.go`:

```go
func TestPreviewCache_CachesPositiveNotNegative(t *testing.T) {
	pc, err := readcache.NewPreviewCache(100, time.Minute)
	require.NoError(t, err)
	ctx := context.Background()

	// Positive: loader runs once, second Get is a hit.
	posCalls := 0
	load := func(context.Context) (pkgmodel.PreviewMessage, bool, error) {
		posCalls++
		return pkgmodel.PreviewMessage{MessageID: "m1"}, true, nil
	}
	p, ok, err := pc.Get(ctx, "r1", load)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "m1", p.MessageID)
	_, _, _ = pc.Get(ctx, "r1", load)
	assert.Equal(t, 1, posCalls, "positive result is cached")

	// Negative (found=false): never cached, loader runs every time.
	negCalls := 0
	negLoad := func(context.Context) (pkgmodel.PreviewMessage, bool, error) {
		negCalls++
		return pkgmodel.PreviewMessage{}, false, nil
	}
	_, ok, _ = pc.Get(ctx, "r2", negLoad)
	require.False(t, ok)
	_, _, _ = pc.Get(ctx, "r2", negLoad)
	assert.Equal(t, 2, negCalls, "negative result is not cached")
}

func TestPreviewCache_ErrorNotCachedAndPropagated(t *testing.T) {
	pc, err := readcache.NewPreviewCache(100, time.Minute)
	require.NoError(t, err)
	ctx := context.Background()

	wantErr := errors.New("cassandra down")
	calls := 0
	load := func(context.Context) (pkgmodel.PreviewMessage, bool, error) {
		calls++
		return pkgmodel.PreviewMessage{}, false, wantErr
	}
	_, _, err = pc.Get(ctx, "r1", load)
	require.ErrorIs(t, err, wantErr)
	_, _, _ = pc.Get(ctx, "r1", load)
	assert.Equal(t, 2, calls, "errors are not cached")
}
```

Ensure `readcache_test.go` imports `"context"`, `"errors"`, `"time"`, testify, `pkgmodel "github.com/hmchangw/chat/pkg/model"`, and the `readcache` package (match the existing test file's import style — it may be `package readcache` internal or `readcache_test`; follow whatever is already there).

- [ ] **Step 2: Run the test — confirm it fails**

Run: `make test SERVICE=history-service` (or `go test` via the Makefile target for the package)
Expected: FAIL — `NewPreviewCache` undefined.

- [ ] **Step 3: Implement `PreviewCache`**

Append to `readcache.go`:

```go
// previewEntry is the cached resolved room preview. found=false is never stored
// (positives-only), so an empty room / read miss re-resolves each time — cheap
// after the single-query preview walk, and it keeps stale "no preview" out of
// the cache.
type previewEntry struct {
	preview pkgmodel.PreviewMessage
	found   bool
}

// PreviewCache caches the resolved last-eligible preview message per room.
// lastMsgAt advances on every message, so the configured TTL bounds how stale a
// preview can be; the room list also carries lastMsgAt and clients update open
// rooms from real-time delivery. Positives-only, singleflight-deduped.
type PreviewCache struct {
	cache *ttlCache[previewEntry]
}

// NewPreviewCache builds a preview cache of size entries with the given TTL.
// size and ttl must be positive.
func NewPreviewCache(size int, ttl time.Duration) (*PreviewCache, error) {
	c, err := newTTLCache[previewEntry](size, ttl, cachemetrics.For("history_room_preview", "l1"))
	if err != nil {
		return nil, err
	}
	return &PreviewCache{cache: c}, nil
}

// Get returns the cached preview for roomID or invokes load on miss. Only a
// found preview is cached; a not-found result (empty room) and errors are
// returned to the caller but not stored.
func (c *PreviewCache) Get(ctx context.Context, roomID string, load func(context.Context) (pkgmodel.PreviewMessage, bool, error)) (pkgmodel.PreviewMessage, bool, error) {
	entry, err := c.cache.getOrLoad(ctx, roomID, func(ctx context.Context) (previewEntry, bool, error) {
		p, found, err := load(ctx)
		if err != nil {
			return previewEntry{}, false, err
		}
		return previewEntry{preview: p, found: found}, found, nil
	})
	if err != nil {
		return pkgmodel.PreviewMessage{}, false, err
	}
	return entry.preview, entry.found, nil
}
```

- [ ] **Step 4: Run tests — confirm pass**

Run: `make test SERVICE=history-service`
Expected: PASS.

- [ ] **Step 5: Write the failing test — singleflight dedups concurrent misses**

```go
func TestPreviewCache_SingleflightDedupsConcurrentMisses(t *testing.T) {
	pc, err := readcache.NewPreviewCache(100, time.Minute)
	require.NoError(t, err)
	ctx := context.Background()

	var calls int32
	start := make(chan struct{})
	load := func(context.Context) (pkgmodel.PreviewMessage, bool, error) {
		atomic.AddInt32(&calls, 1)
		<-start // hold all callers inside the loader until released
		return pkgmodel.PreviewMessage{MessageID: "m1"}, true, nil
	}

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() { defer wg.Done(); _, _, _ = pc.Get(ctx, "r1", load) }()
	}
	// Give goroutines time to coalesce on the same key, then release.
	time.Sleep(20 * time.Millisecond)
	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "concurrent misses coalesce to one load")
}
```

Add imports `"sync"` and `"sync/atomic"` to the test file. (The `time.Sleep` here is a test-only coordination aid for exercising concurrency, not production goroutine synchronization.)

- [ ] **Step 6: Run tests — confirm pass**

Run: `make test SERVICE=history-service`
Expected: PASS (singleflight is provided by the underlying `ttlCache`).

- [ ] **Step 7: Lint and commit**

Run: `make lint`
```bash
git add history-service/internal/readcache/readcache.go history-service/internal/readcache/readcache_test.go
git commit -m "feat(history-service): add positives-only room preview readcache"
```

---

## Task 3: Wire the preview cache into the service, config, and main

Add an optional `PreviewCache` dependency to `HistoryService` (variadic option — no churn to existing `New` callers), route `RoomsGet` through it, add env config, and construct it in `main.go`.

**Files:**
- Modify: `history-service/internal/service/service.go` (interface, option, field, variadic `New`)
- Modify: `history-service/internal/service/rooms.go` (`resolvePreview`; `RoomsGet` uses it)
- Modify: `history-service/internal/config/config.go` (env fields + validate)
- Modify: `history-service/cmd/main.go` (construct + pass option)
- Test: `history-service/internal/service/rooms_test.go`

**Interfaces:**
- Consumes: `readcache.NewPreviewCache` (Task 2); `models.PreviewMessage` (alias of `pkgmodel.PreviewMessage`, so `*readcache.PreviewCache` satisfies the service interface).
- Produces: `service.PreviewCache` interface; `service.Option`; `service.WithPreviewCache(pc PreviewCache) Option`; `New(..., opts ...Option)`; `(*HistoryService).resolvePreview(ctx, roomID, now) (models.PreviewMessage, bool)`.

- [ ] **Step 1: Write the failing test — installed cache serves the second read**

Add a cache-enabled service helper and test to `rooms_test.go`:

```go
func newRoomsServiceWithPreviewCache(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository) {
	ctrl := gomock.NewController(t)
	msgs := mocks.NewMockMessageRepository(ctrl)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	rooms := mocks.NewMockRoomRepository(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadRooms := mocks.NewMockThreadRoomRepository(ctrl)
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	users := mocks.NewMockUserStore(ctrl)
	apps := mocks.NewMockAppStore(ctrl)
	cfg := &config.Config{MessageHistoryFloorDays: 90, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10, PinEnabled: true}
	pc, err := readcache.NewPreviewCache(100, time.Minute)
	require.NoError(t, err)
	svc := service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg, service.WithPreviewCache(pc))
	return svc, msgs, rooms
}

func TestHistoryService_RoomsGet_PreviewCacheHitSkipsRead(t *testing.T) {
	svc, msgs, rooms := newRoomsServiceWithPreviewCache(t)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil).Times(1)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt, Sender: models.Participant{Account: "alice"}},
		}, false), nil).Times(1)

	for i := 0; i < 3; i++ {
		resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
		require.NoError(t, err)
		require.Equal(t, "m1", resp.Rooms["r1"].MessageID)
	}
	// Times(1) on both reads asserts the 2nd and 3rd calls were cache hits.
}
```

Add `readcache "github.com/hmchangw/chat/history-service/internal/readcache"` to the test imports.

- [ ] **Step 2: Run the test — confirm it fails**

Run: `make test SERVICE=history-service`
Expected: FAIL — `service.WithPreviewCache` / variadic `New` undefined (compile error).

- [ ] **Step 3: Add the option and field to `service.go`**

In `service.go`, add near the other interface definitions:

```go
// PreviewCache fronts the per-room preview resolve on the rooms.get read path.
// Positives are cached; not-found and errors pass through. *readcache.PreviewCache
// satisfies it.
type PreviewCache interface {
	Get(ctx context.Context, roomID string, load func(context.Context) (models.PreviewMessage, bool, error)) (models.PreviewMessage, bool, error)
}

// Option configures optional HistoryService dependencies.
type Option func(*HistoryService)

// WithPreviewCache installs a room-preview cache used by RoomsGet. Without it,
// previews resolve directly (uncached).
func WithPreviewCache(pc PreviewCache) Option {
	return func(s *HistoryService) { s.previewCache = pc }
}
```

Add the field to the `HistoryService` struct:

```go
	previewCache PreviewCache
```

Change `New` to accept and apply options (append `opts ...Option` to the signature; existing zero-option callers keep compiling):

```go
func New(
	msgs MessageRepository,
	subs SubscriptionRepository,
	rooms RoomRepository,
	pub EventPublisher,
	threadRooms ThreadRoomRepository,
	threadSubs ThreadSubscriptionRepository,
	users UserStore,
	apps AppStore,
	cfg *config.Config,
	opts ...Option,
) *HistoryService {
	s := &HistoryService{
		// ... existing field assignments unchanged ...
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}
```

- [ ] **Step 4: Add `resolvePreview` and use it in `RoomsGet`**

In `rooms.go`, add:

```go
// resolvePreview resolves one room's preview, serving it from the preview cache
// when installed. The cache is positives-only, so empty rooms and read failures
// fall through to a fresh resolve. previewAfterMutation (edit/delete) keeps
// calling roomLastPreviewMessage directly so mutations always see fresh state.
func (s *HistoryService) resolvePreview(ctx context.Context, roomID string, now time.Time) (models.PreviewMessage, bool) {
	if s.previewCache == nil {
		return s.roomLastPreviewMessage(ctx, roomID, now)
	}
	preview, ok, err := s.previewCache.Get(ctx, roomID, func(ctx context.Context) (models.PreviewMessage, bool, error) {
		p, found := s.roomLastPreviewMessage(ctx, roomID, now)
		return p, found, nil
	})
	if err != nil {
		// ctx cancelled while waiting on a shared load — degrade like a read miss.
		return models.PreviewMessage{}, false
	}
	return preview, ok
}
```

In `RoomsGet`, change the goroutine body from `lm, ok := s.roomLastPreviewMessage(c, roomID, now)` to:

```go
			lm, ok := s.resolvePreview(c, roomID, now)
```

- [ ] **Step 5: Run tests — confirm pass**

Run: `make test SERVICE=history-service`
Expected: PASS — the cache-hit test now sees a single read; all other tests (nil cache → direct resolve) unchanged.

- [ ] **Step 6: Add config fields and validation**

In `config/config.go`, after the `RoomCache*` fields:

```go
	// Room-list preview cache (resolved last-eligible message per room).
	// Positives-only; lastMsgAt volatility ⇒ short TTL. Set size or ttl to 0 to disable.
	PreviewCacheSize int           `env:"HISTORY_PREVIEW_CACHE_SIZE" envDefault:"50000"`
	PreviewCacheTTL  time.Duration `env:"HISTORY_PREVIEW_CACHE_TTL"  envDefault:"10s"`
```

In `validate`, add:

```go
	if cfg.PreviewCacheSize < 0 {
		return fmt.Errorf("HISTORY_PREVIEW_CACHE_SIZE must be >= 0, got %d", cfg.PreviewCacheSize)
	}
	if cfg.PreviewCacheTTL < 0 {
		return fmt.Errorf("HISTORY_PREVIEW_CACHE_TTL must be >= 0, got %s", cfg.PreviewCacheTTL)
	}
```

If `config` has a unit test file, add a case asserting a negative `PreviewCacheSize`/`PreviewCacheTTL` fails `validate`, mirroring the existing `RoomCache` validation tests.

- [ ] **Step 7: Wire the cache in `main.go`**

In `cmd/main.go`, just before the `svc := service.New(...)` line (`:173`):

```go
	var opts []service.Option
	if cfg.PreviewCacheSize > 0 && cfg.PreviewCacheTTL > 0 {
		pc, err := readcache.NewPreviewCache(cfg.PreviewCacheSize, cfg.PreviewCacheTTL)
		if err != nil {
			slog.Error("init preview cache failed", "error", err)
			os.Exit(1)
		}
		opts = append(opts, service.WithPreviewCache(pc))
		slog.Info("preview cache enabled", "size", cfg.PreviewCacheSize, "ttl", cfg.PreviewCacheTTL)
	}
```

Change the constructor call to pass `opts...`:

```go
	svc := service.New(cassRepo, subSource, roomSource, pub, threadRoomRepo, threadSubRepo, userStore, appRepo, &cfg, opts...)
```

- [ ] **Step 8: Build, lint, and commit**

Run: `make build SERVICE=history-service && make lint && make test SERVICE=history-service`
Expected: all green.
```bash
git add history-service/internal/service/service.go history-service/internal/service/rooms.go \
        history-service/internal/config/config.go history-service/cmd/main.go \
        history-service/internal/service/rooms_test.go
git commit -m "feat(history-service): route rooms.get preview through optional readcache"
```

---

## Task 4: `rooms.get` batch-size instrumentation

Record the number of distinct rooms per `rooms.get` request as an OpenTelemetry histogram. Combined with the preview cache's free hit/miss counters, this sizes the Phase-2 decision (cache miss rate + batch size → history-service/Cassandra QPS). Walk-depth instrumentation is deferred (it needs cassrepo plumbing — see the design spec's Out-of-scope note).

**Files:**
- Create: `history-service/internal/service/metrics.go`
- Modify: `history-service/internal/service/rooms.go` (record after dedup in `RoomsGet`)

**Interfaces:**
- Consumes: `go.opentelemetry.io/otel`, `go.opentelemetry.io/otel/metric`, `go.opentelemetry.io/otel/metric/noop`.
- Produces: package-level `roomsGetBatchSize metric.Int64Histogram`.

- [ ] **Step 1: Create the histogram instrument**

Create `history-service/internal/service/metrics.go`:

```go
package service

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// roomsGetBatchSize records the number of distinct rooms per rooms.get request.
// Paired with the history_room_preview cache hit/miss counters, it sizes the
// history-service read load that the Phase-2 denormalization would remove.
var roomsGetBatchSize metric.Int64Histogram

func init() {
	h, err := otel.Meter("history-service").Int64Histogram(
		"history_rooms_get_batch_size",
		metric.WithDescription("Distinct rooms per rooms.get request."),
	)
	if err != nil {
		// Fall back to a no-op instrument so recording is always safe even if the
		// global meter provider rejects instrument creation at init time.
		h, _ = noop.NewMeterProvider().Meter("history-service").Int64Histogram("history_rooms_get_batch_size")
	}
	roomsGetBatchSize = h
}
```

- [ ] **Step 2: Record the batch size in `RoomsGet`**

In `rooms.go`, in `RoomsGet`, immediately after `ids := dedupRoomIDs(req.RoomIDs)`:

```go
	roomsGetBatchSize.Record(c, int64(len(ids)))
```

- [ ] **Step 3: Build and verify it compiles**

Run: `make build SERVICE=history-service`
Expected: PASS. (No new unit test — the instrument is a no-op without a meter provider; behavior is unchanged and asserting on OTel exports in a unit test adds no value. Existing `RoomsGet` tests still pass.)

- [ ] **Step 4: Run the full suite and lint**

Run: `make test SERVICE=history-service && make lint`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add history-service/internal/service/metrics.go history-service/internal/service/rooms.go
git commit -m "feat(history-service): record rooms.get batch-size metric"
```

---

## Final verification

- [ ] Run `make test SERVICE=history-service` — all unit tests green under `-race`.
- [ ] Run `make lint` — clean.
- [ ] Run `make test-integration SERVICE=history-service` — the cassrepo/service integration tests (real Cassandra walk) still resolve previews correctly with the new small-page escalation.
- [ ] Confirm coverage on `history-service/internal/service` and `history-service/internal/readcache` ≥ 80% (`go test -coverprofile` via the Makefile).
- [ ] Push the branch and stop — Phase 2 remains gated on the metrics from 1c/1b per the design spec.

## Self-review notes

- **Spec coverage:** 1a → Task 1; 1b → Tasks 2–3; 1c → Task 4 (batch size + free cache hit/miss; walk-depth explicitly deferred). Exit criteria are a runtime/measurement decision, not code.
- **No write-path change** — broadcast-worker, message-worker, user-service untouched; matches the "zero-write Phase 1" constraint.
- **Type consistency:** `models.PreviewMessage` is a type alias of `pkgmodel.PreviewMessage`, so `readcache.PreviewCache.Get` (typed `pkgmodel.PreviewMessage`) satisfies `service.PreviewCache` (typed `models.PreviewMessage`). `cassrepo.PageRequest{PageSize}` and `cassrepo.Page{Data, HasNext}` names match the repository source.
- **Shared function caution:** Task 1 changes `roomLastPreviewMessage`, used by both `RoomsGet` and `previewAfterMutation`; the full suite is run in Task 1 Step 4 and the final verification.
