# Mongo-Outage Survival for Send + History-Load — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep sending messages and loading room history working for a 30–60 minute MongoDB outage by making the subscription-authorization decision survive outside Mongo and fail-fast when Mongo is unreachable.

**Architecture:** Extend the existing message-pipeline caching layer with (1) a shared two-tier subscription-authz cache (`pkg/subauthcache`: existing per-service L1 + a new shared L2 Valkey read-through), (2) a long L2 TTL (~90 min) so an L2 hit — which short-circuits before Mongo — is the outage buffer, and (3) a reusable circuit breaker (`pkg/circuitbreaker`) wrapping the Mongo reads so cold misses fail fast instead of stalling on Mongo's 10s timeout. Only the subscription decision is hard-gated on Mongo; all other Mongo reads on these paths fail-open to safe defaults.

**Tech Stack:** Go 1.25, MongoDB (`mongo-driver/v2`), Valkey (`pkg/valkeyutil`, cluster-mode), NATS/JetStream, `hashicorp/golang-lru/v2/expirable`, `golang.org/x/sync/singleflight`, `go.uber.org/mock`, `stretchr/testify`, `testcontainers-go` via `pkg/testutil`.

## Global Constraints

- Go 1.25; monorepo, single root `go.mod`. Use `make` targets, never raw `go`.
- TDD mandatory (Red-Green-Refactor); ≥80% coverage per package, ≥90% for new `pkg/` packages.
- All tests use `-race` (the Makefile handles it). Unit tests never touch real Mongo/NATS/Valkey; integration tests are `//go:build integration` and use `pkg/testutil` containers.
- Errors: wrap with context via `fmt.Errorf("...: %w", err)`; client-facing errors via `pkg/errcode`; never compare errors by string (`errors.Is`/`errors.As`).
- Logging: `log/slog` JSON only; structured key-value fields; never log tokens/bodies (IDs are fine).
- Config: `caarlos0/env` typed structs, `SCREAMING_SNAKE_CASE`, always `envDefault` for non-secrets.
- No new third-party dependencies (all needed libs already vendored).
- Packages: short lowercase single-word; never `utils`/`helpers`/`common`/`base`.
- Positive-only caching convention (from `2026-05-18` design): never cache "not subscribed" or transient errors.
- Valkey L2 is fail-open: a nil client or any Valkey error degrades to the loader; only the Mongo result governs the returned error (mirror `pkg/roommetacache.ReadThrough`).
- L2 key convention: `{roomID}` hash-tag to colocate in the room's cluster slot (mirror `roommetacache.MetaKey`).
- No `docs/client-api.md` change: request/response schemas and events are unchanged — this is a server-side resilience change only.

---

## File Structure

**New files:**
- `pkg/circuitbreaker/circuitbreaker.go` — breaker state machine (closed/open/half-open).
- `pkg/circuitbreaker/circuitbreaker_test.go` — unit tests.
- `pkg/subauthcache/subauthcache.go` — `SubAuth` projection, `SubKey`, `FetchFromMongo`, `ReadThrough` (L2 + metrics).
- `pkg/subauthcache/subauthcache_test.go` — unit tests.

**Modified files:**
- `message-gatekeeper/store_mongo.go` — `GetSubscription` via `subauthcache.ReadThrough` behind a breaker; `GetRoomMeta` behind the same breaker.
- `message-gatekeeper/main.go` — config knobs + breaker construction; reuse the already-connected Valkey client for the subauth L2.
- `message-gatekeeper/handler.go` — large-room-cap fail-open when `GetRoomMeta` errors.
- `message-gatekeeper/handler_test.go`, `store_integration_test.go` — tests.
- `history-service/internal/readcache/readcache.go` — `SubscriptionCache` loader via `subauthcache.ReadThrough`.
- `history-service/internal/config/config.go` — config knobs.
- `history-service/cmd/main.go` — breaker + subauth L2 wiring.
- `history-service/internal/service/room_times.go` — room-times fail-open to `now`/floor on error.
- `history-service/internal/service/utils.go` — thread the room-times fail-open through `checkAccessAndRoomTimes`.
- history-service tests alongside the above.

---

## Task 1: `pkg/circuitbreaker` — reusable breaker primitive

**Files:**
- Create: `pkg/circuitbreaker/circuitbreaker.go`
- Test: `pkg/circuitbreaker/circuitbreaker_test.go`

**Interfaces:**
- Consumes: nothing (leaf package, stdlib only).
- Produces:
  - `type State int` with `StateClosed`, `StateOpen`, `StateHalfOpen`.
  - `var ErrOpen = errors.New("circuit breaker open")`
  - `func New(threshold int, cooldown time.Duration, opts ...Option) *Breaker`
  - `func WithClock(now func() time.Time) Option`
  - `func (b *Breaker) Do(fn func() error) error`
  - `func (b *Breaker) State() State`

- [ ] **Step 1: Write the failing tests**

```go
package circuitbreaker

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errBoom = errors.New("boom")

func TestBreaker_ClosedPassesThroughAndResetsOnSuccess(t *testing.T) {
	b := New(3, time.Second)
	assert.Equal(t, StateClosed, b.State())
	// two failures below threshold stay closed
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	assert.Equal(t, StateClosed, b.State())
	// a success resets the failure count
	require.NoError(t, b.Do(func() error { return nil }))
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	assert.Equal(t, StateClosed, b.State(), "success should have reset the counter")
}

func TestBreaker_OpensAfterThresholdAndFastFails(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(3, time.Minute, WithClock(func() time.Time { return now }))
	for i := 0; i < 3; i++ {
		require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	}
	assert.Equal(t, StateOpen, b.State())
	// While open, fn is NOT invoked and ErrOpen is returned immediately.
	called := false
	err := b.Do(func() error { called = true; return nil })
	require.ErrorIs(t, err, ErrOpen)
	assert.False(t, called, "fn must not run while open")
}

func TestBreaker_HalfOpenProbeSuccessCloses(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(1, time.Minute, WithClock(func() time.Time { return now }))
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	require.Equal(t, StateOpen, b.State())
	// advance past cooldown -> next Do is the half-open probe
	now = now.Add(2 * time.Minute)
	require.NoError(t, b.Do(func() error { return nil }))
	assert.Equal(t, StateClosed, b.State())
}

func TestBreaker_HalfOpenProbeFailureReopens(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(1, time.Minute, WithClock(func() time.Time { return now }))
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	now = now.Add(2 * time.Minute)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	assert.Equal(t, StateOpen, b.State(), "failed probe must reopen")
	// still open immediately after (cooldown restarts)
	require.ErrorIs(t, b.Do(func() error { return nil }), ErrOpen)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=circuitbreaker` (or `go test ./pkg/circuitbreaker/...` if the Makefile keys services by directory — use `make test` and grep).
Expected: FAIL — `undefined: New`, `undefined: StateClosed`, etc.

- [ ] **Step 3: Write the implementation**

```go
// Package circuitbreaker is a small, dependency-free circuit breaker used to
// fail-fast around a flaky downstream (e.g. MongoDB) instead of stalling every
// caller on the downstream's own timeout. It has three states: closed (calls
// pass through), open (calls fast-fail with ErrOpen), and half-open (a single
// probe call is allowed to test recovery).
package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

// ErrOpen is returned by Do when the breaker is open and the wrapped function
// is therefore not invoked.
var ErrOpen = errors.New("circuit breaker open")

// State is the breaker's current state.
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

// Breaker is a concurrency-safe circuit breaker. The zero value is not usable;
// construct with New.
type Breaker struct {
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time
	probing  bool // true while a half-open probe is in flight
}

// Option configures a Breaker at construction.
type Option func(*Breaker)

// WithClock overrides the time source (for tests).
func WithClock(now func() time.Time) Option {
	return func(b *Breaker) { b.now = now }
}

// New builds a breaker that opens after threshold consecutive failures and
// stays open for cooldown before allowing a half-open probe.
func New(threshold int, cooldown time.Duration, opts ...Option) *Breaker {
	b := &Breaker{threshold: threshold, cooldown: cooldown, now: time.Now, state: StateClosed}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// State returns the breaker's current state, advancing open->half-open if the
// cooldown has elapsed.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeHalfOpenLocked()
	return b.state
}

// Do runs fn unless the breaker is open. When open (and past cooldown, unless a
// probe is already in flight) it returns ErrOpen without invoking fn. The
// result of fn updates the breaker: success closes it, failure increments the
// counter and may (re)open it.
func (b *Breaker) Do(fn func() error) error {
	b.mu.Lock()
	b.maybeHalfOpenLocked()
	switch b.state {
	case StateOpen:
		b.mu.Unlock()
		return ErrOpen
	case StateHalfOpen:
		if b.probing {
			b.mu.Unlock()
			return ErrOpen
		}
		b.probing = true
	}
	b.mu.Unlock()

	err := fn()

	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
	if err != nil {
		b.failures++
		if b.state == StateHalfOpen || b.failures >= b.threshold {
			b.state = StateOpen
			b.openedAt = b.now()
		}
		return err
	}
	b.failures = 0
	b.state = StateClosed
	return nil
}

// maybeHalfOpenLocked transitions open->half-open once the cooldown elapses.
// Caller must hold b.mu.
func (b *Breaker) maybeHalfOpenLocked() {
	if b.state == StateOpen && b.now().Sub(b.openedAt) >= b.cooldown {
		b.state = StateHalfOpen
		b.probing = false
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test` and confirm the `pkg/circuitbreaker` tests pass.
Expected: PASS (all four tests).

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: no findings in `pkg/circuitbreaker`.

- [ ] **Step 6: Commit**

```bash
git add pkg/circuitbreaker/
git commit -m "Add circuitbreaker package for fail-fast around flaky downstreams"
```

---

## Task 2: `pkg/subauthcache` — shared two-tier subscription-authz L2

**Files:**
- Create: `pkg/subauthcache/subauthcache.go`
- Test: `pkg/subauthcache/subauthcache_test.go`

**Interfaces:**
- Consumes: `pkg/valkeyutil` (`Client`, `GetJSON`, `SetJSONWithTTL`, `ErrCacheMiss`), `pkg/cachemetrics`, `pkg/model`, `mongo-driver/v2`.
- Produces:
  - `type SubAuth struct { ID, Account string; Roles []model.Role; HistorySharedSince *int64 }`
  - `func SubKey(roomID, account string) string`
  - `type Recorder interface { Hit(context.Context); Miss(context.Context); Error(context.Context) }`
  - `type Loader func(ctx context.Context, roomID, account string) (SubAuth, bool, error)` — returns `(auth, subscribed, err)`; `subscribed=false` means a confirmed non-subscriber.
  - `func FetchFromMongo(ctx context.Context, subscriptions *mongo.Collection, roomID, account string) (SubAuth, bool, error)`
  - `func ReadThrough(ctx context.Context, client valkeyutil.Client, loader Loader, roomID, account string, ttl time.Duration, rec Recorder) (SubAuth, bool, error)`

**Interface note:** `ReadThrough`'s `loader` is where the caller injects the circuit breaker — the caller passes a closure that runs `FetchFromMongo` inside `breaker.Do(...)`. Keeping the breaker out of this package preserves the clean boundary (this package knows caching, not breaker policy).

- [ ] **Step 1: Write the failing tests**

```go
package subauthcache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// fakeValkey is an in-memory valkeyutil.Client for tests.
type fakeValkey struct {
	store   map[string]string
	getErr  error
	setErr  error
	getHits int
	setHits int
}

func newFakeValkey() *fakeValkey { return &fakeValkey{store: map[string]string{}} }

func (f *fakeValkey) Get(_ context.Context, key string) (string, error) {
	f.getHits++
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.store[key]
	if !ok {
		return "", valkeyutil.ErrCacheMiss
	}
	return v, nil
}
func (f *fakeValkey) Set(_ context.Context, key, value string, _ time.Duration) error {
	f.setHits++
	if f.setErr != nil {
		return f.setErr
	}
	f.store[key] = value
	return nil
}
func (f *fakeValkey) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	return false, nil
}
func (f *fakeValkey) IncrEx(context.Context, string, time.Duration) (int64, error) { return 0, nil }
func (f *fakeValkey) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		delete(f.store, k)
	}
	return nil
}
func (f *fakeValkey) Close() error { return nil }

// spyRecorder counts hit/miss/error.
type spyRecorder struct{ hit, miss, err int }

func (s *spyRecorder) Hit(context.Context)   { s.hit++ }
func (s *spyRecorder) Miss(context.Context)  { s.miss++ }
func (s *spyRecorder) Error(context.Context) { s.err++ }

func TestSubKey(t *testing.T) {
	assert.Equal(t, "sub:{room1}:alice", SubKey("room1", "alice"))
}

func TestReadThrough_L2Hit_SkipsLoader(t *testing.T) {
	fv := newFakeValkey()
	rec := &spyRecorder{}
	// pre-populate L2
	seed := SubAuth{ID: "u1", Account: "alice", Roles: []model.Role{model.RoleOwner}}
	_, _, err := ReadThrough(context.Background(), fv,
		func(context.Context, string, string) (SubAuth, bool, error) { return seed, true, nil },
		"room1", "alice", time.Hour, rec)
	require.NoError(t, err)
	// second call: loader must NOT be invoked
	got, subscribed, err := ReadThrough(context.Background(), fv,
		func(context.Context, string, string) (SubAuth, bool, error) {
			t.Fatal("loader must not run on L2 hit")
			return SubAuth{}, false, nil
		}, "room1", "alice", time.Hour, rec)
	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, []model.Role{model.RoleOwner}, got.Roles)
	assert.GreaterOrEqual(t, rec.hit, 1)
}

func TestReadThrough_L2Miss_LoadsAndPopulates(t *testing.T) {
	fv := newFakeValkey()
	rec := &spyRecorder{}
	loads := 0
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		loads++
		return SubAuth{ID: "u1", Account: "alice"}, true, nil
	}
	_, subscribed, err := ReadThrough(context.Background(), fv, loader, "room1", "alice", time.Hour, rec)
	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, 1, loads)
	assert.Equal(t, 1, fv.setHits, "subscribed result must populate L2")
	assert.Equal(t, 1, rec.miss)
}

func TestReadThrough_NotSubscribed_NotCached(t *testing.T) {
	fv := newFakeValkey()
	rec := &spyRecorder{}
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{}, false, nil // confirmed non-subscriber
	}
	_, subscribed, err := ReadThrough(context.Background(), fv, loader, "room1", "bob", time.Hour, rec)
	require.NoError(t, err)
	assert.False(t, subscribed)
	assert.Equal(t, 0, fv.setHits, "negative result must not be cached")
}

func TestReadThrough_LoaderError_Propagates_NoCache(t *testing.T) {
	fv := newFakeValkey()
	rec := &spyRecorder{}
	sentinel := errors.New("mongo down / breaker open")
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{}, false, sentinel
	}
	_, _, err := ReadThrough(context.Background(), fv, loader, "room1", "alice", time.Hour, rec)
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 0, fv.setHits)
}

func TestReadThrough_NilClient_FailsOpenToLoader(t *testing.T) {
	rec := &spyRecorder{}
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{ID: "u1"}, true, nil
	}
	got, subscribed, err := ReadThrough(context.Background(), nil, loader, "room1", "alice", time.Hour, rec)
	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
}

func TestReadThrough_ValkeyGetError_FailsOpenToLoader(t *testing.T) {
	fv := newFakeValkey()
	fv.getErr = errors.New("valkey unreachable")
	rec := &spyRecorder{}
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{ID: "u1"}, true, nil
	}
	got, subscribed, err := ReadThrough(context.Background(), fv, loader, "room1", "alice", time.Hour, rec)
	require.NoError(t, err, "a Valkey error must degrade to the loader, not fail the call")
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, 1, rec.err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test`
Expected: FAIL — `undefined: ReadThrough`, `undefined: SubKey`, `undefined: SubAuth`.

- [ ] **Step 3: Write the implementation**

```go
// Package subauthcache is the shared L2 (Valkey) tier for the subscription
// authorization decision read on the hot path of message-gatekeeper (send) and
// history-service (load history). Both services front it with their own
// process-local L1 cache; sharing the L2 means a user active in either journey
// warms the other.
//
// It stores a single superset projection (SubAuth) so one L2 entry serves both
// consumers: gatekeeper reads ID+Roles, history reads HistorySharedSince.
// Positive-only: only confirmed subscribers are cached; "not subscribed" and
// loader errors are never stored. Fail-open: a nil client or any Valkey error
// degrades to the loader — only the loader's result governs the returned error.
package subauthcache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// SubAuth is the shared L2 projection of a subscription. Its presence in L2
// means "subscribed"; absence is never cached. json tags pin the wire format.
type SubAuth struct {
	ID                 string       `json:"id"`
	Account            string       `json:"account"`
	Roles              []model.Role `json:"roles,omitempty"`
	HistorySharedSince *int64       `json:"historySharedSince,omitempty"` // epoch millis; nil = full access
}

// Recorder records L2 hit/miss/error outcomes. cachemetrics.Recorder satisfies it.
type Recorder interface {
	Hit(ctx context.Context)
	Miss(ctx context.Context)
	Error(ctx context.Context)
}

// Loader fetches a fresh SubAuth from the source of truth. It returns
// (auth, subscribed, err): subscribed=false is a confirmed non-subscriber (not
// an error). The caller injects the circuit breaker by wrapping FetchFromMongo
// in this closure.
type Loader func(ctx context.Context, roomID, account string) (SubAuth, bool, error)

// SubKey is the L2 key for a (roomID, account) subscription. The {roomID}
// hash-tag colocates it in the room's cluster slot, matching house convention.
func SubKey(roomID, account string) string {
	return "sub:{" + roomID + "}:" + account
}

// FetchFromMongo reads the subscription projection both services need. Returns
// (zero, false, nil) when the user is not subscribed (Mongo ErrNoDocuments).
func FetchFromMongo(ctx context.Context, subscriptions *mongo.Collection, roomID, account string) (SubAuth, bool, error) {
	var sub model.Subscription
	filter := bson.M{"u.account": account, "roomId": roomID}
	proj := options.FindOne().SetProjection(bson.M{"u._id": 1, "u.account": 1, "roles": 1, "historySharedSince": 1})
	if err := subscriptions.FindOne(ctx, filter, proj).Decode(&sub); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return SubAuth{}, false, nil
		}
		return SubAuth{}, false, fmt.Errorf("find subscription for %s in %s: %w", account, roomID, err)
	}
	return fromSubscription(&sub), true, nil
}

func fromSubscription(sub *model.Subscription) SubAuth {
	a := SubAuth{
		ID:      sub.User.ID,
		Account: sub.User.Account,
		Roles:   sub.Roles,
	}
	if sub.HistorySharedSince != nil {
		ms := sub.HistorySharedSince.UTC().UnixMilli()
		a.HistorySharedSince = &ms
	}
	return a
}

// ReadThrough resolves a SubAuth through the L2 (Valkey) tier: GET on the cache
// key, and on miss (or any L2 error) fall back to loader and repopulate L2 with
// ttl when the loader reports subscribed=true. Fail-open — a nil client or any
// Valkey error degrades to loader; only loader's result governs the error.
func ReadThrough(ctx context.Context, client valkeyutil.Client, loader Loader, roomID, account string, ttl time.Duration, rec Recorder) (SubAuth, bool, error) {
	if client != nil {
		if auth, found := readL2(ctx, client, roomID, account, rec); found {
			return auth, true, nil
		}
	}
	auth, subscribed, err := loader(ctx, roomID, account)
	if err != nil {
		return SubAuth{}, false, err
	}
	if subscribed && client != nil {
		if err := valkeyutil.SetJSONWithTTL(ctx, client, SubKey(roomID, account), auth, ttl); err != nil {
			slog.WarnContext(ctx, "subauth L2 populate failed (TTL will reconcile)",
				"room_id", roomID, "error", err)
		}
	}
	return auth, subscribed, nil
}

// readL2 attempts the L2 read; records the outcome. Returns found=true only on
// a hit. A clean miss records Miss; any other error records Error and returns
// found=false so the caller falls through to the loader.
func readL2(ctx context.Context, client valkeyutil.Client, roomID, account string, rec Recorder) (SubAuth, bool) {
	var cached SubAuth
	err := valkeyutil.GetJSON(ctx, client, SubKey(roomID, account), &cached)
	if err == nil {
		rec.Hit(ctx)
		return cached, true
	}
	if errors.Is(err, valkeyutil.ErrCacheMiss) {
		rec.Miss(ctx)
		return SubAuth{}, false
	}
	rec.Error(ctx)
	slog.WarnContext(ctx, "subauth L2 read failed, falling back to loader",
		"room_id", roomID, "error", err)
	return SubAuth{}, false
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test`
Expected: PASS (all `subauthcache` tests).

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: no findings.

- [ ] **Step 6: Commit**

```bash
git add pkg/subauthcache/
git commit -m "Add subauthcache shared L2 subscription-authz cache"
```

---

## Task 3: Wire subauthcache + breaker into message-gatekeeper (send path)

**Files:**
- Modify: `message-gatekeeper/store_mongo.go`
- Modify: `message-gatekeeper/main.go:40-48` (config) and `:117-137` (wiring)
- Modify: `message-gatekeeper/handler.go:315-330` (large-room fail-open)
- Test: `message-gatekeeper/handler_test.go`, `message-gatekeeper/store_integration_test.go`

**Interfaces:**
- Consumes: `subauthcache.{SubAuth, SubKey, FetchFromMongo, ReadThrough, Loader}`, `circuitbreaker.{New, Breaker, ErrOpen}`, existing `roommetacache.ReadThrough`.
- Produces: unchanged `Store` interface (`GetSubscription`, `GetRoomMeta`); `MongoStore` now holds a `*circuitbreaker.Breaker` and a `subAuthTTL time.Duration` and a `subRec subauthcache.Recorder`.

- [ ] **Step 1: Write the failing handler test (large-room fail-open)**

Add to `message-gatekeeper/handler_test.go`. This asserts that when `GetRoomMeta` returns an error (Mongo down + breaker open), a non-thread, non-bypass send is **allowed** (fail-open) rather than Nak'd.

```go
func TestProcessMessage_RoomMetaError_FailsOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	// Subscriber with no bypass role -> large-room cap would normally apply.
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "room1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}}, nil)
	// Mongo down: room-meta lookup errors.
	store.EXPECT().GetRoomMeta(gomock.Any(), "room1").
		Return(roommetacache.Meta{}, errors.New("mongo unavailable"))

	var published []*nats.Msg
	pub := func(_ context.Context, m *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
		published = append(published, m)
		return &jetstream.PubAck{}, nil
	}
	h := NewHandler(store, nil, pub, noopReply, "site1", stubParentFetcher{}, 100, 10, 1<<20, "https://chat.example")

	req := &model.SendMessageRequest{
		ID: validMsgID(t), RequestID: validReqID(t), Content: "hi",
	}
	_, err := h.processMessage(context.Background(), "alice", "room1", "site1", req)
	require.NoError(t, err, "room-meta error must fail-open, not block the send")
	require.Len(t, published, 1, "message should still be published to canonical")
}
```

(Reuse the file's existing helpers: `noopReply`, `stubParentFetcher`, `validMsgID`, `validReqID` — if a helper name differs, match the existing one in `handler_test.go`.)

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=message-gatekeeper`
Expected: FAIL — current code returns a wrapped infra error from `GetRoomMeta`, so `processMessage` returns non-nil and publishes nothing.

- [ ] **Step 3: Implement large-room fail-open in handler.go**

Replace the error branch in `processMessage`'s large-room block (`message-gatekeeper/handler.go`, currently lines ~316-319):

```go
	if !isThreadReply && !bypass {
		meta, err := h.store.GetRoomMeta(ctx, roomID)
		if err != nil {
			// Fail-open: during a Mongo outage the room-meta read is
			// unavailable. The large-room cap is a spam/noise control, not an
			// access control, so allow the post rather than block the send.
			slog.WarnContext(ctx, "room-meta unavailable, bypassing large-room cap (fail-open)",
				"request_id", req.RequestID, "room_id", roomID, "error", err)
		} else if meta.UserCount > h.largeRoomThreshold {
			slog.Info("send blocked",
				"reason", string(errcode.MessageLargeRoomPostRestricted),
				"account", account, "room_id", roomID,
				"userCount", meta.UserCount, "threshold", h.largeRoomThreshold,
			)
			return nil, errLargeRoomPostRestricted
		}
	}
```

- [ ] **Step 4: Run to verify the handler test passes**

Run: `make test SERVICE=message-gatekeeper`
Expected: PASS for `TestProcessMessage_RoomMetaError_FailsOpen`.

- [ ] **Step 5: Rewire MongoStore to use the breaker + subauthcache**

Edit `message-gatekeeper/store_mongo.go`:

```go
type MongoStore struct {
	subscriptions *mongo.Collection
	rooms         *mongo.Collection
	valkey        valkeyutil.Client // nil disables the L2 tier (pure Mongo)
	metaTTL       time.Duration
	subTTL        time.Duration
	breaker       *circuitbreaker.Breaker
	metaRec       roommetacache.Recorder
	subRec        subauthcache.Recorder
}

func NewMongoStore(db *mongo.Database, valkey valkeyutil.Client, metaTTL, subTTL time.Duration, breaker *circuitbreaker.Breaker) *MongoStore {
	return &MongoStore{
		subscriptions: db.Collection("subscriptions"),
		rooms:         db.Collection("rooms"),
		valkey:        valkey,
		metaTTL:       metaTTL,
		subTTL:        subTTL,
		breaker:       breaker,
		metaRec:       cachemetrics.For("roommeta", "l2"),
		subRec:        cachemetrics.For("subauth", "l2"),
	}
}

func (s *MongoStore) GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error) {
	loader := func(ctx context.Context, roomID, account string) (subauthcache.SubAuth, bool, error) {
		var (
			auth       subauthcache.SubAuth
			subscribed bool
		)
		err := s.breaker.Do(func() error {
			var e error
			auth, subscribed, e = subauthcache.FetchFromMongo(ctx, s.subscriptions, roomID, account)
			return e
		})
		return auth, subscribed, err
	}
	auth, subscribed, err := subauthcache.ReadThrough(ctx, s.valkey, loader, roomID, account, s.subTTL, s.subRec)
	if err != nil {
		return nil, fmt.Errorf("get subscription for %s in %s: %w", account, roomID, err)
	}
	if !subscribed {
		return nil, fmt.Errorf("user %s not subscribed to room %s: %w", account, roomID, errNotSubscribed)
	}
	return &model.Subscription{
		User:  model.SubscriptionUser{ID: auth.ID, Account: auth.Account},
		Roles: auth.Roles,
	}, nil
}

func (s *MongoStore) GetRoomMeta(ctx context.Context, roomID string) (roommetacache.Meta, error) {
	var meta roommetacache.Meta
	err := s.breaker.Do(func() error {
		var e error
		meta, e = roommetacache.ReadThrough(ctx, s.valkey, s.rooms, roomID, s.metaTTL, s.metaRec)
		return e
	})
	if err != nil {
		return roommetacache.Meta{}, fmt.Errorf("get room meta for %s: %w", roomID, err)
	}
	return meta, nil
}
```

Add the imports `"github.com/hmchangw/chat/pkg/circuitbreaker"` and `"github.com/hmchangw/chat/pkg/subauthcache"`.

**Note on `errNotSubscribed`:** the existing `subcache.go` L1 wrapper matches `errors.Is(err, errNotSubscribed)` and does not cache it — preserved because `GetSubscription` still wraps `errNotSubscribed` with `%w`. The breaker's `ErrOpen` surfaces as a non-`errNotSubscribed` error → the L1 wrapper treats it as a transient error (not cached), and the handler Naks (send retried when Mongo returns). A cold user during the outage is thus denied with a retryable infra error, which is the accepted behavior.

- [ ] **Step 6: Add config knobs + wiring in main.go**

In the `Config` struct (`message-gatekeeper/main.go:40-48` area) add:

```go
	SubL2TTL           time.Duration           `env:"GATEKEEPER_SUB_L2_TTL"        envDefault:"90m"`
	MongoBreakerFails  int                     `env:"GATEKEEPER_MONGO_BREAKER_FAILS"    envDefault:"5"`
	MongoBreakerCool   time.Duration           `env:"GATEKEEPER_MONGO_BREAKER_COOLDOWN" envDefault:"10s"`
```

In the wiring block, replace the `NewMongoStore` construction (currently `message-gatekeeper/main.go:117`):

```go
	breaker := circuitbreaker.New(cfg.MongoBreakerFails, cfg.MongoBreakerCool)
	mongoStore := NewMongoStore(db, metaValkey, cfg.RoomMetaL2TTL, cfg.SubL2TTL, breaker)
```

The Valkey client `metaValkey` (already connected for room-meta L2) is reused for the subauth L2 — no second connection. Extend the `slog.Info("gatekeeper caches enabled", ...)` line to include `"sub_l2_ttl", cfg.SubL2TTL`. Add the `circuitbreaker` import.

- [ ] **Step 7: Update existing MongoStore construction sites**

`NewMongoStore`'s signature changed. Search for other callers and update them:

Run: `grep -rn "NewMongoStore(" message-gatekeeper/`
For each test caller, pass a breaker and TTL, e.g. `NewMongoStore(db, nil, 15*time.Minute, 90*time.Minute, circuitbreaker.New(5, 10*time.Second))`.

- [ ] **Step 8: Add integration test — send survives Mongo down**

Add to `message-gatekeeper/store_integration_test.go` (build tag `//go:build integration`). Uses the shared Valkey cluster + a Mongo container; warms a room, stops Mongo, asserts a warm send resolves the subscription from L2 and a cold room errors.

```go
//go:build integration

func TestMongoStore_SubscriptionSurvivesMongoOutage(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "gk_outage")
	valkey := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
	t.Cleanup(func() { testutil.FlushValkey(t) })

	// Seed a subscription in Mongo.
	_, err := db.Collection("subscriptions").InsertOne(ctx, bson.M{
		"_id": "s1", "roomId": "room1", "roles": []string{},
		"u": bson.M{"_id": "u1", "account": "alice"},
	})
	require.NoError(t, err)

	breaker := circuitbreaker.New(1, 50*time.Millisecond)
	store := NewMongoStore(db, valkey, 15*time.Minute, 90*time.Minute, breaker)

	// Warm the L2 while Mongo is up.
	sub, err := store.GetSubscription(ctx, "alice", "room1")
	require.NoError(t, err)
	require.Equal(t, "u1", sub.User.ID)

	// Simulate Mongo down: point the store at a dead collection by closing the client.
	// (Use testutil's Mongo stop helper if available; otherwise a canceled deadline
	// on a fresh store whose Mongo is terminated. See testutil.MongoDB docs.)
	testutil.StopMongo(t) // terminates the shared Mongo container for this test's DB

	// Warm room still resolves from L2 (no Mongo hit).
	got, err := store.GetSubscription(ctx, "alice", "room1")
	require.NoError(t, err, "warm subscription must survive the outage via L2")
	require.Equal(t, "u1", got.ID)

	// Cold room: not in L2, Mongo down -> error (denied). Trip + fast-fail via breaker.
	_, err = store.GetSubscription(ctx, "bob", "coldroom")
	require.Error(t, err)
}
```

**Note:** if `pkg/testutil` has no `StopMongo` helper, this step's outage simulation instead uses a `MongoStore` constructed against a Mongo client with a 1ms operation timeout after warming (so the L2-miss loader times out and trips the breaker). Pick whichever the testutil surface supports; the assertions (warm survives, cold denied) are unchanged. Check `pkg/testutil` first: `grep -rn "func Stop\|func Terminate" pkg/testutil/mongo*.go`.

- [ ] **Step 9: Run unit + integration tests**

Run: `make test SERVICE=message-gatekeeper`
Run: `make test-integration SERVICE=message-gatekeeper`
Expected: PASS.

- [ ] **Step 10: Lint + commit**

```bash
make lint
git add message-gatekeeper/
git commit -m "Wire subauthcache + circuit breaker into message-gatekeeper for Mongo-outage survival"
```

---

## Task 4: Wire subauthcache + breaker into history-service (load-history path)

**Files:**
- Modify: `history-service/internal/readcache/readcache.go` (SubscriptionCache loader)
- Modify: `history-service/internal/config/config.go` (config knobs)
- Modify: `history-service/cmd/main.go` (breaker + subauth L2 wiring)
- Modify: `history-service/internal/service/room_times.go` and `internal/service/utils.go` (room-times fail-open)
- Test: readcache, service, and integration tests alongside.

**Interfaces:**
- Consumes: `subauthcache.{ReadThrough, FetchFromMongo, SubAuth, Loader}`, `circuitbreaker.{New, Breaker}`.
- Produces: `SubscriptionCache` unchanged public API (`GetHistorySharedSince`, `GetSubscription`); its L1 loader now delegates through the shared L2. New `checkAccessAndRoomTimes` behavior: room-times error → `now`/floor fallback instead of propagating.

- [ ] **Step 1: Write the failing service test (room-times fail-open)**

Add to `history-service/internal/service/room_times_test.go` (or `messages_test.go` if that's where the harness lives). Assert that when `GetRoomTimes` errors, `LoadHistory` still succeeds using `now` as ceiling and the configured history floor (i.e. it reads from Cassandra and returns messages rather than erroring).

```go
func TestLoadHistory_RoomTimesError_FailsOpenToNowFloor(t *testing.T) {
	// subscription access OK (subscribed, full access)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "alice", "room1").Return(nil, true, nil)
	// room-times unavailable (Mongo down)
	rooms := mocks.NewMockRoomRepository(ctrl)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "room1").Return(time.Time{}, time.Time{}, errors.New("mongo down")).AnyTimes()
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), "room1").Return(nil, errors.New("mongo down")).AnyTimes()
	// Cassandra read returns a page.
	reader := mocks.NewMockMessageReader(ctrl)
	reader.EXPECT().GetMessagesBefore(gomock.Any(), "room1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(cassrepo.Page[models.Message]{Data: []models.Message{{ID: "m1"}}}, nil)

	svc := newTestService(t, withSubs(subs), withRooms(rooms), withReader(reader))
	resp, err := svc.LoadHistory(newCtx("alice", "room1"), models.LoadHistoryRequest{})
	require.NoError(t, err, "room-times error must fail-open, not block the read")
	require.Len(t, resp.Messages, 1)
}
```

(Match the file's existing test-construction helpers — `newTestService`, `newCtx`, the `with*` option funcs, and mock names. If the harness differs, mirror the pattern used by the nearest existing `LoadHistory` test.)

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=history-service`
Expected: FAIL — `resolveRoomTimesOrError` currently returns the wrapped error, so `checkAccessAndRoomTimes` returns it and `LoadHistory` errors.

- [ ] **Step 3: Implement room-times fail-open**

In `history-service/internal/service/room_times.go`, change `resolveRoomTimesOrError` so a non-`ErrNoDocuments` error degrades to zero times (the callers already clamp a zero `createdAt` to the history floor and use `now` as the ceiling):

```go
func (s *HistoryService) resolveRoomTimesOrError(
	ctx context.Context,
	roomID string,
	meta *models.RoomMeta,
	now time.Time,
) (lastMsgAt, createdAt time.Time, err error) {
	lastMsgAt, createdAt, err = s.resolveRoomTimes(ctx, roomID, meta, now)
	if err == nil {
		return lastMsgAt, createdAt, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return time.Time{}, time.Time{}, errcode.NotFound("room not found")
	}
	// Fail-open: a transient room-times failure (Mongo outage) must not block a
	// read. Zero times make the walk use now as the ceiling and the configured
	// history floor as the floor — a wider but correct bucket walk.
	slog.WarnContext(ctx, "room-times unavailable, falling back to now/floor (fail-open)",
		"room_id", roomID, "error", err)
	return time.Time{}, time.Time{}, nil
}
```

Add `"log/slog"` to the imports if not present. **Verify** the two bound-computing sites tolerate zero times:
- `LoadHistory` (`messages.go:72-80`): already clamps `walkFloor` when `createdAt.IsZero()` and caps `before` only when `lastMsgAt` is non-zero — OK.
- `walkBounds` (`room_times.go:42`): confirm it treats zero `lastMsgAt` as "use now" and zero `createdAt` as "use floor". If it does not, add that clamp. Read it before editing.

- [ ] **Step 4: Run to verify the service test passes**

Run: `make test SERVICE=history-service`
Expected: PASS for `TestLoadHistory_RoomTimesError_FailsOpenToNowFloor`.

- [ ] **Step 5: Rewire the readcache SubscriptionCache loader through the shared L2**

The cleanest injection point: give `SubscriptionCache` an optional L2 loader. Change `NewSubscriptionCache` to accept the shared-L2 read-through function, and have the L1 `getOrLoad` loader call it. Edit `history-service/internal/readcache/readcache.go`:

```go
// SubAuthReadThrough is the shared-L2 subscription read the L1 cache fronts.
// history-service injects a closure that runs subauthcache.ReadThrough behind
// the circuit breaker. account/roomID order matches SubscriptionSource.
type SubAuthReadThrough func(ctx context.Context, account, roomID string) (sharedSince *time.Time, subscribed bool, err error)

// SubscriptionCache caches positive subscription access checks (L1), backed by
// a shared L2 read-through when configured.
type SubscriptionCache struct {
	inner   SubscriptionSource
	l2      SubAuthReadThrough // nil => L1 fronts inner directly (no shared L2)
	cache   *ttlCache[subEntry]
}

// NewSubscriptionCache wraps inner with an LRU+TTL cache. When l2 is non-nil,
// L1 misses resolve through the shared L2 read-through instead of inner.
func NewSubscriptionCache(inner SubscriptionSource, l2 SubAuthReadThrough, size int, ttl time.Duration) (*SubscriptionCache, error) {
	cache, err := newTTLCache[subEntry](size, ttl, cachemetrics.For("history_sub", "l1"))
	if err != nil {
		return nil, err
	}
	return &SubscriptionCache{inner: inner, l2: l2, cache: cache}, nil
}

func (c *SubscriptionCache) GetHistorySharedSince(ctx context.Context, account, roomID string) (*time.Time, bool, error) {
	key := account + "\x00" + roomID
	entry, err := c.cache.getOrLoad(ctx, key, func(ctx context.Context) (subEntry, bool, error) {
		var (
			ss         *time.Time
			subscribed bool
			err        error
		)
		if c.l2 != nil {
			ss, subscribed, err = c.l2(ctx, account, roomID)
		} else {
			ss, subscribed, err = c.inner.GetHistorySharedSince(ctx, account, roomID)
		}
		if err != nil {
			return subEntry{}, false, err
		}
		return subEntry{sharedSince: ss, subscribed: subscribed}, subscribed, nil
	})
	if err != nil {
		return nil, false, err
	}
	return entry.sharedSince, entry.subscribed, nil
}
```

`GetSubscription` (used by pin/unpin) stays delegating to `inner` unchanged.

- [ ] **Step 6: Add a readcache unit test for the L2 delegation**

```go
func TestSubscriptionCache_UsesL2WhenProvided(t *testing.T) {
	calls := 0
	l2 := func(_ context.Context, account, roomID string) (*time.Time, bool, error) {
		calls++
		return nil, true, nil
	}
	// inner must NOT be called when l2 is set.
	inner := stubSubSource{err: errors.New("inner must not be called")}
	c, err := NewSubscriptionCache(inner, l2, 100, time.Minute)
	require.NoError(t, err)
	_, subscribed, err := c.GetHistorySharedSince(context.Background(), "alice", "room1")
	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, 1, calls)
	// second call is an L1 hit -> l2 not called again
	_, _, err = c.GetHistorySharedSince(context.Background(), "alice", "room1")
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}
```

(Define `stubSubSource` locally satisfying `SubscriptionSource` if one does not already exist in the test file.)

- [ ] **Step 7: Add config knobs**

In `history-service/internal/config/config.go` (near the existing `SubCacheSize`/`SubCacheTTL`):

```go
	// SubL2TTL is the shared Valkey L2 retention for subscription authz — the
	// outage buffer. Long by design (default 90m) so an L2 hit carries the
	// access decision through a Mongo outage. 0 disables the L2 tier.
	SubL2TTL time.Duration `env:"HISTORY_SUB_L2_TTL" envDefault:"90m"`

	MongoBreakerFails    int           `env:"HISTORY_MONGO_BREAKER_FAILS"    envDefault:"5"`
	MongoBreakerCooldown time.Duration `env:"HISTORY_MONGO_BREAKER_COOLDOWN" envDefault:"10s"`
```

Add validation in `validate()` mirroring the existing `SubCache*` guards (reject negative values).

- [ ] **Step 8: Wire the breaker + L2 in main.go**

In `history-service/cmd/main.go`, where the subscription Mongo repo and readcache are constructed: build the breaker, build the shared-L2 closure, and pass it to `NewSubscriptionCache`. The Valkey client for history-service: reuse the existing Valkey client if one is already connected (grep for `valkeyutil.ConnectCluster` in `cmd/main.go`); if history-service has none yet, connect one guarded by `len(cfg.ValkeyAddrs) > 0`, mirroring gatekeeper's block, and `nil` disables L2.

```go
	breaker := circuitbreaker.New(cfg.MongoBreakerFails, cfg.MongoBreakerCooldown)
	subRec := cachemetrics.For("subauth", "l2")
	subL2 := func(ctx context.Context, account, roomID string) (*time.Time, bool, error) {
		loader := func(ctx context.Context, roomID, account string) (subauthcache.SubAuth, bool, error) {
			var (
				auth subauthcache.SubAuth
				sub  bool
			)
			err := breaker.Do(func() error {
				var e error
				auth, sub, e = subauthcache.FetchFromMongo(ctx, subscriptionsColl, roomID, account)
				return e
			})
			return auth, sub, err
		}
		auth, subscribed, err := subauthcache.ReadThrough(ctx, valkeyClient, loader, roomID, account, cfg.SubL2TTL, subRec)
		if err != nil {
			return nil, false, err
		}
		if !subscribed {
			return nil, false, nil
		}
		var ss *time.Time
		if auth.HistorySharedSince != nil {
			t := time.UnixMilli(*auth.HistorySharedSince).UTC()
			ss = &t
		}
		return ss, true, nil
	}
	subCache, err := readcache.NewSubscriptionCache(subscriptionRepo, subL2, cfg.SubCacheSize, cfg.SubCacheTTL)
```

(`subscriptionsColl` is `db.Collection("subscriptions")`; `subscriptionRepo` is the existing `*mongorepo.SubscriptionRepo` still used for `GetSubscription`/pin. `valkeyClient` is nil when Valkey is unconfigured, which disables the L2 tier — fail-open.) Add imports `circuitbreaker`, `subauthcache`, `cachemetrics`.

- [ ] **Step 9: Add integration test — history-load survives Mongo down**

Add to history-service's integration test package (build tag `//go:build integration`), mirroring Task 3 Step 8's structure: seed a subscription, warm the access check via `GetHistorySharedSince` (populating L2), stop Mongo (or apply the 1ms-timeout technique), then assert `LoadHistory` for the warm room returns Cassandra messages while a cold room's access check errors. Seed at least one Cassandra message via the existing `cassrepo` test helpers so the warm read returns data.

- [ ] **Step 10: Run unit + integration tests**

Run: `make test SERVICE=history-service`
Run: `make test-integration SERVICE=history-service`
Expected: PASS.

- [ ] **Step 11: Lint + commit**

```bash
make lint
git add history-service/
git commit -m "Wire subauthcache + circuit breaker into history-service for Mongo-outage survival"
```

---

## Task 5: Observability — breaker state + served-stale counters

**Files:**
- Modify: `pkg/circuitbreaker/circuitbreaker.go` (transition callback hook)
- Modify: `message-gatekeeper/main.go`, `history-service/cmd/main.go` (register a state gauge + transition log)
- Test: `pkg/circuitbreaker/circuitbreaker_test.go`

**Interfaces:**
- Consumes: `circuitbreaker.Breaker`.
- Produces: `func WithOnTransition(fn func(from, to State)) Option` on the breaker.

- [ ] **Step 1: Write the failing test**

```go
func TestBreaker_OnTransitionFires(t *testing.T) {
	now := time.Unix(0, 0)
	var transitions []string
	b := New(1, time.Minute,
		WithClock(func() time.Time { return now }),
		WithOnTransition(func(from, to State) {
			transitions = append(transitions, from.String()+"->"+to.String())
		}),
	)
	require.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom) // closed->open
	now = now.Add(2 * time.Minute)
	require.NoError(t, b.Do(func() error { return nil })) // half-open probe -> closed
	assert.Equal(t, []string{"closed->open", "open->half-open", "half-open->closed"}, transitions)
}
```

Add a `String()` method to `State` in the same step (needed by the test and by log lines).

- [ ] **Step 2: Run to verify it fails**

Run: `make test`
Expected: FAIL — `undefined: WithOnTransition`, `State has no field or method String`.

- [ ] **Step 3: Implement `String()`, `WithOnTransition`, and fire on every state change**

Add to `circuitbreaker.go`: a `func (s State) String() string` (closed/open/half-open/unknown); an `onTransition func(from, to State)` field + `WithOnTransition` option; and in the locked mutation paths (`maybeHalfOpenLocked` and the success/failure branches of `Do`), when `state` changes, capture `(old, new)` and invoke `onTransition` **after** releasing the lock (or via a deferred call with captured values) so the callback can't deadlock on the breaker.

Implementation sketch for `Do`'s epilogue:

```go
	b.mu.Lock()
	old := b.state
	// ... mutate b.state / b.failures as before ...
	newState := b.state
	cb := b.onTransition
	b.mu.Unlock()
	if cb != nil && old != newState {
		cb(old, newState)
	}
	return err
```

Refactor `maybeHalfOpenLocked` similarly (return whether it transitioned so the caller can fire the callback outside the lock). Keep the state machine behavior identical — this task only adds the observability hook.

- [ ] **Step 4: Run to verify it passes**

Run: `make test`
Expected: PASS.

- [ ] **Step 5: Register a gauge + transition log at each wiring site**

In both `message-gatekeeper/main.go` and `history-service/cmd/main.go`, pass `circuitbreaker.WithOnTransition(...)` when building the breaker:

```go
	breaker := circuitbreaker.New(cfg.MongoBreakerFails, cfg.MongoBreakerCool,
		circuitbreaker.WithOnTransition(func(from, to circuitbreaker.State) {
			slog.Warn("mongo circuit breaker transition", "from", from.String(), "to", to.String())
			mongoBreakerState.Set(float64(to)) // Prometheus gauge via the o11y SDK
		}),
	)
```

Register `mongoBreakerState` as a Prometheus gauge through the service's existing `pkg/obs`/o11y metric registration (follow how the service registers its other custom metrics; if it registers none, use the o11y SDK meter to create a gauge named `mongo_breaker_state`). The `slog.Warn` carries only state labels — no account/room data.

- [ ] **Step 6: Run lint + tests + commit**

```bash
make lint
make test SERVICE=message-gatekeeper
make test SERVICE=history-service
git add pkg/circuitbreaker/ message-gatekeeper/ history-service/
git commit -m "Add circuit-breaker transition hook, state gauge, and transition logs"
```

---

## Task 6: Verification pass — SAST, full build, coverage

**Files:** none (verification only).

- [ ] **Step 1: Build both services**

Run: `make build SERVICE=message-gatekeeper && make build SERVICE=history-service`
Expected: both build clean.

- [ ] **Step 2: Regenerate mocks if any store interface changed**

The `Store` interfaces did not change signature, but `NewSubscriptionCache`'s signature did. If any mock is generated from a changed interface, run `make generate SERVICE=history-service`. Then `make test`.

- [ ] **Step 3: Coverage check on new packages**

Run: `go test -race -coverprofile=cover.out ./pkg/circuitbreaker/... ./pkg/subauthcache/... && go tool cover -func=cover.out | tail -1`
Expected: ≥90% for both new packages. Add table cases for any uncovered branch (e.g. `State().String()` unknown case, `readL2` error branch).

- [ ] **Step 4: SAST**

Run: `make sast`
Expected: no medium+ findings. If a false positive appears, suppress only with a justified `// #nosec <RULE> -- reason` directly above the line.

- [ ] **Step 5: Full unit + integration suite**

Run: `make test`
Run: `make test-integration SERVICE=message-gatekeeper && make test-integration SERVICE=history-service`
Expected: all green.

- [ ] **Step 6: Final commit (if any fixups)**

```bash
git add -A
git commit -m "Fixups from verification pass (coverage, sast, mocks)"
```

---

## Self-Review

**Spec coverage:**
- Mechanism 1 (shared L2 subscription cache) → Task 2 (`pkg/subauthcache`), wired in Tasks 3 & 4. ✓
- Mechanism 2 (long L2 TTL as outage buffer) → `SubL2TTL` default `90m` in Tasks 3 & 4 config. ✓
- Mechanism 3 (circuit breaker) → Task 1 (`pkg/circuitbreaker`), wired in Tasks 3 & 4. ✓
- Send-path behavior (warm allow / cold deny / large-room fail-open / Cassandra durability) → Task 3 (handler fail-open + integration). ✓
- History-path behavior (shared warmth, room-times now/floor fallback, best-effort min-seen, Cassandra reads) → Task 4. ✓
- Positive-only, fail-open-on-Valkey conventions → enforced in `subauthcache.ReadThrough` (Task 2 tests). ✓
- Observability (breaker gauge, served-stale/transition logs) → Task 5. ✓
- Testing (unit + testcontainers with Mongo stopped) → Tasks 1–4 + Task 6. ✓
- No `docs/client-api.md` change → stated in Global Constraints; no schema/event touched. ✓
- Accepted gaps (cold users denied, outage-window revocation) → encoded in Task 3 Step 5 note + cold-room assertions. ✓

**Placeholder scan:** No "TBD"/"handle edge cases"/"similar to Task N"; every code step shows code. The only conditional instructions are the two "check the testutil surface / match existing helper names" notes, which are explicit grep-first-then-choose directions, not deferred work.

**Type consistency:** `SubAuth`, `SubKey`, `FetchFromMongo`, `ReadThrough`, `Loader` names identical across Tasks 2/3/4. `NewMongoStore` new signature `(db, valkey, metaTTL, subTTL, breaker)` used consistently in Task 3 Steps 5/7/8. `circuitbreaker.New(threshold, cooldown, ...Option)`, `Do`, `State`, `ErrOpen`, `WithClock`, `WithOnTransition`, `State.String()` consistent across Tasks 1/3/4/5. `NewSubscriptionCache(inner, l2, size, ttl)` consistent across Task 4 Steps 5/6/8.
