# At-Rest DEK Mongo-Outage Survival Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the per-room at-rest encryption key (DEK) reachable without MongoDB, so reading encrypted history (`history-service`) and persisting a plain message to Cassandra (`message-worker`) both survive a Mongo outage for active rooms.

**Architecture:** Add a Valkey L2 for **Vault-wrapped** DEKs as a `DEKStore` **decorator** (`pkg/atrest`), so `cipher.go` needs no changes at all — both `Cipher.Decrypt` (history reads) and `Cipher.Encrypt` (message-worker persists) benefit through the existing interface. The decorator is fail-open (nil client / any Valkey error → Mongo), positive-only, breaker-guarded on the Mongo call, and re-arms an L2 hit's TTL while the breaker is not closed so an outage of any length is survivable. On top of that, `message-worker`'s two Mongo *enrichment* reads (sender, mentions) fail-open to the self-describing canonical event.

**Tech Stack:** Go 1.25, MongoDB (`mongo-driver/v2`), Valkey (`pkg/valkeyutil`, cluster-mode), Vault (`pkg/atrest` KeyWrapper), Cassandra (`gocql`), NATS/JetStream, `go.uber.org/mock`, `stretchr/testify`, `testcontainers-go` via `pkg/testutil`.

## Global Constraints

- Go 1.25, monorepo, single root `go.mod`. Use `make` targets, never raw `go` (`make test SERVICE=<name>`, `make lint`, `make generate SERVICE=<name>`).
- **This branch is stacked on PR #188** (`claude/offline-messaging-history-q3atep`). `pkg/circuitbreaker` and `pkg/subauthcache` come from that base — reuse them **as-is**, do not modify them.
- TDD mandatory (Red → Green → Refactor). Write the failing test first, confirm it fails for the right reason, then implement.
- ≥80% coverage per package; ≥90% for the new `pkg/atrest` L2 code.
- All tests run with `-race` (the Makefile handles it). Unit tests never touch real Mongo/Valkey/Vault; integration tests are `//go:build integration` using `pkg/testutil` containers.
- **Valkey must only ever hold the Vault-wrapped DEK** (`RoomDataKey.WrappedDEK` ciphertext) — never a plaintext key or unwrapped AEAD.
- Fail-open everywhere: a nil Valkey client or ANY Valkey error degrades to the Mongo store; only the Mongo/Vault result governs the returned error.
- Positive-only: never cache a "row absent" result (`Get` returning `(nil, nil)`) — that value drives lazy DEK creation.
- L2 key convention: `{roomID}` hash-tag to colocate in the room's cluster slot, matching `roommetacache.MetaKey` / `subauthcache.SubKey`.
- Errors: wrap with `fmt.Errorf("...: %w", err)`; compare with `errors.Is`/`errors.As`, never by string.
- `log/slog` JSON only; log IDs/errors only — **never** log key material, wrapped or unwrapped.
- Config via `caarlos0/env`, `SCREAMING_SNAKE_CASE`, always `envDefault` for non-secrets.
- Exact config values: `ATREST_DEK_L2_TTL`=`90m`, `ATREST_DEK_BREAKER_FAILS`=`5`, `ATREST_DEK_BREAKER_COOLDOWN`=`10s`.
- No `docs/client-api.md` change (no client-facing wire schema or event changes).
- Do NOT add third-party dependencies — everything needed is vendored.

---

## File Structure

**New files:**
- `pkg/atrest/dek_store_l2.go` — the L2 `DEKStore` decorator (read-through, populate, mutation invalidation, breaker, sliding TTL).
- `pkg/atrest/dek_store_l2_test.go` — unit tests.
- `pkg/atrest/dek_store_l2_integration_test.go` — integration tests (real Valkey + Mongo).

**Modified files:**
- `history-service/internal/config/config.go` — DEK L2 config knobs + validation.
- `history-service/internal/config/config_test.go` — validation tests.
- `history-service/cmd/main.go` — wrap the Mongo DEK store with the L2 decorator (reusing the existing Valkey client, separate DEK breaker).
- `message-worker/handler.go` — sender + mention fail-opens.
- `message-worker/handler_test.go` — fail-open tests.
- `message-worker/main.go` — Valkey client + DEK L2 wiring + config knobs.
- `message-worker/deploy/docker-compose.yml` — `VALKEY_ADDRS`.

**Explicitly NOT modified:** `pkg/atrest/cipher.go`, `pkg/atrest/cache.go`, `pkg/circuitbreaker`, `pkg/subauthcache`. The decorator sits behind the existing `DEKStore` interface, so the cipher is untouched.

---

## Task 1: `pkg/atrest` L2 DEK store decorator

**Files:**
- Create: `pkg/atrest/dek_store_l2.go`
- Test: `pkg/atrest/dek_store_l2_test.go`

**Interfaces:**
- Consumes: existing `atrest.DEKStore` interface (`Get(ctx, roomID) (*RoomDataKey, error)`, `Upsert(ctx, key RoomDataKey) error`, `Replace(ctx, key RoomDataKey) error`), `atrest.RoomDataKey{ID string; WrappedDEK []byte; CreatedAt time.Time}`, `pkg/valkeyutil` (`Client`, `GetJSON`, `SetJSONWithTTL`, `ErrCacheMiss`), `pkg/circuitbreaker` (`*Breaker`, `Do`, `State`, `StateClosed`), `pkg/cachemetrics`.
- Produces:
  - `func DEKKey(roomID string) string` → `"dek:{<roomID>}"`
  - `type L2Recorder interface { Hit(context.Context); Miss(context.Context); Error(context.Context) }`
  - `func NewL2DEKStore(inner DEKStore, client valkeyutil.Client, ttl time.Duration, breaker *circuitbreaker.Breaker, rec L2Recorder) DEKStore`

**Design notes the implementer must honor:**
- `Get`: L2 read → hit returns the cached `*RoomDataKey`; miss (or any L2 error, or L2 disabled) → `breaker.Do(inner.Get)` → on a non-nil row, populate L2 → return. A `(nil, nil)` row (absent) is **never** cached.
- `Upsert`/`Replace`: delegate to `inner`, then **invalidate** the L2 key (best-effort `Del`). Rotation (`Replace`) rewraps the DEK under a new KEK, so a stale L2 entry would fail to unwrap — invalidation is a correctness requirement, not an optimization.
- **Sliding TTL:** on an L2 hit, re-arm the entry's TTL **only while `breaker.State() != StateClosed`**. Gating on the breaker means a missed invalidation still self-heals within one TTL during normal operation, while an outage of any length keeps active rooms' keys alive.
- L2 is disabled when `client == nil` **or** `ttl <= 0` (Valkey treats `ttl==0` as store-forever; never persist a wrapped DEK with no expiry).

- [ ] **Step 1: Write the failing tests**

Create `pkg/atrest/dek_store_l2_test.go`:

```go
package atrest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// fakeL2Valkey is an in-memory valkeyutil.Client for tests.
type fakeL2Valkey struct {
	store   map[string]string
	getErr  error
	setErr  error
	getHits int
	setHits int
	delHits int
}

func newFakeL2Valkey() *fakeL2Valkey { return &fakeL2Valkey{store: map[string]string{}} }

func (f *fakeL2Valkey) Get(_ context.Context, key string) (string, error) {
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

func (f *fakeL2Valkey) Set(_ context.Context, key, value string, _ time.Duration) error {
	f.setHits++
	if f.setErr != nil {
		return f.setErr
	}
	f.store[key] = value
	return nil
}

func (f *fakeL2Valkey) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	return false, nil
}
func (f *fakeL2Valkey) IncrEx(context.Context, string, time.Duration) (int64, error) { return 0, nil }
func (f *fakeL2Valkey) Del(_ context.Context, keys ...string) error {
	f.delHits++
	for _, k := range keys {
		delete(f.store, k)
	}
	return nil
}
func (f *fakeL2Valkey) Close() error { return nil }

// fakeInnerStore is an in-memory DEKStore standing in for Mongo.
type fakeInnerStore struct {
	rows      map[string]RoomDataKey
	getErr    error
	getCalls  int
	upsertHit int
	replHit   int
}

func newFakeInnerStore() *fakeInnerStore {
	return &fakeInnerStore{rows: map[string]RoomDataKey{}}
}

func (s *fakeInnerStore) Get(_ context.Context, roomID string) (*RoomDataKey, error) {
	s.getCalls++
	if s.getErr != nil {
		return nil, s.getErr
	}
	row, ok := s.rows[roomID]
	if !ok {
		return nil, nil // absent — drives lazy DEK creation; must never be cached
	}
	return &row, nil
}

func (s *fakeInnerStore) Upsert(_ context.Context, key RoomDataKey) error {
	s.upsertHit++
	if _, exists := s.rows[key.ID]; !exists {
		s.rows[key.ID] = key
	}
	return nil
}

func (s *fakeInnerStore) Replace(_ context.Context, key RoomDataKey) error {
	s.replHit++
	s.rows[key.ID] = key
	return nil
}

type spyL2Recorder struct{ hit, miss, err int }

func (s *spyL2Recorder) Hit(context.Context)   { s.hit++ }
func (s *spyL2Recorder) Miss(context.Context)  { s.miss++ }
func (s *spyL2Recorder) Error(context.Context) { s.err++ }

// openBreaker returns a breaker already tripped open (threshold 1, long cooldown).
func openBreaker(t *testing.T) *circuitbreaker.Breaker {
	t.Helper()
	b := circuitbreaker.New(1, time.Hour)
	_ = b.Do(func() error { return errors.New("trip") })
	require.Equal(t, circuitbreaker.StateOpen, b.State())
	return b
}

func healthyBreaker() *circuitbreaker.Breaker { return circuitbreaker.New(5, time.Second) }

func seedRow(roomID string) RoomDataKey {
	return RoomDataKey{ID: roomID, WrappedDEK: []byte("wrapped-ciphertext"), CreatedAt: time.Unix(0, 0).UTC()}
}

func TestDEKKey(t *testing.T) {
	assert.Equal(t, "dek:{room1}", DEKKey("room1"))
}

func TestL2DEKStore_Get_MissThenMongoPopulatesL2(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

	got, err := s.Get(context.Background(), "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, []byte("wrapped-ciphertext"), got.WrappedDEK)
	assert.Equal(t, 1, inner.getCalls)
	assert.Equal(t, 1, fv.setHits, "a found row must populate L2")
	assert.Equal(t, 1, rec.miss)
}

func TestL2DEKStore_Get_L2HitSkipsMongo(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

	_, err := s.Get(context.Background(), "room1") // warm
	require.NoError(t, err)
	before := inner.getCalls

	got, err := s.Get(context.Background(), "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, []byte("wrapped-ciphertext"), got.WrappedDEK)
	assert.Equal(t, before, inner.getCalls, "an L2 hit must not reach Mongo")
	assert.GreaterOrEqual(t, rec.hit, 1)
}

// The outage case this whole design exists for.
func TestL2DEKStore_Get_ServesFromL2WhileMongoDown(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)
	_, err := s.Get(context.Background(), "room1") // warm L2 while healthy
	require.NoError(t, err)

	inner.getErr = errors.New("mongo unreachable")
	got, err := s.Get(context.Background(), "room1")
	require.NoError(t, err, "a warmed room must resolve from L2 during a Mongo outage")
	require.NotNil(t, got)
	assert.Equal(t, []byte("wrapped-ciphertext"), got.WrappedDEK)
}

func TestL2DEKStore_Get_ColdRoomDuringOutageErrors(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.getErr = errors.New("mongo unreachable")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

	_, err := s.Get(context.Background(), "coldroom")
	require.Error(t, err, "a cold room has no key and must surface the error")
}

func TestL2DEKStore_Get_AbsentRowNotCached(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

	got, err := s.Get(context.Background(), "newroom")
	require.NoError(t, err)
	assert.Nil(t, got, "absent row must surface as (nil, nil) so the cipher lazily creates a DEK")
	assert.Equal(t, 0, fv.setHits, "an absent row must never be cached")
}

func TestL2DEKStore_Get_BreakerOpenFastFailsWithoutMongo(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	s := NewL2DEKStore(inner, fv, time.Hour, openBreaker(t), rec)

	_, err := s.Get(context.Background(), "coldroom")
	require.ErrorIs(t, err, circuitbreaker.ErrOpen)
	assert.Equal(t, 0, inner.getCalls, "an open breaker must not call Mongo")
}

func TestL2DEKStore_Get_ValkeyErrorFailsOpenToMongo(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	fv.getErr = errors.New("valkey unreachable")
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)

	got, err := s.Get(context.Background(), "room1")
	require.NoError(t, err, "a Valkey error must degrade to Mongo, not fail the call")
	require.NotNil(t, got)
	assert.Equal(t, 1, rec.err)
}

func TestL2DEKStore_Get_NilClientAndNonPositiveTTLBypassL2(t *testing.T) {
	cases := []struct {
		name   string
		client valkeyutil.Client
		ttl    time.Duration
	}{
		{"nil client", nil, time.Hour},
		{"zero ttl", newFakeL2Valkey(), 0},
		{"negative ttl", newFakeL2Valkey(), -time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := newFakeInnerStore()
			inner.rows["room1"] = seedRow("room1")
			s := NewL2DEKStore(inner, tc.client, tc.ttl, healthyBreaker(), &spyL2Recorder{})

			got, err := s.Get(context.Background(), "room1")
			require.NoError(t, err)
			require.NotNil(t, got)
			if fv, ok := tc.client.(*fakeL2Valkey); ok {
				assert.Equal(t, 0, fv.getHits, "L2 disabled must not read Valkey")
				assert.Equal(t, 0, fv.setHits, "L2 disabled must not write Valkey")
			}
		})
	}
}

// Rotation correctness: a stale wrapped DEK would fail to unwrap under the new KEK.
func TestL2DEKStore_ReplaceInvalidatesL2(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)
	_, err := s.Get(context.Background(), "room1") // warm
	require.NoError(t, err)
	require.NotEmpty(t, fv.store[DEKKey("room1")])

	rotated := RoomDataKey{ID: "room1", WrappedDEK: []byte("rewrapped"), CreatedAt: time.Unix(1, 0).UTC()}
	require.NoError(t, s.Replace(context.Background(), rotated))
	assert.Empty(t, fv.store[DEKKey("room1")], "rotation must invalidate the stale L2 entry")

	got, err := s.Get(context.Background(), "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, []byte("rewrapped"), got.WrappedDEK, "post-rotation reads must see the new wrapped DEK")
}

func TestL2DEKStore_UpsertInvalidatesL2(t *testing.T) {
	fv, inner, rec := newFakeL2Valkey(), newFakeInnerStore(), &spyL2Recorder{}
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), rec)
	_, err := s.Get(context.Background(), "room1")
	require.NoError(t, err)

	require.NoError(t, s.Upsert(context.Background(), seedRow("room1")))
	assert.Empty(t, fv.store[DEKKey("room1")], "upsert must invalidate the L2 entry")
	assert.Equal(t, 1, inner.upsertHit)
}

func TestL2DEKStore_SlidesTTLOnHitOnlyWhileBreakerOpen(t *testing.T) {
	t.Run("degraded slides", func(t *testing.T) {
		fv, inner := newFakeL2Valkey(), newFakeInnerStore()
		inner.rows["room1"] = seedRow("room1")
		b := healthyBreaker()
		s := NewL2DEKStore(inner, fv, time.Hour, b, &spyL2Recorder{})
		_, err := s.Get(context.Background(), "room1") // warm (setHits=1)
		require.NoError(t, err)

		// Trip the breaker, then hit L2 again.
		inner.getErr = errors.New("mongo down")
		for i := 0; i < 5; i++ {
			_, _ = s.Get(context.Background(), "coldroom")
		}
		require.NotEqual(t, circuitbreaker.StateClosed, b.State())
		before := fv.setHits

		_, err = s.Get(context.Background(), "room1")
		require.NoError(t, err)
		assert.Equal(t, before+1, fv.setHits, "a degraded L2 hit must re-arm the TTL")
	})

	t.Run("healthy does not slide", func(t *testing.T) {
		fv, inner := newFakeL2Valkey(), newFakeInnerStore()
		inner.rows["room1"] = seedRow("room1")
		s := NewL2DEKStore(inner, fv, time.Hour, healthyBreaker(), &spyL2Recorder{})
		_, err := s.Get(context.Background(), "room1") // warm
		require.NoError(t, err)
		before := fv.setHits

		_, err = s.Get(context.Background(), "room1")
		require.NoError(t, err)
		assert.Equal(t, before, fv.setHits, "a healthy L2 hit must not re-arm")
	})
}

func TestL2DEKStore_NilRecorderDoesNotPanic(t *testing.T) {
	inner := newFakeInnerStore()
	inner.rows["room1"] = seedRow("room1")
	s := NewL2DEKStore(inner, newFakeL2Valkey(), time.Hour, healthyBreaker(), nil)
	got, err := s.Get(context.Background(), "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/atrest/... -run 'TestL2DEKStore|TestDEKKey'`
Expected: FAIL — `undefined: NewL2DEKStore`, `undefined: DEKKey`.

- [ ] **Step 3: Write the implementation**

Create `pkg/atrest/dek_store_l2.go`:

```go
package atrest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// L2Recorder records L2 (Valkey) hit/miss/error outcomes. cachemetrics.Recorder
// satisfies it; tests substitute a spy.
type L2Recorder interface {
	Hit(ctx context.Context)
	Miss(ctx context.Context)
	Error(ctx context.Context)
}

// noopL2Recorder is the fallback when a nil recorder is supplied, so the
// exported constructor can't produce a nil-panicking store.
type noopL2Recorder struct{}

func (noopL2Recorder) Hit(context.Context)   {}
func (noopL2Recorder) Miss(context.Context)  {}
func (noopL2Recorder) Error(context.Context) {}

// DEKKey is the L2 key for a room's wrapped DEK. The {roomID} hash-tag
// colocates it in the room's cluster slot, matching house convention.
func DEKKey(roomID string) string {
	return "dek:{" + roomID + "}"
}

// l2DEKStore decorates a DEKStore with a Valkey L2 tier holding the
// Vault-WRAPPED DEK record. It exists so a room's key stays reachable while
// MongoDB is unavailable: the in-process DEK cache expires on a fixed TTL
// stamped at fetch time, so without an L2 an active room loses its key
// mid-outage and encrypt/decrypt start failing.
//
// Only ciphertext is stored — the wrapped DEK is exactly what Mongo holds, so
// an attacker with Valkey access still needs the Vault KEK.
//
// Fail-open: a nil client, a non-positive ttl, or any Valkey error degrades to
// the inner store; only the inner store's result governs the returned error.
// Positive-only: an absent row (nil, nil) is never cached, since that value is
// what drives lazy DEK creation in the cipher.
type l2DEKStore struct {
	inner   DEKStore
	client  valkeyutil.Client
	ttl     time.Duration
	breaker *circuitbreaker.Breaker
	metrics L2Recorder
}

// NewL2DEKStore wraps inner with a Valkey L2 tier. Pass a nil client (or a
// non-positive ttl) to disable the L2 and get inner's behavior unchanged.
// breaker guards the inner (Mongo) fetch so a cold miss during an outage
// fast-fails instead of stalling; it must not be nil.
func NewL2DEKStore(inner DEKStore, client valkeyutil.Client, ttl time.Duration, breaker *circuitbreaker.Breaker, rec L2Recorder) DEKStore {
	if rec == nil {
		rec = noopL2Recorder{}
	}
	return &l2DEKStore{inner: inner, client: client, ttl: ttl, breaker: breaker, metrics: rec}
}

func (s *l2DEKStore) l2Enabled() bool { return s.client != nil && s.ttl > 0 }

// degraded reports whether Mongo is not confirmed healthy, i.e. the breaker is
// open or half-open. While degraded, an L2 hit re-arms its TTL so an actively
// read room's key survives an outage of any length. Gating on the breaker keeps
// normal-mode behavior unchanged, so a missed invalidation still self-heals
// within one TTL.
func (s *l2DEKStore) degraded() bool {
	return s.breaker.State() != circuitbreaker.StateClosed
}

func (s *l2DEKStore) Get(ctx context.Context, roomID string) (*RoomDataKey, error) {
	if s.l2Enabled() {
		if row, found := s.readL2(ctx, roomID); found {
			if s.degraded() {
				s.writeL2(ctx, roomID, row, "slide")
			}
			return row, nil
		}
	}

	var row *RoomDataKey
	err := s.breaker.Do(func() error {
		var e error
		row, e = s.inner.Get(ctx, roomID)
		return e
	})
	if err != nil {
		return nil, fmt.Errorf("dek l2 read-through for room %s: %w", roomID, err)
	}
	// A nil row means "no DEK yet" — never cached; the cipher creates one.
	if row != nil && s.l2Enabled() {
		s.writeL2(ctx, roomID, row, "populate")
	}
	return row, nil
}

// Upsert delegates and then invalidates the L2 entry so a subsequent read
// re-resolves authoritatively.
func (s *l2DEKStore) Upsert(ctx context.Context, key RoomDataKey) error { //nolint:gocritic // hugeParam: value receiver required by the DEKStore interface
	if err := s.inner.Upsert(ctx, key); err != nil {
		return err
	}
	s.invalidate(ctx, key.ID)
	return nil
}

// Replace delegates and then invalidates the L2 entry. This is a correctness
// requirement, not an optimization: KEK rotation rewraps the DEK, so a stale
// cached wrapped-DEK would fail to unwrap under the new KEK.
func (s *l2DEKStore) Replace(ctx context.Context, key RoomDataKey) error { //nolint:gocritic // hugeParam: value receiver required by the DEKStore interface
	if err := s.inner.Replace(ctx, key); err != nil {
		return err
	}
	s.invalidate(ctx, key.ID)
	return nil
}

// readL2 attempts the L2 read and records the outcome. found=true only on a hit;
// a clean miss records Miss, any other error records Error — both fall through
// to the inner store.
func (s *l2DEKStore) readL2(ctx context.Context, roomID string) (*RoomDataKey, bool) {
	var cached RoomDataKey
	err := valkeyutil.GetJSON(ctx, s.client, DEKKey(roomID), &cached)
	if err == nil {
		s.metrics.Hit(ctx)
		return &cached, true
	}
	if errors.Is(err, valkeyutil.ErrCacheMiss) {
		s.metrics.Miss(ctx)
		return nil, false
	}
	s.metrics.Error(ctx)
	slog.WarnContext(ctx, "dek L2 read failed, falling back to mongo",
		"room_id", roomID, "error", err)
	return nil, false
}

// writeL2 stores (or re-arms) the wrapped-DEK record. Best-effort: a failure is
// logged and swallowed — the caller already has the value, and the next
// Mongo-healthy read repopulates. phase is a coarse tag ("populate"/"slide").
func (s *l2DEKStore) writeL2(ctx context.Context, roomID string, row *RoomDataKey, phase string) {
	if err := valkeyutil.SetJSONWithTTL(ctx, s.client, DEKKey(roomID), row, s.ttl); err != nil {
		slog.WarnContext(ctx, "dek L2 write failed (TTL will reconcile)",
			"room_id", roomID, "phase", phase, "error", err)
	}
}

// invalidate best-effort deletes the L2 entry after an authoritative write.
func (s *l2DEKStore) invalidate(ctx context.Context, roomID string) {
	if !s.l2Enabled() {
		return
	}
	if err := s.client.Del(ctx, DEKKey(roomID)); err != nil {
		slog.WarnContext(ctx, "dek L2 invalidate failed (TTL will reconcile)",
			"room_id", roomID, "error", err)
	}
}

// DefaultL2Recorder is the shared metrics recorder for the DEK L2 tier, so
// every service emits the same cache="atrestdek",tier="l2" series.
func DefaultL2Recorder() L2Recorder { return cachemetrics.For("atrestdek", "l2") }
```

**Note on JSON:** `RoomDataKey` currently has only `bson` tags. `valkeyutil.GetJSON`/`SetJSONWithTTL` use `encoding/json`, which falls back to field names (`ID`, `WrappedDEK`, `CreatedAt`) — that round-trips correctly since both sides use the same struct, and `[]byte` marshals as base64. Do **not** add `json` tags in this task unless a test proves a mismatch; if you do add them, add them to all three fields and keep them `camelCase` per house convention.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race ./pkg/atrest/... -run 'TestL2DEKStore|TestDEKKey'`
Expected: PASS (all cases).

- [ ] **Step 5: Run the full package + lint**

Run: `make test SERVICE=pkg/atrest` (or `go test -race ./pkg/atrest/...`) — the pre-existing cipher/cache tests must still pass.
Run: `make lint`
Expected: all green, 0 lint issues.

- [ ] **Step 6: Commit**

```bash
git add pkg/atrest/dek_store_l2.go pkg/atrest/dek_store_l2_test.go
git commit -m "Add Valkey L2 decorator for Vault-wrapped at-rest DEKs"
```

---

## Task 2: `pkg/atrest` L2 integration test

**Files:**
- Create: `pkg/atrest/dek_store_l2_integration_test.go`

**Interfaces:**
- Consumes: `NewL2DEKStore`, `DEKKey`, `NewMongoDEKStore`, `RoomDataKey` (Task 1 + existing), `pkg/testutil` (`MongoDB`, `SharedValkeyCluster`, `FlushValkey`, `RunTests`), `valkeyutil.WrapClusterClient`.
- Produces: nothing consumed by later tasks.

**Environment note:** Integration tests may not be runnable locally (they need Docker; if the registry is blocked they are CI-deferred). **Make them compile** — verify with `go vet -tags=integration ./pkg/atrest/...` — and do not treat an inability to *run* them as a task failure. Report the compile result.

- [ ] **Step 1: Write the integration test (this file must define TestMain)**

Verified: `pkg/atrest` has **no** `TestMain` in any `_test.go` today (`grep -rn "func TestMain" pkg/atrest/*_test.go` returns nothing), so the new file below **must** define it — the package's integration tests need `testutil.RunTests` to drive container cleanup. Re-run that grep first; if some other file has since added one, drop the `TestMain` from this file instead (two in one package fail to compile).

Create `pkg/atrest/dek_store_l2_integration_test.go`:

```go
//go:build integration

package atrest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// TestMain drives testutil's container cleanup for this package's integration
// tests. pkg/atrest has no other TestMain — see Step 1.
func TestMain(m *testing.M) { testutil.RunTests(m) }

// TestL2DEKStore_SurvivesMongoOutage is the end-to-end proof of this feature:
// a room whose wrapped DEK is warm in Valkey still resolves while the Mongo DEK
// store is unusable, and a cold room does not.
func TestL2DEKStore_SurvivesMongoOutage(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "atrest_dek_l2")
	valkey := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
	t.Cleanup(func() { testutil.FlushValkey(t) })

	mongoStore := NewMongoDEKStore(db.Collection(CollectionName))
	row := RoomDataKey{ID: "room1", WrappedDEK: []byte("wrapped-ciphertext"), CreatedAt: time.Now().UTC().Truncate(time.Millisecond)}
	require.NoError(t, mongoStore.Upsert(ctx, row))

	breaker := circuitbreaker.New(1, 50*time.Millisecond)
	store := NewL2DEKStore(mongoStore, valkey, time.Hour, breaker, nil)

	// Warm the L2 while Mongo is healthy.
	got, err := store.Get(ctx, "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, row.WrappedDEK, got.WrappedDEK)

	// Simulate the Mongo outage with a store whose collection is unusable, while
	// the SAME (already warmed) Valkey L2 stays healthy.
	brokenMongo := NewMongoDEKStore(db.Client().Database("nonexistent-db-for-outage").Collection(CollectionName))
	outageStore := NewL2DEKStore(brokenMongo, valkey, time.Hour, circuitbreaker.New(1, 50*time.Millisecond), nil)

	warm, err := outageStore.Get(ctx, "room1")
	require.NoError(t, err, "warm room must resolve from the L2 during a Mongo outage")
	require.NotNil(t, warm)
	assert.Equal(t, row.WrappedDEK, warm.WrappedDEK)
}

// TestL2DEKStore_ReplaceInvalidates_Integration proves rotation correctness
// against a real Valkey: after Replace, the next read sees the new wrapped DEK.
func TestL2DEKStore_ReplaceInvalidates_Integration(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "atrest_dek_l2_rot")
	valkey := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
	t.Cleanup(func() { testutil.FlushValkey(t) })

	mongoStore := NewMongoDEKStore(db.Collection(CollectionName))
	require.NoError(t, mongoStore.Upsert(ctx, RoomDataKey{ID: "room2", WrappedDEK: []byte("old"), CreatedAt: time.Now().UTC()}))

	store := NewL2DEKStore(mongoStore, valkey, time.Hour, circuitbreaker.New(5, time.Second), nil)
	_, err := store.Get(ctx, "room2") // warm
	require.NoError(t, err)

	require.NoError(t, store.Replace(ctx, RoomDataKey{ID: "room2", WrappedDEK: []byte("rewrapped"), CreatedAt: time.Now().UTC()}))

	got, err := store.Get(ctx, "room2")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, []byte("rewrapped"), got.WrappedDEK, "rotation must not be masked by a stale L2 entry")
}
```

- [ ] **Step 3: Verify it compiles**

Run: `go vet -tags=integration ./pkg/atrest/...`
Expected: exit 0, no output. (If `TestMain` is missing for the `integration` tag in this package, add `func TestMain(m *testing.M) { testutil.RunTests(m) }` to this file — but only if Step 1 showed none exists.)

- [ ] **Step 4: Attempt to run (optional; CI-deferred if Docker is unavailable)**

Run: `make test-integration SERVICE=pkg/atrest`
Expected: PASS if Docker works. If the image pull is blocked, note it and move on — do not treat it as a failure.

- [ ] **Step 5: Commit**

```bash
git add pkg/atrest/dek_store_l2_integration_test.go
git commit -m "Add integration tests for the at-rest DEK L2"
```

---

## Task 3: Wire the DEK L2 into history-service (restores the encrypted-read guarantee)

**Files:**
- Modify: `history-service/internal/config/config.go`
- Modify: `history-service/internal/config/config_test.go`
- Modify: `history-service/cmd/main.go`

**Interfaces:**
- Consumes: `atrest.NewL2DEKStore`, `atrest.DefaultL2Recorder` (Task 1), `circuitbreaker.New` (from the #188 base), the Valkey client `subValkey` already connected in `main.go` by #188.
- Produces: nothing consumed by later tasks.

**Context the implementer needs:** `history-service/cmd/main.go` already (from #188) connects `subValkey` when `len(cfg.ValkeyAddrs) > 0`, and builds a `breaker` for the *subscription* loader. This task adds a **separate** DEK breaker — do not reuse the subscription breaker, otherwise DEK-fetch failures and subscription-fetch failures would reset each other's counters (the same independent-breaker rule #188 applied to gatekeeper's room-meta reads).

- [ ] **Step 1: Write the failing config validation tests**

Add to `history-service/internal/config/config_test.go`:

```go
func TestValidate_RejectsNegativeDEKL2TTL(t *testing.T) {
	cfg := baseValid()
	cfg.DEKL2TTL = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ATREST_DEK_L2_TTL")
}

func TestValidate_RejectsNegativeDEKBreakerFails(t *testing.T) {
	cfg := baseValid()
	cfg.DEKBreakerFails = -1
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ATREST_DEK_BREAKER_FAILS")
}

func TestValidate_RejectsNegativeDEKBreakerCooldown(t *testing.T) {
	cfg := baseValid()
	cfg.DEKBreakerCooldown = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ATREST_DEK_BREAKER_COOLDOWN")
}
```

Also extend the existing `baseValid()` helper in that file with the new valid values (add these three lines inside the returned `Config` literal, keeping the existing fields):

```go
		DEKL2TTL:           90 * time.Minute,
		DEKBreakerFails:    5,
		DEKBreakerCooldown: 10 * time.Second,
```

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=history-service`
Expected: FAIL — `cfg.DEKL2TTL undefined` (and the other two fields).

- [ ] **Step 3: Add the config fields + validation**

In `history-service/internal/config/config.go`, add to the `Config` struct (place next to the existing `SubL2TTL`/`MongoBreaker*` block added by #188):

```go
	// DEKL2TTL is the Valkey L2 retention for Vault-wrapped at-rest DEKs — the
	// outage buffer for decrypting history. The in-process DEK cache expires on
	// a fixed TTL stamped at fetch time, so without this L2 an active room loses
	// its key partway through a Mongo outage. 0 disables the DEK L2 tier.
	DEKL2TTL time.Duration `env:"ATREST_DEK_L2_TTL" envDefault:"90m"`

	// DEKBreakerFails/DEKBreakerCooldown configure the circuit breaker guarding
	// the Mongo DEK fetch. Kept separate from the subscription breaker so the
	// two failure signals never reset each other.
	DEKBreakerFails    int           `env:"ATREST_DEK_BREAKER_FAILS"    envDefault:"5"`
	DEKBreakerCooldown time.Duration `env:"ATREST_DEK_BREAKER_COOLDOWN" envDefault:"10s"`
```

And in `validate(cfg *Config)`, before the final `return nil`:

```go
	if cfg.DEKL2TTL < 0 {
		return fmt.Errorf("ATREST_DEK_L2_TTL must be >= 0, got %s", cfg.DEKL2TTL)
	}
	if cfg.DEKBreakerFails < 0 {
		return fmt.Errorf("ATREST_DEK_BREAKER_FAILS must be >= 0, got %d", cfg.DEKBreakerFails)
	}
	if cfg.DEKBreakerCooldown < 0 {
		return fmt.Errorf("ATREST_DEK_BREAKER_COOLDOWN must be >= 0, got %s", cfg.DEKBreakerCooldown)
	}
```

- [ ] **Step 4: Run to verify the config tests pass**

Run: `make test SERVICE=history-service`
Expected: PASS.

- [ ] **Step 5: Wire the L2 decorator in main.go**

In `history-service/cmd/main.go`, find the at-rest block (it currently reads roughly):

```go
	if cfg.Atrest.Enabled {
		w, err := atrest.NewVaultKeyWrapper(ctx, cfg.Vault)
		...
		vaultWrapper = w
		// DEKs are written by other services; pin to primary so a fresh key isn't
		// missed on a lagging secondary.
		dekColl := mongoClient.Database(cfg.Mongo.DB).Collection(atrest.CollectionName,
			options.Collection().SetReadPreference(readpref.Primary()))
		cipher = atrest.NewCipher(w, atrest.NewMongoDEKStore(dekColl), cfg.Atrest)
	}
```

Replace the `cipher = atrest.NewCipher(...)` line with the decorated store (keep everything above it unchanged):

```go
		// Front the Mongo DEK store with the shared Valkey L2 so an active room's
		// key stays reachable during a Mongo outage (the in-process DEK cache
		// expires on a fixed TTL and cannot refetch while Mongo is down).
		// subValkey is the client already connected for the subauth L2; a nil
		// client disables the tier. The DEK breaker is deliberately separate from
		// the subscription breaker so the two health signals stay independent.
		dekBreaker := circuitbreaker.New(cfg.DEKBreakerFails, cfg.DEKBreakerCooldown)
		dekStore := atrest.NewL2DEKStore(atrest.NewMongoDEKStore(dekColl), subValkey,
			cfg.DEKL2TTL, dekBreaker, atrest.DefaultL2Recorder())
		cipher = atrest.NewCipher(w, dekStore, cfg.Atrest)
		slog.Info("at-rest DEK L2 configured", "enabled", subValkey != nil && cfg.DEKL2TTL > 0, "ttl", cfg.DEKL2TTL)
```

**Ordering — already correct, just don't break it:** verified that `subValkey` is connected at `history-service/cmd/main.go:160` and the `if cfg.Atrest.Enabled {` block starts at `:175`, so `subValkey` is already in scope here. `dekColl` is likewise already a local variable (`:184`) — reuse it as shown; do not re-declare it.

- [ ] **Step 6: Build and run the service tests**

Run: `go build ./history-service/...`
Run: `make test SERVICE=history-service`
Run: `make lint`
Expected: build OK, tests pass, 0 lint issues.

- [ ] **Step 7: Commit**

```bash
git add history-service/internal/config/config.go history-service/internal/config/config_test.go history-service/cmd/main.go
git commit -m "Front history-service DEK reads with the Valkey L2"
```

---

## Task 4: message-worker sender + mention fail-opens

**Files:**
- Modify: `message-worker/handler.go`
- Modify: `message-worker/handler_test.go`

**Interfaces:**
- Consumes: existing `message-worker` types — `Handler`, `processMessage(ctx, data []byte, isMigration bool) error`, `cassParticipant{ID, EngName, CompanyName, Account string}`, `h.userStore` (`FindUserByAccount`, `FindUsersByAccounts`), `h.store.SaveMessage`.
- Produces: nothing consumed by later tasks.

**Context:** In `processMessage`, two Mongo-backed calls currently abort the persist on error:
1. `mention.Resolve(ctx, evt.Message.Content, h.userStore.FindUsersByAccounts)` → `return fmt.Errorf("resolve mentions: %w", err)`
2. `h.userStore.FindUserByAccount(ctx, evt.Message.UserAccount)` → for a non-system message, `return fmt.Errorf("lookup user %s: %w", ...)`

Both are *enrichment*: the canonical event already carries `UserID`, `UserAccount`, and the gatekeeper-composed `UserDisplayName`, and message content (including literal `@tokens`) persists regardless. Make both fail-open so the Cassandra write still happens.

- [ ] **Step 1: Write the failing tests**

Add to `message-worker/handler_test.go` (match the file's existing harness — inspect a nearby `processMessage` test for the exact mock/constructor names and reuse them; the mocks are `MockStore`/`MockThreadStore`/`MockUserStore` generated by mockgen):

```go
func TestProcessMessage_UserLookupError_FailsOpenFromEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	users := NewMockUserStore(ctrl)

	// Mongo is down: both enrichment lookups fail.
	users.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(nil, errors.New("mongo down")).AnyTimes()
	users.EXPECT().FindUserByAccount(gomock.Any(), "alice").Return(nil, errors.New("mongo down"))

	// The persist must still happen, with the sender projected from the event.
	var savedSender *cassParticipant
	store.EXPECT().SaveMessage(gomock.Any(), gomock.Any(), gomock.Any(), "site1").
		DoAndReturn(func(_ context.Context, _ *model.Message, sender *cassParticipant, _ string) error {
			savedSender = sender
			return nil
		})

	h := newTestHandler(t, store, users)

	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site1",
		Message: model.Message{
			ID: "msg1", RoomID: "room1", Content: "hello",
			UserID: "u1", UserAccount: "alice", UserDisplayName: "Alice Anderson",
			CreatedAt: time.Now().UTC(),
		},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	require.NoError(t, h.processMessage(context.Background(), data, false),
		"a Mongo enrichment failure must not block the Cassandra persist")
	require.NotNil(t, savedSender)
	assert.Equal(t, "u1", savedSender.ID)
	assert.Equal(t, "alice", savedSender.Account)
	assert.Equal(t, "Alice Anderson", savedSender.EngName,
		"the event's composed display name is the fallback for the sender name")
}

func TestProcessMessage_MentionResolveError_PersistsWithoutMentions(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	users := NewMockUserStore(ctrl)

	users.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(nil, errors.New("mongo down")).AnyTimes()
	users.EXPECT().FindUserByAccount(gomock.Any(), "alice").
		Return(&model.User{ID: "u1", Account: "alice", EngName: "Alice"}, nil)

	var saved *model.Message
	store.EXPECT().SaveMessage(gomock.Any(), gomock.Any(), gomock.Any(), "site1").
		DoAndReturn(func(_ context.Context, m *model.Message, _ *cassParticipant, _ string) error {
			saved = m
			return nil
		})

	h := newTestHandler(t, store, users)

	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site1",
		Message: model.Message{
			ID: "msg2", RoomID: "room1", Content: "hey @bob",
			UserID: "u1", UserAccount: "alice", UserDisplayName: "Alice",
			CreatedAt: time.Now().UTC(),
		},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	require.NoError(t, h.processMessage(context.Background(), data, false))
	require.NotNil(t, saved)
	assert.Empty(t, saved.Mentions, "unresolved mentions degrade to empty, not an error")
	assert.Equal(t, "hey @bob", saved.Content, "content including the @token persists intact")
}
```

**If the file has no `newTestHandler` helper**, use whatever constructor the neighbouring tests use (e.g. `NewHandler(store, users, threadStore, "site1", publishFn)`) and pass a `NewMockThreadStore(ctrl)` plus a no-op publish function; keep the call identical to the existing tests so the harness stays consistent.

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=message-worker`
Expected: FAIL — `processMessage` currently returns the wrapped enrichment error and never calls `SaveMessage`.

- [ ] **Step 3: Implement the mention fail-open**

In `message-worker/handler.go`, replace the mention-resolution error branch:

```go
	resolved, err := mention.Resolve(ctx, evt.Message.Content, h.userStore.FindUsersByAccounts)
	if err != nil {
		// Fail-open: mention resolution is enrichment, not durability. The content
		// (including the literal @tokens) persists intact; only the resolved
		// participant list is lost, so a mentioned user may miss a notification
		// during the outage. Blocking the write would be strictly worse.
		slog.WarnContext(ctx, "mention resolution failed, persisting without mentions",
			"error", err, "message_id", evt.Message.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
	} else {
		evt.Message.Mentions = resolved.Participants
	}
```

- [ ] **Step 4: Implement the sender fail-open**

Still in `processMessage`, replace the non-system-message error branch of the user lookup so it projects the sender from the event:

```go
	var sender *cassParticipant
	user, err := h.userStore.FindUserByAccount(ctx, evt.Message.UserAccount)
	switch {
	case err == nil:
		sender = &cassParticipant{
			ID:          user.ID,
			EngName:     user.EngName,
			CompanyName: user.ChineseName,
			Account:     evt.Message.UserAccount,
		}
	case model.IsSystemMessageType(evt.Message.Type):
		// System messages may have no real user; proceed with nil sender.
		slog.WarnContext(ctx, "user not found for system message, using nil sender",
			"account", evt.Message.UserAccount, "type", evt.Message.Type,
			"request_id", natsutil.RequestIDFromContext(ctx))
	default:
		// Fail-open: project the sender from the canonical event, which already
		// carries the identity the gatekeeper resolved at send time. Only the
		// EngName/ChineseName split is lost (UserDisplayName is already the
		// composed render-ready name), so the write proceeds rather than
		// NAK-buffering until Mongo returns.
		slog.WarnContext(ctx, "sender lookup failed, projecting sender from event",
			"error", err, "account", evt.Message.UserAccount,
			"message_id", evt.Message.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		sender = &cassParticipant{
			ID:      evt.Message.UserID,
			EngName: evt.Message.UserDisplayName,
			Account: evt.Message.UserAccount,
		}
	}
```

- [ ] **Step 5: Run to verify the tests pass**

Run: `make test SERVICE=message-worker`
Expected: PASS, including the pre-existing tests. If a pre-existing test asserted the old block-on-error behavior, update it to assert the new fail-open contract and say so in your report — do not delete it.

- [ ] **Step 6: Lint and commit**

```bash
make lint
git add message-worker/handler.go message-worker/handler_test.go
git commit -m "Fail open on message-worker sender and mention enrichment"
```

---

## Task 5: Wire the DEK L2 into message-worker

**Files:**
- Modify: `message-worker/main.go`
- Modify: `message-worker/deploy/docker-compose.yml`

**Interfaces:**
- Consumes: `atrest.NewL2DEKStore`, `atrest.DefaultL2Recorder` (Task 1), `circuitbreaker.New`, `valkeyutil.ConnectCluster`.
- Produces: nothing consumed by later tasks.

**Context:** `message-worker/main.go` has **no Valkey client today** — this task adds one, guarded so an empty `VALKEY_ADDRS` leaves behavior unchanged. Mirror the connect block already used by `message-gatekeeper/main.go` and `history-service/cmd/main.go`.

- [ ] **Step 1: Add the config knobs**

In `message-worker/main.go`, add to the `Config` struct:

```go
	ValkeyAddrs        []string      `env:"VALKEY_ADDRS"    envSeparator:","`
	ValkeyPassword     string        `env:"VALKEY_PASSWORD" envDefault:""`
	DEKL2TTL           time.Duration `env:"ATREST_DEK_L2_TTL"           envDefault:"90m"`
	DEKBreakerFails    int           `env:"ATREST_DEK_BREAKER_FAILS"    envDefault:"5"`
	DEKBreakerCooldown time.Duration `env:"ATREST_DEK_BREAKER_COOLDOWN" envDefault:"10s"`
```

- [ ] **Step 2: Connect Valkey and decorate the DEK store**

In `main()`, add the Valkey connect block **before** the `if cfg.Atrest.Enabled {` block:

```go
	// Valkey fronting the at-rest DEK L2. Empty VALKEY_ADDRS disables the tier
	// (the DEK store falls straight through to Mongo, as before).
	var dekValkey valkeyutil.Client
	if len(cfg.ValkeyAddrs) > 0 {
		dekValkey, err = valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword,
			valkeyutil.WithObservability(sdk),
			valkeyutil.WithRequireParentSpan(true),
		)
		if err != nil {
			slog.Error("valkey connect (dek L2) failed", "error", err)
			os.Exit(1)
		}
		slog.Info("at-rest DEK L2 cache enabled", "ttl", cfg.DEKL2TTL)
	}
```

Then, inside the `if cfg.Atrest.Enabled {` block, replace the `cipher = atrest.NewCipher(...)` line:

```go
		dekBreaker := circuitbreaker.New(cfg.DEKBreakerFails, cfg.DEKBreakerCooldown)
		dekStore := atrest.NewL2DEKStore(atrest.NewMongoDEKStore(dekColl), dekValkey,
			cfg.DEKL2TTL, dekBreaker, atrest.DefaultL2Recorder())
		cipher = atrest.NewCipher(w, dekStore, cfg.Atrest)
```

**Verified context:** `message-worker/main.go:142` already declares `dekColl := db.Collection(atrest.CollectionName)` and `:143` is the `cipher = atrest.NewCipher(w, atrest.NewMongoDEKStore(dekColl), cfg.Atrest)` line to replace — reuse `dekColl`, do not re-declare it. Add the imports `"github.com/hmchangw/chat/pkg/circuitbreaker"` and `"github.com/hmchangw/chat/pkg/valkeyutil"`.

- [ ] **Step 3: Add Valkey to the local compose**

In `message-worker/deploy/docker-compose.yml`, add to the `environment:` list. Verified: this file uses the overridable `${VAR:-default}` form (e.g. `MONGO_URI=${MONGO_URI:-mongodb://mongodb:27017}` at `:20`), so match that form:

```yaml
      # Shared Valkey cluster fronts the at-rest DEK L2 so the Cassandra write
      # path survives a Mongo outage. Empty disables the tier (fail-open).
      - VALKEY_ADDRS=${VALKEY_ADDRS:-valkey:6379}
```

- [ ] **Step 4: Build, test, lint**

Run: `go build ./message-worker/...`
Run: `make test SERVICE=message-worker`
Run: `make lint`
Run: `python3 -c "import yaml; yaml.safe_load(open('message-worker/deploy/docker-compose.yml')); print('YAML OK')"`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add message-worker/main.go message-worker/deploy/docker-compose.yml
git commit -m "Wire the at-rest DEK L2 into message-worker"
```

---

## Task 6: Verification pass

**Files:** none (verification only).

- [ ] **Step 1: Build everything**

Run: `go build ./...`
Run: `make build SERVICE=message-worker && make build SERVICE=history-service`
Expected: clean.

- [ ] **Step 2: Regenerate mocks if any mocked interface changed**

The `DEKStore` interface is unchanged (the L2 is a decorator), so no regeneration should be needed. Confirm:
Run: `make generate SERVICE=message-worker && make generate SERVICE=history-service`
Run: `git status --porcelain | grep -i mock || echo "NO MOCK DRIFT"`
Expected: `NO MOCK DRIFT`.

- [ ] **Step 3: Coverage on the new code**

Run: `go test -race -coverprofile=/tmp/atrest.cover ./pkg/atrest/... && go tool cover -func=/tmp/atrest.cover | grep -E "dek_store_l2|total:"`
Expected: the `dek_store_l2.go` functions at or near 100%; package total ≥80%. Add table cases for any uncovered branch.

- [ ] **Step 4: Full unit suites (race)**

Run: `go test -race -count=1 ./pkg/atrest/... ./message-worker/... ./history-service/...`
Expected: all `ok`. Note: `history-service/internal/cassrepo` has a **pre-existing flaky data race** in its `walker_test.go` test fake that also reproduces on `main` — if it trips, re-run that package alone to confirm, and report it as pre-existing rather than fixing it here.

- [ ] **Step 5: Integration compile + SAST**

Run: `go vet -tags=integration ./pkg/atrest/... ./message-worker/... ./history-service/...`
Run: `make sast-gosec`
Expected: vet exit 0; gosec clean. (`govulncheck`/`semgrep` may be network-blocked — defer those to CI and say so.)

- [ ] **Step 6: Final lint + commit any fixups**

```bash
make lint
git add -A
git commit -m "Fixups from verification pass"
```

---

## Self-Review

**Spec coverage:**
- Mechanism 1 (DEK Valkey L2, wrapped-only, fail-open, positive-only, breaker-guarded) → Task 1; integration proof → Task 2. ✓
- Read-path restoration (`history-service`, reusing #188's Valkey client, separate DEK breaker) → Task 3. ✓
- Write-path enrichment fail-opens (sender, mentions) → Task 4. ✓
- Write-path DEK L2 wiring + compose → Task 5. ✓
- Sliding TTL while degraded (so an outage of any length is survivable, not just one TTL window) → Task 1, Step 3 + its test. ✓
- Cold path unchanged (transient error → existing `jsretry.Settle` NAK-backoff on write; error on read) → preserved by construction: Tasks 4/5 add no new error swallowing on the DEK path, and Task 1 returns the wrapped error on a cold miss (tested by `TestL2DEKStore_ColdRoomDuringOutageErrors`). ✓
- Rotation correctness (`Replace`/`Upsert` invalidate) → Task 1 unit tests + Task 2 integration test. ✓
- Observability: `cache="atrestdek",tier="l2"` series via `DefaultL2Recorder` → Task 1. **Gap accepted:** the spec also lists a "decrypt failures attributable to an unresolvable DEK" counter; that lives in `cipher.go`/`cassrepo`, which this plan deliberately does not modify. It is recorded here as a **follow-up**, not silently dropped — the L2 hit/miss/error series plus the breaker gauge already show the outage behavior.
- Config values (`ATREST_DEK_L2_TTL`=90m, `ATREST_DEK_BREAKER_FAILS`=5, `ATREST_DEK_BREAKER_COOLDOWN`=10s) → Tasks 3 and 5, exact. ✓
- Non-goals (thread replies, cold rooms, Vault outages, raising the L1 TTL as a "fix") → no task attempts them. ✓

**Placeholder scan:** No "TBD"/"handle edge cases"/"similar to Task N". Every code step carries complete code. Three assumptions that started as conditionals were verified against the tree and are now stated as facts with line numbers: `pkg/atrest` has no `TestMain` (so Task 2's file defines one), `history-service/cmd/main.go:160/175/184` already orders `subValkey` before the at-rest block and declares `dekColl`, and `message-worker/main.go:142` already declares `dekColl` (compose uses `${VAR:-default}`). The one remaining inspect-then-choose instruction is Task 4's "match the existing test-harness constructor", which carries an explicit fallback.

**Type consistency:** `NewL2DEKStore(inner DEKStore, client valkeyutil.Client, ttl time.Duration, breaker *circuitbreaker.Breaker, rec L2Recorder) DEKStore`, `DEKKey(roomID string) string`, `L2Recorder`, and `DefaultL2Recorder()` are used identically in Tasks 1, 2, 3, and 5. `RoomDataKey{ID, WrappedDEK, CreatedAt}` matches `pkg/atrest/atrest.go`. `cassParticipant{ID, EngName, CompanyName, Account}` matches the field names used in `message-worker/handler.go`. Config field names `DEKL2TTL`/`DEKBreakerFails`/`DEKBreakerCooldown` are identical in Tasks 3 and 5.
