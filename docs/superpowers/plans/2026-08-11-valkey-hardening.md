# Valkey Timeout and Startup Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound Valkey failure latency with timeout profiles and a circuit breaker, and remove the fatal startup PING that crashloops seven services during a Valkey outage.

**Architecture:** All changes concentrate in `pkg/valkeyutil`, which every consumer already depends on. Two named timeout `Profile` values replace go-redis defaults; a `sony/gobreaker` circuit breaker wraps the `Client` interface so a downed Valkey short-circuits instead of burning the timeout budget per call; and a new `ConnectClusterLazy` demotes the startup PING from fatal to a log. Consumers need almost no changes because they already branch on `ErrCacheMiss` and fall back on everything else — the breaker's `ErrUnavailable` lands in the existing fallback path. `botplatform-service` is the exception: it fails open on both Valkey-backed controls so bots keep serving through an outage (see Task 9).

**Tech Stack:** Go 1.25, `github.com/redis/go-redis/v9` v9.18.0, `github.com/sony/gobreaker/v2` v2.4.0 (new), OpenTelemetry `go.opentelemetry.io/otel` v1.44.0, `go.uber.org/mock`, `stretchr/testify`.

**Spec:** `docs/superpowers/specs/2026-08-11-valkey-hardening-design.md`

## Global Constraints

- Branch: `claude/valkey-outage-impact-m5dxjd`. Never push elsewhere.
- TDD is mandatory (CLAUDE.md §4): write the failing test, run it, confirm it fails, then implement. Never write implementation before its test.
- Use `make` targets only — never raw `go` commands. `make test SERVICE=<name>`, `make lint`, `make fmt`, `make sast`. `SERVICE` is a path relative to the repo root, so shared packages use `SERVICE=pkg/valkeyutil` (never `../pkg/...`).
- Two exceptions, both sanctioned and both having no `make` equivalent: `go get` to add the approved dependency (Task 3), and `go test -coverprofile` / `go tool cover` for the coverage check (CLAUDE.md §4 prescribes exactly these). Everything else goes through `make`.
- All tests run with `-race` (the Makefile handles this).
- Minimum 80% coverage per package; `pkg/valkeyutil` is shared infrastructure and targets 90%.
- Logging is `log/slog` with structured key-value fields. Never interpolate. Never log keys, tokens, or message bodies.
- Error wrapping: `fmt.Errorf("short description: %w", err)` describing what the current function was doing. Never bare `err`, never `"error: %w"`.
- New third-party dependency `github.com/sony/gobreaker/v2` is pre-approved by the spec. Add no others.
- Timeout profile values are **code constants**, not environment variables. This is a deliberate, spec-documented departure from CLAUDE.md §6.
- Integration tests need the `//go:build integration` tag and a `TestMain` calling `testutil.RunTests(m)`.
- Commit after each task's tests pass. The pre-commit hook runs lint and tests.

## File Structure

| File | Responsibility |
|------|---------------|
| `pkg/valkeyutil/profile.go` (create) | `Profile` type, `CacheProfile`, `StoreProfile`, `ClusterOptionsFor`. |
| `pkg/valkeyutil/profile_test.go` (create) | Profile → `ClusterOptions` mapping tests. |
| `pkg/valkeyutil/breaker.go` (create) | `breakerClient`, `ErrUnavailable`, `IsUnavailable`, `isSuccessful`. |
| `pkg/valkeyutil/breaker_test.go` (create) | State machine, `isSuccessful` classification, concurrency. |
| `pkg/valkeyutil/metrics.go` (create) | Breaker transition counter + state gauge. |
| `pkg/valkeyutil/valkey.go` (modify) | `ConnectClusterLazy`; `ConnectCluster` uses profiles + breaker. |
| `pkg/valkeyutil/observability.go` (modify) | `WithProfile`, `WithBreakerName`, `WithoutCircuitBreaker` options. |
| `user-presence-service/presencestore/store.go` (modify) | `NewValkeyStoreLazy` using `StoreProfile`. |
| 6 × `main.go` (modify) | Switch to lazy connect, drop `os.Exit`. |
| `pkg/roommetacache/valkey.go`, `notification-worker/members.go`, `search-service/handler.go` (modify) | Gate warn logs on `!IsUnavailable`. |
| `botplatform-service/middleware.go`, `middleware_idempotency.go`, `main.go` (modify) | Unconditional fail-open + lazy connect (supersedes the posture split; see Task 9). |
| `botplatform-service/metrics.go` (create) | `bot_control_bypassed_total` counter. |

---

### Task 1: Timeout profiles

**Files:**
- Create: `pkg/valkeyutil/profile.go`
- Create: `pkg/valkeyutil/profile_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `type Profile struct{ DialTimeout, ReadTimeout, WriteTimeout, PoolTimeout time.Duration; MaxRetries int }`; vars `CacheProfile`, `StoreProfile`; `func ClusterOptionsFor(addrs []string, password string, p Profile) *redis.ClusterOptions`.

Background: `pkg/valkeyutil/valkey.go:40` currently builds `redis.ClusterOptions` with only `Addrs` and `Password`, so go-redis defaults apply — 5s dial, 3s read, 3 retries, and `ContextTimeoutEnabled: false`. That last one means a caller's context deadline does **not** bound the socket read.

- [ ] **Step 1: Write the failing test**

Create `pkg/valkeyutil/profile_test.go`:

```go
package valkeyutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClusterOptionsFor(t *testing.T) {
	tests := []struct {
		name    string
		profile Profile
		wantRead time.Duration
		wantRetries int
	}{
		{"cache profile", CacheProfile, 150 * time.Millisecond, 1},
		{"store profile", StoreProfile, 500 * time.Millisecond, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClusterOptionsFor([]string{"host:6379"}, "pw", tt.profile)

			assert.Equal(t, []string{"host:6379"}, opts.Addrs)
			assert.Equal(t, "pw", opts.Password)
			assert.Equal(t, tt.wantRead, opts.ReadTimeout)
			assert.Equal(t, tt.wantRead, opts.WriteTimeout)
			assert.Equal(t, tt.wantRetries, opts.MaxRetries)
			assert.Equal(t, time.Second, opts.DialTimeout)
			// The critical one: without this, a caller's context deadline does
			// not bound the socket read and the profile buys nothing.
			assert.True(t, opts.ContextTimeoutEnabled, "ContextTimeoutEnabled must be set")
		})
	}
}

func TestProfiles_CacheIsTighterThanStore(t *testing.T) {
	// Presence uses Valkey as its store of record, so it gets more headroom
	// than the cache consumers, which all have a Mongo/ES fallback.
	assert.Less(t, CacheProfile.ReadTimeout, StoreProfile.ReadTimeout)
	assert.Less(t, CacheProfile.MaxRetries, StoreProfile.MaxRetries)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/valkeyutil`
(If the Makefile's `SERVICE` var does not resolve `pkg/` paths, run `make test` and confirm the `pkg/valkeyutil` package fails to build.)
Expected: FAIL — `undefined: Profile`, `undefined: CacheProfile`, `undefined: ClusterOptionsFor`.

- [ ] **Step 3: Write minimal implementation**

Create `pkg/valkeyutil/profile.go`:

```go
package valkeyutil

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// Profile is a bounded timeout budget for a Valkey client. go-redis defaults
// (5s dial, 3s read, 3 retries) are far too loose for a cache on the message
// hot path: against a blackholing Valkey they turn a fail-open read into a
// multi-second stall on every call.
//
// These are code constants rather than environment variables by design (see
// the spec). They are internal tuning, and a wrong value silently reintroduces
// the stall this package exists to prevent.
type Profile struct {
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolTimeout  time.Duration
	MaxRetries   int
}

var (
	// CacheProfile serves consumers that have a backing store to fall back to
	// (room meta, room subscriptions, search restricted-rooms). Failing fast
	// matters more than succeeding slowly — the fallback is always available.
	CacheProfile = Profile{
		DialTimeout:  time.Second,
		ReadTimeout:  150 * time.Millisecond,
		WriteTimeout: 150 * time.Millisecond,
		PoolTimeout:  250 * time.Millisecond,
		MaxRetries:   1,
	}

	// StoreProfile serves user-presence-service, where Valkey is the store of
	// record and no fallback exists. A cache-tight ceiling on a Lua EVAL under
	// load would manufacture failures nothing can absorb.
	StoreProfile = Profile{
		DialTimeout:  time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
		PoolTimeout:  time.Second,
		MaxRetries:   2,
	}
)

// ClusterOptionsFor builds the go-redis cluster options for a profile.
// Exported so user-presence-service/presencestore, which constructs its own
// client for Lua scripting, applies the same budget without duplicating it.
func ClusterOptionsFor(addrs []string, password string, p Profile) *redis.ClusterOptions {
	return &redis.ClusterOptions{
		Addrs:        addrs,
		Password:     password,
		DialTimeout:  p.DialTimeout,
		ReadTimeout:  p.ReadTimeout,
		WriteTimeout: p.WriteTimeout,
		PoolTimeout:  p.PoolTimeout,
		MaxRetries:   p.MaxRetries,
		// Without this, go-redis ignores the caller's context deadline for
		// socket reads and only ReadTimeout applies.
		ContextTimeoutEnabled: true,
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/valkeyutil` (or `make test`)
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/valkeyutil/profile.go pkg/valkeyutil/profile_test.go
git commit -m "feat(valkeyutil): add CacheProfile and StoreProfile timeout budgets"
```

---

### Task 2: Apply profiles in ConnectCluster, prove latency is bounded

**Files:**
- Modify: `pkg/valkeyutil/valkey.go:39-61` (`ConnectCluster`)
- Modify: `pkg/valkeyutil/observability.go:19-22` (`connectConfig`), `:58-66` (`newConnectConfig`)
- Modify: `pkg/valkeyutil/valkey_test.go` (append)

**Interfaces:**
- Consumes: `ClusterOptionsFor`, `CacheProfile`, `StoreProfile` from Task 1.
- Produces: `func WithProfile(p Profile) Option`. `ConnectCluster` keeps its existing signature `(ctx, addrs, password, opts ...Option) (Client, error)` and now defaults to `CacheProfile`.

- [ ] **Step 1: Write the failing test**

Append to `pkg/valkeyutil/valkey_test.go`:

```go
// blackholeListener accepts TCP connections and never responds. This is the
// failure mode that matters: a Valkey that refuses connections fails fast on
// its own, but one that silently drops packets stalls every call until a
// timeout fires. Connection-refused testing never catches it.
func blackholeListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			// Hold the connection open and never write. Closed by t.Cleanup.
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})
	return ln.Addr().String()
}

func TestConnectCluster_BoundedLatencyAgainstBlackhole(t *testing.T) {
	addr := blackholeListener(t)

	start := time.Now()
	_, err := ConnectCluster(context.Background(), []string{addr}, "")
	elapsed := time.Since(start)

	require.Error(t, err, "connect against a blackhole must fail, not hang")
	// The 5s ping ceiling in ConnectCluster is the outer bound; the profile
	// should bring us well inside it. go-redis defaults (5s dial + 3s read x3
	// retries) would blow through this.
	assert.Less(t, elapsed, 6*time.Second, "connect must be bounded by dial/read timeouts")
}

func TestWithProfile_OverridesDefault(t *testing.T) {
	cfg := newConnectConfig(WithProfile(StoreProfile))
	assert.Equal(t, StoreProfile, cfg.profile)
}

func TestNewConnectConfig_DefaultsToCacheProfile(t *testing.T) {
	cfg := newConnectConfig()
	assert.Equal(t, CacheProfile, cfg.profile)
}
```

Add `net`, `time`, `context` to the test file's imports if not already present, plus `require` from `github.com/stretchr/testify/require`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/valkeyutil`
Expected: FAIL — `undefined: WithProfile`, and `cfg.profile` is not a field of `connectConfig`.

- [ ] **Step 3: Write minimal implementation**

In `pkg/valkeyutil/observability.go`, add the field to `connectConfig`:

```go
type connectConfig struct {
	obs       Observability
	redisOpts []o11yredis.Option
	profile   Profile
}
```

Add the option (place it near the other `With*` functions):

```go
// WithProfile selects the timeout budget for the client. Defaults to
// CacheProfile; user-presence-service passes StoreProfile because Valkey is
// its store of record rather than a cache.
func WithProfile(p Profile) Option {
	return func(c *connectConfig) { c.profile = p }
}
```

Change `newConnectConfig` to seed the default:

```go
func newConnectConfig(opts ...Option) connectConfig {
	cfg := connectConfig{profile: CacheProfile}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}
```

In `pkg/valkeyutil/valkey.go`, rewrite the head of `ConnectCluster` so the config is read **before** the client is built:

```go
func ConnectCluster(ctx context.Context, addrs []string, password string, opts ...Option) (Client, error) {
	cc := newConnectConfig(opts...)
	c := redis.NewClusterClient(ClusterOptionsFor(addrs, password, cc.profile))
	if err := instrumentCluster(c, cc); err != nil {
		if closeErr := c.Close(); closeErr != nil {
			slog.Warn("valkey cluster close after failed instrument", "error", closeErr)
		}
		return nil, err
	}
	// ... rest of the function (ping gate) unchanged for now
```

Note the ordering change: the existing code calls `newConnectConfig(opts...)` inline inside `instrumentCluster(c, newConnectConfig(opts...))`. Hoist it to a local so the same config drives both the options and the instrumentation.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/valkeyutil`
Expected: PASS. The blackhole test should complete in roughly 1–2 seconds.

- [ ] **Step 5: Commit**

```bash
git add pkg/valkeyutil/valkey.go pkg/valkeyutil/observability.go pkg/valkeyutil/valkey_test.go
git commit -m "feat(valkeyutil): apply timeout profiles to cluster clients"
```

---

### Task 3: Circuit breaker

**Files:**
- Create: `pkg/valkeyutil/breaker.go`
- Create: `pkg/valkeyutil/breaker_test.go`
- Modify: `go.mod`, `go.sum`

**Interfaces:**
- Consumes: `Client`, `ErrCacheMiss` from `pkg/valkeyutil/valkey.go:18-32`.
- Produces: `var ErrUnavailable error`; `func IsUnavailable(err error) bool`; `func NewBreakerClient(inner Client, name string) Client`; `func isSuccessful(err error) bool` (unexported, tested directly since it is the highest-risk logic in the package).

Background: bounded timeouts still cost ~300ms per call during an outage. At message volume that is its own throughput problem, so the client short-circuits once Valkey is demonstrably down.

- [ ] **Step 1: Add the dependency**

```bash
go get github.com/sony/gobreaker/v2@v2.4.0
```

Then verify it resolved to exactly `v2.4.0`:

```bash
grep gobreaker go.mod
```

- [ ] **Step 2: Write the failing test**

Create `pkg/valkeyutil/breaker_test.go`:

```go
package valkeyutil

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubClient is a Client whose Get result is controlled per call.
type stubClient struct {
	mu   sync.Mutex
	err  error
	val  string
	gets int
}

func (s *stubClient) Get(ctx context.Context, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	return s.val, s.err
}
func (s *stubClient) Set(ctx context.Context, key, value string, ttl time.Duration) error { return s.err }
func (s *stubClient) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return true, s.err
}
func (s *stubClient) IncrEx(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	return 1, s.err
}
func (s *stubClient) Del(ctx context.Context, keys ...string) error { return s.err }
func (s *stubClient) Close() error                                  { return nil }

func (s *stubClient) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}
func (s *stubClient) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

// This is the test that matters most. gobreaker counts every returned error as
// a failure, so a cache miss — which is the cache working correctly — would
// trip the breaker and disable Valkey for a perfectly healthy workload.
func TestIsSuccessful(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is success", nil, true},
		{"cache miss is success, not a failure", ErrCacheMiss, true},
		{"wrapped cache miss is success", fmt.Errorf("valkey get json: %w", ErrCacheMiss), true},
		{"context canceled is success", context.Canceled, true},
		{"transport error is failure", errors.New("dial tcp: i/o timeout"), false},
		{"plain error is failure", errors.New("x"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isSuccessful(tt.err))
		})
	}
}

func TestIsSuccessful_StringSimilarErrorIsStillAFailure(t *testing.T) {
	// Only errors.Is-matching misses count as success. An error that merely
	// reads like a miss must still trip the breaker.
	lookalike := errors.New("valkey get json: " + ErrCacheMiss.Error())
	assert.False(t, isSuccessful(lookalike))
}

func TestBreaker_CacheMissesNeverTrip(t *testing.T) {
	stub := &stubClient{err: ErrCacheMiss}
	c := NewBreakerClient(stub, "test-miss")

	for i := 0; i < 50; i++ {
		_, err := c.Get(context.Background(), "k")
		require.ErrorIs(t, err, ErrCacheMiss)
	}
	assert.Equal(t, 50, stub.callCount(), "every call must reach the inner client")
}

func TestBreaker_TripsAfterConsecutiveFailures(t *testing.T) {
	boom := errors.New("i/o timeout")
	stub := &stubClient{err: boom}
	c := NewBreakerClient(stub, "test-trip")

	for i := 0; i < breakerFailureThreshold; i++ {
		_, err := c.Get(context.Background(), "k")
		require.ErrorIs(t, err, boom)
	}
	inner := stub.callCount()

	// Breaker is now open: calls short-circuit without touching the inner client.
	_, err := c.Get(context.Background(), "k")
	assert.True(t, IsUnavailable(err), "expected ErrUnavailable, got %v", err)
	assert.Equal(t, inner, stub.callCount(), "open breaker must not call inner")
}

func TestBreaker_RecoversAfterCooldown(t *testing.T) {
	boom := errors.New("i/o timeout")
	stub := &stubClient{err: boom}
	c := NewBreakerClient(stub, "test-recover")

	for i := 0; i < breakerFailureThreshold; i++ {
		_, _ = c.Get(context.Background(), "k")
	}
	require.True(t, IsUnavailable(mustErr(c.Get(context.Background(), "k"))))

	// Valkey comes back; wait out the cooldown so the breaker half-opens.
	stub.setErr(nil)
	stub.val = "v"
	time.Sleep(breakerCooldown + 100*time.Millisecond)

	got, err := c.Get(context.Background(), "k")
	require.NoError(t, err)
	assert.Equal(t, "v", got)
}

func TestBreaker_ConcurrentCallersAreSafe(t *testing.T) {
	stub := &stubClient{err: errors.New("i/o timeout")}
	c := NewBreakerClient(stub, "test-concurrent")

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Get(context.Background(), "k")
		}()
	}
	wg.Wait() // -race asserts no data race in the breaker itself
}

func TestBreaker_PassesThroughAllMethods(t *testing.T) {
	stub := &stubClient{val: "v"}
	c := NewBreakerClient(stub, "test-methods")
	ctx := context.Background()

	got, err := c.Get(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "v", got)

	require.NoError(t, c.Set(ctx, "k", "v", time.Minute))

	ok, err := c.SetNX(ctx, "k", "v", time.Minute)
	require.NoError(t, err)
	assert.True(t, ok)

	n, err := c.IncrEx(ctx, "k", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	require.NoError(t, c.Del(ctx, "k"))
	require.NoError(t, c.Close())
}

func mustErr(_ string, err error) error { return err }
```

- [ ] **Step 3: Run test to verify it fails**

Run: `make test SERVICE=pkg/valkeyutil`
Expected: FAIL — `undefined: isSuccessful`, `NewBreakerClient`, `IsUnavailable`, `breakerFailureThreshold`, `breakerCooldown`.

- [ ] **Step 4: Write minimal implementation**

Create `pkg/valkeyutil/breaker.go`:

```go
package valkeyutil

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/sony/gobreaker/v2"
)

const (
	// breakerFailureThreshold rides out a single blip but trips on a real
	// outage.
	breakerFailureThreshold = 5
	// breakerCooldown is how long the breaker stays open before allowing one
	// half-open probe through.
	breakerCooldown = 5 * time.Second
)

// ErrUnavailable is returned when the circuit breaker is open — Valkey is
// known-down and the call short-circuited without a network round-trip.
//
// Every fail-open consumer already branches errors.Is(err, ErrCacheMiss) for
// the miss case and falls back on everything else, so ErrUnavailable lands in
// the fallback path with no consumer change required.
var ErrUnavailable = errors.New("valkey: circuit open")

// IsUnavailable reports whether err came from an open circuit breaker rather
// than a live failure. Call sites use it to suppress per-call warn logs during
// an outage — the breaker already logs each state transition once.
func IsUnavailable(err error) bool {
	return errors.Is(err, ErrUnavailable)
}

// isSuccessful classifies an inner-client error for breaker accounting.
//
// This is the most consequential function in the package. gobreaker treats
// every returned error as a failure, so without this a cold or sparse keyspace
// would trip the breaker on ordinary cache misses and disable Valkey for a
// workload that is behaving perfectly — a self-inflicted outage. Only genuine
// transport failures may count toward the trip threshold.
func isSuccessful(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, ErrCacheMiss) || errors.Is(err, context.Canceled)
}

// breakerClient wraps a Client with one shared circuit breaker. Reachability
// is a property of the connection, not of the command, so all methods share a
// single breaker rather than one per operation.
type breakerClient struct {
	inner Client
	cb    *gobreaker.CircuitBreaker[any]
}

// NewBreakerClient wraps inner with a circuit breaker named name (used in log
// and metric labels).
func NewBreakerClient(inner Client, name string) Client {
	cb := gobreaker.NewCircuitBreaker[any](gobreaker.Settings{
		Name:        name,
		MaxRequests: 1, // one half-open probe, so recovery never stampedes
		Interval:    0, // never auto-reset counts while closed
		Timeout:     breakerCooldown,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= breakerFailureThreshold
		},
		IsSuccessful: isSuccessful,
		OnStateChange: func(name string, from, to gobreaker.State) {
			slog.Warn("valkey circuit breaker state change",
				"breaker", name, "from", from.String(), "to", to.String())
			recordBreakerTransition(name, from.String(), to.String(), to)
		},
	})
	return &breakerClient{inner: inner, cb: cb}
}

// exec runs fn through the breaker, translating gobreaker's own rejection
// errors into ErrUnavailable so consumers have one sentinel to match on.
func (b *breakerClient) exec(fn func() (any, error)) (any, error) {
	v, err := b.cb.Execute(fn)
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return nil, ErrUnavailable
	}
	return v, err
}

func (b *breakerClient) Get(ctx context.Context, key string) (string, error) {
	v, err := b.exec(func() (any, error) { return b.inner.Get(ctx, key) })
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

func (b *breakerClient) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	_, err := b.exec(func() (any, error) { return nil, b.inner.Set(ctx, key, value, ttl) })
	return err
}

func (b *breakerClient) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	v, err := b.exec(func() (any, error) { return b.inner.SetNX(ctx, key, value, ttl) })
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}

func (b *breakerClient) IncrEx(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	v, err := b.exec(func() (any, error) { return b.inner.IncrEx(ctx, key, ttl) })
	if err != nil {
		return 0, err
	}
	return v.(int64), nil
}

func (b *breakerClient) Del(ctx context.Context, keys ...string) error {
	_, err := b.exec(func() (any, error) { return nil, b.inner.Del(ctx, keys...) })
	return err
}

// Close bypasses the breaker — shutdown must not be blocked by an open circuit.
func (b *breakerClient) Close() error { return b.inner.Close() }
```

Note: `recordBreakerTransition` is defined in Task 4. To keep this task compiling on its own, add a temporary no-op at the bottom of `breaker.go` and delete it in Task 4:

```go
// Replaced by the real implementation in metrics.go (Task 4).
func recordBreakerTransition(name, from, to string, state gobreaker.State) {}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=pkg/valkeyutil`
Expected: PASS. `TestBreaker_RecoversAfterCooldown` takes ~5s because it waits out the real cooldown.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum pkg/valkeyutil/breaker.go pkg/valkeyutil/breaker_test.go
git commit -m "feat(valkeyutil): add circuit breaker with cache-miss-aware failure accounting"
```

---

### Task 4: Breaker observability

**Files:**
- Create: `pkg/valkeyutil/metrics.go`
- Modify: `pkg/valkeyutil/breaker.go` (delete the temporary no-op from Task 3)
- Create: `pkg/valkeyutil/metrics_test.go`

**Interfaces:**
- Consumes: `gobreaker.State` from Task 3's `OnStateChange`.
- Produces: `func recordBreakerTransition(name, from, to string, state gobreaker.State)`.

Follow the instrument-init pattern already used by `pkg/roomkeymetrics/roomkeymetrics.go:21-49` — package `init()`, `otel.Meter(...)`, and a `noop` fallback so the program still runs when no MeterProvider is installed.

- [ ] **Step 1: Write the failing test**

Create `pkg/valkeyutil/metrics_test.go`:

```go
package valkeyutil

import (
	"testing"

	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/assert"
)

func TestStateValue(t *testing.T) {
	tests := []struct {
		name  string
		state gobreaker.State
		want  int64
	}{
		{"closed", gobreaker.StateClosed, 0},
		{"half-open", gobreaker.StateHalfOpen, 1},
		{"open", gobreaker.StateOpen, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, stateValue(tt.state))
		})
	}
}

func TestRecordBreakerTransition_NoPanicWithoutMeterProvider(t *testing.T) {
	// Instruments fall back to no-ops when no MeterProvider is installed;
	// recording must stay safe on the hot path regardless.
	assert.NotPanics(t, func() {
		recordBreakerTransition("test", "closed", "open", gobreaker.StateOpen)
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/valkeyutil`
Expected: FAIL — `undefined: stateValue`.

- [ ] **Step 3: Write minimal implementation**

Delete the temporary no-op `recordBreakerTransition` from the bottom of `pkg/valkeyutil/breaker.go`.

Create `pkg/valkeyutil/metrics.go`:

```go
package valkeyutil

import (
	"context"

	"github.com/sony/gobreaker/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

var (
	// breakerTransitions counts circuit breaker state changes, tagged by
	// breaker name and the from/to states.
	breakerTransitions metric.Int64Counter
	// breakerState reports the current state: 0 closed, 1 half-open, 2 open.
	breakerState metric.Int64Gauge
)

func init() {
	m := otel.Meter("valkey")

	var err error
	breakerTransitions, err = m.Int64Counter(
		"valkey_breaker_transitions_total",
		metric.WithDescription("Valkey circuit breaker state transitions, by breaker and from/to state"),
	)
	if err != nil {
		breakerTransitions, _ = noop.NewMeterProvider().Meter("valkey").
			Int64Counter("valkey_breaker_transitions_total")
	}

	breakerState, err = m.Int64Gauge(
		"valkey_breaker_state",
		metric.WithDescription("Current Valkey circuit breaker state: 0 closed, 1 half-open, 2 open"),
	)
	if err != nil {
		breakerState, _ = noop.NewMeterProvider().Meter("valkey").
			Int64Gauge("valkey_breaker_state")
	}
}

// stateValue encodes a breaker state for the gauge.
func stateValue(s gobreaker.State) int64 {
	switch s {
	case gobreaker.StateHalfOpen:
		return 1
	case gobreaker.StateOpen:
		return 2
	default:
		return 0
	}
}

// recordBreakerTransition emits the transition counter and current-state gauge.
func recordBreakerTransition(name, from, to string, state gobreaker.State) {
	ctx := context.Background()
	breakerTransitions.Add(ctx, 1, metric.WithAttributes(
		attribute.String("breaker", name),
		attribute.String("from", from),
		attribute.String("to", to),
	))
	breakerState.Record(ctx, stateValue(state), metric.WithAttributes(
		attribute.String("breaker", name),
	))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/valkeyutil`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/valkeyutil/metrics.go pkg/valkeyutil/metrics_test.go pkg/valkeyutil/breaker.go
git commit -m "feat(valkeyutil): emit circuit breaker transition and state metrics"
```

---

### Task 5: ConnectClusterLazy

**Files:**
- Modify: `pkg/valkeyutil/valkey.go` (add `ConnectClusterLazy`, wire the breaker into both constructors)
- Modify: `pkg/valkeyutil/observability.go` (add `WithBreakerName`, `WithoutCircuitBreaker`)
- Modify: `pkg/valkeyutil/valkey_test.go` (append)

**Interfaces:**
- Consumes: `NewBreakerClient` (Task 3), `ClusterOptionsFor` + profiles (Tasks 1–2).
- Produces: `func ConnectClusterLazy(ctx context.Context, addrs []string, password string, opts ...Option) (Client, error)`; `func WithBreakerName(name string) Option`; `func WithoutCircuitBreaker() Option`.

Background: the crashloop comes entirely from the startup PING at `pkg/valkeyutil/valkey.go:52` being fatal. go-redis already dials lazily and self-heals per call, so no background retry goroutine is needed — and therefore no goroutine termination path to get wrong (CLAUDE.md §3).

- [ ] **Step 1: Write the failing test**

Append to `pkg/valkeyutil/valkey_test.go`:

```go
func TestConnectClusterLazy_ReturnsUsableClientWhenUnreachable(t *testing.T) {
	addr := blackholeListener(t)

	client, err := ConnectClusterLazy(context.Background(), []string{addr}, "")
	require.NoError(t, err, "lazy connect must not fail on an unreachable Valkey")
	require.NotNil(t, client)
	t.Cleanup(func() { _ = client.Close() })

	// The client is usable; the call itself errors rather than panicking.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, getErr := client.Get(ctx, "some-key")
	assert.Error(t, getErr)
}

func TestConnectClusterLazy_BoundedFirstCall(t *testing.T) {
	addr := blackholeListener(t)
	client, err := ConnectClusterLazy(context.Background(), []string{addr}, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	start := time.Now()
	_, _ = client.Get(context.Background(), "k")
	assert.Less(t, time.Since(start), 5*time.Second, "CacheProfile must bound the first call")
}

func TestWithoutCircuitBreaker(t *testing.T) {
	cfg := newConnectConfig(WithoutCircuitBreaker())
	assert.False(t, cfg.breaker)
}

func TestNewConnectConfig_BreakerOnByDefault(t *testing.T) {
	cfg := newConnectConfig()
	assert.True(t, cfg.breaker)
	assert.Equal(t, "valkey", cfg.breakerName)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/valkeyutil`
Expected: FAIL — `undefined: ConnectClusterLazy`, `WithoutCircuitBreaker`; `cfg.breaker` undefined.

- [ ] **Step 3: Write minimal implementation**

In `pkg/valkeyutil/observability.go`, extend `connectConfig` and its defaults:

```go
type connectConfig struct {
	obs         Observability
	redisOpts   []o11yredis.Option
	profile     Profile
	breaker     bool
	breakerName string
}
```

```go
func newConnectConfig(opts ...Option) connectConfig {
	cfg := connectConfig{profile: CacheProfile, breaker: true, breakerName: "valkey"}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}
```

Add the two options:

```go
// WithBreakerName labels the circuit breaker in logs and metrics. Defaults to
// "valkey"; pass the service name when several clients coexist in one process.
func WithBreakerName(name string) Option {
	return func(c *connectConfig) { c.breakerName = name }
}

// WithoutCircuitBreaker disables the breaker. Intended for tests and one-shot
// CLI tools, where short-circuiting adds nothing.
func WithoutCircuitBreaker() Option {
	return func(c *connectConfig) { c.breaker = false }
}
```

In `pkg/valkeyutil/valkey.go`, factor the shared construction and add the lazy variant:

```go
// buildCluster constructs and instruments the raw cluster client.
func buildCluster(addrs []string, password string, cc connectConfig) (*redis.ClusterClient, error) {
	c := redis.NewClusterClient(ClusterOptionsFor(addrs, password, cc.profile))
	if err := instrumentCluster(c, cc); err != nil {
		if closeErr := c.Close(); closeErr != nil {
			slog.Warn("valkey cluster close after failed instrument", "error", closeErr)
		}
		return nil, err
	}
	return c, nil
}

// wrap applies the circuit breaker unless disabled.
func wrap(c *redis.ClusterClient, cc connectConfig) Client {
	base := Client(&clusterClient{c: c})
	if !cc.breaker {
		return base
	}
	return NewBreakerClient(base, cc.breakerName)
}

// ConnectClusterLazy builds an instrumented cluster client without gating on
// reachability. The PING becomes a non-fatal probe: unreachable Valkey is
// logged and a usable Client is still returned.
//
// This is what services must use. go-redis dials lazily and self-heals per
// call, so a Valkey outage no longer prevents a pod from starting — which
// otherwise turns any rollout or scale-up during an outage into a
// CrashLoopBackOff on the message path.
//
// The returned error covers construction and instrumentation failures only.
func ConnectClusterLazy(ctx context.Context, addrs []string, password string, opts ...Option) (Client, error) {
	cc := newConnectConfig(opts...)
	c, err := buildCluster(addrs, password, cc)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Ping(pingCtx).Err(); err != nil {
		slog.Warn("valkey cluster unreachable at startup; continuing with lazy connect",
			"addrs", addrs, "error", err)
	} else {
		slog.Info("connected to Valkey cluster", "addrs", addrs)
	}
	return wrap(c, cc), nil
}
```

Rewrite `ConnectCluster` to reuse the same helpers, keeping its fail-fast contract for the one-shot CLI:

```go
// ConnectCluster dials a Valkey cluster, verifies connectivity with PING, and
// returns a Client. It fails when Valkey is unreachable.
//
// Long-running services must use ConnectClusterLazy instead — a fatal startup
// probe crashloops the pod during a Valkey outage. This variant remains for
// one-shot CLI tools (tools/seed-sample-data), where failing fast is correct.
func ConnectCluster(ctx context.Context, addrs []string, password string, opts ...Option) (Client, error) {
	cc := newConnectConfig(opts...)
	c, err := buildCluster(addrs, password, cc)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Ping(pingCtx).Err(); err != nil {
		if closeErr := c.Close(); closeErr != nil {
			slog.Warn("valkey cluster close after failed connect", "error", closeErr)
		}
		return nil, fmt.Errorf("valkey cluster connect: %w", err)
	}
	slog.Info("connected to Valkey cluster", "addrs", addrs)
	return wrap(c, cc), nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/valkeyutil`
Expected: PASS

- [ ] **Step 5: Run the integration suite to confirm no regression**

Run: `make test-integration SERVICE=pkg/valkeyutil`
Expected: PASS (requires Docker; uses `testutil.SharedValkeyCluster`).

- [ ] **Step 6: Commit**

```bash
git add pkg/valkeyutil/valkey.go pkg/valkeyutil/observability.go pkg/valkeyutil/valkey_test.go
git commit -m "feat(valkeyutil): add ConnectClusterLazy and wire the breaker into constructors"
```

---

### Task 6: Presence store lazy connect

**Files:**
- Modify: `user-presence-service/presencestore/store.go:184-203`
- Modify: `user-presence-service/main.go:95-102`
- Modify: `user-presence-service/presencestore/store_test.go` (create if absent)

**Interfaces:**
- Consumes: `valkeyutil.ClusterOptionsFor`, `valkeyutil.StoreProfile` (Task 1).
- Produces: `func NewValkeyStoreLazy(cfg ClusterConfig, staleThreshold, connsTTL time.Duration) *Store`.

`presencestore` builds its own `*redis.ClusterClient` because it needs Lua scripting, so it cannot use `valkeyutil.Client`. It uses `ClusterOptionsFor` so the timeout budget is not duplicated. Per the spec, presence starts and serves `errcode` errors rather than exiting.

- [ ] **Step 1: Write the failing test**

Create or append to `user-presence-service/presencestore/store_test.go`:

```go
package presencestore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewValkeyStoreLazy_ReturnsStoreWhenUnreachable(t *testing.T) {
	// 203.0.113.0/24 is TEST-NET-3 — reserved, never routable.
	store := NewValkeyStoreLazy(
		ClusterConfig{Addrs: []string{"203.0.113.1:6379"}},
		30*time.Second, time.Minute,
	)
	require.NotNil(t, store, "lazy construction must always yield a store")
	t.Cleanup(func() { _ = store.Close() })
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=user-presence-service`
Expected: FAIL — `undefined: NewValkeyStoreLazy`.

- [ ] **Step 3: Write minimal implementation**

In `user-presence-service/presencestore/store.go`, add below `NewValkeyStore`:

```go
// NewValkeyStoreLazy builds the store without gating on reachability. Valkey
// is presence's store of record, so an outage means requests return errors —
// but the pod must still start, otherwise any rollout during an outage
// crashloops the service. go-redis reconnects on its own.
func NewValkeyStoreLazy(cfg ClusterConfig, staleThreshold, connsTTL time.Duration) *Store {
	c := redis.NewClusterClient(valkeyutil.ClusterOptionsFor(cfg.Addrs, cfg.Password, valkeyutil.StoreProfile))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		slog.Warn("valkey cluster unreachable at startup; continuing with lazy connect",
			"addrs", cfg.Addrs, "error", err)
	}
	return NewValkeyStoreFromClient(c, staleThreshold, connsTTL)
}
```

Add the `valkeyutil` import: `"github.com/hmchangw/chat/pkg/valkeyutil"`.

Also update `NewValkeyStore` to use the same options so the eager path shares the budget:

```go
	c := redis.NewClusterClient(valkeyutil.ClusterOptionsFor(cfg.Addrs, cfg.Password, valkeyutil.StoreProfile))
```

In `user-presence-service/main.go`, replace lines 95–102:

```go
	store := presencestore.NewValkeyStoreLazy(
		presencestore.ClusterConfig{Addrs: cfg.Valkey.Addrs, Password: cfg.Valkey.Password},
		cfg.Presence.StaleThreshold, cfg.Presence.ConnsTTL,
	)
```

Remove the now-unused `err` handling block. Verify `err` is still used later in `main` (it is — `mongoutil.Connect` on the next line reassigns it); if the compiler reports `err` declared and not used, change the Mongo line from `mongoClient, err :=` to keep `:=` since `mongoClient` is new.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=user-presence-service`
Expected: PASS

- [ ] **Step 5: Verify it builds**

Run: `make build SERVICE=user-presence-service`
Expected: success

- [ ] **Step 6: Commit**

```bash
git add user-presence-service/
git commit -m "feat(user-presence-service): lazy Valkey connect with StoreProfile timeouts"
```

---

### Task 7: Switch the five cache services to lazy connect

**Files:**
- Modify: `message-gatekeeper/main.go:104-115`
- Modify: `broadcast-worker/main.go:122-133`
- Modify: `room-worker/main.go:149-160`
- Modify: `notification-worker/main.go:166-173`
- Modify: `search-service/main.go:152-159`

**Interfaces:**
- Consumes: `valkeyutil.ConnectClusterLazy` (Task 5).
- Produces: nothing new.

Each site currently calls `ConnectCluster` and treats failure as fatal. All five use the default `CacheProfile`, so only the constructor name and the error branch change. Add `valkeyutil.WithBreakerName("<service-name>")` so metrics distinguish services.

- [ ] **Step 1: Modify message-gatekeeper**

`message-gatekeeper/main.go`, replacing lines 104–115:

```go
	var metaValkey valkeyutil.Client
	if len(cfg.ValkeyAddrs) > 0 {
		// Lazy: an unreachable Valkey must not stop the gatekeeper from
		// starting — room-meta reads fall back to Mongo.
		metaValkey, err = valkeyutil.ConnectClusterLazy(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword,
			valkeyutil.WithObservability(sdk),
			valkeyutil.WithRequireParentSpan(true),
			valkeyutil.WithBreakerName("message-gatekeeper"),
		)
		if err != nil {
			slog.Error("valkey client build (room-meta L2) failed", "error", err)
			os.Exit(1)
		}
		slog.Info("room-meta L2 cache enabled", "ttl", cfg.RoomMetaL2TTL)
	}
```

The retained `os.Exit(1)` is correct here: `ConnectClusterLazy` only errors on construction/instrumentation failure, which is a programming or configuration fault, not an outage.

- [ ] **Step 2: Modify broadcast-worker**

`broadcast-worker/main.go`, lines 122–133 — identical shape, with `valkeyutil.WithBreakerName("broadcast-worker")` and the log message `"valkey client build (room-meta L2) failed"`.

- [ ] **Step 3: Modify room-worker**

`room-worker/main.go`, lines 149–160 — identical shape, with `valkeyutil.WithBreakerName("room-worker")` and the log message `"valkey client build (room-meta L2 invalidation) failed"`. Keep the trailing `slog.Info("room-meta L2 invalidation enabled")`.

- [ ] **Step 4: Modify notification-worker**

`notification-worker/main.go`, lines 166–173. This site is unconditional (no `len(cfg.ValkeyAddrs) > 0` guard):

```go
	valkeyClient, err := valkeyutil.ConnectClusterLazy(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword,
		valkeyutil.WithObservability(sdk),
		valkeyutil.WithRequireParentSpan(true),
		valkeyutil.WithBreakerName("notification-worker"),
	)
	if err != nil {
		slog.Error("valkey client build failed", "error", err)
		os.Exit(1)
	}
```

- [ ] **Step 5: Modify search-service**

`search-service/main.go`, lines 152–159:

```go
	valkey, err := valkeyutil.ConnectClusterLazy(ctx, cfg.Valkey.Addrs, cfg.Valkey.Password,
		valkeyutil.WithObservability(sdk),
		valkeyutil.WithRequireParentSpan(true),
		valkeyutil.WithBreakerName("search-service"),
	)
	if err != nil {
		slog.Error("valkey client build failed", "error", err)
		os.Exit(1)
	}
```

- [ ] **Step 6: Verify every service builds and tests pass**

```bash
make build SERVICE=message-gatekeeper
make build SERVICE=broadcast-worker
make build SERVICE=room-worker
make build SERVICE=notification-worker
make build SERVICE=search-service
make test
```

Expected: all succeed.

- [ ] **Step 7: Commit**

```bash
git add message-gatekeeper/main.go broadcast-worker/main.go room-worker/main.go notification-worker/main.go search-service/main.go
git commit -m "feat: lazy Valkey connect across the five cache consumers"
```

---

### Task 8: Suppress the per-call warn flood

**Files:**
- Modify: `pkg/roommetacache/valkey.go:45-48`
- Modify: `notification-worker/members.go:45-47`
- Modify: `search-service/handler.go:226-228`
- Modify: `pkg/roommetacache/valkey_test.go` (append)

**Interfaces:**
- Consumes: `valkeyutil.IsUnavailable` (Task 3).
- Produces: nothing new.

An open breaker returns `ErrUnavailable` on every call. All three sites currently log a WARN per call, which during an outage is a log flood at message volume. The breaker already logs each state transition once, so these become redundant.

The `cachemetrics` `Error` recording stays unconditional — the metric is the signal; the log is the noise.

- [ ] **Step 1: Write the failing test**

Append to `pkg/roommetacache/valkey_test.go`. Use the package's existing fake Valkey client and spy recorder — check the top of that file for their names and reuse them rather than defining new ones.

```go
// countingHandler counts emitted log records so the test can assert on log
// volume, which is the actual behavior being changed here.
type countingHandler struct {
	slog.Handler
	warns int
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn {
		h.warns++
	}
	return nil
}
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *countingHandler) WithGroup(string) slog.Handler      { return h }

// withCountingLogger swaps the default slog logger for the duration of a test.
func withCountingLogger(t *testing.T) *countingHandler {
	t.Helper()
	h := &countingHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return h
}

func TestReadL2_UnavailableRecordsErrorButDoesNotLog(t *testing.T) {
	// An open circuit breaker fires on every call. The error metric is the
	// signal and must still fire; the per-call warn is the noise and must not,
	// or an outage floods the logs at message rate.
	logs := withCountingLogger(t)
	rec := &spyRecorder{}
	client := &fakeClient{getErr: valkeyutil.ErrUnavailable}

	meta, found := readL2(context.Background(), client, "room-1", rec)

	assert.False(t, found, "unavailable must fall through to Mongo")
	assert.Equal(t, Meta{}, meta)
	assert.Equal(t, 1, rec.errors, "error metric must still fire")
	assert.Equal(t, 0, logs.warns, "open breaker must not log per call")
}

func TestReadL2_LiveFailureStillLogs(t *testing.T) {
	// A genuine transport failure is rare and diagnostic — keep logging it.
	logs := withCountingLogger(t)
	rec := &spyRecorder{}
	client := &fakeClient{getErr: errors.New("i/o timeout")}

	_, found := readL2(context.Background(), client, "room-1", rec)

	assert.False(t, found)
	assert.Equal(t, 1, rec.errors)
	assert.Equal(t, 1, logs.warns, "live failures must still log")
}
```

Adjust `spyRecorder` and `fakeClient` to match the existing test helpers in the file. If the file has no fake client, add one satisfying `valkeyutil.Client` with a settable `getErr`. Note these two tests mutate the process-wide default logger, so they must not run with `t.Parallel()`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/roommetacache`
Expected: FAIL on `TestReadL2_UnavailableRecordsErrorButDoesNotLog` with `expected 0, got 1` for the warn count — current code logs unconditionally. `TestReadL2_LiveFailureStillLogs` should already pass, which confirms the new test is discriminating rather than trivially true.

- [ ] **Step 3: Write the implementation**

`pkg/roommetacache/valkey.go`, replacing lines 45–48:

```go
	rec.Error(ctx)
	// An open circuit breaker fires on every call; the breaker already logs
	// each state transition once, so logging here would flood at message rate.
	if !valkeyutil.IsUnavailable(err) {
		slog.WarnContext(ctx, "room meta L2 read failed, falling back to mongo",
			"room_id", roomID, "error", err)
	}
	return Meta{}, false
```

Apply the same gate to the populate warning at `pkg/roommetacache/valkey.go:73-76` and to `BustMeta` at `:88-91`.

`notification-worker/members.go`, replacing lines 45–47:

```go
	} else if !errors.Is(err, valkeyutil.ErrCacheMiss) && !valkeyutil.IsUnavailable(err) {
		slog.Warn("roomsubcache get failed, falling back to mongo", "error", err, "roomId", roomID)
	}
```

Note the `Set` warning at `members.go:61-63` and the `Invalidate` warning at `:79-81` need the same gate.

`search-service/handler.go`, replacing lines 226–228:

```go
	if cerr != nil && !valkeyutil.IsUnavailable(cerr) {
		slog.Warn("valkey read failed; falling through to ES", "account", account, "error", cerr)
	}
```

Leave the rest of `loadRestricted` untouched — the `cache_err=%v` fold at line 240 and the `cerr == nil` guard on the SET at line 254 are both still correct.

Add the `valkeyutil` import to `notification-worker/members.go` (already present) and `search-service/handler.go` (add if absent).

- [ ] **Step 4: Run tests to verify they pass**

```bash
make test SERVICE=pkg/roommetacache
make test SERVICE=notification-worker
make test SERVICE=search-service
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/roommetacache/ notification-worker/members.go search-service/handler.go
git commit -m "feat: suppress per-call Valkey warns while the circuit breaker is open"
```

---

### Task 9: botplatform-service posture split

> **SUPERSEDED — implemented as unconditional fail-open.** The posture split below kept the
> three room-management endpoints fail-closed. That was overturned by an explicit requirement
> that bots keep serving through a Valkey outage, plus two findings:
>
> 1. Only `POST /rooms` carries real duplicate cost. `members/add` and `members/remove` are
>    naturally idempotent (`room-service` resolves net-new members; removing an absent member
>    is a no-op), and DM rooms dedupe via the deterministic `idgen.BuildDMRoomID`.
> 2. The sentinel never provided durable dedup anyway — it releases on non-5xx and re-keys
>    every 60s bucket, so it only ever guarded *overlapping* retries. Failing open widens a
>    window already open in healthy operation.
>
> Shipped instead: `ConnectClusterLazy` + fail-open on both middlewares, no `failOpen`
> parameter and no `routes.go` change. The `bot_control_bypassed_total` counter below was
> kept as specified. See `progress.md` for the full analysis.

**Files:**
- Create: `botplatform-service/metrics.go`
- Modify: `botplatform-service/middleware.go:107-156` (`botRateLimit`)
- Modify: `botplatform-service/middleware_idempotency.go:38-95` (`botIdempotency`)
- Modify: `botplatform-service/routes.go:22-32`
- Modify: `botplatform-service/middleware_ratelimit_test.go`, `botplatform-service/middleware_idempotency_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks (the middlewares take `incrExClient` / `sentinelClient`, not `valkeyutil.Client`).
- Produces: `func botRateLimit(client incrExClient, perCaller, perGlobal int, failOpen bool) gin.HandlerFunc`; `func botIdempotency(client sentinelClient, siteID, endpoint string, sentinelTTL time.Duration, resourceIDFrom resourceIDFunc, tp timeProvider, failOpen bool) gin.HandlerFunc`; `func recordControlBypassed(ctx context.Context, control string)`.

Per the spec: rate limiting is a **protective** control and is suspended on Valkey error; idempotency is a **correctness** control and splits — message send fails open, room management stays fail-closed.

- [ ] **Step 1: Write the failing tests**

Append to `botplatform-service/middleware_ratelimit_test.go`:

```go
func TestBotRateLimit_FailOpenOnValkeyError(t *testing.T) {
	// Rate limiting protects the platform, not the data. During a Valkey
	// outage availability wins: the request proceeds.
	client := &stubIncrEx{err: errors.New("i/o timeout")}
	called := false

	r := gin.New()
	r.POST("/x", withBotPrincipal("bot-1"), botRateLimit(client, 10, 100, true), func(c *gin.Context) {
		called = true
		c.Status(http.StatusOK)
	})

	w := performPost(t, r, "/x", `{}`)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, called, "handler must run when the limiter fails open")
}

func TestBotRateLimit_FailClosedStillAvailable(t *testing.T) {
	client := &stubIncrEx{err: errors.New("i/o timeout")}
	r := gin.New()
	r.POST("/x", withBotPrincipal("bot-1"), botRateLimit(client, 10, 100, false), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := performPost(t, r, "/x", `{}`)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestBotRateLimit_LimitStillEnforcedWhenValkeyHealthy(t *testing.T) {
	// Fail-open must not weaken the limit when Valkey is up.
	client := &stubIncrEx{n: 11}
	r := gin.New()
	r.POST("/x", withBotPrincipal("bot-1"), botRateLimit(client, 10, 100, true), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := performPost(t, r, "/x", `{}`)
	assert.Equal(t, http.StatusTooManyRequests, w.Code)
}
```

Append to `botplatform-service/middleware_idempotency_test.go`:

```go
func TestBotIdempotency_MessageSendFailsOpen(t *testing.T) {
	client := &stubSentinel{setNXErr: errors.New("i/o timeout")}
	called := false

	r := gin.New()
	r.POST("/x", withBotPrincipal("bot-1"),
		botIdempotency(client, "site-a", "sendRoom", time.Minute, func(*gin.Context) string { return "r1" }, nil, true),
		func(c *gin.Context) { called = true; c.Status(http.StatusOK) })

	w := performPost(t, r, "/x", `{"text":"hi"}`)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, called, "message send must proceed without the sentinel")
}

func TestBotIdempotency_RoomMgmtFailsClosed(t *testing.T) {
	// A duplicate room creation or member add is expensive and awkward to
	// undo, so these stay fail-closed even during an outage.
	client := &stubSentinel{setNXErr: errors.New("i/o timeout")}
	called := false

	r := gin.New()
	r.POST("/x", withBotPrincipal("bot-1"),
		botIdempotency(client, "site-a", "createRoom", time.Minute, func(*gin.Context) string { return "" }, nil, false),
		func(c *gin.Context) { called = true; c.Status(http.StatusOK) })

	w := performPost(t, r, "/x", `{"name":"r"}`)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.False(t, called, "room management must not proceed without the sentinel")
}

func TestBotIdempotency_FailOpenStillRejectsInFlightDuplicate(t *testing.T) {
	// Fail-open applies only to Valkey errors, never to a healthy refusal.
	client := &stubSentinel{acquired: false}
	r := gin.New()
	r.POST("/x", withBotPrincipal("bot-1"),
		botIdempotency(client, "site-a", "sendRoom", time.Minute, func(*gin.Context) string { return "r1" }, nil, true),
		func(c *gin.Context) { c.Status(http.StatusOK) })

	w := performPost(t, r, "/x", `{"text":"hi"}`)
	assert.Equal(t, http.StatusConflict, w.Code) // whatever errBotInFlight maps to
}
```

Reuse the existing stubs and helpers in those test files (`stubIncrEx`, `stubSentinel`, `withBotPrincipal`, `performPost` or their real names — check the file headers). If a helper does not exist, add it in the test file. Confirm the expected status for `errBotInFlight` by reading its definition in `botplatform-service/helper.go` and correct the last assertion to match.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=botplatform-service`
Expected: FAIL — too many arguments to `botRateLimit` and `botIdempotency`.

- [ ] **Step 3: Add the bypass metric**

Create `botplatform-service/metrics.go`:

```go
package main

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// controlBypassed counts requests that skipped a Valkey-backed control because
// Valkey was unavailable. A non-zero rate means abuse protection or duplicate
// suppression is off and should be alerted on.
var controlBypassed metric.Int64Counter

func init() {
	m := otel.Meter("botplatform")
	var err error
	controlBypassed, err = m.Int64Counter(
		"bot_control_bypassed_total",
		metric.WithDescription("Bot requests that bypassed a Valkey-backed control due to Valkey unavailability"),
	)
	if err != nil {
		controlBypassed, _ = noop.NewMeterProvider().Meter("botplatform").
			Int64Counter("bot_control_bypassed_total")
	}
}

// recordControlBypassed emits the bypass counter for the named control.
func recordControlBypassed(ctx context.Context, control string) {
	controlBypassed.Add(ctx, 1, metric.WithAttributes(attribute.String("control", control)))
}
```

- [ ] **Step 4: Implement the rate-limit fail-open path**

In `botplatform-service/middleware.go`, change the signature and both error branches:

```go
// botRateLimit enforces per-caller then global fixed-window counters (60s each); 0 disables.
// Per-caller first so a rejected caller doesn't consume the global budget.
//
// failOpen governs behavior when Valkey itself errors. Rate limiting is a
// protective control — it guards the platform, not the data — so during a
// Valkey outage availability wins and the request proceeds. A suspended
// control is never silent: it logs and increments bot_control_bypassed_total.
func botRateLimit(client incrExClient, perCaller, perGlobal int, failOpen bool) gin.HandlerFunc {
	const window = time.Minute

	return func(c *gin.Context) {
		if perCaller <= 0 && perGlobal <= 0 {
			c.Next()
			return
		}

		ctx := c.Request.Context()

		pr := botPrincipalFrom(c)
		if pr == nil {
			errhttp.Write(ctx, c, errcode.Internal("bot rate limit: missing principal"))
			c.Abort()
			return
		}

		// bypass handles a Valkey failure per the configured posture. It
		// reports true when the request should proceed uncounted.
		bypass := func(scope string, err error) bool {
			if !failOpen {
				errhttp.Write(ctx, c, errcode.Internal("bot rate limit "+scope, errcode.WithCause(err)))
				c.Abort()
				return false
			}
			slog.WarnContext(ctx, "bot rate limit suspended: valkey unavailable",
				"scope", scope, "error", err)
			recordControlBypassed(ctx, "ratelimit")
			return true
		}

		if perCaller > 0 {
			n, err := client.IncrEx(ctx, "botrl:caller:"+pr.UserID, window)
			if err != nil {
				if !bypass("caller", err) {
					return
				}
			} else if n > int64(perCaller) {
				c.Header("Retry-After", "60")
				errhttp.Write(ctx, c, errBotRateLimitedCaller)
				c.Abort()
				return
			}
		}

		if perGlobal > 0 {
			n, err := client.IncrEx(ctx, "botrl:global", window)
			if err != nil {
				if !bypass("global", err) {
					return
				}
			} else if n > int64(perGlobal) {
				c.Header("Retry-After", "60")
				errhttp.Write(ctx, c, errBotRateLimitedGlobal)
				c.Abort()
				return
			}
		}

		c.Next()
	}
}
```

Add `"log/slog"` to the imports if absent.

- [ ] **Step 5: Implement the idempotency posture split**

In `botplatform-service/middleware_idempotency.go`, add the parameter and change only the `SetNX` error branch:

```go
// botIdempotency is a Valkey-backed sentinel: SET NX per opID, then Del on non-5xx (5xx keeps
// the sentinel so a retry can't race the still-running original). Response body is not cached.
//
// failOpen governs behavior when Valkey itself errors, and differs by endpoint
// class. Message send passes true: a duplicate message is visible but
// recoverable, and the bot write API stays up. Room management passes false: a
// duplicate room creation or member add is expensive and awkward to undo, so
// those endpoints keep returning errcode.Internal.
//
// failOpen never applies to a healthy refusal — an unacquired sentinel is
// still a rejected in-flight duplicate.
func botIdempotency(
	client sentinelClient,
	siteID, endpoint string,
	sentinelTTL time.Duration,
	resourceIDFrom resourceIDFunc,
	tp timeProvider,
	failOpen bool,
) gin.HandlerFunc {
```

Replace the `SetNX` error branch:

```go
		acquired, err := client.SetNX(ctx, key, "processing", sentinelTTL)
		if err != nil {
			if !failOpen {
				errhttp.Write(ctx, c, errcode.Internal("bot idempotency: acquire", errcode.WithCause(err)))
				c.Abort()
				return
			}
			slog.WarnContext(ctx, "bot idempotency suspended: valkey unavailable",
				"endpoint", endpoint, "error", err)
			recordControlBypassed(ctx, "idempotency")
			c.Next()
			return
		}
```

The `c.Next(); return` is deliberate: with no sentinel acquired there is nothing to release, so the trailing `Del` block must be skipped.

- [ ] **Step 6: Wire the postures in routes.go**

In `botplatform-service/routes.go`, replace lines 24–32:

```go
	if valkey != nil {
		// Rate limiting is protective — suspend it rather than reject traffic
		// when Valkey is down.
		rateLimit = botRateLimit(valkey, cfg.BotRateLimitPerCallerPerMin, cfg.BotRateLimitGlobalPerMin, true)
		// Message send fails open: a duplicate message is recoverable.
		msgIdem = func(endpoint string, resourceFrom resourceIDFunc) gin.HandlerFunc {
			return botIdempotency(valkey, cfg.SiteID, endpoint, cfg.BotIdempotencyMsgTTL, resourceFrom, nil, true)
		}
		// Room management stays fail-closed: a duplicate create or member add
		// is expensive and awkward to undo.
		roomMgmtIdem = func(endpoint string, resourceFrom resourceIDFunc) gin.HandlerFunc {
			return botIdempotency(valkey, cfg.SiteID, endpoint, cfg.BotIdempotencyRoomMgmtTTL, resourceFrom, nil, false)
		}
	}
```

- [ ] **Step 7: Switch botplatform to lazy connect**

In `botplatform-service/main.go`, replace lines 79–86:

```go
	var valkey valkeyutil.Client
	if len(cfg.ValkeyAddrs) > 0 {
		valkey, err = valkeyutil.ConnectClusterLazy(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword,
			valkeyutil.WithObservability(sdk),
			valkeyutil.WithRequireParentSpan(true),
			valkeyutil.WithBreakerName("botplatform-service"),
		)
		if err != nil {
			return fmt.Errorf("build valkey client: %w", err)
		}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `make test SERVICE=botplatform-service`
Expected: PASS

- [ ] **Step 9: Verify the build**

Run: `make build SERVICE=botplatform-service`
Expected: success

- [ ] **Step 10: Commit**

```bash
git add botplatform-service/
git commit -m "feat(botplatform-service): suspend rate limiting and split idempotency posture on Valkey outage"
```

---

### Task 10: Full verification and documentation

**Files:**
- Modify: `docs/health-probes.md` (append a short section)

**Interfaces:**
- Consumes: everything above.
- Produces: nothing.

No `docs/client-api.md` update is required: nothing in this plan changes a client-facing request/response schema or a `pkg/model` wire struct. The `botplatform-service` HTTP endpoints keep their shapes — only which status they return when Valkey is down, which is already covered by the generic error envelope.

- [ ] **Step 1: Run the full unit suite**

Run: `make test`
Expected: PASS across all packages.

- [ ] **Step 2: Check coverage on the new package**

```bash
go test -coverprofile=/tmp/valkeyutil.out ./pkg/valkeyutil/ && go tool cover -func=/tmp/valkeyutil.out | tail -1
```

Expected: 90% or above. If below, add table cases to `breaker_test.go` for the untested method wrappers before proceeding.

- [ ] **Step 3: Run lint and formatting**

```bash
make fmt
make lint
```

Expected: clean.

- [ ] **Step 4: Run SAST**

Run: `make sast`
Expected: no medium-or-above findings. This is a blocking CI gate.

- [ ] **Step 5: Run integration tests for the touched packages**

```bash
make test-integration SERVICE=pkg/valkeyutil
make test-integration SERVICE=pkg/roommetacache
make test-integration SERVICE=botplatform-service
make test-integration SERVICE=user-presence-service
```

Expected: PASS (requires Docker).

- [ ] **Step 6: Document the behavior change**

Append to `docs/health-probes.md`, after the "Liveness" section:

```markdown
## Valkey is never a startup gate

Services build their Valkey client through `valkeyutil.ConnectClusterLazy`,
which logs an unreachable cluster and returns a usable client rather than
failing. This is deliberate and mirrors the readiness reasoning above: a shared
datastore is the same for every replica, so making it fatal at startup means a
Valkey outage that overlaps a rollout, scale-up, or node drain crashloops every
pod at once — including the message path.

Cache consumers degrade to Mongo or Elasticsearch; `user-presence-service`
returns `errcode` errors until Valkey returns. A circuit breaker
(`valkey_breaker_state`) short-circuits calls while the cluster is down so the
degraded path does not pay a timeout per request.

`valkeyutil.ConnectCluster` retains the fail-fast behavior and is used only by
one-shot CLI tools.
```

- [ ] **Step 7: Commit and push**

```bash
git add docs/health-probes.md
git commit -m "docs(health-probes): record that Valkey is never a startup gate"
git push -u origin claude/valkey-outage-impact-m5dxjd
```

---

## Self-Review Notes

**Spec coverage.** Timeout profiles → Task 1–2. Circuit breaker including the `IsSuccessful` hazard → Task 3. Observability (`valkey_breaker_transitions`, `valkey_breaker_state`, `bot_control_bypassed`) → Tasks 4 and 9. Lazy connect across all seven services → Tasks 5–7 and 9 step 7. Log-flood gating at the three named call sites → Task 8. botplatform posture split → Task 9. Blackhole latency test → Task 2. Testing requirements → distributed, with the full gate in Task 10.

**Known ordering constraint.** Task 3 introduces a temporary no-op `recordBreakerTransition` so it compiles standalone; Task 4 step 3 deletes it. Do not run Task 4 before Task 3.

**Type consistency.** `Profile`, `CacheProfile`, `StoreProfile`, and `ClusterOptionsFor` are defined in Task 1 and consumed under those exact names in Tasks 2, 5, and 6. `ErrUnavailable` / `IsUnavailable` / `NewBreakerClient` are defined in Task 3 and consumed in Tasks 5 and 8. `recordBreakerTransition(name, from, to string, state gobreaker.State)` has the same five-part signature in its Task 3 stub and its Task 4 implementation. `botRateLimit` and `botIdempotency` gain their `failOpen bool` as the final parameter in Task 9 and are called with it in the same task.

**Deliberate departures from the codebase norm, both spec-sanctioned.** Timeout profiles are code constants rather than `caarlos0/env` config (CLAUDE.md §6), and the circuit breaker is on by default in both constructors, so existing `ConnectCluster` callers get it without opting in. Reviewers will stop on both; the reasoning is in the spec.
