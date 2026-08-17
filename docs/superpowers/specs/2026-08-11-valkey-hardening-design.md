# Valkey Timeout and Startup Hardening

An audit of the Valkey surface asked whether a Valkey outage brings the system
down. It does not — the cache consumers are all written fail-open and degrade to
Mongo or Elasticsearch. But two mechanical traps sit underneath that correctness
and can convert a cache incident into a chat outage. This spec closes both.

Neither trap is a logic bug in the consumers. Both live in how the client is
constructed.

## The two traps

**The timeout trap.** `valkeyutil.ConnectCluster` builds its `redis.ClusterOptions`
with only `Addrs` and `Password` (`pkg/valkeyutil/valkey.go:40`), and
`presencestore.NewValkeyStore` does the same (`user-presence-service/presencestore/store.go:193`).
Every timeout is therefore a go-redis default: 5s dial, 3s read, 3 retries. Against
a Valkey that *refuses* connections this is harmless — the dial fails instantly. Against
one that **blackholes** packets, which is the more common degraded mode, every
hot-path GET burns seconds before the Mongo fallback runs. Per message. On the
JetStream consumers that means throughput collapse, ack-pending saturation, and a
growing stream backlog.

The code is fail-open. The network behavior is fail-slow. Fail-open only works if
it fails fast.

**The startup trap.** `ConnectCluster` gates on a `PING` and returns an error on
failure (`pkg/valkeyutil/valkey.go:52`); every caller treats that as fatal. Seven
services exit: `notification-worker/main.go:170` and `search-service/main.go:156`
unconditionally, and `message-gatekeeper/main.go:110`, `broadcast-worker/main.go:128`,
`room-worker/main.go:155`, `botplatform-service/main.go:84`,
`user-presence-service/main.go:100` whenever `VALKEY_ADDRS` is set — which
production sets.

Running pods survive an outage, because liveness is process-up-only and readiness
probes NATS alone (`docs/health-probes.md`). But any rollout, HPA scale-up, node
drain, or OOM-kill during the outage puts the core message path —
`message-gatekeeper` and `broadcast-worker` — into `CrashLoopBackOff`. A cache
outage that happens to overlap a deploy becomes a chat outage.

## Scope

- Bounded, profile-driven timeouts on every Valkey client.
- A circuit breaker so the degraded path costs approximately nothing.
- Removal of the fatal startup `PING` gate across all seven services.
- Two posture changes in `botplatform-service`, described below.

### Out of scope

- The fail-open behavior of the cache consumers (`roommetacache`, `roomsubcache`,
  `search-service`). It is already correct; this spec only makes it fast.
- Valkey topology, replication, and failover — an ops concern.
- `user-presence-service`'s lack of a fallback store. Valkey is legitimately its
  store of record; serving errors is the right failure mode, not a design defect.

## Timeout profiles

Two named profiles in `pkg/valkeyutil`, selected per call site.

| Knob | `CacheProfile` | `StoreProfile` | go-redis default |
|------|---------------|----------------|------------------|
| `DialTimeout` | 1s | 1s | 5s |
| `ReadTimeout` / `WriteTimeout` | 150ms | 500ms | 3s |
| `MaxRetries` | 1 | 2 | 3 |
| `PoolTimeout` | 250ms | 1s | ~`ReadTimeout`+1s |
| `ContextTimeoutEnabled` | true | true | **false** |

`CacheProfile` serves the five cache consumers. `StoreProfile` serves
`user-presence-service`, where Valkey is the store of record rather than a cache —
a 150ms ceiling on a Lua `EVAL` under load would manufacture failures that no
fallback can absorb.

`ContextTimeoutEnabled` is the least obvious and most important entry. With it
false, which is today's state, a caller's `context.WithTimeout` does **not** bound
the socket read. The 10s `fetchTimeout` guards in `pkg/roommetacache/roommetacache.go:34`
and `notification-worker/members.go:18` are consequently the only ceiling on a
hung Valkey call, and they are two orders of magnitude above the intended budget.

These are code constants, not environment variables. CLAUDE.md §6 requires
deployment configuration to come from the environment, but these are internal
tuning constants: exposing them would add seven services' worth of surface that
nobody will set correctly under incident pressure, and a wrong value reintroduces
exactly the trap being closed.

## Circuit breaker

Bounded timeouts still cost roughly 300ms on every hot-path call during an
outage. At message volume that is its own throughput problem, so the client
short-circuits once Valkey is demonstrably down.

`github.com/sony/gobreaker/v2`, pinned at `v2.4.0`. This is a new third-party
dependency, approved during design in preference to a hand-rolled breaker.

One `*gobreaker.CircuitBreaker[any]` per `Client` instance, shared across all five
interface methods — reachability is a property of the connection, not of the
command. Values box through `any`, which is free relative to a network round-trip.

| Setting | Value | Reason |
|---------|-------|--------|
| `ReadyToTrip` | `ConsecutiveFailures >= 5` | Rides out a single blip; trips on a real outage. |
| `Timeout` | 5s | Open → half-open cooldown. |
| `MaxRequests` | 1 | One half-open probe, so recovery never stampedes. |
| `Interval` | 0 | No auto-reset while closed; consecutive-failure semantics. |
| `OnStateChange` | transition log + metric | See Observability. |

### Error classification is load-bearing

gobreaker's default counts every returned error as a failure.
`valkeyutil.ErrCacheMiss` is not a failure — it is the cache working correctly —
and a cold or sparse keyspace would otherwise trip the breaker and disable
Valkey for a workload that is behaving perfectly. `Settings.IsSuccessful` must
classify `ErrCacheMiss` as a success. Only transport errors count toward the
trip threshold.

Some errors are evidence of nothing either way, and for those *both* scores are
wrong. Counting them failures opens the breaker on a healthy cache and stampedes
the fallback store; counting them successes clears `ConsecutiveFailures` and can
hold the breaker closed straight through a real outage. gobreaker v2 provides a
third outcome for exactly this — `Settings.IsExcluded` drops a call from the
accounting entirely — and it is consulted before `IsSuccessful`. Two errors are
excluded:

| Error | Why it is excluded |
|---|---|
| `context.Canceled` | The caller went away. An outage generates these in bulk — one slow sibling makes an errgroup cancel all the others at once. |
| `redis.ErrPoolTimeout` | Local saturation: our own concurrency outran the pool, so Valkey was never asked. |

`context.DeadlineExceeded` is deliberately **not** excluded. With
`ContextTimeoutEnabled` bounding socket reads, a deadline is precisely how a
blackholing Valkey surfaces — the degraded mode this design targets — so it must
keep counting as a failure.

This is the single detail most likely to be got wrong, and each way of getting
it wrong disables the protection in a different direction. Both predicates get
dedicated tests, including a test that an excluded error does not reset the
consecutive-failure count.

### Callers need almost no changes

When open, the breaker surfaces `gobreaker.ErrOpenState` / `ErrTooManyRequests`,
which `valkeyutil` maps to an exported `ErrUnavailable` alongside a new
`valkeyutil.IsUnavailable(err)` helper.

Every fail-open consumer already branches `errors.Is(err, ErrCacheMiss)` → treat
as miss, everything else → log and fall back. `ErrUnavailable` lands in the
fallback branch with no code change at all.

The one genuine edit is log volume: a per-message WARN during an outage is a log
flood. The breaker logs the closed→open and open→closed transitions once, and the
three existing call-site warnings are gated on `!IsUnavailable(err)`:

- `pkg/roommetacache/valkey.go:46`
- `notification-worker/members.go:46`
- `search-service/handler.go:227`

## Startup: lazy connect

The fix is smaller than the trap suggests. go-redis already dials lazily and
self-heals per call, so nothing needs to retry in the background. The crashloop
comes entirely from the startup `PING` being treated as fatal.

`valkeyutil.ConnectClusterLazy` performs identical construction and
instrumentation, demotes the `PING` to a non-fatal reachability log, and always
returns a usable `Client`. `presencestore.NewValkeyStore` gets the same treatment.
All seven call sites switch to it and drop their `os.Exit(1)` / error return.

No background goroutine is introduced, so there is no termination path to get
wrong (CLAUDE.md §3, concurrency).

`ConnectCluster` is retained for `tools/seed-sample-data`, a one-shot CLI where
failing fast on an unreachable Valkey is the correct behavior.

## Per-service posture after the change

| Service | Boot with Valkey down | Runtime behavior |
|---------|----------------------|------------------|
| `message-gatekeeper` | starts | breaker opens; room-meta reads go to Mongo |
| `broadcast-worker` | starts | breaker opens; room-meta reads go to Mongo |
| `room-worker` | starts | `BustMeta` no-ops; L2 TTL reconciles |
| `notification-worker` | starts | member fan-out falls back to Mongo |
| `search-service` | starts | restricted-rooms falls through to Elasticsearch |
| `user-presence-service` | starts | `errcode` errors per request; self-heals |
| `botplatform-service` | starts | see below |

### botplatform-service

Two Valkey-backed controls sit on the same requests, and they degrade
differently because they protect different things.

**Rate limiting is suspended.** It is a protective control — it guards the
platform, not the data — so availability wins during an outage. `botRateLimit`
(`botplatform-service/middleware.go:128`) gains a fail-open path: on a Valkey
error it logs WARN, emits a metric, and calls `c.Next()` instead of aborting with
`errcode.Internal`.

**Idempotency splits by endpoint.** It is a correctness control: the sentinel is
what stops a retrying bot from double-executing within the 60s bucket. A
duplicate message is visible but recoverable; a duplicate room creation or member
add is expensive and awkward to undo. So:

| Endpoints | Posture on Valkey error |
|-----------|------------------------|
| `POST /api/v1/rooms/:roomID/messages`, `POST /api/v1/dms/:userID/messages` | fail **open** — proceed without the sentinel |
| `POST /api/v1/rooms`, `.../members/add`, `.../members/remove` | fail **closed** — `errcode.Internal`, as today |

`routes.go` already builds the two classes through separate closures
(`botplatform-service/routes.go:27` for messages, `:30` for room management), so
this is a `failOpen bool` parameter on `botIdempotency` rather than a
restructure. `botRateLimit` at `:25` takes the same parameter, set `true`.

A suspended control must never be silent — both fail-open paths log and emit the
metric below.

## Observability

| Signal | Type | Purpose |
|--------|------|---------|
| `valkey_breaker_transitions{from,to}` | counter | Emitted from `OnStateChange`. |
| `valkey_breaker_state` | gauge | Current state, for dashboards. |
| `bot_control_bypassed{control="ratelimit"\|"idempotency"}` | counter | A bypassed control is an alertable condition — abuse protection is off. |

Existing `cachemetrics` hit/miss/error recording is unchanged.

## Testing

TDD throughout, per CLAUDE.md §4. Red before green on every item.

**Breaker.** Table-driven unit tests over closed → open → half-open → closed,
threshold boundaries, and concurrent callers under `-race`. `IsSuccessful` gets
its own table asserting that `ErrCacheMiss` and `context.Canceled` never trip the
breaker while transport errors do.

**Bounded latency.** A `net.Listen` that accepts connections and never responds —
a deterministic blackhole. This is the failure mode that connection-refused
testing misses entirely, and it is the one the timeout profiles exist to bound.
Assert a `Get` returns within the profile budget.

**Lazy connect.** Assert a usable `Client` comes back from an unreachable address
and that the first call returns an error rather than panicking.

**botplatform.** Extend the existing tables in `middleware_ratelimit_test.go` and
`middleware_idempotency_test.go` with a Valkey-error case per posture, including
the fail-closed room-management case.

**Integration.** `testutil.SharedValkeyCluster` for the healthy-path regression,
with `testutil.FlushValkey` registered in `t.Cleanup` per CLAUDE.md §4.

Coverage floor is 80%; `pkg/valkeyutil` is shared infrastructure and targets 90%.

## Risks

**The profiles are tighter than production has ever run.** A p99 Valkey latency
above 150ms would start failing cache reads that today succeed slowly. The
consumers all fall back correctly, so the failure mode is extra Mongo load rather
than errors — but the `cachemetrics` error series should be watched after rollout,
and `CacheProfile` loosened if it proves tight.

**Suspended rate limiting removes abuse protection during an outage**, at exactly
the moment the platform is already degraded. This is the accepted cost of the
availability posture chosen during design; the `bot_control_bypassed` alert is the
compensating control.

**Fail-open message idempotency admits duplicate bot messages** during an outage.
Bounded by the 60s bucket and limited to bots that retry, and duplicates remain
impossible on the expensive room-management paths.
