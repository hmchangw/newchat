# subscription.list over HTTP — design

**Date:** 2026-08-22
**Branch:** `claude/subscription-list-http-endpoint-lq11oc`
**Status:** approved design, pending implementation plan

## 1. Problem

The desktop client initializes its sidebar with `subscription.list`, a NATS
request/reply RPC
(`chat.user.{account}.request.user.{siteID}.subscription.list`, served by
`user-service/service/subscriptions.go:30`). A typical user holds **200–300
subscriptions**, each row carrying the nested `room` object and a
`previewMessage` whose `content` history-service returns as the **full message
body** by design (`history-service/internal/service/rooms_test.go:308`: "Content
is returned in full — the client truncates for display"). user-service now
truncates it to `PREVIEW_CONTENT_CHARS` runes (default 50) on both transports, so
a 400-row page carries ~20 KB of preview text rather than up to 8 MB.

The NATS max payload is **128 KB**. A full sidebar does not fit, so the client
must page at ~40 rows and issue 5–8 round trips at startup. `MAX_SUBSCRIPTION_LIMIT`
(default 1000) is fictional in practice: any page large enough to matter exceeds
the payload ceiling.

A second, less obvious problem compounds it. `AggregateSubscriptions`
(`user-service/mongorepo/subscriptions.go:250`) carries this note at line 283:

> Scaling ceiling: the room join + activity sort run over the full matched set
> before the skip/limit page (the sort key lives on the joined room, so it can't
> be pushed past the lookup).

The `$lookup` + sort cost is **O(total subscriptions for the account), independent
of page size**. Every 40-row page pays the full join for all 300 rows. Paging
therefore multiplies the dominant server cost by the number of pages: today's
5-page init is roughly **5× the Mongo work of a single 200-row page**.

## 2. Goals

1. A RESTful HTTP endpoint returning the same data as `subscription.list`, free
   of the 128 KB ceiling, so the client fetches its sidebar in one request.
2. Performance and stability under a 10-pod production deployment, including the
   mass-reconnect bursts that rolling deploys produce.
3. Gin and `net/http` configured deliberately for high concurrency rather than
   left on defaults (which impose no concurrency bound at all — see §9).

## 3. Non-goals

- **No caching layer**, no ETag/`If-None-Match`, no Valkey page cache. Deferred
  deliberately; revisit only if measurements demand it.
- **No `sonic` on this path.** `AppSubscription.appViewUrl` is map-shaped, and
  CLAUDE.md forbids adopting sonic where a payload marshals `map` fields without
  a byte-identity review. gzip dominates the win regardless.
- **No change to the NATS endpoint's wire contract.** Its defaults and cap are
  untouched; only the shared internals improve.
- **No Kubernetes manifests.** They do not live in this repo. §13 records the
  operational requirements for whoever owns them.

## 4. Architecture

```
                    ┌─────────────────────── user-service pod ───────────────────────┐
                    │                                                                │
  desktop client ──HTTP──►  Gin :8080  ──┐                                           │
                    │      (limiter,     │                                           │
                    │       auth, gzip)  │                                           │
                    │                    ├──► service.ListSubscriptionsFor ──┬─► Mongo client "http"
  desktop client ──NATS──►  natsrouter ──┘         (transport-neutral)       │      (pool 128)
                    │      (MAX_CONCURRENCY)                                 │
                    │                                                        └─► Mongo client "nats"
  kubelet      ────HTTP──►  health :8081  (never limited)                           (pool 100)
                    │                                                                │
                    └────────────────────────────────────────────────────────────────┘
                                          │
                          chunked fan-out │ rooms.get / GetRoomsInfo
                                          ▼
                              history-service / room-service
```

One process, three listeners, two Mongo clients, one shared service core.

## 5. HTTP contract

### Endpoint

```
GET /api/v1/subscriptions
```

No `{account}` path segment: the account is derived from the verified credential
(§6), so a caller can only ever read its own subscriptions. This preserves the
guarantee NATS subject scoping gives today.

### Query parameters

| Param | Type | Required | Notes |
|---|---|---|---|
| `type` | string | yes | `current` \| `rooms` \| `apps` |
| `favorite` | boolean | no | Filter to favorites; pins the self-DM first |
| `updatedWithinDays` | integer | no | `rooms` type only; must be ≥ 0 |
| `includeLastMessage` | boolean | no | Omitted ⇒ include (backward-compatible) |
| `offset` | integer | no | Negative ⇒ 0. Default 0 |
| `limit` | integer | no | Omitted or ≤ 0 ⇒ `HTTP_SUBSCRIPTION_DEFAULT_LIMIT` (**40**); capped at `HTTP_SUBSCRIPTION_MAX_LIMIT` (**400**) |

Bound into a dedicated struct using `*bool` / `*int` fields so "omitted" stays
distinguishable from `false` / `0`. The NATS contract depends on that distinction
(`includeLastMessage` omitted means *include*, not *exclude*), and a plain `bool`
would silently invert it.

**Default 40, max 400, frontend is expected to send `limit=200`.** The default
matches the NATS default so an unparameterized call behaves identically on both
transports; the frontend opts into the large page explicitly.

### Success response

Byte-identical to the NATS reply — the same `models.PagedSubscriptionListResponse`:

```json
{
  "subscriptions": [ /* Subscription[] — see client-api.md §3.0 */ ],
  "hasMore": false
}
```

`200 OK`, `Content-Type: application/json`, `Content-Encoding: gzip` when
negotiated (§10).

### Error responses

All errors use the standard `errcode` envelope written by `errhttp.Write`, so
codes and reasons are identical to the NATS ones.

| Condition | Status | `code` | `reason` |
|---|---|---|---|
| Unknown `type` | 400 | `bad_request` | — |
| Negative `updatedWithinDays` | 400 | `bad_request` | — |
| Both `ssoToken` and `x-auth-token` | 400 | `bad_request` | `ambiguous_token` |
| No credential | 401 | `unauthenticated` | `missing_fields` |
| Invalid / expired credential | 401 | `unauthenticated` | `invalid_sso_token` / `sso_token_expired` |
| Session auth not configured | 503 | `unavailable` | `upstream_unavailable` |
| **Pod at concurrency capacity** | **429** | `too_many_requests` | `overloaded` |
| Internal failure | 500 | `internal` | — |

### 429 shed response

```
HTTP/1.1 429 Too Many Requests
Retry-After: 1
Content-Type: application/json
X-Request-ID: 01970a4f-8c2d-7c9a-abcd-e0123456789f

{
  "code": "too_many_requests",
  "error": "server is at capacity, retry shortly",
  "reason": "overloaded"
}
```

A new reason `UserOverloaded Reason = "overloaded"` goes in
`pkg/errcode/codes_user.go`. Note that `errhttp.Write` only writes status and
body (`pkg/errcode/errhttp/write.go:13`), so the limiter sets `Retry-After`
itself before calling it — the header will not appear otherwise. `errcode.TooManyRequests` already maps to 429
(`pkg/errcode/category.go:42`), and `Retry-After` alongside a 429 already has
precedent in this repo (`pkg/errcode/codes_botplatform.go:30`).

**Why 429 and not 503.** Semantically 503 is the better fit — this is server
capacity, not a per-client quota. It loses on operational grounds: Envoy/Istio
outlier detection ejects a host after consecutive 5xx responses. During a
reconnect burst a capacity 503 would eject healthy pods from the load balancer,
concentrating the same traffic onto fewer pods and amplifying the incident. 429
is a 4xx, does not trip outlier detection, and is what HTTP clients already back
off on.

## 6. Authentication

New `user-service/middleware.go`, following `upload-service/middleware.go`
(dual credential):

- **`ssoToken` header** → verified by the `pkg/oidc` validator that user-service
  **already constructs** for `sso.set` / `sso.refresh` (`user-service/oidc.go`).
  Account from `claims.PreferredUsername`, falling back to `claims.Name`.
- **`x-user-id` + `x-auth-token`** → `botauth.Authenticate` against botplatform.
- Both present → 400 `ambiguous_token`. Neither → 401 `missing_fields`.

New dependency for user-service: `BOTPLATFORM_URL` plus a Resty client. When
unset, the session-token branch returns 503 `upstream_unavailable` and the SSO
branch still works — the same degradation auth-service uses
(`auth-service/main.go:40`).

**The frontend should use `ssoToken` on this endpoint.** `botauth.Validator`
caps concurrent upstream validations at `maxInFlight = 64` and sheds beyond it;
on a mass-reconnect path that ceiling would bind before our own limiter does, and
it makes sidebar initialization depend on botplatform being reachable. SSO
verification is a local JWKS check with no network hop. This is documented as the
recommended credential in `client-api.md`.

## 7. Transport-neutral service core

`ListSubscriptions` currently takes `*natsrouter.Context` and reads
`c.Param("account")`. `natsrouter.NewContext` exists but is documented as a test
constructor whose `Msg` and chain methods panic outside a live chain — it must
not be faked in a production HTTP path.

Instead, extract the core and make the NATS handler an adapter:

```go
// user-service/service/subscriptions.go
func (s *UserService) ListSubscriptionsFor(ctx context.Context, account string,
    req models.SubscriptionListRequest, defaultLimit, maxLimit int,
) (*models.PagedSubscriptionListResponse, error)

func (s *UserService) ListSubscriptions(c *natsrouter.Context, req models.SubscriptionListRequest,
) (*models.PagedSubscriptionListResponse, error) {
    account := c.Param("account")
    c.WithLogValues("account", account)
    return s.ListSubscriptionsFor(c, account, req, s.defaultLimit, s.maxSubs)
}
```

The page bounds become parameters so each transport passes its own — `normalizePage`
(`subscriptions.go:61`) already takes them as arguments, so no change there.

The private helpers on this path change from `(c *natsrouter.Context)` to
`(ctx context.Context, account string)`: `enrichWithRoomInfoAndLastMsg`,
`enrichLocal`, `enrichCrossSite`, `enrichLastMessage`, `buildListItems`,
`lookupApps`, `lookupHRInfo`. `*natsrouter.Context` already satisfies
`context.Context`, so the three other callers (`getChannels`, `getDM`,
`getByRoomID`) become `s.enrichWithRoomInfoAndLastMsg(c, c.Param("account"), …)`
and behave identically. Roughly eight signatures in one package, mechanical, plus
test churn in `subscriptions_test.go` and `enrich_test.go`.

## 8. Chunked enrichment fan-out — a correctness prerequisite

**Without this, a large page silently returns degraded data.** Today
`enrichLastMessage` (`subscriptions.go:330`) issues one `rooms.get` per site and
documents the assumption:

> One call per site: a subscription page is bounded well under history-service's
> 100-roomId batch cap, so no chunk-split is needed.

True at 40. False at 200:

- `history-service` hard-rejects `rooms.get` above `maxRoomsGetBatch = 100`
  (`history-service/internal/service/rooms.go:18`).
- `room-service`'s `GetRoomsInfo` accepts up to `MAX_BATCH_SIZE` (default 1000),
  but bounds its **reply** via `marshalBounded` (`room-service/helper.go:209`),
  and every reply crosses NATS under the same 128 KB ceiling.

Both failures are caught and logged as "degraded", so a naive 200-row page would
return **every row with no `previewMessage`, and every cross-site row with no
`room` object** — a `200 OK` full of missing data.

Changes:

- `chunkRoomIDs(ids []string, size int) [][]string`.
- `enrichLastMessage` and `enrichCrossSite` fan out over **(site × chunk)** pairs
  instead of (site) pairs, sharing one semaphore.
- `ROOM_BATCH_CHUNK`, default **100** — history-service's hard cap, and small
  enough to keep each reply well under 128 KB.
- `maxSiteFanout` (currently the constant 8 at `subscriptions.go:24`) becomes
  configurable, since the unit of work is now smaller and more numerous.

**Degradation becomes finer, not coarser.** Today one failed RPC loses a whole
site's previews; with chunks it loses only that chunk's rooms. That is a strict
improvement for the existing NATS path too — anyone who set `limit=200` there is
losing previews silently today.

## 9. Concurrency, shedding, and memory

### What the defaults actually are

Verified against Go 1.25.13 and gin v1.12.0 sources:

- `net/http` spawns `go c.serve(connCtx)` per accepted connection
  (`net/http/server.go:3491`). There is **no** connection or concurrency cap in
  `http.Server` — no `MaxConns`, no semaphore.
- Gin adds none either; it is an `http.Handler` with a `sync.Pool` of contexts.
- The only defaults are per-connection sizing: `MaxHeaderBytes` 1 MB
  (`server.go:931`), ~4 KB read buffer (`server.go:2007`), 4 KB write buffer
  (`server.go:2008`), 2 KB chunk buffer (`server.go:341`). HTTP/2 alone has
  `MaxConcurrentStreams` 250 (`h2_bundle.go:4007`) — per connection, so it
  multiplies rather than caps.
- On fd exhaustion the accept loop logs and retries with backoff
  (`server.go:3467-3476`); it does not refuse cleanly. Clients hang.

So an unguarded Gin server accepts requests until memory is exhausted. The only
existing backpressure is accidental: the Mongo driver's default `maxPoolSize` of
100, which **blocks rather than sheds** and is shared with the NATS handlers.

### The limiter

`pkg/ginutil.MaxConcurrency(n int)`: a buffered-channel semaphore with a
**non-blocking** acquire; on failure it writes the 429 above and aborts. No
queueing — a queued request is a request whose client has already given up.

**`HTTP_MAX_CONCURRENCY` default 512.** Derivation:

*Throughput* (Little's law, `N = arrival_rate × latency`): at a healthy ~150 ms
per 200-row request, 256 in flight serves ~1,700 req/s per pod, ~17,000 across
10 pods. A 200,000-user reconnect spread over 60 s is 3,300 req/s fleet-wide =
330 req/s per pod = ~66 in flight. Roughly 4× headroom. The reason to size above
~128 is not throughput but **absorbing latency spikes**: at 500 ms per request
the same arrival rate needs ~165 in flight, and at 1 s it needs ~330.

*Memory*: ~1.2 MB live per in-flight 200-row request (decoded
`[]EnrichedSubscription` ≈ 500 KB, BSON cursor batch ≈ 300 KB, marshalled JSON
≈ 150 KB, plus slack). At 512 that is ~620 MB live.

**Plus the gzip encoders, which the first draft of this table under-counted.**
A `klauspost` `BestSpeed` writer is **814 KB** once its compressor is
materialized (measured: `gzip.NewWriterLevel` + one `Write` + `Close` allocates
813,863 B in 14 allocations — the level-1 hash table, history buffer, window and
token array; the writer itself is only 160 B until first use). An encoder is held
only for the response *write* phase, not the whole request, and `gzipPool`
recycles it, so the standing cost is roughly `peak concurrent writes × 814 KB`
rather than `HTTP_MAX_CONCURRENCY × 814 KB` — but `sync.Pool` only drops entries
at a GC, so a burst's peak is retained for up to two cycles. Budget ~100 MB of
encoders at 512 under normal write overlap, and treat `GOMEMLIMIT` as the thing
that makes a worse overlap degrade instead of OOMKill.

**Measured** (`BenchmarkPageMarshal`, `BenchmarkPageMarshalGzip`, rows carrying a
full `previewMessage`):

| Page | JSON bytes | Marshal allocs |
|---|---|---|
| 40 rows (NATS default) | 29.6 KB | 38.6 KB / 121 allocs |
| 200 rows (frontend default) | 148 KB | 185 KB / 601 allocs |
| 400 rows (HTTP max) | 296 KB | 361 KB / 1201 allocs |

The 200-row payload landed at 148 KB against a 150–300 KB estimate, so the
sizing above holds. The integration test measured a real page compressing
131 KB → 4.9 KB; that ratio is flattered by uniform seed data, but even a
conservative 5× puts a 200-row page under 30 KB on the wire.

| Pod memory limit | Safe `HTTP_MAX_CONCURRENCY` |
|---|---|
| 1 GiB | 256 |
| 2 GiB | 512 |
| 4 GiB | 1024 |

Formula for ops: `N ≈ (0.35 × pod_memory_bytes) / 1.2MB`. The **512 default
therefore assumes a 2 GiB pod** — drop it to 256 on a 1 GiB one.

The HTTP door is deliberately wider than the NATS router's `MAX_CONCURRENCY=256`:
the browser reconnect storm this endpoint exists to absorb arrives here, and an
HTTP page is bounded read work, whereas the NATS door also fronts writes.

### Placement

The limiter is registered **on the `/api/v1` group only**, and health probes live
on a different listener entirely (§11). A shed liveness probe would be read by
kubelet as a failed check and restart pods *during* the burst, converting an
overload into an outage. Two independent mechanisms prevent that.

The limiter runs **before** authentication, so a burst does not spend OIDC
verification or a botplatform round-trip on requests it will reject. The
trade-off is accepted: acquire is O(1) and a slot is held only for the request's
duration, so unauthenticated traffic cannot pin the budget.

### GOMEMLIMIT

New `pkg/memlimit`:

```go
// SetFromCgroup sets the soft memory limit to fraction × the container limit.
// No-op when GOMEMLIMIT is already set in the environment, or when the cgroup
// reports no limit. Returns the limit applied and whether it was applied.
func SetFromCgroup(fraction float64) (int64, bool, error)
```

Reads `/sys/fs/cgroup/memory.max` (cgroup v2), falling back to
`/sys/fs/cgroup/memory/memory.limit_in_bytes` (v1); treats `max` and
implausibly-large sentinels as unlimited; calls `debug.SetMemoryLimit`. Wired as
the first statement in user-service's `main`, at **0.8**.

This is the safety net under every number above. All of them rest on a ~1.2 MB
per-request estimate; if that estimate is off by 2×, a limiter alone gets the pod
OOMKilled. `GOMEMLIMIT` makes the runtime collect more aggressively as it nears
the ceiling, so a mis-estimate degrades into latency instead of a crash loop —
and with 10 pods, one OOMKill during a reconnect storm cascades onto the rest.

## 10. Gin and http.Server tuning

- `gin.New()`, never `gin.Default()` — its logger writes unstructured text and
  violates the slog-JSON rule. `gin.SetMode(gin.ReleaseMode)`.
- Middleware order: `ginutil.CORS` → `o11ygin.Middleware` (traces + metrics, per
  `auth-service/main.go:117`) → `gin.Recovery` → `ginutil.RequestID` →
  `ginutil.AccessLog` → `ginutil.MaxConcurrency` → `ginutil.Gzip` → auth →
  handler.
- **`pkg/ginutil.Gzip(minSize int)`** (new): pooled `klauspost/compress/gzip`
  writers — already a direct dependency, no new module. Compresses only above
  `HTTP_GZIP_MIN_BYTES` (default 1024) and only when the client sends
  `Accept-Encoding: gzip`; sets `Vary: Accept-Encoding`. A 200-row page is
  roughly 150–300 KB of JSON and compresses to ~30–60 KB.
- **Per-request timeout: `HTTP_HANDLER_TIMEOUT`, default 30 s**, applied with
  `context.WithTimeout`. The enrichment fan-out already checks `ctx.Err()`
  between RPCs, so an abandoned request stops doing work.
- `http.Server`: `ReadHeaderTimeout: 5s`, `ReadTimeout: 10s`,
  **`WriteTimeout: 35s`**, `IdleTimeout: 120s`, `MaxHeaderBytes: 16 << 10`.

  `WriteTimeout` **must exceed** the handler budget: `net/http` starts that clock
  when request headers are read, so a 30 s handler under a 30 s `WriteTimeout`
  has its connection killed mid-write. Budget chain:
  **frontend 60 s > WriteTimeout 35 s > handler ctx 30 s > per-RPC 5 s.**
- Go 1.25 derives `GOMAXPROCS` from the cgroup CPU limit natively; no
  `automaxprocs` dependency.

### A correction worth recording

`json.NewEncoder(w).Encode(v)` does **not** stream: it marshals the whole value
into a pooled `encodeState` and issues a single `Write`
(`encoding/json/stream.go:209-233`). Its only advantage over `c.JSON` is the
pooled buffer, which `json.Marshal` also gets. The memory figures in §9 assume
full buffering and are unaffected.

Genuine streaming would mean hand-writing `{"subscriptions":[` → `Encode` per row
→ `],"hasMore":…}`, capping the encode buffer at one row and saving ~150 KB per
in-flight request (~77 MB at N=512). It is a discrete, revertable step in the
implementation plan, gated on a test asserting byte-equality against
`json.Marshal` of the same struct.

## 11. Health probes

**user-service currently has no health probes at all** — it is missing the
`HEALTH_ADDR` listener that 20 other services in this repo already run. This
design adds it, per `docs/health-probes.md`:

```go
healthStop, err := health.Serve(cfg.HealthAddr, 5*time.Second, natsutil.HealthCheck(nc))
```

`/healthz` and `/readyz` on `HEALTH_ADDR` (default `:8081`), a **separate
listener from the Gin API server**, with no limiter, no auth, and no gzip. Per
the documented convention, readiness probes only this pod's NATS connection —
never Mongo — so a shared-database blip cannot flip all 10 pods `NotReady` at
once.

## 12. Configuration

New in `user-service/config`, nested under `envPrefix:"HTTP_"` alongside the
existing `MONGO_` / `NATS_` blocks:

| Env var | Default | Purpose |
|---|---|---|
| `HTTP_PORT` | `8080` | Gin listener |
| `HTTP_MAX_CONCURRENCY` | `512` | In-flight cap; 0 disables |
| `HTTP_MAX_CONNS` | `2048` | Accepted-connection cap; 0 disables |
| `HTTP_HANDLER_TIMEOUT` | `30s` | Per-request context budget |
| `HTTP_WRITE_TIMEOUT` | `35s` | Must exceed the handler timeout |
| `HTTP_GZIP_MIN_BYTES` | `1024` | Compression threshold |
| `HTTP_MONGO_MAX_POOL_SIZE` | `128` | HTTP-only Mongo pool (§13) |
| `HTTP_MONGO_MIN_POOL_SIZE` | `0` | Warm floor; per member, so non-zero is a standing cost |
| `HTTP_MONGO_MAX_IDLE_TIME` | `5m` | Reap idle pooled connections; 0 = never |
| `HTTP_SUBSCRIPTION_DEFAULT_LIMIT` | `40` | Page size when `limit` omitted |
| `HTTP_SUBSCRIPTION_MAX_LIMIT` | `400` | Hard page ceiling |
| `ROOM_BATCH_CHUNK` | `100` | Enrichment fan-out chunk size |
| `PREVIEW_CONTENT_CHARS` | `50` | Truncate `previewMessage.content` to N runes; 0 disables |
| `MAX_SITE_FANOUT` | `8` | Concurrent enrichment calls in flight |
| `MONGO_MAX_POOL_SIZE` | `100` | NATS-path pool, now explicit |
| `MONGO_MIN_POOL_SIZE` | `0` | NATS-path warm floor; per member, so non-zero is a standing cost |
| `MONGO_MAX_IDLE_TIME` | `5m` | Reap idle NATS-path connections; 0 = never |
| `HEALTH_ADDR` | `:8081` | Probe listener |
| `BOTPLATFORM_URL` | *(unset)* | Session-token auth; unset ⇒ SSO only |
| `GOMEMLIMIT_FRACTION` | `0.8` | Soft memory limit as a fraction of the cgroup limit |

`Load()` validates: `HTTP_SUBSCRIPTION_DEFAULT_LIMIT ≤ HTTP_SUBSCRIPTION_MAX_LIMIT`,
`HTTP_HANDLER_TIMEOUT > 0` (checked before the next rule — `ginutil.Timeout`
disables itself at `≤ 0`, so a zero would pass "write exceeds handler" while
silently dropping the budget), `HTTP_WRITE_TIMEOUT > HTTP_HANDLER_TIMEOUT`,
`ROOM_BATCH_CHUNK` in `[1, 100]`, `HTTP_MAX_CONCURRENCY ≥ 0`,
`HTTP_MAX_CONNS ≥ 0` with `0` disabling the limiter and any positive value
required to exceed `HTTP_MAX_CONCURRENCY` (keep-alive connections outnumber
in-flight requests), `HTTP_MONGO_MAX_IDLE_TIME ≥ 0`, `MONGO_MAX_IDLE_TIME ≥ 0`,
`MONGO_MIN_POOL_SIZE ≤ MONGO_MAX_POOL_SIZE`, `PREVIEW_CONTENT_CHARS ≥ 0`,
`GOMEMLIMIT_FRACTION` in `(0, 1]` — fail fast at startup, matching the existing
validation style.

## 13. Isolation: a dedicated Mongo client for HTTP

The HTTP path gets its **own `*mongo.Client`**, so a large page cannot exhaust
the pool the NATS RPC handlers depend on:

```go
httpMongo, err := mongoutil.ConnectRead(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password,
    mongoutil.WithMaxPoolSize(cfg.HTTP.MongoMaxPoolSize),   // 128
    mongoutil.WithMinPoolSize(cfg.HTTP.MongoMinPoolSize),   // 0, see the footprint note
    mongoutil.WithMaxIdleTime(cfg.HTTP.MongoMaxIdleTime),   // 5m, see the reaping note below
    mongoutil.WithObservability(sdk),
)
```

`ConnectRead` (`pkg/mongoutil/mongo.go:30`) already bakes in
`secondaryPreferred`, which is exactly right for this read-only path — and it is
the existing repo pattern for a service splitting read and write clients.

Three repos are rebuilt against that client — `SubscriptionRepo`, `UserRepo` (HR
info), `AppRepo` (app metadata) — and a **second `*service.UserService` instance**
is constructed from them. Both instances share the stateless clients
(`roomclient`, `historyclient`, `presenceclient`, publishers) and the Valkey badge
cache; only the Mongo repos differ. `EnsureIndexes` runs once, on the NATS client
only.

The NATS-path client also gets an **explicit** `WithMaxPoolSize(100)`. It is
already effectively 100 by driver default; making it explicit means the number is
chosen rather than inherited.

Pool sizing rationale: a request holds a Mongo connection for roughly 40% of its
wall time (aggregate ≈ 60 ms, HR + app lookups ≈ 20 ms, of ~200 ms total), so 512
in-flight requests imply ~200 concurrent checkouts. 128 sits below that on
purpose: the pool is the second door, and queueing for a connection is cheaper
than melting the replica set.

**Cost, flagged for the DBA.** `MaxPoolSize` is enforced **per server**, not per
client, so the ceiling is the pool limits times the number of replica-set members
a client talks to — with a three-member set that is up to (100 + 128) × 3 ≈ **684
connections per pod**, ~6,840 across ten pods, not the 228/2,280 an earlier draft
of this document claimed. `MinPoolSize` is likewise per server, so the HTTP
minimum is therefore held on each active member, which is why it now defaults to
**0**: a warm floor is a connection the cluster carries whether or not traffic
arrives, and cold-checkout latency is the cheaper price. Sustained HTTP cost is
`MinPoolSize × members × pods` = **0** at the defaults; peak is
`MaxPoolSize × members × pods` = 3,840. Size these against the replica set's
budget rather than the per-pod intuition — failover shifts pool creation between
members and briefly multiplies the count.

That ceiling used to be *sticky*: the driver reaps idle connections only when
`maxIdleTimeMS` is set, and 0 (its default) means never
(`x/mongo/driver/topology/server_options.go:145`, `pool.go:195`). A burst that
grew the pool held those sockets for the life of the process, and failover
re-growing them on a new member added to the total rather than replacing it. The
two clients now set a reaping interval — `HTTP_MONGO_MAX_IDLE_TIME` and
`MONGO_MAX_IDLE_TIME`, 5m each — so 684 is a transient peak that drains back
toward `MinPoolSize × members` once the burst passes. `MONGO_MIN_POOL_SIZE`
mirrors the HTTP knob and defaults to 0 for the same per-member reason.

## 14. Deployment and operations

**A rolling deploy is the reconnect storm.** With 10 pods, every deploy
re-initializes a tenth of the client base at once. This endpoint's worst-case load
is self-inflicted and routine, not exceptional — the sizing above must survive
ordinary Tuesday deploys.

Requirements for whoever owns the manifests:

1. **`preStop` sleep of ~5 s** before shutdown, so the load balancer deregisters
   the pod before it stops accepting. Without it, every rollout produces a burst
   of connection errors on top of the reconnect burst.
2. **Shutdown ordering** in `shutdown.Wait`: `httpServer.Shutdown` → health server
   stop → `router.Shutdown` → `nc.Drain` → both Mongo clients → Valkey → obs.
   HTTP drains first so in-flight requests can still reach NATS and Mongo.
   `terminationGracePeriodSeconds` must exceed the 25 s shutdown budget plus the
   `preStop` sleep.
3. **Client-side reconnect jitter.** Without it, 10 pods restarting in sequence
   produce 10 synchronized thundering herds. Frontend concern, recorded here
   because the server sizing assumes it.
4. **Memory limit set and matched to `HTTP_MAX_CONCURRENCY`** per the §9 table.
   `GOMEMLIMIT` derives from the cgroup automatically, so the limit must actually
   be set on the pod for it to engage.
5. **`deploy/docker-compose.yml`** exposes `HTTP_PORT` and `HEALTH_ADDR` for local
   development.

## 15. Observability

- `o11ygin.Middleware` gives per-route traces and RED metrics, matching auth-service.
- **Shed counter** and **in-flight gauge** from `ginutil.MaxConcurrency`, via the
  meter already built by `obs.Init`. Without them, "is 512 the right number" is
  unanswerable in production. Alert on any sustained shed rate: it is the signal
  to resize *before* users notice.
- Chunk-level degradation logs one `slog.Warn` per failed chunk with `site`,
  `chunk_index`, `request_id` — replacing today's per-site warning.
- `ginutil.AccessLog` already emits method, path, status, latency, request ID.

## 16. Testing

TDD throughout: tests first, confirmed failing, then implementation.

**Unit.** Query binding (every parameter; omitted vs. zero-valued; negative
`limit`; unknown `type`; `limit` above max clamps to 400). Auth middleware (both
credentials; ambiguous; missing; expired; botplatform unavailable). `chunkRoomIDs`
boundaries (0, 1, 99, 100, 101, 250). Chunked fan-out asserting exact call counts
against mocked `HistoryClient` / `RoomClient` (250 rooms ⇒ 100/100/50), and
per-chunk degradation leaving sibling chunks intact. Gzip middleware (below and
above threshold; absent `Accept-Encoding`; `Vary` header). Limiter (sheds at N+1
with 429 + `Retry-After`; releases on completion), and that the health listener is constructed without one.
`memlimit` (cgroup v2, v1, unlimited, `GOMEMLIMIT` already set). Config validation
rejections.

**Integration** (`//go:build integration`, `testutil.MongoDB`): full HTTP →
service → Mongo path at `limit=200`, asserting `previewMessage` present on every
row — the regression test for §8.

**Benchmark.** `go test -bench` over marshal + gzip at 40 / 200 / 400 rows, so the
memory and payload figures in this document become measurements rather than
estimates.

**Coverage.** ≥ 80% per the repo floor; ≥ 90% on the new middleware and the
chunking logic.

## 17. Documentation

- `docs/client-api.md`: a new HTTP section for the endpoint — auth headers, the
  parameter table, a success example, the full error table, and the 429 shed
  example verbatim from §5. Notes `ssoToken` as the recommended credential and
  `limit=200` as the expected client value.
- `docs/health-probes.md`: move user-service from the implicit "all other
  services" row into an explicit row, since it now runs both an API listener and
  a health listener.
- `docs/superpowers/specs/` — this document.

## 18. Risks

| Risk | Mitigation |
|---|---|
| Per-request memory estimate (~1.2 MB) is wrong | `GOMEMLIMIT` at 0.8 turns a mis-estimate into GC pressure rather than OOMKill; the benchmark in §16 replaces the estimate with a measurement before rollout |
| Transient enrichment peak is unbounded per process | Chunks decode concurrently and are truncated only once decoded, so one request touches up to `MAX_SITE_FANOUT × ROOM_BATCH_CHUNK × 20 KB` = 16 MB before truncation, and `HTTP_MAX_CONCURRENCY=512` multiplies that. It is short-lived garbage — unreachable the moment `truncatePreviews` clones — so `GOMEMLIMIT` applies backpressure to it, unlike retained page data. The root fix is a length cap on history-service's `rooms.get` reply, tracked separately; until then `MAX_SITE_FANOUT` and `ROOM_BATCH_CHUNK` are the env-tunable levers |
| Server truncation at 50 runes is shorter than the client's own 140-char preview cap (`chat-frontend/src/lib/previewText.js`) | Deliberate — the sidebar renders one line and the byte saving is the point. `PREVIEW_CONTENT_CHARS` raises it without a rebuild if the shorter snippet reads badly; note the cut is on the raw body, so a row opening with markdown can flatten to less than 50 rendered characters |
| 2,280 replica-set connections across 10 pods | Flagged for the DBA; pool sizes are env-tunable without a redeploy of code |
| Service-layer refactor touches helpers shared by three other endpoints | Purely mechanical; existing tests for `getChannels` / `getDM` / `getByRoomID` are the safety net and must stay green unmodified |
| Session-token auth adds a botplatform dependency to a hot path | SSO is the documented and recommended credential; the branch degrades to 503 when `BOTPLATFORM_URL` is unset |
| Chunking changes behavior on the existing NATS path | It only makes degradation finer-grained; covered by tests asserting identical output at page sizes under 100 |
| Chunking multiplies downstream RPC count: a 400-row page issues 4 `rooms.get` calls where it issued 1 | Unavoidable — the single call was rejected outright above 100 ids, so it was 4 calls or no previews. But it raises load on room-service and history-service, which shed at their own `MAX_CONCURRENCY`, and a shed reply degrades silently per the documented enrichment contract (§13.2 / client-api.md). Watch their shed counters when the HTTP fleet scales, and size `HTTP_MAX_CONCURRENCY` with the downstream services in mind, not just this pod's memory |
| Isolation stops at Mongo | The HTTP path has its own handler budget and connection pool, but shares the NATS connection and the services behind it with the RPC path. Full isolation would need a second NATS connection or a separate deployment — deliberately out of scope |

## 19. Implementation order

Each step is independently testable and reviewable; the branch stays green throughout.

1. `pkg/ginutil`: `MaxConcurrency` + `Gzip` middleware, with tests.
2. `pkg/memlimit`: cgroup-derived `GOMEMLIMIT`, with tests.
3. `pkg/errcode`: `UserOverloaded` reason.
4. Service-layer refactor: transport-neutral `ListSubscriptionsFor` (§7). No
   behavior change; existing tests must pass unmodified except for signatures.
5. Chunked enrichment fan-out (§8) — the correctness prerequisite, landed before
   any large page is reachable.
6. Config block + validation (§12).
7. Dedicated HTTP Mongo client and the second service instance (§13).
8. Auth middleware (§6).
9. Gin server, routes, handler, `http.Server` tuning, shutdown ordering (§5, §10).
10. Health probe listener (§11).
11. Optional: per-row JSON streaming (§10), behind a byte-equality test.
12. Benchmarks, `docs/client-api.md`, `deploy/docker-compose.yml`.
