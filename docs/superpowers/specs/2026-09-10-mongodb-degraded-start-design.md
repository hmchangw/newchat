# MongoDB degraded start

Let services that do not depend entirely on MongoDB start while MongoDB is
down, instead of crashlooping until it returns.

## Problem

`mongoutil.Connect` pings MongoDB and returns an error when the ping fails
(`pkg/mongoutil/mongo.go:53-63`). All ~25 call sites treat that error as fatal —
most as `slog.Error(...); os.Exit(1)`, a few (e.g. `tcard-service`) by returning
it from a `run()` that main exits non-zero on. A MongoDB outage therefore stops
every service from starting, including services whose primary datastore is
Cassandra, Elasticsearch or Valkey and which need MongoDB only for enrichment.

The pods already running survive an outage. Any pod that restarts during one — a
rolling deploy, a node drain, an OOM kill, a scale-up — cannot come back. The
blast radius of a MongoDB outage grows with every unrelated pod event.

Runtime resilience is already in place and is not the gap:

- `PoolConfig.ServerSelectionTimeout` defaults to `2s`, so an unreachable
  MongoDB is reported rather than hung on.
- `pkg/circuitbreaker` and `mongoutil.BreakerConfig` fence call sites once
  MongoDB stops answering.
- Valkey-backed L2 tiers (`userstore`, `roommetacache`, `roomsubcache`,
  `subauthcache`, `atrest`) front the hot reads. They are external and shared,
  so a pod that starts cold during an outage still gets warm cache hits — this
  is what makes a degraded start serve real traffic rather than fail everything.
  This holds for five of the eight; `search-service` and `user-presence-service`
  have only process-local caches on the MongoDB path and `tcard-service` an
  in-memory snapshot gating its own `/readyz` (see `docs/health-probes.md`).
- Enrichment paths fail open; JetStream workers NAK and retry.

`docs/health-probes.md` already states the matching philosophy: liveness never
probes dependencies, readiness reflects per-pod serve-ability rather than
shared-backend health, and "the application returns proper errcode errors when a
datastore is down — that's the right failure mode, not yanking pods".

The process simply refuses to reach the runtime that was built for this. The
only hard startup gate is `Connect`'s ping: every in-scope service's
`EnsureIndexes` call already warns and continues.

## Scope

Eight services start degraded. Each keeps its primary job working when MongoDB
is unreachable:

| Service | Primary datastore | MongoDB used for |
|---|---|---|
| `tcard-service` | in-memory card cache, background refresh | card source, already cached with a background refresh loop |
| `search-service` | Elasticsearch | HR/app enrichment; in-process LRU only, no breaker or Valkey tier — a cold pod pays `ServerSelectionTimeout` per uncached lookup |
| `user-presence-service` | Valkey | `userstore` profile enrichment; pod-local L1 only (Valkey is the presence store) — the presence write path never touches MongoDB |
| `broadcast-worker` | Valkey L2 (`roomsubcache`, `roommetacache`) | one buffered, best-effort, never-awaited preview write |
| `message-gatekeeper` | Valkey L2 (`subauthcache`, `roommetacache`) + in-process LRU | validation lookups |
| `history-service` | Cassandra | room meta, thread repos, DEK — all L2-fronted |
| `message-worker` | Cassandra | thread state (NAKs and retries), enrichment (fails open: an L2-cold sender is persisted projected, with `@mentions` dropped — immediate-but-degraded over delayed-but-complete) |
| `notification-worker` | Valkey L2 (`roomsubcache`) | subscription and user lookups |

Every other service keeps failing fast. MongoDB is their job, so a pod that
cannot reach it can do no useful work: `room-service`, `user-service`,
`room-worker`, `roomlist-worker`, `inbox-worker`, `admin-service`,
`portal-service`, `botplatform-service`, `upload-service`, `media-service`,
`bot-*`, `teams-*`, `hr-sync-worker`, `search-sync-worker`.

Membership is decided in code, not by deployment configuration. There is no env
knob: an operator cannot make `room-service` start degraded, and cannot make
`broadcast-worker` stop.

`room-service` is changed by this work despite staying fail-fast: it takes over
one index that a degradable service can no longer be trusted to create. See
"Index ownership" below.

## Design

### `pkg/mongoutil`

One new field on `connectConfig`, one new `Option`, one branch in `connect()`.

```go
// WithDegradedStart downgrades an unreachable MongoDB at startup from an error
// to a warning: Connect returns a usable client and the service starts. For a
// service whose primary datastore is not MongoDB — the driver reconnects on its
// own (SDAM) once MongoDB returns, and the call sites already handle its errors.
func WithDegradedStart() Option {
	return func(c *connectConfig) { c.degradedStart = true }
}
```

`connect()` becomes:

```go
if cleanup != nil {
	cleanups.Store(client, cleanup)
}
pingCtx, cancel := context.WithTimeout(ctx, startupPingTimeout)
defer cancel()
if err := client.Ping(pingCtx, nil); err != nil {
	if !cfg.degradedStart || isAuthError(err) {
		Disconnect(context.Background(), client)
		return nil, fmt.Errorf("mongo ping: %w", err)
	}
	slog.Warn("mongo ping failed at startup; continuing in degraded mode",
		"uri", sanitizeURI(uri), "error", err)
	return client, nil
}
slog.Info("connected to MongoDB", "uri", sanitizeURI(uri))
```

Three details are load-bearing:

- **The cleanup is stored before the ping.** The degraded path keeps the client,
  so the client must already own its o11y instrumentation teardown, or
  `Disconnect` at shutdown would not flush the SDK's pool metrics.
- **The strict path calls the exported `Disconnect`**, which already does
  `LoadAndDelete` + cleanup + disconnect. Moving the `Store` up lets the
  failure path reuse it instead of hand-rolling the same three steps, so the
  strict path gets shorter, not longer.
- **The warn line goes through the existing `sanitizeURI`.** A connection string
  can embed credentials in userinfo and in query options; the log must not.

### What the ping failure means

The rule is one line: **rejected credentials are fatal; everything else starts
degraded.**

Auth is the only startup failure that is separable. A wrong password is a
server *rejection* — the driver surfaces it as the public
`mongo.CommandError` (`wrapErrors` in `mongo/errors.go` exists precisely so
callers classify "without relying on internal or x packages"), code 18
`AuthenticationFailed`, 13 `Unauthorized` or 11 `UserNotFound` (a wrong user or
`authSource` — the server looked and found nobody). `isAuthError` is one
`errors.As`.

Nothing else is. A wrong port reports `connection refused`; a wrong hostname
reports a DNS error; **a down MongoDB reports exactly those**. From the
network's view they are one event: nothing answering at that address. There is
no signal to split them on, and treating a DNS failure as fatal would crashloop
a degradable service during the very outage it exists to survive, when DNS
flaps. TLS failures are buried inside the same server-selection error as
unreachability; separating them needs driver internals. So they all start
degraded — and that is fine, because for these eight services the right
response to "wrong host" and "MongoDB down" is the same: start, serve from L2,
log loudly. The warning names the error, so an operator who finds MongoDB
healthy knows to look at the config.

This makes the predicate **fail-open**, deliberately. Fail-closed ("degrade only
when positively unreachable") is right for a fail-fast service. For a service
*designated* degradable, a false degrade costs a warning and a cache-served
pod; a false fatal costs the outage. The cheaper error is the default.

### Why the ping is bounded

The driver sets no operation timeout by default and every service passes
`context.Background()`. A MongoDB that still answers `hello` (so server
selection succeeds) but is too overloaded to serve a `ping` would hang startup
indefinitely — neither failing nor degrading. `startupPingBound` bounds it:
10s (comfortably above the pooled 2s selection default plus a dial and auth
handshake), or the configured server-selection timeout plus a 5s margin when
that is longer, so a pool-less caller on the driver's 30s default or an
operator riding out an election is not capped by a constant they cannot see.
A timeout is "not auth" and degrades. This applies to all 25 services, not
only the eight: a fail-fast service gets a bounded exit instead of a wedged
pod.

### Why the rejection is read from the pool too

With `MONGO_MIN_POOL_SIZE > 0` the pool's warm-up connections race the ping to
the server; the first to fail SCRAM clears the pool, and the ping's own
checkout then fails with "pool cleared" — no code — so the ping alone would
start the service degraded on rejected credentials. The pool emits
`ConnectionPoolCleared` with the handshake error before it fails the queued
checkouts, so `Connect` records that error through a pool monitor
(`credentialProbe`) and classifies it together with the ping's. Retrying the
ping would not do: every pool re-ready re-runs the same race.

An unreachable MongoDB that is also *overloaded* rather than down needs no
special case: the driver's own reconnect loop (SDAM) keeps re-dialling every
host after `mongo.Connect`, so the degraded client heals the moment MongoDB
answers. No retry loop is written here; the ping is a one-time check.

An unparseable URI and an o11y instrumentation failure still return an error:
they fail at `mongo.Connect`, before the ping.

### Per-service wiring

One added argument at each of the eight `Connect` call sites (option set shown
is illustrative — each service keeps whichever options it passes today):

```go
mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
	mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk),
	mongoutil.WithReadPreference(readPref), mongoutil.WithDegradedStart())
```

Nothing else in those services changes. The `os.Exit(1)` after each `Connect`
stays and still fires for misconfiguration. The seventeen out-of-scope call
sites are untouched, so their behaviour is unchanged by construction rather
than by review.

### Index ownership: `thread_rooms.parentMessageId`

Degraded start makes one existing hazard materially more likely, so this change
closes it rather than deferring it.

`message-worker` is the **sole** creator of the unique index on
`thread_rooms.parentMessageId` (`message-worker/store_mongo.go:41-46`).
`broadcast-worker` creates a different, non-unique `(parentMessageId, siteId)`
index; `history-service` creates `roomId`-prefixed ones; `room-service` creates
none. No fail-fast service backstops it.

`CreateThreadRoom` has **no read-before-insert**
(`message-worker/handler.go:392-401`). It blind-inserts a fresh UUIDv7 and
relies entirely on that unique index to distinguish a first reply from a
subsequent one:

```go
err := h.threadStore.CreateThreadRoom(ctx, &threadRoom)
switch {
case err == nil:                             // first reply — create subscriptions
case errors.Is(err, errThreadRoomExists):    // subsequent reply — reached only via duplicate key
```

Without the index, every reply to a parent creates a new thread room and takes
the first-reply branch. The thread fragments into one thread-room-per-reply,
and because `thread_messages_by_thread` is partitioned by `thread_room_id`, the
replies scatter across separate Cassandra partitions. Permanently; it does not
self-heal.

Before this change, skipping the index required a narrow race — the ping
succeeds and `CreateOne` then fails. After it, "MongoDB down at startup" — the
exact scenario this feature exists for — becomes a routine path to skipping it,
and the damage lands the moment MongoDB recovers and the NAKed replies redeliver.
The mitigating fact is that in an established cluster the index already exists,
so skipping the *assertion* is harmless; the hazard bites only where the index
is not there yet — a new site, a fresh cluster, a restore from backup. Low
probability, high and irreversible impact.

**Fix: `room-service` owns the index; `message-worker` co-creates it and gates
its writes on it.** `room-service` already owns the sibling
`thread_subscriptions` unique key (`room-service/store_mongo.go:144-157`),
already holds a `thread_rooms` collection handle and reads and writes the
collection (`store_mongo.go:71`, `:2033`, `:2088`), and is fail-fast.

- `room-service.EnsureIndexes` gains the assertion, via
  `mongoutil.EnsureIndexWithRepair` — not a plain `CreateOne` — preserving
  `message-worker`'s current repair of a pre-existing non-unique
  `parentMessageId_1`, and builds it ahead of its read-path indexes so the
  shared 30s startup budget reaches it.
- `message-worker` keeps creating both `thread_rooms.parentMessageId` and
  `thread_subscriptions.(threadRoomId,userAccount)` with the identical spec
  through the non-destructive `mongoutil.EnsureIndex` (an identical
  `CreateOne` is idempotent, so two creators cannot conflict), and confirms
  each before the writes that rely on it (`indexGate`, below). It never
  repairs: two repairers racing on one dirty index can drop each other's
  freshly built replacement, and a gate that had already confirmed the index
  would then admit writes without it. A conflicting index closes the gate
  (NAK) until `room-service`, the only repairer, has fixed it.
  Verify-and-warn alone — the first revision of this design, using the new
  `mongoutil.WarnMissingUniqueIndexes` — was shown in review to leave the
  recovery window open: a degraded pod resumes consuming the moment MongoDB
  returns, before the crashlooping owner has restarted, and two replies to one
  parent (or two concurrent subscription upserts for one user) then both
  insert. `WarnMissingUniqueIndexes` remains in `pkg/mongoutil` for
  dependents that only read an index, since `WarnMissingIndexes` matches on
  *name* alone and passes a same-keys index that lost its unique option.

Honest limit of the fix: `room-service`'s own `EnsureIndexes` is also
warn-and-continue (`room-service/main.go:277-279`). Moving ownership does not
make the assertion unconditional — it makes it happen from a service that
**cannot start while MongoDB is down**, which is the actual improvement. The
residual risk (room-service starts against a healthy MongoDB and `createIndexes`
still fails) is the narrow pre-existing race, unchanged, and identical to the
one already carried by every other unique index room-service owns.

An IaC-owned index was considered and rejected: unlike Cassandra, which has
`docker-local/cassandra/init/*.cql`, this repo has no MongoDB schema-init
mechanism, so "ops owns it" would leave local dev, integration tests and fresh
sites with no creator at all.

### The rule this establishes

> A degradable service must not be the sole creator of an index whose absence
> is a correctness hazard for its own write path.

Applied across the eight in-scope services, exactly two create a unique index:

- `message-worker` — `thread_rooms.parentMessageId`. In scope, moved above.
- `tcard-service` — `cards (path, _tcardVersion)`. **Exempt**: `tcard-service`
  never writes `cards` (no `InsertOne`/`UpdateOne`/`ReplaceOne`/`BulkWrite` in
  the service), so no write path of its own depends on the constraint.

Performance-only indexes stay with their degradable services —
`broadcast-worker`'s `(parentMessageId, siteId)` and `history-service`'s
`roomId`-prefixed thread indexes. Accepted risk: a site first brought up during
an outage runs without them until the next healthy restart of that service, and
pays collection scans meanwhile. That degrades latency, not integrity, and it
self-heals; the unique index does neither.

### Health probes

Unchanged. `/healthz` stays process-up only and `/readyz` stays NATS-only, for
the reason `docs/health-probes.md` already gives: MongoDB is shared by every
replica, so probing it in readiness flips all pods `NotReady` at once on a blip
and invites correlated rollout and PDB churn. A pod running degraded is
genuinely serving traffic from its caches, and reporting it `NotReady` would be
wrong.

Per-pod MongoDB reachability is observable from the startup warn line, from the
existing `pkg/circuitbreaker` state gauges on services that wire a breaker, and
from MongoDB's own cluster monitoring — which is where shared-backend health
belongs.

## Testing

TDD, red first.

Unit (`pkg/mongoutil/mongo_test.go`), no I/O:

- `WithDegradedStart` sets `degradedStart` on a config built by
  `newConnectConfig`, and composes with the other options.
- The default config leaves it false, so `Connect` without the option is
  unchanged.

Integration (`pkg/mongoutil/mongo_integration_test.go`, build tag `integration`),
because the behaviour needs a socket and the unit-test rules forbid reaching a
database from a unit test:

- Unreachable address, no option: `Connect` returns an error and a nil client.
  This pins today's behaviour so the change cannot silently widen.
- Unreachable address, with the option: `Connect` returns a nil error and a
  usable client, and a subsequent operation on it returns a server-selection
  error rather than panicking.
- Reachable MongoDB via `testutil.MongoURI(t)`, with the option: startup and
  operations behave exactly as without it, proving the option does not alter the
  happy path.
- Rejected credentials against `testutil`'s Mongo (no `--auth`, no users, so
  any credentials fail SCRAM with code 18), with the option: `Connect` still
  returns an error. This is the one case degraded start must not tolerate.

Unit, no I/O: `isAuthError` over a table of `mongo.CommandError` values (codes
18, 13 and 11, wrapped, other codes, non-command errors) — a plain value, so the
red is real. `missingUniqueIndexes` over a table of name→unique maps.
Integration: `listIndexUniqueness` reports a plain index as non-unique and a
`SetUnique(true)` index as unique.

Index ownership:

- `room-service/integration_test.go`: after `EnsureIndexes`, `thread_rooms`
  carries `parentMessageId_1` and it is unique. Plus the repair case — seed a
  non-unique `parentMessageId_1` first and assert it is replaced by the unique
  spec, mirroring the existing `thread_subscriptions` repair coverage.
- `message-worker/integration_test.go`: `EnsureIndexes` no longer creates the
  index, and warns when it is absent.

  **Test churn is deliberately one edit, in one place.** Nine call sites
  (`:525`, `:657`, `:746`, `:833`, `:880`, `:892`, `:947`, `:1099`, `:2100`)
  rely on that index for their subsequent-reply assertions, but all nine reach
  MongoDB through the same three-line helper, `setupMongo`
  (`message-worker/integration_test.go:194`). Creating the index there covers
  every one of them, so **no call site changes**: each
  `require.NoError(t, ...EnsureIndexes(ctx))` line stays verbatim and still
  asserts something real — that the startup gate returns nil once the index is
  confirmed. Keeping the diff to one helper is what keeps this branch cheap to
  rebase.

  Seeding it in the test helper stands in for `room-service` having run
  first; `message-worker`'s own gates then find the identical spec already in
  place. Separate fresh-database tests cover the gates creating it themselves.

The driver's own SDAM recovery once MongoDB returns is the driver's contract and
is not re-tested here.

`pkg/mongoutil` is a shared package, so the 90% coverage target applies.

## Documentation

- `docs/health-probes.md` — a section naming the eight degradable services,
  what a degraded start means, and why the probes are deliberately unchanged.
- `CLAUDE.md` §6 MongoDB — two rules: which services pass `WithDegradedStart()`
  and that MongoDB-primary services must keep failing fast; and the
  sole-creator rule above, so the next unique index added to a degradable
  service is caught at review rather than in production.
- `docs/client-api.md` is not touched: no client-facing handler and no
  `pkg/model` struct changes.

## Out of scope

**No `mongo.up` gauge.** A background reachability prober exporting a per-pod
gauge was considered and dropped as more machinery than the problem warrants —
a goroutine per service with its own termination path, a new exported type, and
a forever-periodic ping, to improve on a greppable startup warn line. A downed
MongoDB is alerted at the cluster level. The trade-off accepted is that the
existing breaker gauges read "closed" on an idle worker even while MongoDB is
unreachable, so they signal that calls are being fenced, not that MongoDB is
down; and that recovery of a pod that started degraded is not directly
observable.

**No env knob.** Membership in the degradable set is a code decision, so an
operator cannot widen or narrow it per deployment.

### Why a degradable writer gates on its unique index

`EnsureIndexes` runs once at startup. When that run failed because MongoDB was
down, the pod is still consuming when MongoDB returns — and it returns for
this pod before the crashlooping owner of the index (`room-service`, in
`CrashLoopBackOff` growing to five minutes) has restarted. On a fresh site the
first two thread replies after recovery would both insert a `thread_rooms`
row, and no later index build can merge them. `message-worker` therefore
confirms `thread_rooms.parentMessageId` on demand (`indexGate`): the first
`CreateThreadRoom` after a failed startup ensure runs the non-destructive
`EnsureIndex` itself, a failure NAKs the reply for retry, and a confirmed index costs an
atomic load per insert thereafter. Concurrent callers share one in-flight
attempt, and a failure is remembered for a backoff interval (5s doubling to
1m), so a duplicate-data `E11000` parks thread replies until a human dedupes,
loudly, rather than turning every write and redelivery into another
collection-wide index build. The attempt honours a sooner caller deadline, so
startup's shared 30s budget covers both gates, and otherwise runs under its
own 30s bound.
