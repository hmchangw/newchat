# Mongo-Outage Survival for Send + History-Load

## Summary

Keep the two most critical user journeys — **sending a message** and
**loading room history** — working for a 30–60 minute MongoDB outage. Neither
journey needs Mongo for its *data* (messages are durable in Cassandra +
JetStream); Mongo is touched only for the **subscription authorization
decision** and a few metadata reads that already have, or can trivially get,
safe fail-open defaults.

The work reduces to three focused mechanisms, all extending the existing
message-pipeline caching layer (`2026-05-18-message-pipeline-mongo-caching`)
rather than introducing a new subsystem:

1. Bring the subscription-authz cache to two tiers (L1 in-process + a new
   shared L2 Valkey), so the authz decision survives outside any one process.
2. Give the subscription L2 a long TTL (~90 min) so an L2 hit — which
   short-circuits before Mongo is ever called — *is* the outage buffer.
3. Add a circuit breaker so a cold cache miss during the outage fails fast to
   the survivable path instead of stalling on Mongo's 10s timeout.

## Motivation

MongoDB is the operational DB (rooms, subscriptions, messages-metadata). When
it is down, most write journeys (create room, join, role change) are expected
to fail. But two read/append journeys are business-critical and should ride
through a bounded outage:

- **Send a message.** Today `message-gatekeeper` hard-blocks on
  `store.GetSubscription` (Mongo) for authorization and, for non-thread /
  non-bypass senders, on `store.GetRoomMeta` (Mongo) for the large-room post
  cap. The message's durability, however, is Cassandra + JetStream: the
  canonical event goes to `MESSAGES_CANONICAL` → `message-worker` → Cassandra.
  Mongo is on the *decision* path, not the *data* path.
- **Load history.** Today `history-service` hard-blocks on
  `checkAccessAndRoomTimes` — the subscription access check (`GetHistorySharedSince`
  / `GetSubscription`, Mongo), plus `GetRoomTimes` (bucket-walk bounds) and
  `GetMinUserLastSeenAt` (read-receipt floor, already best-effort). The
  messages themselves come from Cassandra.

So across both journeys the *only* hard Mongo gate is the **subscription
decision**. Everything else is either a safe-defaultable metadata read or
already best-effort.

### What already exists (and why it is not enough)

The `2026-05-18` design added process-local caching to cut Mongo load, **not
to survive an outage**:

| Concern | Today | Outage gap |
|---|---|---|
| Room-meta (large-room cap) | L1 LRU + **L2 Valkey** read-through (`pkg/roommetacache`), 15m L2, active bust by room-worker | Fails-open anyway — low risk |
| Room member list (fan-out) | L2 Valkey (`pkg/roomsubcache`) | Not on these two paths |
| **Subscription authz** (hard gate on *both* journeys) | **L1 only** (gatekeeper `subcache.go` 2m; history `readcache.go` 2m), positive-only, **no L2** | **The core gap** |
| Mongo-down behavior | Any cache miss → read-through calls Mongo → **fails** | No stale-serve, no fast-fail |

The existing caches are freshness caches: L1 TTLs are 2 minutes, and on a miss
the read-through path calls Mongo and returns its error. For an outage longer
than the TTL, every entry expires and every request fails.

## Design principle

Make the **subscription decision survivable**, and make every request
**fail-fast to that survivable path** instead of stalling on Mongo's timeout.
All other Mongo reads on these two paths degrade to safe fail-open defaults.

This aligns with the accepted posture (confirmed during brainstorming):

- **Fail-open on staleness.** During the outage we may serve a subscription
  decision that is up to the outage-TTL stale (e.g. a user whose access was
  revoked minutes ago can still send/read). A bounded window of over-permissive
  access to a room the user was *just* legitimately in is a far smaller harm
  than locking the entire active userbase out for an hour.
- **"Currently-active users is enough."** We rely on warming the caches with
  real traffic, not a full local replica of all subscriptions. Users/rooms not
  seen within the outage-TTL window are locked out until Mongo returns —
  accepted.

## Mechanisms

### Mechanism 1 — Two-tier subscription-authz cache (shared L2)

New package `pkg/subauthcache`, mirroring the shape of `pkg/roommetacache`:

- **L1**: in-process LRU+TTL, singleflight-deduped (as the current
  `subcache.go` / `readcache.go` already are).
- **L2**: Valkey read-through, key `sub:{roomID}:{account}` — the `{roomID}`
  hash-tag colocates the entry in the room's cluster slot, matching house
  convention (`pkg/roommetacache.MetaKey`, `pkg/roomkeystore`).
- **Shared by both services.** `message-gatekeeper` and `history-service` both
  read/write the *same* L2 entries, so a user active in either journey warms
  the other (loaded history → already warm to send, and vice versa).

L2 stores one **superset projection** so both consumers are served from a
single entry:

```go
// SubAuth is the shared L2 projection of a subscription. Presence of the
// entry means "subscribed"; absence is not cached (positive-only).
type SubAuth struct {
    ID                 string       `json:"id"`                 // user entity ID
    Account            string       `json:"account"`
    Roles              []model.Role `json:"roles"`              // gatekeeper: canBypassLargeRoomCap
    HistorySharedSince *int64       `json:"historySharedSince,omitempty"` // history: access floor
}
```

Each service projects locally: gatekeeper reads `ID` + `Roles`; history reads
`HistorySharedSince` (and treats presence as `subscribed=true`).

**Positive-only** (matches the existing convention and the fail-open posture):
`errNotSubscribed` and transient errors are never cached. Revocation therefore
propagates by the next successful Mongo read once Mongo is healthy; during an
outage it is honored fail-open up to the outage TTL.

### Mechanism 2 — Long L2 TTL as the outage buffer

Because an L2 **hit short-circuits before Mongo is ever called**, outage
survival falls out of giving the subscription L2 a long TTL — default **90
minutes** (`*_SUB_L2_TTL`), covering the 60-minute target plus margin.

- Mongo healthy: L1 (2m) absorbs the hot path; on L1 miss the L2 read-through
  refreshes the entry, so warm entries stay fresh on the normal cadence.
- Mongo down: L1 expires at 2m and falls to L2, which still holds the 90-min
  entry and serves it with **no Mongo call and no failure**.

No separate "retention map" is needed — the long L2 TTL *is* the retention
window. The L1 tier keeps its existing 2m freshness TTL unchanged.

Room-meta L2 already exists at 15m; it fails-open regardless, so its TTL is
left as-is (a bump is optional and out of scope).

### Mechanism 3 — Circuit breaker (fail-fast)

New package `pkg/circuitbreaker` (none exists today). Without it, every L1+L2
**miss** during the outage still calls the Mongo loader and eats the full 10s
`fetchTimeout` before failing — pinning singleflight goroutines and destroying
latency for exactly the cold requests.

- Wraps the **terminal Mongo loader** inside the read-through chain (below L2).
- **Closed → Open** after N consecutive loader failures (config
  `*_MONGO_BREAKER_FAILS`, default 5). While **Open**, the loader fast-fails
  instantly (no 10s wait), so a cold miss returns immediately.
- **Half-Open** after a cooldown (`*_MONGO_BREAKER_COOLDOWN`, default 10s):
  one probe; success closes the breaker, failure re-opens. This both detects
  recovery and shields a recovering Mongo from a retry storm.

The breaker is a small, reusable primitive (closed/open/half-open state
machine with a failure counter and a cooldown timer); it is transport- and
store-agnostic and injected into the read-through loaders.

## Behavior during a full Mongo outage

### Send (message-gatekeeper)

| Step | Behavior |
|---|---|
| Subscription authz | L1 miss → **L2 hit** (active user, within 90m) → **allowed**. L2 miss → breaker-open fast-fail → **denied** (cold user; accepted gap). |
| Large-room cap | Uses cached `userCount` where warm (enforced); on true miss + breaker-open → **fail-open** (allow post). |
| Durability | Publishes to `MESSAGES_CANONICAL` → `message-worker` → Cassandra. **No Mongo.** Message is durably sent and appears in history. |

### Load history (history-service)

| Step | Behavior |
|---|---|
| Access check | Same L1 → L2 → breaker chain as gatekeeper, **sharing L2 warmth**. Warm user → allowed with cached `HistorySharedSince` floor. |
| Room-times (bucket bounds) | Miss / breaker-open → fall back to `now` (ceiling) + configured history floor. Slightly wider bucket walk; correct results. |
| MinUserLastSeenAt | Already best-effort → `nil`. |
| Messages | Read from **Cassandra**. History loads, including messages sent mid-outage. |

## Accepted gaps / non-goals

- **Cold users/rooms** not seen within ~90 min are locked out until Mongo
  returns (the "active-users-is-enough" tradeoff).
- **Subscription writes during the outage** (joins, role changes, new rooms)
  do not take effect — those are Mongo writes, out of scope.
- **Revocation during the outage** is honored fail-open up to the outage TTL.
- **Full local replica** of all subscriptions is explicitly not built (YAGNI
  for this posture).
- **Valkey down too** degrades to L1-only behavior (Option A from
  brainstorming): active users on non-restarted pods keep working; the L2 is
  best-effort and never blocks a request.

## Configuration

All via `caarlos0/env` with `envDefault`, per house convention. New knobs:

| Env var | Service(s) | Default | Purpose |
|---|---|---|---|
| `*_SUB_L2_TTL` (e.g. `GATEKEEPER_SUB_L2_TTL`, `HISTORY_SUB_L2_TTL`) | gatekeeper, history | `90m` | Subscription L2 (outage) retention window |
| `*_MONGO_BREAKER_FAILS` | both | `5` | Consecutive failures to open the breaker |
| `*_MONGO_BREAKER_COOLDOWN` | both | `10s` | Open→half-open cooldown |

L2 is disabled (falls back to L1-only) when the Valkey client is nil, matching
the existing `roommetacache` fail-open wiring.

## Observability

- Reuse `pkg/cachemetrics` — the new sub-authz L2 emits the standard
  `cache="subauth",tier="l2"` hit/miss/error series; L1 keeps its series.
- Add a **breaker-state gauge** (closed/open/half-open) per service.
- Add a **served-stale-during-outage counter** (L2 hit while the breaker is
  open — i.e. requests that survived *because* of this design).
- `slog.Warn` on the closed→open and open→closed transitions (metadata only;
  never account/room bodies beyond IDs, per logging rules).

## Testing (TDD)

Red-Green-Refactor throughout; ≥80% coverage, ≥90% on the new packages.

**Unit**
- `pkg/circuitbreaker`: table-driven state transitions (closed→open on N
  fails, open fast-fails, cooldown→half-open, half-open success closes /
  failure re-opens), concurrency-safe under `-race`.
- `pkg/subauthcache`: L1 hit; L1 miss→L2 hit; L1+L2 miss→loader; loader error
  with breaker open → no Mongo call; positive-only (negative/error not
  cached); L2 fail-open on Valkey error (degrade to loader); singleflight
  dedup.
- `message-gatekeeper` handler: Mongo-down (mock loader errors + breaker open)
  → warm sub (L2 hit) **allows** send; cold sub (L2 miss) **denies**;
  large-room cap fails-open on room-meta miss.
- `history-service` service: same warm-allows / cold-denies for the access
  check; room-times miss → now/floor fallback bounds.

**Integration (testcontainers, `//go:build integration`)**
- Valkey up (via `testutil.SharedValkeyCluster`) + **Mongo stopped**: a
  pre-warmed room's `send` and `LoadHistory` both succeed; a cold room is
  denied. Messages sent during the simulated outage are readable from
  Cassandra once history-load runs.

## Files touched (anticipated)

- **New**: `pkg/circuitbreaker/`, `pkg/subauthcache/` (L1 + L2 + read-through,
  mirroring `pkg/roommetacache`), with unit tests.
- **message-gatekeeper**: wire `subauthcache` in place of `subcache.go`'s
  L1-only store; add breaker to the Mongo loader; config knobs in `main.go`;
  large-room-cap fail-open on miss.
- **history-service**: wire `subauthcache` into `readcache.go`'s subscription
  cache; breaker on the Mongo loaders; room-times now/floor fallback; config
  knobs in `internal/config`.
- **Docs**: no `docs/client-api.md` change — request/response schemas and
  events are unchanged; this is purely a resilience/behavior change on the
  server side.
