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

**Sliding TTL removes the ceiling for active rooms.** The fixed 90-min TTL
bounds survival to one window; for a longer outage, entries expire and cold
misses fail. To lift that ceiling, `subauthcache.ReadThrough` accepts
`WithSlideOnDegraded(func() bool)` and **re-arms an L2 hit's TTL** whenever the
predicate reports true — wired to "the Mongo circuit breaker is not closed"
(open or half-open). During an outage, L1 expires every ~2m and falls through to
L2, so an actively-read entry is pushed forward on each hit and **never expires
for the outage's duration, however long**. It is gated on the breaker so
normal-mode freshness is untouched (no re-arm when healthy → entries still
expire at the TTL and re-read Mongo), and it re-bounds to the TTL on recovery
(re-arming stops once the breaker recloses). The re-arm reuses the existing
`Set` primitive (no `valkeyutil.Client` change) and is best-effort.

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

**Update — active L2 invalidation.** `subauthcache.BustSub(ctx, client, roomID,
account)` deletes a single L2 entry; `subauthcache.BustSubs(ctx, client,
roomID, accounts)` deletes many in one Valkey round trip (safe in cluster mode
specifically because `SubKey` hash-tags on `{roomID}`, so every key for one
room's subscribers lands in the same slot). One of the two is called
immediately after every authoritative subscription write that can make a
cached positive `SubAuth` wrong, across every service that mutates a
subscription:

- **room-worker**: member removal/self-leave and owner-demotion
  (`processRemoveIndividual`), bulk org-removal (`processRemoveOrg`, batched
  via `BustSubs`), and Teams-migration reconciliation's hard-delete of
  departed members (`reconcileTeamsRoom`, also batched).
- **room-service**: role updates (`updateRole`) and the restricted-room bulk
  role rewrite (`roomRestricted`, batched via `BustSubs` over every
  subscriber — `ApplySubscriptionRestriction` can rewrite `Roles` for the
  whole room, not just the account named in the request).
- **inbox-worker** (federated destination-site replicas of the above):
  `handleMemberRemoved` (batched) and `handleRoleUpdated`, plus
  `handleRoomVisibilityChanged` — the federated counterpart of
  `roomRestricted`'s bulk rewrite, which was initially missed because
  `InboxStore` had no way to enumerate a room's local subscribers;
  `ListSubscriptionAccountsByRoom` closes that gap and feeds `BustSubs`.
- **bot-room-service**: member removal (`handleRemove`) — this service had no
  Valkey wiring at all until this pass. It busts by account, not the userID
  the RPC is keyed on: `u.Account` from the same `FindUser(userID)` lookup
  already used to populate `subscriptions.u.account` at add-time via
  `UpsertSubscription`, which is exactly what `SubKey`/`FetchFromMongo` key
  and filter on.

This means the "Revocation during the outage is honored fail-open up to the
outage TTL" bullet above now describes only genuine-outage behavior — the
mutating write itself fails while Mongo is down, so there is nothing to bust
until Mongo recovers, at which point the write and the bust land together. In
normal operation (Mongo reachable), removal/role/visibility changes — local or
federated, single-account or bulk — take effect on the next request, not after
up to 90 minutes, so the 2026-05-18 message-pipeline design's "remove is an
immediate-effect tool" guarantee holds for the L2 tier across every
subscription-mutating write path, not just the ones a mutation happens to
target a single account. The only residual staleness is a failed
`BustSub`/`BustSubs` call itself (Valkey error), which is best-effort and
reconciled by the L2 TTL like every other bust in this codebase
(`roommetacache.BustMeta`).

## Configuration

All via `caarlos0/env` with `envDefault`, per house convention. New knobs:

| Env var | Service(s) | Default | Purpose |
|---|---|---|---|
| `*_SUB_L2_TTL` (e.g. `GATEKEEPER_SUB_L2_TTL`, `HISTORY_SUB_L2_TTL`) | gatekeeper, history | `90m` | Subscription L2 (outage) retention window |
| `*_MONGO_BREAKER_FAILS` | both | `5` | Consecutive failures to open the breaker; `0` disables it (calls always pass through) |
| `*_MONGO_BREAKER_COOLDOWN` | both | `10s` | Open→half-open cooldown |

L2 is disabled (falls back to L1-only) when the Valkey client is nil, matching
the existing `roommetacache` fail-open wiring.

## Observability

- Reuse `pkg/cachemetrics` — the new sub-authz L2 emits the standard
  `cache="subauth",tier="l2"` hit/miss/error series; L1 keeps its series.
- Add a **breaker-state gauge** (closed/open/half-open): one shared
  `circuit_breaker_state` instrument, with every breaker wired through
  `circuitbreaker.Tracked(ctx, name)`. The `breaker` attribute
  (`subscription`, `roommeta`, `atrestdek`) is required — several breakers per
  service record to the same instrument, so without it their datapoints
  overwrite each other and the gauge describes whichever breaker moved last.
  The downstream is a label, not part of the metric name: the breaker guards
  whatever a service puts behind it, not MongoDB specifically.
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
