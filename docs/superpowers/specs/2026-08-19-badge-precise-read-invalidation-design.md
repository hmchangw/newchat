# Badge Cache: Precise Read Invalidation + Bounded-Staleness Marker

**Status:** Draft (pending review)
**Date:** 2026-08-19
**Supersedes:** `2026-08-10-badge-cache-invalidation-design.md` §4 (read rows) and §7 (accuracy model)

## 1. Problem

`BADGE_COUNT_CACHE_FIRST` is now `true` in production, so `subscription.count`
(`unread=true`) serves `SCARD` on a freshness-marker hit. The taskbar badge is
nonetheless still paid for out of MongoDB, because the cache is bypassed on its
hottest trigger.

Every read path calls `ClearAll` (`room-service/handler.go:1391`,
`inbox-worker/handler.go:366`), which deletes the set *and* the marker. The
desktop client refetches the count immediately after every `markRoomRead`
(`chat-frontend/src/context/RoomEventsContext/useUnreadCount.js`). So opening a
room — the most frequent action in the product — deterministically forces the
full recompute:

- `$match` on `(u.account, muted, roomType)`, `$limit 1000`
- a **correlated `$lookup` into `rooms`, run once per subscription**
- `$unwind` + `$regex` deleted-name filter + `$addFields`, decoded into the fat
  `EnrichedSubscription`
- a `GetRoomsMeta` RPC per remote site

(`user-service/mongorepo/subscriptions.go:467`, `user-service/service/subscriptions.go:580`.)

**The count RPC is not the only trigger.** `fetchUnreadCounts` runs over the full
badge audience *before* presence filtering (`notification-worker/handler.go:251`;
survivors are filtered at `:244`, but the badge phase deliberately ignores them —
"bumps must land even for members who won't be pushed"). So every message
re-consults the set of every non-muted recipient, **including online ones**, and a
bump miss falls through to `Seed` → `unreadRooms` → the same aggregation. A user
who reads a room therefore pays a recompute twice over: once when their client
refetches the count, and again on the next message in any of their rooms, via the
push pipeline. The read invalidation is load-bearing on both paths, which is why
fixing it survives even a future where clients compute their own badge.

§4 of the superseded spec chose `ClearAll` deliberately, on an assumption stated
in its own rule of thumb: *"the desktop client calls `subscription.count` right
after, which rebuilds the set from Mongo — the same aggregation that call already
runs today."* That was correct while the flag was `false` and the recompute was
unconditional. Flipping the flag retired the assumption; §4 was never revisited.

The second reason for `ClearAll` was sound and still is: the guarded `ClearRoom`
it replaced was **wrong**. Per §1 of the superseded spec, the thread-read clear
checked only thread state and so removed rooms that were still message-unread,
and inbox-worker paid a `SubscriptionHasThreadUnread` Mongo query per federated
read event purely to drive the guard. This design does not reinstate that guard.

## 2. Design overview

Two changes, both confined to the room-read path and the marker's lifetime:

1. **Precise removal on room read.** A room read is the one invalidation whose
   post-state is derivable without asking Mongo anything new: `lastSeenAt` is set
   to `now`, so message-unread is settled by construction, and `threadUnread`
   comes back from the read's own write (`FindOneAndUpdate`, same round trip, no
   extra query). Empty ⇒ `ClearRoom` (exact, marker survives).
   Non-empty ⇒ **no cache write at all** — the room was unread before the read
   and remains unread after it, so an exact materialization needs no edit.
2. **The marker becomes a bounded-staleness token.** Today the marker is
   refreshed by every bump, so an active user's set can go indefinitely without
   ever being checked against Mongo. It gets its own, shorter TTL
   (`BADGE_MARKER_TTL`, user-service only), refreshed **only** by Seed/Reseed —
   the paths that actually verify against Mongo. Marker absent ⇒ miss ⇒
   recompute + Reseed, even when the set still exists.

Change 1 removes the recompute from the hot path. Change 2 is what makes that
safe: it converts "drift persists until the next `ClearAll` or the 24h set TTL"
into "drift persists at most `BADGE_MARKER_TTL`", and it is the tuning knob if
the accuracy trade turns out wrong in production.

Thread-read paths (`thread.read`, `thread.read.all`) keep `ClearAll`. Their
post-state genuinely is ambiguous — clearing one thread says nothing about
message-unread — and they are rare next to room reads. This is exactly the guard
the superseded spec deleted, and it stays deleted.

## 3. `pkg/badgecache` changes

### 3.1 Marker TTL

`New` gains a functional option rather than a fourth positional parameter, so
the two clear-only callers are untouched:

```go
func New(rdb redis.UniversalClient, ttl time.Duration, maxCount int, opts ...Option) *Cache
func WithMarkerTTL(d time.Duration) Option
```

Absent the option, `markerTTL == ttl` — today's behavior exactly. Only
`user-service` passes it (see §4.3); `room-service` and `inbox-worker` call
`ClearRoom`/`ClearAll` only and never write a marker, so their construction sites
do not change.

**Invariant: `markerTTL <= ttl`.** If the marker outlived the set, `Count` would
read marker-present + set-expired as a *fresh zero* and report an empty badge for
a user with unread rooms. `WithMarkerTTL` clamps to `ttl` and logs a warning;
user-service's config validation rejects the misconfiguration outright (§4.3).

### 3.2 Script changes

**bump** — a bump is not a verification, so it must no longer re-bless the set:

```lua
if redis.call('EXISTS', KEYS[2]) == 0 then return -1 end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
return redis.call('SCARD', KEYS[1])
```

Two behavior changes: the hit condition is now **marker existence alone** (set
present + marker expired is a miss, not a hit — this is the mechanism that bounds
staleness), and the marker's TTL is left untouched so it continues counting down
from the last Mongo verification. The marker-present/set-absent case still works:
`SADD` recreates the set, count 1, no recompute.

**seed** — on a miss the caller holds fresh Mongo truth, and any surviving set is
by definition unverified, so seed replaces instead of unioning:

```lua
redis.call('DEL', KEYS[1])
if #ARGV > 2 then
  redis.call('SADD', KEYS[1], unpack(ARGV, 3))
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
return redis.call('SCARD', KEYS[1])
```

ARGV becomes `[setTTL, markerTTL, roomIDs...]`. Without this, a stale leftover
set would contaminate a freshly computed answer — reachable now that a set can
outlive its marker.

**reseed** — unchanged except that the marker is stamped with `markerTTL`.

`ClearRoom` (SREM, marker untouched), `ClearAll` (DEL both), and `Count`
(`EXISTS marker` + `SCARD`) are unchanged.

## 4. Service changes

### 4.1 room-service — `messageRead`

The `ClearAll` at `handler.go:1391` becomes:

```go
// A read settles message-unread (lastSeenAt = now); the room stays unread only
// if a followed thread is unread. Empty ⇒ exact removal, marker survives.
// Non-empty ⇒ the room was and remains unread, so the set needs no edit.
if userSiteID == h.siteID && h.badge != nil && threadUnread == 0 {
    h.badge.ClearRoom(ctx, account, roomID)
}
```

`threadUnread` is the **post-update** count, not the pre-write snapshot at
`handler.go:1344`. `UpdateSubscriptionRead` (`store_mongo.go:1098`) changes
shape to match inbox-worker's (§4.2):

```go
UpdateSubscriptionRead(ctx context.Context, roomID, account string,
    lastSeenAt time.Time) (threadUnread int, err error)
```

`UpdateOne` → `FindOneAndUpdate` with `SetProjection({threadUnread: 1})` and
`ReturnDocument(After)` — the same single round trip, with
`model.ErrSubscriptionNotFound` now driven by `mongo.ErrNoDocuments` instead of
`MatchedCount == 0`.

**Why not the existing snapshot.** `sub` is already in hand and
`subscriptionReadProjection` (`store_mongo.go:316`) already carries
`threadUnread`, so the snapshot is free — but it is read *before* the write, and
`threadUnread` is written by a different service on an unordered path (§6, race
2). Reading it from the write's own return value collapses the vulnerable
interval from `[GetSubscription, SREM]` to `[write, SREM]`, removing the larger
part of the window for one extra return value on a method whose sibling is
changing shape anyway. `sub` is still fetched — the `subscription.update` publish
needs it — just no longer consulted for this decision.

The existing `userSiteID == h.siteID` guard is preserved, and placement is
unchanged (before the room-floor early returns), so the badge is maintained on
every successful read.

Store interface change ⇒ `make generate` for `room-service/mock_store_test.go`.

**Considered and rejected:** additionally guarding on
`room.LastMsgAt <= lastSeenAt` using the `room` already fetched in the same
errgroup. It protects nothing: `lastMsgAt` carries the *message's* timestamp, so
any message the user has just read compares as older than `now` whether or not
broadcast-worker's 250 ms coalescer (`LAST_MSG_FLUSH_INTERVAL`) has flushed it.
The comparison is the same one the recompute performs, so the guard can only
duplicate a decision already made. Dropped as dead weight.

### 4.2 inbox-worker — `handleSubscriptionRead`

The federated leaf needs the same post-state, and the query the superseded spec
priced as "an extra Mongo read" is avoidable: the update it already issues can
return the document.

```go
UpdateSubscriptionRead(ctx context.Context, roomID, account string,
    lastSeenAt time.Time, alert bool) (applied bool, threadUnread int, err error)
```

`main.go:430` switches `UpdateOne` → `FindOneAndUpdate` with
`SetProjection({threadUnread: 1})` and `ReturnDocument(After)` — the same single
round trip, keeping the `$lt lastSeenAt` order-safety filter and the
`naksIfSubscriptionMissing` path (now driven by `mongo.ErrNoDocuments`).

```go
if applied && h.badge != nil && threadUnread == 0 {
    h.badge.ClearRoom(ctx, e.Account, e.RoomID)
}
```

`applied == false` means the `$lt` guard rejected an out-of-order or replayed
event. Today such an event still calls `ClearAll`, discarding a good set on a
no-op write; gating on `applied` fixes that incidentally.

Store interface change ⇒ `make generate` for `inbox-worker/mock_store_test.go`.

### 4.3 user-service — config

```go
// BadgeMarkerTTL bounds how long the badge set may go without verification
// against Mongo: the marker is stamped only by Seed/Reseed (which compute from
// Mongo) and is never refreshed by bumps, so it doubles as the maximum badge
// staleness. Must be <= BADGE_CACHE_TTL — a marker outliving its set would read
// as a fresh zero.
BadgeMarkerTTL time.Duration `env:"BADGE_MARKER_TTL" envDefault:"10m"`
```

Validation alongside the existing `BADGE_CACHE_TTL` check
(`user-service/config/config.go:109`): reject `<= 0`, and reject
`> BadgeCacheTTL` with both values named. `main.go:173` passes
`badgecache.WithMarkerTTL(cfg.BadgeMarkerTTL)`. Local
`user-service/deploy/docker-compose.yml` sets it explicitly.

No config change in room-service or inbox-worker: neither writes a marker.

### 4.4 user-service — a degraded compute must not stamp the marker

`unreadRooms` degrades per-site: when a cross-site `GetRoomsMeta` RPC fails, that
site's rooms are dropped from the result and the failure is only logged — it
still returns `(ids, nil)` (`service/subscriptions.go:628`, `:662`).
`CountSubscriptions` then calls `Reseed(ids)` unconditionally, which **stamps the
marker**, blessing a knowingly-incomplete set as verified.

Today this is masked: the next room read `ClearAll`s the set seconds later. Once
reads stop wiping the set, one transient cross-site blip would bake an
undercount in for a full marker window — spending the staleness budget on a
failure we already knew about.

```go
func (s *UserService) unreadRooms(c *natsrouter.Context, account string) (ids []string, degraded bool, err error)
```

`degraded` is set where the per-site goroutine currently logs and returns. Both
callers still return their best-effort count to the client — that behavior is
unchanged — but skip the cache write entirely when `degraded` is true:

- `CountSubscriptions`: skip `Reseed`, so the next call recomputes.
- `BadgeCountBatch`: skip `Seed` and fall through to `cappedUnion`, the existing
  cache-down arithmetic.

A degraded result is not worth caching, so no marker-less write variant is
needed — the whole cache write is skipped, and the marker's meaning (§6) holds
without exception.

## 5. Revised invalidation policy

Supersedes §4 of the 2026-08-10 spec. Changed rows in bold.

| Event | room-service (actor home-local) | inbox-worker (federated home replica) |
|---|---|---|
| `message.read`, no unread threads | **ClearRoom** (exact; marker survives) | **ClearRoom** (exact; marker survives) |
| `message.read`, unread threads remain | **no cache write** (still unread) | **no cache write** (still unread) |
| `message.read`, `$lt` guard rejected | — | **no cache write** (no write applied) |
| thread read | ClearAll (unchanged) | ClearAll (unchanged) |
| `thread.read.all` | ClearAll (unchanged) | ClearAll (unchanged) |
| mute → muted | ClearRoom (unchanged) | ClearRoom (unchanged) |
| mute → unmuted | ClearAll (unchanged) | ClearAll (unchanged) |
| `member_removed` | — | ClearRoom (unchanged) |

## 6. Revised accuracy model

Supersedes §7. The marker's meaning tightens from *"the set is accurate"* to
**"the set was computed from Mongo within `BADGE_MARKER_TTL`, and every edit
since was exact."** Exact edits are: bump `SADD` (a new message makes the room
unread), read `SREM` (a fully-read room is not unread), mute `SREM`,
`member_removed` `SREM`. Everything inexact deletes both keys.

### 6.1 Residual drift

**Race 1 — a plain message.** For a message to be wrongly dropped it must be
newer than the read's `lastSeenAt` (otherwise the user genuinely read it) *and*
its bump `SADD` must land before our `SREM`. The `SADD` travels
gatekeeper → MESSAGES-CANONICAL → notification-worker → `badge.count.batch` RPC →
Valkey, while the `SREM` is one Mongo round trip away, so this requires the read
handler's write to outlast that entire pipeline. Reachable only under a Mongo
stall (step-down, failover, write-concern backpressure); otherwise the bump lands
after the `SREM`, finds the marker still present, and correctly re-adds the room.

**Race 2 — a threaded reply.** This is the reachable one.
`Subscription.threadUnread` is written by **message-worker**
(`store_mongo.go:196`, `$addToSet`), while the badge bump comes from
**notification-worker**. Both are independent durable consumers of
MESSAGES-CANONICAL (`message-worker/main.go:182`,
`notification-worker/main.go:264`) — **nothing orders them**. So:

```
t0  reply lands in a thread the user follows, in room R
t1  notification-worker bumps        → SADD R        (correct: R is unread)
t2  the user marks R read
      · write lastSeenAt = now, read back threadUnread → still []
      · SREM R                                        (wrong: R is thread-unread)
t3  message-worker → $addToSet threadUnread = [parentID]
```

Unlike race 1 there is no "they read it anyway" escape: marking a *room* read
does not drain `threadUnread` (only `thread.read` does), so the reply may predate
the read and still be legitimately unread. The window is the skew between the two
consumers handling the same reply — small under normal load, seconds under
message-worker backlog or a JetStream redelivery. §4.1's post-update read shrinks
the exposure to `[write, SREM]` but cannot close it; only giving one component
ownership of both writes would (§11).

The cross-site path has the same shape: inbox-worker writes `threadUnread` for
`thread_unread_added` (`main.go:646`) while the bump originates from the remote
site's notification-worker.

**Missed bump** (Valkey blip): unchanged from today.

**Race 3 — concurrent Seed on a marker miss.** `seedScript` `DEL`s before
`SADD` (replace, not union), so two concurrent `Seed`s for the same account —
both arriving on a marker miss, each carrying a different `triggerRoomID` —
are last-writer-wins rather than unioning:

```
t0  marker expires
t1  a message in room A misses  → computes ids from Mongo
t2  a message in room B misses  → computes ids from Mongo
t3  B's Seed(ids, rB) lands     → set = ids ∪ {rB}
t4  A's Seed(ids, rA) lands     → DEL, set = ids ∪ {rA}   (rB lost)
```

Neither trigger room is recoverable from `ids`, because broadcast-worker's
`lastMsgAt` coalescer (250ms flush) means Mongo has not caught up on either by
the time each `Seed` reads it. Consequence: undercount by one room, bounded by
`BADGE_MARKER_TTL` and self-healing — inside the envelope §6 already
advertises. Most reachable exactly at marker expiry, when a per-account burst
of misses is most likely. §11's parked A3 item (per-account singleflight)
is the structural fix — collapsing concurrent misses into one Mongo
recompute would make this race unreachable rather than merely bounded.

Failure direction in every case remains *undercount-until-recompute*, never a
stuck overcount.

### 6.2 Self-heal

Drift is not left to chance, and does not depend on the user touching the room
again. The marker's TTL **is** the recovery clock:

```
marker stamped (EX markerTTL) ── only by Seed/Reseed, which compute from Mongo
        ├── bump SADD ......... does NOT refresh it   (§3.2)
        ├── read SREM ......... does NOT touch it
        └── ClearAll .......... deletes it outright
        ▼
marker expires ≤ markerTTL after the last Mongo verification
        ▼
next Count / BumpBatch finds no marker ⇒ miss
        ▼
recompute from Mongo + Reseed ⇒ set correct again
```

Nothing can extend the marker's life except an actual Mongo recompute — which is
why §4.4 forbids stamping it after a degraded compute. So **time since last
verification is capped at `BADGE_MARKER_TTL`** (10m default) rather than at
`BADGE_CACHE_TTL` (24h), for both the count path and the push path, whichever
arrives first after expiry. An idle account recomputes on its next request, so
the number a user is actually shown is never more than one marker window stale.

The marker still never outlives the set's accuracy: every path that deletes or
degrades the set deletes the marker in the same script or call, and
`markerTTL <= ttl` is enforced at construction and in config validation.

## 7. Expected effect

Per active user, Mongo recomputes drop to **at most one per `BADGE_MARKER_TTL`
window**, whichever path — count or push — arrives first after the marker
expires. `ClearAll` events (thread reads, unmutes, membership changes) add one
each, as today.

Today's baseline is roughly one recompute per room open from the client's
post-read refetch, *plus* one per read from the push pipeline's next
full-audience bump (§1), plus reconnects. A user opening 30 rooms an hour is
therefore paying well over 30 recomputes/hour and drops to ~6 at the 10m default;
heavier users and users in busier rooms benefit proportionally more, since the
push-triggered half scales with message volume rather than with their own
activity.

Worth measuring before and after: `subscription.count` p99 and the count of
`GetActiveSubscriptions` aggregations per minute.

**Assumptions to verify during rollout.** §6.1's relative latencies are inferred
from pipeline shape, not measured (the *ordering* facts — parallel consumers, who
writes `threadUnread` — are confirmed in code). Two are worth checking against
real consumer lag: that notification-worker's bump outpaces a read handler's
Mongo write (race 1's improbability rests on it), and how wide the
message-worker/notification-worker skew actually runs (race 2's window is exactly
that). Neither affects §6.2's bound, which holds regardless; a wide skew is an
argument for lowering `BADGE_MARKER_TTL`, not for abandoning the approach.

## 8. Rollout and rollback

No new feature flag. The changes are individually safe under mixed binaries:

**Deploy user-service first**, then room-service and inbox-worker. The bound
(change 2) must never lag the precision (change 1):

- **user-service first, old room-service/inbox-worker:** old binaries still
  `ClearAll` on read, which remains correct — the set goes cold and recomputes.
  Mongo load is unchanged from today (the read `ClearAll` already dominates the
  10m marker expiry); only the saving is deferred. Safe to sit in this state
  indefinitely.
- **The reverse order is the one to avoid, and worse than "bounded by 24h":**
  new room-service/inbox-worker with an old user-service means precise
  `ClearRoom` leaves a marker that the old bump script keeps refreshing — the
  OLD `bumpScript` re-stamps the marker on **every** message
  (`redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])`), not just on a Seed/Reseed
  recompute. For an active account the marker never expires at all, so drift
  is bounded not by the 24h set TTL but by the next `ClearAll` event — a
  thread read, an unmute, or a membership change — which for many users is
  days, or never. This is the exact combination §6's accuracy model rules out,
  and it is why the deploy order above is a hard requirement, not a
  preference.

Once user-service is fully rolled out, before deploying room-service/
inbox-worker, either let old markers age out (up to `BADGE_MARKER_TTL`, 10m
default) or purge them directly: `SCAN` for `badge:fresh:*` (the keys are
hash-tagged, so a cluster-wide `SCAN` per node finds them) and `DEL` each. This
matters even though the deploy order above is correct, because user-service
itself rolls out gradually: during that window, old user-service pods still
stamp markers with the full `BADGE_CACHE_TTL` (24h), and nothing in the new
code shortens a live marker — `Count` returns early on a fresh marker without
reseeding, and the new `bumpScript` deliberately never touches marker TTL. So
an account touched only by an old pod during the rollout window keeps a 24h
staleness window that survives the user-service rollout and carries into the
room-service/inbox-worker rollout — the very state this section exists to
prevent. This is largely self-limiting: while user-service is still mixed,
the still-old room-service/inbox-worker keep `ClearAll`-ing on every read,
which deletes those markers for any active user within minutes. The residue
that needs the explicit purge is idle accounts whose marker was stamped by an
old pod and who don't read anything before the next deploy.

Rollback is `BADGE_MARKER_TTL` — lowering it (e.g. `1m`) tightens drift toward
today's behavior at proportionally more recomputes, without a redeploy. Setting
it equal to `BADGE_CACHE_TTL` restores the old marker lifetime; only the
read-path precision then needs a redeploy to revert.

## 9. Testing

TDD, red first, per CLAUDE.md §4. Minimum 80% coverage; `pkg/badgecache` is
shared-`pkg/` code and targets 90%+.

**pkg/badgecache (unit + integration against the shared Valkey cluster):**
- `WithMarkerTTL` absent ⇒ `markerTTL == ttl` (backward compatibility)
- `WithMarkerTTL` greater than `ttl` ⇒ clamped to `ttl`
- Seed/Reseed stamp the marker with `markerTTL`, the set with `ttl` (assert both
  `PTTL`s)
- bump does **not** extend the marker's TTL (assert `PTTL` strictly decreases
  across a bump)
- bump with set present + marker absent ⇒ miss (`-1`), no writes
- bump with marker present + set absent ⇒ hit, count 1
- Seed replaces rather than unions when an unverified set survives
- NOSCRIPT retry still heals with the new ARGV shapes

**room-service (unit, `handler_test.go`):** post-update `threadUnread == 0` ⇒
exactly one `ClearRoom(account, roomID)` and no `ClearAll`; post-update non-empty
⇒ no badge call at all; **the decision follows the post-update value, not the
pre-write snapshot** (mock returns a snapshot and a post-update value that
disagree, in both directions); cross-site actor ⇒ no local badge call
(unchanged); nil badge ⇒ no panic.

**room-service (integration):** `FindOneAndUpdate` returns the post-update
`threadUnread` and still maps a missing subscription to
`model.ErrSubscriptionNotFound`.

**user-service (unit, degraded computes):** a failing cross-site site ⇒
`CountSubscriptions` returns the best-effort count but calls **no** `Reseed`;
`BadgeCountBatch` returns the `cappedUnion` count but calls **no** `Seed`; the
non-degraded paths still write as today.

**inbox-worker (unit, `handler_test.go`):** applied + `threadUnread == 0` ⇒
`ClearRoom`; applied + non-empty ⇒ no badge call; `applied == false` (`$lt` guard
rejected) ⇒ no badge call; missing subscription still NAKs.

**inbox-worker (integration):** `FindOneAndUpdate` returns the post-update
`threadUnread` and preserves the `$lt` order guard.

**user-service (unit):** `BADGE_MARKER_TTL` parse, default, and both validation
rejections; marker expiry ⇒ `Count` stale ⇒ recompute + `Reseed`.

## 10. Documentation updates (same PR)

- `docs/client-api.md` — the `subscription.count` **Caching** note (line 5551):
  staleness is bounded by `BADGE_MARKER_TTL`, not "invalidated on every read".
  Prose only; no request/response schema change, so the derived views
  (`request-reply.md`, `events.md`) are untouched.
- `docs/notification-worker-downstream-contracts.md` — accuracy model.
- `2026-08-10-badge-cache-invalidation-design.md` — header note marking §4's read
  rows and §7 superseded by this document.

## 11. Out of scope

- **A2** — replacing the correlated `$lookup` in `GetActiveSubscriptions` with a
  projected subs query plus one `rooms.find({_id: {$in: ...}})`. Makes the
  surviving recompute cheaper; independent of this change.
- **A3** — per-account singleflight and a recompute cooldown in user-service, for
  reconnect storms and deploys.
- **Closing race 2 structurally** — giving one component ownership of both the
  `threadUnread` write and the badge write, so they stop being two unordered
  consumers of the same message. This is the only fix that eliminates (rather
  than narrows) race 2.

  **It is not the same as the "move badge writes into broadcast-worker"
  consolidation** parked in the superseded spec's §9, and doing that alone would
  not close this race. `threadUnread` is written by `message-worker`
  (`handler.go:663` → `AddThreadUnread`), and `broadcast-worker` is a *separate*
  MESSAGES-CANONICAL consumer (`main.go:193`) — so moving the badge write there
  swaps which pair of unordered consumers races without removing the race. The
  badge write has to land in `message-worker`, or `threadUnread` has to move
  alongside it.

  Two constraints bind whichever component takes it. The badge set lives in the
  **user's home-site** Valkey while these workers run at the **room's** site, so
  a direct Valkey write only covers same-site members and federated members still
  need the `badge.count.batch` RPC. And the miss path (`Seed` from the Mongo
  unread aggregation) lives in `user-service`, so a new writer can bump but must
  never stamp the marker — only Mongo-verifying paths may, per §6.2.
- Any client change (piggybacked counts, refetch cadence): server-side only by
  constraint. Optimistic local decrement on mark-read — the client adjusting its
  own badge instantly and reconciling with the server's number — is the shape to
  reach for if badge/sidebar mismatch complaints appear; it keeps the server as
  the single authority.
- Making the Valkey set authoritative with a background reconciler (approach C).
