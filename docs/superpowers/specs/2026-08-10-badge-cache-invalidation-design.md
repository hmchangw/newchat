# Badge Cache: ClearAll Invalidation, Full-Audience Bumps, Cache-First Count

> **Partially superseded** by `2026-08-19-badge-precise-read-invalidation-design.md`:
> the read rows of §4 (reads now remove exactly the room read rather than
> dropping the set) and the §7 accuracy model (drift is now bounded by the
> marker TTL). §4's ClearAll choice rested on the recompute being unconditional,
> which stopped being true when `BADGE_COUNT_CACHE_FIRST` was flipped to `true`.

**Status:** Approved (pending implementation)
**Date:** 2026-08-10
**Depends on:** 2026-08-02-thread-unread-badge-design.md (implemented)

## 1. Problem

The Valkey badge set (`pkg/badgecache`, one SET of unread room IDs per account)
currently serves only the push path (`badge.count.batch`). Three gaps:

1. **Mute toggle staleness.** Muting a room does not remove its entry; the
   pushed badge overcounts until reseed/TTL. Unmuting never re-adds an unread
   room. (The compute paths already exclude muted via
   `activeSubscriptionFilter`, and the Bump path is mute-safe because
   notification-worker filters muted members before the RPC — only the toggle
   is unhandled.)
2. **Read-path guard asymmetries.** The read hooks exist on every path, but the
   thread-read clear checks only thread state (`threadUnread` drained), so it
   removes a room that is still message-unread; `thread.read.all`'s `ClearAll`
   wipes message-unread rooms too. Both undercount until the next bump/reseed.
   inbox-worker additionally pays a `SubscriptionHasThreadUnread` Mongo query
   per federated read event just to drive the guard.
3. **Desktop count never uses the cache.** `subscription.count` (unread=true)
   always runs the Mongo `$lookup` aggregation + cross-site `GetRoomsMeta`
   fan-out, then reseeds. Every desktop badge refresh pays the expensive path,
   and desktop (Mongo-computed) and mobile (cache-computed) badges can disagree.

## 2. Design overview

Three coordinated changes that make the set a genuinely maintained
materialization (the Slack model: per-user unread state updated on every
message and every read; one cheap read to hydrate the badge):

1. **ClearAll-on-invalidation.** Any event that can *decrease* the count drops
   the whole set (no guards); events that *increase* it SADD (bump) or seed.
   A wrongly-dropped set is never wrong — just cold; the next touch recomputes
   from Mongo truth.
2. **Bump all non-muted recipients.** notification-worker's badge RPC audience
   widens from push survivors to the full badge audience, so online users' sets
   stay current between reads.
3. **Marker-gated cache-first count.** `subscription.count` (unread=true)
   serves `SCARD` when a freshness marker exists; misses fall back to today's
   compute + reseed. Gated by env until all writers run the new invalidation.

## 3. pkg/badgecache changes

### 3.1 Freshness marker

New sibling key `badge:fresh:{account}` (same `{account}` hash tag → same
cluster slot as the set, so scripts may address both). Semantics: *marker
exists ⇒ the set is an accurate materialization of the account's unread rooms,
where a missing/empty set means zero unread.* Marker and set always carry the
same TTL, refreshed together.

### 3.2 Script / method changes

All scripts gain `KEYS[2]` = marker key.

- **bump**: miss (`-1`) only when *neither* set nor marker exists. When the
  marker exists but the set doesn't (fresh all-read state), SADD creates the
  set — a first message after all-read is an O(1) hit (count 1), not a reseed.
  On hit: `SADD` + `EXPIRE` set, `SET marker EX ttl`, return `SCARD`.
- **seed / reseed**: unchanged set behavior; additionally always
  `SET marker EX ttl` — including the empty-ids case, which now records
  "fresh, zero unread" instead of leaving no trace.
- **ClearAll**: `DEL` both keys.
- **ClearRoom**: `SREM` set only. The marker survives — removing one room
  keeps the materialization exact (used by mute-on and member_removed, where
  the removal is precise and no recompute is needed).
- **New `Count(ctx, account) (n int, fresh bool)`**: pipelined
  `EXISTS marker` + `SCARD set`. `fresh=false` on missing marker or any Valkey
  error (fail-open — caller recomputes). `fresh=true, n=0` is the legitimate
  all-read answer.

`BumpBatch` keeps its pipelined shape (EVALSHA batch + one pipelined EVAL
retry pass on NOSCRIPT); scripts just gain the second key.

## 4. Invalidation policy (who clears what)

| Event | room-service (actor home-local) | inbox-worker (federated home replica) |
|---|---|---|
| `message.read` | **ClearAll** (replaces `ThreadUnread`-guarded ClearRoom) | **ClearAll** (replaces post-state check + ClearRoom) |
| thread read | **ClearAll** (replaces drained-guard ClearRoom) | **ClearAll** (replaces post-state check + ClearRoom) |
| `thread.read.all` | ClearAll (unchanged) | ClearAll (unchanged) |
| mute → muted | **ClearRoom** (exact removal; set stays fresh) | **ClearRoom** |
| mute → unmuted | **ClearAll** (recompute re-adds the room iff unread) | **ClearAll** |
| `member_removed` | — | ClearRoom (unchanged; exact removal) |

Rules of thumb encoded above: reads invalidate wholesale (the desktop client
calls `subscription.count` right after, which rebuilds the set from Mongo —
the same aggregation that call already runs today); mute-on and member-removed
are precise single-room removals that keep the marker; unmute needs an unread
check we don't want to do inline, so it recomputes.

Both room-service hooks keep the existing `userSiteID == h.siteID` guard —
cross-site actors' home replicas are handled by inbox-worker via the already-
federated events (`subscription_read`, `thread_read`, `thread_read_all`,
`subscription_mute_toggled` carries `Muted` for direction).

**Removal:** `SubscriptionHasThreadUnread` leaves the inbox-worker store
interface and implementation (its only two call sites were the guards deleted
here).

## 5. notification-worker: full badge audience

Split the recipient pipeline into two sets:

- **badgeAccounts** — members minus sender, muted, restricted-gated, and (for
  thread-only replies) non-followers/non-mentioned. This is "everyone whose
  badge state this message changes."
- **push survivors** — badgeAccounts further filtered by hook veto,
  `EligibleForPush`, and presence (`shouldPush`), exactly as today.

`fetchUnreadCounts` is called with **badgeAccounts** (bumping everyone's set,
counts fetched per home site as today); the push payload's `unreadCounts` still
carries only each batch's survivor accounts (`filterUnreadCounts` unchanged).
Hook/eligibility/presence are push-delivery concerns — a hook-vetoed or online
member's room is still unread, so their set must still be bumped.

Cost: `BumpBatch` is pipelined per cluster node, so Valkey cost per message is
~unchanged; the per-site RPC batches grow from survivors to badge audience
(bounded by room size, same shape). Extra counts in the reply are ignored.

## 6. user-service: cache-first `subscription.count`

```go
if unread {
    if s.badgeCountCacheFirst {
        if n, fresh := s.badge.Count(c, account); fresh {
            return n            // SCARD; may legitimately be 0
        }
    }
    ids := s.unreadRooms(c, account)   // today's compute
    s.badge.Reseed(c, account, ids)    // writes marker
    return len(ids)
}
```

- `badgeCache` interface gains `Count`; the noop impl returns `(0, false)`.
- Returned count stays uncapped (as today); no wire change to
  `subscription.count` — no `docs/client-api.md` schema edit, but its prose
  note on count freshness and `docs/notification-worker-downstream-contracts.md`'s
  accuracy model are updated.
- Mobile (pushed) and desktop (RPC) badges now derive from the same set —
  consistent by construction.

### 6.1 Rollout gate (required, not optional)

`BADGE_COUNT_CACHE_FIRST` (user-service env, default **false**).

Hazard if served immediately: an **old** room-service/inbox-worker binary's
`ClearAll` deletes only the set, leaving a marker written by a new
user-service — `Count` would then return a fresh-looking **0** for a user with
unread rooms, wrong for up to the full marker TTL. Therefore: deploy the new
`pkg/badgecache` (marker-aware clears) to **all badge writers (room-service,
inbox-worker, user-service, notification-worker path)** first, then flip
`BADGE_COUNT_CACHE_FIRST=true`. Until the flip, behavior is exactly today's
(compute + reseed), with reseed already writing markers so the cache warms up.

Existing sets without markers self-migrate: count misses → recompute → marker
written. No data migration.

## 7. Accuracy model (updated)

The set is maintained on both edges: arrival (full-audience bump, exact for
the trigger room) and decrease (ClearAll/ClearRoom per §4). Residual drift —
a missed bump (Valkey blip), a `ClearAll` racing a concurrent bump — is
bounded by: the next read/mute event (ClearAll → recompute), the next count
miss, and the TTL backstop (`BADGE_CACHE_TTL`, default 24h). Failure direction
on races is *undercount-until-recompute*, never a stuck overcount. The
`badge:fresh` marker never outlives the set's accuracy because every code path
that deletes or degrades the set deletes the marker in the same script/call.

## 8. Testing

- **pkg/badgecache (integration, Valkey cluster):** marker written by
  seed/reseed (incl. empty reseed → fresh zero); bump hit on marker-only state
  creates the set (count 1); ClearRoom keeps marker; ClearAll removes both;
  `Count` fresh/stale/error triples; NOSCRIPT retry still heals with two keys.
- **room-service / inbox-worker (unit):** each event row in §4's table drives
  exactly the specified call — including mute direction — and the removed
  guards/`SubscriptionHasThreadUnread` expectations are gone.
- **notification-worker (unit):** the badge RPC audience excludes the sender,
  muted, restricted-gated, and thread-scope-excluded members, but INCLUDES
  hook-vetoed, push-ineligible, and online members; push payload accounts stay
  survivor-only.
- **user-service (unit):** gate off → always compute; gate on + fresh →
  SCARD without repo calls (including fresh zero); gate on + stale → compute +
  reseed. Config parse/validation for the new env.

## 9. Out of scope

- Moving badge writes into broadcast-worker (separate consolidation track).
- `hasMention` federation from broadcast-worker (separate PR, noted earlier).
- A `wantCounts` hint on `badge.count.batch` to slim replies (YAGNI for now).
