# Unread Count: Projection Narrowing on the Badge Path

**Date:** 2026-08-23
**Status:** Proposed
**Related:** `2026-08-19-badge-precise-read-invalidation-design.md` (§11, lever "A2"),
PR #197 (`subscription.list` phased read), PR #353 (`Del-` filter removal, **merged** as `66324f6`)

## 1. Problem

`GetActiveSubscriptions` (`user-service/mongorepo/subscriptions.go:496`) serves the
unread badge. It is reached from two call sites, both hot:

- `CountSubscriptions` with `unread:true` (`user-service/service/subscriptions.go:565`),
  which the desktop client refetches after every `markRoomRead` — opening a room is
  the most frequent action in the product.
- `BadgeCountBatch` (`user-service/service/badge.go:29`), per account, on the push
  path, whenever the Valkey badge set misses.

Both go through `unreadRooms` (`service/subscriptions.go:586`), which reads exactly
**five** fields off each row: `RoomID`, `SiteID`, `LastSeenAt`, `ThreadUnread`, and
the joined `LastMsgAt`.

What it is handed instead is `model.EnrichedSubscription`: the whole subscription
document — `u`, `roles`, `name`, `roomType`, `joinedAt`, `hasMention`, `alert`,
`muted`, `favorite`, `sectionId`, `sectionOrder`, `open`, `restricted`,
`externalAccess`, six `*UpdatedAt` fields, `origin`, `historySharedSince`,
`isSubscribed` — plus all eleven fields the rooms join adds, including the room's
E2E key material (`encKeyPriv`, `encKeyVer`).

PR #197 measured this shape directly while optimizing the list path: 759 bytes per
`EnrichedSubscription` row against 130 bytes for its narrowed equivalent, ≈5.8×.
That measurement was never applied to the badge path.

## 2. Scope

**In:**

- `GetActiveSubscriptions` (`mongorepo/subscriptions.go:496`) — one added stage and a
  changed return type.
- Its `SubscriptionRepository` entry (`service/service.go:30`) and the regenerated
  `service/mocks/mock_repository.go`.
- `unreadRooms`'s decode target (`service/subscriptions.go:586`).
- A new row type in `user-service/models/`.

**Out:**

- `CountActiveSubscriptions` — PR #353 (merged) replaced its aggregation with a plain
  `CountDocuments`, so there is no join left to narrow.
- `roomsEnrichStages` (`:80`) — shared by four other callers; untouched.
- `subscription.list`, the cross-site `GetRoomsMeta` lane, `pkg/badgecache`.
- Every client-facing schema: no handler registration, request/response struct or
  server→client event struct changes.

## 3. Design

### 3.1 The projection

The pipeline keeps its current shape — `$match` → `originFilterStage` → `$limit` →
`roomsEnrichStages(true)` — and gains one terminal `$project` naming the five
consumed fields with `_id: 0`.

Placement matters and is safe at both ends:

- The `origin` exclusion is folded into `activeFilter` (#353) and applies in the
  leading `$match`, so dropping `origin` from the output cannot affect filtering.
- Nothing after `roomsEnrichStages` reads a room field other than `lastMsgAt`, which
  `$addFields` hoists to the top level before the projection whitelists it.

Behavior is unchanged in every respect. The cap still precedes the join, and since
#353 removed the deleted-room drop, no stage after the cap can drop a row — the
interface's "the page is exactly limit" contract holds verbatim.

**Superseded by #353:** earlier drafts of this section argued the projection was
safe because the `^Del-` `$match` consumed `room.name` inside `roomsEnrichStages`
before the terminal stage dropped it. That filter no longer exists, so the argument
is moot rather than wrong — the projection needs no such justification now.

### 3.2 The row type

`GetActiveSubscriptions` returns `[]models.ActiveSubscription` — a new five-field
type in `user-service/models/subscription.go`, alongside the service's other owned
types. `LastMsgAt` stays `*time.Time` so `timeutil.TimeToMillis` is unchanged.

A narrow type rather than a narrowly-populated `EnrichedSubscription`: leaving the
old type in place while filling five of its forty-one fields is a silent trap. A later
reader of `sub.Room`, `sub.UserCount` or `sub.HasMention` on this result would get
a zero value with nothing to signal that the field was never fetched. The narrow
type makes the contract compile-time.

Blast radius is contained — `unreadRooms` is the only consumer of this method's
result.

### 3.3 What this buys, and what it does not

This is a **wire-and-decode** win, not a Mongo-CPU one. The `$lookup` still fetches
eleven room fields per row inside the pipeline; the terminal `$project` stops ten of
them from crossing the wire and being unmarshalled into a fat struct in
user-service. This is the term PR #197 measured on its own path at 759 B → 130 B.
Measured here: `models.ActiveSubscription` 167 B against `model.EnrichedSubscription`
895 B on representative rows, 5.4×. `threadUnread` is a variable-length array, so
the ratio differs from PR #197's.

Stated plainly so nobody expects more: **`encKey.priv` is still read inside the
pipeline. It is no longer shipped to user-service.** Eliminating the read requires a
caller-specific `$lookup` projection, which means not sharing `roomsEnrichStages` —
out of scope here, and one of the reasons the de-join remains worth revisiting.

No seek relief. Counting unread requires `lastMsgAt` for every active room, so the
per-row room lookups are irreducible without a cache or denormalization.

## 4. Rejected alternatives

**Return `[]string` of unread room IDs from the repo.** Leaner still, but it would
move the unread comparison into repository code for local rows while the cross-site
lane keeps folding it in `unreadRooms` via `unread()`. Two spellings of one
predicate — including the subtle never-read (`LastSeenAt == nil`) and no-messages
(`LastMsgAt == nil`) cases — is exactly the duplication this codebase has been
consolidating away from.

**Push the comparison into the pipeline** (`$expr: {$gt: [...]}`, count with
`$count`). Same objection, plus `$expr` across a joined field is unindexable, so
there is no compensating index win.

**De-join into a projected subscriptions read plus a batched `rooms` `$in`.** The
technique PR #197 applied to the list path. It saves per-row subpipeline setup,
`$unwind` and `$addFields`, but not one index seek — the list path's win came from a
page/N asymmetry (500 seeks to return 20 rows) that the unread path does not have,
since every active room's `lastMsgAt` is genuinely needed. Deferred; see §9.

## 5. Drift guard

The hazard this change introduces is silent, not loud: someone adds a field to
`unreadRooms`, forgets the `$project`, and reads a zero value instead of getting an
error — a wrong badge with no failure anywhere.

PR #197 hit the same hazard and solved it with a reflection helper (`bsonTagPaths`
in `subscriptions_lite_test.go`) that derives the expected key set from a struct's
bson tags. This design mirrors it: a unit test asserting the projection's key set
equals `ActiveSubscription`'s bson tags exactly, so adding a field to the struct
without projecting it fails the test. If #197 lands first, the two helpers converge
into one.

## 6. Error handling and degradation

Unchanged. Same `Aggregate` call and the same `fmt.Errorf("...: %w", err)` wrap; no
new failure mode. `unreadRooms`'s per-site degradation semantics, the `degraded`
flag, and the rule that a degraded result is never cached are all untouched.

## 7. Testing

TDD throughout — every test written first and confirmed failing for the intended
reason.

**Unit:**

- Projection/struct drift guard (§5).
- `ActiveSubscription` decodes correctly from a full subscription document (it is a
  subset of the same shape).
- BSON-size assertion in PR #197's style: `bson.Marshal` one representative row of
  each type and assert only the size *direction* (`assert.Less`), logging the ratio
  via `t.Logf` rather than pinning it — a pinned ratio would be brittle against any
  future `pkg/model` field addition.

**Integration** (`//go:build integration`, `testutil.MongoDB`, under the package's
existing `TestMain`):

- One account fixture spanning local, cross-site, muted, closed and soft-deleted
  rooms; assert the returned rows carry values identical to today's and that the
  dropped fields are absent.
- Cross-site row (no local room document) still comes back with a nil `LastMsgAt`
  rather than being dropped.
- `threadUnread` survives the projection.

The bulk of the diff is existing service tests whose mocked `GetActiveSubscriptions`
returns `[]model.EnrichedSubscription`, moving to the new type. Run `make generate`
before testing — the store interface changes.

## 8. Measurement

No timed baseline exists for `subscription.count`, and this sandbox has no Docker
daemon, so the byte measurement of §7, following the method PR #197 already used
and published, is what stands in for it here.

Measured: `models.ActiveSubscription` 167 B against `model.EnrichedSubscription`
895 B on representative rows, 5.4×. PR #197's 759 B figure for the enriched row
is the same measurement on its own fixture.

Correctness is covered by CI's `test-integration (user-service)` job against a
real MongoDB.

Post-deploy confirmation: `subscription.count` latency traces, and user-service
memory/GC pressure, which is where a 5.4× reduction in decoded document volume on a
hot path should show up first.

## 9. Interaction with PR #197 and PR #353

**#353 has since merged** (`66324f6`) and this branch was rebased onto it; #197
remains open. This design depends on neither.

- **#353** removed `Del-` filtering entirely, dropping `roomsEnrichStages`'s
  parameter and its regex `$match`, folding `originFilterStage` into `activeFilter`,
  and deleting `SubscriptionRepo.siteID`. The rebase adopted all of it: this change
  adds a stage *after* the enrich stages and calls no filter of its own. Two
  consequences worth recording — the integration assertion that a `^Del-` room is
  excluded became "no room name is filtered from the active set", and the
  short-page caveat this document used to carry is obsolete.
- **#197** rewrites `AggregateSubscriptions` and adds `NewSubscriptionRepo`
  parameters, but leaves `GetActiveSubscriptions` untouched. Its out-of-scope note
  argues the method needs no attention because `$limit` precedes the join. That
  argument is correct about *seeks* and is why §4 defers the de-join — but it
  reasons only about seek count, and does not address the document-volume term that
  #197 itself led with, nor the fact that this path runs on far hotter triggers than
  the list path.

## 10. Documentation

No `docs/client-api.md` obligation: no client-facing handler, request/response
struct or event struct changes. The `SubscriptionRepository` doc comment on
`GetActiveSubscriptions` is updated to name the new return type.

## 11. Follow-ups (not in this change)

- Removing the `encKey.priv` read from the badge path, which needs a
  caller-specific `$lookup` rather than the shared stage.
- The de-join, and/or reusing #197's `sortKeyCache` to serve `lastMsgAt` — the
  latter carries a staleness asymmetry the list path does not have: a stale-old
  `lastMsgAt` here is a false negative (a room that just received a message reads as
  read) bounded by the cache TTL, tolerable only if the Valkey bump is the freshness
  path, i.e. with `BADGE_COUNT_CACHE_FIRST=true`.
- Lever "A3" from the 2026-08-19 spec: per-account singleflight and a recompute
  cooldown for reconnect storms and deploys.
