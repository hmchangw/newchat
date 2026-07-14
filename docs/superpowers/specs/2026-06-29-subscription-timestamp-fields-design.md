# Subscription Timestamp Fields — Design

**Date:** 2026-06-29
**Status:** Approved
**Branch:** `claude/subscription-timestamp-fields-jcro8y`

## Problem

The `Subscription` model carries a single coarse `UpdatedAt` timestamp
(`_updatedAt` in Mongo, `updatedAt` on the wire). Meanwhile the codebase
already maintains a set of *per-attribute* order-safety guard timestamps in
the Mongo `subscriptions` collection — `rolesUpdatedAt`, `muteUpdatedAt`,
`favoriteUpdatedAt`, `nameUpdatedAt`, `visibilityUpdatedAt` — written by
room-service / room-worker / inbox-worker to make federated `$set` writes
order-independent (see
`docs/superpowers/specs/2026-06-11-inbox-worker-throughput-and-ordering-design.md`).
These guard fields are **not** represented on the `Subscription` struct, so
clients cannot see when each attribute last changed, and the model exposes
only the catch-all `updatedAt` instead.

This change promotes four of those guard fields onto the model (exposing them
to clients), renames one for clarity, and drops the catch-all `updatedAt`
from the model.

## Decisions

1. **`_updatedAt` stays on the Mongo document; nothing in code reads it.**
   Only the `UpdatedAt` Go field and its `updatedAt` wire serialization are
   removed. As of #369 the `updatedWithinDays` activity window keys on
   `room.lastMsgAt`, **not** `_updatedAt`, so removing the model field has
   zero filter impact. The dead `_updatedAt` inclusion in
   `subscriptionProjection` is also removed. `_updatedAt` remains on existing
   documents (legacy migration data) but is unreferenced by any Go code.
2. **New fields are exposed on the wire**, each `omitempty` on both `json`
   and `bson` (mirroring `favoriteUpdatedAt`).
3. **Rename `visibilityUpdatedAt` → `restrictUpdatedAt`** for the persisted
   bson field name and the Go parameter/variable names. The store method was
   initially kept as `ApplySubscriptionVisibility`, but per PR #453 review
   feedback (mliu33) it is **also renamed to `ApplySubscriptionRestriction`**
   in both room-service and inbox-worker.
4. **`favoriteUpdatedAt` is left out of scope — already done.** As of #423
   the model already carries `FavoriteUpdatedAt` (`bson:"favoriteUpdatedAt"`),
   which replaced the never-written `favoritedAt`. No further work.

## Rebase note (2026-06-29)

Re-scoped against `origin/main` after significant intervening development:
- `FavoritedAt` → `FavoriteUpdatedAt` already landed (#423).
- Read model split into `Subscription` (persisted) + `EnrichedSubscription`
  (read-time baseline); new fields go on `Subscription`.
- Activity window repointed to `room.lastMsgAt` (#369) — `_updatedAt` is now
  dead in code.
- `nameUpdatedAt` is stamped by **room-worker** (`UpdateSubscriptionNamesForRoom`),
  not room-service. No write-path change is needed here regardless.
- client-api docs split into `docs/client-api/{request-reply,events}.md`, but
  the subscription field table still lives in `docs/client-api.md`.

## The model change (`pkg/model/subscription.go`)

Remove:

```go
UpdatedAt *time.Time `json:"updatedAt,omitempty" bson:"_updatedAt,omitempty"`
```

Add (alongside the existing `FavoriteUpdatedAt`):

```go
MuteUpdatedAt     *time.Time `json:"muteUpdatedAt,omitempty"     bson:"muteUpdatedAt,omitempty"`
RolesUpdatedAt    *time.Time `json:"rolesUpdatedAt,omitempty"    bson:"rolesUpdatedAt,omitempty"`
NameUpdatedAt     *time.Time `json:"nameUpdatedAt,omitempty"     bson:"nameUpdatedAt,omitempty"`
RestrictUpdatedAt *time.Time `json:"restrictUpdatedAt,omitempty" bson:"restrictUpdatedAt,omitempty"`
```

The bson tags match the existing persisted guard-field names (already written
by the write path), except `restrictUpdatedAt` which is the renamed
`visibilityUpdatedAt`. No backfill: omitempty + lazy seeding means existing
documents simply omit the fields until the next guarded write.

## Rename: `visibilityUpdatedAt` → `restrictUpdatedAt`

Persisted bson field name and Go param/var names change; method names stay.
Touch points (from grep):

- `room-service/store.go` — `ApplySubscriptionVisibility` param doc + signature param name.
- `room-service/store_mongo.go:1602` — param name + the two `$set`/guard writes (`"visibilityUpdatedAt"` → `"restrictUpdatedAt"`).
- `room-service/handler.go` — any local var / comment referencing `visibilityUpdatedAt`.
- `inbox-worker/handler.go:57,61` — store interface method param + comment.
- `inbox-worker/main.go` — `mongoInboxStore.ApplySubscriptionVisibility` param + `$exists`/`$lt` guard keys + `$set` write.
- Mocks: `room-service/mock_store_test.go`, `inbox-worker/mock_store_test.go` (regenerated via `make generate`, not hand-edited).
- Tests: `room-service/integration_test.go`, `room-service/handler_test.go`, `inbox-worker/integration_test.go:1294-1315`, `inbox-worker/handler_test.go:314-318` — Mongo seed maps and assertions keyed on `"visibilityUpdatedAt"`.

Note: `nameUpdatedAt`/`muteUpdatedAt`/`rolesUpdatedAt` are **not** renamed — their
write paths (room-worker `UpdateSubscriptionNamesForRoom`, room-service
`ToggleSubscriptionMute`/`SetOwnerRole`, and the inbox-worker mirrors) are
untouched. Only their model representation + projection + docs are added.

The bson key rename is a **wire/storage rename of a guard field**: because
the guard treats `$exists:false` as "older than any event," any pre-existing
document carrying the old `visibilityUpdatedAt` key will be treated as
un-stamped under the new `restrictUpdatedAt` key and accept the first new
write. This matches the original lazy-seeding contract — acceptable, noted
here for reviewers.

## user-service projection (`user-service/mongorepo/subscriptions.go`)

- `subscriptionProjection` (~line 164): add `muteUpdatedAt`, `rolesUpdatedAt`,
  `nameUpdatedAt`, `restrictUpdatedAt` so they decode into the struct
  (`favoriteUpdatedAt` is already projected); **remove** the now-dead
  `_updatedAt: 1` line (nothing decodes it and no `$match` uses it).
- The aggregate list path (`roomsEnrichStages`) projects away only `room`, so
  it carries the new sub fields through automatically.
- The `updatedWithinDays` `$match` already keys on `lastMsgAt` (~line 251) —
  **unchanged**.

## Documentation (`docs/client-api.md`)

The subscription field table still lives in the monolithic `docs/client-api.md`
(the `docs/client-api/` split covers messages/reactions/events only).

- Subscription field table (~line 735): remove the `updatedAt` row, add rows
  for `muteUpdatedAt`, `rolesUpdatedAt`, `nameUpdatedAt`, `restrictUpdatedAt`
  (all "Optional. RFC3339 timestamp. When the subscription's
  {mute,roles,name,restrict} state last changed."). The `favoriteUpdatedAt`
  row (~734) already exists.
- Shared-field prose (~line 4113): drop `updatedAt` from the common-field list.
- subscription.list JSON examples (~lines 4143, 4172, 4195, 4272, 4345, 4410):
  remove the `updatedAt` entries inside subscription rows (verify each is a
  subscription row, not a nested message).
- The `updatedWithinDays` filter description (~line 4084) already reads
  correctly (keys on `room.lastMsgAt`) — **no change**.

## Tests (TDD: red → green → refactor)

- `pkg/model/model_test.go:4065` — `TestSubscriptionBaseMetadata_RoundTrip`:
  drop `UpdatedAt`, add the four new fields to `src`; update the omitempty
  absence-assertion list from `{favoriteUpdatedAt, updatedAt}` to
  `{favoriteUpdatedAt, muteUpdatedAt, rolesUpdatedAt, nameUpdatedAt,
  restrictUpdatedAt}` (drop `updatedAt`).
- `user-service/mongorepo/subscriptions_test.go` — add coverage asserting the
  new timestamp fields decode onto returned subscriptions. (The window test
  already keys on `room.lastMsgAt`; no `_updatedAt` seed dependency remains.)
- room-service / inbox-worker existing tests: rename the `visibilityUpdatedAt`
  guard-field key in seeds/assertions to `restrictUpdatedAt`; confirm guard
  behavior (`$exists`/`$lt`) unchanged.

## Out of scope

- `favoriteUpdatedAt` promotion — already modeled (#423).
- Historical spec docs that mention `visibilityUpdatedAt` (records, not code).
- Any change to how/when the guard fields are stamped — only their model
  representation, wire exposure, and the one rename change.

## Verification

`make generate` (mocks change) → `make lint` → `make test` → `make sast`.
client-api.md updated in the same PR per CLAUDE.md.
