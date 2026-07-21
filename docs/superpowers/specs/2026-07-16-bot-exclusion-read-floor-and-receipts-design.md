# Exclude bots from the main-room read-floor & read receipts — design

**Date:** 2026-07-16
**Branch:** `claude/bot-user-distinction-ln46az`
**Status:** Approved (pending spec review)
**Author:** co-authored with Claude

## 1. Background

The platform models bots as ordinary users. A distinction is already made — and
is load-bearing — in two places:

- **Member counts:** bot subscriptions increment `rooms.appCount`, never
  `rooms.userCount` (`room-worker/store_mongo.go` `ReconcileMemberCounts` /
  `ApplyMemberCountDelta`, keyed off the persisted `subscription.u.isBot` flag).
- **Push notifications:** `EligibleForPush` short-circuits on `m.IsBot`
  (`notification-worker/routing.go`).

That distinction was **not** extended to read state. Two subscription queries in
`room-service` treat bots as ordinary members:

| # | Site | Function (`room-service/store_mongo.go`) |
|---|------|------------------------------------------|
| A | Room read-floor | `MinSubscriptionLastSeenByRoomID` (:1161) |
| B | Room read receipts | `ListReadReceipts` (:1212) |

**The read-floor.** `rooms.minUserLastSeenAt` is the room-wide "everyone has read
up to here" watermark — the minimum `lastSeenAt` across the room's
subscriptions, but only when **every** subscription has a real (post-zero)
`lastSeenAt`; if any member has never read, the floor is `nil` ("not everyone is
caught up"). `MinSubscriptionLastSeenByRoomID` computes it with a single index
seek (first sub by ascending `lastSeenAt`); the existing docstring states
outright: *"Bots are ordinary subscriptions and are counted: a botDM room (the
bot never reads) therefore always resolves to nil."*

**The bug.** A bot is a subscription with a `lastSeenAt`. A passive bot never
issues `message.read`, so its `lastSeenAt` stays at the BSON zero date forever.
Under the strict rule, one never-reading member forces the floor to `nil` — so
**any room containing a passive bot reports "nobody is fully caught up"
permanently**, even when every human has read everything. The floor never
advances. (A bot that actively posts self-advances its own `lastSeenAt` via
`broadcast-worker`'s `AdvanceSubscriptionLastSeen`, so the freeze is worst for
listener bots, and total for botDMs.)

**Read receipts.** `ListReadReceipts` filters only `roomId`, `lastSeenAt >=
since`, and `u.account != sender`. There is no bot filter, so a bot that
advanced its `lastSeenAt` (by posting) could surface as a "reader" of a message.

## 2. Scope

**In scope:** exclude bots from sites A and B in `room-service`, using the
`subscription.u.isBot` flag that is already persisted and indexed.

**Out of scope (explicitly not touched):**

- **Threads (C/D).** `MinThreadSubscriptionLastSeenByThreadRoomID` and
  `ListThreadReadReceipts` have the same gap, but `thread_subscriptions` stores
  `userAccount`/`userId` flat with **no `isBot` field**. Covering threads
  requires a schema addition + write-path stamping + a backfill migration, and
  is deferred to its own spec.
- Member-count and notification paths — already correct.
- Any new client API surface: no `chat.user.*` handler registration,
  request/response schema, error case, or event struct changes (see §6).

## 3. The change

Single predicate added to both queries:

```
"u.isBot": bson.M{"$ne": true}
```

`$ne: true` (not `isBot: false`) is deliberate: legacy subscription docs written
before the `u.isBot` flag existed omit the field, and must count as **humans**.
`$ne: true` matches missing/false/null; `isBot: false` would wrongly drop
flagless legacy humans. This mirrors how `ReconcileMemberCounts` already derives
the human count by subtraction rather than an equality match.

### 3.1 Component A — `MinSubscriptionLastSeenByRoomID`

Filter changes from `{roomId}` to `{roomId, "u.isBot": {"$ne": true}}`. The
existing single-seek logic is otherwise unchanged: sort `lastSeenAt` ascending,
inspect the first (now first *non-bot*) document.

- **botDM behavior change (intended).** A botDM (1 human + 1 bot) previously
  always resolved to `nil`; it now resolves to the human's `lastSeenAt`. This is
  the correct direction and the whole point of the change.
- **Bot-only room.** No non-bot subs → `ErrNoDocuments` → `nil` (harmless,
  correct — there is no human read position to report).
- The docstring's "Bots are ordinary subscriptions and are counted" paragraph is
  corrected to describe the exclusion.

### 3.2 Component B — `ListReadReceipts`

`"u.isBot": {"$ne": true}` is added to the `$match` stage. A bot never appears in
a read-receipt list even if it self-advanced its `lastSeenAt`.

### 3.3 Propagation (no wiring changes)

The floor's only write path is the `messageRead` handler
(`room-service/handler.go:1262`): `UpdateSubscriptionRead` →
`MinSubscriptionLastSeenByRoomID` (:1352) → `UpdateRoomMinUserLastSeenAt`
(:1362). Fixing the store method flows straight into `rooms.minUserLastSeenAt`
at the single choke point. The user-service subscription-list `$lookup` reads
that stored field and inherits the fix for free. No handler, model, or
cross-service change is required.

## 4. Indexing

Keep the existing `{roomId, lastSeenAt}` index; apply `u.isBot $ne true` as a
residual filter. Rationale:

- A `$ne` predicate does not produce tight index bounds, so a compound
  `{roomId, u.isBot, lastSeenAt}` would force a `SORT_MERGE` of the two isBot
  ranges rather than the clean single seek we have today — no win.
- Rooms carry very few bots. Never-read bots (zero `lastSeenAt`) sort first, so
  the seek may fetch those leading bot docs before reaching the first human —
  bounded by the room's bot count (typically 0–few), negligible on the
  message-read hot path.

Honest tradeoff, documented in the code: adding the residual filter means the
query is no longer strictly index-covered (the planner must fetch candidate docs
to evaluate `u.isBot`). Accepted; no new index is introduced.

## 5. Testing (TDD: Red → Green → Refactor)

All coverage is integration-level (these are Mongo queries); the `messageRead`
handler unit tests mock the store and are unaffected by the predicate.

**A — extend `TestMongoStore_MinSubscriptionLastSeenByRoomID_Integration`
(`integration_test.go:2003`):**

- **RED (new):** humans all-read + a passive bot (`isBot:true`, zero
  `lastSeenAt`) → floor = human minimum, **not** `nil`. (Fails pre-change.)
- **Update `"botdm"` case (:2081):** was `nil`; now the human's `lastSeenAt`.
- Bot with a `lastSeenAt` **later** than all humans → floor still = human
  minimum (bot excluded from the min).
- Bot-only room → `nil`.
- Legacy human sub missing `u.isBot` still counts (guards the `$ne: true`
  choice).

**B — extend `TestMongoStore_ListReadReceipts_Integration`:**

- A bot whose `lastSeenAt >= message time` is **excluded** from the receipt
  list; humans past the message remain included.

Coverage stays at or above the 80% floor for `room-service`; both changed
methods keep meaningful error/edge coverage.

## 6. Docs

- **`docs/client-api.md`:** no schema, error, or event change — the
  `minUserLastSeenAt` field and the read-receipt response shape are identical.
  Only the computed semantics change. If the field's prose description warrants
  a one-line "bots excluded" clarification it will be added, but no structural
  edit is required, and no derived-view (`request-reply.md` / `events.md`) update
  is triggered.
- **Code docstrings:** update `MinSubscriptionLastSeenByRoomID` and
  `ListReadReceipts` to state that bots are excluded (the former's current text
  says the opposite and must be corrected).

## 7. Rollout / risk

- **Backward compatible.** No schema or wire change; the `u.isBot` flag already
  exists in production and is stamped at sub-creation.
- **Behavior shift:** rooms with a passive bot begin advancing
  `minUserLastSeenAt` (previously pinned at `nil`), and botDM floors start
  tracking the human. This is the intended fix; clients already handle both a
  present and an absent floor.
- **Self-correcting:** the floor is recomputed on every `message.read`, so the
  corrected value lands on the next read in each affected room with no migration
  or backfill.
