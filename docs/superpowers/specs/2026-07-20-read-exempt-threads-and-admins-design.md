# Exclude bots & admins from read-floor and receipts, incl. threads — design

**Date:** 2026-07-20
**Branch:** `claude/bot-user-distinction-ln46az`
**Status:** Approved (revised per PR #88 review)
**Author:** co-authored with Claude

> **Revision (per review):** an earlier draft added a persisted `readExempt`
> flag (bot OR p_ OR admin-role). Review feedback (mliu33) was that no stored
> field is needed and it complicates the data migration: `p_` accounts already
> represent both platform and admin accounts, so the exclusion can be done
> **directly by account** in the read queries. This document reflects that
> simpler approach.

## 1. Background

PR #78 excluded bots from the **main-room** read-floor (`rooms.minUserLastSeenAt`)
and read receipts by filtering `u.isBot != true`. Two gaps remained:

1. **Threads.** `MinThreadSubscriptionLastSeenByThreadRoomID` and
   `ListThreadReadReceipts` still counted bots. A bot that authors a message a
   human threads a reply to becomes the thread's parent-author subscriber
   (`buildThreadSubscription`, `LastSeenAt: nil`) and pins the thread floor at
   nil forever. (Raised by mliu33 on PR #78.)
2. **Admins.** Admin accounts use the `p_` prefix
   (`IsPlatformAdminAccount`); their user doc carries `roles:[user]`. They were
   not excluded from read state.

## 2. Approach

Exclude non-human participants **by account, in the read queries** — no stored
flag, no stamping, no migration:

- **Bots** — `.bot` suffix.
- **Platform/admin accounts** — `p_` prefix (covers both platform and admin).

Two query-side regex constants in `room-service/store_mongo.go` (the query
equivalents of `model.IsBot` / `model.IsPlatformAdminAccount`):

```go
const (
    pseudoAccountRegex      = `^p_`        // p_ platform/admin accounts
    botOrPseudoAccountRegex = `(\.bot$|^p_)` // bots + p_ accounts
)
```

## 3. Query changes (`room-service/store_mongo.go`)

Main-room subscriptions already carry `u.isBot` (stamped at creation for
`.bot`/`p_`), so those queries keep `u.isBot $ne true` and add a `p_`-account
guard for subs whose `isBot` may be unset (e.g. cross-site mirror subs, which are
not stamped). Thread subscriptions carry no flag, so they filter purely by
account.

- `MinSubscriptionLastSeenByRoomID` / `ListReadReceipts`:
  `u.isBot $ne true` **and** `u.account $not /^p_/`.
- `MinThreadSubscriptionLastSeenByThreadRoomID` / `ListThreadReadReceipts`:
  `userAccount $not /(\.bot$|^p_)/`.

The read-receipt `$match` combines the `p_` guard with the existing
`$ne excludeAccount` on the same account field. Indexing is unchanged: the
`(roomId, lastSeenAt)` / `(threadRoomId, lastSeenAt)` indexes serve the sort; the
account/isBot predicates are residual filters (few such subs per room).

## 4. What this removes

No changes to `pkg/model` (no new field), `room-worker`, `inbox-worker`, or
`message-worker` — subscription/thread-subscription creation is untouched, and
the cross-site mirror is unchanged (the room's origin site already holds every
subscription, including remote members, so the account filter covers them at
query time). No data migration.

## 5. Testing (TDD)

Extend the four `room-service` integration tests: a `p_` account (isBot=false)
excluded from the main-room floor and receipts; a `.bot` account excluded from
the thread floor and receipts; ordinary members still counted. Existing PR #78
bot cases continue to pass via the retained `u.isBot` predicate. Coverage stays
at/above the 80% floor.

## 6. Rollout / risk

- **Backward compatible.** Pure query change; no schema, no field, no migration.
- **Behavior:** rooms/threads with a bot or `p_` member stop being frozen by that
  member; floors recompute on the next read.

## Addendum (2026-07-23) — `p_` taxonomy split

This note's §2 exclusion ("Platform/admin accounts — `p_` prefix (covers both
platform and admin)") predates the `p_` taxonomy split landed on branch
`claude/bot-add-remove-cv9pa0` (commit "refactor(model): split p_ taxonomy").
Under the split, the blanket `^p_` exclusion is **narrowed to the platform-admin
pseudo-account only**:

- **Excluded from read floors & receipts** — the platform-admin pseudo-account,
  identified by the configurable prefix `model.PlatformAdminAccountPrefix()`
  (env `ADMIN_ACCT_PREFIX`, default `p_tchatadmin_`). Bot-like; `IsBot` / admin
  prefix.
- **Counted like ordinary users** — every other `p_…` account (QA test users,
  e.g. `p_qa1`, `p_webhook`). Their unread state **holds** the read floor and
  they **surface** as read-receipt readers, exactly like a human member.

The two hardcoded regex constants from §2 (`pseudoAccountRegex = "^p_"`,
`botOrPseudoAccountRegex = "(\.bot$|^p_)"`) are therefore replaced in
`room-service/store_mongo.go` by the derived, `regexp.QuoteMeta`-escaped forms
`platformAdminRegex()` / `botOrPlatformAdminRegex()` (helper.go), which track the
configured prefix — the query-side mirrors of `model.IsPlatformAdminAccount` /
`model.IsBot`. The four query sites and the integration tests are updated to the
narrowed taxonomy; the queries are otherwise unchanged (same indexes, same
`u.isBot` predicate on the main-room paths).
