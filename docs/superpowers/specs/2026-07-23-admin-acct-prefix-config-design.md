# Configurable admin-account prefix for read-floor & receipts — design

**Date:** 2026-07-23
**Branch:** `claude/read-receipt-filter-config-6jc5yl`
**Status:** Draft (pending author review)
**Author:** co-authored with Claude

> **Assumptions made while drafting** (clarifying questions were declined; flag
> any you disagree with on review):
> 1. **Scope is read-state only.** Only the four read-floor / read-receipt
>    queries in `room-service` switch to the config value. The other `p_` users
>    (`helper.go`'s `platformAdminRegex` for mention autocomplete,
>    `model.IsPlatformAdminAccount` for DM/botDM classification) are **left on
>    `p_`** — they are separate concerns (see §6).
> 2. **Narrowing is intended.** The default changes the excluded set from *any*
>    `p_` account to only `p_chatadmin_` accounts. Non-admin `p_*` accounts
>    (e.g. `p_admin`, platform/webhook accounts) that are **not** flagged
>    `isBot` will start being **counted** in read floors and receipts again.
>    This is the point of the change; the risk note is in §8.
> 3. **Empty prefix disables the admin filter** rather than failing fast or
>    matching everything (see §5.3).

## 1. Background

PR #78 excluded bots from the main-room read-floor (`rooms.minUserLastSeenAt`)
and read receipts (`u.isBot != true`). PR #88 extended the exclusion to
**threads** and to **admin/platform accounts** by adding a query-side `p_`
prefix guard. The exclusion currently hardcodes the prefix as two package-level
regex constants in `room-service/store_mongo.go`:

```go
const (
    pseudoAccountRegex      = `^p_`          // p_ platform/admin accounts
    botOrPseudoAccountRegex = `(\.bot$|^p_)` // bots + p_ accounts
)
```

The prefix `p_` is broad: it matches every platform and webhook account, not
just admins. We want the admin exclusion to be operator-configurable and, by
default, scoped to true admin accounts (`p_chatadmin_`).

## 2. Goal

Replace the hardcoded `p_` prefix used by the read-floor / read-receipt queries
with a config variable:

| Env var | Default | Applies to |
|---------|---------|-----------|
| `ADMIN_ACCT_PREFIX` | `p_chatadmin_` | `room-service` read-floor & read-receipt queries only |

The bot filter (`.bot` suffix / `u.isBot`) is **unchanged** — only the `p_`
half becomes configurable.

## 3. Affected queries (`room-service/store_mongo.go`)

All four are the query-side non-human exclusion; only the admin-prefix portion
changes.

| Method | Bot filter (unchanged) | Admin filter (was `^p_`) |
|--------|------------------------|--------------------------|
| `MinSubscriptionLastSeenByRoomID` (room floor) | `u.isBot $ne true` | `u.account $not /^<prefix>/` |
| `ListReadReceipts` (room receipts) | `u.isBot $ne true` | `u.account $not /^<prefix>/` |
| `MinThreadSubscriptionLastSeenByThreadRoomID` (thread floor) | via combined regex | `userAccount $not /(\.bot$\|^<prefix>)/` |
| `ListThreadReadReceipts` (thread receipts) | via combined regex | `userAccount $not /(\.bot$\|^<prefix>)/` |

Main-room queries detect bots via the stored `u.isBot` flag and the admin prefix
via a separate `u.account` predicate. Thread queries carry no `isBot` flag, so
they detect both bot and admin via one combined regex.

## 4. Configuration & wiring

### 4.1 Config (`room-service/main.go`)

Add to the `config` struct, parsed by `caarlos0/env` like every other field:

```go
AdminAcctPrefix string `env:"ADMIN_ACCT_PREFIX" envDefault:"p_chatadmin_"`
```

### 4.2 Store construction

`NewMongoStore` gains a **functional option** rather than a positional arg,
because it has ~130 zero-arg call sites in `room-service/integration_test.go`; a
required positional arg would force noisy edits to all of them. The variadic
option keeps every existing call compiling (they get the default) and matches
the repo's options idiom (`pkg/roommetacache`, `pkg/mongoutil`). The two regex
strings are precomputed once at construction (the prefix is fixed for the
process lifetime — no per-query cost):

```go
type Option func(*MongoStore)

func WithAdminAcctPrefix(prefix string) Option {
    return func(s *MongoStore) {
        s.adminAcctRegex, s.botOrAdminRegex = adminAccountPatterns(prefix)
    }
}

func NewMongoStore(db *mongo.Database, opts ...Option) *MongoStore {
    adminRegex, botOrAdminRegex := adminAccountPatterns(defaultAdminAcctPrefix)
    s := &MongoStore{
        // ...existing collections...
        adminAcctRegex:  adminRegex,      // e.g. "^p_chatadmin_"; "" => no admin exclusion
        botOrAdminRegex: botOrAdminRegex, // e.g. "(\.bot$|^p_chatadmin_)"
    }
    for _, opt := range opts {
        opt(s)
    }
    return s
}
```

Default prefix is `p_chatadmin_` (`defaultAdminAcctPrefix`); `main.go` overrides
via `store := NewMongoStore(db, WithAdminAcctPrefix(cfg.AdminAcctPrefix))`.

### 4.3 Regex builder (pure, unit-tested)

The `pseudoAccountRegex` / `botOrPseudoAccountRegex` constants are **removed** and
replaced by a pure helper. It reuses the existing `botAccountRegex` (`\.bot$`)
constant and **regex-escapes** the operator-supplied prefix (`p_chatadmin_` is
already regex-literal, but a config value must never be trusted to be — an
unescaped `.` or `(` would silently change matching):

```go
// adminAccountPatterns builds the query-side account exclusion regexes from a
// configured admin-account prefix. An empty prefix disables the admin-account
// exclusion (adminRegex==""), leaving only the bot filter in effect.
func adminAccountPatterns(prefix string) (adminRegex, botOrAdminRegex string) {
    if prefix == "" {
        return "", botAccountRegex // `\.bot$` — bots only
    }
    q := regexp.QuoteMeta(prefix)
    return "^" + q, `(\.bot$|^` + q + `)`
}
```

## 5. Query construction detail

### 5.1 Main-room (`MinSubscriptionLastSeenByRoomID`, `ListReadReceipts`)

The `u.isBot $ne true` predicate always stays. The admin-prefix predicate is
added **only when `adminAcctRegex != ""`**:

```go
match := bson.M{"roomId": roomID, "u.isBot": bson.M{"$ne": true}}
if s.adminAcctRegex != "" {
    match["u.account"] = bson.M{"$not": bson.Regex{Pattern: s.adminAcctRegex}}
}
```

For `ListReadReceipts`, the admin predicate shares the `u.account` field with the
existing `$ne excludeAccount`, so when the prefix is set:
`"u.account": bson.M{"$ne": excludeAccount, "$not": bson.Regex{Pattern: s.adminAcctRegex}}`,
and when empty: `"u.account": bson.M{"$ne": excludeAccount}`.

### 5.2 Thread (`MinThreadSubscriptionLastSeenByThreadRoomID`, `ListThreadReadReceipts`)

Use the combined `s.botOrAdminRegex` directly. Because the bot half is always
present, `botOrAdminRegex` is never empty, so no conditional is needed:
`"userAccount": bson.M{"$not": bson.Regex{Pattern: s.botOrAdminRegex}}` (plus
`$ne excludeAccount` in the receipt list).

### 5.3 Empty-prefix semantics

If an operator sets `ADMIN_ACCT_PREFIX=` (empty), the admin exclusion is
**disabled** — the bot filter still applies. This deliberately avoids the failure
mode where an empty prefix would produce the anchored regex `^`, which matches
*every* account and would wrongly exclude the whole room from its own read floor.
We prefer "disable the feature" over "fail startup" because an empty prefix is a
coherent operator intent (no admin accounts to exclude), not a malformed value.

## 6. Out of scope (intentional divergence)

These keep the literal `p_` prefix and are **not** touched:

- `room-service/helper.go` `platformAdminRegex = ^p_` — mention-autocomplete
  hides `p_` accounts (`subscription.mentionable`).
- `pkg/model` `IsPlatformAdminAccount` (`^p_`) — DM/botDM classification,
  channel-membership guards, `filterBots`.

Rationale: the request is specifically about read-state (`minLastSeenAt` /
read receipts). Admin *mentionability* and *DM classification* are unrelated
policies; folding them into the same knob would change DM and mention behavior
and widen the blast radius across `pkg/model` and several services. If a single
unified admin marker is desired later, that is a separate, larger change. The
divergence (read-state on `p_chatadmin_`, other concerns on `p_`) is documented
here so it is not mistaken for drift.

## 7. Testing (TDD)

Red → green → refactor, per project rules; keep coverage at/above the 80% floor.

- **Unit — `adminAccountPatterns` (new, `store_mongo_test.go` or a focused
  helper test):** table-driven over
  - `p_chatadmin_` → `("^p_chatadmin_", "(\.bot$|^p_chatadmin_)")`
  - `""` → `("", "\.bot$")` (admin exclusion disabled)
  - a prefix with regex metacharacters (e.g. `p.admin(`) → escaped via
    `QuoteMeta`, verified to match literally and not as a regex.
- **Integration — extend the four `room-service` tests** (the ones PR #78/#88
  already touch) using the default prefix:
  - `p_chatadmin_alice` (isBot=false) → **excluded** from room floor & receipts
    and thread floor & receipts.
  - `p_somewebhook` (isBot=false, does **not** match `p_chatadmin_`) → now
    **counted** (the narrowing — the regression-relevant case; asserts the new
    default really is narrower than `p_`).
  - `helper.bot` → still excluded (bot filter unchanged).
  - Ordinary human members → still counted.
  - (Optional) a case constructing the store with a **custom** prefix and one
    with the **empty** prefix, asserting the admin filter is honored / disabled
    respectively.
- Update any existing `NewMongoStore(db)` call sites in tests to pass a prefix.

## 8. Rollout / risk

- **No schema, no migration, no wire change.** Pure config + query change.
  Request/response payloads, error cases, and emitted events are unchanged, so
  no `docs/client-api.md` (or its derived views) update is required — the read
  receipt / floor RPC contracts are untouched. Client-api examples elsewhere use
  `p_admin` illustratively; reconciling that prose is optional and left to the
  author.
- **Self-correcting.** Floors are recomputed on every `message.read` /
  `thread.read`, so any room/thread whose stored `minUserLastSeenAt` was
  computed under the old `p_` rule converges to the new value on the next read —
  no backfill needed.
- **Narrowing regression risk (assumption #2).** If non-admin `p_*` accounts
  that are **not** `isBot` exist in production (e.g. `p_admin`, platform/webhook
  accounts), they will re-enter read floors and receipts. If that is undesirable,
  the mitigation is to keep `ADMIN_ACCT_PREFIX=p_` in that deployment (the config
  makes this a per-environment choice) or to ensure such accounts are `isBot`.
- **Local dev.** Add `ADMIN_ACCT_PREFIX` to `room-service/deploy/docker-compose.yml`
  (optional, it has a default) so the value is visible to operators; no CI
  pipeline change needed.

## 9. Open questions

1. Confirm assumption #1 (read-state only) vs. unifying all `p_` admin checks.
2. Confirm assumption #2 (narrowing to `p_chatadmin_` is intended, including the
   re-inclusion of non-admin `p_*` non-bot accounts).
3. Confirm assumption #3 (empty prefix disables the admin filter).
