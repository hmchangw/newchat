# One-time backfill: `users.deactivated` → `users.active`

**Required alongside the deploy of the active-field migration** (the PR that
replaced `model.User.Deactivated` with `Active *bool`). Run once per site,
against each site's operational MongoDB.

## Why

Before the migration, deactivations performed through admin-service wrote
`deactivated: true`. After it, every reader (admin-service, botplatform-service,
media-service, user-service) looks only at `active`, where a missing field means
**active**. Without this backfill, any account disabled via the old admin path
is silently reactivated.

## Commands (mongosh, per site database)

```js
// Preserve old deactivations under the new field, drop the old one.
db.users.updateMany(
  { deactivated: true },
  { $set: { active: false }, $unset: { deactivated: "" } }
)

// Clean up any remaining deactivated:false leftovers.
db.users.updateMany(
  { deactivated: { $exists: true } },
  { $unset: { deactivated: "" } }
)
```

Both commands are idempotent — safe to re-run. Order matters only in that the
first must run before the second (the second would otherwise discard
`deactivated: true` markers before they are converted).

## Verification

```js
db.users.countDocuments({ deactivated: { $exists: true } }) // must be 0
db.users.countDocuments({ active: false })                  // == previously deactivated accounts
```

## Timing

Running it **before** the deploy is also safe: pre-migration user-service
already filters on `active: {$ne: false}`, and the old admin-service only ever
read `deactivated` for accounts it had itself deactivated. Run it after the
deploy only if no admin deactivations happen in the gap.
