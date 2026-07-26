# Active-Field Migration Design

**Date:** 2026-07-26
**Status:** Approved (autonomous session — decisions recorded below)

## Problem

`pkg/model.User` carries `Deactivated bool` (`bson:"deactivated"`), but the rest of the
system has already converged on the opposite-polarity `active` field:

- `user-service` filters users with `active: {$ne: false}` (missing = active) and its
  integration tests seed `active: true/false` docs.
- The company-wide user sync and the data-migration transformer treat deactivation as
  `active: false` (`data-migration/oplog-collections-transformer/users.go`).
- Client-facing derived views (`botplatform` login `me.active`, media-service drive
  members `active`) already speak `active`.

This split is a live inconsistency: a deactivation performed through admin-service
writes `deactivated: true`, which user-service ignores entirely — the user keeps
resolving as active there. Unifying on `active` is therefore a correctness fix, not
just a rename.

## Requirements

1. New field `Active *bool` on `model.User`; a missing/nil field means **active**.
2. The field is not serialized to the frontend (`json:"-"` on the model).
3. All code and living docs replace `deactivated` with `active`; no dual-field state.

## Approaches Considered

- **A. Clean cut (chosen):** switch storage, model, admin wire contract, and
  admin-frontend to `active` in one PR. The monorepo owns every reader and writer, and
  user-service already reads only `active`, so there is nothing to stay compatible with.
- **B. Storage-only:** store `active` but keep the admin API field named `deactivated`.
  Rejected — leaves `deactivated` in code and docs, contrary to the unification goal.
- **C. Dual-write window:** write both fields for one release. Rejected — buys nothing
  (no external consumer of `deactivated`) and adds cleanup debt.

## Design

### pkg/model

```go
Active *bool `json:"-" bson:"active,omitempty"`
```

- Replaces `Deactivated bool`. `json:"-"` keeps it out of every client payload;
  `bson` `omitempty` drops the nil pointer so "never set" stays absent in Mongo.
- New nil-safe helper, the single source of truth for the missing-means-active rule:

```go
// IsActive reports whether the user is active. A nil user is not active; a
// nil (missing) field or explicit true counts as active — only a stored
// active:false deactivates.
func (u *User) IsActive() bool
```

All former `u.Deactivated` readers become `!u.IsActive()`; derived wire fields
(`me.active`, drive-member `active`) become `u.IsActive()`.

### Storage (Mongo)

- admin-service `DeactivateAndRevoke` sets `active: false` (method name keeps the verb —
  the action is still deactivation).
- admin-service `UpdateUser` / `UserUpdate` carries `Active *bool`; reactivation writes
  `active: true` (an explicit true, equivalent to absent per the read rule).
- Projections that listed `deactivated: 1` (admin-service `userProjection`,
  `userAuthProjection`; botplatform `FindUserByAccount`; media-service `UserByAccount`)
  list `active: 1`.
- user-service is already correct; only comments change.

### Admin API wire contract (admin-service + admin-frontend)

- `UserView.deactivated` (omitted-when-false) → `active` (always present, from
  `IsActive()`).
- `PATCH /v1/admin/users/:account` body field `deactivated *bool` → `active *bool`.
  `active: false` is the deactivate branch (transactional flag-flip + session revoke;
  cannot be mixed with other fields — reason code `mixed_deactivate_patch` is
  unchanged); `active: true` reactivates via the plain update branch and may be
  combined with other fields.
- Audit details key `deactivated` → `active` (value is the boolean sent).
- admin-frontend: `UserView.active: boolean` (adapter default `raw.active ?? true`),
  `UpdateUserPatch.active?: boolean`, table badge and edit-dialog checkbox derive from
  `active` (UI copy may still say "Deactivated" — that is prose, not a field), CSS
  class `is-deactivated` → `is-inactive`.

### Login gates

botplatform-service and admin-service login handlers deny when `!u.IsActive()`, at the
same post-password-verify position (timing posture unchanged).

### Docs

`docs/client-api.md` §9 (UserView table, 9.1–9.4 examples and field tables) moves to
`active`. Derived views (`docs/client-api/request-reply.md`, `events.md`) contain no
`deactivated` field references — no change. Historical specs/plans under
`docs/superpowers/` are session records and are left untouched. Prose uses of the verb
"deactivate/deactivated" (e.g. login-denial descriptions) remain — the migration
targets the field, not the English word.

### Data note (ops)

Any doc that got `deactivated: true` from the old admin-service path is currently — and
will remain — treated as active by readers. To preserve those deactivations, ops must
run the one-time backfill documented in `docs/active-field-backfill.md` (the runbook is
the authoritative copy). No code-level migration is needed; readers never look at
`deactivated` after this PR.

## Testing

- `pkg/model`: table-driven `IsActive` tests (nil user, nil field, true, false); BSON
  round-trip preserving `active: false`; JSON marshal never emits `active`.
- admin-service: handler tests move to the `active` field (mixed-patch guard now keyed
  on `active: false`); login test uses an inactive user; integration tests assert
  `active: false` lands in Mongo and sessions are revoked.
- botplatform-service / media-service: existing deactivated-account tests re-expressed
  with `Active` pointer; `me.active` / member `active` derived from `IsActive()`.
- admin-frontend: vitest suites updated to the `active` wire field.
