# Remove the unused `Subscription.AvatarURL` field

**Date:** 2026-06-22
**Status:** Approved (design)
**Scope:** Remove the dead `Subscription.AvatarURL` field across the backend. One change, shippable as a single commit. Frontend UI and Mongo data cleanup are explicitly **out of scope**.

## Background

A new avatar mechanism is planned: a standalone `avatar-service` that the
frontend queries directly (the frontend assembles the request content itself).
Ahead of that, we remove the old, unused subscription-level avatar field so the
new design starts from a clean slate.

## Problem

`Subscription.AvatarURL` (`pkg/model/subscription.go:77`) is a **dead field**.
The full footprint is four sites, none of which is a writer:

| Site | Role |
|------|------|
| `pkg/model/subscription.go:77` | Field definition (`json/bson:"avatarUrl,omitempty"`) |
| `user-service/mongorepo/subscriptions.go:142` | Read projection (`"avatarUrl": 1`) |
| `pkg/model/model_test.go` | Roundtrip + omitempty assertions (3 lines) |
| `docs/client-api.md:507` | Subscription field-table row |

Findings from the codebase sweep:

- **No writer anywhere.** No service (`room-service`, `room-worker`,
  `user-service`, …) ever `$set`s or assigns `avatarUrl`. It was introduced in
  PR #279 (user-service consolidation) but never wired to a write path.
- **Never on the wire.** The field is `omitempty` and always empty, so it is
  never serialized in any response or `subscription.update` event. No client can
  observe it today.
- **Frontend does not read it.** The frontend "avatar" is a separate
  initials-circle UI (`.message-row-avatar` + `senderInitial()`); it never reads
  `subscription.avatarUrl`. Left untouched.
- **`App.AvatarURL` does not exist in code** (only in historical superpowers
  plans); not in scope.

## Design

Delete the field and its three downstream references — a clean removal, since
nothing produces or consumes it.

| File | Change |
|------|--------|
| `pkg/model/subscription.go` | Delete the `AvatarURL` field (line 77). The preceding doc comment already describes only the metadata group + `UpdatedAt` (it never named `AvatarURL`), so it stays as-is. |
| `user-service/mongorepo/subscriptions.go` | Delete `"avatarUrl": 1` from `subscriptionProjection` (line 142). |
| `pkg/model/model_test.go` | Drop the `AvatarURL` literal field, the `raw["avatarUrl"]` assertion, and `"avatarUrl"` from the omitempty-key list. Keep all other field assertions intact. |
| `docs/client-api.md` | Delete the `avatarUrl` row from the Subscription field table (line 507). |

### Out of scope

- Frontend initials-circle avatar UI — kept; will be revisited when
  `avatar-service` lands.
- Mongo `$unset avatarUrl` data cleanup — unnecessary (Go never wrote the field;
  any residual data is inert once the projection is gone).

## Verification (removal-shaped TDD)

A removal has no new behavior to red-test; the discipline is "test suite stays
green and stops referencing the deleted field."

1. Edit `model_test.go` to drop the `AvatarURL` assertions.
2. `make build SERVICE=user-service` — projection removal still compiles.
3. `make test SERVICE=user-service` and `make test` (pkg/model) — all green.
4. `make lint` — clean.

## Commit plan

Single atomic commit on `ds-feat/remove-old-avatar-info`: code + `client-api.md`
together (client-facing change must update the API doc in the same change).
Message: remove the unused `Subscription.AvatarURL` field.
