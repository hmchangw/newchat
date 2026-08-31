# admin-service — Production Readiness Review

**Service:** `admin-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

Idiomatic, exceptionally well-commented Gin service with textbook route/middleware wiring and an unusually well-reasoned timeout/budget design. It is dragged down by **one security defect that four of the six experts found independently**: three of the four session-revoke paths delete sessions in Mongo but never bust the Valkey session cache, so **a reset password or a deactivated admin keeps authenticating for the cache window**. `pkg/session` was explicitly redesigned to close exactly this hole — its bulk deletes return IDs *because* "returning only a count is what let a revoked token keep authenticating from cache" — and this service's store interface re-creates it by returning only `error`. The service's own test asserts the invariant for the other two paths.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 9 | 21 | 15 | 4 | **49** |

---

## 2. Go code quality — 4 / 5

Idiomatic, exceptionally well-commented Go with consistent contextual error wrapping, correct `errcode` Tier-1 usage and **no secret leakage**. Two genuine correctness defects plus small inconsistencies keep it off 5.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **The three transactional revoke paths delete sessions but never bust the session cache** — `setPassword`, the deactivate branch of `updateUser`, and self-service change-password. The sibling paths do (`handler.go:599`, `:620`, `login.go:130`). `pkg/session/session.go:36-40` states why the bulk deletes return ids at all: *"Returning only a count is what let a revoked token keep authenticating from cache until its refresh window elapsed."* `UpdateUserPasswordAndRevoke`/`DeactivateAndRevoke` return no ids, so **the bust is structurally impossible** | `handler.go:701`, `:483`; `login.go:201`; `store.go:63`, `:71` |
| medium | `authenticate` collapses **every** `FindByHash` error — including a Mongo outage — into `errcode.Unauthenticated("invalid session token")`. `pkg/session` exports `ErrNotFound` precisely so the two can be told apart. As written, a database blip logs at Info and reads to the console as an expired session, **mass-logging admins out** | `middleware.go:41-47`; `pkg/session/session.go:48` |
| medium | `handleChangePassword` wraps every `GetUserForAuth` error raw, so a user deleted mid-session yields a 500 instead of the 404/401 every other call site produces by branching on `ErrUserNotFound` | `login.go:176-180` vs `handler.go:330`, `:352`, `:485`, `:498`, `:702` |
| medium | `handleChangePassword` hand-builds its `AuditEntry` with `idgen.GenerateID()` (17-char base62) while every other audit row uses `h.auditEntry` with `idgen.GenerateUUIDv7()` — **two `_id` formats in one `admin_audit` collection** — and the duplicated construction bypasses the `SiteID`/principal stamping the helper centralises | `login.go:206-215` vs `handler.go:251` |
| low | `loginDenied` logs at Warn and then hands the same denial to `errhttp.Write`, which `Classify`-logs it again. Defensible as a deliberate auth-outcome audit line, but it should then not also go through the classifying writer | `login.go:144-148` |
| low | `loginDenied(c *gin.Context, ctx context.Context, …)` takes `context.Context` as its **second** parameter, against the Go convention and `revive`'s `context-as-argument` | `login.go:144` |
| low | An empty PATCH body returns `200 {"status":"ok"}` and still writes a `user.update` audit row, even though `UpdateUser` wrote nothing — **a no-op masquerading as a successful edit in the audit trail** | `handler.go:496`, `:513-519`; `store_mongo.go:225-227` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |

### Recommendations
- `high` — Change `UpdateUserPasswordAndRevoke` and `DeactivateAndRevoke` to return the revoked session ids (mirroring `session.Store`'s bulk deletes), and call `sessioncache.BustMany` in `setPassword`, the `updateUser` deactivate branch, and `handleChangePassword`. Pin each with a test alongside `revokebust_test.go`.
- `medium` — In `authenticate`, branch on `errors.Is(err, session.ErrNotFound)` for the 401 and return a wrapped error otherwise, so a store failure surfaces as `internal`.
- `medium` — Branch `ErrUserNotFound` in `handleChangePassword` to a typed 401/404 rather than a wrapped 500.
- `medium` — Replace the hand-built entry with `h.audit(ctx, c, "password_change_self", …)` so every `admin_audit._id` is a UUIDv7 and the stamping lives in one place.
- `low` — Drop the extra `WarnContext` in `loginDenied` (or move it to a dedicated non-error auth log) and reorder its parameters to `(ctx, c, account)`; reject an all-nil PATCH with `errcode.BadRequest` before touching the store.

