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

---

## 3. Architecture — 3 / 5

Boundaries, DI and consumer-defined interfaces are textbook, and the timeout/budget design is unusually well reasoned — but the store interface reintroduces a session-cache invalidation gap `pkg/session` was redesigned to close, and two shared knobs are re-declared locally.

### Verified clean
Routes live only in `routes.go`; `GET /healthz` + `/readyz` exposed; request-ID and access-log middleware wired; Gin `ReadTimeout`/`WriteTimeout` set; the outbound client comes from `restyutil` with an explicit timeout; no `os.Getenv`; no raw `fmt.Sprintf` subjects (all via `pkg/subject`); `pkg/shutdown.Wait` with the documented HTTP order; `Pool mongoutil.PoolConfig` and `Valkey valkeyutil.Config` correctly mounted as named fields.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | `UpdateUserPasswordAndRevoke` and `DeactivateAndRevoke` delete sessions inside their own Mongo transaction and **return no session IDs**, so no `sessioncache.Bust*` can run on the three most security-critical revoke paths (see Chapter 2) | `store.go:67`, `:74`; `store_mongo.go:296`, `:322` |
| medium | `pkg/session.DeleteForAccountExcept` now has **zero production callers repo-wide** — orphaned when admin-service moved the revoke into its own transaction. Dead contract surface that hides the gap above | `pkg/session/session.go:42`, impl `:135` |
| medium | `SESSIONS_MAX_PER_ACCOUNT` and `BCRYPT_COST` are re-declared with their own tag + `envDefault` here **and** in `botplatform-service`. Both services write the same shared `session.Collection`, so the per-account cap is a shared knob | `config.go:38`, `:44`; `botplatform-service/config.go:34`, `:37` |
| medium | Cross-site fanout publishes straight into the remote INBOX with **at-most-once** semantics: a failed publish is reported in `syncFailures` and healed only by a manual `POST …/resync`. `user_account_updated` and `user_permissions_updated` appear in **neither** `pkg/outbox` partition set, so the durable OUTBOX retry lane does not cover this service | `handler.go:214`; `permissions.go:382`; `pkg/outbox/outbox.go:21`, `:51` |
| medium | One `Handler` and one 16-method `AdminStore` span six unrelated domains across 2,317 non-test lines — exactly the size at which the sanctioned sub-package layout applies. The flat layout is still legal, but it is now the reason a permission change and a file upload share a struct | `handler.go:31`; `store.go:46` |
| low | `AdminStore.EnsureIndexes` is never called **through the interface** — `main.go` and every integration test call it on the concrete type. Interface surface no consumer uses, paid for by every mock and fake | `store.go:110` |
| low | `loadConfig` validates timeouts, pool and client-update settings but **not** `BcryptCost` or `SessionsMaxPerAccount`; `botplatform-service` fails fast on both. `BCRYPT_COST=0` silently degrades to bcrypt's default | `config.go:38`, `:44` |
| low | Startup uses two failure idioms in one function: every path returns a wrapped error, but the read-preference branch calls `os.Exit(1)` inline, bypassing `run() error` | `main.go:79-81` |
| nitpick | `gin.Recovery()` is registered **third**, after `ginutil.CORS()` and `obsMW`; a panic in either before `c.Next()` is unrecovered | `main.go:45` |
| nitpick | `h.valkey` is assigned post-construction although the `handlerOption` seam exists two lines earlier | `main.go:125`; `handler.go:73` |

### Recommendations
- `high` — Change the two revoke methods to return `[]string` of revoked session `_id`s and call `sessioncache.BustMany` at all three sites; then delete the orphaned `DeleteForAccountExcept`.
- `medium` — Move `SESSIONS_MAX_PER_ACCOUNT` (and `BCRYPT_COST` if `pkg/pwhash` should own it) into a `session.CapConfig`/`pwhash.Config` mounted as a named field in both services.
- `medium` — Either route the two INBOX event types through `outbox.Publish` (adding them to `ConcurrentEventTypes`), or record in code why manual resync is the accepted heal path, so the divergence from the federation rule is deliberate on the page.
- `medium` — Split along the existing seams: `permissions.go` and `client_update.go` are already self-contained; promoting them to sub-packages with their own narrow store interfaces would cut `AdminStore` roughly in half.
- `low` — Validate `BcryptCost ∈ [4,31]` and `SessionsMaxPerAccount > 0`; replace `main.go:79-81`'s `os.Exit` with a wrapped return.
- `nitpick` — Move `gin.Recovery()` to the first `r.Use`; inject Valkey via a `withValkey` option.

