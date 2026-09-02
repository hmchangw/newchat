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
| Count | 0 | 10 | 21 | 14 | 6 | **51** |

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

---

## 4. Test coverage — 2 / 5

Coverage is **68.9% (1055 statements)**, below the §4 80% floor, so the dimension is floored at 2. The suite is structurally excellent — the shortfall is concentrated exactly where it hurts.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | 68.9%, under the §4 80% merge floor | `coverage_by_service.txt:30` |
| high | **Every `ShouldBindJSON` malformed-body → 400 branch in the service is uncovered** — all five. Systematic, not incidental: the tests always hand in well-formed structs, so a binding-tag regression (a field flipped to `binding:"required"`) would ship silently **on every mutating admin endpoint** | `handler.go:383`, `:456`, `:674`; `permissions.go:143`, `:462` |
| medium | Two mutating-path store-error branches uncovered: `DeactivateAndRevoke` → `ErrUserNotFound` → 404, and non-conflict `CreateUser` failure → 500. Only the conflict sibling is tested, so **the `errors.Is` discrimination itself is never exercised** | `handler.go:485`, `:428` |
| medium | `store_mongo.go` is **0% in the unit profile** (all 19 methods). The integration suite covers most of it, but `GetUserForAuth` and `Ping` appear in **neither** — and `Ping` is the sole dependency of `/readyz`, so the readiness probe's real failure mode is untested end-to-end | `store_mongo.go:186`, `:581`; `handler.go:657` |
| medium | **No test asserts the routes→middleware wiring.** `registerRoutes` reads 100% only because `client_update_test.go` executes it; nothing fails if a future route is hung off `r` instead of the `admin` group and **ships unauthenticated**. `requireAdmin` itself is well tested in isolation, which makes the wiring the only unguarded link | `routes.go:17-31`; `middleware_test.go:34` |
| medium | `pwhash.Hash` failure → 500 uncovered at all three sites, despite being **trivially forceable**: `Config.BcryptCost` is injected and bcrypt rejects cost > 31 | `handler.go:396`, `:687`; `login.go:189` |
| low | The `seen[acct]` dedup branch — an applicant/approver repeating a subject account — is never taken, so the documented "not reported twice" contract is unverified | `permissions.go:213`, contract at `:205-209` |
| low | Best-effort/degraded branches uncovered: audit-write failure logging, HR bootstrap marshal failure, fanout envelope marshal failure, on-duty RPC `reply == nil`. `roomRPC` is an injected interface, so the nil-reply case is one fake away | `handler.go:209`, `:234`, `:266`; `room_onduty.go:101` |
| nitpick | Multipart relay error paths in the client-update proxy are uncovered, leaving `buildUploadBody` at 82.1%; each leaks a `b.Close()` obligation if mis-edited | `client_update.go:259`, `:265`, `:275`, `:308` |

**Verified, not assumed:** the integration suite is fully compliant — `//go:build integration`, `package main`, `TestMain` via `testutil.RunTestsWithPrewarm(m, EnsureMongoReplicaSet, EnsureNATS)`, containers exclusively from `pkg/testutil` with no inline `GenericContainer`. Mocks are mockgen-generated and unedited. `publish` and `roomRPC` are injected fields, so no unit test touches NATS. No `time.Sleep`, no package-level mutable test state, no order dependence. Tests are densely table-driven (27 subtests in `handler_test.go`, 26 in `permissions_test.go`), and `router_test.go:18` is a genuinely high-value regression guard on the fanout-deadline invariant.

### Recommendations
- `high` — Add one table-driven `TestHandler_MalformedBody` covering all five bind sites with truncated JSON, wrong-typed field and empty body; assert 400 + `errcode.AuthMissingFields`.
- `medium` — Cover the `errors.Is(err, ErrUserNotFound)` → 404 vs generic → 500 split at both sites; these are the branches that decide what an operator sees during an incident.
- `medium` — Add integration coverage for `GetUserForAuth` (inactive/missing/valid) and `Ping`, plus a `/readyz` test asserting the degraded response when `Ping` fails.
- `medium` — Add a routes-wiring test that walks `gin.Engine.Routes()` and asserts every `/v1/admin/*` path returns 401 without a bearer token — cheap, and it closes the only gap `middleware_test.go` cannot see.
- `medium` — Set `BcryptCost: 32` in a subtest to drive the three `pwhash.Hash` 500 paths.
- `low` — Use a `roomRequester` fake returning `(nil, nil)`; target `permissions.go:213` and the `client_update.go` multipart paths — these plus the above are worth roughly the 11-point shortfall without vanity padding.

---

## 5. Maintainability — 3 / 5

Unusually well-commented and cleanly split by topic for its size, but `package main` now carries five unrelated domains behind one 15-method store and one 8-field `Handler`, with a 183-line handler, a duplicated fanout loop, and a cache-invalidation seam that already leaks.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | The service has outgrown the flat single-package layout: 4,205 production lines in `package main` spanning **five unrelated domains** — admin user CRUD, session auth + self-service password, a permission ledger with its own timezone/date semantics, a room-duty NATS RPC, and a streaming artifact relay. Adding a sixth surface means another file in the same package plus another method on `AdminStore` and a full mock regeneration | `handler.go:31`; `permissions.go:26`; `room_onduty.go:343`; `client_update.go:291` |
| high | **Session-cache invalidation is split across the store/handler boundary and silently missing on two of four revoke paths** — see Chapters 2–3. Revocation semantics differ by which endpoint you happen to call, and **nothing in the interface signals it** | `handler.go:678`; `store_mongo.go:296`, `:322` |
| high | `createPermissions` is **183 lines**: ten sequential validation gates, subject/applicant/approver classification via an **index-position trick** (`i < len(subjects)`), grant construction, store write, audit batch and fanout — all inline. The positional coupling to how `checkAccounts` was appended is the kind of invariant a future edit breaks silently | `permissions.go:138-296`, trick at `:220`, append at `:197` |
| medium | The cross-site fanout loop is duplicated near-verbatim and **has already drifted** — the same publish failure logs at `Warn` in one copy and `Error` in the other | `handler.go:205` and `permissions.go:370` |
| medium | `AdminStore` is a 15-method interface mixing users, audit and permissions, regenerating a 293-line mock whenever any one domain changes | `store.go:258` |
| medium | Audit-entry construction is duplicated and **has already diverged**: `h.auditEntry` stamps `idgen.GenerateUUIDv7()`, but `handleChangePassword` hand-builds the same struct with `idgen.GenerateID()` and `nowMillis()` instead of the shared clock — **two `_id` formats in one collection, from one service** | `handler.go:251` vs `login.go:206` |
| medium | The user field list is maintained in **three hand-synchronized places with no compile-time link**: `userProjection` (19 bson keys), `userView` (19 fields), `toView` (19 assignments). Adding a user field requires editing all three plus `fanoutProjection` | `store_mongo.go:89`; `handler.go:82` |
| low | Dead references: comments describe a `requireAuth` middleware that **does not exist**; `authenticate` was factored out to serve it and now has a single caller | `middleware.go:29`, `:80` |
| low | The timeout-ordering invariant spanning six knobs is enforced by ~90 lines of prose across two files, and `checkHandlerTimeout` is applied to two of the three timeouts, with the third excluded by comment only. A new timeout has no mechanical rule to follow | `config.go:394`; `client_update.go:361` |
| nitpick | Two time sources coexist — the stubbable `nowMillis` var and direct `time.Now().UTC()` — so only some timestamps are test-controllable; `handler_test.go` (1,904 lines) mirrors the production god-file | `handler.go:79`, `:266` |

### Recommendations
- `high` — Take the sanctioned sub-package exception: move `permissions.go`+tests to `permissions/`, `client_update.go` to `clientupdate/`, `login.go`+`middleware.go` to `auth/`, each with its own narrow store interface and `mocks/`. `AdminStore` then splits into `UserStore`/`AuditStore`/`PermissionStore` naturally.
- `high` — Make the two transactional revoke methods return the deleted session IDs and call `sessioncache.BustMany` in `setPassword` and the deactivate branch, so all four revoke paths invalidate identically.
- `medium` — Extract `fanoutEnvelopes(ctx, evType, payloads) []string` and have both fanout sites call it; settle on one log level.
- `medium` — Decompose `createPermissions` into `bindPermissionRequest`, `validateAccounts(...)` returning `(unknown, inactive []string)` with **explicit roles** instead of the positional test, and `buildGrants`.
- `medium` — Route `handleChangePassword` through `h.audit`/`h.auditEntry` so `admin_audit._id` has one format and one clock.
- `low` — Generate `userProjection` from the `userView` field set (or add a test asserting the three lists match); delete the `requireAuth` comment references and inline `authenticate` into its only caller.

---

## 6. Integration — 4 / 5

Cross-service contracts are unusually disciplined — every subject comes from `pkg/subject`, every event struct carries a `Timestamp` set at the publish site, and both INBOX event types have a live consumer in `inbox-worker` — but two revoke paths skip the session-cache bust and the fanout has no durable retry lane.

### The `docs/client-api.md` obligation does **not** bind this service
The rule is scoped to handlers on `chat.user.{account}.…` subjects and `auth-service` HTTP routes. admin-service registers **no NATS handler at all** (it is an outbound RPC client only) and no auth-service route. It is nonetheless documented voluntarily and accurately: all 17 routes in `routes.go` map 1:1 onto `docs/client-api.md` §9.1–9.17, and the derived view matches — including `syncFailures`, the resync lanes and the `client_update.upload` audit action. **No drift found**; `events.md` correctly omits these.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | `setPassword`, `handleChangePassword` and the deactivate branch revoke sessions in Mongo but never call `sessioncache.Bust*`, so revoked tokens keep authenticating **in every peer service that resolves through the shared session cache**. The service's own test asserts this exact invariant for the other two paths ("every revoked session must be evicted from the cache that authorizes it"), and its comment puts the window at ~67 minutes | `handler.go:697`, `:492`; `login.go:198`; `store_mongo.go:296-298`, `:321-322`; `revokebust_test.go:36-48` |
| medium | Cross-site `user_account_updated` / `user_permissions_updated` are **direct JetStream publishes into the remote INBOX with no OUTBOX buffering**, so a failed cross-gateway publish is surfaced as a response field and then lost. The federation contract buffers origin-side federation through OUTBOX "so a failed cross-gateway publish is durably retried rather than lost"; this service instead relies on an operator noticing `syncFailures` and calling `POST …/resync`. A deliberate, documented trade — but a **manual**-healing lane where the rest of the fleet has an automatic one, and a SIGTERM mid-fanout drops the signal entirely | `handler.go:214`; `permissions.go:361-386`, rationale at `:391-403` |
| medium | `CLAUDE.md`'s federation section does not list `admin-service` among the cross-site publishers, so **its two INBOX event types are undocumented in the binding architecture doc** | `CLAUDE.md` §"Stream bootstrap ownership"; publishers at `handler.go:214`, `permissions.go:361` |
| medium | Audit `_id`s are generated in **two different formats for the same collection**: `idgen.GenerateUUIDv7()` (32-char hex) everywhere except the self-service password change, which uses `idgen.GenerateID()` (17-char base62). Same collection, two key shapes, hence two B-tree insert patterns — and `ListAudit` sorts on `timestamp` + `_id` desc, so a tie-break across the two formats is not even consistently ordered | `login.go:207` vs `handler.go:251`; `store_mongo.go:353` |
| low | `subject.OrgSyncUsersUpsert` documents its parameter as the **central** site id, but admin-service passes its own `SITE_ID`. Benign in practice (hr-sync-worker provisions `HR-{siteID}` per entry in `SITE_IDS`, and room-worker/message-worker do the same), but the builder's doc comment invites a future caller to pass a real central id and silently publish to a stream nobody owns | `handler.go:238`; `pkg/subject/subject.go:1774-1776` |
| nitpick | `RoomRestrictedRequest.Timestamp` is deliberately left zero at the publish site. Correct here — it is a request/reply body, not a JetStream event, and room-service overwrites it on acceptance, and the comment says so — but it is the one `pkg/model` struct this service marshals without stamping a timestamp, so it reads as a violation until you read the comment | `room_onduty.go:71-78`; `pkg/model/room.go:160` |

### Recommendations
- `high` — Widen the two revoke methods to return the deleted session `_id`s (both already run `DeleteMany`; switch to a projected `Find`+`DeleteMany` inside the same transaction) and call `sessioncache.BustMany` at all three call sites, matching `handler.go:599`.
- `medium` — Route the two INBOX event types through `pkg/outbox.Publish` with both added to `ConcurrentEventTypes` (both are watermark-guarded and order-insensitive by construction), keeping `syncFailures` as a fast-path signal rather than the only recovery path.
- `medium` — Add `admin-service` to `CLAUDE.md`'s cross-site-publisher list, naming the two event types it originates.
- `medium` — Replace `idgen.GenerateID()` at `login.go:207` with `GenerateUUIDv7()`, or better, build that entry through `h.auditEntry` so there is exactly one audit-ID construction site.
- `low` — Rename `OrgSyncUsersUpsert`'s parameter to `siteID` and correct the doc comment; add a fanout-lane test asserting the subject built for a remote peer is exactly `chat.inbox.{dest}.external.user_account_updated`.

---

## 7. Performance — 3 / 5

Projection discipline, batching and goroutine budgets are genuinely strong — but the list endpoints rest on an unindexed double collection scan with unbounded `skip`, the permission batch writes are uncapped, and two revoke paths leave the session cache stale.

### Verified clean
Every find/aggregation carries an explicit projection (`userProjection`, `fanoutProjection`, `accountStateProjection`, `permissionGrantProjection`). **No `$lookup` anywhere.** `FindAccountStates` and `GetUserPermissionsForAccounts` replace what would be N lookups with one `$in`. `mongo.ErrNoDocuments` handled at every expected-miss site. No `time.Sleep` synchronization. The fanout errgroups are per-destination, deadline-bounded and always joined — no leak. The upload relay **streams** rather than buffering. No JetStream consumers, so the `jsretry`/`BackOff` rules do not apply.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | `SearchUsers` runs an **unindexed `$or` of three case-insensitive `$regex`** over the whole (all-sites) `users` collection — and does it **twice per page** (`CountDocuments` + `Find`). `EnsureIndexes` deliberately creates no supporting index. With `q==""` the count is still a full scan, since `CountDocuments` never uses collection metadata | `store_mongo.go:133`, `:137`, `:38-41` |
| high | Sessions deleted inside the two transactional store methods are **never busted from the Valkey session cache**. `SESSION_CACHE_TTL` defaults to 90 m and **slides on read**, so a revoked or deactivated token keeps authenticating from cache **indefinitely while in active use** — the exact failure `pkg/session/session.go:36-39` says the id-returning contract exists to prevent | `store_mongo.go:296`, `:322` vs `handler.go:599`, `:620` |
| medium | `subjectAccounts` has **no upper bound** — only emptiness is checked. The whole list then flows into a `$in`, an `InsertMany`, an `UpdateMany` `$in` and an `AppendAuditMany` — **the last three inside one Mongo transaction**. Chunking exists only for the fanout (`fanoutChunkSize = 1000`), not for the DB write; a large batch risks the 16 MB BSON limit and the 60 s transaction lifetime. `resyncPermissions` has the same uncapped list | `permissions.go:155`, `:197`, `:471`; `store_mongo.go:437`, `:442`, `:575` |
| medium | `parsePaging` accepts any `page >= 1` with **no ceiling**, and all three lists use offset paging. `?page=10000000&limit=100` makes the server walk ~10⁹ index entries per request, **on three separate endpoints** | `handler.go:281`; `store_mongo.go:141`, `:376`, `:518` |
| medium | Unauthenticated `POST /v1/login` performs an **unconditional bcrypt (cost 10) on every request**, including both denial arms, with no shedding: `applyBaseMiddleware` installs no concurrency limiter even though `ginutil.MaxConcurrency` exists and is used by user-service. A trivial request flood saturates all CPU | `login.go:74`, `:85`; `main.go:44-50` |
| low | `ListAudit`'s `actor` and `action` filters have no supporting index — only `siteId+timestamp` and `siteId+targetAccount+timestamp` are created. Those two filters degrade to a residual scan of an unboundedly growing collection, **paid twice** (count and find) | `store_mongo.go:359-365`, `:44-56` |
| low | `listSessions` calls `ListForAccount` with no limit. Bounded in practice by `SESSIONS_MAX_PER_ACCOUNT=100`, but the cap is enforced **only on the login path**, so pre-cap or misconfigured data returns unbounded | `handler.go:576`; `pkg/session/session.go:181` |
| nitpick | `EnsureIndexes` failure is logged and startup continues; a cold deploy against a fresh DB then serves every list from an unindexed collection with no readiness signal | `main.go:92-94` |

### Recommendations
- `high` — Add a `users` index for the search path (text or prefix-anchored), or replace the unanchored `$regex` with an anchored match; drop the per-page `CountDocuments` in favour of `EstimatedDocumentCount` when `q==""`, or return `hasMore` from a `limit+1` fetch instead of `total`.
- `high` — Make the two revoke methods return the deleted session `_id`s (the pattern `pkg/session.deleteAndReportIDs` already implements) and call `sessioncache.BustMany` at all three sites.
- `medium` — Cap `subjectAccounts`/`accounts` at an explicit constant (the `AppendAuditMany` comment already assumes ~200) and reject over it with a 400, so transaction size is bounded by contract.
- `medium` — Clamp `page` in `parsePaging` (e.g. so `skip <= 10_000`), and prefer keyset pagination on `{recordedAt,_id}` / `{timestamp,_id}` for the audit and ledger browses — both already have matching sort indexes.
- `medium` — Install `ginutil.MaxConcurrency` on the router (or at minimum on `/v1/login`) so bcrypt work is shed with 429 rather than queued.
- `low` — Add `admin_audit` indexes on `{siteId, actorAccount, timestamp:-1}` and `{siteId, action, timestamp:-1}`; fail readiness (not just log) when `EnsureIndexes` fails.

---

## 8. Prioritized action list

| # | Sev | Action | Dimension | Evidence | Why |
|---|-----|--------|-----------|----------|-----|
| 1 | `high` | Return revoked session IDs from `UpdateUserPasswordAndRevoke` and `DeactivateAndRevoke`, and call `sessioncache.BustMany` in `setPassword`, `handleChangePassword` and the deactivate branch | Code quality / Arch / Integration / Perf | `store.go:67`, `:74`; `store_mongo.go:296`, `:322`; `handler.go:483`, `:701`; `login.go:201` | **Four of six experts found this independently.** A reset password or a deactivated admin keeps authenticating from the session cache — and because `SESSION_CACHE_TTL` slides on read, an actively-used token stays valid indefinitely. `pkg/session` was explicitly redesigned to close this exact hole; the store interface re-opens it by returning only `error`. The service's own test asserts the invariant for the other two paths. |
| 2 | `high` | Index the `SearchUsers` path (or anchor the regex) and drop the per-page `CountDocuments` | Performance | `store_mongo.go:133`, `:137`, `:38-41` | Two unindexed full scans of the all-sites `users` collection **per page view**, on the admin console's primary screen — and `EnsureIndexes` deliberately creates nothing to help. |
| 3 | `medium` | Install `ginutil.MaxConcurrency`, at minimum on `/v1/login` | Performance | `main.go:44-50`; `login.go:74` | An unauthenticated endpoint runs cost-10 bcrypt on **every** request including both denial arms, with no shedding. A trivial flood saturates CPU. The helper exists and user-service already uses it. |
| 4 | `high` | Add a table-driven `TestHandler_MalformedBody` across all five bind sites | Test coverage | `handler.go:383`, `:456`, `:674`; `permissions.go:143`, `:462` | **Every** malformed-body 400 branch on **every** mutating admin endpoint is untested — a systematic gap, so a binding-tag regression ships silently service-wide. |
| 5 | `medium` | Branch `session.ErrNotFound` in `authenticate`; branch `ErrUserNotFound` in `handleChangePassword` | Code quality | `middleware.go:41-47`; `login.go:176-180` | Today a **Mongo outage reads to every admin as an expired session** and mass-logs them out, logged at Info. `pkg/session` exports the sentinel precisely so these can be told apart. |
| 6 | `medium` | Cap `subjectAccounts` and `accounts`, and clamp `page` in `parsePaging` | Performance | `permissions.go:155`, `:471`; `handler.go:281` | Three uncapped writes inside **one Mongo transaction** (16 MB BSON / 60 s lifetime), and unbounded offset paging on three endpoints. |
| 7 | `high` | Decompose `createPermissions`, replacing the `i < len(subjects)` positional trick with explicit roles | Maintainability | `permissions.go:138-296`, `:220` | 183 lines whose account-classification correctness depends on the order a slice was appended 23 lines earlier — silently breakable by any future edit. |
| 8 | `medium` | Route `handleChangePassword` through `h.auditEntry`; extract one shared `fanoutEnvelopes` helper | Maintainability / Integration | `login.go:206` vs `handler.go:251`; `handler.go:205` vs `permissions.go:370` | Two `_id` formats already coexist in one audit collection, and the duplicated fanout has already drifted to two different log levels. Both are live divergences, not hypotheticals. |
| 9 | `high` | Take the sanctioned sub-package split (`permissions/`, `clientupdate/`, `auth/`) | Maintainability / Architecture | `handler.go:31`; `store.go:46` | Five unrelated domains behind one `Handler` and one 15-method store; a permission change and a file upload currently share a struct and a mock. |
| 10 | `medium` | Route the two INBOX event types through `outbox.Publish`, or document the manual-resync trade in `CLAUDE.md` | Integration / Architecture | `handler.go:214`; `permissions.go:361` | This is the only cross-site publisher in the fleet with **manual** healing, and a SIGTERM mid-fanout drops even the `syncFailures` signal. Whichever way it goes, `CLAUDE.md` should name admin-service as a publisher. |

### Verdict

**Fix item 1 before shipping.** It is a security defect — revocation that does not revoke — found independently by four experts, with the repo's own `pkg/session` documentation and this service's own test both stating the invariant it breaks. Items 2–3 are capacity cliffs on the admin console's most-used screen and its unauthenticated login endpoint. The rest is solid, thoughtfully-commented work whose main debt is that five domains have accumulated in one package.
