# tcard-service — Production Readiness Review

**Service:** `tcard-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.5 / 5

Textbook conformance to the repo's Gin-service shape, and a read path that is genuinely excellent: a lock-free `atomic.Pointer` snapshot means a card fetch does zero Mongo reads, zero locks and zero re-marshalling — the cached bytes go straight to the wire.

The weakness is on the write and refresh side, and the two halves compound. **`POST /api/v1/cards/refresh` and `POST /api/v1/cards/validate` have no authentication or authorization** — `refresh` is an unauthenticated trigger for an unbounded full-collection scan. And **`Load` starts its 60-second budget before acquiring a non-context-aware mutex**, so N concurrent refresh calls serialize into N sequential full scans, each pinning a goroutine and a request context past its own HTTP deadline, with later waiters reaching Mongo with almost none of their budget left. Separately, `/validate` is advisory only: it checks "highest version" against an in-memory snapshot and then writes nothing, so two authors validating the same version concurrently both pass.

Coverage is 69.7%, and the service's own CI gate excludes two files before measuring — reporting ~97% and passing the same 80% threshold this audit fails.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 4 | 9 | 13 | 1 | **27** |

---

## 2. Go code quality — 4 / 5

Idiomatic, well-commented, `slog`-clean and errcode-correct throughout; the only real defects are a sentinel compared with `!=` instead of `errors.Is`, decorative struct tags, and an errcode surface with no `reason` on an endpoint whose whole purpose is client-side branching.

### Findings
- `medium` — `err != http.ErrServerClosed` compares a sentinel directly instead of `errors.Is` — `tcard-service/main.go:150`
  The repo's newer Gin services (`media-service/main.go:143`, `admin-service/main.go:174`, `botplatform-service/main.go:160`) use `errors.Is`; this is the older half of a split convention and breaks if the stdlib ever wraps.
- `medium` — `HandleValidate` returns eleven distinct `errcode.BadRequest` plus one `Conflict` with no `WithReason`, and there is no `pkg/errcode/codes_tcard.go` — `tcard-service/handler.go:144-169`, `handler.go:174-215`
  A card-authoring client must branch on prose strings to tell "path must have exactly 3 segments" from "version must be 1.5". CLAUDE.md §3 reserves `WithReason` for exactly this case.
- `low` — `card`'s `json`/`bson` tags are dead: the doc is decoded into `bson.D` and the struct is built field-by-field in `docToCard`, and `Template` is written to the wire raw via `c.Data` — `tcard-service/store.go:12-16`, `store_mongo.go:73-96`, `handler.go:100`
  Tags that no codec reads mislead the next reader into thinking the shape is wire-bearing.
- `low` — `cardDoc` carries `json` tags only, against CLAUDE.md's "all model structs get both `json` and `bson`" — `tcard-service/store.go:20-27`
  The inline comment ("never persisted") justifies it; flagged so the deviation is on record, not to be changed.
- `low` — the skip warning cannot name the offending document, because the projection removes `_id` and the branch fires precisely when `path` is absent — `tcard-service/store_mongo.go:41`, `store_mongo.go:60`
  An operator gets "a card doc is broken" with nothing to grep on.
- `low` — SAST audit coverage is incomplete: `gosec` and the 18 repo-owned semgrep rules are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (blocked egress). Environmental, not a service defect — per `GLOBAL_PREP.md`.

### Recommendations
- `medium` — Switch `main.go:150` to `!errors.Is(err, http.ErrServerClosed)`, matching `media-service`/`admin-service`.
- `medium` — Add `pkg/errcode/codes_tcard.go` with reasons for the validate failure classes (`CardPathShape`, `CardVersionNotSemver`, `CardSchemaPinned`, `CardVersionNotHighest`) and attach them via `WithReason`.
- `low` — Delete the tags on `card`, or add a round-trip test that makes them load-bearing.
- `low` — Keep `_id` in the projection and log it in the skip warning at `store_mongo.go:60` (it is already dropped from the payload in `docToCard`, so nothing leaks).

---

---

## 3. Architecture — 4 / 5

Textbook conformance to the repo's Gin-service shape — consumer-defined store, constructor DI, typed `caarlos0/env` config, `pkg/shutdown.Wait`, shared knobs mounted as named fields — undercut by two unauthenticated endpoints and a validate/write split that leaves the service authoritative for reads but with no write path.

### Findings
- `high` — `POST /api/v1/cards/refresh` and `POST /api/v1/cards/validate` have no authentication or authorization; `registerRoutes` attaches no guard and `main.go` installs no auth middleware — `tcard-service/routes.go:8-10`, `main.go:110-117`
  `refresh` is an unauthenticated trigger for an unbounded full-collection Mongo scan. The repo already has the pattern for this (`admin-service/routes.go:17` groups mutating routes behind `requireAdmin`).
- `medium` — `/validate` is advisory only: it checks "highest version" against an in-memory snapshot and then writes nothing, and the code itself documents that cards arrive out-of-band — `tcard-service/handler.go:163-168`
  Two authors validate the same version concurrently and both pass; the only real guard is the unique index at `store_mongo.go:26-29`, whose duplicate-key error the client never sees because this service does not do the insert. The endpoint's contract is weaker than it appears.
- `low` — `MONGO_READ_PREFERENCE` is re-declared per service (19 copies, with divergent `envDefault`s) rather than owned by `mongoutil` and mounted as a named field — `tcard-service/main.go:40`, e.g. `portal-service/main.go:62`, `upload-service/main.go:57`
  Fleet-wide, not a tcard defect, and the differing defaults are deliberate policy; noted because CLAUDE.md §6 names exactly this shape as the cause of divergent shared-key config.

Confirmed conformant, no finding: consumer-owned `CardStore` with only `ListCards` (`store.go:29-32`); `NewCardHandler` constructor DI (`handler.go:44`); file organization matches CLAUDE.md exactly; `MONGO_URI` marked `required`, everything else defaulted, `Pool mongoutil.PoolConfig` / `HTTP ginutil.TimeoutConfig` mounted as named fields (`main.go:27-44`); middleware order identical to `portal-service/main.go:150-157`; `pkg/shutdown.Wait` with a 25s budget under the 30s grace period (`main.go:136-147`). The service touches no NATS, so `BOOTSTRAP_STREAMS`, `pkg/stream`, and the INBOX/OUTBOX ownership rules do not apply.

### Recommendations
- `high` — Put `/refresh` and `/validate` behind an authorization guard modelled on `admin-service`'s `requireAdmin` group, or document the ingress ACL that fronts them.
- `medium` — Decide the ownership question: either give tcard-service the write path (`POST /register` was planned and removed — see `handler_test.go:491`) so validate-and-insert is atomic against the unique index, or demote `/validate` to a lint endpoint and say so in its response.
- `low` — Raise `MONGO_READ_PREFERENCE` into `mongoutil` as a named config field with per-service `envDefault` set at the mount point.

---

---

## 4. Test coverage — 2 / 5

**69.7% (223/320 statements)** — below the CLAUDE.md §4 80% floor, so a `high` finding and a floored score; but the number is almost entirely startup and Mongo-driver code, and the unit-testable logic is at 96–100%.

### Findings
- `high` — 69.7% is under the 80% floor — `tcard-service` (per `coverage_by_service.txt`)
  Per-file: `main.go` 0/66, `store_mongo.go` 13/38, `handler.go` 84/87 (96.6%), `cache.go` 94/96 (97.9%), `semver.go` 27/28, `routes.go` 5/5. All 66 uncovered `main.go` statements are `run()`'s wiring, and 25 of `store_mongo.go`'s are the Mongo-driver funcs that `integration_test.go:19-104` does cover under the `integration` tag — that profile is simply not merged into the repo-wide run.
- `medium` — the service's own CI gate excludes `main.go` and `store_mongo.go` before measuring, so it reports ~97% and passes the same 80% threshold this audit's profile fails — `tcard-service/deploy/azure-pipelines.yml:26-28`, `:49-55`
  Two gates disagree by 27 points on the same code. Whatever the answer, only one number should be authoritative.
- `medium` — the shutdown path in `RefreshLoop` — `if ctx.Err() != nil { return }` after a cancelled `Load` — is never executed by any test — `tcard-service/cache.go:132-134`
  This is the branch that makes `refreshWG.Wait()` in `main.go:142` terminate instead of looping for another 30s; a regression here turns graceful shutdown into a timeout.
- `medium` — three `validateCard` required-field branches are uncovered: `type`, `schema`, and `version` empty — `tcard-service/handler.go:180-185`
  `path` and `_tcardVersion` empty are tested; the other three are not, so the table in `handler_test.go:448-481` reads as complete but is not.
- `low` — `parseSemver`'s `strconv.Atoi` error branch is uncovered and is reachable, not dead: `allDigits` admits `"99999999999999999999"`, which overflows to `ErrRange` — `tcard-service/semver.go:23-26`
- `low` — `List`'s mixed semver/non-semver ordering branch (`if oki != okj`) is uncovered — `tcard-service/cache.go:206-208`
  `TestCardCache_List_NonSemverVersionOrder` (`cache_test.go:412`) uses only non-semver versions, so the mixed case the comment promises a total order for is untested.
- `low` — `docToCard`'s `bson.MarshalExtJSON` failure branch is uncovered — `tcard-service/store_mongo.go:93-95`

Quality is otherwise strong: tests are `package main` (`handler_test.go:1`), table-driven with descriptive subtest names, mocks are generated `go.uber.org/mock` (`mock_store_test.go:1-6`), no real DB or NATS in unit tests, and the integration file has the required build tag and `TestMain(m) { testutil.RunTests(m) }` with `testutil.MongoDB` containers, no inline `testcontainers.GenericContainer` (`integration_test.go:1-20`).

### Recommendations
- `high` — Close the four named branches: cancel-during-`Load` in `RefreshLoop`, the three empty-field validate cases, `parseSemver` integer overflow, and the mixed semver/non-semver sort. That is ~8 statements and lifts the non-`main.go` files to ~100%.
- `medium` — Reconcile the two coverage gates: either merge the `integration` profile into the repo-wide number, or apply the pipeline's `main.go`/`store_mongo.go` exclusion repo-wide. Do not leave both.
- `medium` — Extract `run()`'s wiring into a testable `newServer(cfg) (*http.Server, func(), error)` so the 66 statements at `main.go:53-156` stop being structurally untestable.
- `low` — Make `deepCard` (`handler_test.go:64-67`) a function rather than a package-level `var`; its `json.RawMessage` is a shared mutable slice across tests.

---

---

## 5. Maintainability — 4 / 5

Six small files, comments that explain *why* rather than restate *what*, no dead code and no duplicated logic; the one structural weakness is a path contract that two parts of the service define differently.

### Findings
- `medium` — the write contract and the read contract disagree on path shape: `validateCard` requires exactly three segments, while the cache and list are explicitly "generic over depth" and serve any depth — `tcard-service/handler.go:190-193` vs `cache.go:151`, `cache.go:184-192`
  A two-segment card can be stored out-of-band, cached, listed and served, but can never be validated. Adding a depth rule later means touching both, and nothing links them.
- `low` — the key-filtering rule is expressed twice, once as a Mongo exclusion projection and once as a `switch` in `docToCard`, and the two lists must stay in sync by hand — `tcard-service/store_mongo.go:40-42` and `store_mongo.go:83`
  The comment at `:38-39` says the projection is bandwidth-only and `docToCard` is the correctness guarantee, which is the right split; a shared `var storageOnlyKeys = []string{"_id", "migratedAt"}` would make it structurally true.
- `low` — `cardCache.List` is 60 lines carrying four responsibilities (prefix match, folder dedup, exact-path detection, three-way sort) — `tcard-service/cache.go:161-221`
  It is the only function in the service that needs a second read to follow.
- `nitpick` — `listResponse` repeats the HTTP status inside the JSON body — `tcard-service/handler.go:29-33`
  Legacy-shaped; harmless, but it makes the response schema untypeable against the other two payloads.

### Recommendations
- `medium` — Hoist the path rule into one predicate (`validCardPath(path string) error` in `semver.go` or a new `path.go`) and call it from both `validateCard` and, at minimum, a startup assertion or the skip path in `ListCards`.
- `low` — Extract the sort comparator from `List` into `func lessCardEntry(a, b cardEntry) bool`; that alone takes `List` from 60 lines to ~40 and makes the mixed-semver case directly unit-testable (see D3).
- `low` — Share the storage-only key list between the projection and `docToCard`.

---

---

## 6. Integration — 4 / 5

Most of this dimension is genuinely not applicable — the service publishes no events, owns no stream and does no federation — and what remains (Mongo contract, ID handling, HTTP surface) is correct except that the client-facing HTTP contract is documented nowhere.

### Findings
- `medium` — the three client-facing HTTP endpoints have no contract document anywhere in `docs/` — `tcard-service/routes.go:8-12`; the only mentions of the service in `docs/` are implementation plans (`docs/superpowers/plans/2026-07-14-tcard-service.md`, `docs/superpowers/plans/2026-08-27-mongodb-read-preference-availability.md`)
  CLAUDE.md's `docs/client-api.md` mandate covers `chat.user.…` NATS subjects and `auth-service` HTTP routes, so this is *not* a rule violation — but the response shapes (`refreshResponse`, `listResponse`, the raw-JSON template body, the 400/404/409/503 matrix) are a real cross-team contract with no written form.
- `low` — the wildcard route makes the version delimiter part of the URL grammar with no shared parser: the client must construct `{path}@{version}.template.json` and the service splits on the last `@` — `tcard-service/handler.go:72-93`
  A path containing `@` is rejected on write (`handler.go:187-189`) but tolerated on read by design; that asymmetry is only discoverable by reading both functions.

Confirmed non-applicable or correct, no finding: no NATS connection, subjects, streams, consumers or `pkg/model` event structs exist in this service, so `pkg/subject` builders, `Timestamp` at the publish site, INBOX/OUTBOX lanes, `outbox.Publish` partition membership, `pkg/jsretry`, `pkg/msgbucket` and `ROOM_KEY_RETIRED_TTL` are all out of scope. IDs are not generated here — `_id` is supplied by the out-of-band writer and deliberately dropped (`store_mongo.go:83`), so no `pkg/idgen` format rule applies. The `(path, _tcardVersion)` unique index is created at startup and proven idempotent by integration test (`store_mongo.go:25-33`, `integration_test.go:86-104`), and `mongoutil.EnsureIndexWithRepair` is the repo's shared helper.

### Recommendations
- `medium` — Add a short `docs/tcard-api.md` (or a §in `docs/client-api.md` if the frontend treats it as one surface) with the field tables and status matrix for the three endpoints, in the existing client-api style.
- `low` — Publish the `{path}@{version}.template.json` grammar as one exported helper used by both the handler and any Go client, so the `@`-in-path asymmetry lives in one place.
- `low` — State in that doc that `/validate` is advisory and does not reserve a version (see D2), so callers do not treat a 200 as a write lock.

---

---

## 7. Performance — 3 / 5

The read path is genuinely excellent — lock-free `atomic.Pointer` snapshot, zero Mongo reads, no re-marshalling — but the refresh path pairs an unauthenticated trigger with a non-cancellable mutex and a timeout that starts before the lock, which is a self-inflicted amplification vector.

### Findings
- `high` — `Load` starts its 60-second budget *before* acquiring `writeMu`, so a queued load can reach `ListCards` with almost none of it left — `tcard-service/cache.go:96-99`
  Two waiters behind one slow scan get 60s minus their wait; the third may get milliseconds and fail with a context deadline that looks like a Mongo fault.
- `high` — `sync.Mutex.Lock` is not context-aware, so a `HandleRefresh` request blocks on `writeMu` past its own HTTP deadline; `cfg.HTTP.Middleware()` sets a request deadline (`main.go:116`) that this wait cannot observe — `tcard-service/cache.go:98`, `handler.go:58`
  Combined with the missing authorization on that route (D2), N concurrent `POST /refresh` calls serialize into N sequential full-collection scans while pinning N goroutines and N request contexts. Each scan is unbounded — `Find(ctx, bson.D{})` with no limit or batch bound (`store_mongo.go:43`).
- `medium` — `POST /validate` reads an unbounded request body: `ShouldBindJSON` with no `http.MaxBytesReader` — `tcard-service/handler.go:148`
  Every other body-accepting service in the repo caps it (`media-service/upload.go:58`, `botplatform-service/bot_handlers.go:193`, `client-update-service/routes.go:27`).
- `low` — template and listing responses are not compressed; `ginutil.Gzip` exists and is used by `user-service/routes.go:26`, but is not installed here — `tcard-service/main.go:110-117`
  Adaptive Card templates are highly compressible JSON served on every client start.
- `low` — the projection is exclusion-based (`_id: 0, migratedAt: 0`) rather than the "select only the fields the caller needs" form CLAUDE.md §6 mandates — `tcard-service/store_mongo.go:40-42`
  The inline comment justifies it correctly (templates are schemaless, so the wanted fields cannot be enumerated) — recorded as a documented exception, not a defect.

Confirmed sound, no finding: reads are lock-free and allocation-free on the hot path (`cache.go:62-69`, `handler.go:100` writes the cached bytes directly); `List` is O(entries) per call but on a bounded catalog and documented as such (`cache.go:159-160`); the refresh goroutine has an explicit termination path via `refreshCancel`/`refreshWG.Wait` (`main.go:96-101`, `main.go:140-142`); no `time.Sleep` for synchronization anywhere; no `$lookup`; no N+1 (one full scan per day, not per request); no JetStream, so the `jsretry`/`BackOff`/`MaxAckPending` rules do not apply.

### Recommendations
- `high` — Move `context.WithTimeout` inside the critical section in `Load`, after `writeMu.Lock()`, so each scan gets its full budget.
- `high` — Make `HandleRefresh` non-blocking under contention: use a `TryLock` (or a single-flight/`chan struct{}` admission gate) and return `errcode.TooManyRequests` when a load is already in flight, instead of queueing requests on a non-cancellable mutex.
- `medium` — Wrap the `/validate` body in `http.MaxBytesReader` with an explicit `TCARD_MAX_BODY_BYTES`, matching `media-service`/`client-update-service`.
- `low` — Install `ginutil.Gzip` on the `/api/v1/cards` routes.
- `low` — Add a batch-size or streaming bound to `ListCards` so catalog growth degrades gradually rather than by cursor timeout.

---

## 8. Prioritized action list

| # | Sev | Action | Dim | Evidence | Why |
|---|-----|--------|-----|----------|-----|
| 1 | `high` | **Put `/refresh` and `/validate` behind an authorization guard** | Architecture | `routes.go:8-10`; no auth middleware installed at `main.go:110-117`; the pattern at `admin-service/routes.go:17` | `refresh` is an **unauthenticated trigger for an unbounded full-collection Mongo scan**, and it is the input half of the amplification in item 2. If an ingress ACL fronts these, say so in the service doc — today nothing in the repo records it. |
| 2 | `high` | **Move `context.WithTimeout` inside `Load`'s critical section, and make `HandleRefresh` non-blocking under contention** | Performance | budget started before the lock `cache.go:96-99`; non-context-aware `writeMu.Lock` `:98`; request deadline set at `main.go:116` | Two waiters behind one slow scan get 60s **minus their wait**; the third may reach `ListCards` with milliseconds and fail with a deadline that looks like a Mongo fault. And `sync.Mutex.Lock` cannot observe the request context, so a queued `HandleRefresh` blocks past its own HTTP deadline. Use `TryLock` (or a single-flight admission gate) and return `errcode.TooManyRequests` when a load is already in flight. |
| 3 | `high` | **Close the four named uncovered branches** | Test coverage | 69.7% (223/320); `cache.go:132-134`; `handler.go:180-185`; `semver.go:23-26`; `cache.go:206-208` | ~8 statements takes the non-`main.go` files to ~100%. Each one matters: the cancel-during-`Load` return is **what makes `refreshWG.Wait()` terminate instead of looping another 30s**; three `validateCard` required-field branches are untested although the table reads as complete; `parseSemver`'s overflow branch is reachable because `allDigits` admits a 20-digit string; and the mixed semver/non-semver sort — the case its comment promises a total order for — is untested. |
| 4 | `medium` | **Reconcile the two coverage gates** | Test coverage | `deploy/azure-pipelines.yml:26-28`, `:49-55` excludes `main.go` and `store_mongo.go` before measuring | **Two gates disagree by 27 points on the same code**, and both claim an 80% threshold. Either merge the integration profile into the repo-wide number or apply the exclusion repo-wide — but not both. |
| 5 | `medium` | **Decide the ownership question for `/validate`** | Architecture | advisory check at `handler.go:163-168`; the real guard is the unique index at `store_mongo.go:26-29`; a removed `POST /register` is still referenced at `handler_test.go:491` | The endpoint checks "highest version" against an in-memory snapshot and **writes nothing**, so two authors validating the same version concurrently both pass. Either give the service the write path so validate-and-insert is atomic against the unique index, or demote it to a lint endpoint and **say so in the response and the docs**. |
| 6 | `medium` | **Add `pkg/errcode/codes_tcard.go` and attach reasons** | Code quality | eleven `BadRequest` plus one `Conflict` with no `WithReason` at `handler.go:144-169`, `:174-215` | A card-authoring client must branch on **prose strings** to tell "path must have exactly 3 segments" from "version must be 1.5". CLAUDE.md §3 reserves `WithReason` for exactly this. |
| 7 | `medium` | **Document the three HTTP endpoints** | Integration | `routes.go:8-12`; the only `docs/` mentions are implementation plans | Not a CLAUDE.md violation — the mandate covers `chat.user.…` subjects and `auth-service` routes — but the response shapes and the 400/404/409/503 matrix are **a real cross-team contract with no written form.** Include that `/validate` is advisory (item 5) so callers do not treat a 200 as a write lock. |
| 8 | `medium` | **Cap the `/validate` request body** | Performance | `ShouldBindJSON` with no `http.MaxBytesReader` at `handler.go:148` | **Every other body-accepting service in the repo caps it** — `media-service/upload.go:58`, `botplatform-service/bot_handlers.go:193`, `client-update-service/routes.go:27`. |
| 9 | `medium` | **Hoist the path rule into one predicate** | Maintainability | three-segment rule `handler.go:190-193` vs "generic over depth" at `cache.go:151`, `:184-192` | The write and read contracts disagree on path shape: **a two-segment card can be stored, cached, listed and served, but can never be validated.** Adding a depth rule later means touching both, and nothing links them. |
| 10 | `medium` | **Extract `run()`'s wiring into a testable `newServer`** | Test coverage / Maint | `main.go:53-156`, 0/66 statements | 66 structurally untestable statements. This is the same refactor that unblocks the coverage reconciliation in item 4. |

**Also worth doing.** Switch `main.go:150` to `errors.Is(err, http.ErrServerClosed)`, matching `media-service`/`admin-service`/`botplatform-service`. Keep `_id` in the projection so the skip warning at `store_mongo.go:60` can name the broken document — today an operator gets "a card doc is broken" with nothing to grep on. Either delete `card`'s dead `json`/`bson` tags or add a round-trip test that makes them load-bearing. Share the storage-only key list between the projection and `docToCard` so the two lists cannot drift by hand. Extract the sort comparator from the 60-line `cardCache.List`, which is the only function in the service needing a second read. Install `ginutil.Gzip` on the card routes — Adaptive Card templates are highly compressible JSON served on every client start. Add a batch or streaming bound to `ListCards` so catalog growth degrades gradually rather than by cursor timeout. And make `deepCard` a function rather than a package-level `var`: its `json.RawMessage` is a shared mutable slice across tests.
