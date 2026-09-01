# portal-service — Production Readiness Review

**Service:** `portal-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

A small Gin service with a genuinely well-engineered read path — a lock-free `atomic.Pointer` directory snapshot with immutable maps, precise projections on both queries, and the fleet's one `$lookup` that carries the CLAUDE.md-required justification comment with a correlated `$expr` on an indexed field. Comment discipline is exemplary; the atomic-swap rationale is explained where a reader needs it.

The service has **no NATS surface at all**, so its integration risk is entirely the client-facing HTTP contract — and `docs/client-api.md` has drifted from the handler in four places, including a **documented `site_unknown` reason the code never emits** and an `upstream_unavailable` attributed to an endpoint that makes no outbound call. Coverage is 58.6%, under the critical line, and the uncovered branch in `resolve` is exactly the dev-fallback bug: a fallback site absent from the registry returns **200 with an empty `baseUrl`** — a syntactically valid response the client cannot act on. Rounding it out, the botplatform forwarder and the unauthenticated login endpoint both lack the connection-pool and load-shedding tuning their siblings already apply.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 2 | 10 | 14 | 2 | **29** |

---

## 2. Go code quality — 4 / 5

Idiomatic, precisely commented, correct errcode tiering and slog discipline; one real reason-code gap and a handful of low-severity drifts from CLAUDE.md.

### Findings
- `medium` — a defined domain reason is never emitted: both site-registry failures return a raw `fmt.Errorf`, which `Classify` collapses to `internal` with an **empty** reason — `portal-service/handler.go:193`, `portal-service/handler.go:299`. `errcode.BotplatformSiteUnknown` ("site_unknown") already exists at `pkg/errcode/codes_botplatform.go:30` and `docs/client-api.md:414` promises it to the frontend.
- `low` — log-and-return: `slog.WarnContext` immediately before `errhttp.Write`, which runs `Classify` and logs again — `portal-service/handler.go:284`, `:291`, `:319`. CLAUDE.md §3 says never log AND return. It is a repo-wide login-audit convention (`admin-service/login.go:145`, `botplatform-service/handler.go:180`), but CLAUDE.md wins; if the audit line is wanted, it should carry a distinct `login_outcome` key and the errcode path should stay quiet.
- `low` — error compared with `!=` instead of `errors.Is` — `portal-service/main.go:191`. Siblings already use `errors.Is` (`media-service/main.go:143`, `admin-service/main.go:174`).
- `low` — a secret carries an `envDefault` — `MONGO_PASSWORD` at `portal-service/main.go:59`. CLAUDE.md: "never default secrets or connection strings".
- `nitpick` — stuttering names `PortalHandler` / `NewPortalHandler` / `PortalHandlerOption` in `package main` — `portal-service/handler.go:104`, `:121`, `:139`. Every other service uses `Handler`/`NewHandler` (e.g. `broadcast-worker/handler.go:79`).
- `low` — audit-coverage gap, not a defect: gosec + repo-owned semgrep clean repo-wide; `govulncheck` and semgrep registry packs unavailable (blocked egress).

### Recommendations
- `medium` — attach `errcode.WithReason(errcode.BotplatformSiteUnknown)` via `errcode.Internal(...)` at `handler.go:193`/`:299`, or delete the promise from `docs/client-api.md:414`; assert the reason in `TestHandleLogin_500_SiteUnknown` (`handler_test.go:652`), which currently checks only the status.
- `low` — drop the pre-`Write` `slog.Warn` calls (`handler.go:284`, `:291`, `:319`) and let `errhttp.Write` be the single log site.
- `low` — `errors.Is(err, http.ErrServerClosed)` at `main.go:191`; make `MONGO_PASSWORD` tag-free or `required` per deployment.
- `nitpick` — rename to `Handler`/`NewHandler`/`handlerOption` for repo consistency.

---

---

## 3. Architecture — 4 / 5

Textbook layering for a small Gin service — consumer-owned store interface, constructor DI with functional options, shared knobs mounted as named fields — marred by one dev-path correctness hole and a duplicated site lookup.

### Findings
- `medium` — dev fallback can return a 200 with an empty `baseUrl`: when `devFallback` is true and the fallback site is absent from `PORTAL_SITE_URLS`, `site` stays the zero value; only `NATSURL` is patched, `BaseURL` is left `""` — `portal-service/handler.go:189-201`, used at `:207` and `:217`. The client gets a syntactically valid response it cannot act on.
- `low` — `NewPortalHandler` takes six positional parameters, four of them bare strings/bools — `portal-service/handler.go:139`. Adding a seventh dev knob touches every call site; `devMode`/`devFallbackSiteID`/`devFallbackNatsURL` want a single `devConfig` struct.
- `low` — site-registry lookup plus its error path is written twice — `portal-service/handler.go:189-195` and `:297-301`.
- Positives verified: `DirectoryStore` defined in the consumer with exactly the two methods used (`store.go:37-45`); `mongoutil.PoolConfig` and `ginutil.TimeoutConfig` mounted as named fields, not re-declared (`main.go:64-65`); routes in `routes.go`; `pkg/shutdown.Wait` with the documented order (`main.go:177-187`); the refresh goroutine has an explicit cancel + `WaitGroup` (`main.go:130-135`, `:181-182`); no `os.Getenv`; no `bootstrap.go` needed since the service touches no stream.

### Recommendations
- `medium` — in `resolve`, treat "dev-fallback site not in the registry" explicitly: fill `BaseURL` from a `PORTAL_DEV_FALLBACK_BASE_URL` (or require the fallback site to be registered and fail at startup in `run()`), rather than emitting `""`.
- `low` — extract `func (h *PortalHandler) siteFor(e employee) (siteURL, error)` and call it from both `resolve` and `HandleLogin`.
- `low` — collapse the four dev/site parameters of `NewPortalHandler` into one options struct.

---

---

## 4. Test coverage — 1 / 5

**58.6% (239 statements)** — below the 60% CLAUDE.md floor, so scored 1, even though the tested code is tested *well*.

### Findings
- `critical` — 58.6% statement coverage, under the CLAUDE.md §4 60% line and far under the 80% floor (`coverage_by_service.txt`).
- Where the deficit is: `handler.go` and `cache.go` are 92–100% per function (`covfunc.txt`); the entire shortfall is `main.go:80 run` at 0.0% and all four `store_mongo.go` functions at 0.0% in the unit profile.
- `high` — `GetByAccount` (`portal-service/store_mongo.go:94`) has **no** integration test — `integration_test.go` covers only `ListEmployees` (`:27`, `:106`) and `EnsureIndexes` (`:115`). The login cache-miss fallback projection (`store_mongo.go:95-100`, which deliberately omits `_id`/`employeeId`) is never validated against real Mongo, including whether `roles` decodes into `[]model.UserRole`.
- `medium` — no test for `GetByAccount` returning an **error** (`handler.go:277-278`, the warn-and-treat-as-miss branch); `handler_test.go:381` and `:405` mock only the found/not-found arms. That is `HandleLogin`'s 92.1% gap.
- `medium` — `resolve`'s uncovered branch (95.7%) is exactly the empty-`baseUrl` bug at `handler.go:197-201`: `testSites` (`handler_test.go:71-75`) contains `site-local`, so the dev-fallback-site-unregistered path is never exercised.
- `low` — `RefreshLoop` 90.9%: the "context cancelled during a failed load" early return (`cache.go:92-94`) is untested.
- Quality is otherwise strong: table-driven subtests with descriptive names, `gomock` from `mock_store_test.go` (generated, matches `make generate`'s zero diff), no real DB/NATS in unit tests, per-test state via `cacheWith` (`handler_test.go:63`), a genuine race test (`cache_test.go:150`), and `TestMain(m) { testutil.RunTests(m) }` with `testutil.MongoDB` containers (`integration_test.go:18`, `:28`).

### Recommendations
- `high` — add `TestMongoDirectoryStore_GetByAccount` to `integration_test.go`: hit/miss/role-decode, and assert `employeeId`/`userId` come back empty (the projection excludes them) so the login path's contract is pinned.
- `medium` — add a `HandleLogin` subtest where the mock returns `(employee{}, false, errors.New(...))` and assert 401 plus no upstream call.
- `medium` — add a dev-mode subtest whose `devFallbackSiteID` is *absent* from the site registry; assert the response's `baseUrl` — it will fail today, which is the point.
- `low` — cover `cache.go:92-94` by cancelling ctx while `ListEmployees` is blocked.

---
