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

**58.6% (239 statements)** — below the 60% line this audit scores as `critical` (CLAUDE.md §4 requires 80%), so scored 1, even though the tested code is tested *well*.

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

---

## 5. Maintainability — 4 / 5

Small, well-factored, and unusually well-commented (WHY not WHAT); the one structural smell is startup-config parsing living in `handler.go`.

### Findings
- `medium` — `handler.go` (363 lines) hosts two functions that are pure startup config and are never called by any handler: `parseSiteURLs` (`portal-service/handler.go:34`) and `parseOTELBaseURL` (`:62`), both invoked only from `run()` (`main.go:92`, `:97`). Their tests are correspondingly the tail of `handler_test.go` (`:511`, `:671`, `:698`), which dilutes the handler test file.
- `low` — `run()` is 117 lines doing config, obs, Mongo, cache, HTTP, and shutdown (`portal-service/main.go:80-197`). Near the limit for one function; the obs+Mongo+store block (`:106-125`) is the natural extraction.
- `low` — duplicated site lookup (see D2) is the one piece of copy-paste logic.
- `nitpick` — `cacheRetryInterval` is a hardcoded const (`main.go:27`) while its sibling `CacheRefreshInterval` is env-driven (`main.go:54`); the retry cadence is the one you actually want to tune during a Mongo incident.
- No dead code, no leaky abstractions, no `utils`-style packages. Comment discipline is exemplary — `cache.go:14-22` and `store_mongo.go:39-41` explain the atomic-swap rationale and the users-primary join, not the syntax.

### Recommendations
- `medium` — move `siteURL`, `parseSiteURLs`, `settingsResponse`, and `parseOTELBaseURL` into a new `config.go` with a matching `config_test.go`; `handler.go` drops to ~250 lines of request handling only.
- `low` — extract `func connectStore(ctx, cfg, sdk) (*mongoDirectoryStore, *mongo.Client, error)` from `run()`.
- `low` — promote `cacheRetryInterval` to `PORTAL_CACHE_RETRY_INTERVAL` with `envDefault:"30s"`.

---

---

## 6. Integration — 3 / 5

No NATS surface at all, so the integration risk is entirely the client-facing HTTP contract — and `docs/client-api.md` has drifted from the handler in four places.

### Findings
- `high` — documented reason never emitted: `docs/client-api.md:414` specifies `500 internal / reason site_unknown` for `POST /api/v1/login`, but `portal-service/handler.go:299` returns a raw error, so the envelope has no `reason`. A frontend branching on it silently falls through to the generic case.
- `medium` — duplicate section numbering: `### 2.5` appears twice, at `docs/client-api.md:356` (POST /api/v1/login) and `docs/client-api.md:809` (GET /api/settings). The table of contents (`docs/client-api.md:46`) lists only the settings one, so the login endpoint has no TOC entry, and the derived view links to colliding anchors — `docs/client-api/request-reply.md:86` and `:254`.
- `medium` — the reason index misattributes an error: `docs/client-api.md:6917` says `upstream_unavailable` comes from portal-service `GET /api/userInfo` "(cannot reach home-site botplatform)". `HandleUserInfo`/`resolve` make no outbound call at all (`portal-service/handler.go:156-221`); the emitter is `POST /api/v1/login` (`handler.go:320`, `:304`).
- `low` — the same index (`docs/client-api.md:6920`) lists `missing_fields` for portal's `GET /api/userInfo` only, but `POST /api/v1/login` also emits it (`portal-service/handler.go:264-266`); §2.5's own table has the row, the index does not.
- `low` — `/healthz` and `/readyz` (`portal-service/routes.go:9-10`) are undocumented; defensible for infra probes, but worth one line since chat-frontend deployments script against them.
- Positives: account validation reuses `subject.IsValidAccountToken` rather than an ad-hoc regex (`handler.go:171`), and the request ID is propagated to botplatform via `natsutil.RequestIDHeader` (`handler.go:314`), asserted in tests. No `pkg/subject` string-building, no OUTBOX/INBOX, no `pkg/idgen` usage — none apply here.

### Recommendations
- `high` — pick one: emit `errcode.BotplatformSiteUnknown` at `handler.go:299`, or delete the `site_unknown` row at `docs/client-api.md:414`. Emitting it is the better fix — the reason already exists and admin/bot clients need to distinguish misconfiguration from a real 500.
- `medium` — renumber the login section (`docs/client-api.md:356`) to a unique `§2.6` (or renumber settings), add the missing TOC entry, and update the two anchors in `docs/client-api/request-reply.md:86,254` in the same PR — CLAUDE.md requires the derived views not to drift.
- `medium` — fix the `upstream_unavailable` attribution at `docs/client-api.md:6917` to name `POST /api/v1/login`, and add `POST /api/v1/login` to the `missing_fields` row at `:6920`.
- `low` — document `/healthz` and `/readyz`, noting `/readyz` returns 503 until the first successful directory load (`handler.go:357-363`).

---

---

## 7. Performance — 3 / 5

The read path is genuinely well-engineered (lock-free snapshot, precise projections), but the outbound login forwarder and the public login endpoint both lack the connection-pool and load-shedding tuning the sibling services already apply.

### Findings
- `medium` — the botplatform forwarder is built without `restyutil.WithMaxIdleConns` — `portal-service/main.go:139`. The stdlib default keeps 2 idle conns per host (`pkg/restyutil/restyutil.go:37-39`), so a third concurrent login pays a fresh TCP+TLS handshake against a single-host upstream. `media-service/main.go:112` and `upload-service/main.go:191` pass `WithMaxIdleConns(32)` on the *same* botplatform client.
- `medium` — the unauthenticated `POST /api/v1/login` (`portal-service/routes.go:7`) has no concurrency shedding. `ginutil.MaxConcurrency` exists and is wired in `user-service/routes.go:25`; without it every in-flight login pins a goroutine plus an upstream socket for up to the 5s resty timeout, with no 429 backpressure.
- `medium` — the directory refresh materializes the whole `users` collection in one slice via `cur.All` (`portal-service/store_mongo.go:84`) under a 1-minute cap (`cache.go:12`), then builds a second full map (`cache.go:71-80`) — peak memory is ~2× the directory, unbounded in the number of provisioned accounts, with no batching or paging.
- `low` — portal queries `users` by `account` (`store_mongo.go:102`) but its own `EnsureIndexes` only indexes `hr_employee` (`store_mongo.go:29-37`). The supporting unique `users.account` index is owned by `user-service` (`user-service/mongorepo/users.go:41-44`) — an undocumented startup-order dependency; in a site where user-service has not run, the login fallback is a collection scan.
- `low` — `WriteTimeout: 10s` (`portal-service/main.go:165`) exactly equals `ginutil.TimeoutConfig`'s default `REQUEST_TIMEOUT` of 10s (`pkg/ginutil/timeoutconfig.go:18`): the per-request middleware has zero headroom to write its timeout response, and raising `REQUEST_TIMEOUT` above 10s has no effect.
- `low` — 401 timing side channel: unknown and non-bot accounts are rejected locally with no upstream round trip (`handler.go:283-288`), while a known bot with a wrong password waits on botplatform (`handler.go:312`). Bodies are byte-identical as `docs/client-api.md:412` claims, but latency distinguishes the arms.
- Positives verified: lock-free `atomic.Pointer` snapshot with immutable maps (`cache.go:23-46`); explicit precise projections on both queries (`store_mongo.go:70-77`, `:95-100`); the `$lookup` carries the CLAUDE.md-required justification comment (`store_mongo.go:43-44`) and its correlated `$expr` equality is on the indexed `account`; `mongo.ErrNoDocuments` handled (`store_mongo.go:103`); no `time.Sleep` synchronization anywhere; the refresh goroutine terminates on cancel (`cache.go:99-104`); `secondaryPreferred` read preference offloads the primary (`main.go:62`).

### Recommendations
- `medium` — add `restyutil.WithMaxIdleConns(32)` at `portal-service/main.go:139`, matching media-service and upload-service.
- `medium` — mount `ginutil.MaxConcurrency` on `POST /api/v1/login` in `routes.go`, with the limit env-driven; it sheds with 429 + `Retry-After` rather than 5xx, which matters for mesh outlier ejection.
- `medium` — bound the directory load: set an aggregation batch size and stream into the new map with `cur.Next` instead of `cur.All` (`store_mongo.go:84`), so peak memory is one map, not two slices plus a map.
- `low` — either add the `users.account` index to portal's `EnsureIndexes` (idempotent, `mongoutil.EnsureIndexWithRepair` already handles the conflict case) or document the user-service dependency in `store_mongo.go`.
- `low` — raise `WriteTimeout` to ~15s (as `admin-service`/`media-service` do) so the request-timeout middleware can actually deliver its response.

---

## 8. Prioritized action list

| # | Sev | Action | Dim | Evidence | Why |
|---|-----|--------|-----|----------|-----|
| 1 | `critical` | **Close the coverage gap and add the missing integration test for `GetByAccount`** | Test coverage | 58.6% (239 stmts); `run` 0.0%; all four store funcs 0.0%; `integration_test.go` covers only `ListEmployees` (`:27`, `:106`) and `EnsureIndexes` (`:115`) | Under the the 60% line this audit scores as `critical` (CLAUDE.md §4 requires 80%). `handler.go`/`cache.go` are 92–100%, so the whole shortfall is `main.go` plus a store whose **login cache-miss fallback projection is never validated against real Mongo** — including whether `roles` decodes into `[]model.UserRole`. That projection deliberately omits `_id`/`employeeId`; nothing pins it. |
| 2 | `high` | **Emit `errcode.BotplatformSiteUnknown`, or delete the promise from the docs** | Quality / Integration | raw `fmt.Errorf` at `handler.go:193`, `:299`; the reason exists at `pkg/errcode/codes_botplatform.go:30`; promised at `docs/client-api.md:414` | `Classify` collapses a raw error to `internal` with an **empty reason**, so a frontend branching on `site_unknown` silently falls through to the generic case. Emitting it is the better fix — admin and bot clients need to distinguish misconfiguration from a real 500. Then assert the reason in `TestHandleLogin_500_SiteUnknown` (`handler_test.go:652`), which today checks only the status. |
| 3 | `medium` | **Fix the dev fallback's empty `baseUrl`** | Architecture / Tests | `handler.go:189-201`, used at `:207`, `:217`; the uncovered branch at `:197-201` | When the fallback site is absent from `PORTAL_SITE_URLS`, only `NATSURL` is patched — `BaseURL` stays `""` and the client gets **a 200 it cannot act on.** `testSites` contains `site-local`, which is why no test catches it. Add the subtest first; it will fail today, which is the point. |
| 4 | `medium` | **Renumber the duplicated §2.5 and fix the two derived-view anchors** | Integration | `docs/client-api.md:356` and `:809` are both `### 2.5`; TOC at `:46` lists only the settings one; colliding anchors at `docs/client-api/request-reply.md:86`, `:254` | **The login endpoint has no TOC entry**, and the derived view links to colliding anchors. CLAUDE.md requires the derived views not to drift from the canonical doc — this is drift the canonical doc itself caused. |
| 5 | `medium` | **Fix the reason-index attributions** | Integration | `docs/client-api.md:6917` attributes `upstream_unavailable` to `GET /api/userInfo`; `:6920` omits `POST /api/v1/login` from `missing_fields` | `HandleUserInfo`/`resolve` **make no outbound call at all** (`handler.go:156-221`); the emitter is `POST /api/v1/login` (`handler.go:304`, `:320`). And the login endpoint does emit `missing_fields` (`handler.go:264-266`) — §2.5's own table has the row, the index does not. |
| 6 | `medium` | **Add `restyutil.WithMaxIdleConns(32)` to the botplatform forwarder** | Performance | `main.go:139`; the cost documented at `pkg/restyutil/restyutil.go:37-39`; siblings do it at `media-service/main.go:112`, `upload-service/main.go:191` | The stdlib default keeps **2 idle connections per host**, so a third concurrent login pays a fresh TCP+TLS handshake against a single-host upstream. The same client, correctly tuned, exists in two sibling services. |
| 7 | `medium` | **Mount `ginutil.MaxConcurrency` on the unauthenticated login route** | Performance | `routes.go:7`; the pattern at `user-service/routes.go:25` | Every in-flight login pins a goroutine plus an upstream socket for up to the 5s resty timeout with **no backpressure**. `MaxConcurrency` sheds with 429 + `Retry-After` rather than 5xx, which matters for mesh outlier ejection. |
| 8 | `medium` | **Bound the directory load** | Performance | `cur.All` at `store_mongo.go:84`; second full map at `cache.go:71-80` | Peak memory is **~2× the directory**, unbounded in provisioned accounts, under a 1-minute cap. Set a batch size and stream into the new map with `cur.Next` so peak is one map, not two slices plus a map. |
| 9 | `medium` | **Move the startup-config parsing out of `handler.go`** | Maintainability | `parseSiteURLs` `handler.go:34`, `parseOTELBaseURL` `:62` — both called only from `run()` (`main.go:92`, `:97`) | Two functions in a 363-line `handler.go` that **no handler calls**, with their tests diluting `handler_test.go`. A `config.go`/`config_test.go` pair drops `handler.go` to ~250 lines of request handling only. |
| 10 | `medium` | **Add a `HandleLogin` subtest for the store-error branch** | Test coverage | `handler.go:277-278`; mocks cover only found/not-found at `handler_test.go:381`, `:405` | The warn-and-treat-as-miss path is `HandleLogin`'s 92.1% gap, and it is the branch that decides whether a Mongo blip logs someone out or 500s them. |

**Also worth doing.** Either add the `users.account` index to portal's own `EnsureIndexes` (idempotent via `mongoutil.EnsureIndexWithRepair`) or document the `user-service` startup-order dependency in `store_mongo.go` — the login fallback is a collection scan without it, and portal indexes only `hr_employee`. Raise `WriteTimeout` above 10s: it currently **exactly equals** `ginutil.TimeoutConfig`'s default `REQUEST_TIMEOUT`, so the per-request middleware has zero headroom to write its own timeout response and raising `REQUEST_TIMEOUT` has no effect. Drop the three log-and-return `slog.Warn` calls before `errhttp.Write`, or give them a distinct `login_outcome` key. Switch `main.go:191` to `errors.Is`, drop the `envDefault` on `MONGO_PASSWORD`, extract the duplicated site lookup into one `siteFor` helper, and rename `PortalHandler`/`NewPortalHandler` to the repo-standard `Handler`/`NewHandler`. Finally, note the 401 timing side channel: unknown accounts are rejected locally while a known bot with a wrong password waits on botplatform — bodies are byte-identical as documented, latency is not.
