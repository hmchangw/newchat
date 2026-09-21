# portal-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `0c04cc0` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.2, Δ +0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

portal-service is a small, well-partitioned Gin service (1,973 lines including tests, no TODOs, textbook error wrapping, projected reads, a justified `$lookup`) whose defects cluster around the unauthenticated login path and index ownership. Four reviewers independently flagged that `HandleLogin` logs and then returns the same error at three sites, so every denied login and upstream outage emits two log lines. `/api/v1/login` has no throttling or admission cap anywhere in the chain, and every unknown username costs a live Mongo read (unknown accounts never enter the cache), so credential-stuffing traffic drives both password guessing and database load at will; that same "just-provisioned account" fallback reads from a `secondaryPreferred` client, so a lagging secondary turns a valid first login into a 401. The service, a read-only consumer, creates a unique index on `hr_employee` that the writer (hr-sync-worker) neither knows about nor handles, via a repair path that can drop the writer's own index. The client contract has drifted in three places: `docs/client-api.md` promises a `site_unknown` reason the code never emits (it returns a raw error that collapses to `internal`), `GET /api/settings` serves a `botLoginEnabled` field the frontend relies on but the docs omit, and the reason index/TOC drift flagged on 2026-08-31 is still unfixed. Startup `EnsureIndexes` runs with no deadline and blocks the listener, and the SSO `/api/userInfo` path gets no cache-miss fallback so a new user is `account_not_ready` for up to two hours while a new bot logs in immediately. Coverage is 58.6% only because `main.go` and `store_mongo.go` are 0%; the CI gate filters `main.go` out on a false "covered by integration tests" justification, and `GetByAccount`, the login fallback, has no test at any tier.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 1 critical, 3 high, 15 medium, 23 low, 9 nitpick (51 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [high] Log-AND-return before `errhttp.Write` in `HandleLogin` (three sites) — `portal-service/handler.go:284`, `:291`, `:319` — `errhttp.Write` runs `errcode.Classify`, which itself logs `request failed` with code/reason/cause (`pkg/errcode/classify.go:40`), and the ctx logger already carries `request_id` + `account` via `WithLogValues` (`handler.go:260`, `:268`). Every denied login therefore emits two lines, and an upstream outage emits a Warn plus an Error per request. CLAUDE.md §6 "Never log AND return". The `:319` line is the only one carrying information Classify lacks (the resty error), because the `Unavailable` at `:320` is built without `WithCause`.
- [medium] Non-200 upstream bodies are relayed verbatim as `application/json` with no classification and no log line — `portal-service/handler.go:324-328`. botplatform's own `HandleLogin` always writes errcode envelopes (`botplatform-service/handler.go:89ff`), but a 502/503/504 from an intermediary (ingress/proxy HTML) is forwarded byte-for-byte under a JSON content-type, and because `Classify` is bypassed a botplatform 5xx storm produces zero portal log lines.
- [low] Sentinel compared with `!=` instead of `errors.Is` — `portal-service/main.go:192` — CLAUDE.md §3 "use `errors.Is`"; works today only because `ListenAndServe` returns the bare sentinel. Five sibling Gin services (`admin-service/main.go:174`, `media-service/main.go:143`, …) already use `errors.Is`.
- [low] Request body validated by hand rather than Gin binding — `portal-service/handler.go:226-229`, `:263` — `loginRequest` has no `binding:"required"` tags; the bind error is swallowed and malformed JSON, a wrong content-type and a missing field all collapse into one `AuthMissingFields` response. CLAUDE.md §6 "Validate request bodies at handler level using Gin binding/validation".
- [low] `GetByAccount` — the only live-read path on the login role gate — has no integration test — `portal-service/store_mongo.go:94-110`; `integration_test.go` covers only `ListEmployees` and `EnsureIndexes` (0 references to `GetByAccount`). The projection that deliberately omits `employeeId` is therefore unverified against a real Mongo.
- [low] Three hop/bound timeouts are hardcoded beside an env-mounted one — `portal-service/main.go:140` (botplatform 5s), `:164-165` (server 10s/10s), `cache.go:12` (load 1m) — while `cfg.HTTP` (`REQUEST_TIMEOUT`, default 10s) is separately configurable, so raising `REQUEST_TIMEOUT` past 10s lets `WriteTimeout` cut the response first. Matches `auth-service/main.go:124-125`, so this is repo-wide drift, not a portal-only fault.
- [nitpick] `PortalHandler`/`NewPortalHandler`/`DirectoryStore` exported from `package main` — `portal-service/handler.go:104`, `:139`, `store.go:37` — CLAUDE.md §3 "keep handler/store implementations unexported within services"; the repo is inconsistent (botplatform uses unexported `handler`, most others `Handler`).
- [nitpick] Test naming deviates from `Test<Type>_<Method>` (`TestHandleLogin_*`, `handler_test.go:417`); `b, _ := io.ReadAll(r.Body)` in a test helper — `handler_test.go:339`; `defer srv.Close()` instead of `t.Cleanup` — `:377`, `:421`.

### Recommendations

- [high] Delete the three `slog.WarnContext` calls at `handler.go:284`, `:291`, `:319` and build the `:320` error as `errcode.Unavailable("upstream unavailable", errcode.WithReason(errcode.BotplatformUpstreamUnavailable), errcode.WithCause(err))` — Classify then emits exactly one correctly-levelled line that still carries the resty cause.
- [medium] On the non-200 relay path (`handler.go:324`) decode the body with `errcode.Parse`; if it is a valid envelope relay it, otherwise route through `errhttp.Write(ctx, c, errcode.Unavailable(..., errcode.WithCause(fmt.Errorf("upstream status %d", resp.StatusCode()))))` — clients always get JSON, and upstream 5xx become visible in portal logs.
- [low] `main.go:192` → `!errors.Is(err, http.ErrServerClosed)`.
- [low] Add `binding:"required"` to `loginRequest` (`handler.go:227-228`) and drop the manual emptiness check, keeping the `AuthMissingFields` reason on bind failure.
- [low] Add `TestMongoDirectoryStore_GetByAccount` (found, not-found, `employeeId` absent from projection) to `integration_test.go` — closes the last 0% store method and exercises the fallback the login gate depends on.
- [low] Mount the botplatform hop timeout as a config field (`PORTAL_BOTPLATFORM_TIMEOUT`, default 5s) or derive it from `cfg.HTTP.RequestTimeout` so the two bounds cannot be tuned apart — `main.go:140`.
- [nitpick] Add a repo semgrep rule flagging `slog.*Context(...)` immediately preceding `errhttp.Write`/`errnats.Reply` — `.semgrep/errcode.yml` currently has no log-AND-return rule, which is why finding 1 passed `make sast`.

## 3. Architecture — score 4

### Evidence

- [medium] A read-only consumer owns (and can drop/recreate) a unique index on a collection written by another service — `portal-service/store_mongo.go:29-37` — `EnsureIndexes` builds `{account:1, unique}` on `hr_employee` via `mongoutil.EnsureIndexWithRepair` (`pkg/mongoutil/indexes.go:144-174`), which on a spec conflict DROPs the existing index. The writer, `hr-sync-worker`, keys its bulk upsert by `_id = employeeId` (`hr-sync-worker/store.go:56-60`) and creates no indexes of its own, so a reader silently imposes a write-time constraint the writer neither knows about nor enforces; a duplicate-account HR row now fails the writer's batch instead of the reader's snapshot. Ownership belongs with the writer.
- [medium] Unauthenticated `/api/v1/login` is forwarded with no throttling or shedding anywhere in the chain, and a cache miss turns each attempt into a live Mongo read — `portal-service/handler.go:270-282`, `routes.go:7`. Portal is deliberately "portal-direct" (not behind the gateway, `docker-local/traefik/dynamic.yml:8-9`); botplatform registers login outside its rate-limited group (`botplatform-service/routes.go:12,17`); `ginutil.MaxConcurrency` exists but only `user-service` uses it. Every unknown username costs one `users.FindOne` (`store_mongo.go:102`), so an attacker drives Mongo load and password-guessing traffic at will.
- [medium] Login's `site_unknown` contract is documented but not implemented — `portal-service/handler.go:297-300` returns a raw `fmt.Errorf`, which `errcode.Classify` collapses to `internal` with an empty `reason` (`pkg/errcode/classify.go:22-23`). `docs/client-api.md:414` promises `500 internal site_unknown`, and `errcode.BotplatformSiteUnknown` exists for exactly this case (`pkg/errcode/codes_botplatform.go:29-30`). `TestHandleLogin_500_SiteUnknown` (`handler_test.go:652-659`) asserts only the status, so the drift is untested.
- [medium] Cache-consistency model is asymmetric: login gets a live-store fallback on a miss, `/api/userInfo` does not — `handler.go:177-184` vs `handler.go:270-282`. The directory is refreshed only every `PORTAL_CACHE_REFRESH_INTERVAL` (default 2h, `main.go:54`) and neither writer (`hr-sync-worker`, `teams-hr-sync`) publishes an invalidation signal, so a newly provisioned SSO user is `403 account_not_ready` for up to 2h while a newly provisioned bot can log in immediately. The `WithDirectoryStore` dependency is already injected; the SSO path simply does not use it.
- [low] An empty directory at startup is not retried at `cacheRetryInterval` — `cache.go:58-63`. `Load` only rejects an empty snapshot when already `Ready()`; a first load of zero rows stores an empty map, returns nil, and `RefreshLoop` (`cache.go:90-97`) then sleeps the full 2h with `/readyz` at 503. The 30s retry (`main.go:24-27`) was meant for exactly this window.
- [low] The cache's stated consistency premise is wrong — `cache.go:14-17` says `hr_employee` is "rewritten wholesale by a daily HR cron", justifying the "mid-rewrite empty snapshot" guard. The actual writer does per-row upserts plus targeted `DeleteMany` (`hr-sync-worker/store.go:58,112`), never a wholesale rewrite. The guard is harmless but the model behind it is stale, and the design comments will mislead the next change.
- [low] Role-aware bot branch in `resolve` is unreachable in production — `handler.go:204-212`. `subject.IsValidAccountToken` rejects dots (`pkg/subject/subject.go:65`) and every bot account carries `.bot` (`pkg/model/user.go:175-177`); the tests acknowledge this (`handler_test.go:268-272`). Dead code on a public route is a maintenance trap.
- [nitpick] Config-parsing helpers live in `handler.go` rather than `main.go` — `handler.go:34-48,62-81` (`parseSiteURLs`, `parseOTELBaseURL`); CLAUDE.md puts config parsing in `main.go`.
- [nitpick] `http.Server` timeouts are hardcoded and incomplete — `main.go:161-166` sets `ReadTimeout`/`WriteTimeout` only (no `IdleTimeout`), unrelated to the env-driven `cfg.HTTP.RequestTimeout`. Matches `auth-service/main.go:124-125`, so it is a repo-wide convention, not a portal defect.

Verified clean: consumer-defined `DirectoryStore` with mockgen (`store.go:9,37-45`); constructor DI with options (`handler.go:139-152`); routes in `routes.go`, `/healthz` + `/readyz`; client errors via `errhttp.Write`; `$lookup` justified (`store_mongo.go:43`); shutdown order via `shutdown.Wait` (`main.go:177-187`) correct for an HTTP service; `go vet`/`go build` clean.

### Recommendations

- [medium] Move the `hr_employee (account) unique` index to `hr-sync-worker`'s store (its `EnsureIndexes`) and drop `EnsureIndexes` from portal — `store_mongo.go:29-37`, `main.go:122-125` — the writer enforces its own invariants; a reader with `secondaryPreferred` should hold no DDL.
- [medium] Add `ginutil.MaxConcurrency` (and a per-IP or per-account limiter) in front of `/api/v1/login`, and bound the store-fallback path — `routes.go:7`, `handler.go:270-282` — so unauthenticated traffic cannot amplify into Mongo reads or unthrottled password guessing.
- [medium] Return `errcode.Internal("no URLs configured for site", errcode.WithReason(errcode.BotplatformSiteUnknown))` from both site-miss branches and assert the reason in the tests — `handler.go:193,299`, `handler_test.go:652` — aligns the wire contract with `docs/client-api.md:414`.
- [medium] Reuse the injected `DirectoryStore` for a `/api/userInfo` cache-miss fallback (same guard shape as login), or subscribe the cache to the HR feed stream `hr-sync-worker` consumes for targeted invalidation — `handler.go:177-184` — closes the 2h `account_not_ready` window for new SSO users.
- [low] Treat a zero-row first load as a failure so `RefreshLoop` retries at `cacheRetryInterval` — `cache.go:58-63`.
- [low] Rewrite the `directoryCache` doc comment to describe the real writer (per-row upsert + delete), and decide whether the empty-snapshot guard is still wanted — `cache.go:11-22,48-50`.
- [nitpick] Move `parseSiteURLs`/`parseOTELBaseURL` into `main.go` (or a `config.go`) and delete the unreachable bot branch in `resolve` — `handler.go:34-81,204-212`.
