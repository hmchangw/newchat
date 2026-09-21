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
