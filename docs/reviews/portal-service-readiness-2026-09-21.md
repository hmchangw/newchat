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

