# translation-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `b5c63f2` (base `main`)  
**Overall score:** 3.7 / 5 (baseline 2026-09-01: 3.7, Δ -0.0)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

translation-service is the only service in the fleet at or above the 80% coverage floor, and its suite is genuinely good: 97% on everything outside `main`, real error and edge paths, a 5XX classified before the body is parsed so an error response carrying stream data cannot leak a translation, and a mid-stream transport drop driven through a hijacked connection. The service is small, readable and cleanly seamed. Its problems are all about what happens when the upstream provider misbehaves. It is the only request/reply service in the repository with an admission cap and no per-request deadline — the shared router package documents the guarded constructor as existing precisely so a service cannot apply half the protection, and this is the half it skips. Worst case a handler holds its slot for about seventy seconds, which exceeds the shutdown budget, so in-flight requests survive graceful shutdown and are killed at the grace period. An access-token outage is worse: the token fetch runs under a plain mutex that is not context-aware and caches nothing on failure, so a hundred admitted handlers queue behind it one at a time and one slow dependency becomes a full service outage that drains for minutes. Both outbound clients keep the standard library's default of two idle connections per host against a hundred-way concurrency cap, so nearly every call pays a fresh handshake, and the stream reader has no size limit at all — a runaway upstream response is buffered whole. Upstream auth rejection is detected by a free-text substring rather than the HTTP status, so a differently-worded 401 never triggers the refresh-and-retry and surfaces as an internal error. Finally, the language vocabulary lives in this service while user-service validates only tag shape, so a user can persist a language that makes every subsequent translate call fail.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 4 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 0 critical, 7 high, 15 medium, 15 low, 8 nitpick (45 total).  
**Highest-risk dimension:** Performance (3).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

