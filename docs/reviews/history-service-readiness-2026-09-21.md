# history-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `1dfa2f3` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.2, Δ +0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The read path is engineered with unusual care — every Mongo read projects, bucket walks are bounded and adaptive, errcode tiering is correct throughout, subjects come from `pkg/subject` and every publish stamps `Timestamp` — and four of six dimensions score 4. It still cannot merge under CLAUDE.md as it stands. Unit coverage is 55.1% because the entire store layer lives behind `//go:build integration` and CI never passes the tag, so 19 integration files (1,900 lines in `write_integration_test.go` alone) gate nothing; the same pipeline's Validate step runs `go build ./history-service/`, which has no Go files under the `cmd/`+`internal/` layout, so it either fails every run or is not gating at all. That layout is itself the only one in the tree and contradicts the CLAUDE.md sub-package exception that names this service as its exemplar. Two contract issues need a decision rather than a patch: `msg.thread` returns replies newest-first while both the canonical doc and the derived view promise oldest-first, and every post-commit canonical publish (edit, delete, pin, react) is fire-and-forget — a lost publish leaves fan-out, search and the preview silently stale with no OUTBOX-style retry. Maintainability is the soft spot: a 342-line hand-wired `main()`, a 9-method `RoomRepository` that leaks preview writes into every cache and breaker decorator, and config validation split across two packages.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 4 |
| Performance | 4 |

**Findings by severity:** 1 critical, 2 high, 17 medium, 26 low, 9 nitpick (55 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

