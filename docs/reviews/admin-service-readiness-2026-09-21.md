# admin-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `8eaf4a7` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.2, Δ +0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

A well-structured admin HTTP surface — routes in `routes.go`, consumer-defined store, explicit projections on every read, no `$lookup`, streamed uploads with body caps, the documented HTTP shutdown order — with one high-severity security gap that two reviewers reached independently: password reset, self-service password change and account deactivation revoke sessions through a hand-rolled `DeleteMany` inside the transaction that returns no ids, so unlike the explicit revoke endpoints they never call `sessioncache.Bust*`, and a reset or deactivated user's token keeps authenticating from the consumer-side cache for the refresh window (~67 min at defaults, indefinitely during a Mongo outage). Two operational gaps follow it: `authenticate` collapses every `FindByHash` error to 401, so a Mongo outage logs the whole console out and pages as auth failures, and `user_account_updated`/`user_permissions_updated` publish straight to remote INBOX lanes with no OUTBOX retry, so a permission revoke that misses a down peer stays effective there until an operator runs resync. `SearchUsers` executes two unanchored case-insensitive regex scans over every site's users per console keystroke, with no sort, so pages repeat rows under HR-sync writes. Coverage is 68.2% — 93% outside the integration-only store — but five handlers lack the malformed-body test CLAUDE.md requires and nothing enforces the floor in any pipeline.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 3 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 4 |
| Performance | 4 |

**Findings by severity:** 0 critical, 4 high, 12 medium, 21 low, 9 nitpick (46 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

