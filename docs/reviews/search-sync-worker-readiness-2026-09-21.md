# search-sync-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `e73a723` (base `main`)  
**Overall score:** 3.0 / 5 (baseline 2026-09-01: 3.2, Δ -0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

search-sync-worker is a well-structured Elasticsearch indexer whose collection abstraction, bulk-error matrix and backoff discipline are all solid, but this audit found one silent production defect that outranks everything else: the `bot-message-sync` consumer's filter subject (`chat.msg.canonical.{site}.*`) never matches the BOT-MESSAGES-CANONICAL stream it binds (`chat.bot.canonical.{site}.>`), and nats-server v2.12.6 no longer rejects a non-subset filter, so the durable is created and delivers nothing — no bot message has ever been indexed, with no log or metric to show it. The fix exists on `origin/fix/search-sync-bot-filter` but is not on this branch, and because the durable's cursor already sits at stream end, correcting the filter in place will not backfill unless the durable is versioned or deleted. Three further gaps recur across dimensions: ES bulk/update-by-query calls have no deadline (a hung ES node wedges a whole collection and then the consumer loop), `main()` is a 319-line untestable monolith that holds most of the 12.6-point coverage shortfall, and the `Collection` interface carries no `context.Context`, so the thread-parent ES lookup and Teams identity Mongo lookup run outside the consumer span and outside shutdown cancellation. The Mongo `ResolveIdentities` resolver has no test at any tier, `ES error.reason` is logged verbatim (and for `mapper_parsing_exception` carries message content), and stream bootstrap is inlined in `main` instead of the repo's `bootstrap.go` helper.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 3 |
| Test coverage | 2 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 0 critical, 7 high, 16 medium, 19 low, 7 nitpick (49 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

