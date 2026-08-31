# user-service — Production Readiness Review

**Service:** `user-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

The best-behaved service in the fleet on code discipline: **zero Section-3 violations found** across ~18.8k lines, no token/password/body ever logged, `errors.Is` throughout, every `fmt.Errorf` wrapping with context. The sub-package decomposition is coherent and the `subscription.list` hot path is genuinely well engineered (TTL'd sort-key LRU, page-sized batch refetches, bounded per-site fan-out, precise projections). Three things hold it back. The **cross-site fan-out is synchronous, sequential and best-effort inside the request handler** — a down peer permanently loses that site's replica of settings that a remote `notification-worker` reads to decide whether to push. The **per-site fan-out pattern is hand-copied five times and has already drifted** — one copy lost its semaphore. And **coverage is 53.2%**, with all six client-facing chatlist RPCs handler-untested.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 8 | 18 | 12 | 5 | **44** |

> **Audit-coverage caveat.** `gosec` and the 18 repo-owned `semgrep` rules are clean repo-wide. `govulncheck` and the semgrep registry packs could not run (egress blocks `vuln.go.dev` / `semgrep.dev`); dependency-CVE coverage is unverified.

