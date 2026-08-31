# room-service — Production Readiness Review

**Service:** `room-service`
**Date:** 2026-08-31
**Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents (code quality, architecture, test coverage, maintainability, integration, performance), each judging against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The most RPC-dense service in the fleet: 31 client-facing handlers across ~10 domains in a single 2,677-line `handler.go`. The engineering inside is good — error wrapping and `errcode` tiering are exemplary, the federation boundary holds cleanly through OUTBOX, all nine federated event types are correctly partitioned, and the Mongo layer shows deliberate performance work (precise projections, over-fetch+1 pagination, batched Go-side rollups replacing correlated `$lookup`s). What drags the score down is *shape and safety net*: a 47-method god store interface, a constructor that takes 13–14 positional args and is then finished by 11 post-construction field pokes, and **57.2% coverage** with `store_mongo.go` at 3.4%. The single most consequential defect is a **request-ID-derived OUTBOX dedup key that collides across the multi-row `moveChat` rebalance**, silently dropping all but one federated event.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

### Findings by severity

| Severity | Count |
|----------|-------|
| critical | 1 |
| high | 9 |
| medium | 21 |
| low | 14 |
| nitpick | 6 |
| **Total** | **51** |

> **Audit-coverage caveat.** `gosec` and the 18 repo-owned `semgrep` rules ran clean repo-wide. `govulncheck` and the `semgrep` registry packs could **not** run (egress policy blocks `vuln.go.dev` / `semgrep.dev`), so dependency-CVE coverage is unverified and must be re-run before shipping.

