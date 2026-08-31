# history-service — Production Readiness Review

**Service:** `history-service`
**Date:** 2026-08-31
**Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents (code quality, architecture, test coverage, maintainability, integration, performance), each judging against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The largest service in the repo (~25.7k lines) and, on the evidence, one of the better-engineered ones: error wrapping, `errcode` tiering, projection discipline, goroutine termination and WHY-comment quality are all genuinely strong, and the bucket-walk read path is deliberately and thoughtfully bounded. Three things hold it back. First, **test coverage at 55.0% sits below the repo's 60% critical line**, with the entire store layer (`cassrepo` 32.1%, `mongorepo` 3.5%) and `cmd/` (11.3%) effectively unexercised outside integration builds. Second, a **real correctness defect in reaction mirroring**: reactions on a `TShow=true` thread reply never reach `messages_by_room`, so the channel timeline permanently loses them on reload. Third, the service is the repo's **only** user of a `cmd/` + `internal/` layout, which `CLAUDE.md` §1 forbids and which blocks reuse of genuinely reusable code. None of these is a shipping blocker on its own; the reaction-mirroring bug is the one that silently corrupts user-visible state.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 4 / 5 |

### Findings by severity

| Severity | Count |
|----------|-------|
| critical | 1 |
| high | 6 |
| medium | 17 |
| low | 16 |
| nitpick | 7 |
| **Total** | **47** |

> **Audit-coverage caveat.** `gosec` and the 18 repo-owned `semgrep` rules ran clean repo-wide (0 findings; rule fixtures 2/2). `govulncheck` and the `semgrep` registry packs could **not** run — `vuln.go.dev` and `semgrep.dev` are blocked by this environment's egress policy (403). Dependency-CVE coverage is therefore unverified and must be re-run on a network-permitted runner before shipping.

