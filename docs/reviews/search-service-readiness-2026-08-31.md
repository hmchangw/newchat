# search-service — Production Readiness Review

**Service:** `search-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

Small files, clean layering, excellent WHY-comments, and **exemplary client-API documentation** — all five RPCs registered via `pkg/subject` builders, documented in the canonical doc *and* both derived views with no field drift. The problems are about **what ships as production while still being a prototype**, and about **unbounded client-driven cost**. `search.apps` threads an `account` through three layers to a `_ = account // referenced in the access-guard $lookup once implemented`; `search.users` is a live registered route whose upstream URL, request body and auth scheme are all unresolved TODOs. And the **messages index read pattern is a hardcoded literal** while the writer's index name is fully operator-configured — a prefix mismatch returns zero hits, silently.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 12 | 20 | 12 | 7 | **51** |

