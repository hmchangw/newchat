# tcard-service — Production Readiness Review

**Service:** `tcard-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.5 / 5

Textbook conformance to the repo's Gin-service shape, and a read path that is genuinely excellent: a lock-free `atomic.Pointer` snapshot means a card fetch does zero Mongo reads, zero locks and zero re-marshalling — the cached bytes go straight to the wire.

The weakness is on the write and refresh side, and the two halves compound. **`POST /api/v1/cards/refresh` and `POST /api/v1/cards/validate` have no authentication or authorization** — `refresh` is an unauthenticated trigger for an unbounded full-collection scan. And **`Load` starts its 60-second budget before acquiring a non-context-aware mutex**, so N concurrent refresh calls serialize into N sequential full scans, each pinning a goroutine and a request context past its own HTTP deadline, with later waiters reaching Mongo with almost none of their budget left. Separately, `/validate` is advisory only: it checks "highest version" against an in-memory snapshot and then writes nothing, so two authors validating the same version concurrently both pass.

Coverage is 69.7%, and the service's own CI gate excludes two files before measuring — reporting ~97% and passing the same 80% threshold this audit fails.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 4 | 9 | 13 | 1 | **27** |
