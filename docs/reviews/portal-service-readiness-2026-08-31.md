# portal-service — Production Readiness Review

**Service:** `portal-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

A small Gin service with a genuinely well-engineered read path — a lock-free `atomic.Pointer` directory snapshot with immutable maps, precise projections on both queries, and the fleet's one `$lookup` that carries the CLAUDE.md-required justification comment with a correlated `$expr` on an indexed field. Comment discipline is exemplary; the atomic-swap rationale is explained where a reader needs it.

The service has **no NATS surface at all**, so its integration risk is entirely the client-facing HTTP contract — and `docs/client-api.md` has drifted from the handler in four places, including a **documented `site_unknown` reason the code never emits** and an `upstream_unavailable` attributed to an endpoint that makes no outbound call. Coverage is 58.6%, under the critical line, and the uncovered branch in `resolve` is exactly the dev-fallback bug: a fallback site absent from the registry returns **200 with an empty `baseUrl`** — a syntactically valid response the client cannot act on. Rounding it out, the botplatform forwarder and the unauthenticated login endpoint both lack the connection-pool and load-shedding tuning their siblings already apply.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 2 | 10 | 14 | 2 | **29** |
