# botplatform-service — Production Readiness Review

**Service:** `botplatform-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

Genuinely well-written Go with narrow consumer-side interfaces, precise Mongo projections, a shared breaker, an L2 session tier, no goroutine leaks and no N+1 — and comments that say WHY. But it is also the service where the most CLAUDE.md rules are breached at once, and where three unbounded-work paths meet on an unauthenticated endpoint.

**The bot rate limiter runs *after* the auth middleware**, so an invalid token is never rate-limited — and because the session cache is positive-only by design, every bogus-token request is one uncapped MongoDB `FindOne`. Token spraying is an unmetered load generator against the same Mongo the breaker exists to protect. Alongside it, **`/api/v1/login` is unauthenticated, has no rate limiter, and runs a full bcrypt verify** (~50–100 ms CPU) per request, and **the idempotency middleware buffers the entire request body with an uncapped `io.ReadAll`.**

Structurally: three of the handler's five dependencies are **poked in after construction**, so every bot route nil-derefs if a wiring line is dropped; `BcryptCost` is parsed and range-validated but **never used by anything**; and the 15 s room-management budget is **unreachable** — the request deadline is 10 s and cuts it first. Coverage is 56.5%, below the critical line, with both federation forwarders and the cross-site routing decision at zero.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 3 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 14 | 18 | 20 | 8 | **61** |
