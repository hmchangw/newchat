# translation-service — Production Readiness Review

**Service:** `translation-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.7 / 5

**The only service in the 35-service fleet that clears the CLAUDE.md 80% coverage floor** — 82.3%, against 34 services below it and 19 below 60%. And the number is not vanity: the error taxonomy, transport drops and concurrency are genuinely exercised. Contract discipline is equally strong — subject builders throughout, `docs/client-api.md` and **both** derived views accurate and in sync, which is rarer in this repo than it should be. Seventeen small single-purpose files, WHY-shaped comments, correct `errcode` tiering, a lock-free token cache with single-flight refresh.

What holds it at 3.7 is that the outbound path has **no deadline and no connection pooling**, and the service ships **exactly half of the router's overload protection**. `pkg/natsrouter` provides `DefaultGuarded` specifically so a service cannot apply the admission cap without the companion timeout — and this service applies the cap alone. The consequence is concrete: a caller gives up in ~2s while a degraded upstream keeps all 100 admission slots occupied for ~35s doing work nobody will read, and every other caller gets "service busy".

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 4 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 3 | 5 | 13 | 3 | **24** |
