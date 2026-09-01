# teams-room-inspector — Production Readiness Review

**Service:** `teams-room-inspector` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

670 lines across nine files, every function short, file layout and DI exactly per CLAUDE.md, and comments that explain WHY — including the one at `pkg/model/teams.go:104-110` explaining why an exceeded batch cap looks like a healthy run rather than a failure. Two bounded queries, an explicit projection, no `$lookup`, secondary-preferred reads.

The service's exposure is that it is **the read side of a federated verification contract, with no deadline and no index guarantee**. `newServer` sets only `ReadTimeout`/`WriteTimeout`, which bound the socket and do **not** cancel the handler context — the repo's own helper says exactly this — so a stalled Mongo read pins the request goroutine and its pooled connection indefinitely, with no `MaxPoolSize` lever because `mongoutil.PoolConfig` is not mounted either. And the subscriptions aggregation depends on an index owned by a different service without calling `mongoutil.WarnMissingIndexes`, so a dropped index degrades a 500-id batch to a full collection scan **with no signal**.

Coverage is 47.7% — below the critical line, though concentrated in wiring rather than logic.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 1 | 7 | 10 | 2 | **21** |
