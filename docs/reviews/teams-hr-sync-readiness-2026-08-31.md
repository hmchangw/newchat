# teams-hr-sync — Production Readiness Review

**Service:** `teams-hr-sync` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

The producer of the workforce feed the whole Teams pipeline depends on, and the service where documentation drift has become a correctness risk in its own right. The code itself is small and well-factored — a well-chosen `emitter` seam for its two modes, correct `pkg/subject` and `pkg/idgen` usage, a request ID minted and propagated onto outbound messages, and a hand-rolled shutdown that is *correct* (the drain deadline is deliberately detached from the cancelled context).

Three things stand out. **`README.md` — which explicitly presents itself as the contract for "an external persister" replacing this worker — is materially wrong in four places**, including a `pkg/hrstore` package that does not exist and a `source:"teams"` scoping that the query does not perform. **A partial publish loses the users half of the feed forever**: the employees upsert is published first and persisted downstream, so the next run's diff finds the rows equal and never re-emits the users — directly contradicting `main.go:38-39`'s claim that "a lost publish self-heals". And **the entire direct-write path is at 0% coverage**, including the two guards whose own comments describe data corruption.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 2 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 2 | 5 | 10 | 9 | 5 | **31** |
