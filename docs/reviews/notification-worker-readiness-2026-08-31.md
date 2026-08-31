# notification-worker — Production Readiness Review

**Service:** `notification-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The hot path is well-engineered — sonic with pretouch, narrowed-candidate batching, L1/L2 room caches, a bounded worker semaphore, precise Mongo projections, clean `jsretry` discipline. Three things pull the score down, and all three were found independently by more than one expert. **A shutdown-time send-on-closed-channel race can panic the process during a routine rolling restart**: the member-event invalidation goroutine is tracked by no `WaitGroup`, yet shutdown stops the iterator and closes the channel it sends into two steps later — a message already returned by `Next()` sends after the close, and a `select` with a `default` does not save it. **The dev bootstrap narrows a shared stream**: it creates `MESSAGES-CANONICAL-{site}` with only the `.created` leaf instead of the declared `.>` wildcard, silently constraining a stream four services consume. And **the mute gate fails open at remote sites** — the user-settings replica this worker reads has no creation or backfill path, so a muted user at a site added after their last settings write gets pushed anyway. Coverage at 59.0% is under the 60% critical line, driven by a 196-statement `main()` at 2.0%.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 2 | 12 | 18 | 13 | 9 | **54** |
