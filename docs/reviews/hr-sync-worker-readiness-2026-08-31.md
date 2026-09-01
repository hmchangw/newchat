# hr-sync-worker — Production Readiness Review

**Service:** `hr-sync-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

A small consumer with correct `errcode.Permanent`-vs-transient tiering, clean `jsretry` discipline, `jobguard` panic containment and good WHY-comments — but three structural problems, each of which is a single-point failure for the HR feed.

**A permanent store error wedges the site's lane forever.** Every store failure is classified transient, `MaxDeliver` is forced to `-1` and `MaxAckPending=1`, so a non-retryable Mongo error retries indefinitely while blocking every subsequent batch. It is reachable today: `portal-service` enforces a **unique `account` index** on `hr_employee` while this worker upserts keyed on `_id = employeeId`, so a rehire yields a permanent E11000 that never becomes permanent to the worker — and the only health check is NATS liveness, which stays green throughout. **A quit deletes across sites**: the batch carries `SiteID`, the handler discards it, and the delete filters on `account` alone. And **stream ownership is inverted** — this consumer creates the producer's stream, while a sibling consumer's code states the opposite ownership model outright.

Coverage is 21.1%, the lowest in the fleet: every store method and all bootstrap/consumer wiring is at 0%.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 6 | 12 | 8 | 1 | **28** |
