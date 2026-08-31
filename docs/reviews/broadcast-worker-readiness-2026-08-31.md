# broadcast-worker — Production Readiness Review

**Service:** `broadcast-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.5 / 5

**The highest-scoring service audited**, and deservedly: this is the user-visible fan-out hot path and it shows deliberate engineering — sonic with pretouch, prebuilt metric attribute sets, singleflight-guarded bounded caches, a coalescing preview writer with a body cap, correct `jsretry.LowLatencyBackoff` settling, and the documented one-MongoDB-write boundary held exactly. The architectural boundaries `CLAUDE.md` describes for this service are all real in the code. Three things keep it off a 4. The mention federation **derives its destination from event data with no check against the configured peer set**, so a stale site ID publishes into an OUTBOX subject no consumer filters on. **Connection strings default to localhost** instead of being `required` — this service is the fleet outlier. And there is an **undocumented third federation lane** (fire-and-forget core NATS) that CLAUDE.md's two-lane model does not describe and that silently dies if ops never exports the subject.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 6 | 19 | 17 | 8 | **50** |

