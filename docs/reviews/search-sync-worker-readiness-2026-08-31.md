# search-sync-worker — Production Readiness Review

**Service:** `search-sync-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

Thoughtful abstractions (`Collection`, `msgFetcher`, `flushPipeline`) and a genuinely well-engineered bulk pipeline — slot-based backpressure, dual size/interval flush triggers, precomputed metric attributes, clean `jsretry` discipline. Three things are seriously wrong. **A `critical`: the bot-message collection binds a stream whose subjects its own consumer filter cannot match**, so in `MODE=default` the consumer is rejected and the pod exits 1 — and a unit test enshrines the wrong filter. **The shipped default tuning trips its own coupling check** (`BULK_BATCH_SIZE × PIPELINE_DEPTH` exceeds the default `MaxAckPending`), and the check only warns. And **no ES request on the flush path carries a deadline**, so a hung connection holds a pipeline slot forever and blocks shutdown.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 2 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 12 | 18 | 13 | 5 | **49** |

