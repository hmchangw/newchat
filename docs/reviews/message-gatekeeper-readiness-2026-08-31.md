# message-gatekeeper — Production Readiness Review

**Service:** `message-gatekeeper` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

The first hop on every user message, and the hot path is genuinely well-engineered: precomputed metric attribute sets, L1+L2 caches with singleflight, precise Mongo projections, correct semaphore consumer pattern, sonic + `Pretouch`, clean `jsretry` discipline. Excluding `main.go` the package is ~91% covered. Three things stand out. **The consumer binds MESSAGES with no `FilterSubjects`, so every verb under `msg.>` is processed as a create** — a client publishing `msg.edit` today is validated as a send and republished to the canonical `.created` subject. **The parent-resolution path has no overall deadline** — a reply that quotes a different parent can hold a worker slot for ~6 s. And the derived client-API view **contradicts the canonical doc** on bot-DM fan-out.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 8 | 20 | 15 | 9 | **52** |

