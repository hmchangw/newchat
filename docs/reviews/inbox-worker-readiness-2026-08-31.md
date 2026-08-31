# inbox-worker — Production Readiness Review

**Service:** `inbox-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

**The lowest-scoring service in the audit — and the most consequential position in the fleet**, since it is the destination side of *all* federation and the sole owner of the INBOX stream. It gets ownership exactly right: `bootstrap.go` sets only `Name + Subjects`, contains no gateway config, fail-fast-verifies in production, and every event type any producer emits is dispatched here. But the ordering guarantee the origin pays for is **thrown away at the destination**: `room_renamed` rides the origin's FIFO lane yet is routed to the concurrent fan-out pool here, so a rename can be applied before the subscription it renames exists — permanently. `subscription_opened` is applied with **no high-water-mark guard** despite its concurrent lane being justified on the claim that one exists. Coverage is **44.1%**, the worst in the fleet, with the entire store and all of `main()` at 0%.

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
| Count | 1 | 13 | 20 | 12 | 6 | **52** |

