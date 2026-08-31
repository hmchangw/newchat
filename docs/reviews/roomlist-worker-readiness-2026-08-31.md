# roomlist-worker — Production Readiness Review

**Service:** `roomlist-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.7 / 5

The strongest service in this audit round, and the only one where no expert found a `critical` and only one found a `high`. It is the worker CLAUDE.md's own architecture notes single out — the one that took the room/subscription writes off `broadcast-worker`'s fan-out path so no MongoDB failure can block delivery — and it honours that contract verifiably: `MaxDeliver=-1`, coalescing with `msgbucket.NewerRow` tie-breaks, bounded batches, back-pressure, a narrow sonic projection, and no read path at all. The tested logic is unusually good: the coalescer, the watermark filters and the settle semantics are each pinned by named regression tests.

What holds it at 3.7 is organisational rather than behavioural. **`main.go` has quietly absorbed five concerns** — the consume loop, the readiness state machine, the self-SIGTERM escalation, the flush-budget validator and the consumer config — four of which already have dedicated `*_test.go` files with **no matching source file**, while `handler.go` contains no handler. And the consumer runs a **third pattern CLAUDE.md does not sanction**: `cons.Messages()` (the high-throughput shape) driven by exactly one goroutine with no semaphore and no `MAX_WORKERS` knob. The reasoning behind that choice is sound and documented; the deviation from binding project law is not.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 1 | 13 | 17 | 11 | **42** |
