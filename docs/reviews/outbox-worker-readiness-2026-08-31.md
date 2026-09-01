# outbox-worker — Production Readiness Review

**Service:** `outbox-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The sole owner of the OUTBOX stream, and it implements the federation topology CLAUDE.md specifies **exactly**: per-peer concurrent and FIFO lanes, `MaxDeliver=-1`, `MaxAckPending=1` on the ordered lane, `pkg/subject`/`pkg/stream`/`pkg/outbox` throughout, and a design that stays generic over event type — adding a new OUTBOX event type needs zero changes here. The handler is 100% covered and the integration suite genuinely exercises a peer outage.

Three findings matter, and two of them undo the isolation the design exists to provide. **A single shared worker pool serves every peer's concurrent lane**, so one unreachable peer's first-delivery burst can hold all 100 slots for the full 3s publish timeout each and throttle forwarding to healthy peers — the per-destination split holds at the JetStream ack-pending level but not at the pool level. **The ordered lane's in-flight work is outside the shutdown drain** — `cc.Stop()` discards buffered messages without waiting for the running callback, and `Closed()` is never awaited. And **an unset `ALL_SITE_IDS` makes the sole OUTBOX owner a silent no-op**: zero peers, zero consumers, one `slog.Warn`, a health check that stays green off the NATS probe — while producers keep filling the stream. The service's own compose default collapses to exactly that.

Coverage is 36.9%, and the uncovered mass is precisely the durable-retry wiring.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 8 | 16 | 13 | 7 | **45** |
