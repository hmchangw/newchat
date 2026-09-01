# push-notification-service — Production Readiness Review

**Service:** `push-notification-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.7 / 5

The lowest-scoring service in the fleet on architecture, and the reason is not a defect in what it does — it is that **the production binary does not do the thing it is named for**. The only `Dispatcher` implementation is `LogDispatcher`, which logs and returns nil, so every push event is acked and dropped. There is no APNs or FCM call anywhere in the service. Whether that is a deliberate not-yet-wired stage or an oversight, it is what ships today, and nothing in the code or the deploy files says which.

Around that gap, three operational guards its eight sibling workers all have are missing: **no `jobguard` panic recovery** (an unrecovered panic kills the process and crash-loops on redelivery), **no failure signal from the consume loop** (any `iter.Next()` error returns silently while the health probe — which reports only NATS connectivity — stays green on a pod consuming nothing), and **a shutdown race** where the consume-loop goroutine is not counted in the WaitGroup, so `wg.Wait()` can observe zero in-flight and let `nc.Drain()` proceed while a handler is about to ack on a drained connection. The peer that documents this exact window is `notification-worker`, its own upstream.

Coverage is 26.9%, and there is no integration test at all.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 3 / 5 |
| 2 | Architecture | 2 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 6 | 9 | 9 | 2 | **27** |
