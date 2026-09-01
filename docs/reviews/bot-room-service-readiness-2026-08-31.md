# bot-room-service — Production Readiness Review

**Service:** `bot-room-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

Boundaries are genuinely good — narrow consumer-defined `RoomStore`/`RoomKeyStore`, `pkg/subject` builders throughout, `pkg/outbox.Publish` for all federation, `pkg/shutdown.Wait` with correct ordering — and the remove/key-rotation test suite is the service's strongest work.

The problem is that it writes into collections four other services read, and **its subscription documents have a different shape from every other writer's**. It omits `joinedAt` and `roles`, which room-service's `member.list` projects and paginates on. And for channel members it sets `siteId` to the **member's** home site rather than the room's — while user-service's `subscription.list` groups rows by `sub.SiteID` to fetch room metadata *from that site*. The DM and owner paths get it right, which is what makes the channel path a bug rather than a convention.

Alongside that: **every membership RPC is a serial per-user N+1** with no batch cap; the room-key fan-out is O(room size) serial publishes on an unbounded roster load inside a 10 s deadline; and **both deferred safety nets run on the request context they were meant to survive** — the failure they exist for is precisely the one that exhausts the budget first. Coverage is 49.0%, with the entire Mongo layer and every DM error path at zero.

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
| Count | 1 | 12 | 20 | 16 | 7 | **56** |
