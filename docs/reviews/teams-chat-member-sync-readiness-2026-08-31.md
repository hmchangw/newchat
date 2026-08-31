# teams-chat-member-sync — Production Readiness Review

**Service:** `teams-chat-member-sync` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

Idiomatic and carefully built — consumer-defined interfaces, constructor DI, `Now` injected for testability, a genuine `-race` exercise on the user cache, batched user resolution memoised run-wide, and correct optimistic-concurrency handling against `teams-chat-sync`'s `updatedAt` watermark. Coverage reads 60.3% only because the unit profile cannot see the integration tests that do cover the store; every business-logic function is at 100%.

The integration dimension is where this service is genuinely weak, and the reason is what sits downstream. `room-worker` treats the `members` list this job writes as **authoritative and deletes every subscription not in it**. Two unguarded paths feed it: **an empty Graph roster is written verbatim and advances the chat**, so one degraded response silently empties a room; and **a member missing from `teams_user` is persisted with an empty `Account`** and the chat is marked done, so that person is permanently omitted with no log, no counter and no retry. Reading `teams_user` through a secondary-preferred client makes the second case more likely, on collections a sibling deliberately keeps on the primary for exactly this reason.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 2 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 4 | 6 | 12 | 2 | **24** |
