# bot-message-handler — Production Readiness Review

**Service:** `bot-message-handler` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

Textbook CLAUDE.md layout — consumer-defined store, constructor DI, `pkg/subject` builders, opt-in bootstrap, correct shutdown order — in 1080 readable lines. The gaps cluster in two places, and they compound.

**Half the client-facing surface is untested.** `handleSendDM` is at **0.0%** coverage: every DM-specific behaviour — the missing-`userID` branch, the `idgen.BuildDMRoomID` derivation, the DM-specific `Forbidden` reply — has no test, because all thirteen handler tests exercise `handleSendRoom`. There are no integration tests at all, so the whole Mongo store is 0% and the `ErrNotFound` translation **every handler branch keys on** is entirely unverified. `Register` is 0% too, so a copy-paste swap of the two route patterns would ship green.

**And the mention path is an N+1 on an unbounded scan, with nothing in front of Mongo.** `canonicalizeMentions` fetches every member of the room and then issues one `FindUser` per mention inside the loop — 11 round trips for a 10-mention message, on top of the 2 the handler already makes — while `ListMemberIDs` streams every subscription document of the room just to answer "is this one user a member". By explicit design there is no cache, and no breaker was mounted alongside that decision.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 2 | 4 | 9 | 15 | 2 | **32** |
