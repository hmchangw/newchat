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

---

## 2. Go code quality — 4 / 5

Idiomatic, well-wrapped, no logging or `errors.Is` violations; one genuine `pkg/errcode` tiering breach and an unvalidated numeric header keep it off 5.

### Findings
- `medium` — infra publish failure is dressed up as an errcode instead of a raw wrapped error: `errcode.Internal("publish canonical", errcode.WithCause(err))` — `bot-message-handler/handler.go:200`. CLAUDE.md Tier 1 is explicit: "For an infra failure, `return fmt.Errorf("…: %w", err)` … do NOT dress it up as an errcode." Every other error site in the file gets this right (`handler.go:65,99,147,267,291`).
- `medium` — `parseHeaderIDs` accepts any `int64` unix-ms from `X-Bot-Created-At` with no range sanity check — `bot-message-handler/handler.go:237-242`. That value becomes `Message.CreatedAt` and is the Cassandra partition/clustering input downstream (`bot-message-worker/store_cassandra.go:70,111`), so a negative or year-3000 value writes a message into an unreachable bucket.
- `low` — `Subscription.SiteID`, `Room.Type/Name/SiteID` are decoded and projected but never read; all three call sites discard the value (`_, err :=`) — `bot-message-handler/store.go:28-39`, `handler.go:59,94,142`. Either enforce `sub.SiteID == h.siteID` (real defence-in-depth, the comment at `handler.go:57` implies it) or shrink the types to an existence check.
- `low` — SAST audit-coverage gap: gosec and repo-owned semgrep are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (blocked egress, per GLOBAL_PREP). Environmental, not a service defect.
- `nitpick` — `publishTimeout` is a hardcoded 2s const while every other timing knob is env-driven — `bot-message-handler/handler.go:28`.

### Recommendations
- `medium` — Replace `handler.go:200` with `fmt.Errorf("publish canonical message: %w", err)`; the boundary already collapses it to `internal`.
- `medium` — Bound `createdAt` in `parseHeaderIDs` (e.g. reject > ±24h from `time.Now()`), returning `BotInvalidHeader`.
- `low` — Assert `sub.SiteID == h.siteID` in both handlers, or delete the unused fields and their projections.
- `nitpick` — Move `publishTimeout` into `config` with an `envDefault:"2s"`.

---
