# broadcast-worker — Production Readiness Review

**Service:** `broadcast-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.5 / 5

**The highest-scoring service audited**, and deservedly: this is the user-visible fan-out hot path and it shows deliberate engineering — sonic with pretouch, prebuilt metric attribute sets, singleflight-guarded bounded caches, a coalescing preview writer with a body cap, correct `jsretry.LowLatencyBackoff` settling, and the documented one-MongoDB-write boundary held exactly. The architectural boundaries `CLAUDE.md` describes for this service are all real in the code. Three things keep it off a 4. The mention federation **derives its destination from event data with no check against the configured peer set**, so a stale site ID publishes into an OUTBOX subject no consumer filters on. **Connection strings default to localhost** instead of being `required` — this service is the fleet outlier. And there is an **undocumented third federation lane** (fire-and-forget core NATS) that CLAUDE.md's two-lane model does not describe and that silently dies if ops never exports the subject.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 6 | 19 | 17 | 8 | **50** |

---

## 2. Go code quality — 4 / 5

Disciplined, idiomatic Go — wrapped errors, structured `slog` with explicit `request_id`, correct `errcode` Tier-1/Tier-3 use, justified suppressions — held back by a silently-discarded decode count and inconsistent malformed-payload handling inside one switch.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | `buildClientMessage` discards `DecodeAttachments`' `skipped` count with a bare `_` and no comment, so **malformed attachments are silently stripped from the client-facing payload** with no log or metric. The same call at `preview.go:64` treats `skipped > 0` as a hard error. Identical input, two opposite policies — and the one that *silently loses data* is the user-visible one | `handler.go:1248` vs `preview.go:64` |
| medium | Malformed-payload handling diverges **within one event switch**: five handlers return `errcode.Permanent(errcode.BadRequest(...))`, but `handleReacted` logs `slog.ErrorContext` and returns `nil` for the same class. Both Ack, but the Permanent path is classified and WARN-logged once by `jsretry.Settle` while the reacted path emits an ERROR and Acks with no settle-side record — two log levels and two shapes for "publisher violated the contract" | `handler.go:816`, `:826` vs `:395`, `:543`, `:597`, `:721`, `:762` |
| low | `publishDMEvents` returns on a mid-loop `sonic.Marshal` failure **after earlier recipients were already published**, NAKing a partially-delivered fan-out — while every other failure in that same loop is deliberately log-and-continue. A marshal failure is deterministic, so redelivery re-publishes to already-served accounts up to `MaxDeliver` before dropping | `handler.go:1196-1198` vs `:1202-1210` |
| low | Room-key errors are wrapped four levels deep with **three restatements of the same phrase**; `CLAUDE.md` asks each wrap to describe what *this* function was doing | `keycache.go:93` → `handler.go:1013` → `:993`/`:1033` → `:1055` |
| low | This is a designated sonic hot-path worker, yet the cross-site room-activity publish marshals with `encoding/json` and the type is absent from `pretouchTypes`. The choice may be deliberate (throttled, low volume) but nothing says so | `roomactivity.go:5`, `:184`; `pretouch.go:11-19` |
| low | `publishToThreadAccounts` logs each failed publish **and** returns an aggregate error that `jsretry.Settle` logs a second time | `handler.go:1284-1289`, `:1294` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Dead `account := account` loop-variable alias; go.mod pins Go 1.25 and `store_mongo.go:141` states the opposite in the same service | `handler.go:1279` |
| nitpick | `publishMutation` erases its typed event to `evt any` purely to reach `sonic.Marshal`, losing the compile-time link between `roomEvtType` and the struct actually marshalled | `handler.go:898` |

### Recommendations
- `medium` — In `buildClientMessage`, capture `skipped` and either log it once (`WarnContext` with `room_id`/`messageID`, **count only**) or record a counter. A bare `_` on a data-loss signal needs at minimum the mandated comment.
- `medium` — Make `handleReacted`'s two guards return `errcode.Permanent(errcode.BadRequest(...))` like their five siblings, and drop the local `slog.ErrorContext` so `jsretry.Settle` owns the single log.
- `low` — Hoist the `sonic.Marshal` out of `publishDMEvents`' per-recipient loop (only `HasMention` varies — marshal the two variants once) so a marshal error is detected before any publish.
- `low` — Collapse the key-fetch wrap chain; drop the redundant wrap at `handler.go:1013`.
- `low` — Either switch `roomActivityPublisher` to sonic and add the type to `pretouchTypes`, or add a one-line comment stating why this lane stays on `encoding/json`.
- `low` — Replace the per-account `ErrorContext` in `publishToThreadAccounts` with an aggregated failure count in the returned error.

