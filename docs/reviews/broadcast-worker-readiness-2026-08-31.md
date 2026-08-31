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

---

## 3. Architecture — 4 / 5

Boundaries hold exactly as `CLAUDE.md` documents them — one MongoDB write, OUTBOX-only mention federation via `outbox.Publish`, correct high-throughput consumer — but an undocumented third cross-site lane and localhost-defaulted connection strings are real production risks.

### Verified clean
`Store` declares only the five methods this service consumes, all five used; the flush-boundary interface `bulkRoomPreviewWriter` is deliberately kept *off* `Store`. DI is constructor + functional options with no globals. **The only Mongo write is the preview `BulkWrite` on rooms' preview-only fields** — exactly the documented boundary. The mention badge goes through `outbox.Publish` with `subject.Outbox`, and `InboxSubscriptionMention` is in exactly one partition set. Zero raw `fmt.Sprintf` subject construction. The consumer is the correct high-throughput shape — `cons.Messages()` + `PullMaxMessages(2*MaxWorkers)` + semaphore sized by `MAX_WORKERS`, never mixed with `Consume()`. `BackOff` derived via `stream.DurableConsumerDefaults`, never hardcoded. Shared knobs mounted as named fields with `envPrefix`. Shutdown follows the documented order.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **An undocumented third federation lane.** Cross-site room-activity uses a fire-and-forget **core-NATS** publish to `chat.roomactivity.{destSiteID}` on every remote peer, bypassing both OUTBOX and INBOX. `CLAUDE.md`'s federation model has exactly two lanes. This one has no stream, no retry and no ack, so **if ops/IaC never exports `chat.roomactivity.>` across the gateway the feature is silently dead** — the publish succeeds locally and nothing arrives. Nothing in `docs/` describes the subject | `roomactivity.go:193`; `main.go:354-355`; consumed at `inbox-worker/main.go:902` |
| medium | Connection strings carry **localhost `envDefault`s instead of `required`**, against §Configuration ("never default secrets or connection strings"). This service is the fleet outlier — room-service, message-worker and message-gatekeeper all use `,required`. A dropped env var here starts cleanly against nothing instead of failing fast; `SITE_ID` defaulting to `"default"` compounds it, since a mis-templated deploy binds the **wrong site's** canonical stream rather than exiting | `main.go:46`, `:48`, `:53` |
| low | `bootstrapStreams` deviates from the documented signature and the disabled branch *verifies* rather than no-ops. The verify is better engineering than a no-op and the widened signature is forced by dual user/bot `MODE` wiring — but `CLAUDE.md` is binding, so either the code or the convention should move | `bootstrap.go:29-41` |
| low | Shutdown drops the core-NATS server-broadcast subscription with `Unsubscribe()` rather than `Drain()`, discarding already-buffered messages; and `HandleServerBroadcast` callbacks are never registered on the `wg` that hook 3 waits on, so an in-flight thread-badge fan-out can be cut off by the later drain and Mongo disconnect | `main.go:455`, `:456-469` |
| low | The preview flush goroutine is spawned unconditionally and only *then* is `previews` nil-checked for the log line; safe, but the guard reads inverted and leaves a goroutine plus channel alive for the whole process when preview persistence is off | `main.go:371-377` |
| nitpick | Deploy layout is `deploy/user/` + `deploy/bot/` subdirectories rather than the flat files §"When Creating Services" specifies — justified by the dual-`MODE` binary, but undeclared | — |
| nitpick | Exported constructor returns an unexported type: `NewMongoStore(...) *mongoStore` | `store_mongo.go:53` |

### Recommendations
- `medium` — Move the room-activity announce onto a durable lane, **or** document `chat.roomactivity.{destSiteID}` in `CLAUDE.md` §Multi-site federation and §NATS Subject Naming as an explicitly best-effort third lane, with its gateway-export requirement called out for ops.
- `medium` — Change `NATS_URL` and `MONGO_URI` to `env:"…,required"` to match the rest of the fleet; consider `SITE_ID,required` too.
- `low` — Reconcile `bootstrapStreams` with `CLAUDE.md` — keep the fail-fast verify and amend the convention text, since a silent no-op against a missing stream is strictly worse.
- `low` — Replace `broadcastSub.Unsubscribe()` with `Drain()` and register the server-broadcast callback on `wg` so hook 3 actually drains it.
- `nitpick` — Hoist the `previews != nil` check to guard the goroutine spawn; rename `NewMongoStore` → `newMongoStore`.

