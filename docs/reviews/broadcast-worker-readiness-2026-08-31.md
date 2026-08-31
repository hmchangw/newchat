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

---

## 4. Test coverage — 2 / 5

Coverage is **67.7% (1067 statements)**, below the §4 80% floor, so the dimension is floored at 2. The number is **not vanity** — the fan-out core is genuinely well tested and the deficit is wiring code never extracted into testable units.

| File | Coverage |
|------|----------|
| `preview_writer.go` | 97.1% |
| `roomactivity.go` | 95.9% |
| `handler.go` | 88.0% |
| `store_mongo.go` | 26.2% |
| **`main.go`** | **3.2%** (of 217 stmts) |

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | 67.7%, under the 80% floor; driver is `main.go` at 3.2% (`main`, `Publish`, `broadcastProcessor` all 0%) | `coverage_by_service.txt` |
| high | **The preview writer's drop branch is 0% covered** and no test file even references `maxPendingPreviews` or `pendingPreviews`. Its own comment says getting this wrong "would take the flush's key-advance branch and stamp the new id over the room's previous body, certifying a preview for a message it does not describe. That is #224, reintroduced by the cap that bounds #289" — **the exact regression the branch exists to prevent has no test** | `preview_writer.go:127` |
| medium | The production per-message settle closure is 0% covered. `consumeloop_test.go` re-implements `jsretry.Settle(ctx, msg, LowLatencyBackoff, …)` as a **parallel copy**, directly contradicting the comment at `main.go:549` ("The integration test drives this exact composition rather than a parallel copy of the loop"). Swapping `LowLatencyBackoff` for `DefaultBackoff`, or regressing to a bare `Nak()`, would fail nothing | `main.go:523`; `consumeloop_test.go:438`, `:545` |
| medium | Untagged **unit** tests start an in-process NATS/JetStream server — `consumeloop_test.go` imports `nats-server/v2/server` with no `//go:build integration` tag, so it runs under `make test`. §4: "Never connect to real databases, NATS, or external services in unit tests." The file's own AckWait-tuning rationale documents that these tests are already load-sensitive on a busy runner | `consumeloop_test.go:12`, `:50-58` |
| medium | `publishThreadMetadata` is 50%: the DM/BotDM fan-out loop, its bot-skip/publish-error wrap and the unknown-room-type warn are 0%. **Thread badge delivery into DM rooms is entirely untested** | `handler.go:674`, `:696-708`, `:750-755` |
| medium | `cachedMetaStore` has **zero coverage in both suites** — the integration test exercises `MongoStore.GetRoomMeta`'s L2 path instead of the wrapper `main.go:289` actually wires into production | `metacache.go:18`, `:27` |
| low | `HandleServerBroadcast` is 66.7%: both non-happy branches (malformed-payload drop, unknown event type) uncovered — and this is a fire-and-forget core-NATS entry point where **dropping is the only failure mode** | `handler.go:216`, `:218-223`, `:233-236` |
| low | `appNameRepo.lookup` and `newAppNameRepo` are 0% in both suites, including the documented no-match branch `BotAwareDisplayName` depends on | `preview.go:142`, `:150-151` |
| nitpick | 90 top-level tests vs 29 `t.Run` calls; the `Missing*_ReturnsError` family would collapse into one table | `handler_test.go:963`, `:1221`, `:1911` |

**Worth recording:** sonic wire-compat is properly pinned — `sonic_wire_test.go:89` asserts semantic equality *and* the deliberate byte divergence, and `:67` pins the attachment-shadow case; no `map`-typed field is sonic-marshalled here, so the key-ordering caveat does not bite. Integration hygiene is clean: `TestMain` → `testutil.RunTests(m)`, containers from `pkg/testutil`, `testutil.FlushValkey` registered, no inline `GenericContainer`, no `time.Sleep`.

### Recommendations
- `high` — Add `TestPreviewWriter_OverTheCapShedsTheBodyAndMarksSealFailure`: buffer `maxPendingPreviews+1` distinct rooms with eligible previews, assert room N+1 flushes with `pvw == nil && pvwFailed == true` (**not** the key-advance branch), and that `pendingPreviews` returns to 0 after `Flush`.
- `medium` — Call `broadcastProcessor(handler)` from `consumeloop_test.go` instead of the hand-rolled closures, so the real backoff choice and Permanent Ack-drop are under test.
- `medium` — Move `consumeloop_test.go` behind `//go:build integration`, or keep the embedded server and document the exception in-file.
- `medium` — Cover `publishThreadMetadata`'s DM path (bots skipped, one publish per human account, error wrapped with room+account) and unit-test `cachedMetaStore.GetRoomMeta` with a mock `Store` — it needs no container.
- `low` — Two table cases for `HandleServerBroadcast` (undecodable bytes, unknown event); extract the remaining `main.go` wiring behind small testable funcs as `buildConsumerConfig`/`guardedProcessor` already are.

---

## 5. Maintainability — 3 / 5

Well-documented and conventional at the package boundary, but the fan-out logic has accreted into one 1,449-line `handler.go` and a 378-line `main()`, with visible duplication a new room type or event type would have to be threaded through by hand.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | `handler.go` is 1,449 lines / 25 methods covering dispatch, six event handlers, four thread variants, encryption, cross-site federation, metric labelling, debug helpers and event builders — the repo's 3rd-largest handler, while peers split the same concerns into topic files (`notification-worker` has 14; `roomlist-worker` has `flush.go`/`projection.go`/`batch.go`) | `handler.go:1-1449` |
| high | `main()` is 378 lines of wiring and has absorbed **four test files' worth of subject matter with no production counterpart**: `config_test.go`, `consumer_config_test.go`, `consumeloop_test.go`, `debug_log_test.go` all test code that lives in `main.go`. A missing `config.go`/`consumer.go` is the clearest signal the file outgrew its remit | `main.go:126-503` |
| medium | **The DM audience comes from two different sources depending on which handler you land in**: `publishMutation` and `publishThreadMetadata` iterate the denormalized `room.Accounts`, while `publishDMEvents` reads `store.ListRoomMembers` through `roomsubcache`. A new message and its later edit can address different recipient sets, and neither call site says why it chose its source | `handler.go:701`, `:918` vs `:1168` |
| medium | The same DM publish loop (bot skip → `subject.UserRoomEvent` → publish → error log) is written out three times | `handler.go:700-707`, `:917-935`, `:1181-1207` |
| medium | **Six copies** of the identical `default: slog.WarnContext(ctx, "unknown room type…")` switch arm — adding a room type means finding all six by grep | `handler.go:300`, `:383`, `:583`, `:636`, `:709`, `:941` |
| medium | Two room representations are both first-class in the store contract (`GetRoom → *model.Room`, `GetRoomMeta → roommetacache.Meta`), and every downstream helper is typed to one or the other. **This type split is what forces the duplicated fan-out above**; each new handler must first pick a room type | `store.go:20-21`; `handler.go:947` vs `:1226` |
| medium | Comment volume: 42% of `preview_writer.go`, 38% of `keycache.go`, 36% of `roomactivity.go`, including 25–35-line design essays above 12-line functions. The WHY is genuinely good, but at this density it is a design doc pasted inline — and `docs/design/` has no broadcast-worker page to hold it | `preview_writer.go:57-88`; `keycache.go:16-44` |
| low | No complexity linter is enabled repo-wide (no `funlen`, `gocyclo`, `cyclop`, `gocognit`, `dupl`), so **nothing mechanically flagged `handler.go` or `main()` as they grew** | `.golangci.yml:1-27` |
| low | Five optional dependencies are encoded as nil-means-disabled, each with its own guard idiom in a different file — one needing a companion bool to stay unambiguous. A sixth optional feature adds a sixth convention | `handler.go:104`, `:437`; `roomactivity.go:74`; `preview_writer.go:107`; `preview.go:47` |
| nitpick | `handler_test.go` is 4,181 lines / 90 tests in one file, mirroring the production sprawl. No dead code found; `staticcheck`'s `unused` is enabled and lint passes | — |

### Recommendations
- `high` — Split `handler.go` along seams it already has: `handler.go` (dispatch + struct + options), `handler_thread.go`, `federation.go`, `roomevent.go` (the four builders), `encrypt.go`. Precedented in-repo, no interface changes.
- `high` — Extract `config.go` (the config struct + validation) and `consumer.go` (`buildConsumerConfig`, `broadcastProcessor`, `guardedProcessor`, `natsPublisher`), giving the four orphan test files real counterparts and leaving `main()` as wiring only.
- `medium` — Introduce one internal fan-out view (`roomFanout{ID, Type, SiteID, Accounts, UserCount, CrossSite, CrossSiteAt}`) that both `model.Room` and `roommetacache.Meta` convert into, then collapse the three DM loops into a single `publishToAccounts` and the six unknown-room-type arms into one helper. **This is the refactor the other three findings all point at.**
- `medium` — Pick one DM audience source (`ListRoomMembers`, since it is cache-backed and survives a Mongo outage) and use it for edits/pins/reactions too, or document at both call sites why `room.Accounts` is authoritative for mutations.
- `medium` — Move the long-form rationale into `docs/design/broadcast-worker.md`, leaving ≤2-line pointers inline.
- `low` — Enable `funlen` (or `cyclop`) with a generous threshold and a grandfathering exclusion, so the next `handler.go` growth is caught at lint time rather than at audit time.

