# search-sync-worker — Production Readiness Review

**Service:** `search-sync-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

Thoughtful abstractions (`Collection`, `msgFetcher`, `flushPipeline`) and a genuinely well-engineered bulk pipeline — slot-based backpressure, dual size/interval flush triggers, precomputed metric attributes, clean `jsretry` discipline. Three things are seriously wrong. **A `critical`: the bot-message collection binds a stream whose subjects its own consumer filter cannot match**, so in `MODE=default` the consumer is rejected and the pod exits 1 — and a unit test enshrines the wrong filter. **The shipped default tuning trips its own coupling check** (`BULK_BATCH_SIZE × PIPELINE_DEPTH` exceeds the default `MaxAckPending`), and the check only warns. And **no ES request on the flush path carries a deadline**, so a hung connection holds a pipeline slot forever and blocks shutdown.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 2 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 12 | 18 | 13 | 5 | **49** |

---

## 2. Go code quality — 4 / 5

Disciplined, heavily-reasoned Go with correct `jsretry`/`errcode` worker tiering and nil-safe metrics; the defects are localized.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **The failed-bulk-item log emits ES's raw `error.reason`, which routinely quotes the offending field value** (`mapper_parsing_exception … Preview of field's value: '…'`) — and for the messages collection that field is `content`, i.e. **the message body**. This contradicts §3 ("Never log … full message bodies") *and* the rule the same file states 130 lines above: "the document body never belongs in an error that reaches the server log". `ErrorType` + `Status` are already logged and carry the diagnosis | `handler.go:293-295`, rule at `:158-159`; `pkg/searchengine/adapter.go:184` |
| medium | `context.Background()` passed to two blocking network calls on the message path, discarding the consumer span context `AddWithContext` was built to carry. Both are untraced and **uncancellable at shutdown** — and the Mongo call has no timeout of its own, so a slow primary can outlast the 25 s drain. Root cause: `Collection.BuildAction(data []byte)` takes no `ctx`, though the ctx is already in hand at `handler.go:98` | `messages.go:220`, `:304`; `collection.go:45` |
| medium | Two flush-failure logs drop the context every other log in the file uses, **breaking trace correlation exactly on the failure path** — `bulkCtx` is in scope and passed to the next three calls; only the `slog` call is context-free | `handler.go:242`, `:261` |
| low | Four bare `return nil, err`. The worst is `consumer_source.go:38`: a raw `Fetch` error surfaces with no indication it came from the domain-scoped HR consumer, and that branch is silently swallowed | `spotlight_org.go:149`; `consumer_source.go:38`; `spotlight.go:69`; `user_room.go:58` |
| low | Three silently-discarded `json.Marshal` errors with no justification comment — the convention exists in this very service ("Error discarded: input is a static map of literals"). `messages.go:376` is **not** that case: it marshals a `MessageDoc` built from event data, and a failure would push an empty/invalid `Doc` into a bulk action rather than fail the message | `messages.go:279`, `:376`; `spotlight_org.go:242` |
| low | Three exported `Handler` methods exist **only for tests** — production uses `AddWithContext`/`Take`+`FlushBatch` exclusively. §4: "Test helpers belong in `_test.go` files only" | `handler.go:82`, `:223`, `:363` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Five metric-constructor fallbacks discard the error with no comment | `metrics.go:69`, `:75`, `:80`, `:85`, `:90` |

### Recommendations
- `medium` — Drop `"error", results[i].Error` from `handler.go:293`; keep `status`/`errorType`/`docID`/`index`. If the reason is needed, gate it behind `DEV_MODE` or truncate to the exception class before the colon.
- `medium` — Add `ctx context.Context` as the first parameter of `Collection.BuildAction`/`BuildActionSeq`/`BuildByQuery` and thread `AddWithContext`'s ctx through both resolvers; give the Mongo resolver its own bounded timeout.
- `medium` — Change the two flush-failure logs to `slog.ErrorContext(bulkCtx, …)`.
- `low` — Wrap the four bare returns; handle or comment the three marshal discards — at `messages.go:376` return the error and let `BuildAction` Ack-drop it as poison.
- `low` — Move `Add`, `Flush` and `MessageCount` into `handler_test.go` as unexported helpers, or delete them.

---

## 3. Architecture — 4 / 5

Boundaries, consumer-defined interfaces, DI, `pkg/stream`/`pkg/subject` discipline and shutdown order are all correct, and the INBOX/HR non-ownership rule is genuinely honoured — but it is honoured by **untested inline code in a 618-line `main.go` that skips the mandated `bootstrapStreams` helper.**

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | Stream bootstrap is written **inline in `main()`** instead of the mandated `bootstrapStreams(ctx, js, siteID, enabled)` helper. §6: "via the service's `bootstrapStreams` helper, **never inline**". Twelve sibling services have the file; this one does not | `main.go:310-323` |
| high | **The "never create INBOX/HR" invariant is enforced by two locals compared by name inside `main()`, and no test asserts it.** A grep of every `*_test.go` finds no reference to `Bootstrap`, `inboxName` or `hrName`. The service's most load-bearing architectural rule is unexecutable by a unit test and **silently breakable by anyone adding a sixth collection who forgets to extend the skip list**. Ownership is a property *of the collection*, not of `main` | `main.go:306-314` |
| medium | `main.go` (618 lines) carries the consumer loop, the flush pipeline, adapters and helpers — not just wiring. §1 scopes `main.go` to config parsing, wiring, startup and shutdown | `main.go:438-618` |
| medium | `ADMIN_ACCT_PREFIX` re-declared per service rather than mounted from `pkg/model`, which owns `SetPlatformAdminAccountPrefix`. The identical tag + `envDefault:"p_admin"` is copy-pasted into **≥5 services** — and **a prefix mismatch mis-classifies platform admins** | `main.go:98-99`; `pkg/model/user.go:126` |
| medium | **A third JetStream consumer pattern**: a `Fetch()` polling loop plus `flushPipeline`, neither `cons.Messages()`+semaphore nor `cons.Consume()`. The design is sound (ES bulk needs batch-shaped delivery, and `PIPELINE_DEPTH` + `checkBatchAckCoupling` bound it deliberately), but `CLAUDE.md` sanctions only two patterns and this service ships neither; `MAX_WORKERS` does not exist here at all | `main.go:498-569`; `consumer_source.go:102-104` |
| low | MongoDB is connected **unconditionally**, but `db` is consumed only in `Mode=teams` — default-mode pods take a hard `os.Exit(1)` startup dependency, plus a connection pool, on a database they never read | `main.go:198-205` vs `:219` |
| low | Store file naming drifts: the Mongo implementation is `teams_user_store.go`, not `store_mongo.go`. (`store.go` correctly holds only the ES `Store`, and the interface is consumer-defined at `messages.go:35`, which is right) | `teams_user_store.go:1` |
| nitpick | `Handler.Add` is a production-exported wrapper used only by tests | `handler.go:79-81` |

### Recommendations
- `high` — Add `search-sync-worker/bootstrap.go` with `bootstrapStreams(ctx, js, siteID, enabled, collections)` matching the sibling signature, move the loop out of `main()`, and unit-test it against a fake `streamManager`.
- `high` — **Move ownership onto the abstraction**: add `OwnsStream() bool` (or a `BootstrapPolicy`) to the `Collection` interface, returning `false` for `inboxMemberCollection` and `spotlightOrgCollection`. Then assert per-collection in a table test that no INBOX/HR stream is ever passed to `CreateOrUpdateStream`. This makes the invariant **travel with new collections** instead of with a name list in `main`.
- `medium` — Extract `consumer.go` (`runConsumer`, `consumerTuning`, `flushPipeline`, `checkBatchAckCoupling`) and `adapters.go`, leaving `main.go` at wiring + shutdown.
- `medium` — Promote the admin-prefix knob into `pkg/model` as a mounted config and delete the five per-service redeclarations in one PR.
- `medium` — Amend `CLAUDE.md` §"JetStream Consumer Pattern" to name a third sanctioned **batch/bulk-sink** pattern (Fetch + bounded flush pipeline) and cite this service, or restate why it is exempt — right now the rule and the fleet disagree.
- `low` — Gate the Mongo connect on `cfg.Mode == "teams"`; unexport or delete `Handler.Add`.

---

## 4. Test coverage — 2 / 5

Coverage is **67.7% (746 statements)**, below the §4 80% floor, so the dimension is floored at 2. The tests that exist are unusually good — real NAK-pacing, poison-drop and pipeline-depth assertions — and **165 of the 241 uncovered statements are in one 322-line `main()`**; the rest of the service is 80–100%.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | 67.7%, under the 80% floor; `main()` alone is 0% and holds 165 of the 241 uncovered statements | `main.go:102` |
| high | **The entire Mongo store `mongoTeamsUserResolver` is 0% with neither a unit nor an integration test.** Its two-hop `teams_user → account → users` join, the "account exists but no `users` row ⇒ empty `UserID`" case, and both error wraps are entirely unexercised — while `testutil.MongoDB(t, prefix)` is already available in this package's `TestMain` | `teams_user_store.go:23`, `:32` |
| high | **`MODE=teams` is wired only in `main()` and has no test at any level**; `teamsMessagesStreamCfg` and `botMessagesStreamCfg` are both 0%. Nothing pins which stream the bot and teams consumers bind to — and **a wrong stream/subject here fails silently** (consumer created, zero deliveries) | `messages.go:104`, `:109` |
| medium | The decode-payload poison branch in `AddWithContext` is uncovered — a truncated/corrupt zstd frame must **Ack-drop, not NAK-loop**, and that Ack plus its accounting is unpinned | `handler.go:90-95` |
| medium | The `BuildByQuery` error branch is uncovered: a malformed `room_renamed` payload must Ack-drop, but today an **inverted Ack/Nak here would redeliver poison to `MaxDeliver` unnoticed** | `handler.go:104-110` |
| medium | The multi-failure branch of the per-message settle loop is uncovered. Every bulk-failure test uses one action per message, so the "first failure decides, later failures only increment" invariant is never exercised for a **fan-out** message, where a permanent 400 followed by a transient 429 must still Ack-drop | `handler.go:280-281` |
| medium | `newESRead`'s failure branches are uncovered despite an injectable `esSearcher` making them trivial unit tests. **Dropping `threadParentMessageCreatedAt` silently changes search-service's restricted-room access filtering** | `thread_parent_resolver.go:45-78` |
| low | Two reachable invalid-input branches in `BuildAction` uncovered (missing `createdAt`, non-positive `Timestamp`), while missing message ID is covered | `messages.go:187-192` |
| low | `newSyncMetrics` instrument-construction error branches (5) uncovered | `metrics.go:68-91` |
| nitpick | Remaining uncovered blocks are unreachable `json.Marshal` errors on struct literals — not worth chasing | `spotlight.go:92`, `:142`, `:173` |

**Test hygiene is compliant:** `package main` throughout, mocks generated into `mock_store_test.go`, no real DB/NATS in unit tests, integration files carry `//go:build integration` with a `TestMain` built on `testutil.PrewarmFailFast` + `TerminateAll`, containers from `pkg/testutil`, no `time.Sleep` synchronization (channel/`Eventually` based, with bounded negative assertions).

### Recommendations
- `high` — Extract the ~12 fail-fast gates from `main()` into `validateConfig(cfg) error` and table-test it. Single largest coverage win, and it turns silent startup misconfiguration into a pinned contract.
- `high` — Add `teams_user_store_integration_test.go` using `testutil.MongoDB`: hit, miss, account-without-`users`-row, empty input, and both query-error wraps.
- `high` — Extract the collection-wiring block into `buildCollections(cfg, engine, db, metrics) []Collection` and unit-test both MODE branches, asserting each collection's `StreamConfig`/`ConsumerName`/`FilterSubjects` — **this also closes the `critical` filter mismatch in Chapter 6.**
- `medium` — Extract the per-collection stream/consumer loop far enough to test that `Bootstrap.Enabled=true` still skips the INBOX and HR stream names — the guard that keeps this service from creating a stream `inbox-worker`/`hr-syncer` owns.
- `medium` — Add three `Handler` cases: corrupt-zstd ⇒ Ack + no buffer growth; `BuildByQuery` error ⇒ Ack-drop; fan-out message failing permanent-then-transient ⇒ Ack-drop with zero `nakDelay`.
- `medium` — Unit-test `newESRead` with a stub `esSearcher` for search error, malformed JSON, zero hits, and zero `CreatedAt`.

---

## 5. Maintainability — 3 / 5

Genuinely thoughtful abstractions and comments that explain WHY, undermined by a 319-line `main()`, a fat interface with two side-channel escape hatches, a mode-boolean threaded through five methods, and production API that exists only for tests.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | `main()` runs 319 lines with 26 `os.Exit(1)` sites, mixing eight distinct jobs — config validation, obs/engine/Mongo init, mode-gated collection construction, **three separate provisioning loops**, NATS + HR wiring, per-collection stream/consumer/handler wiring, health, shutdown. **Nothing in this function is reachable from a test**, which directly causes the 67.7% | `main.go:102` |
| high | ~55 lines of copy-pasted `if X <= 0 { slog.Error; os.Exit(1) }` inline in `main()`, with **no `validate()` method anywhere** in the service. `config_test.go` tests only env-tag defaults, so every one of these nine guards is untested — and adding a tenth knob means another copy of the block | `main.go:110-160` |
| high | **The consumer runtime lives in `main.go`, not in a file of its own** — yet its tests are already split out as `consumer_pipeline_test.go` and `consumer_config_test.go`. **Tests are named after production files that do not exist**; the split has been made mentally but not on disk | `main.go:448`, `:484`, `:498`, `:575`, `:591`, `:611` |
| medium | `Handler.Add`, `Handler.Flush` and `Handler.MessageCount` have **zero production callers** — grep finds them only in test files. §4: "Test helpers belong in `_test.go` files only — NEVER put test helpers in production code" | `handler.go:81`, `:223`, `:363` |
| medium | `messageCollection` serves three sources via a `teamsOnly` bool gating five methods plus a `streamCfg func` field; the teams path shares no code with the canonical path beyond the index name. **A fourth source means a second boolean or a tri-state** | `messages.go:51`, gates at `:123`, `:133`, `:141`, `:156`, `:169` |
| medium | `Collection` is an 8-method interface where three methods exist so most implementers can return `""`/`nil`, routed around by two optional-capability assertions. **`spotlightOrgCollection.BuildAction` is the leak made explicit: it satisfies the interface by returning an error saying it must never be called** | `collection.go:13`; `spotlight_org.go:117` |
| medium | `spotlightCollection.BuildAction` and `userRoomCollection.BuildAction` duplicate the same 15-line skeleton, differing only in the action built | `spotlight.go:68`; `user_room.go:57` |
| low | `BuildAction(data []byte)` carries no `context.Context`, so the two collections doing I/O **fabricate one** — both calls invisible to shutdown and to the message's trace | `messages.go:220`, `:304` |
| nitpick | Stale planning comment ("runConsumer (Task 2) will hold a msgFetcher"); `nakAll`'s comment names "the two defensive paths in `Flush`", both of which moved to `FlushBatch` | `consumer_source.go:24`; `handler.go:351` |

### Recommendations
- `high` — Extract `func (c config) validate() (time.Time, error)` returning the parsed `syncMessagesFrom`, replacing all nine inline guards; `main()` then does one error check. Table-drive it in `config_test.go`.
- `high` — Move `flushPipeline`, `consumerTuning`, `runConsumer`, `checkBatchAckCoupling` into `consumer.go`, and `engineAdapter`+`buildConsumerConfig` into `consumer_source.go`, **matching the test files that already exist**. Target `main.go` under 250 lines.
- `high` — Extract the three provisioning loops into one `provision(ctx, engine, collections) error`, and the collection construction into `buildCollections(...)` — both unit-testable with the existing fakes.
- `medium` — Delete the three test-only `Handler` methods; give the tests thin `_test.go` wrappers.
- `medium` — Split `teamsMessageCollection` into its own type in `messages_teams.go`, dropping the `teamsOnly` bool and the four early-returns.
- `medium` — Add a shared `forEachMemberAccount(...)` helper beside `parseMemberEvent`; both inbox collections shrink to their switch bodies.
- `low` — Shrink `Collection` to the four methods every implementer needs and move provisioning to an optional `provisioner` interface asserted in the setup loop; `spotlightOrgCollection` then drops its unusable stub.

---

## 6. Integration — 2 / 5

Subject builders, INBOX lane handling and stream ownership are exemplary — but **the bot-message consumer is wired to a filter that cannot match its own stream**, and JetStream fetch/batch errors are swallowed without a log or a metric.

| Sev | Finding | Evidence |
|-----|---------|----------|
| **critical** | **The bot-message collection binds `BOT-MESSAGES-CANONICAL-{site}` (subjects `chat.bot.canonical.{site}.>`) but its consumer filter is `chat.msg.canonical.{site}.*`** — reached because `newBotMessageCollection` sets only `streamCfg`, not a filter. The filter is **not a subset of the stream's subjects**, so `CreateOrUpdateConsumer` is rejected and `main.go:344-356` **exits 1 in `MODE=default`**; were it accepted, **no bot message would ever be indexed**. Bot messages are published on `chat.bot.canonical.{site}.created` with the same `model.MessageEvent` shape — only the subject is wrong | `messages.go:129`, `:74-80`; `pkg/stream/stream.go:94-99`; `bot-message-handler/handler.go:199` |
| high | **The wrong filter is enshrined by a unit test** asserting `chat.msg.canonical.site-a.*` for the bot collection. No integration test binds the bot collection to a real stream, so **nothing in CI can catch it** | `messages_test.go:621-622` |
| high | **JetStream errors are invisible.** `msgBatch` exposes only `Messages()` although the o11y batch implements `Error() error`, so post-fetch failures (consumer deleted, leader change, no heartbeat) are never surfaced; and the `Fetch` error path logs nothing, records no metric, and `continue`s immediately — **a persistently failing consumer spins hot and silently indexes nothing** | `consumer_source.go:19-21`; `main.go:538-550` |
| medium | A Teams author-resolution failure is **Acked permanently**: `resolveTeamsIdentities` returns `nil` on Mongo error, `buildTeamsActions` then emits docs with empty `userId`/`userAccount` that flush successfully and Ack. **Nothing re-indexes them.** The hazard is documented and mitigated only via read preference — there is no metric or retry for the error branch itself | `messages.go:304-307`, `:265-268`; `handler.go:301-317`; `main.go:48-53` |
| medium | Enrichment calls drop the message context (`context.Background()` in both resolvers), so the ES parent lookup and Mongo lookup fall **outside the trace and outside shutdown cancellation** | `messages.go:220`, `:304` |
| low | Stream bootstrap is inline rather than the mandated `bootstrap.go` helper. The behaviour is correct (INBOX and HR excluded by name comparison, only `Name + Subjects` set), but the deviation **makes the exclusion easy to lose when a collection is added** | `main.go:311-325`, `:307-314` |
| nitpick | The spotlight room-name LWW guard compares **two different hosts' clocks** — the rename ts is stamped in room-service, `roomNameUpdatedAt` on member docs in room-worker. Skew widens the already-documented rename/add race | `spotlight.go:143`, `:186` |

**Verified clean:** every subject comes from `pkg/subject` with no `fmt.Sprintf` subject building; both INBOX lanes are consumed and **neither INBOX nor HR is ever created**; all four consumed inbox types carry a publish-site `time.Now().UTC().UnixMilli()` timestamp; consumer config derives from `stream.DurableConsumerDefaults` with no hardcoded `BackOff`; the service registers no `chat.user.` handler, so `docs/client-api.md` is not implicated.

### Recommendations
- `critical` — Give `messageCollection` a `filterSubjects func(siteID) []string` field beside `streamCfg`, set it to `subject.BotCanonicalWildcard` (or `stream.Resolve(stream.PipelineBot, siteID).CanonicalWildcard`) for the bot collection, and **fix `messages_test.go:622`**.
- `high` — Add an assertion that **every collection's `FilterSubjects(siteID)` is a subset of its own `StreamConfig(siteID).Subjects`** — a table test over all five collections, so this class of drift cannot recur.
- `high` — Add `Error() error` to `msgBatch`, check it after draining `Messages()`, and log + count both it and the `Fetch` error with a short backoff before `continue`.
- `medium` — On `ResolveIdentities` error, return the error so the batch Naks (or emit a dedicated failure metric plus a re-index path) instead of indexing author-less documents.
- `medium` — Thread `msgCtx` from `AddWithContext` into `BuildAction` and the resolvers so the ES and Mongo lookups inherit the span and shutdown deadline.
- `low` — Move bootstrap into `bootstrap.go`, with the INBOX/HR exclusion expressed there.

