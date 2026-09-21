# message-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `2369149` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.2, Δ +0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The persistence hot path is correct where it matters most: plaintext creates pin `USING TIMESTAMP`, encrypted creates and the legacy-strip UPDATEs do not, `store_cassandra_writetime_test.go` pins both halves, `sonic` and `jsonwarm.Pretouch` are in place, the consumer opts into the outage retry budget, the #484 degraded start gates every document-creating thread write on the index gate, and Mongo is ordered before Cassandra so an outage NAKs before anything persists. Two design gaps carry the medium findings. The enrichment columns (`sender`, `mentions`) are re-resolved fail-open on every delivery yet bound under one pinned timestamp — the per-cell mixed-enrichment window CLAUDE.md itself names as unfixed — and a consume-loop exit is invisible to `/readyz`, so a pod that has silently stopped persisting history keeps reporting healthy; roomlist-worker already carries the readiness pattern to port. Performance is well-sized but leaves round-trips on the table: every subsequent thread reply pays two Paxos LWTs it can skip on non-redeliveries, thread-mention marking costs 2k+1 serial Mongo writes per reply, thread-store calls carry no deadline, and a 20 KB `tshow` reply's three-copy unlogged batch exceeds Cassandra's default fail threshold deterministically and then NAK-loops for the hour-long budget. `thread_reply_added` publishes the reply's own `CreatedAt` instead of the `TLM` the store computed, regressing thread freshness under out-of-order redelivery. Coverage is 56.3% with the parent `thread_room_id` CAS stamp untested at any layer; seven hand-aligned INSERT literals guard the pin rule only by comment and one hand-listed test; the README describes a service that no longer exists.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 4 |
| Performance | 4 |

**Findings by severity:** 1 critical, 1 high, 22 medium, 21 low, 7 nitpick (52 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] Log-AND-return on the Teams persist failure path — `message-worker/teamsbatch.go:140` — `slog.ErrorContext("teams batch: save message failed", …)` then `return res, err`; the error propagates unchanged through `handleBatch` (`teamsbatch.go:84`) into `jsretry.Settle` (`teamsbatch.go:74`), which logs it again at `pkg/jsretry/jsretry.go:138` ("message failed — retrying"). Every Cassandra outage produces two ERROR lines per message, violating the "never log AND return" rule.
- [medium] Six bare `return err` in the Mongo thread store — `message-worker/store_mongo.go:52,62,65,94,106,122` — each forwards `IndexGate.Ready`'s error without the local frame ("create thread room", "insert thread subscription", …). The callee does wrap (`pkg/mongoutil/indexgate.go:74`), so no context is lost below, but the store op in progress is invisible in the NAK log line. Direct breach of "never return bare err".
- [medium] README describes a service that no longer exists — `message-worker/README.md:3-56` — claims it consumes `MESSAGES-{siteID}` on `chat.user.{account}.room.{roomID}.{siteID}.msg.send`, publishes to a `FANOUT-{siteID}` stream (no such stream in `pkg/stream/stream.go`), writes a `messages` table with `content`/`user_id` columns and touches `rooms.updatedAt`. Actual code binds `MESSAGES-CANONICAL` `.created` (`main.go:380`), writes `messages_by_room`/`messages_by_id`/`thread_messages_by_thread` (`store_cassandra.go:188-295`) and `thread_rooms`/`thread_subscriptions`. Config table lists `NATS_URL` with a default; it is `required` (`main.go:42`). An on-call reader is actively misled.
- [low] Two ERROR logs bypass the context-aware API — `message-worker/store_cassandra.go:465,480` — `slog.Error(...)` instead of `slog.ErrorContext(ctx, ...)` on the thread_room_id-stamp misses. `request_id` is passed by hand but the o11y handler's trace/span correlation (which reads ctx) is lost on exactly the lines that flag a permanently broken thread read. Only two non-ctx call sites in the service.
- [low] `pretouch` list drifted from the sonic call sites — `message-worker/pretouch.go:12-14` — warms `model.InboxEvent`, which this service never sonic-marshals (`outbox.Publish` uses `encoding/json`, `pkg/outbox/outbox.go:96,106`), while `model.ThreadUnreadAddedEvent` (`handler.go:752`) and `model.TeamsBatchRequest` (`teamsbatch.go:68`) are sonic-coded and not warmed. Cold first-call compile lands on the live thread-unread path.
- [low] Dead code — `message-worker/teamstransform.go:113` — `reactionShortcode` is referenced only by a comment (`:71`) and its own test; no production caller.
- [low] Mode magic strings — `main.go:87,241,374`, `bootstrap.go:46` — `"default"`/`"teams"` compared as raw literals in four places with no typed constant; a typo compiles.
- [low] Mixed JSON codecs in one service — `main.go:276`, `teamsbatch.go:163`, `teamstransform.go:33` use `encoding/json` while the same paths' outer envelope uses sonic (`teamsbatch.go:68`). CLAUDE.md names message-worker a sonic worker; the split is undocumented at the call sites.
- [nitpick] Corrupted UTF-8 (`â` for em-dash) in comments — `teamstransform.go:72,94`.
- [nitpick] `"error", failed` binds an int count to the conventional error key — `teamsbatch.go:96` — log pipelines that type the `error` field as string will choke or mis-index.
- [nitpick] Comment cites hard-coded line numbers that have drifted — `main.go:147` ("handler.go:159-201"; the Mongo-before-Cassandra ordering now lives at `handler.go:207-234`).
- [nitpick] `Handler`/`NewHandler`/`CassandraStore`/`NewCassandraStore`/`DefaultTransformer` are exported inside `package main` — `handler.go:34,53`, `store_cassandra.go:60,71`, `teamstransform.go:23` — contrary to "keep handler/store implementations unexported within services" (repo-wide habit, zero runtime effect).

Verified compliant (not findings): every plaintext create INSERT pins `USING TIMESTAMP` (`store_cassandra.go:192,203,284,295,316`); encrypted creates (`:241,251,363,374,391`) and the three legacy-strip UPDATEs (`:163-165`) are unpinned; `UpdateParentMessageThreadRoomID` (`:454`) and `threadcount.Maintain` carry no pin. `errcode.Permanent` used only for poison payloads (`handler.go:98`, `teamsbatch.go:64,71`); `errors.Is` throughout; no bare `Nak()`; no body/token logging; request_id on every WARN/ERROR line; `go vet` and `gofmt -l` clean.

### Recommendations

- [medium] Drop the `slog.ErrorContext` at `teamsbatch.go:140` (keep the metric) or switch `teamsbatch.go:74` to `jsretry.SettleQuiet` — one ERROR per failed message instead of two.
- [medium] Wrap the six `IndexGate.Ready` returns — `store_mongo.go:52,62,65,94,106,122` — e.g. `fmt.Errorf("create thread room: %w", err)`; NAK logs then name the operation.
- [medium] Rewrite `message-worker/README.md` against the current pipeline (stream, subject, three Cassandra tables, thread collections, `MODE`, required env vars) or delete it — a wrong runbook is worse than none.
- [low] `store_cassandra.go:465,480` → `slog.ErrorContext(ctx, …)`; drop the now-redundant hand-passed fields if the handler injects them.
- [low] Align `pretouch.go:12-14` with the actual sonic sites: add `ThreadUnreadAddedEvent`, `TeamsBatchRequest`; remove `InboxEvent`.
- [low] Delete `reactionShortcode` (`teamstransform.go:113`) and its test until reactions migrate; introduce `const modeDefault, modeTeams` and use them at `main.go:87,241,374`, `bootstrap.go:46`.
- [nitpick] Fix the mojibake at `teamstransform.go:72,94` and rename the `error` log key at `teamsbatch.go:96` to `failed`.

## 3. Architecture — score 4

### Evidence

- [medium] Consume-loop exit is invisible to readiness — `message-worker/main.go:297-301` — when `iter.Next()` errors (consumer deleted, iterator closed) the goroutine records `LoopFailed` and returns; the process stays up and `/readyz` keeps passing because the only check registered at `main.go:321-323` is `natsutil.HealthCheck(nc)`. A pod that has silently stopped persisting history reports healthy. `roomlist-worker/main.go:242-355` already solves this with a `consumeState` readiness check; `notification-worker/main.go:345-349` shares the gap.
- [medium] Enrichment columns violate the pin precondition (known, documented, unfixed) — `message-worker/store_cassandra.go:117-128`, `handler.go:102-115`, `handler.go:136-161` — plaintext creates bind `USING TIMESTAMP writeTS(CreatedAt)` (`store_cassandra.go:192,203,284,295,316`), but `sender` and `mentions` are re-resolved fail-open on every delivery, so a degraded first attempt and a healthy retry bind different values under one timestamp and Cassandra breaks the tie per cell by value. The row stays readable but may keep degraded/mixed enrichment permanently. CLAUDE.md names the fix (deterministic values from the canonical event, or a separate unpinned enrichment mutation); it has not been done.
- [medium] `USER_CACHE_SIZE`/`USER_CACHE_TTL` re-declared with their own `envDefault` — `message-worker/main.go:58-59` — the same tags appear in eight services (`broadcast-worker/main.go:73-74`, `message-gatekeeper/main.go:60-61`, `notification-worker/main.go:70-71`, …) while `pkg/userstore/ttlconfig.go:13-14` owns only `USER_L2_TTL`. CLAUDE.md: a shared knob is declared once in the owning package and mounted as a named field. Repo-wide, not service-specific, but this service is a violator.
- [low] Bootstrap-disabled path is not a no-op — `message-worker/bootstrap.go:58-64` — CLAUDE.md says the helper "no-ops when `Enabled=false`"; here it calls `js.Stream()` and fails startup if the stream is missing. Strictly better (fail-fast on a misprovisioned deploy) and tested (`bootstrap_test.go:385-391`), but it requires stream-info permission on the prod NATS account and diverges from the documented contract. Name+Subjects-only creation is honoured (`bootstrap.go:50-53`).
- [low] Consumer-owned interfaces scattered outside `store.go` — `message-worker/teamssender.go:436-447` (`identityResolver`, `HRIdentityStore`), `teamstransform.go:532-534` (`MessageTransformer`) — the layout rule puts store interfaces plus their `//go:generate` in `store.go`; `mock_hridentity_test.go` exists but its directive is not in `store.go:76-77`.
- [low] `README.md` describes a service that no longer exists — `message-worker/README.md:63-116` — claims it consumes `MESSAGES`, validates subscriptions, publishes `FANOUT-{siteID}`, replies on `chat.user.{account}.response.*`, and writes a `messages` table. Actual: consumes `MESSAGES-CANONICAL` `.created` only (`main.go:380`), writes `messages_by_room`/`messages_by_id`/`thread_messages_by_thread`. Misleads on-call.
- [nitpick] Mode is compared as raw string literals in four places — `main.go:87`, `main.go:241`, `main.go:374`, `bootstrap.go:46` — no `modeDefault`/`modeTeams` constants; a typo compiles.
- [nitpick] Duplicate Dockerfile — `message-worker/deploy/teams/Dockerfile` is byte-identical to `deploy/Dockerfile`; only the compose env differs (`MODE=teams`).

Verified correct (no finding): high-throughput pattern `cons.Messages()` + `sem` sized `MaxWorkers` + `PullMaxMessages(2*MaxWorkers)` + `wg` (`main.go:257-319`), loop goroutine counted in `wg` so shutdown cannot pass a message in flight; durables `message-worker`/`message-worker-teams` (`main.go:375,379`); backoff via `stream.DurableConsumerDefaults(stream.WithOutageRetryBudget(s, jsretry.DefaultBackoff))` (`main.go:373`), no `cc.BackOff`; zero bare `Nak()`/`NakWithDelay(0)` — all settles via `jsretry.Settle` (`handler.go:89`, `teamsbatch.go:308-318`); every subject via `pkg/subject` (`main.go:280,287,376,380`, `handler.go:827`) and OUTBOX via `outbox.Publish` (`handler.go:759,798`) with both types in `ConcurrentEventTypes` (`pkg/outbox/outbox.go:35,38`); `Store`/`ThreadStore` in `store.go:80-126`, constructor DI with publish injected as `PublishFunc` (`handler.go:53`); shutdown `iter.Stop → wg.Wait(timeout) → Drain → Cassandra → Mongo → Vault → Valkey → health → obs` (`main.go:331-359`); `jobguard.Run` wraps dispatch (`main.go:391`). Cassandra: plaintext pinned, encrypted unpinned, strips unpinned as separate UPDATEs (`store_cassandra.go:163-165,245,255,368,379,396`), pinned by `store_cassandra_writetime_test.go`. #484 degraded start: `WithDegradedStart` (`main.go:156`), best-effort `EnsureIndexes` (`main.go:210-214`), and `IndexGate.Ready` before every document-creating thread write (`store_mongo.go:61-65,93,105,121`) so a resumed consumer cannot insert ahead of room-service's unique index; `handler.go:207-234` orders Mongo before Cassandra so a Mongo outage NAKs before persisting.

### Recommendations

- [medium] Port roomlist-worker's `consumeState` readiness check into the consume goroutine — `main.go:297-301`, register alongside `natsutil.HealthCheck(nc)` at `main.go:321-323` — a dead loop then fails `/readyz` and Kubernetes restarts the pod instead of leaving history unpersisted.
- [medium] Make enrichment deterministic or separate — either project `sender`/`mentions` solely from the canonical event (gatekeeper already resolves them) or write them as an unpinned follow-up `UPDATE` — closes the documented per-cell mixed-enrichment window at `store_cassandra.go:117-128`.
- [medium] Move `USER_CACHE_SIZE`/`USER_CACHE_TTL` into `pkg/userstore` (e.g. `CacheConfig` next to `TTLConfig`) and mount it as a named field in every consumer — one declaration, one default; needs a small repo-wide PR.
- [low] Either update CLAUDE.md to sanction verify-on-disabled or revert `bootstrap.go:58-64` to a no-op — the contract and the code should agree, and ops must know prod needs `$JS.API.STREAM.INFO` permission.
- [low] Rewrite `README.md` to the current flow and consolidate `HRIdentityStore`/`MessageTransformer` (+ mockgen directive) into `store.go`; add `modeDefault`/`modeTeams` constants.

## 4. Test coverage — score 1

### Evidence

- [critical] coverage below repo minimum 80%, currently 56.3% — `message-worker/store_cassandra.go:454` — 481/854 statements; 24 of 69 functions at 0%. The whole store layer is integration-only: `store_cassandra.go:454` (UpdateParentMessageThreadRoomID), `:497` (GetQuotedParentSnapshot), `:550` (GetMessageCreatedAt), `:566` (GetMessageSender), all of `store_mongo.go:34-244` (12 methods), `hridentity.go:25-52`, `teamsbatch.go:38-74`, `main.go:76-360`. CLAUDE.md §4 floors this at score 1.
- [high] The parent `thread_room_id` stamp has no test in either suite — `message-worker/store_cassandra.go:454-489` — 0% unit; the `IF EXISTS` CAS and both `!applied` miss branches (`:464-470`, `:479-488`, whose own comment says a silent miss "permanently breaks thread reads") are never asserted. `integration_test.go:498-633` drives the handler through the stamp but never SELECTs the parent row's `thread_room_id` (the only such assertion, `:358`, is on the reply row).
- [medium] Encrypted create error paths are dead in the unit profile — `message-worker/store_cassandra.go:223-225` — `cipher.Encrypt` failure (`:223`, `:342`) and `executeBatch` failure in the encrypted/thread paths (`:256`, `:323`, `:398`) are zero-hit: `stubCipher` (`store_cassandra_writetime_test.go:20-30`) cannot fail and `store_cassandra_batch_test.go:282` covers only plaintext `SaveMessage`.
- [medium] Teams lane settle wiring untested — `message-worker/teamsbatch.go:59-74` — `consume` (poison Ack on corrupt frame / malformed JSON vs NAK on infra error) is 0% in both suites; `teamsbatch_integration_test.go:322` calls `handleBatch` directly and `teamsbatch_test.go:112-130` stops at `handleBatch`. `newTeamsBatchHandler` (`:38-53`) is also unit-0%.
- [medium] Thread-path store errors never reach the entry point — `message-worker/handler_test.go:2141-2151` — `stubHistoryAllMembers` plus 17 blanket `AddReplyAccounts(...).Return(nil).AnyTimes()` stubs (e.g. `handler_test.go:2570`) make `GetHistorySharedSince` error (`handler.go:665-667`), `AddReplyAccounts` errors (`:427-429`, `:692-694`) and the `handleThreadRoomAndSubscriptions`/`fanOutThreadUnread` NAK propagation (`:208-210`, `:230-232`) zero-hit. §4 requires store-error cases per handler method.
- [medium] `main` composition is untestable as written — `message-worker/main.go:215-233` — the publish closure that picks core `nc.PublishMsg` vs `js.PublishMsg(WithMsgID)` by `msgID`, the teams user-fanout closure (`:275-286`) and the shutdown ordering (`:331-359`) are inline in a 285-line `main`. `canonicalProcessor` was extracted (`:389`) precisely for testability; the publisher was not.
- [low] In-process NATS broker under `make test` — `message-worker/consumeloop_test.go:35-68` — untagged test starts `natsserver.NewServer` (§4: never connect to real NATS in unit tests) and relies on wall-clock assertions (`:236` Eventually 20s, `:264` Never 2s), adding ≥2s per run and timing sensitivity under `-race`.
- [low] Teams migration error branches unit-0% — `message-worker/teamssender.go:64-66` — store/publish failures (`:64`, `:72`, `:89`) and `teamstransform.go:38-40`, `:53-55`, `:60-62`, `:83-85`, `:99-101` (decode/resolve errors); `nats_metrics.go:48-52` counter fallback; `toMentionSet` non-empty branch `store_cassandra.go:47-56` never hit.

### Recommendations

- [critical] Extend the injectable-seam pattern already on `CassandraStore` (`store_cassandra.go:67-68` `newBatch`/`executeBatch`) with a `query`/`scanCAS` seam so `GetMessageSender`, `GetMessageCreatedAt`, `GetQuotedParentSnapshot` (incl. the `enc_meta == nil` contract error `:523-528` and decrypt error `:530`) and `UpdateParentMessageThreadRoomID` get unit tests — this alone moves ~70 statements and is the fastest route toward 80%.
- [high] Add `TestCassandraStore_UpdateParentMessageThreadRoomID` (integration): stamp lands on both tables; absent parent returns nil and logs; and add a parent-row `thread_room_id` SELECT to `TestHandler_Integration_ThreadReply` (`integration_test.go:569`).
- [medium] Add a failing cipher stub and `executeBatch` error rows to `saveCases()` (`store_cassandra_writetime_test.go:81`) so the three encrypted/thread wrap messages are asserted.
- [medium] Unit-test `teamsBatchHandler.consume` with `fakeJSMsg` (`handler_test.go:2490`): corrupt frame → Ack, malformed JSON → Ack, `SaveMessage` infra error → `NakWithDelay > 0`.
- [medium] Replace the blanket `AnyTimes` stubs in the thread-reply cases with explicit expectations and add `CreateThreadRoom error`, `GetHistorySharedSince error`, `AddReplyAccounts error` → NAK rows to `TestHandler_ProcessMessage` (`handler_test.go:30`).
- [medium] Extract `newPublisher(nc, js, publishMetrics) PublishFunc` from `main.go:215-233` and test the msgID branch; same for the teams user-fanout closure.
- [low] Move `consumeloop_test.go` behind `//go:build integration` on `testutil.NATS`, or drop the `assert.Never` and shorten the Eventually window.
