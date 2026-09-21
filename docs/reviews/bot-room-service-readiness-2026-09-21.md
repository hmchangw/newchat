# bot-room-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `1b686d0` (base `main`)  
**Overall score:** 2.5 / 5 (baseline 2026-09-01: 2.8, Δ -0.3)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

bot-room-service is the bot-platform counterpart of room-service, and its handler logic, error tiering and room-key rotation net read well — but three reviewers independently found the same broken wire contract. Its system messages publish a bare `model.Message` onto BOT-MESSAGES-CANONICAL while every consumer on that stream decodes a `MessageEvent` envelope, so each membership system message decodes to a zero-valued message: bot-message-worker tries to persist an empty partition key and NAK-loops it through the retry budget, and search-sync-worker drops it as poison. Every bot membership system message is lost from history, and the service's own tests pin the wrong shape. Two further contract breaks sit in MongoDB: rooms are written with a legacy `t:"c"/"d"` field instead of the canonical `type`, so room-service and the room-meta cache decode bot rooms as typeless, and subscription rows omit `joinedAt`, `open` and `roles`, which the member-list index and sidebar visibility depend on, so a locally-created membership and its federated copy diverge. The `member_added` dedup ID is keyed on the room/user/destination triple rather than the subscription ID that the remove path already uses, so a remove-then-re-add inside the duplicate window silently drops the re-add. Member batches are unbounded and processed as sequential N+1 round trips under one 10-second budget, and the deferred key-rotation safety net runs on the request context, so when the failure is the timeout the net cannot fire. Coverage is 49.4% with 14 of 30 functions at zero, no mockgen mocks, and no `deploy/azure-pipelines.yml` at all — a gap shared by all three bot services.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 2 |
| Performance | 3 |

**Findings by severity:** 1 critical, 10 high, 18 medium, 19 low, 7 nitpick (55 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 3

### Evidence

- [high] Unit coverage is 49.4% (204/413 stmts), below the 80% floor CLAUDE.md marks as must-not-merge — `bot-room-service/store_mongo.go:23-196`, `bot-room-service/main.go:64-171` — Every `store_mongo.go` function, `Register`, `run` and `parsePeers` are 0%. `store_mongo.go` is covered only by `integration_test.go` (tagged), so the unit profile never sees it; `parsePeers` is pure string logic with no test at all.
- [high] No `deploy/azure-pipelines.yml`, so no per-service CI gate exists — `bot-room-service/deploy/` (only `Dockerfile`, `docker-compose.yml`) — CLAUDE.md §"Docker" and §"When Creating Services" require it; no root pipeline references `bot-room-service` either (`grep -rl bot-room-service --include=*.yml` hits only the compose file). Same gap in `bot-message-handler`, `bot-message-worker`, `broadcast-worker`, `notification-worker`, `roomlist-worker`, `push-notification-service`, so treat it as a bot-platform-wide omission, not a one-off.
- [medium] Store mocking bypasses the mandated mockgen path — `bot-room-service/store.go:13` (no `//go:generate mockgen` directive), no `mock_store_test.go`; `bot-room-service/roomkey_test.go:11-13` admits "no gomock/mockgen infrastructure" — 25 sibling services carry `mock_store_test.go`. The hand fake `fakeStore` (`handler_test.go:33-47`) calls `f.InsertRoomFn`/`UpsertSubscriptionFn`/`DeleteSubscriptionFn`/`FindUserFn` without nil checks, so any test that omits one panics with a nil func instead of a readable assertion.
- [medium] `parseIdentity` error paths (missing header, malformed JSON, empty id/account) have no test — `bot-room-service/handler.go:722-738` (66.7% covered; `grep parseIdentity|BotInvalidHeader *_test.go` returns nothing) — this is the caller-identity gate for all five RPCs; the `errcode.WithCause(err)` branch at `:731` is unexercised.
- [medium] No table-driven tests despite many scenario-per-handler cases — `bot-room-service/handler_test.go` (0 `t.Run`, 41 top-level `TestHandleX_Scenario` funcs) — CLAUDE.md §4 prefers table-driven subtests for input/output variations; only `handler_remove_key_test.go:185-193` uses the pattern. The rest of §4 is respected: `package main`, testify, `TestMain` via `testutil.RunTests` (`integration_test.go:20`), no shared mutable state.
- [low] Bare `return err` — `bot-room-service/handler.go:612` — `rotateAndFanOut` returns `roomkeystore.CommitRotation`'s error unwrapped; CLAUDE.md §3 says always wrap with what this function was doing. The callee does wrap (`pkg/roomkeystore/commit.go:29,38`), so the log line is still readable, but the rule is violated.
- [low] Store model structs carry no `json`/`bson` tags and field names are hand-mapped in three places — `bot-room-service/store.go:70-108`, `store_mongo.go:48-55` (insert map), `:72-84` (decode struct), `:196-219` (`participantBSON`) — CLAUDE.md §3 "All model structs get both json and bson tags". The triple mapping is where a renamed column silently diverges between insert and read.
- [low] `FindUser` projection over-fetches — `bot-room-service/store_mongo.go:158` selects `engName`, `chineseName`, `roles`; the handler reads only `u.ID`, `u.Account`, `u.SiteID` (`handler.go:126,155,254-256,335-336,495`). CLAUDE.md MongoDB rule: project only the fields the caller needs.
- [low] Service JetStream-publishes to OUTBOX and BOT-MESSAGES-CANONICAL but has no `Bootstrap bootstrapConfig`/`bootstrap.go` and its compose sets no `BOOTSTRAP_STREAMS` — `bot-room-service/main.go:25-62`, `deploy/docker-compose.yml` — CLAUDE.md "each such service's config includes `Bootstrap`"; sibling publisher `room-service/main.go:70,263` follows it. Dev-only impact (fresh NATS needs another service up first).
- [nitpick] Misleading dedup comment — `bot-room-service/sysmsg.go:61` claims the suffix "defeats double-emit on retry", but every suffix is `h.now().UnixMilli()` (`handler.go:278,372,545`), so a client retry mints a new `Nats-Msg-Id`. Handler-level idempotency (ErrDuplicate/all-dup batch/wasThere=false) is what actually prevents a second sysmsg; the comment should say so.
- [nitpick] Log key inconsistency: `"room_id"` at `bot-room-service/handler.go:149` vs `"roomID"` everywhere else (`handler.go:457,558,563`, `sysmsg.go:42,58,67`) — breaks log-field filtering across the service.
- [nitpick] Hard-coded 2s sysmsg publish timeout — `bot-room-service/sysmsg.go:62` — while `REQUEST_TIMEOUT` (`main.go:58`) governs the handler; not configurable, not tied to the request budget.

### Recommendations

- [high] Add `deploy/azure-pipelines.yml` cloned from `room-service/deploy/azure-pipelines.yml` (paths `bot-room-service/`, `pkg/`) — restores the lint/test/SAST gate for this service; do the same for the other bot-platform services listed above.
- [high] Raise unit coverage past 80%: add a table-driven `TestParsePeers` (`main.go:171`), a `TestParseIdentity` table (missing/malformed/empty-id/valid), tests for `Register` via `natsrouter` route inspection, and untested branches in `handleDMEnsure` (`FindRoom` warn path `:145-150`) and `handleCreate` (member not-found `:246-249`, remote seed `:263-268`) — these are the cheapest 100+ statements.
- [medium] Add `//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main` to `store.go:13` and migrate `fakeStore`/`fakeKeyStore` to gomock — removes the nil-func panic hazard and aligns with the 25 sibling services.
- [medium] Wrap the error at `handler.go:612`: `return fmt.Errorf("commit key rotation: %w", err)`.
- [low] Tag `Room`/`Participant`/`Subscription` with `bson`/`json` (`store.go:70-108`) and marshal them directly in `InsertRoom`/`FindRoom`/`participantBSON` — collapses three hand-kept field maps into one.
- [low] Trim the `FindUser` projection at `store_mongo.go:158` to `_id, account, siteId`.
- [nitpick] Normalise the `room_id` key at `handler.go:149` to `roomID`; fix the `sysmsg.go:61` comment.

## 3. Architecture — score 3

### Evidence

- [high] Sysmsg publishes a bare `model.Message` onto BOT-MESSAGES-CANONICAL, but every consumer decodes a `model.MessageEvent` envelope — `sysmsg.go:46-65` — The stream's producer contract is `bot-message-handler/handler.go:188-193`; `bot-message-worker/handler.go:32-39` and `search-sync-worker/messages.go:180-185` both unmarshal into `MessageEvent`. A bare `Message` decodes without error into a zero-value `evt.Message`, so bot-message-worker hands `SaveMessage` (`bot-message-worker/store_cassandra.go:88-121`, no validation) empty ids and a zero `created_at` — an unusable row, or an InvalidRequest that `isPermanent` (`:70-75`) treats as transient and NAK-loops — and search-sync-worker Ack-drops each as poison (`search-sync-worker/handler.go:124-131`). Every membership system message is lost from history. The service's own test asserts the wrong shape (`handler_test.go:417-418`) and the comment at `sysmsg.go:17` ("same wire shape as bot-message-handler") is false. The bare form also lacks the mandated event-level `Timestamp`.
- [medium] No startup verification of the two streams this service publishes to — `main.go:129,144` — Siblings `room-service/bootstrap.go:57-59` and `bot-message-handler/bootstrap.go:35-37` fail fast in production via `js.Stream()`. Here a misprovisioned OUTBOX or BOT-MESSAGES-CANONICAL surfaces only at first publish; for sysmsgs that is a `slog.Warn` (`sysmsg.go:65-68`), so the loss is silent. No `bootstrapConfig`/`BOOTSTRAP_` field exists.
- [medium] `store.go` has no `//go:generate mockgen` directive and no `mock_store_test.go`; tests use hand-written fakes — `store.go:13`, `handler_test.go:24-54`, `roomkey_test.go:11-13` — The per-service layout puts the mockgen directive in `store.go`; `roomkey_test.go:13` documents the gap. The repo-wide "mocks not stale" fact cannot cover a service that generates nothing.
- [medium] `handleCreate` is a multi-write, non-idempotent request/reply with a server-generated id — `handler.go:195-231` — Room insert, owner subscription, key `Set` and N member upserts run sequentially with no rollback and no idempotency key, so a caller's timeout-retry creates a second room and a mid-sequence failure orphans one. The `ErrDuplicate → Conflict` branch (`:208-209`) can never fire for a fresh random id.
- [medium] `deploy/azure-pipelines.yml` is absent — `bot-room-service/deploy/` — CLAUDE.md: "Each service also has `<service>/deploy/azure-pipelines.yml`"; 29 of 37 deploy dirs have it, `room-service` included; all three `bot-*` services lack it.
- [low] Dead dependency: `allSiteIDs` is stored on the handler but never read — `handler.go:62,80`; `main.go:140,171-184` — `parsePeers` exists only to fill it; federation targets come from each user's `SiteID`. It misleads readers about how destinations are chosen.
- [low] Sysmsg dedup id is not deterministic across retries, contrary to its comment — `sysmsg.go:63-64`; `handler.go:278,372,545` — The suffix is this call's wall-clock millisecond, so a retry never reproduces the id; "defeats double-emit on retry" holds only because add/remove retries are all-dup and emit nothing. The spec asked for `bot-create:{roomID}` (`docs/superpowers/specs/2026-07-15-bot-messaging-pipeline-design.md:349`).
- [low] Two collaborators are injected post-construction; one silently disables a feature when forgotten — `main.go:142-144`; `sysmsg.go:36-38` — `valkey` and `sysmsgPub` bypass `newHandler`; a nil `sysmsgPub` turns every sysmsg into an unlogged no-op. Fleet pattern (`room-service/main.go:385`), but a required collaborator should not be optional by omission.

### Recommendations

- [high] Wrap the sysmsg in `model.MessageEvent{Event: model.EventCreated, Message: msg, SiteID: h.siteID, Timestamp: now.UnixMilli()}`, fix the `sysmsg.go:17` comment, and change `handler_test.go:417-418` (and `:470`) to decode `MessageEvent` — `sysmsg.go:46-58` — Restores the stream contract so bot-message-worker persists and search-sync-worker filters the sysmsg instead of poisoning both.
- [high] Add a cross-service wire-compat test that runs bot-room-service's sysmsg bytes through `bot-message-worker`'s decoder and asserts a non-empty `evt.Message.ID` — new `_test.go` in either service — The shape drifted under a green suite because each side only tested itself.
- [medium] Add `bootstrap.go` with `Bootstrap bootstrapConfig` (`BOOTSTRAP_STREAMS`, default false) that verifies `stream.Outbox(siteID)` and `stream.BotMessagesCanonical(siteID)` via `js.Stream()` when disabled — `main.go` after `:97` — Mirrors `bot-message-handler/bootstrap.go:35-37`; a misprovisioned deploy fails at startup.
- [medium] Add `//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main` and migrate `fakeStore`/`fakeKeyStore` to gomock — `store.go:13,53` — Brings the service under `make generate`'s staleness guard.
- [medium] Give `handleCreate` an idempotency key (client `requestId`, or derive the room id from `(botID, requestId)` and treat `ErrDuplicate` as replay) — `handler.go:195-212` — A BP retry converges on one room; the Conflict branch becomes reachable.
- [low] Delete `allSiteIDs`, its `newHandler` parameter, `parsePeers` and `config.AllSiteIDs` — `handler.go:62,76-80`; `main.go:29-30,140,171-184` — Removes a dead knob.
- [low] Move `sysmsgPub` and `valkey` into `newHandler`'s parameters — `handler.go:76-86`; `main.go:141-144` — Makes the sysmsg publisher a declared dependency.
