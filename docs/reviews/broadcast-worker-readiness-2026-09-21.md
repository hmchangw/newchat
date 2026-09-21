# broadcast-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `1a95503` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.5, Δ -0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The CLAUDE.md hot-path contract holds under inspection: `sonic` on every per-message codec call with `jsonwarm.Pretouch` at boot, exactly one MongoDB write and it is buffered, coalesced and never returned to the handler, `jsretry.Settle` only, every subject from `pkg/subject`, member resolution batched with no N+1 — code quality and architecture both score 4. The high finding is a client contract regression: `buildRoomEvent` never sets `systemMsg`, a field #382 added to this exact function and #188 dropped without touching `docs/client-api.md` or `events.md`, so in encrypted rooms a rename or member-add now advances unread state and reorders every member's sidebar; no test asserts the field. Three reviewers flagged the same fleet pattern from here: the "must match across services" knobs (`ROOM_KEY_RETIRED_TTL`, `ROOM_LOCALITY_GRACE`, `USER_CACHE_*`) are re-declared per service because no owning config type exists. Performance is sound in sizing and batching but has no per-message deadline or heartbeat, so a slow Mongo read holds a `MAX_WORKERS` slot past `AckWait` and the redelivery fans the same message out twice; thread fan-out spawns an unbounded goroutine per follower; the preview seal is awaited before fan-out. DM mutations read membership from a different, uncached, differently-gated source than creates, so an unsubscribed botDM member gets edits but not messages. Coverage is 68.1%, every mutation handler's store-error branch is untested, the preview-writer shed path is asserted only by comment, and the documented dev harness cannot start.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 0 critical, 4 high, 16 medium, 19 low, 11 nitpick (50 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [low] `encoding/json` on a publish path of a sonic-mandated hot-path worker — `broadcast-worker/roomactivity.go:187` — `json.Marshal(evt)` for `RoomActivityEvent`; CLAUDE.md names broadcast-worker as a sonic codec worker. Impact is small (throttled to one publish per room per `ROOM_ACTIVITY_REFRESH_INTERVAL`), but it is the one codec inconsistency in the service. (`handler.go:7` imports `encoding/json` only for the `json.RawMessage` type at :1004/:1044, which is fine.)
- [low] Bare `return err` in five handler call sites — `broadcast-worker/handler.go:294`, `:298`, `:427`, `:698`, `:855` — CLAUDE.md §3 says never return bare `err`. The callees (`publishChannelEvent`, `publishDMEvents`, `publishRoomEvent`, `publishMutation`) already wrap with room/subject context so no diagnostic is lost, but the pattern is inconsistent with the sibling sites (`:335`, `:564`, `:627`, `:634`, `:736`) that do wrap.
- [low] Mixed structured-log key casing in one file — `broadcast-worker/handler.go:208` (`messageID`), `:289` (`room_id`), `:818-821` (`roomID`, `siteID`) — 17 `room_id` vs 4 `roomID`, 9 `messageID`, 4 `siteID`, 4 `parentMessageID`. A log query on `room_id` misses the `handleReacted` drop lines; CLAUDE.md asks for structured fields but the key vocabulary is not stable.
- [low] `skipped` from `cassandra.DecodeAttachments` discarded without a comment — `broadcast-worker/handler.go:1264` — `decoded, _ := cassandra.DecodeAttachments(msg.Attachments)`. The identical condition is treated as an error one file over (`preview.go:64-68`, "a skip means malformed upstream"), so the client event silently drops malformed attachments while the preview refuses them. CLAUDE.md: comment if intentionally discarded.
- [nitpick] Cross-service notification type is a magic string — `broadcast-worker/handler.go:867` — `Type: "reaction"`; `pkg/model/event.go:155` only documents it in a comment, so the producer and notification-worker cannot share a constant and drift is uncatchable at compile time.
- [nitpick] Redundant loop-variable copy — `broadcast-worker/handler.go:1295` — `account := account` is a no-op under go 1.25 (`store_mongo.go:141` already relies on per-iteration loop vars in its own `#nosec G601` note).
- [nitpick] Duplicated sender-composition logic — `broadcast-worker/handler.go:1253-1263` vs `broadcast-worker/preview.go:87-95` — both build a `model.Participant` from `msg` + `userMap`, with different name fallbacks (account string vs blank + `BotAwareDisplayName`). The divergence looks intentional (preview mirrors history-service's walk) but is undocumented at the client-message site.
- [nitpick] Anonymous projection struct declared twice — `broadcast-worker/preview.go:137-139` and `:143-145` — `struct{ Name string \`bson:"name"\` }` should be a named type.
- [nitpick] Pretouch list omits two sonic-marshalled types — `broadcast-worker/pretouch.go:11-19` vs `handler.go:470` (`SubscriptionMentionEvent`) and `:874` (`NotificationEvent`) — first reaction/remote-mention message pays codec compilation on the fan-out goroutine.

### Recommendations

- [low] Switch `roomactivity.go:187` to `sonic.Marshal` — `broadcast-worker/roomactivity.go:187` — closes the codec split; `RoomActivityEvent` has no map fields and no byte-identity consumer, so the sonic wire-compat caveat does not apply.
- [low] Wrap the five bare returns with the operation being performed — `broadcast-worker/handler.go:294,298,427,698,855` — e.g. `fmt.Errorf("fan out created event for room %s: %w", meta.ID, err)`, matching the sibling sites.
- [low] Normalise log keys to snake_case (`room_id`, `message_id`, `site_id`, `parent_message_id`) — `broadcast-worker/handler.go` (all 21 camelCase sites) — makes room/message log correlation a single query across the service; consider enabling `sloglint` key-naming in `.golangci.yml` to hold it.
- [low] Decide one policy for malformed attachment blobs and comment it — `broadcast-worker/handler.go:1264` — either log `skipped` with `room_id`/`message_id` (lenient, matches `DecodeAttachments`' stated intent) or fail like `preview.go:67`; today the same bad blob is silently dropped for clients and rejected for the preview.
- [nitpick] Add `model.NotificationTypeReaction = "reaction"` in `pkg/model/event.go:155` and use it at `handler.go:867` and in notification-worker.
- [nitpick] Name the apps projection type once and delete `account := account` — `broadcast-worker/preview.go:137`, `handler.go:1295`.

## 3. Architecture — score 4

### Evidence

- [medium] DM/botDM fan-out has two membership sources with different gates — `broadcast-worker/handler.go:918` and `handler.go:701` — `publishMutation` and `publishThreadMetadata` iterate `room.Accounts` from the uncached, breaker-fenced `GetRoom` (`store_mongo.go:87`) with only the `isBot` filter, while `publishDMEvents` (`handler.go:1174-1200`) reads `ListRoomMembers` through roomsubcache and applies the botDM `IsSubscribed` gate. A human who unsubscribed from a botDM receives edit/delete/pin/react/thread-badge events (resurrecting the room the comment at `handler.go:1193-1196` says must not be resurrected) but not new messages; and during a Mongo outage creates keep flowing from L2 while every mutation NAKs for the whole outage-retry window.
- [medium] The cross-site test harness cannot start — `broadcast-worker/deploy/user/docker-compose.test.yml:36-37` — it sets `INPUT_STREAM`/`INPUT_SUBJECT_FILTER`, which no `config` field reads (`main.go:45-122`), and omits `MODE`, tagged `env:"MODE,required"` at `main.go:90`. `deploy/Makefile up` therefore exits at `env.ParseAs` (`main.go:128-131`); the seed/verify scenarios under `deploy/test/` are dead.
- [low] Bot deployment shares the user deployment's core-NATS lane — `broadcast-worker/main.go:424` — the queue group is the literal `"broadcast-worker"` on the unscoped `ServerBroadcastWildcard(siteID)`, while the JetStream durable is mode-scoped via `cfg.Mode.ConsumerName` (`main.go:338`). `message-worker/handler.go:827` publishes user thread-badge events there, so they are load-balanced across the user and bot pipelines. Same binary and Mongo today, but a config divergence between the two deployments (`ROOM_SUBJECT_MODE`, `THREAD_VIEW_SUBJECT_ENABLED`) silently misroutes a share.
- [low] Shared knobs re-declared per service — `broadcast-worker/main.go:73-82` — `USER_CACHE_SIZE/TTL`, `ROOM_META_CACHE_SIZE/TTL`, `ROOM_KEY_GRACE_PERIOD`, `ROOM_KEY_CACHE_TTL`, `ROOM_KEY_RETIRED_TTL` each carry their own env tag and `envDefault` here and in message-gatekeeper, message-worker, notification-worker, room-worker, room-service and bot-room-service (grep-verified). CLAUDE.md requires one declaration in the owning package; `ROOM_KEY_RETIRED_TTL` is the one CLAUDE.md says MUST match across four services and has no `roomkeystore` config type to mount.
- [low] `bootstrapStreams` verifies instead of no-ops when disabled — `broadcast-worker/bootstrap.go:38-41` — `js.Stream()` failure exits the process, so a site whose IaC has not yet provisioned MESSAGES-CANONICAL cannot start the worker. Matches `roomlist-worker/bootstrap.go:41`, so it is a repo pattern, but it contradicts the "no-ops when Enabled=false" contract. Subjects also come from `wiring.CanonicalWildcard` (`main.go:333`) rather than `stream.MessagesCanonical(siteID).Subjects`; byte-identical today (`pkg/stream/stream.go:22-27`, `pkg/subject/subject.go:700`) but two sources of truth.
- [low] Server-broadcast lane is fire-and-forget over core NATS — `broadcast-worker/handler.go:217-239` — `HandleServerBroadcast` performs a Mongo `GetRoom` (`handler.go:666`) and drops on any error or during a restart; thread reply-count badges are lost with no retry. Documented at `main.go:422-423`, so a design decision, but it is the one delivery path in the service with no durability.
- [nitpick] Preview seal runs before the read that gates the message — `broadcast-worker/handler.go:272` vs `handler.go:280` — `previewForInserted` spends up to `previewSealTimeout` (`preview.go:32`) of cipher/app-name work, then `GetRoomMeta` may fail and NAK; every redelivery repeats the seal. Only the write is deferred; the seal is on the hot path.
- [nitpick] Exported identifiers in `package main` — `broadcast-worker/handler.go:50-101`, `keycache.go:49`, `store_mongo.go:53` — `Publisher`, `Handler`, `NewHandler`, `NewMongoStore`, `CachedKeyProvider`; CLAUDE.md says keep handler/store implementations unexported. Repo-wide pattern.

### Recommendations

- [medium] Route DM/botDM mutations through `ListRoomMembers` and reuse the `IsSubscribed` gate — `broadcast-worker/handler.go:918`, `handler.go:701` — one membership source for all DM fan-out; closes the resurrected-botDM inconsistency and lets mutations survive a Mongo outage from L2 exactly as creates do.
- [medium] Fix or delete the harness — `broadcast-worker/deploy/user/docker-compose.test.yml:36-37` — replace the two dead vars with `MODE=user`; add a `bot` scenario or state the harness is user-only. Dead harnesses erode trust in `deploy/test/verify/*`.
- [low] Scope the core-NATS queue group by pipeline — `broadcast-worker/main.go:424` — `cfg.Mode.ConsumerName("broadcast-worker")`, and either have the bot pipeline subscribe a bot-scoped subject or not subscribe at all, so user events never land on the bot deployment.
- [low] Move the room-key knobs into a `roomkeystore.Config` (`ROOM_KEY_GRACE_PERIOD`, `ROOM_KEY_RETIRED_TTL`) and the L1 cache sizes into `userstore`/`roommetacache` config types, mounted as named fields — `broadcast-worker/main.go:73-82` — the tag-level default then cannot diverge across the four services CLAUDE.md says must agree.
- [low] Either change CLAUDE.md to say "verify, fail fast" or make `bootstrapStreams` a true no-op when disabled and pass `stream.MessagesCanonical(siteID).Subjects` — `broadcast-worker/bootstrap.go:28-42`, `main.go:333` — so the documented contract and the two source-of-truth strings converge.
- [nitpick] Seal the preview after `GetRoomMeta` succeeds — `broadcast-worker/handler.go:272-283` — no wasted cipher/app-name work on a redelivery that the meta read will NAK anyway.

## 4. Test coverage — score 2

### Evidence

- [high] coverage below repo minimum 80%, currently 68.1% — `broadcast-worker/handler.go:181` — 736/1081 statements from the repo-wide unit run; CLAUDE.md §4 makes 80% a merge gate. The shortfall is concentrated in `main.go` (`main` 0%, 124–509) and the Mongo store (`store_mongo.go:25,87,110,122,136,195,222` at 0–17%), whose real coverage lives only behind the `integration` tag.
- [high] store-error paths on every mutation handler are untested — `broadcast-worker/handler.go:404` — the `GetRoom` error branch has zero hits at 404 (edit), 552 (thread edit), 604 (thread delete), 730 (delete), 767 (pin), 792 (unpin), 837 (react). CLAUDE.md §4 requires store-error tests per handler; only the created path (`GetRoomMeta`, handler_test.go:397–470, 708) and the server-broadcast path (handler_test.go:2298) cover a store failure.
- [medium] preview-writer shed branch has no test — `broadcast-worker/preview_writer.go:127` — the `default:` arm that fires when `pendingPreviews >= maxPendingPreviews` (127–133) is the only zero block in the file, and no `_test.go` references `maxPendingPreviews` or `pendingPreviews`. It is the memory bound (#289) and the guard against re-introducing #224; both are asserted only by comment.
- [medium] production settle composition never executed — `broadcast-worker/main.go:526` — `broadcastProcessor` (0%) is where `settleBackoff(err)` selects `BackpressureBackoff` for a shedding history-service. `consumeloop_test.go:437–438` and `:545` re-implement the processor with a hard-coded `jsretry.LowLatencyBackoff`, so the backoff selection is verified only in isolation (`backoff_test.go:16`), and `natsPublisher.Publish` (`main.go:512`, the real fan-out publisher with its metrics hook) is 0% too.
- [medium] embedded NATS server in an untagged unit test — `broadcast-worker/consumeloop_test.go:89` — `natsserver.NewServer` with JetStream runs under plain `make test`; CLAUDE.md §4 says unit tests never connect to real NATS. The tests are valuable (they drive `natsmetrics.Consume` + `guardedProcessor`), but belong under `//go:build integration` using `testutil.NATS(t)`.
- [medium] DM/botDM thread-badge branch untested — `broadcast-worker/handler.go:700` — `publishThreadMetadata` is 50%: the per-member loop (701–707, including the returned publish error) and the `default` arm (709) have zero hits. Every `TestHandleServerBroadcast_*` and `TestHandleThreadDeleted_*WithBadgeUpdate` case uses a channel room. Also the TShow=true deleted-reply badge at `handler.go:741` is never reached.
- [medium] DM mutation publish failure untested — `broadcast-worker/handler.go:923` — the log-and-continue arm (923–933) and the `MarkTerminalFromContext` at 936–938 in `publishMutation` have zero hits; `failOn` publishers are only used for channel lanes (handler_test.go:1163, 4207) and thread accounts (nats_metrics_test.go:74).
- [low] encrypted-edit key failure untested — `broadcast-worker/handler.go:993` — `encryptEditedContent` error branches (993, 997, 1001) and the caller at 422–424 are zero; key-store nil/error is only exercised on the created path (handler_test.go:585–620) and the thread-view seal (3689).
- [low] two production wrappers have no test at all — `broadcast-worker/metacache.go:18` — `cachedMetaStore` (the L1 wired at `main.go:301`, sitting on every message) and `appNameRepo.lookup` (`preview.go:150`, the `assistant.name` Mongo filter on the bot-preview path) are untested in both the unit and integration suites.
- [low] unknown-event / malformed-payload arms uncovered — `broadcast-worker/handler.go:205` — `HandleMessage` default (205–210) and `HandleServerBroadcast` unmarshal-error and default arms (219–224, 234–237) have zero hits despite `TestHandleMessage_DispatchesByEvent`.
- [nitpick] integration NATS conn closed instead of drained — `broadcast-worker/integration_test.go:582` — `t.Cleanup(nc.Close)` where CLAUDE.md asks for Drain/Shutdown; the three other tests (407, 465, 517) do it right.

### Recommendations

- [high] Add a table-driven `TestHandler_Mutations_GetRoomError` covering edit/delete/pin/unpin/react/thread-edit/thread-delete with `store.EXPECT().GetRoom(...).Return(nil, err)` and `assert.Empty(pub.records)` — `handler.go:404,552,604,730,767,792,837` — closes the §4 store-error requirement and ~14 statements in one test.
- [high] Test the shed path: buffer `maxPendingPreviews+1` rooms with sealed previews, flush, assert the last room has `pvw == nil && pvwFailed == true` and `pendingPreviews` decrements when a shed room later heals — `preview_writer.go:125–133` — locks in the #224/#289 contract the comment describes.
- [medium] Make `consumeloop_test.go` build `broadcastProcessor(handler)` instead of a local closure, and add a case where the parent fetcher returns `errcode.Unavailable` and the redelivery gap is observed to be `BackpressureBackoff[0]` — `main.go:526–551` — the shedding schedule is the reason `settleBackoff` exists.
- [medium] Move `consumeloop_test.go` under `//go:build integration` on `testutil.NATS(t)` — `consumeloop_test.go:89` — restores the unit/integration boundary and takes the embedded server out of `make test`.
- [medium] Add DM-room variants of the thread badge (`HandleServerBroadcast` and `EventDeleted` with `TShow=true`, `NewTCount` set) with one member publish failing — `handler.go:700–707, 741` — verifies the returned error and the bot skip.
- [medium] Add a DM edit/delete case with `failOn` for one member, asserting the other member still receives the event and the handler returns nil — `handler.go:923–938`.
- [low] Add an integration test for `appNameRepo.lookup` (hit and miss on `assistant.name`) and a unit test for `newCachedMetaStore` hit/miss counting through `MockStore` — `preview.go:150`, `metacache.go:18`.
