# room-service — Production Readiness Review

**Service:** `room-service`
**Date:** 2026-08-31
**Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents (code quality, architecture, test coverage, maintainability, integration, performance), each judging against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The most RPC-dense service in the fleet: 31 client-facing handlers across ~10 domains in a single 2,677-line `handler.go`. The engineering inside is good — error wrapping and `errcode` tiering are exemplary, the federation boundary holds cleanly through OUTBOX, all nine federated event types are correctly partitioned, and the Mongo layer shows deliberate performance work (precise projections, over-fetch+1 pagination, batched Go-side rollups replacing correlated `$lookup`s). What drags the score down is *shape and safety net*: a 47-method god store interface, a constructor that takes 13–14 positional args and is then finished by 11 post-construction field pokes, and **57.2% coverage** with `store_mongo.go` at 3.4%. The single most consequential defect is a **request-ID-derived OUTBOX dedup key that collides across the multi-row `moveChat` rebalance**, silently dropping all but one federated event.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

### Findings by severity

| Severity | Count |
|----------|-------|
| critical | 1 |
| high | 9 |
| medium | 21 |
| low | 14 |
| nitpick | 6 |
| **Total** | **51** |

> **Audit-coverage caveat.** `gosec` and the 18 repo-owned `semgrep` rules ran clean repo-wide. `govulncheck` and the `semgrep` registry packs could **not** run (egress policy blocks `vuln.go.dev` / `semgrep.dev`), so dependency-CVE coverage is unverified and must be re-run before shipping.

---

## 2. Go code quality — 4 / 5

Error handling, `errcode` tiering and wrapping discipline are exemplary across ~2,700 lines of handler and ~2,100 lines of store. The deductions are logging-context drift and a constructor that has outgrown positional DI.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **17 `slog.*` sites inside request handlers pass no `context.Context` and no `request_id`**, so they emit with no `trace_id`/`span_id`/`request_id` | `handler.go:1185`, `:1427`, `:1516`, `:1529`, `:1542`, `:1548`, `:1554`, `:1729`, `:1871`, `:1905`, `:1916`, `:1921`, `:1927`, `:2243` |
| medium | Two `slog.Debug` calls are **dead code — they can never emit**. `logctx.Handler.Enabled` gates sub-INFO records on `honoredThreshold(ctx)`, which defaults to `LevelInfo` when the ctx carries no admission; `slog.Debug` uses `context.Background()`, so DEBUG is dropped even for a request the operator explicitly admitted via `DEBUG_LOG_*` | `handler.go:1246`, `:1981`; `pkg/logctx/handler.go:30-35`, `:60-65` |
| medium | `NewHandler` takes 14 positional parameters, then `main.go` mutates 11 more dependencies onto the struct after construction | `handler.go:102`; `main.go:373-383` |
| medium | `handler.go` is 2,677 lines / 104 KB in one file; `addMembers` 165 lines, `roomRestricted` 161, `messageRead` 135 | `handler.go:887`, `:2033`, `:1381` |
| low | Exported constructor returns an unexported type — `NewNATSMemberListClient` returns `*natsMemberListClient`, though `MemberListClient` exists at `:27` | `memberlist_client.go:55` |
| low | Broad export surface in a `package main` service where nothing external can consume it | `store.go:13-17`, `:61`; `store_mongo.go:26`; `handler.go:46` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | `ReadReceiptRow` and `RoomBotAppEntry` carry `bson` tags only | `store.go:47-52`, `:56-59` |

The 17 uncorrelated log sites are **drift, not ignorance** — `handler.go:869`, `:1751`, `:2193` already use `slog.ErrorContext(ctx, …, "request_id", natsutil.RequestIDFromContext(ctx))`. That matters because the affected lines are the read-receipt and thread-read fan-out failures: the highest-value error logs in the service, and currently the ones an operator cannot join to a request.

### Recommendations
- `high` — Convert all 17 sites to the `*Context` variants with `ctx` + `request_id`, matching `handler.go:869`. Then add a repo-owned semgrep rule under `.semgrep/` (with its `.go` fixture, per §2) banning a bare `slog.Error|Warn|Info|Debug(` inside any function holding a `context.Context` — otherwise this drifts straight back.
- `medium` — Rewrite the two dead `slog.Debug` calls as `slog.Log(ctx, logctx.LevelFlow, …)`, the pattern already used at `handler.go:407`, `:1040`. As written the `DEBUG_LOG_*` knob buys nothing on these paths.
- `medium` — Replace the 14-arg `NewHandler` + 11 trailing assignments with a required-deps struct plus functional options, mirroring `StoreOption`/`memberListClientOption` already in this service.
- `medium` — Split `handler.go` along its existing seams: `handler_read.go` (~1381–1935), `handler_members.go` (~654–1110), `handler_chatlist.go` (~2340+).
- `low` — Return `MemberListClient` from `NewNATSMemberListClient`; unexport the store implementation.
- `low` — Re-run `make sast-vuln` with `vuln.go.dev` reachable before shipping.

---

## 3. Architecture — 3 / 5

The federation, subject, bootstrap and shutdown boundaries are all correct and carefully reasoned. The service has, however, clearly outgrown the flat layout.

### Verified clean (and load-bearing)

**The federation boundary holds.** Every cross-site relay goes through `outbox.Publish` on the local OUTBOX; there is no direct remote-INBOX publish anywhere in the service (`handler.go:882-885`). All nine event types it emits — `InboxRoleUpdated`, `InboxSubscriptionRead`, `InboxThreadRead`, `InboxThreadReadAll`, `InboxSubscriptionMuteToggled`/`FavoriteToggled`/`SectionMoved`/`Opened`, `InboxRoomRestricted` — are present in `pkg/outbox.ConcurrentEventTypes` (`pkg/outbox/outbox.go:20-46`). Also verified: no `os.Getenv`; zero raw `fmt.Sprintf` subject building across all 41 sites; stream configs from `pkg/stream`; typed `caarlos0/env` config with fail-fast validation (`main.go:170-205`); `pkg/shutdown.Wait` in the documented order (`main.go:395-420`); the two goroutines in `requireMembershipAndGetRoom` are `WaitGroup`-bounded (`handler.go:538-547`).

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | Mongo driver semantics decided in the **handler** layer: `handler.go` imports the mongo driver and branches on `mongo.ErrNoDocuments`; `handler_teams.go:192` branches on `mongo.IsDuplicateKeyError`. A non-Mongo `RoomStore` cannot satisfy the handler's contract | `handler.go:18`, `:780`, `:1999`, `:2011`, `:2071`, `:2526` |
| medium | Root cause of that leak: `GetSubscriptionWithMembership` returns a wrapped `mongo.ErrNoDocuments` while **every sibling method** maps to `model.ErrSubscriptionNotFound` — which is why `handler.go:780` must test both | `store_mongo.go:356` vs `:257`, `:274`, `:969`, `:1089`, `:1110`, `:1174` |
| medium | `RoomStore` is a **47-method interface** spanning rooms, subscriptions, thread rooms, members, orgs, users, apps and bot menus, implemented by a 65-method `MongoStore` holding 15 collection handles | `store.go:61-284`; `store_mongo.go:26-44` |
| medium | Split constructor: 14 positional args, then ten more dependencies set by direct field assignment. Correctness rests on scattered nil checks, not the compiler | `handler.go:87`; `main.go:355-368` |
| medium | **Shared knobs re-declared per service**, against §6's "declared once, in the package that owns the thing it configures": `BADGE_CACHE_TTL` duplicated in `inbox-worker` and `user-service`; `ROOM_KEY_RETIRED_TTL`/`ROOM_KEY_GRACE_PERIOD` duplicated in `room-worker`, `bot-room-service`, `broadcast-worker`. Neither `pkg/badgecache` nor `pkg/roomkeystore` exports a config type | `main.go:65`, `:67`, `:115` |
| low | The sanctioned sub-package layout exists precisely for request/reply services this large; only Teams was ever split out | `handler.go` (2,677 lines); `handler_test.go` (8,171 lines) |
| low | `MongoStore`/`NewMongoStore` exported though nothing outside `package main` consumes them; 11 of 15 sibling services use the unexported form | `store_mongo.go:26`, `:59` |
| nitpick | `bootstrapStreams` verifies only ROOMS, though the service also publishes to MESSAGES-CANONICAL and OUTBOX | `bootstrap.go:44-59` |
| nitpick | `bootstrapStreams` does not "no-op when disabled" as `CLAUDE.md` words it — it verifies via `js.Stream` and fails startup. This is the repo-wide pattern (12/12 `bootstrap.go` files), so **the convention text is stale, not the code** | `bootstrap.go:55` |

On the shared-knob finding: `CLAUDE.md` names this divergence as producing exactly the failure it warns about — a short retired-TTL permanently breaking `key.get` for messages already on the wire. Today all four services agree at 30m; nothing but coincidence keeps them agreeing.

### Recommendations
- `medium` — Map `GetSubscriptionWithMembership`'s miss to `model.ErrSubscriptionNotFound`, then delete the `mongo` import from `handler.go`/`handler_teams.go`, adding a store-level `ErrDuplicate` sentinel for the Teams idempotency path.
- `medium` — Move `BadgeCacheTTL` into a `badgecache.Config` and `RoomKeyRetiredTTL`/`GracePeriod` into a `roomkeystore.TTLConfig`, mounted as named fields in all four services.
- `medium` — Fold the ten post-construction assignments into an options-style constructor.
- `low` — Split `RoomStore` along its natural seams (`RoomReader`, `SubscriptionStore`, `MemberStore`, `ThreadStore`, `DirectoryStore`), keeping one `MongoStore` implementing all of them; adopt the sanctioned sub-package layout; unexport the store.
- `nitpick` — Extend `bootstrapStreams` to verify MESSAGES-CANONICAL and OUTBOX (verify only — never create).

---

## 4. Test coverage — 1 / 5

Statement-weighted coverage is **57.2% (1317/2301 statements)** — below the `CLAUDE.md` §4 60% line, so the dimension is floored at 1. The distribution is unusual and worth stating plainly: the handler layer is genuinely well tested at 87.5%, and the deficit is concentrated in the store and `main`.

| File | Coverage |
|------|----------|
| `bootstrap.go` | 100% |
| `helper.go` | 95.6% |
| `reader_history.go` | 92.0% |
| `handler_teams.go` | 91.8% |
| `handler.go` | 87.5% (1047/1196) |
| `memberlist_client.go` | 76.6% |
| **`main.go`** | **8.1%** (14/172) |
| **`store_mongo.go`** | **3.4%** (23/674) |

60 of 65 `store_mongo` methods read 0% in the unit profile because they are only reachable under `//go:build integration` — the number is real, but the risk is narrower than 57.2% alone suggests.

| Sev | Finding | Evidence |
|-----|---------|----------|
| critical | 57.2%, under the §4 60% bar and far under the 80% merge floor | `store_mongo.go:1` |
| high | The section-ordering Mongo path is untested in **both** profiles: `ComputeSectionOrder`, `MoveSubscriptionSection`, `sectionOrderExtreme`, `findOneAndUpdateSub` are never called from any non-mock test; `RebalanceSection` has exactly one integration call. Float-midpoint ordering with a rebalance trigger is precisely the logic that fails on precision exhaustion and ties | `store_mongo.go:1185` |
| high | `moveChat` is the weakest handler at 67.7%: the rebalance branch and the single-room cross-site `section_moved` federation branch are both uncovered — the same code path as the `critical` dedup defect in Chapter 6 | `handler.go:2371-2459` |
| high | The Teams-meeting Mongo store has zero coverage anywhere: `GetTeamsMeeting`/`InsertTeamsMeeting` appear only against hand stubs, so the duplicate-key/idempotent-insert behaviour the stub *fakes* is never validated against real Mongo | `store_mongo.go:178` |
| medium | Cross-site error-decode fallbacks in the member-list federation client are uncovered — the legacy-peer branch and the `ee.Metadata` propagation branch. These are the mixed-version paths that only fire during a rolling upgrade | `memberlist_client.go:119-134` |
| medium | Best-effort read-receipt fan-out failure paths uncovered; these **log-and-swallow**, so a regression is silent in production and invisible in CI | `handler.go:1541-1556`, `:1902-1930` |
| medium | `handleCreateRoomDMOrBotDM` at 69.6% — the lowest-covered create path, while its two siblings are at 88.9% / 86.4% | `handler.go:270`, uncovered `:273-311` |
| low | Unit tests start a real embedded `nats-server` and use a fixed `time.Sleep(500ms)` to force the timeout path — §4 forbids both real NATS in unit tests and `time.Sleep` for synchronization | `memberlist_client_test.go:22`, `:189` |
| low | `handler_teams_test.go` (738 lines) contains **zero** table-driven cases despite testing many input variations of one flow, while `handler_test.go` does this well (45 tables) | `handler_teams_test.go:1` |
| nitpick | Integration hygiene is correct: `TestMain` → `testutil.RunTests(m)`, all 15 Mongo handles via `testutil.MongoDB(t, prefix)`, one `testutil.NATS(t)`, no inline `GenericContainer` | `main_test.go:11` |

### Recommendations
- `critical` — Add integration tests for the ~60 zero-coverage `store_mongo` methods, starting with the section-ordering quartet and the Teams-meeting pair; that alone moves the largest untested block.
- `high` — Cover `moveChat`'s rebalance and cross-site `section_moved` branches with mocked-store cases (`needRebalance=true`, `userSiteID != h.siteID`), mirroring the `federate mute-toggled` failure assertions already in `handler_test.go`.
- `high` — Add a stub-free integration test asserting `InsertTeamsMeeting` idempotency on duplicate key against real Mongo, so the hand-stubbed `mongo.WriteException` matches reality.
- `medium` — Table-drive `memberlist_client_test.go` over the remote-error envelope shapes: canonical errcode, `RoomNotMember` remap, legacy/non-canonical code, empty message, populated `Metadata`.
- `medium` — Assert the log-and-continue fan-out failures by injecting a failing `publishCore` — the publish function is already a struct field, so this needs no new seam.
- `low` — Replace the 500 ms responder `time.Sleep` with a channel the test closes after asserting the deadline, and move the embedded-NATS tests behind the `integration` tag.

---

## 5. Maintainability — 3 / 5

Internally disciplined — WHY-comments, centralized sentinels, real helpers like `federateOne`/`boundedReply`, no dead code — but the service has outgrown the flat single-file layout, and dependency injection is now half-constructor, half-mutation.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | `handler.go` is 2,677 lines registering **31 RPCs across ~10 unrelated domains** in one file and one type: room CRUD, membership + roles, room E2E keys, read floors, read receipts, threads, chatlist sections, room-app tabs/command menus, org/mentionable directory, Teams calling | `handler.go:127-155` |
| high | Hybrid DI: `NewHandler` takes 13 positional args, then 11 more dependencies are assigned by field mutation in `main`. Nothing at compile time forces those 11 — a forgotten `handler.mentionableMaxLimit` silently yields a 0 limit *despite* the startup validation at `main.go:193`. The comments openly state the reason is churn avoidance, not design | `handler.go:102`, `:52-54`, `:97-99`; `main.go:373-383` |
| medium | `RoomStore` is one 47-method interface backed by a 2,125-line `MongoStore`; every test double pays for all 47 (`mock_store_test.go`, 42 KB). It splits along the same seams as the handler — the last of which is already proven by the separate `TeamsMeetingStore` | `store.go:61-284`, `:330` |
| medium | Duplicated toggle skeleton: `muteToggle`, `favoriteToggle` and `moveChat` repeat the same seven steps verbatim; only mute adds the badge-cache branch | `handler.go:2203`, `:2280`, `:2340` |
| medium | Thread and non-thread read-floor fan-out are parallel near-copies; the DM pair is line-for-line identical apart from the payload builder and log keys, so a change to fan-out semantics must be made twice | `handler.go:1501`, `:1512`, `:1539` vs `:1890`, `:1902`, `:1913` |
| medium | The "resolve home site → marshal → federate if remote" block is inlined at 7 call sites; `federateOne` abstracts the publish but not the preamble that actually repeats | `handler.go:839`, `:1440`, `:1743`, `:2272`, `:2324`, `:2457`, `:2506` |
| medium | `handler_test.go` is 8,171 lines / 251 test funcs and `integration_test.go` 4,827 / 78 — navigation and merge-conflict cost scales with the handler file it mirrors. `handler_teams.go` + its test already prove the per-domain split works here | — |
| low | `main()` is 261 lines with eight sequential validate-and-`os.Exit` blocks before any wiring; the validation half is mechanical and testable if lifted out | `main.go:165-425` |
| low | `enrichRoomMembersStages` is a 65-line inline BSON aggregation DSL with two `$lookup`s — the hardest thing in the service to modify safely — and it carries **no `// $lookup justification:` comment**, which `CLAUDE.md` requires when touching a `$lookup` site | `store_mongo.go:683-747` |

For scale context: room-service's production code is 6,460 lines, on par with `user-service` (6,965) and `history-service` (7,998) — both of which use the **sanctioned** sub-package layout for exactly this "larger request/reply surface" case. room-service qualifies but stays flat.

### Recommendations
- `high` — Split `handler.go` along its existing domain seams into `handler_room.go`, `handler_member.go`, `handler_read.go`, `handler_thread.go`, `handler_chatlist.go`, `handler_roomapp.go`, `handler_key.go`, keeping `Register` in `handler.go` and mirroring the `handler_teams.go` precedent. Pure file moves, no behaviour change — do this **before** the deeper refactor.
- `high` — Then adopt the sanctioned sub-package layout (`config/`, `service/`, `mongorepo/`, `service/mocks/`).
- `high` — Replace the 13-arg `NewHandler` + 11 field mutations with a `HandlerDeps` struct validated once in the constructor, so an unset dependency is a construction error rather than a silent zero value.
- `medium` — Extract `toggleSubscriptionFlag(...)` to collapse mute/favorite/move-chat, and `federateToUserHome(...)` to absorb the 7 repeated preambles.
- `medium` — Unify the thread/non-thread fan-out behind one publisher parameterized by payload builder and log fields; the DM path should exist once.
- `medium` — Split `RoomStore` into per-domain interfaces sharing the one `MongoStore`; split `store_mongo.go` to match and regenerate mocks per interface.
- `low` — Lift the eight `main()` validation blocks into `validate(cfg) error`; add the missing `$lookup` justification comment.

---

## 6. Integration — 4 / 5

Integration hygiene is strong — every subject goes through `pkg/subject` builders, all nine federated event types are inside `outbox.ConcurrentEventTypes`, and all 22 `chat.user.…` RPCs are documented. One defect stands out, and it is the most consequential finding in this review.

### The `moveChat` dedup collision

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | In the `moveChat` rebalance loop, every iteration calls `federateOne` with the same `ctx` and the same `destSiteID`, so **all N `subscription_section_moved` events get an identical `Nats-Msg-Id`**. `natsutil.InboxDedupID` ignores the per-row `payloadSeed` whenever a request ID is present and returns `requestID + ":" + destSiteID`. OUTBOX sets no explicit `Duplicates`, so JetStream's default 2-minute dedup window **drops every event after the first** | `handler.go:2419-2439`; `pkg/natsutil/request_id.go:149-155`; `pkg/stream/stream.go:77-82` |

A cross-site section move that renumbers siblings federates only one row. The remote replica keeps stale `sectionOrder` for the rest, with **no reconciliation path**. The single-row `else` branch (`handler.go:2456`) is unaffected — which is exactly why the bug is invisible in the common case. It compounds with Chapter 4's finding that `moveChat`'s rebalance branch is one of the service's least-covered paths.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | `handler.go:2436` passes `row.RoomID+":"+account` as `dedupSeed` — dead as a uniqueness mechanism (used only on the missing-request-ID fallback), which makes the collision above *look defended when it is not*. The same shape at five other sites is safe only because those are single-event handlers | `handler.go:1440`, `:1743`, `:2272`, `:2318`, `:2500` |
| medium | JetStream msg-ID dedup is **stream-wide, not per-subject**, so `requestID:destSiteID` is unsafe for any handler that ever federates two events to one destination in one request. Nothing in `federateOne` enforces one-event-per-(request, destination) | `handler.go:882-885` |
| low | `bootstrapStreams` fail-fast verification covers only ROOMS, though the service also publishes to `OUTBOX-{siteID}` and `MESSAGES-CANONICAL-{siteID}`. The doc comment claims it verifies "the streams it publishes to"; a missing OUTBOX surfaces only at the first cross-site RPC | `bootstrap.go:34-63` |
| low | `bot-room-service/deploy/docker-compose.yml:21` hardcodes `ROOM_KEY_RETIRED_TTL=30m` while room-service, room-worker and broadcast-worker all use `${ROOM_KEY_RETIRED_TTL:-30m}`. Defaults agree today, but an operator-set env moves three services and not the fourth — exactly the divergence `CLAUDE.md` forbids | `bot-room-service/deploy/docker-compose.yml:21` |
| low | Self-DM builds a DM room id via `idgen.BuildDMRoomID(requester.ID, requester.ID)` for a one-participant room. `CLAUDE.md` states a DM room is "always exactly two participants"; the code is deliberate and commented, but the rule as written does not sanction it | `handler.go:264` |
| nitpick | All five `RoomCanonical` publishes pass `msgID=""`, so a client retry after a NATS request timeout double-enqueues onto ROOMS; idempotence rests entirely on `room-worker`. The Teams path already shows the alternative with `natsutil.CanonicalDedupID` | `handler.go:403`, `:737`, `:1034`, `:2031`, `:2242` vs `handler_teams.go:269` |

### Verified clean
No raw `fmt.Sprintf` subject construction anywhere. Every `Timestamp` field is set at the publish site from `time.Now().UTC()` (nine sites), including `roomRestricted`, which correctly **overwrites the caller's value server-side** at `handler.go:2097` before federating. The OUTBOX subject is the `CLAUDE.md` form `chat.outbox.{origin}.{dest}.{eventType}`. No INBOX/OUTBOX stream creation here. Cross-site `errcode.Parse` used correctly. No JetStream consumers (pure publisher + request/reply), so consumer-pattern rules are N/A.

### Recommendations
- `high` — Make the OUTBOX dedup key unique per event, not per request: change `federateOne` to `dedupID = InboxDedupID(ctx, destSiteID, seed) + ":" + seed` (or fold in `eventType`+`roomID`), and add a `handler_test.go` case asserting the rebalance loop emits N **distinct** `Nats-Msg-Id`s to one destination.
- `medium` — Add a regression test for cross-site `moveChat` *with* a rebalance, capturing the injected publish func's `msgID` per call — no test currently greps for `rebalanc` plus federation.
- `medium` — Document the one-event-per-(request, destination) invariant on `federateOne`, or remove the misleading `dedupSeed` fallback so the key's real uniqueness domain is visible at every call site.
- `low` — Extend `bootstrapStreams` to `Stream()`-verify OUTBOX and MESSAGES-CANONICAL on the production path (verify only — ownership stays with `outbox-worker`/ops).
- `low` — Change `bot-room-service/deploy/docker-compose.yml:21` to `${ROOM_KEY_RETIRED_TTL:-30m}` so all four services move together.
- `low` — Reconcile the self-DM case with the "exactly two participants" DM rule — amend the rule to name the note-to-self exception.

---

## 7. Performance — 4 / 5

Strong, deliberately engineered performance discipline — precise projections, over-fetch+1 pagination, batched Go-side rollups replacing correlated `$lookup`s, parallelized independent reads, concurrency and pool guards — with two real hot-path gaps.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | `ListMentionableSubscriptions` runs **two correlated `$lookup`s per subscription *before*** the keyword `$regex` `$match` and `$limit`. Mention autocomplete fires per keystroke; a 1,000-member room costs ~2,000 index lookups per keystroke to return ≤`limit` rows. Neither lookup is guarded by member type (unlike `enrichRoomMembersStages:686`, which gates on `$$mtyp`), so bots pay the users join and humans pay the apps join | `store_mongo.go:1760`, `:1775`, `:1801`, `:1803` |
| high | `ListOrgMembers` has **no `$limit`, no pagination**, and its handler is not wrapped in `boundedReply`. A large `sectId`/`deptId` materializes the whole org into memory and into one NATS reply; oversized replies fail the RPC outright at `max_payload`. Only 3 handlers use the `boundedReply` guard, and this is exactly the shape it exists for | `store_mongo.go:995`; `handler.go:419-426` |
| medium | `expandChannelRefs` resolves channel refs **serially**, each with its own 5 s `MEMBER_LIST_TIMEOUT`, and `len(req.Channels)` is never capped — two slow refs exhaust the 10 s `REQUEST_TIMEOUT`. Cross-site refs are independent NATS RPCs and should ride an `errgroup`, as `validateMembershipRefs:1108` and `roomsInfoBatch:1220` already do | `handler.go:1119-1155`; `main.go:64` |
| medium | `ListReadReceipts` places the users `$lookup` **before** `$limit`; the `lastSeenAt >= since` match narrows it, but a busy room still joins every recent reader to discard all but `limit` | `store_mongo.go:1471`, `:1484` |
| medium | `GetTeamsMeeting` fetches the whole document with **no projection** — the only find/aggregation in the service missing one, against §MongoDB's "always project precisely" | `store_mongo.go:180` |
| low | `GetSubscriptionWithMembership`'s org lookup uses `$expr` with `$or` on `member.id`. The service's **own comment at `:635`** documents that this exact shape cannot use an index, and that it was removed from the enrichment path for that reason; it survives here on the remove-member path | `store_mongo.go:315-330` |
| low | Cross-site federation publishes one synchronous JetStream `PublishMsg` per remote site, serially, inside the request handler — bounded by peer count, but each is a full publish-ack RTT on the caller's latency budget | `handler.go:2162-2166` |
| nitpick | Coverage is 57.2% and the untested paths include the enrichment/lookup pipelines above, so a perf refactor there would land **without a regression net** | — |

### Recommendations
- `high` — In `ListMentionableSubscriptions`, push the cheap discriminator forward: compute `isApp` in an `$addFields` before the joins and gate each `$lookup` on it (mirroring `enrichRoomMembersStages`'s `$$mtyp` guard), halving the joins. Better still, apply the account-side regex in the initial `$match` and follow the `attachOrgDisplay` precedent — one batched `users`/`apps` fetch keyed by the surviving accounts, rolled up in Go.
- `high` — Give `ListOrgMembers` a `limit`/`offset` using the existing `pagedLimit`/`trimOverFetch` helpers, and wrap `listOrgMembers` in `boundedReply`.
- `medium` — Parallelize `expandChannelRefs` with `errgroup.SetLimit`, and reject requests whose `len(req.Channels)` exceeds a new cap, so the request budget is bounded by construction rather than by the guard timeout.
- `medium` — Reorder `ListReadReceipts` to `$match → $sort → $limit → $lookup`, so the join fans out over at most `limit` rows.
- `medium` — Add the missing projection to `GetTeamsMeeting`.
- `low` — Replace the `$expr $or` with two `$eq` lookups unioned in Go, matching the `FindExistingOrgIDs` two-`Distinct` pattern already justified at `store_mongo.go:1015-1027`.
- `low` — Batch the per-site federation publishes with `PublishAsync` + a single `PublishAsyncComplete` wait, keeping all-or-nothing error semantics.

