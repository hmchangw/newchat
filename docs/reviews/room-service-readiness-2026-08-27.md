# Production-Readiness Review — `room-service`

| | |
|---|---|
| **Service** | `room-service` |
| **Date** | 2026-08-27 |
| **Branch** | `claude/new-session-439bi9` |
| **Overall score** | **2.83 / 5** |
| **Method** | Six independent expert audits (code quality + SAST, architecture, test coverage, maintainability, integration, performance) against `CLAUDE.md` and general Go-microservice practice |

## Executive summary

`room-service` is a well-built service carrying a large and mostly undisclosed amount of structural debt. The things that are easy to get wrong in this codebase are right: every one of its 27 NATS registrations and 12 publishes goes through `pkg/subject` (zero raw `fmt.Sprintf` subjects), stream configs come from `pkg/stream`, `bootstrapStreams` respects the `BOOTSTRAP_STREAMS` opt-in and sets only `Name + Subjects`, all cross-site federation goes through `outbox.Publish` to the local OUTBOX rather than a direct remote INBOX publish, every federated event type is registered in exactly one `pkg/outbox` partition set, every published event carries a `Timestamp` set at the publish site, config is a typed `caarlos0/env` struct with no `os.Getenv` anywhere, shutdown uses `pkg/shutdown.Wait` with the documented ordering, and `gosec` is clean. Handler-level error-path testing is genuinely strong and mocks are fresh.

What holds it back is scale and verification. The service is 6,314 production LOC in nine flat files, with a 2,643-line `handler.go` spanning eight unrelated domains and a 47-method `RoomStore` god interface — both peers of comparable size (`user-service`, `history-service`) already moved to the sanctioned sub-package layout and keep no file over 993 lines. Unit coverage is **57.7%**, below the repo's 60% critical threshold, because the 2,046-line `store_mongo.go` sits at 2.6% and is verified only behind the `integration` build tag that `make test` does not run; no CI job enforces the documented 80% floor. On the runtime side the store leaks `mongo.ErrNoDocuments`/`IsDuplicateKeyError` into handlers, a missing room returns 500 from four RPCs and 404 from two others, several hot reads fetch whole documents against the "always project precisely" rule, mention autocomplete joins an entire room before `$limit`, and a per-bot `GetApp` loop N+1s without a request-size cap. One correctness bug stands out: rebalanced `section_moved` federation events all share a `Nats-Msg-Id` and are silently deduplicated by JetStream, leaving cross-site users with stale `sectionOrder`.

None of this blocks the service from running — it runs, and its tests pass. It blocks confident change. Close the coverage gap and enforce it, fix the dedup-ID bug and the not-found/500 inconsistency, then decompose.

## Dimension scores

| # | Dimension | Score |
|---|-----------|:-----:|
| 1 | Go code quality (incl. SAST) | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | **1 / 5** |
| 4 | Maintainability | **2 / 5** |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |
| | **Average** | **2.83 / 5** |

## Findings by severity

| Severity | Count |
|----------|:-----:|
| `critical` | 1 |
| `high` | 16 |
| `medium` | 30 |
| `low` | 19 |
| `nitpick` | 14 |
| **Total** | **80** |

The single `critical` is unit coverage at 57.7%, below the repo minimum of 80% and below the 60% threshold at which CLAUDE.md Section 4 floors the dimension at 1.

---

# 1. Go code quality — 4 / 5

Idiomatic, consistently formatted, and clean under `gosec`. The deductions are a genuine error-classification bug in the store, an inconsistent not-found contract across RPCs, and observability discipline that slipped in about a fifth of the log sites.

## SAST

`make sast` exits 2 — `gosec=PASS govulncheck=FAIL semgrep=FAIL`.

- **gosec v2** (`-severity medium -confidence medium -tests=true`): **PASS**, repo-wide and scoped to `./room-service/...` — 0 findings. Both `#nosec` suppressions in the service (`handler.go:487`, `handler.go:506`, G117 on `RoomKeyGetResponse.PrivateKey`) use the correct gosec-native form with justifications.
- **semgrep**, repo-local rules only (`--config=.semgrep/ room-service/`): 9 rules, **0 findings** — includes the `errcode` `WithCause` guard and `room-subject-publish-must-route`.
- **govulncheck** and the `p/golang` / `p/security-audit` semgrep registry rulesets **could not run**: the sandbox proxy returns 403 for `https://vuln.go.dev/index/modules.json.gz` and `https://semgrep.dev/c/p/golang`. `make tools` also failed (`pipx needs uv>=0.9.17, /root/.local/bin/uv reports 0.8.17`); gosec/govulncheck installed anyway, semgrep manually via `pipx install --backend pip`.

**No room-service SAST finding to fold in.** CVE reachability and the registry rulesets remain unverified in this environment — re-run `make sast` on a runner with network egress before treating the gate as green.

## Findings

- **[high] `GetRoom` mislabels every infra failure as "not found"** — `room-service/store_mongo.go:238` — `if err := s.rooms.FindOne(...).Decode(&room); err != nil { return nil, fmt.Errorf("room %q not found: %w", id, err) }` wraps Mongo timeouts and network errors in a "not found" message. `GetSubscription` immediately below (`store_mongo.go:281`) does it correctly by branching on `mongo.ErrNoDocuments`. `GetRoomAppRead` (`store_mongo.go:247`) repeats the bug.
- **[high] The same condition returns two different wire codes across RPCs** — `room-service/handler.go:663`, `:757`, `:885`, `:1385` — `removeMember`, `updateRole`, `addMembers` and `messageRead` all `return nil, fmt.Errorf("get room: %w", err)`, so a deleted room surfaces as `internal` (500). `roomRename` (`handler.go:1976`) and `roomRestricted` (`handler.go:2048`) branch to `errRoomNotFound` (404) for the identical case. Clients cannot distinguish "room gone" from "server broken".
- **[medium] 21 `slog.*` calls in request paths drop the request ID and trace correlation** — `room-service/handler.go:1165,1226,1406,1444,1495,1508,1521,1527,1533,1708,1850,1884,1895,1900,1906,1960,2032,2209,2566,2573` — e.g. `slog.Error("publish message_read DM event failed", "error", err, "roomId", roomID, "account", account)` with `ctx` in scope. CLAUDE.md Section 3 requires the correlation ID in all log lines; the non-`Context` variants also bypass the o11y handler's trace correlation. The service uses `ErrorContext`/`DebugContext` elsewhere (`:1745`, `:2242`), so this is inconsistency, not policy.
- **[medium] Three casings for the same structured-log key** — `room-service/handler.go:1495` (`"roomId"`) vs `:1960` (`"roomID"`) vs `:1230` (`"site_id"`) — counted across slog calls: `roomId` ×13, `roomID` ×4, `room_id` ×2, `siteId` ×1, `site_id` ×2. A log query for one room must union three field names.
- **[medium] Four hot reads fetch whole documents** — `room-service/store_mongo.go:875` (`GetUser`), `:887` (`GetApp`), `:899` (`FindDMSubscription`), `:723` (`getRoomSubscriptions`: `s.subscriptions.Find(ctx, bson.M{"roomId": roomID}, opts)` with no projection) — against CLAUDE.md Section 6's "always project precisely". The file's own `subscriptionReadProjection` comment calls the full Subscription doc "reflection-heavy", yet `getRoomSubscriptions` decodes it in full for every member on the `ListRoomMembers` fallback path.
- **[medium] Marshal error silently discarded** — `room-service/handler.go:2207` — `if canonData, err := json.Marshal(canonEvt); err == nil { ... }` has no `else`; a marshal failure drops the notification-worker cache-invalidation event with no log line. CLAUDE.md Section 3: "Never ignore errors silently — comment if intentionally discarded."
- **[medium] Unbounded member fetch in the Teams RPCs** — `room-service/handler_teams.go:62`, `:128` — `h.store.ListRoomMembers(ctx, roomID, nil, nil, false)` passes `limit=nil`, loading every member (up to `MAX_ROOM_SIZE`=1000) *before* `countIndividualMembers(...) > h.roomMembersLimit` (500) or `len(emails) > h.roomMembersCallLimit` (20) rejects. `expandChannelRefs` (`handler.go:1101`) shows the right pattern with `listLimit := h.maxRoomSize + 1`.
- **[low] `RoomStore` is a 52-method god interface** — `room-service/store.go:61` — satisfies "define interfaces in the consumer" only technically; no call site needs more than a handful, and it forces a 958-line regenerated mock.
- **[low] Store DTOs carry `bson` but no `json` tags** — `room-service/store.go:47` (`ReadReceiptRow`), `:56` (`RoomBotAppEntry`) — CLAUDE.md Section 3 requires both.
- **[low] Hardcoded 5s deadlines duplicate the configurable guard** — `room-service/handler.go:1194,1246,2538,2589` — `context.WithTimeout(ctx, 5*time.Second)` while `natsrouter.GuardConfig.RequestTimeout` (`REQUEST_TIMEOUT`, default 10s) already bounds every handler. Two un-reconciled deadline sources, one untunable.
- **[low] Numeric config accepted without validation** — `room-service/main.go:55-57,86-87` — `MAX_ROOM_SIZE`, `MAX_BATCH_SIZE`, `ROOM_MEMBERS_LIMIT`, `ROOM_MEMBERS_CALL_LIMIT` are never checked, unlike `MemberListTimeout` (`main.go:157`) and `RestrictedRoomMinMembers` (`:161`). `MAX_ROOM_SIZE=0` silently makes `expandChannelRefs` use `listLimit=1`.
- **[low] Six `$lookup` sites lack the required inline justification** — `room-service/store_mongo.go:289,302,317,656,673,1617` — five other sites (`:1395,:1453,:1543,:1678,:1693`) carry `// $lookup justification:` correctly.
- **[nitpick] `var ctx context.Context = c` appears 24×** — `room-service/handler.go:145` et al. — only one other occurrence exists in the repo; `user-service/service/me.go:18` passes `c` directly.
- **[nitpick] `c.WithLogValues` used once** — `room-service/handler.go:1739` — vs 28× in `user-service/service/`, so most room-service router log lines lack account/roomID.
- **[nitpick] Stale doc comment** — `room-service/helper.go:105` — says `errAppAccessDenied` covers "neither a room member nor a platform admin", but `authorizeRoomAppRead` (`handler.go:2468`) checks only `model.IsRoomMember(sub)`.
- **[nitpick] `boundedReply` marshals twice** — `room-service/helper.go:227` — discards the body from `marshalBounded`, then the router re-marshals the same struct.

## Recommendations

- **[high]** Fix `GetRoom`/`GetRoomAppRead` to branch on `mongo.ErrNoDocuments` the way `GetSubscription` (`store_mongo.go:281`) does, returning a `model.ErrRoomNotFound`-style sentinel; then map that sentinel to `errRoomNotFound` at all six `GetRoom` call sites so every RPC returns 404, not 500, for a missing room.
- **[medium]** Convert the 21 bare `slog.*` calls in `handler.go` to their `*Context` variants and normalize log keys to one casing (`roomId`/`siteId` are the repo-wide majority). A `forbidigo` rule on `slog.Error(`/`slog.Warn(` in handler files prevents regression.
- **[medium]** Add explicit projections to `GetUser`, `GetApp`, `FindDMSubscription`, `GetTeamsMeeting` and `getRoomSubscriptions`, mirroring the `roomReadProjection`/`subscriptionReadProjection` pattern already in the file, and extend the existing projection-drift integration test to cover them.
- **[medium]** Pass `&h.roomMembersLimit`/`&h.roomMembersCallLimit` (plus one, to detect overflow) as the `limit` argument in `handler_teams.go:62,128` so the cap is enforced at the query rather than after the full load.
- **[medium]** Handle the discarded marshal error at `handler.go:2207` — log via `slog.ErrorContext` or restructure as an explicit `if err != nil` — so a dropped canonical member event is observable.
- **[low]** Validate `MAX_ROOM_SIZE`, `MAX_BATCH_SIZE`, `ROOM_MEMBERS_LIMIT`, `ROOM_MEMBERS_CALL_LIMIT` (`> 0`) in `main.go` alongside the existing guards, and replace the four hardcoded `5*time.Second` deadlines with a fraction of `cfg.Guard.RequestTimeout` or a dedicated env var.
- **[low]** Add `json` tags to `ReadReceiptRow`/`RoomBotAppEntry`, add the missing `// $lookup justification:` comments at the six sites listed above, and correct the stale `errAppAccessDenied` comment at `helper.go:105`.

---

# 2. Architecture — 4 / 5

Every repo-specific convention that carries real operational risk is respected. The deductions are a store boundary that leaks its driver, a best-effort index policy applied to constraints another service depends on, and a service surface that has outgrown the layout it still uses.

## Verified clean

Worth stating explicitly, because these are the conventions most often broken:

- No `os.Getenv` anywhere — config is a typed `caarlos0/env` struct with fail-fast validation.
- Zero raw `fmt.Sprintf` subject construction; all 27 registrations and 12 publishes go through `pkg/subject`.
- Stream configs come from `pkg/stream` with the `<STREAM>-<siteID>` name pattern.
- `bootstrapStreams(ctx, js, siteID, enabled)` (`room-service/bootstrap.go:43-61`) sets only `Name + Subjects` and matches `room-worker`/`outbox-worker`/`inbox-worker` byte-for-byte in shape.
- All cross-site federation goes through `outbox.Publish` to the local OUTBOX (`room-service/handler.go:862-865`) — never a direct remote INBOX publish. All event types used are in `pkg/outbox.ConcurrentEventTypes`.
- Store interfaces are consumer-defined and split by concern: `RoomStore`, `RoomKeyStore`, `MessageReader`, `TeamsMeetingStore`, `DEKProvisioner`, `MemberListClient`, `badgeCache`.
- `shutdown.Wait(25s)` with drain-before-DB ordering.
- No JetStream consumer, so the `MAX_WORKERS` semaphore pattern is correctly N/A. No `time.Sleep`, no leaked goroutines.

## Findings

- **[high] MongoDB driver errors leak across the store boundary into handlers** — `room-service/handler.go:774,1978,1990,2050,2492`, `room-service/handler_teams.go:192` — handlers branch on `errors.Is(err, mongo.ErrNoDocuments)` and `mongo.IsDuplicateKeyError(err)`, forcing `handler.go:18` to import `go.mongodb.org/mongo-driver/v2/mongo`. The *interface* is specified in driver terms: `store.go:333` documents `InsertTeamsMeeting` as "returns a duplicate-key error (`mongo.IsDuplicateKeyError`), which the handler treats as…". Clean sentinels already exist (`ErrRoomNotFound` at `store.go:16`, `model.ErrSubscriptionNotFound`); a non-Mongo implementation of `RoomStore` would silently break these branches.
- **[high] Startup continues when unique-index creation fails, including constraints another service depends on** — `room-service/main.go:222-224` — `slog.Warn("ensure store indexes failed; continuing (indexes are best-effort)")`. But `store_mongo.go:123-127` creates the *unique* keys `room_members(rid,member.type,member.id)` and `subscriptions(roomId,u.account)` whose stated purpose (`store_mongo.go:99,120-122`) is "required for retry-safe writes by **room-worker**". A partial failure degrades a different service's idempotency invisibly. These are not best-effort.
- **[medium] Cross-service schema ownership inversion plus startup DDL migration** — `room-service/store_mongo.go:119-127`, `:152-159` — room-service is not the writer of `room_members` yet provisions its uniqueness constraint, and runs a live index swap (`EnsureIndexWithRepair` then `DropOne("threadRoomId_1_userId_1")`) at every replica start. Index provisioning for a collection owned by another service belongs in ops/IaC or the owning service.
- **[medium] Only 1 of the 3 streams room-service publishes to is verified at startup** — `room-service/bootstrap.go:43-60` — handles `stream.Rooms(siteID)` alone. The service also publishes to OUTBOX (`handler.go:864` → `outbox.Publish`) and MESSAGES-CANONICAL (`handler_teams.go:269`). The helper's own fail-fast rationale ("a missing stream means the deploy is broken before the first publish") therefore does not cover federation or the Teams system message. It correctly does *not* create them — but it should verify.
- **[medium] The ROOMS operation vocabulary is an untyped string contract duplicated across two services** — `room-service/handler.go:397` (`"create"`), `:734` (`"member.remove"`), `:1014` (`"member.add"`), `:2010` (`"room.rename"`) passed to `subject.RoomCanonical(siteID, operation string)`; the consumer matches with `strings.HasSuffix(subj, ".member.add")` at `room-worker/handler.go:256`. Contrast the OUTBOX lane, where `pkg/outbox/outbox.go:89` *rejects* an event type absent from the declared partition. No such guard exists here; a typo lands in ROOMS unconsumed.
- **[medium] Handler is a partially-constructed god object** — `room-service/handler.go:45-88` (22 fields), `:90` (13-positional-arg `NewHandler`), then `main.go:335-342` sets seven more deps by field assignment (`dekProvisioner`, `badge`, `graphClient`, `directoryClient`, `teamsMeetingStore`, `teamsEmailDomain`, `roomMembersLimit`, `roomMembersCallLimit`). `NewHandler` alone does not yield a valid handler, defeating constructor injection.
- **[medium] Service surface has outgrown the flat layout the repo sanctions an exception for** — `room-service/handler.go` is 2,643 lines / 26 registered RPCs (`:115-143`), `store.go:61-280` declares a 47-method `RoomStore`, `handler_test.go` is 8,048 lines. CLAUDE.md explicitly permits sub-packages for "request/reply services with a larger surface (e.g. `user-service`, `history-service`)"; `user-service/` uses `config/ models/ mongorepo/ service/`. room-service is materially larger and still flat, mixing rooms, subscriptions/sections, threads, room keys, read receipts, Teams/Graph and app tabs in one struct.
- **[low] Three concurrency idioms in one file, one swallowing errors** — `room-service/handler.go:536-544` (raw `sync.WaitGroup` with shared vars), `:1087-1096` (`errgroup` with captured error vars and `_ = g.Wait()` discarding the group error), `:1201-1222` (idiomatic `errgroup`). All terminate, but `:1092` is an errcheck-suppressed discard.
- **[low] Request-ID correlation dropped in ~20 log sites** — `room-service/handler.go:1165,1226,1406,1444,1495,1508,1521,1708,1850,1960,2032,2209,2566` — bare `slog.Warn/Error/Info/Debug` rather than the `…Context` variants used at `:1268,:1788,:2159`, so the ID `natsrouter` puts on the context is lost. (Also reported under Code quality.)
- **[nitpick] Hardcoded timeouts outside the typed config** — `room-service/handler.go:1194`, `:1246` (`5*time.Second`), `reader_history.go:18` (`historyRequestTimeout = 2s`), `handler.go:1322` (`queryChunkSize = 500`) — while `natsrouter.GuardConfig.RequestTimeout` is already wired.
- **[nitpick] Inconsistent `pkg/subject` builder naming at the registration site** — `room-service/handler.go:116-142` — mixes `…Pattern(h.siteID)` with bare `RoomRestricted`, `RoomKeyEnsure`, `ThreadRoomInfoBatch` and `…Subscribe` suffixes for the identical "subscription pattern" role.
- **[nitpick] Six `$lookup` sites lack the required inline justification** — `room-service/store_mongo.go:289,302,317,656,673,1617` — sixteen others correctly carry `// $lookup justification: …`.

## Recommendations

- **[high]** Make `EnsureIndexes` failure fatal for the unique indexes (`room_members`, `subscriptions`, `thread_subscriptions`, `teams_meetings`) at `main.go:222`; keep warn-and-continue only for non-unique performance indexes. Better still: move the `room_members`/`subscriptions` unique keys to room-worker (the writer) or IaC, and have room-service call `mongoutil.WarnMissingIndexes` for them exactly as it already does for the user-service-owned `users.account` (`store_mongo.go:129-130`).
- **[high]** Push Mongo error translation entirely into `store_mongo.go`: every method returns only `ErrRoomNotFound`/`model.ErrSubscriptionNotFound`, add an `ErrDuplicateMeeting` sentinel for `InsertTeamsMeeting`, rewrite `store.go:333`'s doc in sentinel terms, and delete the `mongo` import from `handler.go`/`handler_teams.go`. Add a test asserting the driver is not imported outside `store_mongo.go`.
- **[medium]** Extend `bootstrapStreams` to cover `stream.Outbox(siteID)` and `stream.MessagesCanonical(siteID)` — verify-only in both the enabled and disabled paths, since those streams are owned by outbox-worker and message-worker; creation stays with ROOMS.
- **[medium]** Promote the ROOMS operation vocabulary to typed constants in `pkg/subject` (mirroring `model.InboxEventType`) with a `RoomCanonical(siteID string, op Operation)` signature and a shared set consumed by both room-service's publishers and room-worker's dispatch at `room-worker/handler.go:256`, so an unknown operation fails at compile time.
- **[medium]** Adopt the sanctioned sub-package layout: split `handler.go` along its natural seams (`rooms/`, `subscriptions/`, `threads/`, `readreceipts/`, `teams/`, `apps/`), giving each a narrow store interface over the shared `MongoStore`. That decomposes the 47-method `RoomStore` and the 22-field `Handler` without changing any wire contract.
- **[medium]** Replace the 13-arg `NewHandler` plus seven post-construction assignments with a `HandlerDeps` struct or functional options, returning an error when a required dependency is missing so an incompletely wired handler cannot reach `Register`.
- **[low]** Convert the remaining bare `slog.*` calls to `…Context` variants, and move the hardcoded 5s/2s timeouts and `queryChunkSize` into the typed config with `envDefault`s.

---

# 3. Test coverage — 1 / 5

Floored at 1 by CLAUDE.md Section 4: unit coverage is **57.7%**, below the 60% threshold that mandates a `critical` finding. This is the highest-risk dimension in the review — not because the tests that exist are bad (handler error-path testing is one of the service's strengths) but because the 2,046-line store implementation is effectively outside the gate that runs on every commit.

## Measurements

**Test run:** `make test SERVICE=room-service` → **PASS**, `ok github.com/hmchangw/chat/room-service 1.656s`. Zero failures, zero skips. `-race` is applied (`Makefile:108`). `go vet -tags=integration ./room-service/...` compiles clean; integration tests could not be *executed* (Docker unavailable in the audit sandbox).

**Unit coverage:** **57.7%** of statements (`go test -coverprofile ./room-service/...`).

| File | Coverage |
|------|:--------:|
| `bootstrap.go` | 100% |
| `helper.go` | 96.0% |
| `reader_history.go` | 92.0% |
| `handler_teams.go` | 91.8% |
| `handler.go` | 87.5% (1048/1198) |
| `memberlist_client.go` | 76.6% |
| `main.go` | 5.7% |
| **`store_mongo.go`** | **2.6% (17/654)** |

Excluding `store_mongo.go` the figure is 79.9%. Integration tests reach ~48 of 58 `MongoStore` methods, so a merged profile would likely land in the low 80s — but the number the floor check applies to is the plain unit figure, **57.7%**.

**Lowest-covered functions:**

| Function | Location | Coverage |
|----------|----------|:--------:|
| `main` | `main.go:136` | 0.0% (246 stmts) |
| `Register` | `handler.go:115` | 0.0% (27 registrations) |
| `EnsureIndexes` | `store_mongo.go:102` | 0.0% |
| `GetTeamsMeeting` / `InsertTeamsMeeting` | `store_mongo.go:180`/`:196` | 0.0% |
| `ListMentionableSubscriptions` | `store_mongo.go:1667` | 0.0% |
| `ComputeSectionOrder` | `store_mongo.go:1121` | 0.0% |
| `botOrPlatformAdminRegex` | `helper.go:122` | 0.0% |
| `withMemberListMetrics` / `withHistoryMetrics` | `memberlist_client.go:48` / `reader_history.go:36` | 0.0% |
| `moveChat` | `handler.go:2306` | 67.7% |
| `boundedReply` | `helper.go:227` | 66.7% |
| `publishThreadChannelEvent` | `handler.go:1881` | 60.0% |

**Mock freshness: clean.** `make generate SERVICE=room-service` produced an empty `git status --porcelain room-service/` and empty `git diff --stat`. Nothing to revert; tree verified clean relative to HEAD.

## Positives

`main_test.go:1-11` is exactly the mandated `TestMain`/`testutil.RunTests` shape. All containers come from `pkg/testutil` (`MongoDB`, `NATS`) with **zero** inline `testcontainers.GenericContainer`/`mongodb.Run`. No test helpers leak into production files. Handler error-path coverage is genuinely strong — store errors, publish failures, cross-site aborts and boundary lengths are all named explicitly.

## Findings

- **[critical] Coverage below repo minimum 80%, currently 57.7%** — `room-service/store_mongo.go:1` — 654 of 2,274 statements live in an essentially unit-untested store. Below the 60% threshold, so this dimension is floored at 1.
- **[high] The 2,046-line store implementation has 2.6% unit coverage** — `room-service/store_mongo.go:102-2046` — every method is verified only behind `//go:build integration`, so the pre-commit hook and `make test` see almost none of it. A projection or filter regression ships green locally.
- **[high] No coverage threshold is enforced in CI for this service** — `room-service/deploy/azure-pipelines.yml:44` writes `coverage-room-service.out` and never inspects it. `tools/coveragecheck` exists but is wired only to loadgen (`Makefile:141-143`). The 80% rule is documented, not enforced.
- **[medium] `main()` is a 246-line untestable block** — `room-service/main.go:136` — six fail-fast guards (`:158-168`) and the nine-step shutdown ordering (`:357-381`) have no test because nothing is extracted into a `run(ctx, cfg) error`.
- **[medium] Unit tests boot a real in-process NATS server** — `room-service/memberlist_client_test.go:22-34`, `room-service/reader_history_test.go:24-35` — CLAUDE.md Section 4 says "Never connect to real databases, NATS, or external services in unit tests". These belong behind the integration tag, or need an injected request func.
- **[medium] Cross-site errcode envelope translation is untested** — `room-service/memberlist_client.go:118-125` (legacy non-canonical-code fallback + warn) and `:128-133` (metadata propagation) are both 0%; only the happy envelope and the `RoomNotMember` remap are covered.
- **[medium] Teams meeting persistence has zero integration coverage** — `room-service/store_mongo.go:180`, `:196` — the duplicate-key race branch is exercised only against a hand-built `mongo.WriteException` (`handler_teams_test.go:26`), never against the real E11000 the `(roomId,siteId)` unique index (`store_mongo.go:173`) emits.
- **[medium] Handler wiring is unverified** — `room-service/handler.go:115-142` — `Register` is 0% in unit tests; a mistyped subject pattern or a dropped registration passes the unit gate. Integration tests call `h.Register` but exercise only ~5 of 27 subjects.
- **[low] Non-rebalance cross-site `section_moved` federation is untested** — `room-service/handler.go:2408-2426` — only `TestHandler_MoveChat_Rebalance_...` (`handler_test.go:6570`) covers the sibling branch.
- **[low] Table-driven guidance largely unused in the biggest file** — `room-service/handler_test.go:50-593` — 19 sequential `TestHandler_UpdateRole_*` funcs duplicate setup where one table would do; 244 top-level funcs vs 22 tables file-wide.
- **[low] Global `slog.Default()` mutation in tests** — `room-service/debug_log_test.go:62-72` — correctly restored via `t.Cleanup`, but it is shared mutable state; combined with zero `t.Parallel()` across all 14 test files, the suite cannot be parallelized safely.
- **[nitpick] Redundant, inconsistent `testing.Short()` skips** — `room-service/integration_test.go:1361,1431,1488,1512,1602,1780,1886,1932` — 8 of 73 integration tests, already gated by the build tag.
- **[nitpick] Ten exported store methods have no direct integration test** — `ComputeSectionOrder`, `FindDMSubscription`, `FindUsersByAccounts`, `GetApp`, `GetUser`, `ListSubscriptionsByRoom`, `MoveSubscriptionSection`, `UpdateRoomVisibility`, plus the two Teams methods above.

## Recommendations

- **[critical]** Close the floor. Either add unit-level tests for `store_mongo.go`'s pure logic (`ComputeSectionOrder`, `sectionOrderExtreme`, the filter/projection builders) or merge unit and integration profiles (`go test -coverprofile -tags integration`) and gate on the merged number — but state explicitly which number CI enforces.
- **[high]** Wire `tools/coveragecheck -profile coverage-room-service.out -min 80` into `.github/workflows/ci.yml` and `room-service/deploy/azure-pipelines.yml:44` so the documented 80% rule actually blocks a merge.
- **[medium]** Extract `func run(ctx context.Context, cfg config) error` from `main.go:136` and test the six startup guards and the shutdown ordering; leave `main()` as a three-line shim.
- **[medium]** Move `startInProcessNATS` (`memberlist_client_test.go:22`) and `startOtelNATS` (`reader_history_test.go:24`) into an `integration`-tagged file backed by `testutil.NATS(t)`, or inject the request function so the unit tests need no server.
- **[medium]** Add a table-driven test over the cross-site envelope decoder covering the non-canonical-code fallback and metadata propagation (`memberlist_client.go:118-133`).
- **[medium]** Add integration tests for `InsertTeamsMeeting`/`GetTeamsMeeting` that provoke the real E11000 against the `(roomId,siteId)` unique index, replacing reliance on `errStubDuplicateKey`.
- **[low]** Add a `Register` wiring test asserting the exact set of subscribed subjects (a fake `natsrouter.Router` recording registrations), so a dropped or mistyped subject fails the unit gate.

---

# 4. Maintainability — 2 / 5

The service works and is internally consistent, but it costs far more to change than its peers of identical size. Two comparable request/reply services already moved to the sanctioned sub-package layout; room-service kept everything in `package main` and now carries a 2,643-line handler, a 47-method store interface and an 8,048-line test file. Nothing in CI arrests further growth.

## Metrics

| | Production | Tests |
|---|---:|---:|
| Files | 9 | 14 |
| LOC | **6,314** | **15,595** |

| File | Lines | Funcs |
|------|------:|------:|
| `handler.go` | **2,643** | 53 (48 `*Handler` methods), 27 RPCs registered at `:115-143` |
| `store_mongo.go` | **2,046** | 62 |
| `main.go` | 382 | 2 (32 `env:` tags) |
| `store.go` | 337 | — |
| `handler_teams.go` | 334 | 12 |
| `helper.go` | 253 | 12 |
| `memberlist_client.go` | 143 | 4 |
| `reader_history.go` | 115 | 4 |
| `bootstrap.go` | 61 | 1 |

**Longest functions:** `addMembers` **165 lines** (`handler.go:867-1031`, ~27 branch points), `roomRestricted` **148** (`:2021-2168`, ~30), `messageRead` **135** (`:1345-1479`), `moveChat` **126** (`:2306-2431`), `messageThreadRead` **116** (`:1628-1743`), `updateRole` 95 (`:741-835`), `removeMember` 90 (`:651-740`), `messageReadReceipt` 90 (`:1538-1627`). Store: `ListMentionableSubscriptions` 103 (`store_mongo.go:1667-1769`), `GetSubscriptionWithMembership` 88 (`:286-373`), `getRoomMembers` 83 (`:523-605`), `EnsureIndexes` 78 (`:102-179`).

**Interfaces:** `RoomStore` = **47 methods** (`store.go:61-282`); four others are small (`RoomKeyStore` 4, `TeamsMeetingStore` 2, `MessageReader` 1, `DEKProvisioner` 1). 19 exported top-level symbols (4 funcs, 15 types). `Handler` struct **22 fields**; `NewHandler` **14 params** (`handler.go:90`).

**Tests:** `handler_test.go` 8,048 lines / 273 test funcs; `integration_test.go` 4,434 / 88.

**Peer comparison:** `user-service` 6,893 LOC across 41 files (largest 867); `history-service` 7,123 across 33 files (largest 993). room-service is the same total size in **9 files with a 2,643-line one**.

## Findings

- **[high] Service has outgrown the flat layout its peers already abandoned** — `room-service/handler.go:1` — at 6,314 LOC it matches `user-service` (6,893) and `history-service` (7,123), both of which use the sanctioned `config/ models/ mongorepo/ service/` split with no file over 993 lines. room-service keeps everything in `package main`, so `handler.go` is 2.7× the largest file in either peer.
- **[high] 47-method `RoomStore` god interface** — `room-service/store.go:61-282` — spans rooms, subscriptions, thread-subscriptions, thread-rooms, room-members, users, apps and bot-cmd-menus. Every new RPC widens it, regenerates a 958-line `mock_store_test.go`, and forces all 273 handler tests through one mock. Splitting by aggregate (RoomReader / SubscriptionStore / ThreadStore / AppDirectory) is the standard fix.
- **[high] `handler.go` mixes eight unrelated domains in one file** — `room-service/handler.go:115-143` — create/members/roles, read position & receipts, threads, encryption keys, chatlist sections, app tabs & bot menus, restricted-room admin, plus federation plumbing. `handler_teams.go` already proves the by-domain split works; nothing was carried further.
- **[high] Driver types leak across the store boundary** — `room-service/handler.go:18` (imports `mongo-driver/v2/mongo`), `:774,1978,1990,2050,2492`, `handler_teams.go:192` — the contract is also inconsistent: `GetSubscription` maps to `model.ErrSubscriptionNotFound` (`store_mongo.go:253-264`) but `GetRoom` wraps the raw driver error (`store_mongo.go:235-242`) despite `ErrRoomNotFound` existing at `store.go:16`. `handler.go:774` and `:1990` defensively test *both*, which is the smell.
- **[medium] Two-phase construction with no compile-time completeness check** — `room-service/main.go:335-342` vs `handler.go:90` — 14 constructor args plus 8 fields assigned afterwards (`dekProvisioner`, `badge`, `graphClient`, `directoryClient`, `teamsMeetingStore`, `teamsEmailDomain`, `roomMembersLimit`, `roomMembersCallLimit`). Forgetting one yields a nil-panic or a silently disabled feature at runtime.
- **[medium] No complexity linting anywhere in the repo** — `.golangci.yml:1-40` — `funlen`, `gocyclo`/`cyclop`, `gocognit`, `dupl` and `maintidx` are all absent, so a 165-line, ~27-branch handler passes CI cleanly and nothing arrests further growth.
- **[medium] Federation epilogue duplicated 10×** — `room-service/handler.go:823,1419,1722,1800,2129,2238,2290,2402,2423,2472` — each site repeats `GetUserSiteID` → `!= h.siteID` → build event → `json.Marshal` → `federateOne` → `fmt.Errorf("federate X: %w")` (7 `GetUserSiteID` call sites at `:818,1657,1769,2213,2275,2359,2457`). `moveChat` carries two near-identical copies (`:2385-2410`, `:2407-2427`). A generic `federateSubscriptionEvent(ctx, account, roomID, eventType, payload)` collapses all ten.
- **[medium] Near-duplicate 54-line aggregation pipelines** — `room-service/store_mongo.go:1379-1431` vs `:1438-1490` — `ListReadReceipts`/`ListThreadReadReceipts` differ only in collection, match keys and bot-regex; identical `$lookup`/`$unwind`/`$replaceWith`/`$limit` tail and identical decode loop. Same room/thread mirroring in `MinSubscriptionLastSeenByRoomID` (`:1322`) vs `MinThreadSubscriptionLastSeenByThreadRoomID` (`:1977`), and `UpdateRoomMinUserLastSeenAt` (`:1366`) vs `UpdateThreadRoomMinUserLastSeenAt` (`:2005`).
- **[medium] Adding one room RPC touches six or more places** — a `pkg/subject` pattern, the registration block (`handler.go:115-143`), a new ~100-line method in the already-2,643-line file, `store.go` interface method #48, the `store_mongo.go` implementation, `make generate` for the 958-line mock, `helper.go` sentinels, and `docs/client-api.md`. The store/mock regeneration step is what makes small changes expensive.
- **[low] Repeated per-handler boilerplate** — `room-service/handler.go` — `var ctx context.Context = c` 22×, the `if span := trace.SpanFromContext(ctx); span.IsRecording()` block 11×, and the optional-body decode `if c.Msg != nil && len(c.Msg.Data) > 0 { … errcode.BadRequest("invalid request") }` 4× (`:435,470,571,610`). All are middleware or helper candidates.
- **[low] Error message no longer matches behavior** — `room-service/helper.go:77` vs `handler.go:635-637` — `errMentionableLimitInvalid` says `"limit must be > 0 and <= room user count + app count"`, but an over-cap limit is now clamped (`limit = min(limit, mentionableCap)`), never rejected. `docs/client-api.md:2044` already documents the clamp, so the string is the stale artifact.
- **[low] 8,048-line `handler_test.go` with 273 test funcs** — no per-domain test files despite `handler_teams_test.go` existing; a failing run is hard to bisect and the file is a merge-conflict magnet.
- **[nitpick] Documentation obligations are actually met** — every `chat.user.…` RPC has a `docs/client-api.md` section (`:1210-2794`); `rooms.info`/`thread.info`/`key.ensure` are `chat.server.` subjects and correctly exempt. (Content drift is reported separately under Integration.)
- **[nitpick] Ad-hoc test-file names** — `debug_log_test.go`, `store_section_order_test.go`, `store_mongo_readpref_test.go` sit outside the documented `handler_test.go`/`integration_test.go` convention. All three are legitimate, well-scoped concern-tests — not a problem in themselves, but a signal that the flat layout is being worked around.

## Recommendations

- **[high]** Adopt the sanctioned sub-package layout used by `user-service`/`history-service`: move config out of `main.go:33-131` into `config/`, the Mongo implementation into `mongorepo/`, and the handlers into `service/` with per-domain files (`rooms.go`, `members.go`, `read.go`, `threads.go`, `keys.go`, `sections.go`, `apps.go`, `teams.go`). Target no file over ~600 lines.
- **[high]** Split `RoomStore` (`store.go:61`) into 4–5 aggregate-scoped interfaces defined next to their consumers; generate mocks per interface so a members-only change stops regenerating a 958-line file the read-path tests depend on.
- **[high]** Close the driver leak: give `GetRoom`/`GetRoomAppRead` (`store_mongo.go:235-251`) the same `ErrRoomNotFound` mapping `GetSubscription` already does, document the duplicate-key contract as a store-level sentinel, then delete the `mongo` import from `handler.go:18` and `handler_teams.go:12` and drop the belt-and-braces double-checks at `:774` and `:1990`.
- **[medium]** Enable `funlen` (~80 lines), `gocyclo`/`cyclop` (~15) and `dupl` in `.golangci.yml`, with a time-boxed nolint baseline for the eight offenders listed above so the debt is visible and cannot grow.
- **[medium]** Extract a `federateSubscriptionEvent` helper covering the 10 call sites (`handler.go:823`…`:2472`) and a `readReceiptRows(coll, match, lookupKey, limit)` helper for `store_mongo.go:1379`/`:1438`; both are mechanical and each removes ~150 lines.
- **[medium]** Replace `NewHandler`'s 14 args plus 8 post-construction assignments (`handler.go:90`, `main.go:335-342`) with a `HandlerDeps` struct or functional options, validating required deps in the constructor so a mis-wired binary fails at startup rather than at first RPC.
- **[low]** Fix `errMentionableLimitInvalid`'s text (`helper.go:77`) to match the clamp behavior, and split `handler_test.go` along the same domain boundaries as the handler split so tests move with their subject.

---

# 5. Integration — 3 / 5

Subject construction, federation routing and event-struct contracts are essentially flawless. The score is held down by one real cross-site correctness bug, a mute event that lands on a consumer that cannot handle it, and four documentation drifts against `docs/client-api.md` — including canonical and derived views disagreeing with each other, which CLAUDE.md Section 5 explicitly forbids.

## Verified clean

Zero raw `fmt.Sprintf` subjects — all 27 registrations and 12 publishes go through `pkg/subject`. Every federated type (`InboxRoleUpdated`, `SubscriptionRead`, `ThreadRead`, `ThreadReadAll`, `MuteToggled`, `FavoriteToggled`, `SectionMoved`, `Opened`, `RoomRestricted`) is in exactly one partition set (`pkg/outbox/outbox.go:22-45`). No INBOX creation. Every published event struct carries `Timestamp` set with `time.Now().UTC().UnixMilli()` at the publish site. `encoding/json` throughout — correct, this is not a sonic hot-path service. No `map[string]interface{}`. IDs via `idgen.GenerateID`/`BuildDMRoomID`/`MessageIDFromRequestID`. No Cassandra or `messages_by_room` access. `ROOM_KEY_RETIRED_TTL=20m` is consistent across room-service, room-worker, bot-room-service and broadcast-worker.

## `docs/client-api.md` check

room-service registers 27 handlers (`handler.go:115-142`); 21 are client-facing `chat.user.…`.

**Documented and accurate (20):** create, member.add, member.remove, member.role-update, room.rename, member.list, member.statuses, subscription.mentionable, message.read, message.thread.read, message.read-receipt, mute.toggle, favorite.toggle, open, orgs.members, app.tabs, app.cmd-menu, teams.call, teams.meeting, teams.call.user.

**Not client-facing, no doc obligation:** `RoomRestricted`, `RoomsInfoBatchSubscribe`, `ThreadRoomInfoBatch`, `RoomThreadReadAllSubscribe`, `RoomKeyEnsure`.

**Drifts:**

- **[high] Drift 1 — error text.** `errNotRoomMember` is `"only room members can perform this action"` (`room-service/helper.go:41`), but the docs publish `"only room members can list members"` at `docs/client-api.md:1363,2293,2350,2388,6776` and `docs/client-api/request-reply.md:682,734,2375` (mute.toggle, chat.move, open, key.get, member.add). Clients matching on text break.
- **[medium] Drift 2 — `subscription.update` action enum.** `docs/client-api.md:1380` omits `"section_moved"`, which room-service publishes (`handler.go:2398,2416`); the derived `docs/client-api/events.md:111` *does* list it, so canonical and derived views disagree. The `roomName` omission list (`:1381`, `:1386`) likewise omits `section_moved`.
- **[medium] Drift 3 — index gaps.** `chat.move` is absent from the §3.1 index (`docs/client-api.md:1186-1208`) but present in the derived index (`request-reply.md:311`); `key.get` is in neither index (documented only in §5, `:6736`).
- **[medium] Drift 4 — `sectionOrder` nullability.** Documented as "Omitted on a remove" (`docs/client-api.md:2338`, `request-reply.md:711`), but `MoveChatResponse.SectionOrder` has no `omitempty` and `pkg/model/event.go:644-647` explicitly states it must always be sent.

## Findings

- **[high] Rebalanced `section_moved` events collapse to one OUTBOX publish** — `room-service/handler.go:2402` — `federateOne` derives the dedup ID via `natsutil.InboxDedupID` (`pkg/natsutil/request_id.go:144-151`), which uses `dedupSeed` **only** when the request ID is empty; the natsrouter `RequestID()` middleware always stamps one (`pkg/natsrouter/middleware.go:42`). Every row in the rebalance loop therefore publishes with the identical `Nats-Msg-Id` (`{requestID}:{destSite}`) and JetStream's duplicate window silently drops all but the first — a cross-site user's home replica keeps stale `sectionOrder` for every renumbered sibling. `handler_test.go:6570` discards `msgID`, so the test cannot catch it.
- **[medium] Mute member-events land on room-worker's unfiltered ROOMS consumer** — `room-service/handler.go:2208` — `chat.room.canonical.{site}.event.member.muted` falls inside `stream.Rooms`' `chat.room.canonical.{site}.>` binding, and room-worker creates its consumer with no `FilterSubject` (`room-worker/main.go:287`, `buildConsumerConfig:433`), so every mute/unmute hits `default: slog.Warn("unknown member operation")` (`room-worker/handler.go:274`).
- **[medium] Startup verifies only one of three streams it publishes to** — `room-service/bootstrap.go:43-61` — the fail-fast `js.Stream()` check covers ROOMS only; `OUTBOX-{siteID}` (`pkg/outbox/outbox.go:108`) and `MESSAGES-CANONICAL-{siteID}` (`handler_teams.go:269`) surface a misprovisioned deploy as a runtime 500. (Also reported under Architecture.)
- **[low] `subject.UserRoomEvent` does not `EncodeAccount`** — `pkg/subject/subject.go:493` — unlike `SubscriptionUpdate` (`:257`) and `RoomKeyUpdate` (`:497`); safe today only because `publishDMEvents`/`publishThreadDMEvents` (`handler.go:1532,1905`) fire on `RoomTypeDM` only.
- **[low] Dead RPC surface** — `room-service/handler.go:138` — `chat.server.request.room.{siteID}.key.ensure` has no caller anywhere in the repo; its comment (`handler.go:1913`, `pkg/subject/subject.go:537`) still says keys live "in Valkey" though `roomkeystore.OpenMongo` is used (`main.go`).
- **[low] Event structs missing `bson` tags** — `pkg/model/event.go:113,260,273` (`CanonicalMemberEvent`, `ThreadReadEvent`, `ThreadReadAllEvent`) — CLAUDE.md Section 3 requires both `json` and `bson` tags.
- **[nitpick] `SubscriptionUpdateEvent.Action` doc comment** — `pkg/model/event.go:84` — omits `"opened"` and `"section_moved"`.
- **[nitpick] `message_read`/`thread_message_read` triggered-event docs** — `docs/client-api.md:2104,2180` — name only `chat.room.{roomID}.event`, omitting the `chat.local.…` target `subject.RoomEventTargets` emits under `ROOM_SUBJECT_MODE=dual|local`, unlike the member-event docs (`:1455`, `:1584`).

## Recommendations

- **[high]** Make `federateOne` compose the dedup ID from the request ID **and** `dedupSeed` (e.g. `InboxDedupID(ctx, dest, seed)` → `base + ":" + seed + ":" + dest`), or pass `requestID+":"+row.RoomID` at `handler.go:2402`; extend `handler_test.go:6570` to assert the captured `msgID`s are distinct.
- **[high]** Fix the `errNotRoomMember` text at all eight doc sites, then add a test or lint step that greps `docs/client-api*.md` for the sentinel strings defined in `helper.go`, so this class of drift fails CI.
- **[medium]** Add `section_moved` to `docs/client-api.md:1380-1386`, add `chat.move` and `key.get` to the §3.1 index, and correct the `sectionOrder` "omitted on remove" note in both canonical and derived views.
- **[medium]** Give room-worker `cc.FilterSubjects = []string{…create, .member.add, .member.remove, .room.rename}` in `buildConsumerConfig`, or move the mute member-event onto its own subject outside `chat.room.canonical.{site}.>`.
- **[medium]** Extend `bootstrapStreams` to verify `stream.Outbox(siteID)` and `stream.MessagesCanonical(siteID)` exist — verify-only; creation stays with their owning services.
- **[low]** Add `EncodeAccount` to `subject.UserRoomEvent` for parity with the other per-user builders, updating broadcast-worker's four call sites in the same PR.
- **[low]** Delete the unused `key.ensure` registration and its subject builder, or document it in `docs/architecture.md` with its intended caller; fix the stale "Valkey" comments while there.

---

# 6. Performance — 3 / 5

No goroutine leaks, no `time.Sleep` synchronization, no lock held across I/O, and every request path carries a bounded context. The cost is concentrated in Mongo access patterns: unprojected whole-document reads, joins that run before their `$limit`, one true N+1 with no request-size cap, and unbounded org expansion. These scale with room and org size, so they are latent rather than visible today.

## Findings

- **[high] N+1 `GetApp` round trip per bot, with no request-size cap** — `room-service/handler.go:915-929` — `for _, a := range dedup(req.Users) { … h.store.GetApp(ctx, a) }` issues one sequential Mongo `FindOne` per `.bot` account. Nothing caps `len(req.Users)` in `addMembers` (`maxBatchSize` is enforced only in `roomsInfoBatch`/`threadRoomInfoBatch`, `handler.go:1183,1243`), so a 1,000-bot body is 1,000 serial round trips inside one 10s-guarded handler. A single `$in` batch would be one query.
- **[high] Mention autocomplete joins the whole room before `$limit`** — `room-service/store_mongo.go:1670-1755` — `$match {roomId}` → two per-row `$lookup`s (users + apps) → `$concat` keyword → regex `$match` → `$limit 3`. Mongo cannot push the limit ahead of the lookups because the filter depends on joined fields, so a 1,000-member room costs ~2,000 index lookups, 1,000 string concats and 1,000 unindexed regex evaluations to return 3 rows — on a per-keystroke path.
- **[high] Same join-before-limit shape in `ListMemberStatuses`** — `room-service/store_mongo.go:1611-1643` — `$limit` is the last stage after the users `$lookup` and the `statusText`/`statusIsShow` filter, so the default limit of 3 (`handler.go:515,587`) still joins every subscription in the room.
- **[high] Whole-document fetches violating "always project precisely"** — `room-service/store_mongo.go:875` (`GetUser`: no projection, decodes all ~24 `model.User` fields including `Services` (credential material), `Settings`, `Permissions`, `Chatlist`, while callers read only ID/Account/EngName/ChineseName/Roles), `:887` (`GetApp`: full `App` including `Sponsors`/`AppViewURL`, callers read only `Assistant.Enabled`), `:899` (`FindDMSubscription`: full ~35-field `Subscription`, callers read only `RoomID` — `handler.go:244,286`), `:1494` (`GetThreadSubscriptionByParent`, caller reads only `ThreadRoomID`).
- **[medium] `getRoomSubscriptions` fetches whole subscription docs, unprojected and unbounded** — `room-service/store_mongo.go:724` — `s.subscriptions.Find(ctx, bson.M{"roomId": roomID}, opts)` has sort/skip/limit but no projection, decoding 35-field structs to read 5 fields. `teamsRoomCall`/`teamsMeeting` call `ListRoomMembers(ctx, roomID, nil, nil, false)` with a nil limit (`handler_teams.go:62,128`), so the fallback path is fully unbounded.
- **[medium] Unbounded org expansion loaded into process memory** — `room-service/store_mongo.go:921-947` (`ListOrgMembers`: every user with `sectId|deptId == orgID`, no `Limit`, returned verbatim to the client via `listOrgMembers`) and `pkg/pipelines/orgdisplay.go:131` (`OrgDisplayUsers` loads every user of every org on the enriched member-list path just to compute `MemberCount`). A 20,000-person department materializes 20,000 docs per request.
- **[medium] Heaviest display reads run on the primary** — `room-service/store_mongo.go:553,511,606` — the enriched `room_members` aggregation, its existence probe and `attachOrgDisplay` all use primary handles, while `roomsSecondary` is used in exactly one place (`store_mongo.go:437`). `GetRoom` (`:238`) is primary on every `messageRead`/`addMembers`/`updateRole` even where it only supplies display counts.
- **[medium] Sequential cross-site fan-out in `expandChannelRefs`** — `room-service/handler.go:1103-1173` — refs are walked one at a time, each a same-site Mongo read or an `nc.Request` bounded at `MEMBER_LIST_TIMEOUT` (5s). With an uncapped `req.Channels`, worst case is K×5s against a 10s handler deadline. An `errgroup` fan-out is the obvious fix and the pattern is already used at `handler.go:1201,1367`.
- **[medium] Every reply from the app RPCs is marshaled twice** — `room-service/helper.go:548-566` plus `handler.go:2585,2642` — `boundedReply` calls `json.Marshal`, throws the bytes away, and `natsrouter.Context.ReplyJSON` (`pkg/natsrouter/context.go:235`) marshals the same value again. 2× CPU and 2× garbage per app-tabs/cmd-menu response.
- **[low] `ListSubscriptionsByRoom` projects `u.account` but decodes into `[]model.Subscription`** — `room-service/store_mongo.go:1910-1924` — a room-restrict on a 1,000-member room allocates 1,000 fat, pointer-heavy structs for one string each; a narrow row struct removes it.
- **[low] Per-query regex string construction on hot paths** — `room-service/helper.go:449-458` — `platformAdminRegex()`/`botOrPlatformAdminRegex()` run `regexp.QuoteMeta` and concatenate on every call, and are invoked inside `MinSubscriptionLastSeenByRoomID` (`store_mongo.go:1345`), `ListReadReceipts` (`:1392`), `CountMembersAndOwners` (`:384`) and `ListMentionableSubscriptions` (`:1675`).
- **[low] `ListRoomMembers` pays two round trips per call** — `room-service/store_mongo.go:511-520` — an `{_id:1}` existence probe followed by the real query.
- **[nitpick] Unhinted slice growth in per-request helpers** — `room-service/helper.go:497-507` (`dedup`), `:486-494` (`filterBots`), `handler.go:1158-1160` (`orgIDs`/`accounts` in `expandChannelRefs`); `dedup(req.Users)` is also computed twice per `addMembers` (`handler.go:915,942`).

## Recommendations

- **[high]** Replace the per-bot `GetApp` loop with a single batch store method (`FindEnabledAssistants(ctx, botAccounts)` → `$in` on `assistant.name`, projecting `assistant.name`/`assistant.enabled`), and cap `len(req.Users)+len(req.Orgs)+len(req.Channels)` against `maxBatchSize` in both `addMembers` and `createRoom` so every downstream `$in` and loop is bounded.
- **[high]** Restructure `ListMentionableSubscriptions` and `ListMemberStatuses` to filter before joining: pre-match on `u.account` (regex on the indexed subscription field) so the `$lookup` runs on a bounded candidate set, or take the two-step route already used by `attachUserDisplayNames` (`store_mongo.go:768`) — page subscriptions, then one `$in` batch against `users`/`apps`.
- **[high]** Add explicit projections to `GetUser`, `GetApp`, `FindDMSubscription`, `GetThreadSubscriptionByParent` and `getRoomSubscriptions`, following the `roomReadProjection`/`subscriptionReadProjection` pattern already established at `store_mongo.go:208-233`. `GetUser` in particular should stop pulling `services` off the wire.
- **[medium]** Bound `ListOrgMembers` and `OrgDisplayUsers` with a limit (or return counts via `CountDocuments` instead of materializing rows), and pass unbounded list responses through `boundedReply` so an oversized org cannot blow the NATS payload.
- **[medium]** Move the enriched member-list aggregation, its existence probe, `attachOrgDisplay`, and the display-only `GetRoom` calls in `listMemberStatuses`/`listMentionableSubscriptions` onto the `*Secondary` handles; keep authz and read-after-write reads on primary as they are today.
- **[medium]** Fan out `expandChannelRefs` with a bounded `errgroup` (`SetLimit(4-8)`) instead of the sequential loop, and drop the double marshal by having `boundedReply` return the already-marshaled bytes for the router to respond with.
- **[low]** Precompute the platform-admin/bot `bson.Regex` values once at `SetPlatformAdminAccountPrefix` time, and decode `ListSubscriptionsByRoom` into a narrow `{u:{account}}` row struct rather than `model.Subscription`.

---

# 7. Prioritized action list

Ordered by severity first, then impact ÷ effort. Items 1–4 are the ones that should land before anyone calls this service production-ready; 5–7 are the highest-leverage scale fixes; 8–10 are the structural work that makes everything after this review cheaper.

### 1. `critical` — Close and enforce the coverage floor
**Dimension:** Test coverage · **Evidence:** `room-service/store_mongo.go:1` (2.6% unit coverage), `room-service/deploy/azure-pipelines.yml:44`

Unit coverage is 57.7%, below both the 80% repo minimum and the 60% threshold. The 2,046-line store is verified only behind `//go:build integration`, which `make test` and the pre-commit hook do not run — so a projection or filter regression ships green locally and CI never objects. Decide which number CI enforces (unit-only with new tests for the store's pure logic, or a merged unit+integration profile), then wire `tools/coveragecheck -min 80` into the pipeline. Everything else in this report is harder to fix safely until this is done.

### 2. `high` — Fix the `section_moved` dedup-ID collision
**Dimension:** Integration · **Evidence:** `room-service/handler.go:2402`, `pkg/natsutil/request_id.go:144-151`

`federateOne`'s dedup ID ignores `dedupSeed` whenever a request ID is present, and the natsrouter middleware always stamps one. Every row in the `moveChat` rebalance loop therefore publishes with an identical `Nats-Msg-Id`, and JetStream drops all but the first — cross-site users keep stale `sectionOrder` on every renumbered sibling. This is silent data divergence, not a degraded path. Compose the ID from request ID *and* seed, and assert distinct `msgID`s in `handler_test.go:6570`.

### 3. `high` — Make a missing room return 404 everywhere, and stop leaking the driver
**Dimensions:** Code quality, Architecture, Maintainability · **Evidence:** `room-service/store_mongo.go:238,247`; `handler.go:663,757,885,1385` vs `:1976,2048`

One fix resolves three findings flagged independently by three auditors. `GetRoom` wraps every failure — including Mongo timeouts — in a "not found" message, four RPCs then surface a genuinely missing room as a 500 while two others return 404, and handlers branch on `mongo.ErrNoDocuments` directly (forcing a driver import into `handler.go:18`). Branch on `mongo.ErrNoDocuments` inside the store, return the existing `ErrRoomNotFound` sentinel, map it at all six call sites, and delete the driver import.

### 4. `high` — Stop treating unique-index creation as best-effort
**Dimension:** Architecture · **Evidence:** `room-service/main.go:222-224`, `store_mongo.go:99,120-127`

Startup logs a warning and continues when `EnsureIndexes` fails — including for the unique keys `room_members(rid,member.type,member.id)` and `subscriptions(roomId,u.account)` whose documented purpose is retry-safe writes *by room-worker*. A partial failure silently degrades another service's idempotency. Make unique-index failure fatal; keep warn-and-continue for non-unique performance indexes only. Better, move those two keys to their writer or to IaC and use `mongoutil.WarnMissingIndexes` here, as the service already does for user-service-owned indexes.

### 5. `high` — Kill the `addMembers` N+1 and cap the request
**Dimension:** Performance · **Evidence:** `room-service/handler.go:915-929`, `:1183,1243`

One sequential `GetApp` per bot account, with no cap on `len(req.Users)` — `maxBatchSize` is enforced on the batch RPCs but not here. A 1,000-bot body is 1,000 serial round trips inside a 10s guard. Replace with a single `$in` batch and cap `len(req.Users)+len(req.Orgs)+len(req.Channels)` in both `addMembers` and `createRoom`, which bounds every downstream loop at once. Small, local, and removes the service's worst latency cliff.

### 6. `high` — Filter before joining in the two autocomplete pipelines
**Dimension:** Performance · **Evidence:** `room-service/store_mongo.go:1670-1755`, `:1611-1643`

`ListMentionableSubscriptions` and `ListMemberStatuses` both place `$limit` after the `$lookup`s, so returning 3 rows from a 1,000-member room costs ~2,000 index lookups plus 1,000 unindexed regex evaluations — on a per-keystroke path. Pre-match on the indexed `u.account` field, or use the two-step page-then-`$in` route already established by `attachUserDisplayNames` (`store_mongo.go:768`).

### 7. `high` — Add the missing projections
**Dimensions:** Code quality, Performance · **Evidence:** `room-service/store_mongo.go:875,887,899,724,1494`

Five hot reads fetch whole documents against CLAUDE.md's "always project precisely" rule. `GetUser` is the notable one: it pulls all ~24 `model.User` fields — including the `services` credential material — off the wire when callers read five. The `roomReadProjection`/`subscriptionReadProjection` pattern at `store_mongo.go:208-233` is already there to copy, and the existing projection-drift integration test extends to cover them.

### 8. `high` — Fix `docs/client-api.md` drift and guard it
**Dimension:** Integration · **Evidence:** `room-service/helper.go:41` vs `docs/client-api.md:1363,2293,2350,2388,6776`

The published `errNotRoomMember` text does not match the sentinel, `section_moved` is missing from the canonical action enum while the derived view lists it (canonical and derived disagreeing is explicitly forbidden), `chat.move` and `key.get` are missing from the §3.1 index, and the `sectionOrder` nullability note contradicts `pkg/model/event.go:644-647`. Fix all four, then add a CI step that greps the docs for sentinel strings defined in `helper.go` so this class of drift cannot recur.

### 9. `medium` — Route mute events where something consumes them
**Dimension:** Integration · **Evidence:** `room-service/handler.go:2208`, `room-worker/main.go:287`, `room-worker/handler.go:274`

`chat.room.canonical.{site}.event.member.muted` falls inside room-worker's unfiltered `chat.room.canonical.{site}.>` consumer binding, so every mute and unmute lands on `default: slog.Warn("unknown member operation")`. Add `FilterSubjects` to room-worker's consumer config, or move the event off that subject tree. Cheap, and it stops a steady stream of misleading warnings from masking real ones.

### 10. `high` — Begin the sub-package decomposition
**Dimensions:** Maintainability, Architecture · **Evidence:** `room-service/handler.go:1` (2,643 lines, 8 domains), `store.go:61-282` (47 methods)

At 6,314 LOC the service matches `user-service` and `history-service` in size, but keeps everything in nine flat files where both peers use the sanctioned `config/ models/ mongorepo/ service/` split with no file over 993 lines. Adding one RPC touches six-plus places and regenerates a 958-line mock that all 273 handler tests depend on. Split `handler.go` along the seams `handler_teams.go` already demonstrates, break `RoomStore` into 4–5 aggregate interfaces, and enable `funlen`/`gocyclo`/`dupl` in `.golangci.yml` with a nolint baseline so the debt stops growing. Do this *after* item 1 — the coverage gate is what makes a refactor of this size safe.
