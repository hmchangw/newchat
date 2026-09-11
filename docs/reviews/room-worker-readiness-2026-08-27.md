# Production Readiness Review — `room-worker`

| | |
|---|---|
| **Service** | `room-worker` |
| **Date** | 2026-08-27 |
| **Branch** | `claude/production-readiness-room-worker-al2g9c` |
| **Base commit** | `309a339` |
| **Overall score** | **2.9 / 5** |

## Executive summary

`room-worker` is a mature, heavily-exercised service whose *conventions* are in good shape and whose *scale properties* are not. The architectural contract holds end to end: stream bootstrap is correctly opt-in and schema-only, every cross-site publish routes through the local OUTBOX via `outbox.Publish` (no direct INBOX writes anywhere), subjects are built exclusively through `pkg/subject`, the JetStream consumer is the correct high-throughput `cons.Messages()` + semaphore shape, and shutdown ordering matches the repo standard. Error wrapping, context threading and `pkg/errcode` tiering are near-textbook. Against that, three things block a clean production sign-off. First, the service has no `msg.InProgress()` heartbeat anywhere, so any handler that exceeds the 30s `AckWait` — large-room member removal, a Teams reconcile batch — is redelivered into a *second concurrent worker* running the same key rotation and fan-out. Second, the Teams migration lane derives its JetStream `Nats-Msg-Id` from the request ID rather than the per-event seed, so all but the first federation/search event of a multi-chat batch are silently deduplicated away; the same lane also assigns non-conforming DM room IDs that will collide with natively-created DMs. Third, package coverage is 62.9% against a repo floor of 80%, and the single JetStream entry point `HandleJetStreamMsg` — the subject-dispatch switch and the Ack-vs-Nak decision — has zero test coverage. Underneath all of it sits a 2,582-line `handler.go` with a 479-line function, which is why several of these defects diverged copy-to-copy rather than being fixed once.

## Dimension scores

| Dimension | Score |
|---|---|
| Go code quality | 3.5 / 5 |
| Architecture | 4 / 5 |
| Test coverage | 2 / 5 |
| Maintainability | 2 / 5 |
| Integration | 3 / 5 |
| Performance | 3 / 5 |
| **Average** | **2.9 / 5** |

## Findings by severity

| Severity | Count |
|---|---|
| `critical` | 4 |
| `high` | 13 |
| `medium` | 23 |
| `low` | 14 |
| `nitpick` | 6 |
| **Total** | **60** |

The four `critical` findings are: no `InProgress()` heartbeat under a 30s `AckWait` (Performance), Teams-lane dedup-ID collapse dropping federation events (Integration), and two structural findings on `handler.go` size and `processAddMembers` complexity (Maintainability).

---

# 1. Go code quality — 3.5 / 5

Well above average: correct `%w` wrapping at nearly every store call site, `ctx` threaded end to end, bounded worker pools with `WaitGroup` (no `time.Sleep`, no leaked goroutines), clean `pkg/errcode` Tier-1/Tier-2 usage, and textbook `main.go` shutdown ordering. Points come off for concrete CLAUDE.md §3 violations, one client-facing error-message leak, and a `handler.go` that has outgrown its file.

## Findings

**`high` — `err.Error()` interpolated into a client-facing error message.** `handler.go:2258` builds `errcode.BadRequest(fmt.Sprintf("unmarshal rename request: %s", err.Error()))`. This contradicts the guard the same file states 770 lines earlier (`handler.go:1487-1489`: *"Never interpolate err.Error() — json.SyntaxError embeds the offending payload substring from an unauthenticated entry-point"*) and CLAUDE.md's "Never expose raw internal errors to clients". Every other unmarshal site (`handler.go:300, 991, 1490`, `teamsroomcreate.go:28`) gets this right.

**`medium` — 22 silently discarded `json.Marshal` errors, none commented.** `handler.go:188, 446, 461, 474, 498, 500, 526, 658, 672, 683, 698, 721, 1181, 1188, 1203, 1227, 1252, 1289, 1341, 1823, 1831, 1853`. CLAUDE.md: "Never ignore errors silently — comment if intentionally discarded." The file is inconsistent with itself: `publishCanonical` (`handler.go:1926`), `publishChannelSysMessages` (`:1866, :1899`) and all of `teamsroomcreate.go` do check. Related: `mustMarshal` (`handler.go:1340-1343`) is named `must*` but returns `nil` on error instead of panicking, inverting the Go convention — a marshal failure would federate an empty payload (`handler.go:747`).

**`medium` — `processAddMembers` is 479 lines** (`handler.go:824-1302`) inside a 2,582-line `handler.go`. Orphaned step numbers (`// 6.` at `:1111`, `// 8.` at `:1157`, `// 10.` at `:1259` — 1-5, 7, 9 gone) and stale ticket markers (`// Task 35:` `:1782`, `// Task 36:` `:1800`, `// Task 37:` `:1837`) show organic growth. CLAUDE.md sanctions a sub-package layout for larger surfaces; this service qualifies.

**`medium` — post-construction field injection instead of the repo's option-func convention.** `main.go:236, 254, 255, 256` assign `handler.publishUsers`, `dekProvisioner`, `valkey`, `reconcileTTL` after `NewHandler`; `handler.go:63-65` admits it is "to avoid churning every NewHandler caller". Every sibling service uses variadic options (`message-worker/handler.go:53`, `broadcast-worker/handler.go:130`, `inbox-worker/handler.go:167`), leaving `Handler` constructible in a half-initialized state.

**`medium` — projection rule violated on two reads.** CLAUDE.md: "every find/aggregation MUST specify an explicit projection". `store_mongo.go:90` (`ListByRoom`) fetches full subscription documents for a whole room; its only production caller (`handler.go:2311-2316`) uses `sub.User.Account` alone. `store_mongo.go:174` (`GetRoom`) fetches whole documents. `GetUserWithMembership`'s `$project` (`store_mongo.go:337`) only *excludes* two fields, returning the entire user doc.

**`medium` — `GetRoom` mislabels every failure as "not found".** `store_mongo.go:173-179` wraps any `Decode` error (network timeout included) as `fmt.Errorf("room %q not found: %w", ...)` and never checks `mongo.ErrNoDocuments`, despite CLAUDE.md requiring the explicit check and despite `ErrRoomNotFound` existing at `store.go:17`. Sibling methods (`GetSubscription` `:297`, `GetApp` `:219`, `ApplyMemberCountDelta` `:158`) do it correctly.

**`low` — bare `err` returns in the store.** `store_mongo.go:195, 668, 674, 740, 744` return unwrapped errors; `:740`/`:744` also drop context from a raw driver error. CLAUDE.md: "Never return bare `err`".

**`low` — 7 non-`Context` slog calls drop the request ID.** `handler.go:113, 2262, 2383, 2391, 2406, 2419, 2541`. CLAUDE.md requires the correlation ID "in all log lines"; surrounding code uses `slog.ErrorContext` correctly. `handler.go:2262-2266` also breaks key casing (`roomID`, `requestID`) against the `room_id`/`request_id` used at ~20 other sites; `:2541` uses `roomId`.

**`low` — dead code.** `ErrNotChannelRoom` (`store.go:18`) is never returned by any store method, making `handler.go:2272` an unreachable branch — `store.go:150-151` documents this. `ErrOwnerNotSubscribed` (`store.go:19`) is entirely unused. `errPermanent` (`handler.go:43`) is production code referenced only by tests.

**`low` — `$lookup` sites lack the mandated justification marker.** `store_mongo.go:325, 341, 415, 437` carry prose comments but not the required `// $lookup justification: …` form, and the dept-aware lookup was recently modified.

**`nitpick`** — `SubscriptionStore` is a ~30-method interface (`store.go:64-166`); detached doc comment (blank line between comment and func) at `handler.go:1355-1357`; duplicated, self-contradicting doc block on `createSelfDM` (`handler.go:1379-1390`); stale `ListByRoom` comment claiming it is "Not part of SubscriptionStore" (`store_mongo.go:85-86`) while `store.go:162` declares it; `go func(acct string)` param-shadowing no longer needed under Go 1.22+ loop semantics (`handler.go:2561`); store DTOs carry `bson` but no `json` tags (`store.go:30-62`).

## SAST results

SAST is a blocking CI gate per CLAUDE.md §5. Two of the three scanners could not reach their upstreams from this environment — that gap is itself worth noting before release.

| Scanner | Result |
|---|---|
| **gosec** (`-severity medium -confidence medium -tests=true ./...`) | **PASS — zero findings**, repo-wide including `room-worker/` |
| **semgrep** | Makefile's registry ruleset (`p/golang`) unreachable — agent proxy returns `403` for `semgrep.dev`. Ran semgrep 1.163.0 against repo-local rules (`--config .semgrep`, ERROR+WARNING): **2 findings, both `errcode-no-multi-wrap-errcode` in `tools/loadgen/soak_rpc.go:497,547` — none in `room-worker/`** |
| **govulncheck** | **Could not run** — proxy returns `403 Forbidden` for `https://vuln.go.dev/index/modules.json.gz`. **No conclusion on dependency CVEs.** |

No medium-or-above SAST finding touches `room-worker/`.

## Recommendations

1. **`high`** — Drop `err.Error()` from `handler.go:2258`; return `permanent(errcode.BadRequest("unmarshal rename request"))`, matching the other four unmarshal sites.
2. **`medium`** — Replace the 22 `json.Marshal` discards with checked marshals (`publishCanonical` shows the pattern), and either make `mustMarshal` actually panic or rename it `marshalOrEmpty` and check at the call site.
3. **`medium`** — Split `handler.go` along the four flows (`handler_add.go` / `handler_remove.go` / `handler_create.go` / `handler_rename.go`) and extract `processAddMembers`' write-planning (needSub/needIRM/backfill) into named helpers.
4. **`medium`** — Move `dekProvisioner`, `valkey`, `publishUsers`, `reconcileTTL` into `NewHandler` variadic options, mirroring `broadcast-worker/handler.go:130`.
5. **`medium`** — Add projections to `ListByRoom` and `GetRoom` (or give `processRoomRename` the existing accounts-only `GetSubscriptionAccounts`), and map `mongo.ErrNoDocuments` to `ErrRoomNotFound` in `GetRoom`.
6. **`low`** — Wrap the five bare `err` returns in `store_mongo.go`; convert the seven `slog.X` calls to `slog.XContext` and normalize keys to `room_id`/`request_id`.
7. **`low`** — Delete `ErrNotChannelRoom` + its dead branch (`handler.go:2272`), `ErrOwnerNotSubscribed`, and move `errPermanent` into a `_test.go` file; fix the four stale/duplicated doc comments.

---

# 2. Architecture — 4 / 5

`room-worker` is conformant on every hard convention this dimension checks. The deductions are for one latent cross-site routing defect and a cluster of DI/boundary erosions in a handler that has outgrown its file.

**Verified clean.** `bootstrap.go:42-59` matches the opt-in contract exactly — Name+Subjects only, from `pkg/stream.Rooms`/`RoomsTeams`, create-when-enabled / verify-else-fail, no federation config — identical in shape to `inbox-worker/bootstrap.go:45` and `outbox-worker/bootstrap.go:40`. All nine cross-site publishes route through `h.federate` → `outbox.Publish` (`handler.go:342`); there is **zero** use of `subject.InboxExternal` in the service, so the order-sensitive `member_added`/`member_removed`/`room_renamed` rule holds. The consumer (`main.go:278-328`) is the high-throughput `cons.Messages()` + `MAX_WORKERS` semaphore + `PullMaxMessages(2*MaxWorkers)` shape, matching `message-worker/main.go:203-250`, with the loop goroutine itself counted in the `WaitGroup` — no pattern mixing. Shutdown (`main.go:343-381`) is `iter.Stop()` → `wg.Wait()` → `router.Shutdown` → `Drain` → DBs → obs-last, at 25s. Config is one typed `caarlos0/env` struct (`main.go:33-95`); no `os.Getenv`. No raw `fmt.Sprintf` subject anywhere in non-test code.

## Findings

**`high` — HR identity fanout is addressed to the local site, not the central one.** `main.go:262` calls `subject.OrgSyncUsersUpsert(cfg.SiteID)`, but that builder's parameter is `centralSiteID` (`pkg/subject/subject.go:1774`) and the only stream owning `chat.hr.{X}.>` is `HR-{centralSiteID}` (`pkg/stream/stream.go` `OrgSyncStream`). Peers that get this right carry an explicit knob — `teams-hr-sync/config.go:34` (`CENTRAL_SITE_ID`), `search-sync-worker/main.go:56` (`HR_CENTRAL_SITE_ID`); room-worker's config has no such field. On any non-central site the JetStream publish finds no stream, so `resolveMember` returns an error (`teamsroomcreate.go:258-260`) and the member is WARN-skipped (`teamsroomcreate.go:125-127`) — silent member loss on the Teams migration path. `message-worker/main.go:226` and `admin-service/handler.go:215` share the defect.

**`medium` — dependencies injected by post-construction field writes.** `NewHandler` (`handler.go:85-95`) takes six deps; four more are assigned directly from main afterwards: `handler.publishUsers` (`main.go:257`), `handler.dekProvisioner` (`:271`), `handler.valkey` (`:272`), `handler.reconcileTTL` (`:273`), plus `SetKeyFanoutWorkers` (`:254`). The store is likewise mutated live via `EnableUserCache`/`EnableRoomMetaCache` (`main.go:215, 226`). Violates "Handler structs hold dependencies injected via constructor"; the code comment at `handler.go:64-67` names the reason (avoiding caller churn), which functional options solve properly.

**`medium` — dispatch on subject string suffixes rather than a `pkg/subject` parser.** `handler.go:255-276` routes with `strings.HasSuffix(subj, ".create")`, `.member.add`, `strings.Contains(subj, ".teams.room.canonical.")`. It is order-dependent (`.teams.create` at `:263` must precede `.create` at `:270`) and `.create` is a catch-all matching any `chat.room.canonical.{site}.*.create`. Siblings dispatch on a typed field (`inbox-worker/handler.go:211`, `message-worker/handler.go:92`), and `subject.ParseOutbox` (`subject.go:247`) is the in-repo precedent for a parser. The `default:` arm (`:274-276`) leaves `err == nil`, so an unroutable subject is silently Acked away by `SettleQuiet` at `:279`.

**`medium` — `SubscriptionStore` is a 31-method god-interface.** `store.go:64-166` spans subscriptions, rooms, users, apps, room_members, thread state, org display, key material and crossSite flags. Consumer-defined (correct) but ISP-defeating: `mock_store_test.go` must stub all 31, and no workflow can take a narrow dependency. The workflow seams already exist in code (add / remove / create / rename / teams).

**`medium` — `handler.go` at 2,582 lines carries five workflows plus a synchronous RPC.** `processAddMembers` alone is 474 lines (`handler.go:824-1298`). `serverCreateDM` (`:1954`) is a request/reply handler sharing the `Handler` struct and the process with the JetStream consumer, so Mongo-pool exhaustion in one starves the other — partly mitigated by `natsrouter.GuardConfig` (`main.go:52, 275`).

**`low` — blank `SiteID` leaks into the remote-site set.** `findRemoteSitesForAccounts` (`handler.go:2220-2241`) filters only `!= h.siteID`, unlike every sibling bucket (`handler.go:535, 732, 1269`; `teamsroomcreate.go:309, 373`). Only `outbox.Publish`'s blank-dest no-op (`pkg/outbox/outbox.go:86`) prevents an invalid subject.

**`low` — durability inferred from the dedup-id argument.** The single `PublishFunc` (`main.go:234-253`) picks core NATS when `msgID == ""` and JetStream otherwise; a forgotten dedup id silently downgrades a durable federation publish. `broadcast-worker/main.go:314-320` shows the cleaner split (a dedicated always-JetStream `outboxPublish`).

**`nitpick` — missing `$lookup` justification markers.** `store_mongo.go:326, 342, 395, 415` have explanatory prose but not the mandated `// $lookup justification: …` comment.

## Recommendations

1. **`high`** — Add `HRCentralSiteID string` (`env:"HR_CENTRAL_SITE_ID,required"`) to room-worker's config and pass it to `subject.OrgSyncUsersUpsert` at `main.go:262`; file the same fix for `message-worker/main.go:226` and `admin-service/handler.go:215`.
2. **`medium`** — Convert the five post-construction assignments (`main.go:254-273`) into `NewHandler` functional options so the handler is fully wired at construction; likewise the two store cache enablers (`main.go:215, 226`).
3. **`medium`** — Add `subject.ParseRoomCanonicalOp(subj) (op string, ok bool)` to `pkg/subject` and switch `handler.go:255-276` to an exact-match `switch op`; make the unknown-op arm return `permanent(errcode.BadRequest(...))` instead of leaving `err == nil`.
4. **`medium`** — Split `SubscriptionStore` (`store.go:64-166`) into per-workflow interfaces (`MemberStore`, `RoomCreateStore`, `RenameStore`, `TeamsMigrationStore`) embedded in the handler, and regenerate mocks.
5. **`medium`** — Extract `processAddMembers`/`processRemoveMember` into `membership.go` and `processCreateRoom`/`finishCreateRoom` into `roomcreate.go`, leaving `handler.go` as struct, constructor and dispatch.
6. **`low`** — Skip `users[i].SiteID == ""` in `findRemoteSitesForAccounts` (`handler.go:2231`) so the invariant is enforced at the call site like every sibling bucket.
7. **`low`** — Split the `PublishFunc` into an ephemeral `publish` and an always-JetStream `durablePublish` (per `broadcast-worker/main.go:314`) so transport choice is expressed in the type rather than inferred from an argument.

---

# 3. Test coverage — 2 / 5

Score is rubric-forced. The suite is genuinely strong on `handler.go` (87.3%, 186 unit tests, deep federation/idempotency/redelivery coverage) and would rate 4/5 on merit, but total package coverage is **62.9%** — below the CLAUDE.md §4 80% floor — which caps this dimension at 2.

## Measurements

**`make test SERVICE=room-worker` — PASS.** `ok github.com/hmchangw/chat/room-worker 1.281s` (exit 0, `-race`). No flakes, no skips.

**Coverage — 62.9%** (`go tool cover -func`, untagged run = what CI runs):

| File | Statement coverage |
|---|---|
| `sysmsg.go` | 100.0% (27/27) |
| `bootstrap.go` | 100.0% (10/10) |
| `handler.go` | 87.3% (864/990) |
| `teamsroomcreate.go` | 82.6% (142/172) |
| `main.go` | 9.2% (18/196) |
| `store_mongo.go` | 0.0% (0/291) |

Lowest-covered functions — 38 sit at 0.0%, of which 36 are every method in `store_mongo.go` (integration-tag-only, so unreachable in the untagged run):

| Function | file:line | % |
|---|---|---|
| `HandleJetStreamMsg` | `handler.go:239` | 0.0 |
| `main` | `main.go:97` | 0.0 |
| `NewMongoStore` | `store_mongo.go:42` | 0.0 |
| `GetUserWithMembership` | `store_mongo.go:318` | 0.0 |
| `GetOrgMembersWithIndividualStatus` | `store_mongo.go:382` | 0.0 |
| `ListAddMemberCandidates` | `store_mongo.go:619` | 0.0 |
| `CreateRoom` | `store_mongo.go:228` | 0.0 |
| `ApplyMemberCountDelta` | `store_mongo.go:144` | 0.0 |
| `roomLocalityForMember` | `handler.go:2428` | 28.6 |
| `publishSubscriptionUpdate` | `handler.go:111` | 50.0 |
| `cleanupThreadMembership` | `handler.go:363` | 60.0 |
| `publishSubscriptionUpdates` | `handler.go:2171` | 71.4 |
| `publishRoomEvent` / `publishMemberEvent` | `handler.go:2402` / `:2415` | 75.0 |
| `fanOutKey` | `handler.go:2528` | 76.7 |
| `publishChannelSysMessages` | `handler.go:1863` | 77.8 |
| `processCreateRoom` | `handler.go:1475` | 79.7 |
| `reconcileTeamsRoom` | `teamsroomcreate.go:45` | 79.8 |

Integration tests were not runnable in this environment (Docker unavailable), so `store_mongo.go` coverage could not be measured directly.

**`make generate` — clean, no diff.** The pinned mockgen v0.6.0 fails against this repo (`x/tools` built for go1.24 vs go1.25 sources), so mockgen was rebuilt against `x/tools@v0.49.0` and `make generate SERVICE=room-worker` re-run: succeeded with `git status --porcelain` and `git diff --stat` both empty; a byte-diff of freshly generated output against `mock_store_test.go` was identical. **Mocks are current — no stale-mock finding.** No revert was needed; tree confirmed clean.

## Findings

**`high` — coverage below repo minimum 80%, currently 62.9%.** CLAUDE.md §4. Driven entirely by `store_mongo.go` (0/291) and `main.go` (18/196).

**`high` — `HandleJetStreamMsg` (`handler.go:239-279`) is 0% covered; no test calls it** (`grep '\.HandleJetStreamMsg(' *_test.go` → empty). Uncovered: the whole subject-dispatch switch, the `natsutil.DecodePayload` failure → `permanent(BadRequest)` branch (`:265-270`), the unknown-subject default (`:273`), and the `jsretry.SettleQuiet` Ack-vs-Nak decision (`:279`). This is the service's only JetStream entry point — a mis-suffixed route or wrong Ack/Nak means a silent drop or an infinite poison-pill loop. A `fakeJSMsg` stub already exists (`handler_test.go:6598-6617`), so the test is ~40 lines.

**`high` — all store coverage is unreachable from the gate CI runs.** `room-worker/deploy/azure-pipelines.yml:44` runs `go test ./room-worker/... -coverprofile` untagged; `integration_test.go` is `//go:build integration` and is never executed in the pipeline, so 291 statements can never earn credit. The integration suite itself is otherwise good (49 tests, correct `testutil.MongoDB` + `TestMain`), but has only 2 `require.Error`/`assert.Error` assertions across 2,463 lines — store error paths are effectively unasserted.

**`medium` — best-effort/fail-open branches are the least covered, and they are precisely the ones that hide incidents.** `roomLocalityForMember` 28.6% (the `GetRoomMeta` error → route-global fallback, `handler.go:2433-2437`, never exercised); `publishSubscriptionUpdate` 50%; `publishRoomEvent`/`publishMemberEvent` 75% (the `h.publish` error-log branch). Subject-selective publish-failure injection already exists for hard-fail paths (`handler_test.go:2104, :2150, :4649`) — the harness just isn't applied to the swallow-and-log paths.

**`medium` — table-driven discipline violates CLAUDE.md §4.** 186 top-level `func Test` in `handler_test.go` against 16 `t.Run` and 9 table literals; `teamsroomcreate_test.go` is 21 funcs / 1 `t.Run`. Clear copy-paste families: `TestProcessRoomRename_{Global,Local,Dual}Mode_*` + `_CrossSiteRoomAlwaysGlobal` (`handler_test.go:7091-7139`) differ only in `h.routeMode`; `TestHandler_PublishChannelSysMessages_*` (`:5805-5916`); `TestFillAsyncError_*` (`:3915-3949`).

**`medium` — the 80% floor is documentation-only for this service.** The pipeline writes `coverage-room-worker.out` and nothing consumes it, even though `tools/coveragecheck` exists and is wired for loadgen (`Makefile:141-143`).

**`medium` — `main.go` at 9.2%.** `main()` (`main.go:97`) is 196 statements of untestable inline wiring — config-validation exits, Valkey/atrest/Vault branches, consumer creation, shutdown ordering. Only `runJobWithRecovery` and `buildConsumerConfig` are reachable.

**`low` — process-global mutation in tests.** `debug_log_test.go:70` mutates `slog.SetDefault` + `logctx.Configure`. Restored via `t.Cleanup`, but it is package-wide shared mutable state (CLAUDE.md §4 Test Independence) that is safe only because nothing in the package calls `t.Parallel()`.

**`low` — key-version skew is thinly covered.** `stubRoomKeyStore` (`mock_publisher_test.go:47`) is the default key store for most handler tests and always returns Version 0 with placeholder bytes, so rotation version-skew is only covered by the four `TestHandler_RotateAndFanOut_*` tests (`handler_test.go:6362-6483`).

**`nitpick`** — `integration_test.go:1690` `startEmbeddedNATS` spins an in-process `natsserver` instead of `testutil.NATS(t)`; not a container, so not strictly a §4 violation, but it diverges from the repo's single-source-of-truth helper.

## Recommendations

1. **`high`** — Add a table-driven `TestHandleJetStreamMsg_Dispatch` using the existing `fakeJSMsg`: one row per subject suffix (`.member.add`, `.member.remove`, `.teams.room.canonical.`, `.teams.create`, `.create`, `.room.rename`, unknown) asserting the correct processor ran and the Ack/Nak outcome, plus a row for an undecodable teams frame asserting Ack (permanent), not Nak.
2. **`high`** — Add a coverage gate to `room-worker/deploy/azure-pipelines.yml` using the existing `tools/coveragecheck`: `-include room-worker/handler.go -min 90`, `-include room-worker/teamsroomcreate.go -min 85`. Gate the files CI can actually measure rather than a package total that store code structurally cannot reach.
3. **`high`** — Run integration tests in CI (`go test -tags integration ./room-worker/...`) and merge both profiles before checking the 80% package floor; otherwise `store_mongo.go`'s 291 statements stay permanently uncounted.
4. **`medium`** — Extract `run(ctx context.Context, cfg config) error` from `main()` (`main.go:97`) and unit-test the validation/exit branches; leave `main()` as a 5-line shim.
5. **`medium`** — Introduce one shared `failingPublisher{failOn func(subj string) bool}` helper and use it to cover `publishRoomEvent`, `publishMemberEvent`, `publishSubscriptionUpdate(s)`, and `roomLocalityForMember`'s `GetRoomMeta` error fallback — asserting the handler swallows-and-logs rather than failing the job.
6. **`medium`** — Collapse the copy-paste test families into tables (start with the four rename route-mode tests at `handler_test.go:7091-7139` and `TestHandler_PublishChannelSysMessages_*` at `:5805`).
7. **`low`** — Add error-path assertions to `integration_test.go` for `mongo.ErrNoDocuments` mapping in `GetRoom`/`GetUser`/`GetSubscription`/`GetApp`, and for `DeleteSubscription`/`DeleteRoomMember` on a missing document.

---

# 4. Maintainability — 2 / 5

## Findings

**`critical` — `handler.go` is a 2,582-line god-file holding six unrelated flows.** It carries add-member, remove-member (individual + org), create-room (channel/DM/botDM/self-DM), rename, a synchronous NATS RPC (`serverCreateDM`, `:1954`), and room-key fan-out. That is 3.3× `message-worker/handler.go` (771), 1.8× `broadcast-worker/handler.go` (1,405), and 3.5× `inbox-worker/handler.go` (743). CLAUDE.md §1 assumes one `handler.go` per service, but every comparable worker keeps that file under ~1,400 lines by splitting concerns into named files (`broadcast-worker/preview.go`, `coalescer.go`, `roomactivity.go`). `room-worker` only did this twice (`teamsroomcreate.go`, `sysmsg.go`).

**`critical` — `processAddMembers` (`handler.go:824-1302`) is 479 lines with 83 branch points and 6 levels of indentation.** It performs: unmarshal → concurrent input load → cross-site marking → needSub/needIRM partition → HSS resolution → user lookup+validation → subscription build → key fetch/validate → sub commit → room-member build → org backfill (nested 5 deep, `:1059-1103`) → count delta/reconcile → cache bust → room re-read → subscription fan-out → member event → internal inbox publish → sys-msg build/publish → per-site federation. No single reader can hold this; it is the largest maintainability liability in the service.

**`high` — four more functions well past a reviewable size:** `processRemoveOrg` `:546-769` (224 lines, 32 branches), `processRemoveIndividual` `:373-545` (173), `processCreateRoom` `:1475-1639` (165), `processRoomRename` `:2243-2401` (159), `serverCreateDM` `:1954-2092` (145). Also `main.go:97-396` — `main()` is 300 lines of wiring; `teamsroomcreate.go:45-215` — `reconcileTeamsRoom` is 171.

**`high` — the "publish an internal INBOX event" block is copy-pasted 7 times** with no helper: `handler.go:467-478`, `:676-687`, `:1196-1211`, `:1824-1835`, `:2334-2353`, `teamsroomcreate.go:292-305`, `:356-368`. Error handling silently diverges between copies (some `slog.ErrorContext`-and-continue, some `return`).

**`high` — the per-destination-site bucket-and-federate loop is duplicated 5 times**: `handler.go:730-750`, `:1266-1295`, `:1762-1780` + `:1838-1858`, `teamsroomcreate.go:307-324`, `:371-388`. A sixth variant (`findRemoteSitesForAccounts`, `:2220`) exists but is used only by rename.

**`medium` — an extracted helper is bypassed by half its call sites.** `publishCanonical` (`:1925`) exists, yet three sys-message publishes rebuild `model.MessageEvent` + `subject.MsgCanonicalCreated` + `CanonicalDedupID` inline: `:520-529`, `:715-724`, `:1246-1255`. Same for `mustMarshal` (`:1340`) — defined once, called once (`:747`), while 22 other sites in the same file use bare `data, _ := json.Marshal(...)`.

**`medium` — dead code.** `ErrOwnerNotSubscribed` (`store.go:19`) is declared and never referenced anywhere. `handler.go:2272-2274` branches on `errors.Is(err, ErrNotChannelRoom)`, which `store.go:150-151` explicitly documents as no longer returned and `store_mongo.go:764-772` confirms (only `ErrRoomNotFound`) — an unreachable branch.

**`medium` — leaky store abstraction: 31 methods on `SubscriptionStore` (`store.go:64-166`), 35 on `MongoStore`, 9 struct fields spanning 7 collections** (`store_mongo.go:22-40`: subscriptions, rooms, room_members, users, apps, thread_rooms, thread_subscriptions). Siblings: `broadcast-worker` 8, `message-worker` 16, `message-gatekeeper` 3. Only `room-service` (55) is larger, and it is the sanctioned large-surface RPC service. The `Handler` struct itself carries 11 fields (`handler.go:59-83`), 5 of which (`dekProvisioner`, `valkey`, `reconcileTTL`, `publishUsers`, `keyFanoutWorkers`) bypass `NewHandler` and are poked directly in `main.go:271-274` — a documented workaround ("to avoid churning every NewHandler caller", `:66-67`) that is itself the refactor signal.

**`low` — stale scaffolding comments betray incremental accretion:** `handler.go:1111` `// 6.`, `:1157` `// 8.`, `:1259` `// 10.` — a numbered plan with steps 1-5, 7, 9 deleted; `:1782`/`:1800`/`:1837` `// Task 35/36/37:` reference tickets no reader can resolve.

**`nitpick`** — `createSelfDM` carries two stacked, partly contradictory doc comments (`:1379-1383` and `:1384-1390`). `buildDMSubs`' doc comment (`:1355`) is orphaned by a blank line at `:1356`, so godoc drops it. The four `$lookup` sites (`store_mongo.go:326, 342, 395, 415`) lack the `// $lookup justification: …` marker CLAUDE.md requires.

## Recommendations

1. **`critical`** — Split `handler.go` by flow into `handler_addmember.go`, `handler_removemember.go`, `handler_createroom.go`, `handler_rename.go`, `handler_syncdm.go`, `keyfanout.go`, leaving `handler.go` with the struct, constructor, and `HandleJetStreamMsg` dispatch (`:239-280`). Zero behaviour change, mechanical, unblocks everything below.
2. **`critical`** — Decompose `processAddMembers` (`:824-1302`) at its existing seams into ~6 named steps: `partitionCandidates`, `resolveAddMemberUsers`, `commitAddMemberWrites`, `buildBackfillMembers` (extract `:1059-1103` first — deepest nesting), `publishAddMemberEvents`, `federateAddMembers`. Target ≤80 lines each.
3. **`high`** — Extract `func (h *Handler) publishInternalInbox(ctx, siteID string, t model.InboxEventType, payload []byte, seed string)` and replace all 7 copies; extract `federateByHomeSite(ctx, roomID, accountsBySite, ...)` and replace all 5 bucketing loops.
4. **`high`** — Route the three inline canonical publishes (`:527`, `:722`, `:1253`) through `publishCanonical`; delete `mustMarshal` or apply it consistently.
5. **`medium`** — Delete `ErrOwnerNotSubscribed` (`store.go:19`) and the unreachable `ErrNotChannelRoom` branch (`handler.go:2272-2274`).
6. **`medium`** — Move the five post-construction `handler.X = …` assignments (`main.go:271-274`) into a functional-options `NewHandler`, and extract `main()`'s wiring into `buildHandler(cfg)` / `startConsumer(...)`.
7. **`low`** — Strip the `// 6.` / `// 8.` / `// 10.` / `// Task 35-37` markers and the duplicated `createSelfDM` doc block; add `$lookup justification:` comments to `store_mongo.go:326, 342, 395, 415`.

---

# 5. Integration — 3 / 5

The live client paths are close to exemplary — zero raw `fmt.Sprintf` subjects, every publish through `pkg/subject` / `outbox.Publish`, `DurableConsumerDefaults` + `jsretry.SettleQuiet` (no bare `Nak`), INBOX never created, all IDs via `pkg/idgen`. The Teams-migration lane is where the contract breaks.

## Findings

**`critical` — Teams lane: federation/search events silently collapse on duplicate `Nats-Msg-Id`.** `natsutil.InboxDedupID` ignores its `payloadSeed` whenever ctx carries a request ID (`pkg/natsutil/request_id.go:144-151`), and room-worker always mints one (`main.go:419-424`). One ROOMS-TEAMS message reconciles N chats under a single request ID (`teamsroomcreate.go:32-38`), and each chat emits up to three INBOX-internal publishes — `member_added` and `member_removed` both via `teamsroomcreate.go:303` (called at `:202` and `:205`) and `member_joinedat_refreshed` at `:366` — all with Msg-Id `{requestID}:{siteID}`. JetStream msg-id dedup is stream-wide, so only the **first** publish survives; removals and joinedAt fixes are dropped. Identically for the OUTBOX forwards, which collapse per destination (`:320-321`, `:384-385`). Invisible to tests: `teamsroomcreate_test.go` never sets a request ID (exercising the seed fallback) and `integration_test.go` has no Teams coverage at all.

**`high` — migrated DM rooms violate the DM room-ID convention.** `teamsroomcreate.go:61` assigns `idgen.DeterministicID([]byte(chat.ID))` (17-char base62) to every migrated room, including those typed DM by `roomTypeFromTeamsChatID` (`:216-221`). CLAUDE.md mandates `idgen.BuildDMRoomID` for DMs precisely so no separate dedup is needed. The native paths (`handler.go:2002`, `:2101`) compute a different ID for the same pair → a second, duplicate DM room after migration.

**`medium` — consumer has no `FilterSubjects`; it eats another service's events.** `buildConsumerConfig` (`main.go:433-440`) and `CreateOrUpdateConsumer` (`main.go:287`) bind all of `chat.room.canonical.{site}.>` (`pkg/stream/stream.go:39-43`). room-service publishes `chat.room.canonical.{site}.event.member.muted` on that stream (`room-service/handler.go:2208`); notification-worker filters for it, but room-worker also receives every one, falls to `default:` (`handler.go:274-276`), WARN-logs and Acks — log noise and ack budget proportional to mute traffic, and it masks genuinely unknown ops.

**`medium` — subject dispatch by `strings.HasSuffix` instead of the canonical parser.** `handler.go:255-273` matches suffixes in a load-bearing order (the comment at `:260-262` concedes `.teams.create` must precede `.create`). `subject.RoomCanonicalOperation` (`pkg/subject/subject.go:1237`) exists for exactly this.

**`medium` — `docs/client-api.md` drift on rename federation.** `docs/client-api.md:1774` still says cross-site rename is "Published directly to `chat.inbox.{remoteSiteID}.external.room_renamed`", but `handler.go:2362-2367` relays via `outbox.Publish`. Every peer event in the doc describes the OUTBOX path (`:1662`, `:1791`).

**`medium` — Teams path swallows federation failures.** `reconcileTeamsRoom` returns errors, but `processTeamsRoomCreate` WARN-and-continues (`teamsroomcreate.go:34-38`) and returns nil → `jsretry.SettleQuiet` Acks. Mongo writes are already committed, so a transient publish failure at `:303`/`:321`/`:366`/`:385` loses the federation permanently. Live paths correctly propagate and Nak.

**`low` — retry backoff outlives the dedup window.** `jsretry.DefaultBackoff` tails at 10m (`pkg/jsretry/jsretry.go:52-58`); the JetStream default duplicate window is 2m and `bootstrap.go:48-51` sets only Name+Subjects, so retries past attempt 3 re-enqueue duplicate federated events into the per-destination FIFO lanes.

**`low` — room-key fan-out loses correlation context.** `main.go:185` wires the sender to the raw `nc.NatsConn()`; `pkg/roomkeysender/roomkeysender.go:69-77` calls `pub.Publish(subj, data)` with no headers, so `room.key` events carry no X-Request-ID/trace, unlike every other publish (`main.go:235`, `:263`).

**`low` — HR identity fanout is undeduped and unverified.** `main.go:257-270` JS-publishes to `chat.hr.{SITE_ID}.users.upsert` with no Msg-Id (a redelivery republishes), on a stream `bootstrap.go:42-59` neither creates nor verifies; a miss makes `resolveMember` fail and the member is silently skipped (`teamsroomcreate.go:123-128`).

**`nitpick` — timestamp discipline is correct.** `RoomKeyEvent` at `handler.go:2481`/`:2506` omits `Timestamp`, but `Sender.Marshal` stamps it (`pkg/roomkeysender/roomkeysender.go:51-53`). `RoomRenamedInboxPayload.Timestamp` (`handler.go:2324`) and `InboxMemberEvent.Timestamp` (`teamsroomcreate.go:282`) intentionally carry acceptance time as a high-water mark, not publish time — worth an inline note.

## Recommendations

1. **`critical`** — Make Teams dedup IDs event-unique: derive them from the seed (room + event type + chat) rather than the ctx request ID — e.g. hash `requestID+":"+seed`, or add an `InboxDedupIDFor(ctx, dest, seed)` variant that always mixes the seed. Add a test that sets a request ID and asserts distinct Msg-Ids across the added/removed/joinedAt publishes of one batch.
2. **`high`** — Use `idgen.BuildDMRoomID(userA.ID, userB.ID)` for migrated 1:1 chats in `teamsroomcreate.go:61`, keeping `DeterministicID` for group/channel rooms; coordinate with teams-room-inspector's mirrored derivation.
3. **`medium`** — Set `FilterSubjects` on the durable to the ops room-worker actually handles, and dispatch via `subject.RoomCanonicalOperation` instead of suffix matching.
4. **`medium`** — Update `docs/client-api.md:1774` to the OUTBOX → `outbox-worker` → destination INBOX wording used elsewhere; re-check `docs/client-api/events.md` for the same claim.
5. **`medium`** — In `processTeamsRoomCreate`, distinguish publish/federation failures (return them so JetStream retries) from per-chat data failures (WARN + continue), and add Teams integration coverage.
6. **`low`** — Give the room-key sender a context-aware publish (headers from `natsutil.HeaderForContext`) so key fan-out is trace-correlated.
7. **`low`** — Attach a `Nats-Msg-Id` to the HR identity fanout and fail fast at startup if `HR-{siteID}` is absent when `MODE=teams`.

---

# 6. Performance — 3 / 5

Well-optimized on the paths that were explicitly tuned (concurrent input loads, singleflight caches, single-marshal pooled key fan-out, `$inc`-delta member counts), with real cliffs on the large-room and large-org paths that were not.

## Findings

**`critical` — no `msg.InProgress()` anywhere; `AckWait` = 30s is a hard wall-clock ceiling on every handler.** `pkg/stream/consumer.go:17` sets `AckWait: 30s`; `grep -rn "InProgress()"` across the repo returns nothing. `processRemoveIndividual` (`handler.go:373`) does: `keyStore.Get` → aggregation → 2 deletes → 2 thread cleanups → 2 full `CountDocuments` → `GetSubscriptionAccounts` → rotate → fan-out to every survivor → 6 publishes. `processTeamsRoomCreate` (`teamsroomcreate.go:32-38`) loops an *entire batch* of chats in one message, each with ~8 round trips. Both blow past 30s well before they finish, and JetStream redelivers into a second concurrent worker running the same rotation and fan-out. **Bites at:** ~5k-member room remove, or a Teams batch >~20 chats.

**`high` — rename reads every subscription document in the room, unprojected.** `store_mongo.go:89-99` (`ListByRoom`) issues `Find(ctx, bson.M{"roomId": roomID})` with no projection, violating CLAUDE.md's "always project precisely". `handler.go:2310-2318` decodes all of it just to build `accounts []string`, then `findRemoteSitesForAccounts` (`handler.go:2224`) re-queries users with an `$in` of the same N accounts. `GetSubscriptionAccounts` (`store_mongo.go:710`) already does exactly this, projected. **Bites at:** 5k-member channel rename → ~5k full subscription docs (roles, name, timestamps, read state) over the wire, plus a 5k-element `$in`, for two string fields. The doc comment at `store_mongo.go:85-88` claims it is "retained for integration test verification" — it is in the production interface (`store.go:162`) and on two hot paths.

**`high` — remove paths run an unconditional O(room) double count; the add path does not.** `ReconcileMemberCounts` (`store_mongo.go:110-134`) issues two `CountDocuments` over the whole room, called unconditionally at `handler.go:414` (remove-individual), `:624` (remove-org), `:1755` (finishCreateRoom) and `teamsroomcreate.go:189`. The add path was already fixed to `ApplyMemberCountDelta` + TTL (`handler.go:1125-1133`, `store_mongo.go:144`). **Bites at:** removing one member from a 50k-member room = two full index scans of 50k keys, every time.

**`high` — org-remove aggregation is unbounded and runs two correlated `$lookup`s per member.** `GetOrgMembersWithIndividualStatus` (`store_mongo.go:382-454`): `$match` on `sectId|deptId` with no `$limit` and no early `$project` (projection is the *last* stage, `:433`), then a per-document `room_members` lookup at `:395` and a second at `:415`. `$lookup` is forbidden by CLAUDE.md absent documented justification; no `// $lookup justification:` comment is present at either site. **Bites at:** removing a 5,000-person department → 5k full user docs through the pipeline + 10k correlated index lookups, all inside one 30s AckWait.

**`high` — unchunked bulk writes and unbounded candidate sets on add.** `ListAddMemberCandidates` (`store_mongo.go:619-683`) has no cap; `BulkCreateSubscriptions` (`:515-529`) and `BulkCreateRoomMembers` (`:556-573`) build one `WriteModel` per candidate with no batching (no chunk helper exists in `pkg/mongoutil/bulk.go`). **Bites at:** adding a 20k-user org → 20k-model BulkWrite, a 20k-element `$in` against `subscriptions`, another against `room_members`.

**`medium` — the same users are fetched twice on every add.** `ListAddMemberCandidates` projects `account/_id/siteId` (`store_mongo.go:640`), then `handler.go:941` calls `FindUsersByAccounts` for the *same* accounts to get names. One pass with `userstore.userProjection` would serve both.

**`medium` — no per-operation deadline on Mongo in the JetStream path.** `mongoutil` never calls `SetTimeout` (`pkg/mongoutil/mongo.go:108-117`, `tuning.go:33-41`), and the worker goroutine passes `msgCtx` with no timeout (`main.go:314-326`). A stalled Mongo pins a `MAX_WORKERS` semaphore slot indefinitely while the message redelivers anyway. Contrast the sync RPC path, correctly bounded by `natsrouter.GuardConfig` (`main.go:275`).

**`medium` — `DeleteThreadSubscriptions` has no index prefixing its filter key.** `store_mongo.go:496-507` filters `{roomId, userAccount: $in}`; no index on `thread_subscriptions` prefixes `roomId` (room-service `store_mongo.go:152-167`, history-service, user-service all key on `threadRoomId`/`userAccount`/`parentMessageId`). Falls back to N index seeks on `userAccount_1` plus a `roomId` filter. **Bites at:** org removal of 5k accounts.

**`low`** — `CreateRoom` does a `bson.Marshal`→`Unmarshal`→`bson.M` round trip per room (`store_mongo.go:231-244`); per-chat in the Teams batch loop.

**`low`** — `GetRoom` (`store_mongo.go:173-179`) is unprojected and pulls `encKey` on every add-path re-read (`handler.go:1148`).

**`nitpick`** — `fanOutKey` (`handler.go:2558-2576`) spawns one goroutine per account rather than N long-lived workers draining a channel; 10k goroutine create/destroy cycles for 10k core-NATS publishes.

**Positives worth preserving:** the `loadAddMemberInputs` errgroup (`handler.go:770`), singleflight in both caches (`pkg/roommetacache/roommetacache.go:110`, `pkg/userstore/cache.go:79`), single-marshal fan-out (`handler.go:2536`), bounded `WaitGroup` shutdown (`main.go:300-328, 352-361`), and no mutex held across I/O anywhere.

## Recommendations

1. **`critical`** — Add an `InProgress()` heartbeat ticker around long handlers (or split remove/rename fan-out onto a follow-up stream message). Without it, every large-room operation is executed at least twice.
2. **`high`** — Replace `ListByRoom` with `GetSubscriptionAccounts` at `handler.go:2310`; delete or hard-restrict `ListByRoom` to `_test` use as its comment claims.
3. **`high`** — Extend `ApplyMemberCountDelta` to the three remove/create call sites (`handler.go:414, 624, 1755`) so `ReconcileMemberCounts` runs only on the TTL drift path.
4. **`high`** — Add an early `$project` (account, siteId, sectId, deptId, names) before the two `$lookup`s in `GetOrgMembersWithIndividualStatus`, and add the required `// $lookup justification:` comments at `store_mongo.go:326, 342, 395, 415`.
5. **`medium`** — Chunk `BulkCreateSubscriptions`/`BulkCreateRoomMembers` at ~1,000 models and page `ListAddMemberCandidates`; add a `mongoutil.ChunkedBulkWrite` helper.
6. **`medium`** — Return the full `userstore.userProjection` from `ListAddMemberCandidates` and drop the second `FindUsersByAccounts` at `handler.go:941`.
7. **`medium`** — Add `options.Client().SetTimeout(...)` in `mongoutil.buildClientOptions`, and add a `thread_subscriptions (roomId, userAccount)` index in the owning service.

---

# 7. Prioritized action list

Ordered by severity first, then impact ÷ effort. Items 1–3 are correctness defects that lose or duplicate user-visible data in production today; they should land before the next large-room or Teams-migration rollout.

| # | Severity | Action | Dimension | Evidence | Why |
|---|---|---|---|---|---|
| 1 | `critical` | Add an `msg.InProgress()` heartbeat around the long handlers (remove-individual, remove-org, Teams batch), or split their fan-out onto a follow-up stream message | Performance | `pkg/stream/consumer.go:17`; `handler.go:373`; `teamsroomcreate.go:32-38` | 30s `AckWait` with no heartbeat means any ~5k-member removal or >20-chat Teams batch is redelivered mid-flight into a **second concurrent worker** running the same key rotation and fan-out. Highest-impact/lowest-effort item on the list — a ticker, not a redesign. |
| 2 | `critical` | Make Teams dedup IDs event-unique — mix the payload seed into the Msg-Id instead of using the ctx request ID alone | Integration | `pkg/natsutil/request_id.go:144-151`; `teamsroomcreate.go:303, 320-321, 366, 384-385`; `main.go:419-424` | One request ID covers N chats × 3 events, so JetStream's stream-wide dedup drops every publish after the first: removals and joinedAt fixes are **silently lost**. No test catches it (tests never set a request ID). |
| 3 | `high` | Use `idgen.BuildDMRoomID` for migrated 1:1 Teams chats | Integration | `teamsroomcreate.go:61, 216-221`; native paths `handler.go:2002, 2101` | `DeterministicID(chat.ID)` produces a different ID than the native DM path for the same pair, so migration creates a **duplicate DM room** alongside the real one. Cheap fix, permanent data mess if shipped. |
| 4 | `high` | Cover `HandleJetStreamMsg` with a table-driven dispatch test (one row per subject suffix + undecodable payload), reusing the existing `fakeJSMsg` | Test coverage | `handler.go:239-279` at 0.0%; stub at `handler_test.go:6598-6617` | The service's only JetStream entry point — the routing switch and the Ack-vs-Nak decision — is entirely untested. A wrong branch means a silent drop or an infinite poison-pill loop. ~40 lines of test. |
| 5 | `high` | Stop reading whole subscription documents on rename: use `GetSubscriptionAccounts` at `handler.go:2310` and restrict `ListByRoom` to tests | Performance / Code quality | `store_mongo.go:85-99`; `handler.go:2310-2318` | A 5k-member rename pulls ~5k full documents to extract two string fields. The projected replacement already exists (`store_mongo.go:710`) — this is a one-line call-site swap. |
| 6 | `high` | Remove `err.Error()` from the client-facing rename error | Code quality | `handler.go:2258` | `json.SyntaxError` embeds the offending payload substring, so raw client input from an unauthenticated entry point is echoed back. The same file documents this exact hazard 770 lines earlier (`:1487-1489`). One-line fix. |
| 7 | `high` | Extend `ApplyMemberCountDelta` to the remove/create call sites so `ReconcileMemberCounts` only runs on TTL drift | Performance | `handler.go:414, 624, 1755`; `store_mongo.go:110-134, 144` | Removing one member from a 50k-member room currently costs two full index scans. The delta path already exists and is used by the add flow — this is applying a solved pattern to three more sites. |
| 8 | `high` | Pass a real `HR_CENTRAL_SITE_ID` to `subject.OrgSyncUsersUpsert` instead of the local site ID | Architecture | `main.go:262`; `pkg/subject/subject.go:1774`; cf. `teams-hr-sync/config.go:34` | On any non-central site the publish finds no stream, `resolveMember` fails, and the member is WARN-skipped — **silent member loss** on Teams migration. Same defect in `message-worker` and `admin-service`; fix all three together. |
| 9 | `high` | Gate coverage in CI: run the integration tag, merge profiles, and enforce per-file minimums via the existing `tools/coveragecheck` | Test coverage | 62.9% vs 80% floor; `room-worker/deploy/azure-pipelines.yml:44`; `Makefile:141-143` | The floor is currently documentation-only — the pipeline writes a profile nothing reads, and `store_mongo.go`'s 291 statements are structurally unreachable in the untagged run. Without a gate, coverage keeps drifting down. |
| 10 | `critical` (structural) | Split `handler.go` by flow and decompose `processAddMembers` into ~6 named steps | Maintainability | `handler.go` 2,582 lines; `processAddMembers` `:824-1302` (479 lines, 83 branches) | Mechanical, zero-behaviour-change, and the prerequisite for the rest: the 7× duplicated INBOX publish and 5× duplicated federation loop are how items 2 and 3 came to diverge copy-by-copy. Ranked last only because nothing breaks in production today if it waits. |

## Note on SAST

`gosec` passed clean repo-wide. `semgrep`'s registry ruleset and `govulncheck` could not reach their upstreams from this environment (proxy `403`), so **dependency CVE status is unverified** — re-run `make sast` in CI, where the gate is authoritative, before signing off on release.
