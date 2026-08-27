# Branch Review — `claude/new-session-439bi9`

| | |
|---|---|
| **Branch** | `claude/new-session-439bi9` |
| **Base** | `origin/main` (local `main` was 9 commits stale — reviewed against the real base) |
| **Date** | 2026-08-27 |
| **PR** | [#395](https://github.com/hmchangw/newchat/pull/395) |
| **Diff** | 2 files, +212 / −4 |
| **Services touched** | `room-service` (1) |
| **`pkg/` changed** | no |
| **Method** | 1 per-service generalist + 5 global lenses (Go, test-automation, bug & security, performance, observability) |

## Executive summary

The branch adds MongoDB projections to five previously-unprojected `room-service` store reads — `GetUser`, `GetApp`, `FindDMSubscription`, `GetThreadSubscriptionByParent` and `getRoomSubscriptions` — plus five `*_ProjectionFields_Integration` tests.

**Top-line risk: low, and the dominant hazard was checked directly.** A projection that omits a field a caller reads yields a zero value rather than an error, so the review's central question was whether each projected field set is a superset of what every caller actually reads. Three lenses traced that independently — including transitively, through `handleCreateSelfDM` / `handleCreateRoomDMOrBotDM` / `handleCreateRoomChannel` / `publishCreateRoom` and through `model.IsPlatformAdmin` — and all five projections came back clean. Critically, **no projected struct is ever written back to Mongo or marshaled into a NATS event**, so there is no data-destruction path; `publishCreateRoom` copies two scalars out before marshaling. Every projected bson key was verified against its struct tag, including the `u._id` nested path where `u.id` would have silently zeroed `Member.ID` in every member list.

The authorization path is fail-closed: `roles` is genuinely projected, and had it been dropped, `model.IsPlatformAdmin` returns `false` on nil/empty — denying, never granting. The claimed security win holds: `users.services`, the bcrypt credential block, no longer leaves Mongo on the `GetUser` path.

**What's actually worth acting on is verification, not correctness.** All five new tests are `//go:build integration`, and `room-service/deploy/azure-pipelines.yml:44` runs `go test` with no `-tags integration` — so the only guard against projection drift is a test CI never executes, and Docker was unavailable when the branch was written, so they have never been run at all. One of the five is additionally weak: `TestMongoStore_ListRoomMembers_SubscriptionProjection_Integration` asserts only included fields and would still pass with the projection deleted. The other four do pin exclusions and are load-bearing.

Beyond that: a comment in the new test contradicts the call it documents (four lenses caught it), the `store.go` interface docs still describe these methods as returning whole entities, and `active` is unprojected — harmless today, but a future `IsActive()` check would silently pass a deactivated account.

## Findings by severity

| Severity | Count |
|----------|:-----:|
| `critical` | 0 |
| `high` | 2 |
| `medium` | 8 |
| `low` | 5 |
| `nitpick` | 7 |
| **Total** | **22** |

Counts are deduplicated across lenses — four separate reviewers flagged the same stale test comment, three the redundant `_id: 1`, and two each the CI gap and the unprojected `active`.

## SAST status

`gosec` **PASS** (0 findings, repo-wide). Repo-local semgrep rules **PASS** (9 rules, 0 findings). `govulncheck` and the semgrep registry rulesets (`p/golang`, `p/security-audit`) **did not run** — the sandbox proxy returns 403 for `vuln.go.dev` and `semgrep.dev`. `make sast` exits 2 on those two network failures, not on any code finding. **Neither can be reported as passing**; re-run on a runner with egress before treating the gate as green.

---

# Service: room-service

## Projection correctness — all five verified superset, no `critical`

The core check. Each projected field set was compared against every field its callers actually read, traced transitively.

1. **`getRoomSubscriptions`** (`room-service/store_mongo.go:715`, applied `:733`) — the loop reads exactly `sub.User.ID` (`:749`), `sub.User.Account` (`:751`), `sub.Roles` (`:754`), `sub.ID` (`:757`), `sub.JoinedAt` (`:759`). `RoomID` comes from the argument (`:758`), not the document. `SubscriptionUser.ID` is `bson:"_id"` (`pkg/model/subscription.go:21`), so `u._id` is correct — `u.id` would have zeroed `Member.ID`. `u.isBot` is unread: `attachUserDisplayNames` partitions on `botAccountPattern` (`store_mongo.go:774`). Callers `handler.go:449`, `handler.go:1123`, `handler_teams.go:62`, `handler_teams.go:128` read only `Member.{Type,ID,Account}` / `IsOwner`. **Superset ✓**
2. **`GetUser`** (`:890`, applied `:898`) — all call sites read only `EngName`/`ChineseName` (`handler.go:164`, `:268`), `ID` (`:253`, `:275`, `:368`), `Account` (`:242`, `:254`, `:316`, `:369`), `Roles` via `model.IsPlatformAdmin` (`handler.go:1987`, `:2045` → `pkg/model/user.go:101`), and `EngName`/`ChineseName`/`Account` (`handler_teams.go:246`). `IsActive()`, `SiteID` and `Settings` are read nowhere in room-service — `GetUserSiteID` is a separate, untouched method (`store_mongo.go:1337`). **Superset ✓**
3. **`GetApp`** (`:912`) — both sites read only `app.Assistant == nil || !app.Assistant.Enabled` (`handler.go:301-303`, `:919-926`). `AppSubscriptionFromApp`, the one function needing the whole document, is called only from `user-service/service/subscriptions.go:143` — a different store. **Superset ✓**
4. **`FindDMSubscription`** (`:931`) — both sites use only `existing.RoomID` (`handler.go:244`, `:296`). **Superset ✓**
5. **`GetThreadSubscriptionByParent`** (`:1533`) — sole site reads only `tsub.ThreadRoomID` (`handler.go:1690`, `:1713`, `:1722`, `:1729`) plus the `ErrThreadSubscriptionNotFound` sentinel (`store_mongo.go:1547`). **Superset ✓**

**Write-back / re-marshal hazard: none.** No projected struct reaches `UpdateOne`/`ReplaceOne`/`InsertOne` or `json.Marshal`. `publishCreateRoom` (`handler.go:368-369`) copies two scalars into `CreateRoomRequest` before marshaling — the `*model.User` never reaches the wire. `listMembers` returns freshly-constructed `RoomMember` values, not decoded documents.

## (a) Diff correctness

Clean. Every bson key matches its tag (`assistant`, `roomId`, `threadRoomId`, `engName`, `chineseName`, `roles`, `joinedAt`). `make build SERVICE=room-service` and `go vet -tags integration ./room-service/` pass; `golangci-lint --build-tags integration` reports 0 issues. `opts.SetProjection` after the conditional `SetLimit` (`:733`) is order-independent.

## (b) Scope drift / refactor-readiness

None. Two files, one behavioral commit, no unrelated refactor. `docs/reviews/` was removed in `fae6117` per CLAUDE.md Section 5. The store interface is unchanged, so no `make generate` is required.

## (c) Abstraction changes

The five new vars mirror `roomReadProjection` / `subscriptionReadProjection` (`store_mongo.go:208`, `:228`) exactly: package-level `bson.D`, doc comment naming the call sites, paired `*_ProjectionFields_Integration` guard. Justified — no premature abstraction.

- **[nitpick]** Explicit `_id: 1` is redundant (Mongo includes `_id` by default), though it matches `membershipExistsProjection` (`:269`).

## (d) Design coherence

- **[medium] Interface docs still describe whole entities** — `room-service/store.go:197-220` documents `GetUser`, `GetApp`, `FindDMSubscription` and `GetThreadSubscriptionByParent` as returning the entity, with no note that the struct is now partial. `GetRoomAppRead` (`store.go:63`) and `ListDefaultChannelTabApps` (`store.go:202-204`) set the precedent of stating the projection in the interface doc. A future caller reading `requester.SiteID` gets `""` silently. Add one projection line to each of the four.

Otherwise the diff fits the service's job precisely and bolts on no unrelated concern.

## (e) Project-pattern adherence

N/A or clean across the board: no subjects, streams, IDs, consumers, outbox usage or `pkg/model` event structs are touched. Mongo driver v2 with `options.FindOne().SetProjection` per existing style. The change directly satisfies CLAUDE.md Section 6's "always project precisely" rule.

## (f) Client-API doc rule — does not fire, correctly

The diff is store-layer only: no `natsrouter.Register` / `QueueSubscribe` registration, no handler body, and no `pkg/model` struct changed. The one client-facing surface reached (`listMembers`, `handler.go:423`) returns `RoomMember` values assembled field-by-field from the projected set, so the wire payload is byte-identical. **No `docs/client-api.md` update is required.**

- **[nitpick]** `integration_test.go:352` comment says "the fallback (enrich=false) build" but the call at `:377` passes `enrich=true`, needed for the `IsOwner` assertion at `:385`.

---

# Go expert

**Verdict: sound change, correct projections, two comment defects and a latent fail-open trap. No blocking Go-idiom violations.**

Correctness independently confirmed by call-site audit — every projected field set is a superset of what callers read. `go vet -tags integration ./room-service/` and `gofmt -l` are both clean. No error-wrapping, `errcode`, log-and-return, struct-tag or concurrency changes appear in the diff, so CLAUDE.md Section 3 is untouched and uncontravened.

## Findings

- **[medium] Dropping `active` makes a future `IsActive()` check fail *open*, and nothing warns** — `room-service/store_mongo.go:883-894` — `model.User.Active` is `*bool` with "nil (field absent) means active" (`pkg/model/user.go:88-94`). `userReadProjection` omits `active`, so a decoded `User` always has `Active == nil`. No room-service call site calls `IsActive()` today (verified by grep), so this is not a live bug — but the failure mode of adding one later is silent: a deactivated user reads as active, with no test or compile error. The doc comment singles out `services` as deliberately excluded; it should do the same for `active`, with a note that any call site adding an active-check must add the field back. This is the same class of trap as `u._id`, which the author *did* call out at `store_mongo.go:713-714`.
- **[medium] A comment states something false about the code** — `room-service/integration_test.go:334` — says "the fallback (**enrich=false**) build of RoomMember", but line 352 passes `enrich=true`, and line 359 asserts `Member.IsOwner`, which `store_mongo.go:751-753` only sets when `enrich` is true. Drop the parenthetical or correct it.
- **[low] The test doesn't pin the path it claims to guard** — `room-service/integration_test.go:337-360` — it reaches `getRoomSubscriptions` only because no `room_members` document exists for `"rmembers"` (probe at `store_mongo.go:511-517`). If that probe or the fixture changes, the test silently runs the aggregation path — which also yields `IsOwner` and `Ts` — and keeps passing while guarding nothing. Either assert the fallback was taken, or call `store.getRoomSubscriptions` directly (same package, so it is reachable).
- **[low] `{Key: "_id", Value: 1}` is a no-op** — `room-service/store_mongo.go:912`, `:931`, `:1534` — Mongo returns `_id` unless explicitly excluded. For `appAssistantReadProjection`, `dmDedupProjection` and `threadSubParentProjection` no caller reads the id, so the file's own narrow-read idiom applies: `{Key: "_id", Value: 0}` (see `store_mongo.go:477`, `:835`, `:859`, `:1337`, `:1961`, `:2001`). Keeping `_id:1` is neither the minimum payload nor the file's convention. `userReadProjection` and `roomMemberSubProjection` legitimately need `_id`.
- **[nitpick] `SetProjection` placement reads as conditional** — `room-service/store_mongo.go:733` — `opts.SetProjection(...)` is unconditional, so it belongs chained onto the `options.Find().SetSort(...)` constructor at line 719 alongside the other unconditional option; trailing it after the conditional `SetSkip`/`SetLimit` block reads as if it were conditional too.
- **[nitpick] Comment form** — four of the five new vars use the `name: prose` form rather than the `name is …` godoc form used by `roomReadProjection` and `subscriptionReadProjection`. Precedent for both exists in-file (`roomAppReadProjection:` at `:217`), so this is cosmetic.

## What's right

Var placement (package-level, immediately above the consuming method), naming, `bson.D` choice, `require`/`assert` split, and `Test<Type>_<Method>_<Scenario>` naming all match the surrounding code. Non-table-driven tests are the correct call here — each case has a distinct fixture and a distinct assertion set, not input/output variations of one function.

---

# Test automation

## Test run

```
$ make test SERVICE=room-service          # after go clean -testcache
go test -race ./room-service/...
ok  	github.com/hmchangw/chat/room-service	1.625s

$ go test -run 'ProjectionFields' -v ./room-service/... | grep -c "^=== RUN"
0

$ make test-integration SERVICE=room-service
--- FAIL: TestMongoStore_ListMentionableSubscriptions_Integration/... (0.00s)
    integration_test.go:40: testutil.MongoDB: start mongo: run mongodb: generic container:
      get provider: rootless Docker not found, failed to create Docker provider
[…every integration test fails identically…]
FAIL	github.com/hmchangw/chat/room-service	0.576s
make: *** [Makefile:122: test-integration] Error 1

$ docker info   ->  docker UNAVAILABLE
$ go vet -tags=integration ./room-service/...   # exit 0 — new tests compile

$ make generate SERVICE=room-service; git status --porcelain room-service/   ->  empty
```

**Mock freshness: clean.** `room-service/store.go` is untouched, mocks are in sync, tree clean — nothing to revert.

**Integration tests have never been executed.** Docker is unavailable in this environment, so the five new tests were confirmed to compile but never run.

## Findings

- **[medium] The projection change has zero coverage in the pre-commit gate** — `room-service/integration_test.go:1` — all five new tests sit under `//go:build integration`; `make test` ran 0 of them (grep count above). Part of this *is* unit-testable without Mongo: the failure mode the author calls out at `room-service/store_mongo.go:713-714` ("`u.id` would project nothing") is pure bson-tag arithmetic. A plain `store_test.go` with no build tag, reflecting each dotted key in the five projection vars against the target struct's `bson` tags and failing on any key that resolves to no field, would run on every commit and catch exactly that typo class — which Mongo-only tests catch ten minutes later, or never.
- **[medium] One of the five tests cannot fail if the projection is deleted** — `room-service/integration_test.go:341-364` — `TestMongoStore_ListRoomMembers_SubscriptionProjection_Integration` asserts only included fields (`ID`, `RoomID`, `Ts`, `Member.ID/Account/IsOwner`). Remove `opts.SetProjection(roomMemberSubProjection)` at `room-service/store_mongo.go:733` and it still passes. The other four each pin exclusions (`GetUser` `:225-230`, `GetApp` `:262-267`, `FindDMSubscription` `:297-300`, `GetThreadSubscriptionByParent` `:330-333`) and genuinely fail on projection removal. This one is a drift guard, not a projection guard — say so in the comment, or assert through a raw `Find` with the same projection.
- **[low] Comment contradicts the call** — `room-service/integration_test.go:336` — says "the fallback (enrich=false) build" but line 353 passes `true`. `enrich=true` is required for the `IsOwner` assertion (`store_mongo.go:753`), so the code is right and the comment is wrong.
- **[low] Exclusion assertions could pass for the wrong reason** — `room-service/integration_test.go:225` — `assert.Empty(t, got.Services.Password.Bcrypt)` proves nothing if the fixture never persisted it; `Services` carries `bson:"services,omitempty"` (`pkg/model/user.go:70`). For the one security-relevant assertion, add a raw `db.Collection("users").FindOne(...)` read-back confirming `services.password.bcrypt` really is in the stored document. Same caveat, lower stakes, for the other `assert.Empty` blocks.
- **[nitpick] Inline insert instead of the helper** — `room-service/integration_test.go:247` — `GetApp` test inserts via `db.Collection("apps").InsertOne` while siblings use `mustInsertX`; five call sites now do this. Extract `mustInsertApp`.
- **[nitpick] No error paths, none table-driven** — none of the five cover the not-found path. Consistent with the pre-existing `GetRoom`/`GetSubscription` projection tests, so no deviation, but CLAUDE.md Section 4's "tests must cover … error paths" is satisfied only by other tests in the file.

## What's right

TDD's 1:1 mapping holds — five changed reads, five tests, each naming the call site it protects. `TestMain` present (`room-service/main_test.go:11`), `testutil.MongoDB` throughout with zero inline `testcontainers.GenericContainer`, per-test DB hashed on `t.Name()` so there is no shared state or order reliance, `-race` on the unit gate. The four exclusion-pinning tests are genuinely load-bearing.

---

# Bug & security

## SAST result

| Scanner | Ran? | Result |
|---|---|---|
| `gosec` (`./...`, severity/confidence medium) | **yes** | **PASS** — 0 findings |
| `semgrep --config=.semgrep/ room-service/` (repo-local rules) | **yes** | **PASS** — 9 rules, 9 files, 0 findings |
| `govulncheck` | **no** | Proxy returned `403 Forbidden` for `https://vuln.go.dev/index/modules.json.gz` |
| `semgrep` registry rulesets (`p/golang`, `p/security-audit`) | **no** | `ProxyError … semgrep.dev:443 … 403 Forbidden` while downloading rulesets |

`make sast` summary: `gosec=PASS govulncheck=FAIL semgrep=FAIL`, exit 2 — both failures are proxy/network, **not** code findings. No medium+ finding is attributable to this branch from the scanners that actually ran. **`govulncheck` and the registry rules cannot be reported as passing.**

## Findings

No `critical`, no `high`. Every claim below was traced to source.

**Projection completeness — verified clean on all five.** `userReadProjection` (`room-service/store_mongo.go:890`) = `_id, account, engName, chineseName, roles`, and all `GetUser` call sites read exactly and only those: `handler.go:164`, `:268` (`EngName`/`ChineseName`), `:275`, `:368-369` (`ID`/`Account`), `:1987`, `:2043` (`model.IsPlatformAdmin` → `Roles`), `handler_teams.go:245-246`. `SiteID`, `Active`, `Settings`, `Permissions` and `Chatlist` are read at **zero** room-service call sites.

**Authorization: fail-closed, confirmed.** `roles` is genuinely projected and the bson tag matches (`pkg/model/user.go:63`, `bson:"roles,omitempty"`). `model.IsPlatformAdmin` (`pkg/model/user.go:96-105`) returns `false` on nil/empty — dropping `roles` would deny, never grant. Both gates then fall through to a stricter check (`roomRename` → owner-role check `handler.go:1988-1998`; `roomRestricted` → `errOnlyAdmins` `handler.go:2044`). No inverted logic anywhere, and no other authz decision in room-service reads a now-unprojected field.

**Write-back hazard: none.** All five results are read-field-only; no projected struct reaches `InsertOne`/`ReplaceOne`/`UpdateOne` or a NATS payload.

**BSON paths: all correct.** `u._id`/`u.account` match `SubscriptionUser` (`pkg/model/subscription.go:21-22`) — the nested-`_id` trap is handled and called out at `store_mongo.go:713-714`. `roomId`, `joinedAt`, `roles` (`subscription.go:29`, `:31`, `:36`), `threadRoomId` (`threadsubscription.go:14`), `assistant` (`app.go:13`), `engName`/`chineseName` (`user.go:59-60`) all match. No projection path collisions.

**Credential exclusion: confirmed.** `users.services` (`pkg/model/user.go:37`, `:71`; bcrypt at `:25`) is absent from `userReadProjection:890-894` — `GetUser` no longer pulls hash material. No other changed read touches credentials; `GetApp` returns only `assistant`, which has no secret fields (`app.go:19-24`). Strict improvement, pinned at `integration_test.go:231`.

- **[low] `roomMemberSubProjection` omits `u.isBot`** — `room-service/store_mongo.go:715-718` — safe today: `RoomMemberEntry` (`pkg/model/member.go:60-75`) has no `IsBot` field, and bot-ness downstream is derived from the account suffix (`attachUserDisplayNames` `store_mongo.go:786`; `membersToCallEmails` `handler_teams.go:281`). Flagged only because it is the one sub-document field a future member-list change would plausibly reach for and silently get `false`, and the new test does not pin it as excluded.
- **[nitpick] Redundant `{Key: "_id", Value: 1}`** — `room-service/store_mongo.go:890`, `:912`, `:931`, `:1533` — Mongo includes `_id` by default. Harmless and consistent with the file's existing style.
- **[nitpick] Package-level `bson.D` shared across concurrent requests** — the driver only marshals them, so there is no race, but a stray mutation would be process-wide.

**Other lens items:** no swallowed errors introduced — all new `opts` paths keep the existing `errors.Is(mongo.ErrNoDocuments)` sentinels. No injection surface: projections are static literals with no user input. No logging change, no new config.

---

# Performance

**Verdict: directionally correct, correctly implemented, regression-free — but the two biggest wins in the code it touches are left on the table.**

## Do the projections deliver? (ranked)

- **Real win — `getRoomSubscriptions`** (`room-service/store_mongo.go:733`) — `Subscription` has ~30 persisted BSON fields (`pkg/model/subscription.go:26-86`, including 5 `*time.Time`, `threadUnread []string` and 8 booleans); the projection keeps 5. Per-member decode drops ~25 reflect-driven field decodes. For a 1000-member room that is ~25k field decodes and (est. 500–700 B → ~150 B/doc) roughly **0.5 MB of wire traffic avoided per `room.members` call**.
- **Win — `GetUser`** (`store_mongo.go:896`) — drops 19 of 24 fields, including `settings`, `permissions`, `chatlist` (unbounded sub-documents; `chatlist` grows with section count) and `services` (bcrypt). A security win as much as a performance one.
- **Moderate — `GetApp`** (`store_mongo.go:916`) — drops the `appViewUrl` map, `sponsors` and `channelTab`; called once **per bot** at `handler.go:919`, so N× per `addMembers`.
- **Noise — `FindDMSubscription`** (`store_mongo.go:935`) and **`GetThreadSubscriptionByParent`** (`store_mongo.go:1537`) — single-document reads on non-hot paths. Justified only as consistency plus the drift guard.

## Findings

- **[high] The covered-query win is one index field away and dwarfs what was banked** — `room-service/store_mongo.go:142-143` — the `subscriptions (roomId, joinedAt, _id)` index backs the sort, but the new projection also needs `u._id`, `u.account` and `roles`, so every row still FETCHes the document. Note `roles` is `[]Role` → **multikey, and MongoDB cannot cover a projection of a multikey field**, so covering is impossible while `roles` is projected. But `roles` is read **only when `enrich==true`** (`store_mongo.go:753-755`), and callers pass `enrich=false` at `handler_teams.go:62`, `handler_teams.go:128` and `handler.go:1123`. Split into enrich/non-enrich projections and extend the index to `(roomId, joinedAt, _id, u.account, u._id)`: the non-enrich path becomes an index-only scan, eliminating ~N document fetches (1000 for a 1000-member room).
- **[medium] `getRoomSubscriptions` is still unbounded when `limit == nil`** — `room-service/store_mongo.go:720-733` — `handler_teams.go:62` and `:128` pass nil, so a large room streams every subscription into `[]model.Subscription` and then `[]model.RoomMember` (two full slices). The projection shrinks each row, not the count.
- **[medium] `ListRoomMembers` does 2 serial Mongo round trips on every member list** — `room-service/store_mongo.go:509-521` — an existence probe followed by the real query.
- **[medium] N+1 `GetApp` per bot, sequential** — `room-service/handler.go:915-929` — `store_mongo.go:855` (`findAppsForDisplay`) already has the `$in` batch shape to reuse.
- **[low] `GetUser` then `GetRoom` run serially** — `room-service/handler.go:1971-1978`, `:2040-2047` — `handler.go:1645-1660` already demonstrates the `errgroup` pattern.
- **[nitpick] Still decodes into full `model.Subscription`** — `room-service/store_mongo.go:739` — though only 5 fields land; a narrow row struct would cut per-member allocation further.
- **[nitpick] `FindDMSubscription` could be covered** — `room-service/store_mongo.go:137` — `(u.account, name)` plus projection `{_id, roomId}`; `roomType` is filtered but unindexed so it still fetches. `(u.account, name, roomType, roomId)` would cover it. One document — low value.

`GetUser` and `GetApp` are not coverable (5 fields / a whole `assistant` sub-document); nothing to do there.

## Regressions

**None found.** All five are pure projection additions. The `{joinedAt, _id}` index-backed sort is unaffected — projection applies post-sort, and the 32 MB sort limit is still avoided. No query was previously covered, so none was un-covered. The `u._id` nested path is correct (`SubscriptionUser.ID` → `bson:"_id"`, `pkg/model/subscription.go:21`); a `u.id` typo would have silently zeroed `Member.ID`, and the new test pins it.

---

# Observability

## Findings

- **[high] The only guard against projection drift is a test CI never runs** — `room-service/integration_test.go:1` vs `room-service/deploy/azure-pipelines.yml:44` — the new pins are `//go:build integration`, but the pipeline runs `go test ./$(SERVICE_NAME)/...` with no `-tags integration`, and there is no integration step anywhere in that file. The entire safety net for a silent-zeroing change exists only behind a manual `make test-integration` (`Makefile:120-124`). Either add an integration lane, or state in the PR that the guard is local-only.
- **[medium] Nothing at runtime would surface a projection bug; one cheap signal is worth adding, four are not** — `room-service/store_mongo.go` contains zero `slog.` calls and no spans, so store methods have no span convention to diverge from. The only per-operation telemetry is the driver command span/metric from `o11ymongo.Instrument` (`pkg/mongoutil/mongo.go:50`), which reports latency and errors but never field content. A dropped projection key therefore surfaces only as a *business* symptom: a missing `engName`/`chineseName` becomes `errInvalidUserData` (`room-service/handler.go:163`); a missing `roles` makes `IsPlatformAdmin` false and yields `errOnlyAdmins` (`handler.go:2044`) — a real admin gets a 403 and no log distinguishes that from a genuine non-admin.

  Cost-weighted recommendation:
  - **Add one branch** in `GetUser` (`store_mongo.go:896`): after a successful decode, if `u.ID == "" || u.Account == ""`, emit `slog.WarnContext(ctx, "room-service GetUser projection returned empty identity", "request_id", natsutil.RequestIDFromContext(ctx), "account", account)`. Both fields are non-optional on every user document, so empty-after-success is unambiguously a projection or schema bug. Request/reply path, negligible cost, and it names the failure the two admin gates otherwise hide.
  - **Do not** add per-field debug logs or a metric to the other four. `FindDMSubscription` and `GetThreadSubscriptionByParent` results are consumed through a single field each (`handler.go:244`, `handler.go:1690`), and `getRoomSubscriptions` (`store_mongo.go:733`) is a list path where per-row logging is a hot-path cost. A new instrument plus dashboards is not justified for a bug class the integration test catches deterministically — fixing the finding above is the better spend.
  - Precedent note: only `tcard-service/store_mongo.go:60` logs from a store layer, so this is a mild new precedent, but compliant with CLAUDE.md Section 3.
- **[medium] `active` is not projected, and `IsActive()` silently returns true when absent** — `room-service/store_mongo.go:890-894` — `Active *bool` nil means active (`pkg/model/user.go:~68-70`). No live regression: `IsActive` and `.Active` appear nowhere in room-service. But a future `requester.IsActive()` would silently pass a deactivated account. Record it in the projection comment as an explicit "if you ever need `IsActive`, add `active` here".
- **[nitpick] `User.String()` is not relied upon, and the projection dulls it** — `pkg/model/user.go:83` omits the hash by design; grep found no site logging a `model.User`, `model.App` or `model.Subscription` value wholesale (no `%v`/`%+v` or slog struct-value use in `room-service/*.go`). The change makes `String()` moot for safety but slightly misleading: `SiteID` is now always `""` on a `GetUser` result, so a future `%v` in a siteID-routing debug session would print a false empty.
- **[nitpick] Comment/assert mismatch** — `room-service/integration_test.go:337` describes "the fallback (enrich=false) build" but calls `ListRoomMembers(ctx, "rmembers", nil, nil, true)` and asserts `IsOwner`, which only `enrich=true` populates (`store_mongo.go:753`).

## Verified clean

**Secret-leak claim holds.** The inclusion projection at `store_mongo.go:890-894` lists only `_id, account, engName, chineseName, roles`; Mongo inclusion semantics exclude everything else, so `Services` (`pkg/model/user.go:69`, the bcrypt block) never leaves Mongo on this path. Pinned at `integration_test.go:231`. Strict improvement.

**ctx / request-ID propagation: no finding.** All five methods pass `ctx` through unchanged; callers derive IDs via `natsutil.RequestIDFromContext(ctx)` (`handler.go:155`, `:286`) and Mongo spans nest under the handler span via ctx. The diff introduces no log statements and no `fmt.Println`/`log.Println`.

---

# Prioritized action list

Ordered by severity, then impact ÷ effort. Items 1–4 are cheap and belong on this PR; 5–7 are follow-ups; 8–9 are optional polish.

### 1. `high` — Decide how the projection guard actually runs
**Dimension:** Observability + Test automation · **Evidence:** `room-service/integration_test.go:1`, `room-service/deploy/azure-pipelines.yml:44`

All five guards are `//go:build integration`; the pipeline runs `go test` with no `-tags integration` and has no integration lane. They also have **never been executed** — Docker was unavailable when the branch was written. So a change whose failure mode is silent field-zeroing currently has zero automated verification anywhere. Minimum bar before merge: run `make test-integration SERVICE=room-service` on a machine with Docker. Better: add the integration lane, or state plainly in the PR that the guard is local-only.

### 2. `high` — Add a no-build-tag projection/bson-tag test
**Dimension:** Test automation · **Evidence:** `room-service/store_mongo.go:713-714`

The exact failure the author called out — `u.id` projecting nothing where `u._id` was meant — is pure bson-tag arithmetic and needs no Mongo. A plain `store_test.go` that reflects each dotted key in the five projection vars against the target struct's `bson` tags, failing on any key resolving to no field, runs on every commit and closes most of the gap from item 1 at a fraction of the cost. Highest impact ÷ effort item in this review.

### 3. `medium` — Fix the false comment
**Dimension:** all four lenses flagged it · **Evidence:** `room-service/integration_test.go:334`

Says "the fallback (enrich=false) build" but the call passes `enrich=true`, which the `IsOwner` assertion requires. A comment that contradicts its code is worse than no comment. One-line fix.

### 4. `medium` — Make the ListRoomMembers test able to fail
**Dimension:** Test automation · **Evidence:** `room-service/integration_test.go:341-364`, `room-service/store_mongo.go:733`

It asserts only included fields, so deleting `opts.SetProjection(roomMemberSubProjection)` leaves it green — it guards drift, not the projection. The other four pin exclusions and do fail. Either assert exclusions through a raw `Find`, or relabel it honestly as a drift guard. While there, assert the fallback path was actually taken (`store_mongo.go:511-517`), since the test reaches `getRoomSubscriptions` only incidentally.

### 5. `medium` — Record `active` and the partial-struct contract
**Dimensions:** Go, Observability, room-service generalist · **Evidence:** `room-service/store_mongo.go:890-894`, `room-service/store.go:197-220`

Two related documentation gaps with real teeth. `userReadProjection` omits `active`, and `Active *bool` nil means active — so a future `IsActive()` check fails **open**, silently passing a deactivated account. And `store.go` still documents all four methods as returning the entity, so a future caller reading `requester.SiteID` gets `""` with no signal. Add "if you need `IsActive`, add `active` here" to the projection comment, and a projection line to each of the four interface docs — following the precedent `GetRoomAppRead` (`store.go:63`) already sets.

### 6. `medium` — One `GetUser` identity warn, and nothing more
**Dimension:** Observability · **Evidence:** `room-service/store_mongo.go:896`, `room-service/handler.go:2044`

A dropped `roles` key turns a real admin's request into a 403 that no log distinguishes from a genuine non-admin. After a successful decode, warn when `u.ID == ""` or `u.Account == ""` — both are non-optional on every user document, so empty-after-success is unambiguously a bug. Deliberately do **not** instrument the other four: single-field consumers and a hot list path don't justify it.

### 7. `high` (deferred) — Take the covered-query win
**Dimension:** Performance · **Evidence:** `room-service/store_mongo.go:142-143`, `:753-755`

`roles` is `[]Role` → multikey, so no projection including it can ever be covered. But `roles` is read only when `enrich==true`, and three callers pass `enrich=false` (`handler_teams.go:62`, `:128`, `handler.go:1123`). Split into enrich/non-enrich projections and extend the index to `(roomId, joinedAt, _id, u.account, u._id)` — the non-enrich path becomes an index-only scan, eliminating ~1000 document fetches for a 1000-member room. This dwarfs the decode saving already banked, but it is an index change and belongs in its own PR.

### 8. `medium` (deferred) — The three perf items this diff left in the code it touched
**Dimension:** Performance · **Evidence:** `room-service/store_mongo.go:720-733`, `:509-521`, `room-service/handler.go:915-929`

`getRoomSubscriptions` is still unbounded when `limit == nil` (two Teams callers pass nil); `ListRoomMembers` still pays 2 serial round trips per call; `addMembers` still N+1s one `GetApp` per bot. All are out of scope for a projection PR — the projection shrinks each row, not the row count — but they are the next three things worth doing to these functions.

### 9. `low` — Use `_id: 0` where no caller reads the id
**Dimension:** Go, Bug & security · **Evidence:** `room-service/store_mongo.go:912`, `:931`, `:1534`

`{_id: 1}` is a no-op — Mongo returns `_id` unless excluded. For `appAssistantReadProjection`, `dmDedupProjection` and `threadSubParentProjection` no caller reads the id, so the file's own narrow-read idiom (`store_mongo.go:477`, `:835`, `:1337`) says `_id: 0`. `userReadProjection` and `roomMemberSubProjection` legitimately keep it.
