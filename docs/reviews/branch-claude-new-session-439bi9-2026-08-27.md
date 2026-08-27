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
