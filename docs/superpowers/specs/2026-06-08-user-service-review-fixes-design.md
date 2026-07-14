# user-service Review-Round Fixes — Design

**Status:** Round 1 (six fixes) implemented (PR #279) — landed via 3-subagent TDD; CodeRabbit notes resolved; branch review run. See the plan's "Execution record" chapter for outcomes.
**Round 2 (this update):** resolves the previously-deferred **#9** retention/window question — design approved, implementation pending. See `## Changes → #9`.
**Date:** 2026-06-08
**Scope:** Targeted correctness/efficiency fixes to `user-service` raised during code review. No new endpoints, no wire-schema changes.

## Context

`user-service` consolidates the old mock-user-service's subscription endpoints into 9 NATS request/reply handlers (MongoDB-backed, cross-site federation via the outbox). A review pass raised 13 points. After triage, six require code changes; the rest are either already-correct behavior or deferred for separate discussion.

## Triage Summary

| # | Verdict | Notes |
|---|---------|-------|
| #1 channel search by member | No change | Already a separate endpoint: `subscription.getChannels` → `FindChannelsByMembers`. |
| #2 enrich dedup | **Fix** | Same room in multiple subs is queried multiple times in one RPC. |
| #3 applyRoomInfo only when Found | No change | Already guarded — `applyRoomInfo` early-returns on `!info.Found`. |
| #4 favorite filter "after" enrichment | No change | It already runs *before* enrichment; `Favorite` is a sub-level field, so filtering first is correct and cheaper. |
| #5 GetChannels append account | No change | Requester membership is already implicit via the `u.account: account` $match. Decision: leave implicit. |
| #6 countUnread dedup | **Fix** | Same dedup as #2, in the unread-count path. |
| #7 validate CreateDMRoom success | **Fix** | `SyncCreateDMReply.Success` is silently dropped by the client. |
| #8 hardcoded constants | **Fix** | Catalog all; extract the clearly-magic inline literals. |
| #9 30-day policy | **Deferred** | Tracked in Open Questions; no code/behavior change this round. |
| #10 getCurrent time window | **Fix** | `current` must return everything — drop the window. |
| #11 getRooms filter field | No change | Decision: `subscription.updatedAt` applied uniformly (incl. cross-site) is the right field for recent activity. Current behavior already does this. |
| #12 FindChannelByMembers include requester | No change | Same as #5 — implicit via `u.account` match. |
| #13 publishStatus empty dest | **Fix** | Guard against a stray `""` dest (e.g. trailing comma in `ALL_SITE_IDS`). |

## Changes

### #2 — Dedup roomIDs in `enrichWithRoomInfo`
Per site, the handler currently appends `subs[j].RoomID` for every sub, so a room appearing in N subscriptions is sent N times in the `GetRoomsInfo` request. Dedup the per-site roomID list (preserve a stable order) before the RPC. The result map is keyed by roomID, so applying it back to every matching sub already fans out correctly — only the request payload shrinks.

### #6 — Dedup roomIDs in `countUnread`
Identical dedup before each per-site `GetRoomsInfo` RPC. Counting still iterates **all** subs (a room shared by two subs is still two subscription rows for unread purposes), only the RPC roomID list is deduped.

### #7 — Validate `CreateDMRoom` success (in roomclient)
`roomclient.CreateDMRoom` decodes `SyncCreateDMReply` but ignores `reply.Success`. Add: after a successful decode, if `!reply.Success`, return an `errcode.Internal("create-dm reported failure")` (an explicit not-success with no error envelope is a malformed/failed result). `apps.go`'s existing `if err != nil` then catches it — no change needed in `apps.go`.

### #10 — `getCurrent` returns everything (no time window)
`aggregateCurrent` currently applies `withinDays` to the `$match`. Remove the window from `aggregateCurrent` entirely so `current` returns the full set, sorted favorite→name (sort unchanged). The `rooms` listType keeps a window — but **#9 (below) re-points that window** from the phantom `subscription.updatedAt` onto `room.lastMsgAt`.

### #13 — `publishStatus` skips empty dest
The fan-out loop skips `dest == s.siteID`. Add a guard so a blank `dest` (`""`) is also skipped — defends against a trailing/leading comma in `ALL_SITE_IDS` producing a stray empty token that would otherwise publish to an empty subject segment.

### #8 — Constants catalog + extraction
Produce a written catalog (below) of every tunable/magic value in `user-service` with its purpose. Extract the clearly-magic inline string literals into named, documented constants:
- `"^Del-"` (soft-deleted room name prefix) — used in `mongorepo/subscriptions.go` deleted-filter.
- `"p_"` / `".bot"` (invalid DM-target markers) — used in `service/subscriptions.go` `GetDM`.

RoomType literals (`"dm"`, `"channel"`, `"botDM"`) inside Mongo pipelines are catalogued but **not** force-extracted — they are idiomatic query literals and a broad swap to `model.RoomType*` constants is churn out of scope for this round.

#### Catalog

| Constant / value | Location | Sets |
|------------------|----------|------|
| `maxSiteFanout = 8` | `service/subscriptions.go` | Max concurrent per-site room-service RPCs in enrich/count fan-out. |
| `maxStatusBytes = 512` | `service/status.go` | Max byte length of a user status text. |
| `roomRPCTimeout = 5s` | `roomclient/client.go` | Per request/reply round-trip timeout to room-service/room-worker. |
| `MaxSubscriptionLimit = 1000` (default) | `config` (`MAX_SUBSCRIPTION_LIMIT`) | `$limit` cap on aggregation result size. |
| `MetricsAddr = ":9090"` (default) | `config` (`METRICS_ADDR`) | Prometheus metrics listen address. |
| `Mongo.DB = "chat"` (default) | `config` (`MONGO_DB`) | Mongo database name. |
| `deletedRoomPrefix = "Del-"` (new) | `mongorepo` | Soft-deleted room name prefix used by the deleted-filter regex. |
| `dmTargetSystemPrefix = "p_"`, `dmTargetBotSuffix = ".bot"` (new) | `service` | Markers rejecting non-human DM targets. |

### #9 — Room-activity retention window (replaces dead `subscription.updatedAt`)

**Bug uncovered during the retention discussion.** The `rooms` listType windows on `subscriptions.updatedAt` (`mongorepo/subscriptions.go`), but **no service ever writes `updatedAt` onto a subscription doc**: `model.Subscription` declares no such field, `newSub` / `BulkCreateSubscriptions` never set it, `markRoomRead` sets only `lastSeenAt`/`alert`, and the cross-site mirror (`inbox-worker`) never sets it. Per-message activity is recorded on the **room** doc (`rooms.lastMsgAt`, bumped once per message by broadcast-worker), not per-member. So in production every subscription lacks `updatedAt`, the `$gte` match drops every row, and `subscription.list?type=rooms&updatedWithinDays=N` returns an **empty list**. The existing integration test passes only because its fixtures hand-seed `updatedAt`. (Confirmed by a full-repo sweep: nothing else reads, writes, sorts, or indexes the field; the frontend wire type has no `updatedAt`; `room-service` declares no overlapping subscription index.)

**Decision — inactivity is a property of the room, not the user.** "Inactive" means the **whole room** had no message for N days, identical for every member — not "this user hasn't engaged." A member-level signal would also require a write per member per message (write amplification on large channels), which we explicitly reject. The correct, already-maintained signal is `rooms.lastMsgAt`.

**Changes:**
1. **Remove the dead dependency** — delete the `match["updatedAt"]` window (`mongorepo/subscriptions.go`); trim the index `{u.account, roomType, updatedAt}` → `{u.account, roomType}` (`mongorepo/store.go`, conflict-free — room-service declares no such index); fix the misleading comment; fix the tests + index assertion that hand-seed/assert `updatedAt`.
2. **Re-point the window onto `room.lastMsgAt`** — when `updatedWithinDays` is set, filter on the joined `room.lastMsgAt` **after** the rooms `$lookup` (inside/after `roomsEnrichStages`, where the field is available): "no message in the room within N days."
3. **Server contract unchanged** — `updatedWithinDays == nil ⇒ no filter` (return everything); **no server-side default and no new env var** — the frontend owns any default (e.g. 30 days) and may pass any value, including > 30. Negative still rejected (`bad_request`). `current` and `apps` continue to ignore the window.
4. **Cross-site rooms** — a cross-site subscription has no local `rooms` doc, so `room.lastMsgAt` is null at Mongo time (its metadata arrives later via the RoomsInfoBatch RPC). Such rows are **kept regardless of the window** (consistent with the existing "always keep cross-site" deleted-filter rule); local windowing can't assess remote activity, and dropping them would hide active remote channels. Precise cross-site windowing (filter after the enrichment RPC) is a possible follow-up, out of scope here.

**Index-migration note:** trimming the index in `EnsureIndexes` only affects newly-provisioned DBs; Mongo won't auto-drop the old 3-key index from existing environments (the 3-key still serves the 2-key prefix, so it's harmless). Reclaim with a one-off `dropIndex` if desired.

## Testing

All changes follow Red-Green-Refactor (CLAUDE.md §4):
- **#2/#6 dedup** — unit tests in `service/*_test.go` asserting the mocked `GetRoomsInfo` receives a deduped roomID slice when a room repeats across subs; behavior (enriched subs / unread count) unchanged.
- **#7** — roomclient test (or a service-level mock test) asserting `!Success` decode yields an error; existing apps tests still pass.
- **#10** — mongorepo integration test asserting `current` ignores `updatedWithinDays` (returns a sub older than the window), while `rooms` still honors it.
- **#13** — status test asserting a blank dest in `allSiteIDs` is not published to.
- **#8** — covered by existing tests; extracted constants are drop-in.
- **#9** — mongorepo integration test: seed room docs with `lastMsgAt`; assert `rooms` + `updatedWithinDays` drops subs whose **room** is stale, keeps fresh-room and cross-site subs; flip the existing `withinDays`-on-subscription test to the room-activity contract; update the index assertion to `{u.account, roomType}`. `current`/`apps` unaffected.

## Resolved (was deferred)

- **#9 — retention/window policy** — discussed and decided; see `## Changes → #9`. The window now keys on **whole-room activity** (`rooms.lastMsgAt`); the phantom `subscription.updatedAt` (and its trailing index key) is removed; the server default is **no-filter** with the frontend owning any default. Root cause: the previous filter was non-functional against real data (the subscription `updatedAt` is never written).

## Out of Scope
- New endpoints or request/response wire-schema changes (none). Note: `docs/client-api.md` *is* updated for #10 to document that `updatedWithinDays` is ignored for the `current` list type — a client-observable behavior change, not a schema change.
- `MembersContain` exact-vs-substring semantics (not raised).
- Broad RoomType-literal extraction across Mongo pipelines.

---

# Round 3 — PR #279 Human-Review Comment Resolutions

**Status:** design approved 2026-06-10; implementation via subagent-driven-development.
**Trigger:** 14 inline review comments by GITMateuszCharczuk on PR #279. Triage: 3 mechanical fixes, 6 explanatory replies (no code change), 5 decisions — resolved by the author as below.

## Triage Summary (Round 3)

| # | Location | Verdict |
|---|----------|---------|
| 1 | `pkg/model/subscription.go:57` enrichment fields | **Reply** — read-time `$lookup` enrichment, documented in-line |
| 2 | `RoomKeysContext.tsx:79` TODO | **Reply** — verified accurate: keys do NOT yet arrive via subscription.list |
| 3 | `docs/user-service-endpoint-consolidation.md` | **Fix** — delete (one-time working note, referenced nowhere) |
| 4 | `room-service/store_mongo.go:105` comment length | **Fix** — shorten 85/86 block to ≤2 lines |
| 5 | `mongorepo/subscriptions.go` comments | **Fix** — `/remove_comments` pass |
| 6 | `mongorepo/store.go` single Store | **Fix** — split per collection (history-service pattern) |
| 7 | `models/app.go` OKResponse | **Reply** — `pkg/errcode` is error-only; no shared success type exists |
| 8 | CLAUDE.md additions | **Keep** — living project doc (endpoint list + sanctioned layout) |
| 9 | `service/status.go` naming | **Fix** — standardize to {Action}{Resource} |
| 10 | `service/apps.go` pagination | **Fix** — add offset pagination to apps.list |
| 11 | `service/subscriptions.go:276` comments | **Fix** — `/remove_comments` pass |
| 12 | `subscriptions.go:123` cross-service RPC | **Reply** — room-service owns live `name/lastMsgAt/lastMentionAllAt` (incl. cross-site); bounded fan-out, per-site degradation |
| 13 | `main.go:104` http server | **Reply** — standard Prometheus `/metrics` scrape listener, same as every service |
| 14 | `config.go` CurrentDomain/GetWithinDays | **Reply** — already absent; removed during consolidation |

## Changes

### R3-A — Store split (per-collection repos)
Mirror `history-service/internal/mongorepo`: collection-name consts, one repo struct per file, `NewXxxRepo(db *mongo.Database)`.
- `SubscriptionRepo` (`mongorepo/subscriptions.go`): `AggregateSubscriptions`, `FindChannelsByMembers`, `GetDMSubscription`, `GetSubscriptionByRoomID`, `CountActiveSubscriptions`, `GetActiveSubscriptions`, `GetAppSubscription`, `SetAppSubscribed` (all on the `subscriptions` collection), shared pipeline helpers (`roomsEnrichStages`, deleted-filter), `siteID`, and `EnsureIndexes` for the three subscription indexes.
- `UserRepo` (`mongorepo/users.go`): `GetUserStatus`, `SetUserStatus`, `EnsureIndexes` for the unique `account` index.
- `AppRepo` (`mongorepo/apps.go`): `GetApp`, `ListApps`. No indexes (apps reads are by `_id`/full scan; `ListApps`'s `$lookup` is served by SubscriptionRepo's `{name, roomType}` index).
- Delete `mongorepo/store.go` (incl. the dead `rooms` typed handle — `$lookup` references the collection by name).
- `service/service.go`: `UserStore` → `SubscriptionRepository` + `UserRepository` + `AppRepository`; `UserService{subs, users, apps, rooms(RoomClient), pub}`; `New(subs, users, apps, rooms, pub, cfg)`; mockgen directive regenerated; compile-time checks per repo.
- `main.go`: construct three repos; call each `EnsureIndexes`.
- Tests: integration tests construct the repo under test (+ seed via `testutil.MongoDB`'s returned `*mongo.Database` — the `db()` helper dies with Store); unit tests use the three new mocks. Pure refactor: no wire change, suite stays green.

### R3-B — Method renames ({Action}{Resource})
`StatusGetByName→GetStatusByName`, `StatusSet→SetStatus`, `AppsList→ListApps`. Go-internal; subjects unchanged; references updated in `RegisterHandlers` + tests.

### R3-C — apps.list pagination
- `models.AppsListRequest{Limit, Offset int}` (optional); response `{apps, total}` where `total` = full catalog count.
- `AppRepo.ListApps(ctx, account string, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPage[models.AppListItem], error)` via `$facet{data: [sort,skip,limit], total: [count]}`. `$literal` injection guard + its integration test preserved. Defaults via `mongoutil.NewOffsetPageRequest` (limit 20, max 100, negatives clamped).
- Router: `apps.list` moves `RegisterNoBody` → `Register`. **Enabler:** `natsrouter.Register` treats an empty payload as the zero-value request (`len(data)==0` → skip unmarshal). Behavior-safe for all existing endpoints (empty body previously only produced `bad_request`); unit-tested in pkg/natsrouter.
- `docs/client-api.md` §3.4 apps.list: request fields table (limit/offset), updated response semantics + JSON example (hard rule: same PR).
- Frontend: untouched (no apps.list caller exists).

### R3-D — Comment cleanups + doc deletion
`/remove_comments` two-pass on touched user-service Go files; `room-service/store_mongo.go` 85/86 comment block ≤2 lines; delete `docs/user-service-endpoint-consolidation.md`.

## Testing
- R3-A: existing unit+integration suites adapted (constructor/mocks); green = done. Compile-time interface checks per repo in integration setup.
- R3-B: rename-only; suite green.
- R3-C: TDD — new natsrouter empty-payload test (Red first); AppRepo pagination integration tests (defaults, offset beyond catalog → empty page + correct total, limit clamp); service-level unit test for request plumbing; injection-guard test retained.
- Coverage floor 80% maintained (`go tool cover` per package).

## Out of Scope
- Caching (analyzed separately: roomsubcache/userstore are wrong key-direction / consistency profile for user-service).
- Frontend apps.list integration.
