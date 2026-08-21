# admin-service: Cross-Site Fanout of User Create / Roles / Active — Design

**Date:** 2026-08-19 (revised 2026-08-20 per colleague review)
**Status:** Approved (2026-08-20 — two-lane design reconfirmed over single-direct and OUTBOX alternatives; lane choice is producer-side only, so a later switch to OUTBOX would not touch inbox-worker)

## Problem

`admin-service` writes to the `users` collection are site-local. `createUser`,
`updateUser` (roles, names, `active=true`) and `DeactivateAndRevoke`
(`active=false`) all filter on `{account, siteId}` and publish nothing
cross-site. Consequences today:

1. **Admin-created accounts never exist on remote sites.** The endpoint is
   the spec'd bot-creation path (`docs/specs/botplatform/auth.md` US4 — no
   separate `/admin/bots` route; `createUserRequest` accepts `roles`) and is
   also used for test accounts. `bot-room-service` federates bots into remote
   rooms via OUTBOX `member_added`. The remote `inbox-worker`'s
   `handleMemberAdded` treats a referenced account with no local `users` doc
   as a transient error and redelivers until the user lands — on the
   **sequential membership lane**, so every later add/remove for that site
   queues behind it. Nothing ever lands the user, so nothing unblocks it.
2. **Remote `active` / `roles` go stale.** After a home-site deactivation the
   remote `notification-worker` still counts the account as push-eligible
   (`{active: {$ne: false}}`). Remote readers of `roles` are few, but not
   none (see the security note below), and the field drifts all the same.

Neither problem is a security hole today — login, sessions and permission
checks all run at the home site — but (1) is a real operational stall and (2)
is visible drift.

**Security note — replicating `roles` widens platform-admin authority
(discovered 2026-08-20).** `room-service` reads `users.roles` by account with
**no `siteId` filter** (`room-service/store_mongo.go:961` `GetUser` matches
`{account}` alone), and feeds the result to `model.IsPlatformAdmin` to authorize
`roomRename` (`room-service/handler.go:1977`) and `roomRestricted`
(`:2034`). Today a remote site has no `users` doc for a home-site admin, so
those checks fall through to the owner path. Once this design replicates
`roles`, a platform admin created at their home site becomes a platform admin
at **every** site for those room operations. That is a deliberate,
accepted consequence of "one account, same roles everywhere" — not an
oversight — but it is an authority expansion, so it needs explicit sign-off in
the PR. Narrowing it later means site-scoping `room-service`'s admin lookup,
which is a `room-service` change and out of scope here.

## Decisions taken

Q-numbers from the 2026-08-19 grilling session; R-numbers from the 2026-08-20
colleague review, which **supersedes** the crossed-through parts.

| # | Decision |
|---|---|
| Q1 | Scope is admin-service's three write paths reaching every remote site. Primary goal: admin-created accounts exist everywhere. Secondary: `active`/`roles`/names not stale. |
| Q2 | ~~Pure status style: WARN only, response unchanged.~~ **Superseded by R2:** fanout failures are surfaced in the HTTP response so the admin frontend can distinguish a DB-write error from a fanout error. ~~Still no retry machinery and no resync endpoint.~~ **Superseded by R8:** a per-user resync endpoint re-delivers the snapshot. |
| Q3 | **Two lanes** (revised by R3): the **create bootstrap** rides the existing HR feed (`chat.hr.{site}.users.upsert`, durable, create only). **All admin-owned account state — roles, names, active — rides a new INBOX event** `user_account_updated` (direct publish, whole snapshot, fired on every trigger). |
| Q4 | HR lane fields (create only): `_id, account, siteId(home), engName, chineseName`. INBOX snapshot fields: `id, account, siteId(home), engName, chineseName, roles, active, timestamp`. Never cross: `services.password`, `requirePasswordChange`, `permissions` (own event), `settings`/`statusText`/`chatlist` (user-service owns), `employeeId`/dept fields (HR owns; admin never sets them). |
| Q5 | Triggers: `createUser`, `updateUser` (roles / names / `active=true`), `DeactivateAndRevoke`. Not: `setPassword`, `changePassword`, `revokeSession(s)`, permissions. All three publish the INBOX snapshot; only `createUser` additionally publishes the HR bootstrap. |
| Q6 | INBOX lane destinations: every entry of `ALL_SITE_IDS` except self (`remoteSites`). HR lane reaches every site by construction (per-site `hr-sync-worker`). |
| Q7 | Remote INBOX apply is an **upsert** keyed by `account` with `$setOnInsert {_id, siteId}`, so either arrival order converges to the same doc with the same `_id`. This is a deliberate exception to the other four user handlers' "missing doc = silent no-op" rule and must be commented as such. |
| Q8 | ~~Name changes ride the HR lane.~~ **Superseded by R3:** name changes ride the INBOX snapshot. The HR lane fires ~~only at create~~ at create and per-user resync (R8). |
| Q9 | HR payload is an identity-only `model.User` (same shape `room-worker` / `message-worker` publish); `roles`, `active`, `services` are never populated on the wire. |
| Q10 | Watermark: new top-level field `accountUpdatedAt`, compared with **`$lte`** (two admin writes can share a millisecond; the apply is an idempotent whole-snapshot `$set`, so a same-ms tie resolves to last-delivered). |
| Q11 | No `Nats-Msg-Id` on either lane (snapshot is idempotent; `$setOnInsert {_id}` already prevents duplicate inserts). |
| Q12 | Fanout failures are **not** written to `admin_audit`. Audit records the admin action, not its replication. (Unchanged by R2 — the response carries the failure, the audit does not.) |
| Q13 | Snapshot shape: `active` is a resolved `bool` (`u.IsActive()`), `roles` is always an array (nil → `[]`), remote always `$set`s both. No "omitted = unchanged" semantics. |
| Q14–15 | `createUser` creates bots **and** test accounts. **All** creates fan out; no branch on `roles`. A never-federated test account becomes one inert row per site (remote user search hits the external HR HTTP API, not `users`; remote `users` reads are by-account lookups). |
| Q16 | HR lane `changeType` is **omitted** (`omitempty`). `hr-sync-worker` ignores it; `search-sync-worker` does not consume `users.upsert`. |
| Q21 | HR-lane publish failure does not fail the request (the local insert already committed; a 5xx would make the admin retry into a 409). **R2 refines it:** the failure is reported as `hrSyncFailed: true` in the 200 response instead of being WARN-only. |
| R1 | **HR lane payloads are zstd-compressed**: `natsutil.EncodeZstd(data)` + `Nats-Encoding: zstd` header, mirroring `teams-hr-sync`'s `publishZstd`/`jetStreamPublish`. `hr-sync-worker` already decodes via `natsutil.DecodePayload` (header-sniffing, so the plain publishes from `room-worker`/`message-worker` stay compatible). |
| R2 | **Fanout errors are surfaced, DB errors stay errors.** A failed local write is the existing errcode envelope (4xx/5xx; frontend branches on `reason`/`code`). A committed write whose fanout partially failed is HTTP 200/201 with `syncFailures: []string` (INBOX lane, per failed site, name matches the permissions precedent) and `hrSyncFailed: bool` (~~create only~~ create and resync, per R8), both `omitempty`. |
| R3 | **Roles, names, and active all sync via direct INBOX publish.** The HR lane is reduced to the durable identity bootstrap (~~create only~~ create and per-user resync, per R8). Consequence: the INBOX snapshot carries the full identity, so a doc created by either lane alone is complete (no nameless stub). |
| R4 | **admin-frontend displays both error kinds** (added 2026-08-20). DB errors already render via `useHandleAdminError` → `formatAsyncJobError` (no change needed). New: the `createUser`/`updateUser` API clients surface `syncFailures`/`hrSyncFailed`, and `CreateUserForm` / `EditUserDialog` render a post-success sync-failure notice following `CreatePermissionsDialog`'s existing `syncFailures` result-banner pattern. The UsersConsole UI already exists (`admin-frontend/src/components/UsersConsole/`). |
| R5 | **Create notice alarms only when both lanes missed a site** (added 2026-08-21). Per Q6 and R3, either lane alone fully covers a site: the durable HR lane reaches every site by construction, and the direct INBOX snapshot is complete on its own. `CreateUserForm` therefore treats a create as synced unless `hrSyncFailed` AND `syncFailures` are both present, and then lists the both-missed sites (heal: re-save the user). **Superseded by R9.** The wire response is unchanged — both fields still come back so the API stays honest. Edits are unaffected: the INBOX snapshot is their only lane, so `EditUserDialog` still surfaces every `syncFailures`. |
| R6 | **Permissions section is deploy-gated** (added 2026-08-21). The admin console renders the Permissions tab only when the runtime config flag `PERMISSIONS_ENABLED` is the literal string `"true"` (nginx envsubst renders it from the container env; default `false`). Backend permission endpoints are unchanged — this gates the UI section only. Dev compose defaults the flag off too (revised 2026-08-21; enable per environment with `PERMISSIONS_ENABLED=true`). |
| R7 | **The user list spans every site** (added 2026-08-21). `GET /v1/admin/users` (store `SearchUsers`) no longer filters by `siteId`: admins see cross-site replicas too, with a Site column. Rows homed at another site are read-only — the actions cell renders nothing for them, matching the server, where every mutating user endpoint stays home-site-scoped (404 elsewhere). |
| R8 | **Per-user Resync action** (added 2026-08-21). `POST /v1/admin/users/:account/resync` re-delivers the current home-site state on both lanes (HR identity bootstrap + INBOX snapshot to every remote site). Like permissions resync it is re-delivery, not re-recording: no user write, no audit entry; receivers re-stamp the watermark with identical values, so it is idempotent. Foreign replicas 404. The UI requires an explicit confirm before firing and applies the ~~R5~~ R9 notice rule to the result. |
| R9 | **Sync notice is driven by the direct lane; HR only picks severity** (added 2026-08-21, revises R5/R8's notice rule). R5 assumed either lane alone fully covers a site, but the durable HR feed carries identity fields only (`hr-sync-worker` never writes roles/status — `users` is the live auth store), so an INBOX miss leaves the remote replica without roles even when HR lands. New rule for `CreateUserForm` and `ResyncUserDialog`: `syncFailures` empty → silent success regardless of `hrSyncFailed` (the INBOX snapshot is complete on its own); `syncFailures` present + `hrSyncFailed` → full-failure notice (account absent at those sites); `syncFailures` present + HR ok → identity-only partial notice (roles/status missing at those sites). All three dialogs direct the admin to the per-user Resync action (R8) instead of re-saving — `EditUserDialog`'s rule was already correct (INBOX is its only lane, any miss alerts); only its guidance copy changed. Wire response unchanged. |

## Approach

One admin write → one local Mongo write → read back the post-write doc → up to
two JetStream publishes → independent remote applies.

```
admin HTTP ──► ① write local users (tx for deactivate) ──► ② read-back snapshot
                                                                 │
                    create only ┌────────────────────────────────┴──── every trigger ──────────┐
                                ▼                                                              ▼
   HR lane (durable bootstrap)                                    INBOX lane (direct, surfaced errors)
   chat.hr.{A}.users.upsert · zstd + Nats-Encoding header         chat.inbox.{B,C,…}.external.user_account_updated
   []IUserWithChange{User{ID,Account,SiteID,EngName,ChineseName}} InboxEvent{Type,SiteID,DestSiteID,Payload,Timestamp}
   ──► HR-A stream ──► hr-sync-worker @ every site                ──► INBOX-{dest} ──► inbox-worker @ dest
       $set identity · $setOnInsert {_id}                             upsert by account · $setOnInsert {_id, siteId}
       (no code change; DecodePayload already sniffs zstd)            $set {engName, chineseName, roles, active,
       failure → hrSyncFailed: true in the 201                              accountUpdatedAt} · $lte guard
                                                                      failures → syncFailures: [site…] in the 200/201
```

Why two lanes: the thing that actually stalls (a missing remote doc for a
federated bot) must land **eventually even when the direct publish to a peer is
lost** — the HR stream parks the event until the peer's `hr-sync-worker`
drains it, which is exactly why `room-worker` and `message-worker` already
publish newly-discovered identities there. The INBOX snapshot is the fast,
complete path for everything else; its loss is tolerable because the next
admin edit re-sends the whole snapshot, and now every loss is also visible in
the response.

### Convergence under reordering

The two lanes have no ordering guarantee. Both applies are "upsert by
`account`, `$set` only my own fields, `$setOnInsert` the same `_id`":

| Order | After 1st | After 2nd |
|---|---|---|
| INBOX first | complete doc: `{_id, account, siteId, engName, chineseName, roles, active, accountUpdatedAt}` | HR `$set`s the same identity values — no visible change |
| HR first | `{_id, account, siteId, engName, chineseName}` — `roles` missing reads `["user"]`, `active` missing reads active | + `roles, active, accountUpdatedAt` (and re-`$set` names) |

Same final document either way. One asymmetry to note: the HR apply carries no
watermark, so an HR bootstrap that arrives **after** a later INBOX rename
briefly reverts the names (`accountUpdatedAt` stays newer); the next admin
edit re-sends the snapshot and heals it. Narrow window (create → rename →
late HR delivery), accepted.

### Stale-event and insert-race handling on the INBOX lane

The apply filter is `{account, $or: [{accountUpdatedAt: {$exists: false}}, {accountUpdatedAt: {$lte: T}}]}`
with `upsert: true`. When the filter matches nothing **and** a doc for that
`account` already exists, Mongo attempts an insert of `{account}` → `E11000`
on the unique `account` index. That happens in two distinct situations:

1. a **newer** snapshot is already stored (genuinely stale event);
2. the HR lane's insert for the same account **raced** ours in the same
   instant — the doc now exists, but carries no `accountUpdatedAt` yet, so our
   fields are not stale at all.

The store therefore does **not** swallow `E11000` blindly: on
`mongo.IsDuplicateKeyError` it re-runs the same update once with
`upsert: false`. Case 1 → `MatchedCount == 0` → return nil (Ack, no-op).
Case 2 → the doc now exists with no/older watermark → `$set` applies. Both
paths must be covered by tests.

## Changes

### `pkg/model/event.go`

```go
InboxUserAccountUpdated InboxEventType = "user_account_updated"

// UserAccountUpdated is the InboxEvent.Payload for user_account_updated: the
// admin-owned account state as a whole snapshot (identity included, so the
// event alone materializes a complete remote doc). Replicated by admin-service
// to every remote site after createUser / updateUser / DeactivateAndRevoke;
// inbox-worker upserts it under the accountUpdatedAt watermark.
type UserAccountUpdated struct {
	ID          string     `json:"id"          bson:"id"`
	Account     string     `json:"account"     bson:"account"`
	SiteID      string     `json:"siteId"      bson:"siteId"` // home site; immutable, $setOnInsert only
	EngName     string     `json:"engName"     bson:"engName"`
	ChineseName string     `json:"chineseName" bson:"chineseName"`
	Roles       []UserRole `json:"roles"       bson:"roles"`     // always an array, never nil
	Active      bool       `json:"active"      bson:"active"`    // resolved via IsActive()
	Timestamp   int64      `json:"timestamp"   bson:"timestamp"` // unix ms; doubles as the watermark
}
```

### `admin-service`

- **`store.go` / `store_mongo.go`** — `UpdateUser` and `DeactivateAndRevoke`
  return the post-write `*model.User` (FindOneAndUpdate `ReturnDocument: After`,
  projected to `_id, account, siteId, engName, chineseName, roles, active`;
  inside the existing transaction for deactivate). No second read.
  `CreateUser` already has the inserted `u`. A PATCH with no fields to set
  returns `(nil, nil)` and the handler skips fanout (nothing changed).
  `make generate` refreshes the mock.
- **`main.go`** — the publish func gains an encoding parameter, mirroring
  `teams-hr-sync`'s `jetStreamPublish` (`natsutil.NewMsg`, guard nil `Header`,
  set `Nats-Encoding` when non-empty). Signature:
  `func(ctx, subj string, data []byte, encoding string) error`. The
  permissions call sites pass `""` (mechanical; tests updated alike).
- **`handler.go`** — new `fanoutUserAccount(ctx, u *model.User, isCreate bool)
  (hrFailed bool, syncFailures []string)`:
  1. `now := time.Now().UTC().UnixMilli()`
  2. HR bootstrap (`isCreate` only): marshal
     `[]model.IUserWithChange{{User: model.User{ID, Account, SiteID, EngName,
     ChineseName}}}` (no `ChangeType`) →
     `publish(ctx, subject.OrgSyncUsersUpsert(cfg.SiteID),
     natsutil.EncodeZstd(data), natsutil.EncodingZstd)`. On error:
     `slog.WarnContext` + `hrFailed = true`.
  3. INBOX snapshot (every call): payload `model.UserAccountUpdated{ID,
     Account, SiteID, EngName, ChineseName, Roles: nonNil(u.Roles),
     Active: u.IsActive(), Timestamp: now}`; for each `dest` in
     `remoteSites(cfg.AllSiteIDs, cfg.SiteID)` wrap in `model.InboxEvent` and
     `publish(ctx, subject.InboxExternal(dest, model.InboxUserAccountUpdated),
     data, "")`. One goroutine per destination — the post-#301 per-destination
     lane shape of `publishPermissionFanout`: budget from `cfg.FanoutTimeout`
     (env `FANOUT_TIMEOUT`, default 30s) capped by the request's absolute
     deadline (`withRequestBudget`), failures aggregated into a
     position-indexed slice. No chunking — the payload is one account. Each
     failure also `slog.WarnContext`s.
  4. Runs synchronously before the HTTP response; never changes the status
     code of a committed write. `nil publish` (tests) publishes nothing and
     reports no failures.
  - Call sites and response contract:
    - `createUser` → 201 with `{...existing fields, "syncFailures": [...],
      "hrSyncFailed": true}` (both `omitempty`).
    - `updateUser` (both branches) → 200 `{"status": "ok", "syncFailures":
      [...]}` (`omitempty`; no HR lane, so no `hrSyncFailed`).
    - A failed **local write** keeps today's errcode envelope — that is the
      "DB error" the frontend distinguishes by envelope-vs-fields.

### `inbox-worker`

- **`handler.go`** — `case model.InboxUserAccountUpdated: return
  h.handleUserAccountUpdated(ctx, &evt)`; unmarshal `model.UserAccountUpdated`,
  call `store.UpsertUserAccount(ctx, &e, time.UnixMilli(e.Timestamp).UTC())`.
  Interface method added to `InboxStore`; `make generate` refreshes
  `mock_store_test.go`.
- **`main.go`** — `mongoInboxStore.UpsertUserAccount`:
  ```go
  filter := bson.M{"account": e.Account, "$or": bson.A{
      bson.M{"accountUpdatedAt": bson.M{"$exists": false}},
      bson.M{"accountUpdatedAt": bson.M{"$lte": updatedAt}},
  }}
  update := bson.M{
      "$setOnInsert": bson.M{"_id": e.ID, "siteId": e.SiteID},
      "$set": bson.M{"engName": e.EngName, "chineseName": e.ChineseName,
          "roles": roles, "active": e.Active, "accountUpdatedAt": updatedAt},
  }
  _, err := s.userCol.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
  if mongo.IsDuplicateKeyError(err) {
      // Doc exists: either a newer snapshot (stale → no match → no-op) or the
      // HR lane's insert raced ours (no watermark yet → $set applies).
      _, err = s.userCol.UpdateOne(ctx, filter, update) // upsert=false
  }
  ```
  `roles` is normalized to a non-nil slice before the write. The method comment
  states why this handler upserts when the other four user handlers do not
  (two-lane arrival order; the created doc must unblock `member_added`).
- Consumer config unchanged — the durable already filters
  `chat.inbox.{site}.external.>`.

### `admin-frontend`

- **`src/api/admin/index.ts`** — `createUser` returns
  `CreateUserResult { user: AdminUser; syncFailures: string[]; hrSyncFailed: boolean }`
  (wire: the 201 body embeds the view plus the two `omitempty` fields; absent →
  `[]` / `false`). `updateUser` returns `UpdateUserResult { syncFailures: string[] }`
  instead of `void`. Only call sites are `CreateUserForm` / `EditUserDialog`.
- **`CreateUserForm.jsx`** — on success with ~~`syncFailures.length > 0 ||
  hrSyncFailed`~~ `syncFailures.length > 0` (R9), swap the form for a result
  notice (the `CreatePermissionsDialog` pattern): "created on this site" +
  severity by `hrSyncFailed` + ~~"re-save to retry"~~ "use Resync" (R8/R9); a
  Done button then calls `onCreated()`. A fully clean result closes
  immediately as today.
- **`EditUserDialog.jsx`** — same shape for `updateUser`: on `syncFailures`,
  show the notice with a Close button calling `onUpdated()`; clean result
  closes immediately as today.
- DB-error display is untouched — non-2xx still throws `AsyncJobError` and
  renders via the existing `useHandleAdminError` banner, which is what keeps
  the two error kinds visually distinct (error banner vs post-success notice).

### Untouched

`pkg/outbox` (these are not OUTBOX types), `hr-sync-worker` (third producer on
an existing contract; `DecodePayload` already handles zstd), `pkg/subject`
(both builders exist), `docs/nats-subject-naming.md` (does not enumerate inbox
event types). ~~`docs/client-api.md` and its derived views~~ — later revisions
(R2, R7, R8) did land in `docs/client-api.md` §9.1/§9.2/§9.4/§9.16 and
`request-reply.md`.

## Edge cases

- **INBOX snapshot lost for a site** — that site's names/roles/active stale
  until the next admin change to that account; the loss is listed in
  `syncFailures`, so the admin sees it at write time. Accepted (Q2/R2).
- **HR bootstrap lost (local NATS blip at create)** — `hrSyncFailed: true` in
  the 201. The INBOX snapshot still materializes complete docs on every
  reachable site; a site that was ALSO unreachable on the INBOX lane has no
  doc until the next admin edit re-sends the snapshot. Visible, narrow,
  accepted.
- **Remote already has the doc from the HR feed** (an HR user being
  deactivated) — filter matches, `$set` only; `_id`/`siteId` untouched.
- **Late HR bootstrap after an INBOX rename** — names briefly revert (HR apply
  has no watermark); `accountUpdatedAt` stays newer; next admin edit heals.
  Narrow window, accepted.
- **Two admin writes in the same ms** — `$lte` lets the later-delivered win;
  both are full snapshots, so the doc is consistent either way.
- **`ALL_SITE_IDS` empty / single-site dev** — INBOX lane publishes nothing
  and reports no failures; HR bootstrap still publishes (harmless;
  `hr-sync-worker` consumes locally).
- **`roles` cleared by admin** — wire carries `[]`, remote `$set roles: []`,
  `model.User` reads that as `["user"]`. Never `$unset`.
- **Empty PATCH** (no fields) — local no-op, no fanout, no failure fields.
- **Both lanes insert in the same instant on a remote** — whichever loses the
  unique-index race retries: `hr-sync-worker` via its normal Nak/redeliver,
  `inbox-worker` via the one-shot non-upsert retry above. Converges.

## Failure modes

| Point | Effect | Recovery | Severity |
|---|---|---|---|
| ① local Mongo write fails | errcode envelope to admin (**the "DB error"**), no publish | admin retries | normal path |
| HR publish fails (local NATS) | `hrSyncFailed: true` in 201 + WARN; bootstrap not parked | visible to admin; INBOX snapshot covers reachable sites; next edit re-sends snapshot | low |
| INBOX publish fails (peer unreachable) | site listed in `syncFailures` + WARN | next admin change to that account re-sends the snapshot | low |
| HR lane: remote `hr-sync-worker` down | event parks in `HR-A` | automatic on restart | none |
| HR lane: remote store write fails | Nak/redeliver, head-of-line on that stream's sequential consumer | pre-existing risk; admin volume is negligible | pre-existing |
| Late HR bootstrap vs newer rename | names briefly revert on that site | next admin edit | low |

## Prerequisite to confirm with ops

The HR lane reaches every site only if every site's `hr-sync-worker` consumes
`HR-A` with a **non-colliding** durable. The durable name is hard-coded
(`"hr-sync-worker"`); several sites creating the same durable on the same
stream in one JetStream domain would **split** delivery, not fan it out. Either
sites are separate domains with an ops-level mirror/source of the HR streams,
or deployment arranges `SITE_IDS`/durables accordingly. `room-worker` and
`message-worker` already depend on this; if it does not hold, that is a
pre-existing latent bug — but this design leans on it harder. Confirm before
rollout; not a blocker for implementation.

## Testing

TDD throughout (red → green → refactor), table-driven where there are variants.

- **`admin-service/handler_test.go`**
  - `createUser` publishes the HR bootstrap **zstd-encoded with the
    `Nats-Encoding: zstd` header** — the test decodes via
    `natsutil.DecodePayload` and asserts an identity-only `[]IUserWithChange`
    (no `roles`, no `changeType`, no `services`) — plus one INBOX event per
    remote site whose snapshot carries id/account/siteId/names/roles/active.
  - `updateUser` with `roles` → snapshot carries new roles; with names →
    snapshot carries new names (no HR publish); `active=true` → `Active: true`.
  - `updateUser active=false` → snapshot `Active: false`.
  - HR publish error → 201 with `hrSyncFailed: true`, WARN, no audit of the
    failure.
  - One INBOX destination erroring → 200/201 with exactly that site in
    `syncFailures`; others delivered.
  - All lanes healthy → neither failure field present in the JSON.
  - `nil publish` → no publishes, no failure fields.
  - No fanout when the local write fails (errcode envelope unchanged).
- **`admin-service` integration (`store`)** — `UpdateUser` /
  `DeactivateAndRevoke` return the post-write doc; deactivate still purges
  sessions in the same transaction.
- **`inbox-worker/handler_test.go`** — dispatch of `user_account_updated` to
  the store with unmarshalled fields; malformed payload → error.
- **`inbox-worker/integration_test.go`** (`mongoInboxStore.UpsertUserAccount`)
  - no doc → inserts complete doc with the given `_id`, `siteId`, names,
    `roles`, `active`, `accountUpdatedAt`.
  - HR-shaped doc exists (`$set` identity as `hr-sync-worker` does) → `$set`
    names/roles/active, `_id`/`siteId` unchanged.
  - older timestamp than stored → duplicate-key → non-upsert retry matches
    nothing → doc unchanged, nil returned.
  - doc exists **without** `accountUpdatedAt` (HR lane landed first / raced)
    → duplicate-key → non-upsert retry applies the snapshot.
  - equal timestamp → applied (`$lte`).
  - `roles: []` stored as an empty array, not null.
- **`admin-frontend`** (vitest; `npm test` + `npm run typecheck` in
  `admin-frontend/`)
  - `admin.test.ts`: `createUser` maps absent fields to `[]`/`false` and
    passes through present ones; `updateUser` returns `syncFailures`.
  - `CreateUserForm.test.jsx`: clean result → `onCreated` immediately, no
    notice; `syncFailures`/`hrSyncFailed` → notice rendered with site list,
    Done → `onCreated`; non-2xx → existing error banner (unchanged path).
  - `EditUserDialog.test.jsx`: clean save closes; `syncFailures` → notice,
    Close → `onUpdated`.
- Coverage: ≥ 80% on every touched package; handlers/stores target 90%+.
  `make lint`, `make test SERVICE=admin-service`, `make test
  SERVICE=inbox-worker`, `make test-integration SERVICE=inbox-worker`,
  `make generate` for mocks.

## Out of scope

Permissions fanout (exists), password/session replication (never), ~~a resync
endpoint (retry = re-saving the user; failures are visible per R2/R4)~~ —
**superseded by R8**, which shipped `POST /users/:account/resync` —
changes to `hr-sync-worker`, a client-facing status event, and the `status`
client-fanout gap noted in the user-sync write-up. (An earlier revision
excluded the admin-frontend display on the mistaken claim that no
user-management page exists — `UsersConsole` does exist, and R4 pulls the
display into scope.)
