# User Permission Whitelist — Design

**Status:** Approved, amended — ready for re-planning
**Date:** 2026-08-10, amended 2026-08-11 (fixed UTC+8 interpretation, admin-frontend in scope,
slim audit dual-write, Metabase column contract), amended 2026-08-12 (cross-site sync:
permission state materialized on the user document and fanned out to every site; user query
moves from the dedicated `permission.get` RPC into `settings.get`; the ledger is demoted to
audit-only; admin-frontend changes deferred to the next round; single-admin-site
deployment assumption — `SiteID` dropped from the ledger, subjects company-wide, reason
wording clarified as optional), amended 2026-08-13 (bulk round: `MaxSubjects` and the
request-body limit removed, fanout chunked at 5,000 accounts/event, console bulk-paste +
resend — see `2026-08-13-permission-bulk-frontend-design.md`), amended 2026-08-17
(post-merge review round: chunk retuned to 1,000 accounts/event for the 128KB broker
`max_payload`; fanout budget is `FANOUT_TIMEOUT` (30s) validated at startup below the 40s
HTTP write timeout; each destination walks every chunk in its own lane so a stalled peer
cannot starve healthy ones; resync hands all state groups to one fanout call — see the
2026-08-13 spec's amendment)

## 1. Overview

A whitelist permission recording whether a user may **view images from outside the
corporate network**. Applications and approvals happen offline (mail / form / sign-off);
an operation admin records the already-approved decision into the system.

Every change is an independent record — `permission_grants` is an append-only ledger,
which is also the audit report a BI tool reads. **The ledger is audit-only**: the current
state is no longer computed from it. Instead, each admin write **materializes a per-user
permission snapshot onto the `users` document and fans it out to every site**, so that all
sites converge on the same state and a user's own site answers the query locally.

### 1.1 Goals

- Operation admins grant and revoke the permission for one or many users, via the HTTP
  API (terminal/curl) and the existing admin-frontend console view.
- A permission change takes effect at the admin's site immediately and **propagates to
  every other site** (each site's `users` collection covers the whole company).
- A user queries their own permission through the existing **`settings.get`** RPC — the
  response gains a `permissions` field. No dedicated permission RPC exists.
- A fanout publish failure is **surfaced to the caller** (`syncFailures` in the response),
  never silently swallowed.
- Every change is durably recorded with applicant, recording time, validity window,
  optional reason, and approver; changes also appear in the existing `admin_audit` trail (slim
  entries); the ledger stays directly readable by Metabase.

### 1.2 Non-goals

- **No enforcement.** media-service and the gateway are not changed. The answer is
  advisory; the hook for future server-side enforcement is specified in §13 but not built.
- **No in-system approval workflow.** There is no `pending` state — a write takes effect
  immediately.
- **No chat-frontend work.** The `settings.get` extension ships without client-side
  integration; clients pick the field up whenever they next read settings.
- **No client push event.** A permission change is visible on the next `settings.get`
  call; no `chat.user.{account}.event.*` fanout is added. (Future option: piggyback on
  the existing `settings.update` event.)
- **No admin-frontend changes in this round.** *(Superseded 2026-08-13: the deferred
  round is specified in `2026-08-13-permission-bulk-frontend-design.md` — paste-list
  bulk input, count-visible submit, `syncFailures` banner + resend, offender strip.)*
- **No Metabase integration.** Only the properties that the ledger is directly readable
  and the required columns exist are preserved (§12).
- **No durable relay (OUTBOX).** The user picked direct INBOX publish with error
  reporting over the OUTBOX relay. §5 records the trade-off and the upgrade path.

## 2. Architecture

No new service. Identity models decide where each path lands:

| Path | Service | Transport | Reason |
|---|---|---|---|
| Admin write | `admin-service` | HTTP (Gin) | Already has session-token auth, `requireAdmin`, an audit trail, and the admin-frontend console. |
| Cross-site sync | `admin-service` → each remote site's INBOX | JetStream direct publish | Same lane as user-service's status/settings fanouts (`chat.inbox.{dest}.external.…`); applied by each site's `inbox-worker`. |
| User query | `user-service` | NATS request/reply (`settings.get`) | The account in the subject **is** the authenticated identity; the permission field rides an RPC that already exists. |

**Why admin cannot use NATS for the write API.** `pkg/principal/scope.go` documents that
admins receive the same scoped signing-key template as an ordinary SSO user
(`chat.user.<account>.>`), so an admin has no cross-account publish rights. The **fanout**
publish is different: it is performed by admin-service's own service credentials
(server-side), not by the admin's user identity — the same class of grant user-service
already holds for its status/settings fanouts. Ops must permit admin-service to publish
`chat.inbox.*.external.>` across the supercluster (§14).

**Why users cannot use HTTP.** Unchanged from the original design: ordinary users hold
only a NATS JWT/NKey; HTTP session tokens exist for admins and bots.

**Deployment assumption — one admin site.** admin-service (and with it the ledger and
every permission write) is deployed at exactly one site company-wide; subjects span the
whole company, not just users homed at the admin site. Consequences threaded through
this design: `PermissionGrant` carries no site column (§3.1), the ledger is the complete
company-wide history (§12), and the subject lookups are site-unfiltered (§4.3, §4.5). If
a second admin site ever appears, nothing corrupts — the watermark guard already
converges multi-writer races (§5.2) — but the ledger splits across sites and a site
discriminator would need to return.

Both services connect to the same `chat` MongoDB database. The `users` collection at every
site covers the whole company (provisioned by the per-site HR feed), which is what makes a
materialized per-user snapshot the natural distribution unit.

## 3. Data model

### 3.1 Ledger collection (unchanged, audit-only)

`permission_grants` in the `chat` database. **Append-only: insert only, never update or
delete.**

```go
// pkg/model/permission.go (existing on the branch, unchanged)
type PermissionKey string

const PermissionExternalImageView PermissionKey = "external.image.view"

const MaxReasonRunes = 1000 // MaxSubjects was removed by the 2026-08-13 bulk round

type PermissionGrant struct {
    ID               string        `json:"id"                      bson:"_id"`
    Permission       PermissionKey `json:"permission"              bson:"permission"`
    SubjectAccount   string        `json:"subjectAccount"          bson:"subjectAccount"`
    Granted          bool          `json:"granted"                 bson:"granted"`
    EffectiveFrom    *time.Time    `json:"effectiveFrom,omitempty" bson:"effectiveFrom,omitempty"`
    ExpiresAt        *time.Time    `json:"expiresAt,omitempty"     bson:"expiresAt,omitempty"`
    ApplicantAccount string        `json:"applicantAccount"        bson:"applicantAccount"`
    ApproverAccount  string        `json:"approverAccount"         bson:"approverAccount"`
    Reason           string        `json:"reason"                  bson:"reason"`
    RecordedBy       string        `json:"recordedBy"              bson:"recordedBy"`
    RecordedAt       time.Time     `json:"recordedAt"              bson:"recordedAt"`
}
```

Field semantics, the pointer/`omitempty` rationale for the window fields, the
"`Granted` must never carry `omitempty`" rule, and the half-open `[from, until)` interval
semantics are unchanged from the 2026-08-11 revision. In brief: `Granted` records **what
this record did**, never rewritten by later rows; `RecordedAt` is the server clock and
doubles as the application time; window fields are absent on revoke rows; `RecordedBy` is
the operation admin. With admin-service at a single site (§2), this one collection is
the **complete company-wide change history**; it is no longer the source of the current
state. The prior revision's `SiteID` column is **dropped** — a single writer site makes
it a constant with zero discriminating power, and `RecordedBy` already identifies the
actor.

### 3.2 Materialized state (new)

```go
// pkg/model/permission.go (new)

// PermissionState is the latest admin decision for one permission key, materialized on
// the user document and replicated to every site. UpdatedAt is the write's RecordedAt
// and doubles as the per-key last-write-wins watermark.
type PermissionState struct {
    Granted       bool       `json:"granted"                 bson:"granted"`
    EffectiveFrom *time.Time `json:"effectiveFrom,omitempty" bson:"effectiveFrom,omitempty"`
    ExpiresAt     *time.Time `json:"expiresAt,omitempty"     bson:"expiresAt,omitempty"`
    UpdatedAt     time.Time  `json:"updatedAt"               bson:"updatedAt"`
}

// Evaluate reports whether the permission is effective at `now`. A nil state, a revoked
// state, or a grant with a missing bound (malformed data) evaluates to false — deny,
// never panic. Same boundary semantics as the former EvaluateGrant: now == EffectiveFrom
// is inside the window, now == ExpiresAt is outside (half-open interval).
func (s *PermissionState) Evaluate(now time.Time) bool

// UserPermissions is the admin-managed snapshot on the user document — one named field
// per known PermissionKey. Deliberately a struct, not a map: the key
// "external.image.view" contains dots, and MongoDB dot-path updates cannot address map
// keys containing dots, which would break the per-key guarded $set.
type UserPermissions struct {
    ExternalImageView *PermissionState `json:"externalImageView,omitempty" bson:"externalImageView,omitempty"`
}

// Evaluated returns the evaluated boolean for every known permission key. Nil-receiver
// safe: a user with no snapshot gets every key = false. This is what settings.get and
// the admin GET's currentlyGranted both call, so the entry points cannot disagree.
func (p *UserPermissions) Evaluated(now time.Time) map[PermissionKey]bool

// PermissionFieldName maps a PermissionKey to its bson field name inside the
// `permissions` sub-document ("external.image.view" → "externalImageView"); reports
// false for unknown keys. Used by the two guarded-update sites (admin-service store,
// inbox-worker store) to build the `permissions.<field>` dot-paths.
func PermissionFieldName(k PermissionKey) (string, bool)
```

`EvaluateGrant(*PermissionGrant, time.Time)` is **removed** — nothing evaluates ledger
rows anymore. `PermissionState.Evaluate` inherits its tests and fail-closed semantics.

```go
// pkg/model/user.go — User gains a sibling field
Permissions *UserPermissions `json:"permissions,omitempty" bson:"permissions,omitempty"`
```

### 3.3 Why a sibling field, not part of the `settings` sub-document

The cross-site settings apply is a **whole-object replace** (`$set {settings: <snapshot>}`
under a single `settingsUpdatedAt` watermark — inbox-worker, "receiver replaces, never
merges"). Any state stored inside that sub-document is only as fresh as the site that
last snapshotted it:

- **Race with zero failures:** an admin grant at site A and an unrelated user settings
  change at site B within the propagation window make B's snapshot (without the grant,
  newer watermark) overwrite A's grant everywhere, while A's event (older watermark) is
  rejected at B. The grant is lost company-wide with no error anywhere.
- **Stale-copy resurrection:** a site that missed a permission event re-broadcasts its
  stale copy inside the next settings snapshot and overwrites fresher state elsewhere.

A snapshot's watermark can say "this bundle is newer", never "every field inside is
newer". Different writers with independent "latest" therefore need **separate
last-write-wins domains**: `permissions` is a sibling of `settings` with its own per-key
watermark (`PermissionState.UpdatedAt`), exactly the way `chatlist` /
`chatlistUpdatedAt` already sit beside `settings` / `settingsUpdatedAt`.

The second, independent reason is the trust boundary: `settings.set`'s request struct
embeds `model.UserSettings`. Keeping admin-managed state out of `UserSettings` means a
user request **structurally** cannot name the field — no code-review convention required.

The client-visible contract is unaffected: `settings.get` composes both fields into one
response (§6). Storage is split; the response is merged.

### 3.4 Validity window — half-open interval (unchanged)

`EffectiveFrom` / `ExpiresAt` store instants for the half-open interval `[from, until)`.
The `+1 day` civil-date conversion at write time, the UTC+8 interpretation rule, and the
display-side reversal are unchanged (§9). The same instants are copied into
`PermissionState`, so evaluation is identical at every site — expiry and delayed
effectiveness flip **automatically at read time**; no site needs a scheduler or a
re-fanout when a window boundary passes.

### 3.5 Evaluation — write-time materialization, read-time decision

"Does alice hold the permission now?" is answered by `user.Permissions` + `Evaluate(now)`
— one document read, no ledger query. The state written by a POST is derived **entirely
from the request** (the row being written is by definition the newest fact this site
knows; cross-site races are resolved by the watermark guard, §5.2):

```go
// grant                                    // revoke
PermissionState{                            PermissionState{
    Granted:       true,                        Granted:   false,   // no window
    EffectiveFrom: &from,  // parseWindow       UpdatedAt: recordedAt,
    ExpiresAt:     &until, // parseWindow   }
    UpdatedAt:     recordedAt,
}
```

### 3.6 Ledger indexes (unchanged)

```
1. {permission:1, subjectAccount:1, recordedAt:-1, _id:-1}
2. {recordedAt:-1, _id:-1}
```

Index 1 backs the filtered ledger browse (`GET` with `subjectAccount` and/or
`permission`); index 2 backs the unfiltered newest-first browse. (Both lose the prior
revision's `siteId` prefix along with the dropped column.) The ESR reasoning, the
`_id` tie-break for same-millisecond batch rows, and the no-TTL rule are unchanged.
admin-service alone creates these indexes. No index is added for the `users.permissions`
field: every read of it is by `account` (already indexed as the user lookup key).

### 3.7 Read freshness

The dedicated primary-read permission repository from the previous revision is gone.
Permission now rides `settings.get`'s existing read path in user-service and the admin
GET's user lookup in admin-service; staleness is bounded by MongoDB replica lag on those
paths (milliseconds), which replaces the previous design's same concern. Not worth a
read-preference change to either existing path.

## 4. Admin API — admin-service (HTTP)

`POST /v1/admin/permissions` and `GET /v1/admin/permissions`, registered under the
existing `requireAdmin` group. Request schemas, the grant/revoke examples, and the
UTC+8 date handling are unchanged from the 2026-08-11 revision.

### 4.1 Write path

```
POST /v1/admin/permissions
 ① validation chain            — unchanged (§4.3)
 ② derive PermissionState      — from the request (§3.5)
 ③ withTransaction {
      InsertMany  permission_grants          (ledger, unchanged)
      UpdateMany  users                      (guarded per-key $set — same guard as §5.2)
    }
 ④ fanout                      — one batch event per remote site, after commit (§5)
 ⑤ AppendAuditMany             — unchanged, best-effort
 ⑥ 201 { created, duplicatesIgnored, grants, syncFailures? }
```

**Why ③ is one transaction:** the ledger row and the local state must not exist without
each other. Ledger-only would mean "audited but not effective at the origin site while
remote sites apply it"; state-only would mean "effective but unaudited" — the worst
failure for a feature that exists to keep records. Both collections live in the same
database; the existing `withTransaction` helper covers it. Failure has exactly one shape:
the whole request 500s and nothing happened.

**Why the origin write is also guarded:** two concurrent POSTs for the same subject can
commit in the opposite order of their timestamps (the state is stamped before the
transaction runs). Unguarded, the origin would keep the last-*committed* state while
every remote converges on the newest *stamp* via the apply guard (§5.2) — divergence at
the origin itself. The guarded `UpdateMany` (filter `updatedAt` missing-or-`$lte` this
write's `UpdatedAt`) gives every site, origin included, the identical last-write-wins
rule. The ledger row is still inserted either way: the admin *did* perform the action;
the audit does not lie. In the no-race case the guard always passes.

**Why fanout is after commit:** a publish cannot join a Mongo transaction, and publishing
before commit would advertise state that never existed if the transaction aborts. After
commit, the worst case is "origin has it, remotes get it late".

**Store shape:** the branch's `InsertPermissionGrants(ctx, grants)` becomes
`RecordPermissionChange(ctx context.Context, grants []*model.PermissionGrant,
state model.PermissionState) error` — one transaction wrapping the ledger `InsertMany`
and the guarded users `UpdateMany`; the subject accounts are derived from the grants.

### 4.2 Response — 201

```json
{
  "created": 2,
  "duplicatesIgnored": [],
  "grants": [
    {"id": "0199f2c3a4b5...", "subjectAccount": "alice"},
    {"id": "0199f2c3a4b6...", "subjectAccount": "bob"}
  ],
  "syncFailures": ["site-b"]
}
```

`syncFailures` (new, `omitempty`) lists the destination sites whose INBOX publish was not
acknowledged. The status stays **201**: the ledger and the origin-site state are already
committed — a 5xx would claim the write failed when it did not. Remediation from the
2026-08-13 round is the **resync endpoint** (bulk-round spec §5 — re-delivers the current
state, writes nothing), surfaced in the console as a warning banner with a resend button;
re-sending the request remains the fallback (§5.4).

### 4.3 Validation (unchanged) and write steps

The validation chain (JSON, known key, `subjectAccounts` **non-empty** — the fixed
200-cap and the request-body limit were removed 2026-08-13, see the bulk-round spec —
dedup-and-report, optional reason ≤ 1000 runes — omitted/empty accepted, stored as `""` —
required fields, grant-window rules, revoke-window-absent, one-query account existence +
subject `IsActive()`) is exactly the 2026-08-11 revision, including all its notes
(backdated `effectiveFrom` allowed, past `expiresAt` rejected on the derived instant,
inactive applicant/approver allowed, rejection messages name the offending accounts) —
with **one scope change**: the existence/active lookup is **site-unfiltered**
(`{account: {$in: …}}`), because subjects span the whole company (§2) and the local
`users` collection covers everyone; `FindAccountStates` loses its `siteID` parameter.
Steps 11+ become:

| # | Step | Failure |
|---|---|---|
| 11 | `withTransaction` wrapping `InsertMany` (ledger) + guarded `UpdateMany` (users) | 500 |
| 12 | Fanout to every remote site; failures → `syncFailures` (§5) | never fails the request |
| 13 | `AppendAuditMany` — one entry per subject, best-effort | logged, not returned |

### 4.4 Audit — slim dual-write (unchanged)

One `admin_audit` entry per subject, `action` `permission.grant` / `permission.revoke`,
`details` carrying only `{permission}`; written via `AppendAuditMany` (single
`InsertMany`), best-effort. Positioning unchanged: the audit entry is a strict data
subset of the ledger row; the dual-write buys the by-actor unified AuditView surface.

### 4.5 Read (`GET /v1/admin/permissions`)

Filters, paging, newest-first ordering, per-row date display (civil dates plus
`expiresAtUTC`), and the both-filters-optional design are unchanged. One change:

**`currentlyGranted` now reads the materialized state, not the ledger.** The
materialized state is the authority every other reader uses — `settings.get` (§6) and
the future enforcement hook (§13) both go through `Evaluated` — so the admin GET joins
the same single decision path instead of keeping a second, ledger-derived one. (It also
stays correct if admin writes ever happen at more than one site.) When both `permission`
and `subjectAccount` are supplied, the handler fetches the subject's `users.permissions`
(store method `GetUserPermissions(ctx, account)`, projection `{permissions: 1}`, filter
`{account}` — subjects may be homed at any site) and returns
`Evaluated(now)[permission]`. `GetLatestPermissionGrant` is removed.

## 5. Cross-site propagation (new)

### 5.1 Event

```go
// pkg/model/event.go
const InboxUserPermissionsUpdated InboxEventType = "user_permissions_updated"

// UserPermissionsUpdated is the cross-site inbox event admin-service publishes after a
// permission grant/revoke batch. One event carries the whole batch: every account in
// Accounts receives the same State. Receivers apply it under the per-key watermark
// guard (State.UpdatedAt), so delivery may be duplicated or reordered safely.
type UserPermissionsUpdated struct {
    Permission PermissionKey   `json:"permission" bson:"permission"`
    Accounts   []string        `json:"accounts"   bson:"accounts"`   // ≤ 1,000 per event (chunked 2026-08-13; retuned 2026-08-17)
    State      PermissionState `json:"state"      bson:"state"`
    Timestamp  int64           `json:"timestamp"  bson:"timestamp"`  // event publish time
}
```

One POST → one event per remote site **per chunk of ≤1,000 accounts** (2026-08-13: the
batch is chunked to stay under the brokers' `max_payload`; 2026-08-17: retuned from 5,000
because our brokers run 128KB, not NATS's 1MB default, and the `InboxEvent` envelope
base64-encodes the payload; each site has its own INBOX stream), published to
`subject.InboxExternal(dest, model.InboxUserPermissionsUpdated)` =
`chat.inbox.{dest}.external.user_permissions_updated`, wrapped in the standard
`InboxEvent` envelope, skipping self, blank, and repeated entries of `ALL_SITE_IDS`. The
payload is marshaled once; each destination publishes every chunk in its own goroutine
lane under one shared budget: `FANOUT_TIMEOUT` capped by the request's absolute deadline (handler entry + 38s, i.e. the 40s HTTP write timeout minus a response margin), so local work that already spent part of the write window can't let the fanout outlive the connection (2026-08-18). No `Nats-Msg-Id` dedup — the guarded apply is idempotent, the same
rationale user-service's publisher documents for status/settings.

**Success means the destination stream acknowledged the publish** — the event is in that
site's mailbox and JetStream's at-least-once delivery to inbox-worker takes over. It does
*not* mean "applied"; nothing tracks end-to-end application, and nothing needs to.

### 5.2 Apply — inbox-worker

New switch case → handler → guarded store method:

```go
// InboxStore
ApplyUserPermissions(ctx context.Context, permission model.PermissionKey,
    accounts []string, state model.PermissionState) error
```

One `UpdateMany` (the batch shares one state and one guard condition):

```js
filter: {
  account: {$in: accounts},
  $or: [
    {"permissions.externalImageView.updatedAt": {$exists: false}},
    {"permissions.externalImageView.updatedAt": {$lte: state.updatedAt}},
  ]
}
update: {$set: {"permissions.externalImageView": state}}
```

(The `externalImageView` path comes from `model.PermissionFieldName`.)

- **`$lte`, not `$lt`** — same rationale as the settings guard: two writes can share a
  millisecond, and dropping the second would leave a site permanently behind; the apply
  is an idempotent whole-state replace, so a same-ms tie resolves to last-delivered.
- **Per-key watermark** (inside the state, not a top-level `permissionsUpdatedAt`): a
  future second permission key must not be blocked or regressed by events for this one.
- **No upsert.** A missing user doc is a silent no-op, matching every existing user event
  (status/settings/chatlist); user docs are provisioned everywhere by the HR feed.
- **Unknown permission key** (a future key reaching a not-yet-upgraded site):
  `slog.Warn` + return nil (Ack). Retrying cannot succeed and must not poison the
  consumer; the state converges when the site is upgraded and the change is re-sent.
- `MatchedCount < len(accounts)` is normal (missing users, newer local state) and is not
  an error.

admin-service's origin-side `UpdateMany` (§4.1 step ③) uses the identical filter/update
shape so every site applies one rule.

### 5.3 Failure handling — the decision

Chosen: **direct publish + report** (over the OUTBOX durable relay; user decision
2026-08-12). Per destination, a failed publish is `slog.Error`-logged and the destination
is appended to `syncFailures`; the loop continues to the remaining sites (one dead peer
must not block the others); the request still returns 201 (§4.2).

Why this differs from `publishStatus`'s swallow-and-continue: status is self-healing —
users change it constantly, and the next change re-broadcasts a full snapshot, so a lost
event's damage is bounded by hours. A permission may not change again for a year, and a
lost revoke is a standing security gap — so the error goes to the one party who can act,
instead of a Warn log nobody watches.

**Upgrade path, recorded:** the event payload and apply are transport-agnostic. If
unattended durability is later required, switching to the OUTBOX relay is a
producer-side-only change (publish `OutboxEvent` instead of direct INBOX; add the type to
`pkg/outbox.ConcurrentEventTypes`; leave `OutboxEvent.RoomID` empty) — no API, model, or
inbox-worker changes.

### 5.4 Remediation — resync first, re-send as fallback

**From the 2026-08-13 round, the primary remediation is the resync endpoint** (bulk-round
spec §5): it re-fans-out the current materialized state and writes nothing — no duplicate
rows, idempotent, safe to repeat. Re-POSTing the identical body remains the fallback when
no dialog is open (e.g. after the crash window below). The re-POST's three effects are
safe by construction:

1. **Ledger**: new rows — duplicate audit entries, append-only noise, harmless (accepted
   with the direct-publish decision).
2. **Origin state**: the guarded `UpdateMany` re-applies the same logical state with a
   newer watermark — idempotent.
3. **Fanout**: re-published to *all* sites; sites that already applied it no-op via the
   guard.

The residual crash window — process dies between commit and fanout — surfaces to the
admin as a failed HTTP request, whose natural response is the (safe) re-send. The truly
silent gap is an admin who sees a timeout and assumes success (§17).

## 6. User query — `settings.get` (user-service)

The dedicated `permission.get` RPC and its whole stack are **removed** (it never merged
and has no caller): `service/permission.go`, `models/permission.go`,
`mongorepo/permissions.go`, the `PermissionRepository` interface + mock + `service.New`
parameter + `main.go` wiring, the `subject.UserPermissionGet`/`…Pattern` builders, the
`"permission"` area in `ParseUserSubject`, the registration line, and both client-api doc
sections.

Instead, `settings.get` (`chat.user.{account}.request.user.{siteID}.settings.get`)
returns:

```json
{
  "fullWidth": true,
  "themePreference": "dark",
  "...": "…(the existing ten fields, unchanged)…",
  "permissions": { "external.image.view": true }
}
```

```go
// user-service/models/settings.go
type SettingsGetResponse struct {
    model.UserSettings                                       // embedded — fields promoted
    Permissions map[model.PermissionKey]bool `json:"permissions"`
}
```

- One Mongo read: `GetUserSettings` becomes `GetUserSettingsAndPermissions(ctx, account)
  (*model.UserSettings, *model.UserPermissions, error)` — same query, projection extended
  to `{settings: 1, permissions: 1}`. The `settings.set` path keeps its existing
  repository method untouched.
- `Permissions` is **always present** and contains **every known key**, evaluated at read
  time via `UserPermissions.Evaluated(now)` — a user with no snapshot gets
  `{"external.image.view": false}`. Read-time evaluation is what makes windows flip
  automatically (§3.4).
- Booleans only — no dates, no audit fields. Same exposure-surface reasoning as the
  original design: the requirement is a yes/no.
- **`settings.set`'s response and the `settings.update` client event are deliberately
  unchanged** (pure `UserSettings`). The field is admin-managed and read-only through
  this surface; a user cannot touch it because it is not part of `UserSettings` at all
  (§3.3).

## 7. Admin console — admin-frontend

**Superseded 2026-08-13:** the deferred console round is specified in
`2026-08-13-permission-bulk-frontend-design.md`. Original 2026-08-12 note follows.

**Unchanged in this round** (user decision 2026-08-12). The already-built PermissionsView
(list-first page, filters, `currentlyGranted` badge, create/revoke dialog with
AccountPicker, result view) ships as-is:

- The wire contracts it uses are backward-compatible: `syncFailures` is additive and
  ignored; `currentlyGranted` keeps its name, type, and both-filters-only presence — only
  its server-side source changes (§4.5).
- Deferred to the next round: surfacing `syncFailures` (warning banner + guidance to
  re-send) and any batch-oriented UX changes that come with the next requirements.

## 8. Authentication

| | Transport | Identity source | Caller supplies |
|---|---|---|---|
| Admin write | HTTP POST | `Authorization: Bearer <session token>` → `requireAdmin` | Session token |
| Fanout publish | NATS JetStream | admin-service's own service credentials | — (server-side) |
| User query | NATS `settings.get` | The `{account}` token in the subject, enforced by the NATS scoped signing key | **Nothing** |

The key property survives the move into `settings.get`: **a user cannot query another
user's permission because they cannot publish another user's subject.** As before, that
guarantee is the ops-owned NATS signing-key template (`chat.user.<account>.>`), an
external contract this design depends on.

## 9. Dates and timezones (unchanged)

The fixed UTC+8 interpretation rule (`tzTaipei = time.FixedZone("UTC+8", 8*60*60)`), the
write-time-only conversion, the half-open-interval reasoning, and the "revocation
involves no dates" analysis all stand unchanged. The API contract also still takes
**calendar dates (`YYYY-MM-DD`), not RFC 3339 instants**, for the unchanged reasons: an
instant contract would push the half-open `+1 day` conversion onto a human typing curl
(silently losing a day), admit meaningless time-of-day precision, and hurt report
readability; RFC 3339 remains output-only (`expiresAtUTC`). One amendment: user-service
now *evaluates* window instants at read
time (`PermissionState.Evaluate`), but evaluation is instant-against-instant comparison,
which §9's own analysis shows is timezone-free — user-service still performs no civil
date or timezone handling.

## 10. Error codes (unchanged)

The eight `Permission*` reasons in `pkg/errcode/codes_permission.go` stand as-is; all are
emitted by the admin endpoints only (the RPC that used `unknown_permission` is gone, and
`settings.get` takes no input to validate). No new reason: a fanout failure is not an
error response (§4.2). Every reason remains registered in `allReasons`.

## 11. Testing

TDD red-green-refactor throughout. Coverage ≥80%, targeting 90%+ on evaluation, guard,
and fanout logic.

| File | Focus |
|---|---|
| `pkg/model/permission_test.go` | `PermissionState.Evaluate` table tests (boundaries: `now == effectiveFrom` → true, `now == expiresAt` → false; nil receiver; nil bounds on a granted state → deny; revoked). `Evaluated`: nil receiver → all known keys false; populated → per-key result. `PermissionFieldName` known/unknown. (Replaces the `EvaluateGrant` tests.) |
| `pkg/model/model_test.go` | Round-trips for `PermissionState`, `UserPermissions`, `UserPermissionsUpdated`; `PermissionGrant` round-trip stays. |
| `admin-service/permissions_test.go` | Validation tests unchanged. New: state derivation (grant window / revoke no-window); fanout — one captured publish per remote site with correct subject and payload, self/blank skipped; publish failure → destination in `syncFailures`, remaining sites still attempted, response 201; transaction failure → no publish, 500; `currentlyGranted` sourced from `GetUserPermissions` + `Evaluated`. |
| `inbox-worker/handler_test.go` | New event: unmarshal + store call with decoded fields; malformed payload → error; unknown permission key → nil (Ack) + no store call. |
| inbox-worker integration | Guarded `UpdateMany`: older watermark rejected, equal applied ($lte), newer applied, missing account no-op, per-key independence (a second key's event is not blocked by the first key's newer watermark). |
| `user-service/service/settings_test.go` | `settings.get` merge: no snapshot → all-false map; granted-in-window → true; expired / not-yet-effective / revoked → false; settings fields unaffected. Removal of the permission RPC tests. |
| admin-service integration | Transaction all-or-nothing across ledger + users; origin guard rejects an older write. |

**Mocks:** admin-service regenerates via its `-source=store.go` directive; user-service's
`//go:generate mockgen` list **drops** `PermissionRepository`; inbox-worker's store mock
gains `ApplyUserPermissions`. Run `make generate` after interface changes.

## 12. Metabase

The ledger keeps the required columns (`subjectAccount`, `granted`, `effectiveFrom`,
`expiresAt`, `applicantAccount`, `approverAccount`, `reason`) and stays directly
readable; date display semantics are unchanged (exclusive-end instants, readers convert).

**One admin site ⇒ one complete ledger.** Every permission write company-wide lands in
the admin site's `permission_grants`, so BI reads a single collection with no cross-site
merging. The current-holders view still reads the **`users` collection** — it shares the
API's evaluation semantics instead of re-deriving latest-wins from history:

```js
[
  {"$match": {"$expr": {"$and": [
    {"$eq":  ["$permissions.externalImageView.granted", true]},
    {"$lte": ["$permissions.externalImageView.effectiveFrom", "$$NOW"]},
    {"$gt":  ["$permissions.externalImageView.expiresAt", "$$NOW"]}
  ]}}},
  {"$project": {"account": 1, "permissions.externalImageView": 1}}
]
```

(`$$NOW` requires the `$expr`/aggregation form; the three conditions must be used as a
whole — missing window fields on a never-granted user compare as `null` and are excluded
by the `$gt`/`$eq` pair, same caveat as the prior revision's ledger aggregation.) The
ledger remains the change-history / audit table it was designed to be.

## 13. Future: server-side enforcement (not built)

`chat.server.request.permission.{siteID}.check` remains the hook shape. It would read the
subject's `users.permissions` and call `Evaluated` — the same pure decision path as
`settings.get` and `currentlyGranted`, so the entry points cannot disagree. Because the
state is materialized at every site, the future check is one local indexed read; no
cross-site call.

## 14. Supporting changes

- **`docker-local/compose.deps.yaml`** — single-node replica set (unchanged from the prior
  revision; the write transaction still requires it), with the
  `?directConnection=true` host-side URI adjustments in the seed tool and jaeger README.
- **`admin-service/main.go` / `middleware.go`** — the 1MB `http.MaxBytesReader` body
  limit added earlier on this branch is **removed again** (user decision 2026-08-13;
  risk recorded in the bulk-round spec §8.1).
- **admin-service NATS wiring (new):**
  - `main.go`: `js, err := nc.JetStream()` — the connection already exists (room on-duty
    RPC); this adds the JetStream context.
  - Config gains `AllSiteIDs []string` (`env:"ALL_SITE_IDS" envSeparator:","`,
    `envDefault:""` — empty means no fanout, which is correct for single-site dev).
  - The handler holds an injected `publishInbox func(ctx context.Context, subj string,
    data []byte) error` field so unit tests capture publishes without NATS (the
    repo-wide JetStream-testing convention).
  - admin-service creates no streams: each site's INBOX belongs to inbox-worker; the
    supercluster routing that makes a remote publish land is ops/IaC, as everywhere else.
- **Deployment note (ops):** admin-service's NATS service account needs publish
  permission on `chat.inbox.*.external.>` — the grant user-service already holds.

## 15. Documentation

| File | Change |
|---|---|
| `docs/client-api.md` settings section | `settings.get` response: add the `permissions` row (`map<permission key, boolean>`, admin-managed, read-only) + updated JSON example; note that `settings.set` cannot touch it. |
| `docs/client-api.md` §3.4 `permission.get` | **Removed** (RPC deleted), including its TOC entry and subject-table row. |
| `docs/client-api.md` §9.13 | Response gains `syncFailures` (field table + semantics: sites whose INBOX publish failed; remediation = re-send). |
| `docs/client-api.md` §9.14 | `currentlyGranted` prose: sourced from the materialized state (same evaluation path as `settings.get`). |
| `docs/client-api.md` §6 | Reason catalog: the eight permission reasons stay; drop the `permission.get` emitter references. |
| `docs/client-api/request-reply.md` | Mirror all of the above (derived view, same PR). |
| `docs/client-api/events.md` | **Unchanged** — `user_permissions_updated` is site→site (INBOX), not a server→client event; the `settings.update` client event is untouched. |

## 16. Delta from the implemented branch

The feature branch (`claude/user-whitelist-permission-api-5942da`, 28 commits, unmerged)
already implements the 2026-08-11 revision. Disposition under this amendment:

**Keep as-is**
- `history-service` `reflect.Pointer` chore; docker-local replica set + host URI fixes;
  admin-service body-limit middleware.
- `pkg/errcode/codes_permission.go` (all eight reasons).
- `pkg/model`: `PermissionKey`, `KnownPermission`, `MaxReasonRunes`, `PermissionGrant`
  (`MaxSubjects` is removed by the 2026-08-13 round).
- admin-service: the whole validation chain (minus the site-scope change below),
  `InsertPermissionGrants`'s transaction+`InsertMany` core (absorbed into the new
  transactional method), `AppendAuditMany`, `listPermissions` paging/date display.
- admin-frontend: everything (PermissionsPage/Table/Dialog/AccountPicker, api client,
  reason copy) — frozen this round.

**Adjust**
- `pkg/model/permission.go`: + `PermissionState` (+`Evaluate`), + `UserPermissions`
  (+`Evaluated`), + `PermissionFieldName`; − `EvaluateGrant`; − `PermissionGrant.SiteID`.
- `pkg/model/user.go`: + `Permissions` sibling field.
- `pkg/model/event.go`: + `InboxUserPermissionsUpdated` + `UserPermissionsUpdated`.
- admin-service `permissions.go`: write path gains state derivation, the users
  `UpdateMany` inside the transaction, post-commit fanout, `syncFailures` in the
  response; `currentlyGranted` re-sourced.
- admin-service `store.go`/`store_mongo.go`: transactional method extended to apply
  state; + `GetUserPermissions`; − `GetLatestPermissionGrant`; `FindAccountStates` and
  `ListPermissionGrants` drop their site filter/parameter (company-wide subjects); both
  ledger indexes drop the `siteId` prefix.
- admin-service `main.go`/config: JetStream context, `ALL_SITE_IDS`, publish-func
  injection.
- user-service `service/settings.go` + `models/settings.go` + `mongorepo/users.go`:
  `SettingsGetResponse` wrapper, extended projection, read-time evaluation.
- inbox-worker `handler.go`/`main.go`: new case + handler + guarded `ApplyUserPermissions`.
- Docs per §15.

**Remove**
- user-service `service/permission.go` (+test), `models/permission.go` (+test),
  `mongorepo/permissions.go` (+test), `PermissionRepository` (interface, mock,
  `service.New` param, `main.go` wiring, registration line).
- `pkg/subject`: `UserPermissionGet`/`UserPermissionGetPattern`, the `"permission"`
  `ParseUserSubject` area (+tests).
- `docs/client-api.md` / `request-reply.md`: the `permission.get` sections.

**Deferred (next round)**
- admin-frontend `syncFailures` surfacing and any batch-UX changes.

## 17. Residual risks

1. **A lost response followed by a form re-submit duplicates ledger rows.** Confined to
   the fallback path from the 2026-08-13 round on — the console's resend is the resync
   endpoint, which writes nothing (§5.4). Duplicates from a re-submit remain harmless
   append-only noise; state converges via the watermark guard.
2. **Silent fanout gap.** Direct publish means a crash between commit and fanout, or an
   admin who ignores a timeout / `syncFailures` and never re-sends, leaves remote sites
   stale until the next change to the same subjects. Accepted with the direct-publish
   decision; the OUTBOX upgrade path is recorded in §5.3.
3. **Console blindness this round.** The current admin-frontend ignores `syncFailures`;
   only curl callers and server logs see sync failures until the next-round UI lands.
   *(Addressed by the 2026-08-13 round: `syncFailures` banner + resend button.)*
4. **Clock skew across admin-service replicas.** `UpdatedAt` comes from the writing
   replica's clock; ordering between near-simultaneous writes is only as correct as NTP
   among the admin site's replicas. A same-millisecond tie under the `$lte` guard
   resolves to last-delivered, which can differ per receiving site — same accepted class
   as the settings lane.
5. **Advisory, not enforced.** Unchanged; the enforcement hook is §13.
6. **The fixed UTC+8 offset carries no DST rules.** Unchanged.
7. **An approver without a chat account cannot be recorded.** Unchanged.
8. **`reason` is permanently retained free text.** Unchanged.
9. **The NATS subject scope and the supercluster routing are ops-owned external
   contracts** — the user-identity guarantee (§8) and the fanout's deliverability (§14)
   both live outside this repository.
10. **A user doc missing at a remote site drops the apply** (silent no-op, no upsert). In
    practice the HR feed provisions users everywhere before an admin would grant to them;
    a re-send heals the exotic case.
11. **No client push.** A client sees a permission change on its next `settings.get`;
    an in-session revoke does not reach an already-loaded client until then.
12. **Subject `IsActive()` can be stale for users homed at another site.** `active` does
    not federate (a known, separate gap); the validation runs against the admin site's
    local copy — the best signal available. Tracked as separate work.
