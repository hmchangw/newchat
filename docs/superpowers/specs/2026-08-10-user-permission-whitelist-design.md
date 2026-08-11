# User Permission Whitelist — Design

**Status:** Approved, ready for planning
**Date:** 2026-08-10, amended 2026-08-11 (fixed UTC+8 interpretation, admin-frontend in scope, slim audit dual-write, Metabase column contract)

## 1. Overview

A whitelist permission recording whether a user may **view images from outside the
corporate network**. Applications and approvals happen offline (mail / form / sign-off);
an operation admin records the already-approved decision into the system.

Every change is an independent record — the collection is an append-only ledger, which
is also the audit report a BI tool reads.

### 1.1 Goals

- Operation admins grant and revoke the permission for one or many users, via the HTTP
  API (terminal/curl) **and an admin-frontend console view**.
- A user can query whether **they themselves** hold the permission.
- Every change is durably recorded with applicant, recording time, validity window,
  reason, and approver.
- Permission changes also appear in the existing `admin_audit` trail (slim entries), so
  the Audit console keeps its "everything an admin did" view.
- The ledger is directly readable by a BI tool (Metabase) with no extra pipeline.

### 1.2 Non-goals

- **No enforcement.** media-service and the gateway are not changed. The answer is
  advisory; the hook for future server-side enforcement is specified in §12 but not built.
- **No in-system approval workflow.** There is no `pending` state — a write takes effect
  immediately.
- **No chat-frontend work.** The NATS RPC ships with no caller; client integration is
  planned separately. (admin-frontend IS in scope — see §5.)
- **No Metabase integration.** Only the properties that the collection is directly
  readable and the required columns exist are preserved (§11).
- **No cross-site federation.** A grant at one site does not propagate to another.

## 2. Architecture

No new service. The write path and the read path have fundamentally different identity
models, so each lands where its identity already works.

| Path | Service | Transport | Reason |
|---|---|---|---|
| Admin write | `admin-service` | HTTP (Gin) | Already has session-token auth, `requireAdmin`, an audit trail, and the admin-frontend console. |
| User query | `user-service` | NATS request/reply | The account in the subject **is** the authenticated identity; no token in the payload. |

**Why admin cannot use NATS.** `pkg/principal/scope.go` documents that admins receive the
same scoped signing-key template as an ordinary SSO user (`chat.user.<account>.>`). An
admin therefore has no cross-account publish rights on NATS. Moving the write path to
NATS would first require changing the NATS signing-key template, which is owned by
ops/IaC.

**Why users cannot use HTTP.** An ordinary user holds only a NATS JWT/NKey. HTTP session
tokens exist for admins and bots. An HTTP query endpoint would require inventing a new
authentication mechanism for ordinary users.

Both services already connect to the same `chat` MongoDB database (`MONGO_DB` defaults to
`chat` in both). Sharing a collection between a writer service and a reader service has
precedent: `users` is written by admin-service and read by user-service.

## 3. Data model

### 3.1 Collection

`permission_grants` in the `chat` database. **Append-only: insert only, never update or
delete.**

```go
// pkg/model/permission.go
type PermissionKey string

const PermissionExternalImageView PermissionKey = "external.image.view"

const (
    MaxReasonRunes = 1000
    MaxSubjects    = 200
)

type PermissionGrant struct {
    ID               string        `json:"id"                      bson:"_id"`
    SiteID           string        `json:"siteId"                  bson:"siteId"`
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

`_id` is `idgen.GenerateUUIDv7()` — 32-char hex, time-ordered for B-tree locality, matching
the convention for high-write collections.

There is deliberately **no `Timezone` field**: the date-interpretation rule is fixed at
UTC+8 (§8) and lives in this spec; repeating the same string on every row would be
redundant.

### 3.2 Field semantics

- `SubjectAccount` — the user being granted the permission; the lookup key.
- `Granted` — **what this record did**, not whether the user currently holds the
  permission. Historical rows are never modified: revoking does not flip an earlier
  row's `granted` to `false`. The value of an append-only ledger is that what happened
  cannot be rewritten — otherwise "did alice hold this in June?" becomes unanswerable,
  and that is exactly the question a security investigation asks.
- `RecordedAt` — server clock. Because a write takes effect immediately, the recording
  action *is* the application event; this column is labelled "application time" in the
  report.
- `RecordedBy` — the operation admin who recorded the change.
- `SiteID` — each site runs its own MongoDB, so in practice this is constant. It is kept
  for lookup-key correctness, to bound an admin's blast radius (`requireAdmin` already
  rejects a session whose `siteId` differs), and as the discriminator if reporting is ever
  centralized across sites.

### 3.3 Optional fields

`EffectiveFrom` and `ExpiresAt` are absent on revoke rows — a revocation has no validity
window. They are pointers/`omitempty` for two reasons:

1. **A value-typed `time.Time` would store `0001-01-01`.** Append-only means that garbage
   would be permanent.
2. **`encoding/json`'s `omitempty` does not omit zero structs.** It applies only to
   `false`, `0`, nil pointers, nil interfaces, and empty arrays/slices/maps/strings — a
   struct is never "empty". A value-typed `time.Time` with `json:",omitempty"` still
   marshals as `"0001-01-01T00:00:00Z"`. Only a pointer works on the JSON side, and the
   admin GET returns these rows as JSON.

Every optional time in `pkg/model` is already a pointer (`subscription.go` has six), so
this matches the house pattern.

**`Granted` must never carry `omitempty`.** A `bool`'s zero value is `false`, so
`granted,omitempty` would drop the field entirely from every revoke row. Go would still
read back `false`, but in the BI report the column would be *missing* rather than `false`,
making "how many revocations this month" unanswerable.

### 3.4 Validity window — half-open interval

`EffectiveFrom` and `ExpiresAt` store instants and represent the half-open interval
**`[from, until)`**:

```
API in    "expiresAt": "2026-12-31"
Stored     2026-12-31T16:00:00Z          (= 2027-01-01 00:00 UTC+8)
API out   "expiresAt": "2026-12-31"      (admin GET)
```

The `+1 day` conversion happens once on write in admin-service and is reversed once on
read. **An admin never sees `2027-01-01`** (the Metabase raw view is the one exception —
§11).

A closed interval storing `23:59:59` drops the last second — `23:59:59.500` falls outside.
Patching it to `.999` shrinks the hole to under a millisecond but depends on the storage
precision happening to be milliseconds: Go's `time.Time` is nanosecond precision and
PostgreSQL is microsecond, so the hole reopens in both. The half-open form has no magic
number and no precision assumption — "December 2026" means `[Dec 1 00:00, Jan 1 00:00)`
for the same reason.

The `+1 day` uses civil date arithmetic — `time.Date(y, m, d+1, 0, 0, 0, 0, tz)`. With a
fixed offset this coincides with `Add(24 * time.Hour)`, but the civil form stays correct
if the interpretation rule ever changes, so it is the required spelling.

### 3.5 Evaluation — latest-wins

The current permission is computed, not stored: take the newest record for
`(siteId, permission, subjectAccount)` and evaluate it.

```go
func EvaluateGrant(latest *PermissionGrant, now time.Time) bool {
    if latest == nil || !latest.Granted {
        return false
    }
    // A grant row should always carry both bounds. If the data is malformed
    // (manual DB edit, an older bug), deny — never panic, never allow.
    if latest.EffectiveFrom == nil || latest.ExpiresAt == nil {
        return false
    }
    return !now.Before(*latest.EffectiveFrom) && now.Before(*latest.ExpiresAt)
}
```

The nil guard is a security decision, not defensive padding: malformed data must fail
closed. It requires its own test.

This is a pure function so that the future server-side enforcement hook (§12) and the
current user query **cannot** produce different answers.

### 3.6 Indexes

```
1. {siteId:1, permission:1, subjectAccount:1, recordedAt:-1, _id:-1}
2. {siteId:1, recordedAt:-1}
```

Index 1 follows the ESR rule (Equality → Sort → Range). The three equality fields come
first — their order among themselves does not affect usability — and `recordedAt:-1,
_id:-1` last means matching entries are already in newest-first order, so **the sort is
free**: no in-memory sort, no 32MB sort-memory ceiling. Without the sort key in the index,
MongoDB would fetch every historical row for that user and permission and sort them in
memory, which scales with a history that by design grows forever.

**`permission` precedes `subjectAccount` deliberately.** The BI aggregation (§11) sorts by
`{subjectAccount, recordedAt, _id}`. With `permission` between the two sort keys that is
not a contiguous suffix of the index and the aggregation degrades to an in-memory sort;
with this ordering the `{siteId, permission}` equality prefix leaves the three sort keys
contiguous.

The `_id` tie-break exists for batch writes: every row in one `InsertMany` shares a
`recordedAt`, so `recordedAt` alone gives no deterministic order. Sort direction must match
the index or index-provided ordering is lost.

Index 2 backs the audit/BI browse ("all changes, newest first, optionally by date range"),
which has no `subjectAccount` equality and so cannot use index 1.

Index 1 is deliberately **not** covering — the projected fields are not in the index, so one
small document fetch remains. Adding them would enable an index-only read at the cost of a
fatter index; not worth it at the expected volume.

**No TTL index.** Expired grants are precisely what the audit needs to retain.

**admin-service alone creates these indexes.** If both services called `EnsureIndexes` and
the specs differed at all, MongoDB returns `IndexKeySpecsConflict` — and both services exit
on `EnsureIndexes` failure, which means a deployment-time crash loop.

### 3.7 Read preference

The user-query repository does **not** opt into `WithReadPreference`, so it inherits the
client default (primary). `user-service` defaults `READ_PREFERENCE` to
`secondaryPreferred`, but repositories opt in individually — `threadSubRepo` and
`ssoTokenRepo` already stay on primary.

Both staleness directions matter here. Reading a stale "no permission" right after a grant
produces a support ticket that "fixes itself" on retry — confusing and expensive. Reading a
stale `granted: true` after a revocation keeps the permission alive for seconds, which is
exactly the property to avoid once this becomes a real enforcement gate. The cost is
negligible: one indexed `findOne` returning a small document, nothing like the subscription
aggregations that motivated secondary reads.

## 4. Admin API — admin-service (HTTP)

`POST /v1/admin/permissions`, registered under the existing `requireAdmin` group. Because
subjects are an array, this does not use the `/users/:account/...` path-parameter shape of
its siblings.

### 4.1 Grant

```json
{
  "permission":        "external.image.view",
  "subjectAccounts":   ["alice", "bob"],
  "granted":           true,
  "effectiveFrom":     "2026-09-01",
  "expiresAt":         "2026-12-31",
  "applicantAccount":  "carol",
  "approverAccount":   "dave",
  "reason":            "On-call staff must review production line photos from outside the fab."
}
```

- `effectiveFrom` is optional — omitted means effective immediately (`recordedAt`).
- `expiresAt` is **required**: there are no permanent grants.
- There is **no `timezone` field**: dates are interpreted under the fixed UTC+8 rule (§8).

### 4.2 Revoke

```json
{
  "permission":        "external.image.view",
  "subjectAccounts":   ["alice"],
  "granted":           false,
  "applicantAccount":  "carol",
  "approverAccount":   "dave",
  "reason":            "Project ended."
}
```

`effectiveFrom` and `expiresAt` **must be absent** — they are meaningless for a revocation
and storing them would mislead the report. `applicantAccount` and `approverAccount` remain
required: a revocation also needs to record who asked and who approved. `reason` is
optional (§4.4 step 6) but still worth recording — why the permission was pulled.

### 4.3 Response — 201

```json
{
  "created": 2,
  "duplicatesIgnored": [],
  "grants": [
    {"id": "0199f2c3a4b5...", "subjectAccount": "alice"},
    {"id": "0199f2c3a4b6...", "subjectAccount": "bob"}
  ]
}
```

### 4.4 Validation

Ordered cheapest-first so the database round trip runs last.

| # | Check | Failure |
|---|---|---|
| 1 | Body ≤ 1MB (`http.MaxBytesReader` middleware) | 400 |
| 2 | Body parses as JSON | 400 `missing_fields` (existing `errcode.AuthMissingFields`, matching `updateUser` and §9's common error table) |
| 3 | `permission` is a known key | 400 `unknown_permission` |
| 4 | `subjectAccounts` non-empty and ≤ `MaxSubjects` | 400 `invalid_subject_count` |
| 5 | Deduplicate `subjectAccounts`, recording what was dropped | — |
| 6 | `reason` ≤ `MaxReasonRunes` when present (`utf8.RuneCountInString`); omitted/empty accepted, stored as `""` | 400 `invalid_reason` |
| 7 | `granted` present (a JSON boolean; omitted or `null` is rejected), `applicantAccount` and `approverAccount` non-empty | 400 `missing_permission_fields` |
| 8 | Grant: dates are `YYYY-MM-DD`, `effectiveFrom <= expiresAt`, `expiresAt` not in the past | 400 `invalid_permission_window` |
| 9 | Revoke: window fields absent | 400 `unexpected_permission_window` |
| 10 | All accounts exist at this site; subjects additionally pass `IsActive()` | 404 `unknown_accounts` / 400 `inactive_subject` |
| 11 | `withTransaction` wrapping `InsertMany` | 500 |
| 12 | `AppendAuditMany` — one entry per subject | — |

Step 10 issues **one** query —
`find({siteId, account: {$in: [...]}}, projection: {account: 1, active: 1})` — then diffs,
rather than N lookups.

Notes on specific rules:

- **Account length is not bounded.** Existence is verified against the database, so a
  length check's only effect would be to reject a legitimately long account someday. Input
  hygiene belongs at the body-size layer, which is where it now lives.
- **Deduplicate and report, never silently.** Rejecting duplicates outright is hostile to
  pasting a list from a spreadsheet; deduplicating silently hides a caller bug and makes
  "created 18" for a 20-account request inexplicable. `duplicatesIgnored` gives both.
- **Backdated `effectiveFrom` is allowed** — an admin transcribes an offline approval whose
  date may precede the recording. **A past `expiresAt` is rejected**: it is almost certainly
  a typo, and a dead-on-arrival grant serves no purpose. "Past" is evaluated on the derived
  instant, not the calendar date: `expiresAt` equal to **today is valid** — its until-instant
  is tomorrow 00:00 local, still ahead of now, so the grant expires tonight. Reject only when
  the until-instant is `<= now`.
- **A deactivated account cannot be a subject** (granting to a disabled account is
  meaningless) but **may be an applicant or approver** — leaving after applying or
  approving is normal.
- **Rejection messages name the offending accounts** (`unknown_accounts`,
  `inactive_subject`). A curl caller has no frontend validation; "invalid request" is not
  actionable. These are identifiers the caller supplied, so nothing is leaked.
- **Transaction.** `InsertMany` has no cross-document atomicity on its own; wrapping it in
  the existing `withTransaction` makes the batch all-or-nothing, so a resend after a
  partial failure cannot produce duplicate rows.

### 4.5 Audit — slim dual-write

One `admin_audit` entry per subject, `action` of `permission.grant` or
`permission.revoke`:

```json
{
  "actorAccount":  "p_admin_wang",
  "action":        "permission.grant",
  "targetAccount": "alice",
  "details":       { "permission": "external.image.view" }
}
```

**Positioning, stated plainly:** every piece of information in the audit entry (who, what,
to whom, when, which site) also exists in the ledger, and the ledger's extra fields
(window, applicant, approver, reason) exist only there — at the data level the audit entry
is a **strict subset** of the ledger row. What the dual-write buys is not data but the
**by-actor unified query surface**: AuditView is the only place that lists every admin
action across all admin features, filterable by actor and `targetAccount`. A security
review asking "what did this compromised admin account do?" gets one place to look.
`details` carries only `{permission}` (disambiguation once more keys exist); everything
else lives in the ledger alone.

This matches the established pattern: all five existing mutating handlers write their
domain collection **and** append an audit entry, and every existing entry targets a single
account. A populated `targetAccount` keeps AuditView's filters — `targetAccount` being the
only indexed one — working.

`AppendAudit` inserts one document at a time, so a 200-subject batch would cost 200 round
trips. Add `AppendAuditMany` backed by `InsertMany` — one round trip.

Audit writes remain best-effort (a failure is logged, not returned), matching the existing
contract. This is precisely why `permission_grants` — written transactionally — is the
authoritative record and `admin_audit` is a convenience index over admin activity.

### 4.6 Read

```
GET /v1/admin/permissions?subjectAccount=alice&permission=external.image.view&page=1&limit=20
```

`subjectAccount` and `permission` are both optional, independently combinable filters;
paging reuses `parsePaging` (default 20, max 100):

- **neither** — every row for the site, newest first (the `{siteId, recordedAt:-1}` index
  exists for exactly this).
- **`subjectAccount` only** — one subject's full ledger.
- **`permission` only** — one permission key across every subject.
- **both** — one subject's ledger for one permission key.

Returns the matched rows newest-first. The top-level **`currentlyGranted`** field is
present **only when both filters are given** — a latest-wins decision across multiple
subjects, or across multiple permissions, is meaningless, so it stays absent otherwise.
Deliberately not named `granted`, because the row-level `granted` means "what this record
did" and the two must not read as the same thing. Each row additionally carries
`expiresAtUTC` (RFC 3339) so a caller can verify the exact instant that was stored without
querying the database.

**Both filters stay optional deliberately, for the same underlying reason.** Exactly one
permission key exists today, so requiring `permission` would make every call type
`permission=external.image.view` — a parameter with one possible value is pure ceremony —
and once more keys exist, omitting it is the real use case: "everything alice holds".
Omitting `subjectAccount` serves the audit/BI use case: "everything granted or revoked at
this site recently", without enumerating subjects up front. The cost: a query that omits
`permission` cannot use index 1 (the equality prefix breaks at the missing middle field)
and falls back to index 2 (`{siteId, recordedAt}`) with a residual filter, or — when
`subjectAccount` is also omitted — index 2 alone with no residual filter at all. At this
collection's volume that is negligible — a known and accepted trade-off; **do not add a
third index for it**.

This endpoint serves both the terminal workflow (checking state before a write and
confirming it afterwards) and the console lookup pane (§5).

## 5. Admin console — admin-frontend

- **Nav:** `AppShell.jsx`'s `SECTIONS` gains `{ key: 'permissions', label: 'Permissions' }`
  with a lazy-loaded `PermissionsView`, following the `AuditView` pattern.
- **API client:** `api/admin/index.ts` gains `createPermissions(token, body)` and
  `listPermissions(token, params)`, following the existing typed-client pattern (Bearer
  auth; non-2xx throws `AsyncJobError`).
- **`PermissionsView`**, two panes:
  - **Form** — grant/revoke mode toggle (revoke hides the date fields); subjects entered
    in a textarea (whitespace/comma separated, parsed client-side with a live count,
    >200 blocked client-side with the server as authority); two `<input type="date">`
    fields whose values are sent verbatim as `YYYY-MM-DD` (**the UI performs no timezone
    handling of any kind**); applicant / approver / reason (rune counter, ≤1000); submit
    **disabled while a request is in flight**; a result panel showing `created`,
    `duplicatesIgnored`, and the account lists from `unknown_accounts` /
    `inactive_subject` rejections.
  - **Lookup** — enter a `subjectAccount` → `currentlyGranted` badge plus the ledger
    table newest-first, using the shared `Pager`.
- **AuditView is untouched** — `permission.grant` / `permission.revoke` rows appear
  automatically, and its action / targetAccount filters work as-is.
- Error display reuses `useHandleAdminError`, with permission-specific reason mappings.
- Tests follow the `AuditView.test.jsx` pattern (Vitest): form validation, request
  payload, reason mapping, in-flight lockout, lookup rendering.

## 6. User query API — user-service (NATS)

```
chat.user.{account}.request.user.{siteID}.permission.get
```

This follows the existing `{area}.{action}` shape (`settings.get`, `chatlist.get`) and is
registered with `natsrouter.Register`. **The subject must be built by a `pkg/subject`
builder**, never a raw `fmt.Sprintf`. In the same change, add `"permission"` to
`ParseUserSubject`'s area whitelist (`pkg/subject/subject.go`) — the router does not use
that parser, so nothing breaks today, but leaving the family's own parser unable to
recognize its newest area is a silent-failure trap for future callers.

**Request:** `{"permission": "external.image.view"}`

**Response:** `{"permission": "external.image.view", "granted": true}`

No permission — never applied, expired, not yet effective, or revoked — returns
`granted: false` with **200, not an error**. "No permission" is a normal answer. An unknown
permission key returns 400.

**No dates are returned.** The requirement is a yes/no, and chat-frontend is out of scope.
This keeps user-service entirely free of timezone and date handling. If an app later wants
to show an expiry date, that is an added field, not a restructure.

`applicantAccount`, `approverAccount`, `reason`, and `recordedBy` are deliberately not
returned. They are audit data an ordinary user's UI does not need, and omitting them
reduces the exposure surface.

## 7. Authentication

| | Transport | Identity source | Caller supplies |
|---|---|---|---|
| Admin write | HTTP POST | `Authorization: Bearer <session token>` → `requireAdmin` validates the session, `sess.SiteID == siteID`, and `admin` in `roles` | Session token |
| User query | NATS request/reply | The `{account}` token in the subject, enforced by the NATS scoped signing key | **Nothing** |

The second row is the most important security property of this design: **a user cannot
query another user's permission because they cannot publish another user's subject.** This
is enforced at the connection layer, not by an application check.

**External dependency, stated explicitly.** That guarantee comes from the NATS
signing-key template owned by ops (`SetScoped(true)` plus the scoped user template
`chat.user.<account>.>`), not from code in this repository. auth-service's own permission
list is documentation; the actual grant lives in the platform team's template. **This
design depends on that template not drifting.**

## 8. Dates and timezones

### 8.1 A fixed UTC+8 interpretation rule

A timezone appears in exactly **one place** in the whole system: when admin-service turns
the submitted date string into an instant at write time. It is a package constant:

```go
var tzTaipei = time.FixedZone("UTC+8", 8*60*60)
```

**First, the most common confusion, dispelled: comparison has no timezone.** The stored
`expiresAt` is an instant; evaluation is `now.Before(until)` — instant against instant.
Whatever machine runs it, whatever its `TZ` environment says, the answer is identical.
`2027-01-01T00:00Z` and "Jan 1, 08:00 in Taipei" are **the same point in time**, not two
things a comparison trick could reconcile.

The only choice that exists is **which instant the write path derives from
`"2026-12-31"`**:

```go
// Spelling A — Go's default (no location supplied means UTC midnight)
time.Parse("2006-01-02", "2027-01-01")
// → 2027-01-01T00:00:00Z = expires at 08:00 Taipei — eight extra hours

// Spelling B — this design
time.Date(2027, 1, 1, 0, 0, 0, 0, tzTaipei)
// → 2026-12-31T16:00:00Z = expires at midnight Taipei, exactly
```

Both spellings evaluate identically afterwards — **the difference is frozen at write time
and no later comparison can undo it**. The table below compares these interpretation
rules:

| Interpretation rule | Instant for "expires 12/31" | Taipei sees | Dresden sees |
|---|---|---|---|
| UTC midnight (= the forgot-the-location default) | `2027-01-01T00:00Z` | Jan 1, **08:00** ⚠️ | Jan 1, 01:00 ⚠️ |
| UTC+8 midnight (this design) | `2026-12-31T16:00Z` | Jan 1, 00:00 ✅ | Dec 31, 17:00 ✅ |

Permission expiry has one safe direction: expiring early is an inconvenience, expiring
late is a security gap. The UTC rule grants the Taiwan-majority user base eight extra
hours; anchoring on the easternmost site means every other site only ever expires early.

Three related decisions:

- **Never `time.Local`.** Containers usually run with `TZ=UTC`, and environments differ;
  letting the process's local zone participate ties expiry to deployment configuration.
  The explicit constant makes the behavior a property of the code.
- **No tzdata.** `time.FixedZone` is pure arithmetic — no IANA database, no
  `import _ "time/tzdata"`, no `apk add tzdata`. The alpine runtime needs nothing.
- **Accepted risk:** a fixed offset carries no DST rules. If Taiwan reintroduced daylight
  saving (it has existed historically and resurfaces politically), summer local time
  would be +9 and stored instants would expire **one hour late** — the unsafe direction.
  Known, accepted, listed in §15.

user-service touches no dates (§6) and is unaffected by this section.

### 8.2 Why the API takes a date rather than an RFC 3339 instant

(First, precision: "ISO 8601" is too permissive as a contract — it admits week dates
(`2026-W01-1`), the basic no-separator format, and fractional-hour offsets. The contract is
**RFC 3339**, a stricter profile.)

Accepting a full instant would move the day→instant conversion to a human typing curl,
which is the least protected place for it:

1. **The half-open interval becomes the caller's problem.** Expressing "through Dec 31"
   means hand-typing `2027-01-01T00:00:00+08:00`; typing `2026-12-31T...` silently loses a
   whole day with nothing to catch it.
2. **Precision leaks.** The format admits `15:30:00`, and the API cannot distinguish
   intent from a slip.
3. **Report readability suffers** — a reader must mentally subtract a day.

RFC 3339 *is* the right form on the **output** side: the admin GET's `expiresAtUTC` lets a
caller verify exactly which instant was stored.

### 8.3 Revocation is unaffected by timezones

The security-critical path is revocation, and it involves no dates at all: `granted: false`
takes effect the moment it is written, under latest-wins. Departures, security incidents,
and cancelled projects all travel that path. Timezones only affect natural expiry, which by
definition is the low-risk case — nobody is watching, no event triggered it.

## 9. Error codes

New `pkg/errcode/codes_permission.go`, eight reasons:

```go
const (
    PermissionUnknownKey       Reason = "unknown_permission"           // 400
    PermissionInvalidSubjects  Reason = "invalid_subject_count"        // 400
    PermissionInvalidReason    Reason = "invalid_reason"               // 400
    PermissionMissingFields    Reason = "missing_permission_fields"    // 400
    PermissionInvalidWindow    Reason = "invalid_permission_window"    // 400
    PermissionUnexpectedWindow Reason = "unexpected_permission_window" // 400
    PermissionInactiveSubject  Reason = "inactive_subject"             // 400
    PermissionUnknownAccounts  Reason = "unknown_accounts"             // 404
)
```

Every new reason **must be registered in `allReasons` in `pkg/errcode/codes_test.go`** —
a documented step in `docs/error-handling.md`; omitting it fails the test suite.

`errcode.Code` is a closed set of eight values with **no 413**. An oversized body returns
**400**, following `media-service/upload.go:58`.

Middleware reuses the existing `AdminNotAuthorized` and `AdminInvalidToken`.

Handlers follow Tier-1 usage: return `errcode.BadRequest(msg, errcode.WithReason(...))`;
adapters are `errhttp.Write` (admin-service) and the natsrouter's automatic return
(user-service); infrastructure failures return `fmt.Errorf("...: %w", err)`.

## 10. Testing

TDD red-green-refactor throughout.

| File | Focus |
|---|---|
| `pkg/model/permission_test.go` | `EvaluateGrant` table tests. **Boundaries are the point**: `now == effectiveFrom` → true (closed end); `now == expiresAt` → **false** (open end). Plus nil ledger, nil bounds on a grant row (must deny, not panic), and revoked rows. |
| `pkg/model/model_test.go` | `PermissionGrant` round-trip via the existing generic helper. |
| `admin-service/handler_test.go` | One subtest per validation rule asserting its reason; duplicates deduped and reported; unknown and inactive accounts rejected; store failure leaves no audit; a batch writes N audit entries. |
| `user-service/service/permission_test.go` | Granted, absent, expired, not yet effective, revoked, no record, unknown key, store error — plus an explicit test that the handler never reads an account from the request body. |
| admin-frontend (Vitest) | Form validation, request payload, reason mapping, in-flight lockout, lookup rendering — following `AuditView.test.jsx`. |
| Integration (`//go:build integration`) | Transaction all-or-nothing; latest-wins including the same-millisecond `_id` tie-break; `explain` confirms `IXSCAN` rather than an in-memory `SORT`. |

Coverage ≥80%, targeting 90%+ on the evaluation logic.

**Mocks:** admin-service is covered automatically by its `-source=store.go` directive.
**user-service's `//go:generate mockgen` list must gain the new repository interface**
before `make generate`.

## 11. Metabase

Not integrated in this change. Two properties are preserved: the collection is directly
readable, and the required columns exist.

**Required columns:** `subjectAccount`, `granted`, `effectiveFrom`, `expiresAt`,
`applicantAccount`, `approverAccount`, `reason` — all direct columns of
`permission_grants`.

**Date display semantics (decided):** `effectiveFrom` / `expiresAt` are the half-open
interval's instants. In the report, `expiresAt` renders as the **exclusive end** — an
admin who entered "expires 12/31" sees `2027-01-01 00:00` in a Taipei-rendering report,
one day later than the input. **Report readers do the conversion themselves.** No
denormalized date-string columns, no required Metabase custom columns.

The change history is the whole table read as-is. A "who currently holds this permission"
list needs an aggregation, because historical rows keep their original `granted` value —
**filtering on `granted = true` would return everyone who ever held it**:

```javascript
[
  {"$match": {"siteId": "site-a", "permission": "external.image.view"}},
  {"$sort":  {"subjectAccount": 1, "recordedAt": -1, "_id": -1}},
  {"$group": {
    "_id":             "$subjectAccount",
    "granted":         {"$first": "$granted"},
    "effectiveFrom":   {"$first": "$effectiveFrom"},
    "expiresAt":       {"$first": "$expiresAt"},
    "approverAccount": {"$first": "$approverAccount"},
    "recordedAt":      {"$first": "$recordedAt"}
  }},
  {"$match": {"$expr": {"$and": [
    {"$eq":  ["$granted", true]},
    {"$lte": ["$effectiveFrom", "$$NOW"]},
    {"$gt":  ["$expiresAt", "$$NOW"]}
  ]}}}
]
```

The `$sort` keys are a contiguous suffix of index 1 after its `{siteId, permission}`
equality prefix.

**The three final conditions must not be reused separately.** On a revoke row the window
fields are absent; MongoDB treats a missing field as `null` in aggregation expressions, and
null sorts below all dates, so `null <= NOW` is **true**. The clause is only correct as a
whole — `$gt` on `expiresAt` and `$eq` on `granted` are what exclude revoke rows.

No materialized "current state" collection is maintained: it would introduce a dual-write
consistency problem, and the indexed query is already fast.

A reason longer than roughly 125 characters is truncated in Metabase's table view and needs
a click to read in full. That figure recurs in community discussion but is not confirmed in
official documentation — verify against the live instance and adjust `MaxReasonRunes` if it
differs. Completeness was chosen over report readability here.

## 12. Future: server-side enforcement (not built)

`chat.server.request.permission.{siteID}.check`, taking an account and a permission key, for
media-service or the gateway. The shape matches the existing server RPC family
(`chat.server.request.<resource>.{siteID}.<action>`), and `chat.server.>` is already
service-only under NATS account permissions. It would share `EvaluateGrant`, so the two
entry points cannot disagree.

The only preparation in this change is making the evaluation a pure function rather than
inline handler logic (§3.5).

## 13. Supporting changes

- **`docker-local/compose.deps.yaml`** — run MongoDB as a single-node replica set, following
  `compose.migration-demo.yaml`'s healthcheck-initiate pattern. Transactions require a
  replica set, and the current standalone container makes the write path fail locally every
  time. This also repairs admin-service's existing password-change and deactivate
  endpoints, which are transactional and therefore already broken in local development.

  One caveat the migration-demo precedent does not cover: its consumers all live inside the
  compose network, but the main `mongodb` is also reached from the host — `make seed`
  defaults to `mongodb://localhost:27017` (`tools/seed-sample-data/main.go`), and
  `tools/jaeger/README.md` documents running service binaries on the host. A replica-set
  member registers exactly one hostname, and whichever side it names, the other side's
  topology discovery breaks. Resolution: register `mongodb:27017` as the member host so
  containers stay zero-config, and host-side URIs add `?directConnection=true` — which
  fully supports transactions against a single-node replica set. Concretely, the seed
  tool's `envDefault` and the jaeger README each change by one line; both are part of this
  change, because a change that knowingly breaks `make seed` is not complete.
- **`admin-service/main.go`** — add an `http.MaxBytesReader` middleware (1MB). The repository
  has precedent (`media-service`, `teams-room-inspector`, `botplatform-service`);
  admin-service is simply missing it, and this is its first endpoint accepting an array.

## 14. Documentation

| File | Change |
|---|---|
| `docs/client-api.md` §3.4 | user-service `permission.get` |
| `docs/client-api.md` §9 | admin `POST` and `GET`, with curl examples |
| `docs/client-api.md` §6 | Eight new entries in the reason catalog |
| `docs/client-api/request-reply.md` | Derived view, same PR |
| `docs/client-api/events.md` | **Unchanged** — no new events |

## 15. Residual risks

1. **A lost response followed by a resend duplicates ledger rows**, and append-only means
   they cannot be deleted. This is the accepted cost of not implementing an idempotency key.
   The console disables submit while a request is in flight, removing most accidental
   resends; the curl path relies on procedure — `GET` to confirm before resending.
2. **Advisory, not enforced.** media-service's image routes have no auth middleware at all
   today, so a user can bypass the check by requesting the URL directly. Deliberately out of
   scope; the hook is §12.
3. **Clock skew across admin-service replicas.** `RecordedAt` comes from the writing
   process's clock, so a revoke written by a slow-clocked replica could sort before a grant
   written slightly earlier by a fast-clocked one. Within the same millisecond the `_id`
   tie-break is effectively random across processes — UUIDv7 guarantees monotonicity only
   within a process. This depends on NTP synchronization; confirm a revocation with `GET`.
4. **The fixed UTC+8 offset carries no DST rules.** If Taiwan reintroduced daylight saving,
   expiry would land one hour late — the unsafe direction. Known and accepted.
5. **An approver without a chat account cannot be recorded** — the cost of verifying account
   existence.
6. **`reason` is permanently retained free text.** No TTL, and who can read it depends on
   Metabase permissions. This belongs in a data-classification review.
7. **The NATS subject scope is an ops-owned external contract** — see §7.
8. **The NATS RPC has no caller at launch.** chat-frontend is out of scope; the admin API
   ships with callers (console + curl).
