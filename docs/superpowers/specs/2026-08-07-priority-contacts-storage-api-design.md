# Priority Contacts — Storage & API (Part 1 of 2)

**Date:** 2026-08-07
**Status:** Approved — ready for implementation planning
**Owner:** user-service (+ inbox-worker for cross-site replication)
**Related:** GitHub issue [#204](https://github.com/hmchangw/newchat/issues/204) "Feature - Priority Contacts Notification Plan" (source plan for this and the follow-up notification-worker spec)

## 0. Scope

Issue #204 proposes two independent-ish pieces of work: (1) storing and
exposing `priorityContacts` via `user-service`, and (2) making
`notification-worker`'s push gate aware of `UserSettings` +
`priorityContacts`. This spec covers **only (1)** — storage, CRUD API, and
cross-site replication. Part (2) will get its own design doc once this one
is implemented and merged, since it depends on the `priorityContacts` field
existing.

`MuteAllMobileNotifications` (a placeholder field the issue proposed adding
now for a future Mobile Phase 1 project) is explicitly **out of scope** —
see §6.

## 1. Background

Today `UserSettings` (`pkg/model/event.go`) has fields like
`MuteAllNotifications` and `AlwaysAllowPriorityNotifications`, but:

- `priorityContacts` doesn't exist anywhere — no model field, no storage, no
  API.
- `user-service.SetSettings` is a partial-update RPC (`settings.set`) that
  writes whichever `UserSettings` fields are present, then fans out the
  full post-update settings two ways: `publishSettingsUpdate` (same-account
  device sync, core NATS) and `publishSettingsInbox` (cross-site
  full-document mirror via an `InboxEvent` of type
  `InboxUserSettingsUpdated`, applied by `inbox-worker` as a **full
  overwrite** of the remote site's local `users.settings`, gated by
  timestamp for last-write-wins).
- Each site holds a local `users` collection that is kept in sync with
  every other site's user data purely through this event mechanism — it is
  not automatically global.

## 2. Data Model

`pkg/model/event.go` — `UserSettings` gains one field:

```go
type UserSettings struct {
    // ... existing fields unchanged ...
    PriorityContacts []string `json:"priorityContacts,omitempty" bson:"priorityContacts,omitempty"`
}
```

- Raw account strings — both regular users and `.bot` accounts. Capped at
  30 entries, enforced at the handler level (not a Mongo schema
  constraint).
- `IsEmpty()` is **not** updated. `PriorityContacts` is never written
  through `SetSettings`/`UpdateUserSettings` — only through the dedicated
  Add/Remove handlers below — so it stays outside the "nothing to write"
  guard.
- `SettingsUpdateEvent` needs no change — it embeds `UserSettings` by
  value, so `PriorityContacts` rides along automatically in the existing
  device-sync fanout.

### New cross-site inbox event

Two new `InboxEventType` constants next to `InboxUserSettingsUpdated`:

```go
const (
    InboxPriorityContactAdded   InboxEventType = "priority_contact_added"
    InboxPriorityContactRemoved InboxEventType = "priority_contact_removed"
)

// PriorityContactChanged is the cross-site inbox payload for one add/remove
// op. It is applied atomically ($addToSet/$pull) on the receiving site,
// never as a full settings overwrite, so it can never race with (or be
// clobbered by) a concurrent SetSettings full-document mirror for an
// unrelated field.
type PriorityContactChanged struct {
    Account        string `json:"account" bson:"account"`
    ContactAccount string `json:"contactAccount" bson:"contactAccount"`
    Timestamp      int64  `json:"timestamp" bson:"timestamp"`
}
```

One struct serves both event types; the operation is implied by which
`InboxEventType` it's wrapped in — mirrors how `UserStatusUpdated` /
`UserSettingsUpdated` are already modeled.

## 3. Components

### `user-service/service/service.go`

`UserRepository` gains three methods (same interface — no new one; priority
contacts are part of the `users` document, same as the rest of
`UserRepository`'s surface). Note the repo method is
`GetPriorityContactAccounts` (raw `[]string`), deliberately named apart from
the client-facing handler `UserService.GetPriorityContacts` (enriched
`[]PriorityContactItem`) — see §3.1:

```go
AddPriorityContact(ctx context.Context, account, contactAccount string) (*model.User, error)
RemovePriorityContact(ctx context.Context, account, contactAccount string) (*model.User, error)
GetPriorityContactAccounts(ctx context.Context, account string) ([]string, error)
```

`AppRepository` (already defined for `GetAppsByAssistants`) is injected
into `UserService` for the bot-existence check and bot enrichment.

### 3.1 Client contract: one call, fully enriched

**All three RPCs reply with the caller's complete, enriched
`[]PriorityContactItem`.** The frontend makes exactly ONE call to render the
priority-contacts list — it never follows up with a user-lookup or app-lookup
RPC to resolve a contact's name, employee ID, org, or app name. Every
cross-collection enrichment happens server-side inside the handler.

| Layer | Name | Returns |
|---|---|---|
| Client-facing handler | `UserService.GetPriorityContacts` | `[]PriorityContactItem` — enriched; what the FE receives |
| Repo | `UserRepo.GetPriorityContactAccounts` | `[]string` — raw stored accounts, internal only |

The repo method cannot return `PriorityContactItem`: enrichment requires
reading a second collection (`apps`), and CLAUDE.md forbids `$lookup`, so the
join belongs in the service layer, not the repo.

### `user-service/mongorepo/users.go`

Three new `UserRepo` methods:

- **`AddPriorityContact`** — `FindOneAndUpdate(After)` with
  `$addToSet: {"settings.priorityContacts": contactAccount}`, projection
  `{_id: 0, settings: 1}`. `(nil, nil)` on `ErrNoDocuments`.
- **`RemovePriorityContact`** — same shape with
  `$pull: {"settings.priorityContacts": contactAccount}`.
- **`GetPriorityContactAccounts`** — `FindOne` projecting only
  `settings.priorityContacts`; returns `[]string{}` (never nil) when the
  field is absent. **Always returns the full stored array, untruncated** —
  see §5 "Uncapped reads."

No new index — the existing unique `{account: 1}` index is the lookup key.

### `user-service/models/prioritycontacts.go` (new)

```go
type PriorityContactAddRequest struct {
    ContactAccount string `json:"contactAccount"`
}

type PriorityContactRemoveRequest struct {
    ContactAccount string `json:"contactAccount"`
}

// PriorityContactItem is the enriched response shape for one contact.
// No AvatarURL: consistent with Participant, SubscriptionHRInfo, and App —
// none of the existing enriched response types carry an avatar URL inline;
// the frontend resolves avatars from `account` via the existing avatar
// convention.
type PriorityContactItem struct {
    Account string `json:"account"`

    // Populated for regular users (account does not end in ".bot"):
    EngName     string `json:"engName,omitempty"`
    ChineseName string `json:"chineseName,omitempty"`
    EmployeeID  string `json:"employeeId,omitempty"`
    SectName    string `json:"sectName,omitempty"`

    // Populated for App bots (account ends in ".bot"):
    AppName string `json:"appName,omitempty"`
}
```

### `user-service/service/prioritycontacts.go` (new)

Three handlers:

| Handler | Subject |
|---|---|
| `GetPriorityContacts` | `chat.user.{account}.request.user.{siteID}.priority-contacts.get` |
| `AddPriorityContact` | `chat.user.{account}.request.user.{siteID}.priority-contacts.add` |
| `RemovePriorityContact` | `chat.user.{account}.request.user.{siteID}.priority-contacts.remove` |

New subject builders in `pkg/subject/subject.go`, following the
`UserSettingsGet` / `UserSettingsGetPattern` naming convention exactly:
`PriorityContactsGet(account, siteID)` / `PriorityContactsGetPattern(siteID)`,
and `PriorityContactsAdd` / `PriorityContactsRemove` siblings.

New `errcode` reasons in `codes_user.go`:

```go
UserPriorityContactSelf         Reason = "priority_contact_self"
UserPriorityContactNotFound     Reason = "priority_contact_not_found"
UserPriorityContactLimitReached Reason = "priority_contact_limit_reached"
```

### `inbox-worker/handler.go`

New case in the inbox event switch routing `InboxPriorityContactAdded` /
`InboxPriorityContactRemoved` to a new `handlePriorityContactChanged(ctx,
evt, op)`, which unmarshals `PriorityContactChanged` and calls new
`AddPriorityContact` / `RemovePriorityContact` methods added to
`mongoInboxStore` (same atomic `$addToSet`/`$pull` ops as user-service's,
**not** the existing full-document `UpdateUserSettings`).

## 4. Data Flow

### `AddPriorityContact`

1. Validate `contactAccount != ""` and `contactAccount != account` →
   `errcode.BadRequest`, with `WithReason(UserPriorityContactSelf)` for the
   self case.
2. **Existence check:** if `model.IsBot(contactAccount)`, look it up via
   `apps.GetAppsByAssistants` and require an enabled assistant; otherwise
   look it up via the `users` collection and require an active user.
   Not found → `errcode.NotFound(..., WithReason(UserPriorityContactNotFound))`.
3. Read the current list via `GetPriorityContactAccounts`; if `len(current) >= 30`
   → `errcode.Forbidden(..., WithReason(UserPriorityContactLimitReached))`.
4. `users.AddPriorityContact(account, contactAccount)` — atomic
   `$addToSet`; a re-add of an existing contact is a no-op, returns 200
   with the unchanged list.
5. One `now := time.Now().UTC().UnixMilli()` shared by both fanouts below
   (mirrors `SetSettings`'s existing pattern, keeping event ordering
   consistent).
6. `publishSettingsUpdate` (existing helper, unchanged) — local device sync,
   carries the full post-update settings.
7. **New:** `publishPriorityContactChanged(account, contactAccount,
   InboxPriorityContactAdded, now)` — cross-site inbox fanout to every
   other site (one event per destination, same loop shape as
   `publishSettingsInbox`), carrying only the delta.
8. Enrich and return the updated list.

### `RemovePriorityContact`

Same shape minus steps 2–3 — no existence check or cap on removal.
`$pull` on a non-existent entry is already a no-op.

### `GetPriorityContacts`

Split the stored list by `model.IsBot`, enrich in parallel via
`GetHRInfoByAccounts` (users) and `GetAppsByAssistants` (bots), build
`[]PriorityContactItem` preserving the original stored order. A missing
HR/app entry (deleted user/app) returns account-only — the frontend handles
graceful display.

### Cross-site receive path

`inbox-worker.handlePriorityContactChanged` applies the same atomic
`$addToSet`/`$pull` remotely. No timestamp-gating is needed (unlike
`UpdateUserSettings`'s regression guard) — these ops are idempotent and
commutative regardless of delivery order, so an out-of-order redelivery is
harmless.

## 5. Design Rules Worth Calling Out

- **Uncapped reads.** `GetPriorityContactAccounts` always returns the full stored
  array — it never truncates to 30. The cap is enforced *only* on the
  `AddPriorityContact` write path. This matters for the rare race where two
  concurrent adds both pass the `len < 30` pre-check before either write
  lands (briefly producing 31 entries): the frontend must see the true,
  untruncated count so it can prompt the user to remove entries.
  `RemovePriorityContact` has no cap check, so pruning down always works
  even while over the limit.
- **Cross-site fanout is atomic per-op, not a full-document mirror.** This
  is the key correction versus the original issue text (which assumed "each
  site already has a complete copy" without accounting for how that copy
  actually stays in sync). Reusing `publishSettingsInbox`'s full-document
  overwrite for a per-contact change would race with an unrelated
  concurrent `SetSettings` mirror. The dedicated
  `InboxPriorityContactAdded`/`Removed` events avoid that.
- Cross-site inbox publish failures are logged and swallowed
  (`slog.WarnContext`), same as `publishSettingsInbox` today. Because the
  underlying ops are idempotent/commutative, a dropped fanout is a delay,
  not silent corruption — it's picked up correctly whenever the next
  add/remove for that pair fires the event again. (True permanent loss is
  the same residual risk `publishSettingsInbox` already accepts today.)

## 6. Out of Scope

- **`MuteAllMobileNotifications`** — dropped from this spec per YAGNI. It
  has no interaction with `priorityContacts` or the storage/API work here;
  it will be added together with the Mobile Phase 1 project that actually
  reads it.
- **`notification-worker` push-gate changes** (issue #204 Part 2) — separate
  design doc, to follow once this one ships.

## 7. Error Handling

Tier 1 throughout, per `docs/error-handling.md`:

| Case | Error |
|---|---|
| `contactAccount == ""` | `errcode.BadRequest("contact account required")` |
| `contactAccount == account` | `errcode.BadRequest("cannot add self", WithReason(UserPriorityContactSelf))` |
| `contactAccount` doesn't resolve to an active user / enabled bot | `errcode.NotFound("contact not found", WithReason(UserPriorityContactNotFound))` |
| List already at 30 on Add | `errcode.Forbidden("priority contact limit reached", WithReason(UserPriorityContactLimitReached))` |
| Caller account not found | `errcode.NotFound("user not found")` — matches `SetSettings`'s existing `u == nil` handling |
| Mongo/infra failure at any step | raw `fmt.Errorf("...: %w", err)` → collapses to `internal` at the boundary |
| Cross-site inbox publish failure | logged + swallowed, non-fatal (see §5) |

## 8. Testing

Per CLAUDE.md's TDD + coverage rules (Red/Green/Refactor, table-driven, 80%
min / 90% target on handlers & store):

- **`user-service/mongorepo/users_test.go`** (extend, integration,
  `testutil.MongoDB`): add/idempotent-re-add/missing-user for
  `AddPriorityContact`; remove/no-op-on-missing/missing-user for
  `RemovePriorityContact`; empty-vs-absent and an untruncated >30-entry case
  for `GetPriorityContactAccounts`.
- **`user-service/service/prioritycontacts_test.go`** (new, unit, mocked
  `UserRepository` + `AppRepository`, table-driven): happy path (user +
  bot contact), empty/self/nonexistent contactAccount, at-cap (30)
  rejection vs. at-29 success, repo error propagation, and assertions that
  both `publishSettingsUpdate` and the new inbox fanout fire with the
  correct payload, for both Add and Remove; enrichment tests for mixed
  user+bot lists and missing HR/app entries.
- **`inbox-worker/handler_test.go`** (extend, unit, mocked store):
  `handlePriorityContactChanged` for both event types — correct store
  method + args, malformed payload (NAK, not permanent — matches existing
  inbox handler error style), store error propagation.
- **`inbox-worker` store integration test** (extend or new): real Mongo
  `$addToSet`/`$pull`, confirming idempotency and that sibling `settings`
  fields are untouched (proving this is a targeted op, not a full-document
  replace).
- Regenerate mocks: `make generate SERVICE=user-service` (and inbox-worker,
  if it has generated mocks) to pick up the new `UserRepository` methods.

## 9. Client API Docs

Since this adds client-facing handlers under
`chat.user.{account}.request.user...`, `docs/client-api.md` and its derived
`docs/client-api/request-reply.md` view must be updated in the same PR:

- New **Priority Contacts** section: field tables for
  `priority-contacts.get` / `.add` (request `{contactAccount}` + response) /
  `.remove` (request + response), each with a JSON example, referencing a
  new shared `PriorityContactItem` type table (§3.0 Shared schemas).
- Error cases table covering the three new reasons plus generic
  `not_found`/`internal`.
- Existing **settings.get** response table gets one row added:
  `priorityContacts: string[]` (omitted when empty, same as other optional
  settings fields), noted as read-only there — mutated only via the
  dedicated endpoints.
- `docs/client-api/events.md` needs no change — `settings.update` already
  documents the full `UserSettings` shape by reference.
- No changes to `docs/cassandra_message_model.md`, stream configs, or
  `pkg/stream` — this is pure request/reply plus one new inbox event pair
  on the already-owned `INBOX` stream; no new streams or bootstrap changes.

## 10. Decisions Log (deviations from issue #204's original text)

| Topic | Issue #204 said | This spec decides |
|---|---|---|
| Cross-site replication | "Each site already holds a complete copy of all users — no cross-site replication needed" | That "complete copy" is achieved only via `publishSettingsInbox`, which Add/Remove don't call. Added dedicated atomic inbox events instead of reusing the full-document mirror. |
| Scope | One combined plan across user-service + notification-worker | Split into two specs/PRs; this one covers user-service storage/API only. |
| `MuteAllMobileNotifications` | Add now as a placeholder | Dropped — YAGNI, deferred to the Mobile Phase 1 project. |
| `contactAccount` existence | Not addressed | Validated on Add — reject accounts that don't resolve to an active user or enabled bot. |
| Uncapped reads | Not addressed | `GetPriorityContactAccounts` never truncates; the 30 cap applies only to Add. |
