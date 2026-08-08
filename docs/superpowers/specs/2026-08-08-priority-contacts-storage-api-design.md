# Priority Contacts — Storage and API (Spec 1 of 2)

Source: [issue #204](https://github.com/hmchangw/newchat/issues/204). Verified against `9a1f606`.

## Scope

Issue #204 covers two things: storing priority contacts, and making `notification-worker`
evaluate them. This spec is **Part 1 only** — the model field, persistence, the CRUD API, and
cross-site replication. `notification-worker` enforcement is Spec 2.

The split is clean: Part 1 has no dependency on Part 2, ships standalone value (the frontend
contact picker builds against it), and Part 2's staleness decisions are easier once the
replication lane is merged and observable.

**Consequence, accepted:** until Spec 2 ships, `priorityContacts` is stored and replicated but
not evaluated — the same state the four existing notification flags are in today.

### Out of scope

| Item | Why |
|---|---|
| `notification-worker` push-gate changes | Spec 2 |
| Remote-site Valkey invalidation (issue comment G4) | Spec 2 — a caching decision with no bearing on storage or API |
| `muteAllMobileNotifications` | Cut. Every `UserSettings` field is `*T` + `omitempty`, so adding it later is purely additive — there is no migration to pre-empt. Add it in the PR that reads it |

## Corrections to the issue

Verified against the code; the plan is wrong or incomplete on these points.

| Issue claim | Reality |
|---|---|
| "New dependency on `AppRepository` — inject at startup" | Already injected: `UserService.apps` (`user-service/service/service.go:104`), used by `threads.go:270`. No wiring needed |
| "Add/Remove publish `SettingsUpdateEvent` → cross-device sync for free" | Incomplete. `SetSettings` publishes **two** fanouts off one timestamp (`settings.go:62-65`); the cross-site one is what Spec 2 depends on. See [Cross-site replication](#cross-site-replication) |
| Enrichment uses the existing `GetHRInfoByAccounts` | Insufficient — it projects only `{account, chineseName, engName}` and returns `SubscriptionHRInfo`, which has no `employeeId` or `sectName` (`pkg/model/subscription.go:153`). `model.User` has both (`user.go:52,61`). Needs a new method |
| Subject `…priority-contacts.get` | Violates house style. Subjects are dot-separated with camelCase verbs (`chatlist.section.create`, `subscription.getChannels`); no hyphens exist in the tree |
| Cap "atomic, no read-modify-write race" | The `$addToSet` is atomic; the read-then-check-then-write cap wrapped around it is not. See [Cap enforcement](#cap-enforcement) |
| `SetSettings` must force-reset `alwaysAllowPriorityNotifications` | Dropped. Spec 2's gate only reads that flag inside `if MuteAll`, so it is already inert when mute is off. The reset changes no delivery behavior and destroys a preference the user must then rediscover |
| Three "nine fields" doc strings become "eleven" (comment G6) | Two become **ten**, one stays **nine** — they are different claims. See [Documentation](#documentation) |

Also unresolved in the issue's own pseudocode (Spec 2's problem, recorded here so it is not
lost): step 3.5 reads `status == "busy"`, but `Presence.Snapshot` runs *after* the candidate
loop closes (`notification-worker/handler.go:196`), so `status` does not exist at that point.

## Data model

`pkg/model/usersettings.go` — one field on `UserSettings`:

```go
PriorityContacts []string `json:"priorityContacts,omitempty" bson:"priorityContacts,omitempty"`
```

Stored at `users.settings.priorityContacts`. Raw account strings — user accounts and `.bot`
accounts alike. Capped at 30.

Embedded rather than a separate collection: the list is owned by one user, always read in the
context of that user's settings, and bounded at ~1 KB. A separate collection would add an index
and a second query to every `settings.get`.

Two deliberate omissions, each commented in place:

- **`IsEmpty()` is not updated.** `priorityContacts` never arrives through `settings.set`, so a
  request carrying only that field correctly falls through to `bad_request "no settings
  provided"`. `SettingsSetRequest` embeds `model.UserSettings`, so a client *can* put the field
  in the body; it is ignored.
- **`UpdateUserSettings` is not extended.** It builds its `$set` from explicit per-field nil
  checks and never names `PriorityContacts`, so the scalar-settings path cannot touch the list.
  Safe by omission, not by accident.

`SettingsUpdateEvent` and `model.UserSettingsUpdated` embed `UserSettings` by value and carry the
new field automatically. No new event type.

## Repository

`user-service/mongorepo/users.go`. `limit` is a parameter, not a repo constant —
`maxPriorityContacts = 30` lives in the service layer as the single source of truth.

| Method | Op | Notes |
|---|---|---|
| `GetPriorityContacts(ctx, account) ([]string, error)` | `FindOne`, project `{_id:0, settings.priorityContacts:1}` | Returns `[]string{}`, never nil. Narrow projection is safe here — it never feeds a fanout |
| `AddPriorityContact(ctx, account, contact string, limit int, at time.Time) (*model.User, error)` | `FindOneAndUpdate` + `$addToSet` | Cap enforced in the filter. `(nil, nil)` on no match |
| `RemovePriorityContact(ctx, account, contact string, at time.Time) (*model.User, error)` | `FindOneAndUpdate` + `$pull` | Idempotent — removing an absent entry is a no-op. `(nil, nil)` on no match |
| `GetPriorityContactUsers(ctx, accounts) (map[string]*models.PriorityContactUser, error)` | `FindMany`, project `{_id:0, account:1, engName:1, chineseName:1, employeeId:1, sectName:1}` | New. **No** active filter. Returns the `user-service/models` type directly, as `GetAppCategories` already does with `models.AppCategory` (`mongorepo/apps.go`) |
| `UserExists(ctx, account) (bool, error)` | `FindOne` with `activeUserFilter`, project `{_id:0, account:1}` | New. Backs the add-time 404. Active-only |
| `UpdateUserSettings` *(existing)* | now also `$set`s `settingsUpdatedAt` | See [Cross-site replication](#cross-site-replication) |

The active-filter asymmetry between `UserExists` and `GetPriorityContactUsers` is deliberate:
strict at add time (you cannot add a deactivated user), lenient at render time (a contact
deactivated *after* being added still renders their name instead of becoming a blank row).

No new index — the existing unique `{account: 1}` on `users` is the lookup key.

### Cap enforcement

Read-then-write races: two concurrent adds both read 29, both pass, the list lands at 31. The cap
rides in the update filter instead:

```go
filter := bson.M{"account": account, "active": bson.M{"$ne": false}, "$expr": bson.M{
    "$lt": bson.A{bson.M{"$size": bson.M{"$ifNull": bson.A{"$settings.priorityContacts", bson.A{}}}}, limit},
}}
update := bson.M{
    "$addToSet": bson.M{"settings.priorityContacts": contact},
    "$set":      bson.M{"settingsUpdatedAt": at},
}
```

No match is ambiguous — user gone, or at cap. The service disambiguates with one re-read, on the
failure path only. A duplicate add at exactly 30 must return success, not `priority_contact_limit`:
it is a no-op, not a violation.

### Projection constraint

**Add and Remove project `{"_id": 0, "settings": 1}` — the whole sub-document, never
`settings.priorityContacts`.** The returned object is the cross-site fanout payload, and
`inbox-worker` applies it as a whole-object `$set` of `settings`. A narrowed projection here
wipes every other setting at every remote site.

Both call sites carry an explicit comment, because "optimise this projection" is an
attractive-looking refactor with a silent, cross-site, data-loss failure mode.

## API

Three RPCs registered in `RegisterHandlers`. Subjects go in `pkg/subject/subject.go` with both
concrete and `Pattern` forms; the concrete forms panic on a wildcard account, matching the
existing settings helpers.

| Handler | Subject | Body | Returns |
|---|---|---|---|
| `GetPriorityContacts` | `chat.user.{account}.request.user.{siteID}.settings.priorityContacts.get` | none (`RegisterNoBody`) | `PriorityContactItem[]` |
| `AddPriorityContact` | `chat.user.{account}.request.user.{siteID}.settings.priorityContacts.add` | `{"contactAccount": "..."}` | `PriorityContactItem[]` |
| `RemovePriorityContact` | `chat.user.{account}.request.user.{siteID}.settings.priorityContacts.remove` | `{"contactAccount": "..."}` | `PriorityContactItem[]` |

Nested under `settings.` exactly as `section` nests under `chatlist` — and honest about where the
data lives. All three return the full enriched list so the acting device re-renders without a
follow-up call.

### Response shape

`user-service/models/prioritycontacts.go`. Follows the `SubscriptionItem` precedent
(`pkg/model/subscription.go:159-200`): a common base plus a nested, `omitempty`, type-specific
object, with an explicit discriminator.

```go
type PriorityContactItem struct {
    Account string               `json:"account"`
    Type    string               `json:"type"`           // "user" | "bot", from model.IsBot(account)
    User    *PriorityContactUser `json:"user,omitempty"` // set when type == "user"
    App     *PriorityContactApp  `json:"app,omitempty"`  // set when type == "bot"
}

type PriorityContactUser struct {
    EngName     string `json:"engName"`
    ChineseName string `json:"chineseName"`
    EmployeeID  string `json:"employeeId"`
    SectName    string `json:"sectName"`
}

type PriorityContactApp struct {
    Name string `json:"name"`
}

const (
    PriorityContactTypeUser = "user"
    PriorityContactTypeBot  = "bot"
)
```

The explicit `type` exists so the frontend never has to infer the row's kind from a `.bot` suffix
or from which fields came back non-empty — the first is ambiguous for a user with a blank
`engName`, the second makes the bot-account naming convention load-bearing on the wire.

The `SubscriptionItem` Go interface machinery (`Base()` + `isSubscriptionItem()`) is **not**
copied. That exists because its base is a persisted struct shared across variants; here there is
no persisted base, so one struct with two nil-able pointers marshals to the same JSON with far
less code.

Both nested pointers are nil when the account no longer resolves — deactivated user, deleted app.
The row degrades to `account` + `type` and the frontend renders a placeholder. This is reachable
even with the add-time existence check, because accounts can be removed after being added.

### Handler logic

**Add:**

1. Reject empty `contactAccount`; reject `contactAccount == account` → `BadRequest`
2. Existence check — `model.IsBot(contact)` selects the source: `GetAppsByAssistants([contact])`
   for bots, `UserExists` for users. Miss → `NotFound` + reason `priority_contact_not_found`
3. `now := time.Now().UTC().UnixMilli()`; cap-guarded `$addToSet`, stamping
   `settingsUpdatedAt = time.UnixMilli(now).UTC()`
4. On no match, one re-read disambiguates:
   - contact already present → success with the current list (duplicate at cap is a no-op)
   - `len >= 30` → `Forbidden` + reason `priority_contact_limit`
   - otherwise → `NotFound("user not found")`, no reason — matches `GetSettings`
5. Publish both fanouts off the shared `now`
6. Enrich and return

A bot's app is required to exist, not to be `Enabled`. A disabled assistant cannot DM the user, so
a priority entry for it is inert rather than wrong, and requiring `Enabled` would make re-adding
fail whenever an admin temporarily disables an app.

**Remove:** validate non-empty → `$pull` → `(nil, nil)` means `NotFound("user not found")` →
publish both fanouts → enrich and return. No existence check: removing an account that no longer
exists is exactly the cleanup case to permit.

**Enrichment** (shared by all three): partition the stored list by `model.IsBot`, then
`GetPriorityContactUsers` and `GetAppsByAssistants`, building rows in **stored order**.
Sequential, not parallel — two queries over at most 30 accounts, matching
`lookupThreadHRInfo` / `lookupThreadApps` in `threads.go`. Both degrade the same way those do: a
lookup failure logs a warn and yields a nil map, so rows return with `account` + `type` and no
nested object rather than failing the call.

### Errors

`pkg/errcode/codes_user.go` — unprefixed wire values, house style:

```go
UserPriorityContactLimit    Reason = "priority_contact_limit"
UserPriorityContactNotFound Reason = "priority_contact_not_found"
```

Self-add returns a plain `BadRequest` with no reason: the picker should not offer the caller, so
there is nothing for the frontend to branch on. Trivially added later if that proves wrong.

Infra failures return raw `fmt.Errorf("…: %w", err)` and collapse to `internal` at the boundary.
`natsrouter` marshals the envelope; handlers never call the adapter and never log-and-return.

## Cross-site replication

**This is the load-bearing constraint of the spec.**

`notification-worker`'s member loader queries `{roomId: roomID}` with no siteID filter
(`notification-worker/main.go:76`), so the worker at a room's home site evaluates every member of
that room, including members who live at other sites. `inbox-worker.handleMemberAdded` requires a
local `users` doc for every referenced account and Naks until one exists — every site is expected
to hold a doc for every user.

So: user U lives at site B and adds a priority contact there. A message lands in a room homed at
**site A** where U is a member. Site A's `notification-worker` reads **site A's** copy of U's
`users` doc. Without cross-site replication, U's priority contacts pierce mute only for rooms
homed at their own site and silently do nothing everywhere else — the common case for any
federated room.

Add and Remove therefore publish **both** fanouts off one shared timestamp, exactly as
`SetSettings` does:

| Fanout | Subject | Consumer |
|---|---|---|
| `publishSettingsUpdate` | `chat.user.{account}.event.settings.update` (core NATS) | caller's other devices |
| `publishSettingsInbox` | `chat.inbox.{dest}.external.user_settings_updated`, one per `ALL_SITE_IDS` | remote `inbox-worker` → local `users` doc |

No new event type, stream, or consumer: `InboxUserSettingsUpdated` already exists and
`inbox-worker` already applies it.

Verified that no user-sync writer clobbers `settings`: `hr-sync-worker.UpsertUserIdentities`
`$set`s only `siteId`/`engName`/`chineseName`/`employeeId`, with a comment stating why it is built
by hand ("a full-doc replace would wipe roles/password/services"); `teams-user-sync` writes a
separate `teams_users` collection.

### Fanout helper

Three call sites publishing the same pair, where getting it wrong means publishing only the client
event, is worth one definition:

```go
// publishSettingsFanouts emits both settings fanouts off one timestamp: the client
// event for the caller's other devices, and the cross-site inbox replica every
// site's notification-worker reads. Both carry the FULL settings sub-document —
// the cross-site apply is a whole-object $set, so a partial payload wipes
// unrelated settings at remote sites.
func (s *UserService) publishSettingsFanouts(c *natsrouter.Context, account string, settings *model.UserSettings, now int64)
```

`SetSettings` switches to it as well.

### Ordering fixes

Two defects in the existing lane, fixed here because this spec starts depending on it.

**Origin never stamps `settingsUpdatedAt`.** `UpdateUserSettings` does not set it, unlike
`UpdateUserChatlist` in the same file (`users.go:174`), whose comment says why: *"without it an
older inbound event could regress local state."* Today the origin's doc has no `settingsUpdatedAt`
at all, so `inbox-worker`'s `$exists: false` branch always matches there and a stale remote event
can overwrite a newer local edit. `UpdateUserSettings`, `AddPriorityContact`, and
`RemovePriorityContact` all stamp it.

**Same-millisecond events are dropped.** The guard is `$lt`, so of two writes stamped in the same
millisecond the second is dropped at every remote site. Relaxed to `$lte`, safe because the apply
is an idempotent whole-object replace. A genuine same-ms tie resolves to last-delivered and heals
on the next write; the exposure is bounded to one contact, not a corrupted list.

A monotonic `settingsVersion` would be exactly correct, but a version counter that must survive a
mixed-version rolling deploy is heavy protection against sub-millisecond collisions. If bulk-add
becomes a real frontend flow, the right answer then is a bulk RPC that writes once.

`settingsUpdatedAt` is a BSON date — `inbox-worker` writes `time.UnixMilli(e.Timestamp).UTC()`
(`handler.go:422`). The origin stamp uses the same conversion off the same shared `now`, so origin
and replica values are directly comparable.

### Cross-device sync

The client event carries `priorityContacts` as `[]string`, while the RPCs return enriched
`PriorityContactItem[]`. A passive device receiving `settings.update` therefore learns the
list's *membership* but cannot render names — it re-issues `settings.priorityContacts.get` when
the received `priorityContacts` differs from its copy.

The enriched payload is deliberately kept off both fanouts. On the client event it would cost an
HR + apps lookup per add/remove; on the inbox lane `inbox-worker` would write denormalized HR data
straight into `settings`, replicating it to every site and drifting the moment someone changes
department. The refetch lands on the device the user is *not* looking at.

## Testing

TDD throughout — red before green, per CLAUDE.md §4. Containers come from `pkg/testutil`; the
existing `TestMain`s already drive `testutil.RunTests`.

| Layer | File | Cases |
|---|---|---|
| Model | `pkg/model/model_test.go` | `PriorityContacts` round-trip via the generic `roundTrip` helper; `IsEmpty()` still true for a settings carrying only `priorityContacts` — pins the `bad_request` fall-through as intentional |
| Handlers | `user-service/service/prioritycontacts_test.go` (new) | Table-driven, mock repo. **Get:** empty, users-only, bots-only, mixed, unresolvable row, HR-lookup failure degrades, apps-lookup failure degrades, stored order preserved. **Add:** happy, empty contact, self-add, unknown user, unknown bot, at-cap, duplicate-at-cap, caller missing, repo error. **Remove:** happy, empty contact, absent contact, caller missing. Both mutations assert **both** fanouts fire with the **same** timestamp |
| Repo | `user-service/mongorepo/users_test.go` (extend, `//go:build integration`) | Cap holds under **concurrent** adds; `$addToSet`/`$pull` idempotence; `settingsUpdatedAt` stamped by Add, Remove, and `UpdateUserSettings`; Add/Remove return the **whole** settings sub-document; `GetPriorityContactUsers` projection; `UserExists` respects the active filter |
| Inbox | `inbox-worker/handler_test.go`, `integration_test.go` (extend) | Same-millisecond event applies under `$lte`; a genuinely older event is still rejected |

Minimum 80% coverage; 90%+ for the new handlers. `make generate` before testing — the
`UserRepository` interface gains five methods.

## Documentation

Same PR, per CLAUDE.md §5.

**`docs/client-api.md`**

- New subsection for the three RPCs under the settings area
- `PriorityContactItem`, `PriorityContactUser`, `PriorityContactApp` as **named** compound types
  with their own field tables, referenced by link — never bare `object`
- A success JSON example per RPC
- `priorityContacts` row added to the `settings.get` response table
- Both new reasons in §6
- Index line `:62`; subject table `:4443`
- `:4435` — *"No other endpoint emits a client-facing event"* becomes false; all three new RPCs
  emit `settings.update`
- `:4750` nine → **ten** (the `settings.update` payload). `:4701` stays **nine** — it describes
  the `settings.set` *request*, and `priorityContacts` is not settable there

**`docs/client-api/request-reply.md`** — the three RPCs.

**`docs/client-api/events.md`** — `:200` nine → **ten**.

## Files

| File | Change |
|---|---|
| `pkg/model/usersettings.go` | `PriorityContacts` field; comments on the `IsEmpty` / `UpdateUserSettings` omissions |
| `pkg/errcode/codes_user.go` | Two reason constants |
| `pkg/subject/subject.go` | Three subjects, concrete + `Pattern` forms |
| `user-service/models/prioritycontacts.go` | New — request and response types |
| `user-service/service/prioritycontacts.go` | New — three handlers plus enrichment |
| `user-service/service/service.go` | Five methods on `UserRepository`; three registrations |
| `user-service/service/settings.go` | `publishSettingsFanouts` helper; `SetSettings` switched to it |
| `user-service/mongorepo/users.go` | Five new methods; `UpdateUserSettings` stamps `settingsUpdatedAt` |
| `user-service/service/mocks/` | Regenerated |
| `inbox-worker/main.go` | `$lt` → `$lte` on the `settingsUpdatedAt` guard |
| Tests + docs | Per the tables above |
