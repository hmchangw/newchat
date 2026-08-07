# Priority Contacts — Storage & API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store `priorityContacts` on the user document and expose it via three new user-service NATS RPCs (`get`/`add`/`remove`), replicated cross-site through dedicated atomic inbox events.

**Architecture:** `priorityContacts []string` is embedded in `UserSettings` (already stored in `users.settings`). `user-service` gets a new `UserRepository` surface (`AddPriorityContact`/`RemovePriorityContact`/`GetPriorityContactAccounts`/`GetDirectoryInfoByAccounts`) backed by atomic Mongo `$addToSet`/`$pull`/projected reads, three new NATS handlers, and a new pair of cross-site `InboxEvent` types that `inbox-worker` applies with the same atomic ops (never a full-settings overwrite) on every other site.

**Tech Stack:** Go 1.25, NATS core request/reply, MongoDB (`go.mongodb.org/mongo-driver/v2`), `go.uber.org/mock` (mockgen), `stretchr/testify`, `testcontainers-go` (via `pkg/testutil`).

**Spec:** `docs/superpowers/specs/2026-08-07-priority-contacts-storage-api-design.md`

## Client contract (non-negotiable)

**All three RPCs return the fully enriched `[]PriorityContactItem` — the frontend makes exactly ONE call to render the priority-contacts list.** `.get`, `.add`, and `.remove` all reply with the caller's complete, enriched list (account + names + employee ID + org for humans, account + app name for bots). The frontend must never have to follow up with a user-lookup or app-lookup RPC to display a contact; all cross-collection enrichment happens server-side inside the handler.

Naming, to keep the layers unambiguous (they are NOT the same function):

| Layer | Name | Returns |
|---|---|---|
| Client-facing handler (`service/prioritycontacts.go`) | `UserService.GetPriorityContacts` | `[]models.PriorityContactItem` — enriched, what the FE receives |
| Repo (`mongorepo/users.go`) | `UserRepo.GetPriorityContactAccounts` | `[]string` — raw stored accounts, internal only |

The repo method cannot return `PriorityContactItem`: enrichment requires reading a second collection (`apps`), and CLAUDE.md forbids `$lookup`, so the join belongs in the service layer.

## Global Constraints

- Go 1.25, monorepo single `go.mod` at root; services are flat `package main` at repo root.
- Never run raw `go` commands — always `make <target>` (see CLAUDE.md §2).
- `pkg/errcode` for ALL client-facing errors; reply via `errnats.Reply`/`errhttp.Write`; raw `fmt.Errorf("...: %w", err)` for infra failures.
- Never compare errors by string — use `errors.Is`/`errors.As`.
- All NATS payloads are JSON with typed structs from `pkg/model` (via `encoding/json` outside the hot-path workers — user-service and inbox-worker are not sonic users).
- Every NATS event struct in `pkg/model` includes `Timestamp int64` set via `time.Now().UTC().UnixMilli()` at the publish site.
- Struct tags: `json` + `bson` on every model field, `camelCase` except `_id`.
- Mongo: no ORMs, native driver only; every find/aggregation has an explicit projection; no `$lookup`.
- TDD required: write the failing test first, confirm it fails, implement minimally, confirm it passes, commit.
- `make generate` after any store-interface change, before testing.
- Minimum 80% coverage; 90%+ target on handlers/store implementations.
- Changes to client-facing NATS handlers (`chat.user.{account}.request...`) require `docs/client-api.md` + derived views updated in the same PR.

## Deviations found while planning (beyond the spec)

These are implementation-level corrections found while grounding the plan in the actual code — no product/scope decision changes, just technical accuracy fixes on top of the approved spec:

1. **Model file location.** `UserSettings` lives in `pkg/model/usersettings.go`, not `pkg/model/event.go` (the spec said `event.go` for this field — it's right about `event.go` for the new `InboxEventType`/`PriorityContactChanged` additions, just wrong about where `UserSettings` itself lives).
2. **A 4th repo method is needed for enrichment.** The spec assumed the get handler's enrichment reuses the existing `UserRepository.GetHRInfoByAccounts`. That method returns `model.SubscriptionHRInfo{Account, Name, EngName}` — it has no `EmployeeID`/`SectName`, which `PriorityContactItem` needs. Rather than widen `SubscriptionHRInfo` (a type already used by unrelated DM-sidebar responses — widening it would ripple into `docs/client-api.md`'s existing DM subscription docs), Task 5 adds a new, purpose-built `UserRepo.GetDirectoryInfoByAccounts` returning `map[string]*model.User` (narrow-projected).
3. **Enrichment degrades, it doesn't error.** `user-service/service/subscriptions.go` already has this exact "split by `model.IsBot`, bulk-enrich, degrade to a nil map + log on failure" pattern (`lookupApps`/`lookupHRInfo`). Task 7 reuses `lookupApps` as-is and adds a `lookupPriorityContactDirectory` sibling with the same degrade-on-error style, instead of propagating enrichment errors as the spec's prose implied.

## File Structure

| File | Responsibility |
|---|---|
| `pkg/model/usersettings.go` | Add `PriorityContacts []string` to `UserSettings`. |
| `pkg/model/event.go` | Add `InboxPriorityContactAdded`/`InboxPriorityContactRemoved` + `PriorityContactChanged`. |
| `pkg/errcode/codes_user.go` | Add 3 new `Reason` constants. |
| `pkg/subject/subject.go` | Add 6 new subject builder functions (get/add/remove × concrete/pattern). |
| `user-service/mongorepo/users.go` | Add `AddPriorityContact`, `RemovePriorityContact`, `GetPriorityContactAccounts`, `GetDirectoryInfoByAccounts` to `UserRepo`. |
| `user-service/service/service.go` | Extend `UserRepository` interface; register 3 new handlers. |
| `user-service/models/prioritycontacts.go` (new) | Request/response structs. |
| `user-service/service/prioritycontacts.go` (new) | 3 NATS handlers + enrichment + cross-site publish helper. |
| `inbox-worker/handler.go` | Extend `InboxStore` interface; 2 new switch cases + handlers. |
| `inbox-worker/main.go` | `mongoInboxStore` implementations of the 2 new store methods. |
| `docs/client-api.md`, `docs/client-api/request-reply.md` | New endpoint docs. |

Test files: `pkg/model/usersettings_test.go`, `pkg/model/model_test.go`, `pkg/errcode/codes_user_test.go`, `pkg/subject/subject_test.go`, `user-service/mongorepo/users_test.go`, `user-service/service/prioritycontacts_test.go` (new), `inbox-worker/handler_test.go`, `inbox-worker/integration_test.go`.

---

### Task 1: Model — `PriorityContacts` field on `UserSettings`

**Files:**
- Modify: `pkg/model/usersettings.go`
- Test: `pkg/model/usersettings_test.go`

**Interfaces:**
- Produces: `model.UserSettings.PriorityContacts []string` (json/bson tag `priorityContacts,omitempty`).

- [ ] **Step 1: Write the failing tests**

Add to `pkg/model/usersettings_test.go`:

```go
// PriorityContacts round-trips over JSON and is omitted when unset — it is never
// touched by the generic settings.set path, only by the dedicated add/remove RPCs.
func TestUserSettings_PriorityContactsRoundTrip(t *testing.T) {
	in := UserSettings{PriorityContacts: []string{"bob", "helper.bot"}}
	data, err := json.Marshal(in)
	require.NoError(t, err)
	assert.JSONEq(t, `{"priorityContacts":["bob","helper.bot"]}`, string(data))

	var out UserSettings
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Equal(t, in, out)
}

func TestUserSettings_PriorityContactsOmittedWhenUnset(t *testing.T) {
	data, err := json.Marshal(UserSettings{FullWidth: ptrBoolForTest(true)})
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, present := raw["priorityContacts"]
	assert.False(t, present, "unset priorityContacts must be omitted, not null")
}

// BSON round-trip guards the bson tag used by the Mongo store + cross-site replica.
func TestUserSettings_PriorityContactsBSONRoundTrip(t *testing.T) {
	in := UserSettings{PriorityContacts: []string{"bob", "carol"}}
	data, err := bson.Marshal(in)
	require.NoError(t, err)
	var out UserSettings
	require.NoError(t, bson.Unmarshal(data, &out))
	assert.Equal(t, in, out)
}

func ptrBoolForTest(b bool) *bool { return &b }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=pkg/model`
Expected: FAIL — `PriorityContacts` undefined on `UserSettings`.

- [ ] **Step 3: Add the field**

In `pkg/model/usersettings.go`, add to the `UserSettings` struct (after `InitialChatScrollPosition`):

```go
	InitialChatScrollPosition        *string `json:"initialChatScrollPosition,omitempty"               bson:"initialChatScrollPosition,omitempty"`
	// PriorityContacts holds up to 30 account strings (regular users and .bot
	// accounts). Never written through SetSettings/UpdateUserSettings — mutated
	// only by the dedicated priority-contacts.add/.remove RPCs — so it is
	// deliberately excluded from IsEmpty()'s nil-check set below.
	PriorityContacts []string `json:"priorityContacts,omitempty" bson:"priorityContacts,omitempty"`
}
```

Do **not** add `PriorityContacts` to the `IsEmpty()` nil-check list — leave `IsEmpty()` exactly as it is today.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/model`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/model/usersettings.go pkg/model/usersettings_test.go
git commit -m "feat(model): add PriorityContacts field to UserSettings"
```

---

### Task 2: Model — cross-site inbox event for priority-contact changes

**Files:**
- Modify: `pkg/model/event.go`
- Test: `pkg/model/model_test.go`

**Interfaces:**
- Consumes: none.
- Produces: `model.InboxPriorityContactAdded`, `model.InboxPriorityContactRemoved` (both `InboxEventType = string`); `model.PriorityContactChanged{Account, ContactAccount string; Timestamp int64}`.

- [ ] **Step 1: Write the failing test**

Add to `pkg/model/model_test.go`, next to `TestUserSettingsUpdated_RoundTrip` (~line 4525):

```go
func TestPriorityContactChanged_RoundTrip(t *testing.T) {
	src := model.PriorityContactChanged{
		Account:        "alice",
		ContactAccount: "bob",
		Timestamp:      1735689600000,
	}
	dst := model.PriorityContactChanged{}
	roundTrip(t, &src, &dst)
}

func TestInboxPriorityContactEventTypes_WireValues(t *testing.T) {
	assert.Equal(t, "priority_contact_added", model.InboxPriorityContactAdded)
	assert.Equal(t, "priority_contact_removed", model.InboxPriorityContactRemoved)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model`
Expected: FAIL — `model.PriorityContactChanged`/`model.InboxPriorityContactAdded` undefined.

- [ ] **Step 3: Add the consts and struct**

In `pkg/model/event.go`, extend the `InboxEventType` const block (after `InboxUserSettingsUpdated`):

```go
	InboxUserStatusUpdated           InboxEventType = "user_status_updated"
	InboxUserSettingsUpdated         InboxEventType = "user_settings_updated"
	InboxPriorityContactAdded        InboxEventType = "priority_contact_added"
	InboxPriorityContactRemoved      InboxEventType = "priority_contact_removed"
)
```

Add a new struct after `UserSettingsUpdated`:

```go
// PriorityContactChanged is the cross-site inbox payload for one priority-contact
// add or remove (InboxPriorityContactAdded / InboxPriorityContactRemoved). The
// receiving site applies it as an atomic $addToSet/$pull, never a full-settings
// overwrite, so it can't race with a concurrent UserSettingsUpdated mirror for an
// unrelated field.
type PriorityContactChanged struct {
	Account        string `json:"account"        bson:"account"`
	ContactAccount string `json:"contactAccount" bson:"contactAccount"`
	Timestamp      int64  `json:"timestamp"      bson:"timestamp"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/model`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/model/event.go pkg/model/model_test.go
git commit -m "feat(model): add priority-contact cross-site inbox event"
```

---

### Task 3: errcode — priority-contact reason constants

**Files:**
- Modify: `pkg/errcode/codes_user.go`
- Test: `pkg/errcode/codes_user_test.go`

**Interfaces:**
- Produces: `errcode.UserPriorityContactSelf`, `errcode.UserPriorityContactNotFound`, `errcode.UserPriorityContactLimitReached` (all `errcode.Reason`).

- [ ] **Step 1: Write the failing test**

Replace `pkg/errcode/codes_user_test.go` with:

```go
package errcode

import "testing"

func TestUserReasons(t *testing.T) {
	cases := map[Reason]string{
		UserAppNotFound:                  "app_not_found",
		UserAppDisabled:                  "app_disabled",
		UserSubscriptionNotFound:         "subscription_not_found",
		UserSSOTokenNotFound:             "sso_token_not_found",
		UserPriorityContactSelf:          "priority_contact_self",
		UserPriorityContactNotFound:      "priority_contact_not_found",
		UserPriorityContactLimitReached:  "priority_contact_limit_reached",
	}
	for r, want := range cases {
		if string(r) != want {
			t.Errorf("reason %q != %q", string(r), want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/errcode`
Expected: FAIL — `UserPriorityContactSelf` (etc.) undefined.

- [ ] **Step 3: Add the constants**

In `pkg/errcode/codes_user.go`:

```go
package errcode

// User-domain reason constants; wire values are unprefixed (house style: RoomUserNotFound = "user_not_found").
const (
	UserAppNotFound          Reason = "app_not_found"
	UserAppDisabled          Reason = "app_disabled"
	UserSubscriptionNotFound Reason = "subscription_not_found"
	UserSSOTokenNotFound     Reason = "sso_token_not_found"

	// UserPriorityContactSelf: priority-contacts.add where contactAccount equals the caller.
	UserPriorityContactSelf Reason = "priority_contact_self"
	// UserPriorityContactNotFound: priority-contacts.add where contactAccount doesn't resolve
	// to an active user or an app with an enabled assistant.
	UserPriorityContactNotFound Reason = "priority_contact_not_found"
	// UserPriorityContactLimitReached: priority-contacts.add where the caller already has 30 entries.
	UserPriorityContactLimitReached Reason = "priority_contact_limit_reached"
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/errcode`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/errcode/codes_user.go pkg/errcode/codes_user_test.go
git commit -m "feat(errcode): add priority-contact reason constants"
```

---

### Task 4: subject — priority-contacts RPC subject builders

**Files:**
- Modify: `pkg/subject/subject.go`
- Test: `pkg/subject/subject_test.go`

**Interfaces:**
- Produces: `subject.PriorityContactsGet(account, siteID string) string`, `subject.PriorityContactsGetPattern(siteID string) string`, and the `Add`/`Remove` siblings of both.

- [ ] **Step 1: Write the failing tests**

In `pkg/subject/subject_test.go`, add rows to the three existing user-service tables.

In `TestUserServiceBuilders` (~line 606), add before the closing `}`:

```go
		{"priority-contacts.get", subject.PriorityContactsGet("alice", "s1"), "chat.user.alice.request.user.s1.priority-contacts.get"},
		{"priority-contacts.add", subject.PriorityContactsAdd("alice", "s1"), "chat.user.alice.request.user.s1.priority-contacts.add"},
		{"priority-contacts.remove", subject.PriorityContactsRemove("alice", "s1"), "chat.user.alice.request.user.s1.priority-contacts.remove"},
```

In `TestUserServiceBuildersRejectWildcardAccounts` (~line 779), add:

```go
		{"PriorityContactsGet", func() { subject.PriorityContactsGet("*", "s1") }},
		{"PriorityContactsAdd", func() { subject.PriorityContactsAdd(">", "s1") }},
		{"PriorityContactsRemove", func() { subject.PriorityContactsRemove("*", "s1") }},
```

In `TestUserServicePatternBuilders` (~line 920), add:

```go
		{"priority-contacts.get", subject.PriorityContactsGetPattern("s1"), "chat.user.{account}.request.user.s1.priority-contacts.get"},
		{"priority-contacts.add", subject.PriorityContactsAddPattern("s1"), "chat.user.{account}.request.user.s1.priority-contacts.add"},
		{"priority-contacts.remove", subject.PriorityContactsRemovePattern("s1"), "chat.user.{account}.request.user.s1.priority-contacts.remove"},
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=pkg/subject`
Expected: FAIL — `subject.PriorityContactsGet` (etc.) undefined.

- [ ] **Step 3: Add the builder functions**

In `pkg/subject/subject.go`, immediately after `UserSettingsSetPattern` (~line 1345):

```go
func PriorityContactsGet(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.priority-contacts.get", account, siteID)
}

func PriorityContactsGetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.priority-contacts.get", siteID)
}

func PriorityContactsAdd(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.priority-contacts.add", account, siteID)
}

func PriorityContactsAddPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.priority-contacts.add", siteID)
}

func PriorityContactsRemove(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.priority-contacts.remove", account, siteID)
}

func PriorityContactsRemovePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.priority-contacts.remove", siteID)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/subject`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add priority-contacts RPC subject builders"
```

---

### Task 5: mongorepo — store methods on `UserRepo`

**Files:**
- Modify: `user-service/mongorepo/users.go`
- Test: `user-service/mongorepo/users_test.go`

**Interfaces:**
- Consumes: `mongoutil.Collection[model.User]` (`r.users`), `activeUserFilter(account) bson.M` (existing, `users.go:53`).
- Produces:
  - `func (r *UserRepo) AddPriorityContact(ctx context.Context, account, contactAccount string) (*model.User, error)`
  - `func (r *UserRepo) RemovePriorityContact(ctx context.Context, account, contactAccount string) (*model.User, error)`
  - `func (r *UserRepo) GetPriorityContactAccounts(ctx context.Context, account string) ([]string, error)` — **contract:** `nil, nil` when no active user matched; `[]string{}, nil` when the user exists but has never set the field; the stored slice otherwise. Never truncates.
  - `func (r *UserRepo) GetDirectoryInfoByAccounts(ctx context.Context, accounts []string) (map[string]*model.User, error)` — narrow projection (`account`, `chineseName`, `engName`, `employeeId`, `sectName`); all other `model.User` fields are zero-valued. Accounts with no doc are omitted from the map.

- [ ] **Step 1: Write the failing tests**

Add to `user-service/mongorepo/users_test.go`:

```go
func TestAddPriorityContact_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users",
		bson.M{"_id": "u-alice", "account": "alice", "active": true},
		bson.M{"_id": "u-ghost", "account": "ghost", "active": false},
	)

	t.Run("adds to empty list", func(t *testing.T) {
		u, err := r.AddPriorityContact(ctx, "alice", "bob")
		require.NoError(t, err)
		require.NotNil(t, u)
		require.NotNil(t, u.Settings)
		assert.Equal(t, []string{"bob"}, u.Settings.PriorityContacts)
	})

	t.Run("idempotent re-add", func(t *testing.T) {
		u, err := r.AddPriorityContact(ctx, "alice", "bob")
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Equal(t, []string{"bob"}, u.Settings.PriorityContacts)
	})

	t.Run("inactive user not found", func(t *testing.T) {
		u, err := r.AddPriorityContact(ctx, "ghost", "bob")
		require.NoError(t, err)
		assert.Nil(t, u)
	})

	t.Run("missing user not found", func(t *testing.T) {
		u, err := r.AddPriorityContact(ctx, "nobody", "bob")
		require.NoError(t, err)
		assert.Nil(t, u)
	})
}

func TestRemovePriorityContact_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users",
		bson.M{"_id": "u-alice", "account": "alice", "active": true,
			"settings": bson.M{"priorityContacts": bson.A{"bob", "carol"}}},
	)

	t.Run("removes existing entry", func(t *testing.T) {
		u, err := r.RemovePriorityContact(ctx, "alice", "bob")
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Equal(t, []string{"carol"}, u.Settings.PriorityContacts)
	})

	t.Run("no-op on missing entry", func(t *testing.T) {
		u, err := r.RemovePriorityContact(ctx, "alice", "nobody")
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Equal(t, []string{"carol"}, u.Settings.PriorityContacts)
	})

	t.Run("missing user not found", func(t *testing.T) {
		u, err := r.RemovePriorityContact(ctx, "nobody", "bob")
		require.NoError(t, err)
		assert.Nil(t, u)
	})
}

func TestGetPriorityContactAccounts_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	over30 := make(bson.A, 31)
	for i := range over30 {
		over30[i] = fmt.Sprintf("contact-%02d", i)
	}
	seed(t, db, "users",
		bson.M{"_id": "u-alice", "account": "alice", "active": true,
			"settings": bson.M{"priorityContacts": bson.A{"bob"}}},
		bson.M{"_id": "u-noset", "account": "noset", "active": true},
		bson.M{"_id": "u-over", "account": "over", "active": true,
			"settings": bson.M{"priorityContacts": over30}},
	)

	t.Run("returns stored list", func(t *testing.T) {
		got, err := r.GetPriorityContactAccounts(ctx, "alice")
		require.NoError(t, err)
		assert.Equal(t, []string{"bob"}, got)
	})

	t.Run("empty slice, not nil, when field never set", func(t *testing.T) {
		got, err := r.GetPriorityContactAccounts(ctx, "noset")
		require.NoError(t, err)
		assert.NotNil(t, got)
		assert.Empty(t, got)
	})

	t.Run("nil when no active user matched", func(t *testing.T) {
		got, err := r.GetPriorityContactAccounts(ctx, "nobody")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("never truncates, even over the 30 cap", func(t *testing.T) {
		got, err := r.GetPriorityContactAccounts(ctx, "over")
		require.NoError(t, err)
		assert.Len(t, got, 31, "GetPriorityContactAccounts must return every stored entry, uncapped")
	})
}

func TestGetDirectoryInfoByAccounts_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users",
		bson.M{"_id": "u-bob", "account": "bob", "active": true,
			"chineseName": "鮑勃", "engName": "Bob", "employeeId": "E1", "sectName": "Engineering"},
	)

	t.Run("found account", func(t *testing.T) {
		got, err := r.GetDirectoryInfoByAccounts(ctx, []string{"bob", "ghost"})
		require.NoError(t, err)
		require.Contains(t, got, "bob")
		assert.Equal(t, "Bob", got["bob"].EngName)
		assert.Equal(t, "鮑勃", got["bob"].ChineseName)
		assert.Equal(t, "E1", got["bob"].EmployeeID)
		assert.Equal(t, "Engineering", got["bob"].SectName)
	})

	t.Run("missing account omitted, not errored", func(t *testing.T) {
		got, err := r.GetDirectoryInfoByAccounts(ctx, []string{"ghost"})
		require.NoError(t, err)
		assert.NotContains(t, got, "ghost")
	})

	t.Run("empty input returns empty map", func(t *testing.T) {
		got, err := r.GetDirectoryInfoByAccounts(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}
```

Add `"fmt"` to the test file's import block if not already present.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test-integration SERVICE=user-service`
Expected: FAIL (build error) — the four new `UserRepo` methods are undefined.

- [ ] **Step 3: Implement the four methods**

In `user-service/mongorepo/users.go`, append after `UpdateUserSettings` (before `SetUserStatus`):

```go
// AddPriorityContact atomically appends contactAccount to settings.priorityContacts via
// $addToSet (idempotent — re-adding an existing contact is a no-op) and returns the updated
// user (Settings projected) in one round-trip; (nil, nil) when no active user matched.
func (r *UserRepo) AddPriorityContact(ctx context.Context, account, contactAccount string) (*model.User, error) {
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetProjection(bson.M{"_id": 0, "settings": 1})
	res := r.users.Raw().FindOneAndUpdate(ctx, activeUserFilter(account),
		bson.M{"$addToSet": bson.M{"settings.priorityContacts": contactAccount}}, opts)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("add priority contact: %w", err)
	}
	var u model.User
	if err := res.Decode(&u); err != nil {
		return nil, fmt.Errorf("decode updated priority contacts: %w", err)
	}
	return &u, nil
}

// RemovePriorityContact atomically removes contactAccount from settings.priorityContacts via
// $pull (idempotent — removing a non-existent entry is a no-op) and returns the updated user
// (Settings projected) in one round-trip; (nil, nil) when no active user matched.
func (r *UserRepo) RemovePriorityContact(ctx context.Context, account, contactAccount string) (*model.User, error) {
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetProjection(bson.M{"_id": 0, "settings": 1})
	res := r.users.Raw().FindOneAndUpdate(ctx, activeUserFilter(account),
		bson.M{"$pull": bson.M{"settings.priorityContacts": contactAccount}}, opts)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("remove priority contact: %w", err)
	}
	var u model.User
	if err := res.Decode(&u); err != nil {
		return nil, fmt.Errorf("decode updated priority contacts: %w", err)
	}
	return &u, nil
}

// GetPriorityContactAccounts returns settings.priorityContacts for account, uncapped and untruncated:
// nil when no active user matched, []string{} (never nil) when the user exists but has never
// set the field, the stored slice otherwise — including any entry count, even above the
// 30-entry write-time cap (a rare add/add race can briefly exceed it; callers must not clip).
func (r *UserRepo) GetPriorityContactAccounts(ctx context.Context, account string) ([]string, error) {
	u, err := r.users.FindOne(ctx, activeUserFilter(account),
		mongoutil.WithProjection(bson.M{"_id": 0, "settings.priorityContacts": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("get priority contacts: %w", err)
	}
	if u == nil {
		return nil, nil
	}
	if u.Settings == nil || u.Settings.PriorityContacts == nil {
		return []string{}, nil
	}
	return u.Settings.PriorityContacts, nil
}

// GetDirectoryInfoByAccounts maps account → the subset of the users doc needed to enrich a
// priority-contacts response (chineseName/engName/employeeId/sectName); all other model.User
// fields are zero-valued — this is a narrow projection, not a full user record. Distinct from
// GetHRInfoByAccounts (which returns the narrower SubscriptionHRInfo shape used by DM sidebar
// enrichment and lacks employeeId/sectName). Accounts with no users doc are omitted.
func (r *UserRepo) GetDirectoryInfoByAccounts(ctx context.Context, accounts []string) (map[string]*model.User, error) {
	if len(accounts) == 0 {
		return map[string]*model.User{}, nil
	}
	rows, err := r.users.FindMany(ctx,
		bson.M{"account": bson.M{"$in": accounts}},
		mongoutil.WithProjection(bson.M{"_id": 0, "account": 1, "chineseName": 1, "engName": 1, "employeeId": 1, "sectName": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("find directory info by accounts: %w", err)
	}
	out := make(map[string]*model.User, len(rows))
	for i := range rows {
		row := rows[i]
		out[row.Account] = &row
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test-integration SERVICE=user-service`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add user-service/mongorepo/users.go user-service/mongorepo/users_test.go
git commit -m "feat(user-service): add priority-contacts store methods to UserRepo"
```

---

### Task 6: service interface + response models

**Files:**
- Modify: `user-service/service/service.go`
- Create: `user-service/models/prioritycontacts.go`

**Interfaces:**
- Consumes: Task 5's four `UserRepo` methods (satisfying the extended interface).
- Produces:
  - `UserRepository` interface gains `AddPriorityContact`, `RemovePriorityContact`, `GetPriorityContactAccounts`, `GetDirectoryInfoByAccounts` (same signatures as Task 5).
  - `models.PriorityContactAddRequest{ContactAccount string}`
  - `models.PriorityContactRemoveRequest{ContactAccount string}`
  - `models.PriorityContactItem{Account, EngName, ChineseName, EmployeeID, SectName, AppName string}`

- [ ] **Step 1: Extend the `UserRepository` interface**

In `user-service/service/service.go`, change:

```go
// UserRepository is the consumer-defined interface for user status persistence.
type UserRepository interface {
	GetUserStatus(ctx context.Context, account string) (*model.User, error)
	SetUserStatus(ctx context.Context, account, text string, isShow *bool) (*model.User, error)
	GetHRInfoByAccounts(ctx context.Context, accounts []string) (map[string]*model.SubscriptionHRInfo, error)
	GetUserSettings(ctx context.Context, account string) (*model.User, error)
	UpdateUserSettings(ctx context.Context, account string, set *model.UserSettings) (*model.User, error)
}
```

to:

```go
// UserRepository is the consumer-defined interface for user status persistence.
type UserRepository interface {
	GetUserStatus(ctx context.Context, account string) (*model.User, error)
	SetUserStatus(ctx context.Context, account, text string, isShow *bool) (*model.User, error)
	GetHRInfoByAccounts(ctx context.Context, accounts []string) (map[string]*model.SubscriptionHRInfo, error)
	GetUserSettings(ctx context.Context, account string) (*model.User, error)
	UpdateUserSettings(ctx context.Context, account string, set *model.UserSettings) (*model.User, error)
	// AddPriorityContact atomically appends contactAccount to settings.priorityContacts via
	// $addToSet. Returns (nil, nil) when no active user matched.
	AddPriorityContact(ctx context.Context, account, contactAccount string) (*model.User, error)
	// RemovePriorityContact atomically removes contactAccount from settings.priorityContacts
	// via $pull. Returns (nil, nil) when no active user matched.
	RemovePriorityContact(ctx context.Context, account, contactAccount string) (*model.User, error)
	// GetPriorityContactAccounts returns settings.priorityContacts for account: nil when no active
	// user matched, []string{} (never nil) when set but empty, the stored (uncapped) slice
	// otherwise.
	GetPriorityContactAccounts(ctx context.Context, account string) ([]string, error)
	// GetDirectoryInfoByAccounts maps account → a narrow users-doc projection
	// (chineseName/engName/employeeId/sectName) for priority-contacts enrichment.
	GetDirectoryInfoByAccounts(ctx context.Context, accounts []string) (map[string]*model.User, error)
}
```

No other change to `service.go` in this step — `AppRepository` and its injection already exist (`s.apps`, wired in `New`).

- [ ] **Step 2: Regenerate mocks**

Run: `make generate SERVICE=user-service`

This regenerates `user-service/service/mocks/mock_repository.go` (via the `//go:generate` directive at `service.go:16`) to add `MockUserRepository.AddPriorityContact`/`RemovePriorityContact`/`GetPriorityContactAccounts`/`GetDirectoryInfoByAccounts`.

- [ ] **Step 3: Verify the package still builds**

Run: `make test SERVICE=user-service`
Expected: PASS (no new behavior yet, just interface + regenerated mocks — existing tests are unaffected).

- [ ] **Step 4: Create the response/request models**

Create `user-service/models/prioritycontacts.go`:

```go
package models

// PriorityContactAddRequest is the body of priority-contacts.add.
type PriorityContactAddRequest struct {
	ContactAccount string `json:"contactAccount"`
}

// PriorityContactRemoveRequest is the body of priority-contacts.remove.
type PriorityContactRemoveRequest struct {
	ContactAccount string `json:"contactAccount"`
}

// PriorityContactItem is the enriched response shape for one priority contact, returned by
// priority-contacts.get/.add/.remove. No AvatarURL: consistent with model.Participant,
// model.SubscriptionHRInfo, and model.App — none of the existing enriched response types carry
// an avatar URL inline; the frontend resolves avatars from Account via the existing convention.
type PriorityContactItem struct {
	Account string `json:"account"`

	// Populated for regular users (Account does not end in ".bot"):
	EngName     string `json:"engName,omitempty"`
	ChineseName string `json:"chineseName,omitempty"`
	EmployeeID  string `json:"employeeId,omitempty"`
	SectName    string `json:"sectName,omitempty"`

	// Populated for App bots (Account ends in ".bot"):
	AppName string `json:"appName,omitempty"`
}
```

No test file for this step — it's plain data structs exercised end-to-end by Task 7's handler tests.

- [ ] **Step 5: Commit**

```bash
git add user-service/service/service.go user-service/service/mocks/mock_repository.go user-service/models/prioritycontacts.go
git commit -m "feat(user-service): extend UserRepository for priority contacts, add response models"
```

---

### Task 7: service handlers — get/add/remove + enrichment + cross-site publish

**Files:**
- Create: `user-service/service/prioritycontacts.go`
- Modify: `user-service/service/service.go` (register handlers)
- Test: `user-service/service/prioritycontacts_test.go` (new)

**Interfaces:**
- Consumes: `UserRepository` (Task 6), `AppRepository.GetAppsByAssistants` (existing), `s.lookupApps` (existing, `subscriptions.go`), `s.publishSettingsUpdate` (existing, `settings.go`), `s.pub EventPublisher`, `s.allSiteIDs`, `s.siteID`, `subject.PriorityContactsGetPattern`/`AddPattern`/`RemovePattern` (Task 4), `errcode.UserPriorityContactSelf`/`NotFound`/`LimitReached` (Task 3), `model.PriorityContactChanged`/`InboxPriorityContactAdded`/`Removed` (Task 2).
- Produces:
  - `func (s *UserService) GetPriorityContacts(c *natsrouter.Context) ([]models.PriorityContactItem, error)`
  - `func (s *UserService) AddPriorityContact(c *natsrouter.Context, req models.PriorityContactAddRequest) ([]models.PriorityContactItem, error)`
  - `func (s *UserService) RemovePriorityContact(c *natsrouter.Context, req models.PriorityContactRemoveRequest) ([]models.PriorityContactItem, error)`

- [ ] **Step 1: Write the failing tests**

Create `user-service/service/prioritycontacts_test.go`:

```go
package service

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/models"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

func expectPriorityContactInbox(pub *mocks.MockEventPublisher, eventType string) {
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", eventType), gomock.Any()).Return(nil).AnyTimes()
}

// --- GetPriorityContacts ---

func TestGetPriorityContacts_Empty(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetPriorityContactAccounts(gomock.Any(), "alice").Return([]string{}, nil)
	items, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	require.NoError(t, err)
	assert.Empty(t, items)
}

func TestGetPriorityContacts_MixedUserAndBotEnrichment(t *testing.T) {
	svc, _, users, apps, _, _, _ := newSvc(t)
	users.EXPECT().GetPriorityContactAccounts(gomock.Any(), "alice").Return([]string{"bob", "helper.bot"}, nil)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), []string{"bob"}).
		Return(map[string]*model.User{"bob": {Account: "bob", EngName: "Bob", ChineseName: "鮑勃", EmployeeID: "E1", SectName: "Eng"}}, nil)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(map[string]*model.App{"helper.bot": {Name: "Helper Bot"}}, nil)

	items, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	require.NoError(t, err)
	require.Len(t, items, 2)
	assert.Equal(t, models.PriorityContactItem{Account: "bob", EngName: "Bob", ChineseName: "鮑勃", EmployeeID: "E1", SectName: "Eng"}, items[0])
	assert.Equal(t, models.PriorityContactItem{Account: "helper.bot", AppName: "Helper Bot"}, items[1])
}

func TestGetPriorityContacts_MissingDirectoryEntryIsAccountOnly(t *testing.T) {
	svc, _, users, apps, _, _, _ := newSvc(t)
	users.EXPECT().GetPriorityContactAccounts(gomock.Any(), "alice").Return([]string{"ghost"}, nil)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), []string{"ghost"}).Return(map[string]*model.User{}, nil)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), nil).Return(map[string]*model.App{}, nil).AnyTimes()

	items, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, models.PriorityContactItem{Account: "ghost"}, items[0])
}

func TestGetPriorityContacts_PreservesStoredOrder(t *testing.T) {
	svc, _, users, apps, _, _, _ := newSvc(t)
	users.EXPECT().GetPriorityContactAccounts(gomock.Any(), "alice").Return([]string{"carol", "bob"}, nil)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), []string{"carol", "bob"}).
		Return(map[string]*model.User{
			"carol": {Account: "carol", EngName: "Carol"},
			"bob":   {Account: "bob", EngName: "Bob"},
		}, nil)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), nil).Return(map[string]*model.App{}, nil).AnyTimes()

	items, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	require.NoError(t, err)
	require.Len(t, items, 2)
	assert.Equal(t, "carol", items[0].Account)
	assert.Equal(t, "bob", items[1].Account)
}

func TestGetPriorityContacts_NotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetPriorityContactAccounts(gomock.Any(), "ghost").Return(nil, nil)
	_, err := svc.GetPriorityContacts(ctx("ghost", "site-a"))
	requireCode(t, err, errcode.CodeNotFound)
}

func TestGetPriorityContacts_StoreError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetPriorityContactAccounts(gomock.Any(), "alice").Return(nil, errors.New("db unavailable"))
	_, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee), "store errors must stay raw")
}

// --- AddPriorityContact ---

func TestAddPriorityContact_HappyPath_User(t *testing.T) {
	svc, _, users, apps, _, _, pub := newSvc(t)
	expectPriorityContactInbox(pub, model.InboxPriorityContactAdded)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), []string{"bob"}).
		Return(map[string]*model.User{"bob": {Account: "bob"}}, nil)
	users.EXPECT().GetPriorityContactAccounts(gomock.Any(), "alice").Return([]string{}, nil)
	updated := &model.UserSettings{PriorityContacts: []string{"bob"}}
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob").Return(&model.User{Settings: updated}, nil)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), []string{"bob"}).Return(map[string]*model.User{"bob": {Account: "bob", EngName: "Bob"}}, nil)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), nil).Return(map[string]*model.App{}, nil).AnyTimes()
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).Return(nil)

	items, err := svc.AddPriorityContact(ctx("alice", "site-a"), models.PriorityContactAddRequest{ContactAccount: "bob"})
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "bob", items[0].Account)
}

func TestAddPriorityContact_HappyPath_Bot(t *testing.T) {
	svc, _, users, apps, _, _, pub := newSvc(t)
	expectPriorityContactInbox(pub, model.InboxPriorityContactAdded)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(map[string]*model.App{"helper.bot": {Name: "Helper Bot", Assistant: &model.AppAssistant{Enabled: true, Name: "helper.bot"}}}, nil)
	users.EXPECT().GetPriorityContactAccounts(gomock.Any(), "alice").Return([]string{}, nil)
	updated := &model.UserSettings{PriorityContacts: []string{"helper.bot"}}
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "helper.bot").Return(&model.User{Settings: updated}, nil)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), nil).Return(map[string]*model.User{}, nil).AnyTimes()
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(map[string]*model.App{"helper.bot": {Name: "Helper Bot"}}, nil)
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).Return(nil)

	items, err := svc.AddPriorityContact(ctx("alice", "site-a"), models.PriorityContactAddRequest{ContactAccount: "helper.bot"})
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "Helper Bot", items[0].AppName)
}

func TestAddPriorityContact_EmptyContactAccount(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	_, err := svc.AddPriorityContact(ctx("alice", "site-a"), models.PriorityContactAddRequest{})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestAddPriorityContact_Self(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	_, err := svc.AddPriorityContact(ctx("alice", "site-a"), models.PriorityContactAddRequest{ContactAccount: "alice"})
	requireCode(t, err, errcode.CodeBadRequest)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.UserPriorityContactSelf, ee.Reason)
}

func TestAddPriorityContact_NonexistentUser(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), []string{"ghost"}).Return(map[string]*model.User{}, nil)
	_, err := svc.AddPriorityContact(ctx("alice", "site-a"), models.PriorityContactAddRequest{ContactAccount: "ghost"})
	requireCode(t, err, errcode.CodeNotFound)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.UserPriorityContactNotFound, ee.Reason)
}

func TestAddPriorityContact_NonexistentBot(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"ghost.bot"}).Return(map[string]*model.App{}, nil)
	_, err := svc.AddPriorityContact(ctx("alice", "site-a"), models.PriorityContactAddRequest{ContactAccount: "ghost.bot"})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestAddPriorityContact_DisabledBot(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(map[string]*model.App{"helper.bot": {Name: "Helper Bot", Assistant: &model.AppAssistant{Enabled: false, Name: "helper.bot"}}}, nil)
	_, err := svc.AddPriorityContact(ctx("alice", "site-a"), models.PriorityContactAddRequest{ContactAccount: "helper.bot"})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestAddPriorityContact_At29Succeeds(t *testing.T) {
	svc, _, users, apps, _, _, pub := newSvc(t)
	expectPriorityContactInbox(pub, model.InboxPriorityContactAdded)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), []string{"bob"}).Return(map[string]*model.User{"bob": {Account: "bob"}}, nil)
	// Values are irrelevant to the cap check (len(current) >= maxPriorityContacts) — 29
	// identical placeholders are fine.
	current := make([]string, 29)
	for i := range current {
		current[i] = "c"
	}
	users.EXPECT().GetPriorityContactAccounts(gomock.Any(), "alice").Return(current, nil)
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob").Return(&model.User{Settings: &model.UserSettings{PriorityContacts: append(current, "bob")}}, nil)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), gomock.Any()).Return(map[string]*model.User{}, nil).AnyTimes()
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), nil).Return(map[string]*model.App{}, nil).AnyTimes()
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).Return(nil)

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"), models.PriorityContactAddRequest{ContactAccount: "bob"})
	require.NoError(t, err)
}

func TestAddPriorityContact_At30Rejected(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), []string{"bob"}).Return(map[string]*model.User{"bob": {Account: "bob"}}, nil)
	current := make([]string, 30)
	users.EXPECT().GetPriorityContactAccounts(gomock.Any(), "alice").Return(current, nil)

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"), models.PriorityContactAddRequest{ContactAccount: "bob"})
	requireCode(t, err, errcode.CodeForbidden)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.UserPriorityContactLimitReached, ee.Reason)
}

func TestAddPriorityContact_RepoError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), []string{"bob"}).Return(map[string]*model.User{"bob": {Account: "bob"}}, nil)
	users.EXPECT().GetPriorityContactAccounts(gomock.Any(), "alice").Return(nil, errors.New("db unavailable"))
	_, err := svc.AddPriorityContact(ctx("alice", "site-a"), models.PriorityContactAddRequest{ContactAccount: "bob"})
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee))
}

func TestAddPriorityContact_PublishesBothFanouts(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), []string{"bob"}).Return(map[string]*model.User{"bob": {Account: "bob"}}, nil)
	users.EXPECT().GetPriorityContactAccounts(gomock.Any(), "alice").Return([]string{}, nil)
	updated := &model.UserSettings{PriorityContacts: []string{"bob"}}
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob").Return(&model.User{Settings: updated}, nil)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), gomock.Any()).Return(map[string]*model.User{}, nil).AnyTimes()

	var clientTS int64
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.SettingsUpdateEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			clientTS = evt.Timestamp
			assert.Equal(t, *updated, evt.Settings)
			return nil
		})
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxPriorityContactAdded), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, "site-a", evt.SiteID)
			assert.Equal(t, "site-b", evt.DestSiteID)
			var p model.PriorityContactChanged
			require.NoError(t, json.Unmarshal(evt.Payload, &p))
			assert.Equal(t, "alice", p.Account)
			assert.Equal(t, "bob", p.ContactAccount)
			assert.Equal(t, clientTS, p.Timestamp, "both fanouts must share one timestamp")
			return nil
		})

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"), models.PriorityContactAddRequest{ContactAccount: "bob"})
	require.NoError(t, err)
}

// --- RemovePriorityContact ---

func TestRemovePriorityContact_HappyPath(t *testing.T) {
	svc, _, users, apps, _, _, pub := newSvc(t)
	expectPriorityContactInbox(pub, model.InboxPriorityContactRemoved)
	updated := &model.UserSettings{PriorityContacts: []string{}}
	users.EXPECT().RemovePriorityContact(gomock.Any(), "alice", "bob").Return(&model.User{Settings: updated}, nil)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), nil).Return(map[string]*model.User{}, nil).AnyTimes()
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), nil).Return(map[string]*model.App{}, nil).AnyTimes()
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).Return(nil)

	items, err := svc.RemovePriorityContact(ctx("alice", "site-a"), models.PriorityContactRemoveRequest{ContactAccount: "bob"})
	require.NoError(t, err)
	assert.Empty(t, items)
}

func TestRemovePriorityContact_EmptyContactAccount(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	_, err := svc.RemovePriorityContact(ctx("alice", "site-a"), models.PriorityContactRemoveRequest{})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestRemovePriorityContact_NoOpOnMissingEntryStillReturns200(t *testing.T) {
	svc, _, users, apps, _, _, pub := newSvc(t)
	expectPriorityContactInbox(pub, model.InboxPriorityContactRemoved)
	updated := &model.UserSettings{PriorityContacts: []string{"carol"}}
	users.EXPECT().RemovePriorityContact(gomock.Any(), "alice", "nobody").Return(&model.User{Settings: updated}, nil)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), []string{"carol"}).Return(map[string]*model.User{}, nil)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), nil).Return(map[string]*model.App{}, nil).AnyTimes()
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).Return(nil)

	items, err := svc.RemovePriorityContact(ctx("alice", "site-a"), models.PriorityContactRemoveRequest{ContactAccount: "nobody"})
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "carol", items[0].Account)
}

func TestRemovePriorityContact_NotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().RemovePriorityContact(gomock.Any(), "ghost", "bob").Return(nil, nil)
	_, err := svc.RemovePriorityContact(ctx("ghost", "site-a"), models.PriorityContactRemoveRequest{ContactAccount: "bob"})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestRemovePriorityContact_RepoError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)
	users.EXPECT().RemovePriorityContact(gomock.Any(), "alice", "bob").Return(nil, errors.New("db unavailable"))
	_, err := svc.RemovePriorityContact(ctx("alice", "site-a"), models.PriorityContactRemoveRequest{ContactAccount: "bob"})
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee))
}

func TestRemovePriorityContact_PublishesBothFanouts(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)
	updated := &model.UserSettings{PriorityContacts: []string{}}
	users.EXPECT().RemovePriorityContact(gomock.Any(), "alice", "bob").Return(&model.User{Settings: updated}, nil)
	users.EXPECT().GetDirectoryInfoByAccounts(gomock.Any(), nil).Return(map[string]*model.User{}, nil).AnyTimes()

	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).Return(nil)
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxPriorityContactRemoved), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			var p model.PriorityContactChanged
			require.NoError(t, json.Unmarshal(evt.Payload, &p))
			assert.Equal(t, "alice", p.Account)
			assert.Equal(t, "bob", p.ContactAccount)
			return nil
		})

	_, err := svc.RemovePriorityContact(ctx("alice", "site-a"), models.PriorityContactRemoveRequest{ContactAccount: "bob"})
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=user-service`
Expected: FAIL (build error) — `svc.GetPriorityContacts`/`AddPriorityContact`/`RemovePriorityContact` undefined.

- [ ] **Step 3: Implement the handlers**

Create `user-service/service/prioritycontacts.go`:

```go
package service

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/models"
)

const maxPriorityContacts = 30

// GetPriorityContacts returns the caller's priority contacts, enriched with directory info.
func (s *UserService) GetPriorityContacts(c *natsrouter.Context) ([]models.PriorityContactItem, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	contacts, err := s.users.GetPriorityContactAccounts(c, account)
	if err != nil {
		return nil, fmt.Errorf("get priority contacts: %w", err)
	}
	if contacts == nil {
		return nil, errcode.NotFound("user not found")
	}
	return s.enrichPriorityContacts(c, contacts), nil
}

// AddPriorityContact adds contactAccount to the caller's priority contacts (idempotent,
// capped at maxPriorityContacts), then fans the change out locally and cross-site.
func (s *UserService) AddPriorityContact(c *natsrouter.Context, req models.PriorityContactAddRequest) ([]models.PriorityContactItem, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	contactAccount := req.ContactAccount
	if contactAccount == "" {
		return nil, errcode.BadRequest("contact account required")
	}
	if contactAccount == account {
		return nil, errcode.BadRequest("cannot add self", errcode.WithReason(errcode.UserPriorityContactSelf))
	}
	if err := s.validatePriorityContactExists(c, contactAccount); err != nil {
		return nil, err
	}
	current, err := s.users.GetPriorityContactAccounts(c, account)
	if err != nil {
		return nil, fmt.Errorf("get priority contacts: %w", err)
	}
	if current == nil {
		return nil, errcode.NotFound("user not found")
	}
	if len(current) >= maxPriorityContacts {
		return nil, errcode.Forbidden("priority contact limit reached", errcode.WithReason(errcode.UserPriorityContactLimitReached))
	}
	u, err := s.users.AddPriorityContact(c, account, contactAccount)
	if err != nil {
		return nil, fmt.Errorf("add priority contact: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	settings := u.Settings
	if settings == nil {
		// Unreachable after a non-empty $addToSet; keep the reply shape total.
		settings = &model.UserSettings{}
	}
	now := time.Now().UTC().UnixMilli()
	s.publishSettingsUpdate(c, account, settings, now)
	s.publishPriorityContactInbox(c, account, contactAccount, model.InboxPriorityContactAdded, now)
	return s.enrichPriorityContacts(c, settings.PriorityContacts), nil
}

// RemovePriorityContact removes contactAccount from the caller's priority contacts (idempotent,
// no cap check — removal always succeeds even over the limit), then fans the change out locally
// and cross-site.
func (s *UserService) RemovePriorityContact(c *natsrouter.Context, req models.PriorityContactRemoveRequest) ([]models.PriorityContactItem, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	contactAccount := req.ContactAccount
	if contactAccount == "" {
		return nil, errcode.BadRequest("contact account required")
	}
	u, err := s.users.RemovePriorityContact(c, account, contactAccount)
	if err != nil {
		return nil, fmt.Errorf("remove priority contact: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	settings := u.Settings
	if settings == nil {
		settings = &model.UserSettings{}
	}
	now := time.Now().UTC().UnixMilli()
	s.publishSettingsUpdate(c, account, settings, now)
	s.publishPriorityContactInbox(c, account, contactAccount, model.InboxPriorityContactRemoved, now)
	return s.enrichPriorityContacts(c, settings.PriorityContacts), nil
}

// validatePriorityContactExists rejects an add whose contactAccount doesn't resolve to a real
// user or an app with an enabled assistant — otherwise a typo or a since-deleted account would
// silently occupy one of the maxPriorityContacts slots forever with no user-visible error.
func (s *UserService) validatePriorityContactExists(c *natsrouter.Context, contactAccount string) error {
	if model.IsBot(contactAccount) {
		apps, err := s.apps.GetAppsByAssistants(c, []string{contactAccount})
		if err != nil {
			return fmt.Errorf("look up bot contact: %w", err)
		}
		app, ok := apps[contactAccount]
		if !ok || app.Assistant == nil || !app.Assistant.Enabled {
			return errcode.NotFound("contact not found", errcode.WithReason(errcode.UserPriorityContactNotFound))
		}
		return nil
	}
	dir, err := s.users.GetDirectoryInfoByAccounts(c, []string{contactAccount})
	if err != nil {
		return fmt.Errorf("look up user contact: %w", err)
	}
	if _, ok := dir[contactAccount]; !ok {
		return errcode.NotFound("contact not found", errcode.WithReason(errcode.UserPriorityContactNotFound))
	}
	return nil
}

// enrichPriorityContacts splits accounts by model.IsBot and enriches each in one bulk lookup
// per kind (directory for humans, app catalog for bots — both degrade to a bare account on
// lookup failure, mirroring lookupApps/lookupHRInfo in subscriptions.go), preserving the
// original stored order. A contact whose record is missing (deleted user/app) is account-only.
func (s *UserService) enrichPriorityContacts(c *natsrouter.Context, accounts []string) []models.PriorityContactItem {
	var userAccounts, botAccounts []string
	for _, a := range accounts {
		if model.IsBot(a) {
			botAccounts = append(botAccounts, a)
		} else {
			userAccounts = append(userAccounts, a)
		}
	}
	dirMap := s.lookupPriorityContactDirectory(c, userAccounts)
	appMap := s.lookupApps(c, botAccounts)
	items := make([]models.PriorityContactItem, len(accounts))
	for i, a := range accounts {
		item := models.PriorityContactItem{Account: a}
		if model.IsBot(a) {
			if app, ok := appMap[a]; ok {
				item.AppName = app.Name
			}
		} else if u, ok := dirMap[a]; ok {
			item.EngName = u.EngName
			item.ChineseName = u.ChineseName
			item.EmployeeID = u.EmployeeID
			item.SectName = u.SectName
		}
		items[i] = item
	}
	return items
}

// lookupPriorityContactDirectory fetches directory info for the given distinct human accounts;
// a lookup failure degrades to nil (account-only in the response) — mirrors lookupHRInfo/lookupApps.
func (s *UserService) lookupPriorityContactDirectory(c *natsrouter.Context, accounts []string) map[string]*model.User {
	if len(accounts) == 0 {
		return nil
	}
	dir, err := s.users.GetDirectoryInfoByAccounts(c, accounts)
	if err != nil {
		slog.WarnContext(c, "priority contact directory lookup degraded", "account", c.Param("account"), "request_id", natsutil.RequestIDFromContext(c), "error", err)
		return nil
	}
	return dir
}

// publishPriorityContactInbox replicates one priority-contact add/remove to every other site's
// external INBOX lane as an atomic $addToSet/$pull (never a full-settings overwrite), so it
// can't race with a concurrent SetSettings mirror for an unrelated field. Mirrors
// publishSettingsInbox in settings.go; errors logged, best-effort.
func (s *UserService) publishPriorityContactInbox(c *natsrouter.Context, account, contactAccount string, eventType model.InboxEventType, now int64) {
	payload, _ := json.Marshal(model.PriorityContactChanged{
		Account:        account,
		ContactAccount: contactAccount,
		Timestamp:      now,
	}) // all primitives — Marshal cannot fail
	for _, dest := range s.allSiteIDs {
		if dest == "" || dest == s.siteID {
			continue
		}
		evt := model.InboxEvent{
			Type:       eventType,
			SiteID:     s.siteID,
			DestSiteID: dest,
			Payload:    payload,
			Timestamp:  now,
		}
		data, err := json.Marshal(evt)
		if err != nil {
			slog.WarnContext(c, "marshal priority contact inbox event", "error", err, "site", s.siteID, "dest", dest, "account", account, "request_id", natsutil.RequestIDFromContext(c))
			continue
		}
		if err := s.pub.Publish(c, subject.InboxExternal(dest, eventType), data); err != nil {
			slog.WarnContext(c, "publish priority contact inbox event", "error", err, "site", s.siteID, "dest", dest, "account", account, "request_id", natsutil.RequestIDFromContext(c))
		}
	}
}
```

Every method above takes `c *natsrouter.Context` directly as its `context.Context` argument (same style as `settings.go`/`subscriptions.go`), so the file does not import `"context"`.

Register the three handlers in `user-service/service/service.go`, in `RegisterHandlers`, immediately after the `settings.set` line:

```go
	natsrouter.RegisterNoBody(r, subject.UserSettingsGetPattern(s.siteID), s.GetSettings)
	natsrouter.Register(r, subject.UserSettingsSetPattern(s.siteID), s.SetSettings)
	natsrouter.RegisterNoBody(r, subject.PriorityContactsGetPattern(s.siteID), s.GetPriorityContacts)
	natsrouter.Register(r, subject.PriorityContactsAddPattern(s.siteID), s.AddPriorityContact)
	natsrouter.Register(r, subject.PriorityContactsRemovePattern(s.siteID), s.RemovePriorityContact)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=user-service`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add user-service/service/prioritycontacts.go user-service/service/prioritycontacts_test.go user-service/service/service.go
git commit -m "feat(user-service): add priority-contacts get/add/remove handlers"
```

---

### Task 8: inbox-worker — cross-site receive path

**Files:**
- Modify: `inbox-worker/handler.go`
- Modify: `inbox-worker/main.go`
- Modify: `inbox-worker/handler_test.go`
- Regenerate: `inbox-worker/mock_store_test.go`

**Interfaces:**
- Consumes: `model.InboxPriorityContactAdded`/`Removed`, `model.PriorityContactChanged` (Task 2).
- Produces:
  - `InboxStore` gains `AddPriorityContact(ctx, account, contactAccount string) error` and `RemovePriorityContact(ctx, account, contactAccount string) error`.
  - `mongoInboxStore` implements both via `s.userCol.UpdateOne`.

- [ ] **Step 1: Write the failing tests**

In `inbox-worker/handler_test.go`, add near `TestHandler_UserSettingsUpdated` (after it):

```go
func TestHandler_PriorityContactAdded(t *testing.T) {
	store := &stubInboxStore{}
	h := NewHandler(store)

	payload, err := json.Marshal(model.PriorityContactChanged{
		Account: "alice", ContactAccount: "bob", Timestamp: 12345,
	})
	require.NoError(t, err)
	evt, err := json.Marshal(model.InboxEvent{
		Type: model.InboxPriorityContactAdded, Payload: payload, Timestamp: 12345,
	})
	require.NoError(t, err)

	require.NoError(t, h.HandleEvent(context.Background(), evt))

	changes := store.getPriorityContactChanges()
	require.Len(t, changes, 1)
	assert.Equal(t, "add", changes[0].op)
	assert.Equal(t, "alice", changes[0].account)
	assert.Equal(t, "bob", changes[0].contactAccount)
}

func TestHandler_PriorityContactAdded_MalformedPayload(t *testing.T) {
	store := &stubInboxStore{}
	h := NewHandler(store)

	evt, err := json.Marshal(model.InboxEvent{
		Type: model.InboxPriorityContactAdded, Payload: []byte("not-json"),
	})
	require.NoError(t, err)

	require.Error(t, h.HandleEvent(context.Background(), evt))
	assert.Empty(t, store.getPriorityContactChanges())
}

func TestHandler_PriorityContactRemoved(t *testing.T) {
	store := &stubInboxStore{}
	h := NewHandler(store)

	payload, err := json.Marshal(model.PriorityContactChanged{
		Account: "alice", ContactAccount: "bob", Timestamp: 12345,
	})
	require.NoError(t, err)
	evt, err := json.Marshal(model.InboxEvent{
		Type: model.InboxPriorityContactRemoved, Payload: payload, Timestamp: 12345,
	})
	require.NoError(t, err)

	require.NoError(t, h.HandleEvent(context.Background(), evt))

	changes := store.getPriorityContactChanges()
	require.Len(t, changes, 1)
	assert.Equal(t, "remove", changes[0].op)
	assert.Equal(t, "alice", changes[0].account)
	assert.Equal(t, "bob", changes[0].contactAccount)
}

func TestHandler_PriorityContactRemoved_MalformedPayload(t *testing.T) {
	store := &stubInboxStore{}
	h := NewHandler(store)

	evt, err := json.Marshal(model.InboxEvent{
		Type: model.InboxPriorityContactRemoved, Payload: []byte("not-json"),
	})
	require.NoError(t, err)

	require.Error(t, h.HandleEvent(context.Background(), evt))
	assert.Empty(t, store.getPriorityContactChanges())
}
```

Add the stub's supporting type, field, and methods. Add `priorityContactChange` next to `userSettingsUpdate` (~line 118):

```go
type priorityContactChange struct {
	op             string // "add" or "remove"
	account        string
	contactAccount string
}
```

Add `priorityContactChanges []priorityContactChange` to the `stubInboxStore` struct's field list (next to `settingsUpdates`).

Add methods next to `UpdateUserSettings`/`getSettingsUpdates` (~line 440):

```go
func (s *stubInboxStore) AddPriorityContact(_ context.Context, account, contactAccount string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.priorityContactChanges = append(s.priorityContactChanges, priorityContactChange{op: "add", account: account, contactAccount: contactAccount})
	return nil
}

func (s *stubInboxStore) RemovePriorityContact(_ context.Context, account, contactAccount string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.priorityContactChanges = append(s.priorityContactChanges, priorityContactChange{op: "remove", account: account, contactAccount: contactAccount})
	return nil
}

func (s *stubInboxStore) getPriorityContactChanges() []priorityContactChange {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]priorityContactChange, len(s.priorityContactChanges))
	copy(cp, s.priorityContactChanges)
	return cp
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=inbox-worker`
Expected: FAIL — not a build error (the stub methods added in Step 1 compile fine, and `model.InboxPriorityContactAdded`/`model.PriorityContactChanged` already exist from Task 2). `HandleEvent`'s switch doesn't route either new event type yet, so it falls through to the `default: unknown event type, skipping` case, which returns `nil`. That makes `TestHandler_PriorityContactAdded`/`Removed` fail on `require.Len(t, changes, 1)` (the stub method was never called), and the two `_MalformedPayload` tests fail on `require.Error(...)` (no error, because routing never reaches `json.Unmarshal`).

- [ ] **Step 3: Extend `InboxStore`, add switch cases and handlers**

In `inbox-worker/handler.go`, extend the `InboxStore` interface (after `UpdateUserSettings`):

```go
	// UpdateUserSettings replaces the local users doc's settings sub-document with the
	// full post-update settings from the origin site, guarded by settingsUpdatedAt so an
	// out-of-order or duplicate delivery can't regress. A missing user is a logged no-op.
	UpdateUserSettings(ctx context.Context, account string, settings *model.UserSettings, updatedAt time.Time) error
	// AddPriorityContact mirrors a cross-site priority-contact add onto the local users doc via
	// an atomic $addToSet — never a full-settings overwrite, so it can't race with a concurrent
	// UpdateUserSettings mirror for an unrelated field. A missing user is a silent no-op.
	AddPriorityContact(ctx context.Context, account, contactAccount string) error
	// RemovePriorityContact mirrors a cross-site priority-contact removal via an atomic $pull.
	// A missing user is a silent no-op.
	RemovePriorityContact(ctx context.Context, account, contactAccount string) error
}
```

Add two cases to the `HandleEvent` switch (after `case model.InboxUserSettingsUpdated:`):

```go
	case model.InboxUserSettingsUpdated:
		return h.handleUserSettingsUpdated(ctx, &evt)
	case model.InboxPriorityContactAdded:
		return h.handlePriorityContactAdded(ctx, &evt)
	case model.InboxPriorityContactRemoved:
		return h.handlePriorityContactRemoved(ctx, &evt)
	default:
```

Add the two handler functions after `handleUserSettingsUpdated`:

```go
// handlePriorityContactAdded mirrors a cross-site priority-contact add via an atomic
// $addToSet — never a full-settings overwrite — so it can't race with a concurrent
// UserSettingsUpdated mirror for an unrelated field.
func (h *Handler) handlePriorityContactAdded(ctx context.Context, evt *model.InboxEvent) error {
	var e model.PriorityContactChanged
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal priority_contact_added payload: %w", err)
	}
	if err := h.store.AddPriorityContact(ctx, e.Account, e.ContactAccount); err != nil {
		return fmt.Errorf("add priority contact for %q: %w", e.Account, err)
	}
	return nil
}

// handlePriorityContactRemoved mirrors a cross-site priority-contact removal via an atomic $pull.
func (h *Handler) handlePriorityContactRemoved(ctx context.Context, evt *model.InboxEvent) error {
	var e model.PriorityContactChanged
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal priority_contact_removed payload: %w", err)
	}
	if err := h.store.RemovePriorityContact(ctx, e.Account, e.ContactAccount); err != nil {
		return fmt.Errorf("remove priority contact for %q: %w", e.Account, err)
	}
	return nil
}
```

In `inbox-worker/main.go`, add the `mongoInboxStore` implementations after `UpdateUserSettings`:

```go
// AddPriorityContact mirrors a cross-site priority-contact add onto the local users doc via an
// atomic, idempotent $addToSet. A missing user (no doc on this site) is a silent no-op.
func (s *mongoInboxStore) AddPriorityContact(ctx context.Context, account, contactAccount string) error {
	if _, err := s.userCol.UpdateOne(ctx,
		bson.M{"account": account},
		bson.M{"$addToSet": bson.M{"settings.priorityContacts": contactAccount}},
	); err != nil {
		return fmt.Errorf("add priority contact for %q: %w", account, err)
	}
	return nil
}

// RemovePriorityContact mirrors a cross-site priority-contact removal onto the local users doc
// via an atomic, idempotent $pull. A missing user (no doc on this site) is a silent no-op.
func (s *mongoInboxStore) RemovePriorityContact(ctx context.Context, account, contactAccount string) error {
	if _, err := s.userCol.UpdateOne(ctx,
		bson.M{"account": account},
		bson.M{"$pull": bson.M{"settings.priorityContacts": contactAccount}},
	); err != nil {
		return fmt.Errorf("remove priority contact for %q: %w", account, err)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=inbox-worker`
Expected: PASS

- [ ] **Step 5: Regenerate `MockInboxStore`**

`inbox-worker` has no active `//go:generate` directive for its mock (it was produced once and checked in; `mock_store_test.go`'s header documents the exact command used). Run it directly from the repo root so `MockInboxStore` stays in sync with the now-larger `InboxStore` interface, even though no current test uses it:

```bash
cd inbox-worker && mockgen -destination=mock_store_test.go -package=main github.com/hmchangw/chat/inbox-worker InboxStore && cd ..
```

Run: `make test SERVICE=inbox-worker`
Expected: PASS (mock regeneration must not break the build).

- [ ] **Step 6: Commit**

```bash
git add inbox-worker/handler.go inbox-worker/main.go inbox-worker/handler_test.go inbox-worker/mock_store_test.go
git commit -m "feat(inbox-worker): mirror cross-site priority-contact add/remove"
```

---

### Task 9: inbox-worker — Mongo integration tests

**Files:**
- Modify: `inbox-worker/integration_test.go`

**Interfaces:**
- Consumes: `mongoInboxStore.AddPriorityContact`/`RemovePriorityContact` (Task 8), `testutil.MongoDB` (existing `setupMongo` helper).

- [ ] **Step 1: Write the failing tests**

Add to `inbox-worker/integration_test.go`, near the other `TestInbox_*` store-level tests:

```go
func TestInbox_AddPriorityContact_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		roomCol:      db.Collection("rooms"),
		userCol:      db.Collection("users"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}

	fullWidth := true
	_, err := store.userCol.InsertOne(ctx, bson.M{
		"_id": "u-alice", "account": "alice",
		"settings": model.UserSettings{FullWidth: &fullWidth},
	})
	require.NoError(t, err)

	require.NoError(t, store.AddPriorityContact(ctx, "alice", "bob"))

	var got model.User
	require.NoError(t, store.userCol.FindOne(ctx, bson.M{"account": "alice"}).Decode(&got))
	require.NotNil(t, got.Settings)
	assert.Equal(t, []string{"bob"}, got.Settings.PriorityContacts)
	require.NotNil(t, got.Settings.FullWidth, "sibling settings field must survive a targeted $addToSet")
	assert.True(t, *got.Settings.FullWidth)

	// Idempotent re-add.
	require.NoError(t, store.AddPriorityContact(ctx, "alice", "bob"))
	require.NoError(t, store.userCol.FindOne(ctx, bson.M{"account": "alice"}).Decode(&got))
	assert.Equal(t, []string{"bob"}, got.Settings.PriorityContacts)
}

func TestInbox_RemovePriorityContact_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		roomCol:      db.Collection("rooms"),
		userCol:      db.Collection("users"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}

	_, err := store.userCol.InsertOne(ctx, bson.M{
		"_id": "u-alice", "account": "alice",
		"settings": model.UserSettings{PriorityContacts: []string{"bob", "carol"}},
	})
	require.NoError(t, err)

	require.NoError(t, store.RemovePriorityContact(ctx, "alice", "bob"))

	var got model.User
	require.NoError(t, store.userCol.FindOne(ctx, bson.M{"account": "alice"}).Decode(&got))
	require.NotNil(t, got.Settings)
	assert.Equal(t, []string{"carol"}, got.Settings.PriorityContacts)

	// No-op on a missing entry.
	require.NoError(t, store.RemovePriorityContact(ctx, "alice", "nobody"))
	require.NoError(t, store.userCol.FindOne(ctx, bson.M{"account": "alice"}).Decode(&got))
	assert.Equal(t, []string{"carol"}, got.Settings.PriorityContacts)
}

func TestInbox_AddPriorityContact_MissingUser_NoError(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := &mongoInboxStore{userCol: db.Collection("users")}
	require.NoError(t, store.AddPriorityContact(ctx, "ghost", "bob"))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test-integration SERVICE=inbox-worker`
Expected: FAIL before Task 8 lands; since Task 8 is already committed at this point in the plan, this should already PASS — run it anyway as the Red/Green record for this file, and treat an unexpected FAIL as a signal Task 8's implementation has a defect.

- [ ] **Step 3: (No implementation step — Task 8 already provides it)**

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test-integration SERVICE=inbox-worker`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add inbox-worker/integration_test.go
git commit -m "test(inbox-worker): add Mongo integration coverage for priority-contact mirroring"
```

---

### Task 10: `docs/client-api.md` + `docs/client-api/request-reply.md`

**Files:**
- Modify: `docs/client-api.md`
- Modify: `docs/client-api/request-reply.md`

**Interfaces:**
- Consumes: the finished RPC shapes from Tasks 4, 6, 7.

- [ ] **Step 1: Update the table of contents** (`docs/client-api.md:62`)

Change:

```
     - [`subscription.getDM`](#subscriptiongetdm) · [`subscription.getByRoomID`](#subscriptiongetbyroomid) · [`subscription.count`](#subscriptioncount) · [`subscription.setAppSubscription`](#subscriptionsetappsubscription) · [`apps.list`](#appslist) · [`apps.categories`](#appscategories) · [`settings.get`](#settingsget) · [`settings.set`](#settingsset)
     - [`sso.set`](#ssoset) · [`sso.refresh`](#ssorefresh)
```

to:

```
     - [`subscription.getDM`](#subscriptiongetdm) · [`subscription.getByRoomID`](#subscriptiongetbyroomid) · [`subscription.count`](#subscriptioncount) · [`subscription.setAppSubscription`](#subscriptionsetappsubscription) · [`apps.list`](#appslist) · [`apps.categories`](#appscategories) · [`settings.get`](#settingsget) · [`settings.set`](#settingsset)
     - [`priority-contacts.get`](#priority-contactsget) · [`priority-contacts.add`](#priority-contactsadd) · [`priority-contacts.remove`](#priority-contactsremove)
     - [`sso.set`](#ssoset) · [`sso.refresh`](#ssorefresh)
```

- [ ] **Step 2: Add `PriorityContactItem` to §3.0 Shared schemas**

After the `#### SubscriptionUser` block (`docs/client-api.md:874-882`), insert:

```markdown
#### PriorityContactItem

One enriched priority-contact entry, returned by the priority-contacts endpoints ([§3.4](#34-user-service)). No `avatarUrl` — consistent with [Participant](#participant): the frontend resolves avatars from `account`.

| Field | Type | Notes |
|---|---|---|
| `account` | string | The contact's account (regular user or `.bot`). Always present. |
| `engName` | string | Optional. Regular users only. |
| `chineseName` | string | Optional. Regular users only. |
| `employeeId` | string | Optional. Regular users only. |
| `sectName` | string | Optional. Regular users only. |
| `appName` | string | Optional. `.bot` accounts only. |
```

- [ ] **Step 3: Update the §3.4 intro, endpoint count, and RPC index table**

At `docs/client-api.md:4383-4409`:

Change `19 NATS request/reply endpoints` to `22 NATS request/reply endpoints`.

Change the events callout line:

```
> **Events:** [`settings.set`](#settingsset) emits [`settings.update`](#settingsupdate-event) to the caller's other devices. No other endpoint emits a client-facing event. (`status.set` and `settings.set` also trigger a server-side cross-site federation update, which is not delivered to clients.)
```

to:

```
> **Events:** [`settings.set`](#settingsset), [`priority-contacts.add`](#priority-contactsadd), and [`priority-contacts.remove`](#priority-contactsremove) emit [`settings.update`](#settingsupdate-event) to the caller's other devices. No other endpoint emits a client-facing event. (`status.set`, `settings.set`, `priority-contacts.add`, and `priority-contacts.remove` also trigger a server-side cross-site federation update, which is not delivered to clients.)
```

Add three rows to the RPC index table, immediately after the `settings.set` row:

```
| `chat.user.{account}.request.user.{siteID}.priority-contacts.get` | [`priority-contacts.get`](#priority-contactsget) |
| `chat.user.{account}.request.user.{siteID}.priority-contacts.add` | [`priority-contacts.add`](#priority-contactsadd) |
| `chat.user.{account}.request.user.{siteID}.priority-contacts.remove` | [`priority-contacts.remove`](#priority-contactsremove) |
```

- [ ] **Step 4: Add the `priorityContacts` row to `settings.get`**

At `docs/client-api.md:4600-4612`, change `All nine fields are optional` to `All ten fields are optional`, and add a row after `showNotificationsInCall`:

```
| `showNotificationsInCall` | boolean | Show notifications in call. |
| `priorityContacts` | string[] | Priority-contact accounts (regular users and `.bot` accounts). Read-only here — mutated only via [`priority-contacts.add`](#priority-contactsadd)/[`priority-contacts.remove`](#priority-contactsremove). |
| `initialChatScrollPosition` | string | Where a chat opens: `"lastRead"` \| `"newest"`. |
```

(Keep `initialChatScrollPosition` last, matching the model struct's field order.)

- [ ] **Step 5: Clarify `settings.set` excludes `priorityContacts`**

At `docs/client-api.md:4647`, change:

```
Any non-empty subset of the nine settings fields (same types as [`settings.get`](#settingsget)):
```

to:

```
Any non-empty subset of the settings fields below (same types as [`settings.get`](#settingsget), excluding `priorityContacts` — that field is mutated only via the dedicated [`priority-contacts.add`](#priority-contactsadd)/[`priority-contacts.remove`](#priority-contactsremove) endpoints, never through `settings.set`):
```

- [ ] **Step 6: Insert the three new RPC sections**

After the `settings.set` block's `##### settings.update event` section ends (`docs/client-api.md:4705`, right before `#### subscription.list`), insert:

```markdown
#### priority-contacts.get

**Subject:** `chat.user.{account}.request.user.{siteID}.priority-contacts.get`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

Returns the calling user's priority contacts, enriched with directory info for display.

##### Request body

None (empty payload).

##### Success response

[PriorityContactItem](#prioritycontactitem)`[]`, in the order contacts were added.

```json
[
  { "account": "bob", "engName": "Bob Lee", "chineseName": "李鮑伯", "employeeId": "E12345", "sectName": "Engineering" },
  { "account": "helper.bot", "appName": "Helper Bot" }
]
```

Never-set user:

```json
[]
```

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| No active user doc for the caller | `not_found` | — | `{ "code": "not_found", "error": "user not found" }` |
| Any other failure | — | — | Collapses to the generic boundary error code — see [§6 Error envelope reference](#6-error-envelope-reference). |

---

#### priority-contacts.add

**Subject:** `chat.user.{account}.request.user.{siteID}.priority-contacts.add`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

Adds one account to the calling user's priority contacts (capped at 30). Idempotent — re-adding an existing contact is a no-op that still returns 200. Fans out the change to the caller's other devices and, internally, to every other site.

##### Request body

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `contactAccount` | string | yes | The account to add — a regular user or a `.bot` account. Must not equal the caller's own account. Must resolve to an existing user, or an app with an enabled assistant. |

```json
{ "contactAccount": "bob" }
```

##### Success response

The caller's full, updated priority contacts list — same shape as [`priority-contacts.get`](#priority-contactsget).

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| `contactAccount` missing | `bad_request` | — | `{ "code": "bad_request", "error": "contact account required" }` |
| `contactAccount` equals caller's account | `bad_request` | `priority_contact_self` | Cannot add yourself. |
| `contactAccount` doesn't resolve to a user or an enabled bot | `not_found` | `priority_contact_not_found` | |
| Caller already has 30 priority contacts | `forbidden` | `priority_contact_limit_reached` | |
| No active user doc for the caller | `not_found` | — | `{ "code": "not_found", "error": "user not found" }` |
| Any other failure | — | — | Collapses to the generic boundary error code — see [§6 Error envelope reference](#6-error-envelope-reference). |

<a id="priority-contactsupdate-event"></a>
##### `settings.update` event

**Emits:** [`settings.update`](#settingsupdate-event), same as `settings.set` — carries the full post-update settings (including `priorityContacts`) to the caller's other devices.

---

#### priority-contacts.remove

**Subject:** `chat.user.{account}.request.user.{siteID}.priority-contacts.remove`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

Removes one account from the calling user's priority contacts. Idempotent — removing an absent contact is a no-op that still returns 200; not capped, so it always succeeds even if the caller is currently over the 30-entry limit. Fans out the change to the caller's other devices and, internally, to every other site.

##### Request body

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `contactAccount` | string | yes | The account to remove. |

```json
{ "contactAccount": "bob" }
```

##### Success response

The caller's full, updated priority contacts list — same shape as [`priority-contacts.get`](#priority-contactsget).

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| `contactAccount` missing | `bad_request` | — | `{ "code": "bad_request", "error": "contact account required" }` |
| No active user doc for the caller | `not_found` | — | `{ "code": "not_found", "error": "user not found" }` |
| Any other failure | — | — | Collapses to the generic boundary error code — see [§6 Error envelope reference](#6-error-envelope-reference). |

**Emits:** [`settings.update`](#settingsupdate-event), same as `settings.set`.

---

```

- [ ] **Step 7: Add the three new reasons to the §6 catalog**

At `docs/client-api.md:6286` (right after the `sso_token_not_found` row), insert:

```
| `priority_contact_self` | bad_request | user-service `priority-contacts.add` (contactAccount equals caller's own account) |
| `priority_contact_not_found` | not_found | user-service `priority-contacts.add` (contactAccount does not resolve to a user or an enabled bot) |
| `priority_contact_limit_reached` | forbidden | user-service `priority-contacts.add` (caller already has 30 priority contacts) |
```

- [ ] **Step 8: Mirror everything into `docs/client-api/request-reply.md`**

At `docs/client-api/request-reply.md:1415`, change:

```
[settings.set](#settingsset) emits [settings.update](events.md#settingsupdate--user-settings-sync);
no other endpoint emits a client-facing event.
```

to:

```
[settings.set](#settingsset), [priority-contacts.add](#priority-contactsadd), and
[priority-contacts.remove](#priority-contactsremove) emit
[settings.update](events.md#settingsupdate--user-settings-sync); no other endpoint emits a
client-facing event.
```

Add three rows to the RPC subject table (after the `settings.set` row, `docs/client-api/request-reply.md:1425`):

```
| `chat.user.{account}.request.user.{siteID}.priority-contacts.get` | [priority-contacts.get](#priority-contactsget) |
| `chat.user.{account}.request.user.{siteID}.priority-contacts.add` | [priority-contacts.add](#priority-contactsadd) |
| `chat.user.{account}.request.user.{siteID}.priority-contacts.remove` | [priority-contacts.remove](#priority-contactsremove) |
```

Update the `settings.get` section (`docs/client-api/request-reply.md:1542-1554`) the same way as Step 4: "All nine fields" → "All ten fields", add the `priorityContacts` row before `initialChatScrollPosition`.

Update the `settings.set` section (`docs/client-api/request-reply.md:1575`) the same way as Step 5.

After the `settings.set` section ends (`docs/client-api/request-reply.md:1597`, before `### subscription.list`), insert the condensed equivalents:

```markdown
### priority-contacts.get

**Subject:** `chat.user.{account}.request.user.{siteID}.priority-contacts.get`

Returns the caller's priority contacts, enriched with directory info.

#### Request body

None (empty payload).

#### Success response

`PriorityContactItem[]`, in the order contacts were added: `{ "account", "engName"?, "chineseName"?, "employeeId"?, "sectName"?, "appName"? }`. `[]` for a never-set user.

#### Errors

`"user not found"` (`not_found`).

**Emits:** None.

---

### priority-contacts.add

**Subject:** `chat.user.{account}.request.user.{siteID}.priority-contacts.add`

Adds one account to the caller's priority contacts (capped at 30). Idempotent.

#### Request body

`{ "contactAccount": "bob" }`

#### Success response

Same shape as [priority-contacts.get](#priority-contactsget).

#### Errors

`"contact account required"` (`bad_request`), `"cannot add self"` (`bad_request`,
`priority_contact_self`), contact not found (`not_found`, `priority_contact_not_found`),
limit reached (`forbidden`, `priority_contact_limit_reached`), `"user not found"` (`not_found`).

**Emits:** [settings.update](events.md#settingsupdate--user-settings-sync) to the caller's other
devices. A server-side cross-site federation update also fires but is not delivered to clients.

---

### priority-contacts.remove

**Subject:** `chat.user.{account}.request.user.{siteID}.priority-contacts.remove`

Removes one account from the caller's priority contacts. Idempotent; not capped.

#### Request body

`{ "contactAccount": "bob" }`

#### Success response

Same shape as [priority-contacts.get](#priority-contactsget).

#### Errors

`"contact account required"` (`bad_request`), `"user not found"` (`not_found`).

**Emits:** [settings.update](events.md#settingsupdate--user-settings-sync) to the caller's other
devices. A server-side cross-site federation update also fires but is not delivered to clients.

---

```

- [ ] **Step 9: Proofread**

Read both files back over the edited ranges and confirm: every new internal link (`#priority-contactsget`, `#priority-contactsadd`, `#priority-contactsremove`, `#prioritycontactitem`) resolves to a heading that actually exists in that file; the endpoint count (`22`) matches the new index table's row count; no leftover `nine fields` phrasing remains in either file.

- [ ] **Step 10: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md
git commit -m "docs: document priority-contacts get/add/remove endpoints"
```

---

### Task 11: Full verification pass

**Files:** none (verification only).

- [ ] **Step 1: Lint**

Run: `make lint`
Expected: no findings in touched files.

- [ ] **Step 2: Unit tests**

Run:
```bash
make test SERVICE=pkg/model
make test SERVICE=pkg/errcode
make test SERVICE=pkg/subject
make test SERVICE=user-service
make test SERVICE=inbox-worker
```
Expected: PASS, and check `-coverprofile` output for `user-service/service`, `user-service/mongorepo`, and `inbox-worker` stays ≥80% (90%+ target on the new handler/store code).

- [ ] **Step 3: Integration tests**

Run:
```bash
make test-integration SERVICE=user-service
make test-integration SERVICE=inbox-worker
```
Expected: PASS (requires Docker).

- [ ] **Step 4: SAST**

Run: `make sast`
Expected: no medium+ findings introduced by this change. If `gosec` flags anything in the new files, fix it — do not suppress without a genuine false positive and a `// #nosec <RULE> -- reason` comment per CLAUDE.md.

- [ ] **Step 5: Full repo build sanity**

Run: `make build SERVICE=user-service && make build SERVICE=inbox-worker`
Expected: both binaries build cleanly.

- [ ] **Step 6: Final commit (if Step 1 or 4 required fixes)**

```bash
git add -A
git commit -m "fix: address lint/sast findings from priority-contacts implementation"
```

If nothing needed fixing, skip this commit — Task 10's commit is the last one.
