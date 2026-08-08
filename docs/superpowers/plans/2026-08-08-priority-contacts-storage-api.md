# Priority Contacts — Storage and API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a user store up to 30 priority contacts on their settings, read them back enriched for display, and have every site receive the list — without changing push behavior yet.

**Architecture:** `priorityContacts` is a `[]string` embedded at `users.settings.priorityContacts`. Three dedicated NATS RPCs own it (`settings.set` never writes it). Every mutation publishes the two existing settings fanouts off one timestamp — the client event for the caller's other devices, and the cross-site INBOX event that gives each site's `notification-worker` a local copy to read in Spec 2.

**Tech Stack:** Go 1.25, MongoDB (`go.mongodb.org/mongo-driver/v2`), NATS request/reply via `pkg/natsrouter`, `go.uber.org/mock` + `testify`, `testcontainers-go` via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-08-08-priority-contacts-storage-api-design.md`

## Global Constraints

- **TDD is mandatory.** Red → Green → Refactor → Commit. Never write implementation before its failing test exists. Never skip running the test and seeing it fail.
- **Use `make`, never raw `go`.** `make test SERVICE=<name>`, `make test-integration SERVICE=<name>`, `make generate SERVICE=<name>`, `make lint`, `make fmt`.
- **Run `make generate SERVICE=user-service` whenever the `UserRepository` interface changes**, before running tests. Generated mocks live in `user-service/service/mocks/mock_repository.go` — never edit by hand.
- **Minimum 80% coverage; target 90%+** for the new handlers.
- **Error handling:** return typed `*errcode.Error` from named constructors for client-facing errors; return raw `fmt.Errorf("doing thing: %w", err)` for infra failures. Never log **and** return the same error — `natsrouter` classifies and logs once.
- **Never edit `mock_repository.go` by hand.**
- The cap is **30**, defined once as `maxPriorityContacts` in `user-service/service/prioritycontacts.go`.
- Bot accounts are identified by `model.IsBot(account)` (a `.bot` suffix) — never re-implement the suffix check.
- Do **not** touch `notification-worker` in this plan. That is Spec 2.

## Refinements to the spec

Two shapes changed after checking house conventions while writing this plan. Both are noted here so the spec and plan don't read as contradicting each other.

1. **Handlers return a wrapper object, not a bare array.** No handler in `user-service` returns `*[]T`; list responses use a named struct (`models.AppsListResponse{Apps: […], HasMore: …}`). So the reply is `PriorityContactsResponse{Contacts: […]}`.
2. **`GetUserPriorityContacts` returns `(*model.User, error)`, not `([]string, error)`.** This mirrors `GetUserSettings` / `GetUserChatlist` and lets the handler distinguish "no such user" (→ `NotFound`) from "user with an empty list" (→ `{"contacts": []}`). A bare slice collapses those two cases.

## File Structure

| File | Responsibility |
|---|---|
| `pkg/model/usersettings.go` | The `PriorityContacts` field + the two deliberate omissions, commented |
| `pkg/errcode/codes_user.go` | Two reason constants |
| `pkg/subject/subject.go` | Six subject builders (concrete + `Pattern`, ×3 RPCs) |
| `user-service/models/prioritycontacts.go` | Request + response wire types (new) |
| `user-service/mongorepo/users.go` | Five new repo methods; `UpdateUserSettings` gains the timestamp stamp |
| `user-service/service/service.go` | Interface methods + handler registration |
| `user-service/service/settings.go` | `publishSettingsFanouts` helper; `SetSettings` switched onto it |
| `user-service/service/prioritycontacts.go` | Three handlers + enrichment (new) |
| `inbox-worker/main.go` | `$lt` → `$lte` on the settings high-water guard |

---

### Task 1: Model field, reason codes, and subjects

Foundation with no behavior. Everything later depends on these names.

**Files:**
- Modify: `pkg/model/usersettings.go:28`
- Modify: `pkg/errcode/codes_user.go:17`
- Modify: `pkg/subject/subject.go` (append after `UserSettingsSetPattern`, ~line 1365)
- Test: `pkg/model/model_test.go`, `pkg/errcode/codes_user_test.go`, `pkg/subject/subject_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `model.UserSettings.PriorityContacts []string`; `errcode.UserPriorityContactLimit`, `errcode.UserPriorityContactNotFound`; `subject.UserPriorityContactsGet/Add/Remove(account, siteID string) string` and `subject.UserPriorityContactsGetPattern/AddPattern/RemovePattern(siteID string) string`.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/model/model_test.go`:

```go
func TestUserSettingsPriorityContactsRoundTrip(t *testing.T) {
	src := model.UserSettings{PriorityContacts: []string{"alice", "helper.bot"}}
	dst := model.UserSettings{}
	roundTrip(t, &src, &dst)
}

// priorityContacts must NOT satisfy settings.set: it is written only by the
// dedicated add/remove RPCs, so a settings.set carrying just this field has to
// fall through to bad_request "no settings provided".
func TestUserSettingsIsEmptyIgnoresPriorityContacts(t *testing.T) {
	s := model.UserSettings{PriorityContacts: []string{"alice"}}
	if !s.IsEmpty() {
		t.Error("IsEmpty() = false, want true")
	}
}
```

In `pkg/errcode/codes_user_test.go`, add two entries to the existing `cases` map in `TestUserReasons`:

```go
		UserPriorityContactLimit:    "priority_contact_limit",
		UserPriorityContactNotFound: "priority_contact_not_found",
```

In `pkg/subject/subject_test.go`, add rows to the three existing tables. To the concrete-subject table (~line 626):

```go
		{"priorityContacts.get", subject.UserPriorityContactsGet("alice", "s1"), "chat.user.alice.request.user.s1.settings.priorityContacts.get"},
		{"priorityContacts.add", subject.UserPriorityContactsAdd("alice", "s1"), "chat.user.alice.request.user.s1.settings.priorityContacts.add"},
		{"priorityContacts.remove", subject.UserPriorityContactsRemove("alice", "s1"), "chat.user.alice.request.user.s1.settings.priorityContacts.remove"},
```

To the wildcard-panic table (~line 798):

```go
		{"UserPriorityContactsGet", func() { subject.UserPriorityContactsGet("*", "s1") }},
		{"UserPriorityContactsAdd", func() { subject.UserPriorityContactsAdd("*", "s1") }},
		{"UserPriorityContactsRemove", func() { subject.UserPriorityContactsRemove("*", "s1") }},
```

To the pattern table (~line 940):

```go
		{"priorityContacts.get", subject.UserPriorityContactsGetPattern("s1"), "chat.user.{account}.request.user.s1.settings.priorityContacts.get"},
		{"priorityContacts.add", subject.UserPriorityContactsAddPattern("s1"), "chat.user.{account}.request.user.s1.settings.priorityContacts.add"},
		{"priorityContacts.remove", subject.UserPriorityContactsRemovePattern("s1"), "chat.user.{account}.request.user.s1.settings.priorityContacts.remove"},
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=pkg`
Expected: FAIL — compile errors, `PriorityContacts` / `UserPriorityContactLimit` / `subject.UserPriorityContactsGet` undefined.

- [ ] **Step 3: Add the model field**

In `pkg/model/usersettings.go`, add as the last field of `UserSettings` (after `InitialChatScrollPosition`):

```go
	// PriorityContacts is written ONLY by the settings.priorityContacts.{add,remove}
	// RPCs, never by settings.set: UpdateUserSettings deliberately never references it,
	// and it is deliberately absent from IsEmpty so a settings.set carrying only this
	// field falls through to bad_request. Holds raw accounts — users and ".bot" alike.
	PriorityContacts []string `json:"priorityContacts,omitempty" bson:"priorityContacts,omitempty"`
```

Leave `IsEmpty()` unchanged.

- [ ] **Step 4: Add the reason codes**

In `pkg/errcode/codes_user.go`, inside the existing `const` block, before the closing paren:

```go

	// Priority-contact reasons — the client branches on each.
	UserPriorityContactLimit    Reason = "priority_contact_limit"
	UserPriorityContactNotFound Reason = "priority_contact_not_found"
```

- [ ] **Step 5: Add the subject builders**

In `pkg/subject/subject.go`, after `UserSettingsSetPattern`:

```go
// Priority-contacts subjects. Nested under settings. because the list lives at
// settings.priorityContacts and rides the settings fanouts, but it gets its own
// RPCs because settings.set never writes it. The concrete forms panic on a
// wildcard account, same guard as the settings helpers.

func UserPriorityContactsGet(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.settings.priorityContacts.get", account, siteID)
}

func UserPriorityContactsGetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.settings.priorityContacts.get", siteID)
}

func UserPriorityContactsAdd(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.settings.priorityContacts.add", account, siteID)
}

func UserPriorityContactsAddPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.settings.priorityContacts.add", siteID)
}

func UserPriorityContactsRemove(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.settings.priorityContacts.remove", account, siteID)
}

func UserPriorityContactsRemovePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.settings.priorityContacts.remove", siteID)
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `make test SERVICE=pkg`
Expected: PASS

- [ ] **Step 7: Lint and commit**

```bash
make fmt && make lint
git add pkg/model/usersettings.go pkg/model/model_test.go pkg/errcode/codes_user.go pkg/errcode/codes_user_test.go pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(model): add priorityContacts field, reason codes, and subjects"
```

---

### Task 2: Wire types

**Files:**
- Create: `user-service/models/prioritycontacts.go`
- Test: `user-service/models/prioritycontacts_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `models.PriorityContactMutateRequest`, `models.PriorityContactUser`, `models.PriorityContactApp`, `models.PriorityContactItem`, `models.PriorityContactsResponse`, `models.PriorityContactTypeUser`, `models.PriorityContactTypeBot`.

- [ ] **Step 1: Write the failing test**

Create `user-service/models/prioritycontacts_test.go`:

The package is internal (`package models`), matching `user-service/models/settings_test.go`.

```go
package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPriorityContactItem_UserRowOmitsApp(t *testing.T) {
	item := PriorityContactItem{
		Account: "alice",
		Type:    PriorityContactTypeUser,
		User: &PriorityContactUser{
			EngName: "Alice", ChineseName: "愛麗絲", EmployeeID: "E1", SectName: "Ops",
		},
	}
	data, err := json.Marshal(item)
	require.NoError(t, err)
	assert.JSONEq(t, `{"account":"alice","type":"user","user":{"engName":"Alice","chineseName":"愛麗絲","employeeId":"E1","sectName":"Ops"}}`, string(data))
}

func TestPriorityContactItem_BotRowOmitsUser(t *testing.T) {
	item := PriorityContactItem{
		Account: "helper.bot",
		Type:    PriorityContactTypeBot,
		App:     &PriorityContactApp{Name: "Helper"},
	}
	data, err := json.Marshal(item)
	require.NoError(t, err)
	assert.JSONEq(t, `{"account":"helper.bot","type":"bot","app":{"name":"Helper"}}`, string(data))
}

// An account that no longer resolves keeps account+type so the client can render
// a placeholder instead of dropping the row.
func TestPriorityContactItem_UnresolvedRowCarriesAccountAndType(t *testing.T) {
	item := PriorityContactItem{Account: "ghost", Type: PriorityContactTypeUser}
	data, err := json.Marshal(item)
	require.NoError(t, err)
	assert.JSONEq(t, `{"account":"ghost","type":"user"}`, string(data))
}

func TestPriorityContactsResponse_EmptyListMarshalsAsArray(t *testing.T) {
	data, err := json.Marshal(PriorityContactsResponse{Contacts: []PriorityContactItem{}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"contacts":[]}`, string(data))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=user-service`
Expected: FAIL — `models.PriorityContactItem` undefined.

- [ ] **Step 3: Create the types**

Create `user-service/models/prioritycontacts.go`:

```go
package models

// PriorityContactItem.Type values — the explicit discriminator, so the client never
// has to infer a row's kind from the ".bot" suffix or from which fields came back set.
const (
	PriorityContactTypeUser = "user"
	PriorityContactTypeBot  = "bot"
)

// PriorityContactMutateRequest is the body of settings.priorityContacts.add and
// settings.priorityContacts.remove.
type PriorityContactMutateRequest struct {
	ContactAccount string `json:"contactAccount"`
}

// PriorityContactUser carries the HR-directory fields rendered for a regular-user
// priority contact.
type PriorityContactUser struct {
	EngName     string `json:"engName"`
	ChineseName string `json:"chineseName"`
	EmployeeID  string `json:"employeeId"`
	SectName    string `json:"sectName"`
}

// PriorityContactApp carries the app name rendered for a bot priority contact.
type PriorityContactApp struct {
	Name string `json:"name"`
}

// PriorityContactItem is one row: the account, an explicit kind, and at most one
// populated detail object. Both detail pointers are nil when the account no longer
// resolves (deactivated user, deleted app) — the row still carries account+type.
type PriorityContactItem struct {
	Account string               `json:"account"`
	Type    string               `json:"type"`
	User    *PriorityContactUser `json:"user,omitempty"`
	App     *PriorityContactApp  `json:"app,omitempty"`
}

// PriorityContactsResponse is the reply for all three priority-contact RPCs: the
// full enriched list in stored order.
type PriorityContactsResponse struct {
	Contacts []PriorityContactItem `json:"contacts"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=user-service`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
make fmt && make lint
git add user-service/models/prioritycontacts.go user-service/models/prioritycontacts_test.go
git commit -m "feat(user-service): add priority contact wire types"
```

---

### Task 3: Repository read methods

**Files:**
- Modify: `user-service/mongorepo/users.go`
- Modify: `user-service/service/service.go:30-39` (the `UserRepository` interface)
- Test: `user-service/mongorepo/users_test.go`

**Interfaces:**
- Consumes: `models.PriorityContactUser` (Task 2).
- Produces, on `UserRepository`:
  - `GetUserPriorityContacts(ctx context.Context, account string) (*model.User, error)` — `(nil, nil)` when no active user
  - `GetPriorityContactUsers(ctx context.Context, accounts []string) (map[string]*models.PriorityContactUser, error)`
  - `UserExists(ctx context.Context, account string) (bool, error)`

- [ ] **Step 1: Write the failing tests**

Append to `user-service/mongorepo/users_test.go`. The file is `package mongorepo` with `//go:build integration`, and uses the shared helpers `newTestUserRepo(t) (*UserRepo, *mongo.Database)` and `seed(t, db, collection, docs...)` from `setup_test.go` — use those, do not call `testutil.MongoDB` directly.

Note `setup_test.go` also carries `var _ service.UserRepository = (*UserRepo)(nil)`, so declaring an interface method without implementing it fails `go vet -tags integration`. That is the intended tripwire.

```go
func TestGetUserPriorityContacts_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users",
		bson.M{"_id": "u1", "account": "alice", "settings": bson.M{"priorityContacts": []string{"bob", "helper.bot"}}},
		bson.M{"_id": "u2", "account": "carol"},
		bson.M{"_id": "u3", "account": "dave", "active": false},
	)

	t.Run("returns the stored list in order", func(t *testing.T) {
		u, err := r.GetUserPriorityContacts(ctx, "alice")
		require.NoError(t, err)
		require.NotNil(t, u)
		require.NotNil(t, u.Settings)
		assert.Equal(t, []string{"bob", "helper.bot"}, u.Settings.PriorityContacts)
	})

	t.Run("user with no settings returns a user with nil settings", func(t *testing.T) {
		u, err := r.GetUserPriorityContacts(ctx, "carol")
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Nil(t, u.Settings)
	})

	t.Run("inactive user is not found", func(t *testing.T) {
		u, err := r.GetUserPriorityContacts(ctx, "dave")
		require.NoError(t, err)
		assert.Nil(t, u)
	})

	t.Run("unknown account is not found", func(t *testing.T) {
		u, err := r.GetUserPriorityContacts(ctx, "ghost")
		require.NoError(t, err)
		assert.Nil(t, u)
	})
}

func TestUserExists_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users",
		bson.M{"_id": "u1", "account": "alice"},
		bson.M{"_id": "u2", "account": "dave", "active": false},
	)

	t.Run("active user exists", func(t *testing.T) {
		ok, err := r.UserExists(ctx, "alice")
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("deactivated user does not", func(t *testing.T) {
		ok, err := r.UserExists(ctx, "dave")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("unknown account does not", func(t *testing.T) {
		ok, err := r.UserExists(ctx, "ghost")
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestGetPriorityContactUsers_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users",
		bson.M{"_id": "u1", "account": "bob", "engName": "Bob", "chineseName": "鮑伯", "employeeId": "E9", "sectName": "Ops"},
		bson.M{"_id": "u2", "account": "erin", "engName": "Erin", "chineseName": "艾琳", "employeeId": "E8", "sectName": "QA", "active": false},
	)

	t.Run("projects all four display fields", func(t *testing.T) {
		got, err := r.GetPriorityContactUsers(ctx, []string{"bob"})
		require.NoError(t, err)
		require.NotNil(t, got["bob"])
		assert.Equal(t, "Bob", got["bob"].EngName)
		assert.Equal(t, "鮑伯", got["bob"].ChineseName)
		assert.Equal(t, "E9", got["bob"].EmployeeID)
		assert.Equal(t, "Ops", got["bob"].SectName)
	})

	// Deliberately NOT active-filtered: a contact deactivated after being added
	// still renders their name instead of collapsing to a bare account row.
	t.Run("includes deactivated users", func(t *testing.T) {
		got, err := r.GetPriorityContactUsers(ctx, []string{"erin"})
		require.NoError(t, err)
		require.NotNil(t, got["erin"])
		assert.Equal(t, "Erin", got["erin"].EngName)
	})

	t.Run("unknown accounts are omitted", func(t *testing.T) {
		got, err := r.GetPriorityContactUsers(ctx, []string{"bob", "ghost"})
		require.NoError(t, err)
		assert.Len(t, got, 1)
	})

	t.Run("empty input returns an empty map", func(t *testing.T) {
		got, err := r.GetPriorityContactUsers(ctx, []string{})
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test-integration SERVICE=user-service`
Expected: FAIL — `r.GetUserPriorityContacts` undefined.

- [ ] **Step 3: Implement the three methods**

In `user-service/mongorepo/users.go`, add the `models` import (`"github.com/hmchangw/chat/user-service/models"` — `apps.go` already imports it, so the module path is proven), then append:

```go
// GetUserPriorityContacts returns the user's stored priority-contact list (Settings
// is nil when the user never set any settings); (nil, nil) when no active user
// matched. The narrow projection is safe here — unlike Add/Remove, this result never
// feeds a settings fanout.
func (r *UserRepo) GetUserPriorityContacts(ctx context.Context, account string) (*model.User, error) {
	return r.users.FindOne(ctx, activeUserFilter(account),
		mongoutil.WithProjection(bson.M{"_id": 0, "settings.priorityContacts": 1}),
	)
}

// UserExists reports whether account names an active user. Backs the add-time
// existence check; active-only by design — a deactivated user cannot be added.
func (r *UserRepo) UserExists(ctx context.Context, account string) (bool, error) {
	u, err := r.users.FindOne(ctx, activeUserFilter(account),
		mongoutil.WithProjection(bson.M{"_id": 0, "account": 1}),
	)
	if err != nil {
		return false, fmt.Errorf("check user exists: %w", err)
	}
	return u != nil, nil
}

// GetPriorityContactUsers maps account → the display fields the priority-contacts
// list renders. GetHRInfoByAccounts is unusable here: it carries no employeeId or
// sectName, and widening it would change every DM subscription payload. Deliberately
// NOT active-filtered — a contact deactivated after being added still renders.
// Accounts with no users doc are omitted.
func (r *UserRepo) GetPriorityContactUsers(ctx context.Context, accounts []string) (map[string]*models.PriorityContactUser, error) {
	type contactUser struct {
		Account     string `bson:"account"`
		EngName     string `bson:"engName"`
		ChineseName string `bson:"chineseName"`
		EmployeeID  string `bson:"employeeId"`
		SectName    string `bson:"sectName"`
	}
	col := mongoutil.NewCollection[contactUser](r.users.Raw())
	rows, err := col.FindMany(ctx,
		bson.M{"account": bson.M{"$in": accounts}},
		mongoutil.WithProjection(bson.M{
			"_id": 0, "account": 1, "engName": 1, "chineseName": 1, "employeeId": 1, "sectName": 1,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("find priority contact users: %w", err)
	}
	out := make(map[string]*models.PriorityContactUser, len(rows))
	for i := range rows {
		out[rows[i].Account] = &models.PriorityContactUser{
			EngName:     rows[i].EngName,
			ChineseName: rows[i].ChineseName,
			EmployeeID:  rows[i].EmployeeID,
			SectName:    rows[i].SectName,
		}
	}
	return out, nil
}
```

- [ ] **Step 4: Declare them on the interface**

In `user-service/service/service.go`, add to `UserRepository` after `UpdateUserChatlist`:

```go
	GetUserPriorityContacts(ctx context.Context, account string) (*model.User, error)
	GetPriorityContactUsers(ctx context.Context, accounts []string) (map[string]*models.PriorityContactUser, error)
	UserExists(ctx context.Context, account string) (bool, error)
```

- [ ] **Step 5: Regenerate mocks**

Run: `make generate SERVICE=user-service`
Expected: `user-service/service/mocks/mock_repository.go` gains the three methods.

- [ ] **Step 6: Run tests to verify they pass**

Run: `make test-integration SERVICE=user-service && make test SERVICE=user-service`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
make fmt && make lint
git add user-service/mongorepo/users.go user-service/mongorepo/users_test.go user-service/service/service.go user-service/service/mocks/mock_repository.go
git commit -m "feat(user-service): add priority contact read repository methods"
```

---

### Task 4: Repository write methods

The cap lives in the update filter so concurrent adds cannot overshoot it.

**Files:**
- Modify: `user-service/mongorepo/users.go`
- Modify: `user-service/service/service.go` (`UserRepository`)
- Test: `user-service/mongorepo/users_test.go`

**Interfaces:**
- Consumes: `activeUserFilter` (existing, `users.go:52`).
- Produces, on `UserRepository`:
  - `AddPriorityContact(ctx context.Context, account, contact string, limit int, at time.Time) (*model.User, error)`
  - `RemovePriorityContact(ctx context.Context, account, contact string, at time.Time) (*model.User, error)`

Both return `(nil, nil)` on no match and project the **whole** `settings` sub-document.

- [ ] **Step 1: Write the failing tests**

Append to `user-service/mongorepo/users_test.go`:

```go
func TestAddPriorityContact_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	at := time.UnixMilli(1_700_000_000_000).UTC()
	seed(t, db, "users",
		bson.M{"_id": "u1", "account": "alice", "settings": bson.M{"themePreference": "dark"}},
		bson.M{"_id": "u2", "account": "dave", "active": false},
	)

	t.Run("adds and returns the whole settings sub-document", func(t *testing.T) {
		u, err := r.AddPriorityContact(ctx, "alice", "bob", 30, at)
		require.NoError(t, err)
		require.NotNil(t, u)
		require.NotNil(t, u.Settings)
		assert.Equal(t, []string{"bob"}, u.Settings.PriorityContacts)
		// The projection MUST stay {"_id":0,"settings":1}: this object is the
		// cross-site fanout payload and inbox-worker $sets it whole.
		require.NotNil(t, u.Settings.ThemePreference)
		assert.Equal(t, "dark", *u.Settings.ThemePreference)
	})

	t.Run("stamps settingsUpdatedAt", func(t *testing.T) {
		var doc struct {
			SettingsUpdatedAt time.Time `bson:"settingsUpdatedAt"`
		}
		require.NoError(t, db.Collection("users").FindOne(ctx, bson.M{"account": "alice"}).Decode(&doc))
		assert.Equal(t, at, doc.SettingsUpdatedAt.UTC())
	})

	t.Run("re-adding is a no-op", func(t *testing.T) {
		u, err := r.AddPriorityContact(ctx, "alice", "bob", 30, at)
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Equal(t, []string{"bob"}, u.Settings.PriorityContacts)
	})

	t.Run("at cap returns no match", func(t *testing.T) {
		u, err := r.AddPriorityContact(ctx, "alice", "carol", 1, at)
		require.NoError(t, err)
		assert.Nil(t, u)
	})

	t.Run("inactive user returns no match", func(t *testing.T) {
		u, err := r.AddPriorityContact(ctx, "dave", "bob", 30, at)
		require.NoError(t, err)
		assert.Nil(t, u)
	})
}

// The whole reason the cap rides in the filter: a read-then-write guard lets two
// concurrent adds both pass at limit-1 and overshoot.
func TestAddPriorityContact_ConcurrentAddsRespectCap_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	at := time.UnixMilli(1_700_000_000_000).UTC()

	// Named `existing`, not `seed` — `seed` is the package-level helper.
	existing := make([]string, 0, 28)
	for i := 0; i < 28; i++ {
		existing = append(existing, fmt.Sprintf("existing%02d", i))
	}
	seed(t, db, "users",
		bson.M{"_id": "u1", "account": "alice", "settings": bson.M{"priorityContacts": existing}},
	)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, _ = r.AddPriorityContact(ctx, "alice", fmt.Sprintf("race%02d", n), 30, at)
		}(i)
	}
	wg.Wait()

	u, err := r.GetUserPriorityContacts(ctx, "alice")
	require.NoError(t, err)
	require.NotNil(t, u.Settings)
	assert.Len(t, u.Settings.PriorityContacts, 30, "cap must hold under concurrent adds")
}

func TestRemovePriorityContact_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	at := time.UnixMilli(1_700_000_000_000).UTC()
	seed(t, db, "users",
		bson.M{"_id": "u1", "account": "alice", "settings": bson.M{
			"themePreference": "dark", "priorityContacts": []string{"bob", "helper.bot"},
		}},
		bson.M{"_id": "u2", "account": "dave", "active": false},
	)

	t.Run("removes and returns the whole settings sub-document", func(t *testing.T) {
		u, err := r.RemovePriorityContact(ctx, "alice", "bob", at)
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Equal(t, []string{"helper.bot"}, u.Settings.PriorityContacts)
		require.NotNil(t, u.Settings.ThemePreference)
		assert.Equal(t, "dark", *u.Settings.ThemePreference)
	})

	t.Run("removing an absent entry is a no-op", func(t *testing.T) {
		u, err := r.RemovePriorityContact(ctx, "alice", "ghost", at)
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Equal(t, []string{"helper.bot"}, u.Settings.PriorityContacts)
	})

	t.Run("inactive user returns no match", func(t *testing.T) {
		u, err := r.RemovePriorityContact(ctx, "dave", "bob", at)
		require.NoError(t, err)
		assert.Nil(t, u)
	})
}
```

Add `"fmt"`, `"sync"`, and `"time"` to `users_test.go`'s imports if not already present.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test-integration SERVICE=user-service`
Expected: FAIL — `r.AddPriorityContact` undefined.

- [ ] **Step 3: Implement the two methods**

In `user-service/mongorepo/users.go`, add `"time"` to the imports, then append:

```go
// AddPriorityContact appends contact via $addToSet and stamps settingsUpdatedAt in
// one atomic update whose filter also enforces the cap — a read-then-check-then-write
// guard would let two concurrent adds both pass at limit-1 and overshoot. Returns
// (nil, nil) when nothing matched; the caller disambiguates "no such user" from
// "at cap".
//
// The projection MUST stay {"_id":0,"settings":1} — the whole sub-document. This
// result feeds publishSettingsFanouts, and the cross-site apply is a whole-object
// $set of settings, so narrowing it to settings.priorityContacts would wipe every
// other setting at every remote site.
func (r *UserRepo) AddPriorityContact(ctx context.Context, account, contact string, limit int, at time.Time) (*model.User, error) {
	filter := activeUserFilter(account)
	filter["$expr"] = bson.M{"$lt": bson.A{
		bson.M{"$size": bson.M{"$ifNull": bson.A{"$settings.priorityContacts", bson.A{}}}},
		limit,
	}}
	update := bson.M{
		"$addToSet": bson.M{"settings.priorityContacts": contact},
		"$set":      bson.M{"settingsUpdatedAt": at},
	}
	return r.mutatePriorityContacts(ctx, filter, update, "add priority contact")
}

// RemovePriorityContact removes contact via $pull and stamps settingsUpdatedAt.
// Idempotent — removing an absent entry is a no-op that still returns the list.
// Returns (nil, nil) when no active user matched. Same whole-settings projection
// requirement as AddPriorityContact, for the same cross-site reason.
func (r *UserRepo) RemovePriorityContact(ctx context.Context, account, contact string, at time.Time) (*model.User, error) {
	update := bson.M{
		"$pull": bson.M{"settings.priorityContacts": contact},
		"$set":  bson.M{"settingsUpdatedAt": at},
	}
	return r.mutatePriorityContacts(ctx, activeUserFilter(account), update, "remove priority contact")
}

// mutatePriorityContacts runs a priority-contact update and decodes the post-update
// settings, so Add and Remove share one copy of the projection the fanouts depend on.
func (r *UserRepo) mutatePriorityContacts(ctx context.Context, filter, update bson.M, op string) (*model.User, error) {
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetProjection(bson.M{"_id": 0, "settings": 1})
	res := r.users.Raw().FindOneAndUpdate(ctx, filter, update, opts)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	var u model.User
	if err := res.Decode(&u); err != nil {
		return nil, fmt.Errorf("decode user after %s: %w", op, err)
	}
	return &u, nil
}
```

- [ ] **Step 4: Declare them on the interface**

In `user-service/service/service.go`, add to `UserRepository`:

```go
	AddPriorityContact(ctx context.Context, account, contact string, limit int, at time.Time) (*model.User, error)
	RemovePriorityContact(ctx context.Context, account, contact string, at time.Time) (*model.User, error)
```

Add `"time"` to that file's imports if absent.

- [ ] **Step 5: Regenerate mocks**

Run: `make generate SERVICE=user-service`

- [ ] **Step 6: Run tests to verify they pass**

Run: `make test-integration SERVICE=user-service && make test SERVICE=user-service`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
make fmt && make lint
git add user-service/mongorepo/users.go user-service/mongorepo/users_test.go user-service/service/service.go user-service/service/mocks/mock_repository.go
git commit -m "feat(user-service): add cap-guarded priority contact write methods"
```

---

### Task 5: Timestamp stamp on `UpdateUserSettings` + fanout helper

Fixes a pre-existing hole: the origin never stamps `settingsUpdatedAt`, so `inbox-worker`'s `$exists: false` branch always matches there and a stale remote event can overwrite a newer local edit. `UpdateUserChatlist` already stamps for exactly this reason.

**Files:**
- Modify: `user-service/mongorepo/users.go:109-152` (`UpdateUserSettings`)
- Modify: `user-service/service/service.go` (`UserRepository`)
- Modify: `user-service/service/settings.go:41-65`
- Test: `user-service/mongorepo/users_test.go`, `user-service/service/settings_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `UpdateUserSettings(ctx context.Context, account string, set *model.UserSettings, at time.Time) (*model.User, error)` — **signature change**, gains `at`
  - `(*UserService).publishSettingsFanouts(c *natsrouter.Context, account string, settings *model.UserSettings, now int64)`

- [ ] **Step 1: Write the failing tests**

Append to `user-service/mongorepo/users_test.go`:

```go
func TestUpdateUserSettings_StampsSettingsUpdatedAt_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	at := time.UnixMilli(1_700_000_000_000).UTC()
	seed(t, db, "users", bson.M{"_id": "u1", "account": "alice"})

	dark := "dark"
	u, err := r.UpdateUserSettings(ctx, "alice", &model.UserSettings{ThemePreference: &dark}, at)
	require.NoError(t, err)
	require.NotNil(t, u)

	// Without this stamp the origin doc has no settingsUpdatedAt, so inbox-worker's
	// $exists:false branch always matches and a stale remote event overwrites a
	// newer local edit.
	var doc struct {
		SettingsUpdatedAt time.Time `bson:"settingsUpdatedAt"`
	}
	require.NoError(t, db.Collection("users").FindOne(ctx, bson.M{"account": "alice"}).Decode(&doc))
	assert.Equal(t, at, doc.SettingsUpdatedAt.UTC())
}
```

This test constructs a `model.UserSettings`, so add `"github.com/hmchangw/chat/pkg/model"` to `users_test.go`'s imports.

Append to `user-service/service/settings_test.go`:

```go
// One mock backs both publishers, so the two fanouts are told apart by subject.
// They must carry the SAME timestamp: the client event and the cross-site replica
// have to agree on ordering.
func TestSetSettings_BothFanoutsShareOneTimestamp(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)

	updated := &model.UserSettings{FullWidth: ptrBool(true)}
	users.EXPECT().UpdateUserSettings(gomock.Any(), "alice", gomock.Any(), gomock.Any()).
		Return(&model.User{Settings: updated}, nil)

	var clientTS, inboxTS int64
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.SettingsUpdateEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			clientTS = evt.Timestamp
			return nil
		})
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserSettingsUpdated), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			var payload model.UserSettingsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &payload))
			inboxTS = payload.Timestamp
			return nil
		})

	_, err := svc.SetSettings(ctx("alice", "site-a"), models.SettingsSetRequest{
		UserSettings: model.UserSettings{FullWidth: ptrBool(true)},
	})
	require.NoError(t, err)
	assert.NotZero(t, clientTS)
	assert.Equal(t, clientTS, inboxTS)
}
```

Existing `UpdateUserSettings` expectations elsewhere in this file take three args and will
no longer compile. Add a fourth `gomock.Any()` to each, and add a fourth parameter to any
`DoAndReturn` callback (`func(_ any, _ string, set *model.UserSettings, _ time.Time) (*model.User, error)`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test-integration SERVICE=user-service`
Expected: FAIL — `UpdateUserSettings` takes 3 args, not 4.

- [ ] **Step 3: Stamp in the repo**

In `user-service/mongorepo/users.go`, change the signature and add the stamp right before `opts` is built:

```go
func (r *UserRepo) UpdateUserSettings(ctx context.Context, account string, set *model.UserSettings, at time.Time) (*model.User, error) {
```

and after the `InitialChatScrollPosition` nil-check block:

```go
	// Stamp the top-level settingsUpdatedAt (the same timestamp the service shares
	// with both fanouts) so the cross-site high-water guard sees a local edit;
	// without it an older inbound event could regress local state. Mirrors
	// UpdateUserChatlist. PriorityContacts is deliberately never referenced here —
	// the dedicated add/remove RPCs own it.
	fields["settingsUpdatedAt"] = at
```

- [ ] **Step 4: Update the interface and the service**

In `user-service/service/service.go`, change the `UserRepository` entry to:

```go
	UpdateUserSettings(ctx context.Context, account string, set *model.UserSettings, at time.Time) (*model.User, error)
```

In `user-service/service/settings.go`, rewrite the body of `SetSettings` between the validation and the return so `now` is computed **before** the write:

```go
	// One timestamp for the write and both fanouts, so the stored high-water mark,
	// the client event, and the cross-site replica all agree on ordering.
	now := time.Now().UTC().UnixMilli()
	u, err := s.users.UpdateUserSettings(c, account, &req.UserSettings, time.UnixMilli(now).UTC())
	if err != nil {
		return nil, fmt.Errorf("set settings: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	settings := u.Settings
	if settings == nil {
		// Unreachable after a non-empty $set; keep the reply shape total.
		settings = &model.UserSettings{}
	}
	s.publishSettingsFanouts(c, account, settings, now)
	return settings, nil
```

- [ ] **Step 5: Add the fanout helper**

In `user-service/service/settings.go`, add above `publishSettingsUpdate`:

```go
// publishSettingsFanouts emits both settings fanouts off one timestamp: the client
// event for the caller's other devices, and the cross-site inbox replica that every
// site's notification-worker reads locally. Both carry the FULL settings
// sub-document — the cross-site apply is a whole-object $set, so a partial payload
// wipes unrelated settings at remote sites. Every settings mutation must call this,
// not just publishSettingsUpdate: a client-only fanout leaves remote sites stale.
func (s *UserService) publishSettingsFanouts(c *natsrouter.Context, account string, settings *model.UserSettings, now int64) {
	s.publishSettingsUpdate(c, account, settings, now)
	s.publishSettingsInbox(c, account, settings, now)
}
```

- [ ] **Step 6: Regenerate mocks and run tests**

Run: `make generate SERVICE=user-service && make test SERVICE=user-service && make test-integration SERVICE=user-service`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
make fmt && make lint
git add user-service/mongorepo/users.go user-service/mongorepo/users_test.go user-service/service/service.go user-service/service/settings.go user-service/service/settings_test.go user-service/service/mocks/mock_repository.go
git commit -m "fix(user-service): stamp settingsUpdatedAt on the origin write, extract fanout helper"
```

---

### Task 6: Relax the cross-site high-water guard to `$lte`

Independent of the rest — can be done in any order after Task 5.

**Files:**
- Modify: `inbox-worker/main.go:204-210`
- Test: `inbox-worker/integration_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing — behavior change only.

- [ ] **Step 1: Write the failing test**

In `inbox-worker/integration_test.go`, next to the existing `"stale event is rejected by the settingsUpdatedAt high-water guard"` subtest (~line 1623), add:

```go
	t.Run("same-millisecond event is applied, not dropped", func(t *testing.T) {
		// The guard was $lt, so of two writes stamped in the same millisecond the
		// second was dropped at every remote site. Tapping through a contact picker
		// makes same-ms add/remove bursts reachable; the apply is an idempotent
		// whole-object replace, so $lte is safe.
		at := time.UnixMilli(1_700_000_000_000).UTC()
		dark, light := "dark", "light"

		require.NoError(t, store.UpdateUserSettings(ctx, "alice", &model.UserSettings{ThemePreference: &dark}, at))
		require.NoError(t, store.UpdateUserSettings(ctx, "alice", &model.UserSettings{ThemePreference: &light}, at))

		var doc struct {
			Settings model.UserSettings `bson:"settings"`
		}
		require.NoError(t, userCol.FindOne(ctx, bson.M{"account": "alice"}).Decode(&doc))
		require.NotNil(t, doc.Settings.ThemePreference)
		assert.Equal(t, "light", *doc.Settings.ThemePreference)
	})
```

Match the surrounding subtest's names for `store`, `ctx`, and `userCol`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=inbox-worker`
Expected: FAIL — got `"dark"`, want `"light"`; the second write was dropped.

- [ ] **Step 3: Relax the guard**

In `inbox-worker/main.go`, in `mongoInboxStore.UpdateUserSettings`:

```go
	// Guard on the settingsUpdatedAt high-water mark so an out-of-order or duplicate
	// event (settings fan to all sites) can't regress to older settings. $lte, not
	// $lt: two writes can share a millisecond, and dropping the second would leave a
	// remote site permanently behind. Safe because the apply is an idempotent
	// whole-object replace — a same-ms tie resolves to last-delivered.
	filter := bson.M{"account": account, "$or": bson.A{
		bson.M{"settingsUpdatedAt": bson.M{"$exists": false}},
		bson.M{"settingsUpdatedAt": bson.M{"$lte": updatedAt}},
	}}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test-integration SERVICE=inbox-worker && make test SERVICE=inbox-worker`
Expected: PASS — including the existing stale-event subtest, which must still reject a genuinely older event.

- [ ] **Step 5: Commit**

```bash
make fmt && make lint
git add inbox-worker/main.go inbox-worker/integration_test.go
git commit -m "fix(inbox-worker): apply same-millisecond settings events with an \$lte high-water guard"
```

---

### Task 7: `GetPriorityContacts` handler and enrichment

**Files:**
- Create: `user-service/service/prioritycontacts.go`
- Modify: `user-service/service/service.go:159` (registration, next to the settings handlers)
- Test: `user-service/service/prioritycontacts_test.go`

**Interfaces:**
- Consumes: `GetUserPriorityContacts`, `GetPriorityContactUsers` (Task 3); `s.apps.GetAppsByAssistants` (existing); `models.PriorityContact*` (Task 2); `subject.UserPriorityContactsGetPattern` (Task 1).
- Produces:
  - `maxPriorityContacts = 30`
  - `(*UserService).GetPriorityContacts(c *natsrouter.Context) (*models.PriorityContactsResponse, error)`
  - `(*UserService).enrichPriorityContacts(c *natsrouter.Context, contacts []string) []models.PriorityContactItem`
  - `storedPriorityContacts(u *model.User) []string`

- [ ] **Step 1: Write the failing tests**

Create `user-service/service/prioritycontacts_test.go`. It is an **internal** test (`package service`) and reuses the existing shared helpers from `service_test.go`: `newSvc(t)` (returns svc, subs, users, apps, rooms, history, pub), `ctx(account, siteID)`, and `requireCode(t, err, code)`. Do **not** write a parallel harness.

```go
package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/models"
)

// requireReason asserts the error carries a specific domain reason, which requireCode
// (code only) cannot express.
func requireReason(t *testing.T, err error, want errcode.Reason) {
	t.Helper()
	require.Error(t, err)
	var ee *errcode.Error
	require.True(t, errors.As(err, &ee), "want *errcode.Error, got %T", err)
	assert.Equal(t, want, ee.Reason)
}

func TestGetPriorityContacts_MixedListPreservesOrder(t *testing.T) {
	svc, _, users, apps, _, _, _ := newSvc(t)

	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").
		Return(&model.User{Settings: &model.UserSettings{
			PriorityContacts: []string{"bob", "helper.bot", "carol"},
		}}, nil)
	users.EXPECT().GetPriorityContactUsers(gomock.Any(), []string{"bob", "carol"}).
		Return(map[string]*models.PriorityContactUser{
			"bob":   {EngName: "Bob", ChineseName: "鮑伯", EmployeeID: "E9", SectName: "Ops"},
			"carol": {EngName: "Carol", ChineseName: "卡蘿", EmployeeID: "E7", SectName: "QA"},
		}, nil)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(map[string]*model.App{"helper.bot": {Name: "Helper"}}, nil)

	resp, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	require.NoError(t, err)
	require.Len(t, resp.Contacts, 3)

	assert.Equal(t, "bob", resp.Contacts[0].Account)
	assert.Equal(t, models.PriorityContactTypeUser, resp.Contacts[0].Type)
	require.NotNil(t, resp.Contacts[0].User)
	assert.Equal(t, "E9", resp.Contacts[0].User.EmployeeID)
	assert.Nil(t, resp.Contacts[0].App)

	assert.Equal(t, "helper.bot", resp.Contacts[1].Account)
	assert.Equal(t, models.PriorityContactTypeBot, resp.Contacts[1].Type)
	require.NotNil(t, resp.Contacts[1].App)
	assert.Equal(t, "Helper", resp.Contacts[1].App.Name)
	assert.Nil(t, resp.Contacts[1].User)

	assert.Equal(t, "carol", resp.Contacts[2].Account)
}

func TestGetPriorityContacts_EmptyList(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").
		Return(&model.User{Settings: nil}, nil)

	resp, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	require.NoError(t, err)
	assert.Empty(t, resp.Contacts)
}

func TestGetPriorityContacts_UserNotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "ghost").Return(nil, nil)

	_, err := svc.GetPriorityContacts(ctx("ghost", "site-a"))
	requireCode(t, err, errcode.CodeNotFound)
}

// An account that no longer resolves keeps account+type so the client renders a
// placeholder instead of the row vanishing.
func TestGetPriorityContacts_UnresolvedAccountsDegrade(t *testing.T) {
	svc, _, users, apps, _, _, _ := newSvc(t)

	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").
		Return(&model.User{Settings: &model.UserSettings{
			PriorityContacts: []string{"ghost", "gone.bot"},
		}}, nil)
	users.EXPECT().GetPriorityContactUsers(gomock.Any(), []string{"ghost"}).
		Return(map[string]*models.PriorityContactUser{}, nil)
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"gone.bot"}).
		Return(map[string]*model.App{}, nil)

	resp, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	require.NoError(t, err)
	require.Len(t, resp.Contacts, 2)
	assert.Equal(t, models.PriorityContactTypeUser, resp.Contacts[0].Type)
	assert.Nil(t, resp.Contacts[0].User)
	assert.Equal(t, models.PriorityContactTypeBot, resp.Contacts[1].Type)
	assert.Nil(t, resp.Contacts[1].App)
}

// A lookup failure degrades the rows rather than failing the call — same posture as
// the thread-list enrichment.
func TestGetPriorityContacts_LookupFailureDegrades(t *testing.T) {
	svc, _, users, apps, _, _, _ := newSvc(t)

	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").
		Return(&model.User{Settings: &model.UserSettings{
			PriorityContacts: []string{"bob", "helper.bot"},
		}}, nil)
	users.EXPECT().GetPriorityContactUsers(gomock.Any(), []string{"bob"}).
		Return(nil, errors.New("db down"))
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(nil, errors.New("db down"))

	resp, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	require.NoError(t, err)
	require.Len(t, resp.Contacts, 2)
	assert.Nil(t, resp.Contacts[0].User)
	assert.Nil(t, resp.Contacts[1].App)
}

func TestGetPriorityContacts_RepoError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").Return(nil, errors.New("db down"))

	_, err := svc.GetPriorityContacts(ctx("alice", "site-a"))
	// Raw wrapped error — the router classifies it, the handler must not pre-classify.
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=user-service`
Expected: FAIL — `svc.GetPriorityContacts` undefined.

- [ ] **Step 3: Implement the handler and enrichment**

Create `user-service/service/prioritycontacts.go`:

```go
package service

import (
	"fmt"
	"log/slog"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/user-service/models"
)

// maxPriorityContacts caps the stored list. The service owns the value; the repo
// takes it as a parameter and enforces it inside the update filter.
const maxPriorityContacts = 30

// GetPriorityContacts returns the caller's priority-contact list, enriched for display.
func (s *UserService) GetPriorityContacts(c *natsrouter.Context) (*models.PriorityContactsResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	u, err := s.users.GetUserPriorityContacts(c, account)
	if err != nil {
		return nil, fmt.Errorf("get priority contacts: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	return &models.PriorityContactsResponse{
		Contacts: s.enrichPriorityContacts(c, storedPriorityContacts(u)),
	}, nil
}

// storedPriorityContacts reads the list off a projected user doc, normalising a
// missing settings sub-document or absent field to an empty slice.
func storedPriorityContacts(u *model.User) []string {
	if u == nil || u.Settings == nil {
		return []string{}
	}
	return u.Settings.PriorityContacts
}

// enrichPriorityContacts builds display rows in stored order. Both lookups degrade
// like the thread-list enrichment: a failure logs a warn and yields a nil map, so
// rows come back with account+type and no detail object rather than failing the call.
func (s *UserService) enrichPriorityContacts(c *natsrouter.Context, contacts []string) []models.PriorityContactItem {
	out := make([]models.PriorityContactItem, 0, len(contacts))
	var userAccounts, botAccounts []string
	for _, a := range contacts {
		if model.IsBot(a) {
			botAccounts = append(botAccounts, a)
		} else {
			userAccounts = append(userAccounts, a)
		}
	}
	// Sequential, not parallel: two queries over at most maxPriorityContacts accounts.
	users := s.lookupPriorityContactUsers(c, userAccounts)
	apps := s.lookupPriorityContactApps(c, botAccounts)

	for _, a := range contacts {
		item := models.PriorityContactItem{Account: a, Type: models.PriorityContactTypeUser}
		if model.IsBot(a) {
			item.Type = models.PriorityContactTypeBot
			if app, ok := apps[a]; ok && app != nil {
				item.App = &models.PriorityContactApp{Name: app.Name}
			}
		} else if cu, ok := users[a]; ok && cu != nil {
			item.User = cu
		}
		out = append(out, item)
	}
	return out
}

// lookupPriorityContactUsers fetches display fields for the regular-user contacts;
// a failure or empty set degrades to nil (rows render without a user object).
func (s *UserService) lookupPriorityContactUsers(c *natsrouter.Context, accounts []string) map[string]*models.PriorityContactUser {
	if len(accounts) == 0 {
		return nil
	}
	got, err := s.users.GetPriorityContactUsers(c, accounts)
	if err != nil {
		slog.WarnContext(c, "priority contact user lookup degraded", "account", c.Param("account"),
			"request_id", natsutil.RequestIDFromContext(c), "error", err)
		return nil
	}
	return got
}

// lookupPriorityContactApps fetches app docs for the bot contacts; a failure or empty
// set degrades to nil (rows render without an app object).
func (s *UserService) lookupPriorityContactApps(c *natsrouter.Context, bots []string) map[string]*model.App {
	if len(bots) == 0 {
		return nil
	}
	got, err := s.apps.GetAppsByAssistants(c, bots)
	if err != nil {
		slog.WarnContext(c, "priority contact app lookup degraded", "account", c.Param("account"),
			"request_id", natsutil.RequestIDFromContext(c), "error", err)
		return nil
	}
	return got
}
```

- [ ] **Step 4: Register the handler**

In `user-service/service/service.go`, after the `UserSettingsSetPattern` registration:

```go
	natsrouter.RegisterNoBody(r, subject.UserPriorityContactsGetPattern(s.siteID), s.GetPriorityContacts)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=user-service`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
make fmt && make lint
git add user-service/service/prioritycontacts.go user-service/service/prioritycontacts_test.go user-service/service/service.go
git commit -m "feat(user-service): add priority contacts get handler with enrichment"
```

---

### Task 8: `AddPriorityContact` handler

**Files:**
- Modify: `user-service/service/prioritycontacts.go`
- Modify: `user-service/service/service.go` (registration)
- Test: `user-service/service/prioritycontacts_test.go`

**Interfaces:**
- Consumes: `AddPriorityContact`, `UserExists`, `GetUserPriorityContacts` (Tasks 3-4); `publishSettingsFanouts` (Task 5); `enrichPriorityContacts`, `storedPriorityContacts`, `maxPriorityContacts` (Task 7); `errcode.UserPriorityContactLimit`, `errcode.UserPriorityContactNotFound` (Task 1).
- Produces:
  - `(*UserService).AddPriorityContact(c *natsrouter.Context, req models.PriorityContactMutateRequest) (*models.PriorityContactsResponse, error)`
  - `(*UserService).priorityContactExists(c *natsrouter.Context, contact string) (bool, error)`

- [ ] **Step 1: Write the failing tests**

Append to `user-service/service/prioritycontacts_test.go`:

```go
func TestAddPriorityContact_Validation(t *testing.T) {
	cases := []struct {
		name    string
		contact string
	}{
		{"empty contact", ""},
		{"self add", "alice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, _, _, _, _, _ := newSvc(t)
			_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
				models.PriorityContactMutateRequest{ContactAccount: tc.contact})
			requireCode(t, err, errcode.CodeBadRequest)
		})
	}
}

func TestAddPriorityContact_UnknownUserIs404(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().UserExists(gomock.Any(), "ghost").Return(false, nil)

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "ghost"})
	requireCode(t, err, errcode.CodeNotFound)
	requireReason(t, err, errcode.UserPriorityContactNotFound)
}

func TestAddPriorityContact_UnknownBotIs404(t *testing.T) {
	svc, _, _, apps, _, _, _ := newSvc(t)

	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"gone.bot"}).
		Return(map[string]*model.App{}, nil)

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "gone.bot"})
	requireReason(t, err, errcode.UserPriorityContactNotFound)
}

// A client-only fanout would leave every remote site with a stale list, so both
// fanouts must fire off one timestamp. One mock backs both publishers — they are
// told apart by subject.
func TestAddPriorityContact_PublishesBothFanoutsWithOneTimestamp(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)

	users.EXPECT().UserExists(gomock.Any(), "bob").Return(true, nil)
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob", 30, gomock.Any()).
		Return(&model.User{Settings: &model.UserSettings{PriorityContacts: []string{"bob"}}}, nil)
	users.EXPECT().GetPriorityContactUsers(gomock.Any(), []string{"bob"}).
		Return(map[string]*models.PriorityContactUser{"bob": {EngName: "Bob"}}, nil)

	var clientTS, inboxTS int64
	var clientContacts []string
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.SettingsUpdateEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			clientTS = evt.Timestamp
			clientContacts = evt.Settings.PriorityContacts
			return nil
		})
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserSettingsUpdated), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			var payload model.UserSettingsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &payload))
			inboxTS = payload.Timestamp
			return nil
		})

	resp, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	require.NoError(t, err)
	require.Len(t, resp.Contacts, 1)
	assert.Equal(t, "bob", resp.Contacts[0].Account)

	assert.NotZero(t, clientTS)
	assert.Equal(t, clientTS, inboxTS)
	// The event carries raw accounts; devices refetch to render names.
	assert.Equal(t, []string{"bob"}, clientContacts)
}

func TestAddPriorityContact_AtCapIsForbidden(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	full := make([]string, 30)
	for i := range full {
		full[i] = fmt.Sprintf("seed%02d", i)
	}
	users.EXPECT().UserExists(gomock.Any(), "bob").Return(true, nil)
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob", 30, gomock.Any()).Return(nil, nil)
	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").
		Return(&model.User{Settings: &model.UserSettings{PriorityContacts: full}}, nil)

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	requireCode(t, err, errcode.CodeForbidden)
	requireReason(t, err, errcode.UserPriorityContactLimit)
}

// A duplicate add at exactly the cap is a no-op, not a violation: the cap filter
// rejects the write, but the contact is already present, so it must succeed.
func TestAddPriorityContact_DuplicateAtCapSucceeds(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	full := []string{"bob"}
	for i := 1; i < 30; i++ {
		full = append(full, fmt.Sprintf("seed%02d", i))
	}
	users.EXPECT().UserExists(gomock.Any(), "bob").Return(true, nil)
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob", 30, gomock.Any()).Return(nil, nil)
	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").
		Return(&model.User{Settings: &model.UserSettings{PriorityContacts: full}}, nil)
	users.EXPECT().GetPriorityContactUsers(gomock.Any(), gomock.Any()).
		Return(map[string]*models.PriorityContactUser{}, nil)

	resp, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	require.NoError(t, err)
	assert.Len(t, resp.Contacts, 30)
}

func TestAddPriorityContact_CallerNotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().UserExists(gomock.Any(), "bob").Return(true, nil)
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob", 30, gomock.Any()).Return(nil, nil)
	users.EXPECT().GetUserPriorityContacts(gomock.Any(), "alice").Return(nil, nil)

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestAddPriorityContact_RepoError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().UserExists(gomock.Any(), "bob").Return(true, nil)
	users.EXPECT().AddPriorityContact(gomock.Any(), "alice", "bob", 30, gomock.Any()).
		Return(nil, errors.New("db down"))

	_, err := svc.AddPriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	require.Error(t, err)
}
```

Add `"encoding/json"`, `"fmt"`, and `"github.com/hmchangw/chat/pkg/subject"` to the test file's imports.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=user-service`
Expected: FAIL — `svc.AddPriorityContact` undefined.

- [ ] **Step 3: Implement the handler**

Append to `user-service/service/prioritycontacts.go` (add `"time"` to the imports):

```go
// AddPriorityContact adds one contact to the caller's list, enforcing the cap inside
// the write, then fans the full post-update settings to the caller's other devices
// and to every other site.
func (s *UserService) AddPriorityContact(c *natsrouter.Context, req models.PriorityContactMutateRequest) (*models.PriorityContactsResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	contact := req.ContactAccount
	if contact == "" {
		return nil, errcode.BadRequest("contactAccount is required")
	}
	if contact == account {
		return nil, errcode.BadRequest("cannot add yourself as a priority contact")
	}

	exists, err := s.priorityContactExists(c, contact)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errcode.NotFound("priority contact not found",
			errcode.WithReason(errcode.UserPriorityContactNotFound))
	}

	// One timestamp for the write and both fanouts.
	now := time.Now().UTC().UnixMilli()
	u, err := s.users.AddPriorityContact(c, account, contact, maxPriorityContacts, time.UnixMilli(now).UTC())
	if err != nil {
		return nil, fmt.Errorf("add priority contact: %w", err)
	}
	if u == nil {
		// The filter rejects "no such user" and "already at cap" alike.
		return s.resolveAddPriorityContactMiss(c, account, contact)
	}
	return s.respondPriorityContacts(c, account, u, now), nil
}

// resolveAddPriorityContactMiss disambiguates a no-match add with one re-read, on
// the failure path only. A duplicate add at exactly the cap is a no-op, not a
// violation, so it succeeds with the unchanged list.
func (s *UserService) resolveAddPriorityContactMiss(c *natsrouter.Context, account, contact string) (*models.PriorityContactsResponse, error) {
	u, err := s.users.GetUserPriorityContacts(c, account)
	if err != nil {
		return nil, fmt.Errorf("resolve priority contact add: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	current := storedPriorityContacts(u)
	for _, a := range current {
		if a == contact {
			return &models.PriorityContactsResponse{Contacts: s.enrichPriorityContacts(c, current)}, nil
		}
	}
	if len(current) >= maxPriorityContacts {
		return nil, errcode.Forbidden("priority contact limit reached",
			errcode.WithReason(errcode.UserPriorityContactLimit))
	}
	// Reachable: the write missed because the list was at cap, then a concurrent
	// RemovePriorityContact for the same account dropped some other contact before
	// this re-read, so we now see the caller's doc, no match, and a list under cap.
	// The add itself is stale, not invalid, so tell the client to retry rather than
	// falsely reporting the caller as not found.
	return nil, errcode.Conflict("priority contacts changed concurrently, retry")
}

// priorityContactExists reports whether contact names something that can send
// messages: an app for a ".bot" account, an active user otherwise. The app need only
// exist, not be Enabled — a disabled assistant makes the entry inert, not wrong, and
// requiring Enabled would break re-adding whenever an admin disables an app.
func (s *UserService) priorityContactExists(c *natsrouter.Context, contact string) (bool, error) {
	if model.IsBot(contact) {
		apps, err := s.apps.GetAppsByAssistants(c, []string{contact})
		if err != nil {
			return false, fmt.Errorf("look up priority contact app: %w", err)
		}
		return apps[contact] != nil, nil
	}
	exists, err := s.users.UserExists(c, contact)
	if err != nil {
		return false, fmt.Errorf("look up priority contact user: %w", err)
	}
	return exists, nil
}

// respondPriorityContacts publishes both settings fanouts off the shared timestamp
// and builds the enriched reply. Shared by add and remove so neither can drift into
// publishing only the client event.
func (s *UserService) respondPriorityContacts(c *natsrouter.Context, account string, u *model.User, now int64) *models.PriorityContactsResponse {
	settings := u.Settings
	if settings == nil {
		// Unreachable after a matched update; keep the reply shape total.
		settings = &model.UserSettings{}
	}
	s.publishSettingsFanouts(c, account, settings, now)
	return &models.PriorityContactsResponse{
		Contacts: s.enrichPriorityContacts(c, settings.PriorityContacts),
	}
}
```

- [ ] **Step 4: Register the handler**

In `user-service/service/service.go`, after the get registration:

```go
	natsrouter.Register(r, subject.UserPriorityContactsAddPattern(s.siteID), s.AddPriorityContact)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=user-service`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
make fmt && make lint
git add user-service/service/prioritycontacts.go user-service/service/prioritycontacts_test.go user-service/service/service.go
git commit -m "feat(user-service): add priority contact add handler"
```

---

### Task 9: `RemovePriorityContact` handler

**Files:**
- Modify: `user-service/service/prioritycontacts.go`
- Modify: `user-service/service/service.go` (registration)
- Test: `user-service/service/prioritycontacts_test.go`

**Interfaces:**
- Consumes: `RemovePriorityContact` (Task 4); `respondPriorityContacts` (Task 8).
- Produces: `(*UserService).RemovePriorityContact(c *natsrouter.Context, req models.PriorityContactMutateRequest) (*models.PriorityContactsResponse, error)`

- [ ] **Step 1: Write the failing tests**

Append to `user-service/service/prioritycontacts_test.go`:

```go
func TestRemovePriorityContact_EmptyContactIsBadRequest(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)

	_, err := svc.RemovePriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: ""})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestRemovePriorityContact_PublishesBothFanoutsWithOneTimestamp(t *testing.T) {
	svc, _, users, apps, _, _, pub := newSvc(t)

	users.EXPECT().RemovePriorityContact(gomock.Any(), "alice", "bob", gomock.Any()).
		Return(&model.User{Settings: &model.UserSettings{PriorityContacts: []string{"helper.bot"}}}, nil)
	// Enrichment runs on the post-removal list — only the bot remains, so
	// GetPriorityContactUsers is never called (no user accounts left to look up).
	apps.EXPECT().GetAppsByAssistants(gomock.Any(), []string{"helper.bot"}).
		Return(map[string]*model.App{"helper.bot": {Name: "Helper"}}, nil)

	var clientTS, inboxTS int64
	pub.EXPECT().Publish(gomock.Any(), subject.SettingsUpdate("alice"), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.SettingsUpdateEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			clientTS = evt.Timestamp
			return nil
		})
	pub.EXPECT().Publish(gomock.Any(), subject.InboxExternal("site-b", model.InboxUserSettingsUpdated), gomock.Any()).
		DoAndReturn(func(_ any, _ string, data []byte) error {
			var evt model.InboxEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			var payload model.UserSettingsUpdated
			require.NoError(t, json.Unmarshal(evt.Payload, &payload))
			inboxTS = payload.Timestamp
			return nil
		})

	resp, err := svc.RemovePriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	require.NoError(t, err)
	require.Len(t, resp.Contacts, 1)
	assert.Equal(t, "helper.bot", resp.Contacts[0].Account)

	assert.NotZero(t, clientTS)
	assert.Equal(t, clientTS, inboxTS)
}

// Removing an entry that isn't in the list is a no-op that still succeeds.
func TestRemovePriorityContact_AbsentContactSucceeds(t *testing.T) {
	svc, _, users, _, _, _, pub := newSvc(t)

	users.EXPECT().RemovePriorityContact(gomock.Any(), "alice", "ghost", gomock.Any()).
		Return(&model.User{Settings: &model.UserSettings{PriorityContacts: []string{}}}, nil)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	resp, err := svc.RemovePriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "ghost"})
	require.NoError(t, err)
	assert.Empty(t, resp.Contacts)
}

func TestRemovePriorityContact_CallerNotFound(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().RemovePriorityContact(gomock.Any(), "alice", "bob", gomock.Any()).Return(nil, nil)

	_, err := svc.RemovePriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestRemovePriorityContact_RepoError(t *testing.T) {
	svc, _, users, _, _, _, _ := newSvc(t)

	users.EXPECT().RemovePriorityContact(gomock.Any(), "alice", "bob", gomock.Any()).
		Return(nil, errors.New("db down"))

	_, err := svc.RemovePriorityContact(ctx("alice", "site-a"),
		models.PriorityContactMutateRequest{ContactAccount: "bob"})
	require.Error(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=user-service`
Expected: FAIL — `svc.RemovePriorityContact` undefined.

- [ ] **Step 3: Implement the handler**

Append to `user-service/service/prioritycontacts.go`:

```go
// RemovePriorityContact removes one contact from the caller's list and fans the full
// post-update settings out. No existence check: removing an account that no longer
// exists is exactly the cleanup case to permit.
func (s *UserService) RemovePriorityContact(c *natsrouter.Context, req models.PriorityContactMutateRequest) (*models.PriorityContactsResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	contact := req.ContactAccount
	if contact == "" {
		return nil, errcode.BadRequest("contactAccount is required")
	}

	now := time.Now().UTC().UnixMilli()
	u, err := s.users.RemovePriorityContact(c, account, contact, time.UnixMilli(now).UTC())
	if err != nil {
		return nil, fmt.Errorf("remove priority contact: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	return s.respondPriorityContacts(c, account, u, now), nil
}
```

- [ ] **Step 4: Register the handler**

In `user-service/service/service.go`, after the add registration:

```go
	natsrouter.Register(r, subject.UserPriorityContactsRemovePattern(s.siteID), s.RemovePriorityContact)
```

- [ ] **Step 5: Run tests and check coverage**

Run: `make test SERVICE=user-service`
Expected: PASS

Then verify the coverage floor:

```bash
go test -coverprofile=/tmp/cover.out ./user-service/service/... && go tool cover -func=/tmp/cover.out | tail -1
```

Expected: ≥80% overall; the new `prioritycontacts.go` functions should be ≥90%. Add cases for any uncovered branch before moving on.

- [ ] **Step 6: Commit**

```bash
make fmt && make lint
git add user-service/service/prioritycontacts.go user-service/service/prioritycontacts_test.go user-service/service/service.go
git commit -m "feat(user-service): add priority contact remove handler"
```

---

### Task 10: Client API documentation

Required by CLAUDE.md §5: three new `chat.user.…` handlers plus a new field on a client-facing struct must update the canonical doc and both derived views in the same PR.

**Files:**
- Modify: `docs/client-api.md`
- Modify: `docs/client-api/request-reply.md`
- Modify: `docs/client-api/events.md:200`

**Interfaces:**
- Consumes: everything above.
- Produces: nothing.

- [ ] **Step 1: Add the three RPCs to `docs/client-api.md`**

Add a subsection after `settings.set`, matching the existing per-RPC layout (subject line, request field table, response field table, JSON example, error table). Requirements:

- `PriorityContactItem`, `PriorityContactUser`, and `PriorityContactApp` each get their **own named field table** with explicit types — never `object` — and are referenced by linked name (e.g. `[PriorityContactItem](#prioritycontactitem)`). Put them in §3.0 Shared schemas since three RPCs reference them.
- Every success response gets a JSON example. `PriorityContactItem[]` examples must show one user row and one bot row.
- Document that `add` is idempotent, that `remove` is idempotent, and that the cap is 30.
- Error rows: `bad_request` (missing `contactAccount`, self-add), `not_found` with reason `priority_contact_not_found` (unknown contact) and without a reason (caller's user doc missing), `forbidden` with reason `priority_contact_limit`.

- [ ] **Step 2: Update the settings sections and cross-references**

In `docs/client-api.md`:

- Index line `:62` — append the three new RPC links next to `settings.get` / `settings.set`
- Subject table `:4443` — add three rows
- `:4435` — the note reading *"No other endpoint emits a client-facing event"* is now false; rewrite it to say `settings.set` **and** the three `settings.priorityContacts.*` RPCs emit `settings.update`, and that all four also trigger the server-side cross-site federation update
- `settings.get` response table (`:4641` onward) — add a `priorityContacts` row, type `string[]`, noting it is read-only here and written only by the dedicated RPCs
- `:4701` — leave at **nine**. It describes the `settings.set` *request*, and `priorityContacts` is not settable
- `:4750` — nine → **ten** (the `settings.update` payload)

- [ ] **Step 3: Update the derived views**

In `docs/client-api/request-reply.md`, add the three RPCs in the same style as the existing settings entries.

In `docs/client-api/events.md:200`, change `all nine fields optional` to `all ten fields optional`, and note that `priorityContacts` arrives as raw accounts — a device that needs display names re-issues `settings.priorityContacts.get`.

- [ ] **Step 4: Verify no drift**

Re-read the three files and confirm every field name, type, and error reason matches the code from Tasks 1-9. In particular, `contactAccount`, `contacts`, `account`, `type`, `user`, `app`, `engName`, `chineseName`, `employeeId`, `sectName`, `name`.

- [ ] **Step 5: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md docs/client-api/events.md
git commit -m "docs(client-api): document the priority contacts RPCs"
```

---

## Final verification

- [ ] `make lint`
- [ ] `make test`
- [ ] `make test-integration SERVICE=user-service`
- [ ] `make test-integration SERVICE=inbox-worker`
- [ ] `make sast`
- [ ] Delete every file under `docs/reviews/` before opening a PR (CLAUDE.md §5) — none are created by this plan, but check.
