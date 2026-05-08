# Large-Room Post Restriction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reject top-level message sends in `message-gatekeeper` when the room has more than 500 members (env-tunable) and the sender is not an owner, admin, or bot. Thread replies and bypass-eligible senders skip the new room fetch entirely. Edits and deletes are unaffected.

**Architecture:** Single change point in `message-gatekeeper` plus two small shared additions. A backward-compatible `Code` field added to `model.ErrorResponse` for typed wire errors. A new `RoleAdmin` constant added to `pkg/model/subscription.go` (constant only — assignment wiring in `room-service` is owned by another team). Approach **B** ("owner fast-path") generalized: the bypass test (owner/admin/bot) runs before the new `Room` lookup, so all bypass-eligible senders pay zero added Mongo cost.

**Tech Stack:** Go 1.25, `caarlos0/env`, `go.mongodb.org/mongo-driver/v2`, `nats.go/jetstream`, `go.uber.org/mock`, `stretchr/testify`, `log/slog`.

**Spec:** `docs/superpowers/specs/2026-05-07-large-room-post-restriction-design.md`

---

## File map

**Shared model & helpers**
- Modify: `pkg/model/error.go` — add `Code string \`json:"code,omitempty"\`` field to `ErrorResponse`.
- Modify: `pkg/model/model_test.go` — add round-trip test for `ErrorResponse` covering both shapes; extend `TestRoleValues` with admin assertion.
- Modify: `pkg/model/subscription.go` — add `RoleAdmin Role = "admin"` constant.
- Modify: `pkg/natsutil/reply.go` — add `MarshalErrorWithCode(errMsg, code string) []byte`.
- Modify: `pkg/natsutil/reply_test.go` — add `TestMarshalErrorWithCode`.

**Gatekeeper internals**
- Create: `message-gatekeeper/helper.go` — `botPattern` regex + `isBot(account string) bool` (duplicates `room-service/helper.go:32` inline; promotion to shared `pkg/botid` is a future cleanup).
- Modify: `message-gatekeeper/store.go` — add `GetRoom` to `Store` interface, add `codedError` type, add `errLargeRoomPostRestricted` sentinel.
- Modify: `message-gatekeeper/store_mongo.go` — add `rooms` collection field to `MongoStore`, implement `GetRoom`.
- Modify: `message-gatekeeper/main.go` — add `LargeRoomThreshold` to `Config`, pass into `NewHandler`.
- Modify: `message-gatekeeper/handler.go` — add `largeRoomThreshold` field to `Handler`, update `NewHandler` constructor, add the rule check in `processMessage`, add `canBypassLargeRoomCap` predicate, add `marshalErrorReply` dispatch helper, update validation-error branch of `HandleJetStreamMsg`.
- Modify: `message-gatekeeper/handler_test.go` — extend the table test, fix `NewHandler` call sites, add `TestCanBypassLargeRoomCap`, add `TestIsBot`, add `TestHandler_marshalErrorReply`.
- Regenerate: `message-gatekeeper/mock_store_test.go` — via `make generate SERVICE=message-gatekeeper`.

---

## Task 1: Add `Code` field to `model.ErrorResponse`

**Files:**
- Modify: `pkg/model/error.go`
- Test: `pkg/model/model_test.go`

- [ ] **Step 1.1: Write the failing round-trip test**

Append to `pkg/model/model_test.go`:

```go
func TestErrorResponseJSON(t *testing.T) {
	t.Run("without code, omitempty hides the field", func(t *testing.T) {
		src := model.ErrorResponse{Error: "boom"}
		data, err := json.Marshal(src)
		require.NoError(t, err)
		assert.JSONEq(t, `{"error":"boom"}`, string(data))
		roundTrip(t, &src, &model.ErrorResponse{})
	})

	t.Run("with code, both fields present", func(t *testing.T) {
		src := model.ErrorResponse{Error: "blocked", Code: "large_room_post_restricted"}
		data, err := json.Marshal(src)
		require.NoError(t, err)
		assert.JSONEq(t, `{"error":"blocked","code":"large_room_post_restricted"}`, string(data))
		roundTrip(t, &src, &model.ErrorResponse{})
	})
}
```

If `assert`, `require`, or `json` are not already imported in `model_test.go`, leave them alone — they are already used by surrounding tests in this file. Confirm by checking the file's import block.

- [ ] **Step 1.2: Run the test and verify it fails**

```bash
cd /home/user/chat && go test ./pkg/model/ -run TestErrorResponseJSON -v
```

Expected: FAIL — `with code, both fields present` subtest fails because `model.ErrorResponse` has no `Code` field, so the literal `model.ErrorResponse{Error: "blocked", Code: "..."}` does not compile.

- [ ] **Step 1.3: Add the `Code` field**

Replace the entire contents of `pkg/model/error.go` with:

```go
package model

type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}
```

- [ ] **Step 1.4: Run the test and verify it passes**

```bash
cd /home/user/chat && go test ./pkg/model/ -run TestErrorResponseJSON -v
```

Expected: PASS for both subtests.

- [ ] **Step 1.5: Run the full `pkg/model` package to confirm no regressions**

```bash
cd /home/user/chat && go test ./pkg/model/ -race
```

Expected: PASS.

- [ ] **Step 1.6: Commit**

```bash
git add pkg/model/error.go pkg/model/model_test.go
git commit -m "feat(model): add backward-compatible Code field to ErrorResponse"
```

---

## Task 2: Add `MarshalErrorWithCode` helper to `pkg/natsutil`

**Files:**
- Modify: `pkg/natsutil/reply.go`
- Test: `pkg/natsutil/reply_test.go`

- [ ] **Step 2.1: Write the failing test**

Append to `pkg/natsutil/reply_test.go`:

```go
func TestMarshalErrorWithCode(t *testing.T) {
	data := natsutil.MarshalErrorWithCode("only owners can post in this room", "large_room_post_restricted")

	var got model.ErrorResponse
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, "only owners can post in this room", got.Error)
	assert.Equal(t, "large_room_post_restricted", got.Code)
}
```

(The existing `TestMarshalError` in this file already imports `model`, `json`, `assert`, `require`, and `natsutil`, so no new imports are needed.)

- [ ] **Step 2.2: Run the test and verify it fails**

```bash
cd /home/user/chat && go test ./pkg/natsutil/ -run TestMarshalErrorWithCode -v
```

Expected: FAIL — `undefined: natsutil.MarshalErrorWithCode`.

- [ ] **Step 2.3: Add the helper**

Append to `pkg/natsutil/reply.go` (after the existing `MarshalError` function):

```go
// MarshalErrorWithCode encodes an error message and machine-readable code
// as a JSON ErrorResponse. The code is omitted from the wire payload when
// empty (omitempty on the Code field).
func MarshalErrorWithCode(errMsg, code string) []byte {
	data, _ := json.Marshal(model.ErrorResponse{Error: errMsg, Code: code})
	return data
}
```

- [ ] **Step 2.4: Run the test and verify it passes**

```bash
cd /home/user/chat && go test ./pkg/natsutil/ -run TestMarshalErrorWithCode -v
```

Expected: PASS.

- [ ] **Step 2.5: Run the full `pkg/natsutil` package**

```bash
cd /home/user/chat && go test ./pkg/natsutil/ -race
```

Expected: PASS — including the existing `TestMarshalError` (verifies backward compatibility of the no-code path).

- [ ] **Step 2.6: Commit**

```bash
git add pkg/natsutil/reply.go pkg/natsutil/reply_test.go
git commit -m "feat(natsutil): add MarshalErrorWithCode helper for coded error replies"
```

---

## Task 3: Add `GetRoom` to gatekeeper `Store` + `MongoStore` impl + regen mocks

**Files:**
- Modify: `message-gatekeeper/store.go`
- Modify: `message-gatekeeper/store_mongo.go`
- Regenerate: `message-gatekeeper/mock_store_test.go`

(No new test for `GetRoom` itself — `MongoStore` implementations in this repo are exercised through handler tests with mocked stores, and the spec explicitly omits integration tests for this rule.)

- [ ] **Step 3.1: Add `GetRoom` to the `Store` interface**

In `message-gatekeeper/store.go`, replace the existing `Store` interface block with:

```go
type Store interface {
	GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error)
	GetRoom(ctx context.Context, roomID string) (*model.Room, error)
}
```

Leave the `//go:generate` directive at the top of the file untouched — it already includes `Store,ParentMessageFetcher` and will pick up the new method when regenerated.

- [ ] **Step 3.2: Update `MongoStore` to hold a `rooms` collection and implement `GetRoom`**

In `message-gatekeeper/store_mongo.go`, replace the entire file contents with:

```go
package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
)

type MongoStore struct {
	subscriptions *mongo.Collection
	rooms         *mongo.Collection
}

func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{
		subscriptions: db.Collection("subscriptions"),
		rooms:         db.Collection("rooms"),
	}
}

func (s *MongoStore) GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error) {
	var sub model.Subscription
	filter := bson.M{"u.account": account, "roomId": roomID}
	if err := s.subscriptions.FindOne(ctx, filter).Decode(&sub); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("user %s not subscribed to room %s: %w", account, roomID, errNotSubscribed)
		}
		return nil, fmt.Errorf("find subscription for user %s in room %s: %w", account, roomID, err)
	}
	return &sub, nil
}

// GetRoom fetches a room document by its ID. Any error (including
// mongo.ErrNoDocuments) is wrapped and returned — the handler treats every
// failure here as an infrastructure error, since reaching this call already
// implies a subscription for the room exists.
func (s *MongoStore) GetRoom(ctx context.Context, roomID string) (*model.Room, error) {
	var room model.Room
	if err := s.rooms.FindOne(ctx, bson.M{"_id": roomID}).Decode(&room); err != nil {
		return nil, fmt.Errorf("find room %q: %w", roomID, err)
	}
	return &room, nil
}
```

- [ ] **Step 3.3: Regenerate the gatekeeper mocks**

```bash
cd /home/user/chat && make generate SERVICE=message-gatekeeper
```

Expected: `mock_store_test.go` is rewritten to include a `GetRoom` mock alongside `GetSubscription`. No errors.

- [ ] **Step 3.4: Verify the package still compiles**

```bash
cd /home/user/chat && go build ./message-gatekeeper/
```

Expected: builds cleanly. Tests are not yet expected to pass (Task 7's new cases are not added yet); but compilation must succeed.

- [ ] **Step 3.5: Run the existing handler tests to confirm no regressions**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -race
```

Expected: existing tests still pass — the new `GetRoom` method is on the interface but no production code calls it yet, so existing scenarios are unchanged. (If gomock complains about strict `MockStore` setups missing `GetRoom`, that means a production code path now calls `GetRoom`, which it shouldn't until Task 7.)

- [ ] **Step 3.6: Commit**

```bash
git add message-gatekeeper/store.go message-gatekeeper/store_mongo.go message-gatekeeper/mock_store_test.go
git commit -m "feat(message-gatekeeper): add GetRoom to Store interface and MongoStore"
```

---

## Task 4: Add `LargeRoomThreshold` config + thread through Handler

**Files:**
- Modify: `message-gatekeeper/main.go`
- Modify: `message-gatekeeper/handler.go`
- Modify: `message-gatekeeper/handler_test.go`

This task only adds plumbing — no behavior change. The threshold field is read but never used yet.

- [ ] **Step 4.1: Add the `Config` field**

In `message-gatekeeper/main.go`, add this line inside the `Config` struct (place it directly below the existing `MaxWorkers` field at line 32):

```go
LargeRoomThreshold int             `env:"LARGE_ROOM_THRESHOLD" envDefault:"500"`
```

- [ ] **Step 4.2: Add the `largeRoomThreshold` field to `Handler` and update `NewHandler`**

In `message-gatekeeper/handler.go`, replace the existing `Handler` struct and `NewHandler` constructor (around lines 42-55) with:

```go
// Handler processes messages from the MESSAGES stream and validates them
// before publishing to MESSAGES_CANONICAL.
type Handler struct {
	store              Store
	publish            publishFunc
	reply              replyFunc
	siteID             string
	parentFetcher      ParentMessageFetcher
	largeRoomThreshold int
}

// NewHandler constructs a new Handler with the given dependencies.
func NewHandler(store Store, publish publishFunc, reply replyFunc, siteID string, parentFetcher ParentMessageFetcher, largeRoomThreshold int) *Handler {
	return &Handler{
		store:              store,
		publish:            publish,
		reply:              reply,
		siteID:             siteID,
		parentFetcher:      parentFetcher,
		largeRoomThreshold: largeRoomThreshold,
	}
}
```

- [ ] **Step 4.3: Update the `main.go` `NewHandler` call site**

In `message-gatekeeper/main.go`, find the line:

```go
handler := NewHandler(store, pub, reply, cfg.SiteID, parentFetcher)
```

(currently around line 87) and replace it with:

```go
handler := NewHandler(store, pub, reply, cfg.SiteID, parentFetcher, cfg.LargeRoomThreshold)
```

- [ ] **Step 4.4: Update the test `NewHandler` call sites**

In `message-gatekeeper/handler_test.go`, there are two `NewHandler` call sites (currently at lines 366 and 394). Replace each:

```go
h := NewHandler(store, pub, reply, "site1", nil)
```

with:

```go
h := NewHandler(store, pub, reply, "site1", nil, 500)
```

- [ ] **Step 4.5: Update the table-test runner's direct struct construction**

In `message-gatekeeper/handler_test.go`, find the inner test loop (currently around line 360):

```go
h := &Handler{
    store:   store,
    publish: pub,
    siteID:  validSiteID,
}
```

Replace with:

```go
h := &Handler{
    store:              store,
    publish:            pub,
    siteID:             validSiteID,
    largeRoomThreshold: 500,
}
```

- [ ] **Step 4.6: Build and run all gatekeeper tests**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -race
```

Expected: all existing tests pass. The new field is unused in production code paths, so there should be no behavior change.

- [ ] **Step 4.7: Commit**

```bash
git add message-gatekeeper/main.go message-gatekeeper/handler.go message-gatekeeper/handler_test.go
git commit -m "feat(message-gatekeeper): add LargeRoomThreshold config (no behavior change)"
```

---

## Task 4a: Add `RoleAdmin` constant to `pkg/model/subscription.go`

**Files:**
- Modify: `pkg/model/subscription.go`
- Test: `pkg/model/model_test.go`

Constant only — no role-update RPC support, no role-promotion logic, no
admin-aware invariants. The bypass clause in Task 5 will reference this.

- [ ] **Step 4a.1: Write the failing assertion**

In `pkg/model/model_test.go`, find the existing `TestRoleValues` function (currently around line 465):

```go
func TestRoleValues(t *testing.T) {
	if model.RoleOwner != "owner" {
		t.Errorf("RoleOwner = %q", model.RoleOwner)
	}
	if model.RoleMember != "member" {
		t.Errorf("RoleMember = %q", model.RoleMember)
	}
}
```

Replace its body with:

```go
func TestRoleValues(t *testing.T) {
	if model.RoleOwner != "owner" {
		t.Errorf("RoleOwner = %q", model.RoleOwner)
	}
	if model.RoleAdmin != "admin" {
		t.Errorf("RoleAdmin = %q", model.RoleAdmin)
	}
	if model.RoleMember != "member" {
		t.Errorf("RoleMember = %q", model.RoleMember)
	}
}
```

- [ ] **Step 4a.2: Run the test and verify it fails**

```bash
cd /home/user/chat && go test ./pkg/model/ -run TestRoleValues -v
```

Expected: FAIL — `undefined: model.RoleAdmin`.

- [ ] **Step 4a.3: Add the constant**

In `pkg/model/subscription.go`, replace the existing `Role` const block:

```go
const (
	RoleOwner  Role = "owner"
	RoleMember Role = "member"
)
```

with:

```go
const (
	RoleOwner  Role = "owner"
	RoleAdmin  Role = "admin"
	RoleMember Role = "member"
)
```

- [ ] **Step 4a.4: Run the test and verify it passes**

```bash
cd /home/user/chat && go test ./pkg/model/ -run TestRoleValues -v
```

Expected: PASS.

- [ ] **Step 4a.5: Run the full `pkg/model` package**

```bash
cd /home/user/chat && go test ./pkg/model/ -race
```

Expected: PASS — adding an enum value should not break any existing test
(callers iterate roles via range; no closed switches in `pkg/model` tests).

- [ ] **Step 4a.6: Commit**

```bash
git add pkg/model/subscription.go pkg/model/model_test.go
git commit -m "feat(model): add RoleAdmin constant (assignment wiring tracked separately)"
```

---

## Task 4b: Add `isBot` helper to `message-gatekeeper`

**Files:**
- Create: `message-gatekeeper/helper.go`
- Test: `message-gatekeeper/handler_test.go`

This duplicates `room-service/helper.go:32-45` inline. Promotion to a shared
`pkg/botid` is a future cleanup tracked in the spec's "Future follow-ups" —
not on this PR's path.

- [ ] **Step 4b.1: Write the failing test**

Append to `message-gatekeeper/handler_test.go`:

```go
func TestIsBot(t *testing.T) {
	cases := []struct {
		name    string
		account string
		want    bool
	}{
		{name: ".bot suffix", account: "helper.bot", want: true},
		{name: "p_ prefix", account: "p_scheduler", want: true},
		{name: "another bot suffix", account: "scheduler.bot", want: true},
		{name: "another p_ prefix", account: "p_webhook", want: true},
		{name: "plain account", account: "alice", want: false},
		{name: "contains bot but not suffix", account: "botmaster", want: false},
		{name: "empty string", account: "", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isBot(tc.account))
		})
	}
}
```

- [ ] **Step 4b.2: Run the test and verify it fails**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -run TestIsBot -v
```

Expected: FAIL — `undefined: isBot`.

- [ ] **Step 4b.3: Create the helper file**

Create new file `message-gatekeeper/helper.go` with the following contents:

```go
package main

import "regexp"

// botPattern matches account names treated as bots. Mirrors
// room-service/helper.go:32. Promotion to a shared pkg/botid is a future
// cleanup — keep both copies in sync if this regex changes here, since the
// other copy is owned by a separate developer.
var botPattern = regexp.MustCompile(`\.bot$|^p_`)

// isBot returns true if an account name matches the bot naming pattern
// (suffix `.bot` or prefix `p_`).
func isBot(account string) bool { return botPattern.MatchString(account) }
```

- [ ] **Step 4b.4: Run the test and verify it passes**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -run TestIsBot -v
```

Expected: PASS for all subtests.

- [ ] **Step 4b.5: Run the full gatekeeper test suite**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -race
```

Expected: PASS.

- [ ] **Step 4b.6: Commit**

```bash
git add message-gatekeeper/helper.go message-gatekeeper/handler_test.go
git commit -m "feat(message-gatekeeper): add inline isBot helper (duplicates room-service convention)"
```

---

## Task 5: Add `canBypassLargeRoomCap` predicate

**Files:**
- Modify: `message-gatekeeper/handler.go`
- Test: `message-gatekeeper/handler_test.go`

- [ ] **Step 5.1: Write the failing predicate test**

Append to `message-gatekeeper/handler_test.go`:

```go
func TestCanBypassLargeRoomCap(t *testing.T) {
	cases := []struct {
		name    string
		roles   []model.Role
		account string
		want    bool
	}{
		{name: "owner role bypasses", roles: []model.Role{model.RoleOwner}, account: "alice", want: true},
		{name: "admin role bypasses", roles: []model.Role{model.RoleAdmin}, account: "alice", want: true},
		{name: "member role does not bypass", roles: []model.Role{model.RoleMember}, account: "alice", want: false},
		{name: "owner + member bypasses", roles: []model.Role{model.RoleMember, model.RoleOwner}, account: "alice", want: true},
		{name: "admin + member bypasses", roles: []model.Role{model.RoleMember, model.RoleAdmin}, account: "alice", want: true},
		{name: "empty roles, plain account", roles: nil, account: "alice", want: false},
		{name: "bot account .bot suffix bypasses regardless of roles", roles: []model.Role{model.RoleMember}, account: "helper.bot", want: true},
		{name: "bot account p_ prefix bypasses regardless of roles", roles: []model.Role{model.RoleMember}, account: "p_scheduler", want: true},
		{name: "bot account with empty roles bypasses", roles: nil, account: "p_webhook", want: true},
		{name: "unknown role string with plain account", roles: []model.Role{"superuser"}, account: "alice", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := &model.Subscription{
				User:  model.SubscriptionUser{Account: tc.account},
				Roles: tc.roles,
			}
			got := canBypassLargeRoomCap(sub)
			assert.Equal(t, tc.want, got)
		})
	}
}
```

- [ ] **Step 5.2: Run the test and verify it fails**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -run TestCanBypassLargeRoomCap -v
```

Expected: FAIL with `undefined: canBypassLargeRoomCap`.

- [ ] **Step 5.3: Add the predicate function**

Append to the bottom of `message-gatekeeper/handler.go`:

```go
// canBypassLargeRoomCap reports whether the subscriber is exempt from the
// large-room post restriction. Owners, admins, and bots bypass.
//
// "Bot" is detected by account-name pattern (\.bot$|^p_) — see helper.go.
// This single function is the edit point if/when the bypass policy changes
// (e.g. promoting isBot to a shared package, adding new roles, etc.).
func canBypassLargeRoomCap(sub *model.Subscription) bool {
	for _, r := range sub.Roles {
		if r == model.RoleOwner || r == model.RoleAdmin {
			return true
		}
	}
	return isBot(sub.User.Account)
}
```

- [ ] **Step 5.4: Run the test and verify it passes**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -run TestCanBypassLargeRoomCap -v
```

Expected: PASS for all subtests.

- [ ] **Step 5.5: Run the full gatekeeper test suite**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -race
```

Expected: PASS.

- [ ] **Step 5.6: Commit**

```bash
git add message-gatekeeper/handler.go message-gatekeeper/handler_test.go
git commit -m "feat(message-gatekeeper): add canBypassLargeRoomCap predicate"
```

---

## Task 6: Add `codedError` type, sentinel, and `marshalErrorReply` dispatch helper

**Files:**
- Modify: `message-gatekeeper/store.go` (sentinel + type live here, alongside `errNotSubscribed`)
- Modify: `message-gatekeeper/handler.go` (add `marshalErrorReply` method, update `HandleJetStreamMsg`)
- Test: `message-gatekeeper/handler_test.go` (focused unit test for `marshalErrorReply`)

- [ ] **Step 6.1: Write the failing dispatch test**

Append to `message-gatekeeper/handler_test.go`:

```go
func TestHandler_marshalErrorReply(t *testing.T) {
	h := &Handler{}

	t.Run("plain error produces uncoded reply", func(t *testing.T) {
		data := h.marshalErrorReply(errors.New("user alice is not subscribed to room R"))
		var got model.ErrorResponse
		require.NoError(t, json.Unmarshal(data, &got))
		assert.Equal(t, "user alice is not subscribed to room R", got.Error)
		assert.Empty(t, got.Code)
		// omitempty: the wire bytes must not contain a "code" key.
		assert.NotContains(t, string(data), `"code"`)
	})

	t.Run("codedError produces coded reply", func(t *testing.T) {
		data := h.marshalErrorReply(errLargeRoomPostRestricted)
		var got model.ErrorResponse
		require.NoError(t, json.Unmarshal(data, &got))
		assert.Equal(t, "only owners can post in this room", got.Error)
		assert.Equal(t, "large_room_post_restricted", got.Code)
	})

	t.Run("wrapped codedError still dispatches", func(t *testing.T) {
		wrapped := fmt.Errorf("context: %w", errLargeRoomPostRestricted)
		data := h.marshalErrorReply(wrapped)
		var got model.ErrorResponse
		require.NoError(t, json.Unmarshal(data, &got))
		assert.Equal(t, "only owners can post in this room", got.Error)
		assert.Equal(t, "large_room_post_restricted", got.Code)
	})
}
```

- [ ] **Step 6.2: Run the test and verify it fails**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -run TestHandler_marshalErrorReply -v
```

Expected: FAIL — `undefined: errLargeRoomPostRestricted` and `undefined: (*Handler).marshalErrorReply`.

- [ ] **Step 6.3: Add the `codedError` type and sentinel in `store.go`**

In `message-gatekeeper/store.go`, replace the existing file contents with:

```go
package main

import (
	"context"
	"errors"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . Store,ParentMessageFetcher

// errNotSubscribed is returned when the user is not subscribed to the room.
var errNotSubscribed = errors.New("not subscribed")

// codedError pairs a stable wire code with a user-safe message. Returned by
// validation paths that want the reply to carry a machine-readable code.
type codedError struct {
	Code    string
	Message string
}

func (e *codedError) Error() string { return e.Message }

// errLargeRoomPostRestricted is returned when a non-owner attempts to post a
// top-level message in a room whose userCount exceeds the configured
// threshold.
var errLargeRoomPostRestricted = &codedError{
	Code:    "large_room_post_restricted",
	Message: "only owners can post in this room",
}

type Store interface {
	GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error)
	GetRoom(ctx context.Context, roomID string) (*model.Room, error)
}

// ParentMessageFetcher resolves a quoted parent message into a snapshot
// suitable for embedding on the new message's canonical event. Implementations
// should treat any failure (not found, RPC timeout, forbidden, etc.) as a
// reason to return an error — the handler soft-fails on every error and ships
// the message without the quote.
type ParentMessageFetcher interface {
	FetchQuotedParent(ctx context.Context, account, roomID, siteID, messageID string) (*cassandra.QuotedParentMessage, error)
}
```

- [ ] **Step 6.4: Add the `marshalErrorReply` method in `handler.go`**

Append to `message-gatekeeper/handler.go` (below `canBypassLargeRoomCap`):

```go
// marshalErrorReply produces the JSON reply payload for a validation error.
// If the error is (or wraps) a *codedError, the reply carries the code;
// otherwise the reply is the legacy uncoded shape.
func (h *Handler) marshalErrorReply(err error) []byte {
	var ce *codedError
	if errors.As(err, &ce) {
		return natsutil.MarshalErrorWithCode(ce.Message, ce.Code)
	}
	return natsutil.MarshalError(err.Error())
}
```

- [ ] **Step 6.5: Wire `marshalErrorReply` into `HandleJetStreamMsg`**

In `message-gatekeeper/handler.go`, find the validation-error reply line currently at line 78:

```go
h.sendReply(ctx, account, msg.Data(), natsutil.MarshalError(err.Error()))
```

Replace with:

```go
h.sendReply(ctx, account, msg.Data(), h.marshalErrorReply(err))
```

- [ ] **Step 6.6: Run the test and verify it passes**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -run TestHandler_marshalErrorReply -v
```

Expected: PASS for all three subtests (plain error, coded error, wrapped coded error).

- [ ] **Step 6.7: Run the full gatekeeper test suite**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -race
```

Expected: PASS, including all existing tests (the dispatch helper still routes plain errors through the legacy path).

- [ ] **Step 6.8: Commit**

```bash
git add message-gatekeeper/store.go message-gatekeeper/handler.go message-gatekeeper/handler_test.go
git commit -m "feat(message-gatekeeper): add codedError sentinel and reply dispatch helper"
```

---

## Task 7: Add the rule check in `processMessage` (the actual gate)

**Files:**
- Modify: `message-gatekeeper/handler.go`
- Modify: `message-gatekeeper/handler_test.go`

This is the task that introduces user-visible behavior. The tests come first per TDD: extend the table-driven `TestHandler_ProcessMessage` so each new scenario fails until the rule is implemented.

- [ ] **Step 7.1: Extend the table-test struct with a per-case `threshold` override**

In `message-gatekeeper/handler_test.go`, find the `tests := []struct{...}` literal inside `TestHandler_ProcessMessage`. Add two new fields to the struct definition:

```go
threshold int                                                       // 0 → use 500
checkErr  func(t *testing.T, err error)                             // optional; called on wantErr cases
```

Place these after the existing `wantInfra bool` field. The full struct definition should look like:

```go
tests := []struct {
    name        string
    account     string
    roomID      string
    siteID      string
    buildData   func() []byte
    setupStore  func(s *MockStore)
    setupPub    func() (publishFunc, *[]publishedMsg)
    wantErr     bool
    wantInfra   bool
    threshold   int
    checkErr    func(t *testing.T, err error)
    checkResult func(t *testing.T, data []byte, published []publishedMsg)
}{
```

- [ ] **Step 7.2: Update the inner runner to honor the new fields**

In the same function, find the inner loop (currently around line 358):

```go
for _, tc := range tests {
    t.Run(tc.name, func(t *testing.T) {
        ctrl := gomock.NewController(t)
        store := NewMockStore(ctrl)
        tc.setupStore(store)

        pub, publishedPtr := tc.setupPub()

        h := &Handler{
            store:              store,
            publish:            pub,
            siteID:             validSiteID,
            largeRoomThreshold: 500,
        }
        ...
```

Replace the `Handler` literal block with:

```go
threshold := tc.threshold
if threshold == 0 {
    threshold = 500
}
h := &Handler{
    store:              store,
    publish:            pub,
    siteID:             validSiteID,
    largeRoomThreshold: threshold,
}
```

Then, in the same function, find the existing `wantErr` branch:

```go
if tc.wantErr {
    require.Error(t, err)
    if tc.wantInfra {
        var ie *infraError
        assert.True(t, errors.As(err, &ie), "expected infraError, got %T: %v", err, err)
    } else {
        var ie *infraError
        assert.False(t, errors.As(err, &ie), "expected non-infra error, got infraError: %v", err)
    }
}
```

Add a `checkErr` call at the end of that block (still inside `if tc.wantErr`):

```go
if tc.wantErr {
    require.Error(t, err)
    if tc.wantInfra {
        var ie *infraError
        assert.True(t, errors.As(err, &ie), "expected infraError, got %T: %v", err, err)
    } else {
        var ie *infraError
        assert.False(t, errors.As(err, &ie), "expected non-infra error, got infraError: %v", err)
    }
    if tc.checkErr != nil {
        tc.checkErr(t, err)
    }
}
```

- [ ] **Step 7.3: Update existing happy-path setupStore expectations to include `GetRoom`**

The existing `sub` in `TestHandler_ProcessMessage` declares `Roles: []model.Role{model.RoleMember}`. Once the rule is added, member sends will call `GetRoom`. To keep the existing happy-path cases green, add a `GetRoom` expectation that returns a small room.

In `message-gatekeeper/handler_test.go`, locate the existing happy-path cases. Two will reach the new rule check (member, non-thread):

**"happy path"** — its `setupStore` currently looks like:

```go
setupStore: func(s *MockStore) {
    s.EXPECT().
        GetSubscription(gomock.Any(), validAccount, validRoomID).
        Return(sub, nil)
},
```

Replace with:

```go
setupStore: func(s *MockStore) {
    s.EXPECT().
        GetSubscription(gomock.Any(), validAccount, validRoomID).
        Return(sub, nil)
    s.EXPECT().
        GetRoom(gomock.Any(), validRoomID).
        Return(&model.Room{ID: validRoomID, UserCount: 1}, nil)
},
```

**"happy path with thread parent"** — leave its `setupStore` UNCHANGED. Thread replies bypass the `GetRoom` fetch entirely (Approach B fast-path), so adding a `GetRoom` expectation here would fail with "unexpected call" once the rule is implemented.

For any other existing happy-path case that uses the shared member `sub` and is **not** a thread reply, apply the same `GetRoom` addition. Inspect the table cases by reading the file end-to-end before making this change. If the case is a validation failure that returns before `GetSubscription`, it does NOT need a `GetRoom` expectation.

- [ ] **Step 7.4: Append the new test cases for the rule**

Inside the `tests := []struct{...}{` literal in `TestHandler_ProcessMessage`, just before the closing `}` (i.e., as the final entries), add the following new cases:

```go
{
    name:    "owner sends in big room — fast-path skips GetRoom",
    account: validAccount,
    roomID:  validRoomID,
    siteID:  validSiteID,
    buildData: func() []byte {
        req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
        data, _ := json.Marshal(req)
        return data
    },
    setupStore: func(s *MockStore) {
        s.EXPECT().
            GetSubscription(gomock.Any(), validAccount, validRoomID).
            Return(&model.Subscription{
                User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
                Roles: []model.Role{model.RoleOwner},
            }, nil)
        // No GetRoom expectation: owners must skip the fetch entirely.
    },
    setupPub: func() (publishFunc, *[]publishedMsg) {
        var published []publishedMsg
        return makePublishFunc(&published, nil), &published
    },
    wantErr: false,
},
{
    name:    "admin sends in big room — fast-path skips GetRoom",
    account: validAccount,
    roomID:  validRoomID,
    siteID:  validSiteID,
    buildData: func() []byte {
        req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
        data, _ := json.Marshal(req)
        return data
    },
    setupStore: func(s *MockStore) {
        s.EXPECT().
            GetSubscription(gomock.Any(), validAccount, validRoomID).
            Return(&model.Subscription{
                User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
                Roles: []model.Role{model.RoleAdmin},
            }, nil)
        // No GetRoom expectation: admins must skip the fetch entirely.
    },
    setupPub: func() (publishFunc, *[]publishedMsg) {
        var published []publishedMsg
        return makePublishFunc(&published, nil), &published
    },
    wantErr: false,
},
{
    name:    "bot account in big room with member role — fast-path skips GetRoom",
    account: "helper.bot",
    roomID:  validRoomID,
    siteID:  validSiteID,
    buildData: func() []byte {
        req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
        data, _ := json.Marshal(req)
        return data
    },
    setupStore: func(s *MockStore) {
        s.EXPECT().
            GetSubscription(gomock.Any(), "helper.bot", validRoomID).
            Return(&model.Subscription{
                User:  model.SubscriptionUser{ID: "u-bot", Account: "helper.bot"},
                Roles: []model.Role{model.RoleMember},
            }, nil)
        // No GetRoom expectation: bot accounts must skip the fetch entirely.
    },
    setupPub: func() (publishFunc, *[]publishedMsg) {
        var published []publishedMsg
        return makePublishFunc(&published, nil), &published
    },
    wantErr: false,
},
{
    name:    "member sends in big room — rejected with codedError",
    account: validAccount,
    roomID:  validRoomID,
    siteID:  validSiteID,
    buildData: func() []byte {
        req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
        data, _ := json.Marshal(req)
        return data
    },
    setupStore: func(s *MockStore) {
        s.EXPECT().
            GetSubscription(gomock.Any(), validAccount, validRoomID).
            Return(&model.Subscription{
                User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
                Roles: []model.Role{model.RoleMember},
            }, nil)
        s.EXPECT().
            GetRoom(gomock.Any(), validRoomID).
            Return(&model.Room{ID: validRoomID, UserCount: 600}, nil)
    },
    setupPub: func() (publishFunc, *[]publishedMsg) {
        var published []publishedMsg
        return makePublishFunc(&published, nil), &published
    },
    wantErr:   true,
    wantInfra: false,
    checkErr: func(t *testing.T, err error) {
        assert.ErrorIs(t, err, errLargeRoomPostRestricted)
    },
},
{
    name:    "member sends in small room — allowed",
    account: validAccount,
    roomID:  validRoomID,
    siteID:  validSiteID,
    buildData: func() []byte {
        req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
        data, _ := json.Marshal(req)
        return data
    },
    setupStore: func(s *MockStore) {
        s.EXPECT().
            GetSubscription(gomock.Any(), validAccount, validRoomID).
            Return(&model.Subscription{
                User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
                Roles: []model.Role{model.RoleMember},
            }, nil)
        s.EXPECT().
            GetRoom(gomock.Any(), validRoomID).
            Return(&model.Room{ID: validRoomID, UserCount: 50}, nil)
    },
    setupPub: func() (publishFunc, *[]publishedMsg) {
        var published []publishedMsg
        return makePublishFunc(&published, nil), &published
    },
    wantErr: false,
},
{
    name:    "boundary: count == threshold — allowed (strict greater-than)",
    account: validAccount,
    roomID:  validRoomID,
    siteID:  validSiteID,
    buildData: func() []byte {
        req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
        data, _ := json.Marshal(req)
        return data
    },
    setupStore: func(s *MockStore) {
        s.EXPECT().
            GetSubscription(gomock.Any(), validAccount, validRoomID).
            Return(&model.Subscription{
                User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
                Roles: []model.Role{model.RoleMember},
            }, nil)
        s.EXPECT().
            GetRoom(gomock.Any(), validRoomID).
            Return(&model.Room{ID: validRoomID, UserCount: 500}, nil)
    },
    setupPub: func() (publishFunc, *[]publishedMsg) {
        var published []publishedMsg
        return makePublishFunc(&published, nil), &published
    },
    wantErr: false,
},
{
    name:    "boundary: count == threshold + 1 — rejected",
    account: validAccount,
    roomID:  validRoomID,
    siteID:  validSiteID,
    buildData: func() []byte {
        req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
        data, _ := json.Marshal(req)
        return data
    },
    setupStore: func(s *MockStore) {
        s.EXPECT().
            GetSubscription(gomock.Any(), validAccount, validRoomID).
            Return(&model.Subscription{
                User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
                Roles: []model.Role{model.RoleMember},
            }, nil)
        s.EXPECT().
            GetRoom(gomock.Any(), validRoomID).
            Return(&model.Room{ID: validRoomID, UserCount: 501}, nil)
    },
    setupPub: func() (publishFunc, *[]publishedMsg) {
        var published []publishedMsg
        return makePublishFunc(&published, nil), &published
    },
    wantErr:   true,
    wantInfra: false,
    checkErr: func(t *testing.T, err error) {
        assert.ErrorIs(t, err, errLargeRoomPostRestricted)
    },
},
{
    name:    "member thread reply in big room — fast-path skips GetRoom",
    account: validAccount,
    roomID:  validRoomID,
    siteID:  validSiteID,
    buildData: func() []byte {
        parentID := idgen.GenerateMessageID()
        parentMillis := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC).UnixMilli()
        return []byte(fmt.Sprintf(
            `{"id":%q,"content":%q,"requestId":"req-1","threadParentMessageId":%q,"threadParentMessageCreatedAt":%d}`,
            idgen.GenerateMessageID(), validContent, parentID, parentMillis,
        ))
    },
    setupStore: func(s *MockStore) {
        s.EXPECT().
            GetSubscription(gomock.Any(), validAccount, validRoomID).
            Return(&model.Subscription{
                User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
                Roles: []model.Role{model.RoleMember},
            }, nil)
        // No GetRoom expectation: thread replies must skip the fetch entirely.
    },
    setupPub: func() (publishFunc, *[]publishedMsg) {
        var published []publishedMsg
        return makePublishFunc(&published, nil), &published
    },
    wantErr: false,
},
{
    name:    "GetRoom infra failure — wrapped as infraError",
    account: validAccount,
    roomID:  validRoomID,
    siteID:  validSiteID,
    buildData: func() []byte {
        req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
        data, _ := json.Marshal(req)
        return data
    },
    setupStore: func(s *MockStore) {
        s.EXPECT().
            GetSubscription(gomock.Any(), validAccount, validRoomID).
            Return(&model.Subscription{
                User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
                Roles: []model.Role{model.RoleMember},
            }, nil)
        s.EXPECT().
            GetRoom(gomock.Any(), validRoomID).
            Return(nil, errors.New("mongo unreachable"))
    },
    setupPub: func() (publishFunc, *[]publishedMsg) {
        return makePublishFunc(nil, nil), nil
    },
    wantErr:   true,
    wantInfra: true,
},
{
    name:    "custom threshold (env=2), 3-person room — rejected",
    account: validAccount,
    roomID:  validRoomID,
    siteID:  validSiteID,
    buildData: func() []byte {
        req := model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: validContent}
        data, _ := json.Marshal(req)
        return data
    },
    setupStore: func(s *MockStore) {
        s.EXPECT().
            GetSubscription(gomock.Any(), validAccount, validRoomID).
            Return(&model.Subscription{
                User:  model.SubscriptionUser{ID: "u1", Account: validAccount},
                Roles: []model.Role{model.RoleMember},
            }, nil)
        s.EXPECT().
            GetRoom(gomock.Any(), validRoomID).
            Return(&model.Room{ID: validRoomID, UserCount: 3}, nil)
    },
    setupPub: func() (publishFunc, *[]publishedMsg) {
        var published []publishedMsg
        return makePublishFunc(&published, nil), &published
    },
    threshold: 2,
    wantErr:   true,
    wantInfra: false,
    checkErr: func(t *testing.T, err error) {
        assert.ErrorIs(t, err, errLargeRoomPostRestricted)
    },
},
```

- [ ] **Step 7.5: Run the new tests and verify they fail**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -run TestHandler_ProcessMessage -v
```

Expected: the new cases FAIL. The reject cases pass `require.Error(t, err)` only because the rule isn't implemented yet — but the `gomock` mocks for `GetRoom` will be reported as "missing call" for the cases that expect `GetRoom`. Some "should bypass" cases (owner, thread) may pass already since they don't need the rule to be implemented (no `GetRoom` mock means no call, which is what we want once the bypass is implemented — but right now there's no rule at all, so they pass for the wrong reason). That's fine; Step 7.6 implements the rule and Step 7.7 verifies all cases pass for the right reason.

- [ ] **Step 7.6: Implement the rule check in `processMessage`**

In `message-gatekeeper/handler.go`, find the existing block in `processMessage` (currently around lines 151-158):

```go
sub, err := h.store.GetSubscription(ctx, account, roomID)
if err != nil {
    if errors.Is(err, errNotSubscribed) {
        return nil, fmt.Errorf("user %s is not subscribed to room %s", account, roomID)
    }
    return nil, &infraError{cause: fmt.Errorf("get subscription for user %s in room %s: %w", account, roomID, err)}
}

// Build Message
now := time.Now().UTC()
```

Insert the new rule block between the closing `}` of the GetSubscription error block and the `// Build Message` comment:

```go
sub, err := h.store.GetSubscription(ctx, account, roomID)
if err != nil {
    if errors.Is(err, errNotSubscribed) {
        return nil, fmt.Errorf("user %s is not subscribed to room %s", account, roomID)
    }
    return nil, &infraError{cause: fmt.Errorf("get subscription for user %s in room %s: %w", account, roomID, err)}
}

// Large-room post restriction: in rooms with more than the configured
// threshold of members, only owners may send top-level messages. Thread
// replies are exempt regardless of room size; owner sends are exempt
// regardless of room size. Both bypasses skip the Room fetch entirely
// (approach B — owner fast-path).
isThreadReply := req.ThreadParentMessageID != ""
if !isThreadReply && !canBypassLargeRoomCap(sub) {
    room, err := h.store.GetRoom(ctx, roomID)
    if err != nil {
        return nil, &infraError{cause: fmt.Errorf("get room %s for cap check: %w", roomID, err)}
    }
    if room.UserCount > h.largeRoomThreshold {
        slog.Info("send blocked",
            "reason", "large_room_post_restricted",
            "account", account,
            "roomID", roomID,
            "userCount", room.UserCount,
            "threshold", h.largeRoomThreshold,
        )
        return nil, errLargeRoomPostRestricted
    }
}

// Build Message
now := time.Now().UTC()
```

- [ ] **Step 7.7: Run the table tests and verify all pass**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -run TestHandler_ProcessMessage -v
```

Expected: PASS for all cases — including the existing ones (with their new `GetRoom` expectations from Step 7.3), the eight new cases, and any other cases in the table.

- [ ] **Step 7.8: Run the full gatekeeper suite with race detector**

```bash
cd /home/user/chat && go test ./message-gatekeeper/ -race
```

Expected: PASS, including `TestCanBypassLargeRoomCap` and `TestHandler_marshalErrorReply`.

- [ ] **Step 7.9: Commit**

```bash
git add message-gatekeeper/handler.go message-gatekeeper/handler_test.go
git commit -m "feat(message-gatekeeper): reject non-owner sends in rooms over threshold"
```

---

## Task 8: Final verification

**Files:** none modified — verification only.

- [ ] **Step 8.1: Run lint**

```bash
cd /home/user/chat && make lint
```

Expected: no warnings or errors. If `golangci-lint` complains about any new code, fix in place and re-run before commit.

- [ ] **Step 8.2: Run all unit tests across the repo with race**

```bash
cd /home/user/chat && make test
```

Expected: PASS. Particularly verifies that nothing in `pkg/model` or `pkg/natsutil` regressed for downstream consumers (history-service, room-service, etc.).

- [ ] **Step 8.3: If any lint or test failure was fixed, commit**

```bash
git add -p   # review
git commit -m "fix: address lint/test issues from large-room rule rollout"
```

(Skip this step if Steps 8.1 and 8.2 both passed cleanly.)

- [ ] **Step 8.4: Push the branch**

```bash
git push -u origin claude/validate-message-sending-5HTd9
```

Expected: clean push. Branch already exists on remote (the spec was pushed earlier); this just adds the new commits.

---

## Cross-task notes

- **Task ordering for predicate dependencies:** Task 5 (`canBypassLargeRoomCap`) references both `model.RoleAdmin` (added in Task 4a) and `isBot` (added in Task 4b). Both must complete before Task 5 starts.
- **Task 3 vs Task 4 ordering:** the mock regen (3.3) must precede the `largeRoomThreshold` plumbing (Task 4) only because the mocks reference the `Store` interface — Task 4 doesn't change the interface, so as long as Task 3 is fully complete first, Task 4 can proceed without mock issues.
- **Task 4a / 4b vs the rest:** Task 4a (`RoleAdmin` constant) only modifies `pkg/model`; Task 4b (`isBot` helper) only modifies `message-gatekeeper`. Both are fully independent of each other and could be reordered; the chosen sequence keeps the model change before the gatekeeper additions.
- **Task 5 vs Task 6 ordering:** the predicate (Task 5) is order-independent of Task 6 (codedError + dispatch). Either can come first. The order in this plan keeps the predicate isolated and easy to test before bringing in the wire format.
- **Don't skip the `make generate` step (3.3).** Without it, Task 7's `MockStore.EXPECT().GetRoom(...)` calls will fail to compile.
- **Bot regex drift:** `botPattern` in `message-gatekeeper/helper.go` (Task 4b) is a deliberate copy of `room-service/helper.go:32`. If you change the regex here, the spec's "Future follow-ups" section calls out promotion to `pkg/botid` as the eventual fix — but is owned by another team. Keep the test cases aligned with `room-service/helper_test.go:72-99` so divergence is detectable.
- **The `slog.Info` log line is emitted on the rejection path only.** No test asserts on the log line itself (capturing slog output in unit tests is brittle); the log content is reviewed during execution by tailing test output if curiosity strikes. The behavior under test is the error and reply payload.
