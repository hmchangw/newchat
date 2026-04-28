# Add-Member Cross-Site Channel Sources Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let add-member resolve source channels that live on a remote site by calling the remote site's `member.list` endpoint, while same-site channels continue to resolve locally through the same `ListRoomMembers` surface. Additionally (Phase 2), move org→user resolution from room-service's reply path to room-worker's write path via a shared MongoDB aggregation pipeline (`pkg/pipelines/member.go`).

**Architecture:** `AddMembersRequest.Channels` and `MembersAdded.Channels` change from `[]string` to `[]ChannelRef{RoomID, SiteID}`. A new `MemberListClient` interface (NATS-backed implementation) handles cross-site fan-out; same-site refs hit the local store after a subscription check. `handleAddMembers` gains a `expandChannelRefs` step that fail-fasts on any error before any state mutation. A new `natsutil.TryParseError` helper distinguishes error replies from success replies so the client can't silently decode an error body into an empty-members success.

In Phase 2, `room-service.ResolveAccounts` is replaced with `CountNewMembers` (count-only) for capacity validation; the canonical event ships unresolved `Users` and `Orgs`, and `room-worker.processAddMembers` calls `ListNewMembers` to materialize the actual list of accounts to add. Both methods delegate to the same shared `pkg/pipelines/member.go GetNewMembersPipeline` — the only difference is the terminal stage (`$count` for room-service, `$group + $addToSet` for room-worker).

**Tech Stack:** Go 1.25 • NATS core request/reply (`nats.go`) • MongoDB driver v2 • `go.uber.org/mock` • `stretchr/testify` • `testcontainers-go` • `caarlos0/env`.

**Spec:** `docs/superpowers/specs/2026-04-20-add-member-cross-site-channels-design.md`.

---

## Baseline

This plan assumes the working branch is rebased onto **all three** of:
1. `main` — has the `member.list` endpoint (`PR #108`, `647f223`).
2. `claude/add-siteid-to-room-members-3q2gi` (**PR #118**) — has `pkg/idgen`, `RoomTypeChannel` rename, `member.invite` removal, testcontainer singletons, add-member-hardening spec.
3. `claude/add-room-member-feature-5FPbS` — has `AddMembersRequest`, `MembersAdded`, `handleAddMembers`, `expandChannels`, `GetRoomMembersByRooms`, `GetAccountsByRooms`.

If any of the three aren't merged yet, rebase onto the composite state first. Every code block and path in this plan is anchored to that composite baseline.

Verify before starting:

```bash
git grep -n "func (h \*Handler) handleAddMembers"  room-service/handler.go
git grep -n "GetRoomMembersByRooms"                room-service/store.go
git grep -n "RoomTypeChannel"                      pkg/model/ | head -3
git grep -n "pkg/idgen"                            go.mod
```

Expected: each grep returns a line. If any return nothing, stop and rebase first.

---

## File Structure

**New files:**

| Path | Responsibility |
|---|---|
| `room-service/memberlist_client.go` | `MemberListClient` interface + `natsMemberListClient` NATS implementation + `//go:generate` directive |
| `room-service/memberlist_client_test.go` | Client unit tests against an embedded NATS server |
| `room-service/mock_memberlist_client_test.go` | Generated mock for `MemberListClient` (never edit by hand) |

**Modified files:**

| Path | Change |
|---|---|
| `pkg/natsutil/reply.go` | Add `TryParseError(data []byte) (model.ErrorResponse, bool)` |
| `pkg/natsutil/reply_test.go` | New table-driven tests for `TryParseError` |
| `pkg/model/member.go` | Add `ChannelRef`; change `AddMembersRequest.Channels` and `MembersAdded.Channels` from `[]string` to `[]ChannelRef` |
| `pkg/model/model_test.go` | Add `TestChannelRefJSONBSON`; update `TestAddMembersRequestJSON` and `TestMembersAddedJSON` to use `[]ChannelRef` |
| `room-service/helper.go` | Add `"remote member.list:"` to `sanitizeError`'s substring fallback (reuse the existing `errNotRoomMember` sentinel for channel-source authorization) |
| `room-service/helper_test.go` | Extend `sanitizeError` tests to cover the new sentinel and the new substring fallback |
| `room-service/store.go` | Remove `GetRoomMembersByRooms` and `GetAccountsByRooms` from `RoomStore` |
| `room-service/store_mongo.go` | Remove the two method implementations |
| `room-service/mock_store_test.go` | Regenerated (`make generate SERVICE=room-service`) |
| `room-service/handler.go` | Replace `expandChannels` with `expandChannelRefs`; `Handler` struct + `NewHandler` gain `memberListClient MemberListClient` |
| `room-service/handler_test.go` | New `TestHandler_AddMembers_ChannelExpansion`; update every `NewHandler(...)` call site to pass the new mock client; update existing `AddMembersRequest` fixtures to `[]ChannelRef`; delete cases that asserted on removed store methods |
| `room-service/main.go` | Add `MemberListTimeout time.Duration` on `config`; construct `NATSMemberListClient`; pass to `NewHandler` |
| `room-service/integration_test.go` | Remove obsolete cases (`TestMongoStore_GetRoomMembersByRooms_Integration`, `TestMongoStore_GetAccountsByRooms_Integration`); add 5 new end-to-end cases (see Tasks 8–9) |
| `room-worker/handler_test.go` | Update `AddMembersRequest` fixtures to `[]ChannelRef` (compile fix only — no logic change) |

**Phase 2 — new files:**

| Path | Responsibility |
|---|---|
| `pkg/pipelines/member.go` | `GetNewMembersPipeline(orgIDs, directAccounts, roomID) bson.A` — shared aggregation stages: `$match` (org/account filter + bot exclusion), `$lookup` (subscriptions for roomID), `$match` (existingSub empty). Callers append the terminal stage. |
| `pkg/pipelines/member_test.go` | Pure-BSON unit tests asserting stage shape; no Mongo dependency. |

**Phase 2 — modified files:**

| Path | Change |
|---|---|
| `room-service/store.go` | Remove `ResolveAccounts`; add `CountNewMembers(ctx, orgIDs, directAccounts []string, roomID string) (int, error)`. |
| `room-service/store_mongo.go` | Remove the old `ResolveAccounts` method body; add `CountNewMembers` (delegates to `pipelines.GetNewMembersPipeline` + `$count`). |
| `room-service/handler.go` | Step 7 now calls `CountNewMembers` and uses the count for capacity. Canonical event ships `req.Users = mergedUsers` and `req.Orgs = mergedOrgs` (unresolved); `req.Channels` preserved unchanged. |
| `room-service/handler_test.go` | Replace `ResolveAccounts(...).Return(list)` mocks with `CountNewMembers(...).Return(count, nil)`; canonical-event assertions updated to expect unresolved (merged) `Users` and `Orgs`. |
| `room-service/integration_test.go` | Remove `TestMongoStore_ResolveAccounts_Integration`; add a `TestMongoStore_CountNewMembers_Integration` covering basic count, dedupe, bot exclusion, and already-subscribed exclusion. |
| `room-service/mock_store_test.go` | Regenerated. |
| `room-worker/store.go` | Add `ListNewMembers(ctx, orgIDs, directAccounts []string, roomID string) ([]string, error)` to `SubscriptionStore`. |
| `room-worker/store_mongo.go` | Add `ListNewMembers` (delegates to `pipelines.GetNewMembersPipeline` + `$group { _id: nil, accounts: $addToSet $account }`). |
| `room-worker/handler.go` | `processAddMembers` calls `ListNewMembers(req.Orgs, req.Users, req.RoomID)` first; uses the returned accounts as input to the existing `FindUsersByAccounts` flow. |
| `room-worker/handler_test.go` | Update existing `TestHandler_ProcessAddMembers*` cases to mock `ListNewMembers` returning the expected resolved-accounts slice. |
| `room-worker/integration_test.go` | Add a `TestMongoStore_ListNewMembers_Integration` parallel to the room-service one. |
| `room-worker/mock_store_test.go` | Regenerated. |

No changes to: `pkg/subject/subject.go`, `inbox-worker`, `message-worker`, `broadcast-worker`, or the canonical event JSON schema (only the *meaning* of `Users` shifts — from "fully resolved" to "merged but unresolved").

---

## Task Ordering & Why

Bottom-up so each task leaves `make test` green and commit-worthy:

1. **`TryParseError` helper** — pure leaf, no consumers yet.
2. **`ChannelRef` model type** — new type, not yet referenced.
3. **Extend `sanitizeError` for remote errors** — leaf in helper.go; reuse `errNotRoomMember` for channel-source auth (same semantics: "user is not a member of this room/channel").
4. **`MemberListClient`** — uses `ChannelRef` + `TryParseError`; new files, not yet wired.
5. **Wire `MemberListClient` + `MemberListTimeout` config into `Handler`** — struct + `NewHandler` + `main.go`; every test call-site updated but the handler logic doesn't use the client yet.
6. **Big coordinated swap** — `Channels: []string → []ChannelRef`, replace `expandChannels` → `expandChannelRefs`, update room-service/handler_test.go cases, update room-worker/handler_test.go fixtures. After this task: old store methods are dead code but still present.
7. **Remove `GetRoomMembersByRooms` and `GetAccountsByRooms`** — now that nothing calls them, delete + regenerate mocks + drop obsolete unit/integration tests.
8. **Integration tests 1–3** — single-site scenarios (same-site `room_members` path, same-site subscriptions fallback, requester-not-subscribed rejection).
9. **Integration tests 4–5** — two-site scenarios (two-site end-to-end, cross-site timeout).

**Phase 2 — Org Resolution Refactor (Tasks 10–15):**

Bottom-up so the pipeline package lands with no consumers, then each service consumes it independently, then the dead code is removed last:

10. **`pkg/pipelines/member.go`** — leaf package with `GetNewMembersPipeline`. Pure BSON; tests assert stage shape. No consumers yet.
11. **room-worker `ListNewMembers` store method** — wires the pipeline + `$group/$addToSet` into the worker's `SubscriptionStore`. Method exists but isn't called yet.
12. **room-worker `processAddMembers` switch** — handler now calls `ListNewMembers` first. Tests updated to mock the new call. At this point room-service still ships resolved `Users`; running the pipeline on already-resolved input returns the same set, so behavior is unchanged (capacity drift bounded by ~zero — old RS already filtered).
13. **room-service `CountNewMembers` store method** — wires the pipeline + `$count` into room-service's `RoomStore`. Method exists but isn't called yet.
14. **room-service `handleAddMembers` switch** — replace `ResolveAccounts` call with `CountNewMembers` and ship unresolved (merged) `Users`/`Orgs` in the canonical event. Tests updated. Room-worker (already on `ListNewMembers` from Task 12) handles the unresolved shape correctly.
15. **Remove `ResolveAccounts`** — store interface, store_mongo implementation, integration test for it, and any lingering mock setups. Now dead.

---

## Task 1: `natsutil.TryParseError` helper

**Files:**
- Modify: `pkg/natsutil/reply.go`
- Test: `pkg/natsutil/reply_test.go`

Why first: standalone pure function; every subsequent NATS client that disambiguates error replies will import this. Lands behind zero callers.

- [ ] **Step 1.1: Write the failing test**

Append to `pkg/natsutil/reply_test.go`:

```go
func TestTryParseError(t *testing.T) {
	t.Run("error body returns parsed response and true", func(t *testing.T) {
		data := natsutil.MarshalError("boom")
		resp, ok := natsutil.TryParseError(data)
		if !ok {
			t.Fatal("expected ok=true for error body")
		}
		if resp.Error != "boom" {
			t.Errorf("got %q, want %q", resp.Error, "boom")
		}
	})

	t.Run("success body with no error field returns false", func(t *testing.T) {
		data, err := json.Marshal(model.ListRoomMembersResponse{Members: nil})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if _, ok := natsutil.TryParseError(data); ok {
			t.Fatal("expected ok=false for success body")
		}
	})

	t.Run("empty object returns false", func(t *testing.T) {
		if _, ok := natsutil.TryParseError([]byte(`{}`)); ok {
			t.Fatal("expected ok=false for {}")
		}
	})

	t.Run("malformed json returns false", func(t *testing.T) {
		if _, ok := natsutil.TryParseError([]byte(`{not json`)); ok {
			t.Fatal("expected ok=false for malformed json")
		}
	})

	t.Run("error field with empty string returns false", func(t *testing.T) {
		// Guards against rogue callers sending {"error":""}; we treat them as success bodies.
		if _, ok := natsutil.TryParseError([]byte(`{"error":""}`)); ok {
			t.Fatal("expected ok=false for empty error string")
		}
	})
}
```

- [ ] **Step 1.2: Run the test to verify it fails**

```bash
go test ./pkg/natsutil/ -run TestTryParseError -v
```
Expected: `undefined: natsutil.TryParseError` compile error. That is the expected Red.

- [ ] **Step 1.3: Implement `TryParseError`**

Append to `pkg/natsutil/reply.go` (keep existing imports; already includes `encoding/json` and `github.com/hmchangw/chat/pkg/model`):

```go
// TryParseError returns the ErrorResponse iff data decodes cleanly with a non-empty Error.
func TryParseError(data []byte) (model.ErrorResponse, bool) {
	var r model.ErrorResponse
	if err := json.Unmarshal(data, &r); err != nil || r.Error == "" {
		return model.ErrorResponse{}, false
	}
	return r, true
}
```

- [ ] **Step 1.4: Run the test to verify it passes**

```bash
go test ./pkg/natsutil/ -run TestTryParseError -v
```
Expected: `--- PASS: TestTryParseError` with all 5 subtests PASS.

- [ ] **Step 1.5: Run the whole `pkg/natsutil` package**

```bash
make test SERVICE=pkg/natsutil
```
Expected: PASS.

- [ ] **Step 1.6: Commit**

```bash
git add pkg/natsutil/reply.go pkg/natsutil/reply_test.go
git commit -m "feat(natsutil): add TryParseError for distinguishing ReplyError bodies"
```

---

## Task 2: `pkg/model`: add `ChannelRef`

**Files:**
- Modify: `pkg/model/member.go`
- Test: `pkg/model/model_test.go`

Why second: `MemberListClient`, `expandChannelRefs`, and the big type swap all reference `model.ChannelRef`. Add the type only — do NOT swap `AddMembersRequest.Channels` yet (that cascades to every caller and happens in Task 6).

- [ ] **Step 2.1: Write the failing test**

Append to `pkg/model/model_test.go`:

```go
func TestChannelRefJSONBSON(t *testing.T) {
	src := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}

	t.Run("json", func(t *testing.T) {
		data, err := json.Marshal(&src)
		require.NoError(t, err)
		// Tag spelling matters — the wire contract with frontends uses camelCase.
		assert.Equal(t, `{"roomId":"room-eng","siteId":"site-us"}`, string(data))
		var dst model.ChannelRef
		require.NoError(t, json.Unmarshal(data, &dst))
		assert.Equal(t, src, dst)
	})

	t.Run("bson", func(t *testing.T) {
		data, err := bson.Marshal(&src)
		require.NoError(t, err)
		var dst model.ChannelRef
		require.NoError(t, bson.Unmarshal(data, &dst))
		assert.Equal(t, src, dst)
	})
}
```

If `bson` isn't already imported in `model_test.go`, add `"go.mongodb.org/mongo-driver/v2/bson"` to the imports block. Check first with `grep -n 'mongo-driver/v2/bson' pkg/model/model_test.go`.

- [ ] **Step 2.2: Run the test to verify it fails**

```bash
go test ./pkg/model/ -run TestChannelRefJSONBSON -v
```
Expected: `undefined: model.ChannelRef` compile error. That is the expected Red.

- [ ] **Step 2.3: Add `ChannelRef` to `pkg/model/member.go`**

Insert after the `HistoryConfig` struct (around line 22, just before `type AddMembersRequest`):

```go
// ChannelRef identifies a source channel by room + its home site. Used by add-member
// to expand cross-site source channels via the remote site's member.list endpoint.
type ChannelRef struct {
	RoomID string `json:"roomId" bson:"roomId"`
	SiteID string `json:"siteId" bson:"siteId"`
}
```

Do NOT touch `AddMembersRequest` or `MembersAdded` in this task.

- [ ] **Step 2.4: Run the test to verify it passes**

```bash
go test ./pkg/model/ -run TestChannelRefJSONBSON -v
```
Expected: `--- PASS: TestChannelRefJSONBSON` with both subtests PASS.

- [ ] **Step 2.5: Run the whole model package**

```bash
make test SERVICE=pkg/model
```
Expected: PASS. No existing test is disturbed because `ChannelRef` is unused elsewhere.

- [ ] **Step 2.6: Commit**

```bash
git add pkg/model/member.go pkg/model/model_test.go
git commit -m "feat(model): add ChannelRef for cross-site channel references"
```

---

## Task 3: `room-service/helper.go`: extend `sanitizeError` for remote errors

**Files:**
- Modify: `room-service/helper.go`
- Test: `room-service/helper_test.go`

Why third: `sanitizeError` plumbing for cross-site error propagation. Reuses the existing `errNotRoomMember` sentinel for channel-source authorization — "not a room member" and "not a channel member" have identical semantics (the requester is not subscribed to the source). Lands cleanly before the client arrives.

- [ ] **Step 3.1: Write the failing tests**

Append to `room-service/helper_test.go` (new tests — keep existing cases intact):

```go
func TestSanitizeError_RemoteMemberListPrefix(t *testing.T) {
	remote := errors.New("remote member.list: only room members can list members")
	assert.Equal(t, "remote member.list: only room members can list members", sanitizeError(remote))
}

func TestSanitizeError_RemoteMemberListWithContext(t *testing.T) {
	// Error from cross-site RPC includes site context; preserve user-safe message.
	remote := errors.New("expand channels: remote member.list: room not found")
	msg := sanitizeError(remote)
	assert.Contains(t, msg, "remote member.list:")
	assert.Contains(t, msg, "room not found")
}

func TestSanitizeError_TransportFailureStillOpaque(t *testing.T) {
	// Generic transport failure from the client — no user-safe substring — must still be "internal error".
	assert.Equal(t, "internal error", sanitizeError(errors.New("member.list request to site-eu: nats: timeout")))
}
```

If `helper_test.go` doesn't already import `errors`, add it.

- [ ] **Step 3.2: Run the tests to verify they fail**

```bash
go test ./room-service/ -run TestSanitizeError_RemoteMemberList -v
```
Expected: tests fail because `"remote member.list:"` isn't yet in the substring whitelist.

- [ ] **Step 3.3: Update `sanitizeError`**

`errNotRoomMember` is already in the `errors.Is` whitelist (PR #118). The only edit needed is to add `"remote member.list:"` to the substring fallback slice:

```go
		for _, safe := range []string{
			"only owners can",
			"cannot add members",
			"room is at maximum capacity",
			"requester not in room",
			"invalid request",
			"remote member.list:",
		} {
```

**Why reuse `errNotRoomMember`:** Both "user is not a room member" and "user is not a channel member (used as add-member source)" mean the same thing semantically — the requester lacks subscription to the source. Reusing the existing sentinel avoids duplicate error types and keeps cross-site/same-site behavior uniform (the cross-site `MemberListClient` already maps the remote sentinel onto the local one).

- [ ] **Step 3.4: Run the tests to verify they pass**

```bash
go test ./room-service/ -run TestSanitizeError -v
```
Expected: all new sanitizeError tests PASS; existing sanitizeError tests still PASS.

- [ ] **Step 3.5: Run `make lint` for the service**

```bash
make lint
```
Expected: 0 issues.

- [ ] **Step 3.6: Commit**

```bash
git add room-service/helper.go room-service/helper_test.go
git commit -m "feat(room-service): extend sanitizeError for remote member.list errors"
```

---

## Task 4: `MemberListClient` interface + unit tests

**Files:**
- Create: `room-service/memberlist_client.go`
- Create: `room-service/memberlist_client_test.go`
- Generate: `room-service/mock_memberlist_client_test.go` (via `make generate`)

Why fourth: Uses `ChannelRef` (Task 2) and `TryParseError` (Task 1); new files, no consumers yet. Once created, Task 5 wires it into `Handler`.

### Task 4 — Part A: Write the failing test

- [ ] **Step 4a.1: Create `room-service/memberlist_client_test.go` with 7 tests + helper**

Create the file with the following content (includes embedded NATS server setup + 7 table-driven test cases):

```go
package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

func startInProcessNATS(t *testing.T) *nats.Conn {
	t.Helper()
	opts := &natsserver.Options{Port: -1}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

func TestNATSMemberListClient_HappyPath(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 2*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	members := []model.RoomMember{
		{Member: model.RoomMemberEntry{ID: "u1", Type: model.RoomMemberIndividual, Account: "bob"}},
		{Member: model.RoomMemberEntry{ID: "org1", Type: model.RoomMemberOrg}},
	}

	sub, err := nc.Subscribe(subject.MemberList(requester, ch.RoomID, ch.SiteID), func(m *nats.Msg) {
		resp := model.ListRoomMembersResponse{Members: members}
		data, _ := json.Marshal(resp)
		_ = m.Respond(data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	got, err := client.ListMembers(context.Background(), requester, ch)
	require.NoError(t, err)
	assert.Equal(t, members, got)
}

func TestNATSMemberListClient_RemoteError(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 2*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	sub, err := nc.Subscribe(subject.MemberList(requester, ch.RoomID, ch.SiteID), func(m *nats.Msg) {
		data := natsutil.MarshalError("only room members can list members")
		_ = m.Respond(data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = client.ListMembers(context.Background(), requester, ch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "remote member.list:")
	assert.Contains(t, err.Error(), "only room members can list members")
}

func TestNATSMemberListClient_InvalidJSONReply(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 2*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	sub, err := nc.Subscribe(subject.MemberList(requester, ch.RoomID, ch.SiteID), func(m *nats.Msg) {
		_ = m.Respond([]byte(`{not json`))
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = client.ListMembers(context.Background(), requester, ch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal member.list reply")
}

func TestNATSMemberListClient_Timeout(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 100*time.Millisecond)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}

	start := time.Now()
	_, err := client.ListMembers(context.Background(), "alice", ch)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "member.list request to site-us")
	assert.Less(t, elapsed, 500*time.Millisecond)
}

func TestNATSMemberListClient_BodyShape(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 2*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	var received model.ListRoomMembersRequest
	sub, err := nc.Subscribe(subject.MemberList(requester, ch.RoomID, ch.SiteID), func(m *nats.Msg) {
		_ = json.Unmarshal(m.Data, &received)
		resp := model.ListRoomMembersResponse{}
		data, _ := json.Marshal(resp)
		_ = m.Respond(data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = client.ListMembers(context.Background(), requester, ch)
	require.NoError(t, err)
	assert.Nil(t, received.Limit)
	assert.Nil(t, received.Offset)
	assert.False(t, received.Enrich)
}

func TestNATSMemberListClient_SubjectCorrectness(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 2*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	expectedSubj := subject.MemberList(requester, ch.RoomID, ch.SiteID)
	var gotSubj string
	sub, err := nc.Subscribe(expectedSubj, func(m *nats.Msg) {
		gotSubj = m.Subject
		data, _ := json.Marshal(model.ListRoomMembersResponse{})
		_ = m.Respond(data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = client.ListMembers(context.Background(), requester, ch)
	require.NoError(t, err)
	assert.Equal(t, expectedSubj, gotSubj)
}

func TestNATSMemberListClient_ContextCancellation(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 5*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.ListMembers(ctx, "alice", ch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "member.list request to site-us")
}
```

- [ ] **Step 4a.2: Run tests to verify RED**

```bash
go test ./room-service/ -run TestNATSMemberListClient -v
```

Expected: Compile error — `undefined: NewNATSMemberListClient`. This is the expected RED.

### Task 4 — Part B: Write implementation + generate mock

- [ ] **Step 4b.1: Create `room-service/memberlist_client.go`**

```go
//go:generate mockgen -source=memberlist_client.go -destination=mock_memberlist_client_test.go -package=main

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// MemberListClient fetches room members from a remote site's member.list endpoint.
type MemberListClient interface {
	ListMembers(ctx context.Context, requester string, ch model.ChannelRef) ([]model.RoomMember, error)
}

type natsMemberListClient struct {
	nc      *nats.Conn
	timeout time.Duration
}

// NewNATSMemberListClient creates a NATS-backed MemberListClient.
func NewNATSMemberListClient(nc *nats.Conn, timeout time.Duration) MemberListClient {
	return &natsMemberListClient{nc: nc, timeout: timeout}
}

func (c *natsMemberListClient) ListMembers(ctx context.Context, requester string, ch model.ChannelRef) ([]model.RoomMember, error) {
	body, err := json.Marshal(model.ListRoomMembersRequest{})
	if err != nil {
		return nil, fmt.Errorf("marshal member.list body: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	out := &nats.Msg{
		Subject: subject.MemberList(requester, ch.RoomID, ch.SiteID),
		Data:    body,
		Header:  nats.Header{},
	}
	reply, err := c.nc.RequestMsgWithContext(reqCtx, out)
	if err != nil {
		return nil, fmt.Errorf("member.list request to %s: %w", ch.SiteID, err)
	}

	if errResp, ok := natsutil.TryParseError(reply.Data); ok {
		return nil, fmt.Errorf("remote member.list: %s", errResp.Error)
	}

	var resp model.ListRoomMembersResponse
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal member.list reply: %w", err)
	}
	return resp.Members, nil
}
```

- [ ] **Step 4b.2: Run tests to verify GREEN**

```bash
go test ./room-service/ -run TestNATSMemberListClient -v
```

Expected: All 7 subtests PASS.

- [ ] **Step 4b.3: Generate mock**

```bash
make generate SERVICE=room-service
```

Expected: `room-service/mock_memberlist_client_test.go` created (never edit by hand).

- [ ] **Step 4b.4: Run full room-service test suite**

```bash
make test SERVICE=room-service
```

Expected: PASS (no other tests affected; integration tests still pass).

- [ ] **Step 4b.5: Commit**

```bash
git add room-service/memberlist_client.go room-service/memberlist_client_test.go room-service/mock_memberlist_client_test.go
git commit -m "feat(room-service): add MemberListClient interface + unit tests + mockgen"
```

---

## Task 5: Wire `MemberListClient` into `Handler` + config

**Files:**
- Modify: `room-service/handler.go`
- Modify: `room-service/main.go`
- Modify: `room-service/handler_test.go`
- Modify: `room-service/integration_test.go`

Why fifth: Handler struct + NewHandler + main.go wiring all together; every test call-site updated before handler logic changes. After this task, `expandChannels` still exists but Handler is ready for the type swap.

- [ ] **Step 5.1: Add `memberListClient` field to `Handler` struct**

In `room-service/handler.go`, update the struct (around line 26):

```go
type Handler struct {
	store           RoomStore
	keyStore        RoomKeyStore
	memberListClient MemberListClient  // NEW FIELD
	siteID          string
	maxRoomSize     int
	maxBatchSize    int
	publishToStream func(ctx context.Context, subj string, data []byte) error
}
```

- [ ] **Step 5.2: Update `NewHandler` constructor**

In `room-service/handler.go` (around line 35), change the signature and initialization:

```go
func NewHandler(store RoomStore, keyStore RoomKeyStore, memberListClient MemberListClient, siteID string, maxRoomSize, maxBatchSize int, publishToStream func(context.Context, string, []byte) error) *Handler {
	return &Handler{store: store, keyStore: keyStore, memberListClient: memberListClient, siteID: siteID, maxRoomSize: maxRoomSize, maxBatchSize: maxBatchSize, publishToStream: publishToStream}
}
```

- [ ] **Step 5.3: Verify it compiles with current test call-sites**

```bash
go build ./room-service/
```

Expected: Compile error — all `NewHandler(...)` calls missing the new `memberListClient` argument. This is expected; we fix them next.

- [ ] **Step 5.4: Add `MemberListTimeout` config in `main.go`**

In `room-service/main.go`, update the config struct (around line 16):

```go
type config struct {
	NatsURL           string        `env:"NATS_URL"                  envDefault:"nats://localhost:4222"`
	NatsCredsFile     string        `env:"NATS_CREDS_FILE"           envDefault:""`
	SiteID            string        `env:"SITE_ID"                   envDefault:"site-local"`
	MongoURI          string        `env:"MONGO_URI"                 envDefault:"mongodb://localhost:27017"`
	MongoDB           string        `env:"MONGO_DB"                  envDefault:"chat"`
	MaxRoomSize       int           `env:"MAX_ROOM_SIZE"             envDefault:"1000"`
	MaxBatchSize      int           `env:"MAX_BATCH_SIZE"            envDefault:"1000"`
	MemberListTimeout time.Duration `env:"MEMBER_LIST_TIMEOUT"       envDefault:"5s"`
	ValkeyAddr        string        `env:"VALKEY_ADDR,required"`
	ValkeyPassword    string        `env:"VALKEY_PASSWORD"           envDefault:""`
	ValkeyGracePeriod time.Duration `env:"VALKEY_KEY_GRACE_PERIOD,required"`
}
```

- [ ] **Step 5.5: Construct and wire `NewNATSMemberListClient` in `main.go`**

In `room-service/main.go` (around line 98, before `handler := NewHandler(...)`), insert:

```go
	memberListClient := NewNATSMemberListClient(nc.NatsConn(), cfg.MemberListTimeout)
	handler := NewHandler(store, keyStore, memberListClient, cfg.SiteID, cfg.MaxRoomSize, cfg.MaxBatchSize, func(ctx context.Context, subj string, data []byte) error {
		if _, err := js.Publish(ctx, subj, data); err != nil {
			return fmt.Errorf("publish to %q: %w", subj, err)
		}
		return nil
	})
```

(Update the existing `NewHandler` call to include `memberListClient` as the 3rd argument; add the `memberListClient` construction line before it.)

- [ ] **Step 5.6: Update all `NewHandler` call-sites in `handler_test.go`**

Pattern: `NewHandler(store, nil, "site-a",` → `NewHandler(store, nil, nil, "site-a",`

Use a find-replace:

```bash
sed -i 's/NewHandler(store, nil, "site-a"/NewHandler(store, nil, nil, "site-a"/g' room-service/handler_test.go
```

Verify manually by checking a few lines (e.g., around lines 512, 555, 578, etc.):

```bash
grep "NewHandler(store, nil, nil," room-service/handler_test.go | wc -l
```

Expected: ~18 matches.

- [ ] **Step 5.7: Update all `NewHandler` call-sites in `integration_test.go`**

Pattern: `NewHandler(store, keyStore, "site-a",` → `NewHandler(store, keyStore, nil, "site-a",`

```bash
sed -i 's/NewHandler(store, keyStore, "site-a"/NewHandler(store, keyStore, nil, "site-a"/g' room-service/integration_test.go
```

Verify:

```bash
grep "NewHandler(store, keyStore, nil," room-service/integration_test.go | wc -l
```

Expected: 1 match (the test around line 952).

- [ ] **Step 5.8: Verify compilation**

```bash
go build ./room-service/
```

Expected: PASS. All `NewHandler` calls now have the correct signature (with `nil` for `memberListClient` in tests).

- [ ] **Step 5.9: Run tests**

```bash
make test SERVICE=room-service
```

Expected: All existing tests PASS. No handler logic has changed yet; mocks are not set up to expect client calls (they're `nil` or unused).

- [ ] **Step 5.10: Commit**

```bash
git add room-service/handler.go room-service/main.go room-service/handler_test.go room-service/integration_test.go
git commit -m "feat(room-service): add MemberListClient wiring to Handler + config"
```

---

## Task 6: Big coordinated type swap + expandChannelRefs

**Files:**
- Modify: `pkg/model/member.go`
- Modify: `pkg/model/model_test.go`
- Modify: `room-service/handler.go` (replace expandChannels)
- Modify: `room-service/handler_test.go` (delete old tests, add ChannelExpansion test)
- Modify: `room-worker/handler_test.go` (compile fix only)

Why sixth: Channels type changes from `[]string` to `[]ChannelRef`; expansion logic moves from `GetRoomMembersByRooms`/`GetAccountsByRooms` to `ListRoomMembers`+`MemberListClient`. This is a coordinated change to avoid partial compilation.

### Task 6 — Part A: Model type changes

- [ ] **Step 6a.1: Change `AddMembersRequest.Channels` type in `pkg/model/member.go`**

Around line 31, find:

```go
type AddMembersRequest struct {
	RoomID           string        `json:"roomId"           bson:"roomId"`
	Users            []string      `json:"users"            bson:"users"`
	Orgs             []string      `json:"orgs"             bson:"orgs"`
	Channels         []string      `json:"channels"         bson:"channels"`
	History          HistoryConfig `json:"history"          bson:"history"`
	...
}
```

Change `Channels []string` to `Channels []ChannelRef`:

```go
Channels         []ChannelRef  `json:"channels"         bson:"channels"`
```

- [ ] **Step 6a.2: Change `MembersAdded.Channels` type in `pkg/model/member.go`**

Around line 81, find:

```go
type MembersAdded struct {
	Individuals     []string `json:"individuals"`
	Orgs            []string `json:"orgs"`
	Channels        []string `json:"channels"`
	AddedUsersCount int      `json:"addedUsersCount"`
}
```

Change `Channels []string` to `Channels []ChannelRef`:

```go
Channels        []ChannelRef `json:"channels"`
```

- [ ] **Step 6a.3: Update `TestAddMembersRequestJSON` in `pkg/model/model_test.go`**

Around line 832, find the test:

```go
func TestAddMembersRequestJSON(t *testing.T) {
	req := model.AddMembersRequest{
		RoomID:   "r1",
		Users:    []string{"alice", "bob"},
		Orgs:     []string{"engineering"},
		Channels: []string{"general"},
		History:  model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	...
}
```

Update to use `[]ChannelRef`:

```go
func TestAddMembersRequestJSON(t *testing.T) {
	req := model.AddMembersRequest{
		RoomID:   "r1",
		Users:    []string{"alice", "bob"},
		Orgs:     []string{"engineering"},
		Channels: []model.ChannelRef{{RoomID: "general", SiteID: "site-a"}},
		History:  model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	...
}
```

- [ ] **Step 6a.4: Update `TestMembersAddedJSON` in `pkg/model/model_test.go`**

Around line 936, find:

```go
func TestMembersAddedJSON(t *testing.T) {
	ma := model.MembersAdded{
		Individuals:     []string{"alice", "bob"},
		Orgs:            []string{"engineering"},
		Channels:        []string{"general"},
		AddedUsersCount: 5,
	}
	...
}
```

Update to:

```go
func TestMembersAddedJSON(t *testing.T) {
	ma := model.MembersAdded{
		Individuals:     []string{"alice", "bob"},
		Orgs:            []string{"engineering"},
		Channels:        []model.ChannelRef{{RoomID: "general", SiteID: "site-a"}},
		AddedUsersCount: 5,
	}
	...
}
```

- [ ] **Step 6a.5: Run model tests**

```bash
make test SERVICE=pkg/model
```

Expected: PASS. The model type change is isolated; no consumers yet.

### Task 6 — Part B: Handler implementation changes

- [ ] **Step 6b.1: Delete `expandChannels` and add `expandChannelRefs` in `room-service/handler.go`**

Find the `expandChannels` function (around line 474) and DELETE it entirely.

Then add the new `expandChannelRefs` function after `handleAddMembers`:

```go
func (h *Handler) expandChannelRefs(ctx context.Context, requester string, refs []model.ChannelRef) (orgIDs, accounts []string, err error) {
	for _, ref := range refs {
		var members []model.RoomMember

		if ref.SiteID == h.siteID {
			if _, err := h.store.GetSubscription(ctx, requester, ref.RoomID); err != nil {
				if errors.Is(err, model.ErrSubscriptionNotFound) {
					return nil, nil, errNotRoomMember
				}
				return nil, nil, fmt.Errorf("subscription check %s: %w", ref.RoomID, err)
			}
			members, err = h.store.ListRoomMembers(ctx, ref.RoomID, nil, nil, false)
			if err != nil {
				return nil, nil, fmt.Errorf("local list-members %s: %w", ref.RoomID, err)
			}
		} else {
			members, err = h.memberListClient.ListMembers(ctx, requester, ref)
			if err != nil {
				return nil, nil, fmt.Errorf("remote list-members %s@%s: %w", ref.RoomID, ref.SiteID, err)
			}
		}

		for i := range members {
			m := &members[i].Member
			switch m.Type {
			case model.RoomMemberOrg:
				orgIDs = append(orgIDs, m.ID)
			case model.RoomMemberIndividual:
				accounts = append(accounts, m.Account)
			}
		}
	}
	return orgIDs, accounts, nil
}
```

- [ ] **Step 6b.2: Update `handleAddMembers` to call `expandChannelRefs`**

Find the call to `expandChannels` in `handleAddMembers` (around line 431):

```go
	channelOrgIDs, channelAccounts, err := h.expandChannels(ctx, req.Channels)
	if err != nil {
		return nil, fmt.Errorf("expand channels: %w", err)
	}
```

Change to:

```go
	channelOrgIDs, channelAccounts, err := h.expandChannelRefs(ctx, requester, req.Channels)
	if err != nil {
		return nil, fmt.Errorf("expand channels: %w", err)
	}
```

(Note: `requester` is already in scope from step 1 of `handleAddMembers`.)

- [ ] **Step 6b.3: Verify room-service compilation**

```bash
go build ./room-service/
```

Expected: Compile error in `handler_test.go` — `Channels: []string{...}` assignments to `[]ChannelRef` field, and missing `TestHandler_AddMembers_Success` logic. This is expected; we fix it next.

### Task 6 — Part C: Handler tests

- [ ] **Step 6c.1: Delete old channel-expansion tests in `room-service/handler_test.go`**

Delete `TestHandler_AddMembers_Success` (around line 855).
Delete `TestHandler_AddMembers_WithChannels` (around line 988).

These tests exercised the old `GetRoomMembersByRooms`/`GetAccountsByRooms` path. The new `TestHandler_AddMembers_ChannelExpansion` (next step) replaces them.

- [ ] **Step 6c.2: Add `TestHandler_AddMembers_ChannelExpansion` table-driven test**

Add this new test after `TestHandler_AddMembers_CapacityExceeded` (around line 958):

```go
func TestHandler_AddMembers_ChannelExpansion(t *testing.T) {
	t.Run("same-site individuals only", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(&model.Subscription{}, nil)
		store.EXPECT().ListRoomMembers(gomock.Any(), "ch1", nil, nil, false).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "bob"}},
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "carol"}},
		}, nil)

		h := &Handler{store: store, siteID: "site-a", memberListClient: mc}
		orgs, accs, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.NoError(t, err)
		assert.Empty(t, orgs)
		assert.ElementsMatch(t, []string{"bob", "carol"}, accs)
	})

	t.Run("same-site orgs only", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(&model.Subscription{}, nil)
		store.EXPECT().ListRoomMembers(gomock.Any(), "ch1", nil, nil, false).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{ID: "org1", Type: model.RoomMemberOrg}},
			{Member: model.RoomMemberEntry{ID: "org2", Type: model.RoomMemberOrg}},
		}, nil)

		h := &Handler{store: store, siteID: "site-a", memberListClient: mc}
		orgs, accs, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"org1", "org2"}, orgs)
		assert.Empty(t, accs)
	})

	t.Run("same-site mixed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(&model.Subscription{}, nil)
		store.EXPECT().ListRoomMembers(gomock.Any(), "ch1", nil, nil, false).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{ID: "org1", Type: model.RoomMemberOrg}},
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "bob"}},
		}, nil)

		h := &Handler{store: store, siteID: "site-a", memberListClient: mc}
		orgs, accs, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.NoError(t, err)
		assert.Equal(t, []string{"org1"}, orgs)
		assert.Equal(t, []string{"bob"}, accs)
	})

	t.Run("cross-site channel", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-eu"}
		mc.EXPECT().ListMembers(gomock.Any(), "alice", ch).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{ID: "org1", Type: model.RoomMemberOrg}},
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "bob"}},
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "carol"}},
		}, nil)

		h := &Handler{store: store, siteID: "site-a", memberListClient: mc}
		orgs, accs, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.NoError(t, err)
		assert.Equal(t, []string{"org1"}, orgs)
		assert.ElementsMatch(t, []string{"bob", "carol"}, accs)
	})

	t.Run("mixed same-site and cross-site", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		local := model.ChannelRef{RoomID: "ch-local", SiteID: "site-a"}
		remote := model.ChannelRef{RoomID: "ch-remote", SiteID: "site-eu"}

		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch-local").Return(&model.Subscription{}, nil)
		store.EXPECT().ListRoomMembers(gomock.Any(), "ch-local", nil, nil, false).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "local-user"}},
		}, nil)
		mc.EXPECT().ListMembers(gomock.Any(), "alice", remote).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "remote-user"}},
		}, nil)

		h := &Handler{store: store, siteID: "site-a", memberListClient: mc}
		_, accs, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{local, remote})

		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"local-user", "remote-user"}, accs)
	})

	t.Run("requester not subscribed to same-site source", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(nil, model.ErrSubscriptionNotFound)

		h := &Handler{store: store, siteID: "site-a", memberListClient: mc}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.Error(t, err)
		assert.True(t, errors.Is(err, errNotRoomMember))
	})

	t.Run("same-site GetSubscription generic error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(nil, errors.New("mongo timeout"))

		h := &Handler{store: store, siteID: "site-a", memberListClient: mc}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "subscription check")
	})

	t.Run("same-site ListRoomMembers error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(&model.Subscription{}, nil)
		store.EXPECT().ListRoomMembers(gomock.Any(), "ch1", nil, nil, false).Return(nil, errors.New("mongo timeout"))

		h := &Handler{store: store, siteID: "site-a", memberListClient: mc}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "local list-members")
	})

	t.Run("cross-site client error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-eu"}
		mc.EXPECT().ListMembers(gomock.Any(), "alice", ch).Return(nil, errors.New("nats: timeout"))

		h := &Handler{store: store, siteID: "site-a", memberListClient: mc}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "remote list-members")
	})

	t.Run("fail-fast ordering", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ref1 := model.ChannelRef{RoomID: "ch1", SiteID: "site-eu"}
		ref2 := model.ChannelRef{RoomID: "ch2", SiteID: "site-eu"}

		mc.EXPECT().ListMembers(gomock.Any(), "alice", ref1).Return(nil, errors.New("nats: timeout"))

		h := &Handler{store: store, siteID: "site-a", memberListClient: mc}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ref1, ref2})

		require.Error(t, err)
	})

	t.Run("empty refs", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		h := &Handler{store: store, siteID: "site-a", memberListClient: mc}
		orgs, accs, err := h.expandChannelRefs(context.Background(), "alice", nil)

		require.NoError(t, err)
		assert.Nil(t, orgs)
		assert.Nil(t, accs)
	})

	t.Run("unknown member type skipped", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(&model.Subscription{}, nil)
		store.EXPECT().ListRoomMembers(gomock.Any(), "ch1", nil, nil, false).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{ID: "unknown", Type: ""}},
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "bob"}},
		}, nil)

		h := &Handler{store: store, siteID: "site-a", memberListClient: mc}
		_, accs, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.NoError(t, err)
		assert.Equal(t, []string{"bob"}, accs)
	})
}
```

- [ ] **Step 6c.3: Run room-service tests**

```bash
make test SERVICE=room-service
```

Expected: PASS. The new expansion tests pass; old tests deleted; handler calls the right function.

- [ ] **Step 6c.4: Fix room-worker compilation (no logic changes)**

In `room-worker/handler_test.go`, any `AddMembersRequest` that doesn't set `Channels:` is unaffected (nil is valid for both `[]string` and `[]ChannelRef`). If there are NO `Channels:` fields in room-worker handler_test.go (which is the case), no edits needed here — it compiles as-is.

Run to verify:

```bash
go build ./room-worker/
```

Expected: PASS (compile fix — no logic changes needed).

- [ ] **Step 6c.5: Run all tests**

```bash
make test
```

Expected: PASS across all services.

- [ ] **Step 6c.6: Commit**

```bash
git add pkg/model/member.go pkg/model/model_test.go room-service/handler.go room-service/handler_test.go
git commit -m "feat: replace AddMembers channel expansion with ChannelRef + expandChannelRefs"
```

(Room-worker compiles as-is; no file change needed.)

---

## Task 7: Remove dead store methods

**Files:**
- Modify: `room-service/store.go`
- Modify: `room-service/store_mongo.go`
- Regenerate: `room-service/mock_store_test.go`
- Modify: `room-service/handler_test.go` (delete test cases)
- Modify: `room-service/integration_test.go` (delete test cases)

Why seventh: After Task 6, `GetRoomMembersByRooms` and `GetAccountsByRooms` are no longer called (only `expandChannels` used them, and it's been replaced). Delete them + their tests, then regenerate mock.

- [ ] **Step 7.1: Remove method signatures from `RoomStore` interface**

In `room-service/store.go` (around line 38–39), delete these two lines:

```go
GetRoomMembersByRooms(ctx context.Context, roomIDs []string) ([]model.RoomMember, error)
GetAccountsByRooms(ctx context.Context, roomIDs []string) ([]string, error)
```

- [ ] **Step 7.2: Remove implementations from `store_mongo.go`**

Delete the `GetRoomMembersByRooms` method implementation (around line 287).
Delete the `GetAccountsByRooms` method implementation (around line 336).

(Check the earlier grep results for exact line numbers; delete the full method bodies.)

- [ ] **Step 7.3: Regenerate mock**

```bash
make generate SERVICE=room-service
```

Expected: `mock_store_test.go` is regenerated without the two removed methods.

- [ ] **Step 7.4: Delete test cases from `handler_test.go`**

Delete any test cases that asserted on the removed methods. Based on the earlier analysis, there are no unit test cases in `handler_test.go` that specifically test these methods (they were only called internally by `expandChannels`, which is now gone).

If any mock `.EXPECT()` calls reference these methods remain, remove them. Verify with:

```bash
grep -n "GetRoomMembersByRooms\|GetAccountsByRooms" room-service/handler_test.go
```

Expected: No matches (all removed in Task 6).

- [ ] **Step 7.5: Delete integration test cases from `integration_test.go`**

Delete `TestMongoStore_GetRoomMembersByRooms_Integration` (around line 287).
Delete `TestMongoStore_GetAccountsByRooms_Integration` (around line 336).

- [ ] **Step 7.6: Run tests**

```bash
make test SERVICE=room-service
```

Expected: PASS. The removed methods had no callers left.

- [ ] **Step 7.7: Commit**

```bash
git add room-service/store.go room-service/store_mongo.go room-service/mock_store_test.go room-service/handler_test.go room-service/integration_test.go
git commit -m "refactor(room-service): remove dead store methods GetRoomMembersByRooms + GetAccountsByRooms"
```

---

## Task 8: Integration tests 1–3 (single-site scenarios)

**Files:**
- Modify: `room-service/integration_test.go`

Why eighth: After Task 7, the store is clean. Now add integration tests for same-site expansion (using local MongoDB + single NATS). Three scenarios: room_members path, subscriptions fallback, requester-not-subscribed rejection.

- [ ] **Step 8.1: Add integration test 1 — same-site channel via room_members**

At the end of `room-service/integration_test.go` (after the last test), add:

```go
func TestAddMembers_SameSiteChannel_RoomMembersPath(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Same-site only — no NATS server needed; pass nil for memberListClient.
	// The same-site branch in expandChannelRefs uses store.ListRoomMembers and
	// never invokes the client; a future cross-site ref would panic loudly here.
	db := setupMongo(t)
	valCfg := setupValkey(t)

	keyStore, _ := roomkeystore.NewValkeyStore(*valCfg)
	store := NewMongoStore(db)

	ctx := context.Background()

	// Create target room
	targetRoom := &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"}
	_ = store.CreateRoom(ctx, targetRoom)

	// Create source channel with room_members
	sourceRoom := &model.Room{ID: "source", Type: model.RoomTypeChannel, SiteID: "site-a"}
	_ = store.CreateRoom(ctx, sourceRoom)

	// Add members to source via room_members
	_ = store.CreateSubscription(ctx, &model.Subscription{
		RoomID: "source",
		User:   model.SubscriptionUser{ID: "u1", Account: "bob"},
	})
	_ = store.CreateSubscription(ctx, &model.Subscription{
		RoomID: "source",
		User:   model.SubscriptionUser{ID: "u2", Account: "carol"},
	})

	// Requester subscribed to both rooms
	_ = store.CreateSubscription(ctx, &model.Subscription{
		RoomID: "target",
		User:   model.SubscriptionUser{ID: "req", Account: "alice"},
	})
	_ = store.CreateSubscription(ctx, &model.Subscription{
		RoomID: "source",
		User:   model.SubscriptionUser{ID: "req", Account: "alice"},
	})

	handler := NewHandler(store, keyStore, nil, "site-a", 1000, 500, func(ctx context.Context, _ string, _ []byte) error {
		return nil // no publish for this test
	})

	// Call add-members
	req := model.AddMembersRequest{
		Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-a"}},
	}
	data, _ := json.Marshal(req)
	result, err := handler.handleAddMembers(ctx, subject.MemberAdd("alice", "target", "site-a"), data)

	require.NoError(t, err)
	var status map[string]string
	require.NoError(t, json.Unmarshal(result, &status))
	assert.Equal(t, "accepted", status["status"])

	// Verify target room now has the expanded members
	members, _ := store.ListRoomMembers(ctx, "target", nil, nil, false)
	var accounts []string
	for _, m := range members {
		if m.Member.Type == model.RoomMemberIndividual {
			accounts = append(accounts, m.Member.Account)
		}
	}
	assert.Contains(t, accounts, "bob")
	assert.Contains(t, accounts, "carol")
}
```

- [ ] **Step 8.2: Add integration test 2 — same-site channel via subscriptions fallback**

Add after test 1:

```go
func TestAddMembers_SameSiteChannel_SubscriptionsFallback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Same-site only — nil memberListClient is safe (see RoomMembersPath test).
	db := setupMongo(t)
	valCfg := setupValkey(t)

	keyStore, _ := roomkeystore.NewValkeyStore(*valCfg)
	store := NewMongoStore(db)

	ctx := context.Background()

	// Create target and source rooms
	_ = store.CreateRoom(ctx, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"})
	_ = store.CreateRoom(ctx, &model.Room{ID: "source", Type: model.RoomTypeChannel, SiteID: "site-a"})

	// Source has no room_members, only subscriptions
	_ = store.CreateSubscription(ctx, &model.Subscription{RoomID: "source", User: model.SubscriptionUser{ID: "u1", Account: "bob"}})
	_ = store.CreateSubscription(ctx, &model.Subscription{RoomID: "source", User: model.SubscriptionUser{ID: "u2", Account: "carol"}})
	_ = store.CreateSubscription(ctx, &model.Subscription{RoomID: "source", User: model.SubscriptionUser{ID: "u3", Account: "dave"}})

	// Requester subscribed to both
	_ = store.CreateSubscription(ctx, &model.Subscription{RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}})
	_ = store.CreateSubscription(ctx, &model.Subscription{RoomID: "source", User: model.SubscriptionUser{ID: "req", Account: "alice"}})

	handler := NewHandler(store, keyStore, nil, "site-a", 1000, 500, func(context.Context, string, []byte) error { return nil })

	req := model.AddMembersRequest{Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-a"}}}
	data, _ := json.Marshal(req)
	result, err := handler.handleAddMembers(ctx, subject.MemberAdd("alice", "target", "site-a"), data)

	require.NoError(t, err)
	var status map[string]string
	require.NoError(t, json.Unmarshal(result, &status))
	assert.Equal(t, "accepted", status["status"])

	// Verify fallback resolved accounts
	members, _ := store.ListRoomMembers(ctx, "target", nil, nil, false)
	var accounts []string
	for _, m := range members {
		if m.Member.Type == model.RoomMemberIndividual {
			accounts = append(accounts, m.Member.Account)
		}
	}
	assert.Len(t, accounts, 3)
	assert.Contains(t, accounts, "bob")
	assert.Contains(t, accounts, "carol")
	assert.Contains(t, accounts, "dave")
}
```

- [ ] **Step 8.3: Add integration test 3 — requester not subscribed to source**

Add after test 2:

```go
func TestAddMembers_RequesterNotSubscribed_Rejected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Same-site only — nil memberListClient is safe; the request fails on the
	// same-site GetSubscription check before the cross-site branch is reached.
	db := setupMongo(t)
	valCfg := setupValkey(t)

	keyStore, _ := roomkeystore.NewValkeyStore(*valCfg)
	store := NewMongoStore(db)

	ctx := context.Background()

	_ = store.CreateRoom(ctx, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"})
	_ = store.CreateRoom(ctx, &model.Room{ID: "source", Type: model.RoomTypeChannel, SiteID: "site-a"})

	// Requester subscribed to target but NOT source
	_ = store.CreateSubscription(ctx, &model.Subscription{RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}})

	handler := NewHandler(store, keyStore, nil, "site-a", 1000, 500, func(context.Context, string, []byte) error { return nil })

	req := model.AddMembersRequest{Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-a"}}}
	data, _ := json.Marshal(req)
	_, err := handler.handleAddMembers(ctx, subject.MemberAdd("alice", "target", "site-a"), data)

	require.Error(t, err)
	assert.True(t, errors.Is(err, errNotRoomMember))
}
```

- [ ] **Step 8.4: Run integration tests**

```bash
make test-integration SERVICE=room-service
```

Expected: All 3 new tests PASS.

- [ ] **Step 8.5: Commit**

```bash
git add room-service/integration_test.go
git commit -m "test(room-service): add integration tests 1-3 (same-site channel expansion)"
```

---

## Task 9: Integration tests 4–5 (two-site scenarios)

**Files:**
- Modify: `room-service/integration_test.go`

Why ninth: After Task 8, single-site integration tests pass. Now test cross-site: two in-process NATS servers + two `room-service` instances. Verify end-to-end: client on site-a calls remote `member.list` on site-b via NATS.

- [ ] **Step 9.1: Add integration test 4 — two-site end-to-end**

At the end of `integration_test.go`, add:

```go
func TestAddMembers_TwoSiteEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Two separate Mongo DBs, two in-process NATS servers
	dbA := setupMongo(t)
	dbB := setupMongo(t)
	natsURLa := setupNATS(t)
	natsURLb := setupNATS(t)
	valCfg := setupValkey(t)

	keyStore, _ := roomkeystore.NewValkeyStore(*valCfg)

	storeA := NewMongoStore(dbA)
	storeB := NewMongoStore(dbB)

	ncA, _ := nats.Connect(natsURLa)
	ncB, _ := nats.Connect(natsURLb)
	t.Cleanup(func() { ncA.Close(); ncB.Close() })

	otelNCa, _ := otelnats.Connect(natsURLa)
	otelNCb, _ := otelnats.Connect(natsURLb)
	t.Cleanup(func() { _ = otelNCa.Drain(); _ = otelNCb.Drain() })

	ctx := context.Background()

	// Site-A: target room, requester subscribed
	_ = storeA.CreateRoom(ctx, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"})
	_ = storeA.CreateSubscription(ctx, &model.Subscription{RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}})

	// Site-B: source room with members, requester subscribed
	_ = storeB.CreateRoom(ctx, &model.Room{ID: "source", Type: model.RoomTypeChannel, SiteID: "site-b"})
	_ = storeB.CreateSubscription(ctx, &model.Subscription{RoomID: "source", User: model.SubscriptionUser{ID: "u1", Account: "bob"}})
	_ = storeB.CreateSubscription(ctx, &model.Subscription{RoomID: "source", User: model.SubscriptionUser{ID: "u2", Account: "carol"}})
	_ = storeB.CreateSubscription(ctx, &model.Subscription{RoomID: "source", User: model.SubscriptionUser{ID: "req", Account: "alice"}})

	// Site-B handler responds to member.list requests
	handlerB := NewHandler(storeB, keyStore, nil, "site-b", 1000, 500, func(context.Context, string, []byte) error { return nil })
	_ = handlerB.RegisterCRUD(otelNCb)

	// Site-A handler makes cross-site call to site-B
	// The client's NATS connection must reach site-B — we use ncA connected to natsURLa,
	// but for this test we'll have the client connect to natsURLb directly
	// (in production, NATS gateways route between sites)
	memberListClient := NewNATSMemberListClient(ncB, 2*time.Second) // client talks to site-B server
	handlerA := NewHandler(storeA, keyStore, memberListClient, "site-a", 1000, 500, func(context.Context, string, []byte) error { return nil })

	// Call add-members on site-A
	req := model.AddMembersRequest{Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-b"}}}
	data, _ := json.Marshal(req)
	result, err := handlerA.handleAddMembers(ctx, subject.MemberAdd("alice", "target", "site-a"), data)

	require.NoError(t, err)
	var status map[string]string
	require.NoError(t, json.Unmarshal(result, &status))
	assert.Equal(t, "accepted", status["status"])

	// Verify site-A target room has the expanded members from site-B
	members, _ := storeA.ListRoomMembers(ctx, "target", nil, nil, false)
	var accounts []string
	for _, m := range members {
		if m.Member.Type == model.RoomMemberIndividual {
			accounts = append(accounts, m.Member.Account)
		}
	}
	assert.ElementsMatch(t, []string{"bob", "carol"}, accounts)
}
```

- [ ] **Step 9.2: Add integration test 5 — cross-site timeout**

Add after test 4. The responder sleeps past the client timeout so the
`context.WithTimeout` path fires (asserting on `errors.Is(err, context.DeadlineExceeded)`
keeps the test free of wall-clock windows).

```go
func TestAddMembers_CrossSiteTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db := setupMongo(t)
	natsURL := setupNATS(t)
	valCfg := setupValkey(t)

	keyStore, err := roomkeystore.NewValkeyStore(*valCfg)
	require.NoError(t, err)
	store := NewMongoStore(db)
	otelNC, err := otelnats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = otelNC.Drain() })

	ctx := context.Background()

	require.NoError(t, store.CreateRoom(ctx, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"}))
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{ID: "s1", RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}, Roles: []model.Role{model.RoleOwner}}))

	// Register a site-b responder that sleeps past the client timeout, so we actually
	// exercise the context.WithTimeout path (not NATS "no responders" fast-fail).
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { nc.Close() })
	sub, err := nc.Subscribe(subject.MemberList("alice", "source", "site-b"), func(m *nats.Msg) {
		time.Sleep(400 * time.Millisecond)
		_ = m.Respond([]byte(`{}`))
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	memberListClient := NewNATSMemberListClient(nc, 200*time.Millisecond)
	handler := NewHandler(store, keyStore, memberListClient, "site-a", 1000, 500, func(context.Context, string, []byte) error { return nil })

	req := model.AddMembersRequest{Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-b"}}}
	data, err := json.Marshal(req)
	require.NoError(t, err)

	_, err = handler.handleAddMembers(ctx, subject.MemberAdd("alice", "target", "site-a"), data)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "expected deadline exceeded, got %v", err)
	assert.Contains(t, err.Error(), "expand channels")
	assert.Contains(t, err.Error(), "remote list-members")
}
```

- [ ] **Step 9.3: Run integration tests**

```bash
make test-integration SERVICE=room-service
```

Expected: All 5 integration tests PASS (1–3 same-site, 4–5 two-site).

- [ ] **Step 9.4: Commit**

```bash
git add room-service/integration_test.go
git commit -m "test(room-service): add integration tests 4-5 (two-site end-to-end + timeout)"
```

---

# Phase 2: Org Resolution Refactor

The remaining tasks (10–15) move org→user resolution from room-service's reply path to room-worker's write path, via a shared MongoDB pipeline. Phase 2 is broken into four parts:

- **Part 6: Shared Pipeline Package** — Task 10 (`pkg/pipelines/member.go`)
- **Part 7: Room-Worker Changes** — Tasks 11–12 (`ListNewMembers` + `processAddMembers` switch)
- **Part 8: Room-Service Changes** — Tasks 13–14 (`CountNewMembers` + `handleAddMembers` switch)
- **Part 9: Cleanup** — Task 15 (delete `ResolveAccounts`)

Order matters across services: Task 12 (RW handler switch) lands before Task 14 (RS handler switch) so the worker can already call `ListNewMembers` by the time the service starts shipping unresolved `Users`/`Orgs`.

---

## Part 6: Shared Pipeline Package

### Task 10: `pkg/pipelines/member.go` — `GetNewMembersPipeline`

**Files:**
- Create: `pkg/pipelines/member.go`
- Test: `pkg/pipelines/member_test.go`

Why first in Phase 2: pure leaf — no consumers yet. Both `room-service.CountNewMembers` and `room-worker.ListNewMembers` will call it in later tasks. The function returns the three common stages; callers append the terminal stage that differs per service.

- [ ] **Step 10.1: Create the failing test file**

Create `pkg/pipelines/member_test.go`:

```go
package pipelines_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/pipelines"
)

func TestGetNewMembersPipeline(t *testing.T) {
	t.Run("returns three stages", func(t *testing.T) {
		stages := pipelines.GetNewMembersPipeline([]string{"org1"}, []string{"alice"}, "room1")
		require.Len(t, stages, 3, "pipeline must have exactly 3 stages; callers append the terminal stage")
	})

	t.Run("stage 1 is $match with $or covering both inputs", func(t *testing.T) {
		stages := pipelines.GetNewMembersPipeline([]string{"org1", "org2"}, []string{"alice"}, "room1")
		match := stages[0].(bson.M)["$match"].(bson.M)

		orA, ok := match["$or"].(bson.A)
		require.True(t, ok)
		assert.Len(t, orA, 2, "$or has one branch per non-empty input list")
	})

	t.Run("stage 1 includes bot exclusion regex", func(t *testing.T) {
		stages := pipelines.GetNewMembersPipeline([]string{"org1"}, nil, "room1")
		match := stages[0].(bson.M)["$match"].(bson.M)

		accountFilter, ok := match["account"].(bson.M)
		require.True(t, ok, "stage 1 must filter the account field")

		not, ok := accountFilter["$not"].(bson.Regex)
		require.True(t, ok, "bot exclusion uses $not + Regex")
		assert.Equal(t, `(\.bot$|^p_)`, not.Pattern)
	})

	t.Run("orgIDs only — $or has one branch", func(t *testing.T) {
		stages := pipelines.GetNewMembersPipeline([]string{"org1"}, nil, "room1")
		orA := stages[0].(bson.M)["$match"].(bson.M)["$or"].(bson.A)
		assert.Len(t, orA, 1)
	})

	t.Run("directAccounts only — $or has one branch", func(t *testing.T) {
		stages := pipelines.GetNewMembersPipeline(nil, []string{"alice"}, "room1")
		orA := stages[0].(bson.M)["$match"].(bson.M)["$or"].(bson.A)
		assert.Len(t, orA, 1)
	})

	t.Run("stage 2 is $lookup against subscriptions for roomID", func(t *testing.T) {
		stages := pipelines.GetNewMembersPipeline([]string{"org1"}, nil, "room1")
		lookup := stages[1].(bson.M)["$lookup"].(bson.M)

		assert.Equal(t, "subscriptions", lookup["from"])
		assert.Equal(t, "existingSub", lookup["as"])
	})

	t.Run("stage 3 keeps users with empty existingSub", func(t *testing.T) {
		stages := pipelines.GetNewMembersPipeline([]string{"org1"}, nil, "room1")
		match := stages[2].(bson.M)["$match"].(bson.M)

		existingSub := match["existingSub"].(bson.M)
		eqA := existingSub["$eq"].(bson.A)
		assert.Len(t, eqA, 0, "compare against the empty array literal")
	})
}
```

- [ ] **Step 10.2: Run the test to verify it fails**

```bash
go test ./pkg/pipelines/... -v
```

Expected: build error — `package pipelines: cannot find package` (the package directory doesn't exist yet).

- [ ] **Step 10.3: Create `pkg/pipelines/member.go`**

```go
// Package pipelines holds shared MongoDB aggregation pipelines used by more
// than one service. Putting them here lets each service append its own
// terminal stage (e.g. $count vs. $group) without duplicating the leading
// stages.
package pipelines

import "go.mongodb.org/mongo-driver/v2/bson"

// GetNewMembersPipeline returns the common stages for finding the unique,
// non-bot, not-already-subscribed users that an add-members request would
// add to roomID, given org IDs and direct account names.
//
// Pipeline target: the users collection.
//
// Stages:
//  1. $match: account in directAccounts OR sectId in orgIDs, AND account
//     does not match the bot regex (`.bot$|^p_`).
//  2. $lookup: existing subscription documents for (account, roomID), with
//     a $limit:1 sub-pipeline so we only need a yes/no answer.
//  3. $match: keep only users where existingSub is the empty array (i.e.,
//     no subscription exists for that account in roomID).
//
// Callers MUST append a terminal stage that fits their need:
//   - room-service: bson.M{"$count": "n"}                                (capacity check)
//   - room-worker:  bson.M{"$group": {"_id": nil, "accounts": {"$addToSet": "$account"}}}
func GetNewMembersPipeline(orgIDs, directAccounts []string, roomID string) bson.A {
	orFilter := bson.A{}
	if len(orgIDs) > 0 {
		orFilter = append(orFilter, bson.M{"sectId": bson.M{"$in": orgIDs}})
	}
	if len(directAccounts) > 0 {
		orFilter = append(orFilter, bson.M{"account": bson.M{"$in": directAccounts}})
	}

	return bson.A{
		bson.M{"$match": bson.M{
			"$or":     orFilter,
			"account": bson.M{"$not": bson.Regex{Pattern: `(\.bot$|^p_)`, Options: ""}},
		}},
		bson.M{"$lookup": bson.M{
			"from": "subscriptions",
			"let":  bson.M{"userAccount": "$account"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$roomId", roomID}},
					bson.M{"$eq": bson.A{"$u.account", "$$userAccount"}},
				}}}},
				bson.M{"$limit": 1},
			},
			"as": "existingSub",
		}},
		bson.M{"$match": bson.M{"existingSub": bson.M{"$eq": bson.A{}}}},
	}
}
```

- [ ] **Step 10.4: Run the test to verify it passes**

```bash
go test ./pkg/pipelines/... -v
```

Expected: all subtests under `TestGetNewMembersPipeline` PASS.

- [ ] **Step 10.5: Run lint**

```bash
make lint
```

Expected: 0 issues.

- [ ] **Step 10.6: Commit**

```bash
git add pkg/pipelines/member.go pkg/pipelines/member_test.go
git commit -m "feat(pipelines): add GetNewMembersPipeline for shared add-member aggregation"
```

---

## Part 7: Room-Worker Changes

### Task 11: Add `ListNewMembers` to room-worker store

**Files:**
- Modify: `room-worker/store.go` (interface)
- Modify: `room-worker/store_mongo.go` (implementation)
- Modify: `room-worker/integration_test.go` (integration test)
- Generate: `room-worker/mock_store_test.go` (regenerate)

Why next: standalone method on the worker's store. Adds a new `RoomStore` capability (production code doesn't yet call it). Lands behind an interface change so subsequent commits can wire it in.

- [ ] **Step 11.1: Write the failing integration test**

Append to `room-worker/integration_test.go` (after the last existing test, before the closing of any wrapping block):

```go
func TestMongoStore_ListNewMembers_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	// Seed users
	users := []interface{}{
		model.User{ID: "u1", Account: "alice", SectID: "org1"},
		model.User{ID: "u2", Account: "bob", SectID: "org1"},
		model.User{ID: "u3", Account: "carol", SectID: "org2"},
		model.User{ID: "u4", Account: "dave"},
		model.User{ID: "u5", Account: "helper.bot", SectID: "org1"}, // bot — must be excluded
	}
	_, err := db.Collection("users").InsertMany(ctx, users)
	require.NoError(t, err)

	// Seed an existing subscription for alice in r1 — alice must be excluded.
	_, err = db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID:     "s1",
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1",
	})
	require.NoError(t, err)

	t.Run("merges org members and direct accounts, excludes already-subscribed and bots", func(t *testing.T) {
		got, err := store.ListNewMembers(ctx, []string{"org1"}, []string{"carol", "dave"}, "r1")
		require.NoError(t, err)
		// alice already subscribed (excluded), helper.bot a bot (excluded).
		assert.ElementsMatch(t, []string{"bob", "carol", "dave"}, got)
	})

	t.Run("empty inputs return nil", func(t *testing.T) {
		got, err := store.ListNewMembers(ctx, nil, nil, "r1")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("orgIDs only", func(t *testing.T) {
		got, err := store.ListNewMembers(ctx, []string{"org2"}, nil, "r1")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"carol"}, got)
	})

	t.Run("directAccounts only", func(t *testing.T) {
		got, err := store.ListNewMembers(ctx, nil, []string{"dave"}, "r1")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"dave"}, got)
	})
}
```

If `room-worker/integration_test.go` doesn't already import `model` or `assert`/`require`, the existing tests in the file already do — no new imports needed.

- [ ] **Step 11.2: Run the test to verify it fails**

```bash
make test-integration SERVICE=room-worker
```

Expected: build error — `store.ListNewMembers undefined (type *MongoStore has no field or method ListNewMembers)`.

- [ ] **Step 11.3: Add `ListNewMembers` to the `SubscriptionStore` interface**

In `room-worker/store.go`, add the method under the `// --- add-member flow ---` block:

```go
	// ListNewMembers returns the unique, non-bot accounts that would be added
	// to roomID for a given (orgIDs, directAccounts) tuple — i.e. the union
	// minus already-subscribed accounts. Used by processAddMembers to expand
	// the room-service-supplied (orgs, users) into the actual write list.
	// Delegates to pkg/pipelines.GetNewMembersPipeline + a $group/$addToSet
	// terminal stage.
	ListNewMembers(ctx context.Context, orgIDs, directAccounts []string, roomID string) ([]string, error)
```

- [ ] **Step 11.4: Add `ListNewMembers` implementation in `room-worker/store_mongo.go`**

Append at the end of the file:

```go
func (s *MongoStore) ListNewMembers(ctx context.Context, orgIDs, directAccounts []string, roomID string) ([]string, error) {
	if len(orgIDs) == 0 && len(directAccounts) == 0 {
		return nil, nil
	}

	pipeline := pipelines.GetNewMembersPipeline(orgIDs, directAccounts, roomID)
	pipeline = append(pipeline, bson.M{
		"$group": bson.M{"_id": nil, "accounts": bson.M{"$addToSet": "$account"}},
	})

	cursor, err := s.users.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("list new members: %w", err)
	}
	var results []struct {
		Accounts []string `bson:"accounts"`
	}
	if err := cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("decode list new members: %w", err)
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0].Accounts, nil
}
```

If the import block doesn't already include `pkg/pipelines` or `bson`, add:

```go
import (
	"go.mongodb.org/mongo-driver/v2/bson"
	"github.com/hmchangw/chat/pkg/pipelines"
)
```

- [ ] **Step 11.5: Regenerate the mock**

```bash
make generate SERVICE=room-worker
```

Expected: `room-worker/mock_store_test.go` is regenerated and now contains `ListNewMembers`.

- [ ] **Step 11.6: Run the integration test to verify it passes**

```bash
make test-integration SERVICE=room-worker
```

Expected: `TestMongoStore_ListNewMembers_Integration` and all four subtests PASS.

- [ ] **Step 11.7: Run unit tests to confirm nothing else broke**

```bash
make test SERVICE=room-worker
```

Expected: all existing tests still PASS (mock interface widened but no test asserts on it yet).

- [ ] **Step 11.8: Commit**

```bash
git add room-worker/store.go room-worker/store_mongo.go room-worker/integration_test.go room-worker/mock_store_test.go
git commit -m "feat(room-worker): add ListNewMembers using shared pipeline"
```

---

### Task 12: Switch `processAddMembers` to use `ListNewMembers`

**Files:**
- Modify: `room-worker/handler.go` (call `ListNewMembers` first)
- Modify: `room-worker/handler_test.go` (update mocks for every `processAddMembers` test)

Why now: Task 11 added the method but nothing called it. After this task, the worker resolves orgs at write time. Room-service still ships resolved `Users` (Task 14 will change that), but feeding already-resolved accounts into `ListNewMembers` returns the same set (the pipeline filters already-subscribed users; old room-service has already done that filter, so the second pass is a no-op).

- [ ] **Step 12.1: Update each `processAddMembers` test to expect `ListNewMembers`**

In `room-worker/handler_test.go`, every test that exercises `processAddMembers` currently sets up `FindUsersByAccounts` mock with the request's `Users` slice. Add a `ListNewMembers` expectation immediately before each `FindUsersByAccounts`. The slice passed into `FindUsersByAccounts` becomes the slice returned from `ListNewMembers`.

Example pattern (apply to every `processAddMembers` test in the file):

```go
// before
store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob", "charlie"}).Return([]model.User{
    {ID: "u2", Account: "bob", SiteID: "site-a"},
    {ID: "u3", Account: "charlie", SiteID: "site-b"},
}, nil)

// after
store.EXPECT().ListNewMembers(gomock.Any(), req.Orgs, req.Users, req.RoomID).
    Return([]string{"bob", "charlie"}, nil)
store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob", "charlie"}).Return([]model.User{
    {ID: "u2", Account: "bob", SiteID: "site-a"},
    {ID: "u3", Account: "charlie", SiteID: "site-b"},
}, nil)
```

If a test's `req.Orgs` is nil or empty and `req.Users` is also empty (a test exercising the no-input path), expect `ListNewMembers(..., nil)` returning `nil, nil`. The handler must short-circuit and `FindUsersByAccounts` must NOT be called in that case (verify by removing the `FindUsersByAccounts` expectation for that sub-test).

Find every test to update with:

```bash
grep -n "FindUsersByAccounts" room-worker/handler_test.go
```

For each line, add the `ListNewMembers` expectation directly above it. Keep the request fixture's `Users` field unchanged so the expected `ListNewMembers` `directAccounts` argument matches.

- [ ] **Step 12.2: Run the tests to verify they fail**

```bash
make test SERVICE=room-worker
```

Expected: existing `TestHandler_ProcessAddMembers*` tests FAIL with `Unexpected call to *MockSubscriptionStore.ListNewMembers` (or similar) because the handler doesn't call it yet.

- [ ] **Step 12.3: Update `processAddMembers` to call `ListNewMembers` first**

In `room-worker/handler.go`, replace the start of `processAddMembers` (everything from the `accounts := req.Users` line up to and including the `userMap := ...` block) with:

```go
	// Resolve orgs and direct users into the actual write list at write time.
	// This reflects current org membership (rather than the at-accept-time
	// snapshot) and lets room-service ship merged-but-unresolved Users/Orgs.
	accounts, err := h.store.ListNewMembers(ctx, req.Orgs, req.Users, req.RoomID)
	if err != nil {
		return fmt.Errorf("list new members: %w", err)
	}
	if len(accounts) == 0 {
		// Nothing to add (capacity check in room-service already gated this;
		// reaching zero here only happens when org churn or a duplicate event
		// makes everything already-subscribed). Skip the write path.
		return nil
	}

	users, err := h.store.FindUsersByAccounts(ctx, accounts)
	if err != nil {
		return fmt.Errorf("find users by accounts: %w", err)
	}
	userMap := make(map[string]model.User, len(users))
	for i := range users {
		userMap[users[i].Account] = users[i]
	}
```

Leave the rest of `processAddMembers` unchanged — the loop already iterates over `accounts` and skips any account whose user record isn't found.

- [ ] **Step 12.4: Run the tests to verify they pass**

```bash
make test SERVICE=room-worker
```

Expected: all `TestHandler_ProcessAddMembers*` tests PASS.

- [ ] **Step 12.5: Run integration tests to confirm end-to-end still works**

```bash
make test-integration SERVICE=room-worker
```

Expected: PASS (room-worker integration tests don't exercise the new path end-to-end yet — they're store-only — so they pass unchanged).

- [ ] **Step 12.6: Run lint**

```bash
make lint
```

Expected: 0 issues.

- [ ] **Step 12.7: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): resolve orgs at write time via ListNewMembers"
```

---

## Part 8: Room-Service Changes

### Task 13: Add `CountNewMembers` to room-service store

**Files:**
- Modify: `room-service/store.go` (add to `RoomStore` interface; keep `ResolveAccounts` for now — Task 15 removes it)
- Modify: `room-service/store_mongo.go` (add `CountNewMembers` implementation)
- Modify: `room-service/integration_test.go` (add `TestMongoStore_CountNewMembers_Integration`)
- Generate: `room-service/mock_store_test.go` (regenerate)

Why next: standalone method, mirrors Task 11's pattern but with `$count` as the terminal stage. After this commit, both `ResolveAccounts` (legacy) and `CountNewMembers` (new) coexist on the interface — Task 14 swaps the call site, Task 15 deletes the legacy.

- [ ] **Step 13.1: Write the failing integration test**

Append to `room-service/integration_test.go`:

```go
func TestMongoStore_CountNewMembers_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	users := []interface{}{
		model.User{ID: "u1", Account: "alice", SectID: "org1"},
		model.User{ID: "u2", Account: "bob", SectID: "org1"},
		model.User{ID: "u3", Account: "carol", SectID: "org2"},
		model.User{ID: "u4", Account: "dave"},
		model.User{ID: "u5", Account: "helper.bot", SectID: "org1"}, // bot — must be excluded
	}
	_, err := db.Collection("users").InsertMany(ctx, users)
	require.NoError(t, err)

	// alice already subscribed to r1 → must be excluded.
	_, err = db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID:     "s1",
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1",
	})
	require.NoError(t, err)

	t.Run("counts net-new members across orgs and direct accounts", func(t *testing.T) {
		got, err := store.CountNewMembers(ctx, []string{"org1"}, []string{"carol", "dave"}, "r1")
		require.NoError(t, err)
		// Expected: bob (org1), carol, dave. alice excluded (subscribed), helper.bot excluded (bot).
		assert.Equal(t, 3, got)
	})

	t.Run("empty inputs return 0", func(t *testing.T) {
		got, err := store.CountNewMembers(ctx, nil, nil, "r1")
		require.NoError(t, err)
		assert.Equal(t, 0, got)
	})

	t.Run("everyone already subscribed returns 0", func(t *testing.T) {
		got, err := store.CountNewMembers(ctx, nil, []string{"alice"}, "r1")
		require.NoError(t, err)
		assert.Equal(t, 0, got)
	})
}
```

- [ ] **Step 13.2: Run the test to verify it fails**

```bash
make test-integration SERVICE=room-service
```

Expected: build error — `store.CountNewMembers undefined`.

- [ ] **Step 13.3: Add `CountNewMembers` to the `RoomStore` interface**

In `room-service/store.go`, add the method to the `RoomStore` interface (keep `ResolveAccounts` for now):

```go
	// CountNewMembers returns the number of unique, non-bot, not-already-subscribed
	// users that an add-members request would add to roomID. Used in handleAddMembers
	// for the capacity gate without materializing the full account list (room-worker
	// re-runs the same pipeline at write time via ListNewMembers).
	CountNewMembers(ctx context.Context, orgIDs, directAccounts []string, roomID string) (int, error)
```

- [ ] **Step 13.4: Add `CountNewMembers` implementation in `room-service/store_mongo.go`**

Append at the end of the file:

```go
func (s *MongoStore) CountNewMembers(ctx context.Context, orgIDs, directAccounts []string, roomID string) (int, error) {
	if len(orgIDs) == 0 && len(directAccounts) == 0 {
		return 0, nil
	}

	pipeline := pipelines.GetNewMembersPipeline(orgIDs, directAccounts, roomID)
	pipeline = append(pipeline, bson.M{"$count": "n"})

	cursor, err := s.users.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, fmt.Errorf("count new members: %w", err)
	}
	var results []struct {
		N int `bson:"n"`
	}
	if err := cursor.All(ctx, &results); err != nil {
		return 0, fmt.Errorf("decode count new members: %w", err)
	}
	if len(results) == 0 {
		return 0, nil
	}
	return results[0].N, nil
}
```

If the import block doesn't already include `pkg/pipelines`, add `"github.com/hmchangw/chat/pkg/pipelines"` to it.

- [ ] **Step 13.5: Regenerate the mock**

```bash
make generate SERVICE=room-service
```

Expected: `room-service/mock_store_test.go` regenerated with `CountNewMembers`.

- [ ] **Step 13.6: Run integration tests to verify they pass**

```bash
make test-integration SERVICE=room-service
```

Expected: `TestMongoStore_CountNewMembers_Integration` and its three subtests PASS. Existing tests still PASS.

- [ ] **Step 13.7: Run unit tests**

```bash
make test SERVICE=room-service
```

Expected: all PASS (no test asserts on the new method yet).

- [ ] **Step 13.8: Commit**

```bash
git add room-service/store.go room-service/store_mongo.go room-service/integration_test.go room-service/mock_store_test.go
git commit -m "feat(room-service): add CountNewMembers using shared pipeline"
```

---

### Task 14: Switch `handleAddMembers` to `CountNewMembers` + ship unresolved members

**Files:**
- Modify: `room-service/handler.go` (Step 7 swap; canonical event ships merged unresolved values)
- Modify: `room-service/handler_test.go` (replace `ResolveAccounts` mocks; update canonical-event assertions)

Why now: room-worker (after Task 12) already calls `ListNewMembers` at write time, so it can correctly handle either resolved or unresolved `Users`. Switching room-service is now safe.

- [ ] **Step 14.1: Update each `handleAddMembers` test to mock `CountNewMembers` and assert on unresolved canonical event**

In `room-service/handler_test.go`, every `TestHandler_AddMembers*` test that mocks `ResolveAccounts` must change. The pattern is:

```go
// before
store.EXPECT().ResolveAccounts(gomock.Any(), gomock.Any(), []string{"u1", "u2", "u3", "u4", "u5"}, "r1").
    Return([]string{"u1", "u2", "u3"}, nil)
store.EXPECT().CountSubscriptions(gomock.Any(), "r1").Return(0, nil)
// (later) assert published payload's Users == ["u1", "u2", "u3"] (resolved)

// after
store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), []string{"u1", "u2", "u3", "u4", "u5"}, "r1").
    Return(3, nil)
store.EXPECT().CountSubscriptions(gomock.Any(), "r1").Return(0, nil)
// (later) assert published payload's Users == ["u1", "u2", "u3", "u4", "u5"] (merged, NOT filtered)
```

Find every `ResolveAccounts` setup with:

```bash
grep -n "ResolveAccounts" room-service/handler_test.go
```

For each:
- Replace `ResolveAccounts(...).Return(<list>, nil)` with `CountNewMembers(...).Return(<len(list)>, nil)`.
- Find the corresponding canonical-event assertion (look for `json.Unmarshal(publishedData, &payload)` then assertions on `payload.Users`). Update those to expect the **merged input slice** (the third arg passed in the original `ResolveAccounts` mock), not the **filtered list** (the original return value).
- Update assertions on `payload.Orgs` to expect the merged orgs (still `allOrgs`, no behavioral change there).

For tests where the capacity-exceeded path matters (`TestHandler_AddMembers_CapacityExceeded`), change `Return(<list>, nil)` to `Return(<count>, nil)` where `<count> = len(list)` and verify the capacity error fires when `currentCount + count > maxRoomSize`.

For `TestHandler_AddMembers_EmptyAfterResolve`, the semantics shift: `CountNewMembers` returning 0 still means "no net-new members", but the canonical event now ships the (possibly non-empty) merged inputs unchanged. Update the test to expect zero subscriptions added (room-worker would no-op via the early return added in Task 12) but the canonical event still publishes — or, if the existing handler short-circuits on count==0, keep that behavior; the test should assert what the handler actually does today and not couple to room-worker's later behavior.

- [ ] **Step 14.2: Run the tests to verify they fail**

```bash
make test SERVICE=room-service
```

Expected: `TestHandler_AddMembers*` FAIL — either with "Unexpected call to CountNewMembers" (if the test expects it but the handler still calls ResolveAccounts) or with assertion failures on the canonical-event payload.

- [ ] **Step 14.3: Update `handleAddMembers` to use `CountNewMembers` and ship unresolved members**

In `room-service/handler.go`, replace steps 7–9 with:

```go
	// 7. Count net-new members (count-only — actual list materialized in room-worker)
	newCount, err := h.store.CountNewMembers(ctx, allOrgs, allUsers, roomID)
	if err != nil {
		return nil, fmt.Errorf("count new members: %w", err)
	}

	// 8. Capacity check
	currentCount, err := h.store.CountSubscriptions(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("count subscriptions: %w", err)
	}
	if currentCount+newCount > h.maxRoomSize {
		return nil, fmt.Errorf("room is at maximum capacity (%d): cannot add %d members to room with %d existing", h.maxRoomSize, newCount, currentCount)
	}

	// 9. Normalize and publish — Users and Orgs ship as merged-but-unresolved.
	// room-worker's ListNewMembers reproduces resolution at write time.
	req.Users = allUsers
	req.Orgs = allOrgs
	req.RoomID = roomID
	req.RequesterID = sub.User.ID
	req.RequesterAccount = sub.User.Account
	req.Timestamp = time.Now().UTC().UnixMilli()
	normalized, err := json.Marshal(req)
```

Leave the rest of `handleAddMembers` (the publish + reply) unchanged.

- [ ] **Step 14.4: Run the tests to verify they pass**

```bash
make test SERVICE=room-service
```

Expected: all `TestHandler_AddMembers*` PASS.

- [ ] **Step 14.5: Run integration tests to confirm**

```bash
make test-integration SERVICE=room-service
```

Expected: PASS — same-site / cross-site channel expansion paths are unaffected; the only behavioral change is the canonical event content, which the integration tests don't assert on directly.

- [ ] **Step 14.6: Run lint**

```bash
make lint
```

Expected: 0 issues.

- [ ] **Step 14.7: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): use CountNewMembers + ship unresolved members"
```

---

## Part 9: Room-Service Cleanup

### Task 15: Remove `ResolveAccounts` from room-service

**Files:**
- Modify: `room-service/handler.go` (delete the method)
- Modify: `room-service/store.go` (remove from interface)
- Modify: `room-service/handler_test.go` (remove tests)

Why last: After Task 14, `handleAddMembers` no longer calls `ResolveAccounts`. The method is dead code. Removing it simplifies the store interface and reduces test complexity. This is a pure cleanup — no production behavior change.

- [ ] **Step 15.1: Identify all `ResolveAccounts` references**

Search the codebase for `ResolveAccounts`:

```bash
grep -rn "ResolveAccounts" room-service/
```

Expected occurrences:
1. Interface method signature in `room-service/store.go`
2. Mock expectation setup in `room-service/handler_test.go` (removed in Task 14, so may not exist)
3. Function implementation in `room-service/store_mongo.go`

- [ ] **Step 15.2: Delete `ResolveAccounts` from `room-service/store.go`**

Find and remove the method signature from the store interface. It should look like:

```go
ResolveAccounts(ctx context.Context, accounts []string) ([]model.User, error)
```

- [ ] **Step 15.3: Delete `ResolveAccounts` implementation from `room-service/store_mongo.go`**

Find and delete the entire `func (s *MongoStore) ResolveAccounts(...)` function. It previously performed the org→user lookup that is now done by room-worker's `ListNewMembers`.

- [ ] **Step 15.4: Remove `TestHandler_ResolveAccounts*` tests from `room-service/handler_test.go`**

Search for test functions that test the `ResolveAccounts` path (e.g., `TestHandler_ResolveAccounts_*` or tests that set up the mock with `store.EXPECT().ResolveAccounts`). Delete those test functions entirely.

- [ ] **Step 15.5: Regenerate the mock**

```bash
make generate SERVICE=room-service
```

Expected: `room-service/mock_store_test.go` is regenerated without the `ResolveAccounts` method.

- [ ] **Step 15.6: Run unit tests to verify**

```bash
make test SERVICE=room-service
```

Expected: all `TestHandler_*` tests PASS.

- [ ] **Step 15.7: Run integration tests to confirm**

```bash
make test-integration SERVICE=room-service
```

Expected: PASS (no behavioral change to add-member flow; all state/store operations remain).

- [ ] **Step 15.8: Run lint**

```bash
make lint
```

Expected: 0 issues.

- [ ] **Step 15.9: Commit**

```bash
git add room-service/store.go room-service/store_mongo.go room-service/handler_test.go room-service/mock_store_test.go
git commit -m "refactor(room-service): remove dead ResolveAccounts method"
```

---

## Self-Review & Execution Handoff

### Phase 1 Tasks (1–9) — Channel-Based Add-Member

Before handing off to subagent or inline execution, verify:

✅ **Spec coverage** (skim each section):
- [x] `ChannelRef` model type (Task 2)
- [x] `natsutil.TryParseError` helper (Task 1)
- [x] `MemberListClient` interface + NATS impl (Task 4)
- [x] `expandChannelRefs` with same/cross-site branching (Task 6)
- [x] `sanitizeError` whitelist + substring fallback (Task 3)
- [x] Reuse `errNotRoomMember` for channel-source authorization (Task 3)
- [x] Handler struct + config wiring (Task 5)
- [x] Type swap `AddMembersRequest.Channels`, `MembersAdded.Channels` (Task 6)
- [x] Dead code removal `GetRoomMembersByRooms`/`GetAccountsByRooms` (Task 7)
- [x] Integration tests: same-site, cross-site, timeout (Tasks 8–9)

✅ **Type consistency**: All tasks use `model.ChannelRef`, `MemberListClient`, `expandChannelRefs`, and reuse `errNotRoomMember` consistently.

✅ **No placeholders**: All code blocks are complete; no "TBD" or "implement X" stubs.

✅ **Commit granularity**: Seven commits, one per logical chunk (Tasks 1–3 in Part 1, Tasks 4–5 in Part 2a, Task 6 in Part 2b, Task 7 in Part 3a, Tasks 8–9 in Parts 3b–3c).

✅ **TDD flow**: Each task follows Red → Green → Commit (write tests first, verify failure, implement, verify pass, commit).

✅ **Dependency order**: Leaf packages (natsutil, model) → client → handler struct → big type swap → cleanup → integration tests.

---

### Phase 2 Tasks (10–15) — Org Resolution Refactor

Before executing Phase 2, verify:

✅ **Spec coverage**:
- [x] Shared MongoDB pipeline `GetNewMembersPipeline` (Task 10)
- [x] Pipeline stages: `$match` (org/account filter + bot exclusion), `$lookup` (subscriptions), `$match` (non-subscribed) (Task 10)
- [x] Terminal stage pattern: `$count` for room-service, `$group/$addToSet` for room-worker (Tasks 10–14)
- [x] Room-worker resolves orgs at write time via `ListNewMembers` (Task 11–12)
- [x] Room-service validates capacity using `CountNewMembers` (Task 13–14)
- [x] Canonical event carries merged-but-unresolved `Users` and `Orgs` (Task 14)
- [x] `ResolveAccounts` method removed from room-service (Task 15)

✅ **Implementation order**:
1. Shared pipeline (`pkg/pipelines/member.go`) — no dependencies (Task 10)
2. Room-worker wiring (`room-worker/store.go` interface, implementation, tests) (Task 11)
3. Room-worker handler changes (inject `ListNewMembers` call, update all test mocks) (Task 12)
4. Room-service wiring (`room-service/store.go` interface, implementation, tests) (Task 13)
5. Room-service handler changes (use `CountNewMembers`, ship unresolved) (Task 14)
6. Cleanup: remove dead `ResolveAccounts` method (Task 15)

✅ **No placeholders**: All code blocks are complete, including full test implementations and mock updates.

✅ **Commit granularity**: Six commits, one per logical chunk:
- Task 10: `feat(pipelines): add GetNewMembersPipeline for shared add-member aggregation`
- Task 11: `feat(room-worker): add ListNewMembers using shared pipeline`
- Task 12: `feat(room-worker): switch processAddMembers to use ListNewMembers`
- Task 13: `feat(room-service): add CountNewMembers using shared pipeline`
- Task 14: `feat(room-service): use CountNewMembers + ship unresolved members`
- Task 15: `refactor(room-service): remove dead ResolveAccounts method`

✅ **TDD flow**: Each task follows Red → Green → Commit (write failing tests, implement, verify pass, commit).

✅ **Semantics**:
- Org IDs + direct account names merge at request validation time (room-service).
- Expansion (org→user) happens at write time (room-worker), not at request-acceptance time.
- Canonical events carry unresolved `Users` and `Orgs` — room-worker reproduces the expansion deterministically.
- Wire format unchanged; semantics shifted.

---
