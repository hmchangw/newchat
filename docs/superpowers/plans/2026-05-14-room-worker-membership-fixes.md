# Room-Worker Membership Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix three room-worker bugs — org-member duplication in `room_members`, empty `Content` on `members_added` sys-messages, and missing sender + empty `Content` on `member_removed` / `member_left` — without changing wire schemas or migrating existing data.

**Architecture:** Introduce `room-worker/sysmsg.go` formatter helpers and update `room-worker/handler.go` membership flows. Also add DM participant persistence spanning `pkg/model` (`Room.UIDs`/`Room.Accounts` fields, `BuildDMParticipants` helper) and the `room-worker` store contract (`UpdateDMParticipants` on `SubscriptionStore`). No wire-protocol changes.

**Tech Stack:** Go 1.25, NATS JetStream, MongoDB driver v2, `go.uber.org/mock` (mockgen), `stretchr/testify`.

**Spec:** [docs/superpowers/specs/2026-05-13-room-worker-membership-fixes-design.md](../specs/2026-05-13-room-worker-membership-fixes-design.md)

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `room-worker/sysmsg.go` | Create | Five `formatX` helpers + `displayName` |
| `room-worker/sysmsg_test.go` | Create | Unit tests for the helpers |
| `room-worker/handler.go` | Modify | `processAddMembers`, `processCreateRoomChannel`, `processRemoveIndividual`, `processRemoveOrg`, `publishChannelSysMessages` |
| `room-worker/handler_test.go` | Modify | New table-driven tests for filter rule, backfill gate, Content, sender, name validation |

`store.go` is extended in Task 12 to add `UpdateDMParticipants` to the `SubscriptionStore` interface for DM participant persistence; the remaining tasks reuse methods already on the interface.

---

## Task 1: Formatter helpers

**Files:**
- Create: `room-worker/sysmsg.go`
- Create: `room-worker/sysmsg_test.go`

- [ ] **Step 1: Write the failing test**

Create `room-worker/sysmsg_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

func TestFormatAddedSingle(t *testing.T) {
	got := formatAddedSingle(
		&model.User{EngName: "Alice", ChineseName: "愛麗絲"},
		&model.User{EngName: "Bob", ChineseName: "鮑勃"},
	)
	assert.Equal(t, "Alice 愛麗絲 added Bob 鮑勃 to the channel", got)
}

func TestFormatAddedMulti(t *testing.T) {
	got := formatAddedMulti(&model.User{EngName: "Alice", ChineseName: "愛麗絲"})
	assert.Equal(t, "Alice 愛麗絲 added members to the channel", got)
}

func TestFormatRemovedUserContent(t *testing.T) {
	got := formatRemovedUserContent(&model.User{EngName: "Bob", ChineseName: "鮑勃"})
	assert.Equal(t, "Bob 鮑勃 has been removed from the channel", got)
}

func TestFormatRemovedOrgContent(t *testing.T) {
	got := formatRemovedOrgContent("Engineering")
	assert.Equal(t, "Engineering has been removed from the channel", got)
}

func TestFormatLeftContent(t *testing.T) {
	got := formatLeftContent(&model.User{EngName: "Bob", ChineseName: "鮑勃"})
	assert.Equal(t, "Bob 鮑勃 left the channel", got)
}

func TestDisplayName_TrimsSingleSide(t *testing.T) {
	// Spec §2.6: TrimSpace(EngName + " " + ChineseName) — when one side is empty,
	// the result still has no leading/trailing whitespace. Callers are responsible
	// for rejecting fully-empty inputs; this test pins the trim behavior only.
	assert.Equal(t, "Bob left the channel", formatLeftContent(&model.User{EngName: "Bob"}))
	assert.Equal(t, "鮑勃 left the channel", formatLeftContent(&model.User{ChineseName: "鮑勃"}))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-worker`
Expected: FAIL with `undefined: formatAddedSingle` (or similar) for each formatter.

- [ ] **Step 3: Write minimal implementation**

Create `room-worker/sysmsg.go`:

```go
package main

import (
	"strings"

	"github.com/hmchangw/chat/pkg/model"
)

func displayName(u *model.User) string {
	return strings.TrimSpace(u.EngName + " " + u.ChineseName)
}

func formatAddedSingle(requester, added *model.User) string {
	return displayName(requester) + " added " + displayName(added) + " to the channel"
}

func formatAddedMulti(requester *model.User) string {
	return displayName(requester) + " added members to the channel"
}

func formatRemovedUserContent(user *model.User) string {
	return displayName(user) + " has been removed from the channel"
}

func formatRemovedOrgContent(sectName string) string {
	return sectName + " has been removed from the channel"
}

func formatLeftContent(user *model.User) string {
	return displayName(user) + " left the channel"
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=room-worker`
Expected: PASS for all six tests in `sysmsg_test.go`.

- [ ] **Step 5: Commit**

```bash
git add room-worker/sysmsg.go room-worker/sysmsg_test.go
git commit -m "feat(room-worker): add system-message formatter helpers"
```

---

## Task 2: Restructure `HasOrgRoomMembers` call + tighten backfill gate

Implements spec §2.2. Backfill must fire only on the first-org transition (`len(req.Orgs) > 0 && !hadOrgsBefore`).

**Files:**
- Modify: `room-worker/handler.go:637-644` and `:677`
- Test: `room-worker/handler_test.go` (new test function)

- [ ] **Step 1: Write the failing tests**

Append to `room-worker/handler_test.go`:

```go
func TestHandler_ProcessAddMembers_BackfillRunsOnFirstOrgTransition(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), []string{"o1"}, []string(nil), roomID).
		Return([]string{"u_new"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u_new"}).
		Return([]model.User{{ID: "u_new", Account: "u_new", SiteID: "site-a", EngName: "New", ChineseName: "新"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil) // first org

	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).Return(nil)

	// First-org transition MUST call GetSubscriptionAccounts (backfill kickoff).
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), roomID).Return([]string{"existing_user"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"existing_user"}).
		Return([]model.User{{ID: "u_e", Account: "existing_user", SiteID: "site-a", EngName: "Ex", ChineseName: "存"}}, nil)

	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Orgs: []string{"o1"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	require.NoError(t, h.processAddMembers(context.Background(), data))
}

func TestHandler_ProcessAddMembers_BackfillSkippedWhenRoomAlreadyHasOrgs(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), []string{"o_new"}, []string(nil), roomID).
		Return([]string{"u_new"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u_new"}).
		Return([]model.User{{ID: "u_new", Account: "u_new", SiteID: "site-a", EngName: "New", ChineseName: "新"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	// Restructured code calls HasOrgRoomMembers unconditionally.
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(true, nil)

	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).Return(nil)
	// NO GetSubscriptionAccounts expectation — backfill must be skipped.

	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Orgs: []string{"o_new"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	require.NoError(t, h.processAddMembers(context.Background(), data))
}
```

Note: the `GetUser(gomock.Any(), "alice")` expectation in both tests anticipates Task 5's requester fetch. The handler doesn't call `GetUser` yet, so this expectation is unmet today — that's deliberate, by the end of Task 5 both tests must pass without changes. If you'd rather keep Task 2 strictly self-contained, drop the `GetUser` line here and re-add it in Task 5 alongside the other mock-expectation updates that step calls out.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=room-worker -run BackfillRunsOnFirstOrgTransition`
Expected: FAIL — current code (handler.go:637-644) short-circuits `HasOrgRoomMembers` when `len(req.Orgs) > 0`, so the expectation is unmet.

Run: `make test SERVICE=room-worker -run BackfillSkippedWhenRoomAlreadyHasOrgs`
Expected: FAIL — current backfill gate fires whenever `len(req.Orgs) > 0`, triggering unexpected `GetSubscriptionAccounts` call.

- [ ] **Step 3: Write minimal implementation**

Replace `room-worker/handler.go:637-644`:

```go
	// Fail closed: defaulting hadOrgsBefore=false on error would trigger spurious first-org backfill.
	hadOrgsBefore, err := h.store.HasOrgRoomMembers(ctx, req.RoomID)
	if err != nil {
		return fmt.Errorf("check existing org room members: %w", err)
	}
	writeIndividuals := len(req.Orgs) > 0 || hadOrgsBefore
```

Replace `room-worker/handler.go:677` (the backfill gate):

```go
	if len(req.Orgs) > 0 && !hadOrgsBefore {
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=room-worker -run BackfillRunsOnFirstOrgTransition`
Expected: PASS.

Run: `make test SERVICE=room-worker -run BackfillSkippedWhenRoomAlreadyHasOrgs`
Expected: PASS (assuming Task 5's `GetUser` expectation is honored or removed).

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "fix(room-worker): tighten backfill gate to first-org transition only"
```

---

## Task 3: Filter `processAddMembers` individual `room_members` write to `req.Users`

Implements spec §2.1 for `processAddMembers`. A user gets an individual `room_members` doc iff their account is in `req.Users`.

**Files:**
- Modify: `room-worker/handler.go:649-661`
- Test: `room-worker/handler_test.go`

- [ ] **Step 1: Write the failing test**

Append to `room-worker/handler_test.go`:

```go
// A1: Users=[u1], Orgs=[o1] (o1 has [u1, u2]). Expect indiv only for u1, org for o1.
func TestHandler_ProcessAddMembers_IndivFilter_DirectAndOrgOverlap(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), []string{"o1"}, []string{"u1"}, roomID).
		Return([]string{"u1", "u2"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1", "u2"}).
		Return([]model.User{
			{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"},
			{ID: "u2_id", Account: "u2", SiteID: "site-a", EngName: "U2", ChineseName: "二"},
		}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil)

	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), roomID).Return([]string{}, nil) // no pre-existing subs

	var captured []*model.RoomMember
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, m []*model.RoomMember) error {
			captured = m
			return nil
		})
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Users: []string{"u1"}, Orgs: []string{"o1"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	require.NoError(t, h.processAddMembers(context.Background(), data))

	// Collect entries by type.
	var indivAccts []string
	var orgIDs []string
	for _, m := range captured {
		switch m.Member.Type {
		case model.RoomMemberIndividual:
			indivAccts = append(indivAccts, m.Member.Account)
		case model.RoomMemberOrg:
			orgIDs = append(orgIDs, m.Member.ID)
		}
	}
	assert.ElementsMatch(t, []string{"u1"}, indivAccts, "indiv docs limited to req.Users")
	assert.ElementsMatch(t, []string{"o1"}, orgIDs)
}

// A2: Users=[], Orgs=[o1]. Expect org only, no indivs.
func TestHandler_ProcessAddMembers_IndivFilter_OrgOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), []string{"o1"}, []string(nil), roomID).
		Return([]string{"u1"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).
		Return([]model.User{{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil)

	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), roomID).Return([]string{}, nil)

	var captured []*model.RoomMember
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, m []*model.RoomMember) error {
			captured = m
			return nil
		})
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Orgs: []string{"o1"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	require.NoError(t, h.processAddMembers(context.Background(), data))

	for _, m := range captured {
		assert.NotEqual(t, model.RoomMemberIndividual, m.Member.Type, "no indiv docs should be written")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=room-worker -run IndivFilter`
Expected: FAIL — current loop at handler.go:650-661 writes indiv docs for every sub, so `u2` will be in `indivAccts` (A1 test) and `u1` will be in indivs (A2 test).

- [ ] **Step 3: Write minimal implementation**

Replace `room-worker/handler.go:649-661`:

```go
	allowedIndiv := make(map[string]struct{}, len(req.Users))
	for _, acc := range req.Users {
		allowedIndiv[acc] = struct{}{}
	}
	if writeIndividuals {
		for _, sub := range subs {
			if _, ok := allowedIndiv[sub.User.Account]; !ok {
				continue
			}
			roomMembers = append(roomMembers, &model.RoomMember{
				ID:     idgen.GenerateUUIDv7(),
				RoomID: req.RoomID,
				Ts:     acceptedAt,
				Member: model.RoomMemberEntry{
					ID:      sub.User.ID,
					Type:    model.RoomMemberIndividual,
					Account: sub.User.Account,
				},
			})
		}
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=room-worker -run IndivFilter`
Expected: PASS.

Also run the existing `TestHandler_ProcessAddMembers_WithOrgs` to confirm no regression:

Run: `make test SERVICE=room-worker -run TestHandler_ProcessAddMembers_WithOrgs`
Expected: PASS (may need its expected-doc list updated if it currently relies on the buggy behavior — investigate before adjusting; do NOT change assertions that test correct membership).

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "fix(room-worker): filter processAddMembers indiv docs to req.Users only"
```

---

## Task 4: Filter `processCreateRoomChannel` individual write to `ResolvedUsers ∪ {requester}`

Implements spec §2.1 for create-room. The filter runs inside the existing `if len(req.ResolvedOrgs) > 0` gate; no-orgs lite-mode is preserved.

**Files:**
- Modify: `room-worker/handler.go:1054-1075`
- Test: `room-worker/handler_test.go`

- [ ] **Step 1: Write the failing test**

Append to `room-worker/handler_test.go`:

```go
// A4: Create channel ResolvedUsers=[u1], ResolvedOrgs=[o1] (o1 has [u1, u2]),
// requester r. Expect indiv docs for r and u1, org doc for o1, no indiv for u2.
func TestHandler_ProcessCreateRoom_Channel_IndivFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	requester := &model.User{ID: "r_id", Account: "r", SiteID: "site-a", EngName: "Req", ChineseName: "請"}

	store.EXPECT().ListNewMembersForNewRoom(gomock.Any(), []string{"o1"}, []string{"u1"}, "r").
		Return([]string{"u1", "u2"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1", "u2"}).
		Return([]model.User{
			{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"},
			{ID: "u2_id", Account: "u2", SiteID: "site-a", EngName: "U2", ChineseName: "二"},
		}, nil)

	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)

	var captured []*model.RoomMember
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, m []*model.RoomMember) error {
			captured = m
			return nil
		})
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }}

	room := &model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}
	req := &model.CreateRoomRequest{
		RoomID:        roomID,
		ResolvedUsers: []string{"u1"},
		ResolvedOrgs:  []string{"o1"},
		Timestamp:     1,
	}
	require.NoError(t, h.processCreateRoomChannel(context.Background(), req, room, requester, "req-1", time.UnixMilli(1).UTC(), time.UnixMilli(2).UTC()))

	var indivAccts []string
	var orgIDs []string
	for _, m := range captured {
		switch m.Member.Type {
		case model.RoomMemberIndividual:
			indivAccts = append(indivAccts, m.Member.Account)
		case model.RoomMemberOrg:
			orgIDs = append(orgIDs, m.Member.ID)
		}
	}
	assert.ElementsMatch(t, []string{"r", "u1"}, indivAccts, "indiv docs limited to ResolvedUsers ∪ {requester}")
	assert.ElementsMatch(t, []string{"o1"}, orgIDs)
}
```

Note: this test calls `processCreateRoomChannel` directly. The existing test file may not yet invoke this function in isolation; if name/signature drift causes issues, mirror the wiring already used by `TestHandler_ProcessAddMembers_*`. The `finishCreateRoom` path will fire publishes — capture or ignore them via the no-op `publish` closure.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-worker -run TestHandler_ProcessCreateRoom_Channel_IndivFilter`
Expected: FAIL — current loop at handler.go:1056-1063 writes indiv docs for every sub (r, u1, u2), so `u2` appears in `indivAccts`.

- [ ] **Step 3: Write minimal implementation**

Replace `room-worker/handler.go:1054-1075` (keep the comment block at 1076-1079 intact):

```go
	if len(req.ResolvedOrgs) > 0 {
		allowedIndiv := make(map[string]struct{}, len(req.ResolvedUsers)+1)
		allowedIndiv[requester.Account] = struct{}{}
		for _, acc := range req.ResolvedUsers {
			allowedIndiv[acc] = struct{}{}
		}
		members := make([]*model.RoomMember, 0, len(subs)+len(req.ResolvedOrgs))
		for _, sub := range subs {
			if _, ok := allowedIndiv[sub.User.Account]; !ok {
				continue
			}
			members = append(members, &model.RoomMember{
				ID:     idgen.GenerateUUIDv7(),
				RoomID: room.ID,
				Ts:     acceptedAt,
				Member: model.RoomMemberEntry{ID: sub.User.ID, Type: model.RoomMemberIndividual, Account: sub.User.Account},
			})
		}
		for _, org := range req.ResolvedOrgs {
			members = append(members, &model.RoomMember{
				ID:     idgen.GenerateUUIDv7(),
				RoomID: room.ID,
				Ts:     acceptedAt,
				Member: model.RoomMemberEntry{ID: org, Type: model.RoomMemberOrg},
			})
		}
		if err := h.store.BulkCreateRoomMembers(ctx, members); err != nil {
			return fmt.Errorf("bulk create room members: %w", err)
		}
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=room-worker -run TestHandler_ProcessCreateRoom_Channel_IndivFilter`
Expected: PASS.

Also run the full create-room test set:

Run: `make test SERVICE=room-worker -run TestHandler_ProcessCreateRoom`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "fix(room-worker): filter create-room indiv docs to ResolvedUsers ∪ {requester}"
```

---

## Task 5: Requester fetch + empty-name validation in `processAddMembers`

Implements spec §2.3. Fetch requester via `store.GetUser`; permanent error on miss or empty `EngName`/`ChineseName`. Also validate added users' name fields.

**Files:**
- Modify: `room-worker/handler.go` (around line 590, after existing user-validation loop)
- Test: `room-worker/handler_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `room-worker/handler_test.go`:

```go
// D1: requester not found → permanent error.
func TestHandler_ProcessAddMembers_RequesterNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), []string(nil), []string{"u1"}, roomID).
		Return([]string{"u1"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).
		Return([]model.User{{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "missing-requester").Return(nil, ErrUserNotFound)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }}
	req := model.AddMembersRequest{RoomID: roomID, RequesterID: "missing-id", RequesterAccount: "missing-requester", Users: []string{"u1"}, Timestamp: 1}
	data, _ := json.Marshal(req)

	err := h.processAddMembers(context.Background(), data)
	require.Error(t, err)
	var perm *permanentError
	assert.ErrorAs(t, err, &perm, "miss should be a permanent error")
}

// D2: requester has empty EngName → permanent error.
func TestHandler_ProcessAddMembers_RequesterEmptyName(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), []string(nil), []string{"u1"}, roomID).
		Return([]string{"u1"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).
		Return([]model.User{{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "", ChineseName: "愛"}, nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }}
	req := model.AddMembersRequest{RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice", Users: []string{"u1"}, Timestamp: 1}
	data, _ := json.Marshal(req)

	err := h.processAddMembers(context.Background(), data)
	require.Error(t, err)
	var perm *permanentError
	assert.ErrorAs(t, err, &perm)
}

// D3: added user has empty ChineseName → permanent error.
func TestHandler_ProcessAddMembers_AddedUserEmptyName(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), []string(nil), []string{"u1"}, roomID).
		Return([]string{"u1"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).
		Return([]model.User{{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: ""}}, nil)
	// Validation for added users should fire before requester fetch; do not mock GetUser here.

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }}
	req := model.AddMembersRequest{RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice", Users: []string{"u1"}, Timestamp: 1}
	data, _ := json.Marshal(req)

	err := h.processAddMembers(context.Background(), data)
	require.Error(t, err)
	var perm *permanentError
	assert.ErrorAs(t, err, &perm)
}
```

Note: the exact `permanentError` type name comes from the existing `newPermanent` constructor in this service. If the type isn't exported, switch to checking the error string (e.g., `assert.Contains(t, err.Error(), "requester")`); the rest of the codebase already does this in negative tests — copy the closest existing pattern.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=room-worker -run RequesterNotFound`
Expected: FAIL — current code doesn't fetch the requester at all, so `GetUser` expectation is unmet (or the handler succeeds, then the assertion on `err` fails).

Same for the other two.

- [ ] **Step 3: Write minimal implementation**

After the existing missing-account loop at `room-worker/handler.go:585-589`, insert:

```go
	for i := range users {
		if users[i].EngName == "" || users[i].ChineseName == "" {
			return newPermanent("user %s missing required name fields (room %s)", users[i].Account, req.RoomID)
		}
	}

	requester, err := h.store.GetUser(ctx, req.RequesterAccount)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return newPermanent("requester %s not found", req.RequesterAccount)
		}
		return fmt.Errorf("get requester: %w", err)
	}
	if requester.EngName == "" || requester.ChineseName == "" {
		return newPermanent("requester %s missing required name fields", req.RequesterAccount)
	}
```

This produces a `requester *model.User` in scope for Task 6 to use.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=room-worker -run "RequesterNotFound|RequesterEmptyName|AddedUserEmptyName"`
Expected: PASS.

Run: `make test SERVICE=room-worker`
Expected: All PASS. Some pre-existing tests (e.g., `TestHandler_ProcessAddMembers`) may now need to provide a `GetUser` mock expectation for the requester — update them minimally with a valid `*model.User` return.

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): fetch requester and validate name fields in processAddMembers"
```

---

## Task 6: `processAddMembers` `members_added` Content

Implements spec §2.3 Content rules. Count-sensitive: 1 → single form, ≥2 → multi form.

**Files:**
- Modify: `room-worker/handler.go:776-784`
- Test: `room-worker/handler_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `room-worker/handler_test.go`:

```go
// B1: len(subs)==1 → single form.
func TestHandler_ProcessAddMembers_Content_Single(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), []string(nil), []string{"u1"}, roomID).
		Return([]string{"u1"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).
		Return([]model.User{{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)
	// No BulkCreateRoomMembers expected (no orgs, no pre-existing orgs → lite-mode add).

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Users: []string{"u1"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	require.NoError(t, h.processAddMembers(context.Background(), data))

	sysMsg := findSysMsg(t, published, "site-a", "members_added")
	assert.Equal(t, "Alice 愛 added U1 一 to the channel", sysMsg.Content)
}

// B2: len(subs)>=2 → multi form.
func TestHandler_ProcessAddMembers_Content_Multi(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), []string(nil), []string{"u1", "u2"}, roomID).
		Return([]string{"u1", "u2"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1", "u2"}).
		Return([]model.User{
			{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"},
			{ID: "u2_id", Account: "u2", SiteID: "site-a", EngName: "U2", ChineseName: "二"},
		}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Users: []string{"u1", "u2"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	require.NoError(t, h.processAddMembers(context.Background(), data))

	sysMsg := findSysMsg(t, published, "site-a", "members_added")
	assert.Equal(t, "Alice 愛 added members to the channel", sysMsg.Content)
}

// findSysMsg locates a published canonical-message envelope on the given site
// with the given Message.Type and returns the inner Message. Add once near the
// top of handler_test.go alongside findMemberAddEvent.
func findSysMsg(t *testing.T, published []publishedMsg, siteID, msgType string) model.Message {
	t.Helper()
	want := subject.MsgCanonicalCreated(siteID)
	for _, p := range published {
		if p.subj != want {
			continue
		}
		var evt model.MessageEvent
		if err := json.Unmarshal(p.data, &evt); err != nil {
			t.Fatalf("unmarshal MessageEvent: %v", err)
		}
		if evt.Message.Type == msgType {
			return evt.Message
		}
	}
	t.Fatalf("no %s sys-message published on %s", msgType, siteID)
	return model.Message{}
}
```

If `findSysMsg` already exists with a different signature, reuse the existing helper instead.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=room-worker -run "Content_Single|Content_Multi"`
Expected: FAIL — `sysMsg.Content` is currently `""` for both.

- [ ] **Step 3: Write minimal implementation**

Replace `room-worker/handler.go:776-784`. Look up the single added user via `userMap` (already built at handler.go:576-579) rather than `users[0]` — `users` ordering tracks `accounts` from `ListNewMembers`, which can differ from `subs` if the resolved set is filtered upstream:

```go
	content := formatAddedMulti(requester)
	if len(subs) == 1 {
		onlyUser := userMap[subs[0].User.Account]
		content = formatAddedSingle(requester, &onlyUser)
	}
	sysMsg := model.Message{
		ID:          idgen.MessageIDFromRequestID(seed, "addmembers"),
		RoomID:      req.RoomID,
		UserID:      req.RequesterID,
		UserAccount: req.RequesterAccount,
		Type:        "members_added",
		Content:     content,
		SysMsgData:  sysMsgData,
		CreatedAt:   acceptedAt,
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=room-worker -run "Content_Single|Content_Multi"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): populate Content on processAddMembers members_added sys-msg"
```

---

## Task 7: `publishChannelSysMessages` `members_added` Content

Implements spec §2.3 multi-form Content for create-room sys-message.

**Files:**
- Modify: `room-worker/handler.go:1248-1256`
- Test: `room-worker/handler_test.go`

- [ ] **Step 1: Write the failing test**

Append to `room-worker/handler_test.go`:

```go
// B3: create-room channel publishes members_added with always-multi form.
func TestHandler_PublishChannelSysMessages_MembersAddedContent(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}}

	room := &model.Room{ID: "r1", Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}
	requester := &model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}
	req := &model.CreateRoomRequest{RoomID: "r1", Users: []string{"u1", "u2"}}

	require.NoError(t, h.publishChannelSysMessages(context.Background(), req, room, requester, 2, "req-1", time.UnixMilli(1).UTC()))

	sysMsg := findSysMsg(t, published, "site-a", model.MessageTypeMembersAdded)
	assert.Equal(t, "Alice 愛 added members to the channel", sysMsg.Content)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-worker -run TestHandler_PublishChannelSysMessages_MembersAddedContent`
Expected: FAIL — `Content` is currently empty.

- [ ] **Step 3: Write minimal implementation**

Replace `room-worker/handler.go:1248-1256`:

```go
	msg2 := model.Message{
		ID:          idgen.MessageIDFromRequestID(requestID, "members_added"),
		RoomID:      room.ID,
		UserID:      requester.ID,
		UserAccount: requester.Account,
		Type:        model.MessageTypeMembersAdded,
		Content:     formatAddedMulti(requester),
		SysMsgData:  sysData2,
		CreatedAt:   acceptedAt.Add(time.Millisecond),
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=room-worker -run TestHandler_PublishChannelSysMessages_MembersAddedContent`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): populate Content on create-room members_added sys-msg"
```

---

## Task 8: `processRemoveIndividual` sender + Content + name validation

Implements spec §2.4 for individual removes and §2.5 (demote-only skip is already in code at line 270-276 — keep). Sets `UserAccount = req.Requester`, populates `Content`, validates fetched user's name fields.

**Files:**
- Modify: `room-worker/handler.go:259-262` (add name validation) and `:353-358` (envelope + Content)
- Test: `room-worker/handler_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `room-worker/handler_test.go`:

```go
// C1: self-leave full removal → member_left with sender + Content.
func TestHandler_ProcessRemoveIndividual_SelfLeave_Content(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetUserWithMembership(gomock.Any(), roomID, "bob").
		Return(&model.UserWithMembership{
			User:             model.User{ID: "u_b", Account: "bob", SiteID: "site-a", EngName: "Bob", ChineseName: "鮑"},
			HasOrgMembership: false,
			Roles:            []model.Role{model.RoleMember},
		}, nil)
	store.EXPECT().DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberIndividual, "u_b").Return(nil)
	store.EXPECT().DeleteSubscription(gomock.Any(), roomID, "bob").Return(int64(1), nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}}

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: "bob", Account: "bob", Timestamp: 1}
	require.NoError(t, h.processRemoveIndividual(context.Background(), &req))

	sysMsg := findSysMsg(t, published, "site-a", "member_left")
	assert.Equal(t, "bob", sysMsg.UserAccount)
	assert.Empty(t, sysMsg.UserID, "UserID must stay empty per spec §2.4")
	assert.Equal(t, "Bob 鮑 left the channel", sysMsg.Content)
}

// C2: removed-by-other full removal → member_removed with sender + Content.
func TestHandler_ProcessRemoveIndividual_RemovedByOther_Content(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetUserWithMembership(gomock.Any(), roomID, "bob").
		Return(&model.UserWithMembership{
			User: model.User{ID: "u_b", Account: "bob", SiteID: "site-a", EngName: "Bob", ChineseName: "鮑"},
		}, nil)
	store.EXPECT().DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberIndividual, "u_b").Return(nil)
	store.EXPECT().DeleteSubscription(gomock.Any(), roomID, "bob").Return(int64(1), nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}}

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: "alice", Account: "bob", Timestamp: 1}
	require.NoError(t, h.processRemoveIndividual(context.Background(), &req))

	sysMsg := findSysMsg(t, published, "site-a", "member_removed")
	assert.Equal(t, "alice", sysMsg.UserAccount)
	assert.Empty(t, sysMsg.UserID)
	assert.Equal(t, "Bob 鮑 has been removed from the channel", sysMsg.Content)
}

// D4: target user has empty ChineseName → permanent error.
func TestHandler_ProcessRemoveIndividual_EmptyName(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	store.EXPECT().GetUserWithMembership(gomock.Any(), "r1", "bob").
		Return(&model.UserWithMembership{
			User: model.User{ID: "u_b", Account: "bob", SiteID: "site-a", EngName: "Bob"},
		}, nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }}
	req := model.RemoveMemberRequest{RoomID: "r1", Requester: "alice", Account: "bob", Timestamp: 1}

	err := h.processRemoveIndividual(context.Background(), &req)
	require.Error(t, err)
	var perm *permanentError
	assert.ErrorAs(t, err, &perm)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=room-worker -run "RemoveIndividual_SelfLeave_Content|RemoveIndividual_RemovedByOther_Content|RemoveIndividual_EmptyName"`
Expected: FAIL — `UserAccount` and `Content` are empty in current sysMsg literal; empty-name path returns nil today.

- [ ] **Step 3: Write minimal implementation**

After `room-worker/handler.go:259-262` (the `GetUserWithMembership` block), insert name validation:

```go
	if user.EngName == "" || user.ChineseName == "" {
		return newPermanent("user %s missing required name fields (room %s)", req.Account, req.RoomID)
	}
```

Replace `room-worker/handler.go:353-358`:

```go
	var content string
	if isSelfLeave {
		content = formatLeftContent(&user.User)
	} else {
		content = formatRemovedUserContent(&user.User)
	}
	sysMsg := model.Message{
		ID:          idgen.MessageIDFromRequestID(seed, "rmindiv"),
		RoomID:      req.RoomID,
		UserAccount: req.Requester,
		Type:        evtType,
		Content:     content,
		SysMsgData:  sysMsgData,
		CreatedAt:   now,
	}
```

Note: `user` here is the local variable from line 259, type `*model.UserWithMembership`. That type embeds `model.User` (see `room-worker/store.go:15-32`), so `&user.User` is the correct way to pass a `*model.User` to the formatter.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=room-worker -run "RemoveIndividual_SelfLeave_Content|RemoveIndividual_RemovedByOther_Content|RemoveIndividual_EmptyName"`
Expected: PASS.

Also confirm the demote-only path still skips sys-message publishing:

Run: `make test SERVICE=room-worker -run TestHandler_ProcessRemoveMember_SelfLeave_DualMembership`
Expected: PASS (no change to this path).

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): set sender + Content on member_removed / member_left sys-msg"
```

---

## Task 9: `processRemoveOrg` unfiltered SectName + sender + Content + empty-SectName error

Implements spec §2.4 for org removes: iterate unfiltered `members` for SectName, permanent error if every member has empty SectName, set `UserAccount = req.Requester`, populate Content.

**Files:**
- Modify: `room-worker/handler.go:433-437` and `:483-496`
- Test: `room-worker/handler_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `room-worker/handler_test.go`:

```go
// C3: org remove with every member also having individual subs (toRemove empty)
// — SectName still populated from unfiltered members; sys-message still published.
func TestHandler_ProcessRemoveOrg_AllOverlap_SectNameFromUnfiltered(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetOrgMembersWithIndividualStatus(gomock.Any(), roomID, "o1").
		Return([]OrgMemberStatus{
			{Account: "u1", SiteID: "site-a", SectName: "Engineering", HasIndividualMembership: true},
			{Account: "u2", SiteID: "site-a", SectName: "Engineering", HasIndividualMembership: true},
		}, nil)
	// toRemove is empty → no DeleteSubscriptionsByAccounts call expected.
	store.EXPECT().DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberOrg, "o1").Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}}

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: "alice", OrgID: "o1", Timestamp: 1}
	require.NoError(t, h.processRemoveOrg(context.Background(), &req))

	sysMsg := findSysMsg(t, published, "site-a", "member_removed")
	assert.Equal(t, "alice", sysMsg.UserAccount)
	assert.Empty(t, sysMsg.UserID)
	assert.Equal(t, "Engineering has been removed from the channel", sysMsg.Content)
}

// D5: every member SectName empty → permanent error.
func TestHandler_ProcessRemoveOrg_AllSectNamesEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	store.EXPECT().GetOrgMembersWithIndividualStatus(gomock.Any(), "r1", "o1").
		Return([]OrgMemberStatus{
			{Account: "u1", SiteID: "site-a", SectName: "", HasIndividualMembership: false},
		}, nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }}
	req := model.RemoveMemberRequest{RoomID: "r1", Requester: "alice", OrgID: "o1", Timestamp: 1}

	err := h.processRemoveOrg(context.Background(), &req)
	require.Error(t, err)
	var perm *permanentError
	assert.ErrorAs(t, err, &perm)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=room-worker -run "ProcessRemoveOrg_AllOverlap|ProcessRemoveOrg_AllSectNamesEmpty"`
Expected: FAIL — current code harvests SectName only from `toRemove` (which is empty here, yielding `""`) and doesn't set `UserAccount`/`Content`.

- [ ] **Step 3: Write minimal implementation**

Replace `room-worker/handler.go:433-437` (the `sectName` harvest) with iteration over the unfiltered `members` slice. The original loop must remain to publish per-account subscription updates for `toRemove`; only the SectName extraction moves:

```go
	sectName := ""
	for _, m := range members {
		if m.SectName != "" {
			sectName = m.SectName
			break
		}
	}
	if sectName == "" {
		return newPermanent("org %s missing SectName on all members (room %s)", req.OrgID, req.RoomID)
	}
	for _, m := range toRemove {
		subEvt := model.SubscriptionUpdateEvent{
```

(Keep the per-account `SubscriptionUpdateEvent` publish loop body that follows at lines 438-451 unchanged.)

Replace `room-worker/handler.go:490-496`:

```go
	sysMsg := model.Message{
		ID:          idgen.MessageIDFromRequestID(seed, "rmorg"),
		RoomID:      req.RoomID,
		UserAccount: req.Requester,
		Type:        "member_removed",
		Content:     formatRemovedOrgContent(sectName),
		SysMsgData:  sysMsgPayload,
		CreatedAt:   now,
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=room-worker -run "ProcessRemoveOrg_AllOverlap|ProcessRemoveOrg_AllSectNamesEmpty"`
Expected: PASS.

Also run the full remove-org test set:

Run: `make test SERVICE=room-worker -run ProcessRemoveMember_OwnerRemovesOrg`
Expected: PASS (this pre-existing test exercises the happy path; its assertions may need expansion to cover Content but should not fail outright).

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): harvest SectName from unfiltered org members; set sender + Content"
```

---

## Task 10: Final verification

- [ ] **Step 1: Run lint**

Run: `make lint SERVICE=room-worker`
Expected: PASS, no warnings.

- [ ] **Step 2: Run full test suite with race detector**

Run: `make test SERVICE=room-worker`
Expected: All PASS.

- [ ] **Step 3: Run mockgen and confirm no unexpected diff**

Run: `make generate SERVICE=room-worker && git diff --stat`
Expected: No diff for tasks 1–10 (the membership fixes don't change `SubscriptionStore`). Task 12 adds `UpdateDMParticipants` to the interface; if you're running Task 10 verification AFTER Task 12, expect a single new entry in `mock_store_test.go` and confirm it is the regenerated mock for the new method, not unrelated churn.

- [ ] **Step 4: Confirm coverage threshold**

Run:
```bash
go test -tags '' -coverprofile=/tmp/cover.out ./room-worker/...
go tool cover -func=/tmp/cover.out | tail -1
```
Expected: total coverage ≥ 80%. If below, add cases for any uncovered error branches introduced in Tasks 5, 8, 9.

- [ ] **Step 5: Commit any coverage fill-in**

```bash
git add room-worker/handler_test.go
git commit -m "test(room-worker): cover remaining branches for membership fixes"
```

(Skip the commit if no changes were needed.)

---

## Task 11: Add `UIDs`/`Accounts` fields + `BuildDMParticipants` helper to `pkg/model`

Implements spec §3.1 (field shape) and §3.2 (pairing invariant). Pure-data additions; no service code touched yet.

**Files:**
- Create: `pkg/model/room_test.go`
- Modify: `pkg/model/room.go`
- Modify: `pkg/model/model_test.go` (optional — only if the file has a generic round-trip cycle for `Room`)

- [ ] **Step 1: Write the failing tests**

Create `pkg/model/room_test.go`:

```go
package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildDMParticipants_SortsByUID(t *testing.T) {
	a := &User{ID: "zzz", Account: "alpha"}
	b := &User{ID: "aaa", Account: "zebra"}
	uids, accounts := BuildDMParticipants(a, b)
	assert.Equal(t, []string{"aaa", "zzz"}, uids)
	assert.Equal(t, []string{"zebra", "alpha"}, accounts, "accounts mirror uid permutation")
}

// Spec §4 F5: non-aligned sort. Users {ID:"zzz", Account:"aaa"} and
// {ID:"aaa", Account:"zzz"}. UIDs sort ascending; Accounts permute to
// preserve the same-user pairing at each index — NOT independently sorted.
func TestBuildDMParticipants_PreservesPairingUnderNonAlignedSort(t *testing.T) {
	user1 := &User{ID: "zzz", Account: "aaa"}
	user2 := &User{ID: "aaa", Account: "zzz"}
	uids, accounts := BuildDMParticipants(user1, user2)
	assert.Equal(t, []string{"aaa", "zzz"}, uids)
	assert.Equal(t, []string{"zzz", "aaa"}, accounts)
}

func TestBuildDMParticipants_AlreadySortedInput(t *testing.T) {
	a := &User{ID: "aaa", Account: "alpha"}
	b := &User{ID: "bbb", Account: "beta"}
	uids, accounts := BuildDMParticipants(a, b)
	assert.Equal(t, []string{"aaa", "bbb"}, uids)
	assert.Equal(t, []string{"alpha", "beta"}, accounts)
}

func TestBuildDMParticipants_SwapInputOrderProducesSameResult(t *testing.T) {
	a := &User{ID: "u1", Account: "alice"}
	b := &User{ID: "u2", Account: "bob"}
	uidsAB, accountsAB := BuildDMParticipants(a, b)
	uidsBA, accountsBA := BuildDMParticipants(b, a)
	assert.Equal(t, uidsAB, uidsBA, "callers passing args in either order must get the same result")
	assert.Equal(t, accountsAB, accountsBA)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/model/`
Expected: FAIL with `undefined: BuildDMParticipants` (compile error).

- [ ] **Step 3: Write minimal implementation**

Edit `pkg/model/room.go`. Add the two fields inside `Room` (immediately after the existing `Restricted` field):

```go
UIDs     []string `json:"uids,omitempty"     bson:"uids,omitempty"`
Accounts []string `json:"accounts,omitempty" bson:"accounts,omitempty"`
```

Append the helper to the bottom of the same file:

```go
// BuildDMParticipants returns sorted-by-UID, paired-by-index participant
// lists for a dm or botDM room. UIDs[i] and Accounts[i] always describe
// the same user. Callers must pass exactly two distinct *User values;
// upstream (room-service capacity check + room-worker counterpart fetch)
// already enforces this invariant.
func BuildDMParticipants(a, b *User) (uids, accounts []string) {
	if a.ID < b.ID {
		return []string{a.ID, b.ID}, []string{a.Account, b.Account}
	}
	return []string{b.ID, a.ID}, []string{b.Account, a.Account}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/model/`
Expected: PASS for all four `TestBuildDMParticipants_*` tests.

- [ ] **Step 5: Optional — extend the round-trip helper coverage**

Check `pkg/model/model_test.go` for a generic JSON/BSON round-trip helper that covers `Room`. If one exists, add two `Room` cases — one with `UIDs`/`Accounts` populated and one with both `nil` — to confirm `omitempty` drops them on the wire. If no Room-specific round-trip case exists, skip this step.

Run: `go test ./pkg/model/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/model/room.go pkg/model/room_test.go pkg/model/model_test.go
git commit -m "feat(model): add Room.UIDs/Accounts + BuildDMParticipants helper"
```

---

## Task 12: Add `UpdateDMParticipants` store method

Implements spec §3.4. Scaffolding for Tasks 13/14 — no behavioral test on its own; the Mongo path is exercised by future integration tests, and Tasks 13/14 cover the call-site behavior via the mock.

**Files:**
- Modify: `room-worker/store.go` (interface)
- Modify: `room-worker/store_mongo.go` (Mongo impl)
- Modify: `room-worker/mock_store_test.go` (regenerated)

- [ ] **Step 1: Add method to the `SubscriptionStore` interface**

In `room-worker/store.go`, locate the `SubscriptionStore` interface and append the method:

```go
// UpdateDMParticipants $sets the room's uids/accounts pair on dm/botDM
// rooms after the counterpart user has been resolved. Idempotent under
// JetStream redelivery; safe to call multiple times with the same args.
UpdateDMParticipants(ctx context.Context, roomID string, uids, accounts []string) error
```

- [ ] **Step 2: Implement on `MongoStore`**

In `room-worker/store_mongo.go`, append:

```go
func (s *MongoStore) UpdateDMParticipants(ctx context.Context, roomID string, uids, accounts []string) error {
	res, err := s.rooms.UpdateOne(ctx,
		bson.M{"_id": roomID},
		bson.M{"$set": bson.M{"uids": uids, "accounts": accounts}},
	)
	if err != nil {
		return fmt.Errorf("update dm participants (room %s): %w", roomID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("update dm participants (room %s): room not found", roomID)
	}
	return nil
}
```

Verify imports already include `go.mongodb.org/mongo-driver/v2/bson` and `fmt` — they should be present from earlier methods in this file.

`MatchedCount == 0` is an error: the handler creates the room before this call, so a zero-match means the doc disappeared (race delete, wrong roomID, replica lag). Surface as a wrapped error so the handler returns it and JetStream retries.

- [ ] **Step 3: Regenerate mocks**

Run: `make generate SERVICE=room-worker`
Expected: `room-worker/mock_store_test.go` gains a `UpdateDMParticipants` mock method and matching `UpdateDMParticipantsCall` helper. No other diffs.

- [ ] **Step 4: Verify compilation**

Run: `go build ./room-worker/...`
Expected: success.

- [ ] **Step 5: Commit**

```bash
git add room-worker/store.go room-worker/store_mongo.go room-worker/mock_store_test.go
git commit -m "feat(room-worker): add UpdateDMParticipants store method"
```

---

## Task 13: Wire `UpdateDMParticipants` into `processCreateRoomDM`

Implements spec §3.3 (call site 1) and acceptance F1.

**Files:**
- Modify: `room-worker/handler.go` (`processCreateRoomDM`)
- Modify: `room-worker/handler_test.go`

- [ ] **Step 1: Write the failing test**

Append to `room-worker/handler_test.go`. Reuse the existing `newCreateRoomTestHandler`, `makeCreateRoomBody`, and `testRequestID` helpers.

```go
// F1: async DM create persists room with UIDs/Accounts sorted by UID and
// paired by index. Pick requester/other IDs whose lex order differs from
// their accounts so the pairing invariant is observable.
func TestProcessCreateRoom_DM_SetsParticipantFields(t *testing.T) {
	h, mockStore, _ := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_zzz", Account: "alice", EngName: "Alice", ChineseName: "愛", SiteID: "site-A"}
	other := &model.User{ID: "u_aaa", Account: "bob", EngName: "Bob", ChineseName: "鮑", SiteID: "site-A"}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "bob").Return(other, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-dm-fields").Return(nil)

	// UIDs sorted ascending: ["u_aaa","u_zzz"]; Accounts mirror that
	// permutation: ["bob","alice"] (bob's id sorted first).
	mockStore.EXPECT().UpdateDMParticipants(gomock.Any(), "room-dm-fields",
		[]string{"u_aaa", "u_zzz"}, []string{"bob", "alice"}).
		Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID:           "room-dm-fields",
		RequesterAccount: "alice",
		Users:            []string{"bob"},
		Timestamp:        time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestProcessCreateRoom_DM_SetsParticipantFields ./room-worker/`
Expected: FAIL — the `UpdateDMParticipants` mock expectation is unmet because `processCreateRoomDM` doesn't call it yet.

- [ ] **Step 3: Write minimal implementation**

In `room-worker/handler.go`, modify `processCreateRoomDM` to call `UpdateDMParticipants` after `BulkCreateSubscriptions` and to mirror the persisted fields onto the in-memory `room`:

```go
func (h *Handler) processCreateRoomDM(ctx context.Context, req *model.CreateRoomRequest, room *model.Room, requester *model.User, requestID string, acceptedAt, now time.Time) error {
	other, err := h.store.GetUser(ctx, req.Users[0])
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return newPermanent("counterpart not found")
		}
		return fmt.Errorf("get counterpart: %w", err)
	}

	subs := buildDMSubs(requester, other, room, acceptedAt)
	if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
		return fmt.Errorf("bulk create subs: %w", err)
	}

	uids, accounts := model.BuildDMParticipants(requester, other)
	if err := h.store.UpdateDMParticipants(ctx, room.ID, uids, accounts); err != nil {
		return fmt.Errorf("update dm participants: %w", err)
	}
	room.UIDs = uids
	room.Accounts = accounts

	return h.finishCreateRoom(ctx, req, room, requester, []*model.User{requester, other}, subs, requestID, now)
}
```

Order rationale: `UpdateDMParticipants` runs AFTER `BulkCreateSubscriptions` so that a worker crash between the two writes leaves the room in the legacy shape (no `uids`/`accounts`) — which existing consumers already tolerate — rather than a uids-set-but-no-subs shape that no consumer expects. JetStream redelivery converges either way.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run TestProcessCreateRoom_DM_SetsParticipantFields ./room-worker/`
Expected: PASS.

Run: `go test -race -run TestProcessCreateRoom_DM ./room-worker/`
Expected: All PASS. Pre-existing tests that exercise `processCreateRoom` with a DM payload (`TestProcessCreateRoom_DM_BuildsTwoSubs`, `TestProcessCreateRoom_DM_EmitsNoSysMessages`, `TestProcessCreateRoom_DM_PublishesLocalInbox`) need a new `mockStore.EXPECT().UpdateDMParticipants(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)` line. Add it just above each existing `BulkCreateSubscriptions` expectation; the mock is order-agnostic by default.

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): set DM participant fields on async DM create"
```

---

## Task 14: Wire `UpdateDMParticipants` into `processCreateRoomBotDM`

Implements spec §3.3 (call site 2) and acceptance F2. Mirrors Task 13's structure; the bot user populates one slot of the pair.

**Files:**
- Modify: `room-worker/handler.go` (`processCreateRoomBotDM`)
- Modify: `room-worker/handler_test.go`

- [ ] **Step 1: Write the failing test**

Append to `room-worker/handler_test.go`:

```go
// F2: async botDM create persists room with UIDs/Accounts paired by index.
func TestProcessCreateRoom_BotDM_SetsParticipantFields(t *testing.T) {
	h, mockStore, _ := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_zzz", Account: "alice", EngName: "Alice", ChineseName: "愛", SiteID: "site-A"}
	bot := &model.User{ID: "u_aaa", Account: "supportbot.bot", EngName: "Support", ChineseName: "支援", SiteID: "site-A"}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "supportbot.bot").Return(bot, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-botdm-fields").Return(nil)

	mockStore.EXPECT().UpdateDMParticipants(gomock.Any(), "room-botdm-fields",
		[]string{"u_aaa", "u_zzz"}, []string{"supportbot.bot", "alice"}).
		Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID:           "room-botdm-fields",
		RequesterAccount: "alice",
		Users:            []string{"supportbot.bot"},
		Timestamp:        time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestProcessCreateRoom_BotDM_SetsParticipantFields ./room-worker/`
Expected: FAIL — `UpdateDMParticipants` expectation unmet.

- [ ] **Step 3: Write minimal implementation**

In `room-worker/handler.go`, modify `processCreateRoomBotDM`:

```go
func (h *Handler) processCreateRoomBotDM(ctx context.Context, req *model.CreateRoomRequest, room *model.Room, requester *model.User, requestID string, acceptedAt, now time.Time) error {
	bot, err := h.store.GetUser(ctx, req.Users[0])
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return newPermanent("bot user not found")
		}
		return fmt.Errorf("get bot user: %w", err)
	}

	subs := buildBotDMSubs(requester, bot, room, acceptedAt)
	if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
		return fmt.Errorf("bulk create subs: %w", err)
	}

	uids, accounts := model.BuildDMParticipants(requester, bot)
	if err := h.store.UpdateDMParticipants(ctx, room.ID, uids, accounts); err != nil {
		return fmt.Errorf("update dm participants: %w", err)
	}
	room.UIDs = uids
	room.Accounts = accounts

	return h.finishCreateRoom(ctx, req, room, requester, []*model.User{requester, bot}, subs, requestID, now)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run TestProcessCreateRoom_BotDM_SetsParticipantFields ./room-worker/`
Expected: PASS.

Run: `go test -race -run TestProcessCreateRoom_BotDM ./room-worker/`
Expected: All PASS. Update any pre-existing botDM test (e.g. `TestProcessCreateRoom_BotDM_HasIsSubscribed`) to expect `UpdateDMParticipants` the same way as Task 13.

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): set DM participant fields on async botDM create"
```

---

## Task 15: Set `UIDs`/`Accounts` inline in `handleSyncCreateDM`

Implements spec §3.3 (call site 3) and acceptance F3. The sync DM path already fetches both users before `CreateRoom`, so the fields are set on the `Room` literal directly — no `UpdateDMParticipants` call.

**Files:**
- Modify: `room-worker/handler.go` (`handleSyncCreateDM`)
- Modify: `room-worker/handler_test.go`

- [ ] **Step 1: Write the failing test**

Append to `room-worker/handler_test.go`:

```go
// F3: sync DM create persists room with UIDs/Accounts set on the initial
// CreateRoom call. No UpdateDMParticipants on this path.
func TestHandleSyncCreateDM_SetsParticipantFieldsOnInitialCreate(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockSubscriptionStore(ctrl)
	h := &Handler{store: mockStore, siteID: "site-A", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }}
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := model.User{ID: "u_zzz", Account: "alice", EngName: "Alice", ChineseName: "愛", SiteID: "site-A"}
	other := model.User{ID: "u_aaa", Account: "bob", EngName: "Bob", ChineseName: "鮑", SiteID: "site-A"}

	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).
		Return([]model.User{requester, other}, nil)

	var captured *model.Room
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *model.Room) error {
			captured = r
			return nil
		})
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().FindDMSubscription(gomock.Any(), "alice", "bob").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: requester.ID, Account: requester.Account}}, nil)
	mockStore.EXPECT().FindDMSubscription(gomock.Any(), "bob", "alice").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: other.ID, Account: other.Account}}, nil)
	// No UpdateDMParticipants expectation — sync path sets fields on the literal.

	reqBody, err := json.Marshal(model.SyncCreateDMRequest{
		RequesterAccount: "alice",
		OtherAccount:     "bob",
		RoomType:         model.RoomTypeDM,
	})
	require.NoError(t, err)

	_, err = h.handleSyncCreateDM(ctx, reqBody)
	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, []string{"u_aaa", "u_zzz"}, captured.UIDs)
	assert.Equal(t, []string{"bob", "alice"}, captured.Accounts, "accounts paired with uid sort order")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestHandleSyncCreateDM_SetsParticipantFieldsOnInitialCreate ./room-worker/`
Expected: FAIL — `captured.UIDs` is nil because the current `Room` literal doesn't set it.

- [ ] **Step 3: Write minimal implementation**

In `room-worker/handler.go`, in `handleSyncCreateDM`, just before the `&model.Room{...}` literal (after `acceptedAt`/`roomID` are computed and `userCount`/`appCount` are decided), compute the participants and add them to the literal:

```go
acceptedAt := time.Now().UTC()
roomID := idgen.BuildDMRoomID(requester.ID, other.ID)

uids, accounts := model.BuildDMParticipants(requester, other)

// DMs/botDMs have a fixed 2-member roster — set counts at creation; no Reconcile needed.
userCount, appCount := 2, 0
if req.RoomType == model.RoomTypeBotDM {
	userCount, appCount = 1, 1
}

room := &model.Room{
	ID:        roomID,
	Name:      "",
	Type:      req.RoomType,
	CreatedBy: requester.ID,
	SiteID:    h.siteID,
	UserCount: userCount,
	AppCount:  appCount,
	UIDs:      uids,
	Accounts:  accounts,
	CreatedAt: acceptedAt,
	UpdatedAt: acceptedAt,
}
```

Nothing else in `handleSyncCreateDM` changes.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run TestHandleSyncCreateDM_SetsParticipantFieldsOnInitialCreate ./room-worker/`
Expected: PASS.

Run: `go test -race -run TestHandleSyncCreateDM ./room-worker/`
Expected: All PASS. Pre-existing `TestHandleSyncCreateDM_*` tests don't need updates — the new fields are additive and not asserted by name elsewhere.

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): set DM participant fields on sync DM create"
```

---

## Task 16: Pin channel-create `omitempty` guarantee

Implements spec acceptance F4. Guards against a future regression that accidentally sets `UIDs`/`Accounts` on non-DM rooms.

**Files:**
- Modify: `room-worker/handler_test.go`

- [ ] **Step 1: Write the test (passes immediately — this is a guard test)**

Append to `room-worker/handler_test.go`:

```go
// F4: channel create does not set UIDs/Accounts; the captured Room has
// both fields nil so `omitempty` drops them from the BSON document. Guard
// test — pins the contract so a future edit can't silently leak DM-only
// fields onto channels.
func TestProcessCreateRoom_Channel_DoesNotSetParticipantFields(t *testing.T) {
	h, mockStore, _ := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_a", Account: "alice", EngName: "Alice", ChineseName: "愛", SiteID: "site-A"}
	bob := model.User{ID: "u_b", Account: "bob", EngName: "Bob", ChineseName: "鮑", SiteID: "site-A"}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)

	var captured *model.Room
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *model.Room) error {
			captured = r
			return nil
		})
	mockStore.EXPECT().ListNewMembersForNewRoom(gomock.Any(), []string(nil), []string{"bob"}, "alice").
		Return([]string{"bob"}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{bob}, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-chan-fields").Return(nil)
	// No UpdateDMParticipants — channel path must never touch the fields.

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID:           "room-chan-fields",
		RequesterAccount: "alice",
		Name:             "team-room",
		ResolvedUsers:    []string{"bob"},
		Timestamp:        time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))
	require.NotNil(t, captured)
	assert.Nil(t, captured.UIDs, "channels must omit UIDs (omitempty drops nil)")
	assert.Nil(t, captured.Accounts, "channels must omit Accounts")
}
```

- [ ] **Step 2: Run test to confirm it passes**

Run: `go test -run TestProcessCreateRoom_Channel_DoesNotSetParticipantFields ./room-worker/`
Expected: PASS — `processCreateRoomChannel` was never modified to set these fields, so the captured Room has nil for both.

The RED-GREEN cycle is intentionally collapsed here because the asserted behavior (do not set the fields) is the default. The test pins the contract so future edits can't violate it silently — a guard test, not a behavior-driver. If this test fails, a regression has crept in.

- [ ] **Step 3: Commit**

```bash
git add room-worker/handler_test.go
git commit -m "test(room-worker): pin channel-create omitempty guarantee for DM fields"
```

---

## Task 17: Final verification (DM participant fields)

- [ ] **Step 1: Run lint**

Run: `make lint SERVICE=room-worker`
Expected: PASS, no warnings.

- [ ] **Step 2: Run full test suite with race detector**

Run: `make test SERVICE=room-worker && go test -race ./pkg/model/`
Expected: All PASS.

- [ ] **Step 3: Confirm mock regeneration is clean**

Run: `make generate SERVICE=room-worker && git diff --stat`
Expected: No diff (Task 12 regenerated already; this confirms idempotency).

- [ ] **Step 4: Confirm coverage on the new helper**

```bash
go test -coverprofile=/tmp/cover.out ./pkg/model/ ./room-worker/
go tool cover -func=/tmp/cover.out | grep -E "BuildDMParticipants|UpdateDMParticipants"
```
Expected: `BuildDMParticipants` 100% from unit tests; `UpdateDMParticipants` (Mongo impl) 0% from unit tests (covered indirectly by integration tests). Handler-side wiring is exercised via the mock in Tasks 13–15, so per-handler coverage in handler.go is unchanged or improved.

---

## Self-Review (post-write)

**Spec coverage:**

| Spec section | Covered by |
|---|---|
| §1 bug list | Tasks 1–9 (helpers + filters + Content + sender + validation) |
| §2.1 indiv write rule | Task 3 (`processAddMembers`) + Task 4 (`processCreateRoomChannel`) |
| §2.2 backfill gate / `hadOrgsBefore` restructure | Task 2 |
| §2.3 `members_added` Content + requester fetch + name validation | Tasks 5, 6, 7 |
| §2.4 remove sender + Content + name validation + SectName fix | Tasks 8, 9 |
| §2.5 emit-vs-skip on remove | Existing demote-only return at handler.go:276 retained by Task 8; org remove always emits via Task 9 |
| §2.6 helpers | Task 1 |
| §3 acceptance criteria A1–A6 | A1, A2 → Task 3; A3 → Task 3 (no orgs in req, hadOrgsBefore=true); A4 → Task 4; A5, A6 → Task 2 |
| §3 acceptance criteria B1–B3 | B1, B2 → Task 6; B3 → Task 7 |
| §3 acceptance criteria C1–C5 | C1, C2, C4 → Task 8; C3, C5 → Task 9 |
| §3 acceptance criteria D1–D5 | D1, D2, D3 → Task 5; D4 → Task 8; D5 → Task 9 |
| §3 acceptance criteria E | Task 10 (membership fixes) + Task 17 (DM fields) |
| §3 DM Participant Fields — field shape (§3.1) | Task 11 |
| §3 DM Participant Fields — pairing invariant + `BuildDMParticipants` (§3.2) | Task 11 |
| §3 DM Participant Fields — three call sites (§3.3) | Tasks 13, 14, 15 |
| §3 DM Participant Fields — store interface (§3.4) | Task 12 |
| §3 DM Participant Fields — test plan (§3.5) | Tasks 11, 13, 14, 15, 16 |
| §4 acceptance F1 (async DM call-site) | Task 13 |
| §4 acceptance F2 (async botDM call-site) | Task 14 |
| §4 acceptance F3 (sync DM call-site, no UpdateDMParticipants) | Task 15 |
| §4 acceptance F4 (channel/discussion fields absent) | Task 16 |
| §4 acceptance F5 (pairing under non-aligned sort) | Task 11's `TestBuildDMParticipants_PreservesPairingUnderNonAlignedSort` |
| §4 acceptance F6 (idempotency on replay) | Architectural property of `$set` in Task 12; replay would call `UpdateDMParticipants` again with the same args, no test needed |
| §4 acceptance F7 (forward-only, no backfill) | No migration task; legacy rooms are untouched by design — nothing to test |

A3 (add `Users=[u1], Orgs=[]` to a room already having an org member) is not exercised by a dedicated test in this plan — Task 2's `BackfillSkippedWhenRoomAlreadyHasOrgs` covers the same control flow with a different account name. If you want strict per-criterion coverage, add a sibling test in Task 3 mirroring that scenario; otherwise, the path is exercised end-to-end.

**Placeholder scan:** No "TBD" / "implement later" / "similar to Task N" instances. Every code-changing step contains the actual code. Task 11's Step 5 ("optional — extend round-trip helper") is a conditional, not a placeholder: it gives the executor a clear yes/no check.

**Type consistency:** `formatAddedSingle`, `formatAddedMulti`, `formatRemovedUserContent`, `formatRemovedOrgContent`, `formatLeftContent`, `displayName` — same names used in Tasks 1, 6, 7, 8, 9. `OrgMemberStatus`, `permanentError`, `newPermanent`, `ErrUserNotFound`, `findSysMsg` — referenced consistently. `model.UserWithMembership.User` accessor noted in Task 8 with a fallback instruction if the actual type differs. New for DM-fields work: `model.BuildDMParticipants`, `SubscriptionStore.UpdateDMParticipants`, `Room.UIDs`, `Room.Accounts` — same names used across Tasks 11–16.

**Known soft spots an executor will need to confirm at the point of edit:**
1. The exact `permanentError` type's exportedness (tests use `errors.As`; if unexported, switch to `assert.Contains(err.Error(), ...)`).
2. The result type of `GetUserWithMembership` (`*model.UserWithMembership` wrapping `model.User`, or `*model.User` with extra fields directly). Adjust the `&user.User` accessor in Task 8 accordingly.
3. `model.MessageTypeMembersAdded` vs the string literal `"members_added"` — Task 7 uses the constant (matches existing code at handler.go:1253), Task 6 uses the literal (matches existing code at handler.go:781); keep both as-is unless lint flags it.
4. Task 11's optional Step 5: the project's `pkg/model/model_test.go` may or may not have a generic round-trip case for `Room`. If a `Room` case exists, extend it with `UIDs`/`Accounts` populated and nil variants. If not, skip — the new `pkg/model/room_test.go` already pins the helper's behavior; explicit round-trip coverage is a nice-to-have, not a correctness gate.
5. Task 13 & 14's Step 4 instructs the executor to add `UpdateDMParticipants` mock expectations to pre-existing DM/botDM `processCreateRoom` tests. The current count of such tests is six (`TestProcessCreateRoom_DM_BuildsTwoSubs`, `TestProcessCreateRoom_DM_EmitsNoSysMessages`, `TestProcessCreateRoom_BotDM_HasIsSubscribed`, `TestProcessCreateRoom_DM_PublishesLocalInbox`, plus any added during the membership-fix work). The executor should grep for `processCreateRoom_DM\|processCreateRoom_BotDM` and update each test that goes through the full handler.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-14-room-worker-membership-fixes.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch with checkpoints.

Which approach?
