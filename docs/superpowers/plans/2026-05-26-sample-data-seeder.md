# Sample Data Seeder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a `make seed` Go binary at `tools/seed-sample-data` that populates MongoDB and Valkey with a small, well-formed, idempotent dataset after `make deps-up`.

**Architecture:** Single-binary CLI under `tools/`. Pure-data fixture builders (rigorously unit-tested) drive idempotent upserts into MongoDB collections (`users`, `rooms`, `subscriptions`, `room_members`, `messages`, `thread_rooms`, `thread_subscriptions`) and Valkey writes (room encryption keys via `pkg/roomkeystore`; per-account search restricted-rooms cache via `pkg/valkeyutil`). All IDs deterministic so re-runs converge on the same final state.

**Tech Stack:** Go 1.25, `pkg/model`, `pkg/idgen`, `pkg/mongoutil`, `pkg/valkeyutil`, `pkg/roomkeystore`, `caarlos0/env/v11`, `go.uber.org/mock` (none required — no interfaces to mock), `stretchr/testify` for assertions.

**Spec:** `docs/superpowers/specs/2026-05-26-sample-data-seeder-design.md`

---

## File Structure

```text
tools/seed-sample-data/
  main.go            # Flag parsing, env, connect, dispatch, summary logging
  fixtures.go        # Pure-data builders (Users, Rooms, Subscriptions, RoomMembers,
                     # Messages, ThreadRooms, ThreadSubscriptions, ValkeyData)
  mongo.go           # Per-collection upsert + delete helpers
  valkey.go          # Room-key writes via roomkeystore + cache writes via valkeyutil
  fixtures_test.go   # Unit tests for builders + cross-collection consistency
  main_test.go       # CLI flag / env parsing tests
  mongo_test.go      # Unit tests for Mongo write helpers (use mongoutil.BulkUpsertByID)
  valkey_test.go     # Unit tests for Valkey key formatting + payload encoding
```

`Makefile` modifications: append `seed`, `seed-reset`, `seed-dry-run` targets.

---

## Task 1: Scaffold tool directory and Makefile targets

**Files:**
- Create: `tools/seed-sample-data/main.go`
- Modify: `Makefile` (append new targets after `seed-reset` line slot — add at the bottom of the file)

- [ ] **Step 1: Create the directory**

```bash
mkdir -p tools/seed-sample-data
```

- [ ] **Step 2: Create skeleton `main.go`**

```go
// Package main is the seed-sample-data CLI: populates MongoDB and Valkey
// with a small, well-formed, idempotent dataset for local development.
// Run via `make seed` after `make deps-up`.
package main

import (
	"log/slog"
	"os"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	slog.Info("seed-sample-data placeholder")
}
```

- [ ] **Step 3: Verify the binary builds**

Run: `go build ./tools/seed-sample-data/`
Expected: exit 0, no output.

- [ ] **Step 4: Add Makefile targets**

Append to `Makefile`:

```makefile

# --- Sample data seeder -----------------------------------------------------
# Populate MongoDB and Valkey with a small idempotent dataset for local dev.
# Run after `make deps-up`. Safe to re-run; `seed-reset` wipes the seed
# records first via stable IDs (never DROP DATABASE) so any hand-added
# dev data survives. `seed-dry-run` prints the plan without writing.
.PHONY: seed seed-reset seed-dry-run

seed:
	go run ./tools/seed-sample-data

seed-reset:
	go run ./tools/seed-sample-data --reset

seed-dry-run:
	go run ./tools/seed-sample-data --dry-run
```

Also append `seed seed-reset seed-dry-run` to the top-level `.PHONY:` line so `make help` style tooling treats them correctly. The existing first `.PHONY:` line is the canonical declaration; the section-level `.PHONY` above is fine on its own — leave it.

- [ ] **Step 5: Verify Makefile targets parse**

Run: `make -n seed seed-reset seed-dry-run`
Expected: prints three `go run` lines, exit 0.

- [ ] **Step 6: Commit**

```bash
git add tools/seed-sample-data/main.go Makefile
git commit -m "feat(seed-sample-data): scaffold tool and Makefile targets"
```

---

## Task 2: Constants and time anchor (`fixtures.go` skeleton)

**Files:**
- Create: `tools/seed-sample-data/fixtures.go`
- Create: `tools/seed-sample-data/fixtures_test.go`

- [ ] **Step 1: Write failing test for the time anchor**

Create `tools/seed-sample-data/fixtures_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSeedBaseTime_IsFixed(t *testing.T) {
	// Time anchor must be a wall-clock constant so re-runs produce
	// identical CreatedAt / LastSeenAt / message timestamps.
	want, err := time.Parse(time.RFC3339, "2026-05-01T09:00:00Z")
	assert.NoError(t, err)
	assert.Equal(t, want, seedBaseTime)
}

func TestSiteIDs_AreLocalAndRemote(t *testing.T) {
	assert.Equal(t, "site-local", siteLocal)
	assert.Equal(t, "site-remote", siteRemote)
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `go test ./tools/seed-sample-data/`
Expected: FAIL — undefined: `seedBaseTime`, `siteLocal`, `siteRemote`.

- [ ] **Step 3: Implement `fixtures.go` constants**

Create `tools/seed-sample-data/fixtures.go`:

```go
// Package main: fixture builders.
//
// All builders return pure data — no I/O, no clocks, no random sources —
// so unit tests can assert exact counts/IDs and so re-runs of `make seed`
// produce the same final state.
package main

import "time"

const (
	siteLocal  = "site-local"
	siteRemote = "site-remote"
)

// seedBaseTime is the wall-clock anchor every seeded timestamp derives
// from. Fixed so re-running the seeder does not drift `CreatedAt`,
// `JoinedAt`, `LastSeenAt`, or message timestamps.
var seedBaseTime = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./tools/seed-sample-data/`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add tools/seed-sample-data/
git commit -m "feat(seed-sample-data): time anchor and site constants"
```

---

## Task 3: Users fixture

**Files:**
- Modify: `tools/seed-sample-data/fixtures.go`
- Modify: `tools/seed-sample-data/fixtures_test.go`

- [ ] **Step 1: Write failing test for `BuildUsers`**

Append to `tools/seed-sample-data/fixtures_test.go`:

```go
func TestBuildUsers_ReturnsExpectedRoster(t *testing.T) {
	users := BuildUsers()

	require.Len(t, users, 10)

	// IDs are stable and ordered for deterministic test output.
	wantIDs := []string{
		"u-alice", "u-bob", "u-carol", "u-dave", "u-eve",
		"u-frank", "u-grace", "u-heidi", "u-ivan", "u-judy",
	}
	gotIDs := make([]string, len(users))
	for i, u := range users {
		gotIDs[i] = u.ID
	}
	assert.Equal(t, wantIDs, gotIDs)
}

func TestBuildUsers_AccountMatchesIDSuffix(t *testing.T) {
	for _, u := range BuildUsers() {
		assert.Equal(t, "u-"+u.Account, u.ID,
			"user %q has account %q but id %q — must match `u-<account>`", u.Account, u.Account, u.ID)
	}
}

func TestBuildUsers_SiteDistribution(t *testing.T) {
	local, remote := 0, 0
	for _, u := range BuildUsers() {
		switch u.SiteID {
		case siteLocal:
			local++
		case siteRemote:
			remote++
		default:
			t.Fatalf("unexpected siteId %q on user %s", u.SiteID, u.ID)
		}
	}
	assert.Equal(t, 8, local, "8 local users (alice..heidi)")
	assert.Equal(t, 2, remote, "2 remote users (ivan, judy)")
}

func TestBuildUsers_RequiredFieldsPopulated(t *testing.T) {
	for _, u := range BuildUsers() {
		assert.NotEmpty(t, u.ID, "id")
		assert.NotEmpty(t, u.Account, "account for %s", u.ID)
		assert.NotEmpty(t, u.SiteID, "siteId for %s", u.ID)
		assert.NotEmpty(t, u.SectID, "sectId for %s", u.ID)
		assert.NotEmpty(t, u.SectName, "sectName for %s", u.ID)
		assert.NotEmpty(t, u.DeptID, "deptId for %s", u.ID)
		assert.NotEmpty(t, u.DeptName, "deptName for %s", u.ID)
		assert.NotEmpty(t, u.EngName, "engName for %s", u.ID)
		assert.NotEmpty(t, u.ChineseName, "chineseName for %s", u.ID)
		assert.NotEmpty(t, u.EmployeeID, "employeeId for %s", u.ID)
	}
}
```

Also add `"github.com/stretchr/testify/require"` to imports if not already present.

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./tools/seed-sample-data/`
Expected: FAIL — undefined: `BuildUsers`.

- [ ] **Step 3: Implement `BuildUsers`**

Append to `tools/seed-sample-data/fixtures.go`:

```go
import (
	"time"

	"github.com/hmchangw/chat/pkg/model"
)
```

(Replace the existing single-line `import "time"` with the grouped import above.) Then append:

```go
// BuildUsers returns the seed roster. alice and bob match the Keycloak
// realm in auth-service/deploy/keycloak/realm-export.json; the rest
// populate rooms so member lists look realistic.
func BuildUsers() []model.User {
	return []model.User{
		{ID: "u-alice", Account: "alice", SiteID: siteLocal, SectID: "eng", SectName: "Engineering", SectTCName: "工程部", DeptID: "eng-backend", DeptName: "Backend", DeptTCName: "後端組", EngName: "Alice Engineer", ChineseName: "王小愛", EmployeeID: "E001"},
		{ID: "u-bob", Account: "bob", SiteID: siteLocal, SectID: "eng", SectName: "Engineering", SectTCName: "工程部", DeptID: "eng-frontend", DeptName: "Frontend", DeptTCName: "前端組", EngName: "Bob Developer", ChineseName: "陳大寶", EmployeeID: "E002"},
		{ID: "u-carol", Account: "carol", SiteID: siteLocal, SectID: "eng", SectName: "Engineering", SectTCName: "工程部", DeptID: "eng-backend", DeptName: "Backend", DeptTCName: "後端組", EngName: "Carol Coder", ChineseName: "林小卡", EmployeeID: "E003"},
		{ID: "u-dave", Account: "dave", SiteID: siteLocal, SectID: "prod", SectName: "Product", SectTCName: "產品部", DeptID: "prod-pm", DeptName: "Product Management", DeptTCName: "產品經理", EngName: "Dave PM", ChineseName: "張小達", EmployeeID: "E004"},
		{ID: "u-eve", Account: "eve", SiteID: siteLocal, SectID: "prod", SectName: "Product", SectTCName: "產品部", DeptID: "prod-pm", DeptName: "Product Management", DeptTCName: "產品經理", EngName: "Eve Manager", ChineseName: "黃小夜", EmployeeID: "E005"},
		{ID: "u-frank", Account: "frank", SiteID: siteLocal, SectID: "design", SectName: "Design", SectTCName: "設計部", DeptID: "design-ux", DeptName: "UX", DeptTCName: "使用者體驗", EngName: "Frank Designer", ChineseName: "吳小法", EmployeeID: "E006"},
		{ID: "u-grace", Account: "grace", SiteID: siteLocal, SectID: "design", SectName: "Design", SectTCName: "設計部", DeptID: "design-ux", DeptName: "UX", DeptTCName: "使用者體驗", EngName: "Grace UX", ChineseName: "蔡小恩", EmployeeID: "E007"},
		{ID: "u-heidi", Account: "heidi", SiteID: siteLocal, SectID: "ops", SectName: "Operations", SectTCName: "營運部", DeptID: "ops-sre", DeptName: "SRE", DeptTCName: "站點可靠性", EngName: "Heidi Ops", ChineseName: "周小海", EmployeeID: "E008"},
		{ID: "u-ivan", Account: "ivan", SiteID: siteRemote, SectID: "eng", SectName: "Engineering (Remote)", SectTCName: "工程部（遠端）", DeptID: "eng-backend", DeptName: "Backend", DeptTCName: "後端組", EngName: "Ivan Remote", ChineseName: "鄭小宜", EmployeeID: "R001"},
		{ID: "u-judy", Account: "judy", SiteID: siteRemote, SectID: "prod", SectName: "Product (Remote)", SectTCName: "產品部（遠端）", DeptID: "prod-pm", DeptName: "Product Management", DeptTCName: "產品經理", EngName: "Judy Cross", ChineseName: "高小朱", EmployeeID: "R002"},
	}
}

// usersByAccount indexes BuildUsers by account for fast lookup in other builders.
func usersByAccount() map[string]model.User {
	out := make(map[string]model.User, 10)
	for _, u := range BuildUsers() {
		out[u.Account] = u
	}
	return out
}
```

The `time` import stays used via `seedBaseTime` from Task 2; no placeholder needed.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./tools/seed-sample-data/ -v`
Expected: PASS (all 4 user tests + the earlier 2 from Task 2).

- [ ] **Step 5: Commit**

```bash
git add tools/seed-sample-data/
git commit -m "feat(seed-sample-data): users fixture (10 across two sites)"
```

---

## Task 4: Rooms fixture

**Files:**
- Modify: `tools/seed-sample-data/fixtures.go`
- Modify: `tools/seed-sample-data/fixtures_test.go`

- [ ] **Step 1: Write failing test for `BuildRooms`**

Append to `fixtures_test.go`:

```go
import (
	// ... existing imports ...
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

func TestBuildRooms_ReturnsSixRooms(t *testing.T) {
	rooms := BuildRooms()
	require.Len(t, rooms, 6)

	byID := make(map[string]model.Room, len(rooms))
	for _, r := range rooms {
		byID[r.ID] = r
	}

	for _, id := range []string{"r-general", "r-eng", "r-design", "r-remote-announce"} {
		_, ok := byID[id]
		assert.True(t, ok, "missing room %q", id)
	}
	_, ok := byID[idgen.BuildDMRoomID("u-alice", "u-bob")]
	assert.True(t, ok, "missing alice-bob DM room")
	_, ok = byID[idgen.BuildDMRoomID("u-carol", "u-eve")]
	assert.True(t, ok, "missing carol-eve DM room")
}

func TestBuildRooms_TypesAndSites(t *testing.T) {
	want := map[string]struct {
		typ    model.RoomType
		siteID string
	}{
		"r-general":                            {model.RoomTypeChannel, siteLocal},
		"r-eng":                                {model.RoomTypeChannel, siteLocal},
		"r-design":                             {model.RoomTypeChannel, siteLocal},
		idgen.BuildDMRoomID("u-alice", "u-bob"): {model.RoomTypeDM, siteLocal},
		idgen.BuildDMRoomID("u-carol", "u-eve"): {model.RoomTypeDM, siteLocal},
		"r-remote-announce":                    {model.RoomTypeChannel, siteRemote},
	}
	for _, r := range BuildRooms() {
		w, ok := want[r.ID]
		require.True(t, ok, "unexpected room id %q", r.ID)
		assert.Equal(t, w.typ, r.Type, "room %s type", r.ID)
		assert.Equal(t, w.siteID, r.SiteID, "room %s siteId", r.ID)
	}
}

func TestBuildRooms_EngIsRestricted(t *testing.T) {
	for _, r := range BuildRooms() {
		if r.ID == "r-eng" {
			assert.True(t, r.Restricted, "r-eng must be restricted for search-cache coverage")
			return
		}
	}
	t.Fatal("r-eng not found")
}

func TestBuildRooms_UserCountMatchesMemberLists(t *testing.T) {
	for _, r := range BuildRooms() {
		assert.Equal(t, len(r.UIDs), r.UserCount, "room %s userCount must match len(UIDs)", r.ID)
		assert.Equal(t, len(r.UIDs), len(r.Accounts), "room %s UIDs/Accounts must be same length", r.ID)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./tools/seed-sample-data/`
Expected: FAIL — undefined: `BuildRooms`.

- [ ] **Step 3: Implement `BuildRooms`**

Update the existing import block in `tools/seed-sample-data/fixtures.go` to add `github.com/hmchangw/chat/pkg/idgen`:

```go
import (
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)
```

Then append the channel/dm builders below the existing code:

```go
// channelRoom builds a channel-type Room with deterministic created/updated
// timestamps and synthesized UIDs/Accounts from the supplied accounts.
// LastMsgAt and LastMsgID are populated by BuildRoomsWithLastMsg later;
// the bare BuildRooms returns rooms with zero LastMsg fields.
func channelRoom(id, name, siteID string, restricted bool, accounts []string) model.Room {
	users := usersByAccount()
	uids := make([]string, len(accounts))
	accs := make([]string, len(accounts))
	for i, a := range accounts {
		u, ok := users[a]
		if !ok {
			panic("seed-sample-data: channelRoom references unknown account " + a)
		}
		uids[i] = u.ID
		accs[i] = u.Account
	}
	return model.Room{
		ID:         id,
		Name:       name,
		Type:       model.RoomTypeChannel,
		SiteID:     siteID,
		UserCount:  len(uids),
		AppCount:   0,
		CreatedAt:  seedBaseTime,
		UpdatedAt:  seedBaseTime,
		Restricted: restricted,
		UIDs:       uids,
		Accounts:   accs,
	}
}

// dmRoom builds a DM-type Room. Uses idgen.BuildDMRoomID for the deterministic
// sorted-concat ID so DM lookups in handlers match.
func dmRoom(accountA, accountB string) model.Room {
	users := usersByAccount()
	a, b := users[accountA], users[accountB]
	uids, accs := model.BuildDMParticipants(&a, &b)
	return model.Room{
		ID:        idgen.BuildDMRoomID(a.ID, b.ID),
		Name:      "", // DM rooms render the counterpart's name on the client; storage Name is empty.
		Type:      model.RoomTypeDM,
		SiteID:    siteLocal,
		UserCount: 2,
		CreatedAt: seedBaseTime,
		UpdatedAt: seedBaseTime,
		UIDs:      uids,
		Accounts:  accs,
	}
}

// BuildRooms returns the seed room set: 3 local channels, 2 local DMs, 1 remote channel.
func BuildRooms() []model.Room {
	return []model.Room{
		channelRoom("r-general", "general", siteLocal, false,
			[]string{"alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi", "ivan"}),
		channelRoom("r-eng", "engineering", siteLocal, true,
			[]string{"alice", "bob", "carol", "ivan"}),
		channelRoom("r-design", "design", siteLocal, false,
			[]string{"frank", "grace", "dave"}),
		dmRoom("alice", "bob"),
		dmRoom("carol", "eve"),
		channelRoom("r-remote-announce", "remote-announce", siteRemote, false,
			[]string{"ivan", "judy", "alice"}),
	}
}

// roomsByID indexes BuildRooms for cross-collection lookups.
func roomsByID() map[string]model.Room {
	out := make(map[string]model.Room, 6)
	for _, r := range BuildRooms() {
		out[r.ID] = r
	}
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./tools/seed-sample-data/ -v`
Expected: PASS (all room tests + earlier tests).

- [ ] **Step 5: Commit**

```bash
git add tools/seed-sample-data/
git commit -m "feat(seed-sample-data): rooms fixture (3 channels, 2 DMs, 1 remote)"
```

---

## Task 5: Room members fixture

**Files:**
- Modify: `tools/seed-sample-data/fixtures.go`
- Modify: `tools/seed-sample-data/fixtures_test.go`

- [ ] **Step 1: Write failing test for `BuildRoomMembers`**

Append to `fixtures_test.go`:

```go
func TestBuildRoomMembers_ChannelsOnly(t *testing.T) {
	members := BuildRoomMembers()

	// Total entries = sum of channel UIDs across channel rooms.
	// r-general:9 + r-eng:4 + r-design:3 + r-remote-announce:3 = 19
	assert.Len(t, members, 19)

	// No DM rooms must appear — DM membership is derived from subscriptions.
	dmAB := idgen.BuildDMRoomID("u-alice", "u-bob")
	dmCE := idgen.BuildDMRoomID("u-carol", "u-eve")
	for _, m := range members {
		assert.NotEqual(t, dmAB, m.RoomID, "alice-bob DM must not appear in room_members")
		assert.NotEqual(t, dmCE, m.RoomID, "carol-eve DM must not appear in room_members")
	}
}

func TestBuildRoomMembers_StableIDFormat(t *testing.T) {
	for _, m := range BuildRoomMembers() {
		want := m.RoomID + ":" + m.Member.ID
		assert.Equal(t, want, m.ID, "RoomMember.ID must be `<roomID>:<userID>`")
		assert.Equal(t, model.RoomMemberIndividual, m.Member.Type)
		assert.NotEmpty(t, m.Member.Account)
		assert.Equal(t, seedBaseTime, m.Ts)
	}
}

func TestBuildRoomMembers_OneEntryPerChannelUser(t *testing.T) {
	members := BuildRoomMembers()
	got := make(map[string]int)
	for _, m := range members {
		got[m.RoomID]++
	}
	assert.Equal(t, 9, got["r-general"])
	assert.Equal(t, 4, got["r-eng"])
	assert.Equal(t, 3, got["r-design"])
	assert.Equal(t, 3, got["r-remote-announce"])
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./tools/seed-sample-data/`
Expected: FAIL — undefined: `BuildRoomMembers`.

- [ ] **Step 3: Implement `BuildRoomMembers`**

Append to `fixtures.go`:

```go
// BuildRoomMembers returns one RoomMember per (channel, user) pair across
// all four channel rooms. DM rooms are intentionally excluded — room-service
// derives DM membership from subscriptions when no room_members document
// exists for the room (see room-service/store_mongo.go:329).
func BuildRoomMembers() []model.RoomMember {
	users := usersByAccount()
	rooms := BuildRooms()
	out := make([]model.RoomMember, 0, 19)
	for _, r := range rooms {
		if r.Type != model.RoomTypeChannel {
			continue
		}
		for _, account := range r.Accounts {
			u := users[account]
			out = append(out, model.RoomMember{
				ID:     r.ID + ":" + u.ID,
				RoomID: r.ID,
				Ts:     seedBaseTime,
				Member: model.RoomMemberEntry{
					ID:      u.ID,
					Type:    model.RoomMemberIndividual,
					Account: u.Account,
				},
			})
		}
	}
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./tools/seed-sample-data/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/seed-sample-data/
git commit -m "feat(seed-sample-data): room_members fixture (channels only, 19 entries)"
```

---

## Task 6: Subscriptions fixture

**Files:**
- Modify: `tools/seed-sample-data/fixtures.go`
- Modify: `tools/seed-sample-data/fixtures_test.go`

- [ ] **Step 1: Write failing test for `BuildSubscriptions`**

Append to `fixtures_test.go`:

```go
func TestBuildSubscriptions_Count(t *testing.T) {
	// One subscription per (user, room) the user belongs to.
	// r-general:9 + r-eng:4 + r-design:3 + 2 DMs (2 each):4 + r-remote-announce:3 = 23
	assert.Len(t, BuildSubscriptions(), 23)
}

func TestBuildSubscriptions_StableID(t *testing.T) {
	for _, s := range BuildSubscriptions() {
		want := "sub:" + s.User.ID + ":" + s.RoomID
		assert.Equal(t, want, s.ID, "Subscription.ID must be `sub:<userID>:<roomID>`")
	}
}

func TestBuildSubscriptions_OwnerRoles(t *testing.T) {
	owners := map[string]string{ // roomID -> expected owner userID
		"r-general":         "u-alice",
		"r-eng":             "u-alice",
		"r-design":          "u-frank",
		"r-remote-announce": "u-ivan",
	}
	got := map[string]string{}
	for _, s := range BuildSubscriptions() {
		for _, role := range s.Roles {
			if role == model.RoleOwner {
				got[s.RoomID] = s.User.ID
			}
		}
	}
	for roomID, wantUser := range owners {
		assert.Equal(t, wantUser, got[roomID], "owner of %s", roomID)
	}
}

func TestBuildSubscriptions_FieldsPopulated(t *testing.T) {
	for _, s := range BuildSubscriptions() {
		assert.NotEmpty(t, s.User.ID)
		assert.NotEmpty(t, s.User.Account)
		assert.NotEmpty(t, s.RoomID)
		assert.NotEmpty(t, s.SiteID)
		assert.NotEmpty(t, s.RoomType)
		assert.NotEmpty(t, s.Roles, "subscription %s has empty roles", s.ID)
		assert.True(t, s.IsSubscribed, "IsSubscribed should be true for seeded subs")
		assert.False(t, s.JoinedAt.IsZero())
	}
}

func TestBuildSubscriptions_DMSubscriptionsHaveCounterpartName(t *testing.T) {
	// DM rooms aren't channels; their subs should still resolve to two
	// counterpart-named entries (each side sees the other's name as room label).
	dmAB := idgen.BuildDMRoomID("u-alice", "u-bob")
	gotForAB := 0
	for _, s := range BuildSubscriptions() {
		if s.RoomID != dmAB {
			continue
		}
		gotForAB++
		if s.User.Account == "alice" {
			assert.Equal(t, "bob", s.Name, "alice's DM sub Name = counterpart account")
		}
		if s.User.Account == "bob" {
			assert.Equal(t, "alice", s.Name, "bob's DM sub Name = counterpart account")
		}
	}
	assert.Equal(t, 2, gotForAB, "DM room must have exactly 2 subscriptions")
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./tools/seed-sample-data/`
Expected: FAIL — undefined: `BuildSubscriptions`.

- [ ] **Step 3: Implement `BuildSubscriptions`**

Append to `fixtures.go`:

```go
// roomOwners maps roomID -> account of the seeded owner.
// Used by BuildSubscriptions to assign RoleOwner.
var roomOwners = map[string]string{
	"r-general":         "alice",
	"r-eng":             "alice",
	"r-design":          "frank",
	"r-remote-announce": "ivan",
}

// BuildSubscriptions returns one Subscription per (user, room) the user
// is a member of. DM subscriptions are emitted as plain Subscription rows
// with Name set to the counterpart's account (matches how dm rooms are
// labeled on the client). Roles are ["owner"] for the seeded owner of
// each channel and ["member"] otherwise; DM members get ["member"].
func BuildSubscriptions() []model.Subscription {
	users := usersByAccount()
	rooms := BuildRooms()
	out := make([]model.Subscription, 0, 23)
	for _, r := range rooms {
		for i, account := range r.Accounts {
			u := users[account]
			roles := []model.Role{model.RoleMember}
			if owner, ok := roomOwners[r.ID]; ok && owner == account {
				roles = []model.Role{model.RoleOwner}
			}
			// DM rooms label the subscription with the counterpart's account.
			name := r.Name
			if r.Type == model.RoomTypeDM {
				other := r.Accounts[1-i] // 2-element slice; flip the index
				name = other
			}
			out = append(out, model.Subscription{
				ID:           "sub:" + u.ID + ":" + r.ID,
				User:         model.SubscriptionUser{ID: u.ID, Account: u.Account, IsBot: false},
				RoomID:       r.ID,
				SiteID:       r.SiteID,
				Roles:        roles,
				Name:         name,
				RoomType:     r.Type,
				IsSubscribed: true,
				JoinedAt:     seedBaseTime,
				HasMention:   false,
				Alert:        true,
				Muted:        false,
			})
		}
	}
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./tools/seed-sample-data/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/seed-sample-data/
git commit -m "feat(seed-sample-data): subscriptions fixture (23 entries)"
```

---

## Task 7: Messages fixture (channels + DMs, no threads yet)

**Files:**
- Modify: `tools/seed-sample-data/fixtures.go`
- Modify: `tools/seed-sample-data/fixtures_test.go`

- [ ] **Step 1: Write failing test for `BuildMessages` (root messages only)**

Append to `fixtures_test.go`:

```go
func TestBuildMessages_TotalCount(t *testing.T) {
	// Per spec: r-general 5 + r-eng 4 root + r-eng 3 thread + r-design 3
	// + alice-bob 3 + carol-eve 2 + r-remote-announce 3 = 23
	assert.Len(t, BuildMessages(), 23)
}

func TestBuildMessages_MonotonicTimestampsPerRoom(t *testing.T) {
	byRoom := map[string][]time.Time{}
	for _, m := range BuildMessages() {
		byRoom[m.RoomID] = append(byRoom[m.RoomID], m.CreatedAt)
	}
	for room, ts := range byRoom {
		for i := 1; i < len(ts); i++ {
			assert.True(t, ts[i].After(ts[i-1]),
				"room %s: timestamps must be strictly increasing (idx %d %v vs %v)", room, i, ts[i-1], ts[i])
		}
	}
}

func TestBuildMessages_DeterministicIDs(t *testing.T) {
	first := BuildMessages()
	second := BuildMessages()
	require.Equal(t, len(first), len(second))
	for i := range first {
		assert.Equal(t, first[i].ID, second[i].ID, "message IDs must be deterministic across calls")
	}
}

func TestBuildMessages_IDsAreValidMessageIDs(t *testing.T) {
	for _, m := range BuildMessages() {
		assert.True(t, idgen.IsValidMessageID(m.ID), "id %q for message in room %s not a valid message id", m.ID, m.RoomID)
	}
}

func TestBuildMessages_AuthorIsRoomMember(t *testing.T) {
	rooms := roomsByID()
	for _, m := range BuildMessages() {
		r, ok := rooms[m.RoomID]
		require.True(t, ok, "message references unknown room %s", m.RoomID)
		found := false
		for _, account := range r.Accounts {
			if account == m.UserAccount {
				found = true
				break
			}
		}
		assert.True(t, found, "message author %s is not a member of room %s", m.UserAccount, m.RoomID)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./tools/seed-sample-data/`
Expected: FAIL — undefined: `BuildMessages`.

- [ ] **Step 3: Implement `BuildMessages`**

Append to `fixtures.go`:

```go
// messageStep is the time gap between consecutive seeded messages within
// a single room. Fixed at 5 minutes so timestamps are strictly monotonic
// and easy to reason about in tests.
const messageStep = 5 * time.Minute

// seedScript drives BuildMessages. Each entry produces one root message
// in roomID by accountAuthor at index n (n = position within that room
// in seedScript order); CreatedAt = rootStart + n*messageStep.
type seedMessage struct {
	roomID  string
	author  string // account
	content string
}

// seedScript lists every seeded message in deterministic order. Thread
// replies hang off the alice "UUIDv7?" message in r-eng (see
// threadParentMessageID below). Indexing per room drives the per-room
// timeline.
var seedScript = []seedMessage{
	// r-general
	{roomID: "r-general", author: "alice", content: "Hi everyone, welcome to general 👋"},
	{roomID: "r-general", author: "bob", content: "Good morning"},
	{roomID: "r-general", author: "dave", content: "Reminder: planning at 10"},
	{roomID: "r-general", author: "eve", content: "On it"},
	{roomID: "r-general", author: "heidi", content: "Ops update: zero incidents overnight."},

	// r-eng — index 1 is the thread parent
	{roomID: "r-eng", author: "carol", content: "Pushed the auth refactor; PR up."},
	{roomID: "r-eng", author: "alice", content: "Should we adopt UUIDv7 for all entity IDs?"},
	{roomID: "r-eng", author: "bob", content: "Quick reminder to rebase before merge."},
	{roomID: "r-eng", author: "ivan", content: "Remote side is green on the latest build."},

	// r-design
	{roomID: "r-design", author: "frank", content: "Posted v2 mocks in the design folder."},
	{roomID: "r-design", author: "grace", content: "Looks great — minor spacing notes inline."},
	{roomID: "r-design", author: "dave", content: "Thanks both, will share with stakeholders."},

	// alice-bob DM
	{roomID: dmIDOf("alice", "bob"), author: "alice", content: "Lunch?"},
	{roomID: dmIDOf("alice", "bob"), author: "bob", content: "Sure, 12:30"},
	{roomID: dmIDOf("alice", "bob"), author: "alice", content: "Perfect"},

	// carol-eve DM
	{roomID: dmIDOf("carol", "eve"), author: "carol", content: "Slides ready for the demo"},
	{roomID: dmIDOf("carol", "eve"), author: "eve", content: "Ship it"},

	// r-remote-announce
	{roomID: "r-remote-announce", author: "ivan", content: "Remote site weekly: highlights below."},
	{roomID: "r-remote-announce", author: "judy", content: "Product update: Q2 roadmap finalized."},
	{roomID: "r-remote-announce", author: "alice", content: "Thanks for the cross-site visibility."},
}

// dmIDOf returns the BuildDMRoomID for two seed accounts.
func dmIDOf(a, b string) string {
	users := usersByAccount()
	return idgen.BuildDMRoomID(users[a].ID, users[b].ID)
}

// threadParentIndex is the index in seedScript of the alice "UUIDv7?" message.
// Kept as a function so a slice reorder breaks the build, not silently behavior.
func threadParentIndex() int {
	for i, m := range seedScript {
		if m.roomID == "r-eng" && m.author == "alice" {
			return i
		}
	}
	panic("seed-sample-data: r-eng alice thread-parent message not found in seedScript")
}

// BuildMessages materializes every message (root + thread replies). Per-room
// timestamps step monotonically from seedBaseTime + 1h by messageStep.
// Message IDs are derived via idgen.MessageIDFromRequestID for idempotency.
func BuildMessages() []model.Message {
	users := usersByAccount()

	// Compute deterministic IDs for root messages first so thread replies
	// can reference the parent's ID.
	rootStart := seedBaseTime.Add(1 * time.Hour)
	perRoomIdx := map[string]int{}

	// Two passes: pass 1 builds roots; pass 2 builds thread replies.
	roots := make([]model.Message, 0, len(seedScript))
	for _, s := range seedScript {
		idx := perRoomIdx[s.roomID]
		perRoomIdx[s.roomID] = idx + 1
		author := users[s.author]
		id := idgen.MessageIDFromRequestID("seed:"+s.roomID, itoa(idx))
		roots = append(roots, model.Message{
			ID:          id,
			RoomID:      s.roomID,
			UserID:      author.ID,
			UserAccount: author.Account,
			Content:     s.content,
			CreatedAt:   rootStart.Add(time.Duration(idx) * messageStep),
		})
	}

	threadParentID := roots[threadParentIndex()].ID
	threadParentCreatedAt := roots[threadParentIndex()].CreatedAt

	// Thread replies live in r-eng's timeline AFTER all roots, so the
	// per-room monotonic invariant holds.
	threadReplies := []struct {
		author  string
		content string
	}{
		{"bob", "+1, current 32-char hex is fine but v7 sort-ability is nice."},
		{"carol", "Subscriptions already use v7; channel rooms are still base62."},
		{"bob", "Let's draft a migration note."},
	}
	replies := make([]model.Message, 0, len(threadReplies))
	engRootCount := perRoomIdx["r-eng"]
	for i, tr := range threadReplies {
		idx := engRootCount + i
		author := users[tr.author]
		id := idgen.MessageIDFromRequestID("seed:r-eng", itoa(idx))
		created := rootStart.Add(time.Duration(idx) * messageStep)
		replies = append(replies, model.Message{
			ID:                           id,
			RoomID:                       "r-eng",
			UserID:                       author.ID,
			UserAccount:                  author.Account,
			Content:                      tr.content,
			CreatedAt:                    created,
			ThreadParentMessageID:        threadParentID,
			ThreadParentMessageCreatedAt: ptrTime(threadParentCreatedAt),
			TShow:                        false,
		})
	}

	return append(roots, replies...)
}

// itoa is a tiny strconv.Itoa wrapper kept local so the import list above
// stays lean — fixtures.go doesn't otherwise need strconv.
func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{digits[i%10]}, b...)
		i /= 10
	}
	return string(b)
}

func ptrTime(t time.Time) *time.Time { return &t }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./tools/seed-sample-data/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/seed-sample-data/
git commit -m "feat(seed-sample-data): messages fixture (23 including thread replies)"
```

---

## Task 8: Thread room + thread subscriptions

**Files:**
- Modify: `tools/seed-sample-data/fixtures.go`
- Modify: `tools/seed-sample-data/fixtures_test.go`

- [ ] **Step 1: Write failing tests**

Append to `fixtures_test.go`:

```go
func TestBuildThreadRooms_OneEntry(t *testing.T) {
	trs := BuildThreadRooms()
	require.Len(t, trs, 1)

	tr := trs[0]
	assert.Equal(t, "tr-uuidv7-debate", tr.ID)
	assert.Equal(t, "r-eng", tr.RoomID)
	assert.Equal(t, siteLocal, tr.SiteID)
	assert.NotEmpty(t, tr.ParentMessageID, "must reference the parent message ID")
	assert.NotEmpty(t, tr.LastMsgID)
	assert.False(t, tr.LastMsgAt.IsZero())
	assert.ElementsMatch(t, []string{"bob", "carol"}, tr.ReplyAccounts)
}

func TestBuildThreadRooms_ParentMessageExistsInBuildMessages(t *testing.T) {
	tr := BuildThreadRooms()[0]
	for _, m := range BuildMessages() {
		if m.ID == tr.ParentMessageID {
			return
		}
	}
	t.Fatalf("thread parent message ID %q is not in BuildMessages()", tr.ParentMessageID)
}

func TestBuildThreadSubscriptions_BobAndCarol(t *testing.T) {
	subs := BuildThreadSubscriptions()
	require.Len(t, subs, 2)
	got := []string{subs[0].UserAccount, subs[1].UserAccount}
	assert.ElementsMatch(t, []string{"bob", "carol"}, got)

	for _, s := range subs {
		assert.Equal(t, "tr-uuidv7-debate", s.ThreadRoomID)
		assert.Equal(t, "r-eng", s.RoomID)
		assert.Equal(t, siteLocal, s.SiteID)
		assert.NotEmpty(t, s.ParentMessageID)
		assert.False(t, s.CreatedAt.IsZero())
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./tools/seed-sample-data/`
Expected: FAIL — undefined: `BuildThreadRooms`, `BuildThreadSubscriptions`.

- [ ] **Step 3: Implement thread builders**

Append to `fixtures.go`:

```go
// threadParentMessage returns the (id, createdAt) of the alice "UUIDv7?"
// message in r-eng. Centralized so BuildThreadRooms and BuildMessages
// stay in lockstep.
func threadParentMessage() (id string, createdAt time.Time) {
	msgs := BuildMessages()
	for _, m := range msgs {
		if m.RoomID == "r-eng" && m.UserAccount == "alice" && m.ThreadParentMessageID == "" {
			return m.ID, m.CreatedAt
		}
	}
	panic("seed-sample-data: r-eng alice thread-parent message not found in BuildMessages")
}

// threadLastMessage returns the (id, createdAt) of the latest thread reply
// in r-eng — used to populate ThreadRoom.LastMsg{ID,At}.
func threadLastMessage() (id string, createdAt time.Time) {
	var latest model.Message
	for _, m := range BuildMessages() {
		if m.ThreadParentMessageID == "" || m.RoomID != "r-eng" {
			continue
		}
		if latest.ID == "" || m.CreatedAt.After(latest.CreatedAt) {
			latest = m
		}
	}
	if latest.ID == "" {
		panic("seed-sample-data: no thread replies found in BuildMessages")
	}
	return latest.ID, latest.CreatedAt
}

// BuildThreadRooms returns the seed thread set: one entry under the
// alice "UUIDv7?" message in r-eng with bob + carol as repliers.
func BuildThreadRooms() []model.ThreadRoom {
	parentID, parentCreated := threadParentMessage()
	lastID, lastAt := threadLastMessage()
	return []model.ThreadRoom{
		{
			ID:                    "tr-uuidv7-debate",
			ParentMessageID:       parentID,
			ThreadParentCreatedAt: parentCreated,
			RoomID:                "r-eng",
			SiteID:                siteLocal,
			LastMsgAt:             lastAt,
			LastMsgID:             lastID,
			ReplyAccounts:         []string{"bob", "carol"},
			CreatedAt:             seedBaseTime,
			UpdatedAt:             seedBaseTime,
		},
	}
}

// BuildThreadSubscriptions returns the seed thread subscriptions for bob
// and carol on the single seeded thread.
func BuildThreadSubscriptions() []model.ThreadSubscription {
	users := usersByAccount()
	parentID, _ := threadParentMessage()
	out := make([]model.ThreadSubscription, 0, 2)
	for _, account := range []string{"bob", "carol"} {
		u := users[account]
		out = append(out, model.ThreadSubscription{
			ID:              "tsub:" + u.ID + ":tr-uuidv7-debate",
			ParentMessageID: parentID,
			RoomID:          "r-eng",
			ThreadRoomID:    "tr-uuidv7-debate",
			UserID:          u.ID,
			UserAccount:     u.Account,
			SiteID:          siteLocal,
			LastSeenAt:      nil,
			HasMention:      false,
			CreatedAt:       seedBaseTime,
			UpdatedAt:       seedBaseTime,
		})
	}
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./tools/seed-sample-data/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/seed-sample-data/
git commit -m "feat(seed-sample-data): thread_rooms + thread_subscriptions fixture"
```

---

## Task 9: Rooms LastMsg backfill + cross-collection consistency tests

**Files:**
- Modify: `tools/seed-sample-data/fixtures.go`
- Modify: `tools/seed-sample-data/fixtures_test.go`

- [ ] **Step 1: Write failing test for `BuildRoomsWithLastMsg`**

Append to `fixtures_test.go`:

```go
func TestBuildRoomsWithLastMsg_PopulatesLastMessageFields(t *testing.T) {
	rooms := BuildRoomsWithLastMsg()
	require.Len(t, rooms, 6)

	// For every room, LastMsgID must match the latest BuildMessages entry
	// in that room (root or thread reply — whichever has the latest CreatedAt).
	latest := map[string]model.Message{}
	for _, m := range BuildMessages() {
		l, ok := latest[m.RoomID]
		if !ok || m.CreatedAt.After(l.CreatedAt) {
			latest[m.RoomID] = m
		}
	}
	for _, r := range rooms {
		want, ok := latest[r.ID]
		require.True(t, ok, "no messages found for seeded room %s", r.ID)
		assert.Equal(t, want.ID, r.LastMsgID, "room %s LastMsgID", r.ID)
		require.NotNil(t, r.LastMsgAt, "room %s LastMsgAt should be set", r.ID)
		assert.True(t, r.LastMsgAt.Equal(want.CreatedAt), "room %s LastMsgAt", r.ID)
	}
}

func TestConsistency_AllSubscriptionsHaveValidRoom(t *testing.T) {
	rooms := roomsByID()
	for _, s := range BuildSubscriptions() {
		_, ok := rooms[s.RoomID]
		assert.True(t, ok, "subscription %s references unknown room %s", s.ID, s.RoomID)
	}
}

func TestConsistency_AllRoomMembersHaveValidUserAndRoom(t *testing.T) {
	users := usersByAccount()
	rooms := roomsByID()
	for _, m := range BuildRoomMembers() {
		_, ok := rooms[m.RoomID]
		assert.True(t, ok, "room_member %s references unknown room", m.ID)
		_, ok = users[m.Member.Account]
		assert.True(t, ok, "room_member %s references unknown account %s", m.ID, m.Member.Account)
	}
}

func TestConsistency_AllMessageAuthorsAreUsers(t *testing.T) {
	users := usersByAccount()
	for _, m := range BuildMessages() {
		_, ok := users[m.UserAccount]
		assert.True(t, ok, "message %s in %s authored by unknown account %s", m.ID, m.RoomID, m.UserAccount)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./tools/seed-sample-data/`
Expected: FAIL — undefined: `BuildRoomsWithLastMsg`.

- [ ] **Step 3: Implement `BuildRoomsWithLastMsg`**

Append to `fixtures.go`:

```go
// BuildRoomsWithLastMsg returns BuildRooms() but with each room's
// LastMsgAt/LastMsgID populated from the latest message in that room.
// This is what mongo.go upserts into the rooms collection — not the bare
// BuildRooms output, which carries zero LastMsg fields.
func BuildRoomsWithLastMsg() []model.Room {
	latest := map[string]model.Message{}
	for _, m := range BuildMessages() {
		l, ok := latest[m.RoomID]
		if !ok || m.CreatedAt.After(l.CreatedAt) {
			latest[m.RoomID] = m
		}
	}
	rooms := BuildRooms()
	for i := range rooms {
		if m, ok := latest[rooms[i].ID]; ok {
			created := m.CreatedAt
			rooms[i].LastMsgAt = &created
			rooms[i].LastMsgID = m.ID
		}
	}
	return rooms
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./tools/seed-sample-data/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/seed-sample-data/
git commit -m "feat(seed-sample-data): rooms LastMsg backfill + consistency tests"
```

---

## Task 10: Valkey fixture (room keys + search restricted-rooms cache)

**Files:**
- Modify: `tools/seed-sample-data/fixtures.go`
- Modify: `tools/seed-sample-data/fixtures_test.go`

- [ ] **Step 1: Write failing tests**

Append to `fixtures_test.go`:

```go
func TestBuildRoomKeys_OneKeyPerRoom(t *testing.T) {
	keys := BuildRoomKeys()
	require.Len(t, keys, 6, "one key per seeded room")
	seen := map[string]bool{}
	for _, k := range keys {
		assert.False(t, seen[k.RoomID], "duplicate room key for %s", k.RoomID)
		seen[k.RoomID] = true
		assert.Len(t, k.PrivateKey, 32, "AES-256 key must be 32 bytes")
	}
}

func TestBuildRoomKeys_DeterministicAcrossCalls(t *testing.T) {
	a := BuildRoomKeys()
	b := BuildRoomKeys()
	require.Equal(t, len(a), len(b))
	for i := range a {
		assert.Equal(t, a[i].RoomID, b[i].RoomID)
		assert.Equal(t, a[i].PrivateKey, b[i].PrivateKey, "room %s key must be stable across calls", a[i].RoomID)
	}
}

func TestBuildRestrictedCache_OneEntryPerEngMember(t *testing.T) {
	entries := BuildRestrictedCache()
	require.Len(t, entries, 4, "alice + bob + carol + ivan are r-eng members")
	accounts := make([]string, len(entries))
	for i, e := range entries {
		accounts[i] = e.Account
		assert.Contains(t, e.Rooms, "r-eng", "%s cache must list r-eng", e.Account)
		assert.Greater(t, e.Rooms["r-eng"], int64(0), "%s r-eng join ts must be positive ms", e.Account)
	}
	assert.ElementsMatch(t, []string{"alice", "bob", "carol", "ivan"}, accounts)
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./tools/seed-sample-data/`
Expected: FAIL — undefined: `BuildRoomKeys`, `BuildRestrictedCache`, etc.

- [ ] **Step 3: Implement Valkey fixture builders**

Append to `fixtures.go`. First, add the new imports at the top of the file (merge with existing):

```go
import (
	"crypto/sha256"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)
```

Then append:

```go
// RoomKeyEntry pairs a room ID with the 32-byte secret to write at
// room:{roomID}:key. The seeder derives the secret from sha256("seed-room-key:" + roomID)
// so re-runs always produce the same key material.
type RoomKeyEntry struct {
	RoomID  string
	KeyPair roomkeystore.RoomKeyPair
}

// BuildRoomKeys returns one stable 32-byte room key per seeded room.
// Order matches BuildRooms.
func BuildRoomKeys() []RoomKeyEntry {
	rooms := BuildRooms()
	out := make([]RoomKeyEntry, 0, len(rooms))
	for _, r := range rooms {
		sum := sha256.Sum256([]byte("seed-room-key:" + r.ID))
		key := make([]byte, 32)
		copy(key, sum[:])
		out = append(out, RoomKeyEntry{
			RoomID:  r.ID,
			KeyPair: roomkeystore.RoomKeyPair{PrivateKey: key},
		})
	}
	return out
}

// RestrictedCacheEntry is a single (account, rooms) entry to write at
// searchservice:restrictedrooms:<account>.
type RestrictedCacheEntry struct {
	Account string
	Rooms   map[string]int64 // roomID -> unix-millis of join
}

// BuildRestrictedCache emits one entry per member of every Restricted
// room in BuildRooms. Currently that's just r-eng (members: alice, bob,
// carol, ivan), so this returns four entries. The join timestamp is
// seedBaseTime in unix-millis so the cache content is deterministic.
func BuildRestrictedCache() []RestrictedCacheEntry {
	joinMs := seedBaseTime.UnixMilli()

	// Aggregate by account in case a future room expansion adds more
	// restricted rooms with overlapping membership.
	byAccount := map[string]map[string]int64{}
	for _, r := range BuildRooms() {
		if !r.Restricted {
			continue
		}
		for _, account := range r.Accounts {
			if _, ok := byAccount[account]; !ok {
				byAccount[account] = map[string]int64{}
			}
			byAccount[account][r.ID] = joinMs
		}
	}

	// Walk the seeded user roster so output ordering is deterministic.
	out := make([]RestrictedCacheEntry, 0, len(byAccount))
	for _, u := range BuildUsers() {
		rooms, ok := byAccount[u.Account]
		if !ok {
			continue
		}
		out = append(out, RestrictedCacheEntry{Account: u.Account, Rooms: rooms})
	}
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./tools/seed-sample-data/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/seed-sample-data/
git commit -m "feat(seed-sample-data): valkey fixtures (room keys + restricted cache)"
```

---

## Task 11: Mongo write helpers (`mongo.go`)

**Files:**
- Create: `tools/seed-sample-data/mongo.go`
- Create: `tools/seed-sample-data/mongo_test.go`

- [ ] **Step 1: Inspect available helpers**

Read `pkg/mongoutil/collection.go` and `pkg/mongoutil/bulk.go` so the Mongo write helpers reuse `BulkUpsertByID` instead of hand-rolling upsert plumbing. Key methods to use:

- `mongoutil.NewCollection[T](*mongo.Collection) *mongoutil.Collection[T]`
- `(*Collection[T]).BulkUpsertByID(ctx, items []T, idFn func(T) string) (*BulkResult, error)`
- `(*Collection[T]).BulkWrite(ctx, models)` for the `--reset` `DeleteMany` path

- [ ] **Step 2: Write failing test for `seedIDs` helper**

Create `tools/seed-sample-data/mongo_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSeedIDs_UsersMatchesBuildUsers(t *testing.T) {
	got := seedIDs(usersIDs())
	assert.Len(t, got, 10)
	assert.Contains(t, got, "u-alice")
	assert.Contains(t, got, "u-judy")
}

func TestSeedIDs_RoomsMatchesBuildRooms(t *testing.T) {
	got := seedIDs(roomIDs())
	assert.Len(t, got, 6)
	assert.Contains(t, got, "r-general")
	assert.Contains(t, got, "r-remote-announce")
}

func TestSeedIDs_AllCollectionsExposeIDs(t *testing.T) {
	// Each ID-extractor returns one entry per fixture row.
	assert.Len(t, usersIDs(), 10)
	assert.Len(t, roomIDs(), 6)
	assert.Len(t, subscriptionIDs(), 23)
	assert.Len(t, roomMemberIDs(), 19)
	assert.Len(t, messageIDs(), 23)
	assert.Len(t, threadRoomIDs(), 1)
	assert.Len(t, threadSubscriptionIDs(), 2)
}
```

- [ ] **Step 3: Run tests to verify failure**

Run: `go test ./tools/seed-sample-data/`
Expected: FAIL — undefined: `seedIDs`, `usersIDs`, etc.

- [ ] **Step 4: Implement `mongo.go`**

Create `tools/seed-sample-data/mongo.go`:

```go
// Package main: Mongo write helpers.
package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// seedIDs is a passthrough that exists so callers can express "the set
// of stable seed IDs for collection X" without exposing the internals.
func seedIDs(ids []string) []string { return ids }

func usersIDs() []string {
	out := make([]string, 0, 10)
	for _, u := range BuildUsers() {
		out = append(out, u.ID)
	}
	return out
}

func roomIDs() []string {
	out := make([]string, 0, 6)
	for _, r := range BuildRooms() {
		out = append(out, r.ID)
	}
	return out
}

func subscriptionIDs() []string {
	out := make([]string, 0, 23)
	for _, s := range BuildSubscriptions() {
		out = append(out, s.ID)
	}
	return out
}

func roomMemberIDs() []string {
	out := make([]string, 0, 19)
	for _, m := range BuildRoomMembers() {
		out = append(out, m.ID)
	}
	return out
}

func messageIDs() []string {
	out := make([]string, 0, 23)
	for _, m := range BuildMessages() {
		out = append(out, m.ID)
	}
	return out
}

func threadRoomIDs() []string {
	out := make([]string, 0, 1)
	for _, t := range BuildThreadRooms() {
		out = append(out, t.ID)
	}
	return out
}

func threadSubscriptionIDs() []string {
	out := make([]string, 0, 2)
	for _, t := range BuildThreadSubscriptions() {
		out = append(out, t.ID)
	}
	return out
}

// mongoCounts captures per-collection upsert counts for the final
// "seed complete" log line.
type mongoCounts struct {
	Users               int64
	Rooms               int64
	Subscriptions       int64
	RoomMembers         int64
	Messages            int64
	ThreadRooms         int64
	ThreadSubscriptions int64
}

// upsertAll writes every Mongo collection. Order does not matter for
// correctness — there are no foreign-key constraints in MongoDB — but
// users-first keeps the log nicely grouped by domain.
func upsertAll(ctx context.Context, db *mongo.Database) (mongoCounts, error) {
	var c mongoCounts

	users := mongoutil.NewCollection[model.User](db.Collection("users"))
	res, err := users.BulkUpsertByID(ctx, BuildUsers(), func(u model.User) string { return u.ID })
	if err != nil {
		return c, fmt.Errorf("seed users: %w", err)
	}
	c.Users = touched(res)

	rooms := mongoutil.NewCollection[model.Room](db.Collection("rooms"))
	res, err = rooms.BulkUpsertByID(ctx, BuildRoomsWithLastMsg(), func(r model.Room) string { return r.ID })
	if err != nil {
		return c, fmt.Errorf("seed rooms: %w", err)
	}
	c.Rooms = touched(res)

	subs := mongoutil.NewCollection[model.Subscription](db.Collection("subscriptions"))
	res, err = subs.BulkUpsertByID(ctx, BuildSubscriptions(), func(s model.Subscription) string { return s.ID })
	if err != nil {
		return c, fmt.Errorf("seed subscriptions: %w", err)
	}
	c.Subscriptions = touched(res)

	members := mongoutil.NewCollection[model.RoomMember](db.Collection("room_members"))
	res, err = members.BulkUpsertByID(ctx, BuildRoomMembers(), func(m model.RoomMember) string { return m.ID })
	if err != nil {
		return c, fmt.Errorf("seed room_members: %w", err)
	}
	c.RoomMembers = touched(res)

	msgs := mongoutil.NewCollection[model.Message](db.Collection("messages"))
	res, err = msgs.BulkUpsertByID(ctx, BuildMessages(), func(m model.Message) string { return m.ID })
	if err != nil {
		return c, fmt.Errorf("seed messages: %w", err)
	}
	c.Messages = touched(res)

	trs := mongoutil.NewCollection[model.ThreadRoom](db.Collection("thread_rooms"))
	res, err = trs.BulkUpsertByID(ctx, BuildThreadRooms(), func(t model.ThreadRoom) string { return t.ID })
	if err != nil {
		return c, fmt.Errorf("seed thread_rooms: %w", err)
	}
	c.ThreadRooms = touched(res)

	tsubs := mongoutil.NewCollection[model.ThreadSubscription](db.Collection("thread_subscriptions"))
	res, err = tsubs.BulkUpsertByID(ctx, BuildThreadSubscriptions(), func(t model.ThreadSubscription) string { return t.ID })
	if err != nil {
		return c, fmt.Errorf("seed thread_subscriptions: %w", err)
	}
	c.ThreadSubscriptions = touched(res)

	return c, nil
}

// touched returns the number of documents the bulk write affected
// (upserted + modified). Matched-but-unchanged documents (an exact re-run
// of the seeder) report 0 modified — that's the "no-op idempotent" path.
func touched(res *mongoutil.BulkResult) int64 {
	if res == nil {
		return 0
	}
	return res.Upserted + res.Modified
}

// deleteAll wipes only the seed records from each collection, identified
// by stable IDs. Never DROP — that would nuke hand-added dev data.
func deleteAll(ctx context.Context, db *mongo.Database) error {
	type del struct {
		name string
		ids  []string
	}
	tasks := []del{
		{"users", usersIDs()},
		{"rooms", roomIDs()},
		{"subscriptions", subscriptionIDs()},
		{"room_members", roomMemberIDs()},
		{"messages", messageIDs()},
		{"thread_rooms", threadRoomIDs()},
		{"thread_subscriptions", threadSubscriptionIDs()},
	}
	for _, t := range tasks {
		if len(t.ids) == 0 {
			continue
		}
		_, err := db.Collection(t.name).DeleteMany(ctx, bson.M{"_id": bson.M{"$in": t.ids}})
		if err != nil {
			return fmt.Errorf("reset %s: %w", t.name, err)
		}
	}
	return nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./tools/seed-sample-data/ -v`
Expected: PASS — `seedIDs` and ID-extractor tests green; new code compiles.

Run: `go build ./tools/seed-sample-data/`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add tools/seed-sample-data/
git commit -m "feat(seed-sample-data): Mongo upsert and reset helpers"
```

---

## Task 12: Valkey write helpers (`valkey.go`)

**Files:**
- Create: `tools/seed-sample-data/valkey.go`
- Create: `tools/seed-sample-data/valkey_test.go`

- [ ] **Step 1: Write failing tests for key/payload formatting**

Create `tools/seed-sample-data/valkey_test.go`:

```go
package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestrictedCacheKey_Format(t *testing.T) {
	assert.Equal(t, "searchservice:restrictedrooms:alice", restrictedCacheKey("alice"))
}

func TestRestrictedCachePayload_IsValidJSONOfRoomsMap(t *testing.T) {
	entry := RestrictedCacheEntry{
		Account: "alice",
		Rooms:   map[string]int64{"r-eng": 1700000000000},
	}
	payload, err := restrictedCachePayload(entry)
	require.NoError(t, err)

	var got map[string]int64
	require.NoError(t, json.Unmarshal([]byte(payload), &got))
	assert.Equal(t, map[string]int64{"r-eng": 1700000000000}, got)
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./tools/seed-sample-data/`
Expected: FAIL — undefined: `restrictedCacheKey`, `restrictedCachePayload`.

- [ ] **Step 3: Implement `valkey.go`**

Create `tools/seed-sample-data/valkey.go`:

```go
// Package main: Valkey write helpers.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// restrictedCacheTTL matches search-service's RESTRICTED_ROOMS_CACHE_TTL default.
const restrictedCacheTTL = 5 * time.Minute

func restrictedCacheKey(account string) string {
	return fmt.Sprintf("searchservice:restrictedrooms:%s", account)
}

func restrictedCachePayload(e RestrictedCacheEntry) (string, error) {
	rooms := e.Rooms
	if rooms == nil {
		rooms = map[string]int64{}
	}
	b, err := json.Marshal(rooms)
	if err != nil {
		return "", fmt.Errorf("marshal restricted cache entry: %w", err)
	}
	return string(b), nil
}

// valkeyCounts captures the number of Valkey writes for the summary log.
type valkeyCounts struct {
	RoomKeys     int
	CacheEntries int
}

// writeValkey writes every seeded room key (via roomkeystore.Set, which uses
// the exact production hash format) and every restricted-rooms cache entry
// (via valkeyutil.Client.Set with restrictedCacheTTL).
func writeValkey(ctx context.Context, keys roomkeystore.RoomKeyStore, client valkeyutil.Client) (valkeyCounts, error) {
	var c valkeyCounts
	for _, k := range BuildRoomKeys() {
		if _, err := keys.Set(ctx, k.RoomID, k.KeyPair); err != nil {
			return c, fmt.Errorf("set room key for %s: %w", k.RoomID, err)
		}
		c.RoomKeys++
	}
	for _, e := range BuildRestrictedCache() {
		payload, err := restrictedCachePayload(e)
		if err != nil {
			return c, fmt.Errorf("encode restricted cache for %s: %w", e.Account, err)
		}
		if err := client.Set(ctx, restrictedCacheKey(e.Account), payload, restrictedCacheTTL); err != nil {
			return c, fmt.Errorf("set restricted cache for %s: %w", e.Account, err)
		}
		c.CacheEntries++
	}
	return c, nil
}

// deleteValkey removes every key this seeder owns from Valkey.
// Order: room keys first (current + previous slot via roomkeystore.Delete),
// then cache entries via client.Del.
func deleteValkey(ctx context.Context, keys roomkeystore.RoomKeyStore, client valkeyutil.Client) error {
	for _, k := range BuildRoomKeys() {
		if err := keys.Delete(ctx, k.RoomID); err != nil {
			return fmt.Errorf("delete room key for %s: %w", k.RoomID, err)
		}
	}
	for _, e := range BuildRestrictedCache() {
		if err := client.Del(ctx, restrictedCacheKey(e.Account)); err != nil {
			return fmt.Errorf("delete restricted cache for %s: %w", e.Account, err)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests and build**

Run: `go test ./tools/seed-sample-data/ -v`
Expected: PASS.

Run: `go build ./tools/seed-sample-data/`
Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add tools/seed-sample-data/
git commit -m "feat(seed-sample-data): valkey upsert and reset helpers"
```

---

## Task 13: `main.go` — env config, flags, connect, dispatch

**Files:**
- Modify: `tools/seed-sample-data/main.go`
- Create: `tools/seed-sample-data/main_test.go`

- [ ] **Step 1: Write failing tests for env/flag parsing**

Create `tools/seed-sample-data/main_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConfig_Defaults(t *testing.T) {
	cfg, err := parseConfig(map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, "mongodb://localhost:27017", cfg.MongoURI)
	assert.Equal(t, "chat", cfg.MongoDB)
	assert.Equal(t, []string{"localhost:6379"}, cfg.ValkeyAddrs)
	assert.Empty(t, cfg.ValkeyPassword)
}

func TestParseConfig_OverridesFromEnv(t *testing.T) {
	cfg, err := parseConfig(map[string]string{
		"MONGO_URI":       "mongodb://example:27017",
		"MONGO_DB":        "altdb",
		"VALKEY_ADDRS":    "host1:6379,host2:6379",
		"VALKEY_PASSWORD": "s3cret",
	})
	require.NoError(t, err)
	assert.Equal(t, "mongodb://example:27017", cfg.MongoURI)
	assert.Equal(t, "altdb", cfg.MongoDB)
	assert.Equal(t, []string{"host1:6379", "host2:6379"}, cfg.ValkeyAddrs)
	assert.Equal(t, "s3cret", cfg.ValkeyPassword)
}

func TestDryRunSummary_HasAllRowCounts(t *testing.T) {
	got := dryRunSummary()
	// Each line is `<collection> <count>` for human-readable scanning.
	for _, want := range []string{
		"users 10",
		"rooms 6",
		"subscriptions 23",
		"room_members 19",
		"messages 23",
		"thread_rooms 1",
		"thread_subscriptions 2",
		"valkey:roomKeys 6",
		"valkey:restrictedCache 4",
	} {
		assert.Contains(t, got, want, "dry-run summary missing %q", want)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./tools/seed-sample-data/`
Expected: FAIL — undefined: `parseConfig`, `dryRunSummary`.

- [ ] **Step 3: Rewrite `main.go`**

Replace `tools/seed-sample-data/main.go` (overwrite the placeholder from Task 1):

```go
// Package main is the seed-sample-data CLI: populates MongoDB and Valkey
// with a small, well-formed, idempotent dataset for local development.
// Run via `make seed` after `make deps-up`.
//
// Flags:
//   (none)     idempotent populate
//   --reset    drop seed records then populate
//   --dry-run  print the plan and exit
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type config struct {
	MongoURI       string   `env:"MONGO_URI"       envDefault:"mongodb://localhost:27017"`
	MongoDB        string   `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername  string   `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword  string   `env:"MONGO_PASSWORD"  envDefault:""`
	ValkeyAddrs    []string `env:"VALKEY_ADDRS"    envDefault:"localhost:6379" envSeparator:","`
	ValkeyPassword string   `env:"VALKEY_PASSWORD" envDefault:""`
}

// parseConfig loads config from the supplied env map. Test seam — callers
// pass their own map so tests don't touch os.Environ. main() uses
// envFromOS() to produce the live map.
func parseConfig(envVars map[string]string) (config, error) {
	var cfg config
	if err := env.ParseWithOptions(&cfg, env.Options{Environment: envVars}); err != nil {
		return cfg, fmt.Errorf("parse env: %w", err)
	}
	return cfg, nil
}

func envFromOS() map[string]string {
	out := map[string]string{}
	for _, e := range os.Environ() {
		i := strings.IndexByte(e, '=')
		if i < 0 {
			continue
		}
		out[e[:i]] = e[i+1:]
	}
	return out
}

// dryRunSummary returns a multi-line human-readable plan: one line per
// collection plus the two valkey domains, in `<key> <count>` format.
func dryRunSummary() string {
	lines := []string{
		fmt.Sprintf("users %d", len(BuildUsers())),
		fmt.Sprintf("rooms %d", len(BuildRooms())),
		fmt.Sprintf("subscriptions %d", len(BuildSubscriptions())),
		fmt.Sprintf("room_members %d", len(BuildRoomMembers())),
		fmt.Sprintf("messages %d", len(BuildMessages())),
		fmt.Sprintf("thread_rooms %d", len(BuildThreadRooms())),
		fmt.Sprintf("thread_subscriptions %d", len(BuildThreadSubscriptions())),
		fmt.Sprintf("valkey:roomKeys %d", len(BuildRoomKeys())),
		fmt.Sprintf("valkey:restrictedCache %d", len(BuildRestrictedCache())),
	}
	return strings.Join(lines, "\n")
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	reset := flag.Bool("reset", false, "delete seed records before re-populating")
	dryRun := flag.Bool("dry-run", false, "print the plan and exit without writing")
	flag.Parse()

	if *dryRun {
		fmt.Println(dryRunSummary())
		return
	}

	cfg, err := parseConfig(envFromOS())
	if err != nil {
		slog.Error("config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		slog.Error("mongo connect", "error", err)
		os.Exit(1)
	}
	defer mongoutil.Disconnect(ctx, mongoClient)
	db := mongoClient.Database(cfg.MongoDB)

	keyStore, err := roomkeystore.NewValkeyClusterStore(roomkeystore.ClusterConfig{
		Addrs:       cfg.ValkeyAddrs,
		Password:    cfg.ValkeyPassword,
		GracePeriod: 5 * time.Minute,
	})
	if err != nil {
		slog.Error("valkey roomkeystore connect", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := keyStore.Close(); err != nil {
			slog.Warn("valkey roomkeystore close", "error", err)
		}
	}()

	valkeyClient, err := valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword)
	if err != nil {
		slog.Error("valkey client connect", "error", err)
		os.Exit(1)
	}
	defer valkeyutil.Disconnect(valkeyClient)

	if *reset {
		if err := deleteAll(ctx, db); err != nil {
			slog.Error("mongo reset", "error", err)
			os.Exit(1)
		}
		if err := deleteValkey(ctx, keyStore, valkeyClient); err != nil {
			slog.Error("valkey reset", "error", err)
			os.Exit(1)
		}
		slog.Info("seed reset complete")
	}

	mc, err := upsertAll(ctx, db)
	if err != nil {
		slog.Error("mongo upsert", "error", err)
		os.Exit(1)
	}

	vc, err := writeValkey(ctx, keyStore, valkeyClient)
	if err != nil {
		slog.Error("valkey write", "error", err)
		os.Exit(1)
	}

	slog.Info("seed complete",
		"users", mc.Users,
		"rooms", mc.Rooms,
		"subscriptions", mc.Subscriptions,
		"roomMembers", mc.RoomMembers,
		"messages", mc.Messages,
		"threadRooms", mc.ThreadRooms,
		"threadSubscriptions", mc.ThreadSubscriptions,
		"valkeyRoomKeys", vc.RoomKeys,
		"valkeyCacheEntries", vc.CacheEntries,
	)
}
```

- [ ] **Step 4: Run unit tests + build**

Run: `go test ./tools/seed-sample-data/ -v`
Expected: PASS — all flag/env tests green plus everything from prior tasks.

Run: `go build ./tools/seed-sample-data/`
Expected: exit 0.

- [ ] **Step 5: Verify `--dry-run` output by hand**

Run: `go run ./tools/seed-sample-data --dry-run`
Expected stdout (exact text):

```text
users 10
rooms 6
subscriptions 23
room_members 19
messages 23
thread_rooms 1
thread_subscriptions 2
valkey:roomKeys 6
valkey:restrictedCache 4
```

- [ ] **Step 6: Commit**

```bash
git add tools/seed-sample-data/
git commit -m "feat(seed-sample-data): main wiring with --reset and --dry-run"
```

---

## Task 14: Verify against the local stack

**Files:**
- No code changes. Verification + a follow-up commit only if a fix is needed.

- [ ] **Step 1: Confirm deps are up**

Run: `docker compose -f docker-local/compose.deps.yaml ps`
Expected: at minimum `chat-local-mongodb` and `chat-local-valkey` are `running` and `healthy`. If not, run `make deps-up`.

- [ ] **Step 2: Run the seeder against the live deps**

Run: `make seed`
Expected: a single `seed complete` JSON log line with all nine numeric fields populated:

```json
{"time":"...","level":"INFO","msg":"seed complete","users":10,"rooms":6,"subscriptions":23,"roomMembers":19,"messages":23,"threadRooms":1,"threadSubscriptions":2,"valkeyRoomKeys":6,"valkeyCacheEntries":4}
```

- [ ] **Step 3: Spot-check Mongo state**

Run (one container exec per query):

```bash
docker exec chat-local-mongodb mongosh chat --quiet --eval 'db.users.countDocuments({})'
docker exec chat-local-mongodb mongosh chat --quiet --eval 'db.rooms.countDocuments({})'
docker exec chat-local-mongodb mongosh chat --quiet --eval 'db.subscriptions.countDocuments({})'
docker exec chat-local-mongodb mongosh chat --quiet --eval 'db.room_members.countDocuments({})'
docker exec chat-local-mongodb mongosh chat --quiet --eval 'db.messages.countDocuments({})'
docker exec chat-local-mongodb mongosh chat --quiet --eval 'db.thread_rooms.countDocuments({})'
docker exec chat-local-mongodb mongosh chat --quiet --eval 'db.thread_subscriptions.countDocuments({})'
```

Expected: `10`, `6`, `23`, `19`, `23`, `1`, `2` (one per command).

- [ ] **Step 4: Spot-check Valkey state**

Run:

```bash
docker exec chat-local-valkey valkey-cli HGETALL 'room:{r-general}:key'
```

Expected: `priv`, `ver` fields with non-empty values (priv is a 44-char base64 string; ver is `0`).

Run:

```bash
docker exec chat-local-valkey valkey-cli GET 'searchservice:restrictedrooms:alice'
```

Expected: JSON map containing `r-eng`, e.g. `{"r-eng":1745917200000}`.

- [ ] **Step 5: Verify idempotency**

Run `make seed` a second time. Expected: identical `seed complete` line; no errors; no duplicated documents — re-run the `countDocuments` checks and confirm the same counts.

- [ ] **Step 6: Verify `--reset` works**

Run: `make seed-reset`
Then re-run the Mongo and Valkey spot-checks from Steps 3 and 4. Expected: same counts again (reset deletes then re-populates).

- [ ] **Step 7: Run the project test + lint gates**

Run:

```bash
make test SERVICE=tools/seed-sample-data
```

Expected: PASS, race detector clean.

Run:

```bash
make lint
```

Expected: PASS (no lint errors introduced by the new package).

- [ ] **Step 8: If any of the above fails, fix and commit**

If a spot-check fails, treat it as a bug in the seeder — diagnose, edit the relevant file, re-test, commit with a `fix(seed-sample-data): …` message. If everything passed on the first try, no commit is needed for this task.

---

## Task 15: Wrap-up — push the branch

**Files:**
- No code changes.

- [ ] **Step 1: Confirm working tree is clean**

Run: `git status`
Expected: `nothing to commit, working tree clean`.

- [ ] **Step 2: Push**

Run: `git push -u origin claude/sample-data-valkey-mongodb-FGADo`
Expected: branch updated on remote.

---

## Self-Review Notes

- **Spec coverage check:**
  - Single Go binary at `tools/seed-sample-data` — Task 1
  - Env vars (`MONGO_URI`, `MONGO_DB`, `VALKEY_ADDRS`) — Task 13
  - 10 users / 6 rooms / 23 subscriptions / 19 room_members / 23 messages / 1 thread room / 2 thread subscriptions — Tasks 3–9
  - Valkey room keys (6) + restricted cache entries (4) — Task 10
  - Idempotency via deterministic IDs + `BulkUpsertByID` + `HSET`/`SET` — Tasks 11–12, verified Task 14 Step 5
  - `--reset` deletes by seed IDs only (no `Drop`) — Task 11 `deleteAll`, Task 12 `deleteValkey`, verified Task 14 Step 6
  - `--dry-run` — Task 13, verified Step 5
  - Makefile targets — Task 1
  - Logging via `slog` JSON — Task 13 main
  - Error wrapping `fmt.Errorf("…: %w", err)` — all write helpers
  - Unit tests + ≥80% coverage on builders — Tasks 2–10
- **Placeholder scan:** none — every step shows exact code or exact commands.
- **Type consistency:** `RoomKeyEntry` defined in Task 10 used in Task 12; `RestrictedCacheEntry` defined in Task 10 used in Task 12; `mongoCounts` / `valkeyCounts` defined in Tasks 11/12 used in Task 13.
- **Sanity-check `--reset` semantics:** Valkey `roomkeystore.Delete` uses a `DEL currentKey prevKey` pipeline (`pkg/roomkeystore/adapter.go:76`) — DEL on absent keys is a no-op, so this is safe on a clean Valkey too.
