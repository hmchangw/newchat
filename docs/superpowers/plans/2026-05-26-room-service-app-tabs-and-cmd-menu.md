# Room-Service `app.tabs` + `app.cmd-menu` RPCs — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two read-only client-facing NATS RPCs to `room-service` — `GetRoomAppTabs` (lists default-channel-tab apps with per-room rewritten tab URLs) and `GetRoomAppCommandMenu` (lists active command-menu blocks for every bot subscribed to the room). Both gated by room membership OR platform-admin role.

**Architecture:** Two thin handlers in `room-service` + three new MongoDB store methods + a shared auth helper (`authorizeRoomAppRead`). URL rewrite preserves `SITE_URL` scheme/host/path-prefix and substitutes `${roomId}` / `${siteId}`. Response payload guarded by a per-handler bound sourced from `nc.MaxPayload()` and surfaced as a `errResponseTooLarge` sentinel. New `pkg/model` types: `User.Roles` + `IsPlatformAdmin`, `IsRoomMember`, `App.AvatarURL` + `App.ChannelTab`, `BotCmdMenu` / `CmdBlock` / `CmdModal`, and wire wrappers.

**Tech Stack:** Go 1.25 · NATS core (request/reply) · MongoDB driver v2 (Find + Aggregate, no JetStream) · `go.uber.org/mock` · `stretchr/testify`.

**Spec:** `docs/superpowers/specs/2026-05-26-room-service-app-tabs-and-cmd-menu-design.md` (commit `8662d05`).

---

## File map

| File | Action |
|------|--------|
| `pkg/subject/subject.go` | Add `RoomAppTabs`, `RoomAppTabsWildcard`, `RoomAppCmdMenu`, `RoomAppCmdMenuWildcard` |
| `pkg/subject/subject_test.go` | Add rows to `TestSubjectBuilders` |
| `pkg/model/user.go` | Add `Roles []string`, `PlatformRoleAdmin`, `IsPlatformAdmin` |
| `pkg/model/subscription.go` | Add `IsRoomMember` |
| `pkg/model/app.go` | Add `AvatarURL`, `ChannelTab`, `AppChannelTab`, `AppChannelTabURL`, `RoomApp`, `GetRoomAppTabsResponse`, `RoomAppAssistant`, `GetRoomAppCommandMenuResponse` |
| `pkg/model/botcmdmenu.go` | New file: `BotCmdMenu`, `CmdBlock`, `CmdModal` |
| `pkg/model/model_test.go` | Round-trip tests + `IsPlatformAdmin`/`IsRoomMember` unit tests |
| `room-service/store.go` | Add `RoomBotAppEntry`, `ListDefaultChannelTabApps`, `ListRoomBotApps`, `ListActiveCmdMenus` |
| `room-service/store_mongo.go` | Add `botCmdMenus` collection handle, three implementations, new compound indexes in `EnsureIndexes` |
| `room-service/integration_test.go` | Add `TestMongoStore_ListDefaultChannelTabApps`, `_ListRoomBotApps`, `_ListActiveCmdMenus`, `_ListActiveCmdMenus_Empty`, `_EnsureIndexes_NewCompoundIndexes` |
| `room-service/mock_store_test.go` | Regenerated via `make generate SERVICE=room-service` (don't hand-edit) |
| `room-service/helper.go` | Add `errAppAccessDenied`, `errResponseTooLarge`, `marshalBounded`, `replyBoundedJSON`; extend `sanitizeError` |
| `room-service/handler.go` | Add `siteURL`, `maxResponseBytes` fields; `authorizeRoomAppRead`, `buildTabURL`, `handleGetRoomAppTabs`, `natsGetRoomAppTabs`, `handleGetRoomAppCommandMenu`, `natsGetRoomAppCommandMenu`; extend `RegisterCRUD` |
| `room-service/handler_test.go` | Tests for `marshalBounded`, `authorizeRoomAppRead`, `buildTabURL`, both handlers |
| `room-service/main.go` | Add `SiteURL string` config field, validate at startup, pass `siteURL` and `nc.MaxPayload()` into `NewHandler` |
| `room-service/deploy/docker-compose.yml` | Add `SITE_URL` to the `room-service` env |
| `docs/client-api.md` | Document the two new RPCs under §3.1 (room-service) |

---

## Task 0: Add subject builders

**Files:**
- Modify: `pkg/subject/subject.go`
- Modify: `pkg/subject/subject_test.go`

- [ ] **Step 1: Write failing tests**

Append four rows to the `tests` table inside `TestSubjectBuilders` in `pkg/subject/subject_test.go` (the existing table starts around line 11; add inside it):

```go
{"RoomAppTabs", subject.RoomAppTabs("alice", "r1", "site-a"),
    "chat.user.alice.request.room.r1.site-a.app.tabs"},
{"RoomAppTabsWildcard", subject.RoomAppTabsWildcard("site-a"),
    "chat.user.*.request.room.*.site-a.app.tabs"},
{"RoomAppCmdMenu", subject.RoomAppCmdMenu("alice", "r1", "site-a"),
    "chat.user.alice.request.room.r1.site-a.app.cmd-menu"},
{"RoomAppCmdMenuWildcard", subject.RoomAppCmdMenuWildcard("site-a"),
    "chat.user.*.request.room.*.site-a.app.cmd-menu"},
```

Add two new standalone tests at the end of the file (after the table) covering the round-trip parse via the shared `ParseUserRoomSubject`:

```go
func TestRoomAppTabs_ParseUserRoomSubject(t *testing.T) {
    subj := subject.RoomAppTabs("alice", "r1", "site-a")
    account, roomID, ok := subject.ParseUserRoomSubject(subj)
    assert.True(t, ok)
    assert.Equal(t, "alice", account)
    assert.Equal(t, "r1", roomID)
}

func TestRoomAppCmdMenu_ParseUserRoomSubject(t *testing.T) {
    subj := subject.RoomAppCmdMenu("alice", "r1", "site-a")
    account, roomID, ok := subject.ParseUserRoomSubject(subj)
    assert.True(t, ok)
    assert.Equal(t, "alice", account)
    assert.Equal(t, "r1", roomID)
}
```

- [ ] **Step 2: Run tests — expect FAIL**

Run: `make test SERVICE=pkg/subject`

Expected: FAIL with "undefined: subject.RoomAppTabs" / "RoomAppTabsWildcard" / "RoomAppCmdMenu" / "RoomAppCmdMenuWildcard".

- [ ] **Step 3: Implement the builders**

Append to `pkg/subject/subject.go` (after `MuteToggleWildcard` around the existing room-scoped builders block):

```go
// RoomAppTabs returns the concrete subject for the GetRoomAppTabs RPC.
// Pair with RoomAppTabsWildcard for room-service's QueueSubscribe.
func RoomAppTabs(account, roomID, siteID string) string {
    return fmt.Sprintf("chat.user.%s.request.room.%s.%s.app.tabs", account, roomID, siteID)
}

// RoomAppTabsWildcard is the per-site subscription pattern for the
// GetRoomAppTabs RPC.
func RoomAppTabsWildcard(siteID string) string {
    return fmt.Sprintf("chat.user.*.request.room.*.%s.app.tabs", siteID)
}

// RoomAppCmdMenu returns the concrete subject for the
// GetRoomAppCommandMenu RPC.
func RoomAppCmdMenu(account, roomID, siteID string) string {
    return fmt.Sprintf("chat.user.%s.request.room.%s.%s.app.cmd-menu", account, roomID, siteID)
}

// RoomAppCmdMenuWildcard is the per-site subscription pattern for the
// GetRoomAppCommandMenu RPC.
func RoomAppCmdMenuWildcard(siteID string) string {
    return fmt.Sprintf("chat.user.*.request.room.*.%s.app.cmd-menu", siteID)
}
```

- [ ] **Step 4: Run tests — expect PASS**

Run: `make test SERVICE=pkg/subject`

Expected: PASS, all rows green.

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add RoomAppTabs and RoomAppCmdMenu builders"
```

---

## Task 1: Add `User.Roles` + `IsPlatformAdmin` to `pkg/model`

**Files:**
- Modify: `pkg/model/user.go`
- Modify: `pkg/model/model_test.go`

- [ ] **Step 1: Write failing tests**

Append to `pkg/model/model_test.go` (after the existing User tests around line 40):

```go
func TestUserJSON_WithRoles(t *testing.T) {
    u := model.User{
        ID:      "u1",
        Account: "alice",
        SiteID:  "site-a",
        Roles:   []string{"user", "admin"},
    }
    roundTrip(t, &u, &model.User{})
}

func TestUserJSON_RolesOmittedWhenEmpty(t *testing.T) {
    u := model.User{ID: "u1", Account: "alice", SiteID: "site-a"}
    data, err := json.Marshal(&u)
    require.NoError(t, err)
    var raw map[string]any
    require.NoError(t, json.Unmarshal(data, &raw))
    _, has := raw["roles"]
    assert.False(t, has, "nil Roles must be omitted from JSON")
}

func TestPlatformRoleAdminConstant(t *testing.T) {
    assert.Equal(t, "admin", model.PlatformRoleAdmin)
}

func TestIsPlatformAdmin(t *testing.T) {
    tests := []struct {
        name string
        u    *model.User
        want bool
    }{
        {"nil receiver", nil, false},
        {"nil roles", &model.User{Account: "alice"}, false},
        {"empty roles", &model.User{Account: "alice", Roles: []string{}}, false},
        {"user role only", &model.User{Account: "alice", Roles: []string{"user"}}, false},
        {"admin role present", &model.User{Account: "alice", Roles: []string{"admin"}}, true},
        {"admin among many", &model.User{Account: "alice", Roles: []string{"user", "admin", "auditor"}}, true},
        {"case-sensitive (Admin not admin)", &model.User{Account: "alice", Roles: []string{"Admin"}}, false},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            assert.Equal(t, tt.want, model.IsPlatformAdmin(tt.u))
        })
    }
}
```

- [ ] **Step 2: Run tests — expect FAIL**

Run: `make test SERVICE=pkg/model`

Expected: FAIL — undefined `model.PlatformRoleAdmin`, `model.IsPlatformAdmin`, missing `Roles` field on `User`.

- [ ] **Step 3: Implement**

Edit `pkg/model/user.go`. Replace the file contents with:

```go
package model

type User struct {
	ID          string   `json:"id"           bson:"_id"`
	Account     string   `json:"account"      bson:"account"`
	SiteID      string   `json:"siteId"       bson:"siteId"`
	SectID      string   `json:"sectId"       bson:"sectId"`
	SectName    string   `json:"sectName"     bson:"sectName"`
	SectTCName  string   `json:"sectTCName"   bson:"sectTCName"`
	DeptID      string   `json:"deptId"       bson:"deptId"`
	DeptName    string   `json:"deptName"     bson:"deptName"`
	DeptTCName  string   `json:"deptTCName"   bson:"deptTCName"`
	EngName     string   `json:"engName"      bson:"engName"`
	ChineseName string   `json:"chineseName"  bson:"chineseName"`
	EmployeeID  string   `json:"employeeId"   bson:"employeeId"`
	Roles       []string `json:"roles,omitempty" bson:"roles,omitempty"`
}

// PlatformRoleAdmin is the platform-admin role string carried in User.Roles.
const PlatformRoleAdmin = "admin"

// IsPlatformAdmin reports whether u holds the platform admin role.
// Returns false for nil receivers and for users without the role.
func IsPlatformAdmin(u *User) bool {
	if u == nil {
		return false
	}
	for _, r := range u.Roles {
		if r == PlatformRoleAdmin {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run tests — expect PASS**

Run: `make test SERVICE=pkg/model`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/user.go pkg/model/model_test.go
git commit -m "feat(model): add User.Roles and IsPlatformAdmin helper"
```

---

## Task 2: Add `IsRoomMember` helper to `pkg/model`

**Files:**
- Modify: `pkg/model/subscription.go`
- Modify: `pkg/model/model_test.go`

- [ ] **Step 1: Write failing test**

Append to `pkg/model/model_test.go`:

```go
func TestIsRoomMember(t *testing.T) {
    assert.False(t, model.IsRoomMember(nil), "nil sub is not a member")
    assert.True(t, model.IsRoomMember(&model.Subscription{RoomID: "r1"}), "non-nil sub is a member")
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `make test SERVICE=pkg/model`

Expected: FAIL — undefined `model.IsRoomMember`.

- [ ] **Step 3: Implement**

Append to `pkg/model/subscription.go` (after the existing types, before any blank-line EOF):

```go
// IsRoomMember reports whether sub represents an active membership.
// Returns false for nil so callers can pass the result of a store lookup
// that returned (nil, ErrSubscriptionNotFound) — the caller is expected
// to have already classified the error and set sub to nil on not-found.
func IsRoomMember(sub *Subscription) bool {
	return sub != nil
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `make test SERVICE=pkg/model`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/subscription.go pkg/model/model_test.go
git commit -m "feat(model): add IsRoomMember predicate"
```

---

## Task 3: Extend `App` with `AvatarURL` and `ChannelTab`

**Files:**
- Modify: `pkg/model/app.go`
- Modify: `pkg/model/model_test.go`

- [ ] **Step 1: Write failing tests**

Append to `pkg/model/model_test.go`:

```go
func TestAppRoundtrip_WithChannelTabAndAvatar(t *testing.T) {
    a := model.App{
        ID:        "app1",
        Name:      "Calendar",
        AvatarURL: "https://cdn.example.com/avatars/calendar.png",
        Assistant: &model.AppAssistant{Enabled: true, Name: "calendar.bot"},
        ChannelTab: &model.AppChannelTab{
            Enabled: true,
            Default: true,
            Name:    "Calendar",
            URL: model.AppChannelTabURL{
                Default: "https://upstream.example.com/calendar/${roomId}/${siteId}/index",
            },
        },
    }
    var dst model.App
    roundTrip(t, &a, &dst)
    require.NotNil(t, dst.ChannelTab)
    assert.True(t, dst.ChannelTab.Enabled)
    assert.True(t, dst.ChannelTab.Default)
    assert.Equal(t, "Calendar", dst.ChannelTab.Name)
    assert.Equal(t, "https://upstream.example.com/calendar/${roomId}/${siteId}/index",
        dst.ChannelTab.URL.Default)
    assert.Equal(t, "https://cdn.example.com/avatars/calendar.png", dst.AvatarURL)
}

func TestAppChannelTabRoundtrip(t *testing.T) {
    tab := model.AppChannelTab{
        Enabled: true,
        Default: false,
        Name:    "Notes",
        URL:     model.AppChannelTabURL{Default: "https://upstream/notes"},
    }
    var dst model.AppChannelTab
    roundTrip(t, &tab, &dst)
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `make test SERVICE=pkg/model`

Expected: FAIL — undefined `AppChannelTab`, `AppChannelTabURL`, missing fields on `App`.

- [ ] **Step 3: Implement**

Edit `pkg/model/app.go`. Replace the existing `App` struct and add the two new nested types. The full file should read:

```go
package model

// App is a read-only view of the apps collection (provisioning is upstream).
type App struct {
	ID          string         `json:"id"                    bson:"_id"`
	Name        string         `json:"name"                  bson:"name"`
	Description string         `json:"description,omitempty" bson:"description,omitempty"`
	AvatarURL   string         `json:"avatarUrl,omitempty"   bson:"avatarUrl,omitempty"`
	Assistant   *AppAssistant  `json:"assistant,omitempty"   bson:"assistant,omitempty"`
	ChannelTab  *AppChannelTab `json:"channelTab,omitempty"  bson:"channelTab,omitempty"`
	Sponsors    []AppSponsor   `json:"sponsors,omitempty"    bson:"sponsors,omitempty"`
}

// AppAssistant: Name is the bot user account (".bot" suffix); botDM requires Enabled==true.
type AppAssistant struct {
	Enabled     bool   `json:"enabled"               bson:"enabled"`
	Name        string `json:"name"                  bson:"name"`
	SettingsURL string `json:"settingsUrl,omitempty" bson:"settingsUrl,omitempty"`
}

// AppChannelTab describes a tab that can be embedded into channel rooms.
// Default==true marks tabs that appear by default in every channel.
type AppChannelTab struct {
	Enabled bool             `json:"enabled" bson:"enabled"`
	Default bool             `json:"default" bson:"default"`
	Name    string           `json:"name"    bson:"name"`
	URL     AppChannelTabURL `json:"url"     bson:"url"`
}

// AppChannelTabURL holds the URL template. Default is the canonical form
// with literal ${roomId} / ${siteId} placeholders that room-service
// substitutes when building per-room tab URLs.
type AppChannelTabURL struct {
	Default string `json:"default" bson:"default"`
}

type AppSponsor struct {
	Name  string `json:"name"  bson:"name"`
	Phone string `json:"phone" bson:"phone"`
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `make test SERVICE=pkg/model`

Expected: PASS (including the existing `TestAppRoundtrip` and `TestAppAssistantDisabledRoundtrip`, which must still pass — they don't set the new fields).

- [ ] **Step 5: Commit**

```bash
git add pkg/model/app.go pkg/model/model_test.go
git commit -m "feat(model): add App.AvatarURL and App.ChannelTab schema"
```

---

## Task 4: Add bot command-menu types

**Files:**
- Create: `pkg/model/botcmdmenu.go`
- Modify: `pkg/model/model_test.go`

- [ ] **Step 1: Write failing tests**

Append to `pkg/model/model_test.go`:

```go
func TestBotCmdMenuRoundtrip(t *testing.T) {
    m := model.BotCmdMenu{
        ID:           "bcm1",
        Name:         "weather.bot",
        ActiveStatus: true,
        CmdBlocks: []model.CmdBlock{
            {Text: "/weather", ActionType: "command", Payload: "weather"},
        },
    }
    var dst model.BotCmdMenu
    roundTrip(t, &m, &dst)
    require.Len(t, dst.CmdBlocks, 1)
    assert.Equal(t, "/weather", dst.CmdBlocks[0].Text)
}

func TestBotCmdMenuRoundtrip_Inactive(t *testing.T) {
    m := model.BotCmdMenu{
        ID:           "bcm2",
        Name:         "weather.bot",
        ActiveStatus: false,
    }
    var dst model.BotCmdMenu
    roundTrip(t, &m, &dst)
    assert.False(t, dst.ActiveStatus)
    assert.Nil(t, dst.CmdBlocks)
}

func TestCmdBlockRoundtrip_Recursive(t *testing.T) {
    block := model.CmdBlock{
        Text:        "menu",
        ActionType:  "open",
        Description: "open the menu",
        Modal: &model.CmdModal{
            Command: "menu.open",
            Param:   "weather",
        },
        Blocks: []model.CmdBlock{
            {Text: "today", Payload: "today"},
            {Text: "tomorrow", Payload: "tomorrow", Blocks: []model.CmdBlock{
                {Text: "morning", Payload: "tomorrow.am"},
            }},
        },
    }
    var dst model.CmdBlock
    roundTrip(t, &block, &dst)
    require.NotNil(t, dst.Modal)
    assert.Equal(t, "menu.open", dst.Modal.Command)
    assert.Equal(t, "weather", dst.Modal.Param)
    require.Len(t, dst.Blocks, 2)
    require.Len(t, dst.Blocks[1].Blocks, 1)
    assert.Equal(t, "morning", dst.Blocks[1].Blocks[0].Text)
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `make test SERVICE=pkg/model`

Expected: FAIL — undefined `model.BotCmdMenu`, `model.CmdBlock`, `model.CmdModal`.

- [ ] **Step 3: Implement — create new file**

Create `pkg/model/botcmdmenu.go`:

```go
package model

// BotCmdMenu is a row in the bot_cmd_menu collection. Name matches an
// AppAssistant.Name (the bot account) and joins back to the owning App
// via that field. ActiveStatus gates whether the menu is currently
// exposed to clients.
type BotCmdMenu struct {
	ID           string     `json:"id"           bson:"_id"`
	Name         string     `json:"name"         bson:"name"`
	ActiveStatus bool       `json:"activeStatus" bson:"activeStatus"`
	CmdBlocks    []CmdBlock `json:"cmdBlocks,omitempty" bson:"cmdBlocks,omitempty"`
}

// CmdBlock is the recursive building block of a bot command menu. A
// block either renders directly (Text+ActionType+Payload), opens a
// modal (Modal), or groups nested blocks (Blocks). Fields are
// optional so the schema can evolve without breaking the wire
// contract.
type CmdBlock struct {
	Text        string     `json:"text,omitempty"        bson:"text,omitempty"`
	ActionType  string     `json:"actionType,omitempty"  bson:"actionType,omitempty"`
	Description string     `json:"description,omitempty" bson:"description,omitempty"`
	Payload     string     `json:"payload,omitempty"     bson:"payload,omitempty"`
	Modal       *CmdModal  `json:"modal,omitempty"       bson:"modal,omitempty"`
	Blocks      []CmdBlock `json:"blocks,omitempty"      bson:"blocks,omitempty"`
}

// CmdModal carries the slash-style command + param a modal-triggering
// block invokes. CmdModal does NOT nest its own blocks — recursive
// rendering happens via CmdBlock.Blocks on the enclosing CmdBlock.
type CmdModal struct {
	Command string `json:"command,omitempty" bson:"command,omitempty"`
	Param   string `json:"param,omitempty"   bson:"param,omitempty"`
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `make test SERVICE=pkg/model`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/botcmdmenu.go pkg/model/model_test.go
git commit -m "feat(model): add BotCmdMenu, CmdBlock, CmdModal types"
```

---

## Task 5: Add wire-format response wrappers

**Files:**
- Modify: `pkg/model/app.go`
- Modify: `pkg/model/model_test.go`

- [ ] **Step 1: Write failing tests**

Append to `pkg/model/model_test.go`:

```go
func TestGetRoomAppTabsResponseRoundtrip(t *testing.T) {
    src := model.GetRoomAppTabsResponse{
        Apps: []model.RoomApp{
            {
                ID:        "app1",
                Name:      "Calendar",
                TabURL:    "https://chat.example.com/calendar/r1/site-a/index",
                Assistant: &model.AppAssistant{Enabled: true, Name: "cal.bot"},
                AvatarURL: "https://cdn/cal.png",
            },
        },
    }
    var dst model.GetRoomAppTabsResponse
    roundTrip(t, &src, &dst)
    require.Len(t, dst.Apps, 1)
    assert.Equal(t, "Calendar", dst.Apps[0].Name)
}

func TestGetRoomAppTabsResponse_EmptyIsArrayNotNull(t *testing.T) {
    src := model.GetRoomAppTabsResponse{Apps: []model.RoomApp{}}
    data, err := json.Marshal(&src)
    require.NoError(t, err)
    assert.Contains(t, string(data), `"apps":[]`)
    assert.NotContains(t, string(data), `"apps":null`)
}

func TestGetRoomAppCommandMenuResponseRoundtrip(t *testing.T) {
    src := model.GetRoomAppCommandMenuResponse{
        AppAssistants: []model.RoomAppAssistant{
            {
                AppName: "Weather Bot",
                Name:    "weather.bot",
                CmdBlocks: []model.CmdBlock{
                    {Text: "/forecast", Payload: "forecast"},
                },
            },
        },
    }
    var dst model.GetRoomAppCommandMenuResponse
    roundTrip(t, &src, &dst)
    require.Len(t, dst.AppAssistants, 1)
    assert.Equal(t, "weather.bot", dst.AppAssistants[0].Name)
}

func TestGetRoomAppCommandMenuResponse_EmptyIsArrayNotNull(t *testing.T) {
    src := model.GetRoomAppCommandMenuResponse{
        AppAssistants: []model.RoomAppAssistant{},
    }
    data, err := json.Marshal(&src)
    require.NoError(t, err)
    assert.Contains(t, string(data), `"appAssistants":[]`)
    assert.NotContains(t, string(data), `"appAssistants":null`)
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `make test SERVICE=pkg/model`

Expected: FAIL — undefined `RoomApp`, `RoomAppAssistant`, `GetRoomAppTabsResponse`, `GetRoomAppCommandMenuResponse`.

- [ ] **Step 3: Implement**

Append to `pkg/model/app.go` (after the existing types):

```go
// RoomApp is a single entry in GetRoomAppTabsResponse.Apps — derived
// from an apps document with the per-room tabUrl substituted in.
type RoomApp struct {
	ID        string        `json:"id"`
	Name      string        `json:"name"`        // = apps.channelTab.name
	TabURL    string        `json:"tabUrl"`      // computed (scheme+host+path-prefix from SITE_URL, ${roomId}/${siteId} substituted)
	Assistant *AppAssistant `json:"assistant,omitempty"`
	AvatarURL string        `json:"avatarUrl,omitempty"`
}

// GetRoomAppTabsResponse is the response body for the
// chat.user.{account}.request.room.{roomID}.{siteID}.app.tabs RPC.
type GetRoomAppTabsResponse struct {
	Apps []RoomApp `json:"apps"`
}

// RoomAppAssistant is a single entry in
// GetRoomAppCommandMenuResponse.AppAssistants.
type RoomAppAssistant struct {
	AppName   string     `json:"appName"`   // = apps.name
	Name      string     `json:"name"`      // = apps.assistant.name (bot account)
	CmdBlocks []CmdBlock `json:"cmdBlocks,omitempty"`
}

// GetRoomAppCommandMenuResponse is the response body for the
// chat.user.{account}.request.room.{roomID}.{siteID}.app.cmd-menu RPC.
type GetRoomAppCommandMenuResponse struct {
	AppAssistants []RoomAppAssistant `json:"appAssistants"`
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `make test SERVICE=pkg/model`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/app.go pkg/model/model_test.go
git commit -m "feat(model): add wire types for GetRoomAppTabs and GetRoomAppCommandMenu"
```

---

## Task 6: Extend `RoomStore` interface + regenerate mocks

**Files:**
- Modify: `room-service/store.go`
- Modify: `room-service/mock_store_test.go` (regenerated, do not hand-edit)

- [ ] **Step 1: Edit `room-service/store.go`**

Add the `RoomBotAppEntry` type near the other store-internal types (near `RoomCounts` / `ReadReceiptRow`), and add three new methods to the `RoomStore` interface. Insert after the existing read-side methods (e.g. after `GetApp`):

```go
// RoomBotAppEntry pairs an assistant's bot account with its owning
// app name — the joined output of ListRoomBotApps.
type RoomBotAppEntry struct {
	AssistantName string `bson:"assistantName"`
	AppName       string `bson:"appName"`
}
```

Append the following methods to the `RoomStore` interface body:

```go
// ListDefaultChannelTabApps returns apps whose channelTab.enabled AND
// channelTab.default are both true, sorted by channelTab.name asc.
// Projection: _id, avatarUrl, assistant, channelTab. Empty result is
// ([], nil).
ListDefaultChannelTabApps(ctx context.Context) ([]model.App, error)

// ListRoomBotApps returns one entry per bot subscribed to roomID,
// joined with the owning app via assistant.name == u.account. Only
// apps with assistant.enabled=true are emitted. Empty result is
// ([], nil); result order is assistantName asc.
ListRoomBotApps(ctx context.Context, roomID string) ([]RoomBotAppEntry, error)

// ListActiveCmdMenus returns bot_cmd_menu documents where
// activeStatus is true AND name IN assistantNames, sorted by name asc.
// Returns ([], nil) when assistantNames is empty (skips the query).
ListActiveCmdMenus(ctx context.Context, assistantNames []string) ([]model.BotCmdMenu, error)
```

- [ ] **Step 2: Regenerate mocks**

Run: `make generate SERVICE=room-service`

Expected: `room-service/mock_store_test.go` is rewritten with the three new methods. Do NOT hand-edit; if regen fails, fix the interface in `store.go` and re-run.

- [ ] **Step 3: Confirm package compiles**

Run: `make test SERVICE=room-service`

Expected: ALL EXISTING TESTS STILL PASS. (The new methods aren't called yet; this confirms the regen produced valid mock code and the interface is well-formed.) If a test fails because some helper still references the old mock surface, that's a regen problem — re-run `make generate SERVICE=room-service`.

- [ ] **Step 4: Commit**

```bash
git add room-service/store.go room-service/mock_store_test.go
git commit -m "feat(room-service): add RoomBotAppEntry and three new store methods"
```

---

## Task 7: Implement `ListDefaultChannelTabApps` + integration test

**Files:**
- Modify: `room-service/store_mongo.go`
- Modify: `room-service/integration_test.go`

- [ ] **Step 1: Write failing integration test**

Append to `room-service/integration_test.go`:

```go
func TestMongoStore_ListDefaultChannelTabApps(t *testing.T) {
    db := setupMongo(t)
    store := NewMongoStore(db)
    ctx := context.Background()

    apps := []any{
        bson.M{
            "_id":  "app-z", "name": "Zeta",
            "channelTab": bson.M{
                "enabled": true, "default": true, "name": "Zeta",
                "url": bson.M{"default": "https://upstream/z"},
            },
        },
        bson.M{
            "_id":  "app-a", "name": "Alpha",
            "channelTab": bson.M{
                "enabled": true, "default": true, "name": "Alpha",
                "url": bson.M{"default": "https://upstream/a"},
            },
        },
        bson.M{
            "_id":  "app-disabled", "name": "Disabled",
            "channelTab": bson.M{
                "enabled": false, "default": true, "name": "Disabled",
                "url": bson.M{"default": "https://upstream/d"},
            },
        },
        bson.M{"_id": "app-notabs", "name": "NoTabs"},
    }
    _, err := db.Collection("apps").InsertMany(ctx, apps)
    require.NoError(t, err)

    got, err := store.ListDefaultChannelTabApps(ctx)
    require.NoError(t, err)
    require.Len(t, got, 2)
    assert.Equal(t, "app-a", got[0].ID, "expected Alpha first by channelTab.name asc")
    assert.Equal(t, "app-z", got[1].ID)
    assert.Equal(t, "Alpha", got[0].ChannelTab.Name)
    // Projection excludes app.name (response uses channelTab.name).
    assert.Empty(t, got[0].Name)
}

func TestMongoStore_ListDefaultChannelTabApps_Empty(t *testing.T) {
    db := setupMongo(t)
    store := NewMongoStore(db)
    got, err := store.ListDefaultChannelTabApps(context.Background())
    require.NoError(t, err)
    assert.Empty(t, got)
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `make test-integration SERVICE=room-service`

Expected: FAIL — undefined method `ListDefaultChannelTabApps` on `*MongoStore`.

- [ ] **Step 3: Implement**

Append to `room-service/store_mongo.go` (after the existing read-side methods, before the bottom of the file):

```go
// ListDefaultChannelTabApps returns apps whose channelTab.enabled
// AND channelTab.default are both true, sorted by channelTab.name asc.
// Projection drops app.name (response uses channelTab.name).
func (s *MongoStore) ListDefaultChannelTabApps(ctx context.Context) ([]model.App, error) {
	opts := options.Find().
		SetSort(bson.D{{Key: "channelTab.name", Value: 1}}).
		SetProjection(bson.M{
			"_id":        1,
			"avatarUrl":  1,
			"assistant":  1,
			"channelTab": 1,
		})
	cursor, err := s.apps.Find(ctx, bson.M{
		"channelTab.enabled": true,
		"channelTab.default": true,
	}, opts)
	if err != nil {
		return nil, fmt.Errorf("list default channel-tab apps: %w", err)
	}
	defer cursor.Close(ctx)
	apps := make([]model.App, 0)
	if err := cursor.All(ctx, &apps); err != nil {
		return nil, fmt.Errorf("decode default channel-tab apps: %w", err)
	}
	return apps, nil
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `make test-integration SERVICE=room-service`

Expected: PASS for `TestMongoStore_ListDefaultChannelTabApps` and `..._Empty`.

- [ ] **Step 5: Commit**

```bash
git add room-service/store_mongo.go room-service/integration_test.go
git commit -m "feat(room-service): implement ListDefaultChannelTabApps"
```

---

## Task 8: Implement `ListRoomBotApps` aggregation + integration test

**Files:**
- Modify: `room-service/store_mongo.go`
- Modify: `room-service/integration_test.go`

- [ ] **Step 1: Write failing integration test**

Append to `room-service/integration_test.go`:

```go
func TestMongoStore_ListRoomBotApps(t *testing.T) {
    db := setupMongo(t)
    store := NewMongoStore(db)
    ctx := context.Background()

    // Apps: one enabled bot, one disabled, one without assistant.
    _, err := db.Collection("apps").InsertMany(ctx, []any{
        bson.M{"_id": "appA", "name": "Weather",
            "assistant": bson.M{"enabled": true, "name": "weather.bot"}},
        bson.M{"_id": "appB", "name": "Stocks",
            "assistant": bson.M{"enabled": false, "name": "stocks.bot"}},
        bson.M{"_id": "appC", "name": "NoBot"},
    })
    require.NoError(t, err)

    // Subscriptions: roomA has 1 bot (weather, enabled) + 1 disabled bot
    // (stocks, but assistant.enabled=false should drop it) + 1 human;
    // roomB has 1 different bot.
    _, err = db.Collection("subscriptions").InsertMany(ctx, []any{
        bson.M{"_id": "s1", "roomId": "roomA",
            "u": bson.M{"_id": "ub1", "account": "weather.bot", "isBot": true}},
        bson.M{"_id": "s2", "roomId": "roomA",
            "u": bson.M{"_id": "ub2", "account": "stocks.bot", "isBot": true}},
        bson.M{"_id": "s3", "roomId": "roomA",
            "u": bson.M{"_id": "uh1", "account": "alice", "isBot": false}},
        bson.M{"_id": "s4", "roomId": "roomB",
            "u": bson.M{"_id": "ub3", "account": "other.bot", "isBot": true}},
    })
    require.NoError(t, err)

    got, err := store.ListRoomBotApps(ctx, "roomA")
    require.NoError(t, err)
    require.Len(t, got, 1, "only enabled assistant should join in")
    assert.Equal(t, "weather.bot", got[0].AssistantName)
    assert.Equal(t, "Weather", got[0].AppName)
}

func TestMongoStore_ListRoomBotApps_Empty(t *testing.T) {
    db := setupMongo(t)
    store := NewMongoStore(db)
    got, err := store.ListRoomBotApps(context.Background(), "ghost-room")
    require.NoError(t, err)
    assert.Empty(t, got)
}

func TestMongoStore_ListRoomBotApps_UniqueIndexProtectsAgainstDupes(t *testing.T) {
    db := setupMongo(t)
    store := NewMongoStore(db)
    require.NoError(t, store.EnsureIndexes(context.Background()))
    ctx := context.Background()

    _, err := db.Collection("apps").InsertOne(ctx, bson.M{
        "_id": "appA", "name": "Weather",
        "assistant": bson.M{"enabled": true, "name": "weather.bot"},
    })
    require.NoError(t, err)
    _, err = db.Collection("subscriptions").InsertOne(ctx, bson.M{
        "_id": "s1", "roomId": "roomA",
        "u": bson.M{"_id": "ub1", "account": "weather.bot", "isBot": true},
    })
    require.NoError(t, err)
    // Duplicate (roomId, u.account) must be rejected by the unique index.
    _, err = db.Collection("subscriptions").InsertOne(ctx, bson.M{
        "_id": "s2", "roomId": "roomA",
        "u": bson.M{"_id": "ub1b", "account": "weather.bot", "isBot": true},
    })
    require.Error(t, err)
    assert.True(t, mongo.IsDuplicateKeyError(err), "expected duplicate-key error from (roomId, u.account) unique index")
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `make test-integration SERVICE=room-service`

Expected: FAIL — undefined `ListRoomBotApps`.

- [ ] **Step 3: Implement**

Append to `room-service/store_mongo.go`:

```go
// ListRoomBotApps returns one entry per bot subscribed to roomID,
// joined with the owning app via assistant.name == u.account. Drops
// apps whose assistant.enabled is false. Output is sorted by
// assistantName asc for deterministic test/client ordering.
func (s *MongoStore) ListRoomBotApps(ctx context.Context, roomID string) ([]RoomBotAppEntry, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"roomId": roomID, "u.isBot": true}}},
		{{Key: "$lookup", Value: bson.M{
			"from": "apps",
			"let":  bson.M{"acct": "$u.account"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$assistant.enabled", true}},
					bson.M{"$eq": bson.A{"$assistant.name", "$$acct"}},
				}}}},
				bson.M{"$project": bson.M{
					"_id":           0,
					"assistantName": "$assistant.name",
					"appName":       "$name",
				}},
			},
			"as": "app",
		}}},
		{{Key: "$unwind", Value: "$app"}},
		{{Key: "$replaceRoot", Value: bson.M{"newRoot": "$app"}}},
		{{Key: "$sort", Value: bson.D{{Key: "assistantName", Value: 1}}}},
	}
	cursor, err := s.subscriptions.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("list room bot apps for %q: %w", roomID, err)
	}
	defer cursor.Close(ctx)
	entries := make([]RoomBotAppEntry, 0)
	if err := cursor.All(ctx, &entries); err != nil {
		return nil, fmt.Errorf("decode room bot apps for %q: %w", roomID, err)
	}
	return entries, nil
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `make test-integration SERVICE=room-service`

Expected: PASS for the three new tests.

- [ ] **Step 5: Commit**

```bash
git add room-service/store_mongo.go room-service/integration_test.go
git commit -m "feat(room-service): implement ListRoomBotApps aggregation"
```

---

## Task 9: Implement `ListActiveCmdMenus` + integration test

**Files:**
- Modify: `room-service/store_mongo.go`
- Modify: `room-service/integration_test.go`

- [ ] **Step 1: Write failing integration test**

Append to `room-service/integration_test.go`:

```go
func TestMongoStore_ListActiveCmdMenus(t *testing.T) {
    db := setupMongo(t)
    store := NewMongoStore(db)
    ctx := context.Background()

    _, err := db.Collection("bot_cmd_menu").InsertMany(ctx, []any{
        bson.M{"_id": "m1", "name": "weather.bot", "activeStatus": true,
            "cmdBlocks": []bson.M{{"text": "/forecast"}}},
        bson.M{"_id": "m2", "name": "weather.bot", "activeStatus": false,
            "cmdBlocks": []bson.M{{"text": "/old"}}},
        bson.M{"_id": "m3", "name": "stocks.bot", "activeStatus": true,
            "cmdBlocks": []bson.M{{"text": "/quote"}}},
        bson.M{"_id": "m4", "name": "other.bot", "activeStatus": true,
            "cmdBlocks": []bson.M{{"text": "/x"}}},
    })
    require.NoError(t, err)

    got, err := store.ListActiveCmdMenus(ctx, []string{"weather.bot", "stocks.bot"})
    require.NoError(t, err)
    require.Len(t, got, 2, "expect only the two active matching rows")
    assert.Equal(t, "stocks.bot", got[0].Name, "sorted by name asc")
    assert.Equal(t, "weather.bot", got[1].Name)
    require.Len(t, got[1].CmdBlocks, 1)
    assert.Equal(t, "/forecast", got[1].CmdBlocks[0].Text)
    // Projection drops _id and activeStatus.
    assert.Empty(t, got[1].ID)
    assert.False(t, got[1].ActiveStatus)
}

func TestMongoStore_ListActiveCmdMenus_EmptyInput(t *testing.T) {
    db := setupMongo(t)
    store := NewMongoStore(db)
    got, err := store.ListActiveCmdMenus(context.Background(), nil)
    require.NoError(t, err)
    assert.Empty(t, got)
}

func TestMongoStore_ListActiveCmdMenus_NoMatches(t *testing.T) {
    db := setupMongo(t)
    store := NewMongoStore(db)
    got, err := store.ListActiveCmdMenus(context.Background(), []string{"unknown.bot"})
    require.NoError(t, err)
    assert.Empty(t, got)
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `make test-integration SERVICE=room-service`

Expected: FAIL — undefined `ListActiveCmdMenus`.

- [ ] **Step 3: Implement**

Add the `botCmdMenus` collection handle to `MongoStore`. Locate the struct and constructor near the top of `room-service/store_mongo.go`. Replace them with:

```go
type MongoStore struct {
	rooms               *mongo.Collection
	subscriptions       *mongo.Collection
	threadSubscriptions *mongo.Collection
	roomMembers         *mongo.Collection
	users               *mongo.Collection
	apps                *mongo.Collection
	botCmdMenus         *mongo.Collection
}

func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{
		rooms:               db.Collection("rooms"),
		subscriptions:       db.Collection("subscriptions"),
		threadSubscriptions: db.Collection("thread_subscriptions"),
		roomMembers:         db.Collection("room_members"),
		users:               db.Collection("users"),
		apps:                db.Collection("apps"),
		botCmdMenus:         db.Collection("bot_cmd_menu"),
	}
}
```

Then append the new method:

```go
// ListActiveCmdMenus returns bot_cmd_menu documents where
// activeStatus is true AND name IN assistantNames, sorted by name asc.
// Returns ([], nil) when assistantNames is empty (skips the query).
// Upstream invariant: at most one active row per name (out-of-scope
// to enforce via a partial unique index on the writer side).
func (s *MongoStore) ListActiveCmdMenus(ctx context.Context, assistantNames []string) ([]model.BotCmdMenu, error) {
	if len(assistantNames) == 0 {
		return []model.BotCmdMenu{}, nil
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "name", Value: 1}}).
		SetProjection(bson.M{
			"_id":       0,
			"name":      1,
			"cmdBlocks": 1,
		})
	cursor, err := s.botCmdMenus.Find(ctx, bson.M{
		"activeStatus": true,
		"name":         bson.M{"$in": assistantNames},
	}, opts)
	if err != nil {
		return nil, fmt.Errorf("list active cmd menus: %w", err)
	}
	defer cursor.Close(ctx)
	menus := make([]model.BotCmdMenu, 0)
	if err := cursor.All(ctx, &menus); err != nil {
		return nil, fmt.Errorf("decode active cmd menus: %w", err)
	}
	return menus, nil
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `make test-integration SERVICE=room-service`

Expected: PASS for the three new tests.

- [ ] **Step 5: Commit**

```bash
git add room-service/store_mongo.go room-service/integration_test.go
git commit -m "feat(room-service): implement ListActiveCmdMenus"
```

---

## Task 10: Add new compound indexes to `EnsureIndexes`

**Files:**
- Modify: `room-service/store_mongo.go`
- Modify: `room-service/integration_test.go`

- [ ] **Step 1: Write failing integration test**

Append to `room-service/integration_test.go`:

```go
func TestMongoStore_EnsureIndexes_NewCompoundIndexes(t *testing.T) {
    db := setupMongo(t)
    store := NewMongoStore(db)
    require.NoError(t, store.EnsureIndexes(context.Background()))

    type idxCheck struct {
        coll     string
        wantKeys bson.D
    }
    checks := []idxCheck{
        {"apps", bson.D{
            {Key: "channelTab.default", Value: int32(1)},
            {Key: "channelTab.enabled", Value: int32(1)},
            {Key: "channelTab.name", Value: int32(1)},
        }},
        {"subscriptions", bson.D{
            {Key: "roomId", Value: int32(1)},
            {Key: "u.isBot", Value: int32(1)},
        }},
        {"bot_cmd_menu", bson.D{
            {Key: "activeStatus", Value: int32(1)},
            {Key: "name", Value: int32(1)},
        }},
    }
    ctx := context.Background()
    for _, c := range checks {
        cursor, err := db.Collection(c.coll).Indexes().List(ctx)
        require.NoError(t, err)
        var idxes []bson.M
        require.NoError(t, cursor.All(ctx, &idxes))
        found := false
        for _, idx := range idxes {
            keys, ok := idx["key"].(bson.M)
            if !ok {
                continue
            }
            if len(keys) != len(c.wantKeys) {
                continue
            }
            match := true
            for _, kv := range c.wantKeys {
                if keys[kv.Key] != kv.Value {
                    match = false
                    break
                }
            }
            if match {
                found = true
                break
            }
        }
        assert.True(t, found, "expected index on %s with keys %v", c.coll, c.wantKeys)
    }
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `make test-integration SERVICE=room-service`

Expected: FAIL — the new indexes don't yet exist; one or more "expected index on …" assertions fail.

- [ ] **Step 3: Implement**

In `room-service/store_mongo.go`, edit `EnsureIndexes`. Insert the three new index creations near the existing apps index block (search for `assistant_name_idx`); append after the last existing CreateOne in the function but before `return nil`:

```go
	if _, err := s.apps.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "channelTab.default", Value: 1},
			{Key: "channelTab.enabled", Value: 1},
			{Key: "channelTab.name", Value: 1},
		},
	}); err != nil {
		return fmt.Errorf("ensure apps (channelTab.default,enabled,name) index: %w", err)
	}
	if _, err := s.subscriptions.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "roomId", Value: 1}, {Key: "u.isBot", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure subscriptions (roomId,u.isBot) index: %w", err)
	}
	if _, err := s.botCmdMenus.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "activeStatus", Value: 1}, {Key: "name", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure bot_cmd_menu (activeStatus,name) index: %w", err)
	}
```

- [ ] **Step 4: Run — expect PASS**

Run: `make test-integration SERVICE=room-service`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add room-service/store_mongo.go room-service/integration_test.go
git commit -m "feat(room-service): index apps/subscriptions/bot_cmd_menu for new reads"
```

---

## Task 11: Add error sentinels and extend `sanitizeError`

**Files:**
- Modify: `room-service/helper.go`

- [ ] **Step 1: Add sentinels**

In `room-service/helper.go`, locate the existing `var (...)` block that declares `errInvalidRole` etc. Append two new sentinels inside that block (keep the existing entries; show the last few lines for context):

```go
	errListLimitInvalid  = errors.New("limit must be > 0")
	errListOffsetInvalid = errors.New("offset must be >= 0")

	errAppAccessDenied  = errors.New("not authorized to access this room's apps")
	errResponseTooLarge = errors.New("response payload exceeds maximum size")
)
```

- [ ] **Step 2: Extend `sanitizeError`**

Locate the `sanitizeError` function. Inside the second `case errors.Is(err, ...)` block (the long `case`/`errors.Is` chain), add the two new sentinels — e.g. immediately before `errors.Is(err, &dmExistsError{})`:

```go
		errors.Is(err, errAppAccessDenied),
		errors.Is(err, errResponseTooLarge),
		errors.Is(err, &dmExistsError{}),
```

- [ ] **Step 3: Confirm compiles**

Run: `make test SERVICE=room-service`

Expected: PASS (existing tests still green; the new sentinels are unused for now, which is fine — Go doesn't error on unused package-level vars).

- [ ] **Step 4: Commit**

```bash
git add room-service/helper.go
git commit -m "feat(room-service): add errAppAccessDenied and errResponseTooLarge sentinels"
```

---

## Task 12: Add `marshalBounded` + `replyBoundedJSON` helpers

**Files:**
- Modify: `room-service/handler.go` (add `maxResponseBytes` field to `Handler`)
- Modify: `room-service/helper.go` (add helpers)
- Modify: `room-service/handler_test.go` (unit tests for `marshalBounded`)

- [ ] **Step 1: Write failing tests**

Append to `room-service/handler_test.go`:

```go
func TestHandler_marshalBounded(t *testing.T) {
    type sample struct {
        Hello string `json:"hello"`
    }
    big := sample{Hello: strings.Repeat("x", 200)}
    small := sample{Hello: "hi"}

    tests := []struct {
        name             string
        maxResponseBytes int64
        value            any
        wantBodyEmpty    bool
        wantErrMsg       string
    }{
        {"under cap", 1024, small, false, ""},
        {"over cap", 64, big, true, errResponseTooLarge.Error()},
        {"disabled zero", 0, big, false, ""},
        {"disabled negative", -1, big, false, ""},
        {"marshal failure", 1024, func() {}, true, "internal error"},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            h := &Handler{maxResponseBytes: tt.maxResponseBytes}
            body, errMsg := h.marshalBounded(tt.value)
            assert.Equal(t, tt.wantErrMsg, errMsg)
            if tt.wantBodyEmpty {
                assert.Nil(t, body)
            } else {
                assert.NotEmpty(t, body)
            }
        })
    }
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `make test SERVICE=room-service`

Expected: FAIL — `Handler` has no `maxResponseBytes` field; undefined `marshalBounded`.

- [ ] **Step 3: Add `maxResponseBytes` field to `Handler`**

In `room-service/handler.go`, edit the `Handler` struct definition (around line 31). Append a new field after `publishCore`:

```go
type Handler struct {
	store RoomStore
	// keyStore is set when VALKEY_ADDRS is configured (always in production; tests may pass nil).
	keyStore          RoomKeyStore
	memberListClient  MemberListClient
	msgReader         MessageReader
	siteID            string
	maxRoomSize       int
	maxBatchSize      int
	memberListTimeout time.Duration
	publishToStream   func(ctx context.Context, subj string, data []byte) error
	publishCore       func(ctx context.Context, subj string, data []byte) error
	siteURL           *url.URL
	maxResponseBytes  int64
}
```

Add the `net/url` import at the top of the file if not already present:

```go
import (
	// existing imports...
	"net/url"
	// rest...
)
```

(Do NOT yet wire `siteURL` / `maxResponseBytes` into `NewHandler` — that happens in Task 14.)

- [ ] **Step 4: Add helpers to `helper.go`**

Append to `room-service/helper.go`:

```go
// marshalBounded marshals v and enforces h.maxResponseBytes (a value
// <= 0 disables the bound). On success returns (body, ""); on marshal
// failure returns (nil, "internal error"); on size violation returns
// (nil, errResponseTooLarge.Error()).
func (h *Handler) marshalBounded(v any) ([]byte, string) {
	body, err := json.Marshal(v)
	if err != nil {
		slog.Error("marshal response failed", "error", err)
		return nil, "internal error"
	}
	if h.maxResponseBytes > 0 && int64(len(body)) > h.maxResponseBytes {
		slog.Error("response exceeds max payload",
			"size", len(body), "limit", h.maxResponseBytes)
		return nil, errResponseTooLarge.Error()
	}
	return body, ""
}

// replyBoundedJSON wraps marshalBounded with the NATS send. nats*
// handlers use this in place of natsutil.ReplyJSON when a response
// payload could exceed the negotiated NATS max_payload.
func (h *Handler) replyBoundedJSON(msg *nats.Msg, v any) {
	body, errMsg := h.marshalBounded(v)
	if errMsg != "" {
		natsutil.ReplyError(msg, errMsg)
		return
	}
	if err := msg.Respond(body); err != nil {
		slog.Error("reply failed", "error", err)
	}
}
```

Add the missing imports at the top of `helper.go`:

```go
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)
```

- [ ] **Step 5: Run — expect PASS**

Run: `make test SERVICE=room-service`

Expected: PASS for `TestHandler_marshalBounded` (all 5 subtests). Existing tests still green.

- [ ] **Step 6: Commit**

```bash
git add room-service/helper.go room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): add marshalBounded and replyBoundedJSON helpers"
```

---

## Task 13: Add `authorizeRoomAppRead` helper

**Files:**
- Modify: `room-service/handler.go`
- Modify: `room-service/handler_test.go`

- [ ] **Step 1: Write failing tests**

Append to `room-service/handler_test.go`:

```go
func TestHandler_authorizeRoomAppRead(t *testing.T) {
    tests := []struct {
        name      string
        setupMock func(*MockRoomStore)
        wantErr   error
    }{
        {
            name: "member allowed",
            setupMock: func(s *MockRoomStore) {
                s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
                    Return(&model.Subscription{
                        User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
                        RoomID: "r1",
                    }, nil)
            },
            wantErr: nil,
        },
        {
            name: "admin allowed (no sub, admin role)",
            setupMock: func(s *MockRoomStore) {
                s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
                    Return(nil, model.ErrSubscriptionNotFound)
                s.EXPECT().GetUser(gomock.Any(), "alice").
                    Return(&model.User{ID: "u1", Account: "alice", Roles: []string{"admin"}}, nil)
            },
            wantErr: nil,
        },
        {
            name: "denied: no sub, no admin role",
            setupMock: func(s *MockRoomStore) {
                s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
                    Return(nil, model.ErrSubscriptionNotFound)
                s.EXPECT().GetUser(gomock.Any(), "alice").
                    Return(&model.User{ID: "u1", Account: "alice", Roles: []string{"user"}}, nil)
            },
            wantErr: errAppAccessDenied,
        },
        {
            name: "denied: no sub, user not found (cross-site admin path)",
            setupMock: func(s *MockRoomStore) {
                s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
                    Return(nil, model.ErrSubscriptionNotFound)
                s.EXPECT().GetUser(gomock.Any(), "alice").
                    Return(nil, ErrUserNotFound)
            },
            wantErr: errAppAccessDenied,
        },
        {
            name: "transient sub-check error propagates",
            setupMock: func(s *MockRoomStore) {
                s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
                    Return(nil, errors.New("mongo unavailable"))
            },
            wantErr: nil, // checked separately below
        },
        {
            name: "transient user-lookup error propagates",
            setupMock: func(s *MockRoomStore) {
                s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
                    Return(nil, model.ErrSubscriptionNotFound)
                s.EXPECT().GetUser(gomock.Any(), "alice").
                    Return(nil, errors.New("mongo unavailable"))
            },
            wantErr: nil,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            ctrl := gomock.NewController(t)
            store := NewMockRoomStore(ctrl)
            tt.setupMock(store)
            h := &Handler{store: store, siteID: "site-a"}
            err := h.authorizeRoomAppRead(context.Background(), "alice", "r1")
            switch tt.name {
            case "transient sub-check error propagates":
                require.Error(t, err)
                assert.Contains(t, err.Error(), "check room membership")
            case "transient user-lookup error propagates":
                require.Error(t, err)
                assert.Contains(t, err.Error(), "check platform admin")
            default:
                if tt.wantErr == nil {
                    assert.NoError(t, err)
                } else {
                    assert.ErrorIs(t, err, tt.wantErr)
                }
            }
        })
    }
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `make test SERVICE=room-service`

Expected: FAIL — undefined method `authorizeRoomAppRead` on `*Handler`.

- [ ] **Step 3: Implement**

Append to `room-service/handler.go` (any sensible spot — e.g. just before `natsCreateRoom`):

```go
// authorizeRoomAppRead allows the request iff the caller has a
// subscription in roomID OR is a platform admin in the local users
// collection. Cross-site admin authority is out of scope: an admin
// whose users document lives on a different site is denied.
func (h *Handler) authorizeRoomAppRead(ctx context.Context, account, roomID string) error {
	sub, err := h.store.GetSubscription(ctx, account, roomID)
	if err != nil && !errors.Is(err, model.ErrSubscriptionNotFound) {
		return fmt.Errorf("check room membership: %w", err)
	}
	if model.IsRoomMember(sub) {
		return nil
	}
	user, err := h.store.GetUser(ctx, account)
	if err != nil && !errors.Is(err, ErrUserNotFound) {
		return fmt.Errorf("check platform admin: %w", err)
	}
	if model.IsPlatformAdmin(user) {
		return nil
	}
	return errAppAccessDenied
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `make test SERVICE=room-service`

Expected: PASS (all 6 subtests).

- [ ] **Step 5: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): add authorizeRoomAppRead shared auth helper"
```

---

## Task 14: Wire `siteURL` + `nc.MaxPayload()` into the service

**Files:**
- Modify: `room-service/main.go`
- Modify: `room-service/handler.go`

- [ ] **Step 1: Extend `Config` and add validation in `main.go`**

Edit `room-service/main.go`. Add `SiteURL` to the `config` struct (around line 22):

```go
type config struct {
	NatsURL           string          `env:"NATS_URL,required"`
	NatsCredsFile     string          `env:"NATS_CREDS_FILE"           envDefault:""`
	SiteID            string          `env:"SITE_ID"                   envDefault:"site-local"`
	SiteURL           string          `env:"SITE_URL,required"`
	// ...all existing fields...
}
```

Add the `net/url` import at the top of `main.go`.

Then, immediately after the `if cfg.MemberListTimeout <= 0` guard, insert SITE_URL validation:

```go
	siteURL, err := url.Parse(cfg.SiteURL)
	if err != nil || siteURL.Scheme == "" || siteURL.Host == "" {
		slog.Error("invalid SITE_URL: must be an absolute URL with scheme and host",
			"value", cfg.SiteURL, "error", err)
		os.Exit(1)
	}
```

- [ ] **Step 2: Update `NewHandler` signature**

Edit `room-service/handler.go`. Replace `NewHandler` (around line 45) with the extended signature:

```go
func NewHandler(
	store RoomStore,
	keyStore RoomKeyStore,
	memberListClient MemberListClient,
	msgReader MessageReader,
	siteID string,
	maxRoomSize, maxBatchSize int,
	memberListTimeout time.Duration,
	publishToStream func(context.Context, string, []byte) error,
	publishCore func(context.Context, string, []byte) error,
	siteURL *url.URL,
	maxResponseBytes int64,
) *Handler {
	return &Handler{
		store:             store,
		keyStore:          keyStore,
		memberListClient:  memberListClient,
		msgReader:         msgReader,
		siteID:            siteID,
		maxRoomSize:       maxRoomSize,
		maxBatchSize:      maxBatchSize,
		memberListTimeout: memberListTimeout,
		publishToStream:   publishToStream,
		publishCore:       publishCore,
		siteURL:           siteURL,
		maxResponseBytes:  maxResponseBytes,
	}
}
```

- [ ] **Step 3: Update the `NewHandler` call in `main.go`**

Find the existing `handler := NewHandler(...)` call (around line 122). Append two new arguments — `siteURL` and `nc.MaxPayload()` — at the end:

```go
	handler := NewHandler(store, keyStore, memberListClient, cassReader, cfg.SiteID, cfg.MaxRoomSize, cfg.MaxBatchSize, cfg.MemberListTimeout,
		func(ctx context.Context, subj string, data []byte) error {
			if _, err := js.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data)); err != nil {
				return fmt.Errorf("publish to %q: %w", subj, err)
			}
			return nil
		},
		func(ctx context.Context, subj string, data []byte) error {
			if err := nc.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data)); err != nil {
				return fmt.Errorf("publish core to %q: %w", subj, err)
			}
			return nil
		},
		siteURL,
		nc.NatsConn().MaxPayload(),
	)
```

Note: `nc` is `*otelnats.Conn`; the underlying NATS connection is accessed via `nc.NatsConn()`. `MaxPayload()` is a method on `*nats.Conn`.

- [ ] **Step 4: Build and confirm compiles**

Run: `make test SERVICE=room-service`

Expected: PASS. (Existing handler tests use `&Handler{...}` struct literals and don't go through `NewHandler`, so they're unaffected.)

- [ ] **Step 5: Commit**

```bash
git add room-service/main.go room-service/handler.go
git commit -m "feat(room-service): wire SITE_URL and NATS max-payload into the handler"
```

---

## Task 15: Implement `handleGetRoomAppTabs` (with URL rewrite)

**Files:**
- Modify: `room-service/handler.go`
- Modify: `room-service/handler_test.go`

- [ ] **Step 1: Write failing tests**

Append to `room-service/handler_test.go`:

```go
func newTabsTestHandler(t *testing.T, siteURL string) (*Handler, *MockRoomStore, *gomock.Controller) {
    t.Helper()
    ctrl := gomock.NewController(t)
    store := NewMockRoomStore(ctrl)
    u, err := url.Parse(siteURL)
    require.NoError(t, err)
    return &Handler{store: store, siteID: "site-a", siteURL: u}, store, ctrl
}

func mockTabApp(id, tabName, urlTemplate string) model.App {
    return model.App{
        ID:        id,
        AvatarURL: "https://cdn/" + id + ".png",
        Assistant: &model.AppAssistant{Enabled: true, Name: id + ".bot"},
        ChannelTab: &model.AppChannelTab{
            Enabled: true, Default: true, Name: tabName,
            URL: model.AppChannelTabURL{Default: urlTemplate},
        },
    }
}

func TestHandler_handleGetRoomAppTabs_MemberAllowed(t *testing.T) {
    h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1"}, nil)
    store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{
        mockTabApp("app1", "Calendar", "https://upstream/cal/${roomId}/${siteId}/index"),
    }, nil)

    subj := subject.RoomAppTabs("alice", "r1", "site-a")
    resp, err := h.handleGetRoomAppTabs(context.Background(), subj, nil)
    require.NoError(t, err)
    require.Len(t, resp.Apps, 1)
    assert.Equal(t, "app1", resp.Apps[0].ID)
    assert.Equal(t, "Calendar", resp.Apps[0].Name)
    assert.Equal(t, "https://chat.example.com/cal/r1/site-a/index", resp.Apps[0].TabURL)
    assert.Equal(t, "https://cdn/app1.png", resp.Apps[0].AvatarURL)
    require.NotNil(t, resp.Apps[0].Assistant)
    assert.Equal(t, "app1.bot", resp.Apps[0].Assistant.Name)
}

func TestHandler_handleGetRoomAppTabs_AdminAllowed(t *testing.T) {
    h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(nil, model.ErrSubscriptionNotFound)
    store.EXPECT().GetUser(gomock.Any(), "alice").
        Return(&model.User{Account: "alice", Roles: []string{"admin"}}, nil)
    store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{}, nil)

    subj := subject.RoomAppTabs("alice", "r1", "site-a")
    resp, err := h.handleGetRoomAppTabs(context.Background(), subj, nil)
    require.NoError(t, err)
    assert.Empty(t, resp.Apps)
}

func TestHandler_handleGetRoomAppTabs_Denied(t *testing.T) {
    h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(nil, model.ErrSubscriptionNotFound)
    store.EXPECT().GetUser(gomock.Any(), "alice").
        Return(&model.User{Account: "alice", Roles: []string{"user"}}, nil)

    subj := subject.RoomAppTabs("alice", "r1", "site-a")
    _, err := h.handleGetRoomAppTabs(context.Background(), subj, nil)
    assert.ErrorIs(t, err, errAppAccessDenied)
}

func TestHandler_handleGetRoomAppTabs_DeniedNoUser(t *testing.T) {
    h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(nil, model.ErrSubscriptionNotFound)
    store.EXPECT().GetUser(gomock.Any(), "alice").
        Return(nil, ErrUserNotFound)

    subj := subject.RoomAppTabs("alice", "r1", "site-a")
    _, err := h.handleGetRoomAppTabs(context.Background(), subj, nil)
    assert.ErrorIs(t, err, errAppAccessDenied)
}

func TestHandler_handleGetRoomAppTabs_EmptyResultIsEmptyArray(t *testing.T) {
    h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
    store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return(nil, nil)

    subj := subject.RoomAppTabs("alice", "r1", "site-a")
    resp, err := h.handleGetRoomAppTabs(context.Background(), subj, nil)
    require.NoError(t, err)
    assert.NotNil(t, resp.Apps, "must initialize empty slice, not nil, so JSON marshals to []")
    assert.Len(t, resp.Apps, 0)
    data, err := json.Marshal(resp)
    require.NoError(t, err)
    assert.Contains(t, string(data), `"apps":[]`)
}

func TestHandler_handleGetRoomAppTabs_URLRewritePathPrefix(t *testing.T) {
    h, store, _ := newTabsTestHandler(t, "https://chat.example.com/chat")
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
    store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{
        mockTabApp("app1", "Calendar", "https://upstream/tab/${roomId}"),
    }, nil)

    subj := subject.RoomAppTabs("alice", "r1", "site-a")
    resp, err := h.handleGetRoomAppTabs(context.Background(), subj, nil)
    require.NoError(t, err)
    assert.Equal(t, "https://chat.example.com/chat/tab/r1", resp.Apps[0].TabURL)
}

func TestHandler_handleGetRoomAppTabs_URLRewriteStripsUserinfo(t *testing.T) {
    h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
    store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{
        mockTabApp("app1", "X", "https://user:pass@upstream/path/${roomId}"),
    }, nil)

    subj := subject.RoomAppTabs("alice", "r1", "site-a")
    resp, err := h.handleGetRoomAppTabs(context.Background(), subj, nil)
    require.NoError(t, err)
    assert.NotContains(t, resp.Apps[0].TabURL, "user")
    assert.NotContains(t, resp.Apps[0].TabURL, "pass")
    assert.Equal(t, "https://chat.example.com/path/r1", resp.Apps[0].TabURL)
}

func TestHandler_handleGetRoomAppTabs_URLRewritePreservesQueryAndFragment(t *testing.T) {
    h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
    store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{
        mockTabApp("app1", "X", "https://upstream/path?room=${roomId}#tab=${siteId}"),
    }, nil)

    subj := subject.RoomAppTabs("alice", "r1", "site-a")
    resp, err := h.handleGetRoomAppTabs(context.Background(), subj, nil)
    require.NoError(t, err)
    assert.Equal(t, "https://chat.example.com/path?room=r1#tab=site-a", resp.Apps[0].TabURL)
}

func TestHandler_handleGetRoomAppTabs_URLRewriteSkipsEmptyAndMalformed(t *testing.T) {
    h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
    store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{
        mockTabApp("ok1", "OK1", "https://upstream/ok1/${roomId}"),
        mockTabApp("empty", "Empty", ""),
        mockTabApp("bad", "Bad", "://malformed"),
        mockTabApp("ok2", "OK2", "https://upstream/ok2/${roomId}"),
    }, nil)

    subj := subject.RoomAppTabs("alice", "r1", "site-a")
    resp, err := h.handleGetRoomAppTabs(context.Background(), subj, nil)
    require.NoError(t, err)
    require.Len(t, resp.Apps, 2, "empty and malformed must be skipped")
    assert.Equal(t, "ok1", resp.Apps[0].ID)
    assert.Equal(t, "ok2", resp.Apps[1].ID)
}

func TestHandler_handleGetRoomAppTabs_InvalidSubject(t *testing.T) {
    h, _, _ := newTabsTestHandler(t, "https://chat.example.com")
    _, err := h.handleGetRoomAppTabs(context.Background(), "not.a.valid.subject", nil)
    assert.Error(t, err)
}

func TestHandler_handleGetRoomAppTabs_StoreListError(t *testing.T) {
    h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
    store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).
        Return(nil, errors.New("mongo down"))

    subj := subject.RoomAppTabs("alice", "r1", "site-a")
    _, err := h.handleGetRoomAppTabs(context.Background(), subj, nil)
    require.Error(t, err)
    assert.Contains(t, err.Error(), "mongo down")
}

func TestHandler_handleGetRoomAppTabs_ContextTimeout(t *testing.T) {
    h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        DoAndReturn(func(ctx context.Context, _, _ string) (*model.Subscription, error) {
            <-ctx.Done()
            return nil, ctx.Err()
        })

    parent, cancel := context.WithCancel(context.Background())
    cancel()
    subj := subject.RoomAppTabs("alice", "r1", "site-a")
    _, err := h.handleGetRoomAppTabs(parent, subj, nil)
    require.Error(t, err)
    assert.True(t,
        errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
        "expected wrapped context error, got %v", err)
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `make test SERVICE=room-service`

Expected: FAIL — undefined `h.handleGetRoomAppTabs`.

- [ ] **Step 3: Implement `buildTabURL` and `handleGetRoomAppTabs`**

Append to `room-service/handler.go`:

```go
// buildTabURL applies the SITE_URL-based scheme/host/path-prefix
// rewrite and the ${roomId}/${siteId} substitution to a channelTab URL
// template. Returns (url, true) on success; (_, false) when the
// template is empty or unparseable (caller should skip + warn).
func (h *Handler) buildTabURL(tmpl, roomID, siteID string) (string, bool) {
	if tmpl == "" {
		return "", false
	}
	// Substitute BEFORE parsing so url.URL.String() doesn't percent-encode
	// the substituted values (roomID/siteID are URL-safe by construction).
	tmpl = strings.ReplaceAll(tmpl, "${roomId}", roomID)
	tmpl = strings.ReplaceAll(tmpl, "${siteId}", siteID)
	u, err := url.Parse(tmpl)
	if err != nil {
		return "", false
	}
	joined := h.siteURL.JoinPath(u.Path)
	joined.User = nil
	joined.RawQuery = u.RawQuery
	joined.Fragment = u.Fragment
	return joined.String(), true
}

// handleGetRoomAppTabs is the business logic for the
// chat.user.{account}.request.room.{roomID}.{siteID}.app.tabs RPC.
func (h *Handler) handleGetRoomAppTabs(ctx context.Context, subj string, _ []byte) (model.GetRoomAppTabsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return model.GetRoomAppTabsResponse{}, fmt.Errorf("invalid app-tabs subject: %s", subj)
	}

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("site.id", h.siteID),
			attribute.String("account", account),
		)
	}

	if err := h.authorizeRoomAppRead(ctx, account, roomID); err != nil {
		return model.GetRoomAppTabsResponse{}, err
	}

	apps, err := h.store.ListDefaultChannelTabApps(ctx)
	if err != nil {
		return model.GetRoomAppTabsResponse{}, fmt.Errorf("list default channel-tab apps: %w", err)
	}

	out := make([]model.RoomApp, 0, len(apps))
	for i := range apps {
		app := &apps[i]
		if app.ChannelTab == nil {
			slog.Warn("skipping app with nil ChannelTab", "appId", app.ID, "roomId", roomID)
			continue
		}
		tabURL, ok := h.buildTabURL(app.ChannelTab.URL.Default, roomID, h.siteID)
		if !ok {
			slog.Warn("skipping app with empty or unparseable channelTab url",
				"appId", app.ID, "roomId", roomID, "template", app.ChannelTab.URL.Default)
			continue
		}
		out = append(out, model.RoomApp{
			ID:        app.ID,
			Name:      app.ChannelTab.Name,
			TabURL:    tabURL,
			Assistant: app.Assistant,
			AvatarURL: app.AvatarURL,
		})
	}
	return model.GetRoomAppTabsResponse{Apps: out}, nil
}

// natsGetRoomAppTabs is the NATS-wrapping entry point for the app.tabs RPC.
func (h *Handler) natsGetRoomAppTabs(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleGetRoomAppTabs(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("get room app tabs failed", "error", err, "subject", m.Msg.Subject)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	h.replyBoundedJSON(m.Msg, resp)
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `make test SERVICE=room-service`

Expected: PASS for all `TestHandler_handleGetRoomAppTabs_*` subtests.

- [ ] **Step 5: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): implement handleGetRoomAppTabs with URL rewrite"
```

---

## Task 16: Implement `handleGetRoomAppCommandMenu`

**Files:**
- Modify: `room-service/handler.go`
- Modify: `room-service/handler_test.go`

- [ ] **Step 1: Write failing tests**

Append to `room-service/handler_test.go`:

```go
func newCmdMenuTestHandler(t *testing.T) (*Handler, *MockRoomStore) {
    t.Helper()
    ctrl := gomock.NewController(t)
    store := NewMockRoomStore(ctrl)
    return &Handler{store: store, siteID: "site-a"}, store
}

func TestHandler_handleGetRoomAppCommandMenu_MemberAllowed_NoBots(t *testing.T) {
    h, store := newCmdMenuTestHandler(t)
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
    store.EXPECT().ListRoomBotApps(gomock.Any(), "r1").Return([]RoomBotAppEntry{}, nil)
    // ListActiveCmdMenus must NOT be called.

    subj := subject.RoomAppCmdMenu("alice", "r1", "site-a")
    resp, err := h.handleGetRoomAppCommandMenu(context.Background(), subj, nil)
    require.NoError(t, err)
    assert.NotNil(t, resp.AppAssistants)
    assert.Len(t, resp.AppAssistants, 0)
    data, err := json.Marshal(resp)
    require.NoError(t, err)
    assert.Contains(t, string(data), `"appAssistants":[]`)
}

func TestHandler_handleGetRoomAppCommandMenu_MemberAllowed_WithMenus(t *testing.T) {
    h, store := newCmdMenuTestHandler(t)
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
    store.EXPECT().ListRoomBotApps(gomock.Any(), "r1").Return([]RoomBotAppEntry{
        {AssistantName: "stocks.bot", AppName: "Stocks"},
        {AssistantName: "weather.bot", AppName: "Weather"},
    }, nil)
    store.EXPECT().ListActiveCmdMenus(gomock.Any(), []string{"stocks.bot", "weather.bot"}).
        Return([]model.BotCmdMenu{
            {Name: "stocks.bot", CmdBlocks: []model.CmdBlock{{Text: "/quote"}}},
            {Name: "weather.bot", CmdBlocks: []model.CmdBlock{{Text: "/forecast"}}},
        }, nil)

    subj := subject.RoomAppCmdMenu("alice", "r1", "site-a")
    resp, err := h.handleGetRoomAppCommandMenu(context.Background(), subj, nil)
    require.NoError(t, err)
    require.Len(t, resp.AppAssistants, 2)
    assert.Equal(t, "Stocks", resp.AppAssistants[0].AppName)
    assert.Equal(t, "stocks.bot", resp.AppAssistants[0].Name)
    require.Len(t, resp.AppAssistants[0].CmdBlocks, 1)
    assert.Equal(t, "/quote", resp.AppAssistants[0].CmdBlocks[0].Text)
    assert.Equal(t, "Weather", resp.AppAssistants[1].AppName)
    assert.Equal(t, "/forecast", resp.AppAssistants[1].CmdBlocks[0].Text)
}

func TestHandler_handleGetRoomAppCommandMenu_MemberAllowed_BotWithoutMenu(t *testing.T) {
    h, store := newCmdMenuTestHandler(t)
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
    store.EXPECT().ListRoomBotApps(gomock.Any(), "r1").Return([]RoomBotAppEntry{
        {AssistantName: "silent.bot", AppName: "Silent"},
    }, nil)
    store.EXPECT().ListActiveCmdMenus(gomock.Any(), []string{"silent.bot"}).
        Return([]model.BotCmdMenu{}, nil)

    subj := subject.RoomAppCmdMenu("alice", "r1", "site-a")
    resp, err := h.handleGetRoomAppCommandMenu(context.Background(), subj, nil)
    require.NoError(t, err)
    require.Len(t, resp.AppAssistants, 1)
    assert.Equal(t, "Silent", resp.AppAssistants[0].AppName)
    assert.Equal(t, "silent.bot", resp.AppAssistants[0].Name)
    assert.Nil(t, resp.AppAssistants[0].CmdBlocks)
}

func TestHandler_handleGetRoomAppCommandMenu_AdminAllowed(t *testing.T) {
    h, store := newCmdMenuTestHandler(t)
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(nil, model.ErrSubscriptionNotFound)
    store.EXPECT().GetUser(gomock.Any(), "alice").
        Return(&model.User{Account: "alice", Roles: []string{"admin"}}, nil)
    store.EXPECT().ListRoomBotApps(gomock.Any(), "r1").Return([]RoomBotAppEntry{}, nil)

    subj := subject.RoomAppCmdMenu("alice", "r1", "site-a")
    resp, err := h.handleGetRoomAppCommandMenu(context.Background(), subj, nil)
    require.NoError(t, err)
    assert.Len(t, resp.AppAssistants, 0)
}

func TestHandler_handleGetRoomAppCommandMenu_Denied(t *testing.T) {
    h, store := newCmdMenuTestHandler(t)
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(nil, model.ErrSubscriptionNotFound)
    store.EXPECT().GetUser(gomock.Any(), "alice").
        Return(&model.User{Account: "alice"}, nil)

    subj := subject.RoomAppCmdMenu("alice", "r1", "site-a")
    _, err := h.handleGetRoomAppCommandMenu(context.Background(), subj, nil)
    assert.ErrorIs(t, err, errAppAccessDenied)
}

func TestHandler_handleGetRoomAppCommandMenu_InvalidSubject(t *testing.T) {
    h, _ := newCmdMenuTestHandler(t)
    _, err := h.handleGetRoomAppCommandMenu(context.Background(), "not.a.valid.subject", nil)
    assert.Error(t, err)
}

func TestHandler_handleGetRoomAppCommandMenu_StoreListRoomBotAppsError(t *testing.T) {
    h, store := newCmdMenuTestHandler(t)
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
    store.EXPECT().ListRoomBotApps(gomock.Any(), "r1").Return(nil, errors.New("mongo down"))

    subj := subject.RoomAppCmdMenu("alice", "r1", "site-a")
    _, err := h.handleGetRoomAppCommandMenu(context.Background(), subj, nil)
    require.Error(t, err)
    assert.Contains(t, err.Error(), "mongo down")
}

func TestHandler_handleGetRoomAppCommandMenu_StoreListActiveCmdMenusError(t *testing.T) {
    h, store := newCmdMenuTestHandler(t)
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
    store.EXPECT().ListRoomBotApps(gomock.Any(), "r1").Return([]RoomBotAppEntry{
        {AssistantName: "weather.bot", AppName: "Weather"},
    }, nil)
    store.EXPECT().ListActiveCmdMenus(gomock.Any(), []string{"weather.bot"}).
        Return(nil, errors.New("mongo down"))

    subj := subject.RoomAppCmdMenu("alice", "r1", "site-a")
    _, err := h.handleGetRoomAppCommandMenu(context.Background(), subj, nil)
    require.Error(t, err)
    assert.Contains(t, err.Error(), "mongo down")
}

func TestHandler_handleGetRoomAppCommandMenu_ContextTimeout(t *testing.T) {
    h, store := newCmdMenuTestHandler(t)
    store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
        DoAndReturn(func(ctx context.Context, _, _ string) (*model.Subscription, error) {
            <-ctx.Done()
            return nil, ctx.Err()
        })

    parent, cancel := context.WithCancel(context.Background())
    cancel()
    subj := subject.RoomAppCmdMenu("alice", "r1", "site-a")
    _, err := h.handleGetRoomAppCommandMenu(parent, subj, nil)
    require.Error(t, err)
    assert.True(t,
        errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
        "expected wrapped context error, got %v", err)
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `make test SERVICE=room-service`

Expected: FAIL — undefined `h.handleGetRoomAppCommandMenu`.

- [ ] **Step 3: Implement**

Append to `room-service/handler.go`:

```go
// handleGetRoomAppCommandMenu is the business logic for the
// chat.user.{account}.request.room.{roomID}.{siteID}.app.cmd-menu RPC.
func (h *Handler) handleGetRoomAppCommandMenu(ctx context.Context, subj string, _ []byte) (model.GetRoomAppCommandMenuResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return model.GetRoomAppCommandMenuResponse{}, fmt.Errorf("invalid app-cmd-menu subject: %s", subj)
	}

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("site.id", h.siteID),
			attribute.String("account", account),
		)
	}

	if err := h.authorizeRoomAppRead(ctx, account, roomID); err != nil {
		return model.GetRoomAppCommandMenuResponse{}, err
	}

	bots, err := h.store.ListRoomBotApps(ctx, roomID)
	if err != nil {
		return model.GetRoomAppCommandMenuResponse{}, fmt.Errorf("list room bot apps: %w", err)
	}
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attribute.Int("bot.count", len(bots)))
	}

	if len(bots) == 0 {
		return model.GetRoomAppCommandMenuResponse{
			AppAssistants: make([]model.RoomAppAssistant, 0),
		}, nil
	}

	names := make([]string, 0, len(bots))
	for _, b := range bots {
		names = append(names, b.AssistantName)
	}
	menus, err := h.store.ListActiveCmdMenus(ctx, names)
	if err != nil {
		return model.GetRoomAppCommandMenuResponse{}, fmt.Errorf("list active cmd menus: %w", err)
	}
	byName := make(map[string][]model.CmdBlock, len(menus))
	for _, m := range menus {
		byName[m.Name] = m.CmdBlocks
	}

	out := make([]model.RoomAppAssistant, 0, len(bots))
	for _, b := range bots {
		out = append(out, model.RoomAppAssistant{
			AppName:   b.AppName,
			Name:      b.AssistantName,
			CmdBlocks: byName[b.AssistantName],
		})
	}
	return model.GetRoomAppCommandMenuResponse{AppAssistants: out}, nil
}

// natsGetRoomAppCommandMenu is the NATS-wrapping entry point.
func (h *Handler) natsGetRoomAppCommandMenu(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleGetRoomAppCommandMenu(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("get room app cmd-menu failed", "error", err, "subject", m.Msg.Subject)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	h.replyBoundedJSON(m.Msg, resp)
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `make test SERVICE=room-service`

Expected: PASS for all `TestHandler_handleGetRoomAppCommandMenu_*` subtests.

- [ ] **Step 5: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): implement handleGetRoomAppCommandMenu"
```

---

## Task 17: Wire both handlers into `RegisterCRUD`

**Files:**
- Modify: `room-service/handler.go`

- [ ] **Step 1: Extend `RegisterCRUD`**

Edit `room-service/handler.go`. Locate `RegisterCRUD` (around line 66). Append two new subscriptions just before `return nil`:

```go
	if _, err := nc.QueueSubscribe(subject.RoomAppTabsWildcard(h.siteID), queue, h.natsGetRoomAppTabs); err != nil {
		return fmt.Errorf("subscribe app tabs: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.RoomAppCmdMenuWildcard(h.siteID), queue, h.natsGetRoomAppCommandMenu); err != nil {
		return fmt.Errorf("subscribe app cmd-menu: %w", err)
	}
	return nil
}
```

- [ ] **Step 2: Confirm compiles + tests pass**

Run: `make test SERVICE=room-service`

Expected: PASS.

Run: `make lint`

Expected: clean (no new lint errors).

- [ ] **Step 3: Commit**

```bash
git add room-service/handler.go
git commit -m "feat(room-service): register app.tabs and app.cmd-menu handlers"
```

---

## Task 18: Update `docs/client-api.md` and `docker-compose.yml`

**Files:**
- Modify: `docs/client-api.md`
- Modify: `room-service/deploy/docker-compose.yml`

- [ ] **Step 1: Inspect current docs for the format to mirror**

Read `docs/client-api.md` and find the `#### List Members` entry under `### 3.1 room-service`. The new entries below follow the same shape (Subject, Request body, Success response, Error response, Triggered events).

- [ ] **Step 2: Append the two new entries under §3.1 (room-service)**

Add the following sections after the last existing `### 3.1` entry (e.g. after `#### Mute Toggle`), or insert in alphabetical order if the section enforces one:

```markdown
#### Get Room App Tabs

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.app.tabs`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

##### Request body

Empty body (`{}` is tolerated). All inputs come from the subject.

##### Success response

| Field    | Type           | Notes |
|----------|----------------|-------|
| `apps`   | array<RoomApp> | Apps whose `channelTab.enabled` AND `channelTab.default` are both true, sorted by `channelTab.name asc`. Empty by default in DM/botDM rooms. |

`RoomApp` fields:

| Field       | Type                | Notes |
|-------------|---------------------|-------|
| `id`        | string              | `apps._id`. |
| `name`      | string              | `apps.channelTab.name`. |
| `tabUrl`    | string              | Computed: `SITE_URL`'s scheme/host/path-prefix + `apps.channelTab.url.default`'s path; `${roomId}` and `${siteId}` are substituted. Apps whose template URL is empty or unparseable are silently skipped. |
| `assistant` | object (optional)   | `apps.assistant` subdocument if set. |
| `avatarUrl` | string (optional)   | `apps.avatarUrl` if set. |

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors: `"not authorized to access this room's apps"` (caller is neither a room member nor a platform admin on the room's site), `"response payload exceeds maximum size"` (rare: response would exceed the NATS server's `max_payload`).

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Get Room App Command Menu

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.app.cmd-menu`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's origin `siteID`.

##### Request body

Empty body (`{}` is tolerated).

##### Success response

| Field           | Type                    | Notes |
|-----------------|-------------------------|-------|
| `appAssistants` | array<RoomAppAssistant> | One entry per bot currently subscribed in the room whose owning app has `assistant.enabled=true`. Sorted by `name asc`. |

`RoomAppAssistant` fields:

| Field       | Type             | Notes |
|-------------|------------------|-------|
| `appName`   | string           | `apps.name`. |
| `name`      | string           | `apps.assistant.name` (the bot account). |
| `cmdBlocks` | array<CmdBlock> (optional) | Active command-menu blocks from `bot_cmd_menu` joined by name. Omitted/nil if no active menu exists for the bot. `CmdBlock` is recursive (`blocks` may contain further `CmdBlock`s) and may carry a `modal` object with `command` and `param`. |

##### Error response

Same envelope and sentinels as Get Room App Tabs.

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`
```

- [ ] **Step 3: Add `SITE_URL` to `room-service/deploy/docker-compose.yml`**

Inspect the file first to see how env vars are declared. Add `SITE_URL=http://localhost:3000` to the `environment:` block of the `room-service` service. Example (only the env section is shown for context; preserve all other entries):

```yaml
    environment:
      NATS_URL: nats://nats:4222
      MONGO_URI: mongodb://mongo:27017
      SITE_ID: site-local
      SITE_URL: http://localhost:3000
      # ...other existing entries...
```

- [ ] **Step 4: Verify changes**

Run: `make lint`

Expected: clean.

Run: `make test SERVICE=room-service`

Expected: PASS.

Read back the two new docs entries to confirm formatting is consistent with neighbours.

- [ ] **Step 5: Commit**

```bash
git add docs/client-api.md room-service/deploy/docker-compose.yml
git commit -m "docs(client-api): document app.tabs and app.cmd-menu RPCs + SITE_URL"
```

---

## Task 19: Final verification

**Files:** (none — verification only)

- [ ] **Step 1: Run the full unit-test suite**

Run: `make test`

Expected: ALL PASS, including the regenerated mocks and all new tests.

- [ ] **Step 2: Run the full integration-test suite**

Run: `make test-integration SERVICE=room-service`

Expected: ALL PASS for the new `TestMongoStore_ListDefaultChannelTabApps`, `_ListRoomBotApps`, `_ListActiveCmdMenus`, `_EnsureIndexes_NewCompoundIndexes`, plus all existing room-service integration tests.

- [ ] **Step 3: Run lint and SAST**

Run: `make lint`

Expected: clean.

Run: `make sast`

Expected: clean (no medium+ findings introduced).

- [ ] **Step 4: Confirm coverage**

Run: `go test -coverprofile=/tmp/room-service.cover.out -race ./room-service/... && go tool cover -func=/tmp/room-service.cover.out | tail -20`

Expected: package-level coverage ≥80%; new handler methods and store methods should each report ≥90%. If `handleGetRoomAppTabs`, `handleGetRoomAppCommandMenu`, `marshalBounded`, `authorizeRoomAppRead`, `buildTabURL`, `ListDefaultChannelTabApps`, `ListRoomBotApps`, or `ListActiveCmdMenus` are below 90%, add focused tests for the uncovered branch before merging.

- [ ] **Step 5: Push the branch**

Run: `git push -u origin claude/determined-fermi-A07HN`

Expected: success (force-with-lease not needed unless the rebase changed history).

---

## Coverage map vs spec

| Spec section | Implemented in |
|---|---|
| Subjects + Subject Builders | Task 0 |
| Wire Format | Tasks 1, 3, 4, 5 |
| Model additions (User.Roles, IsPlatformAdmin, IsRoomMember, App.AvatarURL/ChannelTab, BotCmdMenu/CmdBlock/CmdModal) | Tasks 1, 2, 3, 4, 5 |
| Authorization helper | Task 13 |
| Handler flow (timeout + OTel + tabs + cmd-menu + nats wrappers + RegisterCRUD) | Tasks 14, 15, 16, 17 |
| Errors (sentinels + sanitizeError) | Task 11 |
| Response Payload Cap (marshalBounded + replyBoundedJSON) | Task 12 |
| Stores (RoomBotAppEntry, three methods, EnsureIndexes additions, botCmdMenus collection) | Tasks 6, 7, 8, 9, 10 |
| URL Rewrite | Task 15 (`buildTabURL`) |
| Wiring (SITE_URL, NewHandler signature, nc.MaxPayload) | Task 14 |
| Local dev (SITE_URL in docker-compose) | Task 18 |
| Client API Doc | Task 18 |
| Testing (subject, model, handler unit, response cap, integration) | Tasks 0, 1–5, 12, 13, 15, 16; integration in 7, 8, 9, 10 |
| Future optimizations | Spec-only (deferred) |
| Risks | Spec-only |
