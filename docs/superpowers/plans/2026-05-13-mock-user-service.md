# Mock User Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a stateless development-only Go service `mock-user-service` that answers 12 user RPC subjects with hardcoded responses, plus the corresponding `pkg/subject` builders, parsers, and wildcards.

**Architecture:** Twelve `natsrouter` request/reply routes registered on a single NATS connection. No database, no JetStream, no shared state. Subject strings owned by `pkg/subject`. Each handler returns either an echoed-input response with fixed defaults or a constant mock value. siteID mismatches map to `ErrNotFound`.

**Tech Stack:** Go 1.25, `natsrouter`, `natsutil`, `caarlos0/env/v11`, `log/slog`, `stretchr/testify`, multi-stage Docker (`golang:1.25.8-alpine` → `alpine:3.21`).

**Spec:** `docs/superpowers/specs/2026-05-13-mock-user-service-design.md`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `pkg/subject/subject.go` | modify | 12 specific builders, 6 parsers, 5 wildcards (append-only at end of file) |
| `pkg/subject/subject_test.go` | modify | round-trip + malformed-subject tables for new builders/parsers/wildcards |
| `mock-user-service/main.go` | create | config parsing, NATS connect, router wire-up, graceful shutdown |
| `mock-user-service/handler.go` | create | Handler struct, 12 RPC methods, request/response types, mock data helpers, `Register(router)` |
| `mock-user-service/handler_test.go` | create | table-driven unit tests for every handler + `checkSite` |
| `mock-user-service/deploy/Dockerfile` | create | multi-stage golang:1.25.8-alpine → alpine:3.21 |
| `mock-user-service/deploy/docker-compose.yml` | create | compose entry on external `chat-local` network |
| `mock-user-service/deploy/azure-pipelines.yml` | create | lint/test/build/push pipeline adapted from notification-worker |
| `docs/client-api.md` | modify | new "3.4 user-service (mock)" section documenting all 12 subjects |

---

## Task 1: `pkg/subject` — specific builders

**Files:**
- Modify: `pkg/subject/subject.go` (append at end of file)
- Modify: `pkg/subject/subject_test.go` (append)

- [ ] **Step 1: Write the failing tests**

Append to `pkg/subject/subject_test.go`:

```go
func TestUserServiceBuilders(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"status.getByName", UserStatusGetByName("alice", "s1"), "chat.user.alice.request.user.s1.status.getByName"},
		{"status.set", UserStatusSet("alice", "s1"), "chat.user.alice.request.user.s1.status.set"},
		{"profile.getByName", UserProfileGetByName("alice", "s1"), "chat.user.alice.request.user.s1.profile.getByName"},
		{"subscription.getCurrent", UserSubscriptionGetCurrent("alice", "s1"), "chat.user.alice.request.user.s1.subscription.getCurrent"},
		{"subscription.getRooms", UserSubscriptionGetRooms("alice", "s1"), "chat.user.alice.request.user.s1.subscription.getRooms"},
		{"subscription.getChannels", UserSubscriptionGetChannels("alice", "s1"), "chat.user.alice.request.user.s1.subscription.getChannels"},
		{"subscription.getDM", UserSubscriptionGetDM("alice", "s1"), "chat.user.alice.request.user.s1.subscription.getDM"},
		{"subscription.getApps", UserSubscriptionGetApps("alice", "s1"), "chat.user.alice.request.user.s1.subscription.getApps"},
		{"subscription.subscribeApp", UserSubscriptionSubscribeApp("alice", "s1"), "chat.user.alice.request.user.s1.subscription.subscribeApp"},
		{"subscription.unsubscribeApp", UserSubscriptionUnsubscribeApp("alice", "s1"), "chat.user.alice.request.user.s1.subscription.unsubscribeApp"},
		{"room.subscription.get", UserRoomSubscriptionGet("alice", "s1", "r1"), "chat.user.alice.request.user.s1.room.r1.subscription.get"},
		{"apps.list", UserAppsList("alice", "s1"), "chat.user.alice.request.user.s1.apps.list"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/subject/... -run TestUserServiceBuilders -v`
Expected: FAIL — `undefined: UserStatusGetByName` (and the others).

- [ ] **Step 3: Implement the builders**

Append to `pkg/subject/subject.go`:

```go
// --- mock-user-service / future user-service builders ---

func UserStatusGetByName(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.user.%s.status.getByName", account, siteID)
}

func UserStatusSet(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.user.%s.status.set", account, siteID)
}

func UserProfileGetByName(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.user.%s.profile.getByName", account, siteID)
}

func UserSubscriptionGetCurrent(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.getCurrent", account, siteID)
}

func UserSubscriptionGetRooms(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.getRooms", account, siteID)
}

func UserSubscriptionGetChannels(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.getChannels", account, siteID)
}

func UserSubscriptionGetDM(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.getDM", account, siteID)
}

func UserSubscriptionGetApps(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.getApps", account, siteID)
}

func UserSubscriptionSubscribeApp(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.subscribeApp", account, siteID)
}

func UserSubscriptionUnsubscribeApp(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.unsubscribeApp", account, siteID)
}

func UserRoomSubscriptionGet(account, siteID, roomID string) string {
	return fmt.Sprintf("chat.user.%s.request.user.%s.room.%s.subscription.get", account, siteID, roomID)
}

func UserAppsList(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.user.%s.apps.list", account, siteID)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/subject/... -run TestUserServiceBuilders -v`
Expected: PASS (12 subtests).

- [ ] **Step 5: Run full subject test suite + lint**

Run: `make test SERVICE=pkg/subject && make lint`
Expected: PASS (no regressions, no lint warnings).

- [ ] **Step 6: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "$(cat <<'EOF'
feat(subject): add user-service builders

Twelve concrete subject builders for the user RPC contract — status,
profile, subscription, room-scoped subscription, and apps.list. Mirror
the existing builder style and live alongside the room/message ones.

https://claude.ai/code/session_01PiDXVupNRXSLgbiBz9DsQH
EOF
)"
```

---

## Task 2: `pkg/subject` — parsers

**Files:**
- Modify: `pkg/subject/subject.go` (append)
- Modify: `pkg/subject/subject_test.go` (append)

- [ ] **Step 1: Write the failing tests**

Append to `pkg/subject/subject_test.go`:

```go
func TestParseUserSubject(t *testing.T) {
	t.Run("status.getByName roundtrips", func(t *testing.T) {
		subj := UserStatusGetByName("alice", "s1")
		account, siteID, area, action, ok := ParseUserSubject(subj)
		assert.True(t, ok)
		assert.Equal(t, "alice", account)
		assert.Equal(t, "s1", siteID)
		assert.Equal(t, "status", area)
		assert.Equal(t, "getByName", action)
	})

	t.Run("apps.list roundtrips", func(t *testing.T) {
		_, _, area, action, ok := ParseUserSubject(UserAppsList("alice", "s1"))
		assert.True(t, ok)
		assert.Equal(t, "apps", area)
		assert.Equal(t, "list", action)
	})

	t.Run("rejects malformed", func(t *testing.T) {
		bad := []string{
			"",
			"chat.user.alice",
			"chat.room.r1.event.metadata.update",
			"chat.user.alice.request.user.s1.status.getByName.extra",
			"chat.user.alice.notrequest.user.s1.status.getByName",
			"chat.user.alice.request.notuser.s1.status.getByName",
			"chat.user.alice.request.user.s1.bogus.action",
			"chat.user.alice.request.user.s1.room.r1.subscription.get",
		}
		for _, s := range bad {
			_, _, _, _, ok := ParseUserSubject(s)
			assert.False(t, ok, "expected ok=false for %q", s)
		}
	})
}

func TestParseStatusSubject(t *testing.T) {
	account, action, ok := ParseStatusSubject(UserStatusSet("alice", "s1"))
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "set", action)

	_, _, ok = ParseStatusSubject(UserProfileGetByName("alice", "s1"))
	assert.False(t, ok, "wrong area must be rejected")
}

func TestParseSubscriptionSubject(t *testing.T) {
	account, action, ok := ParseSubscriptionSubject(UserSubscriptionGetCurrent("alice", "s1"))
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "getCurrent", action)

	_, _, ok = ParseSubscriptionSubject(UserStatusSet("alice", "s1"))
	assert.False(t, ok)
}

func TestParseProfileSubject(t *testing.T) {
	account, action, ok := ParseProfileSubject(UserProfileGetByName("alice", "s1"))
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "getByName", action)

	_, _, ok = ParseProfileSubject(UserStatusSet("alice", "s1"))
	assert.False(t, ok)
}

func TestParseAppsSubject(t *testing.T) {
	account, action, ok := ParseAppsSubject(UserAppsList("alice", "s1"))
	assert.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "list", action)

	_, _, ok = ParseAppsSubject(UserStatusSet("alice", "s1"))
	assert.False(t, ok)
}

func TestParseRoomSubject(t *testing.T) {
	t.Run("subscription.get roundtrips", func(t *testing.T) {
		subj := UserRoomSubscriptionGet("alice", "s1", "r1")
		account, roomID, action, ok := ParseRoomSubject(subj)
		assert.True(t, ok)
		assert.Equal(t, "alice", account)
		assert.Equal(t, "r1", roomID)
		assert.Equal(t, "subscription.get", action)
	})

	t.Run("joins multi-token action", func(t *testing.T) {
		_, _, action, ok := ParseRoomSubject("chat.user.alice.request.user.s1.room.r1.a.b.c")
		assert.True(t, ok)
		assert.Equal(t, "a.b.c", action)
	})

	t.Run("rejects malformed", func(t *testing.T) {
		bad := []string{
			"",
			"chat.user.alice.request.user.s1.status.getByName",
			"chat.user.alice.request.user.s1.room.r1",
			"chat.user.alice.notrequest.user.s1.room.r1.subscription.get",
			"chat.user.alice.request.notuser.s1.room.r1.subscription.get",
			"chat.user.alice.request.user.s1.notroom.r1.subscription.get",
		}
		for _, s := range bad {
			_, _, _, ok := ParseRoomSubject(s)
			assert.False(t, ok, "expected ok=false for %q", s)
		}
	})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/subject/... -run "TestParse(User|Status|Subscription|Profile|Apps|Room)Subject" -v`
Expected: FAIL — `undefined: ParseUserSubject` (and others).

- [ ] **Step 3: Implement the parsers**

Append to `pkg/subject/subject.go`:

```go
// ParseUserSubject parses any 8-token subject of the form
//   chat.user.{account}.request.user.{siteID}.{area}.{action}
// where area is one of "status", "subscription", "profile", "apps".
// Does NOT match the room-scoped form — use ParseRoomSubject for those.
func ParseUserSubject(subj string) (account, siteID, area, action string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) != 8 {
		return "", "", "", "", false
	}
	if parts[0] != "chat" || parts[1] != "user" || parts[3] != "request" || parts[4] != "user" {
		return "", "", "", "", false
	}
	switch parts[6] {
	case "status", "subscription", "profile", "apps":
	default:
		return "", "", "", "", false
	}
	return parts[2], parts[5], parts[6], parts[7], true
}

func ParseStatusSubject(subj string) (account, action string, ok bool) {
	a, _, area, act, k := ParseUserSubject(subj)
	if !k || area != "status" {
		return "", "", false
	}
	return a, act, true
}

func ParseSubscriptionSubject(subj string) (account, action string, ok bool) {
	a, _, area, act, k := ParseUserSubject(subj)
	if !k || area != "subscription" {
		return "", "", false
	}
	return a, act, true
}

func ParseProfileSubject(subj string) (account, action string, ok bool) {
	a, _, area, act, k := ParseUserSubject(subj)
	if !k || area != "profile" {
		return "", "", false
	}
	return a, act, true
}

func ParseAppsSubject(subj string) (account, action string, ok bool) {
	a, _, area, act, k := ParseUserSubject(subj)
	if !k || area != "apps" {
		return "", "", false
	}
	return a, act, true
}

// ParseRoomSubject parses the 10-token room-scoped form
//   chat.user.{account}.request.user.{siteID}.room.{roomID}.{area}.{action}
// Returns the trailing `{action}` token (e.g. "get" for subscription.get).
// Returns ok=false if the subject is not exactly 10 tokens or does not
// start with chat.user.*.request.user.*.room.*.
func ParseRoomSubject(subj string) (account, roomID, action string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) != 10 {
		return "", "", "", false
	}
	if parts[0] != "chat" || parts[1] != "user" || parts[3] != "request" || parts[4] != "user" || parts[6] != "room" {
		return "", "", "", false
	}
	return parts[2], parts[7], parts[9], true
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/subject/... -run "TestParse(User|Status|Subscription|Profile|Apps|Room)Subject" -v`
Expected: PASS.

- [ ] **Step 5: Run full subject suite + lint**

Run: `make test SERVICE=pkg/subject && make lint`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "$(cat <<'EOF'
feat(subject): add user-service parsers

ParseUserSubject for the 8-token area.action form, four narrow wrappers
that validate area, and ParseRoomSubject for the room-scoped form whose
action tail is joined back into a single string.

https://claude.ai/code/session_01PiDXVupNRXSLgbiBz9DsQH
EOF
)"
```

---

## Task 3: `pkg/subject` — wildcards

**Files:**
- Modify: `pkg/subject/subject.go` (append)
- Modify: `pkg/subject/subject_test.go` (append)

- [ ] **Step 1: Write the failing tests**

Append to `pkg/subject/subject_test.go`:

```go
func TestUserServiceWildcards(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"status", UserStatusWildCard("s1"), "chat.user.*.request.user.s1.status.>"},
		{"subscription", UserSubscriptionWildCard("s1"), "chat.user.*.request.user.s1.subscription.>"},
		{"profile", UserProfileWildCard("s1"), "chat.user.*.request.user.s1.profile.>"},
		{"room", UserRoomWildCard("s1"), "chat.user.*.request.user.s1.room.>"},
		{"apps", UserAppsWildCard("s1"), "chat.user.*.request.user.s1.apps.>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/subject/... -run TestUserServiceWildcards -v`
Expected: FAIL — `undefined: UserStatusWildCard` (etc.).

- [ ] **Step 3: Implement the wildcards**

Append to `pkg/subject/subject.go`:

```go
func UserStatusWildCard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.user.%s.status.>", siteID)
}

func UserSubscriptionWildCard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.user.%s.subscription.>", siteID)
}

func UserProfileWildCard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.user.%s.profile.>", siteID)
}

func UserRoomWildCard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.user.%s.room.>", siteID)
}

func UserAppsWildCard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.user.%s.apps.>", siteID)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/subject/... -run TestUserServiceWildcards -v`
Expected: PASS.

- [ ] **Step 5: Run full subject suite + lint**

Run: `make test SERVICE=pkg/subject && make lint`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "$(cat <<'EOF'
feat(subject): add user-service area wildcards

Five area-scoped wildcard builders (status, subscription, profile, room,
apps) for services that want one NATS subscription per area instead of
one per route.

https://claude.ai/code/session_01PiDXVupNRXSLgbiBz9DsQH
EOF
)"
```

---

## Task 4: `mock-user-service` — handler skeleton

**Files:**
- Create: `mock-user-service/handler.go`
- Create: `mock-user-service/handler_test.go`

- [ ] **Step 1: Write the failing test**

Create `mock-user-service/handler_test.go`:

```go
package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsrouter"
)

func newCtx(params map[string]string) *natsrouter.Context {
	return natsrouter.NewContext(params)
}

func TestHandler_CheckSite(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("match", func(t *testing.T) {
		err := h.checkSite(newCtx(map[string]string{"siteID": "site-local"}))
		assert.NoError(t, err)
	})

	t.Run("mismatch returns ErrNotFound", func(t *testing.T) {
		err := h.checkSite(newCtx(map[string]string{"siteID": "site-other"}))
		require.Error(t, err)
		var routeErr *natsrouter.RouteError
		require.True(t, errors.As(err, &routeErr), "want *natsrouter.RouteError, got %T", err)
		assert.Equal(t, natsrouter.CodeNotFound, routeErr.Code)
	})
}

func TestBuildMockSub(t *testing.T) {
	sub := buildMockSub("alice", "site-local")
	assert.Equal(t, "alice", sub.User.Account)
	assert.Equal(t, "site-local", sub.SiteID)
	assert.NotEmpty(t, sub.ID)
	assert.NotEmpty(t, sub.RoomID)
}

func TestBuildMockApp(t *testing.T) {
	app := buildMockApp("app-1", "Mock One")
	assert.Equal(t, "app-1", app.ID)
	assert.Equal(t, "Mock One", app.Name)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./mock-user-service/... -run "TestHandler_CheckSite|TestBuildMockSub|TestBuildMockApp" -v`
Expected: FAIL — package `mock-user-service` does not exist.

- [ ] **Step 3: Create the handler skeleton**

Create `mock-user-service/handler.go`:

```go
package main

import (
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// --- mock data constants ---

const (
	mockStatusText   = "available"
	mockStatusIsShow = true
	mockDisplayName  = "Mock User"
	mockEmail        = "mock@example.test"
)

var mockJoinedAt = time.Unix(0, 0).UTC()

// --- request / response types ---

type statusGetByNameReq struct {
	Name string `json:"name"`
}

type statusSetReq struct {
	StatusText   string `json:"statusText"`
	StatusIsShow bool   `json:"statusIsShow"`
}

type statusResp struct {
	Name         string `json:"name"`
	StatusText   string `json:"statusText"`
	StatusIsShow bool   `json:"statusIsShow"`
}

type profileGetByNameReq struct {
	Name string `json:"name"`
}

type profileResp struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
	Email       string `json:"email"`
}

type getSubsReq struct {
	Favorite       *bool    `json:"favorite,omitempty"`
	MembersContain []string `json:"membersContain,omitempty"`
	AccountNames   []string `json:"accountNames,omitempty"`
}

type getAppSubsReq struct {
	Favorite *bool `json:"favorite,omitempty"`
}

type getDMSubReq struct {
	TargetAccount string `json:"targetAccount"`
}

type appSubscriptionReq struct {
	AppID string `json:"appId"`
}

type subscriptionListResp struct {
	Subscriptions []model.Subscription `json:"subscriptions"`
	Total         int                  `json:"total"`
}

type dmSubscriptionResp struct {
	Subscription model.Subscription `json:"subscription"`
}

type roomSubscriptionResp struct {
	Subscription model.Subscription `json:"subscription"`
}

type appListResp struct {
	Apps  []model.App `json:"apps"`
	Total int         `json:"total"`
}

type okResp struct {
	Success bool `json:"success"`
}

// --- handler ---

type Handler struct {
	siteID string
}

func NewHandler(siteID string) *Handler {
	return &Handler{siteID: siteID}
}

func (h *Handler) checkSite(c *natsrouter.Context) error {
	if c.Param("siteID") != h.siteID {
		return natsrouter.ErrNotFound("unknown site")
	}
	return nil
}

// --- mock data helpers ---

func buildMockSub(account, siteID string) model.Subscription {
	return model.Subscription{
		ID:       "mock-sub-" + account,
		User:     model.SubscriptionUser{ID: "mock-user-" + account, Account: account},
		RoomID:   "mock-room",
		SiteID:   siteID,
		Roles:    []model.Role{model.RoleMember},
		Name:     "Mock Room",
		RoomType: model.RoomTypeChannel,
		JoinedAt: mockJoinedAt,
	}
}

func buildMockApp(id, name string) model.App {
	return model.App{ID: id, Name: name}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./mock-user-service/... -run "TestHandler_CheckSite|TestBuildMockSub|TestBuildMockApp" -v`
Expected: PASS.

- [ ] **Step 5: Verify build + lint**

Run: `go build ./mock-user-service/... && make lint`
Expected: clean build, no lint warnings.

- [ ] **Step 6: Commit**

```bash
git add mock-user-service/handler.go mock-user-service/handler_test.go
git commit -m "$(cat <<'EOF'
feat(mock-user-service): scaffold handler skeleton

Handler struct, request/response types, mock data constants and
builders, and the siteID check helper. Handlers themselves are added
in follow-up commits.

https://claude.ai/code/session_01PiDXVupNRXSLgbiBz9DsQH
EOF
)"
```

---

## Task 5: status handlers

**Files:**
- Modify: `mock-user-service/handler.go`
- Modify: `mock-user-service/handler_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `mock-user-service/handler_test.go`:

```go
func TestHandler_StatusGetByName(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("happy path echoes Name", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})
		resp, err := h.statusGetByName(c, statusGetByNameReq{Name: "bob"})
		require.NoError(t, err)
		assert.Equal(t, "bob", resp.Name)
		assert.Equal(t, mockStatusText, resp.StatusText)
		assert.Equal(t, mockStatusIsShow, resp.StatusIsShow)
	})

	t.Run("siteID mismatch", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.statusGetByName(c, statusGetByNameReq{Name: "bob"})
		require.Error(t, err)
	})
}

func TestHandler_StatusSet(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("happy path returns success", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})
		resp, err := h.statusSet(c, statusSetReq{StatusText: "busy", StatusIsShow: false})
		require.NoError(t, err)
		assert.True(t, resp.Success)
	})

	t.Run("siteID mismatch", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.statusSet(c, statusSetReq{})
		require.Error(t, err)
	})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./mock-user-service/... -run "TestHandler_Status" -v`
Expected: FAIL — `h.statusGetByName undefined`.

- [ ] **Step 3: Implement the handlers**

Append to `mock-user-service/handler.go`:

```go
func (h *Handler) statusGetByName(c *natsrouter.Context, req statusGetByNameReq) (*statusResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	return &statusResp{
		Name:         req.Name,
		StatusText:   mockStatusText,
		StatusIsShow: mockStatusIsShow,
	}, nil
}

func (h *Handler) statusSet(c *natsrouter.Context, req statusSetReq) (*okResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	return &okResp{Success: true}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./mock-user-service/... -run "TestHandler_Status" -v`
Expected: PASS.

- [ ] **Step 5: Build + lint**

Run: `go build ./mock-user-service/... && make lint`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add mock-user-service/handler.go mock-user-service/handler_test.go
git commit -m "$(cat <<'EOF'
feat(mock-user-service): add status handlers

status.getByName echoes the requested Name with fixed StatusText /
StatusIsShow defaults; status.set is a no-op returning success. Both
gate on siteID match.

https://claude.ai/code/session_01PiDXVupNRXSLgbiBz9DsQH
EOF
)"
```

---

## Task 6: profile handler

**Files:**
- Modify: `mock-user-service/handler.go`
- Modify: `mock-user-service/handler_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `mock-user-service/handler_test.go`:

```go
func TestHandler_ProfileGetByName(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("happy path echoes Name", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})
		resp, err := h.profileGetByName(c, profileGetByNameReq{Name: "bob"})
		require.NoError(t, err)
		assert.Equal(t, "bob", resp.Name)
		assert.Equal(t, mockDisplayName, resp.DisplayName)
		assert.Equal(t, mockEmail, resp.Email)
	})

	t.Run("siteID mismatch", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.profileGetByName(c, profileGetByNameReq{Name: "bob"})
		require.Error(t, err)
	})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./mock-user-service/... -run "TestHandler_Profile" -v`
Expected: FAIL.

- [ ] **Step 3: Implement the handler**

Append to `mock-user-service/handler.go`:

```go
func (h *Handler) profileGetByName(c *natsrouter.Context, req profileGetByNameReq) (*profileResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	return &profileResp{
		Name:        req.Name,
		DisplayName: mockDisplayName,
		Email:       mockEmail,
	}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./mock-user-service/... -run "TestHandler_Profile" -v`
Expected: PASS.

- [ ] **Step 5: Build + lint**

Run: `go build ./mock-user-service/... && make lint`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add mock-user-service/handler.go mock-user-service/handler_test.go
git commit -m "$(cat <<'EOF'
feat(mock-user-service): add profile handler

profile.getByName echoes the requested Name with fixed DisplayName /
Email defaults; gates on siteID match.

https://claude.ai/code/session_01PiDXVupNRXSLgbiBz9DsQH
EOF
)"
```

---

## Task 7: subscription list handlers

Four routes that share the same list response shape: `getCurrent`, `getRooms`, `getChannels`, `getApps`.

**Files:**
- Modify: `mock-user-service/handler.go`
- Modify: `mock-user-service/handler_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `mock-user-service/handler_test.go`:

```go
func TestHandler_SubscriptionListHandlers(t *testing.T) {
	h := NewHandler("site-local")
	c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})

	type listFn func() (*subscriptionListResp, error)
	cases := []struct {
		name string
		fn   listFn
	}{
		{"getCurrent", func() (*subscriptionListResp, error) { return h.subscriptionGetCurrent(c, getSubsReq{}) }},
		{"getRooms", func() (*subscriptionListResp, error) { return h.subscriptionGetRooms(c, getSubsReq{}) }},
		{"getChannels", func() (*subscriptionListResp, error) { return h.subscriptionGetChannels(c, getSubsReq{}) }},
		{"getApps", func() (*subscriptionListResp, error) { return h.subscriptionGetApps(c, getAppSubsReq{}) }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := tt.fn()
			require.NoError(t, err)
			assert.Equal(t, 2, resp.Total)
			require.Len(t, resp.Subscriptions, 2)
			for _, sub := range resp.Subscriptions {
				assert.Equal(t, "alice", sub.User.Account)
				assert.Equal(t, "site-local", sub.SiteID)
			}
		})
	}
}

func TestHandler_SubscriptionListHandlers_SiteMismatch(t *testing.T) {
	h := NewHandler("site-local")
	c := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})

	_, err := h.subscriptionGetCurrent(c, getSubsReq{})
	require.Error(t, err)
	_, err = h.subscriptionGetRooms(c, getSubsReq{})
	require.Error(t, err)
	_, err = h.subscriptionGetChannels(c, getSubsReq{})
	require.Error(t, err)
	_, err = h.subscriptionGetApps(c, getAppSubsReq{})
	require.Error(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./mock-user-service/... -run "TestHandler_SubscriptionListHandlers" -v`
Expected: FAIL.

- [ ] **Step 3: Implement the handlers**

Append to `mock-user-service/handler.go`:

```go
func (h *Handler) mockSubList(account string) []model.Subscription {
	return []model.Subscription{
		buildMockSub(account, h.siteID),
		buildMockSub(account, h.siteID),
	}
}

func (h *Handler) subscriptionGetCurrent(c *natsrouter.Context, req getSubsReq) (*subscriptionListResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	subs := h.mockSubList(c.Param("account"))
	return &subscriptionListResp{Subscriptions: subs, Total: len(subs)}, nil
}

func (h *Handler) subscriptionGetRooms(c *natsrouter.Context, req getSubsReq) (*subscriptionListResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	subs := h.mockSubList(c.Param("account"))
	return &subscriptionListResp{Subscriptions: subs, Total: len(subs)}, nil
}

func (h *Handler) subscriptionGetChannels(c *natsrouter.Context, req getSubsReq) (*subscriptionListResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	subs := h.mockSubList(c.Param("account"))
	return &subscriptionListResp{Subscriptions: subs, Total: len(subs)}, nil
}

func (h *Handler) subscriptionGetApps(c *natsrouter.Context, req getAppSubsReq) (*subscriptionListResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	subs := h.mockSubList(c.Param("account"))
	return &subscriptionListResp{Subscriptions: subs, Total: len(subs)}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./mock-user-service/... -run "TestHandler_SubscriptionListHandlers" -v`
Expected: PASS.

- [ ] **Step 5: Build + lint**

Run: `go build ./mock-user-service/... && make lint`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add mock-user-service/handler.go mock-user-service/handler_test.go
git commit -m "$(cat <<'EOF'
feat(mock-user-service): add subscription list handlers

getCurrent, getRooms, getChannels, and getApps each return two mock
Subscriptions whose User.Account matches the subject's account param.
Filter fields on the request are accepted and ignored. All gate on
siteID match.

https://claude.ai/code/session_01PiDXVupNRXSLgbiBz9DsQH
EOF
)"
```

---

## Task 8: subscription DM + subscribe/unsubscribe handlers

**Files:**
- Modify: `mock-user-service/handler.go`
- Modify: `mock-user-service/handler_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `mock-user-service/handler_test.go`:

```go
func TestHandler_SubscriptionGetDM(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("returns single sub for target", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})
		resp, err := h.subscriptionGetDM(c, getDMSubReq{TargetAccount: "bob"})
		require.NoError(t, err)
		assert.Equal(t, "bob", resp.Subscription.User.Account)
		assert.Equal(t, "site-local", resp.Subscription.SiteID)
	})

	t.Run("siteID mismatch", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.subscriptionGetDM(c, getDMSubReq{TargetAccount: "bob"})
		require.Error(t, err)
	})
}

func TestHandler_SubscriptionAppOps(t *testing.T) {
	h := NewHandler("site-local")
	c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})

	t.Run("subscribeApp returns success", func(t *testing.T) {
		resp, err := h.subscriptionSubscribeApp(c, appSubscriptionReq{AppID: "app-1"})
		require.NoError(t, err)
		assert.True(t, resp.Success)
	})

	t.Run("unsubscribeApp returns success", func(t *testing.T) {
		resp, err := h.subscriptionUnsubscribeApp(c, appSubscriptionReq{AppID: "app-1"})
		require.NoError(t, err)
		assert.True(t, resp.Success)
	})

	t.Run("subscribeApp siteID mismatch", func(t *testing.T) {
		badC := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.subscriptionSubscribeApp(badC, appSubscriptionReq{})
		require.Error(t, err)
	})

	t.Run("unsubscribeApp siteID mismatch", func(t *testing.T) {
		badC := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.subscriptionUnsubscribeApp(badC, appSubscriptionReq{})
		require.Error(t, err)
	})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./mock-user-service/... -run "TestHandler_SubscriptionGetDM|TestHandler_SubscriptionAppOps" -v`
Expected: FAIL.

- [ ] **Step 3: Implement the handlers**

Append to `mock-user-service/handler.go`:

```go
func (h *Handler) subscriptionGetDM(c *natsrouter.Context, req getDMSubReq) (*dmSubscriptionResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	return &dmSubscriptionResp{Subscription: buildMockSub(req.TargetAccount, h.siteID)}, nil
}

func (h *Handler) subscriptionSubscribeApp(c *natsrouter.Context, req appSubscriptionReq) (*okResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	return &okResp{Success: true}, nil
}

func (h *Handler) subscriptionUnsubscribeApp(c *natsrouter.Context, req appSubscriptionReq) (*okResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	_ = req
	return &okResp{Success: true}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./mock-user-service/... -run "TestHandler_SubscriptionGetDM|TestHandler_SubscriptionAppOps" -v`
Expected: PASS.

- [ ] **Step 5: Build + lint**

Run: `go build ./mock-user-service/... && make lint`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add mock-user-service/handler.go mock-user-service/handler_test.go
git commit -m "$(cat <<'EOF'
feat(mock-user-service): add DM + app subscription handlers

subscription.getDM returns a single mock Subscription whose
User.Account matches the request's TargetAccount; subscribeApp and
unsubscribeApp are no-ops returning Success=true. All gate on siteID
match.

https://claude.ai/code/session_01PiDXVupNRXSLgbiBz9DsQH
EOF
)"
```

---

## Task 9: room subscription + apps list handlers

Two `RegisterNoBody`-style handlers.

**Files:**
- Modify: `mock-user-service/handler.go`
- Modify: `mock-user-service/handler_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `mock-user-service/handler_test.go`:

```go
func TestHandler_RoomSubscriptionGet(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("echoes roomID from param", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-local", "roomID": "r-42"})
		resp, err := h.roomSubscriptionGet(c)
		require.NoError(t, err)
		assert.Equal(t, "r-42", resp.Subscription.RoomID)
		assert.Equal(t, "alice", resp.Subscription.User.Account)
		assert.Equal(t, "site-local", resp.Subscription.SiteID)
	})

	t.Run("siteID mismatch", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-x", "roomID": "r-42"})
		_, err := h.roomSubscriptionGet(c)
		require.Error(t, err)
	})
}

func TestHandler_AppsList(t *testing.T) {
	h := NewHandler("site-local")

	t.Run("returns two mock apps", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-local"})
		resp, err := h.appsList(c)
		require.NoError(t, err)
		assert.Equal(t, 2, resp.Total)
		require.Len(t, resp.Apps, 2)
		assert.NotEmpty(t, resp.Apps[0].ID)
		assert.NotEmpty(t, resp.Apps[0].Name)
	})

	t.Run("siteID mismatch", func(t *testing.T) {
		c := newCtx(map[string]string{"account": "alice", "siteID": "site-x"})
		_, err := h.appsList(c)
		require.Error(t, err)
	})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./mock-user-service/... -run "TestHandler_RoomSubscriptionGet|TestHandler_AppsList" -v`
Expected: FAIL.

- [ ] **Step 3: Implement the handlers**

Append to `mock-user-service/handler.go`:

```go
func (h *Handler) roomSubscriptionGet(c *natsrouter.Context) (*roomSubscriptionResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	sub := buildMockSub(c.Param("account"), h.siteID)
	sub.RoomID = c.Param("roomID")
	return &roomSubscriptionResp{Subscription: sub}, nil
}

func (h *Handler) appsList(c *natsrouter.Context) (*appListResp, error) {
	if err := h.checkSite(c); err != nil {
		return nil, err
	}
	apps := []model.App{
		buildMockApp("app-1", "Mock App One"),
		buildMockApp("app-2", "Mock App Two"),
	}
	return &appListResp{Apps: apps, Total: len(apps)}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./mock-user-service/... -run "TestHandler_RoomSubscriptionGet|TestHandler_AppsList" -v`
Expected: PASS.

- [ ] **Step 5: Build + lint**

Run: `go build ./mock-user-service/... && make lint`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add mock-user-service/handler.go mock-user-service/handler_test.go
git commit -m "$(cat <<'EOF'
feat(mock-user-service): add room subscription + apps list handlers

room.subscription.get echoes the roomID from the subject into the
returned mock Subscription; apps.list returns two mock apps. Both
gate on siteID match.

https://claude.ai/code/session_01PiDXVupNRXSLgbiBz9DsQH
EOF
)"
```

---

## Task 10: Register all routes + main.go

**Files:**
- Modify: `mock-user-service/handler.go` (add `Register` method at the bottom)
- Create: `mock-user-service/main.go`

- [ ] **Step 1: Add the Register method**

Append to `mock-user-service/handler.go`:

```go
// Register subscribes every mock user RPC route on the supplied router.
func (h *Handler) Register(r *natsrouter.Router) {
	natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.status.getByName", h.statusGetByName)
	natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.status.set", h.statusSet)
	natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.profile.getByName", h.profileGetByName)
	natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.getCurrent", h.subscriptionGetCurrent)
	natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.getRooms", h.subscriptionGetRooms)
	natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.getChannels", h.subscriptionGetChannels)
	natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.getDM", h.subscriptionGetDM)
	natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.getApps", h.subscriptionGetApps)
	natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.subscribeApp", h.subscriptionSubscribeApp)
	natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.unsubscribeApp", h.subscriptionUnsubscribeApp)
	natsrouter.RegisterNoBody(r, "chat.user.{account}.request.user.{siteID}.room.{roomID}.subscription.get", h.roomSubscriptionGet)
	natsrouter.RegisterNoBody(r, "chat.user.{account}.request.user.{siteID}.apps.list", h.appsList)
}
```

- [ ] **Step 2: Create main.go**

Create `mock-user-service/main.go`:

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
)

type config struct {
	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string `env:"SITE_ID"         envDefault:"site-local"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "mock-user-service")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	router := natsrouter.Default(nc, "mock-user-service")
	router.Use(natsrouter.HandlerTimeout(5 * time.Second))

	handler := NewHandler(cfg.SiteID)
	handler.Register(router)

	slog.Info("mock-user-service running", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
	)
}
```

- [ ] **Step 3: Build + run full unit suite + lint**

Run: `go build ./mock-user-service/... && go test ./mock-user-service/... -race -v && make lint`
Expected: clean build, all 12 handler test groups + helper tests pass, no lint warnings.

- [ ] **Step 4: Verify natsrouter pattern shape compiles**

If `natsrouter.Register` complains about a missing import or shape mismatch, re-check that each handler's signature matches `func(*natsrouter.Context, ReqT) (*RespT, error)` and that `RegisterNoBody` matches `func(*natsrouter.Context) (*RespT, error)`. No code change should be needed; this step is a sanity gate.

- [ ] **Step 5: Commit**

```bash
git add mock-user-service/handler.go mock-user-service/main.go
git commit -m "$(cat <<'EOF'
feat(mock-user-service): wire main + register all 12 routes

main.go parses env config, connects to NATS, builds a Default router
with a 5s HandlerTimeout, registers every handler, and shuts down
gracefully (router → nc.Drain → tracer). The Register method on
Handler subscribes all 12 user RPC subjects in one place.

https://claude.ai/code/session_01PiDXVupNRXSLgbiBz9DsQH
EOF
)"
```

---

## Task 11: deploy/ artifacts

**Files:**
- Create: `mock-user-service/deploy/Dockerfile`
- Create: `mock-user-service/deploy/docker-compose.yml`
- Create: `mock-user-service/deploy/azure-pipelines.yml`

- [ ] **Step 1: Verify parent dir exists**

Run: `ls mock-user-service/ && mkdir -p mock-user-service/deploy`
Expected: directory listing, then no error.

- [ ] **Step 2: Create the Dockerfile**

Create `mock-user-service/deploy/Dockerfile`:

```dockerfile
FROM golang:1.25.8-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY mock-user-service/ mock-user-service/

RUN CGO_ENABLED=0 go build -o /mock-user-service ./mock-user-service/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /mock-user-service /mock-user-service

ENTRYPOINT ["/mock-user-service"]
```

- [ ] **Step 3: Create docker-compose.yml**

Create `mock-user-service/deploy/docker-compose.yml`:

```yaml
name: mock-user-service

services:
  mock-user-service:
    build:
      context: ../..
      dockerfile: mock-user-service/deploy/Dockerfile
    environment:
      - NATS_URL=nats://nats:4222
      - NATS_CREDS_FILE=/etc/nats/backend.creds
      - SITE_ID=site-local
    volumes:
      - ../../docker-local/backend.creds:/etc/nats/backend.creds:ro
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

- [ ] **Step 4: Create azure-pipelines.yml**

Create `mock-user-service/deploy/azure-pipelines.yml`:

```yaml
trigger:
  branches:
    include:
      - main
      - develop
  paths:
    include:
      - mock-user-service/
      - pkg/

pr:
  branches:
    include:
      - main
  paths:
    include:
      - mock-user-service/
      - pkg/

variables:
  GO_VERSION: '1.25.8'
  SERVICE_NAME: mock-user-service
  REGISTRY: '$(containerRegistry)'

stages:
  - stage: Validate
    displayName: 'Lint & Test'
    jobs:
      - job: LintAndTest
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: GoTool@0
            inputs:
              version: '$(GO_VERSION)'
            displayName: 'Install Go $(GO_VERSION)'

          - script: go vet ./$(SERVICE_NAME)/... ./pkg/...
            displayName: 'Go Vet'

          - script: go test ./pkg/... -v -race -coverprofile=coverage-pkg.out
            displayName: 'Test shared packages'

          - script: go test ./$(SERVICE_NAME)/... -v -race -coverprofile=coverage-$(SERVICE_NAME).out
            displayName: 'Test $(SERVICE_NAME)'

          - script: go build -o /dev/null ./$(SERVICE_NAME)/
            displayName: 'Build $(SERVICE_NAME)'

  - stage: Build
    displayName: 'Build & Push Image'
    dependsOn: Validate
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - job: BuildImage
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: Docker@2
            inputs:
              containerRegistry: '$(containerRegistry)'
              repository: 'chat/$(SERVICE_NAME)'
              command: 'buildAndPush'
              Dockerfile: '$(SERVICE_NAME)/deploy/Dockerfile'
              buildContext: '.'
              tags: |
                $(Build.BuildId)
                latest
            displayName: 'Build & push $(SERVICE_NAME)'
```

- [ ] **Step 5: Verify Docker build (if Docker available; else skip)**

Run: `docker build -f mock-user-service/deploy/Dockerfile -t mock-user-service:test .` (from repo root)
Expected: image builds successfully. If Docker isn't available on this host, skip and rely on CI.

- [ ] **Step 6: Commit**

```bash
git add mock-user-service/deploy/
git commit -m "$(cat <<'EOF'
chore(mock-user-service): add deploy artifacts

Multi-stage Dockerfile, docker-compose entry joining the external
chat-local network, and an Azure pipeline adapted from
notification-worker (vet → pkg tests → service tests → build → push).

https://claude.ai/code/session_01PiDXVupNRXSLgbiBz9DsQH
EOF
)"
```

---

## Task 12: docs/client-api.md update

**Files:**
- Modify: `docs/client-api.md` (insert a new `### 3.4 user-service (mock)` section between sections 3.3 and 4)

- [ ] **Step 1: Locate the insertion point**

Run: `grep -n "^## 4\. Message Send" docs/client-api.md`
Expected: prints a single line number — call it `N`. The new section is inserted before that line.

- [ ] **Step 2: Insert the new section**

Insert the following block immediately before the `## 4. Message Send` heading. (Use `Edit` with the unique `## 4. Message Send` line as the anchor.)

```markdown
### 3.4 user-service (mock)

> **Dev-only.** Implemented by `mock-user-service` with hardcoded
> responses. Subjects and shapes are stable; switch to a real
> implementation later by swapping the handler bodies.

All subjects share the prefix `chat.user.{account}.request.user.{siteID}.`
unless noted. siteID must match the deployed mock's `SITE_ID`; otherwise
the reply is `{"error":"unknown site","code":"not_found"}`.

| # | Suffix | Builder | Request | Response |
|---|---|---|---|---|
| 1 | `status.getByName` | `subject.UserStatusGetByName(account, siteID)` | `{"name": "<string>"}` | `{"name": "<echoed>", "statusText": "available", "statusIsShow": true}` |
| 2 | `status.set` | `subject.UserStatusSet(account, siteID)` | `{"statusText": "<string>", "statusIsShow": <bool>}` | `{"success": true}` |
| 3 | `profile.getByName` | `subject.UserProfileGetByName(account, siteID)` | `{"name": "<string>"}` | `{"name": "<echoed>", "displayName": "Mock User", "email": "mock@example.test"}` |
| 4 | `subscription.getCurrent` | `subject.UserSubscriptionGetCurrent(account, siteID)` | `{"favorite": <bool?>, "membersContain": <string[]?>, "accountNames": <string[]?>}` | `{"subscriptions": [<Subscription>, <Subscription>], "total": 2}` |
| 5 | `subscription.getRooms` | `subject.UserSubscriptionGetRooms(account, siteID)` | same as #4 | same as #4 |
| 6 | `subscription.getChannels` | `subject.UserSubscriptionGetChannels(account, siteID)` | same as #4 | same as #4 |
| 7 | `subscription.getDM` | `subject.UserSubscriptionGetDM(account, siteID)` | `{"targetAccount": "<string>"}` | `{"subscription": <Subscription with User.Account == targetAccount>}` |
| 8 | `subscription.getApps` | `subject.UserSubscriptionGetApps(account, siteID)` | `{"favorite": <bool?>}` | `{"subscriptions": [<Subscription>, <Subscription>], "total": 2}` |
| 9 | `subscription.subscribeApp` | `subject.UserSubscriptionSubscribeApp(account, siteID)` | `{"appId": "<string>"}` | `{"success": true}` |
| 10 | `subscription.unsubscribeApp` | `subject.UserSubscriptionUnsubscribeApp(account, siteID)` | `{"appId": "<string>"}` | `{"success": true}` |
| 11 | `room.{roomID}.subscription.get` | `subject.UserRoomSubscriptionGet(account, siteID, roomID)` | _no body_ | `{"subscription": <Subscription with RoomID == roomID>}` |
| 12 | `apps.list` | `subject.UserAppsList(account, siteID)` | _no body_ | `{"apps": [<App>, <App>], "total": 2}` |

`<Subscription>` and `<App>` are the standard `pkg/model.Subscription`
and `pkg/model.App` JSON shapes. Filter fields on request bodies (`favorite`,
`membersContain`, `accountNames`) are accepted and ignored by the mock —
every list endpoint returns the same two mock entries regardless of input.

**Error envelope:** all routes return the standard `{"error": "...", "code": "..."}`
shape on failure. The only error returned by the mock is `unknown site`
(`code: not_found`).
```

- [ ] **Step 3: Verify the edit**

Run: `grep -n "^### 3\.4 user-service" docs/client-api.md`
Expected: prints exactly one line, located before the `## 4. Message Send` line from Step 1.

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md
git commit -m "$(cat <<'EOF'
docs(client-api): document mock user-service RPCs

Adds section 3.4 covering all 12 mock user-service subjects with their
builders, request/response shapes, and the single error case. Flagged
dev-only at the top of the section.

https://claude.ai/code/session_01PiDXVupNRXSLgbiBz9DsQH
EOF
)"
```

---

## Task 13: Final integration smoke + push

- [ ] **Step 1: Run the whole test suite with race**

Run: `go test ./... -race`
Expected: PASS for `pkg/subject`, `mock-user-service`, and all other unchanged packages (no regressions).

- [ ] **Step 2: Final lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 3: Push the branch**

Run: `git push origin claude/mock-user-service-fGyWo`
Expected: remote tracking updates without conflicts. If a network error occurs, retry up to 4 times with 2s/4s/8s/16s backoff.

- [ ] **Step 4: Summarize to user**

Report: number of commits added on top of the spec, the final commit hash, and one line per task confirming pass.

---

## Self-review notes

- Spec coverage: each of the 12 routes has a dedicated handler task; the `pkg/subject` builders/parsers/wildcards (12/6/5) each have their own task; deploy/ and docs/ each have a task. Matches spec sections 1:1.
- Type consistency: handler signatures match the natsrouter `Register[Req, Resp]` and `RegisterNoBody[Resp]` shapes — return `(*RespT, error)` everywhere. `getAppSubsReq` (route 8) is distinct from `getSubsReq` (routes 4–6) per the spec; subscription list handlers for the three `getSubsReq` routes share a request type but each is registered as a separate route with its own handler method.
- No `TODO`/`TBD`/placeholder steps remain. The Docker build step (Task 11 Step 5) is the only optional one and explicitly says "skip if Docker isn't available".
