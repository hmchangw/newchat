# teams-chat-member-sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A run-to-completion job (k8s CronJob) that resolves the authoritative member list for Teams chats flagged `needMemberSync=true`, writing members + `needCreateRoom=true` back to `teams_chat`.

**Architecture:** One global job, structured identically to `teams-chat-sync`. It reads `teams_chat` where `needMemberSync=true` (secondary-preferred read client), fans chat IDs to `MAX_WORKERS` goroutines that call Graph `GET /chats/{id}/members`, derives each member's account from its UPN (falling back to a batched, cross-run-cached `teams_user` lookup by userId), and `$set`s `{members, needCreateRoom:true, needMemberSync:false, updatedAt}` through a write client. A failed chat keeps `needMemberSync=true` and is retried next run.

**Tech Stack:** Go 1.25, `pkg/msgraph` (extended with `ChatMembersReader`), MongoDB via `pkg/mongoutil` (read + write clients), `caarlos0/env`, `log/slog`, mockgen + testify, testcontainers via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-07-15-teams-chat-member-sync-design.md`

## Global Constraints

- Branch: all commits go to `claude/teams-chat-sync-service-9ispb0` (combined PR #71 on `hmchangw/newchat`); never push elsewhere. `origin` already points at `hmchangw/newchat`.
- Before the first commit run: `git config user.email noreply@anthropic.com && git config user.name Claude`.
- Every commit message ends with these two trailer lines:
  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_013cXnGa3vcWcJbZeGFNH5pL
  ```
- Use `make` targets, not raw `go`, EXCEPT single-test runs during TDD where `go test -race -run <Name> ./<pkg>/` is the only way to scope a run.
- Red-Green-Refactor for all new code: write the failing test, see it fail, implement, see it pass, commit.
- BSON/JSON tags on `teams_chat`/`teams_user` use `siteId` (lowercase d) and camelCase — match the existing `pkg/model/teams.go` and `pkg/model/teamsuser.go`. Do not "fix" casing.
- Account derivation: `account = lowercased local-part of the UPN` (text before `@`), matching `teams-user-sync`'s `splitUPN`. **Only the UPN is consulted — the Graph member `email` field is ignored.**
- Mongo split: reads via `mongoutil.ConnectRead` (secondary-preferred), writes via `mongoutil.Connect`. Config `MONGO_READ_*` / `MONGO_WRITE_*`, both URIs `required,notEmpty`.
- Errors: raw `fmt.Errorf("what this fn was doing: %w", err)` throughout (no `pkg/errcode` — no client boundary). Never log the client secret, tokens, or raw Graph response bodies.
- `log/slog` JSON only, structured key-value fields.
- No tracer init (this repo's cronjobs use plain slog; `pkg/otelutil` is absent).
- No `docs/client-api.md` change: no client-facing handler, no client-facing `pkg/model` struct is touched.
- Coverage floor 80% for business logic (the `main()`/`run()` wiring is exempt, matching the sibling jobs).

---

### Task 1: pkg/model — add NeedCreateRoom to TeamsChat

**Files:**
- Modify: `pkg/model/teams.go` (the `TeamsChat` struct)
- Test: `pkg/model/model_test.go` (append)

**Interfaces:**
- Consumes: nothing new.
- Produces (used by Tasks 3, 4, 6): `model.TeamsChat.NeedCreateRoom bool` with json/bson tag `needCreateRoom`.

- [ ] **Step 1: Write the failing test**

Append to `pkg/model/model_test.go`. Use whole-second UTC times so BSON millisecond precision cannot mismatch.

```go
func TestTeamsChatJSON_NeedCreateRoom(t *testing.T) {
	c := model.TeamsChat{
		ID:                  "19:g1@thread.v2",
		ChatType:            "group",
		CreatedDateTime:     time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
		Members:             []model.TeamsChatMember{{ID: "u1", Account: "alice"}},
		SiteID:              "site-a",
		UpdatedAt:           time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC),
		NeedMemberSync:      false,
		NeedCreateRoom:      true,
	}
	roundTrip(t, &c, &model.TeamsChat{})

	data, err := bson.Marshal(&c)
	require.NoError(t, err)
	var raw bson.M
	require.NoError(t, bson.Unmarshal(data, &raw))
	assert.Contains(t, raw, "needCreateRoom", "BSON doc must have needCreateRoom key")
	assert.Equal(t, true, raw["needCreateRoom"])
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -race -run TestTeamsChatJSON_NeedCreateRoom ./pkg/model/`
Expected: FAIL to compile — `unknown field NeedCreateRoom`.

- [ ] **Step 3: Add the field**

In `pkg/model/teams.go`, add `NeedCreateRoom` to the `TeamsChat` struct, directly after `NeedMemberSync`:

```go
	NeedMemberSync      bool              `json:"needMemberSync" bson:"needMemberSync"`
	NeedCreateRoom      bool              `json:"needCreateRoom" bson:"needCreateRoom"` // set true by teams-chat-member-sync; consumed by room creation
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race -run TestTeamsChatJSON_NeedCreateRoom ./pkg/model/`
Expected: PASS. Then `make test SERVICE=pkg/model`.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/teams.go pkg/model/model_test.go
git commit -m "feat(model): add NeedCreateRoom to TeamsChat"
```

---

### Task 2: pkg/msgraph — ChatMembersReader

**Files:**
- Create: `pkg/msgraph/members.go`
- Modify: `pkg/msgraph/msgraph.go` (add one field to `graphClient`)
- Test: `pkg/msgraph/members_test.go`

**Interfaces:**
- Consumes: existing `graphClient`, `Config`, `Option`, `New`, `accessToken`, `getThrottled`, `tokenResponse` (all in `pkg/msgraph`).
- Produces (used by Tasks 3, 5, 6):
  - `msgraph.ChatMembersReader` with `ListChatMembers(ctx context.Context, chatID string) ([]ChatMemberDetail, error)`
  - `msgraph.NewChatMembersClient(cfg Config, opts ...Option) ChatMembersReader`
  - `msgraph.ChatMemberDetail{UserID, UserPrincipalName string; VisibleHistoryStartDateTime time.Time}`
  - `msgraph.WithMembersPageSize(n int) Option`

- [ ] **Step 1: Write the failing tests**

Create `pkg/msgraph/members_test.go` (mirror the httptest style of `chats_test.go`; the token stub reuses the package's unexported `tokenResponse`):

```go
package msgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMembersTokenServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok-mem", ExpiresIn: 3600}) // #nosec G117 -- test mock encodes a fake OAuth token response; dummy value, not a real secret
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestMembers(tokenURL, baseURL string, opts ...Option) ChatMembersReader {
	all := append([]Option{WithTokenURL(tokenURL), WithBaseURL(baseURL)}, opts...)
	return NewChatMembersClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s"}, all...)
}

func TestListChatMembers_Success_QueryShape(t *testing.T) {
	tokenSrv := newMembersTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer tok-mem", r.Header.Get("Authorization"))
		assert.Equal(t, "/chats/19:abc@thread.v2/members", r.URL.Path)
		q := r.URL.Query()
		assert.Equal(t, "userId,userPrincipalName,visibleHistoryStartDateTime", q.Get("$select"))
		assert.Equal(t, "50", q.Get("$top"), "default page size")
		_, _ = w.Write([]byte(`{"value":[
			{"userId":"u1","userPrincipalName":"Alice@corp.example","visibleHistoryStartDateTime":"2026-04-02T08:00:00Z"},
			{"userId":"u2","userPrincipalName":null,"visibleHistoryStartDateTime":"0001-01-01T00:00:00Z"}
		]}`))
	}))
	defer graphSrv.Close()

	members, err := newTestMembers(tokenSrv.URL, graphSrv.URL).
		ListChatMembers(context.Background(), "19:abc@thread.v2")
	require.NoError(t, err)
	require.Len(t, members, 2)
	assert.Equal(t, "u1", members[0].UserID)
	assert.Equal(t, "Alice@corp.example", members[0].UserPrincipalName)
	assert.Equal(t, "u2", members[1].UserID)
	assert.Equal(t, "", members[1].UserPrincipalName, "null UPN decodes to empty")
	assert.True(t, members[1].VisibleHistoryStartDateTime.IsZero())
}

func TestListChatMembers_CustomPageSize(t *testing.T) {
	tokenSrv := newMembersTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "10", r.URL.Query().Get("$top"))
		_, _ = w.Write([]byte(`{"value":[]}`))
	}))
	defer graphSrv.Close()

	_, err := newTestMembers(tokenSrv.URL, graphSrv.URL, WithMembersPageSize(10)).
		ListChatMembers(context.Background(), "19:abc@thread.v2")
	require.NoError(t, err)
}

func TestListChatMembers_FollowsNextLink(t *testing.T) {
	tokenSrv := newMembersTokenServer(t)
	var calls int
	var graphSrv *httptest.Server
	graphSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("$skiptoken") == "" {
			_, _ = fmt.Fprintf(w, `{"value":[{"userId":"u1","userPrincipalName":"a@x","visibleHistoryStartDateTime":"2026-04-02T08:00:00Z"}],
				"@odata.nextLink":"%s/chats/19:abc@thread.v2/members?$skiptoken=page2"}`, graphSrv.URL)
			return
		}
		_, _ = w.Write([]byte(`{"value":[{"userId":"u2","userPrincipalName":"b@x","visibleHistoryStartDateTime":"2026-04-03T08:00:00Z"}]}`))
	}))
	defer graphSrv.Close()

	members, err := newTestMembers(tokenSrv.URL, graphSrv.URL).
		ListChatMembers(context.Background(), "19:abc@thread.v2")
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	require.Len(t, members, 2)
	assert.Equal(t, "u1", members[0].UserID)
	assert.Equal(t, "u2", members[1].UserID)
}

func TestListChatMembers_RetriesOn429(t *testing.T) {
	tokenSrv := newMembersTokenServer(t)
	var calls int
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"value":[]}`))
	}))
	defer graphSrv.Close()

	_, err := newTestMembers(tokenSrv.URL, graphSrv.URL).
		ListChatMembers(context.Background(), "19:abc@thread.v2")
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
}

func TestListChatMembers_GraphError(t *testing.T) {
	tokenSrv := newMembersTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":"Forbidden","message":"nope"}}`))
	}))
	defer graphSrv.Close()

	_, err := newTestMembers(tokenSrv.URL, graphSrv.URL).
		ListChatMembers(context.Background(), "19:abc@thread.v2")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 403")
	assert.Contains(t, err.Error(), "Forbidden")
	assert.NotContains(t, err.Error(), "nope", "raw Graph message must not be surfaced")
}

func TestListChatMembers_EmptyChatID(t *testing.T) {
	tokenSrv := newMembersTokenServer(t)
	_, err := newTestMembers(tokenSrv.URL, "http://unused.invalid").
		ListChatMembers(context.Background(), "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chatID is required")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -race -run TestListChatMembers ./pkg/msgraph/`
Expected: FAIL to compile — `undefined: ChatMembersReader`, `undefined: NewChatMembersClient`, `undefined: WithMembersPageSize`.

- [ ] **Step 3: Add the `membersPageSize` field to `graphClient`**

In `pkg/msgraph/msgraph.go`, add a field to the `graphClient` struct right after the existing `chatsPageSize` field:

```go
	// membersPageSize is the $top for ListChatMembers first-page requests;
	// <= 0 means defaultMembersPageSize. Set via WithMembersPageSize.
	membersPageSize int
```

- [ ] **Step 4: Implement `pkg/msgraph/members.go`**

```go
package msgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// ChatMembersReader lists a chat's members. Consumed by teams-chat-member-sync;
// kept separate from ChatsReader so consumers depend only on the surface they
// use. App-only (Chat.Read.All / ChatMember.Read.All).
type ChatMembersReader interface {
	// ListChatMembers returns the chat's members, following @odata.nextLink
	// pagination. Throttled (429/503) responses are retried per Retry-After and
	// arm the shared tenant-wide gate, exactly like ListUserChats.
	ListChatMembers(ctx context.Context, chatID string) ([]ChatMemberDetail, error)
}

// NewChatMembersClient returns an app-only chat-members reader (shares the
// graph client; New always returns a *graphClient).
//
//nolint:gocritic // hugeParam: startup-only constructor; Config passed by value is intentional.
func NewChatMembersClient(cfg Config, opts ...Option) ChatMembersReader {
	return New(cfg, opts...).(*graphClient)
}

// ChatMemberDetail is the subset of an aadUserConversationMember returned by
// GET /chats/{id}/members. Only the UPN is consulted for the account; the
// Graph email field is intentionally not requested.
type ChatMemberDetail struct {
	UserID                      string    `json:"userId"`
	UserPrincipalName           string    `json:"userPrincipalName"`
	VisibleHistoryStartDateTime time.Time `json:"visibleHistoryStartDateTime"`
}

// defaultMembersPageSize is the $top sent on the first members request when no
// override is configured. Later pages follow @odata.nextLink.
const defaultMembersPageSize = 50

// WithMembersPageSize overrides the $top page size ListChatMembers requests.
// Values <= 0 fall back to defaultMembersPageSize.
func WithMembersPageSize(n int) Option {
	return func(g *graphClient) { g.membersPageSize = n }
}

func (g *graphClient) ListChatMembers(ctx context.Context, chatID string) ([]ChatMemberDetail, error) {
	if chatID == "" {
		return nil, fmt.Errorf("list chat members: chatID is required")
	}
	token, err := g.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire graph token: %w", err)
	}

	q := url.Values{}
	q.Set("$select", "userId,userPrincipalName,visibleHistoryStartDateTime")
	pageSize := g.membersPageSize
	if pageSize <= 0 {
		pageSize = defaultMembersPageSize
	}
	q.Set("$top", strconv.Itoa(pageSize))
	next := fmt.Sprintf("%s/chats/%s/members?%s", g.baseURL, url.PathEscape(chatID), q.Encode())

	var members []ChatMemberDetail
	for next != "" {
		body, err := g.getThrottled(ctx, token, next)
		if err != nil {
			return nil, err
		}
		var page struct {
			Value    []ChatMemberDetail `json:"value"`
			NextLink string             `json:"@odata.nextLink"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("decode chat members response: %w", err)
		}
		members = append(members, page.Value...)
		next = page.NextLink
	}
	return members, nil
}
```

Note: `getThrottled` wraps its errors as `"get chats: …"` — that string is generic enough to reuse here; do not fork it.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test -race -run TestListChatMembers ./pkg/msgraph/`
Expected: PASS. Then `make test SERVICE=pkg/msgraph`.

- [ ] **Step 6: Commit**

```bash
git add pkg/msgraph/members.go pkg/msgraph/members_test.go pkg/msgraph/msgraph.go
git commit -m "feat(msgraph): add ChatMembersReader for listing chat members"
```

---

### Task 3: teams-chat-member-sync store interfaces + Mongo impl (read/write split)

**Files:**
- Create: `teams-chat-member-sync/store.go`
- Create: `teams-chat-member-sync/store_mongo.go`
- Create: `teams-chat-member-sync/mock_store_test.go` (generated — never hand-edit)
- Test: `teams-chat-member-sync/store_mongo_test.go`

**Interfaces:**
- Consumes: `model.TeamsChat`, `model.TeamsChatMember`, `model.TeamsUser`; `mongoutil.Collection`, `mongoutil.WithProjection`; `msgraph.ChatMemberDetail` (Task 2).
- Produces (used by Tasks 4–7):
  - `TeamsChatStore{ ListChatsToSync(ctx) ([]string, error); SetMembersSynced(ctx, chatID string, members []model.TeamsChatMember, now time.Time) error }`
  - `TeamsUserStore{ AccountsByIDs(ctx, ids []string) (map[string]string, error) }`
  - `membersFetcher{ ListChatMembers(ctx, chatID string) ([]msgraph.ChatMemberDetail, error) }` (consumer-side Graph interface; mocked as `MockmembersFetcher`)
  - `newMongoStore(readDB, writeDB *mongo.Database) *mongoStore` (implements both stores)
  - `setMembersSyncedUpdate(members []model.TeamsChatMember, now time.Time) bson.M` (pure — the `$set` document builder)

- [ ] **Step 1: Write `store.go` (interfaces — needed for mocks and the failing test to compile)**

```go
package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// TeamsChatStore reads chats needing member sync and writes back the resolved
// member list. Satisfied by *mongoStore. ListChatsToSync uses the read client;
// SetMembersSynced the write client.
type TeamsChatStore interface {
	// ListChatsToSync returns the _id of every teams_chat with
	// needMemberSync=true.
	ListChatsToSync(ctx context.Context) ([]string, error)
	// SetMembersSynced replaces the chat's members and hands it to the next
	// stage: $set {members, needCreateRoom:true, needMemberSync:false,
	// updatedAt:now}.
	SetMembersSynced(ctx context.Context, chatID string, members []model.TeamsChatMember, now time.Time) error
}

// TeamsUserStore resolves userIds to accounts from teams_user (read client),
// for members whose Graph UPN was absent. Satisfied by *mongoStore.
type TeamsUserStore interface {
	// AccountsByIDs returns userId->account for the ids present in teams_user;
	// ids without a record are absent from the map.
	AccountsByIDs(ctx context.Context, ids []string) (map[string]string, error)
}

// membersFetcher is the Graph surface the sync consumes (interface defined in
// the consumer per repo convention; satisfied by msgraph.ChatMembersReader).
type membersFetcher interface {
	ListChatMembers(ctx context.Context, chatID string) ([]msgraph.ChatMemberDetail, error)
}
```

- [ ] **Step 2: Write the failing unit test for the update-document builder**

Create `teams-chat-member-sync/store_mongo_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

func TestSetMembersSyncedUpdate(t *testing.T) {
	now := time.Date(2026, 7, 15, 3, 4, 5, 0, time.UTC)
	members := []model.TeamsChatMember{
		{ID: "u1", Account: "alice", VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
		{ID: "guest", Account: ""},
	}
	update := setMembersSyncedUpdate(members, now)

	set, ok := update["$set"].(bson.M)
	require.True(t, ok, "update must have a $set bson.M")
	assert.Equal(t, members, set["members"])
	assert.Equal(t, true, set["needCreateRoom"])
	assert.Equal(t, false, set["needMemberSync"])
	assert.Equal(t, now, set["updatedAt"])
	assert.Len(t, set, 4, "$set writes exactly members, needCreateRoom, needMemberSync, updatedAt")
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test -race -run TestSetMembersSyncedUpdate ./teams-chat-member-sync/`
Expected: FAIL to compile — `undefined: setMembersSyncedUpdate`.

- [ ] **Step 4: Implement `store_mongo.go`**

```go
package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// mongoStore implements TeamsChatStore and TeamsUserStore over two databases:
// readDB (the teams_chat scan + teams_user resolution, typically a
// secondary-preferred client) and writeDB (the teams_chat member update).
type mongoStore struct {
	readChats  *mongoutil.Collection[model.TeamsChat]
	writeChats *mongoutil.Collection[model.TeamsChat]
	readUsers  *mongoutil.Collection[model.TeamsUser]
}

func newMongoStore(readDB, writeDB *mongo.Database) *mongoStore {
	return &mongoStore{
		readChats:  mongoutil.NewCollection[model.TeamsChat](readDB.Collection("teams_chat")),
		writeChats: mongoutil.NewCollection[model.TeamsChat](writeDB.Collection("teams_chat")),
		readUsers:  mongoutil.NewCollection[model.TeamsUser](readDB.Collection("teams_user")),
	}
}

// ListChatsToSync returns the _id of every teams_chat with needMemberSync=true.
// Served by the read client. FindMany decodes into TeamsChat with only _id
// projected (other fields zero); we read just the ID.
func (s *mongoStore) ListChatsToSync(ctx context.Context) ([]string, error) {
	chats, err := s.readChats.FindMany(ctx, bson.M{"needMemberSync": true},
		mongoutil.WithProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, fmt.Errorf("find chats needing member sync: %w", err)
	}
	ids := make([]string, 0, len(chats))
	for _, c := range chats {
		ids = append(ids, c.ID)
	}
	return ids, nil
}

// SetMembersSynced writes the resolved member list and advances the chat to the
// room-creation stage. Written by the write client.
func (s *mongoStore) SetMembersSynced(ctx context.Context, chatID string, members []model.TeamsChatMember, now time.Time) error {
	if _, err := s.writeChats.Raw().UpdateByID(ctx, chatID, setMembersSyncedUpdate(members, now)); err != nil {
		return fmt.Errorf("set chat members synced: %w", err)
	}
	return nil
}

// setMembersSyncedUpdate builds the $set document for a completed member sync.
func setMembersSyncedUpdate(members []model.TeamsChatMember, now time.Time) bson.M {
	return bson.M{"$set": bson.M{
		"members":        members,
		"needCreateRoom": true,
		"needMemberSync": false,
		"updatedAt":      now,
	}}
}

// AccountsByIDs resolves userIds to accounts from teams_user (read client),
// projecting _id and account. Ids without a record are absent from the map.
func (s *mongoStore) AccountsByIDs(ctx context.Context, ids []string) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	users, err := s.readUsers.FindMany(ctx, bson.M{"_id": bson.M{"$in": ids}},
		mongoutil.WithProjection(bson.M{"_id": 1, "account": 1}))
	if err != nil {
		return nil, fmt.Errorf("find teams users by id: %w", err)
	}
	for _, u := range users {
		out[u.ID] = u.Account
	}
	return out, nil
}
```

- [ ] **Step 5: Generate mocks and run the unit test**

Run: `make generate SERVICE=teams-chat-member-sync`
Expected: `teams-chat-member-sync/mock_store_test.go` created with `MockTeamsChatStore`, `MockTeamsUserStore`, `MockmembersFetcher`.

Run: `go test -race -run TestSetMembersSyncedUpdate ./teams-chat-member-sync/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add teams-chat-member-sync/store.go teams-chat-member-sync/store_mongo.go teams-chat-member-sync/store_mongo_test.go teams-chat-member-sync/mock_store_test.go
git commit -m "feat(teams-chat-member-sync): store interfaces and Mongo impl (read/write split)"
```

---

### Task 4: Mongo store integration tests

**Files:**
- Create: `teams-chat-member-sync/integration_test.go`

**Interfaces:**
- Consumes: `newMongoStore`, `TeamsChatStore`, `TeamsUserStore` (Task 3); `testutil.MongoDB(t, prefix)`, `testutil.RunTests(m)`.

- [ ] **Step 1: Write the integration tests**

Create `teams-chat-member-sync/integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func seedChats(t *testing.T, store *mongoStore, chats ...model.TeamsChat) {
	t.Helper()
	docs := make([]any, 0, len(chats))
	for _, c := range chats {
		docs = append(docs, c)
	}
	_, err := store.writeChats.Raw().InsertMany(context.Background(), docs)
	require.NoError(t, err)
}

func TestMongoStore_ListChatsToSync(t *testing.T) {
	db := testutil.MongoDB(t, "teamsmembersync")
	store := newMongoStore(db, db)
	seedChats(t, store,
		model.TeamsChat{ID: "19:need1", ChatType: "group", NeedMemberSync: true},
		model.TeamsChat{ID: "19:done1", ChatType: "group", NeedMemberSync: false},
		model.TeamsChat{ID: "19:need2", ChatType: "meeting", NeedMemberSync: true},
	)

	ids, err := store.ListChatsToSync(context.Background())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"19:need1", "19:need2"}, ids)
}

func TestMongoStore_SetMembersSynced(t *testing.T) {
	db := testutil.MongoDB(t, "teamsmembersync")
	store := newMongoStore(db, db)
	ctx := context.Background()
	seedChats(t, store, model.TeamsChat{
		ID: "19:g1", ChatType: "group", NeedMemberSync: true, NeedCreateRoom: false,
		Members:   []model.TeamsChatMember{{ID: "old", Account: "old"}},
		UpdatedAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
	})

	now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	members := []model.TeamsChatMember{
		{ID: "u1", Account: "alice", VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
		{ID: "u2", Account: "bob"},
	}
	require.NoError(t, store.SetMembersSynced(ctx, "19:g1", members, now))

	got, err := store.writeChats.FindByID(ctx, "19:g1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.NeedMemberSync, "needMemberSync cleared")
	assert.True(t, got.NeedCreateRoom, "needCreateRoom set")
	assert.True(t, got.UpdatedAt.Equal(now))
	require.Len(t, got.Members, 2, "members fully replaced")
	assert.Equal(t, "u1", got.Members[0].ID)
	assert.Equal(t, "alice", got.Members[0].Account)
	assert.True(t, got.Members[0].VisibleHistoryStartDateTime.Equal(members[0].VisibleHistoryStartDateTime))
}

func TestMongoStore_AccountsByIDs(t *testing.T) {
	db := testutil.MongoDB(t, "teamsmembersync")
	store := newMongoStore(db, db)
	ctx := context.Background()
	_, err := store.readUsers.Raw().InsertMany(ctx, []any{
		model.TeamsUser{ID: "u1", UPN: "alice@corp.example", Account: "alice", SiteID: "site-a"},
		model.TeamsUser{ID: "u2", UPN: "bob@corp.example", Account: "bob", SiteID: "site-b"},
	})
	require.NoError(t, err)

	got, err := store.AccountsByIDs(ctx, []string{"u1", "u2", "ghost"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"u1": "alice", "u2": "bob"}, got, "unknown id absent from map")

	empty, err := store.AccountsByIDs(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}
```

- [ ] **Step 2: Run the integration tests**

Run: `make test-integration SERVICE=teams-chat-member-sync` (requires Docker)
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add teams-chat-member-sync/integration_test.go
git commit -m "test(teams-chat-member-sync): Mongo store integration tests"
```

---

### Task 5: Sync core — account cache + member mapping

**Files:**
- Create: `teams-chat-member-sync/syncer.go`
- Test: `teams-chat-member-sync/syncer_test.go`

**Interfaces:**
- Consumes: `model.TeamsChat`/`TeamsChatMember`, `msgraph.ChatMemberDetail`, store interfaces (Task 3).
- Produces (used by Task 6):
  - `syncConfig{MaxWorkers int; Now func() time.Time}`
  - `newSyncer(chats TeamsChatStore, users TeamsUserStore, graph membersFetcher, cfg syncConfig) *syncer`
  - `accountFromUPN(upn string) string`
  - `newAccountCache(users TeamsUserStore) *accountCache`
  - `(*accountCache).resolve(ctx context.Context, ids []string) (map[string]string, error)`
  - `(*syncer).buildMembers(ctx context.Context, raw []msgraph.ChatMemberDetail) ([]model.TeamsChatMember, error)`

- [ ] **Step 1: Write the failing tests**

Create `teams-chat-member-sync/syncer_test.go`:

```go
package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

func TestAccountFromUPN(t *testing.T) {
	tests := []struct {
		name, upn, want string
	}{
		{"simple", "Alice@corp.example", "alice"},
		{"already lower", "bob@x.y", "bob"},
		{"empty", "", ""},
		{"no at", "noatsign", ""},
		{"leading at", "@corp.example", ""},
		{"dotted local", "a.b@corp.example", "a.b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, accountFromUPN(tc.upn))
		})
	}
}

func TestAccountCache_BatchesAndCachesHitsAndMisses(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	// First resolve: u1,u2 uncached -> one batched call. u2 unknown -> miss.
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) (map[string]string, error) {
			assert.ElementsMatch(t, []string{"u1", "u2"}, ids)
			return map[string]string{"u1": "alice"}, nil
		}).Times(1)

	c := newAccountCache(users)
	got, err := c.resolve(context.Background(), []string{"u1", "u2"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"u1": "alice", "u2": ""}, got, "miss cached as empty")

	// Second resolve of the same ids issues NO new query (mock capped at 1).
	got2, err := c.resolve(context.Background(), []string{"u1", "u2"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"u1": "alice", "u2": ""}, got2)
}

func TestAccountCache_OnlyQueriesUncached(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	gomock.InOrder(
		users.EXPECT().AccountsByIDs(gomock.Any(), []string{"u1"}).Return(map[string]string{"u1": "alice"}, nil),
		users.EXPECT().AccountsByIDs(gomock.Any(), []string{"u2"}).Return(map[string]string{"u2": "bob"}, nil),
	)
	c := newAccountCache(users)
	_, err := c.resolve(context.Background(), []string{"u1"})
	require.NoError(t, err)
	// u1 now cached; only u2 is queried.
	got, err := c.resolve(context.Background(), []string{"u1", "u2"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"u1": "alice", "u2": "bob"}, got)
}

func TestAccountCache_ConcurrentResolveNoRace(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) (map[string]string, error) {
			out := make(map[string]string, len(ids))
			for _, id := range ids {
				out[id] = "acct-" + id
			}
			return out, nil
		}).AnyTimes()

	c := newAccountCache(users)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.resolve(context.Background(), []string{"u1", "u2", "u3"})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

func newTestSyncer(t *testing.T, workers int) (*syncer, *MockTeamsChatStore, *MockTeamsUserStore, *MockmembersFetcher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	chats := NewMockTeamsChatStore(ctrl)
	users := NewMockTeamsUserStore(ctrl)
	graph := NewMockmembersFetcher(ctrl)
	s := newSyncer(chats, users, graph, syncConfig{MaxWorkers: workers, Now: func() time.Time {
		return time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	}})
	return s, chats, users, graph
}

func TestBuildMembers_UPNPresentNoLookup(t *testing.T) {
	s, _, users, _ := newTestSyncer(t, 1)
	// No AccountsByIDs call expected: every member has a UPN.
	_ = users
	raw := []msgraph.ChatMemberDetail{
		{UserID: "u1", UserPrincipalName: "Alice@corp.example", VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
		{UserID: "u2", UserPrincipalName: "Bob@corp.example"},
	}
	got, err := s.buildMembers(context.Background(), raw)
	require.NoError(t, err)
	assert.Equal(t, []model.TeamsChatMember{
		{ID: "u1", Account: "alice", VisibleHistoryStartDateTime: raw[0].VisibleHistoryStartDateTime},
		{ID: "u2", Account: "bob"},
	}, got)
}

func TestBuildMembers_MissingUPNFallsBackToLookup(t *testing.T) {
	s, _, users, _ := newTestSyncer(t, 1)
	// The two UPN-less members (u2, ghost) are resolved in one batched call.
	// ghost is not in teams_user, so it comes back absent -> account "".
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) (map[string]string, error) {
			assert.ElementsMatch(t, []string{"u2", "ghost"}, ids)
			return map[string]string{"u2": "bob"}, nil
		})
	raw := []msgraph.ChatMemberDetail{
		{UserID: "u1", UserPrincipalName: "alice@corp.example"}, // UPN present -> no lookup
		{UserID: "u2", UserPrincipalName: ""},                   // no UPN -> lookup hit
		{UserID: "ghost", UserPrincipalName: ""},                // no UPN, unknown -> ""
	}
	got, err := s.buildMembers(context.Background(), raw)
	require.NoError(t, err)
	assert.Equal(t, []model.TeamsChatMember{
		{ID: "u1", Account: "alice"},
		{ID: "u2", Account: "bob"},
		{ID: "ghost", Account: ""},
	}, got)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -race -run 'TestAccountFromUPN|TestAccountCache|TestBuildMembers' ./teams-chat-member-sync/`
Expected: FAIL to compile — `undefined: accountFromUPN`, `undefined: newAccountCache`, `undefined: newSyncer`.

- [ ] **Step 3: Implement the sync core in `syncer.go`**

```go
package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// syncConfig carries the orchestration knobs. Now is injectable for tests.
type syncConfig struct {
	MaxWorkers int
	Now        func() time.Time
}

// syncer runs one full member sync.
type syncer struct {
	chats TeamsChatStore
	users TeamsUserStore
	graph membersFetcher
	cfg   syncConfig
	cache *accountCache
}

func newSyncer(chats TeamsChatStore, users TeamsUserStore, graph membersFetcher, cfg syncConfig) *syncer {
	return &syncer{chats: chats, users: users, graph: graph, cfg: cfg, cache: newAccountCache(users)}
}

// accountFromUPN returns the lowercased local part of a UPN (text before '@'),
// or "" when the UPN is empty or has no local part. Matches teams-user-sync's
// account derivation.
func accountFromUPN(upn string) string {
	at := strings.Index(upn, "@")
	if at <= 0 {
		return ""
	}
	return strings.ToLower(upn[:at])
}

// accountCache is a process-wide userId->account cache shared by all workers.
// It batches uncached ids into a single AccountsByIDs query and caches misses
// (as "") so each userId is resolved at most once per run.
type accountCache struct {
	users TeamsUserStore
	mu    sync.Mutex
	cache map[string]string
}

func newAccountCache(users TeamsUserStore) *accountCache {
	return &accountCache{users: users, cache: make(map[string]string)}
}

// resolve returns account for each requested id, querying teams_user only for
// ids not already cached and caching every result including misses.
func (c *accountCache) resolve(ctx context.Context, ids []string) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	var missing []string

	c.mu.Lock()
	for _, id := range ids {
		if acct, ok := c.cache[id]; ok {
			out[id] = acct
		} else {
			missing = append(missing, id)
		}
	}
	c.mu.Unlock()

	if len(missing) == 0 {
		return out, nil
	}
	found, err := c.users.AccountsByIDs(ctx, missing)
	if err != nil {
		return nil, fmt.Errorf("resolve accounts: %w", err)
	}

	c.mu.Lock()
	for _, id := range missing {
		acct := found[id] // "" when absent — a cached miss
		c.cache[id] = acct
		out[id] = acct
	}
	c.mu.Unlock()
	return out, nil
}

// buildMembers maps Graph members to stored members. UPN-carrying members
// resolve locally; the rest are resolved in one batched, cached teams_user
// lookup. Unknown members keep account "".
func (s *syncer) buildMembers(ctx context.Context, raw []msgraph.ChatMemberDetail) ([]model.TeamsChatMember, error) {
	accounts := make(map[string]string, len(raw)) // userID -> account, for UPN-less members
	var needLookup []string
	for _, m := range raw {
		if acct := accountFromUPN(m.UserPrincipalName); acct != "" {
			accounts[m.UserID] = acct
			continue
		}
		needLookup = append(needLookup, m.UserID)
	}
	if len(needLookup) > 0 {
		resolved, err := s.cache.resolve(ctx, needLookup)
		if err != nil {
			return nil, err
		}
		for id, acct := range resolved {
			accounts[id] = acct
		}
	}

	members := make([]model.TeamsChatMember, 0, len(raw))
	for _, m := range raw {
		members = append(members, model.TeamsChatMember{
			ID:                          m.UserID,
			Account:                     accounts[m.UserID],
			VisibleHistoryStartDateTime: m.VisibleHistoryStartDateTime,
		})
	}
	return members, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race -run 'TestAccountFromUPN|TestAccountCache|TestBuildMembers' ./teams-chat-member-sync/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add teams-chat-member-sync/syncer.go teams-chat-member-sync/syncer_test.go
git commit -m "feat(teams-chat-member-sync): account cache + member mapping"
```

---

### Task 6: Worker pool — run() and syncChat()

**Files:**
- Modify: `teams-chat-member-sync/syncer.go` (append)
- Test: `teams-chat-member-sync/worker_test.go`

**Interfaces:**
- Consumes: everything from Tasks 3 and 5; mocks `MockTeamsChatStore`, `MockTeamsUserStore`, `MockmembersFetcher`; test helper `newTestSyncer` (Task 5).
- Produces (used by Task 7): `(*syncer).run(ctx context.Context) error` — non-nil when any chat failed.

- [ ] **Step 1: Write the failing tests**

Create `teams-chat-member-sync/worker_test.go`:

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

var wtNow = time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)

func upnMember(id, upn string) msgraph.ChatMemberDetail {
	return msgraph.ChatMemberDetail{UserID: id, UserPrincipalName: upn}
}

func TestRun_HappyPath(t *testing.T) {
	s, chats, _, graph := newTestSyncer(t, 2)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]string{"19:g1"}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:g1").
		Return([]msgraph.ChatMemberDetail{upnMember("u1", "alice@x"), upnMember("u2", "bob@x")}, nil)
	chats.EXPECT().SetMembersSynced(gomock.Any(), "19:g1", gomock.Len(2), wtNow).DoAndReturn(
		func(_ context.Context, _ string, members []model.TeamsChatMember, _ time.Time) error {
			assert.Equal(t, "alice", members[0].Account)
			assert.Equal(t, "bob", members[1].Account)
			return nil
		})

	require.NoError(t, s.run(context.Background()))
}

func TestRun_NoChatsIsNoOp(t *testing.T) {
	s, chats, _, _ := newTestSyncer(t, 1)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return(nil, nil)
	require.NoError(t, s.run(context.Background()))
}

func TestRun_GraphFailureKeepsFlagAndFailsRun(t *testing.T) {
	s, chats, _, graph := newTestSyncer(t, 1)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]string{"19:bad", "19:ok"}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:bad").
		Return(nil, fmt.Errorf("graph returned status 429"))
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:ok").
		Return([]msgraph.ChatMemberDetail{upnMember("u1", "a@x")}, nil)
	chats.EXPECT().SetMembersSynced(gomock.Any(), "19:ok", gomock.Len(1), wtNow).Return(nil)
	// No SetMembersSynced for 19:bad: its needMemberSync must stay true.

	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 2 chats failed")
}

func TestRun_WriteFailureFailsChat(t *testing.T) {
	s, chats, _, graph := newTestSyncer(t, 1)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]string{"19:g1"}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:g1").
		Return([]msgraph.ChatMemberDetail{upnMember("u1", "a@x")}, nil)
	chats.EXPECT().SetMembersSynced(gomock.Any(), "19:g1", gomock.Any(), wtNow).
		Return(fmt.Errorf("mongo down"))

	require.Error(t, s.run(context.Background()))
}

func TestRun_ListChatsFailure(t *testing.T) {
	s, chats, _, _ := newTestSyncer(t, 1)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return(nil, fmt.Errorf("mongo down"))
	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load chats")
}

func TestRun_SharedMemberResolvedOncePerRun(t *testing.T) {
	s, chats, users, graph := newTestSyncer(t, 4)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]string{"19:a", "19:b"}, nil)
	// Both chats contain the same UPN-less member u9.
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:a").
		Return([]msgraph.ChatMemberDetail{{UserID: "u9"}}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:b").
		Return([]msgraph.ChatMemberDetail{{UserID: "u9"}}, nil)
	var calls int
	var mu sync.Mutex
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) (map[string]string, error) {
			mu.Lock()
			calls++
			mu.Unlock()
			return map[string]string{"u9": "nine"}, nil
		}).MaxTimes(2) // cache dedups; with a concurrency window it is 1, worst case 2 — never per-chat unbounded
	chats.EXPECT().SetMembersSynced(gomock.Any(), gomock.Any(), gomock.Len(1), wtNow).Return(nil).Times(2)

	require.NoError(t, s.run(context.Background()))
	mu.Lock()
	assert.LessOrEqual(t, calls, 2)
	mu.Unlock()
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -race -run TestRun_ ./teams-chat-member-sync/`
Expected: FAIL to compile — `s.run undefined`.

- [ ] **Step 3: Append run/syncChat to `syncer.go`**

Add `"log/slog"` and `"sync/atomic"` to the imports, then append:

```go
// summary is the per-run outcome. Total is written only by the dispatching
// goroutine; the atomics are updated by workers.
type summary struct {
	Total             int
	Succeeded, Failed atomic.Int64
	MembersWritten    atomic.Int64
}

// run executes one full member sync: load the flagged chats, fan them out to
// MaxWorkers workers, wait, and report. It returns an error when any chat
// failed so main exits non-zero and the CronJob records the failure.
func (s *syncer) run(ctx context.Context) error {
	ids, err := s.chats.ListChatsToSync(ctx)
	if err != nil {
		return fmt.Errorf("load chats needing member sync: %w", err)
	}

	var sum summary
	sum.Total = len(ids)

	jobs := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < s.cfg.MaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chatID := range jobs {
				if err := s.syncChat(ctx, chatID, &sum); err != nil {
					sum.Failed.Add(1)
					slog.Error("teams chat member sync: chat failed", "chatID", chatID, "error", err)
					continue
				}
				sum.Succeeded.Add(1)
			}
		}()
	}
	for _, id := range ids {
		jobs <- id
	}
	close(jobs)
	wg.Wait()

	slog.Info("teams chat member sync: run complete",
		"chatsTotal", sum.Total, "chatsSucceeded", sum.Succeeded.Load(),
		"chatsFailed", sum.Failed.Load(), "membersWritten", sum.MembersWritten.Load())

	if failed := sum.Failed.Load(); failed > 0 {
		return fmt.Errorf("%d of %d chats failed", failed, sum.Total)
	}
	return nil
}

// syncChat fetches one chat's members, resolves accounts, and writes the list
// back. On any error the chat's needMemberSync is left true (no SetMembersSynced)
// so it is retried next run.
func (s *syncer) syncChat(ctx context.Context, chatID string, sum *summary) error {
	raw, err := s.graph.ListChatMembers(ctx, chatID)
	if err != nil {
		return fmt.Errorf("list chat members: %w", err)
	}
	members, err := s.buildMembers(ctx, raw)
	if err != nil {
		return fmt.Errorf("build members: %w", err)
	}
	if err := s.chats.SetMembersSynced(ctx, chatID, members, s.cfg.Now()); err != nil {
		return fmt.Errorf("set members synced: %w", err)
	}
	sum.MembersWritten.Add(int64(len(members)))
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race -run 'TestRun_|TestAccountCache|TestBuildMembers' ./teams-chat-member-sync/`
Expected: PASS (the `-race` flag matters — the shared-member test exercises real concurrency on the cache).

- [ ] **Step 5: Commit**

```bash
git add teams-chat-member-sync/syncer.go teams-chat-member-sync/worker_test.go
git commit -m "feat(teams-chat-member-sync): worker pool with per-chat retry semantics"
```

---

### Task 7: main.go — config and wiring

**Files:**
- Create: `teams-chat-member-sync/main.go`
- Test: `teams-chat-member-sync/main_test.go`

**Interfaces:**
- Consumes: `newMongoStore` (Task 3), `newSyncer`/`syncConfig`/`(*syncer).run` (Tasks 5–6), `msgraph.NewChatMembersClient` + `WithMembersPageSize` (Task 2), `mongoutil.Connect`/`ConnectRead`/`Disconnect`.
- Produces: the `teams-chat-member-sync` binary.

- [ ] **Step 1: Write the failing config tests**

Create `teams-chat-member-sync/main_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("MONGO_READ_URI", "mongodb://localhost:27017")
	t.Setenv("MONGO_WRITE_URI", "mongodb://localhost:27017")
	t.Setenv("GRAPH_TENANT_ID", "tenant")
	t.Setenv("GRAPH_CLIENT_ID", "client")
	t.Setenv("GRAPH_CLIENT_SECRET", "secret")
}

func TestConfig_Defaults(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Equal(t, "chat", cfg.MongoReadDB)
	assert.Equal(t, "chat", cfg.MongoWriteDB)
	assert.Equal(t, 8, cfg.MaxWorkers)
	assert.Equal(t, 30*time.Minute, cfg.RunTimeout)
	assert.Equal(t, 50, cfg.GraphMembersPageSize)
	assert.False(t, cfg.GraphTLSInsecureSkipVerify)
}

func TestConfig_MissingRequired(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MONGO_WRITE_URI", "") // required,notEmpty
	_, err := env.ParseAs[Config]()
	require.Error(t, err)
}

func baseConfig() Config {
	return Config{
		MongoReadURI: "mongodb://localhost:27017", MongoReadDB: "chat",
		MongoWriteURI: "mongodb://localhost:27017", MongoWriteDB: "chat",
		MaxWorkers: 8, RunTimeout: 30 * time.Minute,
		GraphTenantID: "tenant", GraphClientID: "client", GraphClientSecret: "secret",
		GraphMembersPageSize: 50,
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(c *Config) {}, false},
		{"zero max workers", func(c *Config) { c.MaxWorkers = 0 }, true},
		{"negative run timeout", func(c *Config) { c.RunTimeout = -time.Second }, true},
		{"zero page size", func(c *Config) { c.GraphMembersPageSize = 0 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.mutate(&cfg)
			err := validateConfig(cfg)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -race -run 'TestConfig_|TestValidateConfig' ./teams-chat-member-sync/`
Expected: FAIL to compile — `undefined: Config`.

- [ ] **Step 3: Implement `main.go`**

```go
// Command teams-chat-member-sync is a run-to-completion job (k8s CronJob) that
// resolves the authoritative member list for teams_chat documents flagged
// needMemberSync=true, then hands them to the room-creation stage by setting
// needCreateRoom=true.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// Config is the job's environment configuration.
type Config struct {
	// Mongo traffic is split into a read client (teams_chat scan + teams_user
	// resolution, secondary-preferred) and a write client (teams_chat member
	// updates), mirroring the sibling teams-chat-sync job.
	MongoReadURI      string `env:"MONGO_READ_URI,required,notEmpty"`
	MongoReadUsername string `env:"MONGO_READ_USERNAME" envDefault:""`
	MongoReadPassword string `env:"MONGO_READ_PASSWORD" envDefault:""`
	MongoReadDB       string `env:"MONGO_READ_DB" envDefault:"chat"`

	MongoWriteURI      string `env:"MONGO_WRITE_URI,required,notEmpty"`
	MongoWriteUsername string `env:"MONGO_WRITE_USERNAME" envDefault:""`
	MongoWritePassword string `env:"MONGO_WRITE_PASSWORD" envDefault:""`
	MongoWriteDB       string `env:"MONGO_WRITE_DB" envDefault:"chat"`

	MaxWorkers int           `env:"MAX_WORKERS" envDefault:"8"`
	RunTimeout time.Duration `env:"RUN_TIMEOUT" envDefault:"30m"`

	GraphTenantID     string `env:"GRAPH_TENANT_ID,required"`
	GraphClientID     string `env:"GRAPH_CLIENT_ID,required"`
	GraphClientSecret string `env:"GRAPH_CLIENT_SECRET,required"`
	// GraphMembersPageSize is the $top page size for GET /chats/{id}/members.
	GraphMembersPageSize int `env:"GRAPH_MEMBERS_PAGE_SIZE" envDefault:"50"`
	// GraphTLSInsecureSkipVerify disables Graph TLS verification (opt-in,
	// default false) for dev/on-prem environments behind a TLS-intercepting
	// proxy. The proxy is taken from HTTPS_PROXY/HTTP_PROXY.
	GraphTLSInsecureSkipVerify bool `env:"GRAPH_TLS_INSECURE_SKIP_VERIFY" envDefault:"false"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("teams-chat-member-sync failed", "error", err)
		os.Exit(1)
	}
}

// validateConfig checks the parsed Config for internal consistency. It isolates
// run()'s pure logic so it is unit testable without wiring any dependency.
//
//nolint:gocritic // hugeParam: cfg is passed by value once at startup; not a hot path
func validateConfig(cfg Config) error {
	if cfg.MaxWorkers <= 0 || cfg.RunTimeout <= 0 {
		return fmt.Errorf("invalid config: MAX_WORKERS and RUN_TIMEOUT must be positive")
	}
	if cfg.GraphMembersPageSize <= 0 {
		return fmt.Errorf("invalid config: GRAPH_MEMBERS_PAGE_SIZE must be positive")
	}
	return nil
}

// run wires dependencies and performs one sync. It returns an error rather than
// calling os.Exit so deferred cleanup always runs.
func run() error {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if err := validateConfig(cfg); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.RunTimeout)
	defer cancel()

	readClient, err := mongoutil.ConnectRead(ctx, cfg.MongoReadURI, cfg.MongoReadUsername, cfg.MongoReadPassword)
	if err != nil {
		return fmt.Errorf("mongo read connect: %w", err)
	}
	defer mongoutil.Disconnect(context.Background(), readClient)

	writeClient, err := mongoutil.Connect(ctx, cfg.MongoWriteURI, cfg.MongoWriteUsername, cfg.MongoWritePassword)
	if err != nil {
		return fmt.Errorf("mongo write connect: %w", err)
	}
	defer mongoutil.Disconnect(context.Background(), writeClient)

	store := newMongoStore(readClient.Database(cfg.MongoReadDB), writeClient.Database(cfg.MongoWriteDB))

	graph := msgraph.NewChatMembersClient(msgraph.Config{
		TenantID:              cfg.GraphTenantID,
		ClientID:              cfg.GraphClientID,
		ClientSecret:          cfg.GraphClientSecret,
		TLSInsecureSkipVerify: cfg.GraphTLSInsecureSkipVerify,
	}, msgraph.WithMembersPageSize(cfg.GraphMembersPageSize))

	s := newSyncer(store, store, graph, syncConfig{
		MaxWorkers: cfg.MaxWorkers,
		Now:        time.Now,
	})
	if err := s.run(ctx); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	slog.Info("teams-chat-member-sync done")
	return nil
}
```

- [ ] **Step 4: Run tests and build**

Run: `go test -race -run 'TestConfig_|TestValidateConfig' ./teams-chat-member-sync/`
Expected: PASS.
Run: `make build SERVICE=teams-chat-member-sync`
Expected: builds `bin/teams-chat-member-sync`.

- [ ] **Step 5: Commit**

```bash
git add teams-chat-member-sync/main.go teams-chat-member-sync/main_test.go
git commit -m "feat(teams-chat-member-sync): config and job wiring"
```

---

### Task 8: Deploy files, full verification, push

**Files:**
- Create: `teams-chat-member-sync/deploy/Dockerfile`
- Create: `teams-chat-member-sync/deploy/docker-compose.yml`
- Create: `teams-chat-member-sync/deploy/azure-pipelines.yml`

**Interfaces:**
- Consumes: the finished service. Produces: CI/CD + local-dev packaging.

- [ ] **Step 1: Write `teams-chat-member-sync/deploy/Dockerfile`**

```dockerfile
FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY teams-chat-member-sync/ teams-chat-member-sync/
RUN CGO_ENABLED=0 go build -o /teams-chat-member-sync ./teams-chat-member-sync/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /teams-chat-member-sync /teams-chat-member-sync
USER app
ENTRYPOINT ["/teams-chat-member-sync"]
```

- [ ] **Step 2: Write `teams-chat-member-sync/deploy/docker-compose.yml`**

```yaml
name: teams-chat-member-sync

services:
  teams-chat-member-sync:
    build:
      context: ../..
      dockerfile: teams-chat-member-sync/deploy/Dockerfile
    environment:
      - MONGO_READ_URI=${MONGO_READ_URI:-mongodb://mongo:27017}
      - MONGO_READ_DB=${MONGO_READ_DB:-chat}
      - MONGO_WRITE_URI=${MONGO_WRITE_URI:-mongodb://mongo:27017}
      - MONGO_WRITE_DB=${MONGO_WRITE_DB:-chat}
      - MAX_WORKERS=${MAX_WORKERS:-8}
      - RUN_TIMEOUT=${RUN_TIMEOUT:-30m}
      - GRAPH_TENANT_ID=${GRAPH_TENANT_ID}
      - GRAPH_CLIENT_ID=${GRAPH_CLIENT_ID}
      - GRAPH_CLIENT_SECRET=${GRAPH_CLIENT_SECRET}
      - GRAPH_MEMBERS_PAGE_SIZE=${GRAPH_MEMBERS_PAGE_SIZE:-50}
      - GRAPH_TLS_INSECURE_SKIP_VERIFY=${GRAPH_TLS_INSECURE_SKIP_VERIFY:-false}
      - HTTPS_PROXY=${HTTPS_PROXY:-}
      - HTTP_PROXY=${HTTP_PROXY:-}
      - NO_PROXY=${NO_PROXY:-}
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

- [ ] **Step 3: Write `teams-chat-member-sync/deploy/azure-pipelines.yml`**

```yaml
trigger:
  branches:
    include:
      - main
      - develop
  paths:
    include:
      - teams-chat-member-sync/
      - pkg/

pr:
  branches:
    include:
      - main
  paths:
    include:
      - teams-chat-member-sync/
      - pkg/

variables:
  GO_VERSION: '1.25.12'
  SERVICE_DIR: teams-chat-member-sync
  IMAGE_NAME: teams-chat-member-sync
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

          - script: go vet ./$(SERVICE_DIR)/... ./pkg/...
            displayName: 'Go Vet'

          - script: go test ./pkg/... -v -race -coverprofile=coverage-pkg.out
            displayName: 'Test shared packages'

          - script: go test ./$(SERVICE_DIR)/... -v -race -coverprofile=coverage-svc.out
            displayName: 'Test $(IMAGE_NAME)'

          - script: go build -o /dev/null ./$(SERVICE_DIR)/
            displayName: 'Build $(IMAGE_NAME)'

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
              repository: 'chat/$(IMAGE_NAME)'
              command: 'buildAndPush'
              Dockerfile: '$(SERVICE_DIR)/deploy/Dockerfile'
              buildContext: '.'
              tags: |
                $(Build.BuildId)
                latest
            displayName: 'Build & push $(IMAGE_NAME)'
```

- [ ] **Step 4: Full verification**

```bash
make fmt
make lint
make test SERVICE=teams-chat-member-sync
make test SERVICE=pkg/msgraph
make test SERVICE=pkg/model
make test-integration SERVICE=teams-chat-member-sync
go test -coverprofile=/tmp/claude-0/-home-user-chat/685e6338-ff2b-5cca-b409-4823ae2eae26/scratchpad/cover-member.out ./teams-chat-member-sync/ && go tool cover -func=/tmp/claude-0/-home-user-chat/685e6338-ff2b-5cca-b409-4823ae2eae26/scratchpad/cover-member.out | tail -1
make sast
```

Expected: all green; business logic (`syncer.go`, `store_mongo.go` builder) ≥ 80% — the only uncovered code is `main()`/`run()` wiring, consistent with the sibling jobs. `make sast`: `gosec` must pass; `govulncheck`/`semgrep` may be blocked by network policy in the sandbox and are confirmed by CI.

- [ ] **Step 5: Commit and push**

```bash
git add teams-chat-member-sync/deploy/
git commit -m "build(teams-chat-member-sync): Dockerfile, compose, CI pipeline"
git push origin claude/teams-chat-sync-service-9ispb0
```

(If push fails on a network error, retry up to 4 times with 2s/4s/8s/16s backoff. Do NOT open a new PR — this rides the existing combined PR #71.)
