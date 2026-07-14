# teams-chat-sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A run-to-completion job (triggered by a k8s CronJob) that mirrors Microsoft Teams chats into the `teams_chat` MongoDB collection, driven by per-user watermarks in the externally-populated `teams_user` collection.

**Architecture:** One global job. It loads all `teams_user` docs into an in-memory cache, fans users out to `MAX_WORKERS` goroutines that call Graph `GET /users/{uid}/chats` (app-only auth, window `(from, startOfDayUTC(now))`), dedups chats across workers via a mutex-guarded claimed set, resolves each chat's `siteID` by member-majority vote, bulk-upserts into `teams_chat` (`siteID` and `createdDateTime` are `$setOnInsert`-only; `oneOnOne` chats are insert-only), then advances each user's `from` watermark only after that user fully succeeded.

**Tech Stack:** Go 1.25, `pkg/msgraph` (extended with a `ChatsReader`), MongoDB via `pkg/mongoutil`, `caarlos0/env`, `log/slog`, `otelutil`, mockgen + testify, testcontainers via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-07-14-teams-chat-sync-design.md`

## Global Constraints

- Branch: all commits go to `claude/teams-chat-sync-service-9ispb0`; never push elsewhere.
- Before the first commit run: `git config user.email noreply@anthropic.com && git config user.name Claude`.
- Every commit message ends with these two trailer lines:
  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_013cXnGa3vcWcJbZeGFNH5pL
  ```
- Use `make` targets, never raw `go` commands, EXCEPT single-test runs during TDD cycles where `go test -race -run <Name> ./<pkg>/` is the only way to scope a run (the Makefile has no per-test granularity).
- All new code follows Red-Green-Refactor: write the failing test, see it fail, implement, see it pass, commit.
- BSON/JSON tags: this feature intentionally uses `siteID` (capital D) for `TeamsUser`/`TeamsChat` — the external `teams_user` writer already uses that key — even though older models use `siteId`. Do not "fix" it.
- All time math in UTC. Graph filter timestamps are RFC3339 UTC.
- Errors: raw `fmt.Errorf("what this fn was doing: %w", err)` everywhere (no `pkg/errcode` — this job has no client boundary). Never log the client secret, tokens, or raw Graph response bodies.
- `log/slog` JSON only, structured key-value fields.
- No NATS, no `SITE_ID` env — the job is global and talks only to MongoDB and Graph.
- No `docs/client-api.md` changes: no client-facing handler and no client-facing `pkg/model` struct is touched (TeamsUser/TeamsChat are internal sync models).
- Coverage floor 80% for the new service package and the new msgraph code.

---

### Task 1: pkg/model Teams sync models

**Files:**
- Modify: `pkg/model/teams.go` (append at end)
- Test: `pkg/model/model_test.go` (append at end)

**Interfaces:**
- Consumes: nothing new.
- Produces (used by Tasks 3–7):
  - `model.TeamsUser{ID, SiteID, Account string; From *time.Time}`
  - `model.TeamsChat{ID, Name, ChatType string; CreatedDateTime, LastUpdatedDateTime time.Time; Members []TeamsChatMember; SiteID string; UpdatedAt time.Time; NeedUserSync bool}`
  - `model.TeamsChatMember{ID, Account string; VisibleHistoryStartDateTime time.Time}`
  - `model.TeamsChatTypeOneOnOne = "oneOnOne"`

- [ ] **Step 1: Write the failing round-trip tests**

First read the existing `roundTrip` helper at the bottom of `pkg/model/model_test.go` to confirm its signature is `roundTrip(t, src, dst)` (marshal src → unmarshal into dst → compare, for both JSON and BSON). Use whole-second UTC times so BSON's millisecond precision cannot cause mismatches. Append to `pkg/model/model_test.go`:

```go
func TestTeamsUserJSON(t *testing.T) {
	from := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	u := model.TeamsUser{ID: "aad-user-1", SiteID: "site-a", Account: "alice", From: &from}
	roundTrip(t, &u, &model.TeamsUser{})
}

func TestTeamsUserJSON_NoFrom(t *testing.T) {
	u := model.TeamsUser{ID: "aad-user-2", SiteID: "site-b", Account: "bob"}
	data, err := json.Marshal(&u)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, has := raw["from"]
	assert.False(t, has, "nil From must be omitted from JSON")
}

func TestTeamsChatJSON(t *testing.T) {
	c := model.TeamsChat{
		ID:                  "19:meeting_abc@thread.v2",
		Name:                "Project X",
		ChatType:            "group",
		CreatedDateTime:     time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 12, 30, 0, 0, time.UTC),
		Members: []model.TeamsChatMember{
			{ID: "aad-user-1", Account: "alice", VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC)},
			{ID: "aad-guest-9", Account: "", VisibleHistoryStartDateTime: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)},
		},
		SiteID:       "site-a",
		UpdatedAt:    time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC),
		NeedUserSync: true,
	}
	roundTrip(t, &c, &model.TeamsChat{})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -race -run 'TestTeams(User|Chat)JSON' ./pkg/model/`
Expected: FAIL to compile — `undefined: model.TeamsUser` etc.

- [ ] **Step 3: Write the models**

Append to `pkg/model/teams.go`:

```go
// TeamsUser mirrors one document of the externally-populated teams_user
// collection. teams-chat-sync reads all fields and writes only From — the
// per-user sync watermark, advanced to startOfDay(now, UTC) after a fully
// successful sync of that user.
type TeamsUser struct {
	ID      string     `json:"id" bson:"_id"` // Teams (AAD) user object id
	SiteID  string     `json:"siteID" bson:"siteID"`
	Account string     `json:"account" bson:"account"`
	From    *time.Time `json:"from,omitempty" bson:"from,omitempty"`
}

// TeamsChatTypeOneOnOne is Graph's chatType for 1:1 chats. A oneOnOne chat
// never changes after creation, so the sync inserts it once and never updates.
const TeamsChatTypeOneOnOne = "oneOnOne"

// TeamsChatMember is one member of a synced Teams chat. Account is empty when
// the member is not in teams_user (guests / users outside the system).
type TeamsChatMember struct {
	ID                          string    `json:"id" bson:"id"`
	Account                     string    `json:"account" bson:"account"`
	VisibleHistoryStartDateTime time.Time `json:"visibleHistoryStartDateTime" bson:"visibleHistoryStartDateTime"`
}

// TeamsChat is one document of the teams_chat collection, owned by
// teams-chat-sync. SiteID is the member-majority site, written only on insert
// ($setOnInsert) and never changed afterwards.
type TeamsChat struct {
	ID                  string            `json:"id" bson:"_id"` // Graph chat id
	Name                string            `json:"name" bson:"name"`
	ChatType            string            `json:"chatType" bson:"chatType"` // oneOnOne | group | meeting
	CreatedDateTime     time.Time         `json:"createdDateTime" bson:"createdDateTime"`
	LastUpdatedDateTime time.Time         `json:"lastUpdatedDateTime" bson:"lastUpdatedDateTime"`
	Members             []TeamsChatMember `json:"members" bson:"members"`
	SiteID              string            `json:"siteID" bson:"siteID"`
	UpdatedAt           time.Time         `json:"updatedAt" bson:"updatedAt"`
	NeedUserSync        bool              `json:"needUserSync" bson:"needUserSync"`
}
```

Add `"time"` to the imports of `pkg/model/teams.go` (currently it has none).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race -run 'TestTeams(User|Chat)JSON' ./pkg/model/`
Expected: PASS. Then `make test SERVICE=pkg/model` to confirm nothing else broke.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/teams.go pkg/model/model_test.go
git commit -m "feat(model): add TeamsUser/TeamsChat sync models"
```
(Append the two Global Constraints trailers to this and every commit message.)

---

### Task 2: pkg/msgraph ChatsReader (list user chats, pagination, throttle retry)

**Files:**
- Create: `pkg/msgraph/chats.go`
- Test: `pkg/msgraph/chats_test.go`

**Interfaces:**
- Consumes: existing `graphClient`, `Config`, `Option`, `New`, `accessToken` (all in `pkg/msgraph/msgraph.go`).
- Produces (used by Tasks 3, 5–7):
  - `msgraph.ChatsReader` with `ListUserChats(ctx context.Context, userID string, from, to time.Time) ([]Chat, error)`
  - `msgraph.NewChatsClient(cfg Config, opts ...Option) ChatsReader`
  - `msgraph.Chat{ID, ChatType, Topic string; CreatedDateTime, LastUpdatedDateTime time.Time; Members []ChatMember}`
  - `msgraph.ChatMember{UserID string; VisibleHistoryStartDateTime time.Time}`

- [ ] **Step 1: Write the failing tests**

Create `pkg/msgraph/chats_test.go`. Follow the existing httptest style in `msgraph_test.go` (stub token + graph servers). Note the token stub reuses the package's unexported `tokenResponse`.

```go
package msgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newChatsTokenServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok-chats", ExpiresIn: 3600}) // #nosec G117 -- test mock encodes a fake OAuth token response; dummy value, not a real secret
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestChats(tokenURL, baseURL string) ChatsReader {
	return NewChatsClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithTokenURL(tokenURL),
		WithBaseURL(baseURL),
	)
}

var (
	chatsFrom = time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	chatsTo   = time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
)

func TestListUserChats_Success_QueryShape(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer tok-chats", r.Header.Get("Authorization"))
		assert.Equal(t, "/users/aad-user-1/chats", r.URL.Path)
		q := r.URL.Query()
		assert.Equal(t,
			"lastUpdatedDateTime gt 2026-04-01T00:00:00Z and lastUpdatedDateTime lt 2026-07-14T00:00:00Z",
			q.Get("$filter"))
		assert.Equal(t, "members", q.Get("$expand"))
		assert.Equal(t, "id,chatType,topic,createdDateTime,lastUpdatedDateTime", q.Get("$select"))
		_, _ = w.Write([]byte(`{"value":[{
			"id":"19:chat1@thread.v2","chatType":"group","topic":"Project X",
			"createdDateTime":"2026-04-02T08:00:00Z","lastUpdatedDateTime":"2026-07-01T09:00:00Z",
			"members":[
				{"@odata.type":"#microsoft.graph.aadUserConversationMember","userId":"aad-user-1","visibleHistoryStartDateTime":"2026-04-02T08:00:00Z"},
				{"@odata.type":"#microsoft.graph.aadUserConversationMember","userId":"aad-user-2","visibleHistoryStartDateTime":"0001-01-01T00:00:00Z"}
			]}]}`))
	}))
	defer graphSrv.Close()

	chats, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.NoError(t, err)
	require.Len(t, chats, 1)
	assert.Equal(t, "19:chat1@thread.v2", chats[0].ID)
	assert.Equal(t, "group", chats[0].ChatType)
	assert.Equal(t, "Project X", chats[0].Topic)
	assert.Equal(t, time.Date(2026, 4, 2, 8, 0, 0, 0, time.UTC), chats[0].CreatedDateTime)
	require.Len(t, chats[0].Members, 2)
	assert.Equal(t, "aad-user-1", chats[0].Members[0].UserID)
	assert.Equal(t, "aad-user-2", chats[0].Members[1].UserID)
}

func TestListUserChats_NullTopicBecomesEmpty(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"value":[{
			"id":"19:one1","chatType":"oneOnOne","topic":null,
			"createdDateTime":"2026-04-02T08:00:00Z","lastUpdatedDateTime":"2026-07-01T09:00:00Z",
			"members":[{"userId":"aad-user-1","visibleHistoryStartDateTime":null}]}]}`))
	}))
	defer graphSrv.Close()

	chats, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.NoError(t, err)
	require.Len(t, chats, 1)
	assert.Equal(t, "", chats[0].Topic)
	assert.True(t, chats[0].Members[0].VisibleHistoryStartDateTime.IsZero())
}

func TestListUserChats_FollowsNextLink(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	var calls int
	var graphSrv *httptest.Server
	graphSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("$skiptoken") == "" {
			_, _ = fmt.Fprintf(w, `{"value":[{"id":"19:p1","chatType":"group","topic":"a",
				"createdDateTime":"2026-04-02T08:00:00Z","lastUpdatedDateTime":"2026-07-01T09:00:00Z","members":[]}],
				"@odata.nextLink":"%s/users/aad-user-1/chats?$skiptoken=page2"}`, graphSrv.URL)
			return
		}
		_, _ = w.Write([]byte(`{"value":[{"id":"19:p2","chatType":"group","topic":"b",
			"createdDateTime":"2026-04-03T08:00:00Z","lastUpdatedDateTime":"2026-07-02T09:00:00Z","members":[]}]}`))
	}))
	defer graphSrv.Close()

	chats, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	require.Len(t, chats, 2)
	assert.Equal(t, "19:p1", chats[0].ID)
	assert.Equal(t, "19:p2", chats[1].ID)
}

func TestListUserChats_RetriesOn429(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	var calls int
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0") // keep the test fast
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"value":[{"id":"19:ok","chatType":"group","topic":"a",
			"createdDateTime":"2026-04-02T08:00:00Z","lastUpdatedDateTime":"2026-07-01T09:00:00Z","members":[]}]}`))
	}))
	defer graphSrv.Close()

	chats, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	require.Len(t, chats, 1)
}

func TestListUserChats_429Exhausted(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	var calls int
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer graphSrv.Close()

	_, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "429")
	assert.Equal(t, 4, calls, "chatsMaxAttempts requests then give up")
}

func TestListUserChats_GraphError(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":"Forbidden","message":"nope"}}`))
	}))
	defer graphSrv.Close()

	_, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 403")
	assert.Contains(t, err.Error(), "Forbidden")
	assert.NotContains(t, err.Error(), "nope", "raw Graph message must not be surfaced")
}

func TestListUserChats_TokenError(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
	}))
	defer tokenSrv.Close()

	_, err := newTestChats(tokenSrv.URL, "http://unused.invalid").
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "acquire graph token")
}

func TestListUserChats_EmptyUserID(t *testing.T) {
	tokenSrv := newChatsTokenServer(t)
	_, err := newTestChats(tokenSrv.URL, "http://unused.invalid").
		ListUserChats(context.Background(), "", chatsFrom, chatsTo)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "userID is required")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -race -run TestListUserChats ./pkg/msgraph/`
Expected: FAIL to compile — `undefined: ChatsReader`, `undefined: NewChatsClient`.

- [ ] **Step 3: Implement `pkg/msgraph/chats.go`**

```go
package msgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// ChatsReader lists a user's Teams chats. Consumed by teams-chat-sync; kept
// separate from Client/DirectoryReader so meeting/directory consumers don't
// depend on the chats surface. App-only (Chat.Read.All).
type ChatsReader interface {
	// ListUserChats returns the user's chats whose lastUpdatedDateTime falls in
	// the exclusive window (from, to), with members expanded. It follows
	// @odata.nextLink pagination and retries throttled (429/503) responses
	// honoring Retry-After.
	ListUserChats(ctx context.Context, userID string, from, to time.Time) ([]Chat, error)
}

// NewChatsClient returns an app-only chats reader (shares the graph client
// used for meetings; New always returns a *graphClient).
//
//nolint:gocritic // hugeParam: startup-only constructor; Config passed by value is intentional.
func NewChatsClient(cfg Config, opts ...Option) ChatsReader {
	return New(cfg, opts...).(*graphClient)
}

// Chat is the subset of a Graph chat resource the sync consumes.
type Chat struct {
	ID                  string       `json:"id"`
	ChatType            string       `json:"chatType"` // oneOnOne | group | meeting
	Topic               string       `json:"topic"`    // Graph null (oneOnOne/unnamed) decodes to ""
	CreatedDateTime     time.Time    `json:"createdDateTime"`
	LastUpdatedDateTime time.Time    `json:"lastUpdatedDateTime"`
	Members             []ChatMember `json:"members"`
}

// ChatMember is the subset of an aadUserConversationMember the sync consumes.
// UserID is the member's AAD object id (the teams_user _id).
type ChatMember struct {
	UserID                      string    `json:"userId"`
	VisibleHistoryStartDateTime time.Time `json:"visibleHistoryStartDateTime"`
}

// Throttle-retry bounds for chat listing. Graph rate-limits per app+tenant;
// the job retries a bounded number of times per request and honors the
// server-provided Retry-After, capped so a hostile header can't stall a worker.
const (
	chatsMaxAttempts      = 4
	chatsDefaultRetryWait = 2 * time.Second
	chatsMaxRetryWait     = 30 * time.Second
)

func (g *graphClient) ListUserChats(ctx context.Context, userID string, from, to time.Time) ([]Chat, error) {
	if userID == "" {
		return nil, fmt.Errorf("list user chats: userID is required")
	}
	token, err := g.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire graph token: %w", err)
	}

	q := url.Values{}
	q.Set("$filter", fmt.Sprintf(
		"lastUpdatedDateTime gt %s and lastUpdatedDateTime lt %s",
		from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339),
	))
	q.Set("$expand", "members")
	q.Set("$select", "id,chatType,topic,createdDateTime,lastUpdatedDateTime")
	next := fmt.Sprintf("%s/users/%s/chats?%s", g.baseURL, url.PathEscape(userID), q.Encode())

	var chats []Chat
	for next != "" {
		body, err := g.getThrottled(ctx, token, next)
		if err != nil {
			return nil, err
		}
		var page struct {
			Value    []Chat `json:"value"`
			NextLink string `json:"@odata.nextLink"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("decode chats response: %w", err)
		}
		chats = append(chats, page.Value...)
		next = page.NextLink
	}
	return chats, nil
}

// getThrottled GETs a Graph URL, retrying 429/503 responses per Retry-After
// (bounded attempts, capped wait, ctx-aware).
func (g *graphClient) getThrottled(ctx context.Context, token, endpoint string) ([]byte, error) {
	for attempt := 1; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("build chats request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := g.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("get chats: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
		if closeErr := resp.Body.Close(); closeErr != nil {
			return nil, fmt.Errorf("close chats response: %w", closeErr)
		}
		if readErr != nil {
			return nil, fmt.Errorf("read chats response: %w", readErr)
		}
		throttled := resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode == http.StatusServiceUnavailable
		switch {
		case resp.StatusCode == http.StatusOK:
			return body, nil
		case throttled && attempt < chatsMaxAttempts:
			if err := waitRetryAfter(ctx, resp.Header.Get("Retry-After")); err != nil {
				return nil, err
			}
		default:
			// Surface only status + Graph error code; never the raw body (it can
			// carry upstream payload).
			var graphErr struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			_ = json.Unmarshal(body, &graphErr)
			if graphErr.Error.Code != "" {
				return nil, fmt.Errorf("get chats: graph returned status %d (%s)", resp.StatusCode, graphErr.Error.Code)
			}
			return nil, fmt.Errorf("get chats: graph returned status %d", resp.StatusCode)
		}
	}
}

// waitRetryAfter waits out a Retry-After header (default when absent/invalid,
// capped), aborting early when ctx is done. Timer-based so a cancelled run
// stops waiting immediately (this is backoff, not goroutine synchronization).
func waitRetryAfter(ctx context.Context, header string) error {
	wait := chatsDefaultRetryWait
	if secs, err := strconv.Atoi(header); err == nil && secs >= 0 {
		wait = time.Duration(secs) * time.Second
	}
	if wait > chatsMaxRetryWait {
		wait = chatsMaxRetryWait
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for graph retry-after: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race -run TestListUserChats ./pkg/msgraph/`
Expected: PASS. Then `make test SERVICE=pkg/msgraph` for the whole package.

- [ ] **Step 5: Commit**

```bash
git add pkg/msgraph/chats.go pkg/msgraph/chats_test.go
git commit -m "feat(msgraph): add ChatsReader for listing user Teams chats"
```

---

### Task 3: teams-chat-sync store interfaces + upsert-document builder

**Files:**
- Create: `teams-chat-sync/store.go`
- Create: `teams-chat-sync/store_mongo.go`
- Create: `teams-chat-sync/mock_store_test.go` (generated — never hand-edit)
- Test: `teams-chat-sync/store_mongo_test.go`

**Interfaces:**
- Consumes: Task 1 models; `msgraph.Chat` from Task 2; `mongoutil.Collection`, `mongoutil.UpsertModel`, `mongoutil.WithProjection`.
- Produces (used by Tasks 4–7):
  - `TeamsUserStore{ ListUsers(ctx) ([]model.TeamsUser, error); SetFrom(ctx, userID string, from time.Time) error }`
  - `TeamsChatStore{ UpsertChats(ctx, chats []model.TeamsChat, now time.Time) error }`
  - `chatsFetcher{ ListUserChats(ctx, userID string, from, to time.Time) ([]msgraph.Chat, error) }` (consumer-side Graph interface; mocked as `MockchatsFetcher`)
  - `newMongoStore(db *mongo.Database) *mongoStore` (implements both stores)
  - `chatUpsertModel(c model.TeamsChat, now time.Time) mongo.WriteModel` (pure)

- [ ] **Step 1: Write `store.go` (interfaces only — needed for both mocks and the failing test to compile)**

```go
package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// TeamsUserStore reads the externally-populated teams_user collection and
// advances per-user watermarks.
type TeamsUserStore interface {
	// ListUsers returns every teams_user projected to exactly the fields the
	// sync needs (_id, siteID, account, from).
	ListUsers(ctx context.Context) ([]model.TeamsUser, error)
	// SetFrom advances one user's watermark after that user fully succeeded.
	SetFrom(ctx context.Context, userID string, from time.Time) error
}

// TeamsChatStore upserts synced chats keyed on _id. oneOnOne chats are
// insert-only; for other chat types createdDateTime and siteID are
// $setOnInsert-only while the mutable fields are refreshed.
type TeamsChatStore interface {
	UpsertChats(ctx context.Context, chats []model.TeamsChat, now time.Time) error
}

// chatsFetcher is the Graph surface the sync consumes (interface defined in
// the consumer per repo convention; satisfied by msgraph.ChatsReader).
type chatsFetcher interface {
	ListUserChats(ctx context.Context, userID string, from, to time.Time) ([]msgraph.Chat, error)
}
```

- [ ] **Step 2: Write the failing unit tests for the upsert-document builder**

Create `teams-chat-sync/store_mongo_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
)

var upsertNow = time.Date(2026, 7, 14, 3, 4, 5, 0, time.UTC)

func asUpdateOne(t *testing.T, m mongo.WriteModel) *mongo.UpdateOneModel {
	t.Helper()
	u, ok := m.(*mongo.UpdateOneModel)
	require.True(t, ok, "chatUpsertModel must return *mongo.UpdateOneModel")
	require.NotNil(t, u.Upsert)
	require.True(t, *u.Upsert)
	return u
}

func TestChatUpsertModel_Group_SplitsSetAndSetOnInsert(t *testing.T) {
	c := model.TeamsChat{
		ID: "19:g1", Name: "Topic", ChatType: "group",
		CreatedDateTime:     time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Members:             []model.TeamsChatMember{{ID: "u1", Account: "alice"}},
		SiteID:              "site-a",
		NeedUserSync:        true,
	}
	u := asUpdateOne(t, chatUpsertModel(c, upsertNow))
	assert.Equal(t, bson.M{"_id": "19:g1"}, u.Filter)

	update, ok := u.Update.(bson.M)
	require.True(t, ok)
	soi, ok := update["$setOnInsert"].(bson.M)
	require.True(t, ok)
	assert.Equal(t, bson.M{
		"createdDateTime": c.CreatedDateTime,
		"siteID":          "site-a",
	}, soi, "siteID and createdDateTime are insert-only")

	set, ok := update["$set"].(bson.M)
	require.True(t, ok)
	assert.Equal(t, "Topic", set["name"])
	assert.Equal(t, "group", set["chatType"])
	assert.Equal(t, c.LastUpdatedDateTime, set["lastUpdatedDateTime"])
	assert.Equal(t, c.Members, set["members"])
	assert.Equal(t, true, set["needUserSync"])
	assert.Equal(t, upsertNow, set["updatedAt"])
	assert.NotContains(t, set, "siteID", "$set must never touch siteID")
	assert.NotContains(t, set, "createdDateTime")
}

func TestChatUpsertModel_OneOnOne_AllSetOnInsert(t *testing.T) {
	c := model.TeamsChat{
		ID: "19:one1", Name: "", ChatType: model.TeamsChatTypeOneOnOne,
		CreatedDateTime:     time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Members:             []model.TeamsChatMember{{ID: "u1", Account: "alice"}, {ID: "u2", Account: "bob"}},
		SiteID:              "site-b",
		NeedUserSync:        false,
	}
	u := asUpdateOne(t, chatUpsertModel(c, upsertNow))
	update, ok := u.Update.(bson.M)
	require.True(t, ok)
	assert.NotContains(t, update, "$set", "oneOnOne must never modify an existing doc")
	soi, ok := update["$setOnInsert"].(bson.M)
	require.True(t, ok)
	assert.Equal(t, bson.M{
		"name":                "",
		"chatType":            model.TeamsChatTypeOneOnOne,
		"createdDateTime":     c.CreatedDateTime,
		"lastUpdatedDateTime": c.LastUpdatedDateTime,
		"members":             c.Members,
		"siteID":              "site-b",
		"updatedAt":           upsertNow,
		"needUserSync":        false,
	}, soi)
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test -race -run TestChatUpsertModel ./teams-chat-sync/`
Expected: FAIL to compile — `undefined: chatUpsertModel` (and mock file missing is fine at this point; nothing references mocks yet).

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

// mongoStore implements TeamsUserStore and TeamsChatStore.
type mongoStore struct {
	users *mongoutil.Collection[model.TeamsUser]
	chats *mongoutil.Collection[model.TeamsChat]
}

func newMongoStore(db *mongo.Database) *mongoStore {
	return &mongoStore{
		users: mongoutil.NewCollection[model.TeamsUser](db.Collection("teams_user")),
		chats: mongoutil.NewCollection[model.TeamsChat](db.Collection("teams_chat")),
	}
}

func (s *mongoStore) ListUsers(ctx context.Context) ([]model.TeamsUser, error) {
	users, err := s.users.FindMany(ctx, bson.M{}, mongoutil.WithProjection(bson.M{
		"_id": 1, "siteID": 1, "account": 1, "from": 1,
	}))
	if err != nil {
		return nil, fmt.Errorf("list teams users: %w", err)
	}
	return users, nil
}

func (s *mongoStore) SetFrom(ctx context.Context, userID string, from time.Time) error {
	if _, err := s.users.Raw().UpdateByID(ctx, userID, bson.M{"$set": bson.M{"from": from}}); err != nil {
		return fmt.Errorf("set teams user watermark: %w", err)
	}
	return nil
}

func (s *mongoStore) UpsertChats(ctx context.Context, chats []model.TeamsChat, now time.Time) error {
	models := make([]mongo.WriteModel, 0, len(chats))
	for _, c := range chats {
		models = append(models, chatUpsertModel(c, now))
	}
	if _, err := s.chats.BulkWrite(ctx, models); err != nil {
		return fmt.Errorf("upsert teams chats: %w", err)
	}
	return nil
}

// chatUpsertModel builds the upsert for one chat. createdDateTime and siteID
// are $setOnInsert-only — once a chat has a siteID it never changes. oneOnOne
// chats put every field under $setOnInsert: they never change after creation,
// so an existing document is never modified (the "ignore oneOnOne update"
// rule enforced atomically, without a read).
func chatUpsertModel(c model.TeamsChat, now time.Time) mongo.WriteModel {
	filter := bson.M{"_id": c.ID}
	if c.ChatType == model.TeamsChatTypeOneOnOne {
		return mongoutil.UpsertModel(filter, bson.M{"$setOnInsert": bson.M{
			"name":                c.Name,
			"chatType":            c.ChatType,
			"createdDateTime":     c.CreatedDateTime,
			"lastUpdatedDateTime": c.LastUpdatedDateTime,
			"members":             c.Members,
			"siteID":              c.SiteID,
			"updatedAt":           now,
			"needUserSync":        c.NeedUserSync,
		}})
	}
	return mongoutil.UpsertModel(filter, bson.M{
		"$setOnInsert": bson.M{
			"createdDateTime": c.CreatedDateTime,
			"siteID":          c.SiteID,
		},
		"$set": bson.M{
			"name":                c.Name,
			"chatType":            c.ChatType,
			"lastUpdatedDateTime": c.LastUpdatedDateTime,
			"members":             c.Members,
			"updatedAt":           now,
			"needUserSync":        c.NeedUserSync,
		},
	})
}
```

- [ ] **Step 5: Generate mocks and run tests**

Run: `make generate SERVICE=teams-chat-sync`
Expected: `teams-chat-sync/mock_store_test.go` created with `MockTeamsUserStore`, `MockTeamsChatStore`, `MockchatsFetcher`.

Run: `go test -race -run TestChatUpsertModel ./teams-chat-sync/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add teams-chat-sync/store.go teams-chat-sync/store_mongo.go teams-chat-sync/store_mongo_test.go teams-chat-sync/mock_store_test.go
git commit -m "feat(teams-chat-sync): store interfaces and Mongo upsert builder"
```

---

### Task 4: Mongo store integration tests

**Files:**
- Create: `teams-chat-sync/integration_test.go`

**Interfaces:**
- Consumes: `newMongoStore`, `TeamsUserStore`, `TeamsChatStore` (Task 3); `testutil.MongoDB(t, prefix)`, `testutil.RunTests(m)`.
- Produces: nothing new — verifies DB semantics the unit tests can't (real `$setOnInsert` behavior, projection, watermark write).

- [ ] **Step 1: Write the integration tests**

Create `teams-chat-sync/integration_test.go`. These verify behavior the pure-function tests can't: that the documents chatUpsertModel builds actually behave correctly against real MongoDB.

```go
//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func seedUsers(t *testing.T, store *mongoStore, users ...model.TeamsUser) {
	t.Helper()
	docs := make([]any, 0, len(users))
	for _, u := range users {
		docs = append(docs, u)
	}
	_, err := store.users.Raw().InsertMany(context.Background(), docs)
	require.NoError(t, err)
}

func TestMongoStore_ListUsers(t *testing.T) {
	db := testutil.MongoDB(t, "teamssync")
	store := newMongoStore(db)
	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	seedUsers(t, store,
		model.TeamsUser{ID: "u1", SiteID: "site-a", Account: "alice", From: &from},
		model.TeamsUser{ID: "u2", SiteID: "site-b", Account: "bob"},
	)

	users, err := store.ListUsers(context.Background())
	require.NoError(t, err)
	require.Len(t, users, 2)
	byID := map[string]model.TeamsUser{users[0].ID: users[0], users[1].ID: users[1]}
	require.NotNil(t, byID["u1"].From)
	assert.True(t, byID["u1"].From.Equal(from))
	assert.Equal(t, "site-a", byID["u1"].SiteID)
	assert.Equal(t, "alice", byID["u1"].Account)
	assert.Nil(t, byID["u2"].From, "user without watermark loads with nil From")
}

func TestMongoStore_SetFrom(t *testing.T) {
	db := testutil.MongoDB(t, "teamssync")
	store := newMongoStore(db)
	seedUsers(t, store, model.TeamsUser{ID: "u1", SiteID: "site-a", Account: "alice"})

	to := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	require.NoError(t, store.SetFrom(context.Background(), "u1", to))

	got, err := store.users.FindByID(context.Background(), "u1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.From)
	assert.True(t, got.From.Equal(to))
}

func groupChat(id, name, siteID string) model.TeamsChat {
	return model.TeamsChat{
		ID: id, Name: name, ChatType: "group",
		CreatedDateTime:     time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Members:             []model.TeamsChatMember{{ID: "u1", Account: "alice"}},
		SiteID:              siteID,
		NeedUserSync:        true,
	}
}

func TestMongoStore_UpsertChats_SiteIDImmutable(t *testing.T) {
	db := testutil.MongoDB(t, "teamssync")
	store := newMongoStore(db)
	ctx := context.Background()
	now1 := time.Date(2026, 7, 14, 1, 0, 0, 0, time.UTC)
	now2 := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)

	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{groupChat("19:g1", "First", "site-a")}, now1))
	// Second sync computes a different majority and a new name.
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{groupChat("19:g1", "Renamed", "site-b")}, now2))

	got, err := store.chats.FindByID(ctx, "19:g1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "site-a", got.SiteID, "siteID must never change after insert")
	assert.Equal(t, "Renamed", got.Name, "mutable fields must refresh")
	assert.True(t, got.UpdatedAt.Equal(now2))
	assert.True(t, got.NeedUserSync)
}

func TestMongoStore_UpsertChats_OneOnOneInsertOnly(t *testing.T) {
	db := testutil.MongoDB(t, "teamssync")
	store := newMongoStore(db)
	ctx := context.Background()
	now1 := time.Date(2026, 7, 14, 1, 0, 0, 0, time.UTC)
	now2 := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)

	one := model.TeamsChat{
		ID: "19:one1", ChatType: model.TeamsChatTypeOneOnOne,
		CreatedDateTime:     time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Members:             []model.TeamsChatMember{{ID: "u1", Account: "alice"}, {ID: "u2", Account: "bob"}},
		SiteID:              "site-a",
	}
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{one}, now1))

	changed := one
	changed.SiteID = "site-b"
	changed.LastUpdatedDateTime = time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{changed}, now2))

	got, err := store.chats.FindByID(ctx, "19:one1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "site-a", got.SiteID)
	assert.True(t, got.LastUpdatedDateTime.Equal(one.LastUpdatedDateTime), "oneOnOne doc must be untouched by re-upsert")
	assert.True(t, got.UpdatedAt.Equal(now1))
	assert.False(t, got.NeedUserSync)
}

func TestMongoStore_UpsertChats_MixedBatchAndEmpty(t *testing.T) {
	db := testutil.MongoDB(t, "teamssync")
	store := newMongoStore(db)
	ctx := context.Background()
	now := time.Date(2026, 7, 14, 1, 0, 0, 0, time.UTC)

	require.NoError(t, store.UpsertChats(ctx, nil, now), "empty batch is a no-op")

	one := model.TeamsChat{ID: "19:one2", ChatType: model.TeamsChatTypeOneOnOne, SiteID: "site-a",
		CreatedDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC), LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)}
	require.NoError(t, store.UpsertChats(ctx, []model.TeamsChat{groupChat("19:g2", "G", "site-a"), one}, now))

	n, err := store.chats.Raw().CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
}
```

- [ ] **Step 2: Run the integration tests**

Run: `make test-integration SERVICE=teams-chat-sync` (requires Docker)
Expected: PASS. If `TestMongoStore_UpsertChats_SiteIDImmutable` or the oneOnOne test fails, the bug is in `chatUpsertModel` — fix there, not in the test.

- [ ] **Step 3: Commit**

```bash
git add teams-chat-sync/integration_test.go
git commit -m "test(teams-chat-sync): Mongo store integration tests"
```

---

### Task 5: Sync core — user cache, siteID vote, chat mapping, claim set

**Files:**
- Create: `teams-chat-sync/syncer.go`
- Test: `teams-chat-sync/syncer_test.go`

**Interfaces:**
- Consumes: Task 1 models, `msgraph.Chat`/`msgraph.ChatMember` (Task 2), store interfaces (Task 3).
- Produces (used by Tasks 6–7):
  - `cachedUser{siteID, account string}`
  - `syncConfig{MaxWorkers int; DefaultFrom time.Time; Now func() time.Time}`
  - `newSyncer(users TeamsUserStore, chats TeamsChatStore, graph chatsFetcher, cfg syncConfig) *syncer`
  - `startOfDayUTC(t time.Time) time.Time`
  - `voteSiteID(members []msgraph.ChatMember, cache map[string]cachedUser) string`
  - `buildChat(gc msgraph.Chat, cache map[string]cachedUser) model.TeamsChat`
  - `(*syncer).claim(chatID string) bool`

- [ ] **Step 1: Write the failing tests**

Create `teams-chat-sync/syncer_test.go`:

```go
package main

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

func TestStartOfDayUTC(t *testing.T) {
	tests := []struct {
		name string
		in   time.Time
		want time.Time
	}{
		{"mid-day utc", time.Date(2026, 7, 14, 13, 45, 6, 7, time.UTC), time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)},
		{"already midnight", time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)},
		{"non-utc zone normalizes to utc day", time.Date(2026, 7, 14, 1, 0, 0, 0, time.FixedZone("UTC+8", 8*3600)), time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.True(t, startOfDayUTC(tc.in).Equal(tc.want))
		})
	}
}

func member(id string) msgraph.ChatMember { return msgraph.ChatMember{UserID: id} }

func TestVoteSiteID(t *testing.T) {
	cache := map[string]cachedUser{
		"a1": {siteID: "site-a", account: "alice"},
		"a2": {siteID: "site-a", account: "amy"},
		"b1": {siteID: "site-b", account: "bob"},
		"c1": {siteID: "site-c", account: "carl"},
	}
	tests := []struct {
		name    string
		members []msgraph.ChatMember
		want    string
	}{
		{"clear majority", []msgraph.ChatMember{member("a1"), member("a2"), member("b1")}, "site-a"},
		{"tie breaks lexicographically", []msgraph.ChatMember{member("a1"), member("b1")}, "site-a"},
		{"tie c vs b picks b", []msgraph.ChatMember{member("c1"), member("b1")}, "site-b"},
		{"unknown members do not vote", []msgraph.ChatMember{member("ghost"), member("b1")}, "site-b"},
		{"all unknown yields empty", []msgraph.ChatMember{member("ghost")}, ""},
		{"no members yields empty", nil, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, voteSiteID(tc.members, cache))
		})
	}
}

func TestBuildChat(t *testing.T) {
	cache := map[string]cachedUser{
		"a1": {siteID: "site-a", account: "alice"},
		"b1": {siteID: "site-b", account: "bob"},
	}
	gc := msgraph.Chat{
		ID: "19:g1", ChatType: "group", Topic: "Project X",
		CreatedDateTime:     time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Members: []msgraph.ChatMember{
			{UserID: "a1", VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
			{UserID: "ghost", VisibleHistoryStartDateTime: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)},
		},
	}
	c := buildChat(gc, cache)
	assert.Equal(t, "19:g1", c.ID)
	assert.Equal(t, "Project X", c.Name)
	assert.Equal(t, "group", c.ChatType)
	assert.Equal(t, "site-a", c.SiteID)
	assert.True(t, c.NeedUserSync)
	assert.Equal(t, []model.TeamsChatMember{
		{ID: "a1", Account: "alice", VisibleHistoryStartDateTime: gc.Members[0].VisibleHistoryStartDateTime},
		{ID: "ghost", Account: "", VisibleHistoryStartDateTime: gc.Members[1].VisibleHistoryStartDateTime},
	}, c.Members, "unknown members kept with empty account")
}

func TestBuildChat_OneOnOne(t *testing.T) {
	c := buildChat(msgraph.Chat{ID: "19:one1", ChatType: model.TeamsChatTypeOneOnOne, Topic: ""}, nil)
	assert.Equal(t, "", c.Name)
	assert.False(t, c.NeedUserSync, "oneOnOne never needs user sync")
}

func TestSyncerClaim_FirstWinsConcurrently(t *testing.T) {
	s := newSyncer(nil, nil, nil, syncConfig{MaxWorkers: 1, Now: time.Now})
	const goroutines = 32
	var wins int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.claim("19:shared") {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, 1, wins, "exactly one goroutine claims a chat id")
	assert.False(t, s.claim("19:shared"))
	assert.True(t, s.claim("19:other"))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -race -run 'TestStartOfDayUTC|TestVoteSiteID|TestBuildChat|TestSyncerClaim' ./teams-chat-sync/`
Expected: FAIL to compile — `undefined: startOfDayUTC`, `undefined: newSyncer`, etc.

- [ ] **Step 3: Implement the sync core in `syncer.go`**

```go
package main

import (
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// cachedUser is the in-memory projection of one teams_user, used for member
// account resolution and the siteID vote.
type cachedUser struct {
	siteID  string
	account string
}

// syncConfig carries the orchestration knobs. Now is injectable for tests.
type syncConfig struct {
	MaxWorkers  int
	DefaultFrom time.Time
	Now         func() time.Time
}

// syncer runs one full teams-chat sync. processed is the cross-worker claimed
// set: the first worker to claim a chat id processes it, later workers skip —
// which doubles as the write-reduction cache since many users share chats.
type syncer struct {
	users TeamsUserStore
	chats TeamsChatStore
	graph chatsFetcher
	cfg   syncConfig

	mu        sync.Mutex
	processed map[string]struct{}
}

func newSyncer(users TeamsUserStore, chats TeamsChatStore, graph chatsFetcher, cfg syncConfig) *syncer {
	return &syncer{users: users, chats: chats, graph: graph, cfg: cfg, processed: make(map[string]struct{})}
}

// startOfDayUTC truncates t to 00:00:00 UTC of the same UTC day.
func startOfDayUTC(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

// claim atomically claims a chat id, returning true only for the first caller.
func (s *syncer) claim(chatID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, done := s.processed[chatID]; done {
		return false
	}
	s.processed[chatID] = struct{}{}
	return true
}

// voteSiteID returns the majority siteID among the members present in the
// cache; ties break to the lexicographically smallest siteID so the result is
// deterministic across runs and map iteration orders. Returns "" when no
// member is known (not expected in practice: the fetching user is always a
// member and always cached).
func voteSiteID(members []msgraph.ChatMember, cache map[string]cachedUser) string {
	counts := make(map[string]int)
	for _, m := range members {
		if cu, ok := cache[m.UserID]; ok {
			counts[cu.siteID]++
		}
	}
	best, bestN := "", 0
	for site, n := range counts {
		if n > bestN || (n == bestN && n > 0 && site < best) {
			best, bestN = site, n
		}
	}
	return best
}

// buildChat maps a Graph chat to the teams_chat model, resolving member
// accounts and the owning site from the user cache. Unknown members are kept
// with an empty account. UpdatedAt is intentionally left zero — the store
// stamps it at write time.
func buildChat(gc msgraph.Chat, cache map[string]cachedUser) model.TeamsChat {
	members := make([]model.TeamsChatMember, 0, len(gc.Members))
	for _, m := range gc.Members {
		members = append(members, model.TeamsChatMember{
			ID:                          m.UserID,
			Account:                     cache[m.UserID].account,
			VisibleHistoryStartDateTime: m.VisibleHistoryStartDateTime,
		})
	}
	return model.TeamsChat{
		ID:                  gc.ID,
		Name:                gc.Topic,
		ChatType:            gc.ChatType,
		CreatedDateTime:     gc.CreatedDateTime,
		LastUpdatedDateTime: gc.LastUpdatedDateTime,
		Members:             members,
		SiteID:              voteSiteID(gc.Members, cache),
		NeedUserSync:        gc.ChatType != model.TeamsChatTypeOneOnOne,
	}
}
```

Note the tie-break condition `n == bestN && n > 0 && site < best`: the `n > 0` guard prevents a zero-count phantom from ever winning, and comparing `site < best` requires `best != ""` implicitly since any first real candidate enters via `n > bestN`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race -run 'TestStartOfDayUTC|TestVoteSiteID|TestBuildChat|TestSyncerClaim' ./teams-chat-sync/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add teams-chat-sync/syncer.go teams-chat-sync/syncer_test.go
git commit -m "feat(teams-chat-sync): user cache, siteID vote, chat mapping, claim set"
```

---

### Task 6: Worker pool — run() and syncUser()

**Files:**
- Modify: `teams-chat-sync/syncer.go` (append)
- Test: `teams-chat-sync/worker_test.go`

**Interfaces:**
- Consumes: everything from Tasks 3 and 5; gomock mocks `MockTeamsUserStore`, `MockTeamsChatStore`, `MockchatsFetcher` (generated in Task 3).
- Produces (used by Task 7):
  - `(*syncer).run(ctx context.Context) error` — returns non-nil when any user failed (the CronJob failure signal).

- [ ] **Step 1: Write the failing tests**

Create `teams-chat-sync/worker_test.go`:

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

var (
	wtDefaultFrom = time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	wtNow         = time.Date(2026, 7, 14, 10, 30, 0, 0, time.UTC)
	wtTo          = time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC) // startOfDayUTC(wtNow)
)

func fixedNow() time.Time { return wtNow }

func newTestSyncer(t *testing.T, workers int) (*syncer, *MockTeamsUserStore, *MockTeamsChatStore, *MockchatsFetcher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	chats := NewMockTeamsChatStore(ctrl)
	graph := NewMockchatsFetcher(ctrl)
	s := newSyncer(users, chats, graph, syncConfig{MaxWorkers: workers, DefaultFrom: wtDefaultFrom, Now: fixedNow})
	return s, users, chats, graph
}

func graphChat(id string, memberIDs ...string) msgraph.Chat {
	ms := make([]msgraph.ChatMember, 0, len(memberIDs))
	for _, m := range memberIDs {
		ms = append(ms, msgraph.ChatMember{UserID: m})
	}
	return msgraph.Chat{
		ID: id, ChatType: "group", Topic: "t-" + id,
		CreatedDateTime:     time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Members:             ms,
	}
}

func TestRun_HappyPath_AdvancesWatermarkAndUsesDefaultFrom(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 2)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"}, // no From -> DefaultFrom
	}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).
		Return([]msgraph.Chat{graphChat("19:g1", "u1")}, nil)
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Len(1), wtNow).Return(nil)
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(nil)

	require.NoError(t, s.run(context.Background()))
}

func TestRun_ExistingFromIsUsed(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 1)
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice", From: &from},
	}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", from, wtTo).Return(nil, nil)
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(nil)
	_ = chats // no chats -> no upsert call

	require.NoError(t, s.run(context.Background()))
}

func TestRun_SkipsUserWithEmptyWindow(t *testing.T) {
	s, users, _, _ := newTestSyncer(t, 1)
	from := wtTo // watermark already at startOfDay(now): nothing to fetch
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice", From: &from},
	}, nil)
	// No ListUserChats, no UpsertChats, no SetFrom expected.

	require.NoError(t, s.run(context.Background()))
}

func TestRun_SharedChatUpsertedOnce(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 4)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
		{ID: "u2", SiteID: "site-b", Account: "bob"},
	}, nil)
	shared := graphChat("19:shared", "u1", "u2")
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).Return([]msgraph.Chat{shared}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u2", wtDefaultFrom, wtTo).Return([]msgraph.Chat{shared}, nil)

	var mu sync.Mutex
	var upserted []string
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Any(), wtNow).DoAndReturn(
		func(_ context.Context, batch []model.TeamsChat, _ time.Time) error {
			mu.Lock()
			defer mu.Unlock()
			for _, c := range batch {
				upserted = append(upserted, c.ID)
			}
			return nil
		}).MaxTimes(2) // the loser's batch may be empty and skip the call entirely
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(nil)
	users.EXPECT().SetFrom(gomock.Any(), "u2", wtTo).Return(nil)

	require.NoError(t, s.run(context.Background()))
	assert.Equal(t, []string{"19:shared"}, upserted, "a chat shared by two users is upserted exactly once")
}

func TestRun_GraphFailureHoldsWatermarkAndFailsRun(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 1)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
		{ID: "u2", SiteID: "site-b", Account: "bob"},
	}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).
		Return(nil, fmt.Errorf("graph returned status 429"))
	graph.EXPECT().ListUserChats(gomock.Any(), "u2", wtDefaultFrom, wtTo).
		Return([]msgraph.Chat{graphChat("19:g2", "u2")}, nil)
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Len(1), wtNow).Return(nil)
	users.EXPECT().SetFrom(gomock.Any(), "u2", wtTo).Return(nil)
	// No SetFrom for u1: its watermark must hold so next run retries the window.

	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 2 users failed")
}

func TestRun_UpsertFailureHoldsWatermark(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 1)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
	}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).
		Return([]msgraph.Chat{graphChat("19:g1", "u1")}, nil)
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Any(), wtNow).Return(fmt.Errorf("mongo down"))
	// No SetFrom expected.

	require.Error(t, s.run(context.Background()))
}

func TestRun_SetFromFailureFailsUser(t *testing.T) {
	s, users, _, graph := newTestSyncer(t, 1)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
	}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).Return(nil, nil)
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(fmt.Errorf("mongo down"))

	require.Error(t, s.run(context.Background()))
}

func TestRun_ListUsersFailure(t *testing.T) {
	s, users, _, _ := newTestSyncer(t, 1)
	users.EXPECT().ListUsers(gomock.Any()).Return(nil, fmt.Errorf("mongo down"))
	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load teams users")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -race -run TestRun_ ./teams-chat-sync/`
Expected: FAIL to compile — `s.run undefined`.

- [ ] **Step 3: Append run/syncUser to `syncer.go`**

Add `"context"`, `"fmt"`, `"log/slog"`, `"sync/atomic"` to the imports, then append:

```go
// summary is the per-run outcome reported in the final log line. Total and
// Skipped are written only by the dispatching goroutine; the atomics are
// updated by workers.
type summary struct {
	Total, Skipped     int
	Succeeded, Failed  atomic.Int64
	Upserted, Deduped  atomic.Int64
}

// run executes one full sync: load the user cache, fan eligible users out to
// MaxWorkers workers, wait, and report. It returns an error when any user
// failed so main exits non-zero and the CronJob records the failure.
func (s *syncer) run(ctx context.Context) error {
	users, err := s.users.ListUsers(ctx)
	if err != nil {
		return fmt.Errorf("load teams users: %w", err)
	}
	cache := make(map[string]cachedUser, len(users))
	for _, u := range users {
		cache[u.ID] = cachedUser{siteID: u.SiteID, account: u.Account}
	}

	to := startOfDayUTC(s.cfg.Now())
	var sum summary
	sum.Total = len(users)

	jobs := make(chan model.TeamsUser)
	var wg sync.WaitGroup
	for i := 0; i < s.cfg.MaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range jobs {
				if err := s.syncUser(ctx, u, to, cache, &sum); err != nil {
					sum.Failed.Add(1)
					slog.Error("teams chat sync: user failed", "userID", u.ID, "error", err)
					continue
				}
				sum.Succeeded.Add(1)
			}
		}()
	}
	for _, u := range users {
		if !s.effectiveFrom(u).Before(to) {
			sum.Skipped++
			continue
		}
		jobs <- u
	}
	close(jobs)
	wg.Wait()

	slog.Info("teams chat sync: run complete",
		"usersTotal", sum.Total, "usersSucceeded", sum.Succeeded.Load(),
		"usersFailed", sum.Failed.Load(), "usersSkipped", sum.Skipped,
		"chatsUpserted", sum.Upserted.Load(), "chatsDeduped", sum.Deduped.Load())

	if failed := sum.Failed.Load(); failed > 0 {
		return fmt.Errorf("%d of %d users failed", failed, sum.Total)
	}
	return nil
}

// effectiveFrom is the user's watermark, falling back to the configured
// default for users that have never synced.
func (s *syncer) effectiveFrom(u model.TeamsUser) time.Time {
	if u.From != nil {
		return *u.From
	}
	return s.cfg.DefaultFrom
}

// syncUser fetches one user's chat window, upserts the chats this worker
// claimed first, and advances the user's watermark only after everything
// succeeded — a failed user keeps its old watermark and is retried next run.
func (s *syncer) syncUser(ctx context.Context, u model.TeamsUser, to time.Time, cache map[string]cachedUser, sum *summary) error {
	graphChats, err := s.graph.ListUserChats(ctx, u.ID, s.effectiveFrom(u), to)
	if err != nil {
		return fmt.Errorf("list user chats: %w", err)
	}
	batch := make([]model.TeamsChat, 0, len(graphChats))
	for _, gc := range graphChats {
		if !s.claim(gc.ID) {
			sum.Deduped.Add(1)
			continue
		}
		batch = append(batch, buildChat(gc, cache))
	}
	if len(batch) > 0 {
		if err := s.chats.UpsertChats(ctx, batch, s.cfg.Now()); err != nil {
			return fmt.Errorf("upsert chats: %w", err)
		}
		sum.Upserted.Add(int64(len(batch)))
	}
	if err := s.users.SetFrom(ctx, u.ID, to); err != nil {
		return fmt.Errorf("advance watermark: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race -run 'TestRun_|TestSyncerClaim' ./teams-chat-sync/`
Expected: PASS (the `-race` flag matters here — the shared-chat test exercises real concurrency).

- [ ] **Step 5: Commit**

```bash
git add teams-chat-sync/syncer.go teams-chat-sync/worker_test.go
git commit -m "feat(teams-chat-sync): worker pool with per-user watermark semantics"
```

---

### Task 7: main.go — config and wiring

**Files:**
- Create: `teams-chat-sync/main.go`
- Test: `teams-chat-sync/main_test.go`

**Interfaces:**
- Consumes: `newMongoStore` (Task 3), `newSyncer`/`syncConfig`/`(*syncer).run` (Tasks 5–6), `msgraph.NewChatsClient` (Task 2), `mongoutil.Connect/Disconnect`, `otelutil.InitTracer`.
- Produces: the `teams-chat-sync` binary.

- [ ] **Step 1: Write the failing config tests**

Create `teams-chat-sync/main_test.go`:

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
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("GRAPH_TENANT_ID", "tenant")
	t.Setenv("GRAPH_CLIENT_ID", "client")
	t.Setenv("GRAPH_CLIENT_SECRET", "secret")
}

func TestConfig_Defaults(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := env.ParseAs[Config]()
	require.NoError(t, err)
	assert.Equal(t, "chat", cfg.MongoDB)
	assert.Equal(t, 8, cfg.MaxWorkers)
	assert.Equal(t, 30*time.Minute, cfg.RunTimeout)
	assert.Equal(t, "2026-04-01T00:00:00Z", cfg.DefaultFrom)
	assert.False(t, cfg.GraphTLSInsecureSkipVerify)

	from, err := time.Parse(time.RFC3339, cfg.DefaultFrom)
	require.NoError(t, err, "the default watermark must be valid RFC3339")
	assert.True(t, from.Equal(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)))
}

func TestConfig_MissingRequired(t *testing.T) {
	t.Setenv("MONGO_URI", "")
	_, err := env.ParseAs[Config]()
	require.Error(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -race -run TestConfig_ ./teams-chat-sync/`
Expected: FAIL to compile — `undefined: Config`.

- [ ] **Step 3: Implement `main.go`**

```go
// Command teams-chat-sync is a run-to-completion job (k8s CronJob) that
// mirrors Microsoft Teams chats into the teams_chat collection. One global
// instance serves the whole federation: it reads every teams_user, fetches
// each user's chat window from Graph, resolves each chat's site by
// member-majority vote, and advances per-user watermarks on success.
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
	"github.com/hmchangw/chat/pkg/otelutil"
)

// Config is the job's environment configuration.
type Config struct {
	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`

	MaxWorkers int           `env:"MAX_WORKERS" envDefault:"8"`
	RunTimeout time.Duration `env:"RUN_TIMEOUT" envDefault:"30m"`
	// DefaultFrom is the RFC3339 UTC watermark used for users that have never
	// synced (teams_user docs without a from field).
	DefaultFrom string `env:"SYNC_DEFAULT_FROM" envDefault:"2026-04-01T00:00:00Z"`

	GraphTenantID     string `env:"GRAPH_TENANT_ID,required"`
	GraphClientID     string `env:"GRAPH_CLIENT_ID,required"`
	GraphClientSecret string `env:"GRAPH_CLIENT_SECRET,required"`
	// GraphTLSInsecureSkipVerify disables Graph TLS verification (opt-in,
	// default false) for dev/on-prem environments behind a TLS-intercepting
	// proxy. The proxy itself is taken from HTTPS_PROXY/HTTP_PROXY.
	GraphTLSInsecureSkipVerify bool `env:"GRAPH_TLS_INSECURE_SKIP_VERIFY" envDefault:"false"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("teams-chat-sync failed", "error", err)
		os.Exit(1)
	}
}

// run wires dependencies and performs one sync. It returns an error rather
// than calling os.Exit so deferred cleanup always runs.
func run() error {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if cfg.MaxWorkers <= 0 || cfg.RunTimeout <= 0 {
		return fmt.Errorf("invalid config: MAX_WORKERS and RUN_TIMEOUT must be positive")
	}
	defaultFrom, err := time.Parse(time.RFC3339, cfg.DefaultFrom)
	if err != nil {
		return fmt.Errorf("parse SYNC_DEFAULT_FROM: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.RunTimeout)
	defer cancel()

	tracerShutdown, err := otelutil.InitTracer(ctx, "teams-chat-sync")
	if err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}
	defer func() {
		if err := tracerShutdown(context.Background()); err != nil {
			slog.Warn("tracer shutdown", "error", err)
		}
	}()

	client, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("mongo connect: %w", err)
	}
	defer mongoutil.Disconnect(context.Background(), client)
	store := newMongoStore(client.Database(cfg.MongoDB))

	graph := msgraph.NewChatsClient(msgraph.Config{
		TenantID:              cfg.GraphTenantID,
		ClientID:              cfg.GraphClientID,
		ClientSecret:          cfg.GraphClientSecret,
		TLSInsecureSkipVerify: cfg.GraphTLSInsecureSkipVerify,
	})

	s := newSyncer(store, store, graph, syncConfig{
		MaxWorkers:  cfg.MaxWorkers,
		DefaultFrom: defaultFrom,
		Now:         time.Now,
	})
	if err := s.run(ctx); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	slog.Info("teams-chat-sync done")
	return nil
}
```

- [ ] **Step 4: Run tests and build**

Run: `go test -race -run TestConfig_ ./teams-chat-sync/`
Expected: PASS.
Run: `make build SERVICE=teams-chat-sync`
Expected: builds `bin/teams-chat-sync` with no errors.

- [ ] **Step 5: Commit**

```bash
git add teams-chat-sync/main.go teams-chat-sync/main_test.go
git commit -m "feat(teams-chat-sync): config and job wiring"
```

---

### Task 8: Deploy files, full verification, push

**Files:**
- Create: `teams-chat-sync/deploy/Dockerfile`
- Create: `teams-chat-sync/deploy/docker-compose.yml`
- Create: `teams-chat-sync/deploy/azure-pipelines.yml`

**Interfaces:**
- Consumes: the finished service. Produces: CI/CD + local-dev packaging (repo convention: every service ships all three files).

- [ ] **Step 1: Write `teams-chat-sync/deploy/Dockerfile`**

```dockerfile
FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY teams-chat-sync/ teams-chat-sync/
RUN CGO_ENABLED=0 go build -o /teams-chat-sync ./teams-chat-sync/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /teams-chat-sync /teams-chat-sync
USER app
ENTRYPOINT ["/teams-chat-sync"]
```

- [ ] **Step 2: Write `teams-chat-sync/deploy/docker-compose.yml`**

The job is run-to-completion (no restart policy, no ports). It joins the shared local network where `mongo` runs; production scheduling is a k8s CronJob (ops-side, not in this repo).

```yaml
name: teams-chat-sync

services:
  teams-chat-sync:
    build:
      context: ../..
      dockerfile: teams-chat-sync/deploy/Dockerfile
    environment:
      - MONGO_URI=${MONGO_URI:-mongodb://mongo:27017}
      - MONGO_DB=${MONGO_DB:-chat}
      - MAX_WORKERS=${MAX_WORKERS:-8}
      - SYNC_DEFAULT_FROM=${SYNC_DEFAULT_FROM:-2026-04-01T00:00:00Z}
      - RUN_TIMEOUT=${RUN_TIMEOUT:-30m}
      - GRAPH_TENANT_ID=${GRAPH_TENANT_ID}
      - GRAPH_CLIENT_ID=${GRAPH_CLIENT_ID}
      - GRAPH_CLIENT_SECRET=${GRAPH_CLIENT_SECRET}
      - GRAPH_TLS_INSECURE_SKIP_VERIFY=${GRAPH_TLS_INSECURE_SKIP_VERIFY:-false}
      # Proxy (optional) is taken from the standard env vars via ProxyFromEnvironment.
      - HTTPS_PROXY=${HTTPS_PROXY:-}
      - HTTP_PROXY=${HTTP_PROXY:-}
      - NO_PROXY=${NO_PROXY:-}
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

- [ ] **Step 3: Write `teams-chat-sync/deploy/azure-pipelines.yml`**

```yaml
trigger:
  branches:
    include:
      - main
      - develop
  paths:
    include:
      - teams-chat-sync/
      - pkg/

pr:
  branches:
    include:
      - main
  paths:
    include:
      - teams-chat-sync/
      - pkg/

variables:
  GO_VERSION: '1.25.12'
  SERVICE_DIR: teams-chat-sync
  IMAGE_NAME: teams-chat-sync
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

          - script: go test ./$(SERVICE_DIR)/... -v -race -coverprofile=coverage-sync.out
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
make test SERVICE=teams-chat-sync
make test SERVICE=pkg/msgraph
make test SERVICE=pkg/model
make test-integration SERVICE=teams-chat-sync
go test -coverprofile=/tmp/claude-0/-home-user-chat/685e6338-ff2b-5cca-b409-4823ae2eae26/scratchpad/cover.out ./teams-chat-sync/ && go tool cover -func=/tmp/claude-0/-home-user-chat/685e6338-ff2b-5cca-b409-4823ae2eae26/scratchpad/cover.out | tail -1
make sast
```

Expected: all green; total coverage for `teams-chat-sync` ≥ 80% (main.go's `run()` wiring is the only untested code — if the floor is missed, extract and test any remaining pure logic rather than integration-testing `run()`).

- [ ] **Step 5: Commit and push**

```bash
git add teams-chat-sync/deploy/
git commit -m "build(teams-chat-sync): Dockerfile, compose, CI pipeline"
git push -u origin claude/teams-chat-sync-service-9ispb0
```

(If the push fails on a network error, retry up to 4 times with 2s/4s/8s/16s backoff. Do NOT create a PR — the user hasn't asked for one.)
