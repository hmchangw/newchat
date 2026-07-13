# teams-user-sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Revision note (2026-07-13, post-implementation):** the shipped code diverges
> from this plan in two reviewed ways — (1) the UPN **domain filter was removed**:
> `splitUPN` returns only `(account, ok)`, `NewSyncer(store, graph, pageSize)` has
> no `emailDomain` parameter, `TEAMS_EMAIL_DOMAIN` config is gone, and the
> `DomainSkipped` stat is now `InvalidUPN` (malformed UPNs only; guests fall out
> as HR-unmatched); (2) msgraph constructors take `*Config` (gocritic hugeParam).
> Affected snippets below have been corrected where reviewers flagged them; the
> repository code is authoritative.

**Goal:** A cron-scheduled batch service that populates the MongoDB `teams_user` collection from Microsoft Graph `/users` pages joined with the `hr` collection's `siteID`, using separate Mongo read/write clients.

**Architecture:** Page-streaming sync: each Graph page (≤500 users) is immediately diffed against `teams_user` by `_id` (read client), missing users are resolved to a `siteID` via the `hr` collection (read client), and the merged records are bulk-upserted (write client). robfig/cron/v3 schedules runs; `cron.SkipIfStillRunning` drops overlapping fires. Spec: `docs/superpowers/specs/2026-07-13-teams-user-sync-design.md`.

**Tech Stack:** Go 1.25, robfig/cron/v3 (new dependency, user-approved), `pkg/msgraph` (extended), `pkg/mongoutil` (extended), `pkg/health`, `pkg/shutdown`, `pkg/idgen`, mockgen + testify, testcontainers via `pkg/testutil`.

## Global Constraints

- All commands via `make` targets — never raw `go test`/`go build` (exception: `go get`/`go mod tidy` for the approved dependency, and `go test -coverprofile` for the coverage check, which CLAUDE.md sanctions).
- TDD: write the failing test first, watch it fail, then implement. Commit after each green cycle.
- Errors: `fmt.Errorf("what this function was doing: %w", err)` — never bare `err`, never `errcode` (nothing here is client-facing).
- Logging: `log/slog` JSON only; never log tokens or Graph response bodies. Structured key-value fields.
- Struct tags: `json` + `bson` on model structs, camelCase, `bson:"_id"` for primary key.
- Mongo: driver `go.mongodb.org/mongo-driver/v2`, explicit projections on every find, no `$lookup`.
- Unit tests: `package main` (same package), mockgen mocks in `mock_store_test.go`, no real databases.
- Integration tests: `//go:build integration` tag, containers only via `pkg/testutil`, `TestMain(m) { testutil.RunTests(m) }` required.
- Coverage: ≥80% per package, target 90%+ for handler/store.
- Config via `caarlos0/env`; secrets `required` with no default.
- New service is a flat directory `teams-user-sync/` at repo root with `deploy/Dockerfile`, `deploy/docker-compose.yml`, `deploy/azure-pipelines.yml`.
- `docs/client-api.md` does NOT need updating — no client-facing handler and `TeamsUser` is a persistence model, not a request/reply or event struct.
- Work on branch `claude/teams-user-sync-service-sb7nuw`.

---

### Task 1: `TeamsUser` model in `pkg/model`

**Files:**
- Create: `pkg/model/teamsuser.go`
- Modify: `pkg/model/model_test.go` (append one test)

**Interfaces:**
- Consumes: nothing.
- Produces: `model.TeamsUser{ID, UPN, Account, SiteID string}` — used by Tasks 5, 6, 10.

- [ ] **Step 1: Write the failing test**

Append to `pkg/model/model_test.go` (it already has the generic `roundTrip[T]` helper at the bottom of the file — do not redefine it):

```go
func TestTeamsUserJSON(t *testing.T) {
	src := model.TeamsUser{
		ID:      "8f4c9e2a-0b1d-4e5f-9a6b-7c8d9e0f1a2b",
		UPN:     "Alice@corp.example",
		Account: "alice",
		SiteID:  "site-a",
	}
	var dst model.TeamsUser
	roundTrip(t, &src, &dst)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model`
Expected: FAIL — `undefined: model.TeamsUser`

- [ ] **Step 3: Write minimal implementation**

Create `pkg/model/teamsuser.go`:

```go
package model

// TeamsUser is the persisted teams_user collection document: a Teams (Azure
// AD) user joined with the HR system's site assignment. Written by
// teams-user-sync; readable by any service that needs the mapping.
type TeamsUser struct {
	// ID is the Teams (Azure AD) user object id.
	ID string `json:"id" bson:"_id"`
	// UPN is the user's userPrincipalName as returned by Graph.
	UPN string `json:"upn" bson:"upn"`
	// Account is the lowercased UPN local part (text before '@') — the value
	// matched against hr.accountName.
	Account string `json:"account" bson:"account"`
	// SiteID is the HR system's site id for the account.
	SiteID string `json:"siteId" bson:"siteId"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/model`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/model/teamsuser.go pkg/model/model_test.go
git commit -m "feat(model): add TeamsUser persistence model for teams_user collection"
```

---

### Task 2: `mongoutil.ConnectRead` + `testutil.MongoURI`

**Files:**
- Modify: `pkg/mongoutil/mongo.go`
- Modify: `pkg/mongoutil/mongo_test.go` (append tests)
- Create: `pkg/mongoutil/mongo_integration_test.go`
- Modify: `pkg/testutil/mongo.go` (add `MongoURI` accessor)

**Interfaces:**
- Consumes: existing `buildClientOptions(uri, username, password)` and `Connect` in `pkg/mongoutil/mongo.go`.
- Produces: `mongoutil.ConnectRead(ctx context.Context, uri, username, password string) (*mongo.Client, error)` — used by Task 8. `testutil.MongoURI(t *testing.T) string` — used by this task's integration test.

- [ ] **Step 1: Write the failing unit test**

Append to `pkg/mongoutil/mongo_test.go` (match the file's existing imports; add `go.mongodb.org/mongo-driver/v2/mongo/readpref`, testify `assert`/`require` if not present):

```go
func TestBuildReadClientOptions_SecondaryPreferred(t *testing.T) {
	opts := buildReadClientOptions("mongodb://localhost:27017", "user", "pass")
	require.NotNil(t, opts.ReadPreference)
	assert.Equal(t, readpref.SecondaryPreferredMode, opts.ReadPreference.Mode())
	// auth wiring is inherited from buildClientOptions
	require.NotNil(t, opts.Auth)
	assert.Equal(t, "user", opts.Auth.Username)
}

func TestBuildReadClientOptions_NoAuthWhenEmpty(t *testing.T) {
	opts := buildReadClientOptions("mongodb://localhost:27017", "", "")
	require.NotNil(t, opts.ReadPreference)
	assert.Equal(t, readpref.SecondaryPreferredMode, opts.ReadPreference.Mode())
	assert.Nil(t, opts.Auth)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/mongoutil`
Expected: FAIL — `undefined: buildReadClientOptions`

- [ ] **Step 3: Implement**

In `pkg/mongoutil/mongo.go`, add the import `go.mongodb.org/mongo-driver/v2/mongo/readpref`, extract the shared connect flow, and add the read variant. The full file after the change:

```go
package mongoutil

import (
	"context"
	"fmt"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func Connect(ctx context.Context, uri, username, password string) (*mongo.Client, error) {
	return connect(ctx, buildClientOptions(uri, username, password), uri)
}

// ConnectRead connects a read-oriented client: the same connect/ping flow as
// Connect with ReadPreference=secondaryPreferred, so reads can be served by
// secondaries. For services that split Mongo traffic into separate read and
// write clients (e.g. teams-user-sync).
func ConnectRead(ctx context.Context, uri, username, password string) (*mongo.Client, error) {
	return connect(ctx, buildReadClientOptions(uri, username, password), uri)
}

func connect(ctx context.Context, opts *options.ClientOptions, uri string) (*mongo.Client, error) {
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("mongo ping: %w", err)
	}
	slog.Info("connected to MongoDB", "uri", uri)
	return client, nil
}

func Disconnect(ctx context.Context, client *mongo.Client) {
	if err := client.Disconnect(ctx); err != nil {
		slog.Error("mongo disconnect failed", "error", err)
	}
}

func buildClientOptions(uri, username, password string) *options.ClientOptions {
	opts := options.Client().ApplyURI(uri)
	if username != "" && password != "" {
		opts.SetAuth(options.Credential{
			Username: username,
			Password: password,
		})
	}
	return opts
}

func buildReadClientOptions(uri, username, password string) *options.ClientOptions {
	return buildClientOptions(uri, username, password).SetReadPreference(readpref.SecondaryPreferred())
}
```

(Keep any other functions already in the file; only `Connect` is refactored to delegate to `connect`.)

- [ ] **Step 4: Run unit tests to verify they pass**

Run: `make test SERVICE=pkg/mongoutil`
Expected: PASS (all existing tests still green — `Connect` behavior unchanged)

- [ ] **Step 5: Add `testutil.MongoURI`**

In `pkg/testutil/mongo.go`: `ensureMongoClient` already obtains the connection string (`uri, err := container.ConnectionString(ctx)` around line 37). Capture it in a package-level variable next to the existing package-level container/client vars (`mongoURI string`), assign it inside `ensureMongoClient` where `ConnectionString` is called, and add:

```go
// MongoURI returns the shared Mongo container's connection string, starting
// the container if needed. For tests that must dial their own client (e.g.
// exercising mongoutil.Connect variants) instead of using MongoDB's handle.
func MongoURI(t *testing.T) string {
	t.Helper()
	if _, err := ensureMongoClient(); err != nil {
		t.Fatalf("testutil.MongoURI: %v", err)
	}
	return mongoURI
}
```

- [ ] **Step 6: Write the integration test**

Create `pkg/mongoutil/mongo_integration_test.go` (the package's integration TestMain already exists in `main_test.go` — do not add another):

```go
//go:build integration

package mongoutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestConnectRead_ConnectsAndReads(t *testing.T) {
	ctx := context.Background()
	client, err := ConnectRead(ctx, testutil.MongoURI(t), "", "")
	require.NoError(t, err)
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	db := client.Database("mongoutil_connect_read_test")
	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	_, err = db.Collection("docs").InsertOne(ctx, bson.M{"_id": "x"})
	require.NoError(t, err)
	n, err := db.Collection("docs").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
}
```

- [ ] **Step 7: Run integration tests**

Run: `make test-integration SERVICE=pkg/mongoutil`
Expected: PASS (requires Docker)

- [ ] **Step 8: Commit**

```bash
git add pkg/mongoutil/mongo.go pkg/mongoutil/mongo_test.go pkg/mongoutil/mongo_integration_test.go pkg/testutil/mongo.go
git commit -m "feat(mongoutil): add ConnectRead read-client helper (secondaryPreferred)"
```

---

### Task 3: `msgraph.ListUsers` pagination

**Files:**
- Modify: `pkg/msgraph/msgraph.go`
- Modify: `pkg/msgraph/msgraph_test.go` (append tests)

**Interfaces:**
- Consumes: existing `graphClient`, `accessToken`, `GraphUser`, `Option`/`WithBaseURL`/`WithTokenURL` in `pkg/msgraph`.
- Produces:
  ```go
  type UserLister interface {
      ListUsers(ctx context.Context, pageSize int, fn func([]GraphUser) error) error
  }
  func NewUserListerClient(cfg Config, opts ...Option) UserLister
  ```
  Used by Tasks 6, 8, 10.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/msgraph/msgraph_test.go`, matching the file's existing httptest style (a token server returning `{"access_token":"tok","expires_in":3600}` plus a graph server):

```go
func TestListUsers_MultiPageFollowsNextLink(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	defer tokenSrv.Close()

	var requests []string
	var graphSrv *httptest.Server
	graphSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.String())
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Query().Get("page") == "2" {
			_, _ = w.Write([]byte(`{"value":[{"id":"u3","userPrincipalName":"carol@corp.example"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"value":[` +
			`{"id":"u1","userPrincipalName":"alice@corp.example"},` +
			`{"id":"u2","userPrincipalName":"bob@corp.example"}],` +
			`"@odata.nextLink":"` + graphSrv.URL + `/users?page=2"}`))
	}))
	defer graphSrv.Close()

	lister := NewUserListerClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithBaseURL(graphSrv.URL), WithTokenURL(tokenSrv.URL),
	)

	var pages [][]GraphUser
	err := lister.ListUsers(context.Background(), 500, func(users []GraphUser) error {
		pages = append(pages, users)
		return nil
	})
	require.NoError(t, err)

	require.Len(t, pages, 2)
	assert.Equal(t, []GraphUser{
		{ID: "u1", UserPrincipalName: "alice@corp.example"},
		{ID: "u2", UserPrincipalName: "bob@corp.example"},
	}, pages[0])
	assert.Equal(t, []GraphUser{{ID: "u3", UserPrincipalName: "carol@corp.example"}}, pages[1])

	// first request carries $top and $select
	require.NotEmpty(t, requests)
	first, err := url.Parse(requests[0])
	require.NoError(t, err)
	assert.Equal(t, "500", first.Query().Get("$top"))
	assert.Equal(t, "id,userPrincipalName", first.Query().Get("$select"))
}

func TestListUsers_CallbackErrorAbortsWalk(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	defer tokenSrv.Close()

	var calls int
	var graphSrv *httptest.Server
	graphSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"value":[{"id":"u1","userPrincipalName":"a@x"}],` +
			`"@odata.nextLink":"` + graphSrv.URL + `/users?page=2"}`))
	}))
	defer graphSrv.Close()

	lister := NewUserListerClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithBaseURL(graphSrv.URL), WithTokenURL(tokenSrv.URL),
	)

	err := lister.ListUsers(context.Background(), 500, func([]GraphUser) error {
		return errors.New("boom")
	})
	require.ErrorContains(t, err, "boom")
	assert.Equal(t, 1, calls, "must not fetch further pages after fn error")
}

func TestListUsers_Non200IsError(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	defer tokenSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer graphSrv.Close()

	lister := NewUserListerClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithBaseURL(graphSrv.URL), WithTokenURL(tokenSrv.URL),
	)

	err := lister.ListUsers(context.Background(), 500, func([]GraphUser) error { return nil })
	require.ErrorContains(t, err, "status 403")
}
```

(Add `errors` and `net/url` to the test file's imports if missing.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=pkg/msgraph`
Expected: FAIL — `undefined: NewUserListerClient`

- [ ] **Step 3: Implement**

In `pkg/msgraph/msgraph.go`, add `"strconv"` to imports. Below the `DirectoryReader` block, add:

```go
// UserLister walks the tenant's user directory page by page. Kept separate
// from Client/DirectoryReader so consumers depend only on the surface they
// use. App-only (User.Read.All).
type UserLister interface {
	// ListUsers calls fn once per page of up to pageSize users
	// (GET /users?$select=id,userPrincipalName&$top={pageSize}), following
	// @odata.nextLink until the directory is exhausted. A non-nil error from
	// fn aborts the walk.
	ListUsers(ctx context.Context, pageSize int, fn func([]GraphUser) error) error
}

// NewUserListerClient returns an app-only user lister (shares the graph
// client used for meetings; New always returns a *graphClient).
func NewUserListerClient(cfg Config, opts ...Option) UserLister {
	return New(cfg, opts...).(*graphClient)
}
```

At the end of the file, add:

```go
// usersPage is one page of the /users walk.
type usersPage struct {
	Value    []GraphUser `json:"value"`
	NextLink string      `json:"@odata.nextLink"`
}

// ListUsers walks GET /users page by page, invoking fn per page. The first
// request carries $select/$top; subsequent pages follow Graph's opaque
// @odata.nextLink verbatim (it embeds the paging state).
func (g *graphClient) ListUsers(ctx context.Context, pageSize int, fn func([]GraphUser) error) error {
	token, err := g.accessToken(ctx)
	if err != nil {
		return fmt.Errorf("acquire graph token: %w", err)
	}
	q := url.Values{}
	q.Set("$select", "id,userPrincipalName")
	q.Set("$top", strconv.Itoa(pageSize))
	next := g.baseURL + "/users?" + q.Encode()
	for next != "" {
		page, err := g.fetchUsersPage(ctx, token, next)
		if err != nil {
			return err
		}
		if err := fn(page.Value); err != nil {
			return fmt.Errorf("process users page: %w", err)
		}
		next = page.NextLink
	}
	return nil
}

func (g *graphClient) fetchUsersPage(ctx context.Context, token, endpoint string) (*usersPage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build list-users request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
	if err != nil {
		return nil, fmt.Errorf("read list-users response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		// Never wrap the response body — surface the status only.
		return nil, fmt.Errorf("list users: graph returned status %d", resp.StatusCode)
	}
	var page usersPage
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, fmt.Errorf("decode list-users response: %w", err)
	}
	return &page, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/msgraph`
Expected: PASS

- [ ] **Step 5: Update `docs/msgraph-client.md`**

Add a short section after "Creating a meeting (idempotent)":

```markdown
## Listing users (paginated)

`UserLister.ListUsers(ctx, pageSize, fn)` walks `GET /users` with
`$select=id,userPrincipalName&$top={pageSize}`, following `@odata.nextLink`
and invoking `fn` once per page. Used by `teams-user-sync` to enumerate the
tenant. Requires the **`User.Read.All`** application permission. Construct
via `NewUserListerClient(cfg)`.
```

- [ ] **Step 6: Commit**

```bash
git add pkg/msgraph/msgraph.go pkg/msgraph/msgraph_test.go docs/msgraph-client.md
git commit -m "feat(msgraph): add paginated ListUsers behind UserLister interface"
```

---

### Task 4: Service config

**Files:**
- Create: `teams-user-sync/config.go`
- Create: `teams-user-sync/config_test.go`

**Interfaces:**
- Consumes: `caarlos0/env/v11` (already in go.mod).
- Produces: `config` struct (fields below) parsed via `env.ParseAs[config]()` — used by Task 8.

- [ ] **Step 1: Write the failing test**

Create `teams-user-sync/config_test.go`:

```go
package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequiredEnv sets the vars without envDefault; tests override as needed.
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("TEAMS_TENANT_ID", "tenant")
	t.Setenv("TEAMS_CLIENT_ID", "client")
	t.Setenv("TEAMS_CLIENT_SECRET", "secret")
	t.Setenv("TEAMS_EMAIL_DOMAIN", "corp.example")
	t.Setenv("MONGO_READ_URI", "mongodb://read:27017")
	t.Setenv("MONGO_WRITE_URI", "mongodb://write:27017")
}

func TestConfig_Defaults(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	assert.Equal(t, "0 2 * * *", cfg.SyncCron)
	assert.False(t, cfg.RunOnStart)
	assert.Equal(t, 500, cfg.GraphPageSize)
	assert.Equal(t, "chat", cfg.MongoReadDB)
	assert.Equal(t, "chat", cfg.MongoWriteDB)
	assert.Equal(t, ":8081", cfg.HealthAddr)
	assert.Equal(t, "tenant", cfg.TeamsTenantID)
	assert.Equal(t, "corp.example", cfg.TeamsEmailDomain)
	assert.Equal(t, "mongodb://read:27017", cfg.MongoReadURI)
	assert.Equal(t, "mongodb://write:27017", cfg.MongoWriteURI)
}

func TestConfig_Overrides(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SYNC_CRON", "30 4 * * *")
	t.Setenv("RUN_ON_START", "true")
	t.Setenv("GRAPH_PAGE_SIZE", "100")
	t.Setenv("MONGO_READ_DB", "replica")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	assert.Equal(t, "30 4 * * *", cfg.SyncCron)
	assert.True(t, cfg.RunOnStart)
	assert.Equal(t, 100, cfg.GraphPageSize)
	assert.Equal(t, "replica", cfg.MongoReadDB)
}

func TestConfig_MissingRequiredFails(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("TEAMS_CLIENT_SECRET", "") // required rejects empty

	_, err := env.ParseAs[config]()
	require.Error(t, err)
}
```

Note: `env:"...,required"` rejects unset vars but accepts empty strings; use `notEmpty` semantics via `required,notEmpty` if the empty-string test fails — check the failure message and adjust the tag (the repo uses both forms; `notEmpty` is what makes `""` fail).

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=teams-user-sync`
Expected: FAIL — `undefined: config`

- [ ] **Step 3: Implement**

Create `teams-user-sync/config.go`:

```go
package main

// config is teams-user-sync's environment configuration. Credentials and
// connection strings are required with no default (fail fast); operational
// knobs default to sane dev values.
type config struct {
	// SyncCron is the 5-field cron expression driving updateUsers runs.
	SyncCron string `env:"SYNC_CRON" envDefault:"0 2 * * *"`
	// RunOnStart additionally fires one sync immediately at startup.
	RunOnStart bool `env:"RUN_ON_START" envDefault:"false"`

	TeamsTenantID     string `env:"TEAMS_TENANT_ID,required,notEmpty"`
	TeamsClientID     string `env:"TEAMS_CLIENT_ID,required,notEmpty"`
	TeamsClientSecret string `env:"TEAMS_CLIENT_SECRET,required,notEmpty"`
	// TeamsEmailDomain filters Graph users: only UPNs under this domain are
	// synced, and the local part is the hr.accountName lookup key.
	TeamsEmailDomain string `env:"TEAMS_EMAIL_DOMAIN,required,notEmpty"`
	// GraphPageSize is Graph's $top per page (max 999).
	GraphPageSize int `env:"GRAPH_PAGE_SIZE" envDefault:"500"`

	MongoReadURI      string `env:"MONGO_READ_URI,required,notEmpty"`
	MongoReadUsername string `env:"MONGO_READ_USERNAME" envDefault:""`
	MongoReadPassword string `env:"MONGO_READ_PASSWORD" envDefault:""`
	MongoReadDB       string `env:"MONGO_READ_DB" envDefault:"chat"`

	MongoWriteURI      string `env:"MONGO_WRITE_URI,required,notEmpty"`
	MongoWriteUsername string `env:"MONGO_WRITE_USERNAME" envDefault:""`
	MongoWritePassword string `env:"MONGO_WRITE_PASSWORD" envDefault:""`
	MongoWriteDB       string `env:"MONGO_WRITE_DB" envDefault:"chat"`

	HealthAddr string `env:"HEALTH_ADDR" envDefault:":8081"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=teams-user-sync`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add teams-user-sync/config.go teams-user-sync/config_test.go
git commit -m "feat(teams-user-sync): env config with read/write mongo split"
```

---

### Task 5: Store interface + Mongo implementation

**Files:**
- Create: `teams-user-sync/store.go`
- Create: `teams-user-sync/store_integration_test.go`
- Create: `teams-user-sync/store_mongo.go`
- Generate: `teams-user-sync/mock_store_test.go`

**Interfaces:**
- Consumes: `model.TeamsUser` (Task 1), `mongoutil.NewCollection[T]` / `BulkUpsert` (existing).
- Produces:
  ```go
  type Store interface {
      ExistingIDs(ctx context.Context, ids []string) (map[string]struct{}, error)
      HRSiteIDs(ctx context.Context, accounts []string) (map[string]string, error)
      UpsertTeamsUsers(ctx context.Context, users []model.TeamsUser) error
  }
  func newMongoStore(readDB, writeDB *mongo.Database) *mongoStore
  ```
  `Store` + generated `MockStore` used by Task 6; `newMongoStore` used by Tasks 8, 10.

- [ ] **Step 1: Define the interface and generate mocks**

Create `teams-user-sync/store.go`:

```go
package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// Store is the persistence surface updateUsers needs. Reads (ExistingIDs,
// HRSiteIDs) are served by the read client; UpsertTeamsUsers by the write
// client.
type Store interface {
	// ExistingIDs returns which of ids already exist in teams_user.
	ExistingIDs(ctx context.Context, ids []string) (map[string]struct{}, error)
	// HRSiteIDs resolves accounts to siteIDs from the hr collection
	// (hr.accountName -> hr.siteID); accounts without a match are absent.
	HRSiteIDs(ctx context.Context, accounts []string) (map[string]string, error)
	// UpsertTeamsUsers bulk-upserts merged records into teams_user, keyed on _id.
	UpsertTeamsUsers(ctx context.Context, users []model.TeamsUser) error
}
```

Run: `make generate SERVICE=teams-user-sync`
Expected: `teams-user-sync/mock_store_test.go` created.

- [ ] **Step 2: Write the failing integration tests**

Create `teams-user-sync/store_integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestMongoStore_ExistingIDs(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	ctx := context.Background()
	store := newMongoStore(db, db)

	_, err := db.Collection("teams_user").InsertMany(ctx, []any{
		bson.M{"_id": "u1", "upn": "a@x", "account": "a", "siteId": "site-a"},
		bson.M{"_id": "u2", "upn": "b@x", "account": "b", "siteId": "site-a"},
	})
	require.NoError(t, err)

	got, err := store.ExistingIDs(ctx, []string{"u1", "u2", "u3"})
	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{"u1": {}, "u2": {}}, got)
}

func TestMongoStore_ExistingIDs_EmptyInput(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	store := newMongoStore(db, db)

	got, err := store.ExistingIDs(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMongoStore_HRSiteIDs(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	ctx := context.Background()
	store := newMongoStore(db, db)

	_, err := db.Collection("hr").InsertMany(ctx, []any{
		bson.M{"accountName": "alice", "siteID": "site-a", "unrelated": "x"},
		bson.M{"accountName": "bob", "siteID": "site-b"},
	})
	require.NoError(t, err)

	got, err := store.HRSiteIDs(ctx, []string{"alice", "bob", "carol"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"alice": "site-a", "bob": "site-b"}, got)
}

func TestMongoStore_HRSiteIDs_EmptyInput(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	store := newMongoStore(db, db)

	got, err := store.HRSiteIDs(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMongoStore_UpsertTeamsUsers_InsertAndIdempotentRerun(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	ctx := context.Background()
	store := newMongoStore(db, db)

	users := []model.TeamsUser{
		{ID: "u1", UPN: "Alice@corp.example", Account: "alice", SiteID: "site-a"},
		{ID: "u2", UPN: "bob@corp.example", Account: "bob", SiteID: "site-b"},
	}
	require.NoError(t, store.UpsertTeamsUsers(ctx, users))
	// rerun with identical payload must not duplicate or error
	require.NoError(t, store.UpsertTeamsUsers(ctx, users))

	n, err := db.Collection("teams_user").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)

	var got model.TeamsUser
	require.NoError(t, db.Collection("teams_user").FindOne(ctx, bson.M{"_id": "u1"}).Decode(&got))
	assert.Equal(t, users[0], got)
}

func TestMongoStore_UpsertTeamsUsers_EmptyInput(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	store := newMongoStore(db, db)
	require.NoError(t, store.UpsertTeamsUsers(context.Background(), nil))
}
```

- [ ] **Step 3: Run integration tests to verify they fail**

Run: `make test-integration SERVICE=teams-user-sync`
Expected: FAIL — `undefined: newMongoStore` (compile error is the red phase here)

- [ ] **Step 4: Implement**

Create `teams-user-sync/store_mongo.go`:

```go
package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const (
	teamsUserCollection = "teams_user"
	hrCollection        = "hr"
)

// mongoStore implements Store over two databases: readDB (teams_user diff +
// hr lookup, typically a read-preference client) and writeDB (teams_user
// upserts).
type mongoStore struct {
	readTeamsUsers  *mongo.Collection
	readHR          *mongo.Collection
	writeTeamsUsers *mongoutil.Collection[model.TeamsUser]
}

func newMongoStore(readDB, writeDB *mongo.Database) *mongoStore {
	return &mongoStore{
		readTeamsUsers:  readDB.Collection(teamsUserCollection),
		readHR:          readDB.Collection(hrCollection),
		writeTeamsUsers: mongoutil.NewCollection[model.TeamsUser](writeDB.Collection(teamsUserCollection)),
	}
}

func (s *mongoStore) ExistingIDs(ctx context.Context, ids []string) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	cur, err := s.readTeamsUsers.Find(ctx,
		bson.M{"_id": bson.M{"$in": ids}},
		options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, fmt.Errorf("find existing teams users: %w", err)
	}
	var rows []struct {
		ID string `bson:"_id"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode existing teams user ids: %w", err)
	}
	for _, r := range rows {
		out[r.ID] = struct{}{}
	}
	return out, nil
}

func (s *mongoStore) HRSiteIDs(ctx context.Context, accounts []string) (map[string]string, error) {
	out := make(map[string]string, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	cur, err := s.readHR.Find(ctx,
		bson.M{"accountName": bson.M{"$in": accounts}},
		options.Find().SetProjection(bson.M{"accountName": 1, "siteID": 1}))
	if err != nil {
		return nil, fmt.Errorf("find hr accounts: %w", err)
	}
	var rows []struct {
		AccountName string `bson:"accountName"`
		SiteID      string `bson:"siteID"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode hr accounts: %w", err)
	}
	for _, r := range rows {
		out[r.AccountName] = r.SiteID
	}
	return out, nil
}

func (s *mongoStore) UpsertTeamsUsers(ctx context.Context, users []model.TeamsUser) error {
	if _, err := s.writeTeamsUsers.BulkUpsert(ctx, users, func(u model.TeamsUser) any {
		return bson.M{"_id": u.ID}
	}); err != nil {
		return fmt.Errorf("bulk upsert teams users: %w", err)
	}
	return nil
}
```

- [ ] **Step 5: Run integration tests to verify they pass**

Run: `make test-integration SERVICE=teams-user-sync`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add teams-user-sync/store.go teams-user-sync/store_mongo.go teams-user-sync/store_integration_test.go teams-user-sync/mock_store_test.go
git commit -m "feat(teams-user-sync): two-client mongo store (teams_user diff, hr lookup, bulk upsert)"
```

---

### Task 6: `Syncer.UpdateUsers` (per-page sync core)

**Files:**
- Create: `teams-user-sync/handler_test.go`
- Create: `teams-user-sync/handler.go`

**Interfaces:**
- Consumes: `Store` + `MockStore` (Task 5), `msgraph.UserLister` + `msgraph.GraphUser` (Task 3), `model.TeamsUser` (Task 1).
- Produces:
  ```go
  func NewSyncer(store Store, graph msgraph.UserLister, emailDomain string, pageSize int) *Syncer
  type RunStats struct{ Pages, Seen, Existing, DomainSkipped, HRUnmatched, Upserted int }
  func (s *Syncer) UpdateUsers(ctx context.Context) (RunStats, error)
  ```
  Used by Tasks 7, 8, 10.

- [ ] **Step 1: Write the failing tests**

Create `teams-user-sync/handler_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// fakeLister feeds canned pages to UpdateUsers without a real Graph client.
type fakeLister struct {
	pages [][]msgraph.GraphUser
	err   error // returned after all pages are delivered
}

func (f *fakeLister) ListUsers(_ context.Context, _ int, fn func([]msgraph.GraphUser) error) error {
	for _, p := range f.pages {
		if err := fn(p); err != nil {
			return err
		}
	}
	return f.err
}

func TestSyncer_UpdateUsers_HappyPathTwoPages(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	lister := &fakeLister{pages: [][]msgraph.GraphUser{
		{
			{ID: "u1", UserPrincipalName: "Alice@corp.example"},
			{ID: "u2", UserPrincipalName: "bob@corp.example"},
		},
		{
			{ID: "u3", UserPrincipalName: "carol@corp.example"},
		},
	}}

	// page 1: u1 new, u2 existing
	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1", "u2"}).
		Return(map[string]struct{}{"u2": {}}, nil)
	store.EXPECT().HRSiteIDs(gomock.Any(), []string{"alice"}).
		Return(map[string]string{"alice": "site-a"}, nil)
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u1", UPN: "Alice@corp.example", Account: "alice", SiteID: "site-a"},
	}).Return(nil)
	// page 2: u3 new
	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u3"}).
		Return(map[string]struct{}{}, nil)
	store.EXPECT().HRSiteIDs(gomock.Any(), []string{"carol"}).
		Return(map[string]string{"carol": "site-b"}, nil)
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u3", UPN: "carol@corp.example", Account: "carol", SiteID: "site-b"},
	}).Return(nil)

	syncer := NewSyncer(store, lister, "corp.example", 500)
	stats, err := syncer.UpdateUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, RunStats{Pages: 2, Seen: 3, Existing: 1, Upserted: 2}, stats)
}

func TestSyncer_UpdateUsers_AllExistingSkipsLookupAndWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	lister := &fakeLister{pages: [][]msgraph.GraphUser{
		{{ID: "u1", UserPrincipalName: "alice@corp.example"}},
	}}

	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1"}).
		Return(map[string]struct{}{"u1": {}}, nil)
	// no HRSiteIDs, no UpsertTeamsUsers

	syncer := NewSyncer(store, lister, "corp.example", 500)
	stats, err := syncer.UpdateUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, RunStats{Pages: 1, Seen: 1, Existing: 1}, stats)
}

func TestSyncer_UpdateUsers_SkipsByDomainAndMalformedUPN(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	lister := &fakeLister{pages: [][]msgraph.GraphUser{{
		{ID: "u1", UserPrincipalName: "guest#EXT#@other.example"}, // wrong domain
		{ID: "u2", UserPrincipalName: "no-at-sign"},               // malformed
		{ID: "u3", UserPrincipalName: "Dave@CORP.EXAMPLE"},        // case-insensitive match
	}}}

	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1", "u2", "u3"}).
		Return(map[string]struct{}{}, nil)
	store.EXPECT().HRSiteIDs(gomock.Any(), []string{"dave"}).
		Return(map[string]string{"dave": "site-a"}, nil)
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u3", UPN: "Dave@CORP.EXAMPLE", Account: "dave", SiteID: "site-a"},
	}).Return(nil)

	syncer := NewSyncer(store, lister, "corp.example", 500)
	stats, err := syncer.UpdateUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, RunStats{Pages: 1, Seen: 3, DomainSkipped: 2, Upserted: 1}, stats)
}

func TestSyncer_UpdateUsers_HRMissSkippedAndCounted(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	lister := &fakeLister{pages: [][]msgraph.GraphUser{{
		{ID: "u1", UserPrincipalName: "alice@corp.example"},
		{ID: "u2", UserPrincipalName: "eve@corp.example"},
	}}}

	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1", "u2"}).
		Return(map[string]struct{}{}, nil)
	store.EXPECT().HRSiteIDs(gomock.Any(), []string{"alice", "eve"}).
		Return(map[string]string{"alice": "site-a"}, nil) // eve unmatched
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u1", UPN: "alice@corp.example", Account: "alice", SiteID: "site-a"},
	}).Return(nil)

	syncer := NewSyncer(store, lister, "corp.example", 500)
	stats, err := syncer.UpdateUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, RunStats{Pages: 1, Seen: 2, HRUnmatched: 1, Upserted: 1}, stats)
}

func TestSyncer_UpdateUsers_AllHRMissSkipsWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	lister := &fakeLister{pages: [][]msgraph.GraphUser{{
		{ID: "u1", UserPrincipalName: "eve@corp.example"},
	}}}

	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1"}).
		Return(map[string]struct{}{}, nil)
	store.EXPECT().HRSiteIDs(gomock.Any(), []string{"eve"}).
		Return(map[string]string{}, nil)
	// no UpsertTeamsUsers

	syncer := NewSyncer(store, lister, "corp.example", 500)
	stats, err := syncer.UpdateUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, RunStats{Pages: 1, Seen: 1, HRUnmatched: 1}, stats)
}

func TestSyncer_UpdateUsers_EmptyPageAndEmptyTenant(t *testing.T) {
	t.Run("empty page makes no store calls", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		lister := &fakeLister{pages: [][]msgraph.GraphUser{{}}}

		syncer := NewSyncer(store, lister, "corp.example", 500)
		stats, err := syncer.UpdateUsers(context.Background())
		require.NoError(t, err)
		assert.Equal(t, RunStats{Pages: 1}, stats)
	})
	t.Run("no pages at all", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		lister := &fakeLister{}

		syncer := NewSyncer(store, lister, "corp.example", 500)
		stats, err := syncer.UpdateUsers(context.Background())
		require.NoError(t, err)
		assert.Equal(t, RunStats{}, stats)
	})
}

func TestSyncer_UpdateUsers_ErrorPaths(t *testing.T) {
	page := [][]msgraph.GraphUser{{{ID: "u1", UserPrincipalName: "alice@corp.example"}}}

	t.Run("graph error aborts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		lister := &fakeLister{err: errors.New("graph down")}

		syncer := NewSyncer(store, lister, "corp.example", 500)
		_, err := syncer.UpdateUsers(context.Background())
		require.ErrorContains(t, err, "graph down")
	})
	t.Run("ExistingIDs error aborts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().ExistingIDs(gomock.Any(), gomock.Any()).
			Return(nil, errors.New("read down"))

		syncer := NewSyncer(store, &fakeLister{pages: page}, "corp.example", 500)
		_, err := syncer.UpdateUsers(context.Background())
		require.ErrorContains(t, err, "read down")
	})
	t.Run("HRSiteIDs error aborts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().ExistingIDs(gomock.Any(), gomock.Any()).
			Return(map[string]struct{}{}, nil)
		store.EXPECT().HRSiteIDs(gomock.Any(), gomock.Any()).
			Return(nil, errors.New("hr down"))

		syncer := NewSyncer(store, &fakeLister{pages: page}, "corp.example", 500)
		_, err := syncer.UpdateUsers(context.Background())
		require.ErrorContains(t, err, "hr down")
	})
	t.Run("Upsert error aborts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().ExistingIDs(gomock.Any(), gomock.Any()).
			Return(map[string]struct{}{}, nil)
		store.EXPECT().HRSiteIDs(gomock.Any(), gomock.Any()).
			Return(map[string]string{"alice": "site-a"}, nil)
		store.EXPECT().UpsertTeamsUsers(gomock.Any(), gomock.Any()).
			Return(errors.New("write down"))

		syncer := NewSyncer(store, &fakeLister{pages: page}, "corp.example", 500)
		_, err := syncer.UpdateUsers(context.Background())
		require.ErrorContains(t, err, "write down")
	})
}

func TestSplitUPN(t *testing.T) {
	tests := []struct {
		name              string
		upn               string
		wantAccount       string
		wantDomain        string
		wantOK            bool
	}{
		{"simple", "alice@corp.example", "alice", "corp.example", true},
		{"uppercase local lowered", "Alice.Smith@corp.example", "alice.smith", "corp.example", true},
		{"guest ext", "guest#EXT#@other.example", "guest#ext#", "other.example", true},
		{"no at sign", "alice", "", "", false},
		{"leading at", "@corp.example", "", "", false},
		{"trailing at", "alice@", "", "", false},
		{"empty", "", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account, domain, ok := splitUPN(tt.upn)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantAccount, account)
			assert.Equal(t, tt.wantDomain, domain)
		})
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=teams-user-sync`
Expected: FAIL — `undefined: NewSyncer`, `undefined: splitUPN`

- [ ] **Step 3: Implement**

Create `teams-user-sync/handler.go`:

```go
package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// Syncer runs updateUsers: walk Graph /users page by page, insert the users
// missing from teams_user that have an HR site assignment.
type Syncer struct {
	store    Store
	graph    msgraph.UserLister
	domain   string
	pageSize int
}

// NewSyncer builds a Syncer. emailDomain scopes which UPNs are synced;
// pageSize is Graph's $top.
func NewSyncer(store Store, graph msgraph.UserLister, emailDomain string, pageSize int) *Syncer {
	return &Syncer{store: store, graph: graph, domain: emailDomain, pageSize: pageSize}
}

// RunStats summarizes one UpdateUsers run for the end-of-run log line.
type RunStats struct {
	Pages         int // Graph pages walked
	Seen          int // users returned by Graph
	Existing      int // already present in teams_user, untouched
	DomainSkipped int // UPN outside the configured domain (or malformed)
	HRUnmatched   int // no hr.accountName match; retried next run
	Upserted      int // written to teams_user
}

// UpdateUsers performs one full sync run. Any Graph or store error aborts the
// run; the next scheduled run retries from scratch (writes are idempotent
// upserts, so partial progress is kept).
func (s *Syncer) UpdateUsers(ctx context.Context) (RunStats, error) {
	var stats RunStats
	if err := s.graph.ListUsers(ctx, s.pageSize, func(users []msgraph.GraphUser) error {
		return s.syncPage(ctx, users, &stats)
	}); err != nil {
		return stats, fmt.Errorf("walk graph users: %w", err)
	}
	return stats, nil
}

func (s *Syncer) syncPage(ctx context.Context, users []msgraph.GraphUser, stats *RunStats) error {
	stats.Pages++
	stats.Seen += len(users)
	if len(users) == 0 {
		return nil
	}

	ids := make([]string, 0, len(users))
	for _, u := range users {
		ids = append(ids, u.ID)
	}
	existing, err := s.store.ExistingIDs(ctx, ids)
	if err != nil {
		return fmt.Errorf("diff teams_user ids: %w", err)
	}
	stats.Existing += len(existing)

	candidates := make([]model.TeamsUser, 0, len(users)-len(existing))
	for _, u := range users {
		if _, ok := existing[u.ID]; ok {
			continue
		}
		account, domain, ok := splitUPN(u.UserPrincipalName)
		if !ok || !strings.EqualFold(domain, s.domain) {
			stats.DomainSkipped++
			continue
		}
		candidates = append(candidates, model.TeamsUser{ID: u.ID, UPN: u.UserPrincipalName, Account: account})
	}
	if len(candidates) == 0 {
		return nil
	}

	accounts := make([]string, 0, len(candidates))
	for _, c := range candidates {
		accounts = append(accounts, c.Account)
	}
	siteIDs, err := s.store.HRSiteIDs(ctx, accounts)
	if err != nil {
		return fmt.Errorf("resolve hr site ids: %w", err)
	}

	merged := make([]model.TeamsUser, 0, len(candidates))
	for _, c := range candidates {
		siteID, ok := siteIDs[c.Account]
		if !ok {
			stats.HRUnmatched++
			continue
		}
		c.SiteID = siteID
		merged = append(merged, c)
	}
	if len(merged) == 0 {
		return nil
	}
	if err := s.store.UpsertTeamsUsers(ctx, merged); err != nil {
		return fmt.Errorf("upsert teams users: %w", err)
	}
	stats.Upserted += len(merged)
	return nil
}

// splitUPN splits a userPrincipalName into its lowercased local part and its
// domain. ok is false when there is no non-empty local part and domain.
func splitUPN(upn string) (account, domain string, ok bool) {
	at := strings.Index(upn, "@")
	if at <= 0 || at == len(upn)-1 {
		return "", "", false
	}
	return strings.ToLower(upn[:at]), upn[at+1:], true
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=teams-user-sync`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add teams-user-sync/handler.go teams-user-sync/handler_test.go
git commit -m "feat(teams-user-sync): page-streaming updateUsers sync core"
```

---

### Task 7: Cron guard + run wrapper (adds robfig/cron)

**Files:**
- Modify: `go.mod` / `go.sum` (add `github.com/robfig/cron/v3` — dependency approved by user during brainstorming)
- Create: `teams-user-sync/scheduler_test.go`
- Create: `teams-user-sync/scheduler.go`

**Interfaces:**
- Consumes: `Syncer.UpdateUsers` (Task 6), `idgen.GenerateRequestID()` (existing).
- Produces:
  ```go
  func guardedJob(run func()) cron.Job   // skip-if-still-running wrapper
  func runSync(syncer *Syncer)           // one run: request id + summary log
  type cronSlogLogger struct{}           // robfig cron.Logger -> slog adapter
  ```
  Used by Task 8.

- [ ] **Step 1: Add the dependency**

```bash
go get github.com/robfig/cron/v3@v3.0.1
go mod tidy
```

- [ ] **Step 2: Write the failing test**

Create `teams-user-sync/scheduler_test.go`:

```go
package main

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGuardedJob_SkipsWhileRunning(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	var runs atomic.Int32

	job := guardedJob(func() {
		runs.Add(1)
		if runs.Load() == 1 {
			close(started) // signal startup once; later runs must not re-close
			<-release
		}
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		job.Run()
	}()
	<-started

	// second fire while the first is still executing returns without running
	job.Run()
	assert.Equal(t, int32(1), runs.Load(), "overlapping fire must be skipped")

	close(release)
	wg.Wait()

	// after the first run finishes, the next fire executes again
	job.Run()
	assert.Equal(t, int32(2), runs.Load(), "guard must release after completion")
}
```

Note the second `job.Run()` reuses the same closure: rebind `release` to an
already-closed channel first so it doesn't block.

- [ ] **Step 3: Run test to verify it fails**

Run: `make test SERVICE=teams-user-sync`
Expected: FAIL — `undefined: guardedJob`

- [ ] **Step 4: Implement**

Create `teams-user-sync/scheduler.go`:

```go
package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/hmchangw/chat/pkg/idgen"
)

// guardedJob wraps run with skip-if-still-running semantics: a fire that
// arrives while the previous run is executing is dropped (robfig logs the
// skip via cronSlogLogger), never queued.
func guardedJob(run func()) cron.Job {
	return cron.NewChain(cron.SkipIfStillRunning(cronSlogLogger{})).Then(cron.FuncJob(run))
}

// runSync executes one updateUsers run under a fresh request id and logs the
// outcome. The Syncer itself is silent; this is the run boundary where errors
// are logged exactly once.
func runSync(syncer *Syncer) {
	logger := slog.With("requestId", idgen.GenerateRequestID())
	logger.Info("teams user sync started")
	start := time.Now()

	stats, err := syncer.UpdateUsers(context.Background())
	fields := []any{
		"pages", stats.Pages,
		"seen", stats.Seen,
		"existing", stats.Existing,
		"domainSkipped", stats.DomainSkipped,
		"hrUnmatched", stats.HRUnmatched,
		"upserted", stats.Upserted,
		"durationMs", time.Since(start).Milliseconds(),
	}
	if err != nil {
		logger.Error("teams user sync failed", append(fields, "error", err)...)
		return
	}
	logger.Info("teams user sync finished", fields...)
}

// cronSlogLogger adapts robfig's cron.Logger to slog (JSON discipline).
type cronSlogLogger struct{}

func (cronSlogLogger) Info(msg string, kv ...any) {
	slog.Info("cron: "+msg, kv...)
}

func (cronSlogLogger) Error(err error, msg string, kv ...any) {
	slog.Error("cron: "+msg, append(kv, "error", err)...)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=teams-user-sync`
Expected: PASS (`-race` is on via the Makefile; the guard test exercises concurrency)

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum teams-user-sync/scheduler.go teams-user-sync/scheduler_test.go
git commit -m "feat(teams-user-sync): cron skip-if-running guard and run wrapper (robfig/cron/v3)"
```

---

### Task 8: `main.go` wiring

**Files:**
- Create: `teams-user-sync/main.go`

**Interfaces:**
- Consumes: `config` (Task 4), `mongoutil.Connect`/`ConnectRead`/`Disconnect` (Task 2), `msgraph.NewUserListerClient` (Task 3), `newMongoStore` (Task 5), `NewSyncer` (Task 6), `guardedJob`/`runSync` (Task 7), `health.Serve`, `shutdown.Wait` (existing).
- Produces: the service binary. No exported surface.

- [ ] **Step 1: Implement**

`main.go` is wiring-only (repo convention: main is not unit-tested; all logic lives in the tested files above). Create `teams-user-sync/main.go`:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/robfig/cron/v3"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/pkg/shutdown"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	// Graph rejects $top outside 1..999; fail fast on a bad knob.
	if cfg.GraphPageSize <= 0 || cfg.GraphPageSize > 999 {
		return fmt.Errorf("GRAPH_PAGE_SIZE must be in 1..999, got %d", cfg.GraphPageSize)
	}

	ctx := context.Background()

	readClient, err := mongoutil.ConnectRead(ctx, cfg.MongoReadURI, cfg.MongoReadUsername, cfg.MongoReadPassword)
	if err != nil {
		return fmt.Errorf("connect mongo read client: %w", err)
	}
	writeClient, err := mongoutil.Connect(ctx, cfg.MongoWriteURI, cfg.MongoWriteUsername, cfg.MongoWritePassword)
	if err != nil {
		return fmt.Errorf("connect mongo write client: %w", err)
	}

	store := newMongoStore(readClient.Database(cfg.MongoReadDB), writeClient.Database(cfg.MongoWriteDB))
	lister := msgraph.NewUserListerClient(msgraph.Config{
		TenantID:     cfg.TeamsTenantID,
		ClientID:     cfg.TeamsClientID,
		ClientSecret: cfg.TeamsClientSecret,
	})
	syncer := NewSyncer(store, lister, cfg.TeamsEmailDomain, cfg.GraphPageSize)

	// One guarded job shared by the schedule and the optional on-start run,
	// so "skip if the previous job is not yet finished" holds across both.
	job := guardedJob(func() { runSync(syncer) })

	c := cron.New(cron.WithLogger(cronSlogLogger{}))
	if _, err := c.AddJob(cfg.SyncCron, job); err != nil {
		return fmt.Errorf("register sync cron %q: %w", cfg.SyncCron, err)
	}
	c.Start()
	slog.Info("sync scheduled", "cron", cfg.SyncCron, "runOnStart", cfg.RunOnStart)

	// The on-start run is not tracked by cron's Stop context, so track it
	// ourselves and wait for it during shutdown.
	var startupRun sync.WaitGroup
	if cfg.RunOnStart {
		startupRun.Add(1)
		go func() {
			defer startupRun.Done()
			job.Run()
		}()
	}

	stopHealth, err := health.Serve(cfg.HealthAddr, 5*time.Second,
		health.Check{Name: "mongo-read", Probe: func(ctx context.Context) error { return readClient.Ping(ctx, nil) }},
		health.Check{Name: "mongo-write", Probe: func(ctx context.Context) error { return writeClient.Ping(ctx, nil) }},
	)
	if err != nil {
		return fmt.Errorf("start health listener: %w", err)
	}

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error {
			stopCtx := c.Stop() // done when the in-flight scheduled job (if any) finishes
			startupDone := make(chan struct{})
			go func() { startupRun.Wait(); close(startupDone) }()
			select {
			case <-stopCtx.Done():
			case <-ctx.Done():
				return fmt.Errorf("stop cron: in-flight sync did not finish before timeout")
			}
			select {
			case <-startupDone:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("stop cron: on-start sync did not finish before timeout")
			}
		},
		stopHealth,
		func(ctx context.Context) error {
			mongoutil.Disconnect(ctx, readClient)
			mongoutil.Disconnect(ctx, writeClient)
			return nil
		},
	)
	return nil
}
```

- [ ] **Step 2: Verify it builds and the whole service package is green**

Run: `make build SERVICE=teams-user-sync && make test SERVICE=teams-user-sync`
Expected: builds, all tests PASS

- [ ] **Step 3: Commit**

```bash
git add teams-user-sync/main.go
git commit -m "feat(teams-user-sync): service wiring — cron schedule, health probes, graceful shutdown"
```

---

### Task 9: Deploy artifacts

**Files:**
- Create: `teams-user-sync/deploy/Dockerfile`
- Create: `teams-user-sync/deploy/docker-compose.yml`
- Create: `teams-user-sync/deploy/azure-pipelines.yml`

**Interfaces:** none (build/deploy only). Modeled on `user-presence-service/deploy/*`.

- [ ] **Step 1: Dockerfile**

Create `teams-user-sync/deploy/Dockerfile`:

```dockerfile
FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY teams-user-sync/ teams-user-sync/
RUN CGO_ENABLED=0 go build -o /teams-user-sync ./teams-user-sync/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /teams-user-sync /teams-user-sync
USER app
ENTRYPOINT ["/teams-user-sync"]
```

- [ ] **Step 2: docker-compose**

Create `teams-user-sync/deploy/docker-compose.yml` (only the dependencies this service needs — Mongo comes from the shared `chat-local` network's `mongodb`, no NATS):

```yaml
name: teams-user-sync

services:
  teams-user-sync:
    build:
      context: ../..
      dockerfile: teams-user-sync/deploy/Dockerfile
    environment:
      - SYNC_CRON=0 2 * * *
      - RUN_ON_START=true
      - TEAMS_TENANT_ID=${TEAMS_TENANT_ID}
      - TEAMS_CLIENT_ID=${TEAMS_CLIENT_ID}
      - TEAMS_CLIENT_SECRET=${TEAMS_CLIENT_SECRET}
      - TEAMS_EMAIL_DOMAIN=${TEAMS_EMAIL_DOMAIN:-dev.local}
      - GRAPH_PAGE_SIZE=500
      - MONGO_READ_URI=mongodb://mongodb:27017
      - MONGO_WRITE_URI=mongodb://mongodb:27017
      - MONGO_READ_DB=chat
      - MONGO_WRITE_DB=chat
      - HEALTH_ADDR=:8081
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

- [ ] **Step 3: azure-pipelines**

Create `teams-user-sync/deploy/azure-pipelines.yml` (copy of the sibling template with the service name swapped):

```yaml
trigger:
  branches:
    include:
      - main
      - develop
  paths:
    include:
      - teams-user-sync/
      - pkg/

pr:
  branches:
    include:
      - main
  paths:
    include:
      - teams-user-sync/
      - pkg/

variables:
  GO_VERSION: '1.25.12'
  SERVICE_NAME: teams-user-sync
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

- [ ] **Step 4: Commit**

```bash
git add teams-user-sync/deploy/
git commit -m "feat(teams-user-sync): deploy artifacts (Dockerfile, compose, azure-pipelines)"
```

---

### Task 10: End-to-end integration test

**Files:**
- Create: `teams-user-sync/integration_test.go`

**Interfaces:**
- Consumes: `newMongoStore` (Task 5), `NewSyncer`/`UpdateUsers`/`RunStats` (Task 6), `msgraph.NewUserListerClient` + `WithBaseURL`/`WithTokenURL` (Task 3), `testutil.MongoDB`.
- Produces: nothing (verification only). Note: `TestMain` already exists in `store_integration_test.go` — do NOT add another.

- [ ] **Step 1: Write the test**

Create `teams-user-sync/integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestUpdateUsers_EndToEnd drives the full pipeline: a fake two-page Graph
// tenant against a real Mongo (one database standing in for both the read
// and write clients), asserting the merged writes and idempotent rerun.
func TestUpdateUsers_EndToEnd(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync_e2e")
	ctx := context.Background()

	_, err := db.Collection("hr").InsertMany(ctx, []any{
		bson.M{"accountName": "alice", "siteID": "site-a"},
		bson.M{"accountName": "old", "siteID": "site-a"},
	})
	require.NoError(t, err)
	_, err = db.Collection("teams_user").InsertOne(ctx,
		bson.M{"_id": "id-existing", "upn": "old@corp.example", "account": "old", "siteId": "site-a"})
	require.NoError(t, err)

	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	t.Cleanup(tokenSrv.Close)

	var graphSrv *httptest.Server
	graphSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "2" {
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []map[string]string{
				{"id": "id-existing", "userPrincipalName": "old@corp.example"},
				{"id": "id-guest", "userPrincipalName": "guest#EXT#@other.example"},
			}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"value": []map[string]string{
				{"id": "id-alice", "userPrincipalName": "Alice@corp.example"},
				{"id": "id-carol", "userPrincipalName": "carol@corp.example"},
			},
			"@odata.nextLink": graphSrv.URL + "/users?page=2",
		})
	}))
	t.Cleanup(graphSrv.Close)

	lister := msgraph.NewUserListerClient(
		&msgraph.Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		msgraph.WithBaseURL(graphSrv.URL), msgraph.WithTokenURL(tokenSrv.URL),
	)
	syncer := NewSyncer(newMongoStore(db, db), lister, 500)

	stats, err := syncer.UpdateUsers(ctx)
	require.NoError(t, err)
	assert.Equal(t, RunStats{
		Pages: 2, Seen: 4, Existing: 1, HRUnmatched: 2, Upserted: 1,
	}, stats)

	var doc model.TeamsUser
	require.NoError(t, db.Collection("teams_user").FindOne(ctx, bson.M{"_id": "id-alice"}).Decode(&doc))
	assert.Equal(t, model.TeamsUser{
		ID: "id-alice", UPN: "Alice@corp.example", Account: "alice", SiteID: "site-a",
	}, doc)

	// rerun: everything either exists or is still HR-unmatched (carol + guest)
	stats2, err := syncer.UpdateUsers(ctx)
	require.NoError(t, err)
	assert.Equal(t, RunStats{
		Pages: 2, Seen: 4, Existing: 2, HRUnmatched: 2, Upserted: 0,
	}, stats2)

	n, err := db.Collection("teams_user").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
}
```

- [ ] **Step 2: Run it**

Run: `make test-integration SERVICE=teams-user-sync`
Expected: PASS. (If it fails, the pipeline has a real bug — debug the failing stage, don't loosen the assertions.)

- [ ] **Step 3: Commit**

```bash
git add teams-user-sync/integration_test.go
git commit -m "test(teams-user-sync): end-to-end sync integration test"
```

---

### Task 11: Full verification sweep

**Files:** none new (fixes only, if anything fails).

- [ ] **Step 1: Format + lint**

Run: `make fmt && make lint`
Expected: no diffs, no lint errors. Fix anything reported.

- [ ] **Step 2: Full unit + integration suites (touched packages)**

Run:
```bash
make test
make test-integration SERVICE=teams-user-sync
make test-integration SERVICE=pkg/mongoutil
```
Expected: all PASS.

- [ ] **Step 3: Coverage check**

Run:
```bash
go test -tags integration -coverprofile=coverage.out ./teams-user-sync/...
go tool cover -func=coverage.out | tail -5
```
Expected: total ≥ 80% (main.go is the only untested file; handler/store/scheduler/config coverage should put the package comfortably over). If below 80%, add unit tests for uncovered branches (check `go tool cover -func` output for the offending functions).

- [ ] **Step 4: SAST**

Run: `make sast`
Expected: clean at medium+. The new Graph calls use `http.NewRequestWithContext` with `io.LimitReader` — no known findings expected. Suppress only genuine false positives with `// #nosec <RULE> -- reason`.

- [ ] **Step 5: Push**

```bash
git push -u origin claude/teams-user-sync-service-sb7nuw
```

(Retry up to 4 times with 2s/4s/8s/16s backoff only on network errors.)

---

## Self-Review Notes

- **Spec coverage:** §1+§3.3 flow → Task 6; §3.1 scheduling/skip → Tasks 7–8; §3.2 Graph pagination → Task 3; §3.4 model → Task 1; §3.5 store + ConnectRead → Tasks 2, 5; §3.6 config → Task 4; §3.7 observability → Tasks 7 (runSync summary log), 8 (health); §5 testing → Tasks 1–7, 10; §6 deploy → Task 9. No client-api doc changes needed (no client-facing surface).
- **Type consistency:** `Store` methods (`ExistingIDs`, `HRSiteIDs`, `UpsertTeamsUsers`) are identical in Tasks 5, 6. `RunStats` fields consistent in Tasks 6, 10. `guardedJob`/`runSync`/`cronSlogLogger` names consistent in Tasks 7, 8. `NewUserListerClient` consistent in Tasks 3, 8, 10.
- **Known judgment calls:** `hr.siteID` field casing is exact (source), while `teams_user` uses camelCase `siteId` (repo convention) — both are asserted in integration tests.
