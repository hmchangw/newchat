# Teams Presence In-Call Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Periodically reflect a user's Microsoft Teams "in a call" state as an `in-call` chat presence (which suppresses notifications), via a one-shot cron binary that reads Graph presence and writes into the existing Valkey presence model.

**Architecture:** Extract the daemon's Valkey store into a shared `user-presence-service/presencestore` package, add an "external/azure" layer to the recompute Lua, and add a `SetExternal` mutation. A new `user-presence-service/sync` (`package main`, cron one-shot) lists tenant users (Graph app-only, cached as an id-map in Valkey), reads Teams presence (Graph ROPC), maps call/meeting activity to `in-call`, and reconciles the external layer + publishes `PresenceState`. Extend `pkg/msgraph` with `ListUsers` and an ROPC `PresenceClient`.

**Tech Stack:** Go 1.25, Valkey (go-redis cluster + Lua), MongoDB (mongo-driver/v2), NATS, Microsoft Graph, `caarlos0/env`, testify, `go.uber.org/mock`, testcontainers (`pkg/testutil`).

**Source spec:** `docs/superpowers/specs/2026-06-22-teams-presence-in-call-sync-design.md` (approved).

**Phases:** A (Tasks 1–6) presencestore + in-call precedence + SetExternal; B (Tasks 7–9) msgraph extensions; C (Tasks 10–17) the sync service, deploy, docs.

---

## Precedence (the core aggregation rule)

Recomputed in `computeLua` from connections (`anyLive`/`anyActive`), manual (`KEYS[2]`), azure (`KEYS[4]`):

```
if not anyLive               -> offline       (invariant)
else:
  manual == 'appear_offline' -> offline       (high manual tier)
  manual == 'away'           -> away          (high manual tier)
  azure  == 'in-call'        -> in-call       (external layer)
  manual == 'online'|'busy'  -> manual        (low manual tier)
  anyActive                  -> online        (connection-derived)
  else                       -> away
```

`in-call` is external-only (the `SetManual` allow-list is unchanged).

## Valkey keys (sync-owned, beyond the daemon's existing `presence:{account}:{conns,manual,status}` + `presence:sweep`)

- `presence:{account}:azure` — string, `"in-call"` or absent, written by `SetExternal` with a TTL safety-net (`EXTERNAL_TTL`). Read as `KEYS[4]`.
- `presence:status:index:azure` — set of accounts currently marked in-call (reconcile diff).
- `presence:idmap:azure` — hash `account → azureObjectID`.
- `presence:idmap:azure:fresh` — marker key with `IDMAP_REFRESH_TTL` gating the full `ListUsers` refresh.

---

## Phase A — presencestore + in-call precedence

### Task 1: Add `StatusInCall` to the model

**Files:**
- Modify: `pkg/model/presence.go`
- Test: `pkg/model/presence_test.go`

- [ ] **Step 1: Write the failing test**

Append to `pkg/model/presence_test.go` (create the file with `package model` + `import "testing"` if absent):

```go
func TestStatusInCall_Value(t *testing.T) {
	if StatusInCall != "in-call" {
		t.Fatalf("StatusInCall = %q, want %q", StatusInCall, "in-call")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/model/ -run TestStatusInCall_Value`
Expected: FAIL — `undefined: StatusInCall`.

- [ ] **Step 3: Add the constant**

In `pkg/model/presence.go`, in the `PresenceStatus` const block (after `StatusAppearOffline`), add:

```go
	// StatusInCall is set by the Teams presence sync (external); it is DND for
	// notifications and is never a valid manual status.
	StatusInCall PresenceStatus = "in-call"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/model/ -run TestStatusInCall_Value`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/model/presence.go pkg/model/presence_test.go
git commit -m "feat(model): add StatusInCall presence value"
```

---

### Task 2: Move the Valkey store into `presencestore` (verbatim move)

Pure relocation — no behavior change. New precedence + `SetExternal` come in Tasks 3–4.

**Files:**
- Create: `user-presence-service/presencestore/store.go`
- Delete: `user-presence-service/store_valkey.go`

- [ ] **Step 1: Create `presencestore/store.go` from the old file**

Copy the entire contents of `user-presence-service/store_valkey.go` into `user-presence-service/presencestore/store.go`, then apply exactly these edits:
- Package clause: `package presencestore`.
- Rename type `valkeyStore` → `Store` everywhere (struct decl + all method receivers `(s *valkeyStore)` → `(s *Store)`).
- `NewValkeyStore` returns `(*Store, error)` (was `(PresenceStore, error)`); its last line returns `newValkeyStoreFromClient(...)` — rename that call too.
- Export `newValkeyStoreFromClient` → `NewValkeyStoreFromClient` (returns `*Store`).
- Move `StatusChange` here (it is currently in `store.go`):

```go
// StatusChange is an account whose effective status was (re)computed.
type StatusChange struct {
	Account   string
	Effective model.PresenceStatus
}
```

- Change `Sweep` to return `[]StatusChange` (already does, now referring to the local type).
- Add the publish helper + type (moved out of the daemon's `handler.go`). Add imports `"log/slog"`, `"github.com/hmchangw/chat/pkg/natsutil"`, `"github.com/hmchangw/chat/pkg/subject"`:

```go
// PublishFunc publishes data to a subject (core NATS).
type PublishFunc func(ctx context.Context, subj string, data []byte) error

// PublishState marshals + publishes a PresenceState to the account's state
// subject. Failures are logged (best-effort fan-out, no caller to surface to).
func PublishState(ctx context.Context, publish PublishFunc, siteID, account string, status model.PresenceStatus, now time.Time) {
	st := model.PresenceState{Account: account, SiteID: siteID, Status: status, Timestamp: now.UTC().UnixMilli()}
	data, err := natsutil.MarshalResponse(st)
	if err != nil {
		slog.Error("publish presence state failed: marshal", "error", err, "account", account)
		return
	}
	if err := publish(ctx, subject.PresenceState(account), data); err != nil {
		slog.Error("publish presence state failed", "error", err, "account", account)
	}
}
```

- [ ] **Step 2: Delete the old file**

```bash
git rm user-presence-service/store_valkey.go
```

(The daemon will not compile until Task 4 Step 2 rewires it. That is expected; Tasks 2–4 land together.)

---

### Task 3: Add the azure layer to the Lua (new precedence)

**Files:**
- Modify: `user-presence-service/presencestore/store.go`

- [ ] **Step 1: Add the azure key builder**

Next to `connsKey`/`manualKey`/`statusKey`:

```go
func azureKey(account string) string { return keyPrefix + "{" + account + "}:azure" }
```

- [ ] **Step 2: Pass azure as `KEYS[4]` in `run`**

In `(s *Store) run`, change the keys slice to:

```go
		[]string{connsKey(account), manualKey(account), statusKey(account), azureKey(account)}, argv...,
```

- [ ] **Step 3: Replace the effective-status computation in `computeLua`**

In the `computeLua` string, replace this block:

```lua
local effective
if not anyLive then effective = 'offline'
elseif anyActive then effective = 'online'
else effective = 'away' end

local manual = redis.call('GET', KEYS[2])
if type(manual) == 'string' and manual ~= '' then
  if manual == 'appear_offline' then
    effective = 'offline'
  elseif anyLive then
    effective = manual
  else
    effective = 'offline'
  end
end
```

with:

```lua
local manual = redis.call('GET', KEYS[2])
local azure  = redis.call('GET', KEYS[4])
local m = ''
if type(manual) == 'string' then m = manual end
local a = ''
if type(azure) == 'string' then a = azure end

local effective
if not anyLive then
  effective = 'offline'
elseif m == 'appear_offline' then
  effective = 'offline'
elseif m == 'away' then
  effective = 'away'
elseif a == 'in-call' then
  effective = 'in-call'
elseif m == 'online' or m == 'busy' then
  effective = m
elseif anyActive then
  effective = 'online'
else
  effective = 'away'
end
```

Also update the `computeLua` doc comment's `KEYS` line to: `// KEYS[1]=conns hash  KEYS[2]=manual  KEYS[3]=status  KEYS[4]=azure`.

- [ ] **Step 4: Verify the Lua string compiles (vet)**

Run: `go vet ./user-presence-service/presencestore/`
Expected: only an error about missing `externalScript` (added next task) — no Lua/syntax errors. Proceed.

---

### Task 4: Add `externalScript` + `SetExternal`, rewire the daemon, regen mock

**Files:**
- Modify: `user-presence-service/presencestore/store.go`
- Modify: `user-presence-service/store.go`, `handler.go`, `sweeper.go`, `main.go`
- Modify: `user-presence-service/handler_test.go`, `sweeper_test.go`, `integration_test.go`
- Regen: `user-presence-service/mock_store_test.go`

- [ ] **Step 1: Add `externalScript` and `SetExternal`**

In `presencestore/store.go`, next to the other scripts:

```go
// externalScript sets the external (Teams) status key with a TTL safety-net, or
// clears it when ARGV[3] is the empty string, then recomputes.
// ARGV[3]=status  ARGV[4]=external_ttl_ms
var externalScript = redis.NewScript(luaHeader + `
if ARGV[3] == '' then
  redis.call('DEL', KEYS[4])
else
  redis.call('SET', KEYS[4], ARGV[3])
  redis.call('PEXPIRE', KEYS[4], tonumber(ARGV[4]))
end
` + computeLua)
```

Add the method (with the other methods):

```go
// SetExternal sets (status == StatusInCall) or clears (status == StatusNone)
// the external Teams override and recomputes. ttl bounds the external key's
// lifetime so a dead sync self-heals.
func (s *Store) SetExternal(ctx context.Context, account string, status model.PresenceStatus, ttl time.Duration) (bool, model.PresenceStatus, error) {
	statusArg := string(status)
	if status == model.StatusNone {
		statusArg = ""
	}
	return s.mutate(ctx, account, externalScript, statusArg, strconv.FormatInt(ttl.Milliseconds(), 10))
}
```

- [ ] **Step 2: Rewire the daemon package**

In `user-presence-service/store.go`:
- Add import `"github.com/hmchangw/chat/user-presence-service/presencestore"`.
- Delete the `StatusChange` struct.
- Change the `Sweep` signature in the `PresenceStore` interface to `Sweep(ctx context.Context, now time.Time) ([]presencestore.StatusChange, error)`.

In `user-presence-service/handler.go`:
- Delete the `PublishFunc` type (lines ~19–20) and the package-level `publishState` function (~245–261).
- Add import `"github.com/hmchangw/chat/user-presence-service/presencestore"`.
- Change the `Handler.publish` field type to `presencestore.PublishFunc`.
- Change `NewHandler`'s `publish` param type to `presencestore.PublishFunc`.
- In the `(h *Handler) publishState` method body, call `presencestore.PublishState(ctx, h.publish, h.siteID, account, status, h.now())`.

In `user-presence-service/sweeper.go`:
- Add import `"github.com/hmchangw/chat/user-presence-service/presencestore"`.
- Change the `Sweeper.publish` field and `NewSweeper`'s param type to `presencestore.PublishFunc`.
- In `tick`, change `publishState(...)` to `presencestore.PublishState(...)`.

In `user-presence-service/main.go`:
- Add import `"github.com/hmchangw/chat/user-presence-service/presencestore"`.
- Change the store construction to:

```go
	store, err := presencestore.NewValkeyStore(
		presencestore.ClusterConfig{Addrs: cfg.Valkey.Addrs, Password: cfg.Valkey.Password},
		cfg.Presence.StaleThreshold, cfg.Presence.ConnsTTL,
	)
```

- [ ] **Step 3: Fix daemon test type references**

In `handler_test.go` and `sweeper_test.go`: replace any `PublishFunc` with `presencestore.PublishFunc` and any bare `StatusChange{` with `presencestore.StatusChange{` (add the import). In `integration_test.go`: replace `newValkeyStoreFromClient(` with `presencestore.NewValkeyStoreFromClient(` and `ClusterConfig{`/`NewValkeyStore(` with the `presencestore.`-qualified forms (add the import). Any test that referenced the daemon-local `valkeyStore` type must use `*presencestore.Store`.

- [ ] **Step 4: Regenerate the mock**

The `PresenceStore` interface signature changed (`Sweep` return type). Run:

```bash
make generate SERVICE=user-presence-service
```

Expected: `mock_store_test.go` regenerated; `Sweep` returns `[]presencestore.StatusChange`.

- [ ] **Step 5: Build + run daemon unit tests**

```bash
go build ./user-presence-service/... ./pkg/...
make test SERVICE=user-presence-service
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add user-presence-service/ pkg/
git commit -m "refactor(presence): extract presencestore; add in-call external layer + SetExternal"
```

---

### Task 5: Presencestore integration tests (precedence + SetExternal)

**Files:**
- Create: `user-presence-service/presencestore/main_test.go`
- Create: `user-presence-service/presencestore/integration_test.go`

- [ ] **Step 1: Write TestMain**

Create `user-presence-service/presencestore/main_test.go`:

```go
//go:build integration

package presencestore

import (
	"testing"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }
```

- [ ] **Step 2: Write the precedence + SetExternal integration tests**

Create `user-presence-service/presencestore/integration_test.go`:

```go
//go:build integration

package presencestore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

const extTTL = 2 * time.Minute

func newStore(t *testing.T) *Store {
	t.Helper()
	client := testutil.StartValkeyCluster(t)
	return NewValkeyStoreFromClient(client, 45*time.Second, 5*time.Minute)
}

func TestSetExternal_InCall_LiveUser(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, eff, err := s.SetActivity(ctx, "alice", "c1", false)
	require.NoError(t, err)
	require.Equal(t, model.StatusOnline, eff)

	changed, eff, err := s.SetExternal(ctx, "alice", model.StatusInCall, extTTL)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, model.StatusInCall, eff)
}

func TestSetExternal_InCall_OfflineStaysOffline(t *testing.T) {
	s := newStore(t)
	changed, eff, err := s.SetExternal(context.Background(), "bob", model.StatusInCall, extTTL)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Equal(t, model.StatusOffline, eff)
}

func TestPrecedence_ManualAwayBeatsInCall(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, _, err := s.SetActivity(ctx, "carol", "c1", false)
	require.NoError(t, err)
	_, _, err = s.SetExternal(ctx, "carol", model.StatusInCall, extTTL)
	require.NoError(t, err)
	_, eff, err := s.SetManual(ctx, "carol", model.StatusAway)
	require.NoError(t, err)
	assert.Equal(t, model.StatusAway, eff)
}

func TestPrecedence_InCallBeatsManualBusy(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, _, err := s.SetActivity(ctx, "dave", "c1", false)
	require.NoError(t, err)
	_, _, err = s.SetManual(ctx, "dave", model.StatusBusy)
	require.NoError(t, err)
	_, eff, err := s.SetExternal(ctx, "dave", model.StatusInCall, extTTL)
	require.NoError(t, err)
	assert.Equal(t, model.StatusInCall, eff)
}

func TestPrecedence_AppearOfflineBeatsInCall(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, _, err := s.SetActivity(ctx, "erin", "c1", false)
	require.NoError(t, err)
	_, _, err = s.SetExternal(ctx, "erin", model.StatusInCall, extTTL)
	require.NoError(t, err)
	_, eff, err := s.SetManual(ctx, "erin", model.StatusAppearOffline)
	require.NoError(t, err)
	assert.Equal(t, model.StatusOffline, eff)
}

func TestSetExternal_Clear_RestoresConnectionDerived(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, _, err := s.SetActivity(ctx, "frank", "c1", false)
	require.NoError(t, err)
	_, _, err = s.SetExternal(ctx, "frank", model.StatusInCall, extTTL)
	require.NoError(t, err)
	_, eff, err := s.SetExternal(ctx, "frank", model.StatusNone, extTTL)
	require.NoError(t, err)
	assert.Equal(t, model.StatusOnline, eff)
}
```

- [ ] **Step 3: Run the integration tests**

Run: `make test-integration SERVICE=user-presence-service`
Expected: PASS (includes the new presencestore package).

- [ ] **Step 4: Commit**

```bash
git add user-presence-service/presencestore/
git commit -m "test(presencestore): precedence matrix + SetExternal integration tests"
```

---

### Task 6: Lint Phase A

- [ ] **Step 1: fmt + lint**

```bash
make fmt
make lint
```

Expected: clean. Fix import ordering / leftover symbols from the move.

- [ ] **Step 2: Commit if fmt changed files**

```bash
git add -A && git commit -m "style(presence): goimports after presencestore extraction" || true
```

---

## Phase B — pkg/msgraph extensions

### Task 7: App-only `ListUsers`

**Files:**
- Modify: `pkg/msgraph/msgraph.go`
- Modify: `pkg/msgraph/msgraph_test.go`

- [ ] **Step 1: Write the failing test**

Append to `pkg/msgraph/msgraph_test.go`:

```go
func TestListUsers_PagesAndReturnsAll(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	var graphURL string
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer tok", r.Header.Get("Authorization"))
		if r.URL.Query().Get("$skiptoken") == "" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value":           []GraphUser{{ID: "id1", Mail: "alice@corp.com", UserPrincipalName: "alice@corp.com"}},
				"@odata.nextLink": graphURL + "/users?$skiptoken=p2",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"value": []GraphUser{{ID: "id2", Mail: "", UserPrincipalName: "bob@corp.com"}},
		})
	}))
	defer graphSrv.Close()
	graphURL = graphSrv.URL

	c := newTestClient(tokenSrv.URL, graphSrv.URL)
	users, err := c.ListUsers(context.Background())
	require.NoError(t, err)
	require.Len(t, users, 2)
	assert.Equal(t, "id1", users[0].ID)
	assert.Equal(t, "bob@corp.com", users[1].UserPrincipalName)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/msgraph/ -run TestListUsers_PagesAndReturnsAll -v`
Expected: FAIL — `c.ListUsers undefined`, `GraphUser undefined`.

- [ ] **Step 3: Implement `GraphUser` + `ListUsers`**

In `pkg/msgraph/msgraph.go`, add to the `Client` interface:

```go
	// ListUsers returns all tenant users (id + mail + userPrincipalName),
	// following @odata.nextLink paging. App-only (User.Read.All).
	ListUsers(ctx context.Context) ([]GraphUser, error)
```

Add the types and method:

```go
// GraphUser is the subset of a Graph user resource the presence sync needs.
type GraphUser struct {
	ID                string `json:"id"`
	Mail              string `json:"mail"`
	UserPrincipalName string `json:"userPrincipalName"`
}

type graphUserPage struct {
	Value    []GraphUser `json:"value"`
	NextLink string      `json:"@odata.nextLink"`
}

func (g *graphClient) ListUsers(ctx context.Context) ([]GraphUser, error) {
	token, err := g.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire graph token: %w", err)
	}
	next := g.baseURL + "/users?$select=id,mail,userPrincipalName&$top=999"
	var out []GraphUser
	for next != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, next, nil)
		if err != nil {
			return nil, fmt.Errorf("build list-users request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := g.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("list users: %w", err)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
		closeErr := resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read list-users response: %w", err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close list-users response: %w", closeErr)
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("list users: graph returned status %d", resp.StatusCode)
		}
		var page graphUserPage
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("decode list-users response: %w", err)
		}
		out = append(out, page.Value...)
		next = page.NextLink
	}
	return out, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/msgraph/ -run TestListUsers -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/msgraph/msgraph.go pkg/msgraph/msgraph_test.go
git commit -m "feat(msgraph): app-only ListUsers with paging"
```

---

### Task 8: ROPC `PresenceClient.GetPresencesByUserId`

**Files:**
- Create: `pkg/msgraph/presence.go`
- Create: `pkg/msgraph/presence_test.go`

- [ ] **Step 1: Write the failing test**

Create `pkg/msgraph/presence_test.go`:

```go
package msgraph

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPresencesByUserId_ROPC(t *testing.T) {
	var grant, user string
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		grant = r.Form.Get("grant_type")
		user = r.Form.Get("username")
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "ptok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer ptok", r.Header.Get("Authorization"))
		assert.Contains(t, r.URL.Path, "/communications/getPresencesByUserId")
		var body struct {
			IDs []string `json:"ids"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, []string{"id1", "id2"}, body.IDs)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"value": []Presence{
				{ID: "id1", Availability: "Busy", Activity: "InACall"},
				{ID: "id2", Availability: "Available", Activity: "Available"},
			},
		})
	}))
	defer graphSrv.Close()

	pc := NewPresenceClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		ROPCCredentials{Username: "svc@corp.com", Password: "pw"},
		WithTokenURL(tokenSrv.URL), WithBaseURL(graphSrv.URL),
	)
	res, err := pc.GetPresencesByUserId(context.Background(), []string{"id1", "id2"})
	require.NoError(t, err)
	require.Len(t, res, 2)
	assert.Equal(t, "InACall", res[0].Activity)
	assert.Equal(t, "password", grant)
	assert.Equal(t, "svc@corp.com", user)
}

func TestGetPresencesByUserId_Empty(t *testing.T) {
	pc := NewPresenceClient(Config{TenantID: "t"}, ROPCCredentials{})
	res, err := pc.GetPresencesByUserId(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, res)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/msgraph/ -run TestGetPresencesByUserId -v`
Expected: FAIL — `NewPresenceClient`, `ROPCCredentials`, `Presence` undefined.

- [ ] **Step 3: Implement `pkg/msgraph/presence.go`**

```go
package msgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Presence is the subset of a Graph presence resource we consume.
type Presence struct {
	ID           string `json:"id"`
	Availability string `json:"availability"`
	Activity     string `json:"activity"`
}

// ROPCCredentials are the service-account credentials for the resource-owner
// password grant used to read presence (Presence.Read.All is delegated).
type ROPCCredentials struct {
	Username string
	Password string
}

// PresenceReader is the Graph presence surface the sync depends on.
type PresenceReader interface {
	GetPresencesByUserId(ctx context.Context, ids []string) ([]Presence, error)
}

// maxPresenceIDs is Graph's documented per-request cap for getPresencesByUserId.
const maxPresenceIDs = 650

type presenceClient struct {
	cfg      Config
	creds    ROPCCredentials
	hc       *http.Client
	baseURL  string
	tokenURL string

	mu      sync.Mutex
	token   string
	tokenAt time.Time
}

// NewPresenceClient builds an ROPC-backed presence reader. It reuses the
// app-only client's options (WithHTTPClient/WithBaseURL/WithTokenURL) by
// constructing a throwaway graphClient to resolve them.
func NewPresenceClient(cfg Config, creds ROPCCredentials, opts ...Option) PresenceReader {
	g := New(cfg, opts...).(*graphClient)
	return &presenceClient{
		cfg: cfg, creds: creds, hc: g.httpClient, baseURL: g.baseURL, tokenURL: g.tokenURL,
	}
}

func (p *presenceClient) accessToken(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" && time.Now().Before(p.tokenAt) {
		return p.token, nil
	}
	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("client_id", p.cfg.ClientID)
	form.Set("client_secret", p.cfg.ClientSecret)
	form.Set("scope", graphScope)
	form.Set("username", p.creds.Username)
	form.Set("password", p.creds.Password)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("build ropc token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("request ropc token: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read ropc token response: %w", err)
	}
	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", fmt.Errorf("decode ropc token response (status %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK || tr.AccessToken == "" {
		return "", fmt.Errorf("ropc token endpoint returned status %d: %s", resp.StatusCode, tr.Error)
	}
	p.token = tr.AccessToken
	lifetime := time.Duration(tr.ExpiresIn) * time.Second
	if lifetime <= tokenExpirySkew {
		lifetime = tokenExpirySkew
	}
	p.tokenAt = time.Now().Add(lifetime - tokenExpirySkew)
	return p.token, nil
}

func (p *presenceClient) GetPresencesByUserId(ctx context.Context, ids []string) ([]Presence, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	token, err := p.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire presence token: %w", err)
	}
	var out []Presence
	for start := 0; start < len(ids); start += maxPresenceIDs {
		end := start + maxPresenceIDs
		if end > len(ids) {
			end = len(ids)
		}
		batch, err := p.fetch(ctx, token, ids[start:end])
		if err != nil {
			return nil, err
		}
		out = append(out, batch...)
	}
	return out, nil
}

func (p *presenceClient) fetch(ctx context.Context, token string, ids []string) ([]Presence, error) {
	payload, err := json.Marshal(struct {
		IDs []string `json:"ids"`
	}{IDs: ids})
	if err != nil {
		return nil, fmt.Errorf("marshal presence ids: %w", err)
	}
	endpoint := p.baseURL + "/communications/getPresencesByUserId"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build presence request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get presences: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
	if err != nil {
		return nil, fmt.Errorf("read presence response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get presences: graph returned status %d", resp.StatusCode)
	}
	var pr struct {
		Value []Presence `json:"value"`
	}
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, fmt.Errorf("decode presence response: %w", err)
	}
	return pr.Value, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/msgraph/ -run TestGetPresencesByUserId -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/msgraph/presence.go pkg/msgraph/presence_test.go
git commit -m "feat(msgraph): ROPC PresenceClient.GetPresencesByUserId"
```

---

### Task 9: Lint + SAST Phase B

- [ ] **Step 1: fmt + lint + gosec**

```bash
make fmt
make lint
make sast-gosec
```

Expected: clean. The ROPC password is config-only and never logged (only `tr.Error` is surfaced). Add `// #nosec <RULE> -- reason` only for a genuine false positive.

- [ ] **Step 2: Commit if needed**

```bash
git add -A && git commit -m "style(msgraph): lint fixes" || true
```

---

## Phase C — the sync cron service

### Task 10: Sync skeleton — interfaces + Mongo account lister

**Files:**
- Create: `user-presence-service/sync/store.go`
- Create: `user-presence-service/sync/store_mongo.go`

- [ ] **Step 1: Define the consumer interfaces**

Create `user-presence-service/sync/store.go`:

```go
package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

//go:generate mockgen -source=store.go -destination=mock_test.go -package=main

// accountLister returns every account homed at this site (presence is
// site-scoped). Only the account field is projected.
type accountLister interface {
	ListSiteAccounts(ctx context.Context, siteID string) ([]string, error)
}

// userLister lists tenant users (Graph app-only). Satisfied by msgraph.Client.
type userLister interface {
	ListUsers(ctx context.Context) ([]msgraph.GraphUser, error)
}

// presenceReader reads Teams presence (Graph ROPC). Satisfied by
// msgraph.PresenceReader.
type presenceReader interface {
	GetPresencesByUserId(ctx context.Context, ids []string) ([]msgraph.Presence, error)
}

// externalApplier applies the per-account external status and reports whether
// the effective status changed. Satisfied by *presencestore.Store.
type externalApplier interface {
	SetExternal(ctx context.Context, account string, status model.PresenceStatus, ttl time.Duration) (bool, model.PresenceStatus, error)
}

// inCallIndex tracks accounts currently marked in-call so a run can clear those
// no longer in a call.
type inCallIndex interface {
	Members(ctx context.Context) ([]string, error)
	Add(ctx context.Context, account string) error
	Remove(ctx context.Context, account string) error
}

// idMapStore caches account -> azureObjectID and gates the periodic ListUsers
// refresh via a freshness marker.
type idMapStore interface {
	Fresh(ctx context.Context) (bool, error)
	Refresh(ctx context.Context, mapping map[string]string, ttl time.Duration) error
	Resolve(ctx context.Context, accounts []string) (map[string]string, error)
}

// statePublisher publishes a PresenceState change (best-effort fan-out).
type statePublisher interface {
	Publish(ctx context.Context, account string, status model.PresenceStatus)
}
```

- [ ] **Step 2: Implement the Mongo account lister**

Create `user-presence-service/sync/store_mongo.go`:

```go
package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mongoAccountStore struct {
	col *mongo.Collection
}

func newMongoAccountStore(col *mongo.Collection) *mongoAccountStore {
	return &mongoAccountStore{col: col}
}

// ListSiteAccounts returns the account of every user homed at siteID.
func (s *mongoAccountStore) ListSiteAccounts(ctx context.Context, siteID string) ([]string, error) {
	cursor, err := s.col.Find(ctx,
		bson.M{"siteId": siteID},
		options.Find().SetProjection(bson.M{"_id": 0, "account": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("list site accounts: %w", err)
	}
	defer cursor.Close(ctx)
	var rows []struct {
		Account string `bson:"account"`
	}
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode site accounts: %w", err)
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if r.Account != "" {
			out = append(out, r.Account)
		}
	}
	return out, nil
}
```

- [ ] **Step 3: Generate mocks**

Run: `make generate SERVICE=user-presence-service/sync`
Expected: `user-presence-service/sync/mock_test.go` created with mocks for all interfaces in `store.go`.

- [ ] **Step 4: Commit**

```bash
git add user-presence-service/sync/store.go user-presence-service/sync/store_mongo.go user-presence-service/sync/mock_test.go
git commit -m "feat(presence-sync): consumer interfaces + mongo account lister"
```

---

### Task 11: Pure mapping helpers (`isInCall`, `accountFromEmail`)

**Files:**
- Create: `user-presence-service/sync/reconcile.go`
- Create: `user-presence-service/sync/reconcile_test.go`

- [ ] **Step 1: Write the failing tests**

Create `user-presence-service/sync/reconcile_test.go`:

```go
package main

import (
	"testing"

	"github.com/hmchangw/chat/pkg/msgraph"
)

func TestIsInCall(t *testing.T) {
	cases := []struct {
		name string
		p    msgraph.Presence
		want bool
	}{
		{"in a call", msgraph.Presence{Availability: "Busy", Activity: "InACall"}, true},
		{"conference", msgraph.Presence{Availability: "Busy", Activity: "InAConferenceCall"}, true},
		{"presenting", msgraph.Presence{Availability: "DoNotDisturb", Activity: "Presenting"}, true},
		{"available", msgraph.Presence{Availability: "Available", Activity: "Available"}, false},
		{"meeting not call", msgraph.Presence{Availability: "Busy", Activity: "InAMeeting"}, false},
		{"away", msgraph.Presence{Availability: "Away", Activity: "Away"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isInCall(tc.p); got != tc.want {
				t.Fatalf("isInCall(%+v) = %v, want %v", tc.p, got, tc.want)
			}
		})
	}
}

func TestAccountFromEmail(t *testing.T) {
	cases := []struct {
		email, domain, want string
		ok                  bool
	}{
		{"alice@corp.com", "corp.com", "alice", true},
		{"Bob@CORP.com", "corp.com", "Bob", true},
		{"carol@other.com", "corp.com", "", false},
		{"nodomain", "corp.com", "", false},
	}
	for _, tc := range cases {
		got, ok := accountFromEmail(tc.email, tc.domain)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("accountFromEmail(%q,%q) = (%q,%v), want (%q,%v)", tc.email, tc.domain, got, ok, tc.want, tc.ok)
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./user-presence-service/sync/ -run 'TestIsInCall|TestAccountFromEmail' -v`
Expected: FAIL — `isInCall`, `accountFromEmail` undefined.

- [ ] **Step 3: Implement the helpers**

Create `user-presence-service/sync/reconcile.go`:

```go
package main

import (
	"strings"

	"github.com/hmchangw/chat/pkg/msgraph"
)

// callActivities are the Teams activities that map to our in-call status
// (call/meeting activities only, per design).
var callActivities = map[string]struct{}{
	"InACall":           {},
	"InAConferenceCall": {},
	"Presenting":        {},
}

// isInCall reports whether a Teams presence reflects an active call.
func isInCall(p msgraph.Presence) bool {
	_, ok := callActivities[p.Activity]
	return ok
}

// accountFromEmail returns the local part of an email when its domain matches
// (case-insensitive on domain); ok=false otherwise.
func accountFromEmail(email, domain string) (string, bool) {
	at := strings.LastIndex(email, "@")
	if at < 0 {
		return "", false
	}
	if !strings.EqualFold(email[at+1:], domain) {
		return "", false
	}
	return email[:at], true
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./user-presence-service/sync/ -run 'TestIsInCall|TestAccountFromEmail' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add user-presence-service/sync/reconcile.go user-presence-service/sync/reconcile_test.go
git commit -m "feat(presence-sync): Teams activity + email mapping helpers"
```

---

### Task 12: Reconciler orchestration (cache-gated)

**Files:**
- Modify: `user-presence-service/sync/reconcile.go`
- Modify: `user-presence-service/sync/reconcile_test.go`

- [ ] **Step 1: Write the failing reconcile tests**

Append to `user-presence-service/sync/reconcile_test.go`:

```go
import (
	"context"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

func newTestReconciler(t *testing.T) (*reconciler, *MockaccountLister, *MockuserLister, *MockpresenceReader, *MockexternalApplier, *MockinCallIndex, *MockidMapStore, *MockstatePublisher) {
	ctrl := gomock.NewController(t)
	accts := NewMockaccountLister(ctrl)
	users := NewMockuserLister(ctrl)
	pres := NewMockpresenceReader(ctrl)
	app := NewMockexternalApplier(ctrl)
	idx := NewMockinCallIndex(ctrl)
	idm := NewMockidMapStore(ctrl)
	pub := NewMockstatePublisher(ctrl)
	cfg := reconcileConfig{SiteID: "site-a", EmailDomain: "corp.com", ExternalTTL: time.Minute, IDMapRefreshTTL: time.Hour}
	return newReconciler(accts, users, pres, app, idx, idm, pub, cfg), accts, users, pres, app, idx, idm, pub
}

func TestReconcile_FreshCache_SetsAndClears(t *testing.T) {
	r, accts, users, pres, app, idx, idm, pub := newTestReconciler(t)
	ctx := context.Background()

	accts.EXPECT().ListSiteAccounts(ctx, "site-a").Return([]string{"alice", "bob"}, nil)
	idm.EXPECT().Fresh(ctx).Return(true, nil) // cache warm: no ListUsers
	idm.EXPECT().Resolve(ctx, []string{"alice", "bob"}).Return(map[string]string{"alice": "ida", "bob": "idb"}, nil)
	pres.EXPECT().GetPresencesByUserId(ctx, gomock.Len(2)).Return([]msgraph.Presence{
		{ID: "ida", Activity: "InACall"},
		{ID: "idb", Activity: "Available"},
	}, nil)
	idx.EXPECT().Members(ctx).Return([]string{"bob"}, nil) // bob was in-call last run

	app.EXPECT().SetExternal(ctx, "alice", model.StatusInCall, time.Minute).Return(true, model.StatusInCall, nil)
	idx.EXPECT().Add(ctx, "alice").Return(nil)
	pub.EXPECT().Publish(ctx, "alice", model.StatusInCall)

	app.EXPECT().SetExternal(ctx, "bob", model.StatusNone, time.Minute).Return(true, model.StatusOnline, nil)
	idx.EXPECT().Remove(ctx, "bob").Return(nil)
	pub.EXPECT().Publish(ctx, "bob", model.StatusOnline)

	users.EXPECT().ListUsers(gomock.Any()).Times(0)

	require.NoError(t, r.run(ctx))
}

func TestReconcile_StaleCache_RefreshesIdMap(t *testing.T) {
	r, accts, users, pres, app, idx, idm, pub := newTestReconciler(t)
	ctx := context.Background()

	accts.EXPECT().ListSiteAccounts(ctx, "site-a").Return([]string{"alice"}, nil)
	idm.EXPECT().Fresh(ctx).Return(false, nil) // stale -> refresh
	users.EXPECT().ListUsers(ctx).Return([]msgraph.GraphUser{
		{ID: "ida", Mail: "alice@corp.com"},
		{ID: "idz", Mail: "stranger@other.com"}, // filtered: wrong domain
	}, nil)
	idm.EXPECT().Refresh(ctx, map[string]string{"alice": "ida"}, time.Hour).Return(nil)
	idm.EXPECT().Resolve(ctx, []string{"alice"}).Return(map[string]string{"alice": "ida"}, nil)
	pres.EXPECT().GetPresencesByUserId(ctx, []string{"ida"}).Return([]msgraph.Presence{
		{ID: "ida", Activity: "InACall"},
	}, nil)
	idx.EXPECT().Members(ctx).Return(nil, nil)
	app.EXPECT().SetExternal(ctx, "alice", model.StatusInCall, time.Minute).Return(true, model.StatusInCall, nil)
	idx.EXPECT().Add(ctx, "alice").Return(nil)
	pub.EXPECT().Publish(ctx, "alice", model.StatusInCall)

	require.NoError(t, r.run(ctx))
}

func TestReconcile_NoChange_NoPublish(t *testing.T) {
	r, accts, users, pres, app, idx, idm, pub := newTestReconciler(t)
	ctx := context.Background()
	_ = users
	_ = pub

	accts.EXPECT().ListSiteAccounts(ctx, "site-a").Return([]string{"alice"}, nil)
	idm.EXPECT().Fresh(ctx).Return(true, nil)
	idm.EXPECT().Resolve(ctx, []string{"alice"}).Return(map[string]string{"alice": "ida"}, nil)
	pres.EXPECT().GetPresencesByUserId(ctx, []string{"ida"}).Return([]msgraph.Presence{
		{ID: "ida", Activity: "InACall"},
	}, nil)
	idx.EXPECT().Members(ctx).Return([]string{"alice"}, nil) // already in-call
	app.EXPECT().SetExternal(ctx, "alice", model.StatusInCall, time.Minute).Return(false, model.StatusInCall, nil)
	idx.EXPECT().Add(ctx, "alice").Return(nil)
	// changed=false -> no Publish

	require.NoError(t, r.run(ctx))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./user-presence-service/sync/ -run TestReconcile -v`
Expected: FAIL — `newReconciler`, `reconcileConfig` undefined.

- [ ] **Step 3: Implement the reconciler**

Append to `user-presence-service/sync/reconcile.go` (add imports `"context"`, `"fmt"`, `"log/slog"`, `"time"`, `"github.com/hmchangw/chat/pkg/model"`):

```go
type reconcileConfig struct {
	SiteID          string
	EmailDomain     string
	ExternalTTL     time.Duration
	IDMapRefreshTTL time.Duration
}

type reconciler struct {
	accts accountLister
	users userLister
	pres  presenceReader
	app   externalApplier
	idx   inCallIndex
	idm   idMapStore
	pub   statePublisher
	cfg   reconcileConfig
}

func newReconciler(accts accountLister, users userLister, pres presenceReader, app externalApplier, idx inCallIndex, idm idMapStore, pub statePublisher, cfg reconcileConfig) *reconciler {
	return &reconciler{accts: accts, users: users, pres: pres, app: app, idx: idx, idm: idm, pub: pub, cfg: cfg}
}

// run performs one full reconciliation.
func (r *reconciler) run(ctx context.Context) error {
	accounts, err := r.accts.ListSiteAccounts(ctx, r.cfg.SiteID)
	if err != nil {
		return fmt.Errorf("list site accounts: %w", err)
	}

	if err := r.refreshIfStale(ctx, accounts); err != nil {
		return err
	}

	idByAccount, err := r.idm.Resolve(ctx, accounts)
	if err != nil {
		return fmt.Errorf("resolve id map: %w", err)
	}
	ids := make([]string, 0, len(idByAccount))
	accountByID := make(map[string]string, len(idByAccount))
	for account, id := range idByAccount {
		ids = append(ids, id)
		accountByID[id] = account
	}

	presences, err := r.pres.GetPresencesByUserId(ctx, ids)
	if err != nil {
		return fmt.Errorf("get presences: %w", err)
	}
	current := make(map[string]struct{}, len(presences))
	for _, p := range presences {
		if !isInCall(p) {
			continue
		}
		if account, ok := accountByID[p.ID]; ok {
			current[account] = struct{}{}
		}
	}

	prev, err := r.idx.Members(ctx)
	if err != nil {
		return fmt.Errorf("read in-call index: %w", err)
	}

	for account := range current {
		if err := r.apply(ctx, account, model.StatusInCall); err != nil {
			return err
		}
	}
	for _, account := range prev {
		if _, still := current[account]; still {
			continue
		}
		if err := r.apply(ctx, account, model.StatusNone); err != nil {
			return err
		}
	}

	slog.Info("teams presence reconcile complete",
		"site", r.cfg.SiteID, "accounts", len(accounts), "inCall", len(current))
	return nil
}

// refreshIfStale rebuilds the id map from a full ListUsers when the freshness
// marker has expired; otherwise it does nothing.
func (r *reconciler) refreshIfStale(ctx context.Context, accounts []string) error {
	fresh, err := r.idm.Fresh(ctx)
	if err != nil {
		return fmt.Errorf("check id map freshness: %w", err)
	}
	if fresh {
		return nil
	}
	ours := make(map[string]struct{}, len(accounts))
	for _, a := range accounts {
		ours[a] = struct{}{}
	}
	tenant, err := r.users.ListUsers(ctx)
	if err != nil {
		return fmt.Errorf("list tenant users: %w", err)
	}
	mapping := make(map[string]string, len(ours))
	for _, u := range tenant {
		if u.ID == "" {
			continue
		}
		email := u.Mail
		if email == "" {
			email = u.UserPrincipalName
		}
		account, ok := accountFromEmail(email, r.cfg.EmailDomain)
		if !ok {
			continue
		}
		if _, mine := ours[account]; !mine {
			continue
		}
		mapping[account] = u.ID
	}
	if err := r.idm.Refresh(ctx, mapping, r.cfg.IDMapRefreshTTL); err != nil {
		return fmt.Errorf("refresh id map: %w", err)
	}
	return nil
}

// apply sets/clears the external status, updates the in-call index, and
// publishes a state change only when the effective status changed.
func (r *reconciler) apply(ctx context.Context, account string, status model.PresenceStatus) error {
	changed, eff, err := r.app.SetExternal(ctx, account, status, r.cfg.ExternalTTL)
	if err != nil {
		return fmt.Errorf("set external %q: %w", account, err)
	}
	if status == model.StatusNone {
		if err := r.idx.Remove(ctx, account); err != nil {
			return fmt.Errorf("index remove %q: %w", account, err)
		}
	} else {
		if err := r.idx.Add(ctx, account); err != nil {
			return fmt.Errorf("index add %q: %w", account, err)
		}
	}
	if changed {
		r.pub.Publish(ctx, account, eff)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./user-presence-service/sync/ -run TestReconcile -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add user-presence-service/sync/reconcile.go user-presence-service/sync/reconcile_test.go
git commit -m "feat(presence-sync): cache-gated reconcile orchestration"
```

---

### Task 13: Valkey adapters (in-call index, id-map, publisher)

**Files:**
- Create: `user-presence-service/sync/valkey.go`

- [ ] **Step 1: Implement the adapters**

Create `user-presence-service/sync/valkey.go`:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-presence-service/presencestore"
)

const (
	inCallIndexKey  = "presence:status:index:azure"
	idMapKey        = "presence:idmap:azure"
	idMapFreshKey   = "presence:idmap:azure:fresh"
)

// --- in-call index ---

type valkeyInCallIndex struct{ c *redis.ClusterClient }

func newValkeyInCallIndex(c *redis.ClusterClient) *valkeyInCallIndex { return &valkeyInCallIndex{c: c} }

func (v *valkeyInCallIndex) Members(ctx context.Context) ([]string, error) {
	m, err := v.c.SMembers(ctx, inCallIndexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("smembers in-call index: %w", err)
	}
	return m, nil
}

func (v *valkeyInCallIndex) Add(ctx context.Context, account string) error {
	if err := v.c.SAdd(ctx, inCallIndexKey, account).Err(); err != nil {
		return fmt.Errorf("sadd in-call index %q: %w", account, err)
	}
	return nil
}

func (v *valkeyInCallIndex) Remove(ctx context.Context, account string) error {
	if err := v.c.SRem(ctx, inCallIndexKey, account).Err(); err != nil {
		return fmt.Errorf("srem in-call index %q: %w", account, err)
	}
	return nil
}

// --- id map ---

type valkeyIDMap struct{ c *redis.ClusterClient }

func newValkeyIDMap(c *redis.ClusterClient) *valkeyIDMap { return &valkeyIDMap{c: c} }

func (v *valkeyIDMap) Fresh(ctx context.Context) (bool, error) {
	n, err := v.c.Exists(ctx, idMapFreshKey).Result()
	if err != nil {
		return false, fmt.Errorf("exists id map marker: %w", err)
	}
	return n == 1, nil
}

// Refresh replaces the id map hash and resets the freshness marker. The marker
// TTL drives the next refresh; the hash itself has no TTL (rebuilt wholesale).
func (v *valkeyIDMap) Refresh(ctx context.Context, mapping map[string]string, ttl time.Duration) error {
	pipe := v.c.TxPipeline()
	pipe.Del(ctx, idMapKey)
	if len(mapping) > 0 {
		vals := make([]any, 0, len(mapping)*2)
		for account, id := range mapping {
			vals = append(vals, account, id)
		}
		pipe.HSet(ctx, idMapKey, vals...)
	}
	pipe.Set(ctx, idMapFreshKey, "1", ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("refresh id map: %w", err)
	}
	return nil
}

// Resolve returns account -> id for the accounts present in the hash.
func (v *valkeyIDMap) Resolve(ctx context.Context, accounts []string) (map[string]string, error) {
	out := make(map[string]string, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	vals, err := v.c.HMGet(ctx, idMapKey, accounts...).Result()
	if err != nil {
		return nil, fmt.Errorf("hmget id map: %w", err)
	}
	for i, raw := range vals {
		if id, ok := raw.(string); ok && id != "" {
			out[accounts[i]] = id
		}
	}
	return out, nil
}

// --- publisher ---

type natsPublisher struct {
	publish presencestore.PublishFunc
	siteID  string
}

func (n natsPublisher) Publish(ctx context.Context, account string, status model.PresenceStatus) {
	presencestore.PublishState(ctx, n.publish, n.siteID, account, status, time.Now())
}
```

Note: `idMapKey`, `idMapFreshKey`, and `presence:{account}:azure` share neither a hash-tag nor a slot — that's fine, they are touched by independent commands (the `Refresh` pipeline uses `TxPipeline` but `idMapKey` and `idMapFreshKey` are different slots; if the cluster rejects the cross-slot MULTI, split into two non-transactional `Pipeline` calls — acceptable, the marker is a best-effort gate). Prefer plain `Pipeline()` if cross-slot transactions error in the integration test (Task 15).

- [ ] **Step 2: Build**

Run: `go build ./user-presence-service/sync/`
Expected: FAIL (no `package main` entrypoint `main()` yet) — but the file itself must compile within the package. Confirm no type errors via `go vet ./user-presence-service/sync/`. Proceed.

- [ ] **Step 3: Commit**

```bash
git add user-presence-service/sync/valkey.go
git commit -m "feat(presence-sync): valkey in-call index, id-map cache, publisher"
```

---

### Task 14: `main.go` wiring

**Files:**
- Create: `user-presence-service/sync/main.go`

- [ ] **Step 1: Write `main.go`**

Create `user-presence-service/sync/main.go`:

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/redis/go-redis/v9"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/user-presence-service/presencestore"
)

// Config is the sync's environment configuration.
type Config struct {
	SiteID          string        `env:"SITE_ID,required"`
	EmailDomain     string        `env:"TEAMS_EMAIL_DOMAIN,required"`
	ExternalTTL     time.Duration `env:"EXTERNAL_TTL" envDefault:"5m"`
	IDMapRefreshTTL time.Duration `env:"IDMAP_REFRESH_TTL" envDefault:"1h"`
	RunTimeout      time.Duration `env:"RUN_TIMEOUT" envDefault:"5m"`
	StaleThreshold  time.Duration `env:"PRESENCE_STALE_THRESHOLD" envDefault:"45s"`
	ConnsTTL        time.Duration `env:"PRESENCE_CONNS_TTL" envDefault:"5m"`

	NATSURL       string `env:"NATS_URL,required"`
	NATSCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`

	ValkeyAddrs    []string `env:"VALKEY_ADDRS,required" envSeparator:","`
	ValkeyPassword string   `env:"VALKEY_PASSWORD" envDefault:""`

	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`

	GraphTenantID     string `env:"GRAPH_TENANT_ID,required"`
	GraphClientID     string `env:"GRAPH_CLIENT_ID,required"`
	GraphClientSecret string `env:"GRAPH_CLIENT_SECRET,required"`
	GraphROPCUser     string `env:"GRAPH_ROPC_USERNAME,required"`
	GraphROPCPassword string `env:"GRAPH_ROPC_PASSWORD,required"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[Config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.RunTimeout)
	defer cancel()

	tracerShutdown, err := otelutil.InitTracer(ctx, "user-presence-sync")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := tracerShutdown(context.Background()); err != nil {
			slog.Warn("tracer shutdown", "error", err)
		}
	}()

	clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: cfg.ValkeyAddrs, Password: cfg.ValkeyPassword,
	})
	defer func() {
		if err := clusterClient.Close(); err != nil {
			slog.Warn("valkey close", "error", err)
		}
	}()
	store := presencestore.NewValkeyStoreFromClient(clusterClient, cfg.StaleThreshold, cfg.ConnsTTL)

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	defer mongoutil.Disconnect(context.Background(), mongoClient)
	accts := newMongoAccountStore(mongoClient.Database(cfg.MongoDB).Collection("users"))

	nc, err := natsutil.Connect(cfg.NATSURL, cfg.NATSCredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := nc.Drain(); err != nil {
			slog.Warn("nats drain", "error", err)
		}
	}()
	publish := func(ctx context.Context, subj string, data []byte) error {
		return nc.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data))
	}

	graphCfg := msgraph.Config{TenantID: cfg.GraphTenantID, ClientID: cfg.GraphClientID, ClientSecret: cfg.GraphClientSecret}
	users := msgraph.New(graphCfg)
	pres := msgraph.NewPresenceClient(graphCfg, msgraph.ROPCCredentials{Username: cfg.GraphROPCUser, Password: cfg.GraphROPCPassword})

	r := newReconciler(
		accts, users, pres, store,
		newValkeyInCallIndex(clusterClient),
		newValkeyIDMap(clusterClient),
		natsPublisher{publish: publish, siteID: cfg.SiteID},
		reconcileConfig{
			SiteID: cfg.SiteID, EmailDomain: cfg.EmailDomain,
			ExternalTTL: cfg.ExternalTTL, IDMapRefreshTTL: cfg.IDMapRefreshTTL,
		},
	)

	if err := r.run(ctx); err != nil {
		slog.Error("reconcile failed", "error", err)
		os.Exit(1)
	}
	slog.Info("user-presence-sync done", "site", cfg.SiteID)
}
```

- [ ] **Step 2: Build the whole service**

Run: `go build ./user-presence-service/...`
Expected: PASS. (`msgraph.New(...)` returns `Client` which has `ListUsers` → satisfies `userLister`; `msgraph.NewPresenceClient(...)` returns `PresenceReader` → satisfies `presenceReader`; `*presencestore.Store` → satisfies `externalApplier`.)

- [ ] **Step 3: Run all sync unit tests + full unit suite**

```bash
go test ./user-presence-service/sync/
make test SERVICE=user-presence-service
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add user-presence-service/sync/main.go
git commit -m "feat(presence-sync): main wiring (one-shot reconcile)"
```

---

### Task 15: id-map adapter integration test

**Files:**
- Create: `user-presence-service/sync/main_test.go`
- Create: `user-presence-service/sync/valkey_integration_test.go`

- [ ] **Step 1: Write TestMain**

Create `user-presence-service/sync/main_test.go`:

```go
//go:build integration

package main

import (
	"testing"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }
```

- [ ] **Step 2: Write the id-map + index integration tests**

Create `user-presence-service/sync/valkey_integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestValkeyIDMap_RefreshResolveFresh(t *testing.T) {
	client := testutil.StartValkeyCluster(t)
	ctx := context.Background()
	idm := newValkeyIDMap(client)

	fresh, err := idm.Fresh(ctx)
	require.NoError(t, err)
	assert.False(t, fresh)

	require.NoError(t, idm.Refresh(ctx, map[string]string{"alice": "ida", "bob": "idb"}, time.Hour))

	fresh, err = idm.Fresh(ctx)
	require.NoError(t, err)
	assert.True(t, fresh)

	got, err := idm.Resolve(ctx, []string{"alice", "carol"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"alice": "ida"}, got)
}

func TestValkeyInCallIndex_AddMembersRemove(t *testing.T) {
	client := testutil.StartValkeyCluster(t)
	ctx := context.Background()
	idx := newValkeyInCallIndex(client)

	require.NoError(t, idx.Add(ctx, "alice"))
	require.NoError(t, idx.Add(ctx, "bob"))
	m, err := idx.Members(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"alice", "bob"}, m)

	require.NoError(t, idx.Remove(ctx, "alice"))
	m, err = idx.Members(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"bob"}, m)
}
```

- [ ] **Step 3: Run the integration tests**

Run: `make test-integration SERVICE=user-presence-service/sync`
Expected: PASS. If `Refresh`'s `TxPipeline` errors with a CROSSSLOT message, switch `v.c.TxPipeline()` to `v.c.Pipeline()` in `valkey.go` and re-run.

- [ ] **Step 4: Commit**

```bash
git add user-presence-service/sync/main_test.go user-presence-service/sync/valkey_integration_test.go
git commit -m "test(presence-sync): id-map + in-call index integration tests"
```

---

### Task 16: Deploy artifacts

**Files:**
- Create: `user-presence-service/sync/deploy/Dockerfile`
- Create: `user-presence-service/sync/deploy/docker-compose.yml`
- Create: `user-presence-service/sync/deploy/azure-pipelines.yml`

- [ ] **Step 1: Dockerfile**

Create `user-presence-service/sync/deploy/Dockerfile`:

```dockerfile
FROM golang:1.25.11-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY user-presence-service/ user-presence-service/
RUN CGO_ENABLED=0 go build -o /user-presence-sync ./user-presence-service/sync/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /user-presence-sync /user-presence-sync
USER app
ENTRYPOINT ["/user-presence-sync"]
```

- [ ] **Step 2: docker-compose (local dev, run-once)**

Create `user-presence-service/sync/deploy/docker-compose.yml`:

```yaml
name: user-presence-sync

services:
  user-presence-sync:
    build:
      context: ../../..
      dockerfile: user-presence-service/sync/deploy/Dockerfile
    environment:
      - SITE_ID=site-local
      - TEAMS_EMAIL_DOMAIN=dev.local
      - EXTERNAL_TTL=5m
      - IDMAP_REFRESH_TTL=1h
      - NATS_URL=nats://nats:4222
      - NATS_CREDS_FILE=/etc/nats/backend.creds
      - VALKEY_ADDRS=valkey:6379
      - MONGO_URI=mongodb://mongo:27017
      - MONGO_DB=chat
      - GRAPH_TENANT_ID=${GRAPH_TENANT_ID}
      - GRAPH_CLIENT_ID=${GRAPH_CLIENT_ID}
      - GRAPH_CLIENT_SECRET=${GRAPH_CLIENT_SECRET}
      - GRAPH_ROPC_USERNAME=${GRAPH_ROPC_USERNAME}
      - GRAPH_ROPC_PASSWORD=${GRAPH_ROPC_PASSWORD}
    volumes:
      - ../../../docker-local/backend.creds:/etc/nats/backend.creds:ro
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

- [ ] **Step 3: azure-pipelines.yml**

Read `user-presence-service/deploy/azure-pipelines.yml`, then create `user-presence-service/sync/deploy/azure-pipelines.yml` by copying it and changing only: the trigger `paths` to include `user-presence-service/sync/*` and `pkg/*`; the build/output binary path to `./user-presence-service/sync/`; and the Docker image/artifact name to `user-presence-sync`. Keep every other stage identical to the source file.

- [ ] **Step 4: Validate the Dockerfile builds**

Run: `docker build -f user-presence-service/sync/deploy/Dockerfile -t user-presence-sync:plan .`
Expected: builds successfully.

- [ ] **Step 5: Commit**

```bash
git add user-presence-service/sync/deploy/
git commit -m "build(presence-sync): Dockerfile, compose, pipeline"
```

---

### Task 17: Docs + full verification + push

**Files:**
- Modify: `docs/client-api.md`

- [ ] **Step 1: Document `in-call` in the presence status enum**

In `docs/client-api.md`, find where the presence `status` values are enumerated (search for `appear_offline`). Add `in-call` as a possible value with a one-line note (minimal prose, matching the doc's style):

> `in-call` — set by the Teams presence sync (external); notifications are suppressed (DND). Not settable as a manual status.

If a `PresenceState` payload field table lists the allowed `status` values, add `in-call` there too.

- [ ] **Step 2: Full unit + lint + sast**

```bash
make test
make lint
make sast
```

Expected: all PASS; no medium+ SAST findings.

- [ ] **Step 3: Full integration for the touched services**

```bash
make test-integration SERVICE=user-presence-service
make test-integration SERVICE=user-presence-service/sync
```

Expected: PASS.

- [ ] **Step 4: Commit docs**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): document in-call presence status"
```

- [ ] **Step 5: Push the branch**

```bash
git push -u origin claude/dreamy-dirac-jr5k0u
```

---

## Self-review notes (addressed)

- **Spec coverage:** D1 status (T1); D2/D3 precedence + invariant (T3, verified T5); D4 direct write via `SetExternal` (T4); D5 one-shot binary (T14); D6 activity mapping (T11); D7 ROPC + app-only auth (T8/T14); D8 email matching (T11/T12); D9 extend `pkg/msgraph` (T7–T8); D10 directory layout (T2/T10–T16); D11 id-map cache + TTL-gated refresh (T12/T13/T15). Keys: azure (T3), status index (T13), idmap + fresh marker (T13). Deploy (T16), docs (T17).
- **Type consistency:** `SetExternal(ctx, account, status, ttl)` identical in `presencestore.Store` (T4) and sync `externalApplier` (T10). `reconcileConfig` fields (`SiteID`, `EmailDomain`, `ExternalTTL`, `IDMapRefreshTTL`) consistent T12/T14. `idMapStore` methods (`Fresh`/`Refresh`/`Resolve`) consistent T10/T12/T13. `StatusInCall`/`StatusNone` used consistently. `PublishFunc`/`PublishState` live only in `presencestore`.
- **Placeholder scan:** no TBD/TODO; all code blocks complete. The one judgement call (cross-slot `TxPipeline` vs `Pipeline`) is flagged with an explicit fallback in T13/T15.
```
