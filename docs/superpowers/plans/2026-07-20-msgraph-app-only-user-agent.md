# App-only Graph client User-Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every request the shared app-only `msgraph` client sends carry a configurable `User-Agent` header (browser-string default), so room-service Teams meeting calls can pass a fronting proxy/WAF like the presence client already does.

**Architecture:** Add a resolved `userAgent` field to `graphClient` (from `Config.UserAgent`, falling back to the shared `defaultUserAgent`), and set the header explicitly at all four request sites — mirroring the existing `presence.go` pattern (Approach A). Wire a `GRAPH_USER_AGENT` env var through room-service into `msgraph.Config.UserAgent`.

**Tech Stack:** Go 1.25, `net/http`, `net/http/httptest` (tests), `caarlos0/env` (config), testify.

Spec: `docs/superpowers/specs/2026-07-20-msgraph-app-only-user-agent-design.md`

## Global Constraints

- Go 1.25; single root `go.mod`. No new third-party dependencies.
- Always use `make` targets, never raw `go`. Unit tests: `make test SERVICE=<name>` runs with `-race`.
- TDD Red-Green-Refactor is mandatory: failing test first, confirm it fails, then minimal implementation.
- Minimum 80% package coverage; target 90%+ for `pkg/` code.
- Never log tokens/secrets. Config from env via `caarlos0/env` only — no `os.Getenv`. Use `envDefault` for non-critical config.
- Follow existing patterns; keep changes minimal and focused.
- No `docs/client-api.md` change — this is an outbound HTTP header to Microsoft Graph, not a `chat.user.` RPC schema change.
- Commit author/committer identity must be `Claude <noreply@anthropic.com>` (`git config user.email noreply@anthropic.com && git config user.name Claude`).

---

## File Structure

- `pkg/msgraph/msgraph.go` — app-only `graphClient`: new `userAgent` field, resolution in `New`, header set at four sites, `defaultUserAgent` constant moved here, `Config.UserAgent` doc updated.
- `pkg/msgraph/presence.go` — remove the `defaultUserAgent` constant declaration (now defined in `msgraph.go`, same package); no other change.
- `pkg/msgraph/msgraph_test.go` — new User-Agent assertions (default, override, directory, list-users).
- `room-service/main.go` — new `GraphUserAgent` config field + wiring into `msgraph.New`.

---

## Task 1: App-only Graph client sends a User-Agent header

**Files:**
- Modify: `pkg/msgraph/msgraph.go` (struct field, `New`, four request sites, moved constant, `Config.UserAgent` doc)
- Modify: `pkg/msgraph/presence.go` (remove moved `defaultUserAgent` constant)
- Test: `pkg/msgraph/msgraph_test.go`

**Interfaces:**
- Consumes: existing `Config.UserAgent string` field; existing package-level `defaultUserAgent` string constant (relocated, value unchanged); existing `New(cfg Config, opts ...Option) Client`, `NewDirectoryClient`, `NewUserListerClient`; test helpers `newTestClient(tokenURL, baseURL)`, `newTestDirectory(tokenURL, baseURL)`.
- Produces: after `New`, the returned `*graphClient` has a non-empty `userAgent`; all outbound requests (token, `CreateOnlineMeeting`, `ResolveAccountIDs`, `ListUsers`) send `User-Agent: <resolved>`.

- [ ] **Step 1: Write the failing tests**

Append these four tests to `pkg/msgraph/msgraph_test.go`:

```go
func TestCreateOnlineMeeting_SendsDefaultUserAgent(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, defaultUserAgent, r.Header.Get("User-Agent"), "token request must carry User-Agent")
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, defaultUserAgent, r.Header.Get("User-Agent"), "meeting request must carry User-Agent")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(OnlineMeeting{ID: "m1", JoinURL: "https://join/1"})
	}))
	defer graphSrv.Close()

	c := newTestClient(tokenSrv.URL, graphSrv.URL)
	_, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{
		ExternalID:     "room-key-1",
		Subject:        "Standup",
		OrganizerEmail: "alice@corp.com",
	})
	require.NoError(t, err)
}

func TestCreateOnlineMeeting_UserAgentOverride(t *testing.T) {
	const custom = "chat-room-service/9.9"
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, custom, r.Header.Get("User-Agent"))
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(OnlineMeeting{ID: "m1", JoinURL: "https://join/1"})
	}))
	defer graphSrv.Close()
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, custom, r.Header.Get("User-Agent"))
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	c := New(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s", UserAgent: custom},
		WithTokenURL(tokenSrv.URL), WithBaseURL(graphSrv.URL),
	)
	_, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{
		ExternalID: "room-key-1", OrganizerEmail: "alice@corp.com",
	})
	require.NoError(t, err)
}

func TestResolveAccountIDs_SendsUserAgent(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, defaultUserAgent, r.Header.Get("User-Agent"))
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, defaultUserAgent, r.Header.Get("User-Agent"), "directory request must carry User-Agent")
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []GraphUser{
			{ID: "ida", UserPrincipalName: "alice@corp.com"},
		}})
	}))
	defer graphSrv.Close()

	c := newTestDirectory(tokenSrv.URL, graphSrv.URL)
	_, err := c.ResolveAccountIDs(context.Background(), []string{"alice"})
	require.NoError(t, err)
}

func TestListUsers_SendsUserAgent(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	defer tokenSrv.Close()
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, defaultUserAgent, r.Header.Get("User-Agent"), "list-users request must carry User-Agent")
		_, _ = w.Write([]byte(`{"value":[{"id":"u1","userPrincipalName":"alice@corp.example"}]}`))
	}))
	defer graphSrv.Close()

	lister := NewUserListerClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithBaseURL(graphSrv.URL), WithTokenURL(tokenSrv.URL),
	)
	err := lister.ListUsers(context.Background(), 500, func([]GraphUser) error { return nil })
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=msgraph`
Expected: FAIL — the four new tests fail their `User-Agent` assertions (empty string ≠ `defaultUserAgent`/custom), because the app-only client sends no explicit `User-Agent`. Existing tests still pass.

- [ ] **Step 3: Move `defaultUserAgent` into `msgraph.go`**

In `pkg/msgraph/presence.go`, delete the constant declaration (lines ~38–44):

```go
// defaultUserAgent is sent on presence requests when Config.UserAgent is empty.
// Microsoft Graph rejects requests without a User-Agent header, and a fronting
// corporate proxy/WAF commonly rejects non-browser agents; a desktop-browser
// string is the value most likely to pass both. Override per-environment via
// Config.UserAgent (GRAPH_USER_AGENT) since a pinned browser version ages.
const defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
	"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
```

In `pkg/msgraph/msgraph.go`, add it to the existing `const (...)` block (the one holding `defaultGraphBaseURL`, `graphScope`, `tokenExpirySkew` at lines ~122–128) with a generalized comment:

```go
	// defaultUserAgent is sent on every app-only and presence Graph request when
	// Config.UserAgent is empty. Microsoft Graph rejects requests without a
	// User-Agent header, and a fronting corporate proxy/WAF commonly rejects
	// non-browser agents; a desktop-browser string is the value most likely to
	// pass both. Override per-environment via Config.UserAgent (GRAPH_USER_AGENT)
	// since a pinned browser version ages.
	defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
		"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
```

- [ ] **Step 4: Add the `userAgent` field and resolve it in `New`**

In `pkg/msgraph/msgraph.go`, add a field to the `graphClient` struct (after `chatsPageSize int`, before the `mu sync.Mutex` block):

```go
	// userAgent is the resolved User-Agent header sent on every request
	// (Config.UserAgent, or defaultUserAgent when empty).
	userAgent string
```

In `New`, after the `g := &graphClient{...}` literal and before the `for _, opt := range opts` loop, resolve it (`UserAgent` is not an `Option`, so there is no ordering constraint against `opts`):

```go
	ua := cfg.UserAgent
	if ua == "" {
		ua = defaultUserAgent
	}
	g.userAgent = ua
```

- [ ] **Step 5: Set the header at all four request sites**

In `pkg/msgraph/msgraph.go`, add `req.Header.Set("User-Agent", g.userAgent)` (use the local request variable's actual name) alongside the existing header sets:

1. `accessToken` — after `req.Header.Set("Content-Type", "application/x-www-form-urlencoded")`:
```go
	req.Header.Set("User-Agent", g.userAgent)
```
2. `CreateOnlineMeeting` — the request var is `httpReq`; after `httpReq.Header.Set("Content-Type", "application/json")`:
```go
	httpReq.Header.Set("User-Agent", g.userAgent)
```
3. `resolveChunk` — after `req.Header.Set("ConsistencyLevel", "eventual")`:
```go
	req.Header.Set("User-Agent", g.userAgent)
```
4. `fetchUsersPage` — after `req.Header.Set("Authorization", "Bearer "+token)`:
```go
	req.Header.Set("User-Agent", g.userAgent)
```

- [ ] **Step 6: Update the `Config.UserAgent` doc comment**

In `pkg/msgraph/msgraph.go`, replace the `UserAgent` field's doc comment (currently "Honored only by the presence client...") so it reflects both clients. `ProxyURL`'s doc is unchanged (still presence-only):

```go
	// UserAgent overrides the User-Agent header sent on every Graph request. When
	// empty the client falls back to defaultUserAgent (a browser string). Honored
	// by both the app-only client (New) and the presence client
	// (NewPresenceClient). Set this to whatever a fronting proxy/WAF expects when
	// the default is rejected.
	UserAgent string
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `make test SERVICE=msgraph`
Expected: PASS — the four new tests pass and all pre-existing `msgraph` tests (including `presence_test.go`, which still references `defaultUserAgent`) still pass.

- [ ] **Step 8: Lint**

Run: `make lint`
Expected: no new findings in `pkg/msgraph`.

- [ ] **Step 9: Commit**

```bash
git add pkg/msgraph/msgraph.go pkg/msgraph/presence.go pkg/msgraph/msgraph_test.go
git commit -m "feat(msgraph): send User-Agent on app-only Graph requests"
```

---

## Task 2: Wire `GRAPH_USER_AGENT` through room-service

**Files:**
- Modify: `room-service/main.go` (config field + `msgraph.New` wiring)

**Interfaces:**
- Consumes: `msgraph.Config.UserAgent` (from Task 1); existing `msgraph.New(msgraph.Config{...})` call block in `main.go` (lines ~152–157); existing `Config` struct with the `Teams*` fields (lines ~48–54).
- Produces: `GRAPH_USER_AGENT` env var populates `cfg.GraphUserAgent`, passed to the Graph client so room-service's meeting requests use it (empty → browser default).

- [ ] **Step 1: Add the config field**

In `room-service/main.go`, add to the `Config` struct immediately after `TeamsTLSInsecure` (line ~54):

```go
	// GraphUserAgent overrides the User-Agent header on Graph requests (meetings
	// path). Empty falls back to the msgraph browser default. Named GRAPH_USER_AGENT
	// for consistency with user-presence-service.
	GraphUserAgent string `env:"GRAPH_USER_AGENT" envDefault:""`
```

- [ ] **Step 2: Wire it into the Graph client**

In `room-service/main.go`, add the field to the existing `msgraph.New(msgraph.Config{...})` literal (after `TLSInsecureSkipVerify: cfg.TeamsTLSInsecure,`, line ~156):

```go
			UserAgent: cfg.GraphUserAgent,
```

- [ ] **Step 3: Verify the service builds**

Run: `make build SERVICE=room-service`
Expected: builds cleanly (no compile errors).

- [ ] **Step 4: Run room-service unit tests**

Run: `make test SERVICE=room-service`
Expected: PASS — existing tests unaffected (config field is a struct-tag/env pass-through; no new unit test needed, consistent with other `Config` fields).

- [ ] **Step 5: Lint**

Run: `make lint`
Expected: no new findings.

- [ ] **Step 6: Commit**

```bash
git add room-service/main.go
git commit -m "feat(room-service): wire GRAPH_USER_AGENT into Graph client"
```

---

## Final verification

- [ ] Run `make test SERVICE=msgraph` and `make test SERVICE=room-service` — both green.
- [ ] Run `make lint` — clean.
- [ ] Confirm `git grep -n "req.Header.Set(\"User-Agent\""` (and `httpReq.Header.Set`) shows the header at all four `msgraph.go` sites plus the two `presence.go` sites.
- [ ] Push: `git push -u origin claude/session-oajhpp`.

## Spec coverage check

- Whole-app-only scope → Task 1 Step 5 (four sites) + tests covering token, meeting, directory, list-users.
- Browser-string default reused + moved constant → Task 1 Steps 3–4.
- `GRAPH_USER_AGENT` env var → Task 2 Steps 1–2.
- Approach A (explicit per-site set) → Task 1 Step 5.
- Doc updates (`Config.UserAgent`, moved constant comment) → Task 1 Steps 3, 6.
- Non-goals honored: no `ProxyURL` for app-only client; no `docs/client-api.md` change.
