# room-service Graph meetings proxy — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let room-service route its Microsoft Graph *meetings* calls through an explicit proxy configured via `GRAPH_PROXY_URL`, overriding the ambient `HTTPS_PROXY`/`HTTP_PROXY` env vars.

**Architecture:** Add an error-returning `msgraph.NewMeetingsClient` constructor that wraps the existing `New` + `applyProxyURL` (mirroring `NewUserListerClient`), so the meetings client honors `Config.ProxyURL` and fails fast on a malformed value. room-service gains a `GRAPH_PROXY_URL` config passed through as `ProxyURL`.

**Tech Stack:** Go 1.25, `net/http` transport proxying, `caarlos0/env` config, `stretchr/testify` assertions.

## Global Constraints

- Go 1.25; monorepo, single root `go.mod`. Services are flat `package main` dirs at repo root.
- Always use `make` targets — never raw `go` commands. Tests run with `-race` via the Makefile.
- TDD: Red → Green → Refactor → Commit. Write the failing test first and confirm it fails before implementing.
- Error wrapping: `fmt.Errorf("short description: %w", err)`; never bare `err`. (The msgraph `applyProxyURL` helper already does this — no new error strings needed beyond reuse.)
- Config: env vars via `caarlos0/env` into a typed struct; `SCREAMING_SNAKE_CASE`; provide `envDefault` for non-secret optional config; never `os.Getenv` directly.
- Never log tokens/secrets; `proxyURL.Redacted()` is already used by `applyProxyURL` for the invalid-URL message.
- Minimum 80% coverage; cover error paths and edge cases. `pkg/` exported funcs need test cases.
- Commit message footer (both lines, verbatim):
  ```
  Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01YG6jRt3et6XhesnSRM21rr
  ```

---

### Task 1: `msgraph.NewMeetingsClient` constructor

**Files:**
- Modify: `pkg/msgraph/msgraph.go` (add `NewMeetingsClient`; update `Config.ProxyURL` docstring near lines 126–130)
- Test: `pkg/msgraph/msgraph_test.go` (add 4 new tests; extend `TestGraphClients_InvalidProxyURL` near lines 448–463)

**Interfaces:**
- Consumes: existing `New(cfg Config, opts ...Option) Client` (returns `*graphClient`), `applyProxyURL(hc *http.Client, rawProxyURL string) error`, `WithHTTPClient`, `stubRoundTripper` (defined in the test file).
- Produces: `func NewMeetingsClient(cfg Config, opts ...Option) (Client, error)` — honors `cfg.ProxyURL`; returns a non-nil error on a malformed proxy URL or a custom (non-`*http.Transport`) RoundTripper; no-op when `ProxyURL` is empty. Consumed by Task 2.

- [ ] **Step 1: Write the failing tests**

Add to `pkg/msgraph/msgraph_test.go` (imports `net/http`, `testify/assert`, `testify/require` are already present in the file):

```go
func TestNewMeetingsClient_RoutesThroughProxy(t *testing.T) {
	c, err := NewMeetingsClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: "http://proxy.corp:8080"},
	)
	require.NoError(t, err)
	g := c.(*graphClient)
	tr, ok := g.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, tr.Proxy, "proxy must be configured on the transport")

	req, err := http.NewRequest(http.MethodGet, "https://graph.microsoft.com/v1.0/me", nil)
	require.NoError(t, err)
	proxyURL, err := tr.Proxy(req)
	require.NoError(t, err)
	require.NotNil(t, proxyURL)
	assert.Equal(t, "http://proxy.corp:8080", proxyURL.String())
}

func TestNewMeetingsClient_EmptyProxyIsNoOp(t *testing.T) {
	c, err := NewMeetingsClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s"})
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestNewMeetingsClient_TLSInsecureAndProxyCompose(t *testing.T) {
	c, err := NewMeetingsClient(Config{
		TenantID:              "t",
		ClientID:              "c",
		ClientSecret:          "s",
		TLSInsecureSkipVerify: true,
		ProxyURL:              "http://proxy.corp:8080",
	})
	require.NoError(t, err)
	g := c.(*graphClient)
	tr, ok := g.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, tr.TLSClientConfig)
	assert.True(t, tr.TLSClientConfig.InsecureSkipVerify, "TLS-insecure must survive proxy application")
	require.NotNil(t, tr.Proxy, "proxy must be configured alongside TLS-insecure")
}

func TestNewMeetingsClient_ProxyRejectsCustomRoundTripper(t *testing.T) {
	_, err := NewMeetingsClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s", ProxyURL: "http://proxy.corp:8080"},
		WithHTTPClient(&http.Client{Transport: stubRoundTripper{}}),
	)
	require.Error(t, err)
}
```

Then extend the existing `TestGraphClients_InvalidProxyURL` (lines 451–463) to also assert `NewMeetingsClient` fails. Add this inside the `t.Run` body, alongside the existing `NewChatsClient`/`NewChatMembersClient`/`NewUserListerClient` assertions:

```go
			_, err = NewMeetingsClient(cfg)
			require.Error(t, err)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=msgraph` (or `cd` scope via the Makefile's SERVICE var)
Expected: FAIL — `undefined: NewMeetingsClient`.

- [ ] **Step 3: Add the constructor**

In `pkg/msgraph/msgraph.go`, add `NewMeetingsClient` immediately after `New` (after line 260). Copy the `NewUserListerClient` shape:

```go
// NewMeetingsClient returns an app-only meetings client that honors
// cfg.ProxyURL (unlike New, which ignores it and relies on the standard proxy
// env vars). It mirrors NewUserListerClient: it applies the proxy after
// construction and reports an invalid value at construction rather than
// surfacing an opaque per-request error.
//
//nolint:gocritic // hugeParam: startup-only constructor; Config passed by value is intentional.
func NewMeetingsClient(cfg Config, opts ...Option) (Client, error) {
	g := New(cfg, opts...).(*graphClient)
	if err := applyProxyURL(g.httpClient, cfg.ProxyURL); err != nil {
		return nil, err
	}
	return g, nil
}
```

- [ ] **Step 4: Update the `Config.ProxyURL` docstring**

In `pkg/msgraph/msgraph.go`, the `ProxyURL` field comment (lines 126–130) currently reads:

```go
	// ProxyURL, when non-empty, routes the client's HTTP requests through this
	// proxy (overriding HTTPS_PROXY/HTTP_PROXY). Honored by the presence, chats,
	// chat-members and user-lister clients — each NewXxxClient applies it and
	// reports an invalid value at construction. The directory and meetings clients
	// (NewDirectoryClient / New) ignore it and rely on the standard proxy env
	// vars. Must include a scheme and host (e.g. "http://proxy.corp:8080").
	ProxyURL string
```

Replace it with (the meetings client now honors it via `NewMeetingsClient`; the bare `New` still ignores it):

```go
	// ProxyURL, when non-empty, routes the client's HTTP requests through this
	// proxy (overriding HTTPS_PROXY/HTTP_PROXY). Honored by the presence, chats,
	// chat-members, user-lister and meetings clients — each NewXxxClient
	// (NewPresenceClient / NewChatsClient / NewChatMembersClient /
	// NewUserListerClient / NewMeetingsClient) applies it and reports an invalid
	// value at construction. The bare New and NewDirectoryClient constructors
	// ignore it and rely on the standard proxy env vars. Must include a scheme
	// and host (e.g. "http://proxy.corp:8080").
	ProxyURL string
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test SERVICE=msgraph`
Expected: PASS — all four new tests and the extended `TestGraphClients_InvalidProxyURL` green.

- [ ] **Step 6: Lint**

Run: `make lint`
Expected: no new findings.

- [ ] **Step 7: Commit**

```bash
git add pkg/msgraph/msgraph.go pkg/msgraph/msgraph_test.go
git commit -m "feat(msgraph): add NewMeetingsClient honoring Config.ProxyURL

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01YG6jRt3et6XhesnSRM21rr"
```

---

### Task 2: room-service config + wiring + docs

**Files:**
- Modify: `room-service/main.go` (add `GraphProxyURL` config field after line 58; switch construction block lines 156–162 to `NewMeetingsClient` with error handling)
- Modify: `docs/msgraph-client.md` (add `GRAPH_PROXY_URL` to the config table; note the meetings client honors `ProxyURL`)

**Interfaces:**
- Consumes: `msgraph.NewMeetingsClient(cfg Config, opts ...Option) (Client, error)` from Task 1.
- Produces: nothing consumed by later tasks (terminal wiring).

- [ ] **Step 1: Add the config field**

In `room-service/main.go`, immediately after the `GraphUserAgent` field (line 58), add:

```go
	// GraphProxyURL, when set, routes the meetings Graph client through this
	// proxy explicitly (overriding HTTPS_PROXY/HTTP_PROXY). Must include a scheme
	// and host, e.g. "http://proxy.corp:8080". Empty falls back to the standard
	// proxy env vars.
	GraphProxyURL string `env:"GRAPH_PROXY_URL" envDefault:""`
```

- [ ] **Step 2: Switch construction to `NewMeetingsClient`**

In `room-service/main.go`, replace the current construction (lines 156–162):

```go
		graphClient = msgraph.New(msgraph.Config{
			TenantID:              cfg.TeamsTenantID,
			ClientID:              cfg.TeamsClientID,
			ClientSecret:          cfg.TeamsClientSecret,
			TLSInsecureSkipVerify: cfg.TeamsTLSInsecure,
			UserAgent:             cfg.GraphUserAgent,
		})
```

with (add `ProxyURL`, capture and check the error using the `err` already in scope from `main`):

```go
		graphClient, err = msgraph.NewMeetingsClient(msgraph.Config{
			TenantID:              cfg.TeamsTenantID,
			ClientID:              cfg.TeamsClientID,
			ClientSecret:          cfg.TeamsClientSecret,
			TLSInsecureSkipVerify: cfg.TeamsTLSInsecure,
			ProxyURL:              cfg.GraphProxyURL,
			UserAgent:             cfg.GraphUserAgent,
		})
		if err != nil {
			slog.Error("build graph meetings client", "error", err)
			os.Exit(1)
		}
```

Note: `graphClient` is declared as `var graphClient msgraph.Client` (line 151) and `err` is already in scope from `main`'s earlier `cfg, err := ...`; use `=` (not `:=`) so neither is shadowed.

- [ ] **Step 3: Verify the build compiles**

Run: `make build SERVICE=room-service`
Expected: builds cleanly (this is the green check for the wiring — a `:=` shadow, wrong type, or unhandled error would fail compilation).

- [ ] **Step 4: Run room-service unit tests**

Run: `make test SERVICE=room-service`
Expected: PASS — existing handler/helper tests unaffected. (No new config-parse unit test is added: room-service's `config` carries `required` env vars, `main_test.go` is integration-tagged, and the `GRAPH_PROXY_URL` env tag / pass-through is fully covered by Task 1's constructor tests plus this compile check — no forced integration test per the spec.)

- [ ] **Step 5: Update `docs/msgraph-client.md`**

In the config table (lines 21–26), add a row after the `TEAMS_EMAIL_DOMAIN` row:

```md
| `GRAPH_PROXY_URL` | Optional. Routes the meetings Graph client through this proxy (scheme+host, e.g. `http://proxy.corp:8080`), overriding `HTTPS_PROXY`/`HTTP_PROXY`. Empty falls back to the standard proxy env vars. |
```

In the "Creating a meeting (idempotent)" section, append a sentence at the end (after line 52):

```md

room-service constructs this client via `NewMeetingsClient(cfg)`, which honors
`Config.ProxyURL` (from `GRAPH_PROXY_URL`) and fails fast on a malformed proxy
value at startup.
```

- [ ] **Step 6: Confirm no client-api docs change is needed**

No `docs/client-api.md` edit: no client-facing NATS/HTTP handler request or response schema changed (only startup config + client construction). Nothing to do — this step is a checkpoint, not an edit.

- [ ] **Step 7: Lint**

Run: `make lint`
Expected: no new findings.

- [ ] **Step 8: Commit**

```bash
git add room-service/main.go docs/msgraph-client.md
git commit -m "feat(room-service): route Graph meetings client through GRAPH_PROXY_URL

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01YG6jRt3et6XhesnSRM21rr"
```

---

## Final verification (after both tasks)

- [ ] `make test SERVICE=msgraph` — green
- [ ] `make test SERVICE=room-service` — green
- [ ] `make build SERVICE=room-service` — builds
- [ ] `make lint` — clean
- [ ] `make sast` — no medium+ findings (SAST is a blocking CI gate)
- [ ] Push: `git push -u origin claude/room-service-proxy-msgraph-8h739h` (retry on network error with backoff 2s/4s/8s/16s)
