# Room-service Meeting Directory App-Only (drop ROPC) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve Teams meeting organizer/attendee Azure object IDs with the app-only `User.Read.All` Service Principal room-service already builds for meetings, removing the ROPC directory client, its `TEAMS_ROPC_*` config, and the service-account password.

**Architecture:** Add one `pkg/msgraph` constructor (`NewMeetingsDirectoryClient`) that returns both the meetings (`Client`) and directory (`DirectoryReader`) surfaces from a single proxy-honoring app-only `*graphClient` (one token cache). Rewire `room-service/main.go` to use it; then delete the now-unused `directory_ropc.go` and its tests. Handler behavior (object-ID organizer path, best-effort attendees) is unchanged — only ROPC wiring and comments/docs change.

**Tech Stack:** Go 1.25, Microsoft Graph (client-credentials OAuth2), `httptest` for unit tests, testify assertions.

## Global Constraints

- Use `make` targets, never raw `go` — `make test SERVICE=<name>`, `make lint`, `make sast`.
- TDD: write the failing test first, confirm it fails, then implement.
- Error wrapping: `fmt.Errorf("short description: %w", err)`; never log tokens/passwords.
- Minimum 80% package coverage; target 90%+ for `pkg/` and handlers.
- `//nolint:gocritic // hugeParam: startup-only constructor; Config passed by value is intentional.` on every `NewXxxClient` constructor that takes `Config` by value (existing convention in `pkg/msgraph`).
- Any change to a client-facing `chat.user.` RPC's schema/error cases updates `docs/client-api.md` in the same PR. (Here the request/response structs are unchanged; only descriptive text changes.)
- Reference the design spec: `docs/superpowers/specs/2026-07-28-room-service-meeting-directory-app-only-design.md`.

---

### Task 1: Add `NewMeetingsDirectoryClient` constructor in `pkg/msgraph`

Adds a single constructor returning both the meetings and directory surfaces backed by one proxy-honoring app-only `*graphClient`. `*graphClient` already implements both `Client` (`CreateOnlineMeeting`) and `DirectoryReader` (`ResolveAccountIDs`) — this constructor just exposes both views of one instance and applies `cfg.ProxyURL` (which the bare `NewDirectoryClient` does not).

**Files:**
- Modify: `pkg/msgraph/msgraph.go` (add constructor after `NewMeetingsClient`, ~line 355)
- Test: `pkg/msgraph/msgraph_test.go` (add tests near the existing meetings/proxy tests)

**Interfaces:**
- Consumes: `New(cfg, opts...) Client` (returns `*graphClient`), `applyProxyURL(hc, raw)`, existing `Client` + `DirectoryReader` interfaces.
- Produces: `func NewMeetingsDirectoryClient(cfg Config, opts ...Option) (Client, DirectoryReader, error)` — both return values are the same `*graphClient` instance; returns a non-nil error only on invalid `cfg.ProxyURL`.

- [ ] **Step 1: Write the failing tests**

Add to `pkg/msgraph/msgraph_test.go`:

```go
func TestNewMeetingsDirectoryClient_BothSurfacesUseAppOnlyToken(t *testing.T) {
	var grant string
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		grant = r.Form.Get("grant_type")
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "apptok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer apptok", r.Header.Get("Authorization"))
		if r.Method == http.MethodGet { // ResolveAccountIDs
			assert.Equal(t, "eventual", r.Header.Get("ConsistencyLevel"))
			assert.Contains(t, r.URL.Query().Get("$filter"), "startsWith(userPrincipalName,'alice@')")
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []GraphUser{
				{ID: "ida", UserPrincipalName: "alice@corp.com"},
			}})
			return
		}
		// CreateOnlineMeeting
		assert.Contains(t, r.URL.Path, "/users/ida/onlineMeetings/createOrGet")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(OnlineMeeting{ID: "m1", JoinURL: "https://join/1"})
	}))
	defer graphSrv.Close()

	client, dir, err := NewMeetingsDirectoryClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithTokenURL(tokenSrv.URL), WithBaseURL(graphSrv.URL),
	)
	require.NoError(t, err)

	got, err := dir.ResolveAccountIDs(context.Background(), []string{"alice"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"alice": "ida"}, got)

	mtg, err := client.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{ExternalID: "k", OrganizerID: "ida"})
	require.NoError(t, err)
	assert.Equal(t, "https://join/1", mtg.JoinURL)

	assert.Equal(t, "client_credentials", grant)
}

func TestNewMeetingsDirectoryClient_SameInstance(t *testing.T) {
	client, dir, err := NewMeetingsDirectoryClient(Config{TenantID: "t", ClientID: "c", ClientSecret: "s"})
	require.NoError(t, err)
	assert.Same(t, client, dir, "both surfaces must be the same *graphClient (one token cache)")
}

func TestNewMeetingsDirectoryClient_InvalidProxyURL(t *testing.T) {
	_, _, err := NewMeetingsDirectoryClient(Config{TenantID: "t", ProxyURL: "://nope"})
	require.Error(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=pkg/msgraph` (or `go test ./pkg/msgraph/ -run TestNewMeetingsDirectoryClient` via the Makefile)
Expected: FAIL — `undefined: NewMeetingsDirectoryClient`.

- [ ] **Step 3: Add the constructor**

In `pkg/msgraph/msgraph.go`, immediately after `NewMeetingsClient` (ends ~line 355), add:

```go
// NewMeetingsDirectoryClient returns the meetings (Client) and directory
// (DirectoryReader) surfaces backed by a single app-only graphClient — one
// token cache serves both. Honors cfg.ProxyURL like NewMeetingsClient (the bare
// NewDirectoryClient does not). Both return values are the same instance.
// room-service uses this so the meeting organizer/attendee object-ID lookup runs
// on the same app-only User.Read.All Service Principal that creates the meeting,
// with no resource-owner (ROPC) credentials.
//
//nolint:gocritic // hugeParam: startup-only constructor; Config passed by value is intentional.
func NewMeetingsDirectoryClient(cfg Config, opts ...Option) (Client, DirectoryReader, error) {
	g := New(cfg, opts...).(*graphClient)
	if err := applyProxyURL(g.httpClient, cfg.ProxyURL); err != nil {
		return nil, nil, err
	}
	return g, g, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/msgraph`
Expected: PASS (all three new tests green; existing tests still green).

- [ ] **Step 5: Commit**

```bash
git add pkg/msgraph/msgraph.go pkg/msgraph/msgraph_test.go
git commit -m "feat(msgraph): add NewMeetingsDirectoryClient app-only constructor

Returns the meetings (Client) and directory (DirectoryReader) surfaces from a
single proxy-honoring app-only graphClient, so room-service can resolve meeting
object IDs on the same Service Principal that creates the meeting."
```

---

### Task 2: Rewire `room-service/main.go` to app-only, drop `TEAMS_ROPC_*` config

Replaces the ROPC directory-client wiring with the single `NewMeetingsDirectoryClient` call and removes the two config fields. After this task `directoryClient` is non-nil whenever `graphClient` is, so the meetings RPC is configured whenever the Azure app creds are present.

**Files:**
- Modify: `room-service/main.go:55-61` (remove `TeamsROPCUsername`/`TeamsROPCPassword` fields + comment), `room-service/main.go:191-225` (rewire client construction)

**Interfaces:**
- Consumes: `msgraph.NewMeetingsDirectoryClient(cfg) (Client, DirectoryReader, error)` from Task 1.
- Produces: unchanged handler wiring — `handler.graphClient` and `handler.directoryClient` are set from one call.

- [ ] **Step 1: Remove the ROPC config fields**

In `room-service/main.go`, delete lines 55-61 (the `TeamsROPCUsername/Password` doc comment and both fields):

```go
	// TeamsROPCUsername/Password are the service-account resource-owner
	// credentials for the ROPC (grant_type=password) directory lookup used to
	// resolve meeting organizer/attendee Azure object IDs (User.Read.All). They
	// reuse TeamsClientID/TeamsClientSecret as the confidential client. When
	// unset the meetings RPC reports not-configured.
	TeamsROPCUsername string `env:"TEAMS_ROPC_USERNAME"      envDefault:""`
	TeamsROPCPassword string `env:"TEAMS_ROPC_PASSWORD"      envDefault:""`
```

So that the block goes straight from `TeamsEmailDomain` (line 54) to the `TeamsTLSInsecure` comment (line 62).

- [ ] **Step 2: Rewire the client construction block**

Replace `room-service/main.go` lines 191-225 (the `var graphClient` / `var directoryClient` block through its closing brace) with:

```go
	// Graph clients back the meetings RPC. Constructed only when the Azure app
	// credentials are present; otherwise the meetings RPC reports not-configured
	// while the deep-link RPCs keep working. One app-only client serves both the
	// meetings (Client) and directory (DirectoryReader, User.Read.All) surfaces —
	// the directory lookup resolves organizer/attendee object IDs on the same
	// Service Principal that creates the meeting.
	var graphClient msgraph.Client
	var directoryClient msgraph.DirectoryReader
	if cfg.TeamsTenantID != "" && cfg.TeamsClientID != "" && cfg.TeamsClientSecret != "" {
		graphCfg := msgraph.Config{
			TenantID:              cfg.TeamsTenantID,
			ClientID:              cfg.TeamsClientID,
			ClientSecret:          cfg.TeamsClientSecret,
			TLSInsecureSkipVerify: cfg.TeamsTLSInsecure,
			ProxyURL:              cfg.GraphProxyURL,
			UserAgent:             cfg.GraphUserAgent,
		}
		if cfg.TeamsTLSInsecure {
			slog.Warn("Graph TLS verification disabled — dev/on-prem only, never production", "TEAMS_TLS_INSECURE", true)
		}
		graphClient, directoryClient, err = msgraph.NewMeetingsDirectoryClient(graphCfg)
		if err != nil {
			slog.Error("build graph meetings client", "error", err)
			os.Exit(1)
		}
	}
```

- [ ] **Step 3: Verify compilation and existing tests pass**

Run: `make test SERVICE=room-service`
Expected: PASS — main compiles, handler tests (which inject a `fakeDirectory`) are unaffected. No test references `TeamsROPCUsername`/`TeamsROPCPassword`, so nothing else breaks.

- [ ] **Step 4: Commit**

```bash
git add room-service/main.go
git commit -m "refactor(room-service): resolve meeting object IDs app-only

Build the meetings + directory clients from one app-only Service Principal via
NewMeetingsDirectoryClient and drop TEAMS_ROPC_USERNAME/PASSWORD. directoryClient
is now non-nil whenever the Azure app creds are set."
```

---

### Task 3: Delete `directory_ropc.go` and its tests

Removes the now-unused ROPC directory client. The `ROPCCredentials` type stays — it lives in `presence.go` and `NewPresenceClient` still uses it for the delegated-only presence call.

**Files:**
- Delete: `pkg/msgraph/directory_ropc.go`
- Modify: `pkg/msgraph/msgraph_test.go:704-761` (delete `TestNewDirectoryROPCClient_ResolvesWithPasswordGrant` and `TestNewDirectoryROPCClient_TokenErrorDoesNotLeakPassword`)

**Interfaces:**
- Consumes: nothing.
- Produces: nothing — pure removal. `NewDirectoryROPCClient` and the unexported `directoryClient` type disappear.

- [ ] **Step 1: Confirm no remaining references outside the file/tests being deleted**

Run: `grep -rn "NewDirectoryROPCClient" --include='*.go' .`
Expected: matches only in `pkg/msgraph/directory_ropc.go` and `pkg/msgraph/msgraph_test.go` (both handled this task). If any other file matches, stop — Task 2 missed a caller.

- [ ] **Step 2: Delete the ROPC client file**

```bash
git rm pkg/msgraph/directory_ropc.go
```

- [ ] **Step 3: Delete the two ROPC tests**

In `pkg/msgraph/msgraph_test.go`, remove `func TestNewDirectoryROPCClient_ResolvesWithPasswordGrant(t *testing.T) { ... }` (starts line 704) and `func TestNewDirectoryROPCClient_TokenErrorDoesNotLeakPassword(t *testing.T) { ... }` (starts line 742) in full — through the closing brace directly before `func TestCreateOnlineMeeting_UsesObjectIDs`.

- [ ] **Step 4: Run tests to verify the package still passes**

Run: `make test SERVICE=pkg/msgraph`
Expected: PASS — no undefined symbols; `ROPCCredentials`/`tokenResponse` are still defined (used by `presence.go`). `make lint` reports no unused imports in the test file (`context`, `json`, etc. are still used by other tests).

- [ ] **Step 5: Commit**

```bash
git add pkg/msgraph/msgraph_test.go
git commit -m "refactor(msgraph): remove unused ROPC directory client

room-service now resolves meeting object IDs app-only via
NewMeetingsDirectoryClient; the grant_type=password directory reader has no
remaining callers. ROPCCredentials stays — presence still uses it."
```

---

### Task 4: Update comments in room-service handlers

Corrects the "ROPC directory" wording to "app-only `User.Read.All`" in the handler struct field doc and the resolution comment. No logic change.

**Files:**
- Modify: `room-service/handler.go:65-68` (directoryClient field comment)
- Modify: `room-service/handler_teams.go:136-139` (resolution comment)

**Interfaces:**
- Consumes: nothing.
- Produces: nothing — comment-only.

- [ ] **Step 1: Update the handler struct field comment**

In `room-service/handler.go`, replace lines 65-67:

```go
	// directoryClient resolves account local-parts to Azure AD object IDs via a
	// ROPC User.Read.All service account. Required by the meetings RPC (nil ->
	// errTeamsNotConfigured); the deep-link call RPCs do not use it.
```

with:

```go
	// directoryClient resolves account local-parts to Azure AD object IDs via the
	// app-only User.Read.All Service Principal (same instance as graphClient).
	// Required by the meetings RPC (nil -> errTeamsNotConfigured); the deep-link
	// call RPCs do not use it.
```

- [ ] **Step 2: Update the resolution comment in handler_teams.go**

In `room-service/handler_teams.go`, replace lines 136-139:

```go
	// Resolve organizer + attendee Azure AD object IDs via the ROPC directory
	// (User.Read.All). account@domain is only a guess; Graph createOrGet needs
	// the real organizer identity in the path, so a failed organizer resolution
	// is fatal. Attendees are best-effort — an unresolved attendee is dropped.
```

with:

```go
	// Resolve organizer + attendee Azure AD object IDs via the app-only directory
	// reader (User.Read.All). account@domain is only a guess; Graph createOrGet
	// needs the real organizer identity in the path, so a failed organizer
	// resolution is fatal. Attendees are best-effort — an unresolved attendee is dropped.
```

- [ ] **Step 3: Verify compilation**

Run: `make test SERVICE=room-service`
Expected: PASS — comment-only change, all handler tests still green.

- [ ] **Step 4: Commit**

```bash
git add room-service/handler.go room-service/handler_teams.go
git commit -m "docs(room-service): correct ROPC comments to app-only User.Read.All"
```

---

### Task 5: Update docs and docker-compose

Rewrites the ROPC references in the msgraph doc, client API doc, and compose file. The client-facing request/response structs are unchanged, so `docs/client-api/request-reply.md` and `docs/client-api/events.md` need no edits (verified: no ROPC references there).

**Files:**
- Modify: `docs/msgraph-client.md:26-28` (env table rows), `docs/msgraph-client.md:66-78` ("Resolving object IDs" section)
- Modify: `docs/client-api.md:2511` (Start Teams Meeting description), `docs/client-api.md:2552` (error row)
- Modify: `room-service/deploy/docker-compose.yml:33-37` (remove ROPC env passthroughs)

**Interfaces:**
- Consumes: nothing.
- Produces: nothing — docs/config only.

- [ ] **Step 1: Update the msgraph-client.md env table**

In `docs/msgraph-client.md`, delete the two `TEAMS_ROPC_*` rows (lines 27-28):

```
| `TEAMS_ROPC_USERNAME` | Service-account UPN for the ROPC directory lookup (`User.Read.All`) that resolves meeting organizer/attendee Azure object IDs. Reuses `TEAMS_CLIENT_ID`/`TEAMS_CLIENT_SECRET` as the confidential client. Meetings RPC is not-configured until set. |
| `TEAMS_ROPC_PASSWORD` | Service-account password for the ROPC directory lookup. |
```

- [ ] **Step 2: Rewrite the "Resolving object IDs" section**

In `docs/msgraph-client.md`, replace the section at lines 66-78:

```markdown
## Resolving object IDs (ROPC directory reader)

Because the organizer path and attendee identities are object IDs — not the
guessed `account@TEAMS_EMAIL_DOMAIN` email — `room-service` first resolves them
through a **ROPC** (`grant_type=password`) directory reader that holds the
**`User.Read.All`** permission delegated to a service account. Construct via
`NewDirectoryROPCClient(cfg, ROPCCredentials{Username, Password})`; it reuses
`TEAMS_CLIENT_ID`/`TEAMS_CLIENT_SECRET` as the confidential client plus
`TEAMS_ROPC_USERNAME`/`TEAMS_ROPC_PASSWORD`, and satisfies the `DirectoryReader`
interface (`ResolveAccountIDs(ctx, accounts) → map[account]objectID`, matching
`startsWith(userPrincipalName,'account@')` so any domain resolves). The organizer
must resolve or the `teams.meeting` request fails; an attendee that does not
resolve is dropped from the invite.
```

with:

```markdown
## Resolving object IDs (app-only directory reader)

Because the organizer path and attendee identities are object IDs — not the
guessed `account@TEAMS_EMAIL_DOMAIN` email — `room-service` first resolves them
through the **app-only** directory reader that holds the **`User.Read.All`**
*application* permission — the same Service Principal (`TEAMS_CLIENT_ID`/
`TEAMS_CLIENT_SECRET`) that creates the meeting, so no resource-owner
credentials are involved. Construct both surfaces together via
`NewMeetingsDirectoryClient(cfg)`, which returns the meetings `Client` and the
`DirectoryReader` backed by one client (one token cache). `ResolveAccountIDs(ctx,
accounts) → map[account]objectID` matches `startsWith(userPrincipalName,'account@')`
so any domain resolves. The organizer must resolve or the `teams.meeting` request
fails; an attendee that does not resolve is dropped from the invite.
```

- [ ] **Step 3: Update client-api.md Start Teams Meeting description**

In `docs/client-api.md` line 2511, replace the sentence:

```
The organizer and attendees are resolved to their Azure AD object IDs via a ROPC `User.Read.All` service account (`TEAMS_ROPC_USERNAME`/`TEAMS_ROPC_PASSWORD`); the organizer object ID scopes Graph's `createOrGet` and attendees are added by object ID.
```

with:

```
The organizer and attendees are resolved to their Azure AD object IDs via the app-only `User.Read.All` Service Principal that creates the meeting; the organizer object ID scopes Graph's `createOrGet` and attendees are added by object ID.
```

- [ ] **Step 4: Update the client-api.md error row**

In `docs/client-api.md` line 2552, replace:

```
| — | `internal` | Teams meetings not configured (including missing ROPC directory credentials), the organizer could not be resolved to an Azure object ID, or the Graph create failed. |
```

with:

```
| — | `internal` | Teams meetings not configured (Azure app credentials unset), the organizer could not be resolved to an Azure object ID, or the Graph create failed. |
```

- [ ] **Step 5: Remove the ROPC env passthroughs from docker-compose**

In `room-service/deploy/docker-compose.yml`, delete lines 33-37 (the ROPC comment and both env vars):

```yaml
      # ROPC service account (reuses TEAMS_CLIENT_ID/SECRET as the confidential
      # client) for the User.Read.All directory lookup that resolves meeting
      # organizer/attendee Azure object IDs. Meetings RPC is not-configured until set.
      - TEAMS_ROPC_USERNAME=
      - TEAMS_ROPC_PASSWORD=
```

so the block goes from `- TEAMS_CLIENT_SECRET=` straight to `- ROOM_MEMBERS_LIMIT=500`.

- [ ] **Step 6: Verify no stray ROPC references remain**

Run: `grep -rn "ROPC\|TEAMS_ROPC" docs/ room-service/ pkg/msgraph/ --include='*.md' --include='*.go' --include='*.yml'`
Expected: matches ONLY in `pkg/msgraph/presence*.go` (the delegated presence path, correctly retained) and the design/plan files under `docs/superpowers/`. No matches in `docs/msgraph-client.md`, `docs/client-api.md`, `room-service/deploy/docker-compose.yml`, `room-service/main.go`, `room-service/handler*.go`.

- [ ] **Step 7: Commit**

```bash
git add docs/msgraph-client.md docs/client-api.md room-service/deploy/docker-compose.yml
git commit -m "docs: describe app-only object-ID resolution, drop TEAMS_ROPC_*"
```

---

### Task 6: Full verification

**Files:** none (verification only).

- [ ] **Step 1: Run the full unit suite for touched packages**

Run: `make test SERVICE=pkg/msgraph` then `make test SERVICE=room-service`
Expected: PASS both.

- [ ] **Step 2: Lint**

Run: `make lint`
Expected: no findings (watch for unused imports in `msgraph_test.go` after the ROPC-test deletion).

- [ ] **Step 3: SAST**

Run: `make sast`
Expected: no medium+ findings (this change removes a `grant_type=password` path; it does not add one).

- [ ] **Step 4: Final ROPC sweep**

Run: `grep -rn "NewDirectoryROPCClient\|TEAMS_ROPC\|TeamsROPC" --include='*.go' --include='*.yml' --include='*.md' . | grep -v docs/superpowers/`
Expected: no matches (all references removed outside the design/plan notes).

- [ ] **Step 5: Delete session-scoped review reports if present**

Before opening a PR: `rm -f docs/reviews/*` if any exist (working notes, not shippable). The design + plan under `docs/superpowers/` stay.
