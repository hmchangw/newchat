# Room-service Teams Meeting Azure AD Object-ID Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create Teams meetings using Azure AD object IDs (resolved via a ROPC `User.Read.All` service account) for the organizer and attendees, instead of the guessed `account@TEAMS_EMAIL_DOMAIN` email.

**Architecture:** Add a ROPC-backed `DirectoryReader` to `pkg/msgraph` (reusing the existing account→objectID resolve logic and the `presence.go` `grant_type=password` token pattern). `room-service`'s `teamsMeeting` resolves the organizer + all individual members to object IDs before the Graph `createOrGet` call; the organizer must resolve (hard fail), attendees are best-effort (drop + log). The Graph meeting payload carries object IDs — organizer in the `/users/{id}` path, attendees via `identity.user.id`.

**Tech Stack:** Go 1.25, `net/http` (hand-rolled Graph client, no SDK), `caarlos0/env` config, `go.uber.org/mock` + `testify` + `httptest` for tests, `log/slog` JSON logging.

## Global Constraints

- Go 1.25; single root `go.mod`; services are flat `package main` at repo root; shared code in `pkg/`.
- Use `make` targets, never raw `go`: `make test SERVICE=<name>`, `make test` (module packages), `make lint`, `make sast`.
- All config via `caarlos0/env` typed struct; `SCREAMING_SNAKE_CASE` env names; `envDefault:""` for non-critical, never `required` for these optional creds.
- Errors: wrap infra with `fmt.Errorf("desc: %w", err)`; client-facing via `pkg/errcode` named constructors; never log AND return; never log tokens/passwords/message bodies.
- Logging: `log/slog` JSON only; structured key-value fields; never interpolate.
- TDD Red-Green-Refactor: write the failing test first, confirm it fails, minimal implementation, confirm pass, commit. Minimum 80% coverage (target 90% for handlers + `pkg/`).
- Every commit message ends with:
  ```
  Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01WT8WmZFBj8xzv4VXoMjh81
  ```
- Never put the model identifier in commit messages/code/docs.
- Design decisions (verbatim): reuse `TEAMS_CLIENT_ID`/`TEAMS_CLIENT_SECRET` as the ROPC confidential client; resolve organizer + attendees; organizer failure = hard error, attendee failure = drop + log; env vars `TEAMS_ROPC_USERNAME` / `TEAMS_ROPC_PASSWORD`; rename `OrganizerEmail`→`OrganizerID`, `AttendeeEmails`→`AttendeeIDs`.

---

## File Structure

- `pkg/msgraph/msgraph.go` — MODIFY: convert `resolveChunk` (method → free func) and split `ResolveAccountIDs` into a shared free func `resolveAccountIDs` + a thin app-only wrapper; rename `CreateOnlineMeetingRequest` fields and change the attendee payload to `identity.user.id`.
- `pkg/msgraph/directory_ropc.go` — CREATE: `directoryClient` (ROPC token cache, `grant_type=password`) + `NewDirectoryROPCClient`, satisfying the existing `DirectoryReader` interface.
- `pkg/msgraph/msgraph_test.go` — MODIFY/ADD: update `OrganizerEmail`/`AttendeeEmails` references to `OrganizerID`/`AttendeeIDs`; add attendee-`identity.user.id` payload assertion; add ROPC directory client tests.
- `room-service/helper.go` — MODIFY: add `errTeamsOrganizerUnresolved` sentinel.
- `room-service/handler.go` — MODIFY: add `directoryClient msgraph.DirectoryReader` handler field.
- `room-service/handler_teams.go` — MODIFY: resolve object IDs in `teamsMeeting`; add `membersToIndividualAccounts`; remove `membersToAttendeeEmails`.
- `room-service/handler_teams_test.go` — MODIFY/ADD: `fakeDirectory` double; update existing assertions; new resolution tests.
- `room-service/main.go` — MODIFY: `TeamsROPCUsername`/`TeamsROPCPassword` config; construct + inject `directoryClient`.
- `docs/client-api.md`, `docs/msgraph-client.md`, `room-service/deploy/docker-compose.yml` — MODIFY: docs + env passthrough.

---

## Task 1: msgraph — ROPC-backed directory reader

**Files:**
- Modify: `pkg/msgraph/msgraph.go` (`ResolveAccountIDs` ~399-419, `resolveChunk` ~443-506)
- Create: `pkg/msgraph/directory_ropc.go`
- Test: `pkg/msgraph/msgraph_test.go`

**Interfaces:**
- Consumes: existing `DirectoryReader` interface, `ROPCCredentials` (from `presence.go`), `Config`, `Option`, `tokenResponse`, `graphScope`, `tokenExpirySkew`, `defaultUserAgent`, `applyProxyURL`, `New`.
- Produces:
  - `func resolveAccountIDs(ctx context.Context, hc *http.Client, baseURL, userAgent, token string, accounts []string) (map[string]string, error)`
  - `func resolveChunk(ctx context.Context, hc *http.Client, baseURL, userAgent, token string, chunk []string, want map[string]struct{}, out map[string]string) error`
  - `func NewDirectoryROPCClient(cfg Config, creds ROPCCredentials, opts ...Option) (DirectoryReader, error)`

- [ ] **Step 1: Write the failing test for the ROPC directory client**

Add to `pkg/msgraph/msgraph_test.go`:

```go
func TestNewDirectoryROPCClient_ResolvesWithPasswordGrant(t *testing.T) {
	var grant, user, pass string
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		grant = r.Form.Get("grant_type")
		user = r.Form.Get("username")
		pass = r.Form.Get("password")
		assert.Equal(t, graphScope, r.Form.Get("scope"))
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "dtok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer dtok", r.Header.Get("Authorization"))
		assert.Equal(t, "eventual", r.Header.Get("ConsistencyLevel"))
		filter := r.URL.Query().Get("$filter")
		assert.Contains(t, filter, "startsWith(userPrincipalName,'alice@')")
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []GraphUser{
			{ID: "ida", UserPrincipalName: "alice@corp.com"},
		}})
	}))
	defer graphSrv.Close()

	d, err := NewDirectoryROPCClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		ROPCCredentials{Username: "svc@corp.com", Password: "pw"},
		WithTokenURL(tokenSrv.URL), WithBaseURL(graphSrv.URL),
	)
	require.NoError(t, err)

	got, err := d.ResolveAccountIDs(context.Background(), []string{"alice"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"alice": "ida"}, got)
	assert.Equal(t, "password", grant)
	assert.Equal(t, "svc@corp.com", user)
	assert.Equal(t, "pw", pass)
}

func TestNewDirectoryROPCClient_TokenErrorDoesNotLeakPassword(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(tokenResponse{Error: "invalid_grant"})
	}))
	defer tokenSrv.Close()

	d, err := NewDirectoryROPCClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		ROPCCredentials{Username: "svc@corp.com", Password: "supersecret"},
		WithTokenURL(tokenSrv.URL), WithBaseURL("http://unused"),
	)
	require.NoError(t, err)

	_, err = d.ResolveAccountIDs(context.Background(), []string{"alice"})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "supersecret")
	assert.Contains(t, err.Error(), "invalid_grant")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=pkg/msgraph` (the Makefile expands `SERVICE=` to `go test -race ./pkg/msgraph/...`).
Expected: FAIL — `NewDirectoryROPCClient` undefined.

- [ ] **Step 3: Extract the shared resolve helpers in `msgraph.go`**

Replace the `graphClient.ResolveAccountIDs` method (currently ~399-419) with a thin wrapper that delegates to a new free function, and convert `resolveChunk` to a free function. The method body moves verbatim into the free functions — only the receiver/`g.` references change to parameters.

New free functions (place directly below the existing `maxAccountsPerQuery` const in `msgraph.go`):

```go
// resolveAccountIDs is the token-agnostic directory lookup shared by the
// app-only graphClient and the ROPC directoryClient. Semantics are unchanged
// from the original graphClient.ResolveAccountIDs: chunked startsWith filter,
// result keyed by lowercased UPN local-part, first match wins.
func resolveAccountIDs(ctx context.Context, hc *http.Client, baseURL, userAgent, token string, accounts []string) (map[string]string, error) {
	out := make(map[string]string, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	want := make(map[string]struct{}, len(accounts))
	for _, a := range accounts {
		want[a] = struct{}{}
	}
	for start := 0; start < len(accounts); start += maxAccountsPerQuery {
		end := min(start+maxAccountsPerQuery, len(accounts))
		if err := resolveChunk(ctx, hc, baseURL, userAgent, token, accounts[start:end], want, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// resolveChunk queries one chunk of accounts and records the account->id matches
// (keyed by the lowercased UPN local-part) into out.
func resolveChunk(ctx context.Context, hc *http.Client, baseURL, userAgent, token string, chunk []string, want map[string]struct{}, out map[string]string) error {
	clauses := make([]string, 0, len(chunk)*2)
	for _, a := range chunk {
		for _, variant := range casedVariants(a) {
			esc := strings.ReplaceAll(variant, "'", "''")
			clauses = append(clauses, fmt.Sprintf("startsWith(userPrincipalName,'%s@')", esc))
		}
	}
	q := url.Values{}
	q.Set("$filter", strings.Join(clauses, " or "))
	q.Set("$select", "id,userPrincipalName")
	q.Set("$count", "true")
	endpoint := baseURL + "/users?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build get-users request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("ConsistencyLevel", "eventual")
	req.Header.Set("User-Agent", userAgent)
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("get users: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
	if err != nil {
		return fmt.Errorf("read get-users response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("get users: graph returned status %d", resp.StatusCode)
	}
	var page struct {
		Value []GraphUser `json:"value"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		return fmt.Errorf("decode get-users response: %w", err)
	}
	for i := range page.Value {
		u := &page.Value[i]
		if u.ID == "" {
			continue
		}
		local, ok := localPart(u.UserPrincipalName)
		if !ok {
			continue
		}
		account := strings.ToLower(local)
		if _, mine := want[account]; !mine {
			continue
		}
		if _, dup := out[account]; dup {
			continue
		}
		out[account] = u.ID
	}
	return nil
}
```

Replace the old `graphClient.ResolveAccountIDs` and `graphClient.resolveChunk` with just the wrapper:

```go
func (g *graphClient) ResolveAccountIDs(ctx context.Context, accounts []string) (map[string]string, error) {
	token, err := g.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire graph token: %w", err)
	}
	return resolveAccountIDs(ctx, g.httpClient, g.baseURL, g.userAgent, token, accounts)
}
```

Delete the now-duplicated inline logic from the old method and delete the old `func (g *graphClient) resolveChunk(...)`.

- [ ] **Step 4: Create `pkg/msgraph/directory_ropc.go`**

```go
package msgraph

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// directoryClient is a ROPC (resource-owner password) backed DirectoryReader.
// It mirrors presenceClient: its own token cache and grant_type=password token
// acquisition, delegating the actual account->objectID resolution to the shared
// resolveAccountIDs helper. Used by room-service to resolve Teams meeting
// organizer/attendee object IDs via a User.Read.All service account.
type directoryClient struct {
	cfg       Config
	creds     ROPCCredentials
	hc        *http.Client
	baseURL   string
	tokenURL  string
	userAgent string

	mu      sync.Mutex
	token   string
	tokenAt time.Time
}

// NewDirectoryROPCClient builds a ROPC-backed DirectoryReader. Like
// NewPresenceClient it resolves options via a throwaway graphClient, applies
// cfg.ProxyURL (reporting an invalid value at construction), and resolves the
// User-Agent.
//
//nolint:gocritic // hugeParam: startup-only constructor; Config passed by value is intentional.
func NewDirectoryROPCClient(cfg Config, creds ROPCCredentials, opts ...Option) (DirectoryReader, error) {
	g := New(cfg, opts...).(*graphClient)
	hc := g.httpClient
	if err := applyProxyURL(hc, cfg.ProxyURL); err != nil {
		return nil, err
	}
	ua := cfg.UserAgent
	if ua == "" {
		ua = defaultUserAgent
	}
	return &directoryClient{
		cfg: cfg, creds: creds, hc: hc, baseURL: g.baseURL, tokenURL: g.tokenURL, userAgent: ua,
	}, nil
}

// accessToken returns a cached delegated bearer token, fetching a fresh one via
// the resource-owner password (ROPC) grant when the cache is empty or expiring.
func (d *directoryClient) accessToken(ctx context.Context) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.token != "" && time.Now().Before(d.tokenAt) {
		return d.token, nil
	}
	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("client_id", d.cfg.ClientID)
	form.Set("client_secret", d.cfg.ClientSecret)
	form.Set("scope", graphScope)
	form.Set("username", d.creds.Username)
	form.Set("password", d.creds.Password)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("build directory ropc token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", d.userAgent)
	resp, err := d.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("request directory ropc token: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read directory ropc token response: %w", err)
	}
	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", fmt.Errorf("decode directory ropc token response (status %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK || tr.AccessToken == "" {
		// Never log credentials; surface only the OAuth error code.
		return "", fmt.Errorf("directory ropc token endpoint returned status %d: %s", resp.StatusCode, tr.Error)
	}
	d.token = tr.AccessToken
	lifetime := time.Duration(tr.ExpiresIn) * time.Second
	if lifetime <= tokenExpirySkew {
		lifetime = tokenExpirySkew
	}
	d.tokenAt = time.Now().Add(lifetime - tokenExpirySkew)
	return d.token, nil
}

// ResolveAccountIDs resolves account local-parts to Azure object IDs using a
// ROPC-acquired delegated token.
func (d *directoryClient) ResolveAccountIDs(ctx context.Context, accounts []string) (map[string]string, error) {
	token, err := d.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire directory ropc token: %w", err)
	}
	return resolveAccountIDs(ctx, d.hc, d.baseURL, d.userAgent, token, accounts)
}
```

Note: `json` is already imported package-wide via `msgraph.go`; since Go imports are per-file, add `"encoding/json"` to `directory_ropc.go`'s import block (used by `json.Unmarshal`). Confirm imports compile.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test SERVICE=pkg/msgraph` and confirm `TestNewDirectoryROPCClient_ResolvesWithPasswordGrant`, `TestNewDirectoryROPCClient_TokenErrorDoesNotLeakPassword`, and the pre-existing `TestResolveAccountIDs_*` all pass.
Expected: PASS (existing app-only resolve tests stay green — the delegation is behavior-preserving).

- [ ] **Step 6: Lint**

Run: `make lint`
Expected: clean (no unused-import, no gosec finding — the password reaches only the form body and the error surfaces only `tr.Error`).

- [ ] **Step 7: Commit**

```bash
git add pkg/msgraph/directory_ropc.go pkg/msgraph/msgraph.go pkg/msgraph/msgraph_test.go
git commit -m "feat(msgraph): ROPC-backed DirectoryReader for object-ID resolution

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01WT8WmZFBj8xzv4VXoMjh81"
```

---

## Task 2: msgraph — object-ID meeting payload

**Files:**
- Modify: `pkg/msgraph/msgraph.go` (`CreateOnlineMeetingRequest` ~93-108, `meetingAttendee` ~520-522, `CreateOnlineMeeting` ~524-602)
- Test: `pkg/msgraph/msgraph_test.go` (existing meeting tests + new payload test)

**Interfaces:**
- Consumes: `CreateOnlineMeeting` from Task 1's client.
- Produces:
  - `CreateOnlineMeetingRequest{ ExternalID, Subject string; OrganizerID string; AttendeeIDs []string }`
  - attendee JSON `{"identity":{"user":{"id":"<objectId>"}}}`.

- [ ] **Step 1: Write the failing test for the object-ID payload**

Add to `pkg/msgraph/msgraph_test.go`:

```go
func TestCreateOnlineMeeting_UsesObjectIDs(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "tok", ExpiresIn: 3600}) // #nosec G117 -- test mock OAuth token
	}))
	defer tokenSrv.Close()

	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// organizer object ID in the createOrGet path
		assert.Contains(t, r.URL.Path, "/users/00000000-org/onlineMeetings/createOrGet")
		var body struct {
			ExternalID   string `json:"externalId"`
			Participants struct {
				Attendees []struct {
					Identity struct {
						User struct {
							ID string `json:"id"`
						} `json:"user"`
					} `json:"identity"`
				} `json:"attendees"`
			} `json:"participants"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "k", body.ExternalID)
		require.Len(t, body.Participants.Attendees, 1)
		assert.Equal(t, "11111111-bob", body.Participants.Attendees[0].Identity.User.ID)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(OnlineMeeting{ID: "m1", JoinURL: "https://join/1"})
	}))
	defer graphSrv.Close()

	c := newTestClient(tokenSrv.URL, graphSrv.URL)
	m, err := c.CreateOnlineMeeting(context.Background(), CreateOnlineMeetingRequest{
		ExternalID: "k", Subject: "Standup", OrganizerID: "00000000-org", AttendeeIDs: []string{"11111111-bob"},
	})
	require.NoError(t, err)
	assert.Equal(t, "m1", m.ID)
}
```

- [ ] **Step 2: Update the existing meeting tests to the renamed fields**

In `pkg/msgraph/msgraph_test.go`, replace every `OrganizerEmail:` with `OrganizerID:` and every `AttendeeEmails:` with `AttendeeIDs:` (occurrences around lines 66, 73, 111, 115, 127, 143, 163, 182, 534, 558). The organizer-path assertion at ~52-54 that expects `alice%40corp.com` must change to whatever object-ID value the test now passes; simplest is to pass `OrganizerID: "alice-oid"` and assert the path contains `/users/alice-oid/onlineMeetings/createOrGet`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `make test`
Expected: FAIL — `CreateOnlineMeetingRequest` has no field `OrganizerID`/`AttendeeIDs`; `onlineMeetingPayload`/attendee shape mismatch.

- [ ] **Step 4: Rename the request fields**

In `msgraph.go`, replace the `CreateOnlineMeetingRequest` struct body:

```go
type CreateOnlineMeetingRequest struct {
	// ExternalID is the stable per-room idempotency key passed to Graph's
	// createOrGet endpoint. Required: createOrGet rejects an empty externalId.
	ExternalID string
	// Subject is the meeting title shown in Teams.
	Subject string
	// OrganizerID is the Azure AD object ID of the meeting organizer, used as
	// the /users/{id} path segment. When empty the application-context default
	// mailbox is used.
	OrganizerID string
	// AttendeeIDs are the Azure AD object IDs of the invited attendees.
	AttendeeIDs []string
}
```

- [ ] **Step 5: Change the attendee payload to `identity.user.id`**

In `msgraph.go`, replace the `meetingAttendee` type and add the identity types:

```go
type meetingParticipants struct {
	Attendees []meetingAttendee `json:"attendees,omitempty"`
}

type meetingAttendee struct {
	Identity meetingIdentitySet `json:"identity"`
}

type meetingIdentitySet struct {
	User meetingIdentity `json:"user"`
}

type meetingIdentity struct {
	ID string `json:"id"`
}
```

In `CreateOnlineMeeting`, update the organizer-path branch and the attendee build loop:

```go
	var endpoint string
	if req.OrganizerID != "" {
		endpoint = fmt.Sprintf("%s/users/%s/onlineMeetings/createOrGet", g.baseURL, url.PathEscape(req.OrganizerID))
	} else {
		endpoint = g.baseURL + "/me/onlineMeetings/createOrGet"
	}

	payload := onlineMeetingPayload{ExternalID: req.ExternalID, Subject: req.Subject}
	if len(req.AttendeeIDs) > 0 {
		attendees := make([]meetingAttendee, 0, len(req.AttendeeIDs))
		for _, id := range req.AttendeeIDs {
			attendees = append(attendees, meetingAttendee{Identity: meetingIdentitySet{User: meetingIdentity{ID: id}}})
		}
		payload.Participants = &meetingParticipants{Attendees: attendees}
	}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `make test`
Expected: PASS — `TestCreateOnlineMeeting_UsesObjectIDs` and all updated meeting tests green.

- [ ] **Step 7: Lint + commit**

```bash
make lint
git add pkg/msgraph/msgraph.go pkg/msgraph/msgraph_test.go
git commit -m "feat(msgraph): meeting payload carries Azure object IDs (organizer path + attendee identity)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01WT8WmZFBj8xzv4VXoMjh81"
```

---

## Task 3: room-service — resolve object IDs in `teamsMeeting`

**Files:**
- Modify: `room-service/helper.go` (teams sentinels block ~79-85)
- Modify: `room-service/handler.go` (Handler struct ~64-68)
- Modify: `room-service/handler_teams.go` (`teamsMeeting` ~99-182; helpers ~259-271)
- Test: `room-service/handler_teams_test.go`

**Interfaces:**
- Consumes: `msgraph.DirectoryReader` (Task 1), renamed `CreateOnlineMeetingRequest{OrganizerID, AttendeeIDs}` (Task 2).
- Produces:
  - Handler field `directoryClient msgraph.DirectoryReader`.
  - `errTeamsOrganizerUnresolved` sentinel.
  - `func membersToIndividualAccounts(members []model.RoomMember) []string` (lowercased individual accounts).

- [ ] **Step 1: Add the handler field and sentinel (compile scaffolding for the tests)**

In `room-service/handler.go`, add to the Teams block of the `Handler` struct (after `graphClient msgraph.Client`):

```go
	// directoryClient resolves account local-parts to Azure AD object IDs via a
	// ROPC User.Read.All service account. Required by the meetings RPC (nil ->
	// errTeamsNotConfigured); the deep-link call RPCs do not use it.
	directoryClient      msgraph.DirectoryReader
```

In `room-service/helper.go`, add to the teams sentinel block (after `errTeamsNotConfigured`):

```go
	errTeamsOrganizerUnresolved = errcode.Internal("could not resolve meeting organizer identity")
```

- [ ] **Step 2: Write the failing tests**

Add a `fakeDirectory` double and tests to `room-service/handler_teams_test.go`:

```go
// fakeDirectory is a hand-rolled msgraph.DirectoryReader double.
type fakeDirectory struct {
	ids       map[string]string
	err       error
	callCount int
	lastArg   []string
}

func (f *fakeDirectory) ResolveAccountIDs(_ context.Context, accounts []string) (map[string]string, error) {
	f.callCount++
	f.lastArg = accounts
	if f.err != nil {
		return nil, f.err
	}
	return f.ids, nil
}

func TestTeamsMeeting_ResolvesObjectIDs(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListRoomMembers(gomock.Any(), "r1", nil, nil, false).
		Return([]model.RoomMember{indMember("alice"), indMember("bob")}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice", EngName: "Alice"}, nil)

	graph := &fakeGraphClient{meeting: &msgraph.OnlineMeeting{ID: "mtg-1", JoinURL: "https://join/1"}}
	dir := &fakeDirectory{ids: map[string]string{"alice": "oid-alice", "bob": "oid-bob"}}
	h := &Handler{
		store: store, siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersLimit: 500,
		graphClient: graph, directoryClient: dir, teamsMeetingStore: newStubTeamsMeetingStore(),
		publishToStream: func(context.Context, string, []byte, string) error { return nil },
	}

	resp, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.NoError(t, err)
	assert.Equal(t, "mtg-1", resp.ID)
	assert.Equal(t, "oid-alice", graph.lastReq.OrganizerID)
	assert.ElementsMatch(t, []string{"oid-alice", "oid-bob"}, graph.lastReq.AttendeeIDs)
	assert.ElementsMatch(t, []string{"alice", "bob"}, dir.lastArg)
}

func TestTeamsMeeting_OrganizerUnresolvedFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListRoomMembers(gomock.Any(), "r1", nil, nil, false).
		Return([]model.RoomMember{indMember("alice"), indMember("bob")}, nil)

	graph := &fakeGraphClient{meeting: &msgraph.OnlineMeeting{ID: "should-not-be-used"}}
	dir := &fakeDirectory{ids: map[string]string{"bob": "oid-bob"}} // organizer alice missing
	h := &Handler{
		store: store, siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersLimit: 500,
		graphClient: graph, directoryClient: dir, teamsMeetingStore: newStubTeamsMeetingStore(),
		publishToStream: func(context.Context, string, []byte, string) error { return nil },
	}

	_, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.ErrorIs(t, err, errTeamsOrganizerUnresolved)
	assert.Equal(t, 0, graph.callCount, "no meeting when organizer unresolved")
}

func TestTeamsMeeting_AttendeeUnresolvedDropped(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListRoomMembers(gomock.Any(), "r1", nil, nil, false).
		Return([]model.RoomMember{indMember("alice"), indMember("bob"), indMember("carol")}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice", EngName: "Alice"}, nil)

	graph := &fakeGraphClient{meeting: &msgraph.OnlineMeeting{ID: "mtg-1", JoinURL: "https://join/1"}}
	dir := &fakeDirectory{ids: map[string]string{"alice": "oid-alice", "bob": "oid-bob"}} // carol missing
	h := &Handler{
		store: store, siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersLimit: 500,
		graphClient: graph, directoryClient: dir, teamsMeetingStore: newStubTeamsMeetingStore(),
		publishToStream: func(context.Context, string, []byte, string) error { return nil },
	}

	resp, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.NoError(t, err)
	assert.Equal(t, "mtg-1", resp.ID)
	assert.ElementsMatch(t, []string{"oid-alice", "oid-bob"}, graph.lastReq.AttendeeIDs, "carol dropped")
}

func TestTeamsMeeting_DirectoryNotConfigured(t *testing.T) {
	h := &Handler{
		store: NewMockRoomStore(gomock.NewController(t)), siteID: "site-a",
		graphClient: &fakeGraphClient{}, teamsMeetingStore: newStubTeamsMeetingStore(),
		// directoryClient nil
	}
	_, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.ErrorIs(t, err, errTeamsNotConfigured)
}

func TestTeamsMeeting_DirectoryErrorFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").Return(nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListRoomMembers(gomock.Any(), "r1", nil, nil, false).
		Return([]model.RoomMember{indMember("alice")}, nil)

	graph := &fakeGraphClient{meeting: &msgraph.OnlineMeeting{ID: "x"}}
	dir := &fakeDirectory{err: errors.New("graph 503")}
	h := &Handler{
		store: store, siteID: "site-a", teamsEmailDomain: "corp.com", roomMembersLimit: 500,
		graphClient: graph, directoryClient: dir, teamsMeetingStore: newStubTeamsMeetingStore(),
		publishToStream: func(context.Context, string, []byte, string) error { return nil },
	}
	_, err := h.teamsMeeting(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.TeamsMeetingRequest{})
	require.Error(t, err)
	assert.Equal(t, 0, graph.callCount)
}
```

- [ ] **Step 3: Update the pre-existing meeting tests to inject `directoryClient` and the renamed fields**

`TestTeamsMeeting_CreatesAndPublishes` (~270) and any other test that reaches the Graph create (e.g. the idempotent-insert-race test ~384) must:
- add `directoryClient: dir` with a `fakeDirectory` resolving every member account (e.g. `{"alice":"oid-alice","bob":"oid-bob"}`),
- change `assert.Equal(t, "alice@corp.com", graph.lastReq.OrganizerEmail)` → `assert.Equal(t, "oid-alice", graph.lastReq.OrganizerID)`,
- change the `AttendeeEmails` assertion → `AttendeeIDs` with the object-ID values.

Fast-path/short-circuit tests that return before the Graph create (e.g. `TestTeamsMeeting_Idempotent_FastPathReadHit` ~334) do NOT need a `directoryClient` **unless** the not-configured gate now runs before the fast-path read. It does not — keep the gate ordering so the fast-path read still precedes resolution (see Step 4), but the nil-gate runs first; give those tests a non-nil `directoryClient: &fakeDirectory{}` to pass the gate.

- [ ] **Step 4: Run the tests to verify they fail**

Run: `make test SERVICE=room-service`
Expected: FAIL — `directoryClient` field / `errTeamsOrganizerUnresolved` exist (Step 1) but `teamsMeeting` still builds emails, so `OrganizerID`/`AttendeeIDs` assertions fail and the resolution tests fail.

- [ ] **Step 5: Rewrite `teamsMeeting` to resolve object IDs**

In `room-service/handler_teams.go`:

1. Add `"log/slog"` to the import block (keep existing imports).

2. Update the not-configured gate (currently ~111-113):

```go
	if h.graphClient == nil || h.teamsMeetingStore == nil || h.directoryClient == nil {
		return nil, errTeamsNotConfigured
	}
```

3. Replace the organizer/attendee derivation (currently ~135-136) and the `CreateOnlineMeeting` call args (~139-144) with resolution logic. After the `countIndividualMembers` limit check, before the Graph call:

```go
	// Resolve organizer + attendee Azure AD object IDs via the ROPC directory
	// (User.Read.All). account@domain is only a guess; Graph createOrGet needs
	// the real organizer identity in the path, so a failed organizer resolution
	// is fatal. Attendees are best-effort — an unresolved attendee is dropped.
	accounts := membersToIndividualAccounts(members)
	organizerKey := strings.ToLower(requesterAccount)
	accounts = appendUnique(accounts, organizerKey)

	idByAccount, err := h.directoryClient.ResolveAccountIDs(ctx, accounts)
	if err != nil {
		return nil, fmt.Errorf("resolve member object ids: %w", err)
	}
	organizerID, ok := idByAccount[organizerKey]
	if !ok || organizerID == "" {
		return nil, errTeamsOrganizerUnresolved
	}

	attendeeIDs := make([]string, 0, len(members))
	dropped := 0
	for i := range members {
		entry := members[i].Member
		if entry.Type != model.RoomMemberIndividual || entry.Account == "" {
			continue
		}
		id, ok := idByAccount[strings.ToLower(entry.Account)]
		if !ok || id == "" {
			dropped++
			continue
		}
		attendeeIDs = append(attendeeIDs, id)
	}
	if dropped > 0 {
		slog.WarnContext(ctx, "dropped unresolved teams meeting attendees", "roomId", roomID, "count", dropped)
	}

	meeting, err := h.graphClient.CreateOnlineMeeting(ctx, msgraph.CreateOnlineMeetingRequest{
		ExternalID:  teamsMeetingExternalID(h.siteID, roomID),
		Subject:     meetingSubject(room),
		OrganizerID: organizerID,
		AttendeeIDs: attendeeIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("create online meeting: %w", err)
	}
```

4. Add the two helpers near the other member helpers (replace `membersToAttendeeEmails` ~259-271, which becomes unused, with `membersToIndividualAccounts`):

```go
// membersToIndividualAccounts returns the lowercased account of every
// individual member (for directory object-ID resolution).
func membersToIndividualAccounts(members []model.RoomMember) []string {
	out := make([]string, 0, len(members))
	for i := range members {
		entry := members[i].Member
		if entry.Type != model.RoomMemberIndividual || entry.Account == "" {
			continue
		}
		out = append(out, strings.ToLower(entry.Account))
	}
	return out
}

// appendUnique appends s to accounts only if not already present.
func appendUnique(accounts []string, s string) []string {
	for _, a := range accounts {
		if a == s {
			return accounts
		}
	}
	return append(accounts, s)
}
```

Confirm `membersToAttendeeEmails` is deleted and `membersToCallEmails`/`teamsEmail` remain (still used by the deep-link RPCs).

- [ ] **Step 6: Run the tests to verify they pass**

Run: `make test SERVICE=room-service`
Expected: PASS — all new resolution tests plus the updated existing meeting tests.

- [ ] **Step 7: Lint + commit**

```bash
make lint
git add room-service/helper.go room-service/handler.go room-service/handler_teams.go room-service/handler_teams_test.go
git commit -m "feat(room-service): resolve Teams meeting organizer/attendees to Azure object IDs

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01WT8WmZFBj8xzv4VXoMjh81"
```

---

## Task 4: room-service wiring, config, docs, compose

**Files:**
- Modify: `room-service/main.go` (config struct ~48-51; graph client construction ~156-173)
- Modify: `room-service/deploy/docker-compose.yml` (~29-32)
- Modify: `docs/client-api.md` (~2453, ~2494)
- Modify: `docs/msgraph-client.md`

**Interfaces:**
- Consumes: `msgraph.NewDirectoryROPCClient` (Task 1), `Handler.directoryClient` (Task 3).
- Produces: wired `directoryClient` when ROPC creds present.

- [ ] **Step 1: Add config fields in `main.go`**

In the `config` struct, after `TeamsClientSecret` (line ~50):

```go
	// TeamsROPCUsername/Password are the service-account resource-owner
	// credentials for the ROPC (grant_type=password) directory lookup used to
	// resolve meeting organizer/attendee Azure object IDs (User.Read.All). They
	// reuse TeamsClientID/TeamsClientSecret as the confidential client. When
	// unset the meetings RPC reports not-configured.
	TeamsROPCUsername string `env:"TEAMS_ROPC_USERNAME" envDefault:""`
	TeamsROPCPassword string `env:"TEAMS_ROPC_PASSWORD" envDefault:""`
```

- [ ] **Step 2: Construct and inject the directory client in `main.go`**

Inside the `if cfg.TeamsTenantID != "" && cfg.TeamsClientID != "" && cfg.TeamsClientSecret != ""` block, after `graphClient, err = msgraph.NewMeetingsClient(...)` succeeds, add the directory client construction (reuse the same `msgraph.Config`). Declare `var directoryClient msgraph.DirectoryReader` alongside `var graphClient msgraph.Client` (~156):

```go
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
		graphClient, err = msgraph.NewMeetingsClient(graphCfg)
		if err != nil {
			slog.Error("build graph meetings client", "error", err)
			os.Exit(1)
		}
		if cfg.TeamsROPCUsername != "" && cfg.TeamsROPCPassword != "" {
			directoryClient, err = msgraph.NewDirectoryROPCClient(graphCfg,
				msgraph.ROPCCredentials{Username: cfg.TeamsROPCUsername, Password: cfg.TeamsROPCPassword})
			if err != nil {
				slog.Error("build graph directory client", "error", err)
				os.Exit(1)
			}
		} else {
			slog.Warn("TEAMS_ROPC_USERNAME/PASSWORD unset — Teams meetings RPC will report not-configured")
		}
	}
```

(This refactors the inline `msgraph.Config{...}` literal into `graphCfg` so both clients share it — keep the behavior identical for `NewMeetingsClient`.)

Then inject alongside the other teams fields (~215-217):

```go
	handler.directoryClient = directoryClient
```

- [ ] **Step 3: Verify the service builds**

Run: `make build SERVICE=room-service`
Expected: builds clean.

- [ ] **Step 4: Update `docker-compose.yml`**

In `room-service/deploy/docker-compose.yml`, after `TEAMS_CLIENT_SECRET=` (line ~32):

```yaml
      # ROPC service account (reuses TEAMS_CLIENT_ID/SECRET as the confidential
      # client) for the User.Read.All directory lookup that resolves meeting
      # organizer/attendee Azure object IDs. Meetings RPC is not-configured until set.
      - TEAMS_ROPC_USERNAME=
      - TEAMS_ROPC_PASSWORD=
```

- [ ] **Step 5: Update `docs/client-api.md`**

At the Start Teams Meeting section (~2453), replace the trailing sentence
"Attendee emails are derived as `account@TEAMS_EMAIL_DOMAIN`." with:

> The organizer and attendees are resolved to their Azure AD object IDs via a ROPC `User.Read.All` service account (`TEAMS_ROPC_USERNAME`/`TEAMS_ROPC_PASSWORD`); the organizer object ID scopes Graph's `createOrGet` and attendees are added by object ID. An attendee that cannot be resolved is omitted; an unresolvable organizer fails the request.

In the error table (~2494), keep the single `internal` row but broaden its "When":

> | — | `internal` | Teams meetings not configured (including missing ROPC directory credentials), the organizer could not be resolved to an Azure object ID, or the Graph create failed. |

Request/reply and event structs are unchanged, so `docs/client-api/request-reply.md` and `docs/client-api/events.md` need no edits (verify with a grep for `teams.meeting` that nothing describes the attendee-email derivation there).

- [ ] **Step 6: Update `docs/msgraph-client.md`**

Add a subsection documenting the ROPC directory reader: `NewDirectoryROPCClient(cfg, creds)` resolves account→objectID via `grant_type=password` reusing `TEAMS_CLIENT_ID`/`TEAMS_CLIENT_SECRET` + `TEAMS_ROPC_USERNAME`/`TEAMS_ROPC_PASSWORD` (User.Read.All), and note that `CreateOnlineMeeting` now takes `OrganizerID`/`AttendeeIDs` (Azure object IDs; attendees serialized as `identity.user.id`). Mirror the existing doc's tone/structure.

- [ ] **Step 7: SAST + full test sweep**

Run: `make sast` and `make test SERVICE=room-service` and `make test SERVICE=pkg/msgraph`.
Expected: no medium+ SAST findings; all tests pass. Verify coverage is ≥80% for the touched packages:
`make test SERVICE=room-service` then inspect the handler_teams coverage is not regressed.

- [ ] **Step 8: Commit**

```bash
git add room-service/main.go room-service/deploy/docker-compose.yml docs/client-api.md docs/msgraph-client.md
git commit -m "feat(room-service): wire ROPC directory client + docs/config for object-ID meetings

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01WT8WmZFBj8xzv4VXoMjh81"
```

- [ ] **Step 9: Push the branch**

```bash
git push -u origin claude/room-service-azure-ad-objectid-vhow1u
```

---

## Self-Review

**Spec coverage:**
- D1 (reuse TEAMS_CLIENT_ID/SECRET) → Task 4 Step 2 (`graphCfg` shared). ✓
- D2 (organizer + attendees) → Task 3 Step 5 (resolve both). ✓
- D3 (organizer hard fail) → Task 3 Step 5 `errTeamsOrganizerUnresolved`; test Step 2 `TestTeamsMeeting_OrganizerUnresolvedFails`. ✓
- D4 (attendee best-effort drop+log) → Task 3 Step 5 `dropped`/`slog.WarnContext`; test `TestTeamsMeeting_AttendeeUnresolvedDropped`. ✓
- D5 (env names) → Task 4 Steps 1/4. ✓
- D6 (field rename) → Task 2. ✓
- D7 (meetings requires ROPC) → Task 3 Step 5 gate; test `TestTeamsMeeting_DirectoryNotConfigured`. ✓
- ROPC directory reader → Task 1. ✓
- Object-ID payload (`identity.user.id`) → Task 2 Steps 5/1. ✓
- Docs (client-api, msgraph-client) + compose → Task 4 Steps 4-6. ✓
- Out-of-scope deep-link RPCs untouched → Task 3 keeps `teamsEmail`/`membersToCallEmails`. ✓

**Placeholder scan:** No TBD/TODO; all steps contain concrete code or exact edits.

**Type consistency:** `OrganizerID`/`AttendeeIDs` used identically across Tasks 2, 3. `directoryClient msgraph.DirectoryReader` field name matches `handler.directoryClient` injection (Task 4) and test structs (Task 3). `resolveAccountIDs`/`resolveChunk` signatures identical between Task 1 producer and callers. `NewDirectoryROPCClient(cfg, creds, opts...)` signature matches Task 1 test and Task 4 call. `errTeamsOrganizerUnresolved` consistent (helper.go + handler_teams.go + test). ✓

**Note on `make test` for `pkg/msgraph`:** the Makefile's `SERVICE=` form expands to `go test -race ./$(SERVICE)/...`, so `make test SERVICE=pkg/msgraph` scopes the run to the msgraph package (no raw `go test` needed).
