# teams-user-sync HR Projection Change Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Derive `teams_user.siteId` from the HR row's `locationURL` (pattern `https://{siteID}.mysite.com`), enrich `teams_user` with HR `engName`/`mail`, and upsert HR-unmatched users with empty HR fields instead of skipping them — with logging for empty extractions and per-page lookup results.

**Architecture:** The store layer becomes a dumb projection (`HRUsers` returns raw `locationURL`/`engName`/`mail` keyed by account); all derivation and logging live in the handler (`syncPage`). The `Syncer` gains an injected `*slog.Logger` so log lines carry `main.go`'s request ID.

**Tech Stack:** Go 1.25, MongoDB (`mongo-driver/v2` via `pkg/mongoutil`), `go.uber.org/mock` (mockgen), testify, testcontainers via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-07-20-teams-user-sync-hrrow-design.md`

## Global Constraints

- Branch: all work on `claude/teams-user-sync-hrrow-x1bsa2`; never push elsewhere.
- Always use `make` targets, never raw `go` commands (exception: `go generate` runs via `make generate`).
- TDD: every task runs its failing test (compile errors count as Red) before implementation.
- `git config user.email` must be `noreply@anthropic.com`, `user.name` must be `Claude` (already set in this clone).
- All logging via `log/slog` key-value pairs; never log tokens/passwords/message bodies (`locationURL` is fine).
- `TeamsUser` is a persisted doc, NOT a client-facing request/reply or event struct — `docs/client-api.md` must NOT be touched.
- BSON/JSON tags are camelCase: `engName`, `mail`, `locationURL`.
- `LocationURL` is NOT persisted to `teams_user`; no `omitempty` on the new model fields.
- Integration tests use `pkg/testutil` helpers only (`testutil.MongoDB(t, prefix)`); `//go:build integration` tag; `TestMain` already exists.
- Coverage floor 80% (target 90%+ for handler/store); the new handler logic must be fully covered by unit tests.

---

### Task 1: Add EngName/Mail to model.TeamsUser

**Files:**
- Modify: `pkg/model/teamsuser.go`
- Test: `pkg/model/model_test.go` (functions `TestTeamsUserJSON` at ~line 4379, `TestTeamsUserBSON` at ~line 4420, `TestTeamsUserBSON_NoFrom` at ~line 4448)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `model.TeamsUser` fields `EngName string` (`json:"engName" bson:"engName"`) and `Mail string` (`json:"mail" bson:"mail"`) — Tasks 3–4 set and assert these.

- [ ] **Step 1: Write the failing tests**

In `pkg/model/model_test.go`, update `TestTeamsUserJSON` — replace the `src` literal:

```go
	src := model.TeamsUser{
		ID:      "8f4c9e2a-0b1d-4e5f-9a6b-7c8d9e0f1a2b",
		UPN:     "Alice@corp.example",
		Account: "alice",
		SiteID:  "site-a",
		EngName: "Alice Smith",
		Mail:    "alice@corp.example",
		From:    &from,
	}
```

Update `TestTeamsUserBSON` — replace the `u` literal and add raw-key assertions after the existing `assert.Equal(t, "alice", rawDoc["account"])` line:

```go
	u := model.TeamsUser{ID: "aad-user-1", UPN: "alice@corp.example", SiteID: "site-a", Account: "alice", EngName: "Alice Smith", Mail: "alice@corp.example", From: &from}
```

```go
	assert.Equal(t, "Alice Smith", rawDoc["engName"])
	assert.Equal(t, "alice@corp.example", rawDoc["mail"])
```

And add two field-equality assertions after `assert.Equal(t, u.Account, dst.Account)`:

```go
	assert.Equal(t, u.EngName, dst.EngName)
	assert.Equal(t, u.Mail, dst.Mail)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=pkg/model`
Expected: FAIL to compile — `unknown field EngName in struct literal of type model.TeamsUser`

- [ ] **Step 3: Add the fields**

In `pkg/model/teamsuser.go`, insert between the `SiteID` field and the `From` comment:

```go
	// EngName is the HR system's English name for the account; empty when the
	// account had no hr row at sync time.
	EngName string `json:"engName" bson:"engName"`
	// Mail is the HR system's mail address for the account; empty when the
	// account had no hr row at sync time.
	Mail string `json:"mail" bson:"mail"`
```

Also update the struct's doc comment first paragraph to mention the new source: change "a Teams (Azure AD) user joined with the HR system's site assignment" to "a Teams (Azure AD) user joined with the HR system's site assignment (derived from the HR locationURL), English name, and mail".

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/model`
Expected: PASS (all `TestTeamsUser*` tests green)

- [ ] **Step 5: Commit**

```bash
git add pkg/model/teamsuser.go pkg/model/model_test.go
git commit -m "feat(model): add EngName and Mail to TeamsUser"
```

---

### Task 2: extractSiteIDFromLocationURL

**Files:**
- Modify: `teams-user-sync/handler.go` (add function at bottom, next to `splitUPN`)
- Test: `teams-user-sync/handler_test.go` (add table test at bottom, next to `TestSplitUPN`)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `func extractSiteIDFromLocationURL(locationURL string) string` — returns the substring after `"://"` and before `".mysite"`, or `""` when either marker is absent. Task 3's `syncPage` calls it.

- [ ] **Step 1: Write the failing test**

Append to `teams-user-sync/handler_test.go`:

```go
func TestExtractSiteIDFromLocationURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{"https url", "https://site-a.mysite.com", "site-a"},
		{"http scheme accepted", "http://site-b.mysite.com", "site-b"},
		{"trailing path", "https://site-a.mysite.com/floor/3", "site-a"},
		{"port after domain", "https://site-a.mysite.com:8443", "site-a"},
		{"no scheme separator", "site-a.mysite.com", ""},
		{"no mysite marker", "https://site-a.othersite.com", ""},
		{"empty siteID between markers", "https://.mysite.com", ""},
		{"empty string", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, extractSiteIDFromLocationURL(tt.url))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=teams-user-sync`
Expected: FAIL to compile — `undefined: extractSiteIDFromLocationURL`

- [ ] **Step 3: Write the implementation**

Append to `teams-user-sync/handler.go` (after `splitUPN`):

```go
// extractSiteIDFromLocationURL returns the substring after "://" and before
// ".mysite" (pattern https://{siteID}.mysite.com); "" when either marker is
// absent.
func extractSiteIDFromLocationURL(locationURL string) string {
	_, rest, ok := strings.Cut(locationURL, "://")
	if !ok {
		return ""
	}
	siteID, _, ok := strings.Cut(rest, ".mysite")
	if !ok {
		return ""
	}
	return siteID
}
```

(`strings` is already imported in `handler.go`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=teams-user-sync`
Expected: PASS (`TestExtractSiteIDFromLocationURL` green; all existing tests still green — nothing else changed yet)

- [ ] **Step 5: Commit**

```bash
git add teams-user-sync/handler.go teams-user-sync/handler_test.go
git commit -m "feat(teams-user-sync): add extractSiteIDFromLocationURL"
```

---

### Task 3: HRUsers store method + syncPage rewrite + logger injection

Everything in `package main` is one compilation unit, and the `NewSyncer` signature and `Store` interface both change — so store, handler, main, mock, and all three test files move together in this task. TDD Red = the rewritten tests fail to compile / fail against the old behavior.

**Files:**
- Modify: `teams-user-sync/store.go`
- Modify: `teams-user-sync/store_mongo.go:24-29` (`hrRow`), `store_mongo.go:66-81` (`HRSiteIDs` → `HRUsers`)
- Modify: `teams-user-sync/handler.go` (Syncer struct, `NewSyncer`, `syncPage`, `RunStats` comment)
- Modify: `teams-user-sync/main.go:72-76`
- Regenerate: `teams-user-sync/mock_store_test.go` (via `make generate SERVICE=teams-user-sync` — never hand-edit)
- Test: `teams-user-sync/handler_test.go`, `teams-user-sync/store_integration_test.go`, `teams-user-sync/integration_test.go`

**Interfaces:**
- Consumes: `model.TeamsUser.EngName`/`.Mail` (Task 1), `extractSiteIDFromLocationURL` (Task 2).
- Produces:
  - `type hrUser struct { LocationURL, EngName, Mail string }` (unexported, in `store.go`)
  - `Store.HRUsers(ctx context.Context, accounts []string) (map[string]hrUser, error)` (replaces `HRSiteIDs`)
  - `func NewSyncer(store Store, graph msgraph.UserLister, pageSize int, logger *slog.Logger) *Syncer` (replaces the 3-arg form)
  - Behavior: every candidate is upserted; HR-unmatched candidates get empty `SiteID`/`EngName`/`Mail` and increment `stats.HRUnmatched`.

- [ ] **Step 1: Rewrite the unit tests (Red)**

Apply ALL of the following edits to `teams-user-sync/handler_test.go`.

1a. Add `"io"` and `"log/slog"` to the imports and a package-level helper after `fakeLister`:

```go
// discardLogger keeps Syncer log output out of test noise.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
```

1b. Every `NewSyncer(store, lister, 500)` / `NewSyncer(store, &fakeLister{pages: page}, 500)` call gains a fourth argument `discardLogger()`. There are 10 call sites.

1c. `TestSyncer_UpdateUsers_HappyPathTwoPages` — replace the two HR/upsert expectation pairs:

```go
	store.EXPECT().HRUsers(gomock.Any(), []string{"alice"}).
		Return(map[string]hrUser{"alice": {LocationURL: "https://site-a.mysite.com", EngName: "Alice Smith", Mail: "alice@corp.example"}}, nil)
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u1", UPN: "Alice@corp.example", Account: "alice", SiteID: "site-a", EngName: "Alice Smith", Mail: "alice@corp.example"},
	}).Return(nil)
```

```go
	store.EXPECT().HRUsers(gomock.Any(), []string{"carol"}).
		Return(map[string]hrUser{"carol": {LocationURL: "https://site-b.mysite.com", EngName: "Carol Jones", Mail: "carol@corp.example"}}, nil)
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u3", UPN: "carol@corp.example", Account: "carol", SiteID: "site-b", EngName: "Carol Jones", Mail: "carol@corp.example"},
	}).Return(nil)
```

1d. `TestSyncer_UpdateUsers_SkipsMalformedUPN` — the guest (no hr row) is now upserted with empty HR fields. Replace the HR/upsert expectations and the stats assertion:

```go
	store.EXPECT().HRUsers(gomock.Any(), []string{"guest#ext#", "dave"}).
		Return(map[string]hrUser{"dave": {LocationURL: "https://site-a.mysite.com", EngName: "Dave Lee", Mail: "dave@corp.example"}}, nil) // guest has no hr row
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u1", UPN: "guest#EXT#@other.example", Account: "guest#ext#"},
		{ID: "u3", UPN: "Dave@CORP.EXAMPLE", Account: "dave", SiteID: "site-a", EngName: "Dave Lee", Mail: "dave@corp.example"},
	}).Return(nil)
```

```go
	assert.Equal(t, RunStats{Pages: 1, Seen: 3, InvalidUPN: 1, HRUnmatched: 1, Upserted: 2}, stats)
```

1e. Rename `TestSyncer_UpdateUsers_HRMissSkippedAndCounted` → `TestSyncer_UpdateUsers_HRMissUpsertedAndCounted`; replace expectations and stats:

```go
	store.EXPECT().HRUsers(gomock.Any(), []string{"alice", "eve"}).
		Return(map[string]hrUser{"alice": {LocationURL: "https://site-a.mysite.com", EngName: "Alice Smith", Mail: "alice@corp.example"}}, nil) // eve unmatched
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u1", UPN: "alice@corp.example", Account: "alice", SiteID: "site-a", EngName: "Alice Smith", Mail: "alice@corp.example"},
		{ID: "u2", UPN: "eve@corp.example", Account: "eve"},
	}).Return(nil)
```

```go
	assert.Equal(t, RunStats{Pages: 1, Seen: 2, HRUnmatched: 1, Upserted: 2}, stats)
```

1f. Rename `TestSyncer_UpdateUsers_AllHRMissSkipsWrite` → `TestSyncer_UpdateUsers_AllHRMissStillUpserts`; the write now happens:

```go
	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1"}).
		Return(map[string]struct{}{}, nil)
	store.EXPECT().HRUsers(gomock.Any(), []string{"eve"}).
		Return(map[string]hrUser{}, nil)
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u1", UPN: "eve@corp.example", Account: "eve"},
	}).Return(nil)
```

```go
	assert.Equal(t, RunStats{Pages: 1, Seen: 1, HRUnmatched: 1, Upserted: 1}, stats)
```

1g. `TestSyncer_UpdateUsers_ErrorPaths` — rename the subtest `"HRSiteIDs error aborts"` to `"HRUsers error aborts"` and change its expectation:

```go
		store.EXPECT().HRUsers(gomock.Any(), gomock.Any()).
			Return(nil, errors.New("hr down"))
```

In the `"Upsert error aborts"` subtest:

```go
		store.EXPECT().HRUsers(gomock.Any(), gomock.Any()).
			Return(map[string]hrUser{"alice": {LocationURL: "https://site-a.mysite.com"}}, nil)
```

1h. Add a new table test for the locationURL branches (after `TestSyncer_UpdateUsers_HRMissUpsertedAndCounted`):

```go
func TestSyncer_UpdateUsers_LocationURLVariants(t *testing.T) {
	tests := []struct {
		name string
		hr   hrUser
		want model.TeamsUser
	}{
		{
			"valid url derives siteID",
			hrUser{LocationURL: "https://site-a.mysite.com", EngName: "Alice Smith", Mail: "alice@corp.example"},
			model.TeamsUser{ID: "u1", UPN: "alice@corp.example", Account: "alice", SiteID: "site-a", EngName: "Alice Smith", Mail: "alice@corp.example"},
		},
		{
			"empty locationURL keeps empty siteID",
			hrUser{EngName: "Alice Smith", Mail: "alice@corp.example"},
			model.TeamsUser{ID: "u1", UPN: "alice@corp.example", Account: "alice", EngName: "Alice Smith", Mail: "alice@corp.example"},
		},
		{
			"malformed locationURL keeps empty siteID",
			hrUser{LocationURL: "site-a.mysite.com", EngName: "Alice Smith", Mail: "alice@corp.example"},
			model.TeamsUser{ID: "u1", UPN: "alice@corp.example", Account: "alice", EngName: "Alice Smith", Mail: "alice@corp.example"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			lister := &fakeLister{pages: [][]msgraph.GraphUser{{
				{ID: "u1", UserPrincipalName: "alice@corp.example"},
			}}}

			store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1"}).
				Return(map[string]struct{}{}, nil)
			store.EXPECT().HRUsers(gomock.Any(), []string{"alice"}).
				Return(map[string]hrUser{"alice": tt.hr}, nil)
			store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{tt.want}).Return(nil)

			syncer := NewSyncer(store, lister, 500, discardLogger())
			stats, err := syncer.UpdateUsers(context.Background())
			require.NoError(t, err)
			assert.Equal(t, RunStats{Pages: 1, Seen: 1, Upserted: 1}, stats)
		})
	}
}
```

- [ ] **Step 2: Run tests to verify they fail (Red)**

Run: `make test SERVICE=teams-user-sync`
Expected: FAIL to compile — `undefined: hrUser`, `store.EXPECT().HRUsers undefined`, wrong argument count to `NewSyncer`

- [ ] **Step 3: Rewrite store.go**

Replace the full contents of `teams-user-sync/store.go` with:

```go
package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// hrUser is the raw HR data resolved for an account; siteID derivation from
// LocationURL happens in the handler.
type hrUser struct {
	LocationURL string
	EngName     string
	Mail        string
}

// Store is the persistence surface updateUsers needs. Reads (ExistingIDs,
// HRUsers) are served by the read client; UpsertTeamsUsers by the write
// client.
type Store interface {
	// ExistingIDs returns which of ids already exist in teams_user.
	ExistingIDs(ctx context.Context, ids []string) (map[string]struct{}, error)
	// HRUsers resolves accounts to their HR data from the hr collection
	// (keyed by hr.accountName); accounts without a match are absent.
	HRUsers(ctx context.Context, accounts []string) (map[string]hrUser, error)
	// UpsertTeamsUsers bulk-upserts merged records into teams_user, keyed on _id.
	UpsertTeamsUsers(ctx context.Context, users []model.TeamsUser) error
}
```

- [ ] **Step 4: Update store_mongo.go**

Replace the `hrRow` type (lines 24–29) with:

```go
// hrRow is the projection decoded from the hr collection: the account, its
// location URL (the siteID source), English name, and mail.
type hrRow struct {
	AccountName string `bson:"accountName"`
	LocationURL string `bson:"locationURL"`
	EngName     string `bson:"engName"`
	Mail        string `bson:"mail"`
}
```

Replace the `HRSiteIDs` method (lines 66–81) with:

```go
func (s *mongoStore) HRUsers(ctx context.Context, accounts []string) (map[string]hrUser, error) {
	out := make(map[string]hrUser, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	rows, err := s.readHR.FindMany(ctx,
		bson.M{"accountName": bson.M{"$in": accounts}},
		mongoutil.WithProjection(bson.M{"accountName": 1, "locationURL": 1, "engName": 1, "mail": 1}))
	if err != nil {
		return nil, fmt.Errorf("find hr accounts: %w", err)
	}
	for _, r := range rows {
		out[r.AccountName] = hrUser{LocationURL: r.LocationURL, EngName: r.EngName, Mail: r.Mail}
	}
	return out, nil
}
```

- [ ] **Step 5: Regenerate the mock**

Run: `make generate SERVICE=teams-user-sync`
Expected: `mock_store_test.go` now has `HRUsers` (and no `HRSiteIDs`). Do not edit it manually.

- [ ] **Step 6: Rewrite handler.go (Syncer + syncPage)**

In `teams-user-sync/handler.go`:

6a. Add `"log/slog"` to the imports.

6b. Replace the `Syncer` struct and `NewSyncer`:

```go
// Syncer runs updateUsers: walk Graph /users page by page, insert the users
// missing from teams_user, joined with their HR data (siteID derived from the
// HR locationURL) when an hr row exists.
type Syncer struct {
	store    Store
	graph    msgraph.UserLister
	pageSize int
	logger   *slog.Logger
}

// NewSyncer builds a Syncer. pageSize is Graph's $top.
func NewSyncer(store Store, graph msgraph.UserLister, pageSize int, logger *slog.Logger) *Syncer {
	return &Syncer{store: store, graph: graph, pageSize: pageSize, logger: logger}
}
```

6c. In `RunStats`, update the `HRUnmatched` field comment:

```go
	HRUnmatched int // no hr.accountName match; upserted with empty HR fields
```

6d. Replace everything in `syncPage` from the `accounts := make(...)` line through the final `return nil` with:

```go
	accounts := make([]string, 0, len(candidates))
	for _, c := range candidates {
		accounts = append(accounts, c.Account)
	}
	hrUsers, err := s.store.HRUsers(ctx, accounts)
	if err != nil {
		return fmt.Errorf("resolve hr users: %w", err)
	}
	s.logger.Info("hr site ids lookup result",
		"requested", len(accounts), "matched", len(hrUsers), "unmatched", len(accounts)-len(hrUsers))

	merged := make([]model.TeamsUser, 0, len(candidates))
	for _, c := range candidates {
		hr, ok := hrUsers[c.Account]
		if !ok {
			stats.HRUnmatched++
			s.logger.Info("hr id not found", "account", c.Account, "userId", c.ID)
			merged = append(merged, c)
			continue
		}
		c.EngName = hr.EngName
		c.Mail = hr.Mail
		if hr.LocationURL == "" {
			s.logger.Warn("hr locationURL is empty", "account", c.Account)
		} else {
			c.SiteID = extractSiteIDFromLocationURL(hr.LocationURL)
			if c.SiteID == "" {
				s.logger.Warn("extract siteID from locationURL returned empty",
					"account", c.Account, "locationURL", hr.LocationURL)
			}
		}
		merged = append(merged, c)
	}
	if err := s.store.UpsertTeamsUsers(ctx, merged); err != nil {
		return fmt.Errorf("upsert teams users: %w", err)
	}
	stats.Upserted += len(merged)
	return nil
```

Note the old `if len(merged) == 0 { return nil }` guard is intentionally gone: every candidate is appended, and `candidates` is already known non-empty at this point.

6e. Update the package-level `Syncer` doc comment on line 12–13 if not already covered by 6b (it is — 6b replaces it).

- [ ] **Step 7: Update main.go**

In `teams-user-sync/main.go`, replace lines 72–76:

```go
	store := newMongoStore(readClient.Database(cfg.MongoDB), writeClient.Database(cfg.MongoDB))
	logger := slog.With("requestId", idgen.GenerateRequestID())
	syncer := NewSyncer(store, lister, cfg.GraphPageSize, logger)

	logger.Info("teams user sync started")
```

(The `logger :=` declaration moves up from its old position; everything after `logger.Info("teams user sync started")` is unchanged.)

- [ ] **Step 8: Run unit tests to verify they pass (Green)**

Run: `make test SERVICE=teams-user-sync`
Expected: PASS — all rewritten tests green.

- [ ] **Step 9: Update store integration tests**

In `teams-user-sync/store_integration_test.go`, replace `TestMongoStore_HRSiteIDs` and `TestMongoStore_HRSiteIDs_EmptyInput` with:

```go
func TestMongoStore_HRUsers(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	ctx := context.Background()
	store := newMongoStore(db, db)

	_, err := db.Collection("hr").InsertMany(ctx, []any{
		bson.M{"accountName": "alice", "locationURL": "https://site-a.mysite.com", "engName": "Alice Smith", "mail": "alice@corp.example", "unrelated": "x"},
		bson.M{"accountName": "bob", "locationURL": "https://site-b.mysite.com", "engName": "Bob Wu", "mail": "bob@corp.example"},
		bson.M{"accountName": "dana"}, // hr row with no HR fields at all
	})
	require.NoError(t, err)

	got, err := store.HRUsers(ctx, []string{"alice", "bob", "carol", "dana"})
	require.NoError(t, err)
	assert.Equal(t, map[string]hrUser{
		"alice": {LocationURL: "https://site-a.mysite.com", EngName: "Alice Smith", Mail: "alice@corp.example"},
		"bob":   {LocationURL: "https://site-b.mysite.com", EngName: "Bob Wu", Mail: "bob@corp.example"},
		"dana":  {},
	}, got)
}

func TestMongoStore_HRUsers_EmptyInput(t *testing.T) {
	db := testutil.MongoDB(t, "teams_user_sync")
	store := newMongoStore(db, db)

	got, err := store.HRUsers(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}
```

Also update `TestMongoStore_UpsertTeamsUsers_InsertAndIdempotentRerun` — replace the `users` literal so the new fields are exercised:

```go
	users := []model.TeamsUser{
		{ID: "u1", UPN: "Alice@corp.example", Account: "alice", SiteID: "site-a", EngName: "Alice Smith", Mail: "alice@corp.example"},
		{ID: "u2", UPN: "bob@corp.example", Account: "bob", SiteID: "site-b"},
	}
```

- [ ] **Step 10: Update the end-to-end integration test**

In `teams-user-sync/integration_test.go`:

10a. Replace the hr fixture insert:

```go
	_, err := db.Collection("hr").InsertMany(ctx, []any{
		bson.M{"accountName": "alice", "locationURL": "https://site-a.mysite.com", "engName": "Alice Smith", "mail": "alice@corp.example"},
		bson.M{"accountName": "old", "locationURL": "https://site-a.mysite.com"},
	})
```

10b. Update the comment on the guest line — it is no longer skipped:

```go
				{"id": "id-guest", "userPrincipalName": "guest#EXT#@other.example"}, // no hr row -> upserted with empty HR fields
```

10c. Replace the `NewSyncer` call:

```go
	syncer := NewSyncer(newMongoStore(db, db), lister, 500, slog.New(slog.NewTextHandler(io.Discard, nil)))
```

and add `"io"` and `"log/slog"` to the imports.

10d. Replace the first stats assertion (carol and guest are now upserted too):

```go
	assert.Equal(t, RunStats{
		Pages: 2, Seen: 4, Existing: 1, HRUnmatched: 2, Upserted: 3,
	}, stats)
```

10e. Replace the id-alice doc assertion:

```go
	var doc model.TeamsUser
	require.NoError(t, db.Collection("teams_user").FindOne(ctx, bson.M{"_id": "id-alice"}).Decode(&doc))
	assert.Equal(t, model.TeamsUser{
		ID: "id-alice", UPN: "Alice@corp.example", Account: "alice", SiteID: "site-a",
		EngName: "Alice Smith", Mail: "alice@corp.example",
	}, doc)

	// the HR-unmatched guest is stored with empty HR-derived fields
	var guest model.TeamsUser
	require.NoError(t, db.Collection("teams_user").FindOne(ctx, bson.M{"_id": "id-guest"}).Decode(&guest))
	assert.Equal(t, model.TeamsUser{
		ID: "id-guest", UPN: "guest#EXT#@other.example", Account: "guest#ext#",
	}, guest)
```

10f. Replace the rerun assertion and final count (everything now exists):

```go
	// rerun: every Graph user now exists in teams_user
	stats2, err := syncer.UpdateUsers(ctx)
	require.NoError(t, err)
	assert.Equal(t, RunStats{
		Pages: 2, Seen: 4, Existing: 4, Upserted: 0,
	}, stats2)

	n, err := db.Collection("teams_user").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 4, n)
```

- [ ] **Step 11: Run integration tests**

Run: `make test-integration SERVICE=teams-user-sync`
Expected: PASS (requires Docker; testcontainers Mongo starts via `pkg/testutil`)

- [ ] **Step 12: Commit**

```bash
git add teams-user-sync/
git commit -m "feat(teams-user-sync): derive siteID from HR locationURL, add engName/mail, upsert HR-unmatched users"
```

---

### Task 4: Full verification and push

**Files:** none created; verification only.

**Interfaces:**
- Consumes: all previous tasks' work.
- Produces: green lint/tests/SAST on the branch, pushed to `origin/claude/teams-user-sync-hrrow-x1bsa2`.

- [ ] **Step 1: Format and lint**

Run: `make fmt && make lint`
Expected: no diffs from fmt (or commit them), lint PASS. If fmt changed files, re-run the Task 3 unit tests before committing the formatting.

- [ ] **Step 2: Full unit test suite**

Run: `make test`
Expected: PASS across the whole repo (catches any consumer of `model.TeamsUser` affected by the new fields — `teams-chat-sync`, `teams-chat-member-sync` compile against it).

- [ ] **Step 3: Coverage check for the touched service**

Run: `go test -race -coverprofile=/tmp/claude-0/-home-user-newchat/c615332d-00b9-5e11-98ec-5435c2f4ea96/scratchpad/cover.out ./teams-user-sync/... && go tool cover -func=/tmp/claude-0/-home-user-newchat/c615332d-00b9-5e11-98ec-5435c2f4ea96/scratchpad/cover.out | tail -5`
Expected: total ≥ 80% (handler logic should be ≥ 90%). (Coverage inspection is the one sanctioned direct `go test` use — the Makefile has no coverage target.)

- [ ] **Step 4: SAST**

Run: `make sast`
Expected: `SAST summary: gosec=PASS govulncheck=PASS semgrep=PASS`. (If tools are missing in this environment, run `make tools` first; if the environment cannot install them, note it in the final report rather than skipping silently.)

- [ ] **Step 5: Push**

```bash
git push -u origin claude/teams-user-sync-hrrow-x1bsa2
```

Retry up to 4 times with exponential backoff (2s, 4s, 8s, 16s) only on network errors.
Expected: branch updated on origin. Do NOT create a PR (not requested).

---

## Self-Review Notes

- Spec coverage: model fields (Task 1), extraction fn (Task 2), hrRow/HRUsers/projection (Task 3 Steps 3–5), syncPage behavior + all four log lines (Task 3 Step 6), logger injection via main.go (Task 3 Step 7), unit + store-integration + e2e tests (Task 3 Steps 1, 9, 10), out-of-scope items untouched (no backfill, no client-api.md, RunStats shape unchanged).
- Type consistency: `hrUser` (Task 3 Step 3) matches usages in tests (Step 1) and `syncPage` (Step 6); `NewSyncer` 4-arg form consistent across handler_test, integration_test, main.go.
- The e2e rerun stats intentionally drop `HRUnmatched` (all users exist on rerun, so the HR lookup never runs for them).
