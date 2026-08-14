# Permission Cross-Site Sync & Bulk Round Implementation Plan

*As-shipped note: this plan describes deltas against a pre-rewrite branch state. The shipped history was rebuilt, so the removals it prescribes (`bodyLimit`, `permission.get`, `MaxSubjects`/`EvaluateGrant`/`SiteID`) never appear in the final commits, and some prescribed shapes (`stateGroupKey`, `GetUserSettingsAndPermissions`) were superseded by later cleanup. The code is the source of truth.*

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rework the already-implemented permission whitelist (branch state = the superseded 2026-08-11 revision) into the final design: permission state materialized on user documents and fanned out to every site in chunks, queried through `settings.get`, with an idempotent resync endpoint, no artificial request limits, and a bulk-capable admin console — then rewrite the branch history into a clean reviewable sequence.

**Architecture:** admin-service writes the append-only ledger AND a per-user `PermissionState` snapshot in one Mongo transaction, then direct-publishes chunked `UserPermissionsUpdated` events to each remote site's INBOX (failures reported as `syncFailures`, healed by a read-only resync endpoint). inbox-worker applies events under a per-key `$lte` watermark guard. user-service composes the snapshot into the `settings.get` response at read time. The dedicated `permission.get` RPC and all request limits are removed.

**Tech Stack:** Go 1.25, Gin, MongoDB driver v2, NATS JetStream (`nc.JetStream()`), mockgen, testify, testcontainers; React + Vitest for admin-frontend.

**Specs (authoritative):** `docs/superpowers/specs/2026-08-10-user-permission-whitelist-design.md` (as amended through 2026-08-13) and `docs/superpowers/specs/2026-08-13-permission-bulk-frontend-design.md`.

## Global Constraints

- All commands via make targets: `make lint`, `make test [SERVICE=x]`, `make generate [SERVICE=x]`, `make test-integration [SERVICE=x]`, `make sast`. Never raw `go` commands.
- TDD per task: write the failing test, see it fail, implement, see it pass, commit. Coverage floor 80%; target 90%+ on evaluation/guard/fanout logic.
- Commits: conventional `type(scope): subject` (+ optional short body). **NEVER add `Co-Authored-By` or any AI-provenance trailer — this user rule overrides the repo-default trailer.** Never push; never touch main. The pre-commit hook runs lint/tests — fix failures, never bypass.
- Watermark guard (both write sites, identical): filter `permissions.<field>.updatedAt` `$exists:false` OR `$lte <state.UpdatedAt>`; update `$set {permissions.<field>: <whole state>}`; **no upsert** (missing user doc = silent no-op); `MatchedCount < len(accounts)` is normal, never an error.
- `<field>` always comes from `model.PermissionFieldName` — never hardcode `externalImageView` in filters outside pkg/model tests.
- `fanoutChunkSize = 5000`. Event type const `user_permissions_updated`. Event `Timestamp` = `time.Now().UTC().UnixMilli()` at publish; `State.UpdatedAt` = the batch's `recordedAt`.
- errcode Tier-1 only (`errcode.BadRequest(...)` etc. + `errhttp.Write` in Gin handlers; natsrouter returns automatically). Never log-and-return the same error. No new reasons — reuse `unknown_permission`, `invalid_subject_count` (now = empty list), `missing_permission_fields`, `invalid_permission_window`, `unexpected_permission_window`, `inactive_subject`, `unknown_accounts`.
- `subjectAccounts`: non-empty, **no cap**; existence/active lookup is **site-unfiltered** (`{account: {$in: …}}`).
- No new third-party dependencies. No `os.Getenv` — config via `caarlos0/env` struct tags.
- Docs rule: `docs/client-api.md` and `docs/client-api/request-reply.md` change together (Task 13); `docs/client-api/events.md` untouched (INBOX events are site→site, not server→client).
- pkg/model deletions (`EvaluateGrant`, `MaxSubjects`, `PermissionGrant.SiteID`) happen ONLY in Task 10, after every consumer is migrated.
- Frontend copy is English, matching the existing console; client-side dedup preserves first-occurrence order.

---

### Task 1: pkg/model — permission state types (additive only)

**Files:**
- Modify: `pkg/model/permission.go` (append after `EvaluateGrant`)
- Modify: `pkg/model/user.go` (User struct, next to `Settings`/`Chatlist` at ~L72-74)
- Modify: `pkg/model/permission_test.go` (append)
- Modify: `pkg/model/model_test.go` (round-trips)

**Interfaces:**
- Consumes: existing `PermissionKey`, `PermissionExternalImageView`, `KnownPermission`.
- Produces (used by Tasks 3-9): `PermissionState` (+`Evaluate(now time.Time) bool`), `UserPermissions` (+`Evaluated(now time.Time) map[PermissionKey]bool`, +`State(k PermissionKey) *PermissionState`), `PermissionFieldName(k PermissionKey) (string, bool)`, `User.Permissions *UserPermissions`.
- Deletes nothing (deletions are Task 10).

- [ ] **Step 1: Write the failing tests** — append to `pkg/model/permission_test.go`:

```go
func TestPermissionState_Evaluate(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	before := now.Add(-time.Hour)
	after := now.Add(time.Hour)
	cases := []struct {
		name  string
		state *PermissionState
		want  bool
	}{
		{"nil state", nil, false},
		{"revoked", &PermissionState{Granted: false, UpdatedAt: now}, false},
		{"granted but nil bounds fails closed", &PermissionState{Granted: true, UpdatedAt: now}, false},
		{"granted but nil expiresAt fails closed", &PermissionState{Granted: true, EffectiveFrom: &before, UpdatedAt: now}, false},
		{"granted but nil effectiveFrom fails closed", &PermissionState{Granted: true, ExpiresAt: &after, UpdatedAt: now}, false},
		{"inside window", &PermissionState{Granted: true, EffectiveFrom: &before, ExpiresAt: &after, UpdatedAt: now}, true},
		{"now == effectiveFrom is inside (closed start)", &PermissionState{Granted: true, EffectiveFrom: &now, ExpiresAt: &after, UpdatedAt: now}, true},
		{"now == expiresAt is outside (open end)", &PermissionState{Granted: true, EffectiveFrom: &before, ExpiresAt: &now, UpdatedAt: now}, false},
		{"not yet effective", &PermissionState{Granted: true, EffectiveFrom: &after, ExpiresAt: &after, UpdatedAt: now}, false},
		{"expired", &PermissionState{Granted: true, EffectiveFrom: &before, ExpiresAt: &before, UpdatedAt: now}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.state.Evaluate(now))
		})
	}
}

func TestUserPermissions_Evaluated(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	before, after := now.Add(-time.Hour), now.Add(time.Hour)

	t.Run("nil receiver yields every known key false", func(t *testing.T) {
		var p *UserPermissions
		assert.Equal(t, map[PermissionKey]bool{PermissionExternalImageView: false}, p.Evaluated(now))
	})
	t.Run("active grant evaluates true", func(t *testing.T) {
		p := &UserPermissions{ExternalImageView: &PermissionState{Granted: true, EffectiveFrom: &before, ExpiresAt: &after, UpdatedAt: now}}
		assert.True(t, p.Evaluated(now)[PermissionExternalImageView])
	})
	t.Run("expired grant evaluates false", func(t *testing.T) {
		p := &UserPermissions{ExternalImageView: &PermissionState{Granted: true, EffectiveFrom: &before, ExpiresAt: &before, UpdatedAt: now}}
		assert.False(t, p.Evaluated(now)[PermissionExternalImageView])
	})
}

func TestUserPermissions_State(t *testing.T) {
	st := &PermissionState{Granted: true}
	p := &UserPermissions{ExternalImageView: st}
	assert.Same(t, st, p.State(PermissionExternalImageView))
	assert.Nil(t, p.State(PermissionKey("nope")))
	var nilP *UserPermissions
	assert.Nil(t, nilP.State(PermissionExternalImageView))
}

func TestPermissionFieldName(t *testing.T) {
	f, ok := PermissionFieldName(PermissionExternalImageView)
	assert.True(t, ok)
	assert.Equal(t, "externalImageView", f)
	_, ok = PermissionFieldName(PermissionKey("nope"))
	assert.False(t, ok)
}
```

Also extend `pkg/model/model_test.go`: add `PermissionState` and `UserPermissions` cases to the generic `roundTrip` table (follow the existing entries' shape; use a fully-populated `PermissionState` with both bounds and `UpdatedAt` set), and set `Permissions: &UserPermissions{...}` on the existing `User` round-trip fixture so the new field is covered.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=pkg/model`
Expected: FAIL — `undefined: PermissionState`, `undefined: PermissionFieldName`.

- [ ] **Step 3: Implement** — append to `pkg/model/permission.go`:

```go
// PermissionState is the latest admin decision for one permission key, materialized on
// the user document and replicated to every site. UpdatedAt is the write's RecordedAt
// and doubles as the per-key last-write-wins watermark.
type PermissionState struct {
	Granted       bool       `json:"granted"                 bson:"granted"`
	EffectiveFrom *time.Time `json:"effectiveFrom,omitempty" bson:"effectiveFrom,omitempty"`
	ExpiresAt     *time.Time `json:"expiresAt,omitempty"     bson:"expiresAt,omitempty"`
	UpdatedAt     time.Time  `json:"updatedAt"               bson:"updatedAt"`
}

// Evaluate reports whether the permission is effective at now. A nil state, a revoked
// state, or a grant missing either bound (malformed data) is false — deny, never panic.
// now == EffectiveFrom is inside the window; now == ExpiresAt is outside (half-open).
func (s *PermissionState) Evaluate(now time.Time) bool {
	if s == nil || !s.Granted {
		return false
	}
	if s.EffectiveFrom == nil || s.ExpiresAt == nil {
		return false
	}
	return !now.Before(*s.EffectiveFrom) && now.Before(*s.ExpiresAt)
}

// UserPermissions is the admin-managed permission snapshot on the user document — one
// named field per known PermissionKey. Deliberately a struct, not a map: the key
// "external.image.view" contains dots, and MongoDB dot-path updates cannot address map
// keys containing dots.
type UserPermissions struct {
	ExternalImageView *PermissionState `json:"externalImageView,omitempty" bson:"externalImageView,omitempty"`
}

// Evaluated returns the evaluated boolean for every known permission key. Nil-receiver
// safe: no snapshot means every key is false. settings.get and the admin GET's
// currentlyGranted both call this, so the entry points cannot disagree.
func (p *UserPermissions) Evaluated(now time.Time) map[PermissionKey]bool {
	out := map[PermissionKey]bool{PermissionExternalImageView: false}
	if p == nil {
		return out
	}
	out[PermissionExternalImageView] = p.ExternalImageView.Evaluate(now)
	return out
}

// State returns the stored state for the given key, nil when absent or unknown.
func (p *UserPermissions) State(k PermissionKey) *PermissionState {
	if p == nil {
		return nil
	}
	if k == PermissionExternalImageView {
		return p.ExternalImageView
	}
	return nil
}

// PermissionFieldName maps a PermissionKey to its bson field name inside the
// `permissions` sub-document ("external.image.view" → "externalImageView"); false for
// unknown keys. The two guarded-update sites (admin-service store, inbox-worker store)
// build their `permissions.<field>` dot-paths from this.
func PermissionFieldName(k PermissionKey) (string, bool) {
	if k == PermissionExternalImageView {
		return "externalImageView", true
	}
	return "", false
}
```

And in `pkg/model/user.go`, directly after the `Settings` field (~L72):

```go
	// Permissions is the admin-managed permission snapshot — a sibling of Settings so
	// the settings whole-object replace can never touch it; nil = nothing ever recorded.
	Permissions *UserPermissions `json:"permissions,omitempty" bson:"permissions,omitempty"`
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/model`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/permission.go pkg/model/user.go pkg/model/permission_test.go pkg/model/model_test.go
git commit -m "feat(model): materialized permission state on the user document"
```

---

### Task 2: pkg/model — UserPermissionsUpdated federation event

**Files:**
- Modify: `pkg/model/event.go` (const block ~L151-168; struct after `UserSettingsUpdated` ~L176)
- Modify: `pkg/model/model_test.go`

**Interfaces:**
- Produces: `InboxUserPermissionsUpdated InboxEventType = "user_permissions_updated"`; `UserPermissionsUpdated{Permission PermissionKey; Accounts []string; State PermissionState; Timestamp int64}`.

- [ ] **Step 1: Write the failing test** — add a `UserPermissionsUpdated` entry to `model_test.go`'s `roundTrip` table (populated `Accounts` of 2, full `State`, non-zero `Timestamp`).

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=pkg/model` — Expected: FAIL `undefined: UserPermissionsUpdated`.

- [ ] **Step 3: Implement** — in `pkg/model/event.go`, add to the `InboxEventType` const block:

```go
	InboxUserPermissionsUpdated InboxEventType = "user_permissions_updated"
```

and after `UserSettingsUpdated`:

```go
// UserPermissionsUpdated is the cross-site inbox event admin-service publishes after a
// permission grant/revoke batch. One event carries one chunk of the batch: every account
// in Accounts receives the same State. Receivers apply it under the per-key watermark
// guard (State.UpdatedAt), so duplicated or reordered delivery is safe.
type UserPermissionsUpdated struct {
	Permission PermissionKey   `json:"permission" bson:"permission"`
	Accounts   []string        `json:"accounts"   bson:"accounts"` // ≤ fanoutChunkSize per event
	State      PermissionState `json:"state"      bson:"state"`
	Timestamp  int64           `json:"timestamp"  bson:"timestamp"`
}
```

- [ ] **Step 4: Run to verify pass** — `make test SERVICE=pkg/model` → PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/event.go pkg/model/model_test.go
git commit -m "feat(model): user_permissions_updated inbox event"
```

---

### Task 3: admin-service — write path: RecordPermissionChange + company-wide subjects

**Files:**
- Modify: `admin-service/store.go` (AdminStore interface)
- Modify: `admin-service/store_mongo.go`
- Modify: `admin-service/permissions.go` (`createPermissions`)
- Modify: `admin-service/permissions_test.go`, `admin-service/integration_test.go`
- Regenerate: `admin-service/mock_store_test.go` (`make generate SERVICE=admin-service`)

**Interfaces:**
- Consumes: Task 1 (`PermissionState`, `PermissionFieldName`), existing `withTransaction`, `parseWindow`, `dedupPreserveOrder`.
- Produces (interface, replaces `InsertPermissionGrants`):
  - `RecordPermissionChange(ctx context.Context, grants []*model.PermissionGrant, state model.PermissionState) error`
  - `FindAccountStates(ctx context.Context, accounts []string) (map[string]bool, error)` — **siteID parameter removed**.
- The validation cap check is removed (non-empty only). `model.MaxSubjects` itself still exists until Task 10 — after this task nothing references it in admin-service.

- [ ] **Step 1: Write/adjust the failing tests** in `permissions_test.go`:
  - Replace mock expectations of `InsertPermissionGrants(...)` with `RecordPermissionChange(gomock.Any(), gomock.Any(), gomock.Any())` capturing both args; assert on a **grant** request: `state.Granted == true`, `state.EffectiveFrom/ExpiresAt` equal the instants `parseWindow` produces for the request dates, `state.UpdatedAt` equals the grants' shared `RecordedAt`; on a **revoke**: `Granted == false`, both bounds nil.
  - Replace `FindAccountStates(gomock.Any(), siteID, accounts)` expectations with the two-arg form.
  - Delete the "subject count over cap" subtest; keep/adjust the empty-list subtest (still `invalid_subject_count`).
  - Add a large-batch acceptance subtest: 10,001 unique accounts pass validation (mock `FindAccountStates` returns all-active; assert `RecordPermissionChange` receives 10,001 grants).
- In `integration_test.go` (existing admin-service integration harness, `//go:build integration`):
  - `TestRecordPermissionChange_TransactionAppliesLedgerAndState`: seed two user docs → call with 2 grants + granted state → both ledger rows exist AND both user docs carry `permissions.externalImageView` equal to the state.
  - `TestRecordPermissionChange_GuardKeepsNewerState`: pre-set a user's `permissions.externalImageView.updatedAt` to `state.UpdatedAt + 1s` → call → ledger row inserted, user doc **unchanged**; with an equal timestamp → state **is** applied ($lte).
  - `TestRecordPermissionChange_MissingUserIsNoop`: grant for an account with no user doc → ledger row inserted, no user doc created.

- [ ] **Step 2: Run to verify failure** — `make generate SERVICE=admin-service` will fail until the interface changes; expected compile failures referencing `RecordPermissionChange`.

- [ ] **Step 3: Implement.**
  - `store.go`: replace the `InsertPermissionGrants` method (keep its doc comment style) and change `FindAccountStates`:

```go
	// RecordPermissionChange appends the batch to the ledger and applies the derived
	// state to the subjects' user documents under the per-key watermark guard — one
	// transaction, all-or-nothing. Subject accounts are derived from the grants.
	RecordPermissionChange(ctx context.Context, grants []*model.PermissionGrant, state model.PermissionState) error
	// FindAccountStates reports account -> IsActive company-wide (the local users
	// collection covers every site's users; subjects may be homed anywhere).
	FindAccountStates(ctx context.Context, accounts []string) (map[string]bool, error)
```

  - `store_mongo.go`: replace `InsertPermissionGrants` with:

```go
func (s *storeMongo) RecordPermissionChange(ctx context.Context, grants []*model.PermissionGrant, state model.PermissionState) error {
	if len(grants) == 0 {
		return nil
	}
	field, ok := model.PermissionFieldName(grants[0].Permission)
	if !ok {
		return fmt.Errorf("record permission change: unknown permission %q", grants[0].Permission)
	}
	docs := make([]any, len(grants))
	accounts := make([]string, len(grants))
	for i, g := range grants {
		docs[i] = g
		accounts[i] = g.SubjectAccount
	}
	path := "permissions." + field
	filter := bson.M{
		"account": bson.M{"$in": accounts},
		"$or": bson.A{
			bson.M{path + ".updatedAt": bson.M{"$exists": false}},
			bson.M{path + ".updatedAt": bson.M{"$lte": state.UpdatedAt}},
		},
	}
	return s.withTransaction(ctx, func(ctx context.Context) error {
		if _, err := s.permGrants.InsertMany(ctx, docs); err != nil {
			return fmt.Errorf("insert permission grants: %w", err)
		}
		// MatchedCount may be < len(accounts): a newer stored state (guard) or a user
		// doc missing locally is a no-op, not an error — same rule as the inbox apply.
		if _, err := s.users.UpdateMany(ctx, filter, bson.M{"$set": bson.M{path: state}}); err != nil {
			return fmt.Errorf("apply user permissions: %w", err)
		}
		return nil
	})
}
```

  - `FindAccountStates`: drop the `siteID` parameter and remove `"siteId"` from its filter (now `bson.M{"account": bson.M{"$in": accounts}}`); projection unchanged.
  - `permissions.go` `createPermissions`:
    - Validation: replace the count check with non-empty only (`invalid_subject_count` when empty); delete the `> model.MaxSubjects` branch.
    - Call `h.store.FindAccountStates(ctx, lookupAccounts)` (no site arg).
    - After building `grants`, derive the state and swap the store call:

```go
	state := model.PermissionState{Granted: *req.Granted, UpdatedAt: now}
	if *req.Granted {
		state.EffectiveFrom = &from
		state.ExpiresAt = &until
	}
	if err := h.store.RecordPermissionChange(ctx, grants, state); err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("record permission change: %w", err))
		return
	}
```

    (keep populating `grant.SiteID` for now — removed with the field in Task 10.)
  - Run `make generate SERVICE=admin-service`.

- [ ] **Step 4: Run to verify pass**

Run: `make test SERVICE=admin-service`, then `make test-integration SERVICE=admin-service`.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add admin-service/
git commit -m "feat(admin-service): materialize permission state in the write transaction"
```

---

### Task 4: admin-service — read path: currentlyGranted from state; ledger browse without site

**Files:**
- Modify: `admin-service/store.go`, `admin-service/store_mongo.go` (also `EnsureIndexes`)
- Modify: `admin-service/permissions.go` (`listPermissions`)
- Modify: `admin-service/permissions_test.go`, `admin-service/integration_test.go`
- Regenerate mocks.

**Interfaces:**
- Produces: `GetUserPermissions(ctx context.Context, account string) (*model.UserPermissions, error)` (nil, nil when no doc).
- Changes: `ListPermissionGrants(ctx context.Context, subjectAccount string, permission model.PermissionKey, page, limit int) ([]model.PermissionGrant, int64, error)` — **siteID parameter removed**.
- Deletes: `GetLatestPermissionGrant` (interface + impl + mocks + tests).
- Ledger indexes become `{permission:1, subjectAccount:1, recordedAt:-1, _id:-1}` and `{recordedAt:-1, _id:-1}`.

- [ ] **Step 1: Failing tests.** In `permissions_test.go`: replace `GetLatestPermissionGrant` expectations with `GetUserPermissions(gomock.Any(), "alice")` returning (a) a currently-valid granted state → `currentlyGranted: true`; (b) `nil, nil` → `false`; (c) an expired state → `false`. Assert `currentlyGranted` still appears only when both filters are set. Adjust `ListPermissionGrants` mock signatures.

- [ ] **Step 2: Verify failure** — `make test SERVICE=admin-service` → compile errors.

- [ ] **Step 3: Implement.**
  - `store_mongo.go`:

```go
// GetUserPermissions returns the account's materialized permission snapshot; (nil, nil)
// when the user or snapshot does not exist. Site-unfiltered: subjects may be homed at
// any site.
func (s *storeMongo) GetUserPermissions(ctx context.Context, account string) (*model.UserPermissions, error) {
	var doc struct {
		Permissions *model.UserPermissions `bson:"permissions"`
	}
	err := s.users.FindOne(ctx, bson.M{"account": account},
		options.FindOne().SetProjection(bson.M{"permissions": 1})).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find user permissions: %w", err)
	}
	return doc.Permissions, nil
}
```

    (match the surrounding file's option-builder style exactly — mirror how `FindAccountStates` builds its projection.)
  - `ListPermissionGrants`: remove the `siteID` param and the `"siteId"` filter entry.
  - `EnsureIndexes`: change the two permission index key docs to `{permission:1, subjectAccount:1, recordedAt:-1, _id:-1}` and `{recordedAt:-1, _id:-1}`.
  - Delete `GetLatestPermissionGrant` from interface + impl.
  - `permissions.go` `listPermissions`: replace the currentlyGranted block:

```go
	if permission != "" && subjectAccount != "" {
		perms, err := h.store.GetUserPermissions(ctx, subjectAccount)
		if err != nil {
			errhttp.Write(ctx, c, fmt.Errorf("get user permissions: %w", err))
			return
		}
		granted := perms.Evaluated(time.Now().UTC())[model.PermissionKey(permission)]
		resp.CurrentlyGranted = &granted
	}
```

  - `make generate SERVICE=admin-service`.

- [ ] **Step 4: Verify pass** — `make test SERVICE=admin-service` and `make test-integration SERVICE=admin-service` → PASS.

- [ ] **Step 5: Commit**

```bash
git add admin-service/
git commit -m "feat(admin-service): currentlyGranted reads the materialized state"
```

---

### Task 5: admin-service — chunked fanout, syncFailures, body-limit removal

**Files:**
- Modify: `admin-service/config.go` (Config struct)
- Modify: `admin-service/main.go` (JetStream + handler wiring; drop `bodyLimit` use at L73)
- Modify: `admin-service/handler.go` (Handler struct ~L24-35 + `newHandler` signature)
- Modify: `admin-service/permissions.go` (fanout helper + response field)
- Modify: `admin-service/middleware.go`, `admin-service/middleware_test.go` (delete `maxRequestBodyBytes` + `bodyLimit` + their tests)
- Modify: `admin-service/permissions_test.go`; touch other `newHandler` call sites in tests (grep `newHandler(`).

**Interfaces:**
- Consumes: `model.UserPermissionsUpdated`, `model.InboxEvent`, `subject.InboxExternal(dest, eventType)`.
- Produces: `Handler.publishInbox func(ctx context.Context, subj string, data []byte) error` (nil in tests that don't fan out → fanout no-ops); `Config.AllSiteIDs []string`; `createPermissionsResponse.SyncFailures []string \`json:"syncFailures,omitempty"\``; unexported `fanoutChunkSize = 5000`; `(h *Handler) publishPermissionFanout(ctx, permission, accounts, state) []string` — reused by Task 6.

- [ ] **Step 1: Failing tests** in `permissions_test.go` (inject a capture func as `publishInbox`; configure `cfg.AllSiteIDs = []string{"site-a", "site-b", "site-c"}` with `cfg.SiteID = "site-a"`):
  - happy path, 3 subjects: exactly 2 publishes (site-b, site-c), subjects `chat.inbox.site-b.external.user_permissions_updated` etc.; unmarshal `InboxEvent` → `Type`, `SiteID: "site-a"`, `DestSiteID`, payload decodes to `UserPermissionsUpdated` with all 3 accounts + the derived state; `syncFailures` absent from the JSON response.
  - chunking: 5,001 accounts → 2 events per dest (first 5,000 + last 1), union of accounts complete, same `State`/`Timestamp` on both.
  - failure aggregation: capture func errors for dest `site-b` only → response `syncFailures == ["site-b"]`, site-c still receives all its chunks, status still 201.
  - blank/self skipped: `AllSiteIDs` containing `""` and `"site-a"` produce no publishes.
  - store failure → zero publishes.
- Body-limit: delete `middleware_test.go`'s bodyLimit tests.

- [ ] **Step 2: Verify failure** — `make test SERVICE=admin-service` → compile/assert failures.

- [ ] **Step 3: Implement.**
  - `config.go`:

```go
	// AllSiteIDs lists every site in the federation (including this one); empty means
	// no cross-site fanout — correct for single-site dev.
	AllSiteIDs []string `env:"ALL_SITE_IDS" envSeparator:"," envDefault:""`
```

  - `handler.go`: add field + parameter:

```go
	publishInbox func(ctx context.Context, subj string, data []byte) error
```

    `newHandler(store AdminStore, sessions session.Store, cfg Config, rpc roomRequester, publishInbox func(ctx context.Context, subj string, data []byte) error) *Handler` — assign it; update every call site (`main.go` + tests; tests that don't exercise fanout pass `nil`).
  - `main.go`: after the NATS connect (L59-62):

```go
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	publishInbox := func(ctx context.Context, subj string, data []byte) error {
		_, err := js.Publish(ctx, subj, data)
		return err
	}
	h := newHandler(st, sessStore, cfg, nc, publishInbox)
```

    and delete the `r.Use(bodyLimit(maxRequestBodyBytes))` line.
  - `middleware.go`: delete `maxRequestBodyBytes` and `bodyLimit`.
  - `permissions.go`: add the helper + wire it after `RecordPermissionChange` succeeds and add `SyncFailures` to the response struct:

```go
// fanoutChunkSize bounds one UserPermissionsUpdated event's Accounts so a worst-case
// event stays ~320KB — 3× margin under NATS's default 1MB max_payload.
const fanoutChunkSize = 5000

// publishPermissionFanout publishes the batch to every remote site's INBOX, one event
// per site per chunk of accounts. Failures are logged and aggregated per destination and
// the loop always continues — one dead peer must not block the others. Returns the
// destinations whose publish was not acknowledged (nil when all landed).
func (h *Handler) publishPermissionFanout(ctx context.Context, permission model.PermissionKey, accounts []string, state model.PermissionState) []string {
	if h.publishInbox == nil || len(accounts) == 0 {
		return nil
	}
	now := time.Now().UTC().UnixMilli()
	// Marshal each chunk's payload once; only the envelope differs per destination.
	var chunks [][]byte
	for start := 0; start < len(accounts); start += fanoutChunkSize {
		end := min(start+fanoutChunkSize, len(accounts))
		payload, err := json.Marshal(model.UserPermissionsUpdated{
			Permission: permission,
			Accounts:   accounts[start:end],
			State:      state,
			Timestamp:  now,
		})
		if err != nil {
			// Cannot serialize the event at all: every destination is out of sync.
			slog.ErrorContext(ctx, "marshal user permissions event", "error", err)
			return remoteSites(h.cfg.AllSiteIDs, h.cfg.SiteID)
		}
		chunks = append(chunks, payload)
	}
	var failures []string
	for _, dest := range h.cfg.AllSiteIDs {
		if dest == "" || dest == h.cfg.SiteID {
			continue
		}
		failed := false
		for _, payload := range chunks {
			evt := model.InboxEvent{
				Type:       model.InboxUserPermissionsUpdated,
				SiteID:     h.cfg.SiteID,
				DestSiteID: dest,
				Payload:    payload,
				Timestamp:  now,
			}
			data, err := json.Marshal(evt)
			if err != nil {
				slog.ErrorContext(ctx, "marshal permissions inbox envelope", "dest", dest, "error", err)
				failed = true
				continue
			}
			if err := h.publishInbox(ctx, subject.InboxExternal(dest, model.InboxUserPermissionsUpdated), data); err != nil {
				// Not self-healing like status/settings: surface it, don't swallow it.
				slog.ErrorContext(ctx, "publish permissions inbox event", "dest", dest, "error", err)
				failed = true
			}
		}
		if failed {
			failures = append(failures, dest)
		}
	}
	return failures
}

// remoteSites filters allSiteIDs down to real remote destinations.
func remoteSites(allSiteIDs []string, self string) []string {
	var out []string
	for _, dest := range allSiteIDs {
		if dest != "" && dest != self {
			out = append(out, dest)
		}
	}
	return out
}
```

    In `createPermissions`, after the store call and before the audit write:

```go
	syncFailures := h.publishPermissionFanout(ctx, key, subjects, state)
```

    and include it in the 201 response (`SyncFailures: syncFailures`).

- [ ] **Step 4: Verify pass** — `make test SERVICE=admin-service` → PASS; `make lint` → clean.

- [ ] **Step 5: Commit**

```bash
git add admin-service/
git commit -m "feat(admin-service): chunked cross-site permission fanout with syncFailures"
```

---

### Task 6: admin-service — resync endpoint

**Files:**
- Modify: `admin-service/routes.go` (add `admin.POST("/permissions/resync", h.resyncPermissions)`)
- Modify: `admin-service/permissions.go`
- Modify: `admin-service/store.go`, `admin-service/store_mongo.go`
- Modify: `admin-service/permissions_test.go`
- Regenerate mocks.

**Interfaces:**
- Consumes: `publishPermissionFanout` (Task 5), `model.UserPermissions.State` (Task 1), `dedupPreserveOrder`.
- Produces: `GetUserPermissionsForAccounts(ctx context.Context, accounts []string) (map[string]*model.UserPermissions, error)` (missing accounts simply absent from the map).

- [ ] **Step 1: Failing tests** in `permissions_test.go`:
  - unknown key → 400 `unknown_permission`; empty accounts → 400 `invalid_subject_count`.
  - two accounts sharing one stored state → **one** fanout group: per remote dest exactly one event whose `Accounts` contains both; `State` equals the stored state (including its stored `UpdatedAt` — NOT a fresh timestamp).
  - two accounts with different stored states → two groups → two events per dest with the matching account/state pairing.
  - an account absent from the store map, and one present with `Permissions == nil` → both skipped, no event mentions them.
  - store mock: assert **no** calls to `RecordPermissionChange` / `AppendAuditMany` (mock without expectations fails on any call).
  - a failing dest → `{"syncFailures":["site-b"]}` with 200.

- [ ] **Step 2: Verify failure** — `make test SERVICE=admin-service`.

- [ ] **Step 3: Implement.**
  - `store.go` + `store_mongo.go`:

```go
// GetUserPermissionsForAccounts returns the materialized snapshots for the given
// accounts in one read; accounts with no user doc are absent from the map.
func (s *storeMongo) GetUserPermissionsForAccounts(ctx context.Context, accounts []string) (map[string]*model.UserPermissions, error) {
	cur, err := s.users.Find(ctx, bson.M{"account": bson.M{"$in": accounts}},
		options.Find().SetProjection(bson.M{"account": 1, "permissions": 1}))
	if err != nil {
		return nil, fmt.Errorf("find user permissions: %w", err)
	}
	defer cur.Close(ctx)
	out := make(map[string]*model.UserPermissions, len(accounts))
	for cur.Next(ctx) {
		var doc struct {
			Account     string                 `bson:"account"`
			Permissions *model.UserPermissions `bson:"permissions"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode user permissions: %w", err)
		}
		out[doc.Account] = doc.Permissions
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("iterate user permissions: %w", err)
	}
	return out, nil
}
```

    (mirror the file's existing cursor-iteration style.)
  - `permissions.go`:

```go
type resyncPermissionsRequest struct {
	Permission string   `json:"permission"`
	Accounts   []string `json:"accounts"`
}

type resyncPermissionsResponse struct {
	SyncFailures []string `json:"syncFailures,omitempty"`
}

// stateGroupKey identifies one distinct stored PermissionState by value. Pointers make
// PermissionState itself non-comparable, so the key flattens the bounds to UnixMilli
// (0 = absent) — exact-instant equality is what "same state" means here.
type stateGroupKey struct {
	granted             bool
	from, until, updated int64
}

func keyOfState(s model.PermissionState) stateGroupKey {
	k := stateGroupKey{granted: s.Granted, updated: s.UpdatedAt.UnixMilli()}
	if s.EffectiveFrom != nil {
		k.from = s.EffectiveFrom.UnixMilli()
	}
	if s.ExpiresAt != nil {
		k.until = s.ExpiresAt.UnixMilli()
	}
	return k
}

// resyncPermissions re-delivers the current materialized state for the given accounts —
// re-delivery, not re-recording: it writes nothing (no ledger, no audit, no user docs)
// and is idempotent (delivered states keep their stored watermarks, so remote guards
// no-op anything already applied).
func (h *Handler) resyncPermissions(c *gin.Context) {
	ctx := c.Request.Context()
	var req resyncPermissionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid request body", errcode.WithReason(errcode.AuthMissingFields)))
		return
	}
	key := model.PermissionKey(req.Permission)
	if !model.KnownPermission(key) {
		errhttp.Write(ctx, c, errcode.BadRequest("unknown permission", errcode.WithReason(errcode.PermissionUnknownKey)))
		return
	}
	accounts, _ := dedupPreserveOrder(req.Accounts)
	if len(accounts) == 0 {
		errhttp.Write(ctx, c, errcode.BadRequest("accounts must be non-empty", errcode.WithReason(errcode.PermissionInvalidSubjects)))
		return
	}
	perms, err := h.store.GetUserPermissionsForAccounts(ctx, accounts)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("load user permissions: %w", err))
		return
	}
	groups := make(map[stateGroupKey][]string)
	states := make(map[stateGroupKey]model.PermissionState)
	for _, account := range accounts {
		st := perms[account].State(key)
		if st == nil {
			continue // no doc or nothing recorded — nothing to sync
		}
		k := keyOfState(*st)
		groups[k] = append(groups[k], account)
		states[k] = *st
	}
	seen := make(map[string]struct{})
	var failures []string
	for k, groupAccounts := range groups {
		for _, dest := range h.publishPermissionFanout(ctx, key, groupAccounts, states[k]) {
			if _, dup := seen[dest]; !dup {
				seen[dest] = struct{}{}
				failures = append(failures, dest)
			}
		}
	}
	c.JSON(http.StatusOK, resyncPermissionsResponse{SyncFailures: failures})
}
```

    (Match the existing handlers' bind-error reason — reuse whatever `createPermissions` uses for a malformed body.)
  - `routes.go`: register the route in the `requireAdmin` group next to the other two.
  - `make generate SERVICE=admin-service`.

- [ ] **Step 4: Verify pass** — `make test SERVICE=admin-service` → PASS.

- [ ] **Step 5: Commit**

```bash
git add admin-service/
git commit -m "feat(admin-service): idempotent permission resync endpoint"
```

---

### Task 7: inbox-worker — apply cross-site permission events

**Files:**
- Modify: `inbox-worker/handler.go` (event switch ~L113-152; `InboxStore` interface ~L79; new handler beside `handleUserSettingsUpdated` ~L415)
- Modify: `inbox-worker/main.go` (guarded store method beside `UpdateUserSettings` ~L203)
- Modify: `inbox-worker/handler_test.go`; inbox-worker integration test file
- Regenerate: inbox-worker mocks (`make generate SERVICE=inbox-worker`)

**Interfaces:**
- Consumes: `model.UserPermissionsUpdated`, `model.PermissionFieldName`, `model.InboxUserPermissionsUpdated`.
- Produces (InboxStore): `ApplyUserPermissions(ctx context.Context, permission model.PermissionKey, accounts []string, state model.PermissionState) error`.

- [ ] **Step 1: Failing tests.**
  - `handler_test.go` (mirror the `handleUserSettingsUpdated` test shapes): valid event → store called with decoded `permission/accounts/state`; malformed payload → error returned; **unknown permission key → returns nil (Ack) and the store is NOT called**.
  - Integration (same package, `//go:build integration`, reuse the worker's existing Mongo harness): seed user docs, then assert on `ApplyUserPermissions`:
    - fresh apply sets `permissions.externalImageView` == state on every listed account;
    - a stored `updatedAt` **newer** than the event's → doc unchanged;
    - an **equal** `updatedAt` → applied ($lte);
    - an account with no user doc → no doc created, others still applied;
    - per-key independence: pre-write a sibling field inside `permissions` (raw `bson.M{"permissions.somethingElse": ...}` seed) → apply → sibling untouched.

- [ ] **Step 2: Verify failure** — `make test SERVICE=inbox-worker` → compile errors (`ApplyUserPermissions` undefined).

- [ ] **Step 3: Implement.**
  - `handler.go` switch case:

```go
	case model.InboxUserPermissionsUpdated:
		return h.handleUserPermissionsUpdated(ctx, &evt)
```

  - handler (place next to `handleUserSettingsUpdated`):

```go
// handleUserPermissionsUpdated applies one chunk of an admin permission batch. A missing
// user doc is a silent no-op (store-level guard), matching the other user events.
func (h *Handler) handleUserPermissionsUpdated(ctx context.Context, evt *model.InboxEvent) error {
	var e model.UserPermissionsUpdated
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal user_permissions_updated payload: %w", err)
	}
	if _, ok := model.PermissionFieldName(e.Permission); !ok {
		// A future permission key reaching a not-yet-upgraded site: retrying cannot
		// succeed and must not poison the consumer — warn and Ack.
		slog.WarnContext(ctx, "unknown permission key in user_permissions_updated", "permission", string(e.Permission))
		return nil
	}
	if err := h.store.ApplyUserPermissions(ctx, e.Permission, e.Accounts, e.State); err != nil {
		return fmt.Errorf("apply user permissions: %w", err)
	}
	return nil
}
```

  - `InboxStore` interface entry (doc-comment style matching `UpdateUserSettings`'s):

```go
	// ApplyUserPermissions applies one permission state to the accounts under the
	// per-key watermark guard. A missing user (no doc on this site) is a silent no-op.
	ApplyUserPermissions(ctx context.Context, permission model.PermissionKey, accounts []string, state model.PermissionState) error
```

  - `main.go` store method (beside `UpdateUserSettings`):

```go
// ApplyUserPermissions applies state to every listed account under the per-key watermark
// guard. $lte, not $lt: two writes can share a millisecond, and the apply is an
// idempotent whole-state replace, so a same-ms tie resolves to last-delivered. No
// upsert — a missing user doc is a silent no-op; MatchedCount < len(accounts) is normal.
func (s *mongoInboxStore) ApplyUserPermissions(ctx context.Context, permission model.PermissionKey, accounts []string, state model.PermissionState) error {
	if len(accounts) == 0 {
		return nil
	}
	field, ok := model.PermissionFieldName(permission)
	if !ok {
		return fmt.Errorf("apply user permissions: unknown permission %q", permission)
	}
	path := "permissions." + field
	filter := bson.M{
		"account": bson.M{"$in": accounts},
		"$or": bson.A{
			bson.M{path + ".updatedAt": bson.M{"$exists": false}},
			bson.M{path + ".updatedAt": bson.M{"$lte": state.UpdatedAt}},
		},
	}
	if _, err := s.userCol.UpdateMany(ctx, filter, bson.M{"$set": bson.M{path: state}}); err != nil {
		return fmt.Errorf("update user permissions: %w", err)
	}
	return nil
}
```

  - `make generate SERVICE=inbox-worker`.

- [ ] **Step 4: Verify pass** — `make test SERVICE=inbox-worker` and `make test-integration SERVICE=inbox-worker` → PASS.

- [ ] **Step 5: Commit**

```bash
git add inbox-worker/
git commit -m "feat(inbox-worker): apply cross-site user permission events"
```

---

### Task 8: user-service — settings.get returns evaluated permissions

**Files:**
- Modify: `user-service/models/settings.go`
- Modify: `user-service/service/settings.go` (`GetSettings` ~L24-38)
- Modify: `user-service/service/service.go` (`UserRepository` interface entry ~L35)
- Modify: `user-service/mongorepo/users.go` (`GetUserSettings` ~L106-110)
- Modify: settings-related tests in `user-service/service/` and `user-service/mongorepo/`
- Regenerate: `make generate SERVICE=user-service`

**Interfaces:**
- Consumes: `model.UserPermissions.Evaluated` (Task 1).
- Produces: `models.SettingsGetResponse{model.UserSettings; Permissions map[model.PermissionKey]bool}`; repo `GetUserSettingsAndPermissions(ctx context.Context, account string) (*model.UserSettings, *model.UserPermissions, error)` (replaces `GetUserSettings`; identical error/ErrNoDocuments semantics).
- Unchanged: `SetSettings`, `UpdateUserSettings`, both settings fanouts, the `settings.update` client event.

- [ ] **Step 1: Failing tests** (service layer, follow the existing GetSettings test shapes):
  - repo returns `(nil, nil, nil)` → response has empty settings and `permissions == {"external.image.view": false}`;
  - granted-in-window snapshot → `true`;
  - expired / not-yet-effective / revoked snapshots → `false`;
  - stored settings fields pass through unchanged beside the new key.
  - mongorepo test: seed a user doc carrying both `settings` and `permissions` → one call returns both; a user with neither → `(nil, nil, nil)`-equivalent per existing semantics.

- [ ] **Step 2: Verify failure** — `make test SERVICE=user-service` → compile errors.

- [ ] **Step 3: Implement.**
  - `models/settings.go`:

```go
// SettingsGetResponse is the settings.get reply: the user's settings plus the evaluated
// admin-managed permissions. The permissions field is read-only on this surface — it is
// not part of UserSettings, so settings.set structurally cannot touch it.
type SettingsGetResponse struct {
	model.UserSettings
	Permissions map[model.PermissionKey]bool `json:"permissions"`
}
```

  - `mongorepo/users.go` — rename/extend (preserve the current function's error wrapping and not-found behavior exactly; only the projection and return values change):

```go
// GetUserSettingsAndPermissions returns the settings sub-document and the admin-managed
// permission snapshot in one read; either may be nil when never written.
func (r *UserRepo) GetUserSettingsAndPermissions(ctx context.Context, account string) (*model.UserSettings, *model.UserPermissions, error) {
	u, err := r.users.FindOne(ctx, activeUserFilter(account),
		mongoutil.WithProjection(bson.M{"_id": 0, "settings": 1, "permissions": 1}))
	// keep the existing GetUserSettings error handling verbatim (incl. its
	// ErrNoDocuments treatment), returning (nil, nil, err-or-nil) accordingly
	if err != nil {
		return nil, nil, err // adjust to match the removed function's exact wrapping
	}
	return u.Settings, u.Permissions, nil
}
```

  - `service/service.go` `UserRepository`: replace the `GetUserSettings` method entry with the new signature.
  - `service/settings.go`:

```go
// GetSettings returns the stored settings verbatim plus the evaluated admin-managed
// permissions — every known key, always present; a user with no snapshot gets false.
func (s *UserService) GetSettings(c *natsrouter.Context) (*models.SettingsGetResponse, error) {
	account := c.Param("account")
	settings, perms, err := s.users.GetUserSettingsAndPermissions(c, account)
	if err != nil {
		return nil, fmt.Errorf("get settings: %w", err)
	}
	if settings == nil {
		settings = &model.UserSettings{}
	}
	return &models.SettingsGetResponse{
		UserSettings: *settings,
		Permissions:  perms.Evaluated(time.Now().UTC()),
	}, nil
}
```

    (preserve whatever nil/error handling the current implementation applies around the repo call — read it first and keep its semantics; only the second return value and response type are new.)
  - `make generate SERVICE=user-service`.

- [ ] **Step 4: Verify pass** — `make test SERVICE=user-service` → PASS; `make test-integration SERVICE=user-service` if the mongorepo has integration coverage for settings.

- [ ] **Step 5: Commit**

```bash
git add user-service/
git commit -m "feat(user-service): settings.get returns evaluated permissions"
```

---

### Task 9: user-service + pkg/subject — remove the permission.get RPC stack

**Files:**
- Delete: `user-service/service/permission.go`, `user-service/service/permission_test.go`, `user-service/models/permission.go`, `user-service/models/permission_test.go`, `user-service/mongorepo/permissions.go`, `user-service/mongorepo/permissions_test.go`
- Modify: `user-service/service/service.go` — remove the `PermissionRepository` interface, the `permissions` field, the `service.New` parameter, the `,PermissionRepository` in the `//go:generate mockgen` directive, and the `natsrouter.Register(r, subject.UserPermissionGetPattern(...), s.GetPermission)` line (~L196)
- Modify: `user-service/main.go` — remove `permissionRepo := mongorepo.NewPermissionRepo(db)` (~L96) and the `service.New` argument
- Modify: every test constructing `service.New(...)` (grep `service.New(` under `user-service/`) — drop the argument
- Modify: `pkg/subject/subject.go` — delete `UserPermissionGet` + `UserPermissionGetPattern` (~L1364-1375) and remove `"permission"` from `ParseUserSubject`'s area whitelist/doc (~L1563-1580); `pkg/subject/subject_test.go` — delete their tests
- Regenerate: `make generate SERVICE=user-service`

**Interfaces:**
- Consumes: nothing new. Produces: nothing — pure removal. After this task `grep -rn "UserPermissionGet\|PermissionRepository\|PermissionGetRequest" --include="*.go" .` must return zero hits.

- [ ] **Step 1: Delete the six files, edit the wiring, regenerate mocks** (removal task — the "failing test" is the compiler: after deleting `service/permission.go` the build fails until every reference is gone).
- [ ] **Step 2: Run the verification greps** above — zero hits — then `make lint` (whole-repo compile) and `make test SERVICE=user-service && make test SERVICE=pkg/subject`.
Expected: PASS.
- [ ] **Step 3: Commit**

```bash
git add -A user-service/ pkg/subject/
git commit -m "refactor(user-service): drop the permission.get RPC (superseded by settings.get)"
```

---

### Task 10: pkg/model cleanup — delete EvaluateGrant, MaxSubjects, PermissionGrant.SiteID

**Files:**
- Modify: `pkg/model/permission.go` — delete `EvaluateGrant`, delete `MaxSubjects` (keep `MaxReasonRunes`), delete the `SiteID` field from `PermissionGrant`
- Modify: `pkg/model/permission_test.go` — delete the `EvaluateGrant` tests
- Modify: `pkg/model/model_test.go` — drop `SiteID` from the `PermissionGrant` round-trip fixture
- Modify: `admin-service/permissions.go` — remove the `SiteID: h.cfg.SiteID` assignment when building grants
- Modify: any admin-service test asserting `SiteID` on grants or referencing `MaxSubjects`

**Interfaces:** pure deletion. Gate: `grep -rn "EvaluateGrant\|MaxSubjects" --include="*.go" .` → zero hits; `grep -rn "SiteID" pkg/model/permission.go admin-service/permissions.go` → zero hits.

- [ ] **Step 1: Delete, run the greps, fix stragglers.**
- [ ] **Step 2: Full verification** — `make lint && make test` (whole repo) and `make test-integration SERVICE=admin-service`.
Expected: PASS.
- [ ] **Step 3: Commit**

```bash
git add -A pkg/model/ admin-service/
git commit -m "refactor(model): drop EvaluateGrant, MaxSubjects, and the ledger SiteID column"
```

---

### Task 11: admin-frontend — api client, paste-list mode, count-visible submit

**Files:**
- Modify: `admin-frontend/src/api/admin/index.ts`
- Modify: `admin-frontend/src/api/admin/admin.test.ts`
- Create: `admin-frontend/src/components/PermissionsView/CreatePermissionsDialog/parseAccounts.js`
- Create: `admin-frontend/src/components/PermissionsView/CreatePermissionsDialog/parseAccounts.test.js`
- Modify: `admin-frontend/src/components/PermissionsView/CreatePermissionsDialog/CreatePermissionsDialog.jsx` + `style.css`
- Modify: `admin-frontend/src/components/PermissionsView/CreatePermissionsDialog/CreatePermissionsDialog.test.jsx`
- Modify: `admin-frontend/src/api/index.ts` if it re-exports the admin client symbols (mirror how `createPermissions` is exported).

**Interfaces:**
- Produces (api client):

```ts
export interface ResyncPermissionsRequest { permission: string; accounts: string[] }
export interface ResyncPermissionsResponse { syncFailures?: string[] }
export async function resyncPermissions(authToken: string, body: ResyncPermissionsRequest): Promise<ResyncPermissionsResponse>
```

  plus `CreatePermissionsResponse` gains `syncFailures?: string[]`.
- Produces (parse helper, used by Task 12's strip action):

```js
export function parsePastedAccounts(text) // -> { accounts: string[], duplicates: number }
```

- [ ] **Step 1: Failing tests.**
  - `parseAccounts.test.js`: splits on spaces/newlines/commas/semicolons/tabs; trims; drops empties; dedups preserving first occurrence and counts duplicates; empty string → `{accounts: [], duplicates: 0}`.
  - `admin.test.ts`: `resyncPermissions` POSTs `/v1/admin/permissions/resync` with the body and Bearer header (follow the existing `createPermissions` test).
  - Dialog tests: toggling to paste mode shows the textarea; typing `"alice bob,alice\ncarol"` shows "3 accounts (1 duplicate removed)"; submit payload uses the parsed list; toggle badges show `Pick (0) / Paste (3)`; empty paste disables submit; the submit button label carries the count ("Grant permission to 3 accounts"); the old max-200 error UI is gone (a 250-account paste is accepted client-side).

- [ ] **Step 2: Verify failure** — run the frontend test suite the way the existing tests document (admin-frontend's package scripts; the repo's Vitest setup).

- [ ] **Step 3: Implement.**
  - `parseAccounts.js`:

```js
// Splits a pasted account list on whitespace/commas/semicolons, deduplicating while
// preserving first-occurrence order. Accounts are kept verbatim (no case folding),
// matching backend semantics.
export function parsePastedAccounts(text) {
  const seen = new Set()
  const accounts = []
  let duplicates = 0
  for (const token of text.split(/[\s,;]+/)) {
    if (!token) continue
    if (seen.has(token)) {
      duplicates += 1
      continue
    }
    seen.add(token)
    accounts.push(token)
  }
  return { accounts, duplicates }
}
```

  - Dialog changes (`CreatePermissionsDialog.jsx`):
    - Delete `MAX_SUBJECTS` (L9) and the over-limit span/error block (L147-156).
    - Add state: `const [inputMode, setInputMode] = useState('pick')`, `const [pasteText, setPasteText] = useState('')`.
    - Derive: `const parsed = parsePastedAccounts(pasteText)`; `const effectiveAccounts = inputMode === 'paste' ? parsed.accounts : subjectAccounts`; `clientInvalid` uses `effectiveAccounts.length === 0` (no cap term); `buildPayload` uses `effectiveAccounts`.
    - Mode toggle (above the subject input; radio-style buttons like the grant/revoke group) labeled `Pick (N)` / `Paste list (M)` with each mode's own count; the active mode's input renders below — pick keeps the existing `AccountPicker`, paste renders:

```jsx
<label htmlFor="permissions-paste">Subject accounts (paste list)</label>
<textarea
  id="permissions-paste"
  className="permissions-paste-textarea"
  value={pasteText}
  onChange={(e) => setPasteText(e.target.value)}
  disabled={submitting}
  placeholder="Accounts separated by spaces, commas, or newlines"
/>
<span className="permissions-subjects-count">
  {parsed.accounts.length} account{parsed.accounts.length === 1 ? '' : 's'}
  {parsed.duplicates > 0 ? ` (${parsed.duplicates} duplicate${parsed.duplicates === 1 ? '' : 's'} removed)` : ''}
</span>
```

    - Submit button label gains the count: `` `Grant permission to ${effectiveAccounts.length} account${…}` `` / `` `Revoke permission from …` `` (keep the `Submitting…` in-flight label).
    - `style.css`: minimal rules for the toggle row and textarea (follow the existing class naming).

- [ ] **Step 4: Verify pass** — frontend test suite green.

- [ ] **Step 5: Commit**

```bash
git add admin-frontend/
git commit -m "feat(admin-frontend): paste-list bulk input with count-visible submit"
```

---

### Task 12: admin-frontend — syncFailures banner, resync button, offender strip, long-list collapse

**Files:**
- Modify: `CreatePermissionsDialog.jsx` + `style.css` + `CreatePermissionsDialog.test.jsx`

**Interfaces:**
- Consumes: `resyncPermissions` + `parsePastedAccounts` (Task 11); `formErrorMetadata.accounts` is a `", "`-joined string (see `errcode.WithMetadata("accounts", strings.Join(...))` in admin-service).

- [ ] **Step 1: Failing tests.**
  - Submit resolving with `syncFailures: ['site-b']` → result view shows a warning banner naming site-b and a "Resend sync" button; clicking it calls `resyncPermissions(token, {permission, accounts: <submitted accounts>})`; while pending the button is disabled; resolving with `{}` → banner switches to "Sync complete"; resolving again with failures keeps the button.
  - No `syncFailures` in the response → no banner, no button.
  - Error path: a 404 `unknown_accounts` with `metadata.accounts = "ghost1, ghost2"` → a "Remove these accounts" button appears; clicking it in paste mode rewrites the textarea without the two accounts; in pick mode it filters `subjectAccounts`.
  - `duplicatesIgnored` of 30 entries renders a count with a collapsed expandable list (first render shows the count, not all 30 inline).

- [ ] **Step 2: Verify failure** — frontend test suite.

- [ ] **Step 3: Implement.**
  - Keep the submitted request in state for resync: add `const [lastSubmitted, setLastSubmitted] = useState(null)`; in `handleSubmit`, on success `setLastSubmitted({ permission: PERMISSION_KEY, accounts: effectiveAccounts })` alongside `setResult(response)`.
  - Result view banner (inside the `result ?` branch, after the success summary):

```jsx
{result.syncFailures?.length > 0 ? (
  <div className="permissions-sync-warning">
    <p>
      Recorded and effective at this site, but sync failed for:{' '}
      {result.syncFailures.join(', ')}. Resend delivers the recorded state again —
      safe to repeat.
    </p>
    <button type="button" onClick={handleResync} disabled={resyncing}>
      {resyncing ? 'Resending…' : 'Resend sync'}
    </button>
  </div>
) : resyncDone ? (
  <p className="permissions-sync-ok">Sync complete.</p>
) : null}
```

    with:

```jsx
const [resyncing, setResyncing] = useState(false)
const [resyncDone, setResyncDone] = useState(false)

const handleResync = async () => {
  if (!lastSubmitted || resyncing) return
  setResyncing(true)
  try {
    const r = await resyncPermissions(authToken, lastSubmitted)
    setResult({ ...result, syncFailures: r.syncFailures ?? [] })
    setResyncDone(!(r.syncFailures?.length > 0))
  } catch (err) {
    const message = handleAdminError(err)
    if (message !== null) setFormError(message)
  } finally {
    setResyncing(false)
  }
}
```

  - Offender strip (in the form-error block, when `formErrorMetadata?.accounts` is a string):

```jsx
const stripOffenders = () => {
  const offenders = new Set(formErrorMetadata.accounts.split(/[\s,]+/).filter(Boolean))
  if (inputMode === 'paste') {
    setPasteText(parsePastedAccounts(pasteText).accounts.filter((a) => !offenders.has(a)).join('\n'))
  } else {
    setSubjectAccounts(subjectAccounts.filter((a) => !offenders.has(a)))
  }
  setFormError(null)
  setFormErrorMetadata(null)
}
```

    rendered as a button under the metadata list: `Remove these accounts`.
  - `duplicatesIgnored` collapse: when `result.duplicatesIgnored.length > 20`, render `<details><summary>{n} duplicates ignored</summary>…joined list…</details>` instead of the inline paragraph; ≤20 keeps today's inline text.

- [ ] **Step 4: Verify pass** — frontend suite green.

- [ ] **Step 5: Commit**

```bash
git add admin-frontend/
git commit -m "feat(admin-frontend): syncFailures banner with resync and offender strip"
```

---

### Task 13: docs — client-api.md + request-reply.md

**Files:**
- Modify: `docs/client-api.md`, `docs/client-api/request-reply.md` (both in this task; `docs/client-api/events.md` untouched)

Apply, in both files where the section exists (client-api.md is canonical/fuller; request-reply.md is the terse mirror):

- [ ] **Step 1: settings.get** — response field table gains `| permissions | map<permission key, boolean> | Evaluated admin-managed permissions; every known key always present; admin-written, read-only via settings — settings.set cannot touch it. |`; update the JSON example to include `"permissions": { "external.image.view": true }`; adjust the "All ten fields" sentence.
- [ ] **Step 2: permission.get** — delete the whole subsection, its TOC entry, and its row in the user-service RPC subject table (both files).
- [ ] **Step 3: §9.13 (create/revoke)** — `subjectAccounts` constraint → "non-empty (no fixed cap)"; `invalid_subject_count` error row → the empty-list case; delete the oversized-body 400 row; response field table + example gain `syncFailures` (`string[]`, omitempty — destinations whose INBOX publish was not acknowledged; remediation: §9.15 resync); add one sentence on chunked fanout being transparent to callers.
- [ ] **Step 4: §9.14 (list)** — `currentlyGranted` prose: computed from the materialized user-document state (same evaluation path as `settings.get`).
- [ ] **Step 5: new §9.15 Resync permission fanout** — `POST /v1/admin/permissions/resync`; request table (`permission` string required known key; `accounts` string[] required non-empty); semantics paragraph (re-delivery only — reads the current materialized state, writes no ledger/audit/user rows, idempotent, unknown/never-granted accounts skipped); success 200 `{ "syncFailures": ["site-b"] }` + empty-object example; error table (`unknown_permission` 400, `invalid_subject_count` 400, 401/403/500); curl example. Add its row to the admin HTTP table and TOC.
- [ ] **Step 6: §6 reason catalog** — remove `permission.get` from the emitters of `unknown_permission`; add the resync endpoint to `unknown_permission` and `invalid_subject_count`.
- [ ] **Step 7: Verify** — `grep -n "permission.get" docs/client-api.md docs/client-api/request-reply.md` → zero hits; render-skim the two new/edited tables for column alignment.
- [ ] **Step 8: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md
git commit -m "docs(client-api): permissions in settings.get, resync endpoint, no request caps"
```

---

### Task 14: Finalize — gates, superseded-plan removal, history restructure

**Files:** none new; deletes `docs/superpowers/plans/2026-08-11-user-permission-whitelist.md`; rewrites branch history.

- [ ] **Step 1: Full gates.**

```bash
make generate && make fmt && make lint && make test
make test-integration SERVICE=admin-service
make test-integration SERVICE=inbox-worker
make test-integration SERVICE=user-service
make sast
```

All must pass (fix anything that fails before proceeding).

- [ ] **Step 2: Drop superseded artifacts.** The 2026-08-11 plan documents the replaced implementation — per the reviewer-clarity requirement it must not reach the final history. Also clear any session review notes:

```bash
git rm docs/superpowers/plans/2026-08-11-user-permission-whitelist.md
git rm -r --ignore-unmatch docs/reviews
git commit -m "docs: drop the superseded implementation plan"
```

- [ ] **Step 3: History restructure.** Goal: the reviewer sees ONLY the final version's story — no superseded implementation, no fix-up churn. Method (no interactive rebase in this environment): safety branch → soft-reset to merge-base → re-commit in logical path groups.

```bash
git branch backup/permission-final
git merge-base origin/main HEAD   # note it; call it $BASE below
git reset --soft $BASE
git restore --staged .
git status --short | wc -l        # sanity: all branch changes now unstaged
```

Then create exactly this sequence (each: `git add <paths>` → `git commit`). Messages are subjects below plus a 1-3 sentence body you write from the final code's perspective (present tense, no history references, **no AI trailers**):

| # | Paths | Subject |
|---|---|---|
| 1 | `history-service/internal/cassrepo/utils.go` | `chore(history-service): replace deprecated reflect.Ptr with reflect.Pointer` |
| 2 | `docker-local/compose.deps.yaml tools/seed-sample-data/ tools/jaeger/README.md` | `fix(docker-local): run mongo as a single-node replica set` |
| 3 | `docs/superpowers/` | `docs(spec): user permission whitelist — design and implementation plan` |
| 4 | `pkg/errcode/` | `feat(errcode): permission error reasons` |
| 5 | `pkg/model/` | `feat(model): permission ledger, materialized state, and federation event` |
| 6 | `admin-service/` | `feat(admin-service): permission write path with cross-site fanout and resync` |
| 7 | `inbox-worker/` | `feat(inbox-worker): apply cross-site user permission events` |
| 8 | `user-service/ pkg/subject/` | `feat(user-service): surface permissions in settings.get` |
| 9 | `admin-frontend/` | `feat(admin-frontend): permissions console with bulk paste and resync` |
| 10 | `docs/client-api.md docs/client-api/` | `docs(client-api): permission endpoints and settings.get permissions` |

After commit 10: `git status --short` MUST be empty. If anything remains, STOP — identify the file, decide its logical commit, amend or insert; do not invent an "misc" commit.

- [ ] **Step 4: Verify the rewrite changed nothing.**

```bash
git diff backup/permission-final HEAD --stat   # MUST be empty
git log --oneline $BASE..HEAD                  # exactly 10 commits, the table above
make lint && make test                          # final green on the rewritten head
```

- [ ] **Step 5: Report.** Do **NOT** push (the remote branch has diverged; pushing requires `--force-with-lease` and happens only on the user's explicit request). Leave `backup/permission-final` in place until the user confirms the new history; note both facts in the completion report.

