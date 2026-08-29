# Admin User Fanout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replicate admin-service user writes (create / roles / names / active) to every remote site — durable HR bootstrap for create, direct INBOX snapshot for state — with fanout failures surfaced in the HTTP response and rendered by the admin frontend.

**Architecture:** Two lanes. `createUser` publishes an identity-only, zstd-encoded `users.upsert` onto the existing HR stream (pull-model durability; `hr-sync-worker` needs no change). Every trigger (create / update / deactivate) also publishes a whole-snapshot `user_account_updated` InboxEvent directly to each remote site's INBOX; `inbox-worker` upserts it under an `accountUpdatedAt` (`$lte`) watermark with `$setOnInsert {_id, siteId}`. Local-write failures keep the errcode envelope; fanout failures ride the 200/201 as `syncFailures`/`hrSyncFailed`, which `CreateUserForm`/`EditUserDialog` render.

**Tech Stack:** Go 1.25, NATS JetStream (`nats.go/jetstream`), MongoDB (`mongo-driver/v2`), `pkg/natsutil` (zstd + headers), mockgen, testify, testcontainers (`pkg/testutil`); React/Vite + vitest for admin-frontend.

**Spec:** `docs/superpowers/specs/2026-08-19-admin-user-fanout-design.md`

## Global Constraints

- TDD every task: failing test first, minimal implementation, then commit. Never write implementation before its test exists.
- All Go commands go through `make` (`make test SERVICE=<name>`, `make test-integration SERVICE=<name>`, `make generate SERVICE=<name>`, `make lint`, `make sast`) — never raw `go` commands.
- Coverage floor 80% per touched package; handlers/stores target 90%+.
- JSON via `encoding/json` (admin-service and inbox-worker are not sonic hot-path services).
- Event timestamps: `time.Now().UTC().UnixMilli()` at the publish site; `Timestamp` doubles as the watermark.
- Watermark compare is **`$lte`**, never `$lt` (same-millisecond writes must not strand remotes).
- `roles` on the wire and in Mongo is always an array (`[]`, never null, never `$unset`).
- Never log AND return an error; fanout failures are `slog.WarnContext` + returned as data, never an error.
- No `Nats-Msg-Id` on either lane; no audit entries for fanout failures.
- `docs/client-api.md` is NOT touched (no `chat.user.*` subject or client-facing struct changes).
- Git commits end with: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- **Rollout order (also the task order): inbox-worker before admin-service.** Old inbox-workers Ack-drop unknown event types — producers must not ship first.
- Frontend commands run in `admin-frontend/`: `npm test`, `npm run typecheck`.

---

### Task 1: `user_account_updated` event model

**Files:**
- Modify: `pkg/model/event.go` (add constant near the other `InboxEventType` constants at ~line 156; add struct near `UserSettingsUpdated` at ~line 178)
- Test: `pkg/model/model_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces: `model.InboxUserAccountUpdated InboxEventType = "user_account_updated"` and
  `model.UserAccountUpdated{ID, Account, SiteID, EngName, ChineseName string; Roles []UserRole; Active bool; Timestamp int64}` — Tasks 2, 3, 6 use these exact names.

- [ ] **Step 1: Write the failing test**

Append to `pkg/model/model_test.go` (package `model`; the file's generic helper is `roundTrip[T any](t *testing.T, src *T, dst *T)`):

```go
func TestUserAccountUpdated_RoundTrip(t *testing.T) {
	src := UserAccountUpdated{
		ID: "u1", Account: "alice", SiteID: "site-a",
		EngName: "Alice", ChineseName: "愛麗絲",
		Roles: []UserRole{UserRoleBot}, Active: true, Timestamp: 1755640000000,
	}
	var dst UserAccountUpdated
	roundTrip(t, &src, &dst)
}

func TestUserAccountUpdated_RoundTrip_EmptyRoles(t *testing.T) {
	src := UserAccountUpdated{ID: "u2", Account: "bob", SiteID: "site-a",
		Roles: []UserRole{}, Active: false, Timestamp: 1}
	var dst UserAccountUpdated
	roundTrip(t, &src, &dst)
	if dst.Roles == nil {
		t.Fatal("empty roles must round-trip as [], not null")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model` — if the Makefile's SERVICE targeting doesn't accept pkg paths, run `make test` and filter output.
Expected: compile error `undefined: UserAccountUpdated`.

- [ ] **Step 3: Write minimal implementation**

In `pkg/model/event.go`, add to the `InboxEventType` const block:

```go
	InboxUserAccountUpdated InboxEventType = "user_account_updated"
```

Add the struct after `UserPermissionsUpdated`:

```go
// UserAccountUpdated is the InboxEvent.Payload for user_account_updated: the
// admin-owned account state as a whole snapshot (identity included, so the
// event alone materializes a complete remote doc). Published by admin-service
// after createUser / updateUser / DeactivateAndRevoke; inbox-worker upserts it
// under the accountUpdatedAt watermark.
type UserAccountUpdated struct {
	ID          string     `json:"id"          bson:"id"`
	Account     string     `json:"account"     bson:"account"`
	SiteID      string     `json:"siteId"      bson:"siteId"` // home site; immutable, $setOnInsert only
	EngName     string     `json:"engName"     bson:"engName"`
	ChineseName string     `json:"chineseName" bson:"chineseName"`
	Roles       []UserRole `json:"roles"       bson:"roles"`     // always an array, never nil
	Active      bool       `json:"active"      bson:"active"`    // resolved via IsActive()
	Timestamp   int64      `json:"timestamp"   bson:"timestamp"` // unix ms; doubles as the watermark
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test` (or the pkg/model-scoped variant from Step 2)
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/event.go pkg/model/model_test.go
git commit -m "feat(model): user_account_updated inbox event

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: inbox-worker store — `UpsertUserAccount`

**Files:**
- Modify: `inbox-worker/handler.go` (the `InboxStore` interface, `//go:generate mockgen` already present at top)
- Modify: `inbox-worker/main.go` (`mongoInboxStore` — add method near `UpdateUserSettings` at ~line 213)
- Test: `inbox-worker/integration_test.go` (`//go:build integration`; follow the file's existing store-construction and `testutil.MongoDB(t, prefix)` setup)

**Interfaces:**
- Consumes: `model.UserAccountUpdated` (Task 1); the file's existing `mongoInboxStore` constructor and its `userCol` (`*mongo.Collection` over `users`).
- Produces: `UpsertUserAccount(ctx context.Context, e *model.UserAccountUpdated, updatedAt time.Time) error` on `InboxStore` — Task 3 calls it.

- [ ] **Step 1: Add the interface method (compile anchor for the mock)**

In `inbox-worker/handler.go`, add to `InboxStore` after `UpdateUserChatlist`:

```go
	// UpsertUserAccount applies the admin-owned account snapshot under the
	// accountUpdatedAt ($lte) watermark, upserting by account with
	// $setOnInsert {_id, siteId}. Unlike the other user_* appliers this one
	// CREATES the doc: the snapshot carries full identity, and materializing
	// it is what unblocks handleMemberAdded's sequential lane when the HR
	// bootstrap lags. E11000 is retried once without upsert to distinguish a
	// stale event (no match → no-op) from the HR lane's insert racing ours
	// (doc without watermark → $set applies).
	UpsertUserAccount(ctx context.Context, e *model.UserAccountUpdated, updatedAt time.Time) error
```

- [ ] **Step 2: Write the failing integration tests**

Append to `inbox-worker/integration_test.go` (mirror the file's existing store construction; create the unique `account` index in the test DB — the race case depends on it):

```go
func TestUpsertUserAccount_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "inbox-user-account")
	store := newTestStore(t, db) // use the file's existing constructor helper name
	ctx := context.Background()
	users := db.Collection("users")
	_, err := users.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "account", Value: 1}}, Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	snap := func(ts int64, active bool, roles []model.UserRole) *model.UserAccountUpdated {
		return &model.UserAccountUpdated{ID: "id-1", Account: "acct-1", SiteID: "site-a",
			EngName: "Eng", ChineseName: "中", Roles: roles, Active: active, Timestamp: ts}
	}
	at := func(ts int64) time.Time { return time.UnixMilli(ts).UTC() }
	readBack := func(t *testing.T) bson.M {
		t.Helper()
		var doc bson.M
		require.NoError(t, users.FindOne(ctx, bson.M{"account": "acct-1"}).Decode(&doc))
		return doc
	}

	t.Run("no doc: inserts complete doc", func(t *testing.T) {
		require.NoError(t, store.UpsertUserAccount(ctx, snap(1000, true, []model.UserRole{model.UserRoleBot}), at(1000)))
		doc := readBack(t)
		assert.Equal(t, "id-1", doc["_id"])
		assert.Equal(t, "site-a", doc["siteId"])
		assert.Equal(t, "Eng", doc["engName"])
		assert.Equal(t, true, doc["active"])
	})
	t.Run("existing HR-shaped doc: $set only, _id kept", func(t *testing.T) {
		_, err := users.UpdateOne(ctx, bson.M{"account": "acct-1"},
			bson.M{"$set": bson.M{"engName": "FromHR"}, "$unset": bson.M{"accountUpdatedAt": "", "roles": "", "active": ""}})
		require.NoError(t, err)
		require.NoError(t, store.UpsertUserAccount(ctx, snap(2000, false, []model.UserRole{}), at(2000)))
		doc := readBack(t)
		assert.Equal(t, "id-1", doc["_id"], "existing _id must survive")
		assert.Equal(t, false, doc["active"])
	})
	t.Run("older timestamp: no-op, nil error", func(t *testing.T) {
		require.NoError(t, store.UpsertUserAccount(ctx, snap(1500, true, []model.UserRole{}), at(1500)))
		assert.Equal(t, false, readBack(t)["active"], "older snapshot must not regress")
	})
	t.Run("equal timestamp: applied ($lte)", func(t *testing.T) {
		require.NoError(t, store.UpsertUserAccount(ctx, snap(2000, true, []model.UserRole{}), at(2000)))
		assert.Equal(t, true, readBack(t)["active"])
	})
	t.Run("doc without watermark (HR raced): applied via non-upsert retry", func(t *testing.T) {
		_, err := users.UpdateOne(ctx, bson.M{"account": "acct-1"}, bson.M{"$unset": bson.M{"accountUpdatedAt": ""}})
		require.NoError(t, err)
		require.NoError(t, store.UpsertUserAccount(ctx, snap(3000, false, []model.UserRole{}), at(3000)))
		assert.Equal(t, false, readBack(t)["active"])
	})
	t.Run("empty roles stored as [], not null", func(t *testing.T) {
		doc := readBack(t)
		roles, ok := doc["roles"].(bson.A)
		require.True(t, ok, "roles must be an array, got %T", doc["roles"])
		assert.Len(t, roles, 0)
	})
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `make test-integration SERVICE=inbox-worker`
Expected: compile error `store.UpsertUserAccount undefined` on `mongoInboxStore`.

- [ ] **Step 4: Implement the store method**

In `inbox-worker/main.go`, next to the other user appliers:

```go
// UpsertUserAccount — see the InboxStore interface comment for why this one
// upserts when the other user_* appliers do not.
func (s *mongoInboxStore) UpsertUserAccount(ctx context.Context, e *model.UserAccountUpdated, updatedAt time.Time) error {
	roles := e.Roles
	if roles == nil {
		roles = []model.UserRole{}
	}
	filter := bson.M{"account": e.Account, "$or": bson.A{
		bson.M{"accountUpdatedAt": bson.M{"$exists": false}},
		bson.M{"accountUpdatedAt": bson.M{"$lte": updatedAt}},
	}}
	update := bson.M{
		"$setOnInsert": bson.M{"_id": e.ID, "siteId": e.SiteID},
		"$set": bson.M{"engName": e.EngName, "chineseName": e.ChineseName,
			"roles": roles, "active": e.Active, "accountUpdatedAt": updatedAt},
	}
	_, err := s.userCol.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
	if mongo.IsDuplicateKeyError(err) {
		// Doc exists: a newer snapshot (stale → retry matches nothing) or the
		// HR lane's insert raced ours (no watermark → retry applies).
		_, err = s.userCol.UpdateOne(ctx, filter, update)
	}
	if err != nil {
		return fmt.Errorf("upsert user account for %q: %w", e.Account, err)
	}
	return nil
}
```

- [ ] **Step 5: Regenerate the mock**

Run: `make generate SERVICE=inbox-worker` (refreshes `mock_store_test.go` with the new method).

- [ ] **Step 6: Run to verify it passes**

Run: `make test-integration SERVICE=inbox-worker` then `make test SERVICE=inbox-worker`
Expected: PASS (unit suite confirms the regenerated mock compiles).

- [ ] **Step 7: Commit**

```bash
git add inbox-worker/handler.go inbox-worker/main.go inbox-worker/mock_store_test.go inbox-worker/integration_test.go
git commit -m "feat(inbox-worker): UpsertUserAccount store method

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: inbox-worker handler — dispatch `user_account_updated`

**Files:**
- Modify: `inbox-worker/handler.go` (the event-type `switch` at ~line 165, and a new handler func near `handleUserSettingsUpdated` at ~line 490)
- Test: `inbox-worker/handler_test.go`

**Interfaces:**
- Consumes: `UpsertUserAccount` (Task 2, via the regenerated mock), `model.UserAccountUpdated` (Task 1).
- Produces: the `case model.InboxUserAccountUpdated` dispatch — no later task consumes code from here; admin-service (Task 6) produces the events it reads.

- [ ] **Step 1: Write the failing tests**

Append to `inbox-worker/handler_test.go` (mirror the file's existing mock-store test setup):

```go
func TestHandler_UserAccountUpdated(t *testing.T) {
	t.Run("dispatches snapshot to store", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockInboxStore(ctrl)
		h := NewHandler(store, "site-b") // match the file's existing constructor call shape
		payload, _ := json.Marshal(model.UserAccountUpdated{
			ID: "u1", Account: "alice", SiteID: "site-a", EngName: "A",
			Roles: []model.UserRole{model.UserRoleBot}, Active: true, Timestamp: 1755640000000,
		})
		evt, _ := json.Marshal(model.InboxEvent{Type: model.InboxUserAccountUpdated,
			SiteID: "site-a", DestSiteID: "site-b", Payload: payload, Timestamp: 1755640000000})
		store.EXPECT().UpsertUserAccount(gomock.Any(), gomock.Any(), time.UnixMilli(1755640000000).UTC()).
			DoAndReturn(func(_ context.Context, e *model.UserAccountUpdated, _ time.Time) error {
				assert.Equal(t, "alice", e.Account)
				assert.Equal(t, "u1", e.ID)
				assert.True(t, e.Active)
				return nil
			})
		require.NoError(t, h.HandleEvent(context.Background(), evt)) // use the file's actual entry method name
	})
	t.Run("malformed payload errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockInboxStore(ctrl)
		h := NewHandler(store, "site-b")
		evt, _ := json.Marshal(model.InboxEvent{Type: model.InboxUserAccountUpdated,
			SiteID: "site-a", DestSiteID: "site-b", Payload: []byte("{nope"), Timestamp: 1})
		assert.Error(t, h.HandleEvent(context.Background(), evt))
	})
	t.Run("store error propagates for redelivery", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockInboxStore(ctrl)
		h := NewHandler(store, "site-b")
		payload, _ := json.Marshal(model.UserAccountUpdated{ID: "u1", Account: "alice", Timestamp: 1})
		evt, _ := json.Marshal(model.InboxEvent{Type: model.InboxUserAccountUpdated, Payload: payload, Timestamp: 1})
		store.EXPECT().UpsertUserAccount(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("mongo down"))
		assert.Error(t, h.HandleEvent(context.Background(), evt))
	})
}
```

(Adjust `NewHandler` arity and the entry-method name to the file's actual signatures — read them before writing; the assertions stay as above.)

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=inbox-worker`
Expected: the dispatch test fails — unknown type is currently WARN + `nil` (Ack-drop), so `UpsertUserAccount` is never called (gomock "missing call").

- [ ] **Step 3: Implement the dispatch + handler**

In the switch, after `case model.InboxUserChatlistUpdated`:

```go
	case model.InboxUserAccountUpdated:
		return h.handleUserAccountUpdated(ctx, &evt)
```

New func:

```go
// handleUserAccountUpdated applies the admin-owned account snapshot; the store
// upserts, so this also materializes admin-created accounts on this site.
func (h *Handler) handleUserAccountUpdated(ctx context.Context, evt *model.InboxEvent) error {
	var e model.UserAccountUpdated
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal user_account_updated payload: %w", err)
	}
	if err := h.store.UpsertUserAccount(ctx, &e, time.UnixMilli(e.Timestamp).UTC()); err != nil {
		return fmt.Errorf("upsert user account for %q: %w", e.Account, err)
	}
	return nil
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `make test SERVICE=inbox-worker`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add inbox-worker/handler.go inbox-worker/handler_test.go
git commit -m "feat(inbox-worker): apply user_account_updated snapshots

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: admin-service store — post-write read-back

**Files:**
- Modify: `admin-service/store.go:56,70` (`UpdateUser`, `DeactivateAndRevoke` signatures)
- Modify: `admin-service/store_mongo.go` (`UpdateUser` at ~line 217, `DeactivateAndRevoke` at ~line 303)
- Modify: `admin-service/handler.go:295,306` (call sites — discard the doc for now; Task 6 consumes it)
- Test: existing `admin-service/handler_test.go` suite stays green (mock returns updated)

This is a behavior-preserving signature change gated by the existing green suite; the new return value gets its behavioral tests in Task 6 (the fanout consumes it). **Deliberate deviation from the spec's "integration (store)" bullet:** admin-service has no testcontainers harness today, and standing one up for a two-method `FindOneAndUpdate` refactor is out of proportion — store-level integration coverage stays deferred; the deactivate-purges-sessions transaction is unchanged code.

**Interfaces:**
- Consumes: nothing new.
- Produces: `UpdateUser(ctx, siteID, account string, fields UserUpdate) (*model.User, error)` — `(nil, nil)` on an empty patch; `DeactivateAndRevoke(ctx, siteID, account string) (*model.User, error)`. Task 6 consumes both.

- [ ] **Step 1: Change the interface and implementations**

`store.go`: update the two signatures (keep the existing doc comments, append: "Returns the post-write doc projected to the fanout fields; UpdateUser returns (nil, nil) when the patch is empty.").

`store_mongo.go` — shared projection near the top of the file:

```go
// fanoutProjection is the post-write read-back: exactly the fields
// fanoutUserAccount publishes. Never include services/password.
var fanoutProjection = bson.M{"_id": 1, "account": 1, "siteId": 1,
	"engName": 1, "chineseName": 1, "roles": 1, "active": 1}
```

`UpdateUser` — replace the `UpdateOne` tail with:

```go
	if len(set) == 0 {
		return nil, nil
	}
	filter := bson.M{"account": account, "siteId": siteID}
	res := s.users.FindOneAndUpdate(ctx, filter, bson.M{"$set": set},
		options.FindOneAndUpdate().SetReturnDocument(options.After).SetProjection(fanoutProjection))
	var u model.User
	if err := res.Decode(&u); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("update user: %w", err)
	}
	return &u, nil
```

`DeactivateAndRevoke` — inside the existing transaction closure, capture the doc:

```go
	var updated *model.User
	err := s.withTransaction(ctx, func(ctx context.Context) error {
		res := s.users.FindOneAndUpdate(ctx, filter, bson.M{"$set": bson.M{"active": false}},
			options.FindOneAndUpdate().SetReturnDocument(options.After).SetProjection(fanoutProjection))
		var u model.User
		if err := res.Decode(&u); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return ErrUserNotFound
			}
			return fmt.Errorf("deactivate user: %w", err)
		}
		// ... existing sessions DeleteMany stays unchanged ...
		updated = &u
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
```

`handler.go` call sites (temporary until Task 6):

```go
	if _, err := h.store.DeactivateAndRevoke(ctx, h.cfg.SiteID, account); err != nil {
	// and
	if _, err := h.store.UpdateUser(ctx, h.cfg.SiteID, account, UserUpdate(req)); err != nil {
```

- [ ] **Step 2: Regenerate the mock and fix expectations**

Run: `make generate SERVICE=admin-service`. Update `handler_test.go` mock expectations mechanically: `.Return(nil)` → `.Return(&model.User{Account: account}, nil)` (or `(nil, ErrUserNotFound)` in not-found cases).

- [ ] **Step 3: Run to verify green**

Run: `make test SERVICE=admin-service`
Expected: PASS with unchanged behavior.

- [ ] **Step 4: Commit**

```bash
git add admin-service/store.go admin-service/store_mongo.go admin-service/handler.go admin-service/mock_store_test.go admin-service/handler_test.go
git commit -m "refactor(admin-service): user writes return the post-write doc

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: admin-service publish func — encoding parameter

**Files:**
- Modify: `admin-service/main.go:76-81` (`publishInbox` closure)
- Modify: `admin-service/handler.go:29,37` (field + constructor param type)
- Modify: `admin-service/permissions.go:369` and the resync path (add `""` argument)
- Test: `admin-service/permissions_test.go` / `handler_test.go` publish helpers gain the parameter

**Interfaces:**
- Consumes: `natsutil.HeaderNatsEncoding` (`pkg/natsutil/compression.go:13`).
- Produces: handler field `publish func(ctx context.Context, subj string, data []byte, encoding string) error` — Task 6 publishes both lanes through it.

- [ ] **Step 1: Update the closure (mirror `teams-hr-sync/main.go:178` `jetStreamPublish`)**

```go
	publish := func(ctx context.Context, subj string, data []byte, encoding string) error {
		msg := natsutil.NewMsg(ctx, subj, data)
		if encoding != "" {
			if msg.Header == nil {
				msg.Header = nats.Header{}
			}
			msg.Header.Set(natsutil.HeaderNatsEncoding, encoding)
		}
		if _, err := js.PublishMsg(ctx, msg); err != nil {
			return fmt.Errorf("publish inbox event: %w", err)
		}
		return nil
	}
```

(Add the `github.com/nats-io/nats.go` import.) Rename the handler field `publishInbox` → `publish`; permissions call sites pass `""`; test publish helpers add the parameter (mechanical).

- [ ] **Step 2: Run to verify green**

Run: `make test SERVICE=admin-service`
Expected: PASS (pure refactor under the existing suite).

- [ ] **Step 3: Commit**

```bash
git add admin-service/main.go admin-service/handler.go admin-service/permissions.go admin-service/permissions_test.go admin-service/handler_test.go
git commit -m "refactor(admin-service): publish func carries a Nats-Encoding value

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: admin-service — `fanoutUserAccount` + response contract

**Files:**
- Modify: `admin-service/handler.go` (new `fanoutUserAccount` + `publishHRBootstrap`; `createUser` at ~197, `updateUser` at ~283 wire them; response structs)
- Test: `admin-service/handler_test.go`

**Interfaces:**
- Consumes: Task 4's returned docs; Task 5's `publish`; `model.UserAccountUpdated` / `model.InboxUserAccountUpdated` (Task 1); `model.IUserWithChange`; `subject.OrgSyncUsersUpsert(siteID)`, `subject.InboxExternal(dest, eventType)`; `natsutil.EncodeZstd` / `natsutil.EncodingZstd`; existing `remoteSites` (`permissions.go:424`), `h.cfg.FanoutTimeout`.
- Produces: HTTP contract — 201 `createUserResponse{userView; SyncFailures []string; HRSyncFailed bool}` and 200 `updateUserResponse{Status string; SyncFailures []string}` (both fanout fields `omitempty`). Tasks 7–9 consume this JSON shape.

- [ ] **Step 1: Write the failing tests**

Append to `admin-service/handler_test.go` a capture helper and the eight cases (reuse the file's existing `newHandler(m, sess, cfg, nil, publish)` shape; `fanoutTestCfg()` already sets `AllSiteIDs`):

```go
type fanoutCapture struct {
	mu    sync.Mutex
	calls []struct {
		Subj, Encoding string
		Data           []byte
	}
	failSubjPrefix string // publishes to subjects with this prefix return an error
}

func (f *fanoutCapture) publish(_ context.Context, subj string, data []byte, encoding string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, struct {
		Subj, Encoding string
		Data           []byte
	}{subj, encoding, data})
	if f.failSubjPrefix != "" && strings.HasPrefix(subj, f.failSubjPrefix) {
		return errors.New("publish failed")
	}
	return nil
}

func decodeHRPayload(t *testing.T, data []byte) []model.IUserWithChange {
	t.Helper()
	msg := &nats.Msg{Data: data, Header: nats.Header{}}
	msg.Header.Set(natsutil.HeaderNatsEncoding, natsutil.EncodingZstd)
	raw, err := natsutil.DecodePayload(msg) // adapt to DecodePayload's actual parameter type
	require.NoError(t, err)
	var users []model.IUserWithChange
	require.NoError(t, json.Unmarshal(raw, &users))
	return users
}
```

Cases (table where natural; each asserts on the capture + the response JSON):

1. **create publishes both lanes** — POST a valid user with `roles:["bot"]`; expect exactly 1 call whose `Subj == subject.OrgSyncUsersUpsert(siteID)` with `Encoding == natsutil.EncodingZstd`, whose decoded payload is one `IUserWithChange` carrying ONLY `ID/Account/SiteID/EngName/ChineseName` (assert `Roles` empty, `ChangeType` empty); plus one call per remote site on `subject.InboxExternal(dest, model.InboxUserAccountUpdated)` with `Encoding == ""` whose `InboxEvent.Payload` unmarshals to the full snapshot (`Active: true`, `Roles: ["bot"]`); response 201 with NO `syncFailures`/`hrSyncFailed` keys (assert on raw JSON body).
2. **update roles → snapshot only** — PATCH `{"roles":["admin"]}`; no HR-subject call; snapshots carry `Roles:["admin"]` and the doc's names (mock returns a full `*model.User`).
3. **update names → snapshot only** — PATCH `{"engName":"New"}`; no HR call; snapshot `EngName:"New"`.
4. **deactivate → snapshot Active:false** — PATCH `{"active":false}`; mock `DeactivateAndRevoke` returns the doc with `Active` pointer false; snapshot `Active:false`; response 200 `{"status":"ok"}` no failure keys.
4b. **reactivate → snapshot Active:true** — PATCH `{"active":true}`; goes through the `UpdateUser` branch (mock returns doc with `Active` pointer true); snapshot `Active: true`; no HR call.
5. **HR publish fails → hrSyncFailed** — `failSubjPrefix = "chat.hr."`; create → 201, body has `"hrSyncFailed":true` and no `syncFailures`.
6. **one INBOX dest fails → exact syncFailures** — `failSubjPrefix = "chat.inbox.site-c."`; expect `"syncFailures":["site-c"]` only, other dests delivered.
7. **nil publish → no calls, no failure fields** — construct handler with `nil` publish; create → 201, plain view.
8. **local write fails → no fanout** — mock `CreateUser` returns an error; expect zero publish calls and the errcode envelope (existing assertion style).

Also: **empty PATCH → no fanout** — mock `UpdateUser` returns `(nil, nil)`; expect zero publish calls, 200 `{"status":"ok"}`.

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=admin-service`
Expected: FAIL — no publishes happen and responses lack the new fields.

- [ ] **Step 3: Implement**

In `handler.go`:

```go
type createUserResponse struct {
	userView
	SyncFailures []string `json:"syncFailures,omitempty"`
	HRSyncFailed bool     `json:"hrSyncFailed,omitempty"`
}

type updateUserResponse struct {
	Status       string   `json:"status"`
	SyncFailures []string `json:"syncFailures,omitempty"`
}

// fanoutUserAccount replicates the admin-owned account state after a committed
// local write: the durable HR identity bootstrap on create, plus the
// user_account_updated snapshot to every remote INBOX. Failures never change
// the HTTP status — they come back as response fields (spec R2) and are
// WARN-logged once here; there is no retry (the next edit re-sends the whole
// snapshot).
func (h *Handler) fanoutUserAccount(ctx context.Context, u *model.User, isCreate bool) (hrFailed bool, syncFailures []string) {
	if h.publish == nil || u == nil {
		return false, nil
	}
	if isCreate {
		hrFailed = h.publishHRBootstrap(ctx, u)
	}
	now := time.Now().UTC().UnixMilli()
	roles := u.Roles
	if roles == nil {
		roles = []model.UserRole{}
	}
	payload, err := json.Marshal(model.UserAccountUpdated{
		ID: u.ID, Account: u.Account, SiteID: u.SiteID,
		EngName: u.EngName, ChineseName: u.ChineseName,
		Roles: roles, Active: u.IsActive(), Timestamp: now,
	})
	if err != nil {
		slog.ErrorContext(ctx, "marshal user account snapshot", "error", err, "account", u.Account)
		return hrFailed, remoteSites(h.cfg.AllSiteIDs, h.cfg.SiteID)
	}
	dests := remoteSites(h.cfg.AllSiteIDs, h.cfg.SiteID)
	failed := make([]bool, len(dests))
	var g errgroup.Group
	for i, dest := range dests {
		g.Go(func() error {
			data, err := json.Marshal(model.InboxEvent{
				Type: model.InboxUserAccountUpdated, SiteID: h.cfg.SiteID,
				DestSiteID: dest, Payload: payload, Timestamp: now,
			})
			if err != nil {
				slog.ErrorContext(ctx, "marshal user account inbox envelope", "dest", dest, "error", err)
				failed[i] = true
				return nil
			}
			if err := h.publish(ctx, subject.InboxExternal(dest, model.InboxUserAccountUpdated), data, ""); err != nil {
				slog.WarnContext(ctx, "publish user account inbox event", "dest", dest, "account", u.Account, "error", err)
				failed[i] = true
			}
			return nil
		})
	}
	_ = g.Wait()
	for i, dest := range dests {
		if failed[i] {
			syncFailures = append(syncFailures, dest)
		}
	}
	return hrFailed, syncFailures
}

// publishHRBootstrap parks the identity-only users.upsert on the local HR
// stream (zstd, per the feed's current convention) so the create eventually
// lands at every site even when a peer is unreachable right now.
func (h *Handler) publishHRBootstrap(ctx context.Context, u *model.User) bool {
	users := []model.IUserWithChange{{User: model.User{
		ID: u.ID, Account: u.Account, SiteID: u.SiteID,
		EngName: u.EngName, ChineseName: u.ChineseName,
	}}}
	data, err := json.Marshal(users)
	if err != nil {
		slog.ErrorContext(ctx, "marshal hr identity bootstrap", "error", err, "account", u.Account)
		return true
	}
	if err := h.publish(ctx, subject.OrgSyncUsersUpsert(h.cfg.SiteID), natsutil.EncodeZstd(data), natsutil.EncodingZstd); err != nil {
		slog.WarnContext(ctx, "publish hr identity bootstrap", "error", err, "account", u.Account)
		return true
	}
	return false
}
```

`createUser` tail (replace `c.JSON(http.StatusCreated, toView(u))`):

```go
	fanCtx, cancel := context.WithTimeout(c.Request.Context(), h.cfg.FanoutTimeout)
	defer cancel()
	hrFailed, failures := h.fanoutUserAccount(fanCtx, u, true)
	c.JSON(http.StatusCreated, createUserResponse{userView: toView(u), SyncFailures: failures, HRSyncFailed: hrFailed})
```

`updateUser` — both branches assign the returned doc (`updated, err := …`), then replace the `gin.H{"status": "ok"}` tail:

```go
	var failures []string
	if updated != nil {
		fanCtx, cancel := context.WithTimeout(c.Request.Context(), h.cfg.FanoutTimeout)
		defer cancel()
		_, failures = h.fanoutUserAccount(fanCtx, updated, false)
	}
	c.JSON(http.StatusOK, updateUserResponse{Status: "ok", SyncFailures: failures})
```

- [ ] **Step 4: Run to verify it passes**

Run: `make test SERVICE=admin-service`
Expected: PASS, all cases.

- [ ] **Step 5: Commit**

```bash
git add admin-service/handler.go admin-service/handler_test.go
git commit -m "feat(admin-service): fan out user create/update cross-site with surfaced failures

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: admin-frontend API client — fanout fields

**Files:**
- Modify: `admin-frontend/src/api/admin/index.ts:212-225` (`createUser`, `updateUser`)
- Test: `admin-frontend/src/api/admin/admin.test.ts` (mirror the file's existing fetch-stub helpers — the permissions `syncFailures` tests at ~line 378 are the template)

**Interfaces:**
- Consumes: Task 6's JSON contract.
- Produces:
  `createUser(authToken, input): Promise<CreateUserResult>` with `CreateUserResult { user: AdminUser; syncFailures: string[]; hrSyncFailed: boolean }`;
  `updateUser(authToken, account, patch): Promise<UpdateUserResult>` with `UpdateUserResult { syncFailures: string[] }`. Tasks 8–9 consume these.

- [ ] **Step 1: Write the failing tests**

```ts
it('createUser defaults absent fanout fields', async () => {
  stubFetch(201, { id: 'u1', account: 'a', siteId: 's', roles: ['bot'], active: true })
  const res = await createUser('tok', { account: 'a', roles: ['bot'], password: 'x' })
  expect(res.user.account).toBe('a')
  expect(res.syncFailures).toEqual([])
  expect(res.hrSyncFailed).toBe(false)
})
it('createUser passes through fanout failures', async () => {
  stubFetch(201, { id: 'u1', account: 'a', siteId: 's', roles: [], active: true,
    syncFailures: ['site-c'], hrSyncFailed: true })
  const res = await createUser('tok', { account: 'a', roles: ['bot'], password: 'x' })
  expect(res.syncFailures).toEqual(['site-c'])
  expect(res.hrSyncFailed).toBe(true)
})
it('updateUser returns syncFailures', async () => {
  stubFetch(200, { status: 'ok', syncFailures: ['site-b'] })
  const res = await updateUser('tok', 'a', { roles: ['admin'] })
  expect(res.syncFailures).toEqual(['site-b'])
})
```

(`stubFetch` = whatever helper the file already uses; keep its real name.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd admin-frontend && npm test`
Expected: FAIL (`res.user` undefined / `res` is void).

- [ ] **Step 3: Implement**

```ts
export interface CreateUserResult {
  user: AdminUser
  syncFailures: string[]
  hrSyncFailed: boolean
}

/** @throws {AsyncJobError} on a non-2xx response (e.g. `account_exists`). A 2xx
 * with `syncFailures`/`hrSyncFailed` means the account committed locally but
 * some cross-site replication did not land — the caller must show it. */
export async function createUser(authToken: string, input: CreateUserInput): Promise<CreateUserResult> {
  const raw = await adminFetch<UserViewWire & { syncFailures?: string[]; hrSyncFailed?: boolean }>(
    authToken, 'POST', '/users', input)
  return { user: normalizeUser(raw), syncFailures: raw.syncFailures ?? [], hrSyncFailed: raw.hrSyncFailed ?? false }
}

export interface UpdateUserResult {
  syncFailures: string[]
}

/** Applies a partial update. A 2xx with `syncFailures` committed locally but
 * did not reach those sites — the caller must show it. */
export async function updateUser(
  authToken: string, account: string, patch: UpdateUserPatch,
): Promise<UpdateUserResult> {
  const raw = await adminFetch<{ status: string; syncFailures?: string[] }>(
    authToken, 'PATCH', `/users/${encodeURIComponent(account)}`, patch)
  return { syncFailures: raw.syncFailures ?? [] }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd admin-frontend && npm test && npm run typecheck`
Expected: PASS both.

- [ ] **Step 5: Commit**

```bash
git add admin-frontend/src/api/admin/index.ts admin-frontend/src/api/admin/admin.test.ts
git commit -m "feat(admin-frontend): surface fanout results from createUser/updateUser

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: CreateUserForm — sync-failure notice

**Files:**
- Modify: `admin-frontend/src/components/UsersConsole/CreateUserForm/CreateUserForm.jsx`
- Test: `admin-frontend/src/components/UsersConsole/CreateUserForm/CreateUserForm.test.jsx`

**Interfaces:**
- Consumes: `CreateUserResult` (Task 7). DB errors keep the existing `handleAdminError` → `setError` banner — untouched.
- Produces: UI only.

- [ ] **Step 1: Write the failing tests**

Append (the file already mocks `@/api` with `createUser: vi.fn()` and provides `fillValidForm()`):

```jsx
it('closes immediately on a clean result', async () => {
  createUser.mockResolvedValue({ user: { account: 'alice' }, syncFailures: [], hrSyncFailed: false })
  const onCreated = vi.fn()
  render(<CreateUserForm authToken="tok" onClose={vi.fn()} onCreated={onCreated} />)
  fillValidForm()
  fireEvent.click(screen.getByRole('button', { name: /create/i }))
  await waitFor(() => expect(onCreated).toHaveBeenCalled())
  expect(screen.queryByText(/sync/i)).toBeNull()
})

it('shows the sync notice and defers onCreated to Done', async () => {
  createUser.mockResolvedValue({ user: { account: 'alice' }, syncFailures: ['site-c'], hrSyncFailed: true })
  const onCreated = vi.fn()
  render(<CreateUserForm authToken="tok" onClose={vi.fn()} onCreated={onCreated} />)
  fillValidForm()
  fireEvent.click(screen.getByRole('button', { name: /create/i }))
  await waitFor(() => expect(screen.getByText(/created on this site/i)).toBeInTheDocument())
  expect(onCreated).not.toHaveBeenCalled()
  expect(screen.getByText(/site-c/)).toBeInTheDocument()
  expect(screen.getByText(/identity sync did not start/i)).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: /done/i }))
  expect(onCreated).toHaveBeenCalled()
})
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd admin-frontend && npm test`
Expected: FAIL (`created on this site` never renders; also update any existing test whose `createUser` mock resolves `undefined` to resolve a clean `CreateUserResult`).

- [ ] **Step 3: Implement**

In `CreateUserForm.jsx` add `const [syncResult, setSyncResult] = useState(null)`; in `handleSubmit`:

```jsx
      const res = await createUser(authToken, { /* unchanged input */ })
      if (res.syncFailures.length > 0 || res.hrSyncFailed) {
        setSyncResult(res)
      } else {
        onCreated()
      }
```

Render the notice instead of the form when `syncResult` is set (same modal; mirror `CreatePermissionsDialog`'s result phase):

```jsx
  if (syncResult) {
    return (
      <Modal onClose={onCreated} labelledBy="create-user-title">
        <h2 id="create-user-title">User created on this site</h2>
        {syncResult.hrSyncFailed && (
          <p role="alert">
            Durable identity sync did not start — remote sites may not learn this
            account until the user is edited again.
          </p>
        )}
        {syncResult.syncFailures.length > 0 && (
          <p role="alert">
            Cross-site sync failed for: {syncResult.syncFailures.join(', ')}. Those
            sites catch up on the next edit of this user.
          </p>
        )}
        <button type="button" onClick={onCreated}>Done</button>
      </Modal>
    )
  }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd admin-frontend && npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add admin-frontend/src/components/UsersConsole/CreateUserForm/
git commit -m "feat(admin-frontend): create-user sync-failure notice

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: EditUserDialog — sync-failure notice

**Files:**
- Modify: `admin-frontend/src/components/UsersConsole/EditUserDialog/EditUserDialog.jsx`
- Test: `admin-frontend/src/components/UsersConsole/EditUserDialog/EditUserDialog.test.jsx`

**Interfaces:**
- Consumes: `UpdateUserResult` (Task 7).
- Produces: UI only.

- [ ] **Step 1: Write the failing tests**

```jsx
it('closes immediately on a clean save', async () => {
  updateUser.mockResolvedValue({ syncFailures: [] })
  const onUpdated = vi.fn()
  render(<EditUserDialog authToken="tok" user={baseUser()} onClose={vi.fn()} onUpdated={onUpdated} />)
  fireEvent.change(screen.getByLabelText(/english name/i), { target: { value: 'New' } })
  fireEvent.click(screen.getByRole('button', { name: /save/i }))
  await waitFor(() => expect(onUpdated).toHaveBeenCalled())
})

it('shows the sync notice when sites failed', async () => {
  updateUser.mockResolvedValue({ syncFailures: ['site-b'] })
  const onUpdated = vi.fn()
  render(<EditUserDialog authToken="tok" user={baseUser()} onClose={vi.fn()} onUpdated={onUpdated} />)
  fireEvent.change(screen.getByLabelText(/english name/i), { target: { value: 'New' } })
  fireEvent.click(screen.getByRole('button', { name: /save/i }))
  await waitFor(() => expect(screen.getByText(/saved on this site/i)).toBeInTheDocument())
  expect(onUpdated).not.toHaveBeenCalled()
  expect(screen.getByText(/site-b/)).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: /close/i }))
  expect(onUpdated).toHaveBeenCalled()
})
```

(`baseUser()` = the file's existing fixture helper; use its real name and the form's real labels/buttons.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd admin-frontend && npm test`
Expected: FAIL.

- [ ] **Step 3: Implement**

In `submitPatch`:

```jsx
      const res = await updateUser(authToken, user.account, patch)
      if (res.syncFailures.length > 0) {
        setSyncFailures(res.syncFailures)
      } else {
        onUpdated()
      }
```

with `const [syncFailures, setSyncFailures] = useState(null)` and, before the form render:

```jsx
  if (syncFailures) {
    return (
      <Modal onClose={onUpdated} labelledBy="edit-user-title">
        <h2 id="edit-user-title">Saved on this site</h2>
        <p role="alert">
          Cross-site sync failed for: {syncFailures.join(', ')}. Re-save this user
          to retry.
        </p>
        <button type="button" onClick={onUpdated}>Close</button>
      </Modal>
    )
  }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd admin-frontend && npm test && npm run typecheck`
Expected: PASS both.

- [ ] **Step 5: Commit**

```bash
git add admin-frontend/src/components/UsersConsole/EditUserDialog/
git commit -m "feat(admin-frontend): edit-user sync-failure notice

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 10: Gates

**Files:** none new.

- [ ] **Step 1: Full verification**

```bash
make generate && git diff --exit-code   # no drifted mocks
make lint
make test
make test-integration SERVICE=inbox-worker
make sast
cd admin-frontend && npm test && npm run typecheck && cd ..
```

Expected: all green; fix anything that isn't before proceeding.

- [ ] **Step 2: Coverage check on the two touched services**

```bash
go test -coverprofile=/tmp/cov-admin.out ./admin-service/... && go tool cover -func=/tmp/cov-admin.out | tail -1
go test -coverprofile=/tmp/cov-inbox.out ./inbox-worker/... && go tool cover -func=/tmp/cov-inbox.out | tail -1
```

(Coverage inspection is the one sanctioned direct `go test` use per CLAUDE.md's coverage section.) Expected: ≥ 80% each; add cases if short.

- [ ] **Step 3: Pre-PR hygiene**

Delete any `docs/reviews/*` files if present; confirm `docs/client-api.md` untouched (`git status`); final commit if anything changed.

- [ ] **Step 4: Stop**

Do NOT create the PR inside this plan — hand back for review (`superpowers:finishing-a-development-branch` decides merge/PR). Remind the operator of the rollout order: **inbox-worker deploys to every site before admin-service**, and the ops HR-topology question (spec §Prerequisite) gates rollout, not code.
