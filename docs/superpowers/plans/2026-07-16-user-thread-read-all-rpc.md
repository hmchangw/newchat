# Clear All Thread Unread RPC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a client-facing `user-service` RPC that clears the unread status of all of a user's threads across all sites in one call (mark-all-threads-read).

**Architecture:** A `user-service` aggregator handler reads the user's home-replica thread-subs, derives the distinct owning sites, and fans out (bounded, per-site-degrade) to a new `room-service` bulk handler on each site. Each `room-service` clears that user's thread-subs (`lastSeenAt=now`, `hasMention=false`) and room-subscription thread-unread state (`threadUnread` removed, `alert=false`) authoritatively on its own site, then reuses the existing per-thread `thread_read` federation to converge the user's home-site replica. Thread-room read-floor recompute and `thread_message_read` receipt events are deliberately skipped.

**Tech Stack:** Go 1.25, NATS request/reply via `pkg/natsrouter` + `user-service/roomclient`, MongoDB (`go.mongodb.org/mongo-driver/v2`), `go.uber.org/mock`, `testify`, `testcontainers-go` (integration).

**Spec:** `docs/superpowers/specs/2026-07-16-user-thread-read-all-rpc-design.md`

## Global Constraints

- Go 1.25; single root `go.mod`; services are flat `package main` dirs; shared code in `pkg/`.
- Always use `make` targets, never raw `go` commands: `make test SERVICE=<path>`, `make test-integration SERVICE=<path>`, `make generate SERVICE=<path>`, `make lint`, `make sast`.
- TDD (Red→Green→Refactor→Commit). Minimum 80% package coverage; target 90%+ for handlers/stores.
- All NATS payloads are typed structs from `pkg/model` (JSON), never `map[string]interface{}`.
- Errors: Tier-1 named `errcode` constructors for client-facing cases; raw `fmt.Errorf("...: %w", err)` for infra (collapses to `internal`). Never log AND return.
- Subjects come from `pkg/subject` builders, never raw `fmt.Sprintf` at call sites.
- Model structs carry both `json` and `bson` tags (`camelCase`, `_id` excepted). These request/response structs are wire-only → `json` tags required; no `bson` needed (not persisted).
- Generated mocks (`mock_store_test.go`, `mocks/mock_repository.go`) are never hand-edited — regenerate via `make generate`.
- Client-facing RPC change → update `docs/client-api.md` AND its derived `docs/client-api/request-reply.md` in the same PR.
- Run `make generate` before tests when interfaces change; `make lint`, `make test`, `make sast` before pushing.

---

### Task 1: Subjects + wire model types

**Files:**
- Modify: `pkg/subject/subject.go` (add four builders near `UserThreadUnreadSummary`, ~line 1081, and `RoomsInfoBatch`/`ThreadRoomInfoBatch`, ~line 285)
- Test: `pkg/subject/subject_test.go`
- Modify: `pkg/model/threadsubscription.go` (append four structs)
- Test: `pkg/model/model_test.go` (roundtrip for the response types)

**Interfaces:**
- Produces:
  - `subject.UserThreadReadAll(account, siteID string) string`
  - `subject.UserThreadReadAllPattern(siteID string) string`
  - `subject.RoomThreadReadAll(siteID string) string`
  - `subject.RoomThreadReadAllSubscribe(siteID string) string`
  - `model.ThreadReadAllRequest struct{}`
  - `model.ThreadReadAllResponse{ ClearedThreads int; UnavailableSites []string }`
  - `model.RoomThreadReadAllRequest{ Account string }`
  - `model.RoomThreadReadAllResponse{ ClearedThreads int }`

- [ ] **Step 1: Write the failing subject tests**

Add to `pkg/subject/subject_test.go`:

```go
func TestUserThreadReadAll(t *testing.T) {
	assert.Equal(t,
		"chat.user.alice.request.user.site-a.thread.read.all",
		UserThreadReadAll("alice", "site-a"))
	assert.Equal(t,
		"chat.user.{account}.request.user.site-a.thread.read.all",
		UserThreadReadAllPattern("site-a"))
}

func TestUserThreadReadAll_PanicsOnWildcardAccount(t *testing.T) {
	assert.Panics(t, func() { UserThreadReadAll("a.*", "site-a") })
}

func TestRoomThreadReadAll(t *testing.T) {
	assert.Equal(t,
		"chat.server.request.room.site-a.thread.read.all",
		RoomThreadReadAll("site-a"))
	assert.Equal(t,
		RoomThreadReadAll("site-a"),
		RoomThreadReadAllSubscribe("site-a"))
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=pkg/subject`
Expected: FAIL — `undefined: UserThreadReadAll` (and the other three).

- [ ] **Step 3: Add the subject builders**

In `pkg/subject/subject.go`, after `UserThreadUnreadSummaryPattern` (~line 1081):

```go
// UserThreadReadAll is the client-facing subject for the cross-site
// clear-all-thread-unread RPC. siteID is the CALLER's own home site — the site
// holding the user's federated thread-subscription replicas and running the
// aggregator. Pair with UserThreadReadAllPattern for user-service registration.
func UserThreadReadAll(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.thread.read.all", account, siteID)
}

// UserThreadReadAllPattern is the natsrouter pattern user-service registers for
// the clear-all-thread-unread RPC (siteID baked in, account left as {account}).
func UserThreadReadAllPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.thread.read.all", siteID)
}
```

And after `ThreadRoomInfoBatch` (~line 287):

```go
// RoomThreadReadAll is the internal server-to-server subject user-service uses to
// ask a site's room-service to clear all of an account's thread-unread state.
func RoomThreadReadAll(siteID string) string {
	return fmt.Sprintf("chat.server.request.room.%s.thread.read.all", siteID)
}

// RoomThreadReadAllSubscribe is room-service's registration subject — the same
// concrete subject, mirroring the RoomsInfoBatch/RoomsInfoBatchSubscribe pair.
func RoomThreadReadAllSubscribe(siteID string) string {
	return fmt.Sprintf("chat.server.request.room.%s.thread.read.all", siteID)
}
```

- [ ] **Step 4: Run the subject tests to verify they pass**

Run: `make test SERVICE=pkg/subject`
Expected: PASS.

- [ ] **Step 5: Write the failing model roundtrip test**

Add to `pkg/model/model_test.go` (uses the existing generic `roundTrip` helper):

```go
func TestThreadReadAllResponse_RoundTrip(t *testing.T) {
	roundTrip(t, ThreadReadAllResponse{ClearedThreads: 7, UnavailableSites: []string{"site-b"}})
	roundTrip(t, ThreadReadAllResponse{})
	roundTrip(t, RoomThreadReadAllRequest{Account: "alice"})
	roundTrip(t, RoomThreadReadAllResponse{ClearedThreads: 3})
}
```

- [ ] **Step 6: Run it to verify it fails**

Run: `make test SERVICE=pkg/model`
Expected: FAIL — `undefined: ThreadReadAllResponse`.

- [ ] **Step 7: Add the model structs**

Append to `pkg/model/threadsubscription.go`:

```go
// ThreadReadAllRequest is the client-facing clear-all-thread-unread request. The
// account rides the subject; no body fields.
type ThreadReadAllRequest struct{}

// ThreadReadAllResponse is the cross-site clear-all-thread-unread result.
// ClearedThreads sums the thread subscriptions cleared on each responding site.
// UnavailableSites lists sites whose bulk-clear RPC failed (their threads may
// remain unread); the overall call still succeeds.
type ThreadReadAllResponse struct {
	ClearedThreads   int      `json:"clearedThreads"`
	UnavailableSites []string `json:"unavailableSites,omitempty"`
}

// RoomThreadReadAllRequest is the server-to-server request user-service sends to a
// site's room-service to clear all of an account's thread-unread state.
type RoomThreadReadAllRequest struct {
	Account string `json:"account"`
}

// RoomThreadReadAllResponse reports how many thread subscriptions the site cleared.
type RoomThreadReadAllResponse struct {
	ClearedThreads int `json:"clearedThreads"`
}
```

- [ ] **Step 8: Run the model test to verify it passes**

Run: `make test SERVICE=pkg/model`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go pkg/model/threadsubscription.go pkg/model/model_test.go
git commit -m "feat(subject,model): add clear-all-thread-unread subjects and wire types"
```

---

### Task 2: room-service store bulk-clear methods

**Files:**
- Modify: `room-service/store.go` (add two methods to the `RoomStore` interface, in the thread-subscription block ~line 186-192)
- Modify: `room-service/store_mongo.go` (implement, near `UpdateThreadSubscriptionRead` ~line 1611)
- Modify: `room-service/mock_store_test.go` (regenerated — do not hand-edit)
- Test: `room-service/integration_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: `model.ThreadSubscription` (fields `ThreadRoomID`, `RoomID`, `ParentMessageID`).
- Produces:
  - `ClearThreadSubscriptionsForAccount(ctx context.Context, account string, now time.Time) ([]model.ThreadSubscription, error)` — sets `lastSeenAt=now`, `updatedAt=now`, `hasMention=false` on every thread-sub of `account`; returns the affected rows (projected: `threadRoomId`, `roomId`, `parentMessageId`). Empty account set → `(nil, nil)`.
  - `ClearSubscriptionThreadUnreadForAccount(ctx context.Context, account string) error` — on every subscription of `account` whose `threadUnread` is non-empty, removes `threadUnread` and sets `alert=false`.

- [ ] **Step 1: Add the two methods to the store interface**

In `room-service/store.go`, inside the `RoomStore` interface after `UpdateThreadSubscriptionRead(...)` (~line 192):

```go
	// ClearThreadSubscriptionsForAccount marks every one of account's thread
	// subscriptions on this site as read (lastSeenAt=now, updatedAt=now,
	// hasMention=false) and returns the affected rows (threadRoomId, roomId,
	// parentMessageId) so the caller can federate a thread_read per thread.
	// Returns (nil, nil) when the account has no thread subscriptions.
	ClearThreadSubscriptionsForAccount(ctx context.Context, account string, now time.Time) ([]model.ThreadSubscription, error)

	// ClearSubscriptionThreadUnreadForAccount clears thread-unread state on every
	// one of account's subscriptions that currently has unread threads: removes
	// threadUnread and sets alert=false. Subscriptions without unread threads are
	// left untouched so a non-thread alert source is preserved.
	ClearSubscriptionThreadUnreadForAccount(ctx context.Context, account string) error
```

- [ ] **Step 2: Regenerate the mock and write the failing integration tests**

Run: `make generate SERVICE=room-service`
(Regenerates `room-service/mock_store_test.go` with the two new methods.)

Add to `room-service/integration_test.go`:

```go
func TestMongoStore_ClearThreadSubscriptionsForAccount_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "roomsvc")
	store := newTestMongoStore(t, db)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	// Two thread subs for alice, one for bob (must be untouched).
	seedThreadSub(t, db, "alice", "r1", "p1", "tr1")
	seedThreadSub(t, db, "alice", "r2", "p2", "tr2")
	seedThreadSub(t, db, "bob", "r1", "p9", "tr9")

	rows, err := store.ClearThreadSubscriptionsForAccount(ctx, "alice", now)
	require.NoError(t, err)
	require.Len(t, rows, 2)

	got := map[string]model.ThreadSubscription{}
	for _, r := range rows {
		got[r.ThreadRoomID] = r
	}
	assert.Equal(t, "r1", got["tr1"].RoomID)
	assert.Equal(t, "p1", got["tr1"].ParentMessageID)

	// alice's docs are read; hasMention cleared; lastSeenAt set.
	for _, tr := range []string{"tr1", "tr2"} {
		ts := readThreadSub(t, db, "alice", tr)
		require.NotNil(t, ts.LastSeenAt)
		assert.WithinDuration(t, now, *ts.LastSeenAt, time.Second)
		assert.False(t, ts.HasMention)
	}
	// bob untouched.
	assert.Nil(t, readThreadSub(t, db, "bob", "tr9").LastSeenAt)
}

func TestMongoStore_ClearThreadSubscriptionsForAccount_Empty_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "roomsvc")
	store := newTestMongoStore(t, db)
	rows, err := store.ClearThreadSubscriptionsForAccount(context.Background(), "nobody", time.Now().UTC())
	require.NoError(t, err)
	assert.Nil(t, rows)
}

func TestMongoStore_ClearSubscriptionThreadUnreadForAccount_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "roomsvc")
	store := newTestMongoStore(t, db)
	ctx := context.Background()

	// alice: r1 has unread threads + alert; r2 has none.
	seedSubscriptionThreadUnread(t, db, "alice", "r1", []string{"p1", "p2"}, true)
	seedSubscriptionThreadUnread(t, db, "alice", "r2", nil, false)
	seedSubscriptionThreadUnread(t, db, "bob", "r1", []string{"p9"}, true)

	require.NoError(t, store.ClearSubscriptionThreadUnreadForAccount(ctx, "alice"))

	r1 := readSubscription(t, db, "alice", "r1")
	assert.Empty(t, r1.ThreadUnread)
	assert.False(t, r1.Alert)
	// bob untouched.
	assert.Equal(t, []string{"p9"}, readSubscription(t, db, "bob", "r1").ThreadUnread)
}
```

> Reuse the file's existing seed/read helpers if present; otherwise add `seedThreadSub`, `readThreadSub`, `seedSubscriptionThreadUnread`, `readSubscription` mirroring the existing thread-read integration helpers (search `integration_test.go` for `threadSubscriptions`/`subscriptions` inserts). Field names: thread_subscriptions uses `userAccount`, `roomId`, `parentMessageId`, `threadRoomId`, `lastSeenAt`, `updatedAt`, `hasMention`; subscriptions uses `u.account`, `roomId`, `threadUnread`, `alert`.

- [ ] **Step 3: Run the integration tests to verify they fail**

Run: `make test-integration SERVICE=room-service`
Expected: FAIL — `store.ClearThreadSubscriptionsForAccount undefined` (method not implemented).

- [ ] **Step 4: Implement the two store methods**

In `room-service/store_mongo.go`, after `UpdateThreadSubscriptionRead` (~line 1628):

```go
// ClearThreadSubscriptionsForAccount marks every one of account's thread
// subscriptions as read and returns the affected rows for federation. Projects
// only the fields the caller federates. No order-safety guard on the source-site
// write; the $lt guard lives on the inbox-worker side.
func (s *MongoStore) ClearThreadSubscriptionsForAccount(ctx context.Context, account string, now time.Time) ([]model.ThreadSubscription, error) {
	filter := bson.M{"userAccount": account}
	cursor, err := s.threadSubscriptions.Find(ctx, filter, options.Find().SetProjection(bson.M{
		"threadRoomId":    1,
		"roomId":          1,
		"parentMessageId": 1,
		"_id":             0,
	}))
	if err != nil {
		return nil, fmt.Errorf("find thread subscriptions for %q: %w", account, err)
	}
	var rows []model.ThreadSubscription
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode thread subscriptions for %q: %w", account, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if _, err := s.threadSubscriptions.UpdateMany(ctx, filter, bson.M{"$set": bson.M{
		"lastSeenAt": now,
		"updatedAt":  now,
		"hasMention": false,
	}}); err != nil {
		return nil, fmt.Errorf("clear thread subscriptions for %q: %w", account, err)
	}
	return rows, nil
}

// ClearSubscriptionThreadUnreadForAccount removes threadUnread and clears alert on
// every one of account's subscriptions that currently has unread threads
// (threadUnread.0 exists). Mirrors the single-thread "empty threadUnread → alert
// cleared" rule; subscriptions with no unread threads are not matched, so a
// non-thread alert is preserved.
func (s *MongoStore) ClearSubscriptionThreadUnreadForAccount(ctx context.Context, account string) error {
	if _, err := s.subscriptions.UpdateMany(ctx,
		bson.M{"u.account": account, "threadUnread.0": bson.M{"$exists": true}},
		bson.M{"$set": bson.M{"alert": false}, "$unset": bson.M{"threadUnread": ""}},
	); err != nil {
		return fmt.Errorf("clear subscription thread-unread for %q: %w", account, err)
	}
	return nil
}
```

- [ ] **Step 5: Run the integration tests to verify they pass**

Run: `make test-integration SERVICE=room-service`
Expected: PASS (the two new tests, plus unaffected existing ones).

- [ ] **Step 6: Commit**

```bash
git add room-service/store.go room-service/store_mongo.go room-service/mock_store_test.go room-service/integration_test.go
git commit -m "feat(room-service): add bulk clear-all-thread-unread store methods"
```

---

### Task 3: room-service bulk-clear handler + registration + federation

**Files:**
- Modify: `room-service/handler.go` (add `clearAllThreadRead`; register in `Register` ~line 114)
- Test: `room-service/handler_test.go`

**Interfaces:**
- Consumes: `model.RoomThreadReadAllRequest`, store methods from Task 2, `h.store.GetUserSiteID`, `h.federateOne`, `model.ThreadReadEvent`, `model.InboxThreadRead`.
- Produces: `(*Handler).clearAllThreadRead(c *natsrouter.Context, req model.RoomThreadReadAllRequest) (*model.RoomThreadReadAllResponse, error)` registered at `subject.RoomThreadReadAllSubscribe(h.siteID)`.

- [ ] **Step 1: Write the failing handler tests**

Add to `room-service/handler_test.go` (uses `newThreadReadFixture` / `ctxParams` / `baseThreadSub`):

```go
func TestHandler_ClearAllThreadRead_LocalUser_NoFederation(t *testing.T) {
	f := newThreadReadFixture(t) // handler.siteID = "site-a"
	rows := []model.ThreadSubscription{
		{ThreadRoomID: "tr1", RoomID: "r1", ParentMessageID: "p1"},
		{ThreadRoomID: "tr2", RoomID: "r2", ParentMessageID: "p2"},
	}
	f.store.EXPECT().ClearThreadSubscriptionsForAccount(gomock.Any(), "alice", gomock.Any()).Return(rows, nil)
	f.store.EXPECT().ClearSubscriptionThreadUnreadForAccount(gomock.Any(), "alice").Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil) // home == handler site

	resp, err := f.handler.clearAllThreadRead(ctxParams(map[string]string{"account": "alice"}), model.RoomThreadReadAllRequest{Account: "alice"})
	require.NoError(t, err)
	assert.Equal(t, 2, resp.ClearedThreads)
	assert.Equal(t, 0, f.publishCalls) // no cross-site federation for a home-local user
}

func TestHandler_ClearAllThreadRead_RemoteUser_FederatesPerThread(t *testing.T) {
	f := newThreadReadFixture(t)
	rows := []model.ThreadSubscription{
		{ThreadRoomID: "tr1", RoomID: "r1", ParentMessageID: "p1"},
		{ThreadRoomID: "tr2", RoomID: "r2", ParentMessageID: "p2"},
	}
	f.store.EXPECT().ClearThreadSubscriptionsForAccount(gomock.Any(), "alice", gomock.Any()).Return(rows, nil)
	f.store.EXPECT().ClearSubscriptionThreadUnreadForAccount(gomock.Any(), "alice").Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil) // remote home

	resp, err := f.handler.clearAllThreadRead(ctxParams(map[string]string{"account": "alice"}), model.RoomThreadReadAllRequest{Account: "alice"})
	require.NoError(t, err)
	assert.Equal(t, 2, resp.ClearedThreads)
	assert.Equal(t, 2, f.publishCalls) // one thread_read per cleared thread

	// Last published payload carries the post-clear state.
	var ev model.ThreadReadEvent
	require.NoError(t, json.Unmarshal(lastInboxPayload(t, f.publishedData), &ev))
	assert.Equal(t, "alice", ev.Account)
	assert.False(t, ev.Alert)
	assert.Empty(t, ev.NewThreadUnread)
}

func TestHandler_ClearAllThreadRead_EmptyAccount_BadRequest(t *testing.T) {
	f := newThreadReadFixture(t)
	_, err := f.handler.clearAllThreadRead(ctxParams(map[string]string{}), model.RoomThreadReadAllRequest{Account: "  "})
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_ClearAllThreadRead_ClearThreadSubsError(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().ClearThreadSubscriptionsForAccount(gomock.Any(), "alice", gomock.Any()).
		Return(nil, fmt.Errorf("mongo down"))
	f.store.EXPECT().ClearSubscriptionThreadUnreadForAccount(gomock.Any(), "alice").Return(nil).AnyTimes()
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil).AnyTimes()

	_, err := f.handler.clearAllThreadRead(ctxParams(map[string]string{"account": "alice"}), model.RoomThreadReadAllRequest{Account: "alice"})
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_ClearAllThreadRead_FederatePublishError(t *testing.T) {
	f := newThreadReadFixture(t)
	f.publishCallErr = fmt.Errorf("nats down")
	f.store.EXPECT().ClearThreadSubscriptionsForAccount(gomock.Any(), "alice", gomock.Any()).
		Return([]model.ThreadSubscription{{ThreadRoomID: "tr1", RoomID: "r1", ParentMessageID: "p1"}}, nil)
	f.store.EXPECT().ClearSubscriptionThreadUnreadForAccount(gomock.Any(), "alice").Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)

	_, err := f.handler.clearAllThreadRead(ctxParams(map[string]string{"account": "alice"}), model.RoomThreadReadAllRequest{Account: "alice"})
	require.Error(t, err)
	assert.Equal(t, 1, f.publishCalls)
}
```

> `lastInboxPayload(t, data)` — the OUTBOX publish wraps the payload in an `OutboxEvent{Envelope: InboxEvent{Payload}}`. If the fixture's other federation tests already unwrap this (search `handler_test.go` for `model.OutboxEvent` / `Envelope`), reuse that helper; otherwise add a small helper that unmarshals `OutboxEvent` → `InboxEvent` → returns `InboxEvent.Payload`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=room-service`
Expected: FAIL — `f.handler.clearAllThreadRead undefined`.

- [ ] **Step 3: Implement the handler**

In `room-service/handler.go`, add near `messageThreadRead` (~line 1625). (`context`, `strings`, `time`, `encoding/json`, `log/slog`, `errgroup`, `errcode`, `natsutil`, `model` are already imported in this file.)

```go
// clearAllThreadRead clears every one of the account's thread-unread indicators on
// this site: thread-subscription read state (lastSeenAt=now, hasMention=false) and
// room-subscription thread-unread state (threadUnread removed, alert=false). It is
// the per-site leaf of the user-service clear-all-thread-unread aggregator. Unlike
// the single-thread path it deliberately skips the thread-room read-floor recompute
// and thread_message_read fan-out (a bulk dismiss must not advance sender receipts).
// For a cross-site user each cleared thread rides the existing thread_read event.
func (h *Handler) clearAllThreadRead(c *natsrouter.Context, req model.RoomThreadReadAllRequest) (*model.RoomThreadReadAllResponse, error) {
	var ctx context.Context = c
	account := strings.TrimSpace(req.Account)
	if account == "" {
		return nil, errcode.BadRequest("account is required")
	}
	c.WithLogValues("account", account)

	now := time.Now().UTC()

	var (
		cleared                      []model.ThreadSubscription
		userSiteID                   string
		clearErr, subErr, siteErr    error
	)
	var g errgroup.Group
	g.Go(func() error {
		var err error
		cleared, err = h.store.ClearThreadSubscriptionsForAccount(ctx, account, now)
		clearErr = err
		return err
	})
	g.Go(func() error {
		err := h.store.ClearSubscriptionThreadUnreadForAccount(ctx, account)
		subErr = err
		return err
	})
	g.Go(func() error {
		s, err := h.store.GetUserSiteID(ctx, account)
		userSiteID, siteErr = s, err
		return err
	})
	_ = g.Wait()
	switch {
	case clearErr != nil:
		return nil, fmt.Errorf("clear thread subscriptions: %w", clearErr)
	case subErr != nil:
		return nil, fmt.Errorf("clear subscription thread-unread: %w", subErr)
	case siteErr != nil:
		return nil, fmt.Errorf("get user siteId: %w", siteErr)
	}

	switch {
	case userSiteID == "":
		slog.WarnContext(ctx, "user not found locally; skipping cross-site inbox", "account", account)
	case userSiteID != h.siteID:
		for i := range cleared {
			row := &cleared[i]
			payload := model.ThreadReadEvent{
				Account:         account,
				RoomID:          row.RoomID,
				ThreadRoomID:    row.ThreadRoomID,
				ParentMessageID: row.ParentMessageID,
				NewThreadUnread: nil,
				Alert:           false,
				LastSeenAt:      now.UnixMilli(),
				Timestamp:       now.UnixMilli(),
			}
			data, err := json.Marshal(payload)
			if err != nil {
				return nil, fmt.Errorf("marshal thread_read payload: %w", err)
			}
			if err := h.federateOne(ctx, row.RoomID, userSiteID, model.InboxThreadRead, data, row.ThreadRoomID+":"+account, now.UnixMilli()); err != nil {
				return nil, fmt.Errorf("federate thread_read: %w", err)
			}
		}
	}

	return &model.RoomThreadReadAllResponse{ClearedThreads: len(cleared)}, nil
}
```

- [ ] **Step 4: Register the handler**

In `room-service/handler.go`, in `Register` after the `ThreadRoomInfoBatch` line (~line 114):

```go
	natsrouter.Register(r, subject.RoomThreadReadAllSubscribe(h.siteID), h.clearAllThreadRead)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test SERVICE=room-service`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): add clear-all-thread-unread bulk handler with per-thread federation"
```

---

### Task 4: user-service RoomClient method

**Files:**
- Modify: `user-service/roomclient/client.go` (add `ClearAllThreadUnread`)
- Modify: `user-service/service/service.go` (add method to the `RoomClient` interface, ~line 44-48)
- Modify: `user-service/service/mocks/mock_repository.go` (regenerated)
- Test: `user-service/roomclient/client_integration_test.go`

**Interfaces:**
- Consumes: `subject.RoomThreadReadAll`, `model.RoomThreadReadAllRequest/Response`.
- Produces: `RoomClient.ClearAllThreadUnread(ctx context.Context, siteID, account string) (int, error)`.

- [ ] **Step 1: Add the method to the RoomClient interface**

In `user-service/service/service.go`, in the `RoomClient` interface (~line 44):

```go
	ClearAllThreadUnread(ctx context.Context, siteID, account string) (int, error)
```

- [ ] **Step 2: Regenerate the user-service mock, write the failing integration test**

Run: `make generate SERVICE=user-service`
(Regenerates `user-service/service/mocks/mock_repository.go`.)

Add to `user-service/roomclient/client_integration_test.go` (mirror the existing `GetThreadRoomInfoBatch` integration test — a stub subscriber that replies on `subject.RoomThreadReadAll(siteID)`):

```go
func TestClient_ClearAllThreadUnread_Integration(t *testing.T) {
	nc := testConn(t) // existing helper connecting to testutil.NATS(t)
	client := New(nc, "site-a")

	sub, err := nc.Subscribe(subject.RoomThreadReadAll("site-b"), func(m *nats.Msg) {
		var req model.RoomThreadReadAllRequest
		_ = json.Unmarshal(m.Data, &req)
		assert.Equal(t, "alice", req.Account)
		resp, _ := json.Marshal(model.RoomThreadReadAllResponse{ClearedThreads: 4})
		_ = m.Respond(resp)
	})
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()

	n, err := client.ClearAllThreadUnread(context.Background(), "site-b", "alice")
	require.NoError(t, err)
	assert.Equal(t, 4, n)
}

func TestClient_ClearAllThreadUnread_RemoteError_Integration(t *testing.T) {
	nc := testConn(t)
	client := New(nc, "site-a")
	sub, err := nc.Subscribe(subject.RoomThreadReadAll("site-b"), func(m *nats.Msg) {
		_ = m.Respond(errcode.Internal("boom").Marshal()) // reply with an error envelope
	})
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()

	_, err = client.ClearAllThreadUnread(context.Background(), "site-b", "alice")
	require.Error(t, err)
}
```

> Match the existing test's connection helper and error-envelope construction (search `client_integration_test.go` for how `GetThreadRoomInfoBatch` builds its stub reply and error case). If the error envelope is produced differently there, mirror that exactly.

- [ ] **Step 3: Run the integration test to verify it fails**

Run: `make test-integration SERVICE=user-service`
Expected: FAIL — `client.ClearAllThreadUnread undefined`.

- [ ] **Step 4: Implement the client method**

In `user-service/roomclient/client.go`, after `GetThreadRoomInfoBatch` (~line 69):

```go
// ClearAllThreadUnread issues the bulk clear-all-thread-unread RPC to room-service
// on the given site; non-OK reply envelopes are relayed via errcode.Parse. Returns
// the number of thread subscriptions cleared on that site.
func (c *Client) ClearAllThreadUnread(ctx context.Context, siteID, account string) (int, error) {
	req, err := json.Marshal(model.RoomThreadReadAllRequest{Account: account})
	if err != nil {
		return 0, fmt.Errorf("marshal clear-all-thread-unread request: %w", err)
	}
	msg, err := c.nc.Request(ctx, subject.RoomThreadReadAll(siteID), req, roomRPCTimeout)
	if err != nil {
		return 0, fmt.Errorf("clear-all-thread-unread rpc: %w", err)
	}
	if e, ok := errcode.Parse(msg.Data); ok {
		return 0, e
	}
	var out model.RoomThreadReadAllResponse
	if err := json.Unmarshal(msg.Data, &out); err != nil {
		return 0, fmt.Errorf("decode clear-all-thread-unread response: %w", err)
	}
	return out.ClearedThreads, nil
}
```

- [ ] **Step 5: Run the integration test to verify it passes**

Run: `make test-integration SERVICE=user-service`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add user-service/roomclient/client.go user-service/service/service.go user-service/service/mocks/mock_repository.go user-service/roomclient/client_integration_test.go
git commit -m "feat(user-service): add ClearAllThreadUnread room client RPC"
```

---

### Task 5: user-service aggregator handler + registration

**Files:**
- Modify: `user-service/service/threadunread.go` (add `ClearAllThreadUnread`)
- Modify: `user-service/service/service.go` (register pattern in `RegisterHandlers`, ~line 125)
- Test: `user-service/service/threadunread_test.go`

**Interfaces:**
- Consumes: `s.threadSubs.ListByAccount`, `s.rooms.ClearAllThreadUnread`, `maxSiteFanout` (package const, defined in `threads.go`), `model.ThreadReadAllRequest/Response`.
- Produces: `(*UserService).ClearAllThreadUnread(c *natsrouter.Context, _ model.ThreadReadAllRequest) (*model.ThreadReadAllResponse, error)` registered at `subject.UserThreadReadAllPattern(s.siteID)`.

- [ ] **Step 1: Write the failing handler tests**

Add to `user-service/service/threadunread_test.go` (mirror the existing `GetThreadUnreadSummary` tests' mock wiring — `newTestService`/mocked `threadSubs` + `rooms`; match the helpers already in that file):

```go
func TestUserService_ClearAllThreadUnread_MultiSite(t *testing.T) {
	f := newThreadUnreadFixture(t) // s.siteID = "site-a" (match existing fixture)
	f.threadSubs.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "tr1", SiteID: "site-a"},
		{ThreadRoomID: "tr2", SiteID: "site-b"},
		{ThreadRoomID: "tr3", SiteID: "site-b"},
	}, nil)
	f.rooms.EXPECT().ClearAllThreadUnread(gomock.Any(), "site-a", "alice").Return(1, nil)
	f.rooms.EXPECT().ClearAllThreadUnread(gomock.Any(), "site-b", "alice").Return(2, nil)

	resp, err := f.svc.ClearAllThreadUnread(ctxAccount("alice"), model.ThreadReadAllRequest{})
	require.NoError(t, err)
	assert.Equal(t, 3, resp.ClearedThreads)
	assert.Empty(t, resp.UnavailableSites)
}

func TestUserService_ClearAllThreadUnread_NoThreads(t *testing.T) {
	f := newThreadUnreadFixture(t)
	f.threadSubs.EXPECT().ListByAccount(gomock.Any(), "alice").Return(nil, nil)
	// No ClearAllThreadUnread calls expected.

	resp, err := f.svc.ClearAllThreadUnread(ctxAccount("alice"), model.ThreadReadAllRequest{})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.ClearedThreads)
}

func TestUserService_ClearAllThreadUnread_SiteDegrades(t *testing.T) {
	f := newThreadUnreadFixture(t)
	f.threadSubs.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "tr1", SiteID: "site-a"},
		{ThreadRoomID: "tr2", SiteID: "site-b"},
	}, nil)
	f.rooms.EXPECT().ClearAllThreadUnread(gomock.Any(), "site-a", "alice").Return(1, nil)
	f.rooms.EXPECT().ClearAllThreadUnread(gomock.Any(), "site-b", "alice").Return(0, fmt.Errorf("site down"))

	resp, err := f.svc.ClearAllThreadUnread(ctxAccount("alice"), model.ThreadReadAllRequest{})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.ClearedThreads)
	assert.Equal(t, []string{"site-b"}, resp.UnavailableSites)
}

func TestUserService_ClearAllThreadUnread_ListError(t *testing.T) {
	f := newThreadUnreadFixture(t)
	f.threadSubs.EXPECT().ListByAccount(gomock.Any(), "alice").Return(nil, fmt.Errorf("mongo down"))

	_, err := f.svc.ClearAllThreadUnread(ctxAccount("alice"), model.ThreadReadAllRequest{})
	require.Error(t, err)
}
```

> Use the fixture/context helpers already in `threadunread_test.go` (e.g. the `GetThreadUnreadSummary` tests build the service with mocked `threadSubs`/`rooms` and a `natsrouter.Context` carrying `account`). Reuse those exact helper names; the names above (`newThreadUnreadFixture`, `f.svc`, `f.threadSubs`, `f.rooms`, `ctxAccount`) are placeholders for whatever that file already defines — do not introduce new ones if equivalents exist.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=user-service`
Expected: FAIL — `f.svc.ClearAllThreadUnread undefined`.

- [ ] **Step 3: Implement the aggregator handler**

In `user-service/service/threadunread.go`, add after `GetThreadUnreadSummary` (~line 110). (`fmt`, `log/slog`, `sync`, `model`, `natsrouter`, `natsutil` already imported.)

```go
// ClearAllThreadUnread is the cross-site "mark all threads read" aggregator: it
// reads the user's home-replica thread-subs, derives the distinct owning sites,
// and asks each site's room-service to clear all of the account's thread-unread
// state. Per-site failures degrade into UnavailableSites rather than failing the
// call, mirroring GetThreadUnreadSummary.
// NATS: chat.user.{account}.request.user.{siteID}.thread.read.all
func (s *UserService) ClearAllThreadUnread(c *natsrouter.Context, _ model.ThreadReadAllRequest) (*model.ThreadReadAllResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)

	rows, err := s.threadSubs.ListByAccount(c, account)
	if err != nil {
		return nil, fmt.Errorf("list thread subscriptions: %w", err)
	}
	if len(rows) == 0 {
		return &model.ThreadReadAllResponse{}, nil
	}

	// Distinct owning sites, in first-seen order.
	seen := make(map[string]struct{}, len(rows))
	sites := make([]string, 0, len(rows))
	for i := range rows {
		site := rows[i].SiteID
		if site == "" {
			continue
		}
		if _, dup := seen[site]; dup {
			continue
		}
		seen[site] = struct{}{}
		sites = append(sites, site)
	}

	type siteResult struct {
		cleared int
		failed  bool
	}
	results := make([]siteResult, len(sites))

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxSiteFanout)
	for i, site := range sites {
		if c.Err() != nil {
			results[i].failed = true
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			n, err := s.rooms.ClearAllThreadUnread(c, site, account)
			if err != nil {
				slog.WarnContext(c, "thread-read-all site degraded",
					"account", account, "site", site,
					"request_id", natsutil.RequestIDFromContext(c), "error", err)
				results[i].failed = true
				return
			}
			results[i].cleared = n
		}()
	}
	wg.Wait()

	resp := &model.ThreadReadAllResponse{}
	for i, site := range sites {
		if results[i].failed {
			resp.UnavailableSites = append(resp.UnavailableSites, site)
			continue
		}
		resp.ClearedThreads += results[i].cleared
	}
	return resp, nil
}
```

- [ ] **Step 4: Register the handler**

In `user-service/service/service.go`, in `RegisterHandlers` after the `UserThreadUnreadSummaryPattern` line (~line 125):

```go
	natsrouter.Register(r, subject.UserThreadReadAllPattern(s.siteID), s.ClearAllThreadUnread)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test SERVICE=user-service`
Expected: PASS.

- [ ] **Step 6: Verify coverage on the new handler**

Run: `go tool cover` per the CLAUDE.md coverage workflow, or re-run `make test SERVICE=user-service` and confirm the four scenarios (multi-site, empty, degrade, list-error) cover happy path + error + boundary. Add a case if any branch is uncovered.

- [ ] **Step 7: Commit**

```bash
git add user-service/service/threadunread.go user-service/service/service.go user-service/service/threadunread_test.go
git commit -m "feat(user-service): add cross-site clear-all-thread-unread RPC"
```

---

### Task 6: Client API documentation

**Files:**
- Modify: `docs/client-api.md` (new "Clear All Thread Unread" section + RPC index entry)
- Modify: `docs/client-api/request-reply.md` (derived request/reply view)

**Interfaces:** none (docs only).

- [ ] **Step 1: Add the RPC index entry**

In `docs/client-api.md`, in the user-service subject index table (near line 4076, the `thread.unread.summary` row), add:

```markdown
| `chat.user.{account}.request.user.{siteID}.thread.read.all` | [Clear All Thread Unread](#clear-all-thread-unread) |
```

- [ ] **Step 2: Add the RPC section**

In `docs/client-api.md`, immediately after the "Get Thread Unread Summary" section (~line 4970), add:

````markdown
#### Clear All Thread Unread

**Subject:** `chat.user.{account}.request.user.{siteID}.thread.read.all`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` is the **caller's own home site** — the site that holds the user's federated thread subscriptions and runs the aggregator.

Clears the unread status of **all** of the user's threads across every site the user participates in — the server side of a "mark all threads read" action. `user-service` reads the user's local thread-subscription replicas, determines the distinct owning sites, and asks each site's `room-service` to clear that user's thread-subscription read state (`lastSeenAt` advanced, `hasMention` cleared) and room-subscription thread-unread state (`threadUnread` removed, `alert` cleared). Sites that fail to respond are listed in `unavailableSites` rather than failing the request.

This is a bulk **dismiss**: it clears only the requesting user's own read state. It does **not** advance thread-room read floors or emit `thread_message_read` receipt events, so other participants' read-receipt UI is unaffected.

##### Request body

Empty object.

```json
{}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `clearedThreads` | number | Total thread subscriptions cleared across all responding sites. `0` when nothing was unread. |
| `unavailableSites` | string[] | Optional. Sites whose per-site clear failed; their threads may remain unread. Omitted when all responded. |

```json
{ "clearedThreads": 7 }
```

##### Error response

| Condition | `code` | Notes |
|-----------|--------|-------|
| Internal failure | `internal` | Local thread-subscription read failed. Per-site RPC failures degrade into `unavailableSites` rather than erroring. |

##### Triggered events — success path

`None — reply only. No thread_message_read receipt events are emitted (bulk dismiss).`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---
````

- [ ] **Step 3: Update the derived request/reply view**

In `docs/client-api/request-reply.md`, add the matching entry for `chat.user.{account}.request.user.{siteID}.thread.read.all` in the same position/format the file uses for `thread.unread.summary` (empty request `{}`, response `{ clearedThreads, unavailableSites? }`). Mirror the surrounding entries' exact structure — do not invent a new format.

- [ ] **Step 4: Verify docs consistency**

Confirm the section heading anchor (`#clear-all-thread-unread`) matches the index link, and that no `events.md` change is needed (no client-visible event was added).

- [ ] **Step 5: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md
git commit -m "docs(client-api): document Clear All Thread Unread RPC"
```

---

## Final verification (before pushing)

- [ ] `make generate` — mocks up to date (no diff).
- [ ] `make lint` — clean.
- [ ] `make test` — all unit tests pass (race detector on).
- [ ] `make test-integration SERVICE=room-service` and `make test-integration SERVICE=user-service` — pass (Docker required).
- [ ] `make sast` — no medium+ findings.
- [ ] `git push -u origin claude/thread-unread-status-rpc-61imdi`.

## Self-Review notes (author)

- **Spec coverage:** §3.1 client RPC → Task 5 + Task 1 (subjects/types). §3.2 internal RPC → Task 1 (subjects) + Task 3 (handler) + Task 4 (client). §4 data flow → Tasks 3+5. §5 component changes → all tasks. §6 semantics (lastSeenAt=now, idempotent, alert only when thread-unread non-empty, partial failure) → Tasks 2 (alert filter `threadUnread.0 $exists`), 3, 5. §7 decisions (reuse per-thread federation) → Task 3. §8 errors → Tasks 3/5. §9 docs → Task 6. §10 testing → each task's test steps.
- **Alert-source check (spec §6):** `ClearSubscriptionThreadUnreadForAccount` matches only subscriptions with a non-empty `threadUnread` and mirrors the single-thread "empty threadUnread → alert=false" rule, so a non-thread alert on a subscription with no unread threads is never touched.
- **Type consistency:** `ClearAllThreadUnread` (client + roomclient + RoomClient iface), `clearAllThreadRead` (room-service handler), `ClearThreadSubscriptionsForAccount` / `ClearSubscriptionThreadUnreadForAccount` (store) used identically across tasks. `ClearedThreads` field name consistent in both response structs.
- **Federation payload:** reuses `model.ThreadReadEvent` + `model.InboxThreadRead` verbatim — no `pkg/outbox` partition change, no `inbox-worker` change.
