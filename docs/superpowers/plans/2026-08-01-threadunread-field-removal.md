# ThreadUnread Field Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete the never-grown `Subscription.ThreadUnread` field and all plumbing that maintains it (room-service write paths, `ThreadReadEvent` wire fields, inbox-worker subscription writes, user-service projection, client-api docs, frontend type), per the approved spec `docs/superpowers/specs/2026-08-01-threadunread-field-removal-design.md`.

**Architecture:** Pure removal, ordered so every commit compiles and passes the pre-commit hook: consumers first (room-service → inbox-worker → user-service/frontend), the `pkg/model` field last (a struct field can only be deleted once no package references it), docs at the end. Thread-unread remains fully derived from `thread_subscriptions.lastSeenAt` vs `thread_rooms.lastMsgAt` — untouched.

**Tech Stack:** Go 1.25, mockgen (`make generate`), testify, testcontainers integration tests, NATS JetStream federation (OUTBOX/INBOX).

## Global Constraints

- All commands via `make` targets — never raw `go` commands (`make test SERVICE=<name>`, `make generate SERVICE=<name>`, `make lint`, `make sast`).
- Lint + tests run in the pre-commit hook — never commit with `--no-verify`.
- Behavior decisions locked by the spec: (1) reading a room always clears `alert`; (2) `message.thread.read` / `thread.read.all` no longer touch room-subscription state at all; (3) `ThreadReadEvent` loses `NewThreadUnread` + `Alert`; (4) no Mongo data migration.
- Do NOT touch: `ThreadUnreadRow`, `ThreadUnreadSummary*`, `ThreadReadAll*`, `RoomThreadReadAll*`, `GetThreadUnreadSummary`, `ClearAllThreadUnread`, `unreadThreadsPipeline`, `subject.UserThreadUnreadSummary*` — live derived-badge machinery whose names merely overlap.
- inbox-worker's `UpdateSubscriptionRead` (subscription_read event, carries real CDC alert values) keeps its `alert` parameter; only room-service's same-named method loses it.
- Integration tests use `pkg/testutil` helpers; run with `make test-integration SERVICE=<name>`.

---

### Task 1: room-service — `message.read` always clears alert

**Files:**
- Modify: `room-service/store.go:125-128` (interface), `room-service/store_mongo.go:1037-1052` (impl)
- Modify: `room-service/handler.go:1296-1359` (`messageRead`)
- Modify: `room-service/handler_test.go` (~3159-3300, messageRead fixtures)
- Modify: `room-service/integration_test.go:1963-2010` (`TestMongoStore_UpdateSubscriptionRead_*`)
- Regenerate: `room-service/mock_store_test.go`

**Interfaces:**
- Consumes: current `UpdateSubscriptionRead(ctx, roomID, account string, lastSeenAt time.Time, alert bool) error`.
- Produces: `UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time) error` — always sets `alert: false`. `messageRead` no longer reads `sub.ThreadUnread`; the federated `SubscriptionReadEvent` keeps its `Alert` field but room-service always sends `false`.

- [ ] **Step 1: Change the store interface + implementation**

In `room-service/store.go`, replace the method and its comment:

```go
	// UpdateSubscriptionRead sets lastSeenAt on the subscription keyed by
	// (roomID, account), clearing alert and hasMention (reading the room
	// dismisses both). Returns model.ErrSubscriptionNotFound when no
	// subscription matches.
	UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time) error
```

In `room-service/store_mongo.go`, the implementation drops the parameter and hard-codes the clear:

```go
// UpdateSubscriptionRead sets lastSeenAt on the subscription keyed by
// (roomID, account), clearing alert and hasMention. Returns
// model.ErrSubscriptionNotFound when no subscription matches.
func (s *MongoStore) UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time) error {
	res, err := s.subscriptions.UpdateOne(ctx,
		bson.M{"roomId": roomID, "u.account": account},
		bson.M{"$set": bson.M{"lastSeenAt": lastSeenAt, "alert": false, "hasMention": false}},
	)
	if err != nil {
		return fmt.Errorf("update subscription read for %q in room %q: %w", account, roomID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("update subscription read for %q in room %q: %w", account, roomID, model.ErrSubscriptionNotFound)
	}
	return nil
}
```

- [ ] **Step 2: Regenerate mocks**

Run: `make generate SERVICE=room-service`

- [ ] **Step 3: Update the messageRead handler tests (red)**

In `room-service/handler_test.go`:
- Rename `TestHandler_MessageRead_AlertStaysTrueWithThreadUnread` (line 3180) to `TestHandler_MessageRead_AlwaysClearsAlert`. Seed the subscription with `Alert: true` and **no** `ThreadUnread`; expect the 4-arg call:

```go
func TestHandler_MessageRead_AlwaysClearsAlert(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
		Alert: true,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any()).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&lastSeen, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &lastSeen).Return(nil)

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
}
```

- In every other `TestHandler_MessageRead_*` test, change the `UpdateSubscriptionRead` EXPECT from 5-arg (`gomock.Any(), "r1", "alice", gomock.Any(), <bool>`) to 4-arg (`gomock.Any(), "r1", "alice", gomock.Any()`).
- In `TestHandler_MessageRead_CrossSite_PublishesInbox` (line 3239): drop `Alert: true, ThreadUnread: []string{"t1"}` from the seeded sub (keep `Alert: true` seeded to prove the clear), use the 4-arg EXPECT, and flip the payload assertion `assert.True(t, inner.Alert)` → `assert.False(t, inner.Alert)`.

- [ ] **Step 4: Run tests to verify the handler fails to compile / fails**

Run: `make test SERVICE=room-service`
Expected: FAIL — `handler.go` still calls the 5-arg `UpdateSubscriptionRead` with `newAlert` (compile error). This is the refactor's red gate.

- [ ] **Step 5: Update `messageRead`**

In `room-service/handler.go:1309-1312`, delete the `newAlert` computation and pass 4 args:

```go
	now := time.Now().UTC()

	if err := h.store.UpdateSubscriptionRead(ctx, roomID, account, now); err != nil {
		return nil, fmt.Errorf("update subscription read: %w", err)
	}
```

And in the `SubscriptionReadEvent` payload (line ~1345), replace `Alert: newAlert` with `Alert: false` plus a comment:

```go
		payload := model.SubscriptionReadEvent{
			Account:    account,
			RoomID:     roomID,
			LastSeenAt: now.UnixMilli(),
			// Reading the room always clears the alert; the field stays on the
			// event because data-migration CDC ships real values through it.
			Alert:     false,
			Timestamp: now.UnixMilli(),
		}
```

- [ ] **Step 6: Update the store integration tests**

In `room-service/integration_test.go:1963-2010`: change all `UpdateSubscriptionRead(ctx, ...)` calls to 4-arg. In `TestMongoStore_UpdateSubscriptionRead_Integration`, seed the subscription with `Alert: true` and assert `got.Alert == false` after the call (pins the hard-coded clear).

- [ ] **Step 7: Run unit tests**

Run: `make test SERVICE=room-service`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add room-service
git commit -m "refactor(room-service): message.read always clears alert

The alert-preservation rule (alert && len(threadUnread) > 0) was dead
code: nothing ever grows Subscription.ThreadUnread, so the computation
was constant false. Drop the alert parameter from
UpdateSubscriptionRead and hard-code the clear."
```

---

### Task 2: room-service — thread reads stop writing subscription state

**Files:**
- Modify: `room-service/handler.go` (`messageThreadRead` ~1560-1662, `clearAllThreadRead` ~1672-1729)
- Modify: `room-service/store.go:195-215` (interface), `room-service/store_mongo.go` (delete `UpdateSubscriptionThreadRead` ~1351-1383, `ClearSubscriptionThreadUnreadForAccount` ~1672-1684; trim `subscriptionReadProjection` line 266)
- Modify: `room-service/handler_test.go` (~4060-4260 thread-read tests; clear-all tests — locate with `grep -n ClearSubscriptionThreadUnreadForAccount room-service/handler_test.go`)
- Modify: `room-service/integration_test.go` (delete `UpdateSubscriptionThreadRead` tests ~2540-2610 and `TestMongoStore_ClearSubscriptionThreadUnreadForAccount_Integration` ~4204-4232; trim projection test ~165-193)
- Regenerate: `room-service/mock_store_test.go`

**Interfaces:**
- Consumes: Task 1's 4-arg `UpdateSubscriptionRead` (unrelated paths).
- Produces: `messageThreadRead` performs a single write (`UpdateThreadSubscriptionRead`) plus the existing floor recompute/federation; `clearAllThreadRead` runs `ClearThreadSubscriptionsForAccount` + `GetUserSiteID` only. `ThreadReadEvent` still carries `NewThreadUnread`/`Alert` fields (removed in Task 5) but room-service now leaves them zero.

- [ ] **Step 1: Make the thread-read handler tests stop expecting subscription writes (red)**

In `room-service/handler_test.go`:
- Delete the `f.store.EXPECT().UpdateSubscriptionThreadRead(...)` line from every `TestHandler_MessageThreadRead_*` test (lines 4138, 4157, 4174, 4191, 4208, 4246, and any later ones — sweep with grep).
- Collapse the now-duplicate happy-path tests: keep `TestHandler_MessageThreadRead_HappyAlertClears` renamed to `TestHandler_MessageThreadRead_Happy`, delete `TestHandler_MessageThreadRead_HappyAlertStays`, `TestHandler_MessageThreadRead_IdempotentIDNotInArray`, and `TestHandler_MessageThreadRead_AlertAlreadyFalse` (they only varied the removed store call's return values).
- In `TestHandler_MessageThreadRead_CrossSite_PublishesInbox`: delete the `assert.Equal(t, []string{"p2"}, inner.NewThreadUnread)` and `assert.True(t, inner.Alert)` assertions.
- Delete every `f.store.EXPECT().ClearSubscriptionThreadUnreadForAccount(...)` line from the `clearAllThreadRead` tests.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=room-service`
Expected: FAIL — gomock reports unexpected calls to `UpdateSubscriptionThreadRead` / `ClearSubscriptionThreadUnreadForAccount` (the handler still makes them).

- [ ] **Step 3: Update the handlers**

In `messageThreadRead` (handler.go ~1608-1652): delete `newThreadUnread`/`newAlert` and the errgroup; call the remaining write directly, and slim the payload:

```go
	now := time.Now().UTC()

	if err := h.store.UpdateThreadSubscriptionRead(ctx, tsub.ThreadRoomID, account, now); err != nil {
		return nil, fmt.Errorf("update thread subscription read: %w", err)
	}

	switch {
	case userSiteID == "":
		slog.Warn("user not found locally; skipping cross-site inbox", "account", account)
	case userSiteID != h.siteID:
		payload := model.ThreadReadEvent{
			Account:         account,
			RoomID:          roomID,
			ThreadRoomID:    tsub.ThreadRoomID,
			ParentMessageID: req.ThreadID,
			LastSeenAt:      now.UnixMilli(),
			Timestamp:       now.UnixMilli(),
		}
		...unchanged marshal + federateOne...
	}
```

In `clearAllThreadRead` (~1686-1707): delete the `subErr` errgroup leg and its `case subErr != nil` switch arm; the errgroup keeps the `ClearThreadSubscriptionsForAccount` and `GetUserSiteID` legs.

- [ ] **Step 4: Delete the store methods**

Remove `UpdateSubscriptionThreadRead` and `ClearSubscriptionThreadUnreadForAccount` from `store.go` (and their doc comments) and `store_mongo.go`. Remove `{Key: "threadUnread", Value: 1},` from `subscriptionReadProjection` (store_mongo.go:266). Run `make generate SERVICE=room-service`.

- [ ] **Step 5: Update integration tests**

In `room-service/integration_test.go`:
- Delete the `UpdateSubscriptionThreadRead` test block (~2540-2610, including the concurrent-removal subtest) and `TestMongoStore_ClearSubscriptionThreadUnreadForAccount_Integration` (~4204-4232).
- In `TestMongoStore_GetSubscription_ProjectionFields_Integration` (~165-193): remove the `ThreadUnread: []string{"t1", "t2"}` seed and the `assert.Equal(t, []string{"t1", "t2"}, got.ThreadUnread)` assertion.
- Sweep the file for other `ThreadUnread:` seeds tied to the deleted tests (line ~178, ~2548, ~4211-4213 all go with their tests).

- [ ] **Step 6: Run unit + integration tests**

Run: `make test SERVICE=room-service` then `make test-integration SERVICE=room-service`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add room-service
git commit -m "refactor(room-service): thread reads no longer write subscription thread-unread

message.thread.read and thread.read.all only operated on the
permanently-empty Subscription.ThreadUnread array. Drop
UpdateSubscriptionThreadRead and ClearSubscriptionThreadUnreadForAccount;
thread reads now advance thread-subscription read state only."
```

---

### Task 3: inbox-worker — slim ApplyThreadRead / ApplyThreadReadAll

**Files:**
- Modify: `inbox-worker/handler.go:42-50` (Store interface), `:340-351` (`handleThreadRead`)
- Modify: `inbox-worker/main.go:447-515` (`ApplyThreadRead`, `ApplyThreadReadAll`)
- Modify: `inbox-worker/handler_test.go` (threadRead capture struct ~72-78, stub ~334-345, tests ~1582-1638)
- Modify: `inbox-worker/integration_test.go` (~721-890, ~1240-1263)
- Regenerate: `inbox-worker/mock_store_test.go`

**Interfaces:**
- Consumes: `model.ThreadReadEvent` (still carrying the doomed fields until Task 5 — the handler just stops reading them).
- Produces: `ApplyThreadRead(ctx context.Context, threadRoomID, account string, lastSeenAt time.Time) error` — `$lt`-guarded thread-subscription advance only. `ApplyThreadReadAll(ctx, account, lastSeenAt)` — signature unchanged, subscription clear removed.

- [ ] **Step 1: Change the Store interface + handler**

In `inbox-worker/handler.go` replace the two declarations:

```go
	// ApplyThreadRead advances the home-replica ThreadSubscription read state
	// (lastSeenAt, updatedAt, hasMention=false) under a $lt lastSeenAt guard.
	ApplyThreadRead(ctx context.Context, threadRoomID, account string, lastSeenAt time.Time) error
	// ApplyThreadReadAll is the federated "mark all threads read" bulk clear on
	// the user's home replica: it advances every one of account's thread
	// subscriptions to lastSeenAt under a per-doc $lt guard (clearing hasMention).
	ApplyThreadReadAll(ctx context.Context, account string, lastSeenAt time.Time) error
```

`handleThreadRead` (line 346) becomes:

```go
	if err := h.store.ApplyThreadRead(ctx, e.ThreadRoomID, e.Account, lastSeenAt); err != nil {
		return fmt.Errorf("apply thread read (thread %q, account %q): %w",
			e.ThreadRoomID, e.Account, err)
	}
```

(Old federated events with `newThreadUnread`/`alert` still decode — unknown JSON fields are ignored.)

- [ ] **Step 2: Update store implementations**

In `inbox-worker/main.go`, `ApplyThreadRead` keeps only the guarded thread-sub update (delete lines 467-484's subscription branch — the `MatchedCount` check and `subFilter`/`subUpdate` writes go away entirely):

```go
func (s *mongoInboxStore) ApplyThreadRead(ctx context.Context, threadRoomID, account string, lastSeenAt time.Time) error {
	tsFilter := bson.M{
		"threadRoomId": threadRoomID,
		"userAccount":  account,
		"$or": bson.A{
			bson.M{"lastSeenAt": nil},
			bson.M{"lastSeenAt": bson.M{"$lt": lastSeenAt}},
		},
	}
	tsUpdate := bson.M{"$set": bson.M{
		"lastSeenAt": lastSeenAt,
		"updatedAt":  lastSeenAt,
		"hasMention": false,
	}}
	if _, err := s.threadSubCol.UpdateOne(ctx, tsFilter, tsUpdate); err != nil {
		return fmt.Errorf("apply thread read on thread subscription for %q in thread room %q: %w",
			account, threadRoomID, err)
	}
	return nil
}
```

In `ApplyThreadReadAll`, delete the `subFilter`/`subUpdate` block (lines 509-513) and update the doc comment to drop the "clears threadUnread + alert" sentence.

- [ ] **Step 3: Regenerate mocks and update unit tests**

Run `make generate SERVICE=inbox-worker`. In `inbox-worker/handler_test.go`:
- The `threadRead` capture struct (~72-78) drops `roomID`, `newThreadUnread`, `alert`; the stub `ApplyThreadRead` (~334) takes the new 4-arg signature.
- `TestHandler_HandleEvent_ThreadRead_Happy` (~1582): keep `NewThreadUnread`/`Alert` in the *payload literal* until Task 5 removes the fields — actually drop them now to avoid a second touch; the test asserts only `threadRoomID`, `account`, `lastSeenAt`.

- [ ] **Step 4: Run unit tests**

Run: `make test SERVICE=inbox-worker`
Expected: PASS

- [ ] **Step 5: Update integration tests**

In `inbox-worker/integration_test.go`:
- `TestInboxStore_ApplyThreadRead_HappyPath` (~721): drop the subscription seed + subscription assertions; call `store.ApplyThreadRead(ctx, "tr1", "alice", now)`; keep the thread-sub assertions.
- The `$unset`-shape test (~820-850) and the "last element removed" variants assert subscription state that no longer exists — delete them.
- The `$lt` order-guard test (~860-890): drop subscription seed/assertions, keep the guard assertions on the thread-sub, 4-arg call.
- `TestInboxStore_ApplyThreadReadAll_*` (~770-816): keep thread-sub advance + bob-untouched thread-sub assertions; delete subscription seeds and subscription assertions.
- `TestInboxStore_ApplyThreadRead_MissingThreadSubscription_NoError` (~1241): drop the subscription seed/assertions; the test becomes "missing thread-sub is a silent no-op" with the 4-arg call.

- [ ] **Step 6: Run integration tests**

Run: `make test-integration SERVICE=inbox-worker`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add inbox-worker
git commit -m "refactor(inbox-worker): thread_read events only advance thread-subscription state

ApplyThreadRead/ApplyThreadReadAll no longer touch the subscriptions
collection; the threadUnread array they maintained is permanently empty
and is being removed. Old in-flight events still decode (unknown JSON
fields ignored)."
```

---

### Task 4: user-service projection + chat-frontend type

**Files:**
- Modify: `user-service/mongorepo/subscriptions.go:180`
- Modify: `chat-frontend/src/api/types.ts:62`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: subscription.list responses stop carrying `threadUnread` (it was always omitted anyway — `omitempty` on an always-empty array).

- [ ] **Step 1: Remove the projection line**

In `user-service/mongorepo/subscriptions.go` delete the line `"threadUnread":      1,` (keep surrounding fields aligned).

- [ ] **Step 2: Remove the frontend type field**

In `chat-frontend/src/api/types.ts` delete the line `threadUnread?: string[]` (declared, never read anywhere in `src/`).

- [ ] **Step 3: Run user-service tests**

Run: `make test SERVICE=user-service`
Expected: PASS (no test seeds the field).

- [ ] **Step 4: Commit**

```bash
git add user-service/mongorepo/subscriptions.go chat-frontend/src/api/types.ts
git commit -m "refactor(user-service,chat-frontend): drop threadUnread projection and type

The field is permanently empty and never read by the frontend."
```

---

### Task 5: pkg/model — delete the field and the wire fields

**Files:**
- Modify: `pkg/model/subscription.go:39`, `pkg/model/event.go:166-177`
- Modify: `pkg/model/model_test.go` (~657-745, ~3716-3754)
- Modify: any straggler fixture found by the final grep

**Interfaces:**
- Consumes: Tasks 1-4 (no package references the members being deleted any more).
- Produces: `model.Subscription` without `ThreadUnread`; `model.ThreadReadEvent{Account, RoomID, ThreadRoomID, ParentMessageID, LastSeenAt, Timestamp}`.

- [ ] **Step 1: Verify no production code still references the members**

Run: `grep -rn "ThreadUnread" --include="*.go" | grep -v "ThreadUnreadRow\|ThreadUnreadSummary\|ThreadReadAll\|ClearAllThreadUnread\|GetThreadUnreadSummary\|newThreadUnreadService\|threadUnread(" `
Expected: hits only in `pkg/model/subscription.go`, `pkg/model/event.go`, `pkg/model/model_test.go` (plus this plan/spec under `docs/`).

- [ ] **Step 2: Delete the field and slim the event**

In `pkg/model/subscription.go` delete line 39 (`ThreadUnread []string ...`).
In `pkg/model/event.go` replace `ThreadReadEvent`:

```go
// ThreadReadEvent is the InboxEvent.Payload for type "thread_read". The
// destination advances the home-replica ThreadSubscription read state
// (lastSeenAt, hasMention) under a $lt order-safety guard.
type ThreadReadEvent struct {
	Account         string `json:"account"`
	RoomID          string `json:"roomId"`
	ThreadRoomID    string `json:"threadRoomId"`
	ParentMessageID string `json:"parentMessageId"`
	LastSeenAt      int64  `json:"lastSeenAt"`
	Timestamp       int64  `json:"timestamp"`
}
```

- [ ] **Step 3: Update model tests**

In `pkg/model/model_test.go`:
- Line 672: delete `ThreadUnread: []string{"parent-1", "parent-2"},` from the `TestSubscriptionJSON` fixture.
- Rename `TestSubscriptionJSON_ThreadUnreadOmittedAlertAlwaysPresent` (line 710) to `TestSubscriptionJSON_AlertAlwaysPresent` and delete the two `hasThreadUnread` lines (727-728); keep the alert/muted/favorite/open presence assertions.
- `TestThreadReadEventJSON` (3716) and `TestInboxEventJSON_ThreadRead` (3730): remove `NewThreadUnread` and `Alert` from the literals.

- [ ] **Step 4: Run the full test suite**

Run: `make test`
Expected: PASS (compilation across the whole tree proves no stragglers).

- [ ] **Step 5: Commit**

```bash
git add pkg/model
git commit -m "refactor(model): remove Subscription.ThreadUnread and slim ThreadReadEvent

No code path ever grew the array (the write-side fan-out designed in
2026-05-28 was never built; all consumers derive thread-unread from
thread_subscriptions vs thread_rooms). Leftover bson fields on old
documents are inert. Mixed-version federation is safe: unknown JSON
fields are ignored, and old consumers receiving the slimmed event
perform today's no-op writes."
```

---

### Task 6: docs + final gates

**Files:**
- Modify: `docs/client-api.md` (lines 892, 1855, 1931-1934, 5346)

**Interfaces:**
- Consumes: the final behavior shipped by Tasks 1-5.
- Produces: client-api.md consistent with the wire reality; derived views verified unchanged (no `threadUnread` references exist there).

- [ ] **Step 1: Edit `docs/client-api.md`**

- Line 892: delete the `threadUnread` row from the Subscription field table.
- Line 1855 (Mark Read behaviour notes): replace the alert-recomputation bullet with:
  `- **Alert cleared:** reading the room clears the subscription's alert flag.`
- Lines 1931-1933 (message.thread.read behaviour notes):
  - Delete the **Alert recomputation** bullet.
  - Replace the **Concurrent local writes** bullet with: `- **Local write:** the RPC advances the caller's ThreadSubscription read state (lastSeenAt, updatedAt, hasMention=false). Room-subscription state is untouched — room-level alert/unread is cleared by Mark Read, and thread-unread is derived at read time.`
  - In the **Cross-site federation** bullet, change the payload list to `{account, roomId, threadRoomId, parentMessageId, lastSeenAt, timestamp}` and the destination description to: the destination `inbox-worker` applies `lastSeenAt`+`updatedAt`+`hasMention=false` to the local ThreadSubscription with an `$lt` order-safety guard so out-of-order delivery cannot regress the thread's read position.
  - Line 1934 (**Not following the thread**): change "no `Subscription`/`ThreadSubscription` writes" to "no `ThreadSubscription` write".
- Line 5346 (Clear All Thread Unread): change "…clear that user's thread-subscription read state (`lastSeenAt` advanced, `hasMention` cleared) and room-subscription thread-unread state (`threadUnread` removed, `alert` cleared)." to "…clear that user's thread-subscription read state (`lastSeenAt` advanced, `hasMention` cleared)."
- Sweep: `grep -n threadUnread docs/client-api.md docs/client-api/*.md` must return no hits afterward.

- [ ] **Step 2: Full verification gates**

Run: `make lint`, `make fmt` (no diff expected), `make test`, `make sast`
Expected: all PASS.

- [ ] **Step 3: Commit and push**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): remove threadUnread from subscription schema and thread-read notes"
git push -u origin claude/threadunread-tracking-analysis-wu3g77
```
