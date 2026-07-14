# Thread-Parent CreatedAt: Gatekeeper-First Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Scope note (post-rebase):** Tasks 4–6 (broadcast-worker store, restricted-thread-accounts helper, and thread fan-out wiring) are **superseded by #435** ("Gate thread-reply visibility by history window"), which merged to main first. This PR was rebased onto that work and ships only Tasks 1–3 + Task 7 docs — the gatekeeper→worker `ThreadParentMessageCreatedAt` propagation, which now feeds #435's existing gate. Tasks 4–6 are retained below as a record of the original plan.

**Goal:** Restore best-effort thread-parent `createdAt` resolution in message-gatekeeper (carried on the canonical event), demote downstream worker fetches to fallbacks, and add fail-closed restricted-history filtering to broadcast-worker's thread fan-out.

**Architecture:** The gatekeeper resolves the parent's `createdAt` via the existing `historyParentFetcher` and ships it on `Message.ThreadParentMessageCreatedAt` (soft-fail: nil on any fetch failure). message-worker and search-sync-worker use the event value when present and fall back to their own store only when absent. broadcast-worker gains a restricted-history filter on all three thread fan-out handlers (created/updated/deleted): recipients with `historySharedSince` set are dropped when the parent `createdAt` is unresolved (fail-closed) or predates their share point; the sender is exempt.

**Tech Stack:** Go 1.25, NATS JetStream, MongoDB (`go.mongodb.org/mongo-driver/v2`), Cassandra (gocql), sonic (hot-path JSON), gomock, testify, testcontainers via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-07-06-thread-parent-fetch-strategy-design.md`

## Global Constraints

- All commands via `make` targets — never raw `go` commands (`make test SERVICE=<name>`, `make generate SERVICE=<name>`, `make lint`, `make test-integration SERVICE=<name>`).
- TDD: write the failing test first, watch it fail, then implement.
- Never edit `mock_*_test.go` manually — regenerate with `make generate SERVICE=<name>`.
- Error wrapping: `fmt.Errorf("what this function was doing: %w", err)`; bare wrapped errors → NAK at the worker boundary.
- Coverage floor 80% per package (target 90%+ for handlers); every touched package must stay above it.
- `notification-worker` is OUT OF SCOPE — do not touch it.
- The client `SendMessageRequest` schema is unchanged — resolution is server-side only.
- Mongo queries always project precisely (only the fields the caller needs).
- Comparison rule everywhere: restricted when `parentCreatedAt == nil` (fail-closed) OR `parentCreatedAt.Before(*historySharedSince)`. Equal timestamps → NOT restricted.

---

### Task 1: message-gatekeeper — restore best-effort resolution

**Files:**
- Modify: `message-gatekeeper/handler.go` (processMessage ~line 333, new helper after processMessage)
- Test: `message-gatekeeper/handler_test.go`

**Interfaces:**
- Consumes: existing `ParentMessageFetcher.FetchQuotedParent(ctx, account, roomID, siteID, messageID) (*cassandra.QuotedParentMessage, error)` (`store.go:35`), existing `MockParentMessageFetcher`.
- Produces: canonical `model.MessageEvent.Message.ThreadParentMessageCreatedAt *time.Time` populated best-effort (nil on fetch failure). Tasks 2, 3, 6 rely on this field being present-when-resolvable.

- [ ] **Step 1: Write the failing tests**

Append to `message-gatekeeper/handler_test.go`. The test file already defines `makePublishFunc`, `publishedMsg`, and constructs `Handler` literals directly (see `TestHandler_processMessage_RejectsInvalidThreadParentMessageID` at ~line 768 for the pattern).

```go
// threadReplyHarness builds a Handler + mocks for a plain thread-reply send
// (no quote). The store expects one successful subscription lookup.
func threadReplyHarness(t *testing.T) (*Handler, *MockParentMessageFetcher, *[]publishedMsg) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().
		GetSubscription(gomock.Any(), "alice", "room-1").
		Return(&model.Subscription{
			User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID: "room-1",
		}, nil)
	fetcher := NewMockParentMessageFetcher(ctrl)
	var published []publishedMsg
	h := &Handler{
		store:              store,
		publish:            makePublishFunc(&published, nil),
		siteID:             "site-a",
		parentFetcher:      fetcher,
		largeRoomThreshold: 500,
	}
	return h, fetcher, &published
}

func TestHandler_processMessage_ThreadParentCreatedAt_ResolvedViaFetch(t *testing.T) {
	h, fetcher, published := threadReplyHarness(t)
	parentID := idgen.GenerateMessageID()
	parentCreatedAt := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	fetcher.EXPECT().
		FetchQuotedParent(gomock.Any(), "alice", "room-1", "site-a", parentID).
		Return(&cassandra.QuotedParentMessage{MessageID: parentID, CreatedAt: parentCreatedAt}, nil)

	req := model.SendMessageRequest{
		ID:                    idgen.GenerateMessageID(),
		Content:               "reply",
		RequestID:             "01970a4f-8c2d-7c9a-abcd-e0123456789f",
		ThreadParentMessageID: parentID,
	}
	data, err := h.processMessage(context.Background(), "alice", "room-1", "site-a", &req)
	require.NoError(t, err)

	var msg model.Message
	require.NoError(t, json.Unmarshal(data, &msg))
	require.NotNil(t, msg.ThreadParentMessageCreatedAt, "resolved createdAt must ride the reply")
	assert.True(t, msg.ThreadParentMessageCreatedAt.Equal(parentCreatedAt))

	require.Len(t, *published, 1)
	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal((*published)[0].data, &evt))
	require.NotNil(t, evt.Message.ThreadParentMessageCreatedAt, "resolved createdAt must ride the canonical event")
	assert.True(t, evt.Message.ThreadParentMessageCreatedAt.Equal(parentCreatedAt))
}

func TestHandler_processMessage_ThreadParentCreatedAt_FetchFails_StillPublishes(t *testing.T) {
	h, fetcher, published := threadReplyHarness(t)
	parentID := idgen.GenerateMessageID()
	fetcher.EXPECT().
		FetchQuotedParent(gomock.Any(), "alice", "room-1", "site-a", parentID).
		Return(nil, errors.New("history unavailable"))

	req := model.SendMessageRequest{
		ID:                    idgen.GenerateMessageID(),
		Content:               "reply",
		RequestID:             "01970a4f-8c2d-7c9a-abcd-e0123456789f",
		ThreadParentMessageID: parentID,
	}
	data, err := h.processMessage(context.Background(), "alice", "room-1", "site-a", &req)
	require.NoError(t, err, "fetch failure must NOT block the send (soft-fail)")

	var msg model.Message
	require.NoError(t, json.Unmarshal(data, &msg))
	assert.Nil(t, msg.ThreadParentMessageCreatedAt, "unresolved value ships as nil")

	require.Len(t, *published, 1, "canonical event must still be published")
	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal((*published)[0].data, &evt))
	assert.Nil(t, evt.Message.ThreadParentMessageCreatedAt)
}

func TestHandler_processMessage_ThreadParentCreatedAt_NilSnapshot_StillPublishes(t *testing.T) {
	h, fetcher, published := threadReplyHarness(t)
	parentID := idgen.GenerateMessageID()
	fetcher.EXPECT().
		FetchQuotedParent(gomock.Any(), "alice", "room-1", "site-a", parentID).
		Return(nil, nil) // contract violation: nil snapshot, nil error

	req := model.SendMessageRequest{
		ID:                    idgen.GenerateMessageID(),
		Content:               "reply",
		RequestID:             "01970a4f-8c2d-7c9a-abcd-e0123456789f",
		ThreadParentMessageID: parentID,
	}
	_, err := h.processMessage(context.Background(), "alice", "room-1", "site-a", &req)
	require.NoError(t, err)
	require.Len(t, *published, 1)
	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal((*published)[0].data, &evt))
	assert.Nil(t, evt.Message.ThreadParentMessageCreatedAt)
}

func TestHandler_processMessage_ThreadParentCreatedAt_ReusesVerifiedQuoteSnapshot(t *testing.T) {
	h, fetcher, published := threadReplyHarness(t)
	parentID := idgen.GenerateMessageID()
	parentCreatedAt := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	// Quoting the thread parent itself: ONE fetch serves both the quote snapshot
	// and the parent createdAt. Exactly one FetchQuotedParent call is expected.
	fetcher.EXPECT().
		FetchQuotedParent(gomock.Any(), "alice", "room-1", "site-a", parentID).
		Return(&cassandra.QuotedParentMessage{
			MessageID: parentID,
			RoomID:    "room-1",
			CreatedAt: parentCreatedAt,
			Msg:       "the parent",
		}, nil).
		Times(1)

	req := model.SendMessageRequest{
		ID:                    idgen.GenerateMessageID(),
		Content:               "reply quoting the parent",
		RequestID:             "01970a4f-8c2d-7c9a-abcd-e0123456789f",
		ThreadParentMessageID: parentID,
		QuotedParentMessageID: parentID,
	}
	_, err := h.processMessage(context.Background(), "alice", "room-1", "site-a", &req)
	require.NoError(t, err)
	require.Len(t, *published, 1)
	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal((*published)[0].data, &evt))
	require.NotNil(t, evt.Message.ThreadParentMessageCreatedAt)
	assert.True(t, evt.Message.ThreadParentMessageCreatedAt.Equal(parentCreatedAt))
}

func TestHandler_processMessage_NonThreadMessage_NoParentFetch(t *testing.T) {
	// threadReplyHarness's fetcher has NO expectations beyond what each test adds,
	// so any FetchQuotedParent call here fails the test. Non-thread sends also hit
	// the large-room gate → expect the room-meta lookup.
	h, _, published := threadReplyHarness(t)
	h.store.(*MockStore).EXPECT().
		GetRoomMeta(gomock.Any(), "room-1").
		Return(roommetacache.Meta{ID: "room-1", UserCount: 1}, nil)

	req := model.SendMessageRequest{
		ID:        idgen.GenerateMessageID(),
		Content:   "plain message",
		RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
	}
	_, err := h.processMessage(context.Background(), "alice", "room-1", "site-a", &req)
	require.NoError(t, err)
	require.Len(t, *published, 1)
}
```

Adjustment notes for the implementer:
- If `Handler.store` is not accessible as `h.store.(*MockStore)` (it's a plain struct field of interface type — it is), restructure `threadReplyHarness` to also return the `*MockStore`.
- Check existing imports at the top of `handler_test.go`; `idgen`, `cassandra`, `roommetacache`, `errors`, `time` are already imported there (verify, add if missing).
- The quote-reuse test relies on `resolveQuoteSnapshot` fetching once with a verified result. If it fails because the quote path requires more store setup, read `resolveQuoteSnapshot` (handler.go:407) and mirror the expectations from the existing quote tests around handler_test.go:1027.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=message-gatekeeper`
Expected: FAIL — the two "resolved" assertions fail with `ThreadParentMessageCreatedAt` nil (field never populated), the no-fetch/soft-fail tests fail on unexpected/missing mock calls.

- [ ] **Step 3: Implement the best-effort resolver**

In `message-gatekeeper/handler.go`, replace the two-line comment at ~line 333:

```go
	// The canonical event does NOT carry the thread parent's createdAt: each consumer
	// re-resolves it from a store it owns, so a Cassandra outage NAK-replays downstream.
```

with:

```go
	// Resolve the thread parent's createdAt server-side, best-effort: a fetch
	// failure ships the event without the value (each consumer falls back to a
	// store it owns), so a Cassandra outage never blocks the send path.
	threadParentCreatedAt := h.resolveThreadParentCreatedAt(ctx, account, roomID, siteID, req, quotedSnapshot, quotedUnverified)
```

Add `ThreadParentMessageCreatedAt: threadParentCreatedAt,` to the `model.Message` literal (after `ThreadParentMessageID`).

Add the helper after `processMessage`:

```go
// resolveThreadParentCreatedAt resolves the thread parent's createdAt
// server-side, returning nil for a non-thread reply. It reuses the quote
// snapshot's CreatedAt when the parent is also the verified quoted message
// (the unverified placeholder carries a synthetic timestamp), otherwise
// fetches by ID. Best-effort: any failure logs a warning and returns nil —
// downstream consumers fall back to their own stores.
func (h *Handler) resolveThreadParentCreatedAt(
	ctx context.Context,
	account, roomID, siteID string,
	req *model.SendMessageRequest,
	quotedSnapshot *cassandra.QuotedParentMessage,
	quotedUnverified bool,
) *time.Time {
	if req.ThreadParentMessageID == "" {
		return nil
	}
	if quotedSnapshot != nil && !quotedUnverified && req.QuotedParentMessageID == req.ThreadParentMessageID {
		t := quotedSnapshot.CreatedAt.UTC()
		return &t
	}
	if h.parentFetcher == nil {
		return nil
	}
	snap, err := h.parentFetcher.FetchQuotedParent(ctx, account, roomID, siteID, req.ThreadParentMessageID)
	if err != nil || snap == nil {
		slog.WarnContext(ctx, "thread parent createdAt resolution failed, publishing without it",
			"error", err,
			"parent_message_id", req.ThreadParentMessageID,
			"request_id", req.RequestID)
		return nil
	}
	t := snap.CreatedAt.UTC()
	return &t
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=message-gatekeeper`
Expected: PASS (all new tests plus the whole existing suite — the field was absent before, so no existing assertion should break; if a test asserts an exact published payload, add the fetcher expectation or confirm it's a non-thread case).

- [ ] **Step 5: Commit**

```bash
git add message-gatekeeper/handler.go message-gatekeeper/handler_test.go
git commit -m "feat(message-gatekeeper): restore thread-parent createdAt resolution, best-effort"
```

---

### Task 2: message-worker — trust the event value, fetch only when absent

**Files:**
- Modify: `message-worker/handler.go:115-126`
- Test: `message-worker/handler_test.go`

**Interfaces:**
- Consumes: `Message.ThreadParentMessageCreatedAt` from Task 1; existing `Store.GetMessageCreatedAt(ctx, messageID) (time.Time, bool, error)`.
- Produces: unchanged persistence behavior — `evt.Message.ThreadParentMessageCreatedAt` is always non-nil past this block (or the handler returned an error → NAK).

- [ ] **Step 1: Write the failing test**

Append to `message-worker/handler_test.go` (mirror the harness of `TestHandler_ProcessMessage_ThreadReply_AdvancesReplierLastSeen` at ~line 641 — direct mocks + `NewHandler(mockStore, mockUserStore, mockThreadStore, "site-a", publishFn)`):

```go
// A thread reply whose event already carries the gatekeeper-resolved parent
// createdAt must NOT hit Cassandra to re-resolve it.
func TestHandler_ProcessMessage_ThreadReply_EventCarriedParentCreatedAt_SkipsLookup(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	parentCreatedAt := now.Add(-time.Hour)
	user := &model.User{ID: "u-1", Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲", SiteID: "site-a"}

	threadMsg := model.Message{
		ID:                           "msg-reply",
		RoomID:                       "r1",
		UserID:                       "u-1",
		UserAccount:                  "alice",
		Content:                      "reply",
		CreatedAt:                    now,
		ThreadParentMessageID:        "msg-parent",
		ThreadParentMessageCreatedAt: &parentCreatedAt,
	}
	threadEvt := model.MessageEvent{Event: model.EventCreated, Message: threadMsg, SiteID: "site-a", Timestamp: now.UnixMilli()}
	data, _ := json.Marshal(threadEvt)
	expectedSender := cassParticipant{ID: "u-1", EngName: "Alice Wang", CompanyName: "愛麗絲", Account: "alice"}

	ctrl := gomock.NewController(t)
	mockStore := NewMockStore(ctrl)
	mockUserStore := NewMockUserStore(ctrl)
	mockThreadStore := NewMockThreadStore(ctrl)
	mockThreadStore.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// The load-bearing assertion: no GetMessageCreatedAt expectation is set, so
	// any call to it fails the test.
	mockUserStore.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil)
	mockThreadStore.EXPECT().CreateThreadRoom(gomock.Any(), gomock.Any()).Return(errThreadRoomExists)
	mockThreadStore.EXPECT().GetThreadRoomByParentMessageID(gomock.Any(), "msg-parent").
		Return(&model.ThreadRoom{ID: "tr-99"}, nil)
	mockStore.EXPECT().GetMessageSender(gomock.Any(), "msg-parent").
		Return(&cassParticipant{ID: "u-parent", Account: "parent-user"}, nil)
	mockStore.EXPECT().UpdateParentMessageThreadRoomID(gomock.Any(), "msg-parent", "r1", parentCreatedAt, "tr-99").Return(nil).AnyTimes()
	mockUserStore.EXPECT().FindUserByID(gomock.Any(), "u-parent").
		Return(&model.User{ID: "u-parent", Account: "parent-user", SiteID: "site-a"}, nil).AnyTimes()
	mockThreadStore.EXPECT().UpsertThreadSubscription(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockThreadStore.EXPECT().UpdateThreadRoomLastMessage(gomock.Any(), "tr-99", "msg-reply", gomock.Any(), now).Return(nil)
	mockThreadStore.EXPECT().AdvanceThreadSubscriptionLastSeen(gomock.Any(), "tr-99", "alice", now).Return(nil)
	mockStore.EXPECT().SaveThreadMessage(gomock.Any(), &threadMsg, &expectedSender, "site-a", "tr-99").
		Return((*int)(nil), nil)

	h := NewHandler(mockStore, mockUserStore, mockThreadStore, "site-a",
		func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
	require.NoError(t, h.processMessage(context.Background(), data, false))
}
```

Harness note: copy the exact expectation set from the existing passing test at ~line 588-623 (`Migration...` variant) and adjust: (a) keep `ThreadParentMessageCreatedAt` set on the message, (b) DELETE the `GetMessageCreatedAt` expectation, (c) use `migration=false` and stub the publish fn. Some expectations above are `.AnyTimes()` where the exact flow differs — tighten to match what the copied test uses.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=message-worker`
Expected: FAIL — gomock: "missing call" is not the failure here; the failure is an *unexpected call* to `GetMessageCreatedAt` (no expectation registered).

- [ ] **Step 3: Implement the skip**

In `message-worker/handler.go`, wrap the existing resolution (lines 116-126) in a nil check:

```go
	if evt.Message.ThreadParentMessageID != "" {
		// The gatekeeper resolves the parent's createdAt best-effort at send time
		// and ships it on the event; trust it when present. Otherwise resolve
		// authoritatively from messages_by_id. A miss → parent's canonical write
		// hasn't landed → NAK for redelivery (bounded by MaxDeliver) rather than
		// persist a null, corrupting partition coords.
		if evt.Message.ThreadParentMessageCreatedAt == nil {
			createdAt, found, err := h.store.GetMessageCreatedAt(ctx, evt.Message.ThreadParentMessageID)
			if err != nil {
				return fmt.Errorf("resolve thread parent createdAt: %w", err)
			}
			if !found {
				return fmt.Errorf("thread parent %s not yet persisted in messages_by_id", evt.Message.ThreadParentMessageID)
			}
			evt.Message.ThreadParentMessageCreatedAt = &createdAt
		}
```

(Everything after — thread room, lastSeen, mentions, save — is unchanged.)

- [ ] **Step 4: Run tests, fix stale expectations**

Run: `make test SERVICE=message-worker`
Expected: mostly PASS. Two known stale spots:
- `handler_test.go:611` (`TestHandler_ProcessMessage_ThreadReply_Migration...`): the event carries `ThreadParentMessageCreatedAt`, so the exact-once `GetMessageCreatedAt` expectation is now an unmet expectation → DELETE that `EXPECT()` line.
- Table-driven suite at ~line 497 uses `.AnyTimes()` → unaffected. Any table case whose event *omits* the field still exercises the fallback.

Re-run until green. Also run: `make test-integration SERVICE=message-worker` (integration events at `integration_test.go:1144` already set the field — they now exercise the trust-the-event path; the fallback path keeps unit coverage).

- [ ] **Step 5: Commit**

```bash
git add message-worker/handler.go message-worker/handler_test.go
git commit -m "feat(message-worker): trust event-carried thread-parent createdAt, fetch only when absent"
```

---

### Task 3: search-sync-worker — skip ES re-resolve when the event carries the value

**Files:**
- Modify: `search-sync-worker/messages.go:103-112`
- Test: `search-sync-worker/messages_reresolve_test.go`

**Interfaces:**
- Consumes: `Message.ThreadParentMessageCreatedAt` from Task 1; existing `parentCreatedAtResolver` interface (messages.go:19).
- Produces: indexed `MessageSearchIndex.ThreadParentCreatedAt` unchanged in shape.

- [ ] **Step 1: Write the failing test**

Append to `search-sync-worker/messages_reresolve_test.go` (it already defines a stub resolver — reuse it; if the existing stub counts calls, use that, otherwise add the counting stub below):

```go
type countingParentResolver struct {
	calls int
	out   time.Time
	ok    bool
}

func (c *countingParentResolver) ResolveParentCreatedAt(_ context.Context, _ string) (time.Time, bool) {
	c.calls++
	return c.out, c.ok
}

func TestBuildAction_EventCarriedParentCreatedAt_SkipsResolver(t *testing.T) {
	eventValue := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	resolver := &countingParentResolver{out: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), ok: true}
	c := newMessageCollection("messages_v2", time.Time{}, false)
	c.parentResolver = resolver

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: time.Now().UnixMilli(),
		Message: model.Message{
			ID:                           "m1",
			RoomID:                       "r1",
			UserID:                       "u1",
			UserAccount:                  "alice",
			Content:                      "reply",
			CreatedAt:                    time.Now().UTC(),
			ThreadParentMessageID:        "parent-1",
			ThreadParentMessageCreatedAt: &eventValue,
		},
	}
	data, _ := json.Marshal(evt)
	actions, err := c.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 1)
	assert.Zero(t, resolver.calls, "resolver must not run when the event carries the value")

	var doc MessageSearchIndex
	require.NoError(t, json.Unmarshal(actions[0].Doc, &doc))
	require.NotNil(t, doc.ThreadParentCreatedAt)
	assert.True(t, doc.ThreadParentCreatedAt.Equal(eventValue), "the event value must win")
}
```

Adaptation notes: check how existing tests in this file construct the collection and extract the indexed doc from `BulkAction` (field may be `Doc`, `Body`, or similar — mirror the existing assertions; `newMessageCollection` takes `(indexPrefix, syncFrom, devMode)`).

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=search-sync-worker`
Expected: FAIL — `resolver.calls` is 1 (resolver currently always runs) and the indexed value is the resolver's 2020 timestamp, not the event value.

- [ ] **Step 3: Implement the skip**

In `search-sync-worker/messages.go`, replace `resolveThreadParentCreatedAt`:

```go
// resolveThreadParentCreatedAt fills the parent createdAt for a thread reply.
// The gatekeeper's best-effort resolution rides the canonical event and wins
// when present; only re-resolve from the ES index when it is absent. No-op for
// nil resolver/non-thread/delete; a miss leaves the field unset.
func (c *messageCollection) resolveThreadParentCreatedAt(evt *model.MessageEvent) {
	if c.parentResolver == nil || evt.Message.ThreadParentMessageID == "" || evt.Event == model.EventDeleted {
		return
	}
	if evt.Message.ThreadParentMessageCreatedAt != nil {
		return
	}
	if createdAt, ok := c.parentResolver.ResolveParentCreatedAt(context.Background(), evt.Message.ThreadParentMessageID); ok {
		evt.Message.ThreadParentMessageCreatedAt = &createdAt
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=search-sync-worker`
Expected: PASS. Existing re-resolve tests built events without the field → still exercise the fallback.

- [ ] **Step 5: Commit**

```bash
git add search-sync-worker/messages.go search-sync-worker/messages_reresolve_test.go
git commit -m "feat(search-sync-worker): skip ES parent re-resolve when the event carries createdAt"
```

---

### Task 4: broadcast-worker store — ThreadRoomInfo + history-since lookup

Interface change first, with behavior-preserving adaptation, so Tasks 5-6 build on a compiling worker.

**Files:**
- Modify: `broadcast-worker/store.go` (interface), `broadcast-worker/store_mongo.go:154-173`, `broadcast-worker/handler.go:985-991` (mechanical), `broadcast-worker/handler_test.go` (mechanical: 7 `GetThreadFollowers` expectations), `broadcast-worker/integration_test.go:408-448` (adapt + extend)
- Regenerate: `broadcast-worker/mock_store_test.go` via `make generate SERVICE=broadcast-worker`

**Interfaces:**
- Produces (Tasks 5-6 consume these exact signatures):

```go
// ThreadRoomInfo is the thread_rooms projection used by thread fan-out.
// ParentCreatedAt is nil when the doc is missing (first-reply race) or predates
// the threadParentCreatedAt field.
type ThreadRoomInfo struct {
	Followers       map[string]struct{}
	ParentCreatedAt *time.Time
}

// In Store (replaces GetThreadFollowers):
GetThreadRoomInfo(ctx context.Context, parentMessageID string) (ThreadRoomInfo, error)
// New:
GetSubscriptionsHistorySince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error)
```

- [ ] **Step 1: Write the failing integration tests**

In `broadcast-worker/integration_test.go`, rename/adapt `TestBroadcastWorker_GetThreadFollowers_Integration` (~line 408) and add the history-since test:

```go
func TestBroadcastWorker_GetThreadRoomInfo_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "bw-threadinfo")
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	ctx := context.Background()

	parentCreatedAt := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	_, err := db.Collection("thread_rooms").InsertMany(ctx, []interface{}{
		bson.M{"parentMessageId": "parent-1", "replyAccounts": []string{"alice", "bob"}, "threadParentCreatedAt": parentCreatedAt},
		bson.M{"parentMessageId": "parent-2", "replyAccounts": []string{"carol"}}, // legacy doc, no createdAt
	})
	require.NoError(t, err)

	t.Run("doc with parent createdAt", func(t *testing.T) {
		info, err := store.GetThreadRoomInfo(ctx, "parent-1")
		require.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"alice": {}, "bob": {}}, info.Followers)
		require.NotNil(t, info.ParentCreatedAt)
		assert.True(t, info.ParentCreatedAt.Equal(parentCreatedAt))
	})
	t.Run("legacy doc without createdAt", func(t *testing.T) {
		info, err := store.GetThreadRoomInfo(ctx, "parent-2")
		require.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"carol": {}}, info.Followers)
		assert.Nil(t, info.ParentCreatedAt)
	})
	t.Run("missing doc", func(t *testing.T) {
		info, err := store.GetThreadRoomInfo(ctx, "nonexistent-parent")
		require.NoError(t, err)
		assert.Empty(t, info.Followers)
		assert.Nil(t, info.ParentCreatedAt)
	})
}

func TestBroadcastWorker_GetSubscriptionsHistorySince_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "bw-histsince")
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	ctx := context.Background()

	since := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		bson.M{"roomId": "r1", "u": bson.M{"_id": "u-bob", "account": "bob"}, "historySharedSince": since},
		bson.M{"roomId": "r1", "u": bson.M{"_id": "u-carol", "account": "carol"}},
		bson.M{"roomId": "r2", "u": bson.M{"_id": "u-bob", "account": "bob"}}, // other room — must not leak
	})
	require.NoError(t, err)

	got, err := store.GetSubscriptionsHistorySince(ctx, "r1", []string{"bob", "carol", "ghost"})
	require.NoError(t, err)
	require.Contains(t, got, "bob")
	require.NotNil(t, got["bob"])
	assert.True(t, got["bob"].Equal(since))
	require.Contains(t, got, "carol")
	assert.Nil(t, got["carol"])
	assert.NotContains(t, got, "ghost", "accounts with no subscription are simply absent")

	empty, err := store.GetSubscriptionsHistorySince(ctx, "r1", nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}
```

(Keep the existing seed/verify style of the file; `testutil.MongoDB` isolates the DB per test.)

- [ ] **Step 2: Verify tests fail to compile**

Run: `make test-integration SERVICE=broadcast-worker`
Expected: COMPILE ERROR — `store.GetThreadRoomInfo`/`GetSubscriptionsHistorySince` undefined.

- [ ] **Step 3: Implement store + interface + mechanical adaptation**

`broadcast-worker/store.go` — add above the interface, replace `GetThreadFollowers` and add the new method:

```go
// ThreadRoomInfo is the thread_rooms projection used by thread fan-out.
// ParentCreatedAt is nil when the doc is missing (first-reply race) or predates
// the threadParentCreatedAt field.
type ThreadRoomInfo struct {
	Followers       map[string]struct{}
	ParentCreatedAt *time.Time
}
```

```go
	GetThreadRoomInfo(ctx context.Context, parentMessageID string) (ThreadRoomInfo, error)
	// GetSubscriptionsHistorySince returns historySharedSince keyed by account for
	// the given room members (nil value = unrestricted). Accounts without a
	// subscription are absent from the map.
	GetSubscriptionsHistorySince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error)
```

`broadcast-worker/store_mongo.go` — replace `GetThreadFollowers` (lines 154-173):

```go
func (m *mongoStore) GetThreadRoomInfo(ctx context.Context, parentMessageID string) (ThreadRoomInfo, error) {
	var doc struct {
		ReplyAccounts         []string  `bson:"replyAccounts"`
		ThreadParentCreatedAt time.Time `bson:"threadParentCreatedAt"`
	}
	opts := options.FindOne().SetProjection(bson.M{"replyAccounts": 1, "threadParentCreatedAt": 1, "_id": 0})
	err := m.threadRoomCol.FindOne(ctx, bson.M{"parentMessageId": parentMessageID}, opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return ThreadRoomInfo{Followers: map[string]struct{}{}}, nil
		}
		return ThreadRoomInfo{}, fmt.Errorf("find thread room by parent %s: %w", parentMessageID, err)
	}
	out := make(map[string]struct{}, len(doc.ReplyAccounts))
	for _, a := range doc.ReplyAccounts {
		if a != "" {
			out[a] = struct{}{}
		}
	}
	info := ThreadRoomInfo{Followers: out}
	if !doc.ThreadParentCreatedAt.IsZero() {
		t := doc.ThreadParentCreatedAt.UTC()
		info.ParentCreatedAt = &t
	}
	return info, nil
}

func (m *mongoStore) GetSubscriptionsHistorySince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error) {
	if len(accounts) == 0 {
		return map[string]*time.Time{}, nil
	}
	filter := bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}}
	opts := options.Find().SetProjection(bson.M{"u.account": 1, "historySharedSince": 1, "_id": 0})
	cur, err := m.subCol.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("find history-since for %d accounts in room %s: %w", len(accounts), roomID, err)
	}
	defer cur.Close(ctx)
	out := make(map[string]*time.Time, len(accounts))
	for cur.Next(ctx) {
		var doc struct {
			U struct {
				Account string `bson:"account"`
			} `bson:"u"`
			HistorySharedSince *time.Time `bson:"historySharedSince"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode history-since doc in room %s: %w", roomID, err)
		}
		out[doc.U.Account] = doc.HistorySharedSince
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("iterate history-since cursor in room %s: %w", roomID, err)
	}
	return out, nil
}
```

`broadcast-worker/handler.go` — mechanical, behavior-preserving (filtering comes in Task 6):

```go
func (h *Handler) channelThreadFanOut(ctx context.Context, parentMsgID, sender string, mentions []string) ([]string, error) {
	info, err := h.store.GetThreadRoomInfo(ctx, parentMsgID)
	if err != nil {
		return nil, fmt.Errorf("get thread room info for parent %s: %w", parentMsgID, err)
	}
	return threadFanOutAccounts(sender, info.Followers, mentions), nil
}
```

Regenerate mocks: `make generate SERVICE=broadcast-worker`

`broadcast-worker/handler_test.go` — mechanical replacement at lines 2018, 2067, 2188, 2241, 2336, 2389 (and the test name at 2229):

```go
// before
store.EXPECT().GetThreadFollowers(gomock.Any(), parentMsgID).Return(followers, nil)
// after
store.EXPECT().GetThreadRoomInfo(gomock.Any(), parentMsgID).Return(ThreadRoomInfo{Followers: followers}, nil)
```

Error case (2241): `Return(ThreadRoomInfo{}, errors.New("db error"))`. Rename `TestHandleThreadUpdated_ChannelRoom_GetThreadFollowersError` → `TestHandleThreadUpdated_ChannelRoom_GetThreadRoomInfoError`.

IMPORTANT: existing tests will now fail with an *unexpected call* to `GetSubscriptionsHistorySince`? No — Task 4 does not call it yet. It is wired in Task 6.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=broadcast-worker && make test-integration SERVICE=broadcast-worker`
Expected: PASS (unit + the two new integration tests).

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/store.go broadcast-worker/store_mongo.go broadcast-worker/handler.go broadcast-worker/handler_test.go broadcast-worker/integration_test.go broadcast-worker/mock_store_test.go
git commit -m "feat(broadcast-worker): thread_rooms parent-createdAt projection + history-since lookup"
```

---

### Task 5: broadcast-worker — pure restriction helper

**Files:**
- Modify: `broadcast-worker/helper.go`
- Test: `broadcast-worker/helper_test.go`

**Interfaces:**
- Produces (Task 6 consumes):

```go
func restrictedThreadAccounts(sender string, parentCreatedAt *time.Time, histBy map[string]*time.Time) map[string]struct{}
```

- [ ] **Step 1: Write the failing test**

Append to `broadcast-worker/helper_test.go`:

```go
func TestRestrictedThreadAccounts(t *testing.T) {
	parent := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	before := parent.Add(-time.Hour) // joined before the parent → sees the thread
	after := parent.Add(time.Hour)   // history starts after the parent → restricted

	tests := []struct {
		name            string
		sender          string
		parentCreatedAt *time.Time
		histBy          map[string]*time.Time
		want            map[string]struct{}
	}{
		{
			name:            "unrestricted members never filtered",
			sender:          "alice",
			parentCreatedAt: &parent,
			histBy:          map[string]*time.Time{"bob": nil, "carol": nil},
			want:            map[string]struct{}{},
		},
		{
			name:            "share point after parent restricts",
			sender:          "alice",
			parentCreatedAt: &parent,
			histBy:          map[string]*time.Time{"bob": &after, "carol": &before},
			want:            map[string]struct{}{"bob": {}},
		},
		{
			name:            "share point equal to parent is NOT restricted",
			sender:          "alice",
			parentCreatedAt: &parent,
			histBy:          map[string]*time.Time{"bob": &parent},
			want:            map[string]struct{}{},
		},
		{
			name:            "unresolved parent fail-closes every restricted member",
			sender:          "alice",
			parentCreatedAt: nil,
			histBy:          map[string]*time.Time{"bob": &before, "carol": nil},
			want:            map[string]struct{}{"bob": {}},
		},
		{
			name:            "sender exempt even when restricted",
			sender:          "alice",
			parentCreatedAt: nil,
			histBy:          map[string]*time.Time{"alice": &after, "bob": &after},
			want:            map[string]struct{}{"bob": {}},
		},
		{
			name:            "empty input",
			sender:          "alice",
			parentCreatedAt: &parent,
			histBy:          map[string]*time.Time{},
			want:            map[string]struct{}{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, restrictedThreadAccounts(tc.sender, tc.parentCreatedAt, tc.histBy))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: COMPILE ERROR — `restrictedThreadAccounts` undefined.

- [ ] **Step 3: Implement**

Append to `broadcast-worker/helper.go` (add `"time"` to imports if absent):

```go
// restrictedThreadAccounts returns the accounts that must NOT receive events
// for a thread: members whose historySharedSince postdates the thread parent,
// or — fail-closed, matching notification-worker — whose historySharedSince is
// set while the parent createdAt is unresolved. The sender is exempt: they
// authored the reply, so their own devices always receive the echo.
func restrictedThreadAccounts(sender string, parentCreatedAt *time.Time, histBy map[string]*time.Time) map[string]struct{} {
	out := map[string]struct{}{}
	for acc, since := range histBy {
		if since == nil || acc == sender {
			continue
		}
		if parentCreatedAt == nil || parentCreatedAt.Before(*since) {
			out[acc] = struct{}{}
		}
	}
	return out
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/helper.go broadcast-worker/helper_test.go
git commit -m "feat(broadcast-worker): restricted-thread-accounts filter helper"
```

---

### Task 6: broadcast-worker — wire the filter into thread fan-out

**Files:**
- Modify: `broadcast-worker/handler.go` — `channelThreadFanOut`, its 3 call sites (lines ~213, ~317, ~370), `handleThreadCreated` DM branch (~254-263), `handleThreadUpdated` DM branch (~332-336), `handleThreadDeleted` DM branch (~383-389), `publishDMEvents` (~815), `publishMutation` (~647) + its non-thread call sites (lines 186, 294, 486, 532, 557, 602 pass `nil`)
- Test: `broadcast-worker/handler_test.go`

**Interfaces:**
- Consumes: Task 4 store methods, Task 5 `restrictedThreadAccounts`.
- Produces (final signatures):

```go
func (h *Handler) channelThreadFanOut(ctx context.Context, roomID, parentMsgID, sender string, mentions []string, eventParentCreatedAt *time.Time) ([]string, error)
func (h *Handler) dmThreadSkipSet(ctx context.Context, roomID string, msg *model.Message) (map[string]struct{}, error)
func (h *Handler) publishDMEvents(ctx context.Context, meta roommetacache.Meta, clientMsg *model.ClientMessage, timestamp int64, mentionedAccounts []string, skip map[string]struct{}) error
func (h *Handler) publishMutation(ctx context.Context, room *model.Room, roomEvtType model.RoomEventType, messageID string, evt any, skip map[string]struct{}) error
```

- [ ] **Step 1: Write the failing tests**

Append to `broadcast-worker/handler_test.go`. Mirror the harness of `TestHandleThreadCreated_ChannelRoom_FansOutToFollowers` (line 2004). Note every channel-thread test from Task 4 will ALSO need a `GetSubscriptionsHistorySince` expectation once the implementation lands — see Step 3 notes.

```go
// Channel thread reply with resolved parent createdAt: members whose
// historySharedSince postdates the parent are dropped from the fan-out.
func TestHandleThreadCreated_ChannelRoom_RestrictedHistoryFiltered(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	parentCreatedAt := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	beforeParent := parentCreatedAt.Add(-time.Hour)
	afterParent := parentCreatedAt.Add(time.Hour)

	store.EXPECT().GetRoomMeta(gomock.Any(), "r1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetThreadRoomInfo(gomock.Any(), "parent-1").
		Return(ThreadRoomInfo{Followers: map[string]struct{}{"bob": {}, "carol": {}}}, nil)
	store.EXPECT().
		GetSubscriptionsHistorySince(gomock.Any(), "r1", gomock.InAnyOrder([]string{"alice", "bob", "carol"})).
		Return(map[string]*time.Time{"alice": nil, "bob": &afterParent, "carol": &beforeParent}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "r1", UserID: "u-alice", UserAccount: "alice",
			Content: "a thread reply", CreatedAt: msgTime,
			ThreadParentMessageID:        "parent-1",
			ThreadParentMessageCreatedAt: &parentCreatedAt, // gatekeeper-resolved
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	// bob (history starts after the parent) is dropped; alice (sender) + carol remain.
	require.Len(t, pub.records, 2)
	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("carol")])
	assert.False(t, subjects[subject.UserRoomEvent("bob")], "restricted member must not receive the thread event")
}

// Unresolved parent createdAt (event nil + thread_rooms nil): fail-closed —
// every restricted member is dropped, unrestricted members still receive.
func TestHandleThreadCreated_ChannelRoom_UnresolvedParent_FailClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	anySince := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC) // even an ancient share point fail-closes

	store.EXPECT().GetRoomMeta(gomock.Any(), "r1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetThreadRoomInfo(gomock.Any(), "parent-1").
		Return(ThreadRoomInfo{Followers: map[string]struct{}{"bob": {}, "carol": {}}}, nil) // ParentCreatedAt nil
	store.EXPECT().
		GetSubscriptionsHistorySince(gomock.Any(), "r1", gomock.InAnyOrder([]string{"alice", "bob", "carol"})).
		Return(map[string]*time.Time{"alice": nil, "bob": &anySince, "carol": nil}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "r1", UserID: "u-alice", UserAccount: "alice",
			Content: "a thread reply", CreatedAt: msgTime,
			ThreadParentMessageID: "parent-1", // no ThreadParentMessageCreatedAt
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("carol")])
	assert.False(t, subjects[subject.UserRoomEvent("bob")], "unresolved parent must fail closed")
}

// The event value wins over thread_rooms: thread_rooms has no createdAt but the
// event carries one that predates bob's share point → bob still filtered; the
// event value newer than carol's share point → carol delivered.
func TestHandleThreadCreated_ChannelRoom_EventValueBeatsThreadRooms(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	eventParent := time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC)
	staleParent := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) // thread_rooms value that must LOSE
	sinceBetween := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	store.EXPECT().GetRoomMeta(gomock.Any(), "r1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetThreadRoomInfo(gomock.Any(), "parent-1").
		Return(ThreadRoomInfo{Followers: map[string]struct{}{"bob": {}}, ParentCreatedAt: &staleParent}, nil)
	// Restricted when parentCreatedAt.Before(since). Stale thread_rooms value
	// (Jan 1) is before bob's share point (Mar 1) → would drop bob. Event value
	// (Jun 5) is after it → keeps bob. Delivery to bob proves the event value won.
	store.EXPECT().
		GetSubscriptionsHistorySince(gomock.Any(), "r1", gomock.InAnyOrder([]string{"alice", "bob"})).
		Return(map[string]*time.Time{"alice": nil, "bob": &sinceBetween}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "r1", UserID: "u-alice", UserAccount: "alice",
			Content: "a thread reply", CreatedAt: msgTime,
			ThreadParentMessageID:        "parent-1",
			ThreadParentMessageCreatedAt: &eventParent,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
	}
	assert.True(t, subjects[subject.UserRoomEvent("bob")], "event-carried parent createdAt must win over thread_rooms")
}

// GetSubscriptionsHistorySince failure → handler error → NAK: never deliver
// past a filter we could not evaluate.
func TestHandleThreadCreated_ChannelRoom_HistorySinceLookupError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	store.EXPECT().GetRoomMeta(gomock.Any(), "r1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetThreadRoomInfo(gomock.Any(), "parent-1").
		Return(ThreadRoomInfo{Followers: map[string]struct{}{"bob": {}}}, nil)
	store.EXPECT().
		GetSubscriptionsHistorySince(gomock.Any(), "r1", gomock.Any()).
		Return(nil, errors.New("db error"))

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "r1", UserID: "u-alice", UserAccount: "alice",
			Content: "x", CreatedAt: msgTime, ThreadParentMessageID: "parent-1",
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, false)
	require.Error(t, h.HandleMessage(context.Background(), data))
	assert.Empty(t, pub.records)
}

// DM thread reply: a restricted member with an unresolved parent is dropped
// (fail-closed), the other member and the sender still receive. thread_rooms
// fallback is consulted because the event lacks the value.
func TestHandleThreadCreated_DMRoom_RestrictedMemberFiltered(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	since := time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC)

	// Adapt room/meta fixtures from TestHandleThreadCreated_DMRoom_FansOutToAllMembers (line 2094).
	store.EXPECT().GetRoomMeta(gomock.Any(), testDMRoom.ID).Return(metaOf(testDMRoom), nil)
	subs := []model.Subscription{
		{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}, RoomID: testDMRoom.ID},
		{User: model.SubscriptionUser{ID: "u-bob", Account: "bob"}, RoomID: testDMRoom.ID, HistorySharedSince: &since},
	}
	// dmThreadSkipSet lists subscriptions, then publishDMEvents lists them again.
	store.EXPECT().ListSubscriptions(gomock.Any(), testDMRoom.ID).Return(subs, nil).Times(2)
	// Restricted member present + event lacks the value → thread_rooms fallback.
	store.EXPECT().GetThreadRoomInfo(gomock.Any(), "parent-1").
		Return(ThreadRoomInfo{Followers: map[string]struct{}{}}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: testDMRoom.ID, UserID: "u-alice", UserAccount: "alice",
			Content: "dm thread reply", CreatedAt: msgTime, ThreadParentMessageID: "parent-1",
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")], "sender always receives")
	assert.False(t, subjects[subject.UserRoomEvent("bob")], "restricted DM member must be dropped fail-closed")
}

// DM with no restricted members: no thread_rooms fallback lookup at all.
func TestHandleThreadCreated_DMRoom_NoRestrictedMembers_NoExtraLookup(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	store.EXPECT().GetRoomMeta(gomock.Any(), testDMRoom.ID).Return(metaOf(testDMRoom), nil)
	subs := []model.Subscription{
		{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}, RoomID: testDMRoom.ID},
		{User: model.SubscriptionUser{ID: "u-bob", Account: "bob"}, RoomID: testDMRoom.ID},
	}
	store.EXPECT().ListSubscriptions(gomock.Any(), testDMRoom.ID).Return(subs, nil).Times(2)
	// NO GetThreadRoomInfo expectation: calling it fails the test.
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: testDMRoom.ID, UserID: "u-alice", UserAccount: "alice",
			Content: "dm thread reply", CreatedAt: msgTime, ThreadParentMessageID: "parent-1",
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))
	require.Len(t, pub.records, 2, "both members receive when nobody is restricted")
}
```

Edit and delete paths get the same guarantee:

```go
// Channel thread EDIT: restricted follower dropped from the edit fan-out.
// Edit events never carry the gatekeeper value → thread_rooms fallback supplies it.
func TestHandleThreadUpdated_ChannelRoom_RestrictedHistoryFiltered(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	parentCreatedAt := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	afterParent := parentCreatedAt.Add(time.Hour)
	editedAt := msgTime.Add(time.Minute)

	// Copy the GetRoom fixture from the existing TestHandleThreadUpdated_ChannelRoom
	// test (~line 2160): the updated path fetches the full room, not the meta.
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(testChannelRoom, nil)
	store.EXPECT().GetThreadRoomInfo(gomock.Any(), "parent-1").
		Return(ThreadRoomInfo{Followers: map[string]struct{}{"bob": {}, "carol": {}}, ParentCreatedAt: &parentCreatedAt}, nil)
	store.EXPECT().
		GetSubscriptionsHistorySince(gomock.Any(), "r1", gomock.InAnyOrder([]string{"alice", "bob", "carol"})).
		Return(map[string]*time.Time{"alice": nil, "bob": &afterParent, "carol": nil}, nil)

	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "r1", UserID: "u-alice", UserAccount: "alice",
			Content: "edited reply", CreatedAt: msgTime,
			EditedAt: &editedAt, UpdatedAt: &editedAt,
			ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("carol")])
	assert.False(t, subjects[subject.UserRoomEvent("bob")], "restricted member must not receive the edit event")
}

// DM thread DELETE: restricted member dropped from the delete fan-out.
func TestHandleThreadDeleted_DMRoom_RestrictedMemberFiltered(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	deletedAt := msgTime.Add(time.Minute)
	since := time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC)

	// Copy the GetRoom fixture from the existing DM thread-delete test: the
	// deleted path fetches the full room (room.Accounts drives DM fan-out).
	store.EXPECT().GetRoom(gomock.Any(), testDMRoom.ID).Return(testDMRoom, nil)
	subs := []model.Subscription{
		{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}, RoomID: testDMRoom.ID},
		{User: model.SubscriptionUser{ID: "u-bob", Account: "bob"}, RoomID: testDMRoom.ID, HistorySharedSince: &since},
	}
	store.EXPECT().ListSubscriptions(gomock.Any(), testDMRoom.ID).Return(subs, nil)
	store.EXPECT().GetThreadRoomInfo(gomock.Any(), "parent-1").
		Return(ThreadRoomInfo{Followers: map[string]struct{}{}}, nil) // unresolved → fail-closed

	evt := model.MessageEvent{
		Event:     model.EventDeleted,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: testDMRoom.ID, UserID: "u-alice", UserAccount: "alice",
			CreatedAt: msgTime, UpdatedAt: &deletedAt,
			ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")], "sender always receives")
	assert.False(t, subjects[subject.UserRoomEvent("bob")], "restricted DM member must not receive the delete event")
}
```

Adaptation notes: mirror the exact `GetRoom` return values, event shape (the delete path may require `evt.NewTCount` for the badge — check the existing DM delete test and copy its full expectation set, e.g. a `GetRoom` call for the badge publish), and any `us`/keystore expectations from the existing thread-updated/thread-deleted tests around lines 2160-2400. Fixture note: `testDMRoom`/`testChannelRoom`/`metaOf`/`testUsers` already exist in this file (see lines 2004+, 2094+); reuse them, don't invent new fixtures.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — new tests break on unexpected/missing mock calls and undropped recipients (compile errors first if signatures don't exist yet — that's the same red).

- [ ] **Step 3: Implement the wiring**

All in `broadcast-worker/handler.go`:

**(a) `channelThreadFanOut`** (replaces Task 4's mechanical version):

```go
// channelThreadFanOut resolves the filtered recipient list for a channel
// thread event: thread followers + @-mentions + sender, minus restricted-
// history members who may not see the thread. eventParentCreatedAt is the
// gatekeeper-resolved value off the canonical event; nil falls back to the
// thread_rooms projection, and a still-unresolved parent fail-closes every
// restricted member (matching notification-worker). The sender is exempt.
func (h *Handler) channelThreadFanOut(ctx context.Context, roomID, parentMsgID, sender string, mentions []string, eventParentCreatedAt *time.Time) ([]string, error) {
	info, err := h.store.GetThreadRoomInfo(ctx, parentMsgID)
	if err != nil {
		return nil, fmt.Errorf("get thread room info for parent %s: %w", parentMsgID, err)
	}
	fanOut := threadFanOutAccounts(sender, info.Followers, mentions)
	if len(fanOut) == 0 {
		return fanOut, nil
	}
	parentCreatedAt := eventParentCreatedAt
	if parentCreatedAt == nil {
		parentCreatedAt = info.ParentCreatedAt
	}
	histBy, err := h.store.GetSubscriptionsHistorySince(ctx, roomID, fanOut)
	if err != nil {
		return nil, fmt.Errorf("get history-since for thread fan-out in room %s: %w", roomID, err)
	}
	restricted := restrictedThreadAccounts(sender, parentCreatedAt, histBy)
	if len(restricted) == 0 {
		return fanOut, nil
	}
	filtered := make([]string, 0, len(fanOut))
	for _, acc := range fanOut {
		if _, drop := restricted[acc]; !drop {
			filtered = append(filtered, acc)
		}
	}
	return filtered, nil
}
```

Call sites gain two args:
- `handleThreadCreated` (~213): `h.channelThreadFanOut(ctx, meta.ID, parentMsgID, msg.UserAccount, parsed.Accounts, msg.ThreadParentMessageCreatedAt)`
- `handleThreadUpdated` (~317) and `handleThreadDeleted` (~370): `h.channelThreadFanOut(ctx, room.ID, parentMsgID, msg.UserAccount, parsed.Accounts, msg.ThreadParentMessageCreatedAt)`

**(b) `dmThreadSkipSet`** (new, place near `channelThreadFanOut`):

```go
// dmThreadSkipSet computes the restricted-history skip set for a DM thread
// event. It lists the room's subscriptions and, only when a member actually
// carries historySharedSince, resolves the parent createdAt (event value
// first, thread_rooms fallback — fail-closed when both miss). Returns nil
// when nobody is restricted.
func (h *Handler) dmThreadSkipSet(ctx context.Context, roomID string, msg *model.Message) (map[string]struct{}, error) {
	subs, err := h.store.ListSubscriptions(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions for DM thread filter in room %s: %w", roomID, err)
	}
	histBy := make(map[string]*time.Time, len(subs))
	anyRestricted := false
	for i := range subs {
		histBy[subs[i].User.Account] = subs[i].HistorySharedSince
		if subs[i].HistorySharedSince != nil {
			anyRestricted = true
		}
	}
	if !anyRestricted {
		return nil, nil
	}
	parentCreatedAt := msg.ThreadParentMessageCreatedAt
	if parentCreatedAt == nil {
		info, err := h.store.GetThreadRoomInfo(ctx, msg.ThreadParentMessageID)
		if err != nil {
			return nil, fmt.Errorf("get thread room info for DM thread filter, parent %s: %w", msg.ThreadParentMessageID, err)
		}
		parentCreatedAt = info.ParentCreatedAt
	}
	return restrictedThreadAccounts(msg.UserAccount, parentCreatedAt, histBy), nil
}
```

**(c) `publishDMEvents`** gains `skip map[string]struct{}` (last param). Inside the member loop, after the `isBot` skip:

```go
		if _, drop := skip[account]; drop {
			continue
		}
```

Call sites: `handleCreated` (~186) passes `nil`; `handleThreadCreated` DM branch passes the computed set.

**(d) `publishMutation`** gains `skip map[string]struct{}` (last param). In the DM branch member loop, after the `isBot` skip, the same 3-line drop. All non-thread call sites (294, 486, 532, 557, 602) pass `nil`; the two DM thread branches pass the computed set.

**(e) `handleThreadCreated` DM branch** (~254-263) becomes:

```go
	case model.RoomTypeDM, model.RoomTypeBotDM:
		// DM thread replies fan out to all members except restricted-history
		// members who may not see the thread (fail-closed on an unresolved
		// parent). lastMsgAt is intentionally NOT updated: thread replies must
		// not trigger hasUnread for non-participants.
		skip, err := h.dmThreadSkipSet(ctx, meta.ID, &msg)
		if err != nil {
			return err
		}
		mentionAccounts := withoutSkipped(resolved.Accounts, skip)
		if len(mentionAccounts) > 0 {
			if err := h.store.SetSubscriptionMentions(ctx, meta.ID, mentionAccounts); err != nil {
				return fmt.Errorf("set subscription mentions: %w", err)
			}
		}
		return h.publishDMEvents(ctx, meta, clientMsg, evt.Timestamp, mentionAccounts, skip)
```

with the small helper next to `restrictedThreadAccounts` in `helper.go`:

```go
// withoutSkipped filters accounts present in skip, preserving order.
func withoutSkipped(accounts []string, skip map[string]struct{}) []string {
	if len(skip) == 0 {
		return accounts
	}
	out := make([]string, 0, len(accounts))
	for _, a := range accounts {
		if _, drop := skip[a]; !drop {
			out = append(out, a)
		}
	}
	return out
}
```

(A restricted member must not get a `hasMention` badge for a message they never receive.)

**(f) `handleThreadUpdated` DM branch** (~332-336):

```go
	case model.RoomTypeDM, model.RoomTypeBotDM:
		// DM thread replies are visible to every non-restricted member, so edits
		// fan out to all members minus the restricted-history skip set.
		skip, err := h.dmThreadSkipSet(ctx, room.ID, &msg)
		if err != nil {
			return err
		}
		return h.publishMutation(ctx, room, model.RoomEventMessageEdited, msg.ID, &edit, skip)
```

**(g) `handleThreadDeleted` DM branch** (~383-389): same shape with `model.RoomEventMessageDeleted` and the existing error wrap.

- [ ] **Step 4: Run tests, fix stale expectations, verify green**

Run: `make test SERVICE=broadcast-worker`
Every pre-existing channel-thread test (fan-out > 0) now needs one more expectation; add to each (lines from Task 4: ~2018, ~2067, ~2188, ~2336, ~2389):

```go
store.EXPECT().GetSubscriptionsHistorySince(gomock.Any(), gomock.Any(), gomock.Any()).
	Return(map[string]*time.Time{}, nil).AnyTimes()
```

DM-path tests (`TestHandleThreadCreated_DMRoom_FansOutToAllMembers` etc.) need their `ListSubscriptions` expectation bumped to `.Times(2)` (skip-set + publish). Iterate until green. Then: `make test-integration SERVICE=broadcast-worker`.

- [ ] **Step 5: Verify coverage**

Run: `make test SERVICE=broadcast-worker` then check the package isn't below the floor (the Makefile prints coverage; if not, run the coverage target the Makefile provides). Handlers touched here target 90%+ — the new tests cover resolved/unresolved/event-wins/error/DM/skip-mention paths.

- [ ] **Step 6: Commit**

```bash
git add broadcast-worker/handler.go broadcast-worker/helper.go broadcast-worker/helper_test.go broadcast-worker/handler_test.go
git commit -m "feat(broadcast-worker): fail-closed restricted-history filter on thread fan-out"
```

---

### Task 7: docs + repo-wide verification

**Files:**
- Modify: `docs/client-api.md` (~line 4834 msg.send response table; ~line 4919 broadcast ClientMessage table), `docs/client-api/request-reply.md` (msg.send response mirror), `docs/client-api/events.md` (~line 269 message table mirror)

**Interfaces:** none — documentation of the Task 1 wire change.

- [ ] **Step 1: Re-add the field rows**

In `docs/client-api.md`, msg.send response table (after the `threadParentMessageId` row at ~4834):

```markdown
| `threadParentMessageCreatedAt` | string | Optional. RFC 3339. Server-resolved best-effort for a thread reply; absent when the parent's createdAt could not be resolved at send time. |
```

In the broadcast message (ClientMessage) table (after `threadParentMessageId` at ~4919):

```markdown
| `threadParentMessageCreatedAt` | string | Optional. RFC 3339. Server-resolved best-effort; absent when unresolved at send time. |
```

Mirror both rows into the derived views: `grep -n "threadParentMessageId" docs/client-api/request-reply.md docs/client-api/events.md` and add the matching row after each hit that corresponds to the two tables above (the views must never drift from the canonical doc).

- [ ] **Step 2: Repo-wide verification**

```bash
make lint
make generate   # verify no drift (git status must stay clean after)
make test
make sast
```

Expected: all green. Fix anything that isn't (goimports via `make fmt`).

- [ ] **Step 3: Commit and push**

```bash
git add docs/client-api.md docs/client-api/request-reply.md docs/client-api/events.md
git commit -m "docs(client-api): re-document best-effort threadParentMessageCreatedAt on send reply and events"
git push -u origin claude/thread-parent-fetch-strategy-64x3g8
```

---

## Execution order & dependencies

- Task 1 is the producer; Tasks 2, 3, 6 consume the event field but are individually shippable (they only change *when* fallbacks run).
- Task 4 → Task 5 → Task 6 are strictly ordered (interface → helper → wiring).
- Task 7 last.
- Out of scope everywhere: notification-worker, `SendMessageRequest` schema, thread badge/tcount metadata fan-out, any caching.
