# Thread-Reply History Visibility Gate + PR #245 Follow-ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Gate thread-reply fan-out and thread-subscription creation for @-mentioned users by their room-subscription `HistorySharedSince`, redirect broadcast-worker's DM thread-mention badge to the thread subscription, batch message-worker's thread-message Cassandra writes, and document `eventTimestamp` in the client API.

**Architecture:** Mirror `notification-worker.isRestricted` as a small per-service `mentionVisible` helper. `broadcast-worker` filters mention-driven recipients before fan-out and marks the *thread* subscription's `hasMention`; `message-worker` skips thread-subscription/`replyAccounts` creation for restricted mentionees. Both read `HistorySharedSince` from the `subscriptions` collection via a precise projection. The Cassandra thread-write path is converted from 2–3 sequential INSERTs to one `UnloggedBatch`, matching `SaveMessage`.

**Tech Stack:** Go 1.25, MongoDB (`mongo-driver/v2`), Cassandra (`gocql`), NATS JetStream, `go.uber.org/mock`, `testify`, `testcontainers-go` via `pkg/testutil`.

## Global Constraints

- Language: Go 1.25; monorepo, single root `go.mod`; services are flat `package main` dirs.
- Use `make` targets only — never raw `go`. Tests run with `-race` (Makefile handles it).
- TDD mandatory: Red → Green → Refactor → Commit for every code change.
- Minimum 80% coverage; target 90%+ for handlers/stores.
- Mocks are generated (`go.uber.org/mock`) into `mock_store_test.go` / `mock_userstore_test.go` — never hand-edit; regenerate with `make generate SERVICE=<name>`.
- Logging: `log/slog` JSON only; every log line includes `"request_id", natsutil.RequestIDFromContext(ctx)`; never log message bodies/tokens.
- Errors: wrap with context `fmt.Errorf("desc: %w", err)`; infra failures return raw wrapped errors (collapse to `internal` at the boundary).
- MongoDB: always project precisely; check `mongo.ErrNoDocuments` when a miss is expected; no `$lookup`.
- Integration tests: `//go:build integration`, `package main`, containers from `pkg/testutil`, `TestMain` calls `testutil.RunTests(m)`.
- All work on branch `claude/broadcast-worker-thread-visibility-d4p1g2`. Commit after every green task.
- Gates before push: `make lint`, `make test`, relevant `make test-integration`, `make sast`.

**Visibility rule (verbatim):** allowed ⟺ `HistorySharedSince == nil` OR `ParentCreatedAt >= HistorySharedSince`. Excluded when `HistorySharedSince` set AND (`ParentCreatedAt == nil` OR `ParentCreatedAt < HistorySharedSince`).

---

### Task 1: broadcast-worker `mentionVisible` helper

**Files:**
- Modify: `broadcast-worker/helper.go`
- Test: `broadcast-worker/helper_test.go`

**Interfaces:**
- Produces: `func mentionVisible(historySharedSince, parentCreatedAt *time.Time) bool`

- [ ] **Step 1: Write the failing test**

Add to `broadcast-worker/helper_test.go` (add `"time"` and `"github.com/stretchr/testify/assert"` imports if absent):

```go
func TestMentionVisible(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	before := base.Add(-time.Hour)
	after := base.Add(time.Hour)
	tests := []struct {
		name    string
		hss     *time.Time
		parent  *time.Time
		visible bool
	}{
		{"nil window is unrestricted", nil, &base, true},
		{"nil window nil parent still unrestricted", nil, nil, true},
		{"parent after window is visible", &before, &base, true},
		{"parent equal to window is visible", &base, &base, true},
		{"parent before window is hidden", &after, &base, false},
		{"set window with nil parent is hidden", &before, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.visible, mentionVisible(tt.hss, tt.parent))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `undefined: mentionVisible`.

- [ ] **Step 3: Write minimal implementation**

Append to `broadcast-worker/helper.go` (add `"time"` to its import block):

```go
// mentionVisible reports whether a mentioned user whose room subscription carries
// historySharedSince may see a thread reply whose parent was created at
// parentCreatedAt. Mirrors notification-worker.isRestricted (inverted to
// "visible"): a nil window means full access; a set window with a missing or
// older parent timestamp means no access.
func mentionVisible(historySharedSince, parentCreatedAt *time.Time) bool {
	if historySharedSince == nil {
		return true
	}
	if parentCreatedAt == nil {
		return false
	}
	return !parentCreatedAt.Before(*historySharedSince)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/helper.go broadcast-worker/helper_test.go
git commit -m "broadcast-worker: add mentionVisible history-window helper"
```

---

### Task 2: broadcast-worker store — history-window read + thread-sub mention write

**Files:**
- Modify: `broadcast-worker/store.go` (Store interface)
- Modify: `broadcast-worker/store_mongo.go` (mongoStore impl + `threadSubCol`)
- Modify: `broadcast-worker/main.go:108` (pass `thread_subscriptions` collection)
- Regenerate: `broadcast-worker/mock_store_test.go`
- Test: `broadcast-worker/integration_test.go`

**Interfaces:**
- Produces:
  - `GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error)`
  - `SetThreadSubscriptionMentions(ctx context.Context, parentMessageID string, accounts []string) error`

**Index note:** `SetThreadSubscriptionMentions` filters `thread_subscriptions` by
`(parentMessageId, userAccount)`. The only existing index on that collection is the
unique `(threadRoomId, userAccount)` created by message-worker — it does **not**
serve a `parentMessageId`-led filter. This task adds a non-unique
`(parentMessageId, userAccount)` index to broadcast-worker's `EnsureIndexes`
(idempotent, co-exists with message-worker's unique index). `GetHistorySharedSince`
queries `subscriptions` by `(roomId, u.account)`, already covered by that
collection's standard `roomId`/membership indexes — no new index needed there.

- [ ] **Step 1: Add methods to the Store interface**

In `broadcast-worker/store.go`, inside `type Store interface { ... }`, add:

```go
	// GetHistorySharedSince returns each account's room-subscription historySharedSince
	// (nil when unrestricted; absent from the map when the account has no subscription).
	GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error)
	// SetThreadSubscriptionMentions sets hasMention=true on the thread_subscriptions
	// rows for the given accounts under the thread identified by parentMessageID.
	SetThreadSubscriptionMentions(ctx context.Context, parentMessageID string, accounts []string) error
```

- [ ] **Step 2: Regenerate the mock and confirm it fails to build**

Run: `make generate SERVICE=broadcast-worker && make test SERVICE=broadcast-worker`
Expected: FAIL — `*mongoStore` does not implement `Store` (missing two methods).

- [ ] **Step 3: Add `threadSubCol` to mongoStore and implement the methods**

In `broadcast-worker/store_mongo.go`:

Add the field to the struct:

```go
type mongoStore struct {
	roomCol       *mongo.Collection
	subCol        *mongo.Collection
	threadRoomCol *mongo.Collection
	threadSubCol  *mongo.Collection
	valkey        valkeyutil.Client
	metaTTL       time.Duration
	metaRec       roommetacache.Recorder
}
```

Update the constructor signature and body:

```go
func NewMongoStore(roomCol, subCol, threadRoomCol, threadSubCol *mongo.Collection, valkey valkeyutil.Client, metaTTL time.Duration) *mongoStore {
	return &mongoStore{
		roomCol:       roomCol,
		subCol:        subCol,
		threadRoomCol: threadRoomCol,
		threadSubCol:  threadSubCol,
		valkey:        valkey,
		metaTTL:       metaTTL,
		metaRec:       cachemetrics.For("roommeta", "l2"),
	}
}
```

Add the backing index to `EnsureIndexes` (alongside the existing `thread_rooms` index), so the `parentMessageId`-led update filter is index-served:

```go
	if _, err := m.threadSubCol.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "parentMessageId", Value: 1}, {Key: "userAccount", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure thread_subscriptions (parentMessageId, userAccount) index: %w", err)
	}
```

Append the two methods:

```go
func (m *mongoStore) GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error) {
	out := make(map[string]*time.Time, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	filter := bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}}
	opts := options.Find().SetProjection(bson.M{"u.account": 1, "historySharedSince": 1, "_id": 0})
	cursor, err := m.subCol.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("query history windows for room %s: %w", roomID, err)
	}
	defer cursor.Close(ctx)
	// Minimal decode shape: the projection returns only u.account + historySharedSince,
	// so decode just those rather than the full model.SubscriptionUser (whose other
	// fields would silently be zero-valued).
	var rows []struct {
		User struct {
			Account string `bson:"account"`
		} `bson:"u"`
		HistorySharedSince *time.Time `bson:"historySharedSince"`
	}
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode history windows: %w", err)
	}
	for i := range rows {
		out[rows[i].User.Account] = rows[i].HistorySharedSince
	}
	return out, nil
}

func (m *mongoStore) SetThreadSubscriptionMentions(ctx context.Context, parentMessageID string, accounts []string) error {
	if len(accounts) == 0 {
		return nil
	}
	filter := bson.M{"parentMessageId": parentMessageID, "userAccount": bson.M{"$in": accounts}}
	update := bson.M{"$set": bson.M{"hasMention": true}}
	if _, err := m.threadSubCol.UpdateMany(ctx, filter, update); err != nil {
		return fmt.Errorf("set thread subscription mentions for parent %s: %w", parentMessageID, err)
	}
	return nil
}
```

- [ ] **Step 4: Update main.go wiring**

In `broadcast-worker/main.go` line 108, add the `thread_subscriptions` collection as the 4th arg:

```go
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), db.Collection("thread_subscriptions"), metaValkey, cfg.RoomMetaL2TTL)
```

- [ ] **Step 5: Verify build/unit green**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS (existing tests unaffected; new methods compile).

- [ ] **Step 6: Write integration tests**

Add to `broadcast-worker/integration_test.go` (follow the existing file's `testutil.MongoDB(t, ...)` + `NewMongoStore(...)` setup pattern; pass the new `thread_subscriptions` collection):

```go
func TestMongoStore_GetHistorySharedSince(t *testing.T) {
	db := testutil.MongoDB(t, "bw_hss")
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"),
		db.Collection("thread_rooms"), db.Collection("thread_subscriptions"), nil, time.Minute)
	ctx := context.Background()
	shared := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		bson.M{"roomId": "r1", "u": bson.M{"_id": "u-al", "account": "alice"}, "historySharedSince": shared},
		bson.M{"roomId": "r1", "u": bson.M{"_id": "u-bo", "account": "bob"}},
	})
	require.NoError(t, err)

	got, err := store.GetHistorySharedSince(ctx, "r1", []string{"alice", "bob", "carol"})
	require.NoError(t, err)
	require.NotNil(t, got["alice"])
	assert.Equal(t, shared.UnixMilli(), got["alice"].UTC().UnixMilli())
	bobWindow, bobPresent := got["bob"]
	require.True(t, bobPresent, "member with a nil window must still be present in the map")
	assert.Nil(t, bobWindow, "member without window decodes to nil")
	_, present := got["carol"]
	assert.False(t, present, "non-member is absent from the map")
}

func TestMongoStore_SetThreadSubscriptionMentions(t *testing.T) {
	db := testutil.MongoDB(t, "bw_tsm")
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"),
		db.Collection("thread_rooms"), db.Collection("thread_subscriptions"), nil, time.Minute)
	ctx := context.Background()
	_, err := db.Collection("thread_subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "s1", "parentMessageId": "p1", "userAccount": "alice", "hasMention": false},
		bson.M{"_id": "s2", "parentMessageId": "p1", "userAccount": "bob", "hasMention": false},
	})
	require.NoError(t, err)

	require.NoError(t, store.SetThreadSubscriptionMentions(ctx, "p1", []string{"alice"}))

	var alice, bob struct {
		HasMention bool `bson:"hasMention"`
	}
	require.NoError(t, db.Collection("thread_subscriptions").FindOne(ctx, bson.M{"_id": "s1"}).Decode(&alice))
	require.NoError(t, db.Collection("thread_subscriptions").FindOne(ctx, bson.M{"_id": "s2"}).Decode(&bob))
	assert.True(t, alice.HasMention, "targeted account flipped to true")
	assert.False(t, bob.HasMention, "untargeted account unchanged")
}
```

- [ ] **Step 7: Run integration tests**

Run: `make test-integration SERVICE=broadcast-worker`
Expected: PASS (both new tests plus existing suite).

- [ ] **Step 8: Commit**

```bash
git add broadcast-worker/store.go broadcast-worker/store_mongo.go broadcast-worker/main.go broadcast-worker/mock_store_test.go broadcast-worker/integration_test.go
git commit -m "broadcast-worker: add GetHistorySharedSince + SetThreadSubscriptionMentions store methods"
```

---

### Task 3: broadcast-worker handler — gate channel fan-out + redirect DM mention badge

**Files:**
- Modify: `broadcast-worker/handler.go` (`handleThreadCreated` + new `allowedThreadMentions` method)
- Test: `broadcast-worker/handler_test.go`

**Interfaces:**
- Consumes: `mentionVisible` (Task 1), `Store.GetHistorySharedSince` + `Store.SetThreadSubscriptionMentions` (Task 2), existing `channelThreadFanOut`, `publishDMEvents`, `publishToThreadAccounts`.
- Produces: `func (h *Handler) allowedThreadMentions(ctx context.Context, roomID string, mentions []string, parentCreatedAt *time.Time) ([]string, error)`

- [ ] **Step 1: Write the failing tests**

Add to `broadcast-worker/handler_test.go`. These mirror the existing `handleThreadCreated` channel/DM tests (reuse their mock-setup helpers); the new assertions are the `GetHistorySharedSince` expectation and the restricted-account exclusion. Example for the channel path:

```go
func TestHandleThreadCreated_ChannelExcludesRestrictedMention(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &capturePublisher{} // existing test double in this package
	h := NewHandler(store, us, pub, nil, false)

	parentAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	joinedAfter := parentAt.Add(time.Hour)
	evt := threadReplyEvent(t, "@alice @bob @carol hi") // existing helper; sets ThreadParentMessageID, TShow=false, ThreadParentMessageCreatedAt=parentAt, RoomID=r1

	store.EXPECT().GetRoomMeta(gomock.Any(), "r1").Return(channelMeta("r1"), nil)
	// alice: member, full access → included. bob: member, joined after parent → excluded.
	// carol: absent from the map → non-member → excluded.
	store.EXPECT().GetHistorySharedSince(gomock.Any(), "r1", gomock.Any()).
		Return(map[string]*time.Time{"alice": nil, "bob": &joinedAfter}, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), evt.Message.ThreadParentMessageID).
		Return(map[string]struct{}{}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(nil, nil)

	require.NoError(t, h.handleCreated(context.Background(), evt))

	got := pub.recipients() // accounts published to (existing helper on capturePublisher)
	assert.Contains(t, got, "alice", "unrestricted member mentionee receives the reply")
	assert.NotContains(t, got, "bob", "member who joined after the parent is excluded")
	assert.NotContains(t, got, "carol", "non-member mentionee is excluded")
}
```

And for the DM path — assert the *thread* subscription is marked and the room subscription is not:

```go
func TestHandleThreadCreated_DMMarksThreadSubscriptionMention(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &capturePublisher{}
	h := NewHandler(store, us, pub, nil, false)

	evt := threadReplyEvent(t, "@bob hi") // RoomID=dm1
	store.EXPECT().GetRoomMeta(gomock.Any(), "dm1").Return(dmMeta("dm1"), nil)
	store.EXPECT().GetHistorySharedSince(gomock.Any(), "dm1", gomock.Any()).
		Return(map[string]*time.Time{"bob": nil}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(nil, nil)
	store.EXPECT().ListSubscriptions(gomock.Any(), "dm1").Return(dmSubs("alice", "bob"), nil)
	store.EXPECT().SetThreadSubscriptionMentions(gomock.Any(), evt.Message.ThreadParentMessageID, []string{"bob"}).Return(nil)
	// SetSubscriptionMentions (room sub) must NOT be called — no EXPECT registered; gomock fails on any unexpected call.

	require.NoError(t, h.handleCreated(context.Background(), evt))
}
```

Note: if the existing `handleThreadCreated` DM tests call `SetSubscriptionMentions`, update those expectations to `SetThreadSubscriptionMentions` as part of this task.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — unexpected/missing calls (`GetHistorySharedSince` not invoked; `SetThreadSubscriptionMentions` not invoked).

- [ ] **Step 3: Implement the handler changes**

In `broadcast-worker/handler.go`, add the helper method (near `channelThreadFanOut`):

```go
// allowedThreadMentions filters mentioned accounts down to room members permitted
// to see the thread by their history window (mentionVisible). Accounts with no
// room subscription (non-members) are excluded. Returns nil for an empty input.
func (h *Handler) allowedThreadMentions(ctx context.Context, roomID string, mentions []string, parentCreatedAt *time.Time) ([]string, error) {
	if len(mentions) == 0 {
		return nil, nil
	}
	windows, err := h.store.GetHistorySharedSince(ctx, roomID, mentions)
	if err != nil {
		return nil, fmt.Errorf("get history windows for room %s: %w", roomID, err)
	}
	allowed := make([]string, 0, len(mentions))
	for _, acc := range mentions {
		hss, isMember := windows[acc]
		// Exclude non-members (no room subscription) outright; keep a member only when
		// their history window admits the thread's parent.
		if !isMember || !mentionVisible(hss, parentCreatedAt) {
			continue
		}
		allowed = append(allowed, acc)
	}
	return allowed, nil
}
```

In `handleThreadCreated`, change the channel `fanOut` computation to gate mentions first:

```go
	var fanOut []string
	if meta.Type == model.RoomTypeChannel {
		allowedMentions, err := h.allowedThreadMentions(ctx, msg.RoomID, parsed.Accounts, msg.ThreadParentMessageCreatedAt)
		if err != nil {
			return err
		}
		fanOut, err = h.channelThreadFanOut(ctx, parentMsgID, msg.UserAccount, allowedMentions)
		if err != nil {
			return fmt.Errorf("channel thread fan-out for parent %s: %w", parentMsgID, err)
		}
		if len(fanOut) == 0 {
			slog.DebugContext(ctx, "no thread subscribers to notify for thread reply",
				"parentMessageID", parentMsgID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			return nil
		}
	}
```

In the DM branch of the `switch meta.Type`, replace the `SetSubscriptionMentions` block with a gated thread-subscription mention write:

```go
	case model.RoomTypeDM, model.RoomTypeBotDM:
		allowedMentions, err := h.allowedThreadMentions(ctx, meta.ID, resolved.Accounts, msg.ThreadParentMessageCreatedAt)
		if err != nil {
			return err
		}
		if len(allowedMentions) > 0 {
			if err := h.store.SetThreadSubscriptionMentions(ctx, parentMsgID, allowedMentions); err != nil {
				return fmt.Errorf("set thread subscription mentions: %w", err)
			}
		}
		return h.publishDMEvents(ctx, meta, clientMsg, evt.Timestamp, resolved.Accounts)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS (new tests + adjusted existing DM tests).

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/handler.go broadcast-worker/handler_test.go
git commit -m "broadcast-worker: gate thread mention fan-out by history window; mark thread-sub hasMention on DM path"
```

---

### Task 4: message-worker `mentionVisible` helper

**Files:**
- Create: `message-worker/helper.go`
- Test: `message-worker/helper_test.go`

**Interfaces:**
- Produces: `func mentionVisible(historySharedSince, parentCreatedAt *time.Time) bool` (identical to Task 1)

- [ ] **Step 1: Write the failing test**

Create `message-worker/helper_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMentionVisible(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	before := base.Add(-time.Hour)
	after := base.Add(time.Hour)
	tests := []struct {
		name    string
		hss     *time.Time
		parent  *time.Time
		visible bool
	}{
		{"nil window is unrestricted", nil, &base, true},
		{"parent after window is visible", &before, &base, true},
		{"parent equal to window is visible", &base, &base, true},
		{"parent before window is hidden", &after, &base, false},
		{"set window with nil parent is hidden", &before, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.visible, mentionVisible(tt.hss, tt.parent))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=message-worker`
Expected: FAIL — `undefined: mentionVisible`.

- [ ] **Step 3: Write minimal implementation**

Create `message-worker/helper.go`:

```go
package main

import "time"

// mentionVisible reports whether a mentioned user whose room subscription carries
// historySharedSince may see a thread reply whose parent was created at
// parentCreatedAt. Mirrors notification-worker.isRestricted (inverted to
// "visible"): a nil window means full access; a set window with a missing or
// older parent timestamp means no access.
func mentionVisible(historySharedSince, parentCreatedAt *time.Time) bool {
	if historySharedSince == nil {
		return true
	}
	if parentCreatedAt == nil {
		return false
	}
	return !parentCreatedAt.Before(*historySharedSince)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=message-worker`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add message-worker/helper.go message-worker/helper_test.go
git commit -m "message-worker: add mentionVisible history-window helper"
```

---

### Task 5: message-worker ThreadStore — history-window read

**Files:**
- Modify: `message-worker/store.go` (ThreadStore interface)
- Modify: `message-worker/store_mongo.go` (`threadStoreMongo` + `subscriptions` collection)
- Regenerate: `message-worker/mock_store_test.go`
- Test: `message-worker/integration_test.go`

**Interfaces:**
- Produces: `GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error)`

- [ ] **Step 1: Add the method to the ThreadStore interface**

In `message-worker/store.go`, inside `type ThreadStore interface { ... }`, add:

```go
	// GetHistorySharedSince returns each account's room-subscription historySharedSince
	// (nil when unrestricted; absent when the account has no subscription in the room).
	GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error)
```

- [ ] **Step 2: Regenerate the mock and confirm build failure**

Run: `make generate SERVICE=message-worker && make test SERVICE=message-worker`
Expected: FAIL — `*threadStoreMongo` does not implement `ThreadStore`.

- [ ] **Step 3: Add the `subscriptions` collection and implement the method**

In `message-worker/store_mongo.go`, add the field and wire it in the constructor:

```go
type threadStoreMongo struct {
	threadRooms         *mongo.Collection
	threadSubscriptions *mongo.Collection
	subscriptions       *mongo.Collection
}
```

```go
func newThreadStoreMongo(db *mongo.Database) *threadStoreMongo {
	return &threadStoreMongo{
		threadRooms:         db.Collection("thread_rooms"),
		threadSubscriptions: db.Collection("thread_subscriptions"),
		subscriptions:       db.Collection("subscriptions"),
	}
}
```

Add the method (add `"go.mongodb.org/mongo-driver/v2/mongo/options"` to imports if not present — it is already imported in this file):

```go
func (s *threadStoreMongo) GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error) {
	out := make(map[string]*time.Time, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	filter := bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}}
	opts := options.Find().SetProjection(bson.M{"u.account": 1, "historySharedSince": 1, "_id": 0})
	cursor, err := s.subscriptions.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("query history windows for room %s: %w", roomID, err)
	}
	defer cursor.Close(ctx)
	// Minimal decode shape: the projection returns only u.account + historySharedSince,
	// so decode just those rather than the full model.SubscriptionUser (whose other
	// fields would silently be zero-valued).
	var rows []struct {
		User struct {
			Account string `bson:"account"`
		} `bson:"u"`
		HistorySharedSince *time.Time `bson:"historySharedSince"`
	}
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode history windows: %w", err)
	}
	for i := range rows {
		out[rows[i].User.Account] = rows[i].HistorySharedSince
	}
	return out, nil
}
```

- [ ] **Step 4: Verify build/unit green**

Run: `make test SERVICE=message-worker`
Expected: PASS.

- [ ] **Step 5: Write the integration test**

Add to `message-worker/integration_test.go` (use the existing `testutil.MongoDB(t, ...)` pattern in that file; construct the store via `newThreadStoreMongo(db)`):

```go
func TestThreadStoreMongo_GetHistorySharedSince(t *testing.T) {
	db := testutil.MongoDB(t, "mw_hss")
	store := newThreadStoreMongo(db)
	ctx := context.Background()
	shared := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		bson.M{"roomId": "r1", "u": bson.M{"_id": "u-al", "account": "alice"}, "historySharedSince": shared},
		bson.M{"roomId": "r1", "u": bson.M{"_id": "u-bo", "account": "bob"}},
	})
	require.NoError(t, err)

	got, err := store.GetHistorySharedSince(ctx, "r1", []string{"alice", "bob", "carol"})
	require.NoError(t, err)
	require.NotNil(t, got["alice"])
	assert.Equal(t, shared.UnixMilli(), got["alice"].UTC().UnixMilli())
	assert.Nil(t, got["bob"])
	_, present := got["carol"]
	assert.False(t, present)
}
```

- [ ] **Step 6: Run integration tests**

Run: `make test-integration SERVICE=message-worker`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add message-worker/store.go message-worker/store_mongo.go message-worker/mock_store_test.go message-worker/integration_test.go
git commit -m "message-worker: add ThreadStore.GetHistorySharedSince over subscriptions"
```

---

### Task 6: message-worker handler — gate thread-mention subscription/replyAccounts

**Files:**
- Modify: `message-worker/handler.go` (`markThreadMentions`)
- Test: `message-worker/handler_test.go`

**Interfaces:**
- Consumes: `mentionVisible` (Task 4), `ThreadStore.GetHistorySharedSince` (Task 5), existing `buildThreadSubscription`, `MarkThreadSubscriptionMention`, `publishThreadSubInboxIfRemote`, `AddReplyAccounts`.

- [ ] **Step 1: Write the failing test**

Add to `message-worker/handler_test.go` (reuse the existing `markThreadMentions` test scaffolding + mock ThreadStore):

```go
func TestMarkThreadMentions_SkipsRestrictedMentionee(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := NewMockThreadStore(ctrl)
	h := NewHandler(NewMockStore(ctrl), NewMockUserStore(ctrl), ts, "siteA", nil)

	parentAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	joinedAfter := parentAt.Add(time.Hour)
	msg := threadReplyMessage(t) // existing helper: RoomID=r1, ThreadParentMessageCreatedAt=&parentAt, UserID=u-sender
	msg.Mentions = []model.Participant{
		{UserID: "u-al", Account: "alice", SiteID: "siteA"},
		{UserID: "u-bo", Account: "bob", SiteID: "siteA"},
		{UserID: "u-ca", Account: "carol", SiteID: "siteA"},
	}

	// alice: member, full access → kept. bob: member, joined after parent → skipped.
	// carol: absent from the map → non-member → skipped.
	ts.EXPECT().GetHistorySharedSince(gomock.Any(), "r1", gomock.Any()).
		Return(map[string]*time.Time{"alice": nil, "bob": &joinedAfter}, nil)
	ts.EXPECT().MarkThreadSubscriptionMention(gomock.Any(), gomock.Cond(func(x any) bool {
		return x.(*model.ThreadSubscription).UserAccount == "alice"
	})).Return(nil)
	ts.EXPECT().AddReplyAccounts(gomock.Any(), "tr1", []string{"alice"}).Return(nil)

	require.NoError(t, h.markThreadMentions(context.Background(), msg, "tr1", "siteA", false))
}
```

(If the existing suite has a "marks all mentions" test that now expects `GetHistorySharedSince`, add that expectation returning an all-`nil` map so every mentionee stays included.)

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=message-worker`
Expected: FAIL — `GetHistorySharedSince` not called / `bob` unexpectedly marked.

- [ ] **Step 3: Implement the gate**

Rewrite `markThreadMentions` in `message-worker/handler.go`:

```go
func (h *Handler) markThreadMentions(ctx context.Context, msg *model.Message, threadRoomID, eventSiteID string, isMigration bool) error {
	var candidates []model.Participant
	for i := range msg.Mentions {
		p := &msg.Mentions[i]
		if p.Account == "all" {
			continue
		}
		if p.UserID == msg.UserID {
			continue
		}
		candidates = append(candidates, *p)
	}
	if len(candidates) == 0 {
		return nil
	}

	accounts := make([]string, 0, len(candidates))
	for i := range candidates {
		accounts = append(accounts, candidates[i].Account)
	}
	windows, err := h.threadStore.GetHistorySharedSince(ctx, msg.RoomID, accounts)
	if err != nil {
		return fmt.Errorf("get history windows for thread mentions: %w", err)
	}

	var mentionedAccounts []string
	for i := range candidates {
		p := &candidates[i]
		hss, isMember := windows[p.Account]
		// Skip non-members (no room subscription) and members whose history window
		// starts after the thread's parent — neither may see the parent, so they are
		// not subscribed, not inboxed, and not added as a follower.
		if !isMember || !mentionVisible(hss, msg.ThreadParentMessageCreatedAt) {
			continue
		}
		if !isMigration {
			sub := h.buildThreadSubscription(msg, threadRoomID, p.UserID, p.Account, eventSiteID, msg.CreatedAt)
			sub.HasMention = true
			if err := h.threadStore.MarkThreadSubscriptionMention(ctx, sub); err != nil {
				return fmt.Errorf("mark thread subscription mention for user %s: %w", p.UserID, err)
			}
			if err := h.publishThreadSubInboxIfRemote(ctx, sub, p.SiteID, msg.ID); err != nil {
				return fmt.Errorf("publish thread mention inbox for user %s: %w", p.UserID, err)
			}
		}
		mentionedAccounts = append(mentionedAccounts, p.Account)
	}
	if len(mentionedAccounts) > 0 {
		if err := h.threadStore.AddReplyAccounts(ctx, threadRoomID, mentionedAccounts); err != nil {
			return fmt.Errorf("add mentioned accounts to thread room replyAccounts: %w", err)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=message-worker`
Expected: PASS (new test + adjusted existing markThreadMentions tests).

- [ ] **Step 5: Commit**

```bash
git add message-worker/handler.go message-worker/handler_test.go
git commit -m "message-worker: gate thread mention subscription + replyAccounts by history window"
```

---

### Task 7: message-worker store — batch the thread-message Cassandra writes

**Files:**
- Modify: `message-worker/store_cassandra.go` (`SaveThreadMessage`, `saveThreadMessageEncrypted`)
- Test: `message-worker/integration_test.go` (existing idempotency tests validate; add a batch-write assertion if not covered)

**Interfaces:**
- Unchanged signatures: `SaveThreadMessage(ctx, msg, sender, siteID, threadRoomID) (*int, error)`.

- [ ] **Step 1: Confirm the existing idempotency integration tests exist and pass (baseline)**

Run: `make test-integration SERVICE=message-worker`
Expected: PASS — includes `TestCassandraStore_SaveThreadMessage_IdempotentOnRedelivery` and `TestSaveThreadMessage_EncryptedPath_SkipsTcountOnRedelivery` (from PR #245). These are the regression guard for this refactor.

- [ ] **Step 2: Refactor `SaveThreadMessage` to a single UnloggedBatch**

Replace the two/three sequential `s.cassSession.Query(...).Exec()` calls in `SaveThreadMessage` (the plaintext path, after the `s.cipher != nil` guard) with:

```go
	mentions := toMentionSet(msg.Mentions)

	batch := s.cassSession.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(
		`INSERT INTO messages_by_id
		 (message_id, created_at, room_id, sender, msg, site_id, updated_at, mentions,
		  thread_room_id, thread_parent_id, thread_parent_created_at, type, sys_msg_data, tshow, quoted_parent_message,
		  attachments, card, card_action)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, msg.Content, siteID, msg.CreatedAt, mentions,
		threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction,
	)
	batch.Query(
		`INSERT INTO thread_messages_by_thread
		 (thread_room_id, created_at, message_id, room_id, thread_parent_id, sender, msg,
		  site_id, updated_at, mentions, type, sys_msg_data, tshow, quoted_parent_message,
		  attachments, card, card_action)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, msg.CreatedAt, msg.ID, msg.RoomID, msg.ThreadParentMessageID,
		sender, msg.Content, siteID, msg.CreatedAt, mentions,
		msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction,
	)
	if msg.TShow {
		batch.Query(
			`INSERT INTO messages_by_room
			 (room_id, bucket, created_at, message_id, sender, msg, site_id, updated_at, mentions,
			  thread_room_id, thread_parent_id, thread_parent_created_at, type, sys_msg_data, tshow, quoted_parent_message,
			  attachments, card, card_action)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			msg.RoomID, s.bucket.Of(msg.CreatedAt), msg.CreatedAt, msg.ID, sender, msg.Content, siteID, msg.CreatedAt, mentions,
			threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
			msg.Attachments, msg.Card, msg.CardAction,
		)
	}
	if err := s.cassSession.ExecuteBatch(batch); err != nil {
		return nil, fmt.Errorf("save thread message %s: %w", msg.ID, err)
	}

	return s.countAndSetParentTcount(ctx, msg, threadRoomID)
```

- [ ] **Step 3: Refactor `saveThreadMessageEncrypted` to a single UnloggedBatch**

Replace the sequential `Query(...).Exec()` calls in `saveThreadMessageEncrypted` (after `encMeta`/`mentions` are computed) with a batch of the two INSERTs plus the conditional TShow INSERT, then `ExecuteBatch`, keeping the encrypted-NULL body bindings verbatim:

```go
	batch := s.cassSession.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(
		`INSERT INTO messages_by_id
		 (message_id, created_at, room_id, sender, site_id, updated_at, mentions,
		  thread_room_id, thread_parent_id, thread_parent_created_at, type, tshow,
		  quoted_parent_message, sys_msg_data,
		  msg, attachments, card, card_action,
		  enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, siteID, msg.CreatedAt, mentions,
		threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.TShow,
		cm.QuotedParentMessage, msg.SysMsgData, payload, encMeta,
	)
	batch.Query(
		`INSERT INTO thread_messages_by_thread
		 (thread_room_id, created_at, message_id, room_id, thread_parent_id,
		  sender, site_id, updated_at, mentions, type, tshow, quoted_parent_message, sys_msg_data,
		  msg, attachments, card, card_action,
		  enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, ?, ?)`,
		threadRoomID, msg.CreatedAt, msg.ID, msg.RoomID, msg.ThreadParentMessageID,
		sender, siteID, msg.CreatedAt, mentions, msg.Type, msg.TShow, cm.QuotedParentMessage, msg.SysMsgData,
		payload, encMeta,
	)
	if msg.TShow {
		batch.Query(
			`INSERT INTO messages_by_room
			 (room_id, bucket, created_at, message_id, sender, site_id, updated_at, mentions,
			  thread_room_id, thread_parent_id, thread_parent_created_at, type, tshow,
			  quoted_parent_message, sys_msg_data,
			  msg, attachments, card, card_action,
			  enc_payload, enc_meta)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, ?, ?)`,
			msg.RoomID, s.bucket.Of(msg.CreatedAt), msg.CreatedAt, msg.ID, sender, siteID, msg.CreatedAt, mentions,
			threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.TShow,
			cm.QuotedParentMessage, msg.SysMsgData, payload, encMeta,
		)
	}
	if err := s.cassSession.ExecuteBatch(batch); err != nil {
		return nil, fmt.Errorf("save thread message %s: %w", msg.ID, err)
	}

	return s.countAndSetParentTcount(ctx, msg, threadRoomID)
```

- [ ] **Step 4: Add a TShow batch-coverage integration test (if not already covered)**

Add to `message-worker/integration_test.go` — asserts the batch writes all three tables for a TShow reply:

```go
func TestCassandraStore_SaveThreadMessage_TShowWritesAllTables(t *testing.T) {
	ks, sess, _ := testutil.CassandraKeyspace(t, "mw_batch")
	_ = ks
	store := NewCassandraStore(sess, msgbucket.Hours(72), nil)
	ctx := context.Background()
	msg := seedThreadReply(t) // existing helper: sets ID, RoomID, CreatedAt, ThreadParentMessageID, ThreadParentMessageCreatedAt
	msg.TShow = true

	_, err := store.SaveThreadMessage(ctx, msg, &cassParticipant{ID: "u1", Account: "alice"}, "siteA", "tr1")
	require.NoError(t, err)

	var count int
	require.NoError(t, sess.Query(`SELECT COUNT(*) FROM messages_by_id WHERE message_id = ?`, msg.ID).WithContext(ctx).Scan(&count))
	assert.Equal(t, 1, count, "messages_by_id row written")
	require.NoError(t, sess.Query(`SELECT COUNT(*) FROM thread_messages_by_thread WHERE thread_room_id = ?`, "tr1").WithContext(ctx).Scan(&count))
	assert.Equal(t, 1, count, "thread_messages_by_thread row written")
	require.NoError(t, sess.Query(`SELECT COUNT(*) FROM messages_by_room WHERE room_id = ? AND bucket = ?`,
		msg.RoomID, msgbucket.Hours(72).Of(msg.CreatedAt)).WithContext(ctx).Scan(&count))
	assert.Equal(t, 1, count, "tshow mirror row written to messages_by_room")
}
```

(If `seedThreadReply`/equivalent helpers differ in the existing file, reuse whatever the neighboring thread tests use — do not invent new schema.)

- [ ] **Step 5: Run integration tests**

Run: `make test-integration SERVICE=message-worker`
Expected: PASS — existing idempotency tests still green (proves batch didn't break redelivery), plus the new TShow assertion.

- [ ] **Step 6: Commit**

```bash
git add message-worker/store_cassandra.go message-worker/integration_test.go
git commit -m "message-worker: batch thread-message writes via UnloggedBatch (match SaveMessage)"
```

---

### Task 8: docs/client-api.md — document `eventTimestamp`

**Files:**
- Modify: `docs/client-api.md`

**Interfaces:** none (documentation only).

- [ ] **Step 1: Add the field row to `message_edited`**

In the `EditRoomEvent` field table (currently ending `timestamp` → `messageId`), insert after the `timestamp` row:

```markdown
| `eventTimestamp` | number | Milliseconds since Unix epoch (UTC). When message-worker published the canonical event. Omitted for legacy events. |
```

And add `"eventTimestamp": 1746518700100,` after the `"timestamp"` line in the `message_edited` JSON example.

- [ ] **Step 2: Add the field row to `message_reacted`**

In the `ReactRoomEvent` field table, insert after the `timestamp` row:

```markdown
| `eventTimestamp` | number | Milliseconds since Unix epoch (UTC). When message-worker published the canonical event. Omitted for legacy events. |
```

And add `"eventTimestamp": 1746518900100,` after the `"timestamp"` line in the `message_reacted` JSON example.

- [ ] **Step 3: Add the field row to `new_message`**

In the `RoomEvent` field table (the `new_message` event), insert after the `timestamp` row:

```markdown
| `eventTimestamp` | number | Milliseconds since Unix epoch (UTC). When message-worker published the canonical event. Omitted for legacy events. |
```

And add `"eventTimestamp": 1746518100100,` after the `"timestamp"` line in both the channel and DM `new_message` JSON examples.

- [ ] **Step 4: Verify no unrelated diff**

Run: `git diff --stat docs/client-api.md`
Expected: only `docs/client-api.md` changed; additions only (3 field rows + example lines).

- [ ] **Step 5: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): document eventTimestamp on new_message, message_edited, message_reacted"
```

---

### Task 9: Final gates + review-notes cleanup

**Files:** none (verification), plus deletion of any `docs/reviews/` artifacts if present.

- [ ] **Step 1: Regenerate all mocks (idempotent check)**

Run: `make generate SERVICE=broadcast-worker && make generate SERVICE=message-worker`
Expected: no diff (mocks already committed in Tasks 2 and 5).

- [ ] **Step 2: Lint**

Run: `make lint`
Expected: 0 issues.

- [ ] **Step 3: Unit tests (all)**

Run: `make test`
Expected: PASS with `-race`.

- [ ] **Step 4: Integration tests (touched services)**

Run: `make test-integration SERVICE=broadcast-worker && make test-integration SERVICE=message-worker`
Expected: PASS.

- [ ] **Step 5: SAST**

Run: `make sast`
Expected: gosec/govulncheck/semgrep PASS (no medium+).

- [ ] **Step 6: Remove session review notes (if any) and push**

```bash
git rm -r --ignore-unmatch docs/reviews
git commit -m "chore: drop session review notes" || true
for i in 1 2 3 4; do git push -u origin claude/broadcast-worker-thread-visibility-d4p1g2 && break || sleep $((2**i)); done
```

---

## Self-Review

**Spec coverage:**
- Part 1 (broadcast-worker gate) → Tasks 1, 2 (`GetHistorySharedSince`), 3 (channel filter). ✅
- Part 2 (broadcast-worker thread-sub hasMention) → Tasks 2 (`SetThreadSubscriptionMentions`), 3 (DM redirect). ✅
- Part 3 (message-worker batch) → Task 7. ✅
- Part 4 (message-worker mention gate) → Tasks 4, 5, 6. ✅
- Part 5 (client-api eventTimestamp) → Task 8. ✅
- Visibility rule mirrored exactly in Tasks 1 & 4 helpers. ✅

**Placeholder scan:** No TBD/TODO; every code step shows full code; test steps show real assertions. Helper references (`threadReplyEvent`, `capturePublisher`, `channelMeta`, `dmMeta`, `dmSubs`, `threadReplyMessage`, `seedThreadReply`) are flagged as "existing helper" — implementers reuse the neighboring tests' scaffolding rather than inventing schema; where a helper may not exist verbatim, the step says to reuse whatever the adjacent tests use.

**Type consistency:** `GetHistorySharedSince(ctx, roomID string, accounts []string) (map[string]*time.Time, error)` is identical in Store (Task 2) and ThreadStore (Task 5). `SetThreadSubscriptionMentions(ctx, parentMessageID string, accounts []string) error` matches between store (Task 2) and handler call (Task 3). `mentionVisible(historySharedSince, parentCreatedAt *time.Time) bool` identical in Tasks 1 & 4. `allowedThreadMentions(ctx, roomID string, mentions []string, parentCreatedAt *time.Time) ([]string, error)` used consistently in Task 3. ✅

---

## Addendum A — extend the mention gate to thread edit/delete (follow-up)

Post-implementation review found `handleThreadUpdated`/`handleThreadDeleted` merge edited/deleted-content mentions into the channel fan-out **ungated**, unlike `handleThreadCreated`. Close the gap symmetrically (spec Part 6).

### Task A1: history-service — carry parent createdAt on edit/delete canonical events

**Files:**
- Modify: `history-service/internal/service/messages.go` (EditMessage + DeleteMessage canonical `model.MessageEvent`)
- Test: `history-service/internal/service/*_test.go` (existing edit/delete canonical-event assertions)

- [ ] **Step 1:** In the `EventUpdated` `model.Message{...}` literal (EditMessage) add `ThreadParentMessageCreatedAt: msg.ThreadParentCreatedAt,` next to `ThreadParentMessageID`.
- [ ] **Step 2:** In the `EventDeleted` `model.Message{...}` literal (DeleteMessage) add the same line.
- [ ] **Step 3:** Add/extend a unit test asserting the published canonical edit and delete events carry `ThreadParentMessageCreatedAt` equal to the parent reply's stored `ThreadParentCreatedAt` for a thread reply.
- [ ] **Step 4:** `make test SERVICE=history-service` → PASS.
- [ ] **Step 5:** Commit: `history-service: carry ThreadParentMessageCreatedAt on canonical edit/delete events`.

### Task A2: broadcast-worker — gate update/delete channel mentions

**Files:**
- Modify: `broadcast-worker/handler.go` (`handleThreadUpdated`, `handleThreadDeleted` channel branches)
- Test: `broadcast-worker/handler_test.go`

- [ ] **Step 1: Write failing tests** — `TestHandleThreadUpdated_ChannelExcludesRestrictedAndNonMemberMentions` and the delete analogue: content `"@bob @carol @dave hi"`, `GetThreadFollowers` returns empty, `GetHistorySharedSince` returns `{"bob": nil, "carol": &joinedAfter}` (carol restricted, dave absent), assert only alice(sender)+bob receive.
- [ ] **Step 2:** Run → FAIL (carol/dave currently included; `GetHistorySharedSince` not called).
- [ ] **Step 3: Implement** — in each channel branch, after `parsed := mention.Parse(msg.Content)`:

```go
		allowedMentions, err := h.allowedThreadMentions(ctx, room.ID, parsed.Accounts, msg.ThreadParentMessageCreatedAt)
		if err != nil {
			return err
		}
		fanOut, err := h.channelThreadFanOut(ctx, parentMsgID, msg.UserAccount, allowedMentions)
```

(replacing the `parsed.Accounts` argument to `channelThreadFanOut`).
- [ ] **Step 4:** Run `make test SERVICE=broadcast-worker` → PASS (existing non-mention update/delete tests unaffected).
- [ ] **Step 5:** Commit: `broadcast-worker: gate thread edit/delete channel mentions by history window`.

### Task A3: gates + push

- [ ] `make lint`, `make test`, `make sast` → all clean; integration builds compile; push.

## Follow-up 2 — PR review comments (parent fetch + parent-sender fan-out)

Three review comments (spec Part 7). Delivered as a **separate commit** on the same branch.

### Task B1: broadcast-worker — fetch parent from history-service (Comments 1 & 3)

**Files:**
- Add: `broadcast-worker/parent_fetcher.go`, `broadcast-worker/parent_fetcher_test.go`
- Modify: `broadcast-worker/handler.go`, `store.go`, `store_mongo.go`, `main.go`
- Test: `broadcast-worker/handler_test.go`, `integration_test.go`, `metacache_integration_test.go`

- [x] **Step 1:** Add `ParentMessageInfo{SenderAccount, CreatedAt}` + `ParentFetcher` interface to `handler.go`; `//go:generate` directive for its mock in `store.go`.
- [x] **Step 2:** Implement `historyParentFetcher` (`newHistoryParentFetcher(nc)`) issuing `nc.Request(subject.MsgGet(account, roomID, siteID), …)`, `errcode.Parse` guard, narrow `{createdAt, sender.account}` projection. Mirror `message-gatekeeper/fetcher_history.go`.
- [x] **Step 3 (Red):** `parent_fetcher_test.go` against an embedded NATS server — happy path (author+createdAt), errcode envelope → typed error, no-responder → error, malformed body → unmarshal error.
- [x] **Step 4:** `channelThreadFanOut(ctx, roomID, siteID, parentMsgID, sender, mentions)` fetches the parent, gates mentions on `parent.CreatedAt`, and passes `parent.SenderAccount` to `threadFanOutAccounts`, which adds it (deduped, bots excluded) after the reply sender. Update the 3 call sites (create=`evt.SiteID`, update/delete=`room.SiteID`).
- [x] **Step 5:** Wire `newHistoryParentFetcher(nc)` into `NewHandler` in `main.go`.
- [x] **Step 6:** Update `handler_test.go` gate tests to source `parent.CreatedAt` via a `stubParentFetcher`; extend `TestThreadFanOutAccounts` with parent-sender cases.
- [x] **Step 7:** `make generate SERVICE=broadcast-worker`, `make test SERVICE=broadcast-worker` → PASS.

### Task B2: broadcast-worker — remove redundant SetThreadSubscriptionMentions (Comment 2)

- [x] **Step 1:** Remove `SetThreadSubscriptionMentions` from `Store` (`store.go`) and `store_mongo.go` (method, `threadSubCol` field, the `(parentMessageId, userAccount)` index); revert `NewMongoStore` to `(roomCol, subCol, threadRoomCol, valkey, metaTTL)`.
- [x] **Step 2:** DM branch of `handleThreadCreated` drops the call; message-worker owns the thread-sub mention badge.
- [x] **Step 3:** Remove the DM-mention EXPECT + the `SetThreadSubscriptionMentions` integration test; revert `NewMongoStore` call sites.
- [x] **Step 4:** `make test SERVICE=broadcast-worker`, integration vet → PASS.

### Task B3: notification-worker — parent fetch + always-notify parent author (Comment 3)

**Files:**
- Add: `notification-worker/parent_fetcher.go`, `notification-worker/parent_fetcher_test.go`
- Modify: `notification-worker/handler.go`, `threads.go`, `main.go`
- Test: `notification-worker/handler_test.go`, `threads_test.go`, `integration_test.go`

- [x] **Step 1:** Add `ParentMessageInfo` + `ParentFetcher` + `historyParentFetcher` (same shape as broadcast-worker); `Parent ParentFetcher` on `HandlerDeps`.
- [x] **Step 2 (Red):** `parent_fetcher_test.go` (embedded NATS, same four cases); `TestHandle_ThreadOnlyReply_ParentSenderAlwaysNotified` (parent author notified before `thread_rooms` exists); `TestHandle_ThreadOnlyReply_ParentFetchError_NAKs`.
- [x] **Step 3:** In the thread branch of `HandleMessage`, fetch the parent (account=reply sender, siteID=`evt.SiteID`); source `parentCreatedAt` from it; set `follows=true` when `m.Account == parent.SenderAccount`. Fetch error → NAK.
- [x] **Step 4:** Remove `ThreadRoomInfo.ParentCreatedAt` (now dead) from `threads.go` + its projection/decode; update `threads_test.go` and the `TestMongoThreadFollowers_Lookup` integration assertions.
- [x] **Step 5:** Wire `newHistoryParentFetcher(nc)` into `HandlerDeps` in `main.go`.
- [x] **Step 6:** `make test SERVICE=notification-worker`, integration vet → PASS.

### Task B4: gates + separate commit

- [x] `make lint`, `make test SERVICE=broadcast-worker`, `make test SERVICE=notification-worker`, `make sast` → clean; integration builds compile.
- [ ] Commit as a **separate** commit (not amended into the feature commit); push.
