# `message.thread.read` RPC — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a new room-service RPC, `chat.user.{account}.request.room.{roomID}.{siteID}.message.thread.read`, that lets a client mark a single thread as read — clearing it from `Subscription.ThreadUnread`, recomputing `Subscription.Alert`, refreshing the `ThreadSubscription`, and federating the result to the user's home site via the outbox/inbox pattern.

**Architecture:** Synchronous NATS request/reply in `room-service`. Source-site is authoritative; user's home site is a cache kept in sync via a new `thread_read` outbox event handled by `inbox-worker`. Two pairs of operations (sub + thread-sub reads; sub + thread-sub writes) run concurrently via `errgroup.WithContext` with manual error-priority inspection.

**Tech Stack:** Go 1.25, NATS JetStream (outbox), MongoDB (`go.mongodb.org/mongo-driver/v2`), `go.uber.org/mock` (mockgen), `stretchr/testify`, `testcontainers-go` (integration), `golang.org/x/sync/errgroup`.

**Branch:** `claude/add-thread-read-rpc-ksasD` (already exists).

**Spec:** `docs/superpowers/specs/2026-05-20-thread-read-rpc-design.md`.

---

## File Map

**`pkg/subject/`**
- Modify: `subject.go` — add `MessageThreadRead` and `MessageThreadReadWildcard` builders.
- Modify: `subject_test.go` — three subject tests.

**`pkg/model/`**
- Modify: `subscription.go` — add `MessageThreadReadRequest`.
- Modify: `threadsubscription.go` — add `ErrThreadSubscriptionNotFound`.
- Modify: `event.go` — add `OutboxThreadRead` const + `ThreadReadEvent`.
- Modify: `model_test.go` — round-trip tests.

**`room-service/`**
- Modify: `helper.go` — new sentinels in the existing `var (...)` block + `sanitizeError` allow-list.
- Modify: `store.go` — three new methods on `RoomStore`.
- Modify: `store_mongo.go` — add `threadSubscriptions` collection handle, implement three methods, add new index in `EnsureIndexes`.
- Modify: `handler.go` — `RegisterCRUD` registration, `natsMessageThreadRead` wrapper, `handleMessageThreadRead` core logic.
- Regenerate: `mock_store_test.go` (via `make generate SERVICE=room-service`).
- Modify: `handler_test.go` — fixture + table-driven cases.
- Modify: `integration_test.go` — store integration tests.

**`inbox-worker/`**
- Modify: `handler.go` — add `case "thread_read":` arm + `handleThreadRead`; extend `InboxStore` interface with `ApplyThreadRead`.
- Modify: `main.go` — implement `ApplyThreadRead` on `mongoInboxStore`.
- Modify: `handler_test.go` — extend `stubInboxStore` + new unit tests.
- Modify: `integration_test.go` — five integration cases.

**`docs/`**
- Modify: `client-api.md` — add "Mark Thread as Read" subsection after the existing "Mark Messages Read".

---

## Task 1 — Subject builders

**Files:**
- Modify: `pkg/subject/subject.go`
- Modify: `pkg/subject/subject_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `pkg/subject/subject_test.go`:

```go
func TestMessageThreadRead(t *testing.T) {
	got := subject.MessageThreadRead("alice", "r1", "site-a")
	want := "chat.user.alice.request.room.r1.site-a.message.thread.read"
	if got != want {
		t.Errorf("MessageThreadRead: got %q, want %q", got, want)
	}
}

func TestMessageThreadReadWildcard(t *testing.T) {
	got := subject.MessageThreadReadWildcard("site-a")
	want := "chat.user.*.request.room.*.site-a.message.thread.read"
	if got != want {
		t.Errorf("MessageThreadReadWildcard: got %q, want %q", got, want)
	}
}

func TestMessageThreadRead_ParseUserRoomSubject(t *testing.T) {
	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok || account != "alice" || roomID != "r1" {
		t.Errorf("ParseUserRoomSubject(%q) = (%q, %q, %v); want (\"alice\", \"r1\", true)",
			subj, account, roomID, ok)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./pkg/subject/ -run 'TestMessageThreadRead' -v`
Expected: FAIL — `subject.MessageThreadRead` / `subject.MessageThreadReadWildcard` undefined.

- [ ] **Step 3: Implement the builders**

Append to `pkg/subject/subject.go` directly below the existing `MessageReadReceiptWildcard` (around line 354):

```go
// MessageThreadRead returns the concrete subject for the per-user
// mark-thread-as-read RPC. Pair with MessageThreadReadWildcard for
// room-service's QueueSubscribe.
func MessageThreadRead(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.message.thread.read", account, roomID, siteID)
}

// MessageThreadReadWildcard is the per-site subscription pattern for
// the mark-thread-as-read RPC.
func MessageThreadReadWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.message.thread.read", siteID)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./pkg/subject/ -run 'TestMessageThreadRead' -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add MessageThreadRead builders"
```

---

## Task 2 — Model types & sentinels

**Files:**
- Modify: `pkg/model/subscription.go`
- Modify: `pkg/model/threadsubscription.go`
- Modify: `pkg/model/event.go`
- Modify: `pkg/model/model_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `pkg/model/model_test.go`:

```go
func TestMessageThreadReadRequestJSON(t *testing.T) {
	src := model.MessageThreadReadRequest{ThreadID: "01970a4f8c2d7c9aQRST"}
	roundTrip(t, &src, &model.MessageThreadReadRequest{})
}

func TestThreadReadEventJSON(t *testing.T) {
	src := model.ThreadReadEvent{
		Account:         "alice",
		RoomID:          "r1",
		ThreadRoomID:    "tr1",
		ParentMessageID: "01970a4f8c2d7c9aQRST",
		NewThreadUnread: []string{"t2", "t3"},
		Alert:           true,
		LastSeenAt:      1735689600000,
		Timestamp:       1735689600001,
	}
	roundTrip(t, &src, &model.ThreadReadEvent{})
}

func TestOutboxEventJSON_ThreadRead(t *testing.T) {
	payload := model.ThreadReadEvent{
		Account: "alice", RoomID: "r1", ThreadRoomID: "tr1",
		ParentMessageID: "p1", NewThreadUnread: []string{"t2"},
		Alert: false, LastSeenAt: 1735689600000, Timestamp: 1735689600001,
	}
	data, err := json.Marshal(&payload)
	require.NoError(t, err)
	src := model.OutboxEvent{
		Type:       model.OutboxThreadRead,
		SiteID:     "site-a",
		DestSiteID: "site-b",
		Payload:    data,
		Timestamp:  1735689600002,
	}
	out, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.OutboxEvent
	require.NoError(t, json.Unmarshal(out, &dst))
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
	}
	if dst.Type != "thread_read" {
		t.Errorf("Type = %q, want thread_read", dst.Type)
	}
	var gotPayload model.ThreadReadEvent
	require.NoError(t, json.Unmarshal(dst.Payload, &gotPayload))
	assert.Equal(t, payload, gotPayload)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./pkg/model/ -run 'TestMessageThreadReadRequestJSON|TestThreadReadEventJSON|TestOutboxEventJSON_ThreadRead' -v`
Expected: FAIL — `model.MessageThreadReadRequest`, `model.ThreadReadEvent`, `model.OutboxThreadRead` undefined.

- [ ] **Step 3: Add the request type**

Append to `pkg/model/subscription.go`:

```go
// MessageThreadReadRequest is the body of the message.thread.read RPC.
// The subject already carries account and roomID; only the thread's
// ParentMessageID is supplied in the body.
type MessageThreadReadRequest struct {
	ThreadID string `json:"threadId"`
}
```

- [ ] **Step 4: Add the sentinel**

Replace the contents of `pkg/model/threadsubscription.go` (preserving the existing `ThreadSubscription` struct exactly) with the same file plus a new sentinel at the top:

```go
package model

import (
	"errors"
	"time"
)

// ErrThreadSubscriptionNotFound is returned when a thread-subscription
// lookup finds no matching document.
var ErrThreadSubscriptionNotFound = errors.New("thread subscription not found")

type ThreadSubscription struct {
	ID              string `json:"id"              bson:"_id"`
	ParentMessageID string `json:"parentMessageId" bson:"parentMessageId"`
	RoomID          string `json:"roomId"          bson:"roomId"`
	ThreadRoomID    string `json:"threadRoomId"    bson:"threadRoomId"`
	UserID          string `json:"userId"          bson:"userId"`
	UserAccount     string `json:"userAccount"     bson:"userAccount"`
	// SiteID is the home site of the room that contains this thread — same
	// semantic as Subscription.SiteID. Across cross-site federation it stays
	// constant: every replica of a given subscription has the same SiteID
	// regardless of which site stores the document.
	SiteID string `json:"siteId" bson:"siteId"`
	// Never add omitempty: unreadThreadsPipeline relies on BSON encoding nil as explicit null, not a missing field.
	LastSeenAt *time.Time `json:"lastSeenAt"  bson:"lastSeenAt"`
	HasMention bool       `json:"hasMention"  bson:"hasMention"`
	CreatedAt  time.Time  `json:"createdAt"   bson:"createdAt"`
	UpdatedAt  time.Time  `json:"updatedAt"   bson:"updatedAt"`
}
```

- [ ] **Step 5: Add the outbox event type + constant**

In `pkg/model/event.go`, extend the existing `OutboxEventType` const block (around lines 81-86) with the new constant:

```go
const (
	OutboxMemberAdded                OutboxEventType = "member_added"
	OutboxMemberRemoved              OutboxEventType = "member_removed"
	OutboxSubscriptionRead           OutboxEventType = "subscription_read"
	OutboxThreadSubscriptionUpserted OutboxEventType = "thread_subscription_upserted"
	OutboxThreadRead                 OutboxEventType = "thread_read"
)
```

Below the existing `SubscriptionReadEvent` struct (around line 99), add:

```go
// ThreadReadEvent is the OutboxEvent.Payload for type "thread_read".
// Sent from the room's home site to the user's home site when a user
// marks a thread as read. The source site computes the authoritative
// result (NewThreadUnread, Alert); the destination applies values
// directly rather than re-deriving. LastSeenAt is UnixMilli (UTC);
// Timestamp is the publish time (UnixMilli, UTC).
type ThreadReadEvent struct {
	Account         string   `json:"account"`
	RoomID          string   `json:"roomId"`
	ThreadRoomID    string   `json:"threadRoomId"`
	ParentMessageID string   `json:"parentMessageId"`
	NewThreadUnread []string `json:"newThreadUnread"`
	Alert           bool     `json:"alert"`
	LastSeenAt      int64    `json:"lastSeenAt"`
	Timestamp       int64    `json:"timestamp"`
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./pkg/model/ -run 'TestMessageThreadReadRequestJSON|TestThreadReadEventJSON|TestOutboxEventJSON_ThreadRead' -v`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
git add pkg/model/subscription.go pkg/model/threadsubscription.go pkg/model/event.go pkg/model/model_test.go
git commit -m "feat(model): add MessageThreadReadRequest, ThreadReadEvent and ErrThreadSubscriptionNotFound"
```

---

## Task 3 — Room-service sentinels & sanitizeError

**Files:**
- Modify: `room-service/helper.go`

This task is small and standalone — it's the only one of its kind where we don't write a test first, because `sanitizeError`'s allow-list is exercised indirectly by the handler tests in Task 5. Adding the sentinels here keeps Tasks 4 and 5 focused.

- [ ] **Step 1: Add the sentinels**

In `room-service/helper.go`, extend the existing standalone `var (...)` block around line 25 (the one containing `errNotRoomMember`) to add two more lines:

```go
var (
	errNotRoomMember     = errors.New("only room members can list members")
	errInvalidOrg        = errors.New("invalid org")
	errInvalidThreadID   = errors.New("threadId is required")
	errThreadSubNotFound = errors.New("thread subscription not found")
)
```

(Preserve the existing entries; only add the two new lines.)

- [ ] **Step 2: Extend `sanitizeError`'s allow-list**

In the same file, locate the multi-line `errors.Is(...)` `case` inside `sanitizeError` (around lines 176-199). Append the two new sentinels to the comma-separated list — for example after `errors.Is(err, errNotMessageSender),`:

```go
		errors.Is(err, errNotMessageSender),
		errors.Is(err, errInvalidThreadID),
		errors.Is(err, errThreadSubNotFound),
		errors.Is(err, &dmExistsError{}),
		errors.Is(err, &channelExpandTimeoutError{}):
```

- [ ] **Step 3: Verify the package still compiles**

Run: `go build ./room-service/...`
Expected: success (no output).

- [ ] **Step 4: Run the existing helper tests to confirm no regression**

Run: `make test SERVICE=room-service`
Expected: PASS (existing tests still green; new sentinels are unused so the compiler will not complain about them — Go permits unused package-level identifiers).

- [ ] **Step 5: Commit**

```bash
git add room-service/helper.go
git commit -m "feat(room-service): add errInvalidThreadID and errThreadSubNotFound sentinels"
```

---

## Task 4 — Room-service store interface + Mongo impl

**Files:**
- Modify: `room-service/store.go`
- Modify: `room-service/store_mongo.go`
- Modify: `room-service/integration_test.go`
- Regenerate: `room-service/mock_store_test.go`

- [ ] **Step 1: Write the failing integration tests**

Append to `room-service/integration_test.go` (the file uses the `//go:build integration` tag; new tests must live in the same file or share that tag). The existing helper `setupMongo(t) *mongo.Database` is reused; the `*MongoStore` is constructed via `NewMongoStore(db)` — same pattern as `TestMongoStore_Integration` at line 122:

```go
func TestMongoStore_GetThreadSubscriptionByParent(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(context.Background()))
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	ts := model.ThreadSubscription{
		ID:              "tsub-1",
		ParentMessageID: "p1",
		RoomID:          "r1",
		ThreadRoomID:    "tr1",
		UserID:          "u1",
		UserAccount:     "alice",
		SiteID:          "site-a",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	_, err := db.Collection("thread_subscriptions").InsertOne(ctx, &ts)
	require.NoError(t, err)

	t.Run("hit", func(t *testing.T) {
		got, err := store.GetThreadSubscriptionByParent(ctx, "alice", "p1", "r1")
		require.NoError(t, err)
		assert.Equal(t, "tsub-1", got.ID)
		assert.Equal(t, "tr1", got.ThreadRoomID)
	})

	t.Run("miss on wrong account", func(t *testing.T) {
		_, err := store.GetThreadSubscriptionByParent(ctx, "bob", "p1", "r1")
		require.ErrorIs(t, err, model.ErrThreadSubscriptionNotFound)
	})

	t.Run("miss on wrong parent", func(t *testing.T) {
		_, err := store.GetThreadSubscriptionByParent(ctx, "alice", "p999", "r1")
		require.ErrorIs(t, err, model.ErrThreadSubscriptionNotFound)
	})

	t.Run("miss on wrong room (defensive filter)", func(t *testing.T) {
		_, err := store.GetThreadSubscriptionByParent(ctx, "alice", "p1", "r-other")
		require.ErrorIs(t, err, model.ErrThreadSubscriptionNotFound)
	})
}

func TestMongoStore_UpdateSubscriptionThreadRead(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(context.Background()))
	ctx := context.Background()

	sub := model.Subscription{
		ID: "sub-1", RoomID: "r1", SiteID: "site-a",
		User:         model.SubscriptionUser{ID: "u1", Account: "alice"},
		JoinedAt:     time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond),
		ThreadUnread: []string{"t1", "t2"},
		Alert:        true,
	}
	_, err := db.Collection("subscriptions").InsertOne(ctx, &sub)
	require.NoError(t, err)

	t.Run("non-empty array path", func(t *testing.T) {
		require.NoError(t, store.UpdateSubscriptionThreadRead(ctx, "r1", "alice", []string{"t2"}, true))
		var got model.Subscription
		require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sub-1"}).Decode(&got))
		assert.Equal(t, []string{"t2"}, got.ThreadUnread)
		assert.True(t, got.Alert)
	})

	t.Run("empty array path unsets threadUnread", func(t *testing.T) {
		require.NoError(t, store.UpdateSubscriptionThreadRead(ctx, "r1", "alice", nil, false))
		// Decode as raw to confirm the field is absent, not present-as-empty.
		var raw bson.M
		require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sub-1"}).Decode(&raw))
		_, present := raw["threadUnread"]
		assert.False(t, present, "threadUnread must be $unset, not stored as empty array")
		assert.Equal(t, false, raw["alert"])
	})

	t.Run("missing subscription returns sentinel", func(t *testing.T) {
		err := store.UpdateSubscriptionThreadRead(ctx, "r-missing", "alice", nil, false)
		require.ErrorIs(t, err, model.ErrSubscriptionNotFound)
	})
}

func TestMongoStore_UpdateThreadSubscriptionRead(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(context.Background()))
	ctx := context.Background()

	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	ts := model.ThreadSubscription{
		ID:              "tsub-1",
		ParentMessageID: "p1",
		RoomID:          "r1",
		ThreadRoomID:    "tr1",
		UserAccount:     "alice",
		HasMention:      true,
		CreatedAt:       created,
		UpdatedAt:       created,
	}
	_, err := db.Collection("thread_subscriptions").InsertOne(ctx, &ts)
	require.NoError(t, err)

	t.Run("happy path", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Millisecond)
		require.NoError(t, store.UpdateThreadSubscriptionRead(ctx, "tr1", "alice", now))
		var got model.ThreadSubscription
		require.NoError(t, db.Collection("thread_subscriptions").FindOne(ctx, bson.M{"_id": "tsub-1"}).Decode(&got))
		require.NotNil(t, got.LastSeenAt)
		assert.Equal(t, now, got.LastSeenAt.UTC().Truncate(time.Millisecond))
		assert.Equal(t, now, got.UpdatedAt.UTC().Truncate(time.Millisecond))
		assert.False(t, got.HasMention)
	})

	t.Run("missing thread subscription returns sentinel", func(t *testing.T) {
		err := store.UpdateThreadSubscriptionRead(ctx, "tr-missing", "alice", time.Now().UTC())
		require.ErrorIs(t, err, model.ErrThreadSubscriptionNotFound)
	})
}
```

- [ ] **Step 2: Run the integration tests to verify they fail**

Run: `make test-integration SERVICE=room-service`
Expected: FAIL — `MongoStore.GetThreadSubscriptionByParent`, `UpdateSubscriptionThreadRead`, `UpdateThreadSubscriptionRead` undefined.

- [ ] **Step 3: Add the three methods to the `RoomStore` interface**

In `room-service/store.go`, append to the `RoomStore` interface (inside the existing `type RoomStore interface { ... }` block, before the final `}`):

```go
	// GetThreadSubscriptionByParent looks up the user's ThreadSubscription
	// by (parentMessageID, account) and additionally enforces that the
	// matched document's roomId equals the supplied roomID. The roomID
	// filter is a defensive correctness check — it prevents a client from
	// pairing a roomID in the request subject with a threadId belonging
	// to a different room. Returns model.ErrThreadSubscriptionNotFound
	// (wrapped) when no document matches the full tuple.
	GetThreadSubscriptionByParent(ctx context.Context, account, parentMessageID, roomID string) (*model.ThreadSubscription, error)

	// UpdateSubscriptionThreadRead overwrites threadUnread and alert on
	// the subscription keyed by (roomID, account). When threadUnread is
	// empty, the field is $unset so JSON round-trip matches the omitempty
	// contract documented in pkg/model. Returns model.ErrSubscriptionNotFound
	// (wrapped) when no subscription matches.
	UpdateSubscriptionThreadRead(ctx context.Context, roomID, account string, threadUnread []string, alert bool) error

	// UpdateThreadSubscriptionRead sets lastSeenAt, updatedAt and
	// hasMention=false on the ThreadSubscription keyed by
	// (threadRoomID, userAccount). Returns
	// model.ErrThreadSubscriptionNotFound (wrapped) when no document matches.
	UpdateThreadSubscriptionRead(ctx context.Context, threadRoomID, account string, lastSeenAt time.Time) error
```

- [ ] **Step 4: Add the `threadSubscriptions` collection handle**

In `room-service/store_mongo.go`, extend the `MongoStore` struct (around line 17) to add the new field, and update `NewMongoStore` (around line 25) to populate it:

```go
type MongoStore struct {
	rooms               *mongo.Collection
	subscriptions       *mongo.Collection
	threadSubscriptions *mongo.Collection
	roomMembers         *mongo.Collection
	users               *mongo.Collection
	apps                *mongo.Collection
}

func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{
		rooms:               db.Collection("rooms"),
		subscriptions:       db.Collection("subscriptions"),
		threadSubscriptions: db.Collection("thread_subscriptions"),
		roomMembers:         db.Collection("room_members"),
		users:               db.Collection("users"),
		apps:                db.Collection("apps"),
	}
}
```

- [ ] **Step 5: Add the new index in `EnsureIndexes`**

In `room-service/store_mongo.go`, locate `EnsureIndexes` (around line 39). Append a new index block before the final `return nil` — placement does not matter functionally:

```go
	// Lookup index for GetThreadSubscriptionByParent. Non-unique because the
	// existing (threadRoomId, userAccount) unique index already protects
	// against duplicate thread subscriptions; this index only needs to
	// satisfy the (parentMessageId, userAccount) equality predicate.
	if _, err := s.threadSubscriptions.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "parentMessageId", Value: 1}, {Key: "userAccount", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure thread_subscriptions (parentMessageId,userAccount) index: %w", err)
	}
```

- [ ] **Step 6: Implement the three methods**

Append to `room-service/store_mongo.go` (after `GetUserSiteID` which lives around line 696):

```go
// GetThreadSubscriptionByParent looks up the user's ThreadSubscription
// by (parentMessageID, account, roomID). The roomID is a defensive
// correctness filter: the supplied threadId must belong to the room
// named in the request subject.
func (s *MongoStore) GetThreadSubscriptionByParent(ctx context.Context, account, parentMessageID, roomID string) (*model.ThreadSubscription, error) {
	var ts model.ThreadSubscription
	err := s.threadSubscriptions.FindOne(ctx, bson.M{
		"parentMessageId": parentMessageID,
		"userAccount":     account,
		"roomId":          roomID,
	}).Decode(&ts)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("find thread subscription for %q parent %q in room %q: %w",
				account, parentMessageID, roomID, model.ErrThreadSubscriptionNotFound)
		}
		return nil, fmt.Errorf("find thread subscription for %q parent %q in room %q: %w",
			account, parentMessageID, roomID, err)
	}
	return &ts, nil
}

// UpdateSubscriptionThreadRead overwrites threadUnread and alert. When
// threadUnread is empty, the field is $unset so it round-trips through
// JSON as nil (matching the Subscription struct's omitempty tag).
func (s *MongoStore) UpdateSubscriptionThreadRead(ctx context.Context, roomID, account string, threadUnread []string, alert bool) error {
	filter := bson.M{"roomId": roomID, "u.account": account}
	var update bson.M
	if len(threadUnread) == 0 {
		update = bson.M{
			"$set":   bson.M{"alert": alert},
			"$unset": bson.M{"threadUnread": ""},
		}
	} else {
		update = bson.M{"$set": bson.M{"threadUnread": threadUnread, "alert": alert}}
	}
	res, err := s.subscriptions.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update subscription thread-read for %q in room %q: %w", account, roomID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("update subscription thread-read for %q in room %q: %w",
			account, roomID, model.ErrSubscriptionNotFound)
	}
	return nil
}

// UpdateThreadSubscriptionRead sets lastSeenAt, updatedAt and clears
// hasMention. No order-safety guard on the source-site write — time.Now()
// is monotonically increasing within a process. The $lt guard exists
// only on the inbox-worker side where federation can deliver out-of-order.
func (s *MongoStore) UpdateThreadSubscriptionRead(ctx context.Context, threadRoomID, account string, lastSeenAt time.Time) error {
	filter := bson.M{"threadRoomId": threadRoomID, "userAccount": account}
	update := bson.M{"$set": bson.M{
		"lastSeenAt": lastSeenAt,
		"updatedAt":  lastSeenAt,
		"hasMention": false,
	}}
	res, err := s.threadSubscriptions.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update thread subscription read for %q in thread room %q: %w",
			account, threadRoomID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("update thread subscription read for %q in thread room %q: %w",
			account, threadRoomID, model.ErrThreadSubscriptionNotFound)
	}
	return nil
}
```

- [ ] **Step 7: Regenerate mocks**

Run: `make generate SERVICE=room-service`
Expected: `room-service/mock_store_test.go` regenerates with mocks for the three new methods. (The file is mock-generated; do not edit it manually.)

- [ ] **Step 8: Re-run integration tests**

Run: `make test-integration SERVICE=room-service`
Expected: PASS, including the new sub-tests added in Step 1.

- [ ] **Step 9: Run unit tests to confirm the regenerated mocks compile**

Run: `make test SERVICE=room-service`
Expected: PASS (existing tests still green).

- [ ] **Step 10: Commit**

```bash
git add room-service/store.go room-service/store_mongo.go room-service/integration_test.go room-service/mock_store_test.go
git commit -m "feat(room-service): add ThreadSubscription store methods for thread-read RPC"
```

---

## Task 5 — Room-service handler

**Files:**
- Modify: `room-service/handler.go`
- Modify: `room-service/handler_test.go`

This task implements both the RPC wiring and the core handler logic with all 16 unit-test cases enumerated in §7.4 of the spec.

- [ ] **Step 1: Write the failing unit tests**

Append to `room-service/handler_test.go`. The fixture mirrors `newMessageReadFixture` (around line 2492):

```go
type threadReadFixture struct {
	store           *MockRoomStore
	handler         *Handler
	publishCalls    int
	publishedSubj   string
	publishedData   []byte
	publishCallErr  error
}

func newThreadReadFixture(t *testing.T) *threadReadFixture {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	f := &threadReadFixture{store: store}
	f.handler = &Handler{
		store:  store,
		siteID: "site-a",
		publishToStream: func(_ context.Context, subj string, data []byte) error {
			f.publishCalls++
			f.publishedSubj = subj
			f.publishedData = data
			return f.publishCallErr
		},
	}
	return f
}

func threadReadBody(t *testing.T, threadID string) []byte {
	t.Helper()
	b, err := json.Marshal(model.MessageThreadReadRequest{ThreadID: threadID})
	require.NoError(t, err)
	return b
}

// baseSub returns a Subscription preloaded with the given ThreadUnread and Alert.
func baseSub(account, roomID string, threadUnread []string, alert bool) *model.Subscription {
	return &model.Subscription{
		User:         model.SubscriptionUser{ID: "u-" + account, Account: account},
		RoomID:       roomID,
		SiteID:       "site-a",
		JoinedAt:     time.Now().UTC().Add(-time.Hour),
		ThreadUnread: threadUnread,
		Alert:        alert,
	}
}

func baseThreadSub(account, roomID, parent, threadRoomID string) *model.ThreadSubscription {
	return &model.ThreadSubscription{
		ID:              "tsub-" + parent,
		ParentMessageID: parent,
		RoomID:          roomID,
		ThreadRoomID:    threadRoomID,
		UserAccount:     account,
		SiteID:          "site-a",
		HasMention:      true,
	}
}

func TestHandler_MessageThreadRead_InvalidSubject(t *testing.T) {
	f := newThreadReadFixture(t)
	_, err := f.handler.handleMessageThreadRead(context.Background(), "garbage", threadReadBody(t, "p1"))
	require.Error(t, err)
}

func TestHandler_MessageThreadRead_EmptyThreadID(t *testing.T) {
	f := newThreadReadFixture(t)
	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, ""))
	require.ErrorIs(t, err, errInvalidThreadID)
}

func TestHandler_MessageThreadRead_MalformedBody(t *testing.T) {
	f := newThreadReadFixture(t)
	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, []byte("{"))
	require.Error(t, err)
}

func TestHandler_MessageThreadRead_NotRoomMember(t *testing.T) {
	f := newThreadReadFixture(t)
	// Sibling read may race ahead — allow .AnyTimes(); the handler must still
	// surface errNotRoomMember regardless of which goroutine finishes first.
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, model.ErrSubscriptionNotFound)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil).AnyTimes()

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.ErrorIs(t, err, errNotRoomMember)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_ThreadSubNotFound(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSub("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(nil, model.ErrThreadSubscriptionNotFound)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.ErrorIs(t, err, errThreadSubNotFound)
	assert.Equal(t, 0, f.publishCalls)
}

// Both-miss priority: errNotRoomMember always wins. Repeat the call enough
// times to exercise both goroutine-completion orderings under -race.
func TestHandler_MessageThreadRead_BothMiss_RoomNotMemberWins(t *testing.T) {
	for i := 0; i < 20; i++ {
		f := newThreadReadFixture(t)
		f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
			Return(nil, model.ErrSubscriptionNotFound)
		f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
			Return(nil, model.ErrThreadSubscriptionNotFound).AnyTimes()

		subj := subject.MessageThreadRead("alice", "r1", "site-a")
		_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
		require.ErrorIs(t, err, errNotRoomMember, "iteration %d", i)
	}
}

func TestHandler_MessageThreadRead_HappyAlertClears(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSub("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice",
		gomock.Len(0), false).Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	resp, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.NoError(t, err)
	var got map[string]string
	require.NoError(t, json.Unmarshal(resp, &got))
	assert.Equal(t, "accepted", got["status"])
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_HappyAlertStays(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSub("alice", "r1", []string{"p1", "p2"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice",
		[]string{"p2"}, true).Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.NoError(t, err)
}

func TestHandler_MessageThreadRead_IdempotentIDNotInArray(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSub("alice", "r1", []string{"p2"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	// p1 isn't in the array; the pull is a no-op; alert stays true (len > 0).
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice",
		[]string{"p2"}, true).Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.NoError(t, err)
}

func TestHandler_MessageThreadRead_AlertAlreadyFalse(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSub("alice", "r1", []string{"p1"}, false), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice",
		gomock.Len(0), false).Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.NoError(t, err)
}

func TestHandler_MessageThreadRead_CrossSite_PublishesOutbox(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSub("alice", "r1", []string{"p1", "p2"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice",
		[]string{"p2"}, true).Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.NoError(t, err)

	require.Equal(t, 1, f.publishCalls)
	assert.Equal(t, "outbox.site-a.to.site-b.thread_read", f.publishedSubj)

	var outer model.OutboxEvent
	require.NoError(t, json.Unmarshal(f.publishedData, &outer))
	assert.Equal(t, model.OutboxThreadRead, outer.Type)
	assert.Equal(t, "site-a", outer.SiteID)
	assert.Equal(t, "site-b", outer.DestSiteID)

	var inner model.ThreadReadEvent
	require.NoError(t, json.Unmarshal(outer.Payload, &inner))
	assert.Equal(t, "alice", inner.Account)
	assert.Equal(t, "r1", inner.RoomID)
	assert.Equal(t, "tr1", inner.ThreadRoomID)
	assert.Equal(t, "p1", inner.ParentMessageID)
	assert.Equal(t, []string{"p2"}, inner.NewThreadUnread)
	assert.True(t, inner.Alert)
	assert.Greater(t, inner.LastSeenAt, int64(0))
	assert.Greater(t, inner.Timestamp, int64(0))
}

func TestHandler_MessageThreadRead_GetUserSiteID_Empty(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSub("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", gomock.Any(), gomock.Any()).
		Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.NoError(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_GetUserSiteID_Error(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSub("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", gomock.Any(), gomock.Any()).
		Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("", fmt.Errorf("boom"))

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_OutboxPublishError(t *testing.T) {
	f := newThreadReadFixture(t)
	f.publishCallErr = fmt.Errorf("nats down")
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSub("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", gomock.Any(), gomock.Any()).
		Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.Error(t, err)
	require.Equal(t, 1, f.publishCalls)
}

func TestHandler_MessageThreadRead_UpdateSubscriptionError(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSub("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("mongo down"))
	// Sibling write may have completed, may be in-flight, may observe ctx cancel.
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil).AnyTimes()

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_UpdateThreadSubscriptionError(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSub("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(fmt.Errorf("mongo down"))

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}
```

- [ ] **Step 2: Run the unit tests to verify they fail**

Run: `go test ./room-service/ -run 'TestHandler_MessageThreadRead' -v`
Expected: FAIL — `handleMessageThreadRead`, `errInvalidThreadID`, etc. all undefined or undeclared.

- [ ] **Step 3: Wire the registration in `RegisterCRUD`**

In `room-service/handler.go`, locate `RegisterCRUD` (around line 62). Add a new registration directly after the existing `MessageReadReceiptWildcard` block (around line 89):

```go
	if _, err := nc.QueueSubscribe(subject.MessageThreadReadWildcard(h.siteID), queue, h.natsMessageThreadRead); err != nil {
		return fmt.Errorf("subscribe message thread read: %w", err)
	}
```

- [ ] **Step 4: Implement the NATS wrapper**

Append to `room-service/handler.go` (place adjacent to `natsMessageRead`, e.g. after the `handleMessageRead` function which ends around line 1068):

```go
func (h *Handler) natsMessageThreadRead(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleMessageThreadRead(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("message thread-read failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to message thread-read", "error", err)
	}
}
```

- [ ] **Step 5: Implement the core handler**

Append to `room-service/handler.go` immediately below `natsMessageThreadRead`:

```go
func (h *Handler) handleMessageThreadRead(ctx context.Context, subj string, data []byte) ([]byte, error) {
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid message-thread-read subject: %s", subj)
	}

	var req model.MessageThreadReadRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("unmarshal thread-read request: %w", err)
	}
	if strings.TrimSpace(req.ThreadID) == "" {
		return nil, errInvalidThreadID
	}

	// Concurrent room-access + thread-sub existence checks. Manual error
	// inspection after Wait() so errNotRoomMember always outranks
	// errThreadSubNotFound regardless of goroutine completion order.
	var (
		sub             *model.Subscription
		tsub            *model.ThreadSubscription
		subErr, tsubErr error
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		s, err := h.store.GetSubscription(gctx, account, roomID)
		sub, subErr = s, err
		return err
	})
	g.Go(func() error {
		t, err := h.store.GetThreadSubscriptionByParent(gctx, account, req.ThreadID, roomID)
		tsub, tsubErr = t, err
		return err
	})
	_ = g.Wait()
	switch {
	case errors.Is(subErr, model.ErrSubscriptionNotFound):
		return nil, errNotRoomMember
	case subErr != nil:
		return nil, fmt.Errorf("get subscription: %w", subErr)
	case errors.Is(tsubErr, model.ErrThreadSubscriptionNotFound):
		return nil, errThreadSubNotFound
	case tsubErr != nil:
		return nil, fmt.Errorf("get thread subscription: %w", tsubErr)
	}

	newThreadUnread := removeThreadID(sub.ThreadUnread, req.ThreadID)
	newAlert := sub.Alert && len(newThreadUnread) > 0
	now := time.Now().UTC()

	wg, wctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		if err := h.store.UpdateSubscriptionThreadRead(wctx, roomID, account, newThreadUnread, newAlert); err != nil {
			return fmt.Errorf("update subscription thread-read: %w", err)
		}
		return nil
	})
	wg.Go(func() error {
		if err := h.store.UpdateThreadSubscriptionRead(wctx, tsub.ThreadRoomID, account, now); err != nil {
			return fmt.Errorf("update thread subscription read: %w", err)
		}
		return nil
	})
	if err := wg.Wait(); err != nil {
		return nil, err
	}

	userSiteID, err := h.store.GetUserSiteID(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("get user siteId: %w", err)
	}
	switch {
	case userSiteID == "":
		slog.Warn("user not found locally; skipping cross-site outbox", "account", account)
	case userSiteID != h.siteID:
		payload := model.ThreadReadEvent{
			Account:         account,
			RoomID:          roomID,
			ThreadRoomID:    tsub.ThreadRoomID,
			ParentMessageID: req.ThreadID,
			NewThreadUnread: newThreadUnread,
			Alert:           newAlert,
			LastSeenAt:      now.UnixMilli(),
			Timestamp:       now.UnixMilli(),
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal thread_read payload: %w", err)
		}
		outbox := model.OutboxEvent{
			Type:       model.OutboxThreadRead,
			SiteID:     h.siteID,
			DestSiteID: userSiteID,
			Payload:    payloadData,
			Timestamp:  now.UnixMilli(),
		}
		outboxData, err := json.Marshal(outbox)
		if err != nil {
			return nil, fmt.Errorf("marshal outbox event: %w", err)
		}
		if err := h.publishToStream(ctx, subject.Outbox(h.siteID, userSiteID, model.OutboxThreadRead), outboxData); err != nil {
			return nil, fmt.Errorf("publish thread_read outbox: %w", err)
		}
	}

	return json.Marshal(map[string]string{"status": "accepted"})
}

// removeThreadID returns a copy of unread with all occurrences of id removed.
// The input slice is never mutated. Returns an empty (non-nil) slice when
// the result is empty, so the caller's len() check still works the same.
func removeThreadID(unread []string, id string) []string {
	out := make([]string, 0, len(unread))
	for _, x := range unread {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}
```

- [ ] **Step 6: Run the unit tests to verify they pass**

Run: `go test ./room-service/ -run 'TestHandler_MessageThreadRead' -race -v`
Expected: PASS — all 14 new tests green, including the 20-iteration both-miss priority test.

- [ ] **Step 7: Run the full room-service test suite to confirm no regression**

Run: `make test SERVICE=room-service`
Expected: PASS.

- [ ] **Step 8: Run lint**

Run: `make lint`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): add message.thread.read RPC handler"
```

---

## Task 6 — Inbox-worker integration

**Files:**
- Modify: `inbox-worker/handler.go`
- Modify: `inbox-worker/main.go`
- Modify: `inbox-worker/handler_test.go`
- Modify: `inbox-worker/integration_test.go`

- [ ] **Step 1: Write the failing unit tests**

Append to `inbox-worker/handler_test.go`. First, define a new fixture type at the top of the test file (next to the existing `subRead` type around line 27):

```go
type threadRead struct {
	roomID          string
	threadRoomID    string
	account         string
	newThreadUnread []string
	alert           bool
	lastSeenAt      time.Time
}
```

Next, modify the `stubInboxStore` struct (around line 34) by adding two fields at the bottom of the struct definition:

```go
type stubInboxStore struct {
	mu                 sync.Mutex
	subscriptions      []model.Subscription
	bulkSubscriptions  []*model.Subscription
	bulkCreateErr      error
	rooms              []model.Room
	roleUpdates        []roleUpdate
	users              []model.User
	subReads           []subRead
	threadSubs         []model.ThreadSubscription
	threadReads        []threadRead
	applyThreadReadErr error
}
```

Then add the stub method below the existing `UpdateSubscriptionRead` (around line 146):

```go
func (s *stubInboxStore) ApplyThreadRead(_ context.Context, roomID, threadRoomID, account string, newThreadUnread []string, alert bool, lastSeenAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applyThreadReadErr != nil {
		return s.applyThreadReadErr
	}
	s.threadReads = append(s.threadReads, threadRead{
		roomID: roomID, threadRoomID: threadRoomID, account: account,
		newThreadUnread: newThreadUnread, alert: alert, lastSeenAt: lastSeenAt,
	})
	return nil
}
```

Then add the unit tests:

```go
func TestHandler_HandleEvent_ThreadRead_Happy(t *testing.T) {
	store := &stubInboxStore{}
	h := NewHandler(store)
	payload := model.ThreadReadEvent{
		Account:         "alice",
		RoomID:          "r1",
		ThreadRoomID:    "tr1",
		ParentMessageID: "p1",
		NewThreadUnread: []string{"p2"},
		Alert:           true,
		LastSeenAt:      1735689600000,
		Timestamp:       1735689600001,
	}
	inner, err := json.Marshal(&payload)
	require.NoError(t, err)
	outer := model.OutboxEvent{
		Type:       model.OutboxThreadRead,
		SiteID:     "site-a",
		DestSiteID: "site-b",
		Payload:    inner,
		Timestamp:  1735689600002,
	}
	data, err := json.Marshal(&outer)
	require.NoError(t, err)

	require.NoError(t, h.HandleEvent(context.Background(), data))
	require.Len(t, store.threadReads, 1)
	tr := store.threadReads[0]
	assert.Equal(t, "r1", tr.roomID)
	assert.Equal(t, "tr1", tr.threadRoomID)
	assert.Equal(t, "alice", tr.account)
	assert.Equal(t, []string{"p2"}, tr.newThreadUnread)
	assert.True(t, tr.alert)
	assert.Equal(t, time.UnixMilli(1735689600000).UTC(), tr.lastSeenAt)
}

func TestHandler_HandleEvent_ThreadRead_MalformedPayload(t *testing.T) {
	store := &stubInboxStore{}
	h := NewHandler(store)
	outer := model.OutboxEvent{Type: model.OutboxThreadRead, Payload: []byte("{")}
	data, err := json.Marshal(&outer)
	require.NoError(t, err)
	err = h.HandleEvent(context.Background(), data)
	require.Error(t, err)
	assert.Len(t, store.threadReads, 0)
}

func TestHandler_HandleEvent_ThreadRead_StoreError(t *testing.T) {
	store := &stubInboxStore{applyThreadReadErr: fmt.Errorf("boom")}
	h := NewHandler(store)
	payload := model.ThreadReadEvent{Account: "a", RoomID: "r", ThreadRoomID: "tr", ParentMessageID: "p"}
	inner, _ := json.Marshal(&payload)
	outer := model.OutboxEvent{Type: model.OutboxThreadRead, Payload: inner}
	data, _ := json.Marshal(&outer)
	err := h.HandleEvent(context.Background(), data)
	require.Error(t, err)
}
```

- [ ] **Step 2: Run the unit tests to verify they fail**

Run: `go test ./inbox-worker/ -run 'TestHandler_HandleEvent_ThreadRead' -v`
Expected: FAIL — `ApplyThreadRead` not in `InboxStore` interface, `handleThreadRead` arm missing.

- [ ] **Step 3: Extend `InboxStore` interface and add the handler arm**

In `inbox-worker/handler.go`, extend the `InboxStore` interface (around line 18) with the new method:

```go
	// ApplyThreadRead mirrors a remote site's thread-read on the local cache.
	// Subscription: authoritative overwrite of threadUnread + alert
	// (idempotent under repeated delivery — the source ships the resulting
	// state). When newThreadUnread is empty, threadUnread is $unset to match
	// the omitempty contract. ThreadSubscription: sets lastSeenAt, updatedAt,
	// hasMention=false, guarded by $lt lastSeenAt so out-of-order deliveries
	// cannot regress. Missing documents on either side are silent no-ops
	// (replays may arrive before the corresponding member_added /
	// thread_subscription_upserted has been processed).
	ApplyThreadRead(ctx context.Context, roomID, threadRoomID, account string, newThreadUnread []string, alert bool, lastSeenAt time.Time) error
```

Extend the `HandleEvent` switch (around line 51) with the new case:

```go
	case "thread_read":
		return h.handleThreadRead(ctx, &evt)
```

Append the new handler method (place adjacent to `handleSubscriptionRead`, around line 190):

```go
func (h *Handler) handleThreadRead(ctx context.Context, evt *model.OutboxEvent) error {
	var e model.ThreadReadEvent
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal thread_read payload: %w", err)
	}
	lastSeenAt := time.UnixMilli(e.LastSeenAt).UTC()
	if err := h.store.ApplyThreadRead(ctx, e.RoomID, e.ThreadRoomID, e.Account, e.NewThreadUnread, e.Alert, lastSeenAt); err != nil {
		return fmt.Errorf("apply thread read (room %q, parent %q, account %q): %w",
			e.RoomID, e.ParentMessageID, e.Account, err)
	}
	return nil
}
```

- [ ] **Step 4: Implement `ApplyThreadRead` on `mongoInboxStore`**

In `inbox-worker/main.go`, append a new method after `UpsertThreadSubscription` (around line 182):

```go
// ApplyThreadRead mirrors a remote site's thread-read on the local cache.
// See the InboxStore interface comment for behavioural details.
func (s *mongoInboxStore) ApplyThreadRead(ctx context.Context, roomID, threadRoomID, account string, newThreadUnread []string, alert bool, lastSeenAt time.Time) error {
	subFilter := bson.M{"roomId": roomID, "u.account": account}
	var subUpdate bson.M
	if len(newThreadUnread) == 0 {
		subUpdate = bson.M{
			"$set":   bson.M{"alert": alert},
			"$unset": bson.M{"threadUnread": ""},
		}
	} else {
		subUpdate = bson.M{"$set": bson.M{"threadUnread": newThreadUnread, "alert": alert}}
	}
	if _, err := s.subCol.UpdateOne(ctx, subFilter, subUpdate); err != nil {
		return fmt.Errorf("apply thread read on subscription for %q in room %q: %w", account, roomID, err)
	}

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

- [ ] **Step 5: Write the failing integration tests**

Append to `inbox-worker/integration_test.go`. The existing helper is `setupMongo(t) *mongo.Database` (around line 27); the store is constructed inline with only the collection handles the test needs — same pattern as the existing `TestInboxWorker_MemberAdded_Integration` around line 35:

```go
func TestInboxStore_ApplyThreadRead_HappyPath(t *testing.T) {
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
	ctx := context.Background()

	// Seed Subscription + ThreadSubscription.
	now := time.Now().UTC().Truncate(time.Millisecond)
	seedSub := model.Subscription{
		ID: "sub-1", RoomID: "r1", SiteID: "site-b",
		User:         model.SubscriptionUser{ID: "u1", Account: "alice"},
		JoinedAt:     now.Add(-time.Hour),
		ThreadUnread: []string{"p1", "p2"},
		Alert:        true,
	}
	_, err := db.Collection("subscriptions").InsertOne(ctx, &seedSub)
	require.NoError(t, err)

	seedTS := model.ThreadSubscription{
		ID: "tsub-1", ParentMessageID: "p1", RoomID: "r1",
		ThreadRoomID: "tr1", UserAccount: "alice", SiteID: "site-b",
		HasMention: true, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
	}
	_, err = db.Collection("thread_subscriptions").InsertOne(ctx, &seedTS)
	require.NoError(t, err)

	require.NoError(t, store.ApplyThreadRead(ctx, "r1", "tr1", "alice", []string{"p2"}, true, now))

	var gotSub model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sub-1"}).Decode(&gotSub))
	assert.Equal(t, []string{"p2"}, gotSub.ThreadUnread)
	assert.True(t, gotSub.Alert)

	var gotTS model.ThreadSubscription
	require.NoError(t, db.Collection("thread_subscriptions").FindOne(ctx, bson.M{"_id": "tsub-1"}).Decode(&gotTS))
	require.NotNil(t, gotTS.LastSeenAt)
	assert.Equal(t, now, gotTS.LastSeenAt.UTC().Truncate(time.Millisecond))
	assert.False(t, gotTS.HasMention)
}

func TestInboxStore_ApplyThreadRead_EmptyArrayUnsetsField(t *testing.T) {
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
	ctx := context.Background()

	seedSub := model.Subscription{
		ID: "sub-1", RoomID: "r1", SiteID: "site-b",
		User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		JoinedAt: time.Now().UTC().Add(-time.Hour), ThreadUnread: []string{"p1"}, Alert: true,
	}
	_, err := db.Collection("subscriptions").InsertOne(ctx, &seedSub)
	require.NoError(t, err)

	require.NoError(t, store.ApplyThreadRead(ctx, "r1", "tr-missing", "alice", nil, false, time.Now().UTC()))

	var raw bson.M
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sub-1"}).Decode(&raw))
	_, present := raw["threadUnread"]
	assert.False(t, present, "threadUnread must be $unset, not stored as empty array")
	assert.Equal(t, false, raw["alert"])
}

func TestInboxStore_ApplyThreadRead_OutOfOrderThreadSub(t *testing.T) {
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
	ctx := context.Background()

	t2 := time.Now().UTC().Truncate(time.Millisecond)
	t1 := t2.Add(-time.Hour)

	// Seed a sub so the (no-guard) overwrite assertion is meaningful.
	seedSub := model.Subscription{
		ID: "sub-1", RoomID: "r1", SiteID: "site-b",
		User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		JoinedAt: t1.Add(-time.Hour), ThreadUnread: []string{"p1", "p2"}, Alert: true,
	}
	_, err := db.Collection("subscriptions").InsertOne(ctx, &seedSub)
	require.NoError(t, err)

	// ThreadSubscription stamped with the *newer* t2.
	seedTS := model.ThreadSubscription{
		ID: "tsub-1", ParentMessageID: "p1", RoomID: "r1",
		ThreadRoomID: "tr1", UserAccount: "alice", SiteID: "site-b",
		LastSeenAt: &t2, UpdatedAt: t2, CreatedAt: t1,
	}
	_, err = db.Collection("thread_subscriptions").InsertOne(ctx, &seedTS)
	require.NoError(t, err)

	// Apply an older (t1) event.
	require.NoError(t, store.ApplyThreadRead(ctx, "r1", "tr1", "alice", []string{"p2"}, false, t1))

	// Subscription IS overwritten (no guard).
	var gotSub model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sub-1"}).Decode(&gotSub))
	assert.Equal(t, []string{"p2"}, gotSub.ThreadUnread)
	assert.False(t, gotSub.Alert)

	// ThreadSubscription is NOT regressed (guard works).
	var gotTS model.ThreadSubscription
	require.NoError(t, db.Collection("thread_subscriptions").FindOne(ctx, bson.M{"_id": "tsub-1"}).Decode(&gotTS))
	require.NotNil(t, gotTS.LastSeenAt)
	assert.Equal(t, t2, gotTS.LastSeenAt.UTC().Truncate(time.Millisecond))
}

func TestInboxStore_ApplyThreadRead_MissingSubscription_NoError(t *testing.T) {
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
	ctx := context.Background()

	// Seed only a ThreadSubscription.
	now := time.Now().UTC().Truncate(time.Millisecond)
	seedTS := model.ThreadSubscription{
		ID: "tsub-1", ParentMessageID: "p1", RoomID: "r1",
		ThreadRoomID: "tr1", UserAccount: "alice", SiteID: "site-b",
		HasMention: true, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
	}
	_, err := db.Collection("thread_subscriptions").InsertOne(ctx, &seedTS)
	require.NoError(t, err)

	require.NoError(t, store.ApplyThreadRead(ctx, "r1", "tr1", "alice", []string{"p2"}, true, now))

	var gotTS model.ThreadSubscription
	require.NoError(t, db.Collection("thread_subscriptions").FindOne(ctx, bson.M{"_id": "tsub-1"}).Decode(&gotTS))
	assert.False(t, gotTS.HasMention)
}

func TestInboxStore_ApplyThreadRead_MissingThreadSubscription_NoError(t *testing.T) {
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
	ctx := context.Background()

	seedSub := model.Subscription{
		ID: "sub-1", RoomID: "r1", SiteID: "site-b",
		User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		JoinedAt: time.Now().UTC().Add(-time.Hour), ThreadUnread: []string{"p1"}, Alert: true,
	}
	_, err := db.Collection("subscriptions").InsertOne(ctx, &seedSub)
	require.NoError(t, err)

	require.NoError(t, store.ApplyThreadRead(ctx, "r1", "tr-missing", "alice", nil, false, time.Now().UTC()))

	var gotSub model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sub-1"}).Decode(&gotSub))
	assert.Nil(t, gotSub.ThreadUnread)
	assert.False(t, gotSub.Alert)
}
```

- [ ] **Step 6: Run all the inbox-worker tests to verify the unit + integration tests pass**

Run: `go test ./inbox-worker/ -run 'TestHandler_HandleEvent_ThreadRead' -race -v`
Expected: PASS (3 unit tests).

Run: `make test-integration SERVICE=inbox-worker`
Expected: PASS (5 new integration tests).

- [ ] **Step 7: Run the full inbox-worker test suite to confirm no regression**

Run: `make test SERVICE=inbox-worker && make lint`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add inbox-worker/handler.go inbox-worker/main.go inbox-worker/handler_test.go inbox-worker/integration_test.go
git commit -m "feat(inbox-worker): mirror thread_read events to local subscription cache"
```

---

## Task 7 — Client-API documentation

**Files:**
- Modify: `docs/client-api.md`

- [ ] **Step 1: Locate the insertion point**

Open `docs/client-api.md`. Find the existing "Mark Messages Read" subsection (around line 736-784). The new subsection goes immediately after it, before "Read Message Receipts" (around line 786).

- [ ] **Step 2: Insert the new subsection**

Insert the following block between the closing `---` of "Mark Messages Read" and the heading `#### Read Message Receipts`:

```markdown
#### Mark Thread as Read

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.message.thread.read`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

A **synchronous RPC** that clears a single thread's unread state for the caller. `room-service` validates room membership and thread-subscription existence, removes the threadId from the user's `Subscription.ThreadUnread`, recomputes the per-subscription `alert` flag, refreshes the `ThreadSubscription` (`lastSeenAt`, `updatedAt`, `hasMention=false`), and — for cross-site users — publishes a `thread_read` event to the user's home-site outbox so the destination `inbox-worker` can mirror both updates.

##### Request body

| Field      | Type   | Notes |
|------------|--------|-------|
| `threadId` | string | Required. The thread's parent message ID. Empty / missing → `threadId is required`. |

```json
{ "threadId": "01970a4f8c2d7c9aQRST" }
```

##### Success response

| Field    | Type   | Notes |
|----------|--------|-------|
| `status` | string | Always `"accepted"`. |

```json
{ "status": "accepted" }
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

- `"only room members can list members"` — the caller has no subscription in the room (sentinel reused across membership-gated RPCs).
- `"thread subscription not found"` — the caller has no `ThreadSubscription` for the supplied `threadId` in the supplied room. Also returned when the thread exists but belongs to a different room than the one in the subject.
- `"threadId is required"` — body is missing `threadId` or sends an empty string.
- `"invalid message-thread-read subject: …"` — the subject is malformed.

##### Behaviour notes

- **Alert recomputation:** new `alert = oldSub.alert && len(newThreadUnread) > 0`. A thread-read can only clear an alert, never set one. When the post-removal `threadUnread` is empty, `alert` becomes false.
- **Concurrent local writes:** the room-`Subscription` update and the `ThreadSubscription` update run in parallel inside an `errgroup`. Both must succeed before the handler proceeds.
- **Cross-site federation:** if the user's home site differs from the handler's site, a `thread_read` event is published to `outbox.{handlerSite}.to.{userSite}.thread_read` with payload `{account, roomId, threadRoomId, parentMessageId, newThreadUnread, alert, lastSeenAt, timestamp}` (timestamps as `int64` UnixMilli). The destination `inbox-worker` applies the supplied `newThreadUnread`+`alert` to the local Subscription cache and applies `lastSeenAt`+`updatedAt`+`hasMention=false` to the local ThreadSubscription with an `$lt` order-safety guard so out-of-order delivery cannot regress the thread's read position.
- **Defensive `roomId` filter:** the thread-subscription lookup additionally enforces that the supplied `threadId` belongs to the room named in the subject. Mismatches return `thread subscription not found` (rather than silently clearing an unrelated thread).
- **No system message, no fan-out events:** thread reads are silent; only the requester receives the `accepted` reply.

##### Triggered events — success path

`None — reply only.` (Cross-site users may observe a delayed cache update on their home site via the outbox/inbox flow above; this is treated as cache convergence rather than a client-visible event for this RPC.)

##### Triggered events — error path

`None — error returned only via the reply subject.`

---
```

- [ ] **Step 3: Verify the doc file is well-formed**

Run: `git diff docs/client-api.md | head -200`
Expected: the insertion appears in the correct location, no other lines modified.

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): document message.thread.read RPC"
```

---

## Task 8 — Full-suite verification + push

**Files:** none (verification only).

- [ ] **Step 1: Run lint + unit + integration across the affected services**

Run: `make lint && make test && make test-integration SERVICE=room-service && make test-integration SERVICE=inbox-worker`
Expected: PASS.

- [ ] **Step 2: Run SAST locally**

Run: `make sast`
Expected: PASS (no medium+ findings introduced).

- [ ] **Step 3: Push the branch**

```bash
git push -u origin claude/add-thread-read-rpc-ksasD
```

If a push fails due to a network error, retry up to four times with exponential backoff (2s, 4s, 8s, 16s). Do not push to any branch other than `claude/add-thread-read-rpc-ksasD`.

- [ ] **Step 4: Done**

The branch is ready for review. Do NOT open a PR unless explicitly asked.

---

## Notes for the implementing engineer

- **TDD discipline.** Each task's tests come first. Run them, see the Red, then write the implementation. Never skip the Red phase.
- **Mock regeneration.** Any time a store interface changes, `make generate SERVICE=<name>` rewrites `mock_store_test.go`. Never hand-edit those files.
- **`make` only.** Per `CLAUDE.md`, always use the Makefile targets — never raw `go test`/`go build` against the affected services. (The plan uses raw `go test` in a few quick-feedback steps; those are fine for inner-loop iteration, but the final verification in Task 8 uses `make`.)
- **`-race` is mandatory.** The handler's `errgroup` introduces concurrency; always run handler tests with `-race`. The Makefile's `test` target already includes this.
- **Don't push to master.** All work lives on `claude/add-thread-read-rpc-ksasD`.
- **Reference design.** When in doubt about a pattern (sanitizeError allow-list shape, outbox publish wrapping, fixture style, etc.), the closest precedent is the `message.read` RPC committed on 2026-05-04. Read those files alongside this plan.
