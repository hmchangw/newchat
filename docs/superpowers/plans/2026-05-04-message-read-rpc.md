# `message.read` RPC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `chat.user.{account}.request.room.{roomID}.{siteID}.message.read` NATS RPC that marks a room as read for a user, recomputes alert state, persists `LastSeenAt`, optionally recomputes `Room.MinUserLastSeenAt`, and federates the read state to the user's home site via outbox/inbox.

**Architecture:** Synchronous handler in `room-service` performs the local subscription update + (conditional) room recompute inline; cross-site users receive a `subscription_read` outbox event that the destination site's `inbox-worker` applies with an out-of-order-safe `$lt` guard. Each member of a room has a subscription doc at *both* the room's home site and the user's home site — the handler updates the room-side copy authoritatively and outboxes a sync to the user-side copy.

**Tech Stack:** Go 1.25, NATS request/reply (`otelnats`), MongoDB (`go.mongodb.org/mongo-driver/v2`), `go.uber.org/mock` for room-service mocks, hand-written stubs for inbox-worker, `testcontainers-go` for integration tests, `testify` assertions.

---

## File Inventory

| File | Disposition | Responsibility |
| --- | --- | --- |
| `pkg/subject/subject.go` | Modify | Add `MessageRead` + `MessageReadWildcard` builders |
| `pkg/subject/subject_test.go` | Modify | Round-trip tests for the new builders |
| `pkg/model/subscription.go` | Modify | Add `MessageReadRequest` |
| `pkg/model/event.go` | Modify | Add `OutboxSubscriptionRead` constant + `SubscriptionReadEvent` struct |
| `pkg/model/model_test.go` | Modify | JSON round-trip for `SubscriptionReadEvent` |
| `room-service/store.go` | Modify | Add 4 methods to `RoomStore` |
| `room-service/store_mongo.go` | Modify | Mongo impls for the 4 methods |
| `room-service/mock_store_test.go` | Regenerate | `make generate SERVICE=room-service` |
| `room-service/handler.go` | Modify | `handleMessageRead` + `natsMessageRead` + `RegisterCRUD` line |
| `room-service/handler_test.go` | Modify | 14 table-driven cases for `handleMessageRead` |
| `room-service/integration_test.go` | Modify | Integration tests for the 4 new store methods |
| `inbox-worker/handler.go` | Modify | Add `UpdateSubscriptionRead` to `InboxStore`; add `case OutboxSubscriptionRead`; add `handleSubscriptionRead` |
| `inbox-worker/main.go` | Modify | Mongo impl `(*mongoInboxStore).UpdateSubscriptionRead` with `$lt` guard |
| `inbox-worker/handler_test.go` | Modify | Extend `stubInboxStore`; add `subscription_read` cases |
| `inbox-worker/integration_test.go` | Modify | Out-of-order safety, idempotent replay, missing-sub no-op |

---

## Task 1: Subject builders

**Files:**
- Modify: `pkg/subject/subject.go`
- Modify: `pkg/subject/subject_test.go`

- [ ] **Step 1: Write failing test in `pkg/subject/subject_test.go`**

Append at the end of the file:

```go
func TestMessageRead(t *testing.T) {
	got := subject.MessageRead("alice", "r1", "site-a")
	want := "chat.user.alice.request.room.r1.site-a.message.read"
	if got != want {
		t.Errorf("MessageRead: got %q, want %q", got, want)
	}
}

func TestMessageReadWildcard(t *testing.T) {
	got := subject.MessageReadWildcard("site-a")
	want := "chat.user.*.request.room.*.site-a.message.read"
	if got != want {
		t.Errorf("MessageReadWildcard: got %q, want %q", got, want)
	}
}

func TestMessageRead_ParseUserRoomSubject(t *testing.T) {
	subj := subject.MessageRead("alice", "r1", "site-a")
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok || account != "alice" || roomID != "r1" {
		t.Errorf("parse: got (%q,%q,%v), want (alice,r1,true)", account, roomID, ok)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/subject/ -run 'TestMessageRead' -v`
Expected: compile error (`MessageRead` / `MessageReadWildcard` undefined).

- [ ] **Step 3: Add the builders to `pkg/subject/subject.go`**

Add at the end of the existing builder section (right after `MemberAddWildcard`):

```go
// MessageRead returns the concrete subject for the per-user message-read RPC.
// Pair with MessageReadWildcard for room-service's QueueSubscribe.
func MessageRead(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.message.read", account, roomID, siteID)
}

// MessageReadWildcard is the per-site subscription pattern for the message-read RPC.
func MessageReadWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.message.read", siteID)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/subject/ -v`
Expected: all tests PASS.

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "Add MessageRead and MessageReadWildcard subject builders

For the new chat.user.{account}.request.room.{roomID}.{siteID}.message.read
RPC handled by room-service.

https://claude.ai/code/session_01G2qCzHCqcLBUPVExe7XzZq"
```

---

## Task 2: Model types — request body, outbox event type, payload

**Files:**
- Modify: `pkg/model/subscription.go`
- Modify: `pkg/model/event.go`
- Modify: `pkg/model/model_test.go`

- [ ] **Step 1: Write failing round-trip test in `pkg/model/model_test.go`**

Append at the end of the file:

```go
func TestSubscriptionReadEventJSON(t *testing.T) {
	src := model.SubscriptionReadEvent{
		Account:    "alice",
		RoomID:     "r1",
		LastSeenAt: 1735689600000,
		Alert:      true,
		Timestamp:  1735689600001,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.SubscriptionReadEvent
	require.NoError(t, json.Unmarshal(data, &dst))
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
	}
}

func TestMessageReadRequestJSON(t *testing.T) {
	src := model.MessageReadRequest{RoomID: "r1"}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.MessageReadRequest
	require.NoError(t, json.Unmarshal(data, &dst))
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
	}
}

func TestOutboxSubscriptionReadConstant(t *testing.T) {
	if model.OutboxSubscriptionRead != "subscription_read" {
		t.Errorf("got %q, want %q", model.OutboxSubscriptionRead, "subscription_read")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/model/ -run 'TestSubscriptionReadEventJSON|TestMessageReadRequestJSON|TestOutboxSubscriptionReadConstant' -v`
Expected: compile error (types undefined).

- [ ] **Step 3: Add `MessageReadRequest` to `pkg/model/subscription.go`**

Append at the end of the file:

```go
// MessageReadRequest is the body of a message.read RPC. The roomID is
// validated against the subject; an empty body is treated as "trust the
// subject".
type MessageReadRequest struct {
	RoomID string `json:"roomId"`
}
```

- [ ] **Step 4: Add the outbox constant and event type to `pkg/model/event.go`**

In the `OutboxEventType` const block, add after `OutboxMemberRemoved`:

```go
	OutboxSubscriptionRead OutboxEventType = "subscription_read"
```

After the `MemberAddEvent` struct (or anywhere in the file is fine), add:

```go
// SubscriptionReadEvent is the OutboxEvent.Payload for type
// "subscription_read". Sent from a room's home site to the user's home site
// when a user marks the room as read; the destination updates its local
// subscription cache. LastSeenAt is UnixMilli (UTC) for cross-language wire
// safety; Timestamp is the publish time.
type SubscriptionReadEvent struct {
	Account    string `json:"account"`
	RoomID     string `json:"roomId"`
	LastSeenAt int64  `json:"lastSeenAt"`
	Alert      bool   `json:"alert"`
	Timestamp  int64  `json:"timestamp"`
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./pkg/model/ -v`
Expected: all tests PASS.

- [ ] **Step 6: Run lint**

Run: `make lint`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add pkg/model/subscription.go pkg/model/event.go pkg/model/model_test.go
git commit -m "Add MessageReadRequest and SubscriptionReadEvent model types

- MessageReadRequest: NATS request body for the message.read RPC
- OutboxSubscriptionRead: outbox event type constant 'subscription_read'
- SubscriptionReadEvent: payload of the cross-site sync event, with
  int64 UnixMilli timestamps for wire safety

https://claude.ai/code/session_01G2qCzHCqcLBUPVExe7XzZq"
```

---

## Task 3: Room-service store — interface, Mongo impl, integration tests

**Files:**
- Modify: `room-service/store.go`
- Modify: `room-service/store_mongo.go`
- Regenerate: `room-service/mock_store_test.go`
- Modify: `room-service/integration_test.go`

- [ ] **Step 1: Write failing integration tests in `room-service/integration_test.go`**

Locate the existing `TestMongoStore_*` block (search for `func TestMongoStore_GetSubscription` or similar) and append these new tests at the end of the file:

```go
func TestMongoStore_UpdateSubscriptionRead(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := main_NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{
		ID:         "s1",
		User:       model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:     "r1",
		SiteID:     "site-a",
		JoinedAt:   time.Now().UTC().Add(-time.Hour),
		Alert:      true,
	}))

	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, store.UpdateSubscriptionRead(ctx, "r1", "alice", now, false))

	got, err := store.GetSubscription(ctx, "alice", "r1")
	require.NoError(t, err)
	assert.Equal(t, false, got.Alert)
	require.NotNil(t, got.LastSeenAt)
	assert.WithinDuration(t, now, *got.LastSeenAt, time.Second)

	err = store.UpdateSubscriptionRead(ctx, "r1", "missing", now, false)
	assert.ErrorIs(t, err, model.ErrSubscriptionNotFound)
}

func TestMongoStore_GetUserSiteID(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := main_NewMongoStore(db)

	_, err := db.Collection("users").InsertOne(ctx, model.User{
		ID:      "u1",
		Account: "alice",
		SiteID:  "site-b",
	})
	require.NoError(t, err)

	got, err := store.GetUserSiteID(ctx, "alice")
	require.NoError(t, err)
	assert.Equal(t, "site-b", got)

	got, err = store.GetUserSiteID(ctx, "missing")
	require.NoError(t, err)
	assert.Equal(t, "", got)
}

func TestMongoStore_MinSubscriptionLastSeenByRoomID(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := main_NewMongoStore(db)

	earliest := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	mid := earliest.Add(15 * time.Minute)
	latest := earliest.Add(45 * time.Minute)

	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", JoinedAt: earliest, LastSeenAt: &latest,
	}))
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{
		ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"},
		RoomID: "r1", JoinedAt: mid, LastSeenAt: &latest,
	}))
	// Sub with zero LastSeenAt — must contribute its joinedAt (mid).
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{
		ID: "s3", User: model.SubscriptionUser{ID: "u3", Account: "carol"},
		RoomID: "r1", JoinedAt: mid,
	}))

	got, err := store.MinSubscriptionLastSeenByRoomID(ctx, "r1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.WithinDuration(t, mid, *got, time.Second)

	// Empty room → nil.
	got, err = store.MinSubscriptionLastSeenByRoomID(ctx, "empty")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestMongoStore_UpdateRoomMinUserLastSeenAt(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := main_NewMongoStore(db)

	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, store.CreateRoom(ctx, &model.Room{
		ID: "r1", Name: "x", Type: model.RoomTypeChannel, CreatedAt: now, UpdatedAt: now,
	}))

	require.NoError(t, store.UpdateRoomMinUserLastSeenAt(ctx, "r1", &now))
	r, err := store.GetRoom(ctx, "r1")
	require.NoError(t, err)
	require.NotNil(t, r.MinUserLastSeenAt)
	assert.WithinDuration(t, now, *r.MinUserLastSeenAt, time.Second)

	require.NoError(t, store.UpdateRoomMinUserLastSeenAt(ctx, "r1", nil))
	r, err = store.GetRoom(ctx, "r1")
	require.NoError(t, err)
	assert.Nil(t, r.MinUserLastSeenAt)
}
```

Note: if the existing tests reference `NewMongoStore` directly without an alias (most likely), use `NewMongoStore` instead of `main_NewMongoStore` — the test file is `package main`. Adjust accordingly.

- [ ] **Step 2: Run tests to verify they fail compilation**

Run: `make test-integration SERVICE=room-service` (or `go test -tags=integration ./room-service/ -run TestMongoStore_UpdateSubscriptionRead -v`)
Expected: compile error (the four methods don't exist on `*MongoStore`).

- [ ] **Step 3: Extend `RoomStore` interface in `room-service/store.go`**

Add `time` to the imports if missing. Append these methods inside the `RoomStore` interface (after `ListOrgMembers`):

```go
	// UpdateSubscriptionRead sets lastSeenAt and alert on the subscription
	// keyed by (roomID, account). Returns model.ErrSubscriptionNotFound
	// (wrapped) when no subscription matches.
	UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) error

	// GetUserSiteID returns the home site of a user looked up by account.
	// Returns ("", nil) when the user is not found locally; callers treat
	// that as "skip cross-site outbox".
	GetUserSiteID(ctx context.Context, account string) (string, error)

	// MinSubscriptionLastSeenByRoomID returns the minimum effective
	// lastSeenAt across all subscriptions for roomID. Subscriptions whose
	// lastSeenAt is the zero value contribute their joinedAt instead.
	// Returns nil when there are no subscriptions for the room.
	MinSubscriptionLastSeenByRoomID(ctx context.Context, roomID string) (*time.Time, error)

	// UpdateRoomMinUserLastSeenAt writes rooms.minUserLastSeenAt for roomID.
	// A nil value clears the field via $unset; a non-nil value writes via $set.
	UpdateRoomMinUserLastSeenAt(ctx context.Context, roomID string, t *time.Time) error
```

- [ ] **Step 4: Implement the methods in `room-service/store_mongo.go`**

Add `time` to the imports (alongside the existing imports). Append at the end of the file:

```go
// UpdateSubscriptionRead sets lastSeenAt and alert on the subscription
// keyed by (roomID, account). Returns model.ErrSubscriptionNotFound when no
// subscription matches.
func (s *MongoStore) UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) error {
	res, err := s.subscriptions.UpdateOne(ctx,
		bson.M{"roomId": roomID, "u.account": account},
		bson.M{"$set": bson.M{"lastSeenAt": lastSeenAt, "alert": alert}},
	)
	if err != nil {
		return fmt.Errorf("update subscription read for %q in room %q: %w", account, roomID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("update subscription read for %q in room %q: %w", account, roomID, model.ErrSubscriptionNotFound)
	}
	return nil
}

// GetUserSiteID looks up users.siteId by account. Returns ("", nil) if no
// user document exists.
func (s *MongoStore) GetUserSiteID(ctx context.Context, account string) (string, error) {
	var doc struct {
		SiteID string `bson:"siteId"`
	}
	err := s.users.FindOne(ctx, bson.M{"account": account},
		options.FindOne().SetProjection(bson.M{"siteId": 1, "_id": 0})).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", nil
		}
		return "", fmt.Errorf("get user siteId for %q: %w", account, err)
	}
	return doc.SiteID, nil
}

// MinSubscriptionLastSeenByRoomID returns the minimum effective lastSeenAt
// across the room's subscriptions, falling back to joinedAt for subs that
// have never been read (lastSeenAt missing or equal to the BSON zero date).
func (s *MongoStore) MinSubscriptionLastSeenByRoomID(ctx context.Context, roomID string) (*time.Time, error) {
	zeroTime := time.Time{}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"roomId": roomID}}},
		{{Key: "$group", Value: bson.M{
			"_id": nil,
			"min": bson.M{"$min": bson.M{"$cond": bson.A{
				bson.M{"$or": bson.A{
					bson.M{"$eq": bson.A{"$lastSeenAt", nil}},
					bson.M{"$lte": bson.A{"$lastSeenAt", zeroTime}},
				}},
				"$joinedAt",
				"$lastSeenAt",
			}}},
		}}},
	}
	cursor, err := s.subscriptions.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate min lastSeenAt for room %q: %w", roomID, err)
	}
	defer cursor.Close(ctx)
	if !cursor.Next(ctx) {
		if err := cursor.Err(); err != nil {
			return nil, fmt.Errorf("iterate min lastSeenAt for room %q: %w", roomID, err)
		}
		return nil, nil
	}
	var result struct {
		Min time.Time `bson:"min"`
	}
	if err := cursor.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode min lastSeenAt for room %q: %w", roomID, err)
	}
	return &result.Min, nil
}

// UpdateRoomMinUserLastSeenAt sets or clears rooms.minUserLastSeenAt for roomID.
func (s *MongoStore) UpdateRoomMinUserLastSeenAt(ctx context.Context, roomID string, t *time.Time) error {
	var update bson.M
	if t == nil {
		update = bson.M{"$unset": bson.M{"minUserLastSeenAt": ""}}
	} else {
		update = bson.M{"$set": bson.M{"minUserLastSeenAt": *t}}
	}
	if _, err := s.rooms.UpdateOne(ctx, bson.M{"_id": roomID}, update); err != nil {
		return fmt.Errorf("update minUserLastSeenAt for room %q: %w", roomID, err)
	}
	return nil
}
```

- [ ] **Step 5: Regenerate room-service mocks**

Run: `make generate SERVICE=room-service`
Expected: `room-service/mock_store_test.go` updated; no errors.

- [ ] **Step 6: Run unit tests to verify the package still builds**

Run: `make test SERVICE=room-service`
Expected: existing tests still PASS (no new tests yet for the handler).

- [ ] **Step 7: Run integration tests**

Run: `make test-integration SERVICE=room-service`
Expected: all four new tests PASS.

- [ ] **Step 8: Run lint**

Run: `make lint`
Expected: no errors.

- [ ] **Step 9: Commit**

```bash
git add room-service/store.go room-service/store_mongo.go room-service/mock_store_test.go room-service/integration_test.go
git commit -m "Add 4 store methods for message.read flow

- UpdateSubscriptionRead: write lastSeenAt + alert by (roomID, account)
- GetUserSiteID: look up users.siteId by account ('' on miss, no error)
- MinSubscriptionLastSeenByRoomID: aggregate min lastSeenAt with
  joinedAt fallback for never-read subs
- UpdateRoomMinUserLastSeenAt: set/unset rooms.minUserLastSeenAt

Existing indexes already cover all new queries.

https://claude.ai/code/session_01G2qCzHCqcLBUPVExe7XzZq"
```

---

## Task 4: Room-service handler — `handleMessageRead` + registration + unit tests

**Files:**
- Modify: `room-service/handler.go`
- Modify: `room-service/handler_test.go`

- [ ] **Step 1: Write the failing handler tests in `room-service/handler_test.go`**

Append at the end of the file:

```go
// --- message.read tests ---

type messageReadFixture struct {
	store           *MockRoomStore
	publishedSubj   string
	publishedData   []byte
	publishCallErr  error
	publishCalls    int
	handler         *Handler
}

func newMessageReadFixture(t *testing.T) *messageReadFixture {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	f := &messageReadFixture{store: store}
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

func TestHandler_MessageRead_InvalidSubject(t *testing.T) {
	f := newMessageReadFixture(t)
	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	_, err := f.handler.handleMessageRead(context.Background(), "garbage", body)
	require.Error(t, err)
}

func TestHandler_MessageRead_RoomIDMismatch(t *testing.T) {
	f := newMessageReadFixture(t)
	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r2"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "room ID mismatch")
}

func TestHandler_MessageRead_NotMember(t *testing.T) {
	f := newMessageReadFixture(t)
	f.store.EXPECT().
		GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, model.ErrSubscriptionNotFound)
	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.ErrorIs(t, err, errNotRoomMember)
}

func TestHandler_MessageRead_HappyLocal_AlertClears(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
		Alert: true, ThreadUnread: nil,
	}, nil)
	f.store.EXPECT().
		UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	minT := lastSeen
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&minT, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &minT).Return(nil)

	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	resp, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.NoError(t, err)

	var got map[string]string
	require.NoError(t, json.Unmarshal(resp, &got))
	assert.Equal(t, "accepted", got["status"])
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageRead_AlertStaysTrueWithThreadUnread(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
		Alert: true, ThreadUnread: []string{"t1"},
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), true).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&lastSeen, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &lastSeen).Return(nil)

	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.NoError(t, err)
}

func TestHandler_MessageRead_LastSeenZeroFallsBackToJoinedAt(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(time.Hour) // joined in the future relative to lastMsg
	lastMsg := time.Now().UTC().Add(-time.Hour)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, // LastSeenAt is zero
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	// Early-return path: joinedAt > lastMsg → no min/update calls.

	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.NoError(t, err)
}

func TestHandler_MessageRead_RoomLastMsgNil_EarlyReturn(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-time.Hour)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", JoinedAt: joined,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: nil}, nil)

	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.NoError(t, err)
}

func TestHandler_MessageRead_CrossSite_PublishesOutbox(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
		Alert: true, ThreadUnread: []string{"t1"},
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), true).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&lastSeen, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &lastSeen).Return(nil)

	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.NoError(t, err)

	assert.Equal(t, 1, f.publishCalls)
	assert.Equal(t, "outbox.site-a.to.site-b.subscription_read", f.publishedSubj)

	var outbox model.OutboxEvent
	require.NoError(t, json.Unmarshal(f.publishedData, &outbox))
	assert.Equal(t, model.OutboxSubscriptionRead, outbox.Type)
	assert.Equal(t, "site-a", outbox.SiteID)
	assert.Equal(t, "site-b", outbox.DestSiteID)

	var inner model.SubscriptionReadEvent
	require.NoError(t, json.Unmarshal(outbox.Payload, &inner))
	assert.Equal(t, "alice", inner.Account)
	assert.Equal(t, "r1", inner.RoomID)
	assert.True(t, inner.Alert)
	assert.Greater(t, inner.LastSeenAt, int64(0))
}

func TestHandler_MessageRead_CrossSite_PublishFailureAborts(t *testing.T) {
	f := newMessageReadFixture(t)
	f.publishCallErr = fmt.Errorf("nats down")
	joined := time.Now().UTC().Add(-time.Hour)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", JoinedAt: joined,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)
	// GetRoom must NOT be called after a publish failure.

	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.Error(t, err)
}

func TestHandler_MessageRead_GetUserSiteIDEmpty_NoPublish(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&lastSeen, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &lastSeen).Return(nil)

	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.NoError(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageRead_GetUserSiteIDError_Aborts(t *testing.T) {
	f := newMessageReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: time.Now().UTC().Add(-time.Hour),
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("", errors.New("mongo down"))

	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageRead_MinNil_ClearsRoomField(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	var nilTime *time.Time
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(nilTime, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", nilTime).Return(nil)

	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.NoError(t, err)
}

func TestHandler_MessageRead_UpdateSubscriptionReadError(t *testing.T) {
	f := newMessageReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: time.Now().UTC().Add(-time.Hour),
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).
		Return(errors.New("mongo down"))

	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.Error(t, err)
}

func TestHandler_MessageRead_GetRoomError(t *testing.T) {
	f := newMessageReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: time.Now().UTC().Add(-time.Hour),
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(nil, errors.New("mongo down"))

	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.Error(t, err)
}

func TestHandler_MessageRead_MinSubscriptionError(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(nil, errors.New("agg failed"))

	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.Error(t, err)
}

func TestHandler_MessageRead_UpdateRoomMinError(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&lastSeen, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", gomock.Any()).Return(errors.New("mongo down"))

	body, _ := json.Marshal(model.MessageReadRequest{RoomID: "r1"})
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, body)
	require.Error(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail to compile**

Run: `make test SERVICE=room-service`
Expected: compile error (`handleMessageRead` undefined).

- [ ] **Step 3: Implement the handler in `room-service/handler.go`**

Add the registration line inside `RegisterCRUD` (after the existing `MemberAdd` line):

```go
	if _, err := nc.QueueSubscribe(subject.MessageReadWildcard(h.siteID), queue, h.natsMessageRead); err != nil {
		return fmt.Errorf("subscribe message read: %w", err)
	}
```

Append two new methods at the end of the file:

```go
func (h *Handler) natsMessageRead(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleMessageRead(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("message read failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to message read", "error", err)
	}
}

func (h *Handler) handleMessageRead(ctx context.Context, subj string, data []byte) ([]byte, error) {
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid message-read subject: %s", subj)
	}
	var req model.MessageReadRequest
	if len(data) > 0 {
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, fmt.Errorf("invalid request: %w", err)
		}
	}
	if req.RoomID != "" && req.RoomID != roomID {
		return nil, fmt.Errorf("room ID mismatch")
	}

	sub, err := h.store.GetSubscription(ctx, account, roomID)
	switch {
	case errors.Is(err, model.ErrSubscriptionNotFound):
		return nil, errNotRoomMember
	case err != nil:
		return nil, fmt.Errorf("get subscription: %w", err)
	}

	newAlert := sub.Alert && len(sub.ThreadUnread) > 0
	originalLastSeen := sub.JoinedAt
	if sub.LastSeenAt != nil {
		originalLastSeen = *sub.LastSeenAt
	}
	now := time.Now().UTC()

	if err := h.store.UpdateSubscriptionRead(ctx, roomID, account, now, newAlert); err != nil {
		return nil, fmt.Errorf("update subscription read: %w", err)
	}

	userSiteID, err := h.store.GetUserSiteID(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("get user siteId: %w", err)
	}
	switch {
	case userSiteID == "":
		slog.Warn("user not found locally; skipping cross-site outbox", "account", account)
	case userSiteID != h.siteID:
		payload := model.SubscriptionReadEvent{
			Account:    account,
			RoomID:     roomID,
			LastSeenAt: now.UnixMilli(),
			Alert:      newAlert,
			Timestamp:  now.UnixMilli(),
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal subscription_read payload: %w", err)
		}
		outbox := model.OutboxEvent{
			Type:       model.OutboxSubscriptionRead,
			SiteID:     h.siteID,
			DestSiteID: userSiteID,
			Payload:    payloadData,
			Timestamp:  now.UnixMilli(),
		}
		outboxData, err := json.Marshal(outbox)
		if err != nil {
			return nil, fmt.Errorf("marshal outbox event: %w", err)
		}
		if err := h.publishToStream(ctx, subject.Outbox(h.siteID, userSiteID, model.OutboxSubscriptionRead), outboxData); err != nil {
			return nil, fmt.Errorf("publish subscription_read outbox: %w", err)
		}
	}

	room, err := h.store.GetRoom(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("get room: %w", err)
	}
	if room.LastMsgAt == nil || originalLastSeen.After(*room.LastMsgAt) {
		return json.Marshal(map[string]string{"status": "accepted"})
	}

	minTime, err := h.store.MinSubscriptionLastSeenByRoomID(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("min subscription lastSeenAt: %w", err)
	}
	if err := h.store.UpdateRoomMinUserLastSeenAt(ctx, roomID, minTime); err != nil {
		return nil, fmt.Errorf("update room minUserLastSeenAt: %w", err)
	}

	return json.Marshal(map[string]string{"status": "accepted"})
}
```

- [ ] **Step 4: Run unit tests**

Run: `make test SERVICE=room-service`
Expected: all tests PASS, including the 14 new ones.

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "Add message.read RPC handler in room-service

Synchronous handler that:
1. Validates room membership via GetSubscription
2. Recomputes Alert as Sub.Alert && len(ThreadUnread) > 0
3. Falls back to JoinedAt when LastSeenAt is zero
4. Persists the new lastSeenAt + alert
5. Publishes a subscription_read outbox event when the user is
   on a different site (always, before the room recompute, so
   the user's home site receives every read receipt)
6. Skips the room recompute when LastMsgAt is nil or the user
   was already up-to-date before this read
7. Otherwise recomputes Room.MinUserLastSeenAt across all
   subscriptions for the room

Returns {\"status\":\"accepted\"} on success.

https://claude.ai/code/session_01G2qCzHCqcLBUPVExe7XzZq"
```

---

## Task 5: Inbox-worker store — `UpdateSubscriptionRead` (interface + Mongo impl + stub)

**Files:**
- Modify: `inbox-worker/handler.go`
- Modify: `inbox-worker/main.go`
- Modify: `inbox-worker/handler_test.go`

- [ ] **Step 1: Extend `InboxStore` interface in `inbox-worker/handler.go`**

Add `time` to the imports at the top of the file (it's already imported). Append this method to the `InboxStore` interface declaration:

```go
	// UpdateSubscriptionRead sets lastSeenAt and alert on the subscription
	// keyed by (roomID, account). Idempotent and order-safe: the write
	// only applies when the stored lastSeenAt is missing or strictly
	// earlier than the supplied value. Older or duplicate events are
	// silent no-ops. Missing-subscription is also a silent no-op.
	UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) error
```

- [ ] **Step 2: Implement on `mongoInboxStore` in `inbox-worker/main.go`**

Add `time` to the imports if not already present. Append after `BulkCreateSubscriptions`:

```go
func (s *mongoInboxStore) UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) error {
	filter := bson.M{
		"roomId":    roomID,
		"u.account": account,
		"$or": bson.A{
			bson.M{"lastSeenAt": bson.M{"$exists": false}},
			bson.M{"lastSeenAt": bson.M{"$lt": lastSeenAt}},
		},
	}
	update := bson.M{"$set": bson.M{"lastSeenAt": lastSeenAt, "alert": alert}}
	if _, err := s.subCol.UpdateOne(ctx, filter, update); err != nil {
		return fmt.Errorf("update subscription read for %q in room %q: %w", account, roomID, err)
	}
	return nil
}
```

- [ ] **Step 3: Extend the in-memory stub in `inbox-worker/handler_test.go`**

Add the imports `"time"` if not already present. Append a method on `stubInboxStore`:

```go
func (s *stubInboxStore) UpdateSubscriptionRead(_ context.Context, roomID, account string, lastSeenAt time.Time, alert bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.subscriptions {
		if s.subscriptions[i].RoomID == roomID && s.subscriptions[i].User.Account == account {
			// Order-safe: skip if stored lastSeenAt is not strictly earlier.
			if s.subscriptions[i].LastSeenAt != nil && !s.subscriptions[i].LastSeenAt.Before(lastSeenAt) {
				return nil
			}
			ls := lastSeenAt
			s.subscriptions[i].LastSeenAt = &ls
			s.subscriptions[i].Alert = alert
			return nil
		}
	}
	return nil // missing-subscription → no-op
}
```

- [ ] **Step 4: Build to verify compilation**

Run: `make test SERVICE=inbox-worker`
Expected: PASS (existing tests remain green; the new method is unused so far).

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add inbox-worker/handler.go inbox-worker/main.go inbox-worker/handler_test.go
git commit -m "Add UpdateSubscriptionRead to InboxStore

Order-safe write: only applies when the stored lastSeenAt is missing
or strictly earlier than the supplied value, so out-of-order federated
delivery cannot regress the user's read state.

https://claude.ai/code/session_01G2qCzHCqcLBUPVExe7XzZq"
```

---

## Task 6: Inbox-worker handler — `subscription_read` arm + unit tests + integration tests

**Files:**
- Modify: `inbox-worker/handler.go`
- Modify: `inbox-worker/handler_test.go`
- Modify: `inbox-worker/integration_test.go`

- [ ] **Step 1: Write failing unit tests in `inbox-worker/handler_test.go`**

Add a tracker field to `stubInboxStore` near the top of the struct (alongside `roleUpdates`):

```go
	subReads []subRead
```

Add the type near `roleUpdate`:

```go
type subRead struct {
	roomID     string
	account    string
	lastSeenAt time.Time
	alert      bool
}
```

Override the stub's `UpdateSubscriptionRead` to record the call (replace the body added in Task 5):

```go
func (s *stubInboxStore) UpdateSubscriptionRead(_ context.Context, roomID, account string, lastSeenAt time.Time, alert bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subReads = append(s.subReads, subRead{roomID, account, lastSeenAt, alert})
	for i := range s.subscriptions {
		if s.subscriptions[i].RoomID == roomID && s.subscriptions[i].User.Account == account {
			if s.subscriptions[i].LastSeenAt != nil && !s.subscriptions[i].LastSeenAt.Before(lastSeenAt) {
				return nil
			}
			ls := lastSeenAt
			s.subscriptions[i].LastSeenAt = &ls
			s.subscriptions[i].Alert = alert
			return nil
		}
	}
	return nil
}

func (s *stubInboxStore) getSubReads() []subRead {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]subRead, len(s.subReads))
	copy(cp, s.subReads)
	return cp
}
```

Append the failing tests at the end of the file:

```go
func TestHandler_HandleEvent_SubscriptionRead_HappyPath(t *testing.T) {
	store := &stubInboxStore{}
	h := NewHandler(store)

	inner := model.SubscriptionReadEvent{
		Account:    "alice",
		RoomID:     "r1",
		LastSeenAt: time.Now().UTC().UnixMilli(),
		Alert:      true,
		Timestamp:  time.Now().UTC().UnixMilli(),
	}
	innerData, err := json.Marshal(inner)
	require.NoError(t, err)
	evt := model.OutboxEvent{
		Type:       model.OutboxSubscriptionRead,
		SiteID:     "site-a",
		DestSiteID: "site-b",
		Payload:    innerData,
		Timestamp:  inner.Timestamp,
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	require.NoError(t, h.HandleEvent(context.Background(), data))

	calls := store.getSubReads()
	require.Len(t, calls, 1)
	assert.Equal(t, "r1", calls[0].roomID)
	assert.Equal(t, "alice", calls[0].account)
	assert.True(t, calls[0].alert)
	assert.Equal(t, time.UnixMilli(inner.LastSeenAt).UTC(), calls[0].lastSeenAt)
}

func TestHandler_HandleEvent_SubscriptionRead_MalformedPayload(t *testing.T) {
	store := &stubInboxStore{}
	h := NewHandler(store)
	evt := model.OutboxEvent{Type: model.OutboxSubscriptionRead, Payload: []byte("not-json")}
	data, _ := json.Marshal(evt)
	require.Error(t, h.HandleEvent(context.Background(), data))
}
```

- [ ] **Step 2: Run unit tests to verify failure**

Run: `make test SERVICE=inbox-worker`
Expected: failure — `subscription_read` is unknown so `subReads` stays empty, and the `MalformedPayload` test currently swallows unknown types as a `slog.Warn` returning nil rather than an error.

- [ ] **Step 3: Add the `subscription_read` arm to `HandleEvent` and the handler method**

In `inbox-worker/handler.go`, add a case to the switch in `HandleEvent`:

```go
	case model.OutboxSubscriptionRead:
		return h.handleSubscriptionRead(ctx, &evt)
```

Append the handler method at the end of the file:

```go
// handleSubscriptionRead applies a cross-site read receipt to the local
// subscription cache. Idempotent and order-safe via the store's $lt guard.
func (h *Handler) handleSubscriptionRead(ctx context.Context, evt *model.OutboxEvent) error {
	var e model.SubscriptionReadEvent
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal subscription_read payload: %w", err)
	}
	lastSeenAt := time.UnixMilli(e.LastSeenAt).UTC()
	if err := h.store.UpdateSubscriptionRead(ctx, e.RoomID, e.Account, lastSeenAt, e.Alert); err != nil {
		return fmt.Errorf("update subscription read for %q in room %q: %w", e.Account, e.RoomID, err)
	}
	return nil
}
```

- [ ] **Step 4: Run unit tests to verify they pass**

Run: `make test SERVICE=inbox-worker`
Expected: all unit tests PASS.

- [ ] **Step 5: Write failing integration tests in `inbox-worker/integration_test.go`**

Locate the existing setup helpers (search for `setupMongo` or similar). Append at the end of the file:

```go
func TestInbox_UpdateSubscriptionRead_HappyPath(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:  db.Collection("subscriptions"),
		roomCol: db.Collection("rooms"),
		userCol: db.Collection("users"),
	}

	joined := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	_, err := store.subCol.InsertOne(ctx, model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", JoinedAt: joined,
	})
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, store.UpdateSubscriptionRead(ctx, "r1", "alice", now, true))

	var got model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	require.NotNil(t, got.LastSeenAt)
	assert.WithinDuration(t, now, *got.LastSeenAt, time.Second)
	assert.True(t, got.Alert)
}

func TestInbox_UpdateSubscriptionRead_OutOfOrderSkipped(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := &mongoInboxStore{subCol: db.Collection("subscriptions"), roomCol: db.Collection("rooms"), userCol: db.Collection("users")}

	t2 := time.Now().UTC().Truncate(time.Millisecond)
	_, err := store.subCol.InsertOne(ctx, model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", JoinedAt: t2.Add(-time.Hour), LastSeenAt: &t2, Alert: true,
	})
	require.NoError(t, err)

	t1 := t2.Add(-time.Minute)
	require.NoError(t, store.UpdateSubscriptionRead(ctx, "r1", "alice", t1, false))

	var got model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	require.NotNil(t, got.LastSeenAt)
	assert.WithinDuration(t, t2, *got.LastSeenAt, time.Second) // unchanged
	assert.True(t, got.Alert)                                  // unchanged
}

func TestInbox_UpdateSubscriptionRead_EqualTimestampSkipped(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := &mongoInboxStore{subCol: db.Collection("subscriptions"), roomCol: db.Collection("rooms"), userCol: db.Collection("users")}

	t1 := time.Now().UTC().Truncate(time.Millisecond)
	_, err := store.subCol.InsertOne(ctx, model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", JoinedAt: t1.Add(-time.Hour), LastSeenAt: &t1, Alert: true,
	})
	require.NoError(t, err)

	require.NoError(t, store.UpdateSubscriptionRead(ctx, "r1", "alice", t1, false))

	var got model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	assert.True(t, got.Alert) // unchanged
}

func TestInbox_UpdateSubscriptionRead_MissingSubscriptionNoOp(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := &mongoInboxStore{subCol: db.Collection("subscriptions"), roomCol: db.Collection("rooms"), userCol: db.Collection("users")}

	now := time.Now().UTC()
	require.NoError(t, store.UpdateSubscriptionRead(ctx, "missing-room", "ghost", now, false))
}
```

If `setupMongo` and the `mongoInboxStore` literal differ in this file, follow the existing pattern (e.g., they may use a constructor or different collection field names). Adjust accordingly without changing semantics.

- [ ] **Step 6: Run integration tests**

Run: `make test-integration SERVICE=inbox-worker`
Expected: all four new tests PASS.

- [ ] **Step 7: Run lint**

Run: `make lint`
Expected: no errors.

- [ ] **Step 8: Commit**

```bash
git add inbox-worker/handler.go inbox-worker/handler_test.go inbox-worker/integration_test.go
git commit -m "Apply subscription_read events in inbox-worker

Add the case OutboxSubscriptionRead arm to HandleEvent and the
handleSubscriptionRead method that converts the int64 UnixMilli
LastSeenAt back to time.Time and applies the order-safe store write.

Integration tests cover: happy path, out-of-order skip, idempotent
replay, and missing-subscription no-op.

https://claude.ai/code/session_01G2qCzHCqcLBUPVExe7XzZq"
```

---

## Task 7: Final verification

- [ ] **Step 1: Run the full unit test suite**

Run: `make test`
Expected: all tests across all services PASS.

- [ ] **Step 2: Run the full integration test suite**

Run: `make test-integration`
Expected: all integration tests PASS.

- [ ] **Step 3: Run lint and formatter**

Run: `make lint && make fmt`
Expected: clean.

- [ ] **Step 4: Push the branch**

```bash
git push -u origin claude/add-message-read-rpc-3u7K9
```
