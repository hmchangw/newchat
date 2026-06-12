//go:build integration

package main

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func setupMongo(t *testing.T) *mongo.Database {
	return testutil.MongoDB(t, "inbox_worker_test")
}

func TestInboxWorker_MemberAdded_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	store := &mongoInboxStore{
		subCol:  db.Collection("subscriptions"),
		roomCol: db.Collection("rooms"),
		userCol: db.Collection("users"),
	}
	handler := NewHandler(store)

	// Seed user for lookup
	_, err := db.Collection("users").InsertOne(ctx, model.User{ID: "u2", Account: "u2", SiteID: "site-b"})
	if err != nil {
		t.Fatalf("seed user: %v", err)
	}

	// Create outbox event for member_added
	hssMillis := time.Now().UTC().UnixMilli()
	change := model.MemberAddEvent{
		Type: "member_added", RoomID: "r1", Accounts: []string{"u2"}, SiteID: "site-b",
		JoinedAt:           time.Now().UTC().UnixMilli(),
		HistorySharedSince: &hssMillis,
		Timestamp:          time.Now().UTC().UnixMilli(),
	}
	changeData, _ := json.Marshal(change)
	evt := model.OutboxEvent{Type: "member_added", SiteID: "site-a", DestSiteID: "site-b", Payload: changeData}
	evtData, _ := json.Marshal(evt)

	if err := handler.HandleEvent(ctx, evtData); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}

	// Verify subscription was created in MongoDB
	var sub model.Subscription
	err = db.Collection("subscriptions").FindOne(ctx, bson.M{"u._id": "u2", "roomId": "r1"}).Decode(&sub)
	if err != nil {
		t.Fatalf("subscription not found: %v", err)
	}
	if len(sub.Roles) == 0 || sub.Roles[0] != model.RoleMember {
		t.Errorf("Roles = %v, want [member]", sub.Roles)
	}

	// handleMemberAdded does not publish SubscriptionUpdateEvent — room-worker
	// publishes on the user's subject and the NATS supercluster routes it to
	// the user's home site.
}

func TestInboxWorker_RoomSync_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	store := &mongoInboxStore{
		subCol:  db.Collection("subscriptions"),
		roomCol: db.Collection("rooms"),
		userCol: db.Collection("users"),
	}
	handler := NewHandler(store)

	room := model.Room{ID: "r1", Name: "synced-room", Type: model.RoomTypeChannel, UserCount: 5}
	roomData, _ := json.Marshal(room)
	evt := model.OutboxEvent{Type: "room_sync", Payload: roomData}
	evtData, _ := json.Marshal(evt)

	if err := handler.HandleEvent(ctx, evtData); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}

	// Verify room was upserted
	var got model.Room
	err := db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r1"}).Decode(&got)
	if err != nil {
		t.Fatalf("room not found: %v", err)
	}
	if got.Name != "synced-room" {
		t.Errorf("Name = %q, want synced-room", got.Name)
	}
}

func TestInboxWorker_RoleUpdated_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	store := &mongoInboxStore{
		subCol:  db.Collection("subscriptions"),
		roomCol: db.Collection("rooms"),
		userCol: db.Collection("users"),
	}
	handler := NewHandler(store)

	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u2", Account: "bob"},
		RoomID: "room-1", SiteID: "site-a", Roles: []model.Role{model.RoleMember},
	})
	if err != nil {
		t.Fatalf("seed subscription: %v", err)
	}

	subEvt := model.SubscriptionUpdateEvent{
		UserID: "u2",
		Subscription: model.Subscription{
			ID: "s1", User: model.SubscriptionUser{ID: "u2", Account: "bob"},
			RoomID: "room-1", SiteID: "site-a", Roles: []model.Role{model.RoleOwner},
		},
		Action: "role_updated", Timestamp: time.Now().UTC().UnixMilli(),
	}
	subEvtData, _ := json.Marshal(subEvt)

	evt := model.OutboxEvent{
		Type: "role_updated", SiteID: "site-a", DestSiteID: "site-b",
		Payload: subEvtData, Timestamp: time.Now().UTC().UnixMilli(),
	}
	evtData, _ := json.Marshal(evt)

	err = handler.HandleEvent(ctx, evtData)
	if err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}

	var sub model.Subscription
	err = db.Collection("subscriptions").FindOne(ctx, bson.M{"u.account": "bob", "roomId": "room-1"}).Decode(&sub)
	if err != nil {
		t.Fatalf("subscription not found: %v", err)
	}
	if !slices.Contains(sub.Roles, model.RoleOwner) {
		t.Errorf("roles = %v, want to contain owner", sub.Roles)
	}

	// No SubscriptionUpdateEvent is published — room-worker already handles
	// user notification via NATS supercluster routing.
}

// TestInboxWorker_BulkCreateSubscriptions_IdempotentUpsert exercises the
// upsert contract: a redelivered BulkCreateSubscriptions for an already-existing
// (roomId, account) must be a no-op on Mongo — neither create a duplicate nor
// overwrite read-state that accumulated since the first delivery.
func TestInboxWorker_BulkCreateSubscriptions_IdempotentUpsert(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:  db.Collection("subscriptions"),
		roomCol: db.Collection("rooms"),
		userCol: db.Collection("users"),
	}

	originalSeenAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	original := &model.Subscription{
		ID:         "sub-existing",
		User:       model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:     "r1",
		SiteID:     "site-origin",
		Roles:      []model.Role{model.RoleMember},
		LastSeenAt: &originalSeenAt,
		Alert:      true,
		JoinedAt:   originalSeenAt,
	}
	require.NoError(t, store.BulkCreateSubscriptions(ctx, []*model.Subscription{original}))

	// Re-issue with a "fresher" copy that has no LastSeenAt — simulates a
	// redelivered outbox event materializing the same sub.
	redelivered := &model.Subscription{
		ID:       "sub-redelivered",
		User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:   "r1",
		SiteID:   "site-origin",
		Roles:    []model.Role{model.RoleMember},
		JoinedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	newOne := &model.Subscription{
		ID:       "sub-new",
		User:     model.SubscriptionUser{ID: "u2", Account: "bob"},
		RoomID:   "r1",
		SiteID:   "site-origin",
		Roles:    []model.Role{model.RoleMember},
		JoinedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	require.NoError(t, store.BulkCreateSubscriptions(ctx, []*model.Subscription{redelivered, newOne}))

	// Exactly two subs in the room: alice (preserved) + bob (newly inserted).
	count, err := store.subCol.CountDocuments(ctx, bson.M{"roomId": "r1"})
	require.NoError(t, err)
	assert.EqualValues(t, 2, count, "redelivery must not duplicate")

	var existing model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"roomId": "r1", "u.account": "alice"}).Decode(&existing))
	assert.Equal(t, "sub-existing", existing.ID, "existing _id must not change")
	require.NotNil(t, existing.LastSeenAt, "LastSeenAt must be preserved on upsert no-op")
	assert.WithinDuration(t, originalSeenAt, *existing.LastSeenAt, time.Second)
	assert.True(t, existing.Alert, "Alert flag must be preserved")

	var fresh model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"roomId": "r1", "u.account": "bob"}).Decode(&fresh))
	assert.Equal(t, "sub-new", fresh.ID, "new sub must be inserted with its caller-supplied _id")
}

func TestInboxWorker_MemberRemoved_Integration(t *testing.T) {
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:  db.Collection("subscriptions"),
		roomCol: db.Collection("rooms"),
	}
	h := NewHandler(store)

	ctx := context.Background()

	_, err := store.subCol.InsertOne(ctx, model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "bob"},
		RoomID: "r1", SiteID: "site-a", Roles: []model.Role{model.RoleMember},
		JoinedAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	memberEvt := model.MemberRemoveEvent{
		Type: "member-removed", RoomID: "r1", Accounts: []string{"bob"}, SiteID: "site-a",
	}
	payload, _ := json.Marshal(memberEvt)
	evt := model.OutboxEvent{
		Type: "member_removed", SiteID: "site-a", DestSiteID: "site-b",
		Payload: payload, Timestamp: time.Now().UnixMilli(),
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, h.HandleEvent(ctx, data))

	// Subscription deleted — room_members lives only on the room's site.
	count, err := store.subCol.CountDocuments(ctx, bson.M{"u._id": "u1", "roomId": "r1"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// No publish — room-worker handles user notification via NATS supercluster.
}

func TestInbox_UpdateSubscriptionRead_HappyPath(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		roomCol:      db.Collection("rooms"),
		userCol:      db.Collection("users"),
		threadSubCol: db.Collection("thread_subscriptions"),
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
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		roomCol:      db.Collection("rooms"),
		userCol:      db.Collection("users"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}

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
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		roomCol:      db.Collection("rooms"),
		userCol:      db.Collection("users"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}

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
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		roomCol:      db.Collection("rooms"),
		userCol:      db.Collection("users"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}

	now := time.Now().UTC()
	require.NoError(t, store.UpdateSubscriptionRead(ctx, "missing-room", "ghost", now, false))
}

func TestInboxWorker_ThreadSubscriptionUpserted_Insert_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		roomCol:      db.Collection("rooms"),
		userCol:      db.Collection("users"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
	require.NoError(t, store.ensureIndexes(ctx))

	handler := NewHandler(store)

	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	// Subscription.SiteID is the room's home site (site-a). Bob's home is site-b
	// (where this inbox-worker instance lives), inferred from the document being
	// stored on this site rather than from the field.
	sub := model.ThreadSubscription{
		ID: "sub-1", ParentMessageID: "pm-1", RoomID: "r1", ThreadRoomID: "tr-1",
		UserID: "u-bob", UserAccount: "bob", SiteID: "site-a",
		HasMention: false, CreatedAt: now, UpdatedAt: now,
	}
	subData, err := json.Marshal(sub)
	require.NoError(t, err)
	evtData, err := json.Marshal(model.OutboxEvent{
		Type: "thread_subscription_upserted", SiteID: "site-a", DestSiteID: "site-b",
		Payload: subData, Timestamp: now.UnixMilli(),
	})
	require.NoError(t, err)

	require.NoError(t, handler.HandleEvent(ctx, evtData))

	var got model.ThreadSubscription
	require.NoError(t, db.Collection("thread_subscriptions").
		FindOne(ctx, bson.M{"threadRoomId": "tr-1", "userId": "u-bob"}).
		Decode(&got))
	assert.Equal(t, "sub-1", got.ID)
	assert.Equal(t, "site-a", got.SiteID, "SiteID is the room's site, preserved across federation")
	assert.False(t, got.HasMention)
	assert.True(t, got.CreatedAt.Equal(now))
	assert.True(t, got.UpdatedAt.Equal(now))
}

func TestInboxWorker_ThreadSubscriptionUpserted_MonotonicMention_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		roomCol:      db.Collection("rooms"),
		userCol:      db.Collection("users"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
	require.NoError(t, store.ensureIndexes(ctx))

	handler := NewHandler(store)
	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	// First event: HasMention=true. Subscription.SiteID is the room's site (site-a).
	mentionSub := model.ThreadSubscription{
		ID: "sub-1", ParentMessageID: "pm-1", RoomID: "r1", ThreadRoomID: "tr-1",
		UserID: "u-bob", UserAccount: "bob", SiteID: "site-a",
		HasMention: true, CreatedAt: now, UpdatedAt: now,
	}
	mentionData, err := json.Marshal(mentionSub)
	require.NoError(t, err)
	mentionEvt, err := json.Marshal(model.OutboxEvent{
		Type: "thread_subscription_upserted", SiteID: "site-a", DestSiteID: "site-b",
		Payload: mentionData, Timestamp: now.UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, handler.HandleEvent(ctx, mentionEvt))

	// Second event: HasMention=false (later updatedAt). Must NOT clear the flag.
	plainSub := mentionSub
	plainSub.HasMention = false
	later := now.Add(time.Minute)
	plainSub.UpdatedAt = later
	plainData, err := json.Marshal(plainSub)
	require.NoError(t, err)
	plainEvt, err := json.Marshal(model.OutboxEvent{
		Type: "thread_subscription_upserted", SiteID: "site-a", DestSiteID: "site-b",
		Payload: plainData, Timestamp: later.UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, handler.HandleEvent(ctx, plainEvt))

	var got model.ThreadSubscription
	require.NoError(t, db.Collection("thread_subscriptions").
		FindOne(ctx, bson.M{"threadRoomId": "tr-1", "userId": "u-bob"}).
		Decode(&got))
	assert.True(t, got.HasMention, "hasMention must remain true after a non-mention event")
	assert.True(t, got.UpdatedAt.Equal(later), "updatedAt must advance to the later event's value")
	// _id and createdAt come from $setOnInsert and must remain from the first event.
	assert.Equal(t, "sub-1", got.ID)
	assert.True(t, got.CreatedAt.Equal(now))

	// Third event: HasMention=true again. Idempotent — still true, updatedAt advances.
	thirdSub := plainSub
	thirdSub.HasMention = true
	evenLater := later.Add(time.Minute)
	thirdSub.UpdatedAt = evenLater
	thirdData, err := json.Marshal(thirdSub)
	require.NoError(t, err)
	thirdEvt, err := json.Marshal(model.OutboxEvent{
		Type: "thread_subscription_upserted", SiteID: "site-a", DestSiteID: "site-b",
		Payload: thirdData, Timestamp: evenLater.UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, handler.HandleEvent(ctx, thirdEvt))

	require.NoError(t, db.Collection("thread_subscriptions").
		FindOne(ctx, bson.M{"threadRoomId": "tr-1", "userId": "u-bob"}).
		Decode(&got))
	assert.True(t, got.HasMention)
	assert.True(t, got.UpdatedAt.Equal(evenLater))
}

// mustInsertUser inserts a user document directly into the users collection.
func mustInsertUser(t *testing.T, db *mongo.Database, u *model.User) {
	t.Helper()
	_, err := db.Collection("users").InsertOne(context.Background(), u)
	require.NoError(t, err)
}

// newIntegrationHandler creates a Handler wired to the given database for integration tests.
func newIntegrationHandler(t *testing.T, db *mongo.Database) *Handler {
	t.Helper()
	store := &mongoInboxStore{
		subCol:  db.Collection("subscriptions"),
		roomCol: db.Collection("rooms"),
		userCol: db.Collection("users"),
	}
	return NewHandler(store)
}

func TestHandleMemberAdded_Channel_PersistsRemoteSubs(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob",
		SiteID: "site-B", EngName: "Bob", ChineseName: "鲍勃"})
	mustInsertUser(t, db, &model.User{ID: "u_ian", Account: "ian",
		SiteID: "site-B", EngName: "Ian", ChineseName: "伊恩"})

	h := newIntegrationHandler(t, db)

	payload, err := json.Marshal(model.MemberAddEvent{
		Type:             "member_added",
		RoomID:           "r_xyz",
		RoomName:         "deal team",
		RoomType:         model.RoomTypeChannel,
		Accounts:         []string{"bob", "ian"},
		SiteID:           "site-A",
		RequesterAccount: "alice",
		JoinedAt:         time.Now().UTC().UnixMilli(),
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	evt, err := json.Marshal(model.OutboxEvent{
		Type:       "member_added",
		SiteID:     "site-A",
		DestSiteID: "site-B",
		Payload:    payload,
	})
	require.NoError(t, err)
	require.NoError(t, h.HandleEvent(ctx, evt))

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": "r_xyz"})
	require.NoError(t, err)
	assert.Equal(t, int64(2), subCount)

	var bobSub model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx,
		bson.M{"roomId": "r_xyz", "u.account": "bob"}).Decode(&bobSub))
	assert.Equal(t, "deal team", bobSub.Name)
	assert.Equal(t, "site-A", bobSub.SiteID)
	assert.Equal(t, model.RoomTypeChannel, bobSub.RoomType)
}

func TestHandleMemberAdded_DM_PersistsRemoteCounterpartSub(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob",
		SiteID: "site-B", EngName: "Bob", ChineseName: "鲍勃"})

	h := newIntegrationHandler(t, db)

	const roomID = "u_aliceu_bob"
	payload, err := json.Marshal(model.MemberAddEvent{
		Type:             "member_added",
		RoomID:           roomID,
		RoomName:         "",
		RoomType:         model.RoomTypeDM,
		Accounts:         []string{"bob"},
		SiteID:           "site-A",
		RequesterAccount: "alice",
		JoinedAt:         time.Now().UTC().UnixMilli(),
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	evt, err := json.Marshal(model.OutboxEvent{
		Type:       "member_added",
		SiteID:     "site-A",
		DestSiteID: "site-B",
		Payload:    payload,
	})
	require.NoError(t, err)
	require.NoError(t, h.HandleEvent(ctx, evt))

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(1), subCount)

	var bobSub model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx,
		bson.M{"roomId": roomID, "u.account": "bob"}).Decode(&bobSub))
	assert.Equal(t, "bob", bobSub.User.Account)
	assert.Equal(t, "alice", bobSub.Name, "DM Subscription.Name = counterpart account (the requester)")
	assert.Equal(t, "site-A", bobSub.SiteID, "sub SiteID is room's home, not this site")
	assert.Equal(t, model.RoomTypeDM, bobSub.RoomType)
	assert.Nil(t, bobSub.Roles, "DMs have no roles")
	assert.False(t, bobSub.IsSubscribed, "DM does not set IsSubscribed=true")
}

// setupNATS connects to the process-shared NATS (JetStream enabled in
// testutil) and returns a JetStream client tied to the test's lifetime.
func setupNATS(t *testing.T) (context.Context, jetstream.JetStream) {
	t.Helper()
	ctx := context.Background()

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { nc.Close() })

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	return ctx, js
}

// TestInboxWorker_FilterScoping_Integration verifies the consumer filters
// out the local lane: a local-lane publish stays unreachable to inbox-worker.
func TestInboxWorker_FilterScoping_Integration(t *testing.T) {
	const siteID = "site-filter"

	ctx, js := setupNATS(t)

	inboxCfg := stream.Inbox(siteID)
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     inboxCfg.Name,
		Subjects: inboxCfg.Subjects,
	})
	require.NoError(t, err)

	cons, err := js.CreateOrUpdateConsumer(ctx, inboxCfg.Name, jetstream.ConsumerConfig{
		Durable:        "inbox-worker",
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{subject.InboxAggregateAll(siteID)},
	})
	require.NoError(t, err)

	_, err = js.Publish(ctx, subject.InboxMemberAdded(siteID), []byte(`{"type":"member_added"}`))
	require.NoError(t, err)
	_, err = js.Publish(ctx, subject.InboxMemberAddedAggregate(siteID), []byte(`{"type":"member_added"}`))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		info, err := js.Stream(ctx, inboxCfg.Name)
		if err != nil {
			return false
		}
		return info.CachedInfo().State.Msgs >= 2
	}, 2*time.Second, 50*time.Millisecond, "stream must accept both publishes")

	info, err := cons.Info(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, info.NumPending,
		"FilterSubjects must scope inbox-worker to the aggregate.> lane only")
}

func TestInboxStore_ApplyThreadRead_HappyPath(t *testing.T) {
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
	ctx := context.Background()

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
		User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
		JoinedAt: time.Now().UTC().Add(-time.Hour), ThreadUnread: []string{"p1"}, Alert: true,
	}
	_, err := db.Collection("subscriptions").InsertOne(ctx, &seedSub)
	require.NoError(t, err)

	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	seedTS := model.ThreadSubscription{
		ID: "tsub-1", ParentMessageID: "p1", RoomID: "r1",
		ThreadRoomID: "tr1", UserAccount: "alice", SiteID: "site-b",
		HasMention: true, CreatedAt: created, UpdatedAt: created,
	}
	_, err = db.Collection("thread_subscriptions").InsertOne(ctx, &seedTS)
	require.NoError(t, err)

	require.NoError(t, store.ApplyThreadRead(ctx, "r1", "tr1", "alice", nil, false, time.Now().UTC()))

	var raw bson.M
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sub-1"}).Decode(&raw))
	_, present := raw["threadUnread"]
	assert.False(t, present, "threadUnread must be $unset, not stored as empty array")
	assert.Equal(t, false, raw["alert"])
}

// Stale event: thread-sub guard rejects, same gate skips the Subscription.
func TestInboxStore_ApplyThreadRead_OutOfOrderThreadSub(t *testing.T) {
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
	ctx := context.Background()

	t2 := time.Now().UTC().Truncate(time.Millisecond)
	t1 := t2.Add(-time.Hour)

	seedSub := model.Subscription{
		ID: "sub-1", RoomID: "r1", SiteID: "site-b",
		User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
		JoinedAt: t1.Add(-time.Hour), ThreadUnread: []string{"p1", "p2"}, Alert: true,
	}
	_, err := db.Collection("subscriptions").InsertOne(ctx, &seedSub)
	require.NoError(t, err)

	seedTS := model.ThreadSubscription{
		ID: "tsub-1", ParentMessageID: "p1", RoomID: "r1",
		ThreadRoomID: "tr1", UserAccount: "alice", SiteID: "site-b",
		LastSeenAt: &t2, UpdatedAt: t2, CreatedAt: t1,
	}
	_, err = db.Collection("thread_subscriptions").InsertOne(ctx, &seedTS)
	require.NoError(t, err)

	require.NoError(t, store.ApplyThreadRead(ctx, "r1", "tr1", "alice", []string{"p2"}, false, t1))

	var gotSub model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sub-1"}).Decode(&gotSub))
	assert.Equal(t, []string{"p1", "p2"}, gotSub.ThreadUnread)
	assert.True(t, gotSub.Alert)

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

// newSubFixture returns a Subscription fixture with Member role and the given name.
func newSubFixture(id, userID, account, roomID, name string) model.Subscription {
	s := newSubFixtureWithRoles(id, userID, account, roomID, []model.Role{model.RoleMember})
	s.Name = name
	return s
}

// newSubFixtureWithRoles returns a Subscription fixture with the given roles.
func newSubFixtureWithRoles(id, userID, account, roomID string, roles []model.Role) model.Subscription {
	return model.Subscription{
		ID:       id,
		User:     model.SubscriptionUser{ID: userID, Account: account},
		RoomID:   roomID,
		SiteID:   "site-a",
		Name:     "n",
		Roles:    roles,
		RoomType: model.RoomTypeChannel,
		JoinedAt: time.Now().UTC(),
	}
}

func TestMongoInboxStore_UpdateSubscriptionNamesForRoom(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "inbox-worker-rename")
	store := &mongoInboxStore{subCol: db.Collection("subscriptions")}

	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		newSubFixture("s1", "u1", "alice", "r1", "old"),
		newSubFixture("s2", "u2", "bob", "r1", "old"),
		newSubFixture("s3", "u3", "carol", "other", "untouched"),
	})
	require.NoError(t, err)

	require.NoError(t, store.UpdateSubscriptionNamesForRoom(ctx, "r1", "new", time.Now().UTC()))

	// r1 subs must be updated.
	cur, err := db.Collection("subscriptions").Find(ctx, bson.M{"roomId": "r1"})
	require.NoError(t, err)
	var r1Subs []model.Subscription
	require.NoError(t, cur.All(ctx, &r1Subs))
	for _, sub := range r1Subs {
		assert.Equal(t, "new", sub.Name, "sub %s should have new name", sub.ID)
	}

	// other-room sub must be untouched.
	var otherSub model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"roomId": "other"}).Decode(&otherSub))
	assert.Equal(t, "untouched", otherSub.Name)
}

func TestMongoInboxStore_ApplySubscriptionVisibility(t *testing.T) {
	seed := func(t *testing.T, db *mongo.Database) {
		t.Helper()
		_, err := db.Collection("subscriptions").InsertMany(context.Background(), []any{
			newSubFixtureWithRoles("s1", "u1", "alice", "r1", []model.Role{model.RoleOwner}),
			newSubFixtureWithRoles("s2", "u2", "bob", "r1", []model.Role{model.RoleMember}),
			newSubFixtureWithRoles("s3", "u3", "carol", "r1", []model.Role{model.RoleMember}),
		})
		require.NoError(t, err)
	}
	loadSubs := func(t *testing.T, db *mongo.Database) []model.Subscription {
		t.Helper()
		cur, err := db.Collection("subscriptions").Find(context.Background(), bson.M{"roomId": "r1"})
		require.NoError(t, err)
		var subs []model.Subscription
		require.NoError(t, cur.All(context.Background(), &subs))
		return subs
	}
	rolesByAccount := func(subs []model.Subscription) map[string][]model.Role {
		out := map[string][]model.Role{}
		for _, sub := range subs {
			out[sub.User.Account] = sub.Roles
		}
		return out
	}

	t.Run("restrict with owner rewrites roles and sets flags", func(t *testing.T) {
		db := testutil.MongoDB(t, "inbox-worker-visibility-restrict")
		store := &mongoInboxStore{subCol: db.Collection("subscriptions")}
		seed(t, db)

		require.NoError(t, store.ApplySubscriptionVisibility(context.Background(), "r1", true, false, "bob", time.Now().UTC()))

		subs := loadSubs(t, db)
		roles := rolesByAccount(subs)
		assert.Equal(t, []model.Role{model.RoleOwner}, roles["bob"], "bob should be owner")
		assert.Equal(t, []model.Role{model.RoleMember}, roles["alice"], "alice should be member")
		assert.Equal(t, []model.Role{model.RoleMember}, roles["carol"], "carol should be member")
		for _, sub := range subs {
			assert.True(t, sub.Restricted, "sub %s Restricted should be true", sub.ID)
			assert.False(t, sub.ExternalAccess, "sub %s ExternalAccess should be false", sub.ID)
		}
	})

	t.Run("flags only when ownerAccount empty (roles untouched)", func(t *testing.T) {
		db := testutil.MongoDB(t, "inbox-worker-visibility-flags")
		store := &mongoInboxStore{subCol: db.Collection("subscriptions")}
		seed(t, db)

		require.NoError(t, store.ApplySubscriptionVisibility(context.Background(), "r1", true, true, "", time.Now().UTC()))

		subs := loadSubs(t, db)
		roles := rolesByAccount(subs)
		// Roles untouched — alice was seeded as owner.
		assert.Equal(t, []model.Role{model.RoleOwner}, roles["alice"], "alice roles must not change")
		assert.Equal(t, []model.Role{model.RoleMember}, roles["bob"], "bob roles must not change")
		for _, sub := range subs {
			assert.True(t, sub.Restricted, "sub %s Restricted should be true", sub.ID)
			assert.True(t, sub.ExternalAccess, "sub %s ExternalAccess should be true", sub.ID)
		}
	})

	t.Run("unrestrict clears flags and ignores ownerAccount", func(t *testing.T) {
		db := testutil.MongoDB(t, "inbox-worker-visibility-unrestrict")
		store := &mongoInboxStore{subCol: db.Collection("subscriptions")}
		seed(t, db)

		require.NoError(t, store.ApplySubscriptionVisibility(context.Background(), "r1", false, false, "bob", time.Now().UTC()))

		subs := loadSubs(t, db)
		roles := rolesByAccount(subs)
		// Roles untouched — alice was seeded as owner.
		assert.Equal(t, []model.Role{model.RoleOwner}, roles["alice"], "alice roles must not change")
		for _, sub := range subs {
			assert.False(t, sub.Restricted, "sub %s Restricted should be false", sub.ID)
			assert.False(t, sub.ExternalAccess, "sub %s ExternalAccess should be false", sub.ID)
		}
	})
}

func TestIntegration_HandleRoomRenamed(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "inbox-worker-rename-handler")
	store := &mongoInboxStore{
		subCol:  db.Collection("subscriptions"),
		roomCol: db.Collection("rooms"),
		userCol: db.Collection("users"),
	}
	h := NewHandler(store)

	// Seed two subscription mirrors for room r1 with old name.
	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		newSubFixture("s1", "u1", "alice", "r1", "old-name"),
		newSubFixture("s2", "u2", "bob", "r1", "old-name"),
	})
	require.NoError(t, err)

	// Construct and marshal the outbox event.
	renamePayload := model.RoomRenamedOutboxPayload{
		RoomID:    "r1",
		NewName:   "renamed",
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	payloadData, err := json.Marshal(renamePayload)
	require.NoError(t, err)
	evt := model.OutboxEvent{
		Type:       string(model.OutboxRoomRenamed),
		SiteID:     "site-a",
		DestSiteID: "site-b",
		Payload:    payloadData,
		Timestamp:  time.Now().UTC().UnixMilli(),
	}
	evtData, err := json.Marshal(evt)
	require.NoError(t, err)

	require.NoError(t, h.HandleEvent(ctx, evtData))

	// All subscriptions for r1 must have Name updated to "renamed".
	cur, err := db.Collection("subscriptions").Find(ctx, bson.M{"roomId": "r1"})
	require.NoError(t, err)
	var subs []model.Subscription
	require.NoError(t, cur.All(ctx, &subs))
	require.Len(t, subs, 2)
	for _, sub := range subs {
		assert.Equal(t, "renamed", sub.Name, "sub %s Name should be updated", sub.ID)
	}
}

func TestIntegration_HandleRoomVisibilityChanged(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "inbox-worker-visibility-handler")
	store := &mongoInboxStore{
		subCol:  db.Collection("subscriptions"),
		roomCol: db.Collection("rooms"),
		userCol: db.Collection("users"),
	}
	h := NewHandler(store)

	// Seed: alice=owner, bob=member, carol=member.
	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		newSubFixtureWithRoles("s1", "u1", "alice", "r1", []model.Role{model.RoleOwner}),
		newSubFixtureWithRoles("s2", "u2", "bob", "r1", []model.Role{model.RoleMember}),
		newSubFixtureWithRoles("s3", "u3", "carol", "r1", []model.Role{model.RoleMember}),
	})
	require.NoError(t, err)

	// Construct and marshal the outbox event: bob becomes new owner.
	visPayload := model.RoomRestrictedOutboxPayload{
		RoomID:         "r1",
		Restricted:     true,
		ExternalAccess: false,
		OwnerAccount:   "bob",
		Timestamp:      time.Now().UTC().UnixMilli(),
	}
	payloadData, err := json.Marshal(visPayload)
	require.NoError(t, err)
	evt := model.OutboxEvent{
		Type:       string(model.OutboxRoomRestricted),
		SiteID:     "site-a",
		DestSiteID: "site-b",
		Payload:    payloadData,
		Timestamp:  time.Now().UTC().UnixMilli(),
	}
	evtData, err := json.Marshal(evt)
	require.NoError(t, err)

	require.NoError(t, h.HandleEvent(ctx, evtData))

	// Load all subs for r1 and build a role map.
	cur, err := db.Collection("subscriptions").Find(ctx, bson.M{"roomId": "r1"})
	require.NoError(t, err)
	var subs []model.Subscription
	require.NoError(t, cur.All(ctx, &subs))
	require.Len(t, subs, 3)

	rolesByAccount := map[string][]model.Role{}
	for _, sub := range subs {
		rolesByAccount[sub.User.Account] = sub.Roles
		// All subs must have Restricted=true and ExternalAccess=false.
		assert.True(t, sub.Restricted, "sub %s Restricted should be true", sub.ID)
		assert.False(t, sub.ExternalAccess, "sub %s ExternalAccess should be false", sub.ID)
	}

	// bob promoted to owner, alice demoted to member, carol stays member.
	assert.Equal(t, []model.Role{model.RoleOwner}, rolesByAccount["bob"], "bob should be owner")
	assert.Equal(t, []model.Role{model.RoleMember}, rolesByAccount["alice"], "alice should be member")
	assert.Equal(t, []model.Role{model.RoleMember}, rolesByAccount["carol"], "carol should be member")
}

// Missing thread-sub: gate doesn't match, Subscription is skipped too.
func TestInboxStore_ApplyThreadRead_MissingThreadSubscription_NoError(t *testing.T) {
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
	ctx := context.Background()

	seedSub := model.Subscription{
		ID: "sub-1", RoomID: "r1", SiteID: "site-b",
		User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
		JoinedAt: time.Now().UTC().Add(-time.Hour), ThreadUnread: []string{"p1"}, Alert: true,
	}
	_, err := db.Collection("subscriptions").InsertOne(ctx, &seedSub)
	require.NoError(t, err)

	require.NoError(t, store.ApplyThreadRead(ctx, "r1", "tr-missing", "alice", nil, false, time.Now().UTC()))

	var gotSub model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sub-1"}).Decode(&gotSub))
	assert.Equal(t, []string{"p1"}, gotSub.ThreadUnread)
	assert.True(t, gotSub.Alert)
}

func newGuardStore(db *mongo.Database) *mongoInboxStore {
	return &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		roomCol:      db.Collection("rooms"),
		userCol:      db.Collection("users"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
}

func TestInbox_UpdateSubscriptionRoles_OutOfOrderSkipped(t *testing.T) {
	ctx := context.Background()
	store := newGuardStore(setupMongo(t))

	// Seed a sub whose roles were last set by a newer event (ts=200).
	_, err := store.subCol.InsertOne(ctx, bson.M{
		"_id": "s1", "roomId": "r1", "u": bson.M{"account": "alice"},
		"roles": []model.Role{model.RoleOwner}, "rolesUpdatedAt": time.UnixMilli(200).UTC(),
	})
	require.NoError(t, err)

	// An older role_updated (ts=100) must be a silent no-op.
	require.NoError(t, store.UpdateSubscriptionRoles(ctx, "alice", "r1", []model.Role{model.RoleMember}, time.UnixMilli(100).UTC()))

	var got model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	assert.Equal(t, []model.Role{model.RoleOwner}, got.Roles) // unchanged
}

func TestInbox_UpdateSubscriptionRoles_NewerApplies(t *testing.T) {
	ctx := context.Background()
	store := newGuardStore(setupMongo(t))

	_, err := store.subCol.InsertOne(ctx, bson.M{
		"_id": "s1", "roomId": "r1", "u": bson.M{"account": "alice"},
		"roles": []model.Role{model.RoleMember}, "rolesUpdatedAt": time.UnixMilli(100).UTC(),
	})
	require.NoError(t, err)

	require.NoError(t, store.UpdateSubscriptionRoles(ctx, "alice", "r1", []model.Role{model.RoleOwner}, time.UnixMilli(200).UTC()))

	var got model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	assert.Equal(t, []model.Role{model.RoleOwner}, got.Roles)
}

func TestInbox_UpdateSubscriptionRoles_MissingSubscriptionErrors(t *testing.T) {
	ctx := context.Background()
	store := newGuardStore(setupMongo(t))

	// No subscription seeded — a genuinely missing sub must still error so the
	// event is redelivered until member_added lands (federation race).
	err := store.UpdateSubscriptionRoles(ctx, "ghost", "r1", []model.Role{model.RoleMember}, time.UnixMilli(100).UTC())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "subscription not found")
}

func TestInbox_UpdateSubscriptionMute_OutOfOrderSkipped(t *testing.T) {
	ctx := context.Background()
	store := newGuardStore(setupMongo(t))

	// Sub last muted=false by a newer event (ts=200).
	_, err := store.subCol.InsertOne(ctx, bson.M{
		"_id": "s1", "roomId": "r1", "u": bson.M{"account": "alice"},
		"muted": false, "muteUpdatedAt": time.UnixMilli(200).UTC(),
	})
	require.NoError(t, err)

	// An older toggle (ts=100) must not regress mute state.
	require.NoError(t, store.UpdateSubscriptionMute(ctx, "r1", "alice", true, time.UnixMilli(100).UTC()))

	var got model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	assert.False(t, got.Muted) // unchanged
}

func TestInbox_UpdateSubscriptionMute_NewerApplies(t *testing.T) {
	ctx := context.Background()
	store := newGuardStore(setupMongo(t))

	_, err := store.subCol.InsertOne(ctx, bson.M{
		"_id": "s1", "roomId": "r1", "u": bson.M{"account": "alice"},
		"muted": false, "muteUpdatedAt": time.UnixMilli(100).UTC(),
	})
	require.NoError(t, err)

	require.NoError(t, store.UpdateSubscriptionMute(ctx, "r1", "alice", true, time.UnixMilli(200).UTC()))

	var got model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	assert.True(t, got.Muted)
}

func TestInbox_UpdateSubscriptionFavorite_OutOfOrderSkipped(t *testing.T) {
	ctx := context.Background()
	store := newGuardStore(setupMongo(t))

	// Sub last favorited=false by a newer event (ts=200).
	_, err := store.subCol.InsertOne(ctx, bson.M{
		"_id": "s1", "roomId": "r1", "u": bson.M{"account": "alice"},
		"favorite": false, "favoriteUpdatedAt": time.UnixMilli(200).UTC(),
	})
	require.NoError(t, err)

	// An older toggle (ts=100) must not regress favorite state.
	require.NoError(t, store.UpdateSubscriptionFavorite(ctx, "r1", "alice", true, time.UnixMilli(100).UTC()))

	var got model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	assert.False(t, got.Favorite) // unchanged
}

func TestInbox_UpdateSubscriptionFavorite_NewerApplies(t *testing.T) {
	ctx := context.Background()
	store := newGuardStore(setupMongo(t))

	_, err := store.subCol.InsertOne(ctx, bson.M{
		"_id": "s1", "roomId": "r1", "u": bson.M{"account": "alice"},
		"favorite": false, "favoriteUpdatedAt": time.UnixMilli(100).UTC(),
	})
	require.NoError(t, err)

	require.NoError(t, store.UpdateSubscriptionFavorite(ctx, "r1", "alice", true, time.UnixMilli(200).UTC()))

	var got model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	assert.True(t, got.Favorite)
}

func TestInbox_UpdateSubscriptionNamesForRoom_OutOfOrderSkipped(t *testing.T) {
	ctx := context.Background()
	store := newGuardStore(setupMongo(t))

	// Sub last renamed by a newer event (ts=200).
	_, err := store.subCol.InsertOne(ctx, bson.M{
		"_id": "s1", "roomId": "r1", "u": bson.M{"account": "alice"},
		"name": "newer", "nameUpdatedAt": time.UnixMilli(200).UTC(),
	})
	require.NoError(t, err)

	// An older rename (ts=100) must not regress the name.
	require.NoError(t, store.UpdateSubscriptionNamesForRoom(ctx, "r1", "older", time.UnixMilli(100).UTC()))

	var got model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	assert.Equal(t, "newer", got.Name) // unchanged
}

func TestInbox_UpdateSubscriptionNamesForRoom_NewerApplies(t *testing.T) {
	ctx := context.Background()
	store := newGuardStore(setupMongo(t))

	_, err := store.subCol.InsertOne(ctx, bson.M{
		"_id": "s1", "roomId": "r1", "u": bson.M{"account": "alice"},
		"name": "older", "nameUpdatedAt": time.UnixMilli(100).UTC(),
	})
	require.NoError(t, err)

	require.NoError(t, store.UpdateSubscriptionNamesForRoom(ctx, "r1", "newer", time.UnixMilli(200).UTC()))

	var got model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	assert.Equal(t, "newer", got.Name)
}

func TestInbox_ApplySubscriptionVisibility_OutOfOrderSkipped(t *testing.T) {
	ctx := context.Background()
	store := newGuardStore(setupMongo(t))

	// Sub last set restricted=true by a newer event (ts=200).
	_, err := store.subCol.InsertOne(ctx, bson.M{
		"_id": "s1", "roomId": "r1", "u": bson.M{"account": "alice"},
		"restricted": true, "externalAccess": false, "visibilityUpdatedAt": time.UnixMilli(200).UTC(),
	})
	require.NoError(t, err)

	// An older unrestrict (ts=100) must not regress visibility state.
	require.NoError(t, store.ApplySubscriptionVisibility(ctx, "r1", false, false, "", time.UnixMilli(100).UTC()))

	var got model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	assert.True(t, got.Restricted) // unchanged
}

func TestInbox_ApplySubscriptionVisibility_NewerApplies(t *testing.T) {
	ctx := context.Background()
	store := newGuardStore(setupMongo(t))

	// Two subs at an older visibilityUpdatedAt; a newer restrict rewrites roles.
	_, err := store.subCol.InsertMany(ctx, []any{
		bson.M{"_id": "s1", "roomId": "r1", "u": bson.M{"account": "alice"},
			"roles": []model.Role{model.RoleOwner}, "restricted": false, "visibilityUpdatedAt": time.UnixMilli(100).UTC()},
		bson.M{"_id": "s2", "roomId": "r1", "u": bson.M{"account": "bob"},
			"roles": []model.Role{model.RoleMember}, "restricted": false, "visibilityUpdatedAt": time.UnixMilli(100).UTC()},
	})
	require.NoError(t, err)

	require.NoError(t, store.ApplySubscriptionVisibility(ctx, "r1", true, false, "bob", time.UnixMilli(200).UTC()))

	var alice, bob model.Subscription
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s1"}).Decode(&alice))
	require.NoError(t, store.subCol.FindOne(ctx, bson.M{"_id": "s2"}).Decode(&bob))
	assert.True(t, alice.Restricted)
	assert.True(t, bob.Restricted)
	assert.Equal(t, []model.Role{model.RoleOwner}, bob.Roles)
	assert.Equal(t, []model.Role{model.RoleMember}, alice.Roles)
}

func TestInbox_UpsertRoom_OlderUpdatedAtSkipped(t *testing.T) {
	ctx := context.Background()
	store := newGuardStore(setupMongo(t))

	t2 := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, store.UpsertRoom(ctx, &model.Room{ID: "r1", Name: "newer", UpdatedAt: t2}))

	// An older room_sync (UpdatedAt=t1<t2) must be a silent no-op, not regress
	// the name back to the stale value.
	t1 := t2.Add(-time.Minute)
	require.NoError(t, store.UpsertRoom(ctx, &model.Room{ID: "r1", Name: "older", UpdatedAt: t1}))

	var got model.Room
	require.NoError(t, store.roomCol.FindOne(ctx, bson.M{"_id": "r1"}).Decode(&got))
	assert.Equal(t, "newer", got.Name) // unchanged
}

func TestInbox_UpsertRoom_NewerUpdatedAtApplies(t *testing.T) {
	ctx := context.Background()
	store := newGuardStore(setupMongo(t))

	t1 := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	require.NoError(t, store.UpsertRoom(ctx, &model.Room{ID: "r1", Name: "older", UpdatedAt: t1}))

	t2 := t1.Add(time.Minute)
	require.NoError(t, store.UpsertRoom(ctx, &model.Room{ID: "r1", Name: "newer", UpdatedAt: t2}))

	var got model.Room
	require.NoError(t, store.roomCol.FindOne(ctx, bson.M{"_id": "r1"}).Decode(&got))
	assert.Equal(t, "newer", got.Name)
}
