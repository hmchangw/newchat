//go:build integration

package main

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
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
func newIntegrationHandler(t *testing.T, db *mongo.Database, _ string) *Handler {
	t.Helper()
	store := &mongoInboxStore{
		subCol:  db.Collection("subscriptions"),
		roomCol: db.Collection("rooms"),
		userCol: db.Collection("users"),
	}
	return NewHandler(store)
}

func TestHandleRoomCreatedPersistsRemoteSubs(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob",
		SiteID: "site-B", EngName: "Bob", ChineseName: "鲍勃"})
	mustInsertUser(t, db, &model.User{ID: "u_ian", Account: "ian",
		SiteID: "site-B", EngName: "Ian", ChineseName: "伊恩"})

	h := newIntegrationHandler(t, db, "site-B")
	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0193"
	ctx = natsutil.WithRequestID(ctx, reqID)

	payload, err := json.Marshal(model.RoomCreatedOutbox{
		RoomID: "r_xyz", RoomType: model.RoomTypeChannel,
		RoomName: "deal team", HomeSiteID: "site-A",
		Accounts:         []string{"bob", "ian"},
		RequesterAccount: "alice",
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, h.handleRoomCreated(ctx, &model.OutboxEvent{Payload: payload}))

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": "r_xyz"})
	require.NoError(t, err)
	assert.Equal(t, int64(2), subCount)

	roomCount, err := db.Collection("rooms").CountDocuments(ctx, bson.M{"_id": "r_xyz"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), roomCount, "inbox-worker must not create room mirror")

	var bobSub model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx,
		bson.M{"roomId": "r_xyz", "u.account": "bob"}).Decode(&bobSub))
	assert.Equal(t, "deal team", bobSub.Name)
	assert.Equal(t, "site-A", bobSub.SiteID)
	assert.Equal(t, model.RoomTypeChannel, bobSub.RoomType)
}

func TestHandleRoomCreatedDM_PersistsRemoteCounterpartSub(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob",
		SiteID: "site-B", EngName: "Bob", ChineseName: "鲍勃"})

	h := newIntegrationHandler(t, db, "site-B")
	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0193"
	ctx = natsutil.WithRequestID(ctx, reqID)

	const roomID = "u_aliceu_bob"
	payload, err := json.Marshal(model.RoomCreatedOutbox{
		RoomID:           roomID,
		RoomType:         model.RoomTypeDM,
		RoomName:         "",
		HomeSiteID:       "site-A",
		Accounts:         []string{"bob"},
		RequesterAccount: "alice",
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, h.handleRoomCreated(ctx, &model.OutboxEvent{Payload: payload}))

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(1), subCount)

	roomCount, err := db.Collection("rooms").CountDocuments(ctx, bson.M{"_id": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(0), roomCount, "inbox-worker must not create room mirror")

	var bobSub model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx,
		bson.M{"roomId": roomID, "u.account": "bob"}).Decode(&bobSub))
	assert.Equal(t, "bob", bobSub.User.Account)
	assert.Equal(t, "alice", bobSub.Name, "DM Subscription.Name = counterpart account")
	assert.Equal(t, "site-A", bobSub.SiteID, "sub SiteID is room's home, not this site")
	assert.Equal(t, model.RoomTypeDM, bobSub.RoomType)
	assert.Nil(t, bobSub.Roles, "DMs have no roles")
	assert.False(t, bobSub.IsSubscribed, "DM does not set IsSubscribed=true")
}
