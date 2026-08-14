//go:build integration

package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/userstore"
)

func setupMongo(t *testing.T) *mongo.Database {
	return testutil.MongoDB(t, "broadcast_worker_test")
}

type recordingPublisher struct {
	mu      sync.Mutex
	records []publishRecord
}

func (p *recordingPublisher) Publish(_ context.Context, subj string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, publishRecord{subject: subj, data: data})
	return nil
}

func (p *recordingPublisher) getRecords() []publishRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]publishRecord, len(p.records))
	copy(cp, p.records)
	return cp
}

func seedUsers(t *testing.T, db *mongo.Database) {
	t.Helper()
	_, err := db.Collection("users").InsertMany(context.Background(), []interface{}{
		model.User{ID: "u-alice", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲", EmployeeID: "E001"},
		model.User{ID: "u-bob", Account: "bob", SiteID: "site-a", EngName: "Bob Chen", ChineseName: "鮑勃", EmployeeID: "E002"},
	})
	require.NoError(t, err)
}

type fakeRoomKeyProvider struct {
	pair *roomkeystore.VersionedKeyPair
}

func (f *fakeRoomKeyProvider) Get(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
	return f.pair, nil
}

func TestBroadcastWorker_ChannelRoom_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r1", Name: "general", Type: model.RoomTypeChannel, UserCount: 2, SiteID: "site-a",
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1"},
		model.Subscription{ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1"},
	})
	require.NoError(t, err)
	seedUsers(t, db)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0, 0, nil)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	key := testRoomKey(t)
	keyStore := &fakeRoomKeyProvider{pair: key}
	handler := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice", Content: "hello", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, handler.HandleMessage(ctx, data))

	records := pub.getRecords()
	require.Len(t, records, 1)
	assert.Equal(t, subject.RoomEvent("r1", true), records[0].subject)

	roomEvt, msg := decryptClientMessage(t, records[0].data, key)
	assert.Equal(t, "site-a", roomEvt.SiteID)
	require.NotNil(t, msg)
	require.NotNil(t, msg.Sender)
	assert.Equal(t, "u1", msg.Sender.UserID)
}

func TestBroadcastWorker_ChannelRoom_IndividualMention_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r3", Name: "dev", Type: model.RoomTypeChannel, UserCount: 2, SiteID: "site-a",
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "s5", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r3"},
		model.Subscription{ID: "s6", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r3"},
	})
	require.NoError(t, err)
	seedUsers(t, db)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0, 0, nil)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	key := testRoomKey(t)
	keyStore := &fakeRoomKeyProvider{pair: key}
	handler := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "m3", RoomID: "r3", UserID: "u1", UserAccount: "alice", Content: "hey @bob", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, handler.HandleMessage(ctx, data))

	records := pub.getRecords()
	roomEvt, _ := decryptClientMessage(t, records[0].data, key)
	require.Len(t, roomEvt.Mentions, 1)
	assert.Equal(t, "bob", roomEvt.Mentions[0].Account)
	assert.Equal(t, "鮑勃", roomEvt.Mentions[0].ChineseName)
	assert.Equal(t, "Bob Chen", roomEvt.Mentions[0].EngName)
	assert.Equal(t, "u-bob", roomEvt.Mentions[0].UserID)
}

func TestBroadcastWorker_DMRoom_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "dm-1", Name: "", Type: model.RoomTypeDM, UserCount: 2, SiteID: "site-a",
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "s7", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "dm-1"},
		model.Subscription{ID: "s8", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "dm-1"},
	})
	require.NoError(t, err)
	seedUsers(t, db)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0, 0, nil)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	keyStore := &fakeRoomKeyProvider{pair: nil}
	handler := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "m4", RoomID: "dm-1", UserID: "u1", UserAccount: "alice", Content: "hey", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, handler.HandleMessage(ctx, data))

	records := pub.getRecords()
	require.Len(t, records, 2)
	var subjects []string
	for _, rec := range records {
		subjects = append(subjects, rec.subject)
	}
	assert.ElementsMatch(t, []string{
		subject.UserRoomEvent("alice"),
		subject.UserRoomEvent("bob"),
	}, subjects)

	for _, rec := range records {
		var roomEvt model.RoomEvent
		require.NoError(t, json.Unmarshal(rec.data, &roomEvt))
		require.NotNil(t, roomEvt.Message)
		require.NotNil(t, roomEvt.Message.Sender)
		assert.Equal(t, "u1", roomEvt.Message.Sender.UserID)
		assert.Equal(t, "alice", roomEvt.Message.Sender.Account)
		assert.Equal(t, "愛麗絲", roomEvt.Message.Sender.ChineseName)
	}
}

func TestBroadcastWorker_ChannelRoom_EncryptionDisabled_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "rNoEnc", Name: "plain", Type: model.RoomTypeChannel, UserCount: 2, SiteID: "site-a",
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "sN1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "rNoEnc"},
		model.Subscription{ID: "sN2", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "rNoEnc"},
	})
	require.NoError(t, err)
	seedUsers(t, db)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0, 0, nil)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}

	// nil keyStore — encryption is disabled, handler must not consult it
	handler := NewHandler(store, us, pub, nil, defaultParentFetcher, false, subject.RouteGlobal)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "mNoEnc", RoomID: "rNoEnc", UserID: "u1", UserAccount: "alice", Content: "plaintext please", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, handler.HandleMessage(ctx, data))

	records := pub.getRecords()
	require.Len(t, records, 1)
	assert.Equal(t, subject.RoomEvent("rNoEnc", true), records[0].subject)

	var roomEvt model.RoomEvent
	require.NoError(t, json.Unmarshal(records[0].data, &roomEvt))
	assert.Equal(t, "site-a", roomEvt.SiteID)
	require.NotNil(t, roomEvt.Message, "plaintext channel event must carry Message")
	assert.Empty(t, roomEvt.EncryptedMessage, "plaintext channel event must NOT carry EncryptedMessage")
	assert.Equal(t, "mNoEnc", roomEvt.Message.ID)
	assert.Equal(t, "plaintext please", roomEvt.Message.Content)
	require.NotNil(t, roomEvt.Message.Sender)
	assert.Equal(t, "u1", roomEvt.Message.Sender.UserID)
	assert.Equal(t, "alice", roomEvt.Message.Sender.Account)
}

func TestBroadcastWorker_GetThreadFollowers_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0, 0, nil)

	// Seed a thread room document with replyAccounts (siteID isolation is handled
	// at the deployment level — each site has its own MongoDB instance).
	_, err := db.Collection("thread_rooms").InsertMany(ctx, []interface{}{
		bson.M{
			"_id":             "tr-1",
			"parentMessageId": "parent-1",
			"replyAccounts":   []string{"bob", "carol", ""},
		},
		bson.M{
			"_id":             "tr-3",
			"parentMessageId": "parent-2",
			"replyAccounts":   []string{"dave"},
		},
	})
	require.NoError(t, err)

	t.Run("returns followers with empty strings filtered", func(t *testing.T) {
		followers, err := store.GetThreadFollowers(ctx, "parent-1")
		require.NoError(t, err)
		// Empty string is filtered out.
		assert.Equal(t, map[string]struct{}{"bob": {}, "carol": {}}, followers)
	})

	t.Run("different parentMessageId returns correct subset", func(t *testing.T) {
		followers, err := store.GetThreadFollowers(ctx, "parent-2")
		require.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"dave": {}}, followers)
	})

	t.Run("no document returns empty map", func(t *testing.T) {
		followers, err := store.GetThreadFollowers(ctx, "nonexistent-parent")
		require.NoError(t, err)
		assert.Empty(t, followers)
	})
}

func TestBroadcastWorker_EnsureIndexes_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0, 0, nil)

	// EnsureIndexes should be idempotent — call it twice without error.
	require.NoError(t, store.EnsureIndexes(ctx))
	require.NoError(t, store.EnsureIndexes(ctx))

	// Verify the compound index was created by listing indexes.
	// MongoDB driver v2 decodes nested documents as bson.D (not bson.M), so we
	// decode the index list into []bson.D and iterate element-by-element.
	cursor, err := db.Collection("thread_rooms").Indexes().List(ctx)
	require.NoError(t, err)
	var idxes []bson.D
	require.NoError(t, cursor.All(ctx, &idxes))

	var found bool
	for _, idx := range idxes {
		var gotKeys bson.D
		for _, elem := range idx {
			if elem.Key == "key" {
				if kd, ok := elem.Value.(bson.D); ok {
					gotKeys = kd
				}
			}
		}
		var hasParent, hasSite bool
		for _, kv := range gotKeys {
			if kv.Key == "parentMessageId" {
				hasParent = true
			}
			if kv.Key == "siteId" {
				hasSite = true
			}
		}
		if hasParent && hasSite {
			found = true
			break
		}
	}
	assert.True(t, found, "compound index on (parentMessageId, siteId) must exist")
}

func TestBroadcastWorker_GetHistorySharedSince_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0, 0, nil)

	shared := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "hss1", User: model.SubscriptionUser{ID: "u-al", Account: "alice"}, RoomID: "r-hss", HistorySharedSince: &shared},
		model.Subscription{ID: "hss2", User: model.SubscriptionUser{ID: "u-bo", Account: "bob"}, RoomID: "r-hss"},
	})
	require.NoError(t, err)

	got, err := store.GetHistorySharedSince(ctx, "r-hss", []string{"alice", "bob", "carol"})
	require.NoError(t, err)
	require.NotNil(t, got["alice"])
	assert.Equal(t, shared.UnixMilli(), got["alice"].UTC().UnixMilli())
	bobWindow, bobPresent := got["bob"]
	require.True(t, bobPresent, "member with a nil window must still be present in the map")
	assert.Nil(t, bobWindow, "member without window decodes to nil")
	_, present := got["carol"]
	assert.False(t, present, "non-member is absent from the map")

	// Empty accounts short-circuits without a query.
	empty, err := store.GetHistorySharedSince(ctx, "r-hss", nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}
