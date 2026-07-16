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

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	key := testRoomKey(t)
	keyStore := &fakeRoomKeyProvider{pair: key}
	handler := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, true)

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
	assert.Equal(t, subject.RoomEvent("r1"), records[0].subject)

	roomEvt, msg := decryptClientMessage(t, records[0].data, key)
	assert.Equal(t, "site-a", roomEvt.SiteID)
	require.NotNil(t, msg)
	require.NotNil(t, msg.Sender)
	assert.Equal(t, "u1", msg.Sender.UserID)

	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r1"}).Decode(&room))
	assert.Equal(t, "m1", room.LastMsgID)
	require.NotNil(t, room.LastMsgAt)
	assert.WithinDuration(t, msgTime, *room.LastMsgAt, time.Millisecond)
}

func TestBroadcastWorker_ChannelRoom_MentionAll_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r2", Name: "announcements", Type: model.RoomTypeChannel, UserCount: 2, SiteID: "site-a",
	})
	require.NoError(t, err)
	seedUsers(t, db)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	key := testRoomKey(t)
	keyStore := &fakeRoomKeyProvider{pair: key}
	handler := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, true)

	msgTime := time.Now().UTC().Truncate(time.Millisecond)
	evt := model.MessageEvent{
		Event:  model.EventCreated,
		SiteID: "site-a",
		Message: model.Message{
			ID: "m2", RoomID: "r2", UserID: "u1", UserAccount: "alice", Content: "hello @All", CreatedAt: msgTime,
		},
	}
	data, _ := json.Marshal(evt)

	require.NoError(t, handler.HandleMessage(ctx, data))

	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r2"}).Decode(&room))
	require.NotNil(t, room.LastMentionAllAt)
	assert.WithinDuration(t, msgTime, *room.LastMentionAllAt, time.Millisecond)
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

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	key := testRoomKey(t)
	keyStore := &fakeRoomKeyProvider{pair: key}
	handler := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, true)

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

	var subBob model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"u.account": "bob", "roomId": "r3"}).Decode(&subBob))
	assert.True(t, subBob.HasMention)

	var subAlice model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"u.account": "alice", "roomId": "r3"}).Decode(&subAlice))
	assert.False(t, subAlice.HasMention)
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

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}
	keyStore := &fakeRoomKeyProvider{pair: nil}
	handler := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, true)

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

	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "dm-1"}).Decode(&room))
	assert.Equal(t, "m4", room.LastMsgID)
	require.NotNil(t, room.LastMsgAt)
	assert.WithinDuration(t, msgTime, *room.LastMsgAt, time.Millisecond)
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

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	us := userstore.NewMongoStore(db.Collection("users"))
	pub := &recordingPublisher{}

	// nil keyStore — encryption is disabled, handler must not consult it
	handler := NewHandler(store, us, pub, nil, defaultParentFetcher, defaultLastMsgFetcher, false)

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
	assert.Equal(t, subject.RoomEvent("rNoEnc"), records[0].subject)

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

	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "rNoEnc"}).Decode(&room))
	assert.Equal(t, "mNoEnc", room.LastMsgID)
	require.NotNil(t, room.LastMsgAt)
	assert.WithinDuration(t, msgTime, *room.LastMsgAt, time.Millisecond)
}

func TestBroadcastWorker_PersistsLastMessage_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r-last", Name: "general", Type: model.RoomTypeChannel, UserCount: 2, SiteID: "site-a",
	})
	require.NoError(t, err)
	seedUsers(t, db)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	cached, err := newCachedMetaStore(store, 10, time.Minute)
	require.NoError(t, err)

	pub := &recordingPublisher{}
	h := NewHandler(cached, userstore.NewMongoStore(db.Collection("users")), pub, &fakeRoomKeyProvider{}, defaultParentFetcher, defaultLastMsgFetcher, false)

	msgTime := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventCreated,
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID:          "msg-last",
			RoomID:      "r-last",
			UserID:      "u-alice",
			UserAccount: "alice",
			Content:     "hi",
			CreatedAt:   msgTime,
		},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	require.NoError(t, h.HandleMessage(ctx, data))

	// Verify the room doc now has lastMsgAt/lastMsgId persisted.
	var got struct {
		LastMsgAt time.Time `bson:"lastMsgAt"`
		LastMsgID string    `bson:"lastMsgId"`
	}
	err = db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-last"}).Decode(&got)
	require.NoError(t, err)
	assert.Equal(t, "msg-last", got.LastMsgID)
	assert.WithinDuration(t, msgTime, got.LastMsgAt, time.Millisecond)
}

func TestBroadcastWorker_BulkUpdateRoomLastMessage_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertMany(ctx, []interface{}{
		model.Room{ID: "r-bulk-a", Name: "a", Type: model.RoomTypeChannel, SiteID: "site-a"},
		model.Room{ID: "r-bulk-b", Name: "b", Type: model.RoomTypeChannel, SiteID: "site-a"},
	})
	require.NoError(t, err)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	t1 := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)
	updates := map[string]roomLastMsgUpdate{
		"r-bulk-a": {msgID: "msg-a", at: t1},
		"r-bulk-b": {msgID: "msg-b", at: t2, lastMentionAllAt: t2},
	}
	require.NoError(t, store.BulkUpdateRoomLastMessage(ctx, updates))

	var a, b struct {
		LastMsgAt        time.Time `bson:"lastMsgAt"`
		LastMsgID        string    `bson:"lastMsgId"`
		LastMentionAllAt time.Time `bson:"lastMentionAllAt"`
	}
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-bulk-a"}).Decode(&a))
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-bulk-b"}).Decode(&b))

	assert.Equal(t, "msg-a", a.LastMsgID)
	assert.WithinDuration(t, t1, a.LastMsgAt, time.Millisecond)
	assert.True(t, a.LastMentionAllAt.IsZero(), "no mention-all → field stays unset")

	assert.Equal(t, "msg-b", b.LastMsgID)
	assert.WithinDuration(t, t2, b.LastMsgAt, time.Millisecond)
	assert.WithinDuration(t, t2, b.LastMentionAllAt, time.Millisecond)
}

func TestBroadcastWorker_RewindRoomLastMessage_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	lastAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	mentionAt := lastAt.Add(-time.Hour)
	deletedAt := lastAt.Add(time.Minute)
	seedRoom := func(t *testing.T, id string) {
		t.Helper()
		_, err := db.Collection("rooms").InsertOne(ctx, bson.M{
			"_id": id, "type": model.RoomTypeChannel, "siteId": "site-a",
			"lastMsgId": "m-deleted", "lastMsgAt": lastAt, "lastMentionAllAt": mentionAt,
			"lastMsg": model.LastMessagePreview{MessageID: "m-deleted", SenderAccount: "alice", Msg: "bye", CreatedAt: lastAt},
		})
		require.NoError(t, err)
	}

	t.Run("matching pointer with survivor rewinds all three fields", func(t *testing.T) {
		seedRoom(t, "r-rw-1")
		survivorAt := lastAt.Add(-2 * time.Hour)
		survivor := &model.LastMessagePreview{MessageID: "m-prev", SenderAccount: "bob", SenderName: "Bob Chen", Msg: "still here", CreatedAt: survivorAt}

		require.NoError(t, store.RewindRoomLastMessage(ctx, "r-rw-1", "m-deleted", &model.LastMessagePointer{MessageID: survivor.MessageID, CreatedAt: survivor.CreatedAt}, survivor, deletedAt))

		var room model.Room
		require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-rw-1"}).Decode(&room))
		assert.Equal(t, "m-prev", room.LastMsgID)
		require.NotNil(t, room.LastMsgAt)
		assert.WithinDuration(t, survivorAt, *room.LastMsgAt, time.Millisecond)
		require.NotNil(t, room.LastMsg)
		assert.Equal(t, "m-prev", room.LastMsg.MessageID)
		assert.Equal(t, "still here", room.LastMsg.Msg)
	})

	t.Run("matching pointer with nil survivor clears fields like a fresh room", func(t *testing.T) {
		seedRoom(t, "r-rw-2")

		require.NoError(t, store.RewindRoomLastMessage(ctx, "r-rw-2", "m-deleted", nil, nil, deletedAt))

		var raw bson.M
		require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-rw-2"}).Decode(&raw))
		assert.Equal(t, "", raw["lastMsgId"], "lastMsgId resets to empty string like a fresh room")
		_, hasAt := raw["lastMsgAt"]
		assert.False(t, hasAt, "lastMsgAt must be unset")
		_, hasPreview := raw["lastMsg"]
		assert.False(t, hasPreview, "lastMsg must be unset")
		_, hasMention := raw["lastMentionAllAt"]
		assert.True(t, hasMention, "lastMentionAllAt must not be touched")
	})

	t.Run("non-matching pointer is a benign no-op", func(t *testing.T) {
		seedRoom(t, "r-rw-3")

		require.NoError(t, store.RewindRoomLastMessage(ctx, "r-rw-3", "m-other", nil, nil, deletedAt))

		var room model.Room
		require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-rw-3"}).Decode(&room))
		assert.Equal(t, "m-deleted", room.LastMsgID, "a concurrent newer message won — nothing rewound")
		require.NotNil(t, room.LastMsg)
		assert.Equal(t, "bye", room.LastMsg.Msg)
	})
}

func TestBroadcastWorker_SetRoomLastMessageEdited_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	lastAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	editedAt := lastAt.Add(time.Minute)
	seedRoom := func(t *testing.T, id string) {
		t.Helper()
		_, err := db.Collection("rooms").InsertOne(ctx, bson.M{
			"_id": id, "type": model.RoomTypeChannel, "siteId": "site-a",
			"lastMsgId": "m-last", "lastMsgAt": lastAt,
			"lastMsg": model.LastMessagePreview{MessageID: "m-last", SenderAccount: "alice", Msg: "original", CreatedAt: lastAt},
		})
		require.NoError(t, err)
	}

	t.Run("matching lastMsgId patches msg and editedAt", func(t *testing.T) {
		seedRoom(t, "r-ed-1")

		require.NoError(t, store.SetRoomLastMessageEdited(ctx, "r-ed-1", "m-last", "rewritten", nil, editedAt))

		var room model.Room
		require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-ed-1"}).Decode(&room))
		require.NotNil(t, room.LastMsg)
		assert.Equal(t, "rewritten", room.LastMsg.Msg)
		require.NotNil(t, room.LastMsg.EditedAt)
		assert.WithinDuration(t, editedAt, *room.LastMsg.EditedAt, time.Millisecond)
		assert.Equal(t, "m-last", room.LastMsg.MessageID, "other preview fields untouched")
	})

	t.Run("non-matching lastMsgId is a benign no-op", func(t *testing.T) {
		seedRoom(t, "r-ed-2")

		require.NoError(t, store.SetRoomLastMessageEdited(ctx, "r-ed-2", "m-other", "rewritten", nil, editedAt))

		var room model.Room
		require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-ed-2"}).Decode(&room))
		require.NotNil(t, room.LastMsg)
		assert.Equal(t, "original", room.LastMsg.Msg, "an edit of a non-latest message must not touch the preview")
		assert.Nil(t, room.LastMsg.EditedAt)
	})

	t.Run("encMsg patch stores ciphertext and blanks msg", func(t *testing.T) {
		seedRoom(t, "r-ed-3")
		enc := json.RawMessage(`{"version":3,"nonce":"bm9uY2U=","ciphertext":"Y2lwaGVy"}`)

		require.NoError(t, store.SetRoomLastMessageEdited(ctx, "r-ed-3", "m-last", "", enc, editedAt))

		var room model.Room
		require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-ed-3"}).Decode(&room))
		require.NotNil(t, room.LastMsg)
		assert.Empty(t, room.LastMsg.Msg, "encrypted patch must blank the plaintext")
		assert.Equal(t, string(enc), string(room.LastMsg.EncMsg), "encMsg round-trips byte-identical")
		require.NotNil(t, room.LastMsg.EditedAt)
		assert.WithinDuration(t, editedAt, *room.LastMsg.EditedAt, time.Millisecond)
	})

	t.Run("plaintext patch unsets stale encMsg", func(t *testing.T) {
		_, err := db.Collection("rooms").InsertOne(ctx, bson.M{
			"_id": "r-ed-4", "type": model.RoomTypeChannel, "siteId": "site-a",
			"lastMsgId": "m-last", "lastMsgAt": lastAt,
			"lastMsg": model.LastMessagePreview{
				MessageID: "m-last", SenderAccount: "alice", CreatedAt: lastAt,
				EncMsg: json.RawMessage(`{"version":1,"nonce":"eA==","ciphertext":"eQ=="}`),
			},
		})
		require.NoError(t, err)

		require.NoError(t, store.SetRoomLastMessageEdited(ctx, "r-ed-4", "m-last", "now plaintext", nil, editedAt))

		// Decode lastMsg into an explicitly typed bson.M: nested documents
		// inside a plain bson.M decode as bson.D in mongo-driver v2, which
		// would make key-presence checks silently miss.
		var raw struct {
			LastMsg bson.M `bson:"lastMsg"`
		}
		require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-ed-4"}).Decode(&raw))
		require.NotNil(t, raw.LastMsg)
		assert.Equal(t, "now plaintext", raw.LastMsg["msg"])
		_, hasEnc := raw.LastMsg["encMsg"]
		assert.False(t, hasEnc, "a plaintext patch must never leave a stale ciphertext behind")
	})
}

func TestBroadcastWorker_UpdateRoomLastMessage_PersistsPreview_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{ID: "r-prev", Name: "p", Type: model.RoomTypeChannel, SiteID: "site-a"})
	require.NoError(t, err)

	at := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	p := &model.LastMessagePreview{MessageID: "m-new", SenderAccount: "alice", SenderName: "Alice Wang", Msg: "hello", CreatedAt: at}
	require.NoError(t, store.UpdateRoomLastMessage(ctx, "r-prev", "m-new", at, false, p))

	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-prev"}).Decode(&room))
	assert.Equal(t, "m-new", room.LastMsgID)
	require.NotNil(t, room.LastMsg)
	assert.Equal(t, "hello", room.LastMsg.Msg)
	assert.Equal(t, "Alice Wang", room.LastMsg.SenderName)
}

func TestBroadcastWorker_BulkUpdateRoomLastMessage_PersistsPreview_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{ID: "r-bulk-p", Name: "p", Type: model.RoomTypeChannel, SiteID: "site-a"})
	require.NoError(t, err)

	at := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	updates := map[string]roomLastMsgUpdate{
		"r-bulk-p": {msgID: "m-bulk", at: at, preview: &model.LastMessagePreview{MessageID: "m-bulk", SenderAccount: "bob", Msg: "bulk hello", CreatedAt: at}},
	}
	require.NoError(t, store.BulkUpdateRoomLastMessage(ctx, updates))

	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-bulk-p"}).Decode(&room))
	assert.Equal(t, "m-bulk", room.LastMsgID)
	require.NotNil(t, room.LastMsg)
	assert.Equal(t, "bulk hello", room.LastMsg.Msg)
}

func TestBroadcastWorker_BulkUpdateRoomLastMessage_EmptyIsNoOp_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)
	require.NoError(t, store.BulkUpdateRoomLastMessage(context.Background(), nil))
	require.NoError(t, store.BulkUpdateRoomLastMessage(context.Background(), map[string]roomLastMsgUpdate{}))
}

func TestBroadcastWorker_GetThreadFollowers_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

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
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

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

func TestAdvanceSubscriptionLastSeen_OnlyAdvances(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	t1 := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID: "s-adv", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r-adv", LastSeenAt: &t1,
	})
	require.NoError(t, err)

	read := func() time.Time {
		var sub model.Subscription
		require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "s-adv"}).Decode(&sub))
		require.NotNil(t, sub.LastSeenAt)
		return sub.LastSeenAt.UTC()
	}

	t2 := t1.Add(time.Minute)
	require.NoError(t, store.AdvanceSubscriptionLastSeen(ctx, "r-adv", "alice", t2))
	assert.WithinDuration(t, t2, read(), time.Millisecond, "newer time advances")

	t0 := t1.Add(-time.Minute)
	require.NoError(t, store.AdvanceSubscriptionLastSeen(ctx, "r-adv", "alice", t0))
	assert.WithinDuration(t, t2, read(), time.Millisecond, "$max never regresses")

	// Missing subscription is a best-effort no-op.
	require.NoError(t, store.AdvanceSubscriptionLastSeen(ctx, "no-room", "nobody", t2))
}

func TestSetSubscriptionMentions_ReadGuard_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	msgAt := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	readAt := msgAt.Add(time.Minute) // already read past the message

	_, err := db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "s-read", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r-mention", LastSeenAt: &readAt},
		model.Subscription{ID: "s-unread", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r-mention"}, // lastSeenAt never set (omitempty)
	})
	require.NoError(t, err)

	require.NoError(t, store.SetSubscriptionMentions(ctx, "r-mention", []string{"alice", "bob"}, msgAt))

	var alice, bob model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "s-read"}).Decode(&alice))
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "s-unread"}).Decode(&bob))

	assert.False(t, alice.HasMention, "already-read subscription must not be re-flagged (#467)")
	assert.True(t, bob.HasMention, "never-read subscription (lastSeenAt absent) must still be flagged")
}

func TestBroadcastWorker_GetHistorySharedSince_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

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

// Drift state: a system message owns lastMsgId/lastMsgAt, while lastMsg still
// previews an older user message. Deleting that user message must replace ONLY
// the preview — the pointer pair keeps tracking the newest message (including
// system notices).
func TestBroadcastWorker_RewindRoomLastMessage_DriftPreviewOnly_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	sysAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	userAt := sysAt.Add(-time.Minute)
	deletedAt := sysAt.Add(time.Minute)
	seedDriftRoom := func(t *testing.T, id string) {
		t.Helper()
		_, err := db.Collection("rooms").InsertOne(ctx, bson.M{
			"_id": id, "type": model.RoomTypeChannel, "siteId": "site-a",
			"lastMsgId": "m-sys", "lastMsgAt": sysAt,
			"lastMsg": model.LastMessagePreview{MessageID: "m-user", SenderAccount: "alice", Msg: "bye", CreatedAt: userAt},
		})
		require.NoError(t, err)
	}

	t.Run("survivor replaces the preview, pointer untouched", func(t *testing.T) {
		seedDriftRoom(t, "r-drift-1")
		survivorAt := userAt.Add(-time.Hour)
		survivor := &model.LastMessagePreview{MessageID: "m-prev", SenderAccount: "bob", Msg: "still here", CreatedAt: survivorAt}

		require.NoError(t, store.RewindRoomLastMessage(ctx, "r-drift-1", "m-user", &model.LastMessagePointer{MessageID: "m-sys", CreatedAt: sysAt}, survivor, deletedAt))

		var room model.Room
		require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-drift-1"}).Decode(&room))
		assert.Equal(t, "m-sys", room.LastMsgID, "lastMsgId keeps tracking the newest (system) message")
		require.NotNil(t, room.LastMsgAt)
		assert.WithinDuration(t, sysAt, *room.LastMsgAt, time.Millisecond, "lastMsgAt untouched")
		require.NotNil(t, room.LastMsg)
		assert.Equal(t, "m-prev", room.LastMsg.MessageID)
	})

	t.Run("nil survivor unsets the preview, pointer untouched", func(t *testing.T) {
		seedDriftRoom(t, "r-drift-2")

		require.NoError(t, store.RewindRoomLastMessage(ctx, "r-drift-2", "m-user", &model.LastMessagePointer{MessageID: "m-sys", CreatedAt: sysAt}, nil, deletedAt))

		var raw bson.M
		require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-drift-2"}).Decode(&raw))
		assert.Equal(t, "m-sys", raw["lastMsgId"])
		_, hasAt := raw["lastMsgAt"]
		assert.True(t, hasAt, "lastMsgAt untouched")
		_, hasPreview := raw["lastMsg"]
		assert.False(t, hasPreview, "lastMsg must be unset")
	})
}

// Pre-rollout rooms have lastMsgId but no lastMsg subdoc: the edit patch must
// no-op instead of fabricating a partial preview missing
// messageId/senderAccount/createdAt.
func TestBroadcastWorker_SetRoomLastMessageEdited_LegacyRoomWithoutPreview_NoOp_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	lastAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{
		"_id": "r-legacy-1", "type": model.RoomTypeChannel, "siteId": "site-a",
		"lastMsgId": "m-last", "lastMsgAt": lastAt,
	})
	require.NoError(t, err)

	require.NoError(t, store.SetRoomLastMessageEdited(ctx, "r-legacy-1", "m-last", "rewritten", nil, lastAt.Add(time.Minute)))

	var raw bson.M
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-legacy-1"}).Decode(&raw))
	_, hasPreview := raw["lastMsg"]
	assert.False(t, hasPreview, "no partial lastMsg may be fabricated on a legacy room")
}

// Drift state edit: the previewed user message is edited while a newer system
// message owns lastMsgId — the preview must still be patched.
func TestBroadcastWorker_SetRoomLastMessageEdited_DriftPreviewPatched_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	sysAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	userAt := sysAt.Add(-time.Minute)
	editedAt := sysAt.Add(time.Minute)
	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{
		"_id": "r-drift-ed", "type": model.RoomTypeChannel, "siteId": "site-a",
		"lastMsgId": "m-sys", "lastMsgAt": sysAt,
		"lastMsg": model.LastMessagePreview{MessageID: "m-user", SenderAccount: "alice", Msg: "original", CreatedAt: userAt},
	})
	require.NoError(t, err)

	require.NoError(t, store.SetRoomLastMessageEdited(ctx, "r-drift-ed", "m-user", "rewritten", nil, editedAt))

	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-drift-ed"}).Decode(&room))
	assert.Equal(t, "m-sys", room.LastMsgID, "pointer untouched")
	require.NotNil(t, room.LastMsg)
	assert.Equal(t, "rewritten", room.LastMsg.Msg)
	require.NotNil(t, room.LastMsg.EditedAt)
	assert.WithinDuration(t, editedAt, *room.LastMsg.EditedAt, time.Millisecond)
}

// #6 phase-1 with a system pointer: deleting the newest message rewinds
// lastMsgId/lastMsgAt to the surviving system notice while lastMsg previews
// the older user message.
func TestBroadcastWorker_RewindRoomLastMessage_SystemPointerUserPreview_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	lastAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	sysAt := lastAt.Add(-time.Minute)
	userAt := lastAt.Add(-time.Hour)
	deletedAt := lastAt.Add(time.Minute)
	seed := func(t *testing.T, id string) {
		t.Helper()
		_, err := db.Collection("rooms").InsertOne(ctx, bson.M{
			"_id": id, "type": model.RoomTypeChannel, "siteId": "site-a",
			"lastMsgId": "m-deleted", "lastMsgAt": lastAt,
			"lastMsg": model.LastMessagePreview{MessageID: "m-deleted", SenderAccount: "alice", Msg: "bye", CreatedAt: lastAt},
		})
		require.NoError(t, err)
	}

	t.Run("system pointer + user survivor", func(t *testing.T) {
		seed(t, "r-sysptr-1")
		pointer := &model.LastMessagePointer{MessageID: "m-sys", CreatedAt: sysAt}
		survivor := &model.LastMessagePreview{MessageID: "m-user", SenderAccount: "bob", Msg: "older", CreatedAt: userAt}

		require.NoError(t, store.RewindRoomLastMessage(ctx, "r-sysptr-1", "m-deleted", pointer, survivor, deletedAt))

		var room model.Room
		require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-sysptr-1"}).Decode(&room))
		assert.Equal(t, "m-sys", room.LastMsgID, "room sorting follows the system notice")
		require.NotNil(t, room.LastMsgAt)
		assert.WithinDuration(t, sysAt, *room.LastMsgAt, time.Millisecond)
		require.NotNil(t, room.LastMsg)
		assert.Equal(t, "m-user", room.LastMsg.MessageID, "the preview follows the user message")
	})

	t.Run("system pointer, no user survivor — preview clears, pointer moves", func(t *testing.T) {
		seed(t, "r-sysptr-2")
		pointer := &model.LastMessagePointer{MessageID: "m-sys", CreatedAt: sysAt}

		require.NoError(t, store.RewindRoomLastMessage(ctx, "r-sysptr-2", "m-deleted", pointer, nil, deletedAt))

		var raw bson.M
		require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-sysptr-2"}).Decode(&raw))
		assert.Equal(t, "m-sys", raw["lastMsgId"])
		_, hasPreview := raw["lastMsg"]
		assert.False(t, hasPreview, "no user survivor ⇒ no preview")
	})
}

// #8: the bulk flush writes updatedAt from the mutation time (touchedAt),
// never the (possibly rewound) pointer time.
func TestBroadcastWorker_BulkUpdate_UpdatedAtUsesTouchedAt_Integration(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), nil, 0)

	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{"_id": "r-touched", "type": model.RoomTypeChannel, "siteId": "site-a"})
	require.NoError(t, err)

	pointerAt := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	touchedAt := pointerAt.Add(2 * time.Hour)
	require.NoError(t, store.BulkUpdateRoomLastMessage(ctx, map[string]roomLastMsgUpdate{
		"r-touched": {msgID: "m-old", at: pointerAt, touchedAt: touchedAt},
	}))

	var raw struct {
		LastMsgAt time.Time `bson:"lastMsgAt"`
		UpdatedAt time.Time `bson:"updatedAt"`
	}
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r-touched"}).Decode(&raw))
	assert.WithinDuration(t, pointerAt, raw.LastMsgAt, time.Millisecond)
	assert.WithinDuration(t, touchedAt, raw.UpdatedAt, time.Millisecond, "updatedAt must not regress to the rewound pointer time")
}
