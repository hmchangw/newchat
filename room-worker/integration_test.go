//go:build integration

package main

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// capturedPublish records a single publish call for later assertion.
type capturedPublish struct {
	subject string
	data    []byte
	msgID   string
}

// publishCapture is a thread-safe slice of captured publish calls plus a
// PublishFunc closure that appends to it.
type publishCapture struct {
	mu       sync.Mutex
	captured []capturedPublish
}

func (p *publishCapture) fn() PublishFunc {
	return func(_ context.Context, subj string, data []byte, msgID string) error {
		p.mu.Lock()
		defer p.mu.Unlock()
		// Copy data: handler reuses marshal buffers and may mutate after publish.
		buf := make([]byte, len(data))
		copy(buf, data)
		p.captured = append(p.captured, capturedPublish{subject: subj, data: buf, msgID: msgID})
		return nil
	}
}

// outboxOnPrefix returns the captured publishes whose subject starts with prefix.
func (p *publishCapture) outboxOnPrefix(prefix string) []capturedPublish {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []capturedPublish
	for _, c := range p.captured {
		if strings.HasPrefix(c.subject, prefix) {
			out = append(out, c)
		}
	}
	return out
}

func setupMongo(t *testing.T) *mongo.Database {
	db := testutil.MongoDB(t, "room_worker_test")
	ensureRoomIdempotencyIndexes(t, db)
	return db
}

// ensureRoomIdempotencyIndexes mirrors the room-service-owned indexes that
// processCreateRoom redelivery handling depends on. In production these are
// created by room-service.EnsureIndexes; integration tests must replicate
// them so duplicate-key on retry works.
func ensureRoomIdempotencyIndexes(t *testing.T, db *mongo.Database) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.Collection("subscriptions").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "roomId", Value: 1}, {Key: "u.account", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		t.Fatalf("ensure subscriptions unique index: %v", err)
	}
	if _, err := db.Collection("room_members").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "rid", Value: 1}, {Key: "member.type", Value: 1}, {Key: "member.id", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		t.Fatalf("ensure room_members unique index: %v", err)
	}
}

func TestMongoStore_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	// Seed a room for ReconcileMemberCounts and GetRoom
	db.Collection("rooms").InsertOne(ctx, model.Room{ID: "r1", Name: "general", UserCount: 1})

	// Test CreateSubscription
	sub := model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u1"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}}
	if err := store.CreateSubscription(ctx, &sub); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}

	// Test ListByRoom
	subs, err := store.ListByRoom(ctx, "r1")
	if err != nil {
		t.Fatalf("ListByRoom: %v", err)
	}
	if len(subs) != 1 || subs[0].User.ID != "u1" {
		t.Errorf("got %+v", subs)
	}

	// Test ReconcileMemberCounts — sets userCount to the current subscription count.
	if err := store.ReconcileMemberCounts(ctx, "r1"); err != nil {
		t.Fatalf("ReconcileMemberCounts: %v", err)
	}
	room, err := store.GetRoom(ctx, "r1")
	if err != nil {
		t.Fatalf("GetRoom: %v", err)
	}
	if room.UserCount != 1 {
		t.Errorf("UserCount = %d, want 1 (actual subscription count)", room.UserCount)
	}
}

func TestMongoStore_GetSubscription_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", Roles: []model.Role{model.RoleOwner},
	})
	if err != nil {
		t.Fatalf("seed subscription: %v", err)
	}

	sub, err := store.GetSubscription(ctx, "alice", "r1")
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}
	if sub.User.Account != "alice" || sub.RoomID != "r1" {
		t.Errorf("got %+v", sub)
	}
	if !slices.Contains(sub.Roles, model.RoleOwner) {
		t.Errorf("roles = %v, want to contain owner", sub.Roles)
	}

	_, err = store.GetSubscription(ctx, "nonexistent", "r1")
	if err == nil {
		t.Error("expected error for nonexistent subscription")
	}
}

func TestMongoStore_GetUserWithMembership_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	_, err := db.Collection("users").InsertOne(ctx, model.User{
		ID: "u1", Account: "alice", SiteID: "site-a", SectID: "eng-org", SectName: "Engineering",
		EngName: "Alice Wang", ChineseName: "愛麗絲",
	})
	require.NoError(t, err)

	t.Run("no org membership and no subscription", func(t *testing.T) {
		result, err := store.GetUserWithMembership(ctx, "r1", "alice")
		require.NoError(t, err)
		assert.Equal(t, "u1", result.ID)
		assert.Equal(t, "alice", result.Account)
		assert.False(t, result.HasOrgMembership)
		assert.Empty(t, result.Roles)
	})

	t.Run("with subscription returns roles", func(t *testing.T) {
		_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
			ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID: "r1", Roles: []model.Role{model.RoleOwner, model.RoleMember},
		})
		require.NoError(t, err)

		result, err := store.GetUserWithMembership(ctx, "r1", "alice")
		require.NoError(t, err)
		assert.ElementsMatch(t, []model.Role{model.RoleOwner, model.RoleMember}, result.Roles)
	})

	t.Run("with org membership in room", func(t *testing.T) {
		_, err := db.Collection("room_members").InsertOne(ctx, model.RoomMember{
			ID: "rm1", RoomID: "r1", Ts: time.Now().UTC(),
			Member: model.RoomMemberEntry{ID: "eng-org", Type: model.RoomMemberOrg},
		})
		require.NoError(t, err)

		result, err := store.GetUserWithMembership(ctx, "r1", "alice")
		require.NoError(t, err)
		assert.True(t, result.HasOrgMembership)
	})

	t.Run("user not found", func(t *testing.T) {
		_, err := store.GetUserWithMembership(ctx, "r1", "nonexistent")
		require.Error(t, err)
		assert.ErrorIs(t, err, mongo.ErrNoDocuments)
	})
}

func TestMongoStore_GetOrgMembersWithIndividualStatus_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	users := db.Collection("users")
	_, err := users.InsertOne(ctx, model.User{ID: "u1", Account: "alice", SiteID: "site-a", SectID: "eng-org", SectName: "Engineering"})
	require.NoError(t, err)
	_, err = users.InsertOne(ctx, model.User{ID: "u2", Account: "bob", SiteID: "site-a", SectID: "eng-org", SectName: "Engineering"})
	require.NoError(t, err)

	_, err = db.Collection("room_members").InsertOne(ctx, model.RoomMember{
		ID: "rm1", RoomID: "r1", Ts: time.Now().UTC(),
		Member: model.RoomMemberEntry{ID: "alice", Type: model.RoomMemberIndividual, Account: "alice"},
	})
	require.NoError(t, err)

	results, err := store.GetOrgMembersWithIndividualStatus(ctx, "r1", "eng-org")
	require.NoError(t, err)
	require.Len(t, results, 2)

	statusMap := make(map[string]bool)
	for _, r := range results {
		statusMap[r.Account] = r.HasIndividualMembership
	}
	assert.True(t, statusMap["alice"])
	assert.False(t, statusMap["bob"])
}

func TestMongoStore_AddRole_RemoveRole_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember},
	})
	if err != nil {
		t.Fatalf("seed subscription: %v", err)
	}

	// Promote: add owner role
	err = store.AddRole(ctx, "alice", "r1", model.RoleOwner)
	if err != nil {
		t.Fatalf("AddRole: %v", err)
	}

	sub, err := store.GetSubscription(ctx, "alice", "r1")
	if err != nil {
		t.Fatalf("GetSubscription after promote: %v", err)
	}
	if !slices.Contains(sub.Roles, model.RoleOwner) {
		t.Errorf("roles after promote = %v, want to contain owner", sub.Roles)
	}
	if !slices.Contains(sub.Roles, model.RoleMember) {
		t.Errorf("roles after promote = %v, want to still contain member", sub.Roles)
	}

	// AddRole is idempotent ($addToSet)
	err = store.AddRole(ctx, "alice", "r1", model.RoleOwner)
	if err != nil {
		t.Fatalf("AddRole idempotent: %v", err)
	}
	sub, err = store.GetSubscription(ctx, "alice", "r1")
	if err != nil {
		t.Fatalf("GetSubscription after idempotent add: %v", err)
	}
	if len(sub.Roles) != 2 {
		t.Errorf("roles after idempotent add = %v, want exactly 2", sub.Roles)
	}

	// Demote: remove owner role
	err = store.RemoveRole(ctx, "alice", "r1", model.RoleOwner)
	if err != nil {
		t.Fatalf("RemoveRole: %v", err)
	}

	sub, err = store.GetSubscription(ctx, "alice", "r1")
	if err != nil {
		t.Fatalf("GetSubscription after demote: %v", err)
	}
	if slices.Contains(sub.Roles, model.RoleOwner) {
		t.Errorf("roles after demote = %v, should not contain owner", sub.Roles)
	}
	if !slices.Contains(sub.Roles, model.RoleMember) {
		t.Errorf("roles after demote = %v, want to contain member", sub.Roles)
	}
}

func TestMongoStore_DeleteSubscription_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember}, JoinedAt: time.Now().UTC(),
	}))

	deleted, err := store.DeleteSubscription(ctx, "r1", "alice")
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	subs, err := store.ListByRoom(ctx, "r1")
	require.NoError(t, err)
	assert.Empty(t, subs)
}

func TestMongoStore_DeleteSubscriptionsByAccounts_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember}, JoinedAt: time.Now().UTC(),
	}))
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{
		ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember}, JoinedAt: time.Now().UTC(),
	}))
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{
		ID: "s3", User: model.SubscriptionUser{ID: "u3", Account: "carol"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember}, JoinedAt: time.Now().UTC(),
	}))

	deleted, err := store.DeleteSubscriptionsByAccounts(ctx, "r1", []string{"alice", "bob"})
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)

	subs, err := store.ListByRoom(ctx, "r1")
	require.NoError(t, err)
	require.Len(t, subs, 1)
	assert.Equal(t, "carol", subs[0].User.Account)
}

func TestMongoStore_ReconcileMemberCounts_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	// Room with a stale userCount (e.g., from a drift scenario).
	room := &model.Room{ID: "r1", Name: "general", UserCount: 10, SiteID: "site-a", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	_, err := db.Collection("rooms").InsertOne(ctx, room)
	require.NoError(t, err)

	// Seed 3 subscriptions for r1 — this is the ground truth.
	_, err = db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1"},
		model.Subscription{ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1"},
		model.Subscription{ID: "s3", User: model.SubscriptionUser{ID: "u3", Account: "carol"}, RoomID: "r1"},
	})
	require.NoError(t, err)

	require.NoError(t, store.ReconcileMemberCounts(ctx, "r1"))

	updated, err := store.GetRoom(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, 3, updated.UserCount, "reconcile must set userCount to actual subscription count")

	// Idempotency: running it again yields the same value.
	require.NoError(t, store.ReconcileMemberCounts(ctx, "r1"))
	updated, err = store.GetRoom(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, 3, updated.UserCount, "reconcile must be idempotent")
}

func TestMongoStore_DeleteRoomMember_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	_, err := db.Collection("room_members").InsertMany(ctx, []interface{}{
		model.RoomMember{
			ID: "rm-ind", RoomID: "r1", Ts: time.Now().UTC(),
			Member: model.RoomMemberEntry{ID: "u1", Type: model.RoomMemberIndividual, Account: "alice"},
		},
		model.RoomMember{
			ID: "rm-org", RoomID: "r1", Ts: time.Now().UTC(),
			Member: model.RoomMemberEntry{ID: "eng-org", Type: model.RoomMemberOrg},
		},
	})
	require.NoError(t, err)

	t.Run("individual deletes by user id", func(t *testing.T) {
		require.NoError(t, store.DeleteRoomMember(ctx, "r1", model.RoomMemberIndividual, "u1"))
		count, err := db.Collection("room_members").CountDocuments(ctx, bson.M{"_id": "rm-ind"})
		require.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})

	t.Run("passing the account for an individual is a no-op", func(t *testing.T) {
		require.NoError(t, store.DeleteRoomMember(ctx, "r1", model.RoomMemberIndividual, "alice"))
	})

	t.Run("org deletes by id", func(t *testing.T) {
		require.NoError(t, store.DeleteRoomMember(ctx, "r1", model.RoomMemberOrg, "eng-org"))
		count, err := db.Collection("room_members").CountDocuments(ctx, bson.M{"_id": "rm-org"})
		require.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})
}

func mustInsertSub(t *testing.T, db *mongo.Database, sub *model.Subscription) {
	t.Helper()
	_, err := db.Collection("subscriptions").InsertOne(context.Background(), sub)
	require.NoError(t, err)
}

func mustInsertRoom(t *testing.T, db *mongo.Database, r *model.Room) {
	t.Helper()
	_, err := db.Collection("rooms").InsertOne(context.Background(), r)
	require.NoError(t, err)
}

func TestMongoStore_ListNewMembers_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	users := []interface{}{
		model.User{ID: "u1", Account: "alice", SectID: "org1"},
		model.User{ID: "u2", Account: "bob", SectID: "org1"},
		model.User{ID: "u3", Account: "carol", SectID: "org2"},
		model.User{ID: "u4", Account: "dave"},
		model.User{ID: "u5", Account: "helper.bot", SectID: "org1"},
	}
	_, err := db.Collection("users").InsertMany(ctx, users)
	require.NoError(t, err)

	_, err = db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID:     "s1",
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1",
	})
	require.NoError(t, err)

	t.Run("merges org members and direct accounts, excludes already-subscribed and bots", func(t *testing.T) {
		got, err := store.ListNewMembers(ctx, []string{"org1"}, []string{"carol", "dave"}, "r1")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"bob", "carol", "dave"}, got)
	})

	t.Run("empty inputs return nil", func(t *testing.T) {
		got, err := store.ListNewMembers(ctx, nil, nil, "r1")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("orgIDs only", func(t *testing.T) {
		got, err := store.ListNewMembers(ctx, []string{"org2"}, nil, "r1")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"carol"}, got)
	})

	t.Run("directAccounts only", func(t *testing.T) {
		got, err := store.ListNewMembers(ctx, nil, []string{"dave"}, "r1")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"dave"}, got)
	})
}

func TestReconcileMemberCountsSplitsBots(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	// Seed: 3 user subs and 1 bot sub for room r1.
	mustInsertSub(t, db, &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1",
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: "s2", User: model.SubscriptionUser{Account: "bob"}, RoomID: "r1",
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: "s3", User: model.SubscriptionUser{Account: "carol"}, RoomID: "r1",
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: "s4", User: model.SubscriptionUser{Account: "weather.bot"}, RoomID: "r1",
	})
	mustInsertRoom(t, db, &model.Room{ID: "r1", Type: model.RoomTypeChannel})

	require.NoError(t, store.ReconcileMemberCounts(ctx, "r1"))

	got, err := store.GetRoom(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, 3, got.UserCount)
	assert.Equal(t, 1, got.AppCount)
}

// mustInsertUser inserts a user document directly into the users collection.
func mustInsertUser(t *testing.T, db *mongo.Database, u *model.User) {
	t.Helper()
	_, err := db.Collection("users").InsertOne(context.Background(), u)
	require.NoError(t, err)
}

// newIntegrationHandler creates a Handler wired to the given store and siteID with a no-op publish function.
func newIntegrationHandler(t *testing.T, store *MongoStore, siteID string) *Handler {
	t.Helper()
	noopPublish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	return NewHandler(store, siteID, noopPublish)
}

func TestProcessCreateRoomChannelPersistsAllState(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	mustInsertUser(t, db, &model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-A",
		EngName: "Alice", ChineseName: "爱丽丝",
	})
	mustInsertUser(t, db, &model.User{
		ID: "u_bob", Account: "bob", SiteID: "site-A",
		EngName: "Bob", ChineseName: "鲍勃",
	})

	h := newIntegrationHandler(t, store, "site-A")
	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0193"
	ctx = natsutil.WithRequestID(ctx, reqID)

	body, err := json.Marshal(model.CreateRoomRequest{
		RoomID: "r_xyz", Name: "deal team",
		Users:            []string{"bob"},
		ResolvedUsers:    []string{"bob"},
		RequesterID:      "u_alice",
		RequesterAccount: "alice",
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, h.processCreateRoom(ctx, body))

	room, err := store.GetRoom(ctx, "r_xyz")
	require.NoError(t, err)
	assert.Equal(t, "deal team", room.Name)
	assert.Equal(t, model.RoomTypeChannel, room.Type)
	assert.Equal(t, 2, room.UserCount)
	assert.Equal(t, 0, room.AppCount)

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": "r_xyz"})
	require.NoError(t, err)
	assert.Equal(t, int64(2), subCount)

	// Lite-mode: with no orgs in the request, room_members stays empty.
	// Membership is implicit in `subscriptions` until an org joins and the
	// add-member backfill loop tracks individuals in room_members.
	rmCount, err := db.Collection("room_members").CountDocuments(ctx, bson.M{"rid": "r_xyz"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), rmCount)
}

func TestProcessCreateRoomDMPersistsTwoSubsAndZeroMembers(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	mustInsertUser(t, db, &model.User{ID: "u_alice", Account: "alice",
		EngName: "A", ChineseName: "A", SiteID: "site-A"})
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob",
		EngName: "B", ChineseName: "B", SiteID: "site-B"})

	h := newIntegrationHandler(t, store, "site-A")
	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0193"
	ctx = natsutil.WithRequestID(ctx, reqID)

	roomID := idgen.BuildDMRoomID("u_alice", "u_bob")
	body, err := json.Marshal(model.CreateRoomRequest{
		RoomID:           roomID,
		Users:            []string{"bob"},
		RequesterID:      "u_alice",
		RequesterAccount: "alice",
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, h.processCreateRoom(ctx, body))

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(2), subCount)

	rmCount, err := db.Collection("room_members").CountDocuments(ctx, bson.M{"rid": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(0), rmCount)

	room, err := store.GetRoom(ctx, roomID)
	require.NoError(t, err)
	assert.Equal(t, model.RoomTypeDM, room.Type)
	// CreatedBy is the requester's User.ID for every room type, including
	// DM/botDM (post-v2 cleanup; previously empty for DM/botDM).
	assert.Equal(t, "u_alice", room.CreatedBy)
}

func TestProcessCreateRoomChannel_OutboxPerRemoteSite(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	mustInsertUser(t, db, &model.User{ID: "u_alice", Account: "alice", SiteID: "site-A",
		EngName: "Alice", ChineseName: "爱丽丝"})
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob", SiteID: "site-B",
		EngName: "Bob", ChineseName: "鲍勃"})
	mustInsertUser(t, db, &model.User{ID: "u_carol", Account: "carol", SiteID: "site-B",
		EngName: "Carol", ChineseName: "卡罗尔"})
	mustInsertUser(t, db, &model.User{ID: "u_ian", Account: "ian", SiteID: "site-C",
		EngName: "Ian", ChineseName: "伊恩"})

	cap := &publishCapture{}
	h := NewHandler(store, "site-A", cap.fn())
	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0193"
	ctx = natsutil.WithRequestID(ctx, reqID)

	roomID := idgen.GenerateID()
	body, err := json.Marshal(model.CreateRoomRequest{
		RoomID: roomID, Name: "deal team",
		Users:            []string{"bob", "carol", "ian"},
		ResolvedUsers:    []string{"bob", "carol", "ian"},
		RequesterID:      "u_alice",
		RequesterAccount: "alice",
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, h.processCreateRoom(ctx, body))

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(4), subCount, "owner + 3 invitees")

	// All subs carry the room's home siteID.
	cur, err := db.Collection("subscriptions").Find(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	var subs []model.Subscription
	require.NoError(t, cur.All(ctx, &subs))
	for _, s := range subs {
		assert.Equal(t, "site-A", s.SiteID, "sub %s siteID", s.User.Account)
	}

	// Filter outbox publishes per destination site.
	pubsB := cap.outboxOnPrefix(subject.Outbox("site-A", "site-B", ""))
	pubsC := cap.outboxOnPrefix(subject.Outbox("site-A", "site-C", ""))
	pubsA := cap.outboxOnPrefix(subject.Outbox("site-A", "site-A", ""))
	require.Len(t, pubsB, 1, "exactly one outbox to site-B")
	require.Len(t, pubsC, 1, "exactly one outbox to site-C")
	assert.Empty(t, pubsA, "no outbox to home site-A")

	// Site-B payload assertions.
	var envB model.OutboxEvent
	require.NoError(t, json.Unmarshal(pubsB[0].data, &envB))
	var payloadB model.RoomCreatedOutbox
	require.NoError(t, json.Unmarshal(envB.Payload, &payloadB))
	assert.ElementsMatch(t, []string{"bob", "carol"}, payloadB.Accounts)
	assert.Equal(t, model.RoomTypeChannel, payloadB.RoomType)
	assert.Equal(t, "deal team", payloadB.RoomName)
	assert.Equal(t, "site-A", payloadB.HomeSiteID)
	assert.Equal(t, "alice", payloadB.RequesterAccount)
	assert.Equal(t, reqID+":site-B", pubsB[0].msgID)

	// Site-C payload assertions.
	var envC model.OutboxEvent
	require.NoError(t, json.Unmarshal(pubsC[0].data, &envC))
	var payloadC model.RoomCreatedOutbox
	require.NoError(t, json.Unmarshal(envC.Payload, &payloadC))
	assert.ElementsMatch(t, []string{"ian"}, payloadC.Accounts)
	assert.Equal(t, model.RoomTypeChannel, payloadC.RoomType)
	assert.Equal(t, "deal team", payloadC.RoomName)
	assert.Equal(t, "site-A", payloadC.HomeSiteID)
	assert.Equal(t, "alice", payloadC.RequesterAccount)
	assert.Equal(t, reqID+":site-C", pubsC[0].msgID)
}

func TestProcessCreateRoomDM_OutboxToCounterpartSite(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	mustInsertUser(t, db, &model.User{ID: "u_alice", Account: "alice", SiteID: "site-A",
		EngName: "Alice", ChineseName: "爱丽丝"})
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob", SiteID: "site-B",
		EngName: "Bob", ChineseName: "鲍勃"})

	cap := &publishCapture{}
	h := NewHandler(store, "site-A", cap.fn())
	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0193"
	ctx = natsutil.WithRequestID(ctx, reqID)

	roomID := idgen.BuildDMRoomID("u_alice", "u_bob")
	body, err := json.Marshal(model.CreateRoomRequest{
		RoomID:           roomID,
		Users:            []string{"bob"},
		RequesterID:      "u_alice",
		RequesterAccount: "alice",
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, h.processCreateRoom(ctx, body))

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(2), subCount)

	cur, err := db.Collection("subscriptions").Find(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	var subs []model.Subscription
	require.NoError(t, cur.All(ctx, &subs))
	for _, s := range subs {
		assert.Equal(t, "site-A", s.SiteID, "sub %s siteID", s.User.Account)
	}

	// Only one outbox publish, to site-B.
	pubsB := cap.outboxOnPrefix(subject.Outbox("site-A", "site-B", ""))
	require.Len(t, pubsB, 1)
	assert.Empty(t, cap.outboxOnPrefix(subject.Outbox("site-A", "site-A", "")))
	assert.Empty(t, cap.outboxOnPrefix(subject.Outbox("site-A", "site-C", "")))

	var env model.OutboxEvent
	require.NoError(t, json.Unmarshal(pubsB[0].data, &env))
	var payload model.RoomCreatedOutbox
	require.NoError(t, json.Unmarshal(env.Payload, &payload))
	assert.Equal(t, model.RoomTypeDM, payload.RoomType)
	assert.Equal(t, "", payload.RoomName, "DM RoomName empty per v2 cleanup")
	assert.ElementsMatch(t, []string{"bob"}, payload.Accounts)
	assert.Equal(t, "alice", payload.RequesterAccount)
	assert.Equal(t, "site-A", payload.HomeSiteID)
	assert.Equal(t, reqID+":site-B", pubsB[0].msgID)
}

func TestProcessAddMembers_OutboxPerRemoteSite(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	// Owner alice already on site-A.
	mustInsertUser(t, db, &model.User{ID: "u_alice", Account: "alice", SiteID: "site-A",
		EngName: "Alice", ChineseName: "爱丽丝"})
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob", SiteID: "site-B",
		EngName: "Bob", ChineseName: "鲍勃"})
	mustInsertUser(t, db, &model.User{ID: "u_ian", Account: "ian", SiteID: "site-C",
		EngName: "Ian", ChineseName: "伊恩"})

	roomID := idgen.GenerateID()
	const roomName = "deal team"
	mustInsertRoom(t, db, &model.Room{
		ID: roomID, Name: roomName, Type: model.RoomTypeChannel,
		SiteID: "site-A", CreatedBy: "u_alice",
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	})
	// Owner sub.
	mustInsertSub(t, db, &model.Subscription{
		ID:     idgen.GenerateUUIDv7(),
		User:   model.SubscriptionUser{ID: "u_alice", Account: "alice"},
		RoomID: roomID, SiteID: "site-A", Name: roomName, RoomType: model.RoomTypeChannel,
		Roles:    []model.Role{model.RoleOwner},
		JoinedAt: time.Now().UTC(),
	})
	// Pre-existing room_members with at least one org row → tracked-individuals mode.
	_, err := db.Collection("room_members").InsertMany(ctx, []interface{}{
		model.RoomMember{
			ID: idgen.GenerateUUIDv7(), RoomID: roomID, Ts: time.Now().UTC(),
			Member: model.RoomMemberEntry{ID: "u_alice", Type: model.RoomMemberIndividual, Account: "alice"},
		},
		model.RoomMember{
			ID: idgen.GenerateUUIDv7(), RoomID: roomID, Ts: time.Now().UTC(),
			Member: model.RoomMemberEntry{ID: "eng-org", Type: model.RoomMemberOrg},
		},
	})
	require.NoError(t, err)

	cap := &publishCapture{}
	h := NewHandler(store, "site-A", cap.fn())
	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0193"
	ctx = natsutil.WithRequestID(ctx, reqID)

	body, err := json.Marshal(model.AddMembersRequest{
		RoomID:           roomID,
		Users:            []string{"bob", "ian"},
		RequesterID:      "u_alice",
		RequesterAccount: "alice",
		History:          model.HistoryConfig{Mode: model.HistoryModeAll},
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, h.processAddMembers(ctx, body))

	// 2 new subs (bob, ian) plus the seeded owner = 3 total for the room.
	newSubs, err := db.Collection("subscriptions").CountDocuments(ctx,
		bson.M{"roomId": roomID, "u.account": bson.M{"$in": []string{"bob", "ian"}}})
	require.NoError(t, err)
	assert.Equal(t, int64(2), newSubs)

	pubsB := cap.outboxOnPrefix(subject.Outbox("site-A", "site-B", "member_added"))
	pubsC := cap.outboxOnPrefix(subject.Outbox("site-A", "site-C", "member_added"))
	pubsA := cap.outboxOnPrefix(subject.Outbox("site-A", "site-A", "member_added"))
	require.Len(t, pubsB, 1)
	require.Len(t, pubsC, 1)
	assert.Empty(t, pubsA, "no member_added outbox to home site-A")

	// Decode site-B event.
	var envB model.OutboxEvent
	require.NoError(t, json.Unmarshal(pubsB[0].data, &envB))
	var evtB model.MemberAddEvent
	require.NoError(t, json.Unmarshal(envB.Payload, &evtB))
	assert.ElementsMatch(t, []string{"bob"}, evtB.Accounts)
	assert.Equal(t, roomName, evtB.RoomName)
	assert.Equal(t, "site-A", evtB.SiteID)
	assert.Equal(t, reqID+":site-B", pubsB[0].msgID)

	// Decode site-C event.
	var envC model.OutboxEvent
	require.NoError(t, json.Unmarshal(pubsC[0].data, &envC))
	var evtC model.MemberAddEvent
	require.NoError(t, json.Unmarshal(envC.Payload, &evtC))
	assert.ElementsMatch(t, []string{"ian"}, evtC.Accounts)
	assert.Equal(t, roomName, evtC.RoomName)
	assert.Equal(t, "site-A", evtC.SiteID)
	assert.Equal(t, reqID+":site-C", pubsC[0].msgID)
}

func TestProcessCreateRoomIdempotentRedelivery(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	mustInsertUser(t, db, &model.User{ID: "u_alice", Account: "alice",
		EngName: "A", ChineseName: "A", SiteID: "site-A"})
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob",
		EngName: "B", ChineseName: "B", SiteID: "site-A"})

	h := newIntegrationHandler(t, store, "site-A")
	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0193"
	ctx = natsutil.WithRequestID(ctx, reqID)

	body, err := json.Marshal(model.CreateRoomRequest{
		RoomID: "r_idem", Name: "team",
		Users:            []string{"bob"},
		ResolvedUsers:    []string{"bob"},
		RequesterID:      "u_alice",
		RequesterAccount: "alice",
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)

	require.NoError(t, h.processCreateRoom(ctx, body))
	require.NoError(t, h.processCreateRoom(ctx, body))

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": "r_idem"})
	require.NoError(t, err)
	assert.Equal(t, int64(2), subCount, "redelivery must not create duplicate subs")
}
