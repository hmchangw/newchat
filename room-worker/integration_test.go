//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
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

	// Seed a subscription for ListByRoom / ReconcileMemberCounts.
	mustInsertSub(t, db, &model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u1"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}})

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

// TestMongoStore_GetUserWithMembership_DeptOnlyMatch_Integration pins the
// dept-aware org-membership lookup: a user added via Orgs:["X"] whose deptId
// is "X" (with no sectId match) must still report HasOrgMembership=true so the
// remove flow preserves their subscription. Checking only sectId would miss
// this case and cause the user's sub to be deleted even though they are still
// org-attached via the dept.
func TestMongoStore_GetUserWithMembership_DeptOnlyMatch_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	// Alice has deptId="X" and NO sectId. The org row in room_members is keyed
	// by member.id="X" — the dept-blind sectId-only lookup would miss it.
	_, err := db.Collection("users").InsertOne(ctx, model.User{
		ID: "u1", Account: "alice", SiteID: "site-a",
		DeptID: "X", DeptName: "Engineering",
	})
	require.NoError(t, err)
	_, err = db.Collection("room_members").InsertOne(ctx, model.RoomMember{
		ID: "rm-org", RoomID: "r1", Ts: time.Now().UTC(),
		Member: model.RoomMemberEntry{ID: "X", Type: model.RoomMemberOrg},
	})
	require.NoError(t, err)

	result, err := store.GetUserWithMembership(ctx, "r1", "alice")
	require.NoError(t, err)
	assert.True(t, result.HasOrgMembership,
		"deptId match must count as org membership — without it, removing the user would orphan an org-attached account")
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
		Member: model.RoomMemberEntry{ID: "u1", Type: model.RoomMemberIndividual, Account: "alice"},
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

func TestMongoStore_RemoveRole_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner},
	})
	if err != nil {
		t.Fatalf("seed subscription: %v", err)
	}

	// Demote: remove owner role, leaving member.
	if err := store.RemoveRole(ctx, "alice", "r1", model.RoleOwner); err != nil {
		t.Fatalf("RemoveRole: %v", err)
	}

	sub, err := store.GetSubscription(ctx, "alice", "r1")
	if err != nil {
		t.Fatalf("GetSubscription after demote: %v", err)
	}
	if slices.Contains(sub.Roles, model.RoleOwner) {
		t.Errorf("roles after demote = %v, should not contain owner", sub.Roles)
	}
	if !slices.Contains(sub.Roles, model.RoleMember) {
		t.Errorf("roles after demote = %v, want to contain member", sub.Roles)
	}

	// RemoveRole is idempotent ($pull) — removing again leaves member intact.
	if err := store.RemoveRole(ctx, "alice", "r1", model.RoleOwner); err != nil {
		t.Fatalf("RemoveRole idempotent: %v", err)
	}
	sub, err = store.GetSubscription(ctx, "alice", "r1")
	if err != nil {
		t.Fatalf("GetSubscription after idempotent remove: %v", err)
	}
	if len(sub.Roles) != 1 || !slices.Contains(sub.Roles, model.RoleMember) {
		t.Errorf("roles after idempotent remove = %v, want exactly [member]", sub.Roles)
	}
}

func TestMongoStore_DeleteSubscription_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	mustInsertSub(t, db, &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember}, JoinedAt: time.Now().UTC(),
	})

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

	mustInsertSub(t, db, &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember}, JoinedAt: time.Now().UTC(),
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember}, JoinedAt: time.Now().UTC(),
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: "s3", User: model.SubscriptionUser{ID: "u3", Account: "carol"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember}, JoinedAt: time.Now().UTC(),
	})

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
	// Bot subs carry u.isBot=true in production (stamped by newSub via
	// model.IsBotAccount); set it explicitly here since the test inserts the
	// document directly. ReconcileMemberCounts now counts off this flag.
	mustInsertSub(t, db, &model.Subscription{
		ID: "s4", User: model.SubscriptionUser{Account: "weather.bot", IsBot: true}, RoomID: "r1",
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

func TestMongoStore_ListAddMemberCandidates_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	// Seed: alice (new), bob (sub only — bug scenario), carol (sub+IRM), dave (bot, excluded).
	mustInsertUser(t, db, &model.User{ID: "u_alice", Account: "alice", SectID: "org-eng", SiteID: "site-a"})
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob", SectID: "org-eng", SiteID: "site-a"})
	mustInsertUser(t, db, &model.User{ID: "u_carol", Account: "carol", SectID: "org-eng", SiteID: "site-a"})
	mustInsertUser(t, db, &model.User{ID: "u_dave", Account: "dave.bot", SectID: "org-eng", SiteID: "site-a"})

	const roomID = "room-1"
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: "site-a",
		User:     model.SubscriptionUser{ID: "u_bob", Account: "bob"},
		RoomType: model.RoomTypeChannel, Roles: []model.Role{model.RoleMember},
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: "site-a",
		User:     model.SubscriptionUser{ID: "u_carol", Account: "carol"},
		RoomType: model.RoomTypeChannel, Roles: []model.Role{model.RoleMember},
	})
	_, err := db.Collection("room_members").InsertOne(ctx, model.RoomMember{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID,
		Member: model.RoomMemberEntry{ID: "u_carol", Type: model.RoomMemberIndividual, Account: "carol"},
	})
	require.NoError(t, err)

	got, err := store.ListAddMemberCandidates(ctx, []string{"org-eng"}, nil, roomID)
	require.NoError(t, err)

	byAccount := map[string]AddMemberCandidate{}
	for _, c := range got {
		byAccount[c.Account] = c
	}
	require.Len(t, byAccount, 3, "bot dave.bot must be excluded")
	assert.Equal(t, AddMemberCandidate{Account: "alice", HasSubscription: false, HasIndividualRoomMember: false}, byAccount["alice"])
	assert.Equal(t, AddMemberCandidate{Account: "bob", HasSubscription: true, HasIndividualRoomMember: false}, byAccount["bob"], "bug scenario: sub exists, IRM does not")
	assert.Equal(t, AddMemberCandidate{Account: "carol", HasSubscription: true, HasIndividualRoomMember: true}, byAccount["carol"])
}

// newIntegrationHandler creates a Handler wired to the given store and siteID with a no-op publish function.
func newIntegrationHandler(t *testing.T, store *MongoStore, siteID string) *Handler {
	t.Helper()
	noopPublish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	return NewHandler(store, siteID, noopPublish, testKeyStore, testKeySender)
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
	assert.Equal(t, []string{"u_alice", "u_bob"}, room.UIDs, "DM participant uids persisted, sorted")
	assert.Equal(t, []string{"alice", "bob"}, room.Accounts, "DM participant accounts persisted, paired by index with uids")
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
	h := NewHandler(store, "site-A", cap.fn(), testKeyStore, testKeySender)
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

	assert.Empty(t, cap.outboxOnPrefix(subject.Outbox("site-A", "site-A", "")),
		"no outbox to home site-A")

	// One member_added outbox per remote site — carries all the info
	// inbox-worker needs for sub creation AND search-sync-worker for MV update.
	memberB := cap.outboxOnPrefix(subject.Outbox("site-A", "site-B", model.OutboxMemberAdded))
	memberC := cap.outboxOnPrefix(subject.Outbox("site-A", "site-C", model.OutboxMemberAdded))
	require.Len(t, memberB, 1, "exactly one member_added outbox to site-B")
	require.Len(t, memberC, 1, "exactly one member_added outbox to site-C")

	var memberEnvB model.OutboxEvent
	require.NoError(t, json.Unmarshal(memberB[0].data, &memberEnvB))
	var memberPayloadB model.MemberAddEvent
	require.NoError(t, json.Unmarshal(memberEnvB.Payload, &memberPayloadB))
	assert.ElementsMatch(t, []string{"bob", "carol"}, memberPayloadB.Accounts)
	assert.Equal(t, model.RoomTypeChannel, memberPayloadB.RoomType)
	assert.Equal(t, "deal team", memberPayloadB.RoomName)
	assert.Equal(t, "site-A", memberPayloadB.SiteID)
	assert.Equal(t, "alice", memberPayloadB.RequesterAccount)
	assert.Nil(t, memberPayloadB.HistorySharedSince)
	assert.Equal(t, reqID+":site-B", memberB[0].msgID)

	var memberEnvC model.OutboxEvent
	require.NoError(t, json.Unmarshal(memberC[0].data, &memberEnvC))
	var memberPayloadC model.MemberAddEvent
	require.NoError(t, json.Unmarshal(memberEnvC.Payload, &memberPayloadC))
	assert.ElementsMatch(t, []string{"ian"}, memberPayloadC.Accounts)
	assert.Equal(t, model.RoomTypeChannel, memberPayloadC.RoomType)
	assert.Equal(t, "deal team", memberPayloadC.RoomName)
	assert.Equal(t, "site-A", memberPayloadC.SiteID)
	assert.Equal(t, "alice", memberPayloadC.RequesterAccount)
	assert.Nil(t, memberPayloadC.HistorySharedSince)
	assert.Equal(t, reqID+":site-C", memberC[0].msgID)
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
	h := NewHandler(store, "site-A", cap.fn(), testKeyStore, testKeySender)
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

	assert.Empty(t, cap.outboxOnPrefix(subject.Outbox("site-A", "site-A", "")))
	assert.Empty(t, cap.outboxOnPrefix(subject.Outbox("site-A", "site-C", "")))

	// One member_added outbox to the recipient's site — carries everything
	// inbox-worker needs to build the DM recipient's sub with the right shape.
	memberB := cap.outboxOnPrefix(subject.Outbox("site-A", "site-B", model.OutboxMemberAdded))
	require.Len(t, memberB, 1)
	var memberEnv model.OutboxEvent
	require.NoError(t, json.Unmarshal(memberB[0].data, &memberEnv))
	var memberPayload model.MemberAddEvent
	require.NoError(t, json.Unmarshal(memberEnv.Payload, &memberPayload))
	assert.Equal(t, model.RoomTypeDM, memberPayload.RoomType)
	assert.Equal(t, "", memberPayload.RoomName, "DM RoomName empty per v2 cleanup")
	assert.ElementsMatch(t, []string{"bob"}, memberPayload.Accounts)
	assert.Equal(t, "alice", memberPayload.RequesterAccount, "DM counterpart resolution depends on this")
	assert.Equal(t, "site-A", memberPayload.SiteID)
	assert.Equal(t, reqID+":site-B", memberB[0].msgID)
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
		SiteID:    "site-A",
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
	h := NewHandler(store, "site-A", cap.fn(), testKeyStore, testKeySender)
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

	// Each remote site receives only its own homed accounts — bob (site-B) and
	// ian (site-C) are partitioned by the userMap[].SiteID filter at the publish
	// site so we never ship cross-site account identities over the wire.
	var envB model.OutboxEvent
	require.NoError(t, json.Unmarshal(pubsB[0].data, &envB))
	var evtB model.MemberAddEvent
	require.NoError(t, json.Unmarshal(envB.Payload, &evtB))
	assert.ElementsMatch(t, []string{"bob"}, evtB.Accounts)
	assert.Equal(t, roomName, evtB.RoomName)
	assert.Equal(t, "site-A", evtB.SiteID)
	assert.Equal(t, reqID+":site-B", pubsB[0].msgID)

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

func TestProcessAddMembers_PublishesLocalInbox_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	mustInsertUser(t, db, &model.User{ID: "u_alice", Account: "alice", SiteID: "site-A",
		EngName: "Alice", ChineseName: "爱丽丝"})
	mustInsertUser(t, db, &model.User{ID: "u_charlie", Account: "charlie", SiteID: "site-A",
		EngName: "Charlie", ChineseName: "查理"})
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob", SiteID: "site-B",
		EngName: "Bob", ChineseName: "鲍勃"})

	roomID := idgen.GenerateID()
	const roomName = "federated-room"
	mustInsertRoom(t, db, &model.Room{
		ID: roomID, Name: roomName, Type: model.RoomTypeChannel,
		SiteID:    "site-A",
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	})
	mustInsertSub(t, db, &model.Subscription{
		ID:     idgen.GenerateUUIDv7(),
		User:   model.SubscriptionUser{ID: "u_alice", Account: "alice"},
		RoomID: roomID, SiteID: "site-A", Name: roomName, RoomType: model.RoomTypeChannel,
		Roles:    []model.Role{model.RoleOwner},
		JoinedAt: time.Now().UTC(),
	})

	cap := &publishCapture{}
	h := NewHandler(store, "site-A", cap.fn(), testKeyStore, testKeySender)
	const reqID = "0193abcd-0193-7abc-89ab-aaaa00000001"
	ctx = natsutil.WithRequestID(ctx, reqID)

	now := time.Now().UTC().UnixMilli()
	body, err := json.Marshal(model.AddMembersRequest{
		RoomID:           roomID,
		Users:            []string{"charlie", "bob"},
		RequesterID:      "u_alice",
		RequesterAccount: "alice",
		History:          model.HistoryConfig{Mode: model.HistoryModeAll},
		Timestamp:        now,
	})
	require.NoError(t, err)
	require.NoError(t, h.processAddMembers(ctx, body))

	pubs := cap.outboxOnPrefix(subject.InboxMemberAdded("site-A"))
	require.Len(t, pubs, 1, "exactly one local INBOX member_added publish per add-members call")

	var outboxEvt model.OutboxEvent
	require.NoError(t, json.Unmarshal(pubs[0].data, &outboxEvt))
	assert.Equal(t, "member_added", outboxEvt.Type)
	assert.Equal(t, "site-A", outboxEvt.SiteID)
	assert.Equal(t, "site-A", outboxEvt.DestSiteID, "self-loop: dest must equal origin")

	var inner model.MemberAddEvent
	require.NoError(t, json.Unmarshal(outboxEvt.Payload, &inner))
	assert.Equal(t, "member_added", inner.Type)
	assert.Equal(t, roomID, inner.RoomID)
	assert.Equal(t, "site-A", inner.SiteID)
	assert.ElementsMatch(t, []string{"charlie", "bob"}, inner.Accounts,
		"local INBOX must carry full add set — same-site (charlie) + remote (bob)")
	assert.Equal(t, reqID+":site-A", pubs[0].msgID,
		"Nats-Msg-Id must be natsutil.OutboxDedupID(ctx, originSite, payloadSeed) so JetStream dedups self-loop replays")

	// members_added sys-message: requester is the sender, Content is server-rendered.
	sysPubs := cap.outboxOnPrefix(subject.MsgCanonicalCreated("site-A"))
	require.Len(t, sysPubs, 1, "exactly one members_added sys-message per add-members call")
	var sysEvt model.MessageEvent
	require.NoError(t, json.Unmarshal(sysPubs[0].data, &sysEvt))
	assert.Equal(t, model.MessageTypeMembersAdded, sysEvt.Message.Type)
	assert.Equal(t, "alice", sysEvt.Message.UserAccount, "sender is the requester")
	assert.Equal(t, `"Alice 爱丽丝" added members to the channel`, sysEvt.Message.Content,
		"multi-add Content uses formatAddedMulti(requester)")
}

func TestProcessRemoveIndividual_PublishesLocalInbox_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	mustInsertUser(t, db, &model.User{ID: "u_alice", Account: "alice", SiteID: "site-A"})
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob", SiteID: "site-B",
		EngName: "Bob", ChineseName: "鲍勃"})

	roomID := idgen.GenerateID()
	mustInsertRoom(t, db, &model.Room{
		ID: roomID, Name: "fed-room", Type: model.RoomTypeChannel, SiteID: "site-A",
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), User: model.SubscriptionUser{ID: "u_bob", Account: "bob"},
		RoomID: roomID, SiteID: "site-A", Name: "fed-room", RoomType: model.RoomTypeChannel,
		Roles: []model.Role{model.RoleMember}, JoinedAt: time.Now().UTC(),
	})
	_, err := db.Collection("room_members").InsertOne(ctx, model.RoomMember{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID, Ts: time.Now().UTC(),
		Member: model.RoomMemberEntry{ID: "u_bob", Type: model.RoomMemberIndividual, Account: "bob"},
	})
	require.NoError(t, err)

	cap := &publishCapture{}
	h := NewHandler(store, "site-A", cap.fn(), testKeyStore, testKeySender)
	const reqID = "0193abcd-0193-7abc-89ab-aaaa00000002"
	ctx = natsutil.WithRequestID(ctx, reqID)

	body, err := json.Marshal(model.RemoveMemberRequest{
		RoomID:    roomID,
		Requester: "alice",
		Account:   "bob",
		Timestamp: time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, h.processRemoveMember(ctx, body))

	pubs := cap.outboxOnPrefix(subject.InboxMemberRemoved("site-A"))
	require.Len(t, pubs, 1)

	var outboxEvt model.OutboxEvent
	require.NoError(t, json.Unmarshal(pubs[0].data, &outboxEvt))
	assert.Equal(t, "member_removed", outboxEvt.Type)
	assert.Equal(t, "site-A", outboxEvt.SiteID)
	assert.Equal(t, "site-A", outboxEvt.DestSiteID)

	var inner model.MemberRemoveEvent
	require.NoError(t, json.Unmarshal(outboxEvt.Payload, &inner))
	assert.Equal(t, "member_removed", inner.Type, "admin-remove: inner type is member_removed")
	assert.Equal(t, roomID, inner.RoomID)
	assert.Equal(t, []string{"bob"}, inner.Accounts)
	assert.Equal(t, reqID+":site-A", pubs[0].msgID)

	// member_removed sys-message: requester is the sender, Content is server-rendered.
	sysPubs := cap.outboxOnPrefix(subject.MsgCanonicalCreated("site-A"))
	require.Len(t, sysPubs, 1, "exactly one member_removed sys-message per remove-member call")
	var sysEvt model.MessageEvent
	require.NoError(t, json.Unmarshal(sysPubs[0].data, &sysEvt))
	assert.Equal(t, model.MessageTypeMemberRemoved, sysEvt.Message.Type)
	assert.Equal(t, "alice", sysEvt.Message.UserAccount, "sender is the requester, not the removed user")
	assert.Equal(t, `"Bob 鲍勃" has been removed from the channel`, sysEvt.Message.Content,
		"forced-remove Content uses formatRemovedUser(user)")
}

// --- Sync DM endpoint integration tests ---

// newIntegSyncDMCtx returns a *natsrouter.Context with the canonical request ID
// for serverCreateDM; it also satisfies context.Context for store calls.
func newIntegSyncDMCtx() *natsrouter.Context {
	c := natsrouter.NewContext(map[string]string{})
	c.SetContext(natsutil.WithRequestID(context.Background(), testRequestID))
	return c
}

func TestSyncCreateDM_DM_PersistsRoomAndSubs(t *testing.T) {
	ctx := newIntegSyncDMCtx()
	db := setupMongo(t)
	store := NewMongoStore(db)
	siteID := "site-A"

	mustInsertUser(t, db, &model.User{ID: "u-alice", Account: "alice", SiteID: siteID, EngName: "Alice", ChineseName: "愛麗絲"})
	mustInsertUser(t, db, &model.User{ID: "u-bob", Account: "bob", SiteID: siteID, EngName: "Bob", ChineseName: "鮑勃"})

	cap := &publishCapture{}
	handler := NewHandler(store, siteID, cap.fn(), testKeyStore, testKeySender)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "bob"}
	got, err := handler.serverCreateDM(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.Success)
	assert.Equal(t, "alice", got.Subscription.User.Account)

	roomID := idgen.BuildDMRoomID("u-alice", "u-bob")
	room, err := store.GetRoom(ctx, roomID)
	require.NoError(t, err)
	assert.Equal(t, model.RoomTypeDM, room.Type)
	assert.Equal(t, siteID, room.SiteID)
	assert.Equal(t, 2, room.UserCount, "DM room.UserCount set at creation; no Reconcile pass")
	assert.Equal(t, 0, room.AppCount)
	assert.Equal(t, []string{"u-alice", "u-bob"}, room.UIDs, "DM participant uids persisted, sorted")
	assert.Equal(t, []string{"alice", "bob"}, room.Accounts, "DM participant accounts persisted, paired by index with uids")

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(2), subCount)

	// Two subscription.update publishes — one per user.
	subjects := map[string]int{}
	cap.mu.Lock()
	for _, p := range cap.captured {
		subjects[p.subject]++
	}
	cap.mu.Unlock()
	assert.Equal(t, 1, subjects[subject.SubscriptionUpdate("alice")])
	assert.Equal(t, 1, subjects[subject.SubscriptionUpdate("bob")])
}

func TestSyncCreateDM_SelfDM_PersistsSingleFavoritedSub(t *testing.T) {
	ctx := newIntegSyncDMCtx()
	db := setupMongo(t)
	store := NewMongoStore(db)
	siteID := "site-A"

	mustInsertUser(t, db, &model.User{ID: "u-alice", Account: "alice", SiteID: siteID, EngName: "Alice", ChineseName: "愛麗絲"})

	cap := &publishCapture{}
	handler := NewHandler(store, siteID, cap.fn(), testKeyStore, testKeySender)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "alice"}
	got, err := handler.serverCreateDM(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.Success)
	assert.Equal(t, "alice", got.Subscription.User.Account)
	assert.True(t, got.Subscription.Favorite)
	assert.True(t, got.Subscription.IsSubscribed)
	assert.Equal(t, model.RoomTypeDM, got.Subscription.RoomType)

	roomID := got.Subscription.RoomID
	require.NotEmpty(t, roomID)

	room, err := store.GetRoom(ctx, roomID)
	require.NoError(t, err)
	assert.Equal(t, model.RoomTypeDM, room.Type)
	assert.Equal(t, siteID, room.SiteID)
	assert.Equal(t, 1, room.UserCount)
	assert.Equal(t, 0, room.AppCount)
	assert.Equal(t, []string{"u-alice"}, room.UIDs)
	assert.Equal(t, []string{"alice"}, room.Accounts)

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(1), subCount, "self-DM is exactly one subscription")

	persisted, err := store.GetSubscription(ctx, "alice", roomID)
	require.NoError(t, err)
	assert.True(t, persisted.Favorite)
	assert.True(t, persisted.IsSubscribed)
	assert.Equal(t, "alice", persisted.Name)
	assert.Equal(t, model.RoomTypeDM, persisted.RoomType)

	subjects := map[string]int{}
	cap.mu.Lock()
	total := len(cap.captured)
	for _, p := range cap.captured {
		subjects[p.subject]++
	}
	cap.mu.Unlock()
	assert.Equal(t, 1, subjects[subject.SubscriptionUpdate("alice")])
	assert.Equal(t, 1, total, "subscription.update only; no outbox (same-site) and no canonical member event (Option C)")
}

func TestSyncCreateDM_BotDM_CrossSiteOutbox(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	siteID := "site-A"

	mustInsertUser(t, db, &model.User{ID: "u-alice", Account: "alice", SiteID: siteID, EngName: "Alice", ChineseName: "愛麗絲"})
	mustInsertUser(t, db, &model.User{ID: "u-bot", Account: "helper.bot", SiteID: "site-B", EngName: "Helper", ChineseName: "助手"})

	cap := &publishCapture{}
	handler := NewHandler(store, siteID, cap.fn(), testKeyStore, testKeySender)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeBotDM, RequesterAccount: "alice", OtherAccount: "helper.bot"}
	_, err := handler.serverCreateDM(newIntegSyncDMCtx(), req)
	require.NoError(t, err)

	pubs := cap.outboxOnPrefix(subject.Outbox(siteID, "site-B", model.OutboxMemberAdded))
	assert.Len(t, pubs, 1, "exactly one outbox to site-B")
}

func TestSyncCreateDM_RetryIdempotent(t *testing.T) {
	ctx := newIntegSyncDMCtx()
	db := setupMongo(t)
	store := NewMongoStore(db)
	siteID := "site-A"

	mustInsertUser(t, db, &model.User{ID: "u-alice", Account: "alice", SiteID: siteID, EngName: "Alice", ChineseName: "愛麗絲"})
	mustInsertUser(t, db, &model.User{ID: "u-bob", Account: "bob", SiteID: siteID, EngName: "Bob", ChineseName: "鮑勃"})

	cap := &publishCapture{}
	handler := NewHandler(store, siteID, cap.fn(), testKeyStore, testKeySender)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "bob"}

	r1, err := handler.serverCreateDM(newIntegSyncDMCtx(), req)
	require.NoError(t, err)
	r2, err := handler.serverCreateDM(newIntegSyncDMCtx(), req)
	require.NoError(t, err)
	require.NotNil(t, r1)
	require.NotNil(t, r2)
	assert.Equal(t, r1.Subscription.RoomID, r2.Subscription.RoomID)

	roomID := idgen.BuildDMRoomID("u-alice", "u-bob")
	room, err := store.GetRoom(ctx, roomID)
	require.NoError(t, err)
	assert.Equal(t, roomID, room.ID)

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(2), subCount, "redelivery must not create duplicate subs")
}

// Federation convergence: the cross-site OUTBOX payload carries the deterministic
// BuildDMRoomID, so the remote inbox-worker (and any replay) writes to the SAME
// room ID as the home site. The payload-derived dedup key (room id + requester
// account + createdAt + dest) is identical across replays, so JetStream dedup
// blocks duplicates.
func TestSyncCreateDM_CrossSite_OutboxPayloadConverges(t *testing.T) {
	ctx := newIntegSyncDMCtx()
	db := setupMongo(t)
	store := NewMongoStore(db)
	siteID := "site-A"

	mustInsertUser(t, db, &model.User{ID: "u-alice", Account: "alice", SiteID: siteID, EngName: "Alice", ChineseName: "愛麗絲"})
	mustInsertUser(t, db, &model.User{ID: "u-bob", Account: "bob", SiteID: "site-B", EngName: "Bob", ChineseName: "鮑勃"})

	cap1 := &publishCapture{}
	handler := NewHandler(store, siteID, cap1.fn(), testKeyStore, testKeySender)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "bob"}
	_, err := handler.serverCreateDM(newIntegSyncDMCtx(), req)
	require.NoError(t, err)

	// 1. Local Mongo room.ID equals the deterministic BuildDMRoomID.
	wantRoomID := idgen.BuildDMRoomID("u-alice", "u-bob")
	persisted, err := store.GetRoom(ctx, wantRoomID)
	require.NoError(t, err)
	assert.Equal(t, wantRoomID, persisted.ID)

	// 2. OUTBOX payload carries the same RoomID + the dedup key includes destSiteID.
	pubs := cap1.outboxOnPrefix(subject.Outbox(siteID, "site-B", model.OutboxMemberAdded))
	require.Len(t, pubs, 1)
	var env model.OutboxEvent
	require.NoError(t, json.Unmarshal(pubs[0].data, &env))
	var payload model.MemberAddEvent
	require.NoError(t, json.Unmarshal(env.Payload, &payload))
	assert.Equal(t, wantRoomID, payload.RoomID,
		"outbox RoomID must match local room.ID so remote site converges")
	assert.Equal(t, model.RoomTypeDM, payload.RoomType)
	assert.Equal(t, "alice", payload.RequesterAccount)
	assert.Equal(t, []string{"bob"}, payload.Accounts)
	assert.Contains(t, pubs[0].msgID, "site-B",
		"Nats-Msg-Id must include destSiteID for JetStream stream dedup")

	// 3. Replay produces the same Nats-Msg-Id because the dedup key is derived
	//    from the (stable) payload seed — room id + requester account + createdAt +
	//    dest — not the request ID. JetStream OUTBOX dedup rejects the second emit.
	cap2 := &publishCapture{}
	handler2 := NewHandler(store, siteID, cap2.fn(), testKeyStore, testKeySender)
	_, err = handler2.serverCreateDM(newIntegSyncDMCtx(), req)
	require.NoError(t, err)
	pubs2 := cap2.outboxOnPrefix(subject.Outbox(siteID, "site-B", model.OutboxMemberAdded))
	require.Len(t, pubs2, 1)
	assert.Equal(t, pubs[0].msgID, pubs2[0].msgID,
		"replay must produce identical Nats-Msg-Id so broker dedup blocks duplicate cross-site events")
}

func TestMongoStore_ListAddMemberCandidates_DeptMatching_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	mustInsertUser(t, db, &model.User{ID: "u_alice", Account: "alice", DeptID: "dept-X", SiteID: "site-a"})
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob", SectID: "dept-X", SiteID: "site-a"})

	got, err := store.ListAddMemberCandidates(ctx, []string{"dept-X"}, nil, "room-1")
	require.NoError(t, err)

	accounts := map[string]bool{}
	for _, c := range got {
		accounts[c.Account] = true
	}
	assert.True(t, accounts["alice"], "alice matches by deptId")
	assert.True(t, accounts["bob"], "bob matches by sectId (orgID coincides)")
	assert.Len(t, got, 2)
}

func setupValkey(t *testing.T) roomkeystore.RoomKeyStore {
	t.Helper()
	return roomkeystore.NewValkeyClusterStoreFromClient(testutil.StartValkeyCluster(t), time.Hour)
}

// startEmbeddedNATS starts an in-process NATS server and returns a connected client.
func startEmbeddedNATS(t *testing.T) *nats.Conn {
	t.Helper()
	opts := &natsserver.Options{Port: -1}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

// TestIntegration_CreateRoom_FansOutRoomKeyEvent verifies that processCreateRoom
// fans out the room key via NATS to every local-site member after a successful create.
//
// Setup: pre-seed key in Valkey (simulating room-service having stored it), seed
// users and the canonical CreateRoomRequest, then drive processCreateRoom and assert
// that RoomKeyEvent publishes arrive on chat.user.{account}.event.room.key for each
// local-site member.
func TestIntegration_CreateRoom_FansOutRoomKeyEvent(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	// Seed users — all on the same site so fanOutRoomKey includes both.
	mustInsertUser(t, db, &model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-A",
		EngName: "Alice", ChineseName: "爱丽丝",
	})
	mustInsertUser(t, db, &model.User{
		ID: "u_bob", Account: "bob", SiteID: "site-A",
		EngName: "Bob", ChineseName: "鲍勃",
	})

	// Pre-seed room key in Valkey (simulating room-service having run Set before the
	// canonical event was published).
	keyStore := setupValkey(t)
	const roomID = "test-fan-out-room"
	seedPair := roomkeystore.RoomKeyPair{
		PrivateKey: bytes.Repeat([]byte{0xAA}, 32),
	}
	_, err := keyStore.Set(ctx, roomID, seedPair)
	require.NoError(t, err)

	// Embedded NATS for key fan-out; subscribe to both accounts' key subjects.
	nc := startEmbeddedNATS(t)

	type received struct {
		subject string
		data    []byte
	}
	var mu sync.Mutex
	var keyMsgs []received

	for _, account := range []string{"alice", "bob"} {
		subj := subject.RoomKeyUpdate(account)
		_, err := nc.Subscribe(subj, func(m *nats.Msg) {
			mu.Lock()
			keyMsgs = append(keyMsgs, received{subject: m.Subject, data: append([]byte(nil), m.Data...)})
			mu.Unlock()
		})
		require.NoError(t, err)
	}
	require.NoError(t, nc.Flush())

	// Wire up the handler with real keyStore and keySender backed by embedded NATS.
	keySender := roomkeysender.NewSender(nc)
	noopPublish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	h := NewHandler(store, "site-A", noopPublish, keyStore, keySender)

	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0001"
	ctx = natsutil.WithRequestID(ctx, reqID)

	body, err := json.Marshal(model.CreateRoomRequest{
		RoomID:           roomID,
		Name:             "crypto room",
		Users:            []string{"bob"},
		ResolvedUsers:    []string{"bob"},
		RequesterID:      "u_alice",
		RequesterAccount: "alice",
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, h.processCreateRoom(ctx, body))

	// Allow a brief window for async NATS delivery.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(keyMsgs) >= 2
	}, 2*time.Second, 20*time.Millisecond, "expected RoomKeyEvent on both member subjects")

	mu.Lock()
	defer mu.Unlock()
	gotSubjects := make([]string, 0, len(keyMsgs))
	for _, m := range keyMsgs {
		gotSubjects = append(gotSubjects, m.subject)
		var evt model.RoomKeyEvent
		require.NoError(t, json.Unmarshal(m.data, &evt))
		assert.Equal(t, roomID, evt.RoomID, "RoomKeyEvent must carry the correct roomID")
		assert.NotEmpty(t, evt.PrivateKey, "PrivateKey must be populated in the client wire payload")
		assert.NotEmpty(t, evt.PrivateKey, "PrivateKey must be populated")
	}
	assert.ElementsMatch(t,
		[]string{subject.RoomKeyUpdate("alice"), subject.RoomKeyUpdate("bob")},
		gotSubjects,
		"key fan-out must reach every local-site member",
	)
}

// TestProcessCreateRoom_BotDM_DoesNotUpsert_Integration locks in that
// processCreateRoom's botDM branch keeps its insert-only contract on a
// JetStream redelivery: a pre-existing muted, inactive botDM subscription
// must NOT be refreshed (Muted, IsSubscribed, JoinedAt
// untouched). The re-subscribe refresh semantic is owned by user-service.
func TestProcessCreateRoom_BotDM_DoesNotUpsert_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	mustInsertUser(t, db, &model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-A",
		EngName: "Alice", ChineseName: "爱丽丝",
	})
	mustInsertUser(t, db, &model.User{
		ID: "u_helper_bot", Account: "helper.bot", SiteID: "site-A",
	})

	roomID := idgen.BuildDMRoomID("u_alice", "u_helper_bot")
	oldJoinedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	mustInsertRoom(t, db, &model.Room{
		ID: roomID, Type: model.RoomTypeBotDM, SiteID: "site-A",
		CreatedAt: oldJoinedAt, UpdatedAt: oldJoinedAt,
		UIDs:     []string{"u_alice", "u_helper_bot"},
		Accounts: []string{"alice", "helper.bot"},
	})
	mustInsertSub(t, db, &model.Subscription{
		ID:           "existing-human-sub",
		User:         model.SubscriptionUser{ID: "u_alice", Account: "alice"},
		RoomID:       roomID,
		SiteID:       "site-A",
		RoomType:     model.RoomTypeBotDM,
		Name:         "helper.bot",
		IsSubscribed: false,
		Muted:        true,
		JoinedAt:     oldJoinedAt,
	})
	mustInsertSub(t, db, &model.Subscription{
		ID:           "existing-bot-sub",
		User:         model.SubscriptionUser{ID: "u_helper_bot", Account: "helper.bot"},
		RoomID:       roomID,
		SiteID:       "site-A",
		RoomType:     model.RoomTypeBotDM,
		Name:         "alice",
		IsSubscribed: false,
		JoinedAt:     oldJoinedAt,
	})

	h := newIntegrationHandler(t, store, "site-A")
	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0193"
	ctx = natsutil.WithRequestID(ctx, reqID)

	body, err := json.Marshal(model.CreateRoomRequest{
		RoomID:           roomID,
		Users:            []string{"helper.bot"},
		RequesterID:      "u_alice",
		RequesterAccount: "alice",
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, h.processCreateRoom(ctx, body))

	got, err := store.GetSubscription(ctx, "alice", roomID)
	require.NoError(t, err)
	assert.True(t, got.Muted,
		"botDM path must NOT clear Muted on redelivery (insert-only contract)")
	assert.False(t, got.IsSubscribed,
		"botDM path must NOT refresh IsSubscribed on redelivery (insert-only contract)")
	assert.True(t, got.JoinedAt.Equal(oldJoinedAt),
		"botDM path must NOT refresh JoinedAt on redelivery (insert-only contract)")

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(2), subCount, "no duplicate subs after re-delivery")
}

// TestProcessCreateRoom_DM_DoesNotUpsert_Integration locks in that
// processCreateRoom's regular-DM branch keeps its insert-only contract:
// a pre-existing regular-DM subscription's state (specifically
// Muted = true and an old JoinedAt) must NOT be refreshed
// when processCreateRoom is replayed for the same (room, user) pair.
// This regression guard prevents accidental upsert wiring on the DM
// branch in future edits.
func TestProcessCreateRoom_DM_DoesNotUpsert_Integration(t *testing.T) {
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

	roomID := idgen.BuildDMRoomID("u_alice", "u_bob")
	oldJoinedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	mustInsertRoom(t, db, &model.Room{
		ID: roomID, Type: model.RoomTypeDM, SiteID: "site-A",
		CreatedAt: oldJoinedAt, UpdatedAt: oldJoinedAt,
		UIDs:     []string{"u_alice", "u_bob"},
		Accounts: []string{"alice", "bob"},
	})
	mustInsertSub(t, db, &model.Subscription{
		ID:       "existing-alice-sub",
		User:     model.SubscriptionUser{ID: "u_alice", Account: "alice"},
		RoomID:   roomID,
		SiteID:   "site-A",
		RoomType: model.RoomTypeDM,
		Name:     "bob",
		Muted:    true,
		JoinedAt: oldJoinedAt,
	})
	mustInsertSub(t, db, &model.Subscription{
		ID:       "existing-bob-sub",
		User:     model.SubscriptionUser{ID: "u_bob", Account: "bob"},
		RoomID:   roomID,
		SiteID:   "site-A",
		RoomType: model.RoomTypeDM,
		Name:     "alice",
		JoinedAt: oldJoinedAt,
	})

	h := newIntegrationHandler(t, store, "site-A")
	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0193"
	ctx = natsutil.WithRequestID(ctx, reqID)

	body, err := json.Marshal(model.CreateRoomRequest{
		RoomID:           roomID,
		Users:            []string{"bob"},
		RequesterID:      "u_alice",
		RequesterAccount: "alice",
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, h.processCreateRoom(ctx, body))

	got, err := store.GetSubscription(ctx, "alice", roomID)
	require.NoError(t, err)
	assert.True(t, got.Muted,
		"regular-DM path must NOT clear Muted on re-create (insert-only contract)")
	assert.True(t, got.JoinedAt.Equal(oldJoinedAt),
		"regular-DM path must NOT refresh JoinedAt on re-create (insert-only contract)")

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(2), subCount, "no duplicate subs after re-create")
}

// TestHandler_ProcessAddMembers_OrgToIndividualUpgrade_Integration verifies
// the end-to-end bug fix: alice was previously added via an org expansion
// (so she has a subscription and an org room_members row, but no individual
// row). An explicit re-add via req.Users must (a) NOT create a duplicate
// subscription and (b) DO write the missing individual row.
func TestHandler_ProcessAddMembers_OrgToIndividualUpgrade_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	cap := &publishCapture{}
	h := NewHandler(store, "site-a", cap.fn(), testKeyStore, testKeySender)

	const roomID = "room-1"
	mustInsertRoom(t, db, &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a", Name: "Room 1"})
	mustInsertUser(t, db, &model.User{ID: "u_alice", Account: "alice", EngName: "Alice", ChineseName: "爱丽丝", SectID: "org-eng", SiteID: "site-a"})
	mustInsertUser(t, db, &model.User{ID: "u_owner", Account: "owner", EngName: "Owner", ChineseName: "拥有者", SiteID: "site-a"})
	// Pre-state: alice in via org-eng. Sub exists, org room_members row exists, no individual row.
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: "site-a",
		User:     model.SubscriptionUser{ID: "u_alice", Account: "alice"},
		RoomType: model.RoomTypeChannel, Roles: []model.Role{model.RoleMember},
	})
	_, err := db.Collection("room_members").InsertOne(ctx, model.RoomMember{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID,
		Member: model.RoomMemberEntry{ID: "org-eng", Type: model.RoomMemberOrg},
	})
	require.NoError(t, err)

	req := model.AddMembersRequest{
		RoomID: roomID, Users: []string{"alice"}, RequesterAccount: "owner", RequesterID: "u_owner",
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	data, _ := json.Marshal(req)
	requestID := idgen.GenerateRequestID()
	require.NoError(t, h.processAddMembers(natsutil.WithRequestID(ctx, requestID), data))

	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": roomID, "u.account": "alice"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), subCount, "no duplicate sub")

	indivCount, err := db.Collection("room_members").CountDocuments(ctx, bson.M{
		"rid": roomID, "member.type": "individual", "member.account": "alice",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), indivCount, "individual room_members row written via upgrade path")
}

func TestMongoStore_GetOrgMembersWithIndividualStatus_DeptAndSect_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	mustInsertUser(t, db, &model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-a",
		DeptID: "X", DeptName: "Engineering", DeptTCName: "工程部",
	})
	mustInsertUser(t, db, &model.User{
		ID: "u_bob", Account: "bob", SiteID: "site-a",
		SectID: "X", SectName: "Eng Sect", SectTCName: "工程組",
	})

	const roomID = "room-1"
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: "site-a",
		User:     model.SubscriptionUser{ID: "u_alice", Account: "alice"},
		RoomType: model.RoomTypeChannel,
	})
	// Bob has an individual room_members row (member.id = user._id).
	_, err := db.Collection("room_members").InsertOne(ctx, model.RoomMember{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID,
		Member: model.RoomMemberEntry{ID: "u_bob", Type: model.RoomMemberIndividual, Account: "bob"},
	})
	require.NoError(t, err)

	got, err := store.GetOrgMembersWithIndividualStatus(ctx, roomID, "X")
	require.NoError(t, err)

	byAccount := map[string]OrgMemberStatus{}
	for _, m := range got {
		byAccount[m.Account] = m
	}
	require.Len(t, byAccount, 2)
	assert.Equal(t, OrgMemberStatus{
		Account: "alice", SiteID: "site-a",
		Name: "Engineering", TCName: "工程部", IsDept: true, HasIndividualMembership: false,
	}, byAccount["alice"])
	assert.Equal(t, OrgMemberStatus{
		Account: "bob", SiteID: "site-a",
		Name: "Eng Sect", TCName: "工程組", IsDept: false, HasIndividualMembership: true,
	}, byAccount["bob"])
}

// Multi-org overlap: alice's sectId matches one org row, her deptId matches
// another. When asking for either org's members, the result MUST mark her
// HasOtherOrgMembership=true so processRemoveOrg knows her subscription stays.
// Without this, removing one of the two orgs would silently orphan her sub.
func TestMongoStore_GetOrgMembersWithIndividualStatus_OtherOrgCovers_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	const roomID = "room-1"
	// alice: sectId="X", deptId="Y" — covered by both org rows simultaneously.
	mustInsertUser(t, db, &model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-a",
		SectID: "X", SectName: "Eng Sect", SectTCName: "工程組",
		DeptID: "Y", DeptName: "Frontend", DeptTCName: "前端",
	})
	// carol: only sectId="X" — when X is removed she's not covered by anything else.
	mustInsertUser(t, db, &model.User{
		ID: "u_carol", Account: "carol", SiteID: "site-a",
		SectID: "X", SectName: "Eng Sect", SectTCName: "工程組",
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: "site-a",
		User:     model.SubscriptionUser{ID: "u_alice", Account: "alice"},
		RoomType: model.RoomTypeChannel,
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: "site-a",
		User:     model.SubscriptionUser{ID: "u_carol", Account: "carol"},
		RoomType: model.RoomTypeChannel,
	})
	// Both X and Y are in the room as org members.
	for _, orgID := range []string{"X", "Y"} {
		_, err := db.Collection("room_members").InsertOne(ctx, model.RoomMember{
			ID: idgen.GenerateUUIDv7(), RoomID: roomID,
			Member: model.RoomMemberEntry{ID: orgID, Type: model.RoomMemberOrg},
		})
		require.NoError(t, err)
	}

	got, err := store.GetOrgMembersWithIndividualStatus(ctx, roomID, "X")
	require.NoError(t, err)

	byAccount := map[string]OrgMemberStatus{}
	for _, m := range got {
		byAccount[m.Account] = m
	}
	require.Len(t, byAccount, 2)
	assert.True(t, byAccount["alice"].HasOtherOrgMembership,
		"alice's deptId Y is also an org row in the room — she stays covered when X is removed")
	assert.False(t, byAccount["carol"].HasOtherOrgMembership,
		"carol has no other org coverage; removing X must drop her")
}

func TestHandler_ProcessCreateRoom_DMConcurrentByCounterpart_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	cap := &publishCapture{}
	h := NewHandler(store, "site-a", cap.fn(), testKeyStore, testKeySender)

	mustInsertUser(t, db, &model.User{ID: "u_alice", Account: "alice", EngName: "Alice", ChineseName: "爱", SiteID: "site-a"})
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob", EngName: "Bob", ChineseName: "鲍", SiteID: "site-a"})

	// Pre-state: alice's worker already raced to create the DM. Room exists + both subs.
	roomID := idgen.BuildDMRoomID("u_alice", "u_bob")
	mustInsertRoom(t, db, &model.Room{
		ID: roomID, Type: model.RoomTypeDM, SiteID: "site-a", Name: "",
		UIDs: []string{"u_alice", "u_bob"}, Accounts: []string{"alice", "bob"},
	})
	// Snapshot pre-existing sub IDs so we can verify the worker doesn't double-insert.
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: "site-a",
		User: model.SubscriptionUser{ID: "u_alice", Account: "alice"},
		Name: "bob", RoomType: model.RoomTypeDM,
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: "site-a",
		User: model.SubscriptionUser{ID: "u_bob", Account: "bob"},
		Name: "alice", RoomType: model.RoomTypeDM,
	})

	type subID struct {
		ID string `bson:"_id"`
	}
	var preSubs []subID
	cursor, err := db.Collection("subscriptions").Find(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	require.NoError(t, cursor.All(ctx, &preSubs))
	require.Len(t, preSubs, 2)
	preIDs := map[string]bool{preSubs[0].ID: true, preSubs[1].ID: true}

	// Bob's worker now processes Bob's canonical create event (Bob raced too).
	req := model.CreateRoomRequest{
		RoomID: roomID, Users: []string{"alice"},
		RequesterID: "u_bob", RequesterAccount: "bob",
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	data, _ := json.Marshal(req)
	requestID := idgen.GenerateRequestID()
	err = h.processCreateRoom(natsutil.WithRequestID(ctx, requestID), data)
	require.NoError(t, err, "bob's race must NOT fail with collision; alice's worker already wrote both subs")

	// One room, two subs, no replacements.
	roomCount, err := db.Collection("rooms").CountDocuments(ctx, bson.M{"_id": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(1), roomCount)

	var postSubs []subID
	cursor, err = db.Collection("subscriptions").Find(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	require.NoError(t, cursor.All(ctx, &postSubs))
	require.Len(t, postSubs, 2, "no extra subscription docs")
	for _, s := range postSubs {
		assert.True(t, preIDs[s.ID], "subscription %s was replaced — worker should reuse pre-existing", s.ID)
	}
}

func newSubFixture(id, userID, account, roomID, name string) model.Subscription {
	s := newSubFixtureWithRoles(id, userID, account, roomID, []model.Role{model.RoleMember})
	s.Name = name
	return s
}

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

func TestMongoStore_UpdateRoomName(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "room-worker-rename")
	store := NewMongoStore(db)

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: "r1", Name: "old", Type: model.RoomTypeChannel, SiteID: "site-a",
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, store.UpdateRoomName(ctx, "r1", "new"))
	got, err := store.GetRoom(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, "new", got.Name)

	assert.ErrorIs(t, store.UpdateRoomName(ctx, "missing", "x"), ErrRoomNotFound)
}

// TestHandler_ProcessCreateRoom_RecoversFromMidWriteCrash exercises the
// recovery path when a worker crashed AFTER CreateRoom succeeded but BEFORE
// BulkCreateSubscriptions wrote any subs. JetStream redelivers the canonical
// create event; the retry must find the existing room (structural match),
// skip past the dup-key, and finish the unfinished subscription writes so
// the room is no longer orphaned. Earlier behavior required the requester
// to already have a sub — that turned mid-write crashes into permanent
// failures and orphaned rooms.
func TestHandler_ProcessCreateRoom_RecoversFromMidWriteCrash(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	h := newIntegrationHandler(t, store, "site-A")

	mustInsertUser(t, db, &model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-A",
		EngName: "Alice", ChineseName: "爱丽丝",
	})
	mustInsertUser(t, db, &model.User{
		ID: "u_bob", Account: "bob", SiteID: "site-A",
		EngName: "Bob", ChineseName: "鲍勃",
	})

	// Pre-state: worker A wrote the room then crashed before BulkCreateSubscriptions.
	// Room exists with ZERO subscriptions (the orphan that the previous reconcile
	// helper turned into a permanent error and an unrecoverable DLQ entry).
	const roomID = "r_midwrite"
	const roomName = "deal team"
	mustInsertRoom(t, db, &model.Room{
		ID: roomID, Name: roomName, Type: model.RoomTypeChannel, SiteID: "site-A",
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	})
	subCount, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	require.Equal(t, int64(0), subCount, "pre-state: orphaned room, no subs")

	// JetStream redelivers; the retry must (a) not fail with a permanent error,
	// (b) write the missing subs, and (c) leave room.UserCount reconciled.
	const reqID = "0193abcd-0193-7abc-89ab-aaaa11111111"
	body, err := json.Marshal(model.CreateRoomRequest{
		RoomID: roomID, Name: roomName,
		Users:            []string{"bob"},
		ResolvedUsers:    []string{"bob"},
		RequesterID:      "u_alice",
		RequesterAccount: "alice",
		Timestamp:        time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, h.processCreateRoom(natsutil.WithRequestID(ctx, reqID), body),
		"mid-write crash recovery: retry must complete the unfinished writes, not return permanent")

	subCount, err = db.Collection("subscriptions").CountDocuments(ctx, bson.M{"roomId": roomID})
	require.NoError(t, err)
	assert.Equal(t, int64(2), subCount, "owner + invitee subs written on retry")

	room, err := store.GetRoom(ctx, roomID)
	require.NoError(t, err)
	assert.Equal(t, 2, room.UserCount, "ReconcileMemberCounts ran on retry")
}

func TestIntegration_ProcessRoomRename(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "room-worker-rename-integ")
	store := NewMongoStore(db)

	const (
		siteID     = "site-a"
		remoteSite = "site-b"
		roomID     = "r-rename-1"
		oldName    = "old-name"
		newName    = "new-name"
	)

	// Seed: channel room + 2 local subs + 1 remote sub + 3 users.
	mustInsertRoom(t, db, &model.Room{
		ID: roomID, Name: oldName, Type: model.RoomTypeChannel, SiteID: siteID,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: roomID, SiteID: siteID, Name: oldName, RoomType: model.RoomTypeChannel,
		Roles: []model.Role{model.RoleOwner}, JoinedAt: time.Now().UTC(),
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), User: model.SubscriptionUser{ID: "u2", Account: "bob"},
		RoomID: roomID, SiteID: siteID, Name: oldName, RoomType: model.RoomTypeChannel,
		Roles: []model.Role{model.RoleMember}, JoinedAt: time.Now().UTC(),
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), User: model.SubscriptionUser{ID: "u3", Account: "carol"},
		RoomID: roomID, SiteID: remoteSite, Name: oldName, RoomType: model.RoomTypeChannel,
		Roles: []model.Role{model.RoleMember}, JoinedAt: time.Now().UTC(),
	})
	mustInsertUser(t, db, &model.User{ID: "u1", Account: "alice", SiteID: siteID})
	mustInsertUser(t, db, &model.User{ID: "u2", Account: "bob", SiteID: siteID})
	mustInsertUser(t, db, &model.User{ID: "u3", Account: "carol", SiteID: remoteSite})

	cap := &publishCapture{}
	h := NewHandler(store, siteID, cap.fn(), testKeyStore, testKeySender)
	const reqID = "01970a4f-8c2d-7c9a-abcd-e0123456789a"
	ctx = natsutil.WithRequestID(ctx, reqID)

	body, err := json.Marshal(model.RenameRoomRequest{
		RoomID:    roomID,
		NewName:   newName,
		Account:   "alice",
		Timestamp: time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, h.processRoomRename(ctx, body))

	// Room name updated in Mongo.
	room, err := store.GetRoom(ctx, roomID)
	require.NoError(t, err)
	assert.Equal(t, newName, room.Name, "room name must be updated")

	// All subscriptions renamed.
	subs, err := store.ListByRoom(ctx, roomID)
	require.NoError(t, err)
	for _, sub := range subs {
		assert.Equal(t, newName, sub.Name, "sub %s name must be updated", sub.User.Account)
	}

	// One sys message published to canonical.
	sysPubs := cap.outboxOnPrefix(subject.MsgCanonicalCreated(siteID))
	require.Len(t, sysPubs, 1, "exactly one sys message published for room rename")
	var sysEvt model.MessageEvent
	require.NoError(t, json.Unmarshal(sysPubs[0].data, &sysEvt))
	assert.Equal(t, model.MessageTypeRoomRenamed, sysEvt.Message.Type)
	assert.Equal(t, "alice", sysEvt.Message.UserAccount)

	// Rename publishes a single room-scoped sys message via canonical; no
	// per-subscription fan-out — clients derive their state from the room event.
	cap.mu.Lock()
	for _, p := range cap.captured {
		assert.False(t,
			strings.HasPrefix(p.subject, "chat.user.") && strings.HasSuffix(p.subject, ".event.subscription.update"),
			"rename must not fan out subscription.update events (got %s)", p.subject)
	}
	cap.mu.Unlock()

	// One outbox publish to remote site-b.
	outboxPubs := cap.outboxOnPrefix(subject.Outbox(siteID, remoteSite, model.OutboxRoomRenamed))
	require.Len(t, outboxPubs, 1, "exactly one outbox publish to remote site-b")
	var outboxEvt model.OutboxEvent
	require.NoError(t, json.Unmarshal(outboxPubs[0].data, &outboxEvt))
	var outboxPayload model.RoomRenamedOutboxPayload
	require.NoError(t, json.Unmarshal(outboxEvt.Payload, &outboxPayload))
	assert.Equal(t, roomID, outboxPayload.RoomID)
	assert.Equal(t, newName, outboxPayload.NewName)
}

// TestMongoStore_UpdateSubscriptionNamesForRoom_StampsTimestamp asserts the
// origin rename write stamps nameUpdatedAt so the doc shares the federated
// event's high-water mark (inbox-worker guards remote applies against it).
func TestMongoStore_UpdateSubscriptionNamesForRoom_StampsTimestamp(t *testing.T) {
	db := testutil.MongoDB(t, "room-worker-rename-stamp")
	store := NewMongoStore(db)
	ctx := context.Background()

	mustInsertSub(t, db, &model.Subscription{
		ID:       "s1",
		User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:   "r1",
		SiteID:   "site-a",
		RoomType: model.RoomTypeChannel,
		Roles:    []model.Role{model.RoleMember},
		JoinedAt: time.Now().UTC(),
		Name:     "old",
	})

	subName := func() (string, time.Time) {
		t.Helper()
		var doc bson.M
		require.NoError(t, db.Collection("subscriptions").
			FindOne(ctx, bson.M{"roomId": "r1", "u.account": "alice"}).Decode(&doc))
		dt, ok := doc["nameUpdatedAt"].(bson.DateTime)
		require.True(t, ok, "nameUpdatedAt is %T, want bson.DateTime", doc["nameUpdatedAt"])
		return doc["name"].(string), dt.Time().UTC()
	}

	ts := time.Now().UTC()
	require.NoError(t, store.UpdateSubscriptionNamesForRoom(ctx, "r1", "new", ts))
	gotName, gotTs := subName()
	assert.Equal(t, "new", gotName)
	assert.Equal(t, ts.UnixMilli(), gotTs.UnixMilli())

	// Older rename is a guarded no-op — name and high-water mark unchanged.
	require.NoError(t, store.UpdateSubscriptionNamesForRoom(ctx, "r1", "stale", ts.Add(-time.Second)))
	gotName, gotTs = subName()
	assert.Equal(t, "new", gotName, "stale rename must not regress a newer name")
	assert.Equal(t, ts.UnixMilli(), gotTs.UnixMilli())

	// Equal-timestamp replay is a guarded no-op — the guard is strict $lt.
	require.NoError(t, store.UpdateSubscriptionNamesForRoom(ctx, "r1", "same-ts", ts))
	gotName, gotTs = subName()
	assert.Equal(t, "new", gotName, "same-timestamp rename must not modify state")
	assert.Equal(t, ts.UnixMilli(), gotTs.UnixMilli())

	// Newer rename advances both name and high-water mark.
	newer := ts.Add(time.Second)
	require.NoError(t, store.UpdateSubscriptionNamesForRoom(ctx, "r1", "newest", newer))
	gotName, gotTs = subName()
	assert.Equal(t, "newest", gotName)
	assert.Equal(t, newer.UnixMilli(), gotTs.UnixMilli())
}
