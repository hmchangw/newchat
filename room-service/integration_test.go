//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func setupMongo(t *testing.T) *mongo.Database {
	return testutil.MongoDB(t, "room_service_test")
}

func setupKeyStore(t *testing.T, db *mongo.Database) roomkeystore.RoomKeyStore {
	t.Helper()
	return roomkeystore.NewMongoStore(db.Collection("rooms"), time.Hour)
}

func setupNATS(t *testing.T) string {
	t.Helper()
	return testutil.NATS(t)
}

func TestMongoStore_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	// Test CreateRoom and GetRoom
	room := model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel, SiteID: "site-a", UserCount: 1}
	mustInsertRoom(t, db, &room)
	got, err := store.GetRoom(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, "general", got.Name)

	// Test CreateSubscription and GetSubscription
	sub := model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}}
	mustInsertSub(t, db, &sub)
	gotSub, err := store.GetSubscription(ctx, "alice", "r1")
	require.NoError(t, err)
	// Bound the slice access explicitly with require.Len before indexing —
	// the prior `len(...) == 0 || s[0] != x` short-circuit guarded the read
	// at runtime, but it sits on operator-evaluation order rather than a
	// panic-exit, which static analyzers flag as a potential out-of-range
	// read. require.Len calls t.FailNow on mismatch so the index is provably
	// in-bounds on every path that reaches it.
	require.Len(t, gotSub.Roles, 1)
	assert.Equal(t, model.RoleOwner, gotSub.Roles[0])

	// Test not found
	_, err = store.GetSubscription(ctx, "u2", "r1")
	assert.Error(t, err, "expected error for missing subscription")
}

// TestMongoStore_CheckMembership_Integration verifies the projected existence
// check: nil when subscribed, model.ErrSubscriptionNotFound when not.
func TestMongoStore_CheckMembership_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	mustInsertSub(t, db, &model.Subscription{
		ID:     "s1",
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1",
		Roles:  []model.Role{model.RoleMember},
	})

	assert.NoError(t, store.CheckMembership(ctx, "alice", "r1"))

	err := store.CheckMembership(ctx, "bob", "r1")
	assert.ErrorIs(t, err, model.ErrSubscriptionNotFound)

	err = store.CheckMembership(ctx, "alice", "r2")
	assert.ErrorIs(t, err, model.ErrSubscriptionNotFound)
}

// TestMongoStore_GetRoom_ProjectionFields_Integration pins the field set that
// GetRoom's projection must return: every Room field read by any handler call
// site. Dropping one from the projection would silently zero it here, so this
// test is the guard for the projected read path.
func TestMongoStore_GetRoom_ProjectionFields_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	lastMsg := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	minSeen := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Millisecond)
	lastMentionAll := time.Now().UTC().Add(-90 * time.Minute).Truncate(time.Millisecond)
	mustInsertRoom(t, db, &model.Room{
		ID: "rproj", Name: "proj-room", Type: model.RoomTypeChannel, SiteID: "site-a",
		UserCount: 7, AppCount: 3, Restricted: true, ExternalAccess: true,
		LastMsgAt: &lastMsg, MinUserLastSeenAt: &minSeen, LastMsgID: "m123",
		LastMentionAllAt: &lastMentionAll,
	})

	got, err := store.GetRoom(ctx, "rproj")
	require.NoError(t, err)
	assert.Equal(t, "rproj", got.ID)
	assert.Equal(t, "proj-room", got.Name)
	assert.Equal(t, model.RoomTypeChannel, got.Type)
	assert.Equal(t, 7, got.UserCount)
	assert.Equal(t, 3, got.AppCount)
	assert.True(t, got.Restricted)
	assert.True(t, got.ExternalAccess)
	require.NotNil(t, got.LastMsgAt)
	assert.WithinDuration(t, lastMsg, *got.LastMsgAt, time.Second)
	require.NotNil(t, got.MinUserLastSeenAt)
	assert.WithinDuration(t, minSeen, *got.MinUserLastSeenAt, time.Second)
	require.NotNil(t, got.LastMentionAllAt, "lastMentionAllAt must be in the projection (read event computes hasGroupMention from it)")
	assert.WithinDuration(t, lastMentionAll, *got.LastMentionAllAt, time.Second)
}

// TestMongoStore_GetSubscription_ProjectionFields_Integration pins the field
// set that GetSubscription's projection must return: every Subscription field
// read by any handler call site. Same guard rationale as the GetRoom variant.
func TestMongoStore_GetSubscription_ProjectionFields_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	lastSeen := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Millisecond)
	mustInsertSub(t, db, &model.Subscription{
		ID:           "sproj",
		User:         model.SubscriptionUser{ID: "u9", Account: "carol", IsBot: true},
		RoomID:       "rproj",
		SiteID:       "site-a",
		Roles:        []model.Role{model.RoleOwner, model.RoleMember},
		Alert:        true,
		ThreadUnread: []string{"t1", "t2"},
		LastSeenAt:   &lastSeen,
	})

	got, err := store.GetSubscription(ctx, "carol", "rproj")
	require.NoError(t, err)
	assert.Equal(t, "u9", got.User.ID)
	assert.Equal(t, "carol", got.User.Account)
	assert.Equal(t, "rproj", got.RoomID)
	assert.Equal(t, "site-a", got.SiteID)
	assert.Equal(t, []model.Role{model.RoleOwner, model.RoleMember}, got.Roles)
	assert.True(t, got.Alert)
	assert.Equal(t, []string{"t1", "t2"}, got.ThreadUnread)
	require.NotNil(t, got.LastSeenAt)
	assert.WithinDuration(t, lastSeen, *got.LastSeenAt, time.Second)
}

func TestMongoStore_GetSubscriptionWithMembership_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	sub := &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", Roles: []model.Role{model.RoleOwner},
		JoinedAt: time.Now().UTC(),
	}
	mustInsertSub(t, db, sub)

	t.Run("no individual or org membership", func(t *testing.T) {
		result, err := store.GetSubscriptionWithMembership(ctx, "r1", "alice")
		require.NoError(t, err)
		assert.Equal(t, "alice", result.Subscription.User.Account)
		assert.False(t, result.HasIndividualMembership)
		assert.False(t, result.HasOrgMembership)
	})

	t.Run("with individual membership", func(t *testing.T) {
		_, err := db.Collection("room_members").InsertOne(ctx, model.RoomMember{
			ID: "rm1", RoomID: "r1", Ts: time.Now().UTC(),
			Member: model.RoomMemberEntry{ID: "alice", Type: model.RoomMemberIndividual, Account: "alice"},
		})
		require.NoError(t, err)

		result, err := store.GetSubscriptionWithMembership(ctx, "r1", "alice")
		require.NoError(t, err)
		assert.True(t, result.HasIndividualMembership)
	})

	t.Run("with org membership", func(t *testing.T) {
		_, err := db.Collection("users").InsertOne(ctx, model.User{
			ID: "u1", Account: "alice", SiteID: "site-a", SectID: "eng-org",
		})
		require.NoError(t, err)

		_, err = db.Collection("room_members").InsertOne(ctx, model.RoomMember{
			ID: "rm-org", RoomID: "r1", Ts: time.Now().UTC(),
			Member: model.RoomMemberEntry{ID: "eng-org", Type: model.RoomMemberOrg},
		})
		require.NoError(t, err)

		result, err := store.GetSubscriptionWithMembership(ctx, "r1", "alice")
		require.NoError(t, err)
		assert.True(t, result.HasOrgMembership)
	})

	t.Run("subscription not found", func(t *testing.T) {
		_, err := store.GetSubscriptionWithMembership(ctx, "r1", "nonexistent")
		require.Error(t, err)
	})
}

// TestMongoStore_GetSubscriptionWithMembership_DeptOnlyMatch_Integration pins
// the dept-aware org-membership lookup: a user added via Orgs:["X"] whose
// deptId is "X" (with no sectId match) must still report HasOrgMembership=true
// so the remove flow preserves their subscription. Checking only sectId would
// miss this case and the dual-membership branch wouldn't fire — the sub would
// be deleted even though the user remains org-attached via the dept.
func TestMongoStore_GetSubscriptionWithMembership_DeptOnlyMatch_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	const roomID = "r-dept-only"
	const account = "alice"

	// Alice has deptId="X" and NO sectId. The org row in room_members is keyed
	// by member.id="X" — the dept-blind sectId-only lookup would miss it.
	mustInsertSub(t, db, &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: account},
		RoomID: roomID, SiteID: "site-a", Roles: []model.Role{model.RoleMember},
		JoinedAt: time.Now().UTC(),
	})
	_, err := db.Collection("users").InsertOne(ctx, model.User{
		ID: "u1", Account: account, SiteID: "site-a",
		DeptID: "X", DeptName: "Engineering",
	})
	require.NoError(t, err)
	_, err = db.Collection("room_members").InsertOne(ctx, model.RoomMember{
		ID: "rm-org", RoomID: roomID, Ts: time.Now().UTC(),
		Member: model.RoomMemberEntry{ID: "X", Type: model.RoomMemberOrg},
	})
	require.NoError(t, err)

	result, err := store.GetSubscriptionWithMembership(ctx, roomID, account)
	require.NoError(t, err)
	assert.True(t, result.HasOrgMembership,
		"deptId match must count as org membership — without it, the dual-membership branch wouldn't fire and the sub would be deleted on remove")
}

func TestMongoStore_CountMembersAndOwners_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	_, err := db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}},
		model.Subscription{ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner, model.RoleMember}},
		model.Subscription{ID: "s3", User: model.SubscriptionUser{ID: "u3", Account: "carol"}, RoomID: "r1", Roles: []model.Role{model.RoleMember}},
		model.Subscription{ID: "s4", User: model.SubscriptionUser{ID: "u4", Account: "dave"}, RoomID: "r2", Roles: []model.Role{model.RoleOwner}},
	})
	require.NoError(t, err)

	t.Run("counts members and owners", func(t *testing.T) {
		counts, err := store.CountMembersAndOwners(ctx, "r1")
		require.NoError(t, err)
		assert.Equal(t, 3, counts.MemberCount)
		assert.Equal(t, 2, counts.OwnerCount)
	})

	t.Run("only owner", func(t *testing.T) {
		counts, err := store.CountMembersAndOwners(ctx, "r2")
		require.NoError(t, err)
		assert.Equal(t, 1, counts.MemberCount)
		assert.Equal(t, 1, counts.OwnerCount)
	})

	t.Run("empty room returns zeros", func(t *testing.T) {
		counts, err := store.CountMembersAndOwners(ctx, "nonexistent")
		require.NoError(t, err)
		assert.Equal(t, 0, counts.MemberCount)
		assert.Equal(t, 0, counts.OwnerCount)
	})
}

func TestMongoStore_CountOwners_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	_, err := db.Collection("subscriptions").InsertMany(ctx, []interface{}{
		model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}},
		model.Subscription{ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}},
		model.Subscription{ID: "s3", User: model.SubscriptionUser{ID: "u3", Account: "charlie"}, RoomID: "r1", Roles: []model.Role{model.RoleMember}},
		model.Subscription{ID: "s4", User: model.SubscriptionUser{ID: "u4", Account: "dave"}, RoomID: "r2", Roles: []model.Role{model.RoleOwner}},
	})
	if err != nil {
		t.Fatalf("seed subscriptions: %v", err)
	}

	count, err := store.CountOwners(ctx, "r1")
	if err != nil {
		t.Fatalf("CountOwners: %v", err)
	}
	if count != 2 {
		t.Errorf("CountOwners(r1) = %d, want 2", count)
	}

	count, err = store.CountOwners(ctx, "r2")
	if err != nil {
		t.Fatalf("CountOwners: %v", err)
	}
	if count != 1 {
		t.Errorf("CountOwners(r2) = %d, want 1", count)
	}

	count, err = store.CountOwners(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("CountOwners: %v", err)
	}
	if count != 0 {
		t.Errorf("CountOwners(nonexistent) = %d, want 0", count)
	}
}

func TestMongoStore_CountNewMembers_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	users := []interface{}{
		model.User{ID: "u1", Account: "alice", SiteID: "site-a", SectID: "org1"},
		model.User{ID: "u2", Account: "bob", SiteID: "site-a", SectID: "org1"},
		model.User{ID: "u3", Account: "charlie", SiteID: "site-a", SectID: "org1"},
		model.User{ID: "u4", Account: "helper.bot", SiteID: "site-a", SectID: "org1"},
		model.User{ID: "u5", Account: "p_webhook", SiteID: "site-a", SectID: "org1"},
		model.User{ID: "u6", Account: "dave", SiteID: "site-a", SectID: "org2"},
		model.User{ID: "u7", Account: "eve", SiteID: "site-a", SectID: "org3"},
	}
	if _, err := store.users.InsertMany(ctx, users); err != nil {
		t.Fatalf("seed users: %v", err)
	}

	subs := []interface{}{
		model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1"},
	}
	if _, err := store.subscriptions.InsertMany(ctx, subs); err != nil {
		t.Fatalf("seed subscriptions: %v", err)
	}

	count, err := store.CountNewMembers(ctx, []string{"org1"}, nil, "r1", "")
	if err != nil {
		t.Fatalf("CountNewMembers org1: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 (bob, charlie; alice already subscribed; bots excluded), got %d", count)
	}

	count, err = store.CountNewMembers(ctx, nil, []string{"eve"}, "r1", "")
	if err != nil {
		t.Fatalf("CountNewMembers direct: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 (eve), got %d", count)
	}

	count, err = store.CountNewMembers(ctx, []string{"org2"}, []string{"dave"}, "r1", "")
	if err != nil {
		t.Fatalf("CountNewMembers dedup: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 (dave; deduped between org2 and direct), got %d", count)
	}

	count, err = store.CountNewMembers(ctx, nil, nil, "r1", "")
	if err != nil {
		t.Fatalf("CountNewMembers empty: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 for empty inputs, got %d", count)
	}

	count, err = store.CountNewMembers(ctx, nil, []string{"alice"}, "r1", "")
	if err != nil {
		t.Fatalf("CountNewMembers all existing: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 when all accounts already members, got %d", count)
	}

	count, err = store.CountNewMembers(ctx, nil, []string{"helper.bot", "p_webhook"}, "r1", "")
	if err != nil {
		t.Fatalf("CountNewMembers bots: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 for bot accounts, got %d", count)
	}
}

func TestMongoStore_ListRoomMembers_Integration(t *testing.T) {
	ctx := context.Background()

	// helper: insert a RoomMember doc directly.
	insertRM := func(t *testing.T, db *mongo.Database, rm model.RoomMember) {
		t.Helper()
		_, err := db.Collection("room_members").InsertOne(ctx, rm)
		require.NoError(t, err)
	}
	insertSub := func(t *testing.T, db *mongo.Database, sub model.Subscription) {
		t.Helper()
		mustInsertSub(t, db, &sub)
	}
	ptr := func(i int) *int { return &i }

	t.Run("returns room_members when populated", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		insertRM(t, db, model.RoomMember{ID: "rm-ind-1", RoomID: "r1", Ts: base.Add(10 * time.Second),
			Member: model.RoomMemberEntry{ID: "u1", Type: model.RoomMemberIndividual, Account: "alice"}})
		insertRM(t, db, model.RoomMember{ID: "rm-ind-2", RoomID: "r1", Ts: base.Add(20 * time.Second),
			Member: model.RoomMemberEntry{ID: "u2", Type: model.RoomMemberIndividual, Account: "bob"}})
		insertRM(t, db, model.RoomMember{ID: "rm-org-1", RoomID: "r1", Ts: base.Add(30 * time.Second),
			Member: model.RoomMemberEntry{ID: "org-1", Type: model.RoomMemberOrg}})

		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, false)
		require.NoError(t, err)
		require.Len(t, got, 3)
		// orgs first, then individuals by ts asc
		assert.Equal(t, model.RoomMemberOrg, got[0].Member.Type)
		assert.Equal(t, "org-1", got[0].Member.ID)
		assert.Equal(t, "alice", got[1].Member.Account)
		assert.Equal(t, "bob", got[2].Member.Account)
	})

	t.Run("falls back to subscriptions when room_members empty", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		insertSub(t, db, model.Subscription{
			ID: "sub-a", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", SiteID: "site-a", JoinedAt: base.Add(10 * time.Second),
		})
		insertSub(t, db, model.Subscription{
			ID: "sub-b", User: model.SubscriptionUser{ID: "u-bob", Account: "bob"},
			RoomID: "r1", SiteID: "site-a", JoinedAt: base.Add(20 * time.Second),
		})

		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, false)
		require.NoError(t, err)
		require.Len(t, got, 2)
		for _, m := range got {
			assert.Equal(t, model.RoomMemberIndividual, m.Member.Type)
			assert.Equal(t, "r1", m.RoomID)
		}
		assert.Equal(t, "sub-a", got[0].ID)
		assert.Equal(t, "alice", got[0].Member.Account)
		assert.Equal(t, "u-alice", got[0].Member.ID)
		assert.Equal(t, "sub-b", got[1].ID)
	})

	t.Run("limit only", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
		for i := 0; i < 5; i++ {
			insertRM(t, db, model.RoomMember{
				ID: fmt.Sprintf("rm-%d", i), RoomID: "r1", Ts: base.Add(time.Duration(i) * time.Second),
				Member: model.RoomMemberEntry{ID: fmt.Sprintf("u%d", i), Type: model.RoomMemberIndividual, Account: fmt.Sprintf("acct%d", i)},
			})
		}

		got, err := store.ListRoomMembers(ctx, "r1", ptr(2), nil, false)
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, "acct0", got[0].Member.Account)
		assert.Equal(t, "acct1", got[1].Member.Account)
	})

	t.Run("offset only", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
		for i := 0; i < 5; i++ {
			insertRM(t, db, model.RoomMember{
				ID: fmt.Sprintf("rm-%d", i), RoomID: "r1", Ts: base.Add(time.Duration(i) * time.Second),
				Member: model.RoomMemberEntry{ID: fmt.Sprintf("u%d", i), Type: model.RoomMemberIndividual, Account: fmt.Sprintf("acct%d", i)},
			})
		}

		got, err := store.ListRoomMembers(ctx, "r1", nil, ptr(2), false)
		require.NoError(t, err)
		require.Len(t, got, 3)
		assert.Equal(t, "acct2", got[0].Member.Account)
		assert.Equal(t, "acct4", got[2].Member.Account)
	})

	t.Run("limit and offset", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
		for i := 0; i < 5; i++ {
			insertRM(t, db, model.RoomMember{
				ID: fmt.Sprintf("rm-%d", i), RoomID: "r1", Ts: base.Add(time.Duration(i) * time.Second),
				Member: model.RoomMemberEntry{ID: fmt.Sprintf("u%d", i), Type: model.RoomMemberIndividual, Account: fmt.Sprintf("acct%d", i)},
			})
		}

		got, err := store.ListRoomMembers(ctx, "r1", ptr(2), ptr(1), false)
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, "acct1", got[0].Member.Account)
		assert.Equal(t, "acct2", got[1].Member.Account)
	})

	t.Run("empty room returns empty slice", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		got, err := store.ListRoomMembers(ctx, "r-empty", nil, nil, false)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("room_members with only orgs does not fall back", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
		insertRM(t, db, model.RoomMember{ID: "rm-org-1", RoomID: "r1", Ts: base.Add(10 * time.Second),
			Member: model.RoomMemberEntry{ID: "org-1", Type: model.RoomMemberOrg}})
		insertRM(t, db, model.RoomMember{ID: "rm-org-2", RoomID: "r1", Ts: base.Add(20 * time.Second),
			Member: model.RoomMemberEntry{ID: "org-2", Type: model.RoomMemberOrg}})
		// Also seed a subscription — must NOT appear in the result since room_members is non-empty.
		insertSub(t, db, model.Subscription{
			ID: "sub-ghost", User: model.SubscriptionUser{ID: "u-ghost", Account: "ghost"},
			RoomID: "r1", SiteID: "site-a", JoinedAt: base,
		})

		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, false)
		require.NoError(t, err)
		require.Len(t, got, 2)
		for _, m := range got {
			assert.Equal(t, model.RoomMemberOrg, m.Member.Type)
		}
	})

	t.Run("stable pagination with identical ts", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		sameTs := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
		for _, id := range []string{"rm-a", "rm-b", "rm-c"} {
			insertRM(t, db, model.RoomMember{ID: id, RoomID: "r1", Ts: sameTs,
				Member: model.RoomMemberEntry{ID: id, Type: model.RoomMemberIndividual, Account: id}})
		}

		seen := map[string]bool{}
		for offset := 0; offset < 3; offset++ {
			page, err := store.ListRoomMembers(ctx, "r1", ptr(1), ptr(offset), false)
			require.NoError(t, err)
			require.Len(t, page, 1)
			id := page[0].ID
			assert.False(t, seen[id], "duplicate id %q across pages", id)
			seen[id] = true
		}
		assert.Len(t, seen, 3, "all 3 docs should appear exactly once across paginated calls")
	})
}

func TestMongoStore_ListRoomMembers_Enrich_Integration(t *testing.T) {
	ctx := context.Background()

	insertRM := func(t *testing.T, db *mongo.Database, rm model.RoomMember) {
		t.Helper()
		_, err := db.Collection("room_members").InsertOne(ctx, rm)
		require.NoError(t, err)
	}
	insertUser := func(t *testing.T, db *mongo.Database, u model.User) {
		t.Helper()
		_, err := db.Collection("users").InsertOne(ctx, u)
		require.NoError(t, err)
	}
	insertSub := func(t *testing.T, db *mongo.Database, sub model.Subscription) {
		t.Helper()
		mustInsertSub(t, db, &sub)
	}
	ptr := func(i int) *int { return &i }

	t.Run("individual enrichment via room_members path", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

		insertUser(t, db, model.User{
			ID: "u-alice", Account: "alice", SiteID: "site-a",
			EngName: "Alice Wang", ChineseName: "愛麗絲",
			SectName: "Cardiology", EmployeeID: "E10293",
		})
		insertSub(t, db, model.Subscription{
			ID: "sub-alice", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", SiteID: "site-a",
			Roles: []model.Role{model.RoleOwner}, JoinedAt: base,
		})
		insertRM(t, db, model.RoomMember{
			ID: "rm-alice", RoomID: "r1", Ts: base,
			Member: model.RoomMemberEntry{ID: "u-alice", Type: model.RoomMemberIndividual, Account: "alice"},
		})

		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 1)
		m := got[0].Member
		assert.Equal(t, "Alice Wang", m.EngName)
		assert.Equal(t, "愛麗絲", m.ChineseName)
		assert.True(t, m.IsOwner)
		assert.Equal(t, "Cardiology", m.SectName)
		assert.Equal(t, "E10293", m.EmployeeID)
		assert.Empty(t, m.OrgName)
		assert.Zero(t, m.MemberCount)
	})

	t.Run("individual non-owner sets IsOwner false", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)

		insertUser(t, db, model.User{ID: "u-bob", Account: "bob", EngName: "Bob", ChineseName: "鮑伯"})
		insertSub(t, db, model.Subscription{
			ID: "sub-bob", User: model.SubscriptionUser{ID: "u-bob", Account: "bob"},
			RoomID: "r1", Roles: []model.Role{model.RoleMember}, JoinedAt: base,
		})
		insertRM(t, db, model.RoomMember{
			ID: "rm-bob", RoomID: "r1", Ts: base,
			Member: model.RoomMemberEntry{ID: "u-bob", Type: model.RoomMemberIndividual, Account: "bob"},
		})

		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.False(t, got[0].Member.IsOwner)
	})

	t.Run("org enrichment via room_members path", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)

		// 3 users share sectId=sect-eng with sectName="Engineering".
		for i, acct := range []string{"a", "b", "c"} {
			insertUser(t, db, model.User{
				ID: fmt.Sprintf("u-%d", i), Account: acct,
				SectID: "sect-eng", SectName: "Engineering",
			})
		}
		insertRM(t, db, model.RoomMember{
			ID: "rm-org", RoomID: "r1", Ts: base,
			Member: model.RoomMemberEntry{ID: "sect-eng", Type: model.RoomMemberOrg},
		})

		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 1)
		m := got[0].Member
		assert.Equal(t, model.RoomMemberOrg, m.Type)
		assert.Equal(t, "Engineering", m.OrgName)
		assert.Equal(t, 3, m.MemberCount)
		assert.Empty(t, m.EngName)
		assert.False(t, m.IsOwner)
	})

	t.Run("enrichment via subscriptions fallback", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)

		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲", SectName: "Cardiology", EmployeeID: "E10293"})
		insertUser(t, db, model.User{ID: "u-bob", Account: "bob", EngName: "Bob", ChineseName: "鮑伯"})
		insertSub(t, db, model.Subscription{
			ID: "sub-a", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", Roles: []model.Role{model.RoleOwner}, JoinedAt: base.Add(10 * time.Second),
		})
		insertSub(t, db, model.Subscription{
			ID: "sub-b", User: model.SubscriptionUser{ID: "u-bob", Account: "bob"},
			RoomID: "r1", Roles: []model.Role{model.RoleMember}, JoinedAt: base.Add(20 * time.Second),
		})

		// NO room_members docs inserted — exercises the subscriptions fallback path.
		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 2)

		alice, bob := got[0].Member, got[1].Member
		assert.Equal(t, "alice", alice.Account)
		assert.Equal(t, "Alice Wang", alice.EngName)
		assert.True(t, alice.IsOwner)
		assert.Equal(t, "Cardiology", alice.SectName)
		assert.Equal(t, "E10293", alice.EmployeeID)
		assert.Equal(t, "bob", bob.Account)
		assert.Equal(t, "Bob", bob.EngName)
		assert.False(t, bob.IsOwner)
		assert.Empty(t, bob.SectName)
	})

	t.Run("enrich=false leaves display fields zero on same seed data", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)

		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲"})
		insertSub(t, db, model.Subscription{
			ID: "sub-alice", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", Roles: []model.Role{model.RoleOwner}, JoinedAt: base,
		})
		insertRM(t, db, model.RoomMember{
			ID: "rm-alice", RoomID: "r1", Ts: base,
			Member: model.RoomMemberEntry{ID: "u-alice", Type: model.RoomMemberIndividual, Account: "alice"},
		})

		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, false)
		require.NoError(t, err)
		require.Len(t, got, 1)
		m := got[0].Member
		assert.Empty(t, m.EngName)
		assert.Empty(t, m.ChineseName)
		assert.False(t, m.IsOwner)
		assert.Empty(t, m.OrgName)
		assert.Zero(t, m.MemberCount)
		assert.Empty(t, m.SectName)
		assert.Empty(t, m.EmployeeID)
		assert.Empty(t, m.OrgDescription)
	})

	t.Run("enrich=true preserves sort and pagination", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)

		// Seed: 2 individuals + 1 org in room_members; users for each.
		insertUser(t, db, model.User{ID: "u-a", Account: "a", EngName: "A"})
		insertUser(t, db, model.User{ID: "u-b", Account: "b", EngName: "B"})
		insertUser(t, db, model.User{ID: "u-c", Account: "c", SectID: "sect-c", SectName: "C"})
		insertRM(t, db, model.RoomMember{ID: "rm-a", RoomID: "r1", Ts: base.Add(10 * time.Second),
			Member: model.RoomMemberEntry{ID: "u-a", Type: model.RoomMemberIndividual, Account: "a"}})
		insertRM(t, db, model.RoomMember{ID: "rm-b", RoomID: "r1", Ts: base.Add(20 * time.Second),
			Member: model.RoomMemberEntry{ID: "u-b", Type: model.RoomMemberIndividual, Account: "b"}})
		insertRM(t, db, model.RoomMember{ID: "rm-org", RoomID: "r1", Ts: base.Add(30 * time.Second),
			Member: model.RoomMemberEntry{ID: "sect-c", Type: model.RoomMemberOrg}})

		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 3)
		// Orgs first, then individuals by ts ascending.
		assert.Equal(t, model.RoomMemberOrg, got[0].Member.Type)
		assert.Equal(t, "a", got[1].Member.Account)
		assert.Equal(t, "b", got[2].Member.Account)

		// Pagination is applied before enrichment — paging to the same slice
		// with enrich=true and enrich=false must yield the same members (by ID).
		bare, err := store.ListRoomMembers(ctx, "r1", ptr(1), ptr(1), false)
		require.NoError(t, err)
		enriched, err := store.ListRoomMembers(ctx, "r1", ptr(1), ptr(1), true)
		require.NoError(t, err)
		require.Len(t, bare, 1)
		require.Len(t, enriched, 1)
		assert.Equal(t, bare[0].ID, enriched[0].ID)
		assert.Equal(t, bare[0].Member.Type, enriched[0].Member.Type)
	})

	// Bug 4 regression: when an org overlaps as both dept and sect, the
	// service-side enrichment used to pick the dept branch unconditionally and
	// drop the sect names — so a dept row with empty deptName collapsed to the
	// orgID fallback while the worker's two-pass tiebreak rendered the sect
	// name. The spec requires byte-identical output across both paths.
	t.Run("org dept-first tiebreak falls back to sect names when dept names are empty", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)

		// One user with deptId="X" + empty deptName; one with sectId="X" +
		// sectName="Engineering". The worker's dept-first-with-fallback logic
		// renders "Engineering"; the service must match exactly.
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice",
			DeptID: "X", DeptName: "", DeptTCName: "",
		})
		insertUser(t, db, model.User{ID: "u-bob", Account: "bob",
			SectID: "X", SectName: "Engineering", SectTCName: "",
		})
		insertRM(t, db, model.RoomMember{
			ID: "rm-org-X", RoomID: "r1", Ts: base,
			Member: model.RoomMemberEntry{ID: "X", Type: model.RoomMemberOrg},
		})

		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "Engineering", got[0].Member.OrgName,
			"empty dept names must fall through to sect names; spec requires room-service output to match room-worker's two-pass tiebreak")
	})

	t.Run("org dept match with a non-empty dept name wins over sect", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)

		// member.id="X" matches one user by deptId (deptName non-empty) and one
		// by sectId. The dept branch must win, and memberCount counts both.
		insertUser(t, db, model.User{ID: "u-fe", Account: "fe", DeptID: "X", DeptName: "Frontend"})
		insertUser(t, db, model.User{ID: "u-eng", Account: "eng", SectID: "X", SectName: "Engineering"})
		insertRM(t, db, model.RoomMember{
			ID: "rm-org-X", RoomID: "r1", Ts: base,
			Member: model.RoomMemberEntry{ID: "X", Type: model.RoomMemberOrg},
		})

		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "Frontend", got[0].Member.OrgName)
		assert.Equal(t, 2, got[0].Member.MemberCount)
	})

	t.Run("org with no matching users falls back to orgID and zero count", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)

		insertRM(t, db, model.RoomMember{
			ID: "rm-org-ghost", RoomID: "r1", Ts: base,
			Member: model.RoomMemberEntry{ID: "ghost-org", Type: model.RoomMemberOrg},
		})

		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "ghost-org", got[0].Member.OrgName)
		assert.Zero(t, got[0].Member.MemberCount)
	})

	t.Run("multiple org rows resolved in one batch", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)

		insertUser(t, db, model.User{ID: "u-1", Account: "1", SectID: "sect-eng", SectName: "Engineering"})
		insertUser(t, db, model.User{ID: "u-2", Account: "2", SectID: "sect-eng", SectName: "Engineering"})
		insertUser(t, db, model.User{ID: "u-3", Account: "3", DeptID: "dept-ops", DeptName: "Operations"})
		insertRM(t, db, model.RoomMember{ID: "rm-eng", RoomID: "r1", Ts: base.Add(time.Second),
			Member: model.RoomMemberEntry{ID: "sect-eng", Type: model.RoomMemberOrg}})
		insertRM(t, db, model.RoomMember{ID: "rm-ops", RoomID: "r1", Ts: base.Add(2 * time.Second),
			Member: model.RoomMemberEntry{ID: "dept-ops", Type: model.RoomMemberOrg}})

		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 2)
		byID := map[string]model.RoomMemberEntry{
			got[0].Member.ID: got[0].Member,
			got[1].Member.ID: got[1].Member,
		}
		assert.Equal(t, "Engineering", byID["sect-eng"].OrgName)
		assert.Equal(t, 2, byID["sect-eng"].MemberCount)
		assert.Equal(t, "Operations", byID["dept-ops"].OrgName)
		assert.Equal(t, 1, byID["dept-ops"].MemberCount)
	})
}

// TestMongoStore_ListRoomMembers_BotEnrichment_Integration verifies that the
// subscriptions-fallback path (Path 2 / attachUserDisplayNames) correctly
// partitions bot vs human accounts: bot accounts are looked up in apps for
// Name, human accounts are looked up in users for EngName/ChineseName.
func TestMongoStore_ListRoomMembers_BotEnrichment_Integration(t *testing.T) {
	ctx := context.Background()

	insertSub := func(t *testing.T, db *mongo.Database, s model.Subscription) {
		t.Helper()
		_, err := db.Collection("subscriptions").InsertOne(ctx, s)
		require.NoError(t, err)
	}
	insertUser := func(t *testing.T, db *mongo.Database, u model.User) {
		t.Helper()
		_, err := db.Collection("users").InsertOne(ctx, u)
		require.NoError(t, err)
	}
	insertApp := func(t *testing.T, db *mongo.Database, doc bson.M) {
		t.Helper()
		_, err := db.Collection("apps").InsertOne(ctx, doc)
		require.NoError(t, err)
	}

	t.Run("botDM room: bot member gets Name from apps; human gets EngName/ChineseName from users", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)

		// Seed human user.
		insertUser(t, db, model.User{
			ID:          "u-alice",
			Account:     "alice",
			EngName:     "Alice Wang",
			ChineseName: "愛麗絲",
		})
		// Seed apps document with assistant.name = bot account.
		insertApp(t, db, bson.M{
			"_id":  "app-weather",
			"name": "Weather App",
			"assistant": bson.M{
				"enabled": true,
				"name":    "weather.bot",
			},
		})
		// Seed subscriptions only (no room_members) — botDM uses the fallback path.
		insertSub(t, db, model.Subscription{
			ID:       "sub-alice",
			User:     model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID:   "botdm-1",
			RoomType: model.RoomTypeBotDM,
			Roles:    []model.Role{model.RoleOwner},
			JoinedAt: base,
		})
		insertSub(t, db, model.Subscription{
			ID:       "sub-bot",
			User:     model.SubscriptionUser{ID: "u-bot", Account: "weather.bot"},
			RoomID:   "botdm-1",
			RoomType: model.RoomTypeBotDM,
			Roles:    []model.Role{model.RoleMember},
			JoinedAt: base.Add(time.Second),
		})

		got, err := store.ListRoomMembers(ctx, "botdm-1", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 2)

		// Map by account for deterministic assertions.
		byAccount := make(map[string]model.RoomMemberEntry)
		for _, m := range got {
			byAccount[m.Member.Account] = m.Member
		}

		human := byAccount["alice"]
		assert.Equal(t, "Alice Wang", human.EngName, "human member must have EngName from users")
		assert.Equal(t, "愛麗絲", human.ChineseName, "human member must have ChineseName from users")
		assert.Empty(t, human.Name, "human member must NOT have Name set")

		bot := byAccount["weather.bot"]
		assert.Equal(t, "Weather App", bot.Name, "bot member must have Name from apps")
		assert.Empty(t, bot.SectName, "bot has no user doc → no sectName")
		assert.Empty(t, bot.EngName, "bot member must NOT have EngName")
		assert.Empty(t, bot.ChineseName, "bot member must NOT have ChineseName")
	})

	t.Run("pure-human DM: only users query fires, no apps side-effects", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)

		insertUser(t, db, model.User{
			ID:          "u-carol",
			Account:     "carol",
			EngName:     "Carol",
			ChineseName: "卡羅爾",
		})
		insertUser(t, db, model.User{
			ID:          "u-dave",
			Account:     "dave",
			EngName:     "Dave",
			ChineseName: "大衛",
		})
		insertSub(t, db, model.Subscription{
			ID:       "sub-carol",
			User:     model.SubscriptionUser{ID: "u-carol", Account: "carol"},
			RoomID:   "dm-2",
			RoomType: model.RoomTypeDM,
			Roles:    []model.Role{model.RoleMember},
			JoinedAt: base,
		})
		insertSub(t, db, model.Subscription{
			ID:       "sub-dave",
			User:     model.SubscriptionUser{ID: "u-dave", Account: "dave"},
			RoomID:   "dm-2",
			RoomType: model.RoomTypeDM,
			Roles:    []model.Role{model.RoleMember},
			JoinedAt: base.Add(time.Second),
		})

		got, err := store.ListRoomMembers(ctx, "dm-2", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 2)

		for _, m := range got {
			assert.NotEmpty(t, m.Member.EngName, "all human members must have EngName")
			assert.Empty(t, m.Member.Name, "no Name on a human member")
		}
	})

	t.Run("humans-only fallback path regression: no bot accounts means apps.Find not called", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)

		insertUser(t, db, model.User{
			ID:          "u-eve",
			Account:     "eve",
			EngName:     "Eve",
			ChineseName: "夏娃",
		})
		insertSub(t, db, model.Subscription{
			ID:       "sub-eve",
			User:     model.SubscriptionUser{ID: "u-eve", Account: "eve"},
			RoomID:   "dm-3",
			RoomType: model.RoomTypeDM,
			Roles:    []model.Role{model.RoleMember},
			JoinedAt: base,
		})

		// No apps collection data at all — verifies no panic/error on empty botAccounts.
		got, err := store.ListRoomMembers(ctx, "dm-3", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "Eve", got[0].Member.EngName)
		assert.Empty(t, got[0].Member.Name)
	})
}

func TestMongoStore_ListOrgMembers_Integration(t *testing.T) {
	ctx := context.Background()

	insertUser := func(t *testing.T, db *mongo.Database, u model.User) {
		t.Helper()
		_, err := db.Collection("users").InsertOne(ctx, u)
		require.NoError(t, err)
	}

	t.Run("returns members sorted by account asc", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		// Inserted in non-alphabetical order to verify the store sorts.
		insertUser(t, db, model.User{ID: "u-charlie", Account: "charlie", EngName: "Charlie", ChineseName: "查理", SiteID: "site-a", SectID: "sect-eng", SectName: "Engineering"})
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", EngName: "Alice", ChineseName: "愛麗絲", SiteID: "site-a", SectID: "sect-eng", SectName: "Engineering"})
		insertUser(t, db, model.User{ID: "u-bob", Account: "bob", EngName: "Bob", ChineseName: "鮑伯", SiteID: "site-a", SectID: "sect-eng", SectName: "Engineering"})

		got, err := store.ListOrgMembers(ctx, "sect-eng")
		require.NoError(t, err)
		require.Len(t, got, 3)
		assert.Equal(t, "alice", got[0].Account)
		assert.Equal(t, "bob", got[1].Account)
		assert.Equal(t, "charlie", got[2].Account)
	})

	t.Run("filters by sectId only", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", EngName: "Alice", SiteID: "site-a", SectID: "sect-eng"})
		insertUser(t, db, model.User{ID: "u-bob", Account: "bob", EngName: "Bob", SiteID: "site-a", SectID: "sect-eng"})
		insertUser(t, db, model.User{ID: "u-carol", Account: "carol", EngName: "Carol", SiteID: "site-a", SectID: "sect-ops"})
		insertUser(t, db, model.User{ID: "u-dave", Account: "dave", EngName: "Dave", SiteID: "site-a", SectID: "sect-ops"})

		got, err := store.ListOrgMembers(ctx, "sect-eng")
		require.NoError(t, err)
		require.Len(t, got, 2)
		accounts := []string{got[0].Account, got[1].Account}
		assert.ElementsMatch(t, []string{"alice", "bob"}, accounts)
	})

	t.Run("empty org returns RoomInvalidOrg reason", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", SectID: "sect-eng"})

		_, err := store.ListOrgMembers(ctx, "sect-nope")
		require.Error(t, err)
		assert.True(t, errcode.HasReason(err, errcode.RoomInvalidOrg), "want RoomInvalidOrg in chain, got %v", err)
	})

	t.Run("returns RoomInvalidOrg reason when neither sectId nor deptId matches", func(t *testing.T) {
		// Users carry both sectId and deptId, but neither field equals the
		// queried orgID — guards against an accidental match on the wrong
		// branch of the $or (e.g. a future query rewrite that collapses to
		// $or:[{sectId:...},{deptId:...}] with the wrong field).
		db := setupMongo(t)
		store := NewMongoStore(db)
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", SectID: "sect-eng", DeptID: "dept-fe"})
		insertUser(t, db, model.User{ID: "u-bob", Account: "bob", SectID: "sect-ops", DeptID: "dept-be"})

		_, err := store.ListOrgMembers(ctx, "sect-nope")
		require.Error(t, err)
		assert.True(t, errcode.HasReason(err, errcode.RoomInvalidOrg), "want RoomInvalidOrg in chain, got %v", err)
	})

	t.Run("returns expected OrgMember shape", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		insertUser(t, db, model.User{
			ID: "u-alice", Account: "alice",
			EngName: "Alice", ChineseName: "愛麗絲",
			SiteID: "site-a", SectID: "sect-eng",
			SectName: "Engineering", EmployeeID: "EMP-001",
		})

		got, err := store.ListOrgMembers(ctx, "sect-eng")
		require.NoError(t, err)
		require.Len(t, got, 1)
		m := got[0]
		assert.Equal(t, "u-alice", m.ID)
		assert.Equal(t, "alice", m.Account)
		assert.Equal(t, "Alice", m.EngName)
		assert.Equal(t, "愛麗絲", m.ChineseName)
		assert.Equal(t, "site-a", m.SiteID)
	})

	t.Run("matches by deptId", func(t *testing.T) {
		// A dept-scoped org: room_members stores member.id = deptId. The
		// query must find users by deptId, not sectId alone — symmetric to
		// the GetUserWithMembership / GetSubscriptionWithMembership fixes
		// in the dept-aware membership pass.
		db := setupMongo(t)
		store := NewMongoStore(db)
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", EngName: "Alice", SiteID: "site-a", SectID: "sect-eng", DeptID: "dept-fe"})
		insertUser(t, db, model.User{ID: "u-bob", Account: "bob", EngName: "Bob", SiteID: "site-a", SectID: "sect-eng", DeptID: "dept-fe"})
		insertUser(t, db, model.User{ID: "u-carol", Account: "carol", EngName: "Carol", SiteID: "site-a", SectID: "sect-eng", DeptID: "dept-be"})

		got, err := store.ListOrgMembers(ctx, "dept-fe")
		require.NoError(t, err)
		require.Len(t, got, 2)
		accounts := []string{got[0].Account, got[1].Account}
		assert.ElementsMatch(t, []string{"alice", "bob"}, accounts)
	})

	t.Run("matches dept users when orgId equals deptId without parent sect match", func(t *testing.T) {
		// Truly dept-only: alice's sectId does NOT equal the orgID, so a
		// regression that dropped the deptId branch would no longer find her.
		db := setupMongo(t)
		store := NewMongoStore(db)
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", SiteID: "site-a", SectID: "sect-other", DeptID: "dept-x"})
		insertUser(t, db, model.User{ID: "u-bob", Account: "bob", SiteID: "site-a", SectID: "sect-other", DeptID: ""})

		got, err := store.ListOrgMembers(ctx, "dept-x")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "alice", got[0].Account)
	})
}

func TestMongoStore_FindExistingOrgIDs_Integration(t *testing.T) {
	ctx := context.Background()

	insertUser := func(t *testing.T, db *mongo.Database, u model.User) {
		t.Helper()
		_, err := db.Collection("users").InsertOne(ctx, u)
		require.NoError(t, err)
	}

	t.Run("returns sectId and deptId matches as a set", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", SiteID: "site-a", SectID: "sect-eng", DeptID: "dept-fe"})
		insertUser(t, db, model.User{ID: "u-bob", Account: "bob", SiteID: "site-a", SectID: "sect-ops", DeptID: "dept-be"})

		got, err := store.FindExistingOrgIDs(ctx, []string{"sect-eng", "dept-be", "missing"})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"sect-eng", "dept-be"}, got)
	})

	t.Run("returns empty when no org matches", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", SiteID: "site-a", SectID: "sect-eng", DeptID: "dept-fe"})

		got, err := store.FindExistingOrgIDs(ctx, []string{"phantom-1", "phantom-2"})
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("empty input is a no-op", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		got, err := store.FindExistingOrgIDs(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("orgId equal to deptId only (no parent sect) still resolves", func(t *testing.T) {
		// Truly dept-only: alice's sectId does NOT equal the orgID, so the
		// existence check must find her via the deptId branch alone.
		db := setupMongo(t)
		store := NewMongoStore(db)
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", SiteID: "site-a", SectID: "sect-other", DeptID: "dept-x"})

		got, err := store.FindExistingOrgIDs(ctx, []string{"dept-x"})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"dept-x"}, got)
	})

	t.Run("orgId matched by sect on one user and dept on another dedupes", func(t *testing.T) {
		// Same orgID "foo" lands on both the sectId branch (via alice) and
		// the deptId branch (via bob). The dedup contract says it shows up
		// once in the result, not twice.
		db := setupMongo(t)
		store := NewMongoStore(db)
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", SiteID: "site-a", SectID: "foo", DeptID: "dept-a"})
		insertUser(t, db, model.User{ID: "u-bob", Account: "bob", SiteID: "site-a", SectID: "sect-b", DeptID: "foo"})

		got, err := store.FindExistingOrgIDs(ctx, []string{"foo"})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"foo"}, got)
		assert.Len(t, got, 1, "overlapping sect+dept match must appear exactly once")
	})
}

func TestMongoStore_FindExistingAccounts_Integration(t *testing.T) {
	ctx := context.Background()

	insertUser := func(t *testing.T, db *mongo.Database, u model.User) {
		t.Helper()
		_, err := db.Collection("users").InsertOne(ctx, u)
		require.NoError(t, err)
	}

	t.Run("returns the matching subset", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", SiteID: "site-a"})
		insertUser(t, db, model.User{ID: "u-bob", Account: "bob", SiteID: "site-a"})

		got, err := store.FindExistingAccounts(ctx, []string{"alice", "bob", "ghost"})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"alice", "bob"}, got)
	})

	t.Run("returns empty when no account matches", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", SiteID: "site-a"})

		got, err := store.FindExistingAccounts(ctx, []string{"ghost-1", "ghost-2"})
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("empty input is a no-op", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		got, err := store.FindExistingAccounts(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestMongoStore_ListRoomsByIDs(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	t1 := now
	t2 := now.Add(1 * time.Second)
	t3 := now.Add(2 * time.Second)
	t4 := now.Add(3 * time.Second)
	t5 := now.Add(4 * time.Second)
	seed := []model.Room{
		{ID: "r1", Name: "one", Type: model.RoomTypeChannel, SiteID: "site-a", LastMsgAt: &t1},
		{ID: "r2", Name: "two", Type: model.RoomTypeChannel, SiteID: "site-a", LastMsgAt: &t2},
		{ID: "r3", Name: "three", Type: model.RoomTypeChannel, SiteID: "site-a", LastMsgAt: &t3},
		{ID: "r4", Name: "four", Type: model.RoomTypeChannel, SiteID: "site-a", LastMsgAt: &t4},
		{ID: "r5", Name: "five", Type: model.RoomTypeChannel, SiteID: "site-a", LastMsgAt: &t5},
	}
	for i := range seed {
		mustInsertRoom(t, db, &seed[i])
	}

	t.Run("returns matches and skips missing", func(t *testing.T) {
		rooms, err := store.ListRoomsByIDs(ctx, []string{"r1", "r3", "r5", "missing"})
		if err != nil {
			t.Fatalf("ListRoomsByIDs: %v", err)
		}
		if len(rooms) != 3 {
			t.Fatalf("got %d rooms, want 3", len(rooms))
		}
		byID := map[string]model.Room{}
		for _, r := range rooms {
			byID[r.ID] = r
		}
		for _, id := range []string{"r1", "r3", "r5"} {
			r, ok := byID[id]
			if !ok {
				t.Errorf("expected roomID %q in result", id)
				continue
			}
			if r.LastMsgAt == nil || r.LastMsgAt.IsZero() {
				t.Errorf("room %q: LastMsgAt is zero or nil", id)
			}
		}
	})

	t.Run("empty slice returns nil without error", func(t *testing.T) {
		rooms, err := store.ListRoomsByIDs(ctx, nil)
		if err != nil {
			t.Fatalf("ListRoomsByIDs(nil): %v", err)
		}
		if rooms != nil {
			t.Errorf("got %v, want nil", rooms)
		}
	})
}

func TestAddMembers_SameSiteChannel_RoomMembersPath(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db := setupMongo(t)
	keyStore := setupKeyStore(t, db)
	store := NewMongoStore(db)

	ctx := context.Background()

	// Target room on site-a
	mustInsertRoom(t, db, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"})
	// Source channel on site-a: seed room_members explicitly so ListRoomMembers takes the room_members
	// branch (not the subscriptions fallback); also seed users so ResolveAccounts can find them.
	mustInsertRoom(t, db, &model.Room{ID: "source", Type: model.RoomTypeChannel, SiteID: "site-a"})
	_, err := db.Collection("users").InsertMany(ctx, []interface{}{
		model.User{ID: "u1", Account: "bob", SiteID: "site-a"},
		model.User{ID: "u2", Account: "carol", SiteID: "site-a"},
		model.User{ID: "u3", Account: "dave", SiteID: "site-a", SectID: "eng-org"},
		model.User{ID: "req", Account: "alice", SiteID: "site-a"},
	})
	require.NoError(t, err)
	// room_members: two individuals + one org — exercises both branches in the RoomMember switch.
	_, err = db.Collection("room_members").InsertMany(ctx, []interface{}{
		model.RoomMember{ID: "rm1", RoomID: "source", Ts: time.Now().UTC(), Member: model.RoomMemberEntry{ID: "u1", Type: model.RoomMemberIndividual, Account: "bob"}},
		model.RoomMember{ID: "rm2", RoomID: "source", Ts: time.Now().UTC(), Member: model.RoomMemberEntry{ID: "u2", Type: model.RoomMemberIndividual, Account: "carol"}},
		model.RoomMember{ID: "rm3", RoomID: "source", Ts: time.Now().UTC(), Member: model.RoomMemberEntry{ID: "eng-org", Type: model.RoomMemberOrg}},
	})
	require.NoError(t, err)
	// Subscriptions: requester must be subscribed on both rooms; the source room's subscriptions
	// collection stays in sync with room_members so existing-subscription filtering works downstream.
	mustInsertSub(t, db, &model.Subscription{ID: "s1", RoomID: "source", User: model.SubscriptionUser{ID: "u1", Account: "bob"}})
	mustInsertSub(t, db, &model.Subscription{ID: "s2", RoomID: "source", User: model.SubscriptionUser{ID: "u2", Account: "carol"}})
	mustInsertSub(t, db, &model.Subscription{ID: "s3", RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}, Roles: []model.Role{model.RoleOwner}})
	mustInsertSub(t, db, &model.Subscription{ID: "s4", RoomID: "source", User: model.SubscriptionUser{ID: "req", Account: "alice"}})

	// Same-site only: pass nil for memberListClient — the same-site branch in
	// expandChannelRefs uses store.ListRoomMembers and never invokes the client.
	var publishedSubj string
	var publishedData []byte
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		publishedSubj = subj
		publishedData = data
		return nil
	}
	handler := NewHandler(store, keyStore, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, publish, func(context.Context, string, []byte) error { return nil }, nil, 0)

	req := model.AddMembersRequest{
		Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-a"}},
	}
	resp, err := handler.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "target"}), req)
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)

	// Verify the canonical event was published with the merged-but-unresolved members.
	// Source channel contributes: bob, carol (individuals) + eng-org (org).
	// Room-worker expands eng-org → dave at write time via ListNewMembers.
	assert.Equal(t, subject.RoomCanonical("site-a", "member.add"), publishedSubj)
	var normalized model.AddMembersRequest
	require.NoError(t, json.Unmarshal(publishedData, &normalized))
	assert.Equal(t, "target", normalized.RoomID)
	assert.Equal(t, "alice", normalized.RequesterAccount)
	assert.Equal(t, "req", normalized.RequesterID)
	assert.NotZero(t, normalized.Timestamp)
	assert.ElementsMatch(t, []string{"eng-org"}, normalized.Orgs)
	assert.ElementsMatch(t, []string{"bob", "carol"}, normalized.Users)
}

func TestAddMembers_SameSiteChannel_SubscriptionsFallback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db := setupMongo(t)
	keyStore := setupKeyStore(t, db)
	store := NewMongoStore(db)

	ctx := context.Background()

	mustInsertRoom(t, db, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"})
	mustInsertRoom(t, db, &model.Room{ID: "source", Type: model.RoomTypeChannel, SiteID: "site-a"})
	// Seed users so ResolveAccounts can find them.
	_, err := db.Collection("users").InsertMany(ctx, []interface{}{
		model.User{ID: "u1", Account: "bob", SiteID: "site-a"},
		model.User{ID: "u2", Account: "carol", SiteID: "site-a"},
		model.User{ID: "u3", Account: "dave", SiteID: "site-a"},
		model.User{ID: "req", Account: "alice", SiteID: "site-a"},
	})
	require.NoError(t, err)
	// Source only has subscriptions (no room_members rows) — ListRoomMembers falls back to subscriptions
	mustInsertSub(t, db, &model.Subscription{ID: "s1", RoomID: "source", User: model.SubscriptionUser{ID: "u1", Account: "bob"}})
	mustInsertSub(t, db, &model.Subscription{ID: "s2", RoomID: "source", User: model.SubscriptionUser{ID: "u2", Account: "carol"}})
	mustInsertSub(t, db, &model.Subscription{ID: "s3", RoomID: "source", User: model.SubscriptionUser{ID: "u3", Account: "dave"}})
	// Requester in both
	mustInsertSub(t, db, &model.Subscription{ID: "s4", RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}, Roles: []model.Role{model.RoleOwner}})
	mustInsertSub(t, db, &model.Subscription{ID: "s5", RoomID: "source", User: model.SubscriptionUser{ID: "req", Account: "alice"}})

	// Same-site only: nil memberListClient is safe (the same-site branch never invokes it).
	var publishedSubj string
	var publishedData []byte
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		publishedSubj = subj
		publishedData = data
		return nil
	}
	handler := NewHandler(store, keyStore, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, publish, func(context.Context, string, []byte) error { return nil }, nil, 0)

	req := model.AddMembersRequest{Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-a"}}}
	resp, err := handler.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "target"}), req)
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)

	// Verify the canonical event was published with the merged-but-unresolved members.
	// Source channel subscriptions: bob, carol, dave, alice (requester).
	// Already-subscribed filtering happens in room-worker via ListNewMembers, not here.
	assert.Equal(t, subject.RoomCanonical("site-a", "member.add"), publishedSubj)
	var normalized model.AddMembersRequest
	require.NoError(t, json.Unmarshal(publishedData, &normalized))
	assert.Equal(t, "target", normalized.RoomID)
	assert.Equal(t, "alice", normalized.RequesterAccount)
	assert.Equal(t, "req", normalized.RequesterID)
	assert.NotZero(t, normalized.Timestamp)
	assert.ElementsMatch(t, []string{"bob", "carol", "dave", "alice"}, normalized.Users)
}

func TestAddMembers_RequesterNotSubscribed_Rejected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db := setupMongo(t)
	keyStore := setupKeyStore(t, db)
	store := NewMongoStore(db)

	mustInsertRoom(t, db, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"})
	mustInsertRoom(t, db, &model.Room{ID: "source", Type: model.RoomTypeChannel, SiteID: "site-a"})
	// Requester subscribed to target but NOT source
	mustInsertSub(t, db, &model.Subscription{RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}, Roles: []model.Role{model.RoleOwner}})

	// Same-site only: nil memberListClient is safe — request fails on the same-site
	// GetSubscription check before reaching the cross-site branch.
	handler := NewHandler(store, keyStore, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, func(context.Context, string, []byte, string) error { return nil }, func(context.Context, string, []byte) error { return nil }, nil, 0)

	req := model.AddMembersRequest{Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-a"}}}
	_, err := handler.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "target"}), req)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errNotRoomMember))
}

func TestAddMembers_TwoSiteEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Two Mongo DBs (distinct prefixes since setupMongo hashes by t.Name()).
	// Only site-B needs a NATS server — site-A's handler talks straight to site-B
	// via the cross-site MemberListClient and uses an in-memory publish closure.
	dbA := testutil.MongoDB(t, "room_service_test_a")
	dbB := testutil.MongoDB(t, "room_service_test_b")
	natsURLb := setupNATS(t)
	keyStore := setupKeyStore(t, dbA)

	storeA := NewMongoStore(dbA)
	storeB := NewMongoStore(dbB)

	otelNCb, err := o11ynats.Connect(context.Background(), natsURLb, noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = otelNCb.Drain() })

	// ctxParams seeds a valid request_id on the *natsrouter.Context (forwarded
	// cross-site to site-B's strict RequireRequestID). This plain ctx is setup-only.
	ctx := context.Background()

	// Site-A: target room; requester subscribed; user document needed for ResolveAccounts.
	mustInsertRoom(t, dbA, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"})
	_, err = dbA.Collection("users").InsertMany(ctx, []interface{}{
		model.User{ID: "req", Account: "alice", SiteID: "site-a"},
		model.User{ID: "u1", Account: "bob", SiteID: "site-a"},
		model.User{ID: "u2", Account: "carol", SiteID: "site-a"},
	})
	require.NoError(t, err)
	mustInsertSub(t, dbA, &model.Subscription{ID: "sa1", RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}, Roles: []model.Role{model.RoleOwner}})

	// Site-B: source channel with members; requester subscribed on site-b too.
	mustInsertRoom(t, dbB, &model.Room{ID: "source", Type: model.RoomTypeChannel, SiteID: "site-b"})
	_, err = dbB.Collection("users").InsertMany(ctx, []interface{}{
		model.User{ID: "u1", Account: "bob", SiteID: "site-b"},
		model.User{ID: "u2", Account: "carol", SiteID: "site-b"},
		model.User{ID: "req", Account: "alice", SiteID: "site-b"},
	})
	require.NoError(t, err)
	mustInsertSub(t, dbB, &model.Subscription{ID: "sb1", RoomID: "source", User: model.SubscriptionUser{ID: "u1", Account: "bob"}})
	mustInsertSub(t, dbB, &model.Subscription{ID: "sb2", RoomID: "source", User: model.SubscriptionUser{ID: "u2", Account: "carol"}})
	mustInsertSub(t, dbB, &model.Subscription{ID: "sb3", RoomID: "source", User: model.SubscriptionUser{ID: "req", Account: "alice"}})

	// Site-B handler registers member.list endpoint (Register subscribes to MemberListWildcard).
	handlerB := NewHandler(storeB, keyStore, nil, nil, "site-b", 1000, 500, 5*time.Second, 5, func(context.Context, string, []byte, string) error { return nil }, func(context.Context, string, []byte) error { return nil }, nil, 0)
	routerB := natsrouter.New(otelNCb, "room-service")
	routerB.Use(natsrouter.RequireRequestID())
	handlerB.Register(routerB)
	t.Cleanup(func() { _ = routerB.Shutdown(context.Background()) })
	require.NoError(t, otelNCb.NatsConn().Flush())

	// Site-A's cross-site client: connect a plain nats.Conn directly to site-B's server.
	// In production NATS gateways handle this routing; for this test we bypass gateways
	// and connect the client directly — the subject and request/reply wiring is the same.
	ncBfromA, err := nats.Connect(natsURLb)
	require.NoError(t, err)
	t.Cleanup(func() { ncBfromA.Close() })
	memberListClient := NewNATSMemberListClient(ncBfromA, 5*time.Second)

	// Capture what site-A publishes to its canonical stream
	var publishedSubj string
	var publishedData []byte
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		publishedSubj = subj
		publishedData = data
		return nil
	}
	handlerA := NewHandler(storeA, keyStore, memberListClient, nil, "site-a", 1000, 500, 5*time.Second, 5, publish, func(context.Context, string, []byte) error { return nil }, nil, 0)

	// Call add-members on site-A with a site-B source channel
	req := model.AddMembersRequest{Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-b"}}}
	resp, err := handlerA.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "target"}), req)
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)

	// Verify the canonical event has site-B members (bob, carol, alice).
	// Already-subscribed filtering (alice on target) happens in room-worker via
	// ListNewMembers, not at the room-service stage.
	assert.Equal(t, subject.RoomCanonical("site-a", "member.add"), publishedSubj)
	var normalized model.AddMembersRequest
	require.NoError(t, json.Unmarshal(publishedData, &normalized))
	assert.Equal(t, "target", normalized.RoomID)
	assert.Contains(t, normalized.Users, "bob")
	assert.Contains(t, normalized.Users, "carol")
	assert.Contains(t, normalized.Users, "alice")
}

func TestAddMembers_CrossSiteTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db := setupMongo(t)
	natsURL := setupNATS(t)
	keyStore := setupKeyStore(t, db)
	store := NewMongoStore(db)
	otelNC, err := o11ynats.Connect(context.Background(), natsURL, noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = otelNC.Drain() })

	// Target on site-a, requester subscribed.
	mustInsertRoom(t, db, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"})
	mustInsertSub(t, db, &model.Subscription{ID: "s1", RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}, Roles: []model.Role{model.RoleOwner}})

	// Register a site-b responder that sleeps past the client timeout, so we actually
	// exercise the context.WithTimeout path (not NATS "no responders" fast-fail).
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { nc.Close() })
	// Sleep just past the 200ms client timeout so the responder doesn't outlive the test
	// and flag as a goroutine leak under -race/leak detectors.
	// Subscriber that intentionally never replies — exercises the client-side
	// timeout path without time.Sleep coordination (CLAUDE.md forbids sleep
	// for goroutine sync). t.Cleanup unsubscribes so the responder doesn't
	// outlive the test.
	sub, err := nc.Subscribe(subject.MemberList("alice", "source", "site-b"), func(_ *nats.Msg) {})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	memberListClient := NewNATSMemberListClient(nc, 200*time.Millisecond)
	handler := NewHandler(store, keyStore, memberListClient, nil, "site-a", 1000, 500, 200*time.Millisecond, 5, func(context.Context, string, []byte, string) error { return nil }, func(context.Context, string, []byte) error { return nil }, nil, 0)

	req := model.AddMembersRequest{Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-b"}}}
	_, err = handler.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "target"}), req)
	require.Error(t, err)
	// Cross-site member.list deadline → Unavailable errcode naming the offending
	// site+roomId so the requester can see which channel source stalled.
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee, "expected *errcode.Error, got %v", err)
	assert.Equal(t, errcode.CodeUnavailable, ee.Code)
	assert.Equal(t, "timeout listing members of channel source@site-b", ee.Message)
}

// TestRoomsInfoBatchRPC_NoRequestID proves the relaxed posture: the base
// middleware mints an X-Request-ID when absent (RequestID, not RequireRequestID),
// so a header-less server-to-server call succeeds instead of being rejected. The
// RoomsInfoBatch read RPC is the representative deterministic route; the minting
// middleware applies to every room-service handler.
func TestRoomsInfoBatchRPC_NoRequestID(t *testing.T) {
	db := setupMongo(t)
	keyStore := setupKeyStore(t, db)
	natsURL := setupNATS(t)

	store := NewMongoStore(db)

	mustInsertRoom(t, db, &model.Room{ID: "r1", Name: "room-1", Type: model.RoomTypeChannel, SiteID: "site-a"})

	otelNC, err := o11ynats.Connect(context.Background(), natsURL, noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = otelNC.Drain() })

	handler := NewHandler(store, keyStore, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, func(context.Context, string, []byte, string) error { return nil }, func(context.Context, string, []byte) error { return nil }, nil, 0)
	router := natsrouter.New(otelNC, "room-service")
	// Production-shaped base: mint, do not require.
	router.Use(natsrouter.Recovery(), natsrouter.RequestID(), natsrouter.Logging())
	handler.Register(router)
	t.Cleanup(func() { _ = router.Shutdown(context.Background()) })
	require.NoError(t, otelNC.NatsConn().Flush())

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { nc.Drain() })

	ctxReq, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Header-less request succeeds — the middleware mints an ID instead of rejecting.
	batchData, err := json.Marshal(model.RoomsInfoBatchRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	msg, err := nc.RequestWithContext(ctxReq, subject.RoomsInfoBatch("site-a"), batchData)
	require.NoError(t, err, "RoomsInfoBatch must answer a header-less request")
	var resp model.RoomsInfoBatchResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp))
	require.Len(t, resp.Rooms, 1)
	assert.Equal(t, "r1", resp.Rooms[0].RoomID)
	assert.True(t, resp.Rooms[0].Found)
}

func TestRoomsInfoBatchRPC(t *testing.T) {
	db := setupMongo(t)
	keyStore := setupKeyStore(t, db)
	natsURL := setupNATS(t)

	store := NewMongoStore(db)
	ctx := context.Background()

	lastMsg := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	earlier := lastMsg.Add(-time.Hour)
	rooms := []model.Room{
		{ID: "r1", Name: "room-1", Type: model.RoomTypeChannel, SiteID: "site-a", LastMsgAt: &lastMsg},
		{ID: "r2", Name: "room-2", Type: model.RoomTypeChannel, SiteID: "site-a"},
		{ID: "r3", Name: "room-3", Type: model.RoomTypeChannel, SiteID: "site-a", LastMsgAt: &earlier},
	}
	for i := range rooms {
		mustInsertRoom(t, db, &rooms[i])
	}

	privKey1 := bytes.Repeat([]byte{0x01}, 32)
	privKey2 := bytes.Repeat([]byte{0x02}, 32)
	_, err := keyStore.Set(ctx, "r1", roomkeystore.RoomKeyPair{PrivateKey: privKey1})
	require.NoError(t, err)
	_, err = keyStore.Set(ctx, "r2", roomkeystore.RoomKeyPair{PrivateKey: privKey2})
	require.NoError(t, err)

	otelNC, err := o11ynats.Connect(context.Background(), natsURL, noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = otelNC.Drain() })

	handler := NewHandler(store, keyStore, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, func(context.Context, string, []byte, string) error { return nil }, func(context.Context, string, []byte) error { return nil }, nil, 0)
	router := natsrouter.New(otelNC, "room-service")
	router.Use(natsrouter.RequireRequestID())
	handler.Register(router)
	t.Cleanup(func() { _ = router.Shutdown(context.Background()) })
	require.NoError(t, otelNC.NatsConn().Flush())

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { nc.Drain() })

	req := model.RoomsInfoBatchRequest{RoomIDs: []string{"r1", "r2", "r3", "missing"}}
	data, err := json.Marshal(req)
	require.NoError(t, err)

	ctxReq, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	reqMsg := natsutil.NewMsg(natsutil.WithRequestID(ctxReq, idgen.GenerateRequestID()),
		subject.RoomsInfoBatch("site-a"), data)
	msg, err := nc.RequestMsgWithContext(ctxReq, reqMsg)
	require.NoError(t, err)

	var resp model.RoomsInfoBatchResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp))
	require.Len(t, resp.Rooms, 4)

	assert.Equal(t, "r1", resp.Rooms[0].RoomID)
	assert.True(t, resp.Rooms[0].Found)
	require.NotNil(t, resp.Rooms[0].LastMsgAt)
	assert.Equal(t, lastMsg.UnixMilli(), *resp.Rooms[0].LastMsgAt)
	require.NotNil(t, resp.Rooms[0].PrivateKey)
	assert.Equal(t, base64.StdEncoding.EncodeToString(privKey1), *resp.Rooms[0].PrivateKey)
	require.NotNil(t, resp.Rooms[0].KeyVersion)
	assert.Equal(t, 0, *resp.Rooms[0].KeyVersion)

	assert.Equal(t, "r2", resp.Rooms[1].RoomID)
	assert.True(t, resp.Rooms[1].Found)
	assert.Nil(t, resp.Rooms[1].LastMsgAt)
	require.NotNil(t, resp.Rooms[1].PrivateKey)
	assert.Equal(t, base64.StdEncoding.EncodeToString(privKey2), *resp.Rooms[1].PrivateKey)

	assert.Equal(t, "r3", resp.Rooms[2].RoomID)
	assert.True(t, resp.Rooms[2].Found)
	assert.Nil(t, resp.Rooms[2].PrivateKey)
	assert.Nil(t, resp.Rooms[2].KeyVersion)

	assert.Equal(t, "missing", resp.Rooms[3].RoomID)
	assert.False(t, resp.Rooms[3].Found)
	assert.Nil(t, resp.Rooms[3].LastMsgAt)
	assert.Nil(t, resp.Rooms[3].PrivateKey)
	assert.Nil(t, resp.Rooms[3].KeyVersion)
}

// TestIntegration_HandleGetRoomKey verifies the client-callable room key get
// RPC end-to-end against real Mongo: a member fetches both the current
// and an explicit version of the key; a non-member is rejected via the
// errNotRoomMember sentinel.
func TestIntegration_HandleGetRoomKey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	keyStore := setupKeyStore(t, db)
	h, _ := newRoomServiceHandler(t, store, keyStore, "site-A")

	const roomID = "room-int"
	// The key is a field of the room document, so the room must exist first.
	mustInsertRoom(t, db, &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-A"})
	// alice is a member; bob is not.
	mustInsertSub(t, db, &model.Subscription{
		ID:     idgen.GenerateUUIDv7(),
		RoomID: roomID,
		SiteID: "site-A",
		User:   model.SubscriptionUser{ID: "u_alice", Account: "alice"},
		Name:   "alice",
	})

	pair := roomkeystore.RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0xAA}, 32)}
	ver, err := keyStore.Set(ctx, roomID, pair)
	require.NoError(t, err)

	// 1. Member fetches current key.
	{
		body, _ := json.Marshal(model.RoomKeyGetRequest{})
		got, err := h.getRoomKey(roomKeyGetCtx(ctx, "alice", roomID, body))
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, roomID, got.RoomID)
		assert.Equal(t, ver, got.Version)
		assert.Equal(t, pair.PrivateKey, got.PrivateKey)
	}

	// 2. Member fetches by explicit version.
	{
		v := ver
		body, _ := json.Marshal(model.RoomKeyGetRequest{Version: &v})
		got, err := h.getRoomKey(roomKeyGetCtx(ctx, "alice", roomID, body))
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, ver, got.Version)
		assert.Equal(t, pair.PrivateKey, got.PrivateKey)
	}

	// 3. Non-member rejected.
	{
		body, _ := json.Marshal(model.RoomKeyGetRequest{})
		_, err := h.getRoomKey(roomKeyGetCtx(ctx, "bob", roomID, body))
		// errNotRoomMember (a typed *errcode.Error) is returned and surfaced for
		// clients via errnats.Reply; assert on identity, not the message text.
		require.ErrorIs(t, err, errNotRoomMember)
	}
}

// roomKeyGetCtx builds a *natsrouter.Context for the getRoomKey handler with the
// account/roomID params and request body the handler reads from c.Msg.Data.
func roomKeyGetCtx(ctx context.Context, account, roomID string, body []byte) *natsrouter.Context {
	c := natsrouter.NewContext(map[string]string{"account": account, "roomID": roomID})
	c.SetContext(ctx)
	c.Msg = &nats.Msg{Data: body}
	return c
}

// mustInsertUser inserts a user document directly into the users collection.
func mustInsertUser(t *testing.T, db *mongo.Database, u *model.User) {
	t.Helper()
	_, err := db.Collection("users").InsertOne(context.Background(), u)
	require.NoError(t, err)
}

// mustInsertRoom inserts a room document directly into the rooms collection.
func mustInsertRoom(t *testing.T, db *mongo.Database, r *model.Room) {
	t.Helper()
	_, err := db.Collection("rooms").InsertOne(context.Background(), r)
	require.NoError(t, err)
}

// mustInsertSub inserts a subscription document directly into the subscriptions collection.
func mustInsertSub(t *testing.T, db *mongo.Database, sub *model.Subscription) {
	t.Helper()
	_, err := db.Collection("subscriptions").InsertOne(context.Background(), sub)
	require.NoError(t, err)
}

// newRoomServiceHandler wires a Handler with a capture closure for published events.
// Returns the handler and a func that returns the most recently published (subject, data).
func newRoomServiceHandler(t *testing.T, store *MongoStore, keyStore RoomKeyStore, siteID string) (*Handler, func() (string, []byte)) {
	t.Helper()
	var lastSubj string
	var lastData []byte
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		lastSubj = subj
		lastData = data
		return nil
	}
	h := NewHandler(store, keyStore, nil, nil, siteID, 1000, 500, 5*time.Second, 5, publish, func(context.Context, string, []byte) error { return nil }, nil, 0)
	return h, func() (string, []byte) { return lastSubj, lastData }
}

func TestCreateRoomChannelEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	keyStore := setupKeyStore(t, db)

	mustInsertUser(t, db, &model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-A",
		EngName: "Alice", ChineseName: "爱丽丝",
	})
	mustInsertUser(t, db, &model.User{
		ID: "u_bob", Account: "bob", SiteID: "site-A",
		EngName: "Bob", ChineseName: "鲍勃",
	})

	h, published := newRoomServiceHandler(t, store, keyStore, "site-A")

	reqID := idgen.GenerateRequestID()
	ctx = natsutil.WithRequestID(ctx, reqID)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	c.SetContext(ctx)

	got, err := h.createRoom(c, model.CreateRoomRequest{
		Name:  "deal team",
		Users: []string{"bob"},
	})
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, model.CreateRoomReplyAccepted, got.Status)
	assert.Equal(t, "channel", got.RoomType)
	assert.NotEmpty(t, got.RoomID)

	publishedSubj, publishedData := published()
	assert.Equal(t, subject.RoomCanonical("site-A", "create"), publishedSubj)
	var canonical model.CreateRoomRequest
	require.NoError(t, json.Unmarshal(publishedData, &canonical))
	assert.Equal(t, got.RoomID, canonical.RoomID)
	assert.Equal(t, "alice", canonical.RequesterAccount)
}

func TestCreateRoomDMAlreadyExists(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	keyStore := setupKeyStore(t, db)

	mustInsertUser(t, db, &model.User{ID: "u_alice", Account: "alice",
		EngName: "Alice", ChineseName: "爱丽丝", SiteID: "site-A"})
	mustInsertUser(t, db, &model.User{ID: "u_bob", Account: "bob",
		EngName: "Bob", ChineseName: "鲍勃", SiteID: "site-A"})

	roomID := idgen.BuildDMRoomID("u_alice", "u_bob")
	mustInsertRoom(t, db, &model.Room{ID: roomID, Type: model.RoomTypeDM, SiteID: "site-A"})
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: "site-A",
		User: model.SubscriptionUser{ID: "u_alice", Account: "alice"},
		Name: "bob", RoomType: model.RoomTypeDM,
	})

	h, _ := newRoomServiceHandler(t, store, keyStore, "site-A")

	reqID := idgen.GenerateRequestID()
	ctx = natsutil.WithRequestID(ctx, reqID)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	c.SetContext(ctx)

	reply, herr := h.createRoom(c, model.CreateRoomRequest{Users: []string{"bob"}})
	require.NoError(t, herr)
	require.NotNil(t, reply)
	assert.Equal(t, model.CreateRoomStatusExists, reply.Status)
	assert.Equal(t, roomID, reply.RoomID)
}

func TestMongoStore_UpdateSubscriptionRead_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	mustInsertSub(t, db, &model.Subscription{
		ID:       "s1",
		User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:   "r1",
		SiteID:   "site-a",
		JoinedAt: time.Now().UTC().Add(-time.Hour),
		Alert:    true,
	})

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

// Regression for #447: room read must clear hasMention (the thread mirror already did).
// Reads the raw doc, not GetSubscription — that projection omits hasMention (would false-pass).
func TestMongoStore_UpdateSubscriptionRead_ClearsHasMention(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	mustInsertSub(t, db, &model.Subscription{
		ID:         "s1",
		User:       model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:     "r1",
		SiteID:     "site-a",
		JoinedAt:   time.Now().UTC().Add(-time.Hour),
		HasMention: true,
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, store.UpdateSubscriptionRead(ctx, "r1", "alice", now, false))

	var raw model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "s1"}).Decode(&raw))
	assert.False(t, raw.HasMention)
	require.NotNil(t, raw.LastSeenAt)
	assert.WithinDuration(t, now, *raw.LastSeenAt, time.Second)
}

func TestMongoStore_GetUserSiteID_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

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

func TestMongoStore_MinSubscriptionLastSeenByRoomID_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	earliest := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	mid := earliest.Add(15 * time.Minute)
	latest := earliest.Add(45 * time.Minute)

	// Room "all-read": every subscription has a usable lastSeenAt, so the floor
	// is the MIN across all of them.
	mustInsertSub(t, db, &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "all-read", JoinedAt: earliest, LastSeenAt: &mid,
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"},
		RoomID: "all-read", JoinedAt: earliest, LastSeenAt: &latest,
	})

	got, err := store.MinSubscriptionLastSeenByRoomID(ctx, "all-read")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.WithinDuration(t, mid, *got, time.Second)

	// Room "one-unread": two members have read, one was invited but has never
	// opened the room. Under the strict floor a single never-read member forces
	// nil — "not everyone has read".
	mustInsertSub(t, db, &model.Subscription{
		ID: "s3", User: model.SubscriptionUser{ID: "u3", Account: "carol"},
		RoomID: "one-unread", JoinedAt: earliest, LastSeenAt: &mid,
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: "s4", User: model.SubscriptionUser{ID: "u4", Account: "dave"},
		RoomID: "one-unread", JoinedAt: earliest, LastSeenAt: &latest,
	})
	// Never-read sub: joined but never opened the room.
	mustInsertSub(t, db, &model.Subscription{
		ID: "s5", User: model.SubscriptionUser{ID: "u5", Account: "erin"},
		RoomID: "one-unread", JoinedAt: earliest,
	})

	got, err = store.MinSubscriptionLastSeenByRoomID(ctx, "one-unread")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Room "none-read": a single sub that has never been read → nil.
	mustInsertSub(t, db, &model.Subscription{
		ID: "s6", User: model.SubscriptionUser{ID: "u6", Account: "frank"},
		RoomID: "none-read", JoinedAt: earliest,
	})
	got, err = store.MinSubscriptionLastSeenByRoomID(ctx, "none-read")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Room "legacy-zero": a sub carrying the BSON zero-date (a legacy document
	// whose lastSeenAt was written as the zero time rather than omitted) counts
	// as unread just like a missing field → nil.
	zeroTime := time.Time{}
	mustInsertSub(t, db, &model.Subscription{
		ID: "s7", User: model.SubscriptionUser{ID: "u7", Account: "grace"},
		RoomID: "legacy-zero", JoinedAt: earliest, LastSeenAt: &zeroTime,
	})
	got, err = store.MinSubscriptionLastSeenByRoomID(ctx, "legacy-zero")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Room "botdm": a human who has read plus a bot that never reads. Bots are
	// not special-cased — the bot counts as an unread member, so a botDM room
	// always resolves to nil even though the human is fully caught up.
	mustInsertSub(t, db, &model.Subscription{
		ID: "s8", User: model.SubscriptionUser{ID: "u8", Account: "heidi"},
		RoomID: "botdm", JoinedAt: earliest, LastSeenAt: &latest,
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: "s9", User: model.SubscriptionUser{ID: "bot1", Account: "assistant", IsBot: true},
		RoomID: "botdm", JoinedAt: earliest,
	})
	got, err = store.MinSubscriptionLastSeenByRoomID(ctx, "botdm")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Room with no subscriptions at all → nil.
	got, err = store.MinSubscriptionLastSeenByRoomID(ctx, "empty")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestMongoStore_UpdateRoomMinUserLastSeenAt_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	now := time.Now().UTC().Truncate(time.Millisecond)
	mustInsertRoom(t, db, &model.Room{
		ID: "r1", Name: "x", Type: model.RoomTypeChannel, CreatedAt: now, UpdatedAt: now,
	})

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

func mustInsertThreadSub(t *testing.T, db *mongo.Database, ts *model.ThreadSubscription) {
	t.Helper()
	_, err := db.Collection("thread_subscriptions").InsertOne(context.Background(), ts)
	require.NoError(t, err)
}

func mustInsertThreadRoom(t *testing.T, db *mongo.Database, tr *model.ThreadRoom) {
	t.Helper()
	_, err := db.Collection("thread_rooms").InsertOne(context.Background(), tr)
	require.NoError(t, err)
}

func TestMongoStore_MinThreadSubscriptionLastSeenByThreadRoomID_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	earliest := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	mid := earliest.Add(15 * time.Minute)
	latest := earliest.Add(45 * time.Minute)

	// "all-read": every subscriber has read → floor is the minimum.
	mustInsertThreadSub(t, db, &model.ThreadSubscription{ID: "ts1", ThreadRoomID: "all-read", UserAccount: "alice", LastSeenAt: &mid})
	mustInsertThreadSub(t, db, &model.ThreadSubscription{ID: "ts2", ThreadRoomID: "all-read", UserAccount: "bob", LastSeenAt: &latest})
	got, err := store.MinThreadSubscriptionLastSeenByThreadRoomID(ctx, "all-read")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.WithinDuration(t, mid, *got, time.Second)

	// "one-unread": one subscriber has never read → nil.
	mustInsertThreadSub(t, db, &model.ThreadSubscription{ID: "ts3", ThreadRoomID: "one-unread", UserAccount: "carol", LastSeenAt: &mid})
	mustInsertThreadSub(t, db, &model.ThreadSubscription{ID: "ts4", ThreadRoomID: "one-unread", UserAccount: "dave"})
	got, err = store.MinThreadSubscriptionLastSeenByThreadRoomID(ctx, "one-unread")
	require.NoError(t, err)
	assert.Nil(t, got)

	// "none-read": single subscriber who has never read → nil.
	mustInsertThreadSub(t, db, &model.ThreadSubscription{ID: "ts5", ThreadRoomID: "none-read", UserAccount: "erin"})
	got, err = store.MinThreadSubscriptionLastSeenByThreadRoomID(ctx, "none-read")
	require.NoError(t, err)
	assert.Nil(t, got)

	// "no-subs": no subscriptions at all → nil.
	got, err = store.MinThreadSubscriptionLastSeenByThreadRoomID(ctx, "no-subs")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestMongoStore_UpdateThreadRoomMinUserLastSeenAt_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	now := time.Now().UTC().Truncate(time.Millisecond)
	mustInsertThreadRoom(t, db, &model.ThreadRoom{
		ID: "tr1", RoomID: "r1", LastMsgAt: now, CreatedAt: now, UpdatedAt: now,
	})

	// Set the floor.
	require.NoError(t, store.UpdateThreadRoomMinUserLastSeenAt(ctx, "tr1", &now))
	tr, err := store.GetThreadRoomByID(ctx, "tr1")
	require.NoError(t, err)
	require.NotNil(t, tr)
	require.NotNil(t, tr.MinUserLastSeenAt)
	assert.WithinDuration(t, now, *tr.MinUserLastSeenAt, time.Second)

	// Clear the floor.
	require.NoError(t, store.UpdateThreadRoomMinUserLastSeenAt(ctx, "tr1", nil))
	tr, err = store.GetThreadRoomByID(ctx, "tr1")
	require.NoError(t, err)
	require.NotNil(t, tr)
	assert.Nil(t, tr.MinUserLastSeenAt)
}

func TestMongoStore_GetThreadRoomInfos(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	store := NewMongoStore(db)

	lastMsg := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	mustInsertThreadRoom(t, db, &model.ThreadRoom{ID: "tr1", RoomID: "room1", LastMsgAt: lastMsg})

	rows, err := store.GetThreadRoomInfos(ctx, []string{"tr1", "missing"})
	require.NoError(t, err)
	require.Len(t, rows, 1) // "missing" is absent, not an error
	assert.Equal(t, "tr1", rows[0].ThreadRoomID)
	assert.Equal(t, lastMsg.UTC().UnixMilli(), rows[0].LastMsgAt.UTC().UnixMilli())
}

func TestMongoStore_CountNewMembers_OrgOnlyUserCountsZero_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	const roomID = "room-1"
	mustInsertUser(t, db, &model.User{ID: "u_alice", Account: "alice", SectID: "org-eng", SiteID: "site-a"})
	// Alice already has a subscription via org-eng — adding her individually should add 0 new subs.
	mustInsertSub(t, db, &model.Subscription{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: "site-a",
		User:     model.SubscriptionUser{ID: "u_alice", Account: "alice"},
		RoomType: model.RoomTypeChannel,
	})

	n, err := store.CountNewMembers(ctx, nil, []string{"alice"}, roomID, "")
	require.NoError(t, err)
	assert.Equal(t, 0, n, "alice already has a sub via org — capacity unchanged")
}

func TestMongoStore_ListReadReceipts_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	_, err := db.Collection("users").InsertMany(ctx, []any{
		bson.M{"_id": "uA", "account": "alice", "chineseName": "愛麗絲", "engName": "Alice"},
		bson.M{"_id": "uB", "account": "bob", "chineseName": "鮑勃", "engName": "Bob"},
		bson.M{"_id": "uC", "account": "carol", "chineseName": "卡羅", "engName": "Carol"},
	})
	require.NoError(t, err)

	msgTime := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "sA", "roomId": "r1", "u": bson.M{"_id": "uA", "account": "alice"}, "lastSeenAt": msgTime.Add(time.Hour)},
		bson.M{"_id": "sB", "roomId": "r1", "u": bson.M{"_id": "uB", "account": "bob"}, "lastSeenAt": msgTime.Add(time.Minute)},
		bson.M{"_id": "sC", "roomId": "r1", "u": bson.M{"_id": "uC", "account": "carol"}, "lastSeenAt": msgTime.Add(-time.Minute)},
	})
	require.NoError(t, err)

	rows, err := store.ListReadReceipts(ctx, "r1", msgTime, "alice", 100)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "uB", rows[0].UserID)
	require.Equal(t, "bob", rows[0].Account)
	require.Equal(t, "鮑勃", rows[0].ChineseName)
	require.Equal(t, "Bob", rows[0].EngName)

	rows, err = store.ListReadReceipts(ctx, "r1", msgTime.Add(2*time.Hour), "alice", 100)
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestMongoStore_ListThreadReadReceipts_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	_, err := db.Collection("users").InsertMany(ctx, []any{
		bson.M{"_id": "uA", "account": "alice", "chineseName": "愛麗絲", "engName": "Alice"},
		bson.M{"_id": "uB", "account": "bob", "chineseName": "鮑勃", "engName": "Bob"},
		bson.M{"_id": "uC", "account": "carol", "chineseName": "卡羅", "engName": "Carol"},
	})
	require.NoError(t, err)

	// thread_subscriptions store userAccount/userId flat (no embedded "u"). Only
	// bob's thread lastSeenAt is at/after the reply; carol's is before; alice is the sender.
	msgTime := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	_, err = db.Collection("thread_subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "tsA", "threadRoomId": "tr1", "userId": "uA", "userAccount": "alice", "lastSeenAt": msgTime.Add(time.Hour)},
		bson.M{"_id": "tsB", "threadRoomId": "tr1", "userId": "uB", "userAccount": "bob", "lastSeenAt": msgTime.Add(time.Minute)},
		bson.M{"_id": "tsC", "threadRoomId": "tr1", "userId": "uC", "userAccount": "carol", "lastSeenAt": msgTime.Add(-time.Minute)},
	})
	require.NoError(t, err)

	rows, err := store.ListThreadReadReceipts(ctx, "tr1", msgTime, "alice", 100)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "uB", rows[0].UserID)
	require.Equal(t, "bob", rows[0].Account)
	require.Equal(t, "鮑勃", rows[0].ChineseName)
	require.Equal(t, "Bob", rows[0].EngName)

	rows, err = store.ListThreadReadReceipts(ctx, "tr1", msgTime.Add(2*time.Hour), "alice", 100)
	require.NoError(t, err)
	require.Empty(t, rows)
}

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

	t.Run("removes specified threadID and returns remaining", func(t *testing.T) {
		newUnread, newAlert, err := store.UpdateSubscriptionThreadRead(ctx, "r1", "alice", "t1")
		require.NoError(t, err)
		assert.Equal(t, []string{"t2"}, newUnread)
		assert.True(t, newAlert)
		var got model.Subscription
		require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sub-1"}).Decode(&got))
		assert.Equal(t, []string{"t2"}, got.ThreadUnread)
		assert.True(t, got.Alert)
	})

	t.Run("last element removed unsets threadUnread field and clears alert", func(t *testing.T) {
		newUnread, newAlert, err := store.UpdateSubscriptionThreadRead(ctx, "r1", "alice", "t2")
		require.NoError(t, err)
		assert.Nil(t, newUnread)
		assert.False(t, newAlert)
		var raw bson.M
		require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sub-1"}).Decode(&raw))
		_, present := raw["threadUnread"]
		assert.False(t, present, "threadUnread must be $unset, not stored as empty array")
		assert.Equal(t, false, raw["alert"])
	})

	t.Run("missing subscription returns sentinel", func(t *testing.T) {
		_, _, err := store.UpdateSubscriptionThreadRead(ctx, "r-missing", "alice", "t1")
		require.ErrorIs(t, err, model.ErrSubscriptionNotFound)
	})

	t.Run("concurrent removals do not lose updates", func(t *testing.T) {
		// Reset subscription to ["c1", "c2"] with alert=true
		_, err := db.Collection("subscriptions").UpdateOne(ctx, bson.M{"_id": "sub-1"},
			bson.M{"$set": bson.M{"threadUnread": []string{"c1", "c2"}, "alert": true}})
		require.NoError(t, err)

		// Two concurrent calls each remove a different threadID
		done := make(chan error, 2)
		go func() {
			_, _, err := store.UpdateSubscriptionThreadRead(ctx, "r1", "alice", "c1")
			done <- err
		}()
		go func() {
			_, _, err := store.UpdateSubscriptionThreadRead(ctx, "r1", "alice", "c2")
			done <- err
		}()
		require.NoError(t, <-done)
		require.NoError(t, <-done)

		// Both removals must have applied — threadUnread should be absent (empty)
		var raw bson.M
		require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sub-1"}).Decode(&raw))
		_, present := raw["threadUnread"]
		assert.False(t, present, "both concurrent removals must apply — no lost updates")
		assert.Equal(t, false, raw["alert"])
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

func TestMongoStore_ToggleSubscriptionMute(t *testing.T) {
	db := testutil.MongoDB(t, "room-svc-mute")
	store := NewMongoStore(db)
	ctx := context.Background()

	sub := &model.Subscription{
		ID:       idgen.GenerateUUIDv7(),
		User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:   "r1",
		RoomType: model.RoomTypeChannel,
		SiteID:   "site-a",
		Roles:    []model.Role{model.RoleMember},
		JoinedAt: time.Now().UTC(),
		Muted:    false,
	}
	mustInsertSub(t, db, sub)

	ts1 := time.Now().UTC()
	got, err := store.ToggleSubscriptionMute(ctx, "r1", "alice", ts1)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.Muted)
	assert.Equal(t, "alice", got.User.Account)
	assert.Equal(t, "r1", got.RoomID)

	// muted is not projected by GetSubscription (no handler reads it from a read
	// result), so verify persistence with a direct field read.
	assert.True(t, subBoolField(t, db, "r1", "alice", "muted"))
	// muteUpdatedAt is stamped at the supplied instant (BSON ms precision) so the
	// origin doc shares the federated event's high-water mark.
	assert.Equal(t, ts1.UnixMilli(), subTimeField(t, db, "r1", "alice", "muteUpdatedAt").UnixMilli())

	ts2 := ts1.Add(time.Second)
	got, err = store.ToggleSubscriptionMute(ctx, "r1", "alice", ts2)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.Muted)
	assert.Equal(t, "alice", got.User.Account)
	assert.Equal(t, "r1", got.RoomID)
	assert.Equal(t, ts2.UnixMilli(), subTimeField(t, db, "r1", "alice", "muteUpdatedAt").UnixMilli())

	gotNil, err := store.ToggleSubscriptionMute(ctx, "missing", "alice", time.Now().UTC())
	assert.Nil(t, gotNil)
	assert.ErrorIs(t, err, model.ErrSubscriptionNotFound)
}

// subTimeField reads a single time.Time field off the (roomID, account) subscription
// doc — used to assert order-guard timestamps the model.Subscription struct doesn't carry.
func subTimeField(t *testing.T, db *mongo.Database, roomID, account, field string) time.Time {
	t.Helper()
	var doc bson.M
	require.NoError(t, db.Collection("subscriptions").
		FindOne(context.Background(), bson.M{"roomId": roomID, "u.account": account}).Decode(&doc))
	v, ok := doc[field]
	require.True(t, ok, "field %q missing on subscription", field)
	dt, ok := v.(bson.DateTime)
	require.True(t, ok, "field %q is %T, want bson.DateTime", field, v)
	return dt.Time().UTC()
}

// subBoolField reads a boolean field straight from the stored subscription
// document. Used to assert persistence of fields (muted, favorite) that
// GetSubscription deliberately does not project, so the read-back cannot rely
// on the projected struct.
func subBoolField(t *testing.T, db *mongo.Database, roomID, account, field string) bool {
	t.Helper()
	var doc bson.M
	require.NoError(t, db.Collection("subscriptions").
		FindOne(context.Background(), bson.M{"roomId": roomID, "u.account": account}).Decode(&doc))
	v, ok := doc[field]
	require.True(t, ok, "field %q missing on subscription", field)
	b, ok := v.(bool)
	require.True(t, ok, "field %q is %T, want bool", field, v)
	return b
}

func TestMongoStore_ToggleSubscriptionFavorite(t *testing.T) {
	db := testutil.MongoDB(t, "room-svc-fav")
	store := NewMongoStore(db)
	ctx := context.Background()

	// Insert a sub via raw BSON without a "favorite" field at all — proves the
	// $ifNull branch handles legacy docs and toggles missing→true on first call.
	rawSub := bson.M{
		"_id":      idgen.GenerateUUIDv7(),
		"u":        bson.M{"_id": "u1", "account": "alice", "isBot": false},
		"roomId":   "r1",
		"roomType": model.RoomTypeChannel,
		"siteId":   "site-a",
		"roles":    []model.Role{model.RoleMember},
		"joinedAt": time.Now().UTC(),
		"muted":    false,
		// no "favorite" key on purpose
	}
	_, err := db.Collection("subscriptions").InsertOne(ctx, rawSub)
	require.NoError(t, err)

	ts1 := time.Now().UTC()
	got, err := store.ToggleSubscriptionFavorite(ctx, "r1", "alice", ts1)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.Favorite, "first toggle on legacy doc must flip missing→true")
	assert.Equal(t, "alice", got.User.Account)
	assert.Equal(t, "r1", got.RoomID)

	// favorite is not projected by GetSubscription (no handler reads it from a
	// read result), so verify persistence with a direct field read.
	assert.True(t, subBoolField(t, db, "r1", "alice", "favorite"))
	// favoriteUpdatedAt is stamped at the supplied instant so the origin doc
	// shares the federated event's high-water mark.
	assert.Equal(t, ts1.UnixMilli(), subTimeField(t, db, "r1", "alice", "favoriteUpdatedAt").UnixMilli())

	got, err = store.ToggleSubscriptionFavorite(ctx, "r1", "alice", time.Now().UTC())
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.Favorite, "second toggle must flip true→false")

	gotNil, err := store.ToggleSubscriptionFavorite(ctx, "missing", "alice", time.Now().UTC())
	assert.Nil(t, gotNil)
	assert.ErrorIs(t, err, model.ErrSubscriptionNotFound)
}

// TestMongoStore_ApplySubscriptionRestriction_StampsTimestamp asserts the origin
// write stamps restrictUpdatedAt so the doc shares the federated event's
// high-water mark (inbox-worker guards remote applies against it).
func TestMongoStore_ApplySubscriptionRestriction_StampsTimestamp(t *testing.T) {
	db := testutil.MongoDB(t, "room-svc-visibility-stamp")
	store := NewMongoStore(db)
	ctx := context.Background()

	mustInsertSub(t, db, &model.Subscription{
		ID:       idgen.GenerateUUIDv7(),
		User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:   "r1",
		RoomType: model.RoomTypeChannel,
		SiteID:   "site-a",
		Roles:    []model.Role{model.RoleOwner},
		JoinedAt: time.Now().UTC(),
	})

	// restrict+owner branch.
	ts1 := time.Now().UTC()
	require.NoError(t, store.ApplySubscriptionRestriction(ctx, "r1", true, false, "alice", ts1))
	assert.Equal(t, ts1.UnixMilli(), subTimeField(t, db, "r1", "alice", "restrictUpdatedAt").UnixMilli())

	// flags-only branch (ownerAccount empty).
	ts2 := ts1.Add(time.Second)
	require.NoError(t, store.ApplySubscriptionRestriction(ctx, "r1", false, false, "", ts2))
	assert.Equal(t, ts2.UnixMilli(), subTimeField(t, db, "r1", "alice", "restrictUpdatedAt").UnixMilli())
}

func TestMongoStore_SetOwnerRole_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "room-svc-role")
	store := NewMongoStore(db)
	ctx := context.Background()

	sub := &model.Subscription{
		ID:       idgen.GenerateUUIDv7(),
		User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:   "r1",
		RoomType: model.RoomTypeChannel,
		SiteID:   "site-a",
		Roles:    []model.Role{model.RoleMember},
		JoinedAt: time.Now().UTC(),
	}
	mustInsertSub(t, db, sub)

	// Promote: owner appended, member retained, order preserved.
	roleTs := time.Now().UTC()
	got, err := store.SetOwnerRole(ctx, "r1", "alice", true, roleTs)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, []model.Role{model.RoleMember, model.RoleOwner}, got.Roles)

	persisted, err := store.GetSubscription(ctx, "alice", "r1")
	require.NoError(t, err)
	assert.Equal(t, []model.Role{model.RoleMember, model.RoleOwner}, persisted.Roles)
	// rolesUpdatedAt is stamped at the supplied instant (BSON ms precision) so the
	// origin doc shares the federated event's high-water mark.
	assert.Equal(t, roleTs.UnixMilli(), subTimeField(t, db, "r1", "alice", "rolesUpdatedAt").UnixMilli())

	// Promote again is idempotent — no duplicate owner.
	got, err = store.SetOwnerRole(ctx, "r1", "alice", true, time.Now().UTC())
	require.NoError(t, err)
	assert.Equal(t, []model.Role{model.RoleMember, model.RoleOwner}, got.Roles)

	// Demote: owner removed, member retained.
	got, err = store.SetOwnerRole(ctx, "r1", "alice", false, time.Now().UTC())
	require.NoError(t, err)
	assert.Equal(t, []model.Role{model.RoleMember}, got.Roles)

	// Demote again is idempotent.
	got, err = store.SetOwnerRole(ctx, "r1", "alice", false, time.Now().UTC())
	require.NoError(t, err)
	assert.Equal(t, []model.Role{model.RoleMember}, got.Roles)

	// Channel-creator parity: an owner seeded WITHOUT member (roles ["owner"], as
	// processCreateRoom assigns the creator) must demote to ["member"], never [].
	creator := &model.Subscription{
		ID:       idgen.GenerateUUIDv7(),
		User:     model.SubscriptionUser{ID: "u2", Account: "carol"},
		RoomID:   "r1",
		RoomType: model.RoomTypeChannel,
		SiteID:   "site-a",
		Roles:    []model.Role{model.RoleOwner},
		JoinedAt: time.Now().UTC(),
	}
	mustInsertSub(t, db, creator)

	got, err = store.SetOwnerRole(ctx, "r1", "carol", false, time.Now().UTC())
	require.NoError(t, err)
	assert.Equal(t, []model.Role{model.RoleMember}, got.Roles, "demoting an owner-only creator must yield [member], never []")

	// Missing subscription → ErrSubscriptionNotFound (wrapped).
	gotNil, err := store.SetOwnerRole(ctx, "missing", "alice", true, time.Now().UTC())
	assert.Nil(t, gotNil)
	assert.ErrorIs(t, err, model.ErrSubscriptionNotFound)
}

// TestMongoStore_ListRoomMembers_OrgDisplay_DeptFirst_Integration verifies that
// when an org member's id matches both a user's deptId and another user's
// sectId, the dept branch wins and the combined "name tcName" string is
// surfaced via Member.OrgName.
func TestMongoStore_ListRoomMembers_OrgDisplay_DeptFirst_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	const roomID = "room-1"
	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a", Name: "R",
	})
	require.NoError(t, err)
	_, err = db.Collection("users").InsertOne(ctx, model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-a",
		DeptID: "X", DeptName: "Engineering", DeptTCName: "工程部",
	})
	require.NoError(t, err)
	_, err = db.Collection("users").InsertOne(ctx, model.User{
		ID: "u_bob", Account: "bob", SiteID: "site-a",
		SectID: "X", SectName: "Sect", SectTCName: "組",
	})
	require.NoError(t, err)
	_, err = db.Collection("room_members").InsertOne(ctx, model.RoomMember{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID,
		Member: model.RoomMemberEntry{ID: "X", Type: model.RoomMemberOrg},
	})
	require.NoError(t, err)

	got, err := store.ListRoomMembers(ctx, roomID, nil, nil, true)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "Engineering 工程部", got[0].Member.OrgName, "dept wins on overlap; name+tcName combined")
}

// TestMongoStore_ListRoomMembers_OrgDisplay_FallbackToOrgId_Integration verifies
// that when no users match the org id at all (neither deptId nor sectId), the
// display string falls back to the raw member.id rather than emitting an empty
// string — matching displayfmt.CombineWithFallback's third-argument semantics.
func TestMongoStore_ListRoomMembers_OrgDisplay_FallbackToOrgId_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	const roomID = "room-1"
	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a", Name: "R",
	})
	require.NoError(t, err)
	_, err = db.Collection("room_members").InsertOne(ctx, model.RoomMember{
		ID: idgen.GenerateUUIDv7(), RoomID: roomID,
		Member: model.RoomMemberEntry{ID: "Y", Type: model.RoomMemberOrg},
	})
	require.NoError(t, err)

	got, err := store.ListRoomMembers(ctx, roomID, nil, nil, true)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "Y", got[0].Member.OrgName, "no matching users → falls back to member.id")
}

func TestMongoStore_ListRoomMembers_OrgDescription_Integration(t *testing.T) {
	ctx := context.Background()

	t.Run("dept-first description wins", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		require.NoError(t, store.EnsureIndexes(ctx))

		const roomID = "room-1"
		_, err := db.Collection("users").InsertOne(ctx, model.User{
			ID: "u_alice", Account: "alice", SiteID: "site-a",
			DeptID: "X", DeptName: "Engineering", DeptDescription: "Dept desc",
		})
		require.NoError(t, err)
		_, err = db.Collection("users").InsertOne(ctx, model.User{
			ID: "u_bob", Account: "bob", SiteID: "site-a",
			SectID: "X", SectName: "Sect", SectDescription: "Sect desc",
		})
		require.NoError(t, err)
		_, err = db.Collection("room_members").InsertOne(ctx, model.RoomMember{
			ID: idgen.GenerateUUIDv7(), RoomID: roomID,
			Member: model.RoomMemberEntry{ID: "X", Type: model.RoomMemberOrg},
		})
		require.NoError(t, err)

		got, err := store.ListRoomMembers(ctx, roomID, nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "Dept desc", got[0].Member.OrgDescription)
	})

	t.Run("sect-only org uses sect description", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		require.NoError(t, store.EnsureIndexes(ctx))

		const roomID = "room-2"
		_, err := db.Collection("users").InsertOne(ctx, model.User{
			ID: "u_c", Account: "c", SiteID: "site-a",
			SectID: "S", SectName: "Sect", SectDescription: "Sect desc",
		})
		require.NoError(t, err)
		_, err = db.Collection("room_members").InsertOne(ctx, model.RoomMember{
			ID: idgen.GenerateUUIDv7(), RoomID: roomID,
			Member: model.RoomMemberEntry{ID: "S", Type: model.RoomMemberOrg},
		})
		require.NoError(t, err)

		got, err := store.ListRoomMembers(ctx, roomID, nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "Sect desc", got[0].Member.OrgDescription)
	})

	t.Run("missing description omitted", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		require.NoError(t, store.EnsureIndexes(ctx))

		const roomID = "room-3"
		_, err := db.Collection("users").InsertOne(ctx, model.User{
			ID: "u_d", Account: "d", SiteID: "site-a",
			SectID: "S2", SectName: "Sect2",
		})
		require.NoError(t, err)
		_, err = db.Collection("room_members").InsertOne(ctx, model.RoomMember{
			ID: idgen.GenerateUUIDv7(), RoomID: roomID,
			Member: model.RoomMemberEntry{ID: "S2", Type: model.RoomMemberOrg},
		})
		require.NoError(t, err)

		got, err := store.ListRoomMembers(ctx, roomID, nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Empty(t, got[0].Member.OrgDescription)
		assert.Equal(t, "Sect2", got[0].Member.OrgName)
	})
}

func TestMongoStore_ListDefaultChannelTabApps(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	apps := []any{
		bson.M{
			"_id": "app-z", "name": "Zeta",
			"channelTab": bson.M{
				"enabled": true, "default": true, "name": "Zeta",
				"url": bson.M{"default": "https://upstream/z"},
			},
		},
		bson.M{
			"_id": "app-a", "name": "Alpha",
			"channelTab": bson.M{
				"enabled": true, "default": true, "name": "Alpha",
				"url": bson.M{"default": "https://upstream/a"},
			},
		},
		bson.M{
			"_id": "app-disabled", "name": "Disabled",
			"channelTab": bson.M{
				"enabled": false, "default": true, "name": "Disabled",
				"url": bson.M{"default": "https://upstream/d"},
			},
		},
		bson.M{"_id": "app-notabs", "name": "NoTabs"},
	}
	_, err := db.Collection("apps").InsertMany(ctx, apps)
	require.NoError(t, err)

	got, err := store.ListDefaultChannelTabApps(ctx)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "app-a", got[0].ID, "expected Alpha first by channelTab.name asc")
	assert.Equal(t, "app-z", got[1].ID)
	assert.Equal(t, "Alpha", got[0].ChannelTab.Name)
	// Projection excludes app.name (response uses channelTab.name).
	assert.Empty(t, got[0].Name)
}

func TestMongoStore_ListDefaultChannelTabApps_Empty(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	got, err := store.ListDefaultChannelTabApps(context.Background())
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMongoStore_ListRoomBotApps(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	// Apps: one enabled bot, one disabled, one without assistant.
	_, err := db.Collection("apps").InsertMany(ctx, []any{
		bson.M{"_id": "appA", "name": "Weather",
			"assistant": bson.M{"enabled": true, "name": "weather.bot"}},
		bson.M{"_id": "appB", "name": "Stocks",
			"assistant": bson.M{"enabled": false, "name": "stocks.bot"}},
		bson.M{"_id": "appC", "name": "NoBot"},
	})
	require.NoError(t, err)

	// Subscriptions: roomA has 1 bot (weather, enabled) + 1 disabled bot
	// (stocks, but assistant.enabled=false should drop it) + 1 human;
	// roomB has 1 different bot.
	_, err = db.Collection("subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "s1", "roomId": "roomA",
			"u": bson.M{"_id": "ub1", "account": "weather.bot", "isBot": true}},
		bson.M{"_id": "s2", "roomId": "roomA",
			"u": bson.M{"_id": "ub2", "account": "stocks.bot", "isBot": true}},
		bson.M{"_id": "s3", "roomId": "roomA",
			"u": bson.M{"_id": "uh1", "account": "alice", "isBot": false}},
		bson.M{"_id": "s4", "roomId": "roomB",
			"u": bson.M{"_id": "ub3", "account": "other.bot", "isBot": true}},
	})
	require.NoError(t, err)

	got, err := store.ListRoomBotApps(ctx, "roomA")
	require.NoError(t, err)
	require.Len(t, got, 1, "only enabled assistant should join in")
	assert.Equal(t, "weather.bot", got[0].AssistantName)
	assert.Equal(t, "Weather", got[0].AppName)
}

func TestMongoStore_ListRoomBotApps_Empty(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	got, err := store.ListRoomBotApps(context.Background(), "ghost-room")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMongoStore_ListRoomBotApps_UniqueIndexProtectsAgainstDupes(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(context.Background()))
	ctx := context.Background()

	_, err := db.Collection("apps").InsertOne(ctx, bson.M{
		"_id": "appA", "name": "Weather",
		"assistant": bson.M{"enabled": true, "name": "weather.bot"},
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertOne(ctx, bson.M{
		"_id": "s1", "roomId": "roomA",
		"u": bson.M{"_id": "ub1", "account": "weather.bot", "isBot": true},
	})
	require.NoError(t, err)
	// Duplicate (roomId, u.account) must be rejected by the unique index.
	_, err = db.Collection("subscriptions").InsertOne(ctx, bson.M{
		"_id": "s2", "roomId": "roomA",
		"u": bson.M{"_id": "ub1b", "account": "weather.bot", "isBot": true},
	})
	require.Error(t, err)
	assert.True(t, mongo.IsDuplicateKeyError(err), "expected duplicate-key error from (roomId, u.account) unique index")
}

func TestMongoStore_ListActiveCmdMenus(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	_, err := db.Collection("bot_cmd_menu").InsertMany(ctx, []any{
		bson.M{"_id": "m1", "name": "weather.bot", "activeStatus": true,
			"cmdBlocks": []bson.M{{"text": "/forecast"}}},
		bson.M{"_id": "m2", "name": "weather.bot", "activeStatus": false,
			"cmdBlocks": []bson.M{{"text": "/old"}}},
		bson.M{"_id": "m3", "name": "stocks.bot", "activeStatus": true,
			"cmdBlocks": []bson.M{{"text": "/quote"}}},
		bson.M{"_id": "m4", "name": "other.bot", "activeStatus": true,
			"cmdBlocks": []bson.M{{"text": "/x"}}},
	})
	require.NoError(t, err)

	got, err := store.ListActiveCmdMenus(ctx, []string{"weather.bot", "stocks.bot"})
	require.NoError(t, err)
	require.Len(t, got, 2, "expect only the two active matching rows")
	assert.Equal(t, "stocks.bot", got[0].Name, "sorted by name asc")
	assert.Equal(t, "weather.bot", got[1].Name)
	require.Len(t, got[1].CmdBlocks, 1)
	assert.Equal(t, "/forecast", got[1].CmdBlocks[0].Text)
	// Projection drops _id and activeStatus.
	assert.Empty(t, got[1].ID)
	assert.False(t, got[1].ActiveStatus)
}

func TestMongoStore_ListActiveCmdMenus_EmptyInput(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	got, err := store.ListActiveCmdMenus(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMongoStore_ListActiveCmdMenus_NoMatches(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	got, err := store.ListActiveCmdMenus(context.Background(), []string{"unknown.bot"})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMongoStore_EnsureIndexes_NewCompoundIndexes(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(context.Background()))

	type idxCheck struct {
		coll     string
		wantKeys bson.D
	}
	checks := []idxCheck{
		{"apps", bson.D{
			{Key: "channelTab.default", Value: int32(1)},
			{Key: "channelTab.enabled", Value: int32(1)},
			{Key: "channelTab.name", Value: int32(1)},
		}},
		{"subscriptions", bson.D{
			{Key: "roomId", Value: int32(1)},
			{Key: "u.isBot", Value: int32(1)},
		}},
		// Backs getRoomSubscriptions: filter roomId, sort {joinedAt, _id} with
		// skip/limit pagination. Without joinedAt+_id in the index the sort
		// spills to an in-memory sort that risks the 32MB limit on large rooms.
		{"subscriptions", bson.D{
			{Key: "roomId", Value: int32(1)},
			{Key: "joinedAt", Value: int32(1)},
			{Key: "_id", Value: int32(1)},
		}},
		// Backs CountOwners: filter {roomId, roles} so the count is index-only
		// instead of scanning every subscription in the room.
		{"subscriptions", bson.D{
			{Key: "roomId", Value: int32(1)},
			{Key: "roles", Value: int32(1)},
		}},
		{"bot_cmd_menu", bson.D{
			{Key: "activeStatus", Value: int32(1)},
			{Key: "name", Value: int32(1)},
		}},
	}
	ctx := context.Background()
	for _, c := range checks {
		cursor, err := db.Collection(c.coll).Indexes().List(ctx)
		require.NoError(t, err)
		var idxes []bson.D
		require.NoError(t, cursor.All(ctx, &idxes))
		found := false
		for _, idx := range idxes {
			var gotKeys bson.D
			for _, elem := range idx {
				if elem.Key == "key" {
					if kd, ok := elem.Value.(bson.D); ok {
						gotKeys = kd
					}
				}
			}
			if len(gotKeys) != len(c.wantKeys) {
				continue
			}
			match := true
			for i, kv := range c.wantKeys {
				if gotKeys[i].Key != kv.Key || gotKeys[i].Value != kv.Value {
					match = false
					break
				}
			}
			if match {
				found = true
				break
			}
		}
		assert.True(t, found, "expected index on %s with keys (in order) %v", c.coll, c.wantKeys)
	}
}

// setupRoomsStream creates the ROOMS_{siteID} JetStream stream and returns a JetStream
// client. The stream captures all chat.room.canonical.{siteID}.* events published by
// the handler's publishToStream closure.
func setupRoomsStream(t *testing.T, nc *nats.Conn, siteID string) jetstream.JetStream {
	t.Helper()
	ctx := context.Background()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	cfg := stream.Rooms(siteID)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.Name,
		Subjects: cfg.Subjects,
	})
	require.NoError(t, err)
	return js
}

// drainJetStreamMsg fetches the first available message from a JetStream consumer
// with a deadline, then acks it and returns the raw data.
func drainJetStreamMsg(t *testing.T, cons jetstream.Consumer, timeout time.Duration) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(timeout))
	require.NoError(t, err)
	select {
	case msg := <-msgs.Messages():
		require.NotNil(t, msg)
		require.NoError(t, msg.Ack())
		return msg.Data()
	case <-ctx.Done():
		t.Fatal("timed out waiting for JetStream message")
		return nil
	}
}

// TestIntegration_RoomRename exercises the room.rename RPC end-to-end through NATS.
func TestIntegration_RoomRename(t *testing.T) {
	const siteID = "site-rename"

	t.Run("owner can rename a channel — canonical event published", func(t *testing.T) {
		db := testutil.MongoDB(t, "room-service-rename")
		store := NewMongoStore(db)
		ctx := context.Background()

		natsURL := setupNATS(t)
		clientNC, err := nats.Connect(natsURL)
		require.NoError(t, err)
		t.Cleanup(func() { _ = clientNC.Drain() })

		// Set up the ROOMS stream so publishToStream can land events.
		js := setupRoomsStream(t, clientNC, siteID)

		// Wire a JetStream-backed publishToStream on the handler.
		handlerNC, err := o11ynats.Connect(context.Background(), natsURL, noop.NewTracerProvider(), propagation.TraceContext{})
		require.NoError(t, err)
		t.Cleanup(func() { _ = handlerNC.Drain() })

		handlerJS, err := jetstream.New(handlerNC.NatsConn())
		require.NoError(t, err)

		publishToStream := func(pCtx context.Context, subj string, data []byte, _ string) error {
			msg := natsutil.NewMsg(pCtx, subj, data)
			_, err := handlerJS.PublishMsg(pCtx, msg)
			return err
		}

		keyStore := setupKeyStore(t, db)
		h := NewHandler(store, keyStore, nil, nil, siteID, 1000, 500, 5*time.Second, 5,
			publishToStream, func(context.Context, string, []byte) error { return nil }, nil, 0)
		router := natsrouter.New(handlerNC, "room-service")
		router.Use(natsrouter.RequireRequestID())
		h.Register(router)
		t.Cleanup(func() { _ = router.Shutdown(context.Background()) })
		require.NoError(t, handlerNC.NatsConn().Flush())

		// Seed: channel room + alice as owner.
		const roomID = "room-rename-1"
		mustInsertRoom(t, db, &model.Room{ID: roomID, Name: "old-name", Type: model.RoomTypeChannel, SiteID: siteID})
		mustInsertUser(t, db, &model.User{ID: "u-alice", Account: "alice", SiteID: siteID, Roles: []model.UserRole{model.UserRoleUser}})
		mustInsertSub(t, db, &model.Subscription{
			ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: siteID,
			User:  model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			Roles: []model.Role{model.RoleOwner},
		})

		// Set up a JetStream consumer to catch the canonical event.
		cons, err := js.CreateOrUpdateConsumer(ctx, stream.Rooms(siteID).Name, jetstream.ConsumerConfig{
			Durable:       "test-rename-consumer",
			AckPolicy:     jetstream.AckExplicitPolicy,
			FilterSubject: subject.RoomCanonical(siteID, "room.rename"),
		})
		require.NoError(t, err)

		reqID := idgen.GenerateRequestID()
		body, err := json.Marshal(model.RenameRoomRequest{NewName: "new-name"})
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: subject.RoomRename("alice", roomID, siteID),
			Data:    body,
			Header:  nats.Header{natsutil.RequestIDHeader: []string{reqID}},
		}
		reply, err := clientNC.RequestMsg(msg, 5*time.Second)
		require.NoError(t, err)

		// Assert accepted response.
		var resp map[string]string
		require.NoError(t, json.Unmarshal(reply.Data, &resp))
		assert.Equal(t, "accepted", resp["status"])
		assert.Equal(t, reqID, resp["requestId"])

		// Assert canonical event landed on the stream.
		raw := drainJetStreamMsg(t, cons, 5*time.Second)
		var event model.RenameRoomRequest
		require.NoError(t, json.Unmarshal(raw, &event))
		assert.Equal(t, roomID, event.RoomID)
		assert.Equal(t, "new-name", event.NewName)
		assert.Equal(t, "alice", event.Account)
		assert.NotZero(t, event.Timestamp)
	})

	t.Run("non-admin non-owner is rejected", func(t *testing.T) {
		db := testutil.MongoDB(t, "room-service-rename-reject")
		store := NewMongoStore(db)

		natsURL := setupNATS(t)
		handlerNC, err := o11ynats.Connect(context.Background(), natsURL, noop.NewTracerProvider(), propagation.TraceContext{})
		require.NoError(t, err)
		t.Cleanup(func() { _ = handlerNC.Drain() })

		clientNC, err := nats.Connect(natsURL)
		require.NoError(t, err)
		t.Cleanup(func() { _ = clientNC.Drain() })

		keyStore := setupKeyStore(t, db)
		h := NewHandler(store, keyStore, nil, nil, siteID, 1000, 500, 5*time.Second, 5,
			func(context.Context, string, []byte, string) error { return nil },
			func(context.Context, string, []byte) error { return nil }, nil, 0)
		router := natsrouter.New(handlerNC, "room-service")
		router.Use(natsrouter.RequireRequestID())
		h.Register(router)
		t.Cleanup(func() { _ = router.Shutdown(context.Background()) })
		require.NoError(t, handlerNC.NatsConn().Flush())

		const roomID = "room-rename-2"
		mustInsertRoom(t, db, &model.Room{ID: roomID, Name: "my-channel", Type: model.RoomTypeChannel, SiteID: siteID})
		// bob is a plain member, not an owner or platform admin.
		mustInsertUser(t, db, &model.User{ID: "u-bob", Account: "bob", SiteID: siteID, Roles: []model.UserRole{model.UserRoleUser}})
		mustInsertSub(t, db, &model.Subscription{
			ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: siteID,
			User:  model.SubscriptionUser{ID: "u-bob", Account: "bob"},
			Roles: []model.Role{model.RoleMember},
		})

		reqID := idgen.GenerateRequestID()
		body, err := json.Marshal(model.RenameRoomRequest{NewName: "hacked-name"})
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: subject.RoomRename("bob", roomID, siteID),
			Data:    body,
			Header:  nats.Header{natsutil.RequestIDHeader: []string{reqID}},
		}
		reply, err := clientNC.RequestMsg(msg, 5*time.Second)
		require.NoError(t, err)

		var errResp errcode.Error
		require.NoError(t, json.Unmarshal(reply.Data, &errResp))
		assert.Contains(t, errResp.Message, "only owners or platform admins can rename a channel")
	})
}

// TestIntegration_RoomRestricted exercises the room.restricted server-to-server
// RPC end-to-end through NATS. The handler does all the work synchronously and
// the reply carries the actual result.
func TestIntegration_RoomRestricted(t *testing.T) {
	const siteID = "site-restricted"

	t.Run("admin syncs restricted state — sys message published + Mongo updated", func(t *testing.T) {
		db := testutil.MongoDB(t, "room-service-restricted")
		store := NewMongoStore(db)
		ctx := context.Background()

		natsURL := setupNATS(t)
		clientNC, err := nats.Connect(natsURL)
		require.NoError(t, err)
		t.Cleanup(func() { _ = clientNC.Drain() })

		handlerNC, err := o11ynats.Connect(context.Background(), natsURL, noop.NewTracerProvider(), propagation.TraceContext{})
		require.NoError(t, err)
		t.Cleanup(func() { _ = handlerNC.Drain() })

		// Capture publishes via the handler's stream-publish callback so we can
		// assert the sys message lands without standing up the MESSAGES_CANONICAL
		// stream just for this test.
		type captured struct {
			subj string
			data []byte
		}
		var (
			mu   sync.Mutex
			pubs []captured
		)
		publishToStream := func(_ context.Context, subj string, data []byte, _ string) error {
			mu.Lock()
			defer mu.Unlock()
			cp := make([]byte, len(data))
			copy(cp, data)
			pubs = append(pubs, captured{subj: subj, data: cp})
			return nil
		}

		keyStore := setupKeyStore(t, db)
		h := NewHandler(store, keyStore, nil, nil, siteID, 1000, 500, 5*time.Second, 5,
			publishToStream, func(context.Context, string, []byte) error { return nil }, nil, 0)
		router := natsrouter.New(handlerNC, "room-service")
		router.Use(natsrouter.RequireRequestID())
		h.Register(router)
		t.Cleanup(func() { _ = router.Shutdown(context.Background()) })
		require.NoError(t, handlerNC.NatsConn().Flush())

		// Seed: channel room + admin + 5 members (the restricted-transition
		// guard requires UserCount >= 5).
		const roomID = "room-restricted-1"
		mustInsertRoom(t, db, &model.Room{
			ID: roomID, Name: "big-channel", Type: model.RoomTypeChannel,
			SiteID: siteID, UserCount: 5,
		})
		mustInsertUser(t, db, &model.User{
			ID: "u-admin1", Account: "admin1", SiteID: siteID,
			Roles: []model.UserRole{model.UserRoleAdmin},
		})
		for i := 0; i < 5; i++ {
			mustInsertSub(t, db, &model.Subscription{
				ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: siteID,
				User: model.SubscriptionUser{
					ID:      fmt.Sprintf("u-member-%d", i),
					Account: fmt.Sprintf("member%d", i),
				},
				Roles: []model.Role{model.RoleMember},
			})
		}
		mustInsertSub(t, db, &model.Subscription{
			ID: idgen.GenerateUUIDv7(), RoomID: roomID, SiteID: siteID,
			User:  model.SubscriptionUser{ID: "u-admin1", Account: "admin1"},
			Roles: []model.Role{model.RoleMember},
		})

		reqID := idgen.GenerateRequestID()
		body, err := json.Marshal(model.RoomRestrictedRequest{
			RoomID: roomID, Account: "admin1",
			Restricted:   true,
			OwnerAccount: "admin1",
		})
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: subject.RoomRestricted(siteID),
			Data:    body,
			Header:  nats.Header{natsutil.RequestIDHeader: []string{reqID}},
		}
		reply, err := clientNC.RequestMsg(msg, 5*time.Second)
		require.NoError(t, err)

		// Sync reply confirms the actual result.
		var resp map[string]string
		require.NoError(t, json.Unmarshal(reply.Data, &resp))
		assert.Equal(t, "ok", resp["status"])
		assert.Equal(t, reqID, resp["requestId"])

		// Mongo: room flags updated.
		updatedRoom, err := store.GetRoom(ctx, roomID)
		require.NoError(t, err)
		assert.True(t, updatedRoom.Restricted)

		// Sys message published to canonical messages stream.
		mu.Lock()
		var sawSysMsg bool
		for _, p := range pubs {
			if p.subj == subject.MsgCanonicalCreated(siteID) {
				sawSysMsg = true
				var msgEvt model.MessageEvent
				require.NoError(t, json.Unmarshal(p.data, &msgEvt))
				assert.Equal(t, model.MessageTypeRoomRestricted, msgEvt.Message.Type)
			}
		}
		mu.Unlock()
		assert.True(t, sawSysMsg, "expected room_restricted sys message to be published")
	})

	t.Run("non-admin requester is rejected", func(t *testing.T) {
		db := testutil.MongoDB(t, "room-service-restricted-reject")
		store := NewMongoStore(db)

		natsURL := setupNATS(t)
		handlerNC, err := o11ynats.Connect(context.Background(), natsURL, noop.NewTracerProvider(), propagation.TraceContext{})
		require.NoError(t, err)
		t.Cleanup(func() { _ = handlerNC.Drain() })

		clientNC, err := nats.Connect(natsURL)
		require.NoError(t, err)
		t.Cleanup(func() { _ = clientNC.Drain() })

		keyStore := setupKeyStore(t, db)
		h := NewHandler(store, keyStore, nil, nil, siteID, 1000, 500, 5*time.Second, 5,
			func(context.Context, string, []byte, string) error { return nil },
			func(context.Context, string, []byte) error { return nil }, nil, 0)
		router := natsrouter.New(handlerNC, "room-service")
		router.Use(natsrouter.RequireRequestID())
		h.Register(router)
		t.Cleanup(func() { _ = router.Shutdown(context.Background()) })
		require.NoError(t, handlerNC.NatsConn().Flush())

		const roomID = "room-restricted-2"
		mustInsertRoom(t, db, &model.Room{ID: roomID, Name: "my-channel", Type: model.RoomTypeChannel, SiteID: siteID, UserCount: 5})
		mustInsertUser(t, db, &model.User{ID: "u-carol", Account: "carol", SiteID: siteID, Roles: []model.UserRole{model.UserRoleUser}})

		reqID := idgen.GenerateRequestID()
		body, err := json.Marshal(model.RoomRestrictedRequest{
			RoomID: roomID, Account: "carol", Restricted: true,
		})
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: subject.RoomRestricted(siteID),
			Data:    body,
			Header:  nats.Header{natsutil.RequestIDHeader: []string{reqID}},
		}
		reply, err := clientNC.RequestMsg(msg, 5*time.Second)
		require.NoError(t, err)

		var errResp errcode.Error
		require.NoError(t, json.Unmarshal(reply.Data, &errResp))
		assert.Contains(t, errResp.Message, "only admins can change room restricted state")
	})
}

func TestMongoStore_ListMemberStatuses_Integration(t *testing.T) {
	ctx := context.Background()

	t.Run("projects five fields and respects limit", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)

		mustInsertUser(t, db, &model.User{
			ID: "u-alice", Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲",
			StatusIsShow: true, StatusText: "available",
		})
		mustInsertUser(t, db, &model.User{
			ID: "u-bob", Account: "bob", EngName: "Bob Chen", ChineseName: "陳博",
			StatusIsShow: false, StatusText: "in a meeting",
		})
		mustInsertSub(t, db, &model.Subscription{
			ID: "sub-a", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", SiteID: "site-a",
		})
		mustInsertSub(t, db, &model.Subscription{
			ID: "sub-b", User: model.SubscriptionUser{ID: "u-bob", Account: "bob"},
			RoomID: "r1", SiteID: "site-a",
		})

		got, err := store.ListMemberStatuses(ctx, "r1", 5)
		require.NoError(t, err)
		require.Len(t, got, 2)
		byAcct := map[string]model.MemberStatus{}
		for _, m := range got {
			byAcct[m.Account] = m
		}
		assert.Equal(t, model.MemberStatus{
			Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲",
			StatusIsShow: true, StatusText: "available",
		}, byAcct["alice"])
		assert.Equal(t, model.MemberStatus{
			Account: "bob", EngName: "Bob Chen", ChineseName: "陳博",
			StatusIsShow: false, StatusText: "in a meeting",
		}, byAcct["bob"])
	})

	t.Run("members with empty statusText are excluded", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		mustInsertUser(t, db, &model.User{
			ID: "u-alice", Account: "alice", EngName: "Alice", ChineseName: "愛",
			StatusIsShow: true, StatusText: "available",
		})
		mustInsertUser(t, db, &model.User{
			ID: "u-bob", Account: "bob", EngName: "Bob", ChineseName: "博", StatusText: "",
		})
		mustInsertSub(t, db, &model.Subscription{
			ID: "sub-a", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", SiteID: "site-a",
		})
		mustInsertSub(t, db, &model.Subscription{
			ID: "sub-b", User: model.SubscriptionUser{ID: "u-bob", Account: "bob"},
			RoomID: "r1", SiteID: "site-a",
		})

		got, err := store.ListMemberStatuses(ctx, "r1", 5)
		require.NoError(t, err)
		require.Len(t, got, 1, "member with empty statusText must be excluded")
		assert.Equal(t, "alice", got[0].Account)
	})

	t.Run("member whose user doc lacks statusText is excluded", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		mustInsertUser(t, db, &model.User{
			ID: "u-alice", Account: "alice", EngName: "Alice", ChineseName: "愛",
			StatusIsShow: true, StatusText: "available",
		})
		// Raw insert with no statusText field at all (bypasses the model's zero value).
		_, err := db.Collection("users").InsertOne(ctx, bson.M{"_id": "u-dave", "account": "dave", "engName": "Dave"})
		require.NoError(t, err)
		mustInsertSub(t, db, &model.Subscription{
			ID: "sub-a", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", SiteID: "site-a",
		})
		mustInsertSub(t, db, &model.Subscription{
			ID: "sub-d", User: model.SubscriptionUser{ID: "u-dave", Account: "dave"},
			RoomID: "r1", SiteID: "site-a",
		})

		got, err := store.ListMemberStatuses(ctx, "r1", 5)
		require.NoError(t, err)
		require.Len(t, got, 1, "member whose user doc lacks statusText must be excluded")
		assert.Equal(t, "alice", got[0].Account)
	})

	t.Run("limit caps the result count", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		for i := 0; i < 5; i++ {
			acct := fmt.Sprintf("user%d", i)
			mustInsertUser(t, db, &model.User{ID: "u-" + acct, Account: acct, EngName: acct, ChineseName: acct, StatusText: acct})
			mustInsertSub(t, db, &model.Subscription{
				ID: "sub-" + acct, User: model.SubscriptionUser{ID: "u-" + acct, Account: acct},
				RoomID: "r1", SiteID: "site-a",
			})
		}
		got, err := store.ListMemberStatuses(ctx, "r1", 2)
		require.NoError(t, err)
		require.Len(t, got, 2)
	})

	t.Run("subscription with missing user doc is dropped", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		mustInsertUser(t, db, &model.User{
			ID: "u-alice", Account: "alice", EngName: "Alice", ChineseName: "愛", StatusText: "x",
		})
		mustInsertSub(t, db, &model.Subscription{
			ID: "sub-a", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", SiteID: "site-a",
		})
		mustInsertSub(t, db, &model.Subscription{
			ID: "sub-ghost", User: model.SubscriptionUser{ID: "u-ghost", Account: "ghost"},
			RoomID: "r1", SiteID: "site-a",
		})
		got, err := store.ListMemberStatuses(ctx, "r1", 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "alice", got[0].Account)
	})

	t.Run("post-join limit returns full result when orphans precede live subs", func(t *testing.T) {
		// Regression: pre-join $limit would drop orphan-prefix subs *before*
		// the user join, under-returning when the first K subscriptions in
		// _id order reference deleted users. Post-join $limit must always
		// return min(limit, liveCount) rows regardless of orphan position.
		db := setupMongo(t)
		store := NewMongoStore(db)
		// Insert orphan subs first so they own the earliest _id values
		// (deterministic ordering — Mongo serves them first under pre-join $limit).
		for i := 0; i < 3; i++ {
			acct := fmt.Sprintf("ghost%d", i)
			mustInsertSub(t, db, &model.Subscription{
				ID:     fmt.Sprintf("sub-ghost%d", i),
				User:   model.SubscriptionUser{ID: "u-" + acct, Account: acct},
				RoomID: "r1", SiteID: "site-a",
			})
		}
		// Then 3 live subs.
		for i := 0; i < 3; i++ {
			acct := fmt.Sprintf("live%d", i)
			mustInsertUser(t, db, &model.User{ID: "u-" + acct, Account: acct, EngName: acct, StatusText: acct})
			mustInsertSub(t, db, &model.Subscription{
				ID:     fmt.Sprintf("sub-live%d", i),
				User:   model.SubscriptionUser{ID: "u-" + acct, Account: acct},
				RoomID: "r1", SiteID: "site-a",
			})
		}
		got, err := store.ListMemberStatuses(ctx, "r1", 3)
		require.NoError(t, err)
		require.Len(t, got, 3, "post-join $limit must deliver full count even when orphans are in the prefix")
		for _, m := range got {
			assert.Contains(t, m.Account, "live", "every returned row must be a live sub, got %q", m.Account)
		}
	})

	t.Run("empty room returns empty slice", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		got, err := store.ListMemberStatuses(ctx, "r-empty", 5)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestMongoStore_ListMentionableSubscriptions_Integration(t *testing.T) {
	ctx := context.Background()

	seedThree := func(t *testing.T, db *mongo.Database) {
		t.Helper()
		mustInsertUser(t, db, &model.User{ID: "u-alice", Account: "alice", SiteID: "site-a",
			EngName: "Alice Wang", ChineseName: "愛麗絲"})
		mustInsertUser(t, db, &model.User{ID: "u-bob", Account: "bob", SiteID: "site-b",
			EngName: "Bob Chen", ChineseName: "陳博"})
		// Bot user document — apps still join through this row when present.
		mustInsertUser(t, db, &model.User{ID: "u-bot", Account: "helper.bot"})
		_, err := db.Collection("apps").InsertOne(ctx, model.App{
			ID:        "app-1",
			Name:      "Helper",
			Assistant: &model.AppAssistant{Enabled: true, Name: "helper.bot"},
		})
		require.NoError(t, err)
		mustInsertSub(t, db, &model.Subscription{ID: "sub-a",
			User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}, RoomID: "r1", SiteID: "site-a"})
		mustInsertSub(t, db, &model.Subscription{ID: "sub-b",
			User: model.SubscriptionUser{ID: "u-bob", Account: "bob"}, RoomID: "r1", SiteID: "site-a"})
		mustInsertSub(t, db, &model.Subscription{ID: "sub-bot",
			User:   model.SubscriptionUser{ID: "u-bot", Account: "helper.bot", IsBot: true},
			RoomID: "r1", SiteID: "site-a"})
	}

	t.Run("classifies user vs app and shapes response", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		seedThree(t, db)

		got, err := store.ListMentionableSubscriptions(ctx, "r1", "", "", 10)
		require.NoError(t, err)
		require.Len(t, got, 3)

		byAcct := map[string]model.MentionableSubscription{}
		for _, s := range got {
			byAcct[s.Account] = s
		}

		require.Contains(t, byAcct, "alice")
		assert.Equal(t, "user", byAcct["alice"].OptionType)
		assert.Equal(t, "u-alice", byAcct["alice"].UserID)
		assert.Equal(t, "site-a", byAcct["alice"].SiteID)
		require.NotNil(t, byAcct["alice"].HRInfo)
		assert.Equal(t, "Alice Wang", byAcct["alice"].HRInfo.EngName)
		assert.Equal(t, "愛麗絲", byAcct["alice"].HRInfo.ChineseName)
		assert.Nil(t, byAcct["alice"].App)

		require.Contains(t, byAcct, "helper.bot")
		assert.Equal(t, "app", byAcct["helper.bot"].OptionType)
		assert.Equal(t, "u-bot", byAcct["helper.bot"].UserID)
		assert.Equal(t, "", byAcct["helper.bot"].SiteID, "app rows must have empty siteId")
		assert.Nil(t, byAcct["helper.bot"].HRInfo)
		require.NotNil(t, byAcct["helper.bot"].App)
		assert.Equal(t, "Helper", byAcct["helper.bot"].App.Name)
		assert.Equal(t, "helper.bot", byAcct["helper.bot"].App.Assistant.Name)
	})

	t.Run("excludeAccount filters caller", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		seedThree(t, db)

		got, err := store.ListMentionableSubscriptions(ctx, "r1", "alice", "", 10)
		require.NoError(t, err)
		require.Len(t, got, 2)
		for _, s := range got {
			assert.NotEqual(t, "alice", s.Account)
		}
	})

	t.Run("filter is case-insensitive substring on keyword", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		seedThree(t, db)

		got, err := store.ListMentionableSubscriptions(ctx, "r1", "", "BOB", 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "bob", got[0].Account)

		got, err = store.ListMentionableSubscriptions(ctx, "r1", "", "陳", 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "bob", got[0].Account)

		got, err = store.ListMentionableSubscriptions(ctx, "r1", "", "Helper", 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "helper.bot", got[0].Account)
	})

	t.Run("escaped filter treats . as a literal dot, not a wildcard", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		// Real bot account with a dot before "bot".
		mustInsertUser(t, db, &model.User{ID: "u-bot", Account: "helper.bot"})
		_, err := db.Collection("apps").InsertOne(ctx, model.App{
			ID: "app-1", Name: "Helper",
			Assistant: &model.AppAssistant{Enabled: true, Name: "helper.bot"},
		})
		require.NoError(t, err)
		mustInsertSub(t, db, &model.Subscription{ID: "sub-bot",
			User:   model.SubscriptionUser{ID: "u-bot", Account: "helper.bot", IsBot: true},
			RoomID: "r1", SiteID: "site-a"})
		// Decoy account that would also match if "." were treated as a wildcard
		// ("helperXbot"). It is a normal (non-bot) account so it classifies as a user.
		mustInsertUser(t, db, &model.User{ID: "u-x", Account: "helperxbot", EngName: "X"})
		mustInsertSub(t, db, &model.Subscription{ID: "sub-x",
			User:   model.SubscriptionUser{ID: "u-x", Account: "helperxbot"},
			RoomID: "r1", SiteID: "site-a"})

		// `helper\.bot` is regexp.QuoteMeta("helper.bot") — the escaped form the
		// handler passes to the store. As a literal it matches only "helper.bot";
		// if the pipeline treated "." as a wildcard it would also match "helperxbot".
		got, err := store.ListMentionableSubscriptions(ctx, "r1", "", `helper\.bot`, 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "helper.bot", got[0].Account)
	})

	t.Run("p_ prefix is hidden from mentionable results", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		mustInsertUser(t, db, &model.User{ID: "u-pa", Account: "p_admin", EngName: "Platform Admin"})
		mustInsertUser(t, db, &model.User{ID: "u-alice", Account: "alice", EngName: "Alice"})
		mustInsertSub(t, db, &model.Subscription{ID: "sub-pa",
			User:   model.SubscriptionUser{ID: "u-pa", Account: "p_admin"},
			RoomID: "r1", SiteID: "site-a"})
		mustInsertSub(t, db, &model.Subscription{ID: "sub-alice",
			User:   model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", SiteID: "site-a"})

		got, err := store.ListMentionableSubscriptions(ctx, "r1", "", "", 10)
		require.NoError(t, err)
		require.Len(t, got, 1, "p_ accounts must be excluded from mentionable results")
		assert.Equal(t, "alice", got[0].Account)
		assert.Equal(t, "user", got[0].OptionType)
	})

	t.Run("limit caps the result count", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		seedThree(t, db)

		got, err := store.ListMentionableSubscriptions(ctx, "r1", "", "", 2)
		require.NoError(t, err)
		require.Len(t, got, 2)
	})

	t.Run("empty room returns empty slice", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		got, err := store.ListMentionableSubscriptions(ctx, "r-empty", "", "", 5)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("orphan bot subscription returns empty app strings, not null", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		mustInsertUser(t, db, &model.User{ID: "u-ghost", Account: "ghost.bot"})
		mustInsertSub(t, db, &model.Subscription{ID: "sub-ghost",
			User:   model.SubscriptionUser{ID: "u-ghost", Account: "ghost.bot", IsBot: true},
			RoomID: "r1", SiteID: "site-a"})

		got, err := store.ListMentionableSubscriptions(ctx, "r1", "", "", 5)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "app", got[0].OptionType)
		require.NotNil(t, got[0].App)
		assert.Equal(t, "", got[0].App.Name)
		assert.Equal(t, "", got[0].App.Assistant.Name)
	})
}

func TestBotAndAdminPredicate_GoAndMongoAgree_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

	// Probe set covers .bot suffix, p_ prefix, non-system accounts, and
	// tricky lookalikes.
	probes := []string{
		"alice",
		"bob.bot",
		"p_assistant",
		"botanist",           // contains "bot" but not at end
		"p",                  // single char, no underscore
		"weird.botanist",     // ends in 'ist', not '.bot'
		"helper.bot.archive", // ".bot" not anchored at end
		"p_",                 // edge: p_ with nothing after
		"P_admin",            // case-sensitive — uppercase P should NOT match
	}

	for _, acct := range probes {
		mustInsertUser(t, db, &model.User{ID: "u-" + acct, Account: acct, EngName: acct})
		mustInsertSub(t, db, &model.Subscription{
			ID:     "sub-" + acct,
			User:   model.SubscriptionUser{ID: "u-" + acct, Account: acct, IsBot: strings.HasSuffix(acct, ".bot")},
			RoomID: "r1", SiteID: "site-a",
		})
	}

	got, err := store.ListMentionableSubscriptions(ctx, "r1", "", "", len(probes)+5)
	require.NoError(t, err)

	// Build the observed Mongo classification: presence in results plus optionType.
	type seen struct {
		present bool
		isApp   bool
	}
	mongo := map[string]seen{}
	for _, s := range got {
		mongo[s.Account] = seen{present: true, isApp: s.OptionType == "app"}
	}

	// Locks Go and Mongo in agreement on bot vs platform-admin vs human:
	//   `.bot` suffix => present + optionType "app"   (Mongo: botAccountRegex)
	//   `p_` prefix   => absent                       (Mongo: $not platformAdminRegex)
	//   otherwise     => present + optionType "user"
	for _, acct := range probes {
		switch {
		case strings.HasSuffix(acct, ".bot"):
			assert.True(t, mongo[acct].present, "%q: bot should appear", acct)
			assert.True(t, mongo[acct].isApp, "%q: bot should be optionType=app", acct)
		case strings.HasPrefix(acct, "p_"):
			assert.False(t, mongo[acct].present, "%q: platform admin must be hidden", acct)
		default:
			assert.True(t, mongo[acct].present, "%q: human should appear", acct)
			assert.False(t, mongo[acct].isApp, "%q: human should be optionType=user", acct)
		}
	}
}

// account is a user's identity, so EnsureIndexes makes users.account unique —
// matching user-service's declaration on the shared collection. A second users
// doc with the same account must violate the unique index.
func TestEnsureIndexes_UsersAccountUnique_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()
	require.NoError(t, store.EnsureIndexes(ctx))

	users := db.Collection("users")
	_, err := users.InsertOne(ctx, bson.M{"_id": "u1", "account": "alice"})
	require.NoError(t, err)
	_, err = users.InsertOne(ctx, bson.M{"_id": "u2", "account": "alice"})
	require.True(t, mongo.IsDuplicateKeyError(err), "expected duplicate-key error, got %v", err)
}

// Building the users.account unique index against a collection that already holds
// duplicate accounts (a dirty pre-rollout environment) must fail at startup with
// an actionable error pointing operators at the dedupe preflight, not a bare
// driver error.
func TestEnsureIndexes_UsersAccountUnique_PreexistingDuplicates_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	users := db.Collection("users")
	_, err := users.InsertOne(ctx, bson.M{"_id": "u1", "account": "alice"})
	require.NoError(t, err)
	_, err = users.InsertOne(ctx, bson.M{"_id": "u2", "account": "alice"})
	require.NoError(t, err)

	err = store.EnsureIndexes(ctx)
	require.Error(t, err)
	require.True(t, mongo.IsDuplicateKeyError(err), "expected duplicate-key error, got %v", err)
	require.Contains(t, err.Error(), "dedupe preflight", "error must direct operators to the dedupe preflight")
}

// A pre-existing NON-unique account_1 index conflicts with EnsureIndexes'
// unique account_1 declaration (IndexOptionsConflict 85 / IndexKeySpecsConflict
// 86 — the latter when the auto-generated name collides). The service must
// surface an actionable error telling the operator to drop the old index rather
// than a bare driver error.
func TestEnsureIndexes_UsersAccountIndexOptionsConflict_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	// Pre-create a non-unique index named account_1 with the same key spec.
	_, err := db.Collection("users").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "account", Value: 1}},
	})
	require.NoError(t, err)

	err = store.EnsureIndexes(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "drop the old non-unique account_1 index",
		"error must direct operators to drop the conflicting non-unique index")
}

func TestMongoStore_ClearThreadSubscriptionsForAccount_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(context.Background()))
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	_, err := db.Collection("thread_subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "tsA1", "threadRoomId": "tr1", "roomId": "r1", "parentMessageId": "p1", "userId": "uA", "userAccount": "alice", "hasMention": true},
		bson.M{"_id": "tsA2", "threadRoomId": "tr2", "roomId": "r2", "parentMessageId": "p2", "userId": "uA", "userAccount": "alice", "hasMention": false},
		bson.M{"_id": "tsB1", "threadRoomId": "tr9", "roomId": "r1", "parentMessageId": "p9", "userId": "uB", "userAccount": "bob", "hasMention": true},
	})
	require.NoError(t, err)

	rows, err := store.ClearThreadSubscriptionsForAccount(ctx, "alice", now)
	require.NoError(t, err)
	require.Len(t, rows, 2)

	got := map[string]model.ThreadSubscription{}
	for _, r := range rows {
		got[r.ThreadRoomID] = r
	}
	assert.Equal(t, "r1", got["tr1"].RoomID)
	assert.Equal(t, "p1", got["tr1"].ParentMessageID)
	assert.Equal(t, "r2", got["tr2"].RoomID)

	// alice's docs: lastSeenAt set to now, hasMention cleared.
	for _, id := range []string{"tsA1", "tsA2"} {
		var raw bson.M
		require.NoError(t, db.Collection("thread_subscriptions").FindOne(ctx, bson.M{"_id": id}).Decode(&raw))
		ls, ok := raw["lastSeenAt"].(bson.DateTime)
		require.True(t, ok, "lastSeenAt must be set for %s", id)
		assert.WithinDuration(t, now, ls.Time(), time.Second)
		assert.Equal(t, false, raw["hasMention"])
	}

	// bob untouched: no lastSeenAt, hasMention still true.
	var bobRaw bson.M
	require.NoError(t, db.Collection("thread_subscriptions").FindOne(ctx, bson.M{"_id": "tsB1"}).Decode(&bobRaw))
	_, present := bobRaw["lastSeenAt"]
	assert.False(t, present, "bob's thread sub must be untouched")
	assert.Equal(t, true, bobRaw["hasMention"])
}

func TestMongoStore_ClearThreadSubscriptionsForAccount_Empty_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(context.Background()))

	rows, err := store.ClearThreadSubscriptionsForAccount(context.Background(), "nobody", time.Now().UTC())
	require.NoError(t, err)
	assert.Nil(t, rows)
}

func TestMongoStore_ClearSubscriptionThreadUnreadForAccount_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(context.Background()))
	ctx := context.Background()

	subs := []model.Subscription{
		{ID: "sA1", RoomID: "r1", SiteID: "site-a", User: model.SubscriptionUser{ID: "uA", Account: "alice"}, ThreadUnread: []string{"p1", "p2"}, Alert: true},
		{ID: "sA2", RoomID: "r2", SiteID: "site-a", User: model.SubscriptionUser{ID: "uA", Account: "alice"}, Alert: false},
		{ID: "sB1", RoomID: "r1", SiteID: "site-a", User: model.SubscriptionUser{ID: "uB", Account: "bob"}, ThreadUnread: []string{"p9"}, Alert: true},
	}
	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{&subs[0], &subs[1], &subs[2]})
	require.NoError(t, err)

	require.NoError(t, store.ClearSubscriptionThreadUnreadForAccount(ctx, "alice"))

	// alice r1: threadUnread unset, alert cleared.
	var r1 bson.M
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sA1"}).Decode(&r1))
	_, present := r1["threadUnread"]
	assert.False(t, present, "threadUnread must be $unset")
	assert.Equal(t, false, r1["alert"])

	// bob r1: untouched.
	var bobRaw model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sB1"}).Decode(&bobRaw))
	assert.Equal(t, []string{"p9"}, bobRaw.ThreadUnread)
	assert.True(t, bobRaw.Alert)
}
