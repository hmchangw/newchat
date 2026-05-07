//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	natsmod "github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/testutil/testimages"
)

func setupMongo(t *testing.T) *mongo.Database {
	return testutil.MongoDB(t, "room_service_test")
}

func setupValkey(t *testing.T) *roomkeystore.Config {
	t.Helper()
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        testimages.Valkey,
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections"),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "6379")
	require.NoError(t, err)
	return &roomkeystore.Config{
		Addr:        fmt.Sprintf("%s:%s", host, port.Port()),
		GracePeriod: time.Hour,
	}
}

func setupNATS(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := natsmod.Run(ctx, testimages.NATS)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })
	url, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	return url
}

func TestMongoStore_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	// Test CreateRoom and GetRoom
	room := model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel, SiteID: "site-a", CreatedBy: "u1", UserCount: 1}
	require.NoError(t, store.CreateRoom(ctx, &room))
	got, err := store.GetRoom(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, "general", got.Name)

	// Test ListRooms
	require.NoError(t, store.CreateRoom(ctx, &model.Room{ID: "r2", Name: "random", Type: model.RoomTypeChannel}))
	rooms, err := store.ListRooms(ctx)
	require.NoError(t, err)
	assert.Len(t, rooms, 2)

	// Test CreateSubscription and GetSubscription
	sub := model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}}
	require.NoError(t, store.CreateSubscription(ctx, &sub))
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

func TestMongoStore_GetSubscriptionWithMembership_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	sub := &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", Roles: []model.Role{model.RoleOwner},
		JoinedAt: time.Now().UTC(),
	}
	require.NoError(t, store.CreateSubscription(ctx, sub))

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
	insertSub := func(t *testing.T, store *MongoStore, sub model.Subscription) {
		t.Helper()
		require.NoError(t, store.CreateSubscription(ctx, &sub))
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
		insertSub(t, store, model.Subscription{
			ID: "sub-a", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", SiteID: "site-a", JoinedAt: base.Add(10 * time.Second),
		})
		insertSub(t, store, model.Subscription{
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
		insertSub(t, store, model.Subscription{
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
	insertSub := func(t *testing.T, store *MongoStore, sub model.Subscription) {
		t.Helper()
		require.NoError(t, store.CreateSubscription(ctx, &sub))
	}
	ptr := func(i int) *int { return &i }

	t.Run("individual enrichment via room_members path", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

		insertUser(t, db, model.User{
			ID: "u-alice", Account: "alice", SiteID: "site-a",
			EngName: "Alice Wang", ChineseName: "愛麗絲",
		})
		insertSub(t, store, model.Subscription{
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
		assert.Empty(t, m.SectName)
		assert.Zero(t, m.MemberCount)
	})

	t.Run("individual non-owner sets IsOwner false", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)

		insertUser(t, db, model.User{ID: "u-bob", Account: "bob", EngName: "Bob", ChineseName: "鮑伯"})
		insertSub(t, store, model.Subscription{
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
		assert.Equal(t, "Engineering", m.SectName)
		assert.Equal(t, 3, m.MemberCount)
		assert.Empty(t, m.EngName)
		assert.False(t, m.IsOwner)
	})

	t.Run("enrichment via subscriptions fallback", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)

		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲"})
		insertUser(t, db, model.User{ID: "u-bob", Account: "bob", EngName: "Bob", ChineseName: "鮑伯"})
		insertSub(t, store, model.Subscription{
			ID: "sub-a", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", Roles: []model.Role{model.RoleOwner}, JoinedAt: base.Add(10 * time.Second),
		})
		insertSub(t, store, model.Subscription{
			ID: "sub-b", User: model.SubscriptionUser{ID: "u-bob", Account: "bob"},
			RoomID: "r1", Roles: []model.Role{model.RoleMember}, JoinedAt: base.Add(20 * time.Second),
		})
		// Note: NO room_members docs inserted — exercises the fallback path.

		got, err := store.ListRoomMembers(ctx, "r1", nil, nil, true)
		require.NoError(t, err)
		require.Len(t, got, 2)

		alice, bob := got[0].Member, got[1].Member
		assert.Equal(t, "alice", alice.Account)
		assert.Equal(t, "Alice Wang", alice.EngName)
		assert.True(t, alice.IsOwner)
		assert.Equal(t, "bob", bob.Account)
		assert.Equal(t, "Bob", bob.EngName)
		assert.False(t, bob.IsOwner)
	})

	t.Run("enrich=false leaves display fields zero on same seed data", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		base := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)

		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲"})
		insertSub(t, store, model.Subscription{
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
		assert.Empty(t, m.SectName)
		assert.Zero(t, m.MemberCount)
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

	t.Run("empty org returns errInvalidOrg", func(t *testing.T) {
		db := setupMongo(t)
		store := NewMongoStore(db)
		insertUser(t, db, model.User{ID: "u-alice", Account: "alice", SectID: "sect-eng"})

		_, err := store.ListOrgMembers(ctx, "sect-nope")
		require.Error(t, err)
		assert.True(t, errors.Is(err, errInvalidOrg), "want errInvalidOrg in chain, got %v", err)
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
		if err := store.CreateRoom(ctx, &seed[i]); err != nil {
			t.Fatalf("seed CreateRoom %q: %v", seed[i].ID, err)
		}
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
	valCfg := setupValkey(t)

	keyStore, err := roomkeystore.NewValkeyStore(*valCfg)
	require.NoError(t, err)
	store := NewMongoStore(db)

	ctx := context.Background()

	// Target room on site-a
	require.NoError(t, store.CreateRoom(ctx, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"}))
	// Source channel on site-a: seed room_members explicitly so ListRoomMembers takes the room_members
	// branch (not the subscriptions fallback); also seed users so ResolveAccounts can find them.
	require.NoError(t, store.CreateRoom(ctx, &model.Room{ID: "source", Type: model.RoomTypeChannel, SiteID: "site-a"}))
	_, err = db.Collection("users").InsertMany(ctx, []interface{}{
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
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{ID: "s1", RoomID: "source", User: model.SubscriptionUser{ID: "u1", Account: "bob"}}))
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{ID: "s2", RoomID: "source", User: model.SubscriptionUser{ID: "u2", Account: "carol"}}))
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{ID: "s3", RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}, Roles: []model.Role{model.RoleOwner}}))
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{ID: "s4", RoomID: "source", User: model.SubscriptionUser{ID: "req", Account: "alice"}}))

	// Same-site only: pass nil for memberListClient — the same-site branch in
	// expandChannelRefs uses store.ListRoomMembers and never invokes the client.
	var publishedSubj string
	var publishedData []byte
	publish := func(_ context.Context, subj string, data []byte) error {
		publishedSubj = subj
		publishedData = data
		return nil
	}
	handler := NewHandler(store, keyStore, nil, "site-a", 1000, 500, 5*time.Second, publish)

	req := model.AddMembersRequest{
		Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-a"}},
	}
	data, err := json.Marshal(req)
	require.NoError(t, err)
	result, err := handler.handleAddMembers(ctx, subject.MemberAdd("alice", "target", "site-a"), data)
	require.NoError(t, err)
	var status map[string]string
	require.NoError(t, json.Unmarshal(result, &status))
	assert.Equal(t, "accepted", status["status"])

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
	valCfg := setupValkey(t)

	keyStore, err := roomkeystore.NewValkeyStore(*valCfg)
	require.NoError(t, err)
	store := NewMongoStore(db)

	ctx := context.Background()

	require.NoError(t, store.CreateRoom(ctx, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"}))
	require.NoError(t, store.CreateRoom(ctx, &model.Room{ID: "source", Type: model.RoomTypeChannel, SiteID: "site-a"}))
	// Seed users so ResolveAccounts can find them.
	_, err = db.Collection("users").InsertMany(ctx, []interface{}{
		model.User{ID: "u1", Account: "bob", SiteID: "site-a"},
		model.User{ID: "u2", Account: "carol", SiteID: "site-a"},
		model.User{ID: "u3", Account: "dave", SiteID: "site-a"},
		model.User{ID: "req", Account: "alice", SiteID: "site-a"},
	})
	require.NoError(t, err)
	// Source only has subscriptions (no room_members rows) — ListRoomMembers falls back to subscriptions
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{ID: "s1", RoomID: "source", User: model.SubscriptionUser{ID: "u1", Account: "bob"}}))
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{ID: "s2", RoomID: "source", User: model.SubscriptionUser{ID: "u2", Account: "carol"}}))
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{ID: "s3", RoomID: "source", User: model.SubscriptionUser{ID: "u3", Account: "dave"}}))
	// Requester in both
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{ID: "s4", RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}, Roles: []model.Role{model.RoleOwner}}))
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{ID: "s5", RoomID: "source", User: model.SubscriptionUser{ID: "req", Account: "alice"}}))

	// Same-site only: nil memberListClient is safe (the same-site branch never invokes it).
	var publishedSubj string
	var publishedData []byte
	publish := func(_ context.Context, subj string, data []byte) error {
		publishedSubj = subj
		publishedData = data
		return nil
	}
	handler := NewHandler(store, keyStore, nil, "site-a", 1000, 500, 5*time.Second, publish)

	req := model.AddMembersRequest{Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-a"}}}
	data, err := json.Marshal(req)
	require.NoError(t, err)
	result, err := handler.handleAddMembers(ctx, subject.MemberAdd("alice", "target", "site-a"), data)
	require.NoError(t, err)
	var status map[string]string
	require.NoError(t, json.Unmarshal(result, &status))
	assert.Equal(t, "accepted", status["status"])

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
	valCfg := setupValkey(t)

	keyStore, err := roomkeystore.NewValkeyStore(*valCfg)
	require.NoError(t, err)
	store := NewMongoStore(db)

	ctx := context.Background()

	require.NoError(t, store.CreateRoom(ctx, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"}))
	require.NoError(t, store.CreateRoom(ctx, &model.Room{ID: "source", Type: model.RoomTypeChannel, SiteID: "site-a"}))
	// Requester subscribed to target but NOT source
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}, Roles: []model.Role{model.RoleOwner}}))

	// Same-site only: nil memberListClient is safe — request fails on the same-site
	// GetSubscription check before reaching the cross-site branch.
	handler := NewHandler(store, keyStore, nil, "site-a", 1000, 500, 5*time.Second, func(context.Context, string, []byte) error { return nil })

	req := model.AddMembersRequest{Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-a"}}}
	data, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = handler.handleAddMembers(ctx, subject.MemberAdd("alice", "target", "site-a"), data)
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
	valCfg := setupValkey(t)

	keyStore, err := roomkeystore.NewValkeyStore(*valCfg)
	require.NoError(t, err)

	storeA := NewMongoStore(dbA)
	storeB := NewMongoStore(dbB)

	otelNCb, err := otelnats.Connect(natsURLb)
	require.NoError(t, err)
	t.Cleanup(func() { _ = otelNCb.Drain() })

	ctx := context.Background()

	// Site-A: target room; requester subscribed; user document needed for ResolveAccounts.
	require.NoError(t, storeA.CreateRoom(ctx, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"}))
	_, err = dbA.Collection("users").InsertMany(ctx, []interface{}{
		model.User{ID: "req", Account: "alice", SiteID: "site-a"},
		model.User{ID: "u1", Account: "bob", SiteID: "site-a"},
		model.User{ID: "u2", Account: "carol", SiteID: "site-a"},
	})
	require.NoError(t, err)
	require.NoError(t, storeA.CreateSubscription(ctx, &model.Subscription{ID: "sa1", RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}, Roles: []model.Role{model.RoleOwner}}))

	// Site-B: source channel with members; requester subscribed on site-b too.
	require.NoError(t, storeB.CreateRoom(ctx, &model.Room{ID: "source", Type: model.RoomTypeChannel, SiteID: "site-b"}))
	_, err = dbB.Collection("users").InsertMany(ctx, []interface{}{
		model.User{ID: "u1", Account: "bob", SiteID: "site-b"},
		model.User{ID: "u2", Account: "carol", SiteID: "site-b"},
		model.User{ID: "req", Account: "alice", SiteID: "site-b"},
	})
	require.NoError(t, err)
	require.NoError(t, storeB.CreateSubscription(ctx, &model.Subscription{ID: "sb1", RoomID: "source", User: model.SubscriptionUser{ID: "u1", Account: "bob"}}))
	require.NoError(t, storeB.CreateSubscription(ctx, &model.Subscription{ID: "sb2", RoomID: "source", User: model.SubscriptionUser{ID: "u2", Account: "carol"}}))
	require.NoError(t, storeB.CreateSubscription(ctx, &model.Subscription{ID: "sb3", RoomID: "source", User: model.SubscriptionUser{ID: "req", Account: "alice"}}))

	// Site-B handler registers member.list endpoint (RegisterCRUD subscribes to MemberListWildcard).
	handlerB := NewHandler(storeB, keyStore, nil, "site-b", 1000, 500, 5*time.Second, func(context.Context, string, []byte) error { return nil })
	require.NoError(t, handlerB.RegisterCRUD(otelNCb))
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
	publish := func(_ context.Context, subj string, data []byte) error {
		publishedSubj = subj
		publishedData = data
		return nil
	}
	handlerA := NewHandler(storeA, keyStore, memberListClient, "site-a", 1000, 500, 5*time.Second, publish)

	// Call add-members on site-A with a site-B source channel
	req := model.AddMembersRequest{Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-b"}}}
	data, err := json.Marshal(req)
	require.NoError(t, err)
	result, err := handlerA.handleAddMembers(ctx, subject.MemberAdd("alice", "target", "site-a"), data)
	require.NoError(t, err)

	var status map[string]string
	require.NoError(t, json.Unmarshal(result, &status))
	assert.Equal(t, "accepted", status["status"])

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
	valCfg := setupValkey(t)

	keyStore, err := roomkeystore.NewValkeyStore(*valCfg)
	require.NoError(t, err)
	store := NewMongoStore(db)
	otelNC, err := otelnats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = otelNC.Drain() })

	ctx := context.Background()

	// Target on site-a, requester subscribed.
	require.NoError(t, store.CreateRoom(ctx, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"}))
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{ID: "s1", RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}, Roles: []model.Role{model.RoleOwner}}))

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
	handler := NewHandler(store, keyStore, memberListClient, "site-a", 1000, 500, 200*time.Millisecond, func(context.Context, string, []byte) error { return nil })

	req := model.AddMembersRequest{Channels: []model.ChannelRef{{RoomID: "source", SiteID: "site-b"}}}
	data, err := json.Marshal(req)
	require.NoError(t, err)

	_, err = handler.handleAddMembers(ctx, subject.MemberAdd("alice", "target", "site-a"), data)
	require.Error(t, err)
	// Cross-site member.list deadline → typed channelExpandTimeoutError naming
	// the offending site+roomId. sanitizeError surfaces the message verbatim.
	var te *channelExpandTimeoutError
	require.ErrorAs(t, err, &te, "expected channelExpandTimeoutError, got %v", err)
	assert.Equal(t, "site-b", te.SiteID)
	assert.Equal(t, "source", te.RoomID)
	assert.Equal(t, "timeout listing members of channel source@site-b", sanitizeError(err))
}

func TestRoomsInfoBatchRPC(t *testing.T) {
	db := setupMongo(t)
	valCfg := setupValkey(t)
	natsURL := setupNATS(t)

	keyStore, err := roomkeystore.NewValkeyStore(*valCfg)
	require.NoError(t, err)

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
		require.NoError(t, store.CreateRoom(ctx, &rooms[i]))
	}

	pubKey := bytes.Repeat([]byte{0xAB}, 65)
	privKey1 := bytes.Repeat([]byte{0x01}, 32)
	privKey2 := bytes.Repeat([]byte{0x02}, 32)
	_, err = keyStore.Set(ctx, "r1", roomkeystore.RoomKeyPair{PublicKey: pubKey, PrivateKey: privKey1})
	require.NoError(t, err)
	_, err = keyStore.Set(ctx, "r2", roomkeystore.RoomKeyPair{PublicKey: pubKey, PrivateKey: privKey2})
	require.NoError(t, err)

	otelNC, err := otelnats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = otelNC.Drain() })

	handler := NewHandler(store, keyStore, nil, "site-a", 1000, 500, 5*time.Second, func(context.Context, string, []byte) error { return nil })
	require.NoError(t, handler.RegisterCRUD(otelNC))
	require.NoError(t, otelNC.NatsConn().Flush())

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { nc.Drain() })

	req := model.RoomsInfoBatchRequest{RoomIDs: []string{"r1", "r2", "r3", "missing"}}
	data, err := json.Marshal(req)
	require.NoError(t, err)

	msg, err := nc.Request(subject.RoomsInfoBatch("site-a"), data, 3*time.Second)
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
	publish := func(_ context.Context, subj string, data []byte) error {
		lastSubj = subj
		lastData = data
		return nil
	}
	h := NewHandler(store, keyStore, nil, siteID, 1000, 500, 5*time.Second, publish)
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

	valCfg := setupValkey(t)
	keyStore, err := roomkeystore.NewValkeyStore(*valCfg)
	require.NoError(t, err)

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

	body, err := json.Marshal(model.CreateRoomRequest{
		Name:  "deal team",
		Users: []string{"bob"},
	})
	require.NoError(t, err)

	resp, err := h.handleCreateRoom(ctx, subject.RoomCreate("alice", "site-A"), body)
	require.NoError(t, err)

	var got model.CreateRoomReply
	require.NoError(t, json.Unmarshal(resp, &got))
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

	valCfg := setupValkey(t)
	keyStore, err := roomkeystore.NewValkeyStore(*valCfg)
	require.NoError(t, err)

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

	body, err := json.Marshal(model.CreateRoomRequest{Users: []string{"bob"}})
	require.NoError(t, err)

	_, herr := h.handleCreateRoom(ctx, subject.RoomCreate("alice", "site-A"), body)
	require.Error(t, herr)

	var dmErr *dmExistsError
	require.True(t, errors.As(herr, &dmErr), "expected dmExistsError, got %T: %v", herr, herr)
	assert.Equal(t, "dm already exists", dmErr.Error())
	assert.Equal(t, roomID, dmErr.RoomID())
}

func TestMongoStore_UpdateSubscriptionRead_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{
		ID:       "s1",
		User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID:   "r1",
		SiteID:   "site-a",
		JoinedAt: time.Now().UTC().Add(-time.Hour),
		Alert:    true,
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

	// Two subs with explicit lastSeenAt + one sub that has never been read
	// (no lastSeenAt). The unread sub MUST be excluded — being invited into a
	// room doesn't mean the user has read anything, so its joinedAt must not
	// pull the room floor down.
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", JoinedAt: earliest, LastSeenAt: &mid,
	}))
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{
		ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"},
		RoomID: "r1", JoinedAt: earliest, LastSeenAt: &latest,
	}))
	// Never-read sub: joined at `earliest` but never opened the room.
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{
		ID: "s3", User: model.SubscriptionUser{ID: "u3", Account: "carol"},
		RoomID: "r1", JoinedAt: earliest,
	}))

	got, err := store.MinSubscriptionLastSeenByRoomID(ctx, "r1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.WithinDuration(t, mid, *got, time.Second)

	// Room with subs but none has lastSeenAt — return nil so the caller can
	// $unset rooms.minUserLastSeenAt.
	require.NoError(t, store.CreateSubscription(ctx, &model.Subscription{
		ID: "s4", User: model.SubscriptionUser{ID: "u4", Account: "dave"},
		RoomID: "r2", JoinedAt: earliest,
	}))
	got, err = store.MinSubscriptionLastSeenByRoomID(ctx, "r2")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Empty room → nil.
	got, err = store.MinSubscriptionLastSeenByRoomID(ctx, "empty")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestMongoStore_UpdateRoomMinUserLastSeenAt_Integration(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)

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
