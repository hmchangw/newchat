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
	"github.com/gocql/gocql"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func setupMongo(t *testing.T) *mongo.Database {
	return testutil.MongoDB(t, "room_service_test")
}

func setupValkey(t *testing.T) roomkeystore.RoomKeyStore {
	t.Helper()
	return roomkeystore.NewValkeyClusterStoreFromClient(testutil.StartValkeyCluster(t), time.Hour)
}

func setupCassandra(t *testing.T) *gocql.Session {
	t.Helper()
	keyspace, adminSession, host := testutil.CassandraKeyspace(t, "room_service_test")
	cql := func(format string) string { return fmt.Sprintf(format, keyspace) }

	require.NoError(t, adminSession.Query(cql(`CREATE TYPE IF NOT EXISTS %s."Participant" (id TEXT, eng_name TEXT, company_name TEXT, app_id TEXT, app_name TEXT, is_bot BOOLEAN, account TEXT)`)).Exec())
	require.NoError(t, adminSession.Query(cql(`CREATE TABLE IF NOT EXISTS %s.messages_by_id (
		message_id TEXT,
		room_id TEXT,
		sender FROZEN<"Participant">,
		created_at TIMESTAMP,
		PRIMARY KEY (message_id, created_at)
	) WITH CLUSTERING ORDER BY (created_at DESC)`)).Exec())

	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.One
	cluster.DisableInitialHostLookup = true
	cluster.Keyspace = keyspace
	ksSession, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(func() { ksSession.Close() })
	return ksSession
}

func TestCassMessageReader_GetMessageRoomAndCreatedAt_Integration(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)

	createdAt := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender) VALUES (?, ?, ?, ?)`,
		"m1", "r1", createdAt,
		map[string]interface{}{"account": "alice", "id": "uA", "eng_name": "Alice"},
	).WithContext(ctx).Exec())

	reader := NewCassMessageReader(session)

	roomID, ts, sender, found, err := reader.GetMessageRoomAndCreatedAt(ctx, "m1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "r1", roomID)
	require.True(t, ts.Equal(createdAt), "createdAt mismatch: got %v, want %v", ts, createdAt)
	require.Equal(t, "alice", sender)

	_, _, _, found, err = reader.GetMessageRoomAndCreatedAt(ctx, "missing")
	require.NoError(t, err)
	require.False(t, found)
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

	// Test ListRooms
	mustInsertRoom(t, db, &model.Room{ID: "r2", Name: "random", Type: model.RoomTypeChannel})
	rooms, err := store.ListRooms(ctx)
	require.NoError(t, err)
	assert.Len(t, rooms, 2)

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
		assert.Empty(t, m.SectName)
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
		insertSub(t, db, model.Subscription{
			ID: "sub-a", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID: "r1", Roles: []model.Role{model.RoleOwner}, JoinedAt: base.Add(10 * time.Second),
		})
		insertSub(t, db, model.Subscription{
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
		assert.Equal(t, "Engineering", got[0].Member.SectName,
			"empty dept names must fall through to sect names; spec requires room-service output to match room-worker's two-pass tiebreak")
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

	t.Run("returns errInvalidOrg when neither sectId nor deptId matches", func(t *testing.T) {
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
	keyStore := setupValkey(t)
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
	publish := func(_ context.Context, subj string, data []byte) error {
		publishedSubj = subj
		publishedData = data
		return nil
	}
	handler := NewHandler(store, keyStore, nil, nil, "site-a", 1000, 500, 5*time.Second, publish, func(context.Context, string, []byte) error { return nil })

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
	keyStore := setupValkey(t)
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
	publish := func(_ context.Context, subj string, data []byte) error {
		publishedSubj = subj
		publishedData = data
		return nil
	}
	handler := NewHandler(store, keyStore, nil, nil, "site-a", 1000, 500, 5*time.Second, publish, func(context.Context, string, []byte) error { return nil })

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
	keyStore := setupValkey(t)
	store := NewMongoStore(db)

	ctx := context.Background()

	mustInsertRoom(t, db, &model.Room{ID: "target", Type: model.RoomTypeChannel, SiteID: "site-a"})
	mustInsertRoom(t, db, &model.Room{ID: "source", Type: model.RoomTypeChannel, SiteID: "site-a"})
	// Requester subscribed to target but NOT source
	mustInsertSub(t, db, &model.Subscription{RoomID: "target", User: model.SubscriptionUser{ID: "req", Account: "alice"}, Roles: []model.Role{model.RoleOwner}})

	// Same-site only: nil memberListClient is safe — request fails on the same-site
	// GetSubscription check before reaching the cross-site branch.
	handler := NewHandler(store, keyStore, nil, nil, "site-a", 1000, 500, 5*time.Second, func(context.Context, string, []byte) error { return nil }, func(context.Context, string, []byte) error { return nil })

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
	keyStore := setupValkey(t)

	storeA := NewMongoStore(dbA)
	storeB := NewMongoStore(dbB)

	otelNCb, err := otelnats.Connect(natsURLb)
	require.NoError(t, err)
	t.Cleanup(func() { _ = otelNCb.Drain() })

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

	// Site-B handler registers member.list endpoint (RegisterCRUD subscribes to MemberListWildcard).
	handlerB := NewHandler(storeB, keyStore, nil, nil, "site-b", 1000, 500, 5*time.Second, func(context.Context, string, []byte) error { return nil }, func(context.Context, string, []byte) error { return nil })
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
	handlerA := NewHandler(storeA, keyStore, memberListClient, nil, "site-a", 1000, 500, 5*time.Second, publish, func(context.Context, string, []byte) error { return nil })

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
	keyStore := setupValkey(t)
	store := NewMongoStore(db)
	otelNC, err := otelnats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = otelNC.Drain() })

	ctx := context.Background()

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
	handler := NewHandler(store, keyStore, memberListClient, nil, "site-a", 1000, 500, 200*time.Millisecond, func(context.Context, string, []byte) error { return nil }, func(context.Context, string, []byte) error { return nil })

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
	keyStore := setupValkey(t)
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

	otelNC, err := otelnats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = otelNC.Drain() })

	handler := NewHandler(store, keyStore, nil, nil, "site-a", 1000, 500, 5*time.Second, func(context.Context, string, []byte) error { return nil }, func(context.Context, string, []byte) error { return nil })
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

// TestIntegration_CreateRoom_PersistsKeyInValkey verifies that handleCreateRoom
// generates and stores a room keypair in Valkey before publishing the canonical
// event. This ensures room-worker's "key MUST exist" gate will always succeed
// on the first delivery.
func TestIntegration_CreateRoom_PersistsKeyInValkey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	db := setupMongo(t)
	store := NewMongoStore(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	keyStore := setupValkey(t)

	mustInsertUser(t, db, &model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-A",
		EngName: "Alice", ChineseName: "爱丽丝",
	})
	mustInsertUser(t, db, &model.User{
		ID: "u_bob", Account: "bob", SiteID: "site-A",
		EngName: "Bob", ChineseName: "鲍勃",
	})

	h, _ := newRoomServiceHandler(t, store, keyStore, "site-A")

	reqID := idgen.GenerateRequestID()
	ctx = natsutil.WithRequestID(ctx, reqID)

	body, err := json.Marshal(model.CreateRoomRequest{
		Name:  "crypto team",
		Users: []string{"bob"},
	})
	require.NoError(t, err)

	resp, err := h.handleCreateRoom(ctx, subject.RoomCreate("alice", "site-A"), body)
	require.NoError(t, err)

	var reply model.CreateRoomReply
	require.NoError(t, json.Unmarshal(resp, &reply))
	assert.Equal(t, model.CreateRoomReplyAccepted, reply.Status)
	assert.NotEmpty(t, reply.RoomID)

	// Assert the keypair was persisted to Valkey before the canonical event was published.
	pair, err := keyStore.Get(ctx, reply.RoomID)
	require.NoError(t, err)
	require.NotNil(t, pair, "room key must be stored in Valkey immediately after create")
	assert.NotEmpty(t, pair.KeyPair.PrivateKey, "private key must be non-empty")
	assert.Equal(t, 0, pair.Version, "freshly created room key must have version 0")
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
	h := NewHandler(store, keyStore, nil, nil, siteID, 1000, 500, 5*time.Second, publish, func(context.Context, string, []byte) error { return nil })
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

	keyStore := setupValkey(t)

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

	keyStore := setupValkey(t)

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
	mustInsertSub(t, db, &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", JoinedAt: earliest, LastSeenAt: &mid,
	})
	mustInsertSub(t, db, &model.Subscription{
		ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"},
		RoomID: "r1", JoinedAt: earliest, LastSeenAt: &latest,
	})
	// Never-read sub: joined at `earliest` but never opened the room.
	mustInsertSub(t, db, &model.Subscription{
		ID: "s3", User: model.SubscriptionUser{ID: "u3", Account: "carol"},
		RoomID: "r1", JoinedAt: earliest,
	})

	got, err := store.MinSubscriptionLastSeenByRoomID(ctx, "r1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.WithinDuration(t, mid, *got, time.Second)

	// Room with subs but none has lastSeenAt — return nil so the caller can
	// $unset rooms.minUserLastSeenAt.
	mustInsertSub(t, db, &model.Subscription{
		ID: "s4", User: model.SubscriptionUser{ID: "u4", Account: "dave"},
		RoomID: "r2", JoinedAt: earliest,
	})
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

	got, err := store.ToggleSubscriptionMute(ctx, "r1", "alice")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.Muted)
	assert.Equal(t, "alice", got.User.Account)
	assert.Equal(t, "r1", got.RoomID)

	persisted, err := store.GetSubscription(ctx, "alice", "r1")
	require.NoError(t, err)
	assert.True(t, persisted.Muted)

	got, err = store.ToggleSubscriptionMute(ctx, "r1", "alice")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.Muted)
	assert.Equal(t, "alice", got.User.Account)
	assert.Equal(t, "r1", got.RoomID)

	gotNil, err := store.ToggleSubscriptionMute(ctx, "missing", "alice")
	assert.Nil(t, gotNil)
	assert.ErrorIs(t, err, model.ErrSubscriptionNotFound)
}

// TestMongoStore_ListRoomMembers_OrgDisplay_DeptFirst_Integration verifies that
// when an org member's id matches both a user's deptId and another user's
// sectId, the dept branch wins and the combined "name tcName" string is
// surfaced via Member.SectName (the existing wire field for org display).
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
	assert.Equal(t, "Engineering 工程部", got[0].Member.SectName, "dept wins on overlap; name+tcName combined")
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
	assert.Equal(t, "Y", got[0].Member.SectName, "no matching users → falls back to member.id")
}
