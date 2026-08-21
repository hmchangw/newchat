//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/pwhash"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) {
	testutil.RunTestsWithPrewarm(m, testutil.EnsureMongoReplicaSet, testutil.EnsureNATS)
}

// seedSession inserts a session row directly into the shared sessions
// collection (owned by pkg/session, not this service's store).
func seedSession(t *testing.T, db *mongo.Database, sess session.Session) {
	t.Helper()
	_, err := db.Collection(session.Collection).InsertOne(context.Background(), sess)
	require.NoError(t, err)
}

// -------------------------------------------------------------------------
// CreateUser + unique-index
// -------------------------------------------------------------------------

func TestIntegration_CreateUser_And_UniqueIndex(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))

	// users.account unique is owned by user-service; create it here so admin's
	// duplicate-account path (ErrAccountExists) is exercised.
	_, ierr := db.Collection("users").Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys:    bson.D{{Key: "account", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, ierr)

	u := &model.User{
		ID:      idgen.GenerateUUIDv7(),
		Account: "alice",
		SiteID:  "site-a",
		Roles:   []model.UserRole{model.UserRoleUser},
	}
	require.NoError(t, st.CreateUser(context.Background(), u))

	// Second insert with same account → ErrAccountExists.
	dup := &model.User{
		ID:      idgen.GenerateUUIDv7(),
		Account: "alice",
		SiteID:  "site-a",
	}
	err := st.CreateUser(context.Background(), dup)
	assert.ErrorIs(t, err, ErrAccountExists)
}

// -------------------------------------------------------------------------
// SearchUsers
// -------------------------------------------------------------------------

func TestIntegration_SearchUsers(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))

	ctx := context.Background()

	// Seed users across two sites.
	users := []model.User{
		{ID: idgen.GenerateUUIDv7(), Account: "alice", SiteID: "site-a", EngName: "Alice Smith"},
		{ID: idgen.GenerateUUIDv7(), Account: "bob", SiteID: "site-a", EngName: "Bob Jones", ChineseName: "阿鮑"},
		{ID: idgen.GenerateUUIDv7(), Account: "charlie", SiteID: "site-b", EngName: "Charlie"},
	}
	for i := range users {
		require.NoError(t, st.CreateUser(ctx, &users[i]))
	}

	t.Run("lists every site's users – no site filter", func(t *testing.T) {
		results, total, err := st.SearchUsers(ctx, "", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, results, 3)
		sites := make(map[string]bool)
		for _, u := range results {
			sites[u.SiteID] = true
		}
		assert.True(t, sites["site-a"] && sites["site-b"], "both sites' users must appear")
	})

	t.Run("filter by q matches account", func(t *testing.T) {
		results, total, err := st.SearchUsers(ctx, "alice", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Equal(t, "alice", results[0].Account)
	})

	t.Run("filter by q matches engName", func(t *testing.T) {
		_, total, err := st.SearchUsers(ctx, "Smith", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
	})

	t.Run("filter by q matches chineseName", func(t *testing.T) {
		results, total, err := st.SearchUsers(ctx, "阿鮑", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Equal(t, "bob", results[0].Account)
	})

	t.Run("pagination – page 1 limit 1", func(t *testing.T) {
		results, total, err := st.SearchUsers(ctx, "", 1, 1)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, results, 1)
	})

	t.Run("pagination – page 3 limit 1", func(t *testing.T) {
		results, total, err := st.SearchUsers(ctx, "", 3, 1)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, results, 1)
	})

	t.Run("no match returns empty slice", func(t *testing.T) {
		results, total, err := st.SearchUsers(ctx, "zzznomatch", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(0), total)
		assert.Empty(t, results)
	})
}

// -------------------------------------------------------------------------
// GetUserByAccount
// -------------------------------------------------------------------------

func TestIntegration_GetUserByAccount(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))

	ctx := context.Background()
	u := &model.User{
		ID:      idgen.GenerateUUIDv7(),
		Account: "diana",
		SiteID:  "site-a",
	}
	require.NoError(t, st.CreateUser(ctx, u))

	t.Run("hit", func(t *testing.T) {
		got, err := st.GetUserByAccount(ctx, "site-a", u.Account)
		require.NoError(t, err)
		assert.Equal(t, u.ID, got.ID)
		assert.Equal(t, "diana", got.Account)
	})

	t.Run("miss returns ErrUserNotFound", func(t *testing.T) {
		_, err := st.GetUserByAccount(ctx, "site-a", "no-such-account")
		assert.ErrorIs(t, err, ErrUserNotFound)
	})

	t.Run("wrong site returns ErrUserNotFound", func(t *testing.T) {
		_, err := st.GetUserByAccount(ctx, "site-b", u.Account)
		assert.ErrorIs(t, err, ErrUserNotFound, "a site-b admin must not read a site-a user by account")
	})
}

// -------------------------------------------------------------------------
// UpdateUser
// -------------------------------------------------------------------------

func TestIntegration_UpdateUser(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))

	ctx := context.Background()
	u := &model.User{
		ID:      idgen.GenerateUUIDv7(),
		Account: "eve",
		SiteID:  "site-a",
		Roles:   []model.UserRole{model.UserRoleUser},
		// Sentinel credential material so the returned-doc projection guard
		// below is real: a widened fanoutProjection would surface this.
		Services: model.Services{Password: model.PasswordCredentials{Bcrypt: "seeded-hash-must-not-be-projected"}},
	}
	require.NoError(t, st.CreateUser(ctx, u))

	t.Run("update roles", func(t *testing.T) {
		newRoles := []model.UserRole{model.UserRoleAdmin}
		updated, err := st.UpdateUser(ctx, "site-a", u.Account, UserUpdate{Roles: &newRoles})
		require.NoError(t, err)

		// The returned doc is the POST-write state (ReturnDocument=After) — the
		// fanout publishes it, so a Before doc would ship stale roles.
		require.NotNil(t, updated)
		assert.Equal(t, []model.UserRole{model.UserRoleAdmin}, updated.Roles)
		assert.Equal(t, u.ID, updated.ID)
		assert.Equal(t, u.Account, updated.Account)
		assert.Equal(t, "site-a", updated.SiteID)
		assert.Empty(t, updated.Services.Password.Bcrypt, "fanoutProjection must never return credential material")

		got, err := st.GetUserByAccount(ctx, "site-a", u.Account)
		require.NoError(t, err)
		assert.Equal(t, []model.UserRole{model.UserRoleAdmin}, got.Roles)
	})

	t.Run("update active on this path does not touch sessions (handler routes active=false to DeactivateAndRevoke instead)", func(t *testing.T) {
		sessStore := session.NewMongoStore(db)
		seedSession(t, db, session.Session{ID: "eve-sess-1", UserID: u.ID, Account: u.Account, SiteID: "site-a", IssuedAt: 1})

		inactive := false
		_, err := st.UpdateUser(ctx, "site-a", u.Account, UserUpdate{Active: &inactive})
		require.NoError(t, err)

		got, err := st.GetUserByAccount(ctx, "site-a", u.Account)
		require.NoError(t, err)
		assert.False(t, got.IsActive())

		sessions, err := sessStore.ListForAccount(ctx, "site-a", u.Account)
		require.NoError(t, err)
		assert.Len(t, sessions, 1, "store.UpdateUser must leave sessions alone — revocation is the handler's job now")
	})

	t.Run("update names", func(t *testing.T) {
		eng := "Eve Updated"
		cn := "更新伊芙"
		_, err := st.UpdateUser(ctx, "site-a", u.Account, UserUpdate{EngName: &eng, ChineseName: &cn})
		require.NoError(t, err)

		got, err := st.GetUserByAccount(ctx, "site-a", u.Account)
		require.NoError(t, err)
		assert.Equal(t, "Eve Updated", got.EngName)
		assert.Equal(t, "更新伊芙", got.ChineseName)
	})

	t.Run("no-op when all fields nil", func(t *testing.T) {
		updated, err := st.UpdateUser(ctx, "site-a", u.Account, UserUpdate{})
		require.NoError(t, err)
		// (nil, nil) on an empty patch — the fanout branches on this to skip publishing.
		assert.Nil(t, updated)
	})

	t.Run("nonexistent id returns ErrUserNotFound", func(t *testing.T) {
		eng := "Ghost"
		_, err := st.UpdateUser(ctx, "site-a", "nonexistent-account", UserUpdate{EngName: &eng})
		assert.ErrorIs(t, err, ErrUserNotFound)
	})
}

// -------------------------------------------------------------------------
// UpdateUserPasswordAndRevoke
// -------------------------------------------------------------------------

func TestIntegration_UpdateUserPasswordAndRevoke(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))

	ctx := context.Background()
	u := &model.User{
		ID:                    idgen.GenerateUUIDv7(),
		Account:               "frank",
		SiteID:                "site-a",
		RequirePasswordChange: true,
	}
	require.NoError(t, st.CreateUser(ctx, u))

	t.Run("sets hash and requirePasswordChange=false", func(t *testing.T) {
		err := st.UpdateUserPasswordAndRevoke(ctx, "site-a", u.Account, "$2a$04$fakehash", false, "")
		require.NoError(t, err)

		// Read back via raw projection to check services.password.bcrypt.
		var raw struct {
			RequirePasswordChange bool `bson:"requirePasswordChange"`
			Services              struct {
				Password struct {
					Bcrypt string `bson:"bcrypt"`
				} `bson:"password"`
			} `bson:"services"`
		}
		err = db.Collection("users").FindOne(ctx, bson.M{"_id": u.ID},
			options.FindOne().SetProjection(bson.M{"requirePasswordChange": 1, "services.password.bcrypt": 1}),
		).Decode(&raw)
		require.NoError(t, err)
		assert.Equal(t, "$2a$04$fakehash", raw.Services.Password.Bcrypt)
		assert.False(t, raw.RequirePasswordChange)
	})

	t.Run("sets requirePasswordChange=true", func(t *testing.T) {
		err := st.UpdateUserPasswordAndRevoke(ctx, "site-a", u.Account, "$2a$04$anotherhash", true, "")
		require.NoError(t, err)

		var raw struct {
			RequirePasswordChange bool `bson:"requirePasswordChange"`
		}
		err = db.Collection("users").FindOne(ctx, bson.M{"_id": u.ID},
			options.FindOne().SetProjection(bson.M{"requirePasswordChange": 1}),
		).Decode(&raw)
		require.NoError(t, err)
		assert.True(t, raw.RequirePasswordChange)
	})

	t.Run("exceptSessionID=\"\" revokes every session for the account", func(t *testing.T) {
		sessStore := session.NewMongoStore(db)
		seedSession(t, db, session.Session{ID: "frank-sess-1", UserID: u.ID, Account: u.Account, SiteID: "site-a", IssuedAt: 1})
		seedSession(t, db, session.Session{ID: "frank-sess-2", UserID: u.ID, Account: u.Account, SiteID: "site-a", IssuedAt: 2})

		err := st.UpdateUserPasswordAndRevoke(ctx, "site-a", u.Account, "$2a$04$rotatedhash", false, "")
		require.NoError(t, err)

		sessions, err := sessStore.ListForAccount(ctx, "site-a", u.Account)
		require.NoError(t, err)
		assert.Empty(t, sessions, "exceptSessionID=\"\" (admin setPassword) must kill every session for the account")
	})

	t.Run("exceptSessionID=<id> preserves caller session, revokes siblings", func(t *testing.T) {
		sessStore := session.NewMongoStore(db)
		seedSession(t, db, session.Session{ID: "frank-caller", UserID: u.ID, Account: u.Account, SiteID: "site-a", IssuedAt: 10})
		seedSession(t, db, session.Session{ID: "frank-sibling-a", UserID: u.ID, Account: u.Account, SiteID: "site-a", IssuedAt: 11})
		seedSession(t, db, session.Session{ID: "frank-sibling-b", UserID: u.ID, Account: u.Account, SiteID: "site-a", IssuedAt: 12})

		err := st.UpdateUserPasswordAndRevoke(ctx, "site-a", u.Account, "$2a$04$callerkeeps", false, "frank-caller")
		require.NoError(t, err)

		sessions, err := sessStore.ListForAccount(ctx, "site-a", u.Account)
		require.NoError(t, err)
		require.Len(t, sessions, 1, "only the caller's session should survive")
		assert.Equal(t, "frank-caller", sessions[0].ID)
	})

	t.Run("nonexistent id returns ErrUserNotFound", func(t *testing.T) {
		err := st.UpdateUserPasswordAndRevoke(ctx, "site-a", "nonexistent-account", "$2a$04$fakehash", false, "")
		assert.ErrorIs(t, err, ErrUserNotFound)
	})
}

// -------------------------------------------------------------------------
// DeactivateAndRevoke
// -------------------------------------------------------------------------

func TestIntegration_DeactivateAndRevoke(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))

	ctx := context.Background()
	u := &model.User{
		ID:      idgen.GenerateUUIDv7(),
		Account: "gwen",
		SiteID:  "site-a",
	}
	require.NoError(t, st.CreateUser(ctx, u))

	t.Run("sets active=false and kills every session for the account atomically", func(t *testing.T) {
		sessStore := session.NewMongoStore(db)
		seedSession(t, db, session.Session{ID: "gwen-sess-1", UserID: u.ID, Account: u.Account, SiteID: "site-a", IssuedAt: 1})
		seedSession(t, db, session.Session{ID: "gwen-sess-2", UserID: u.ID, Account: u.Account, SiteID: "site-a", IssuedAt: 2})

		updated, err := st.DeactivateAndRevoke(ctx, "site-a", u.Account)
		require.NoError(t, err)

		// Post-write doc (ReturnDocument=After): active must already be false,
		// otherwise the fanout would publish the user as still active.
		require.NotNil(t, updated)
		assert.Equal(t, u.ID, updated.ID)
		assert.False(t, updated.IsActive())

		var raw struct {
			Active *bool `bson:"active"`
		}
		err = db.Collection("users").FindOne(ctx, bson.M{"_id": u.ID},
			options.FindOne().SetProjection(bson.M{"active": 1}),
		).Decode(&raw)
		require.NoError(t, err)
		require.NotNil(t, raw.Active, "deactivation must write an explicit active:false")
		assert.False(t, *raw.Active)

		sessions, err := sessStore.ListForAccount(ctx, "site-a", u.Account)
		require.NoError(t, err)
		assert.Empty(t, sessions, "deactivate must kill every session for the account")
	})

	t.Run("nonexistent account returns ErrUserNotFound", func(t *testing.T) {
		_, err := st.DeactivateAndRevoke(ctx, "site-a", "ghost-account")
		assert.ErrorIs(t, err, ErrUserNotFound)
	})
}

// -------------------------------------------------------------------------
// Sessions
// -------------------------------------------------------------------------

// TestIntegration_Sessions exercises the session.Store surface the way
// admin-service's handlers use it (listSessions/revokeSession/revokeAllSessions
// and the middleware's FindByHash). Insert/FindByHash/DeleteBeyondCap/
// DeleteForAccountExcept already have their own coverage in
// pkg/session/session_integration_test.go; this focuses on the
// site+account-scoped surface (ListForAccount/DeleteByID/DeleteForAccount)
// that only admin-service calls.
func TestIntegration_Sessions(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	sessStore := session.NewMongoStore(db)
	require.NoError(t, sessStore.EnsureIndexes(context.Background()))

	ctx := context.Background()
	userID := idgen.GenerateUUIDv7()
	otherUserID := idgen.GenerateUUIDv7()

	// Seed sessions directly.
	sessA := session.Session{ID: "hash-a", UserID: userID, Account: "grace", SiteID: "site-a", Roles: []string{"admin"}, IssuedAt: 1000}
	sessB := session.Session{ID: "hash-b", UserID: userID, Account: "grace", SiteID: "site-a", Roles: []string{"admin"}, IssuedAt: 2000}
	sessC := session.Session{ID: "hash-c", UserID: otherUserID, Account: "other", SiteID: "site-a", Roles: []string{"user"}, IssuedAt: 3000}
	seedSession(t, db, sessA)
	seedSession(t, db, sessB)
	seedSession(t, db, sessC)

	t.Run("FindByHash hit", func(t *testing.T) {
		got, err := sessStore.FindByHash(ctx, "hash-a")
		require.NoError(t, err)
		assert.Equal(t, userID, got.UserID)
	})

	t.Run("FindByHash miss returns session.ErrNotFound", func(t *testing.T) {
		_, err := sessStore.FindByHash(ctx, "no-such-hash")
		assert.ErrorIs(t, err, session.ErrNotFound)
	})

	t.Run("ListForAccount returns only that account's sessions", func(t *testing.T) {
		sessions, err := sessStore.ListForAccount(ctx, "site-a", "grace")
		require.NoError(t, err)
		assert.Len(t, sessions, 2)
		for _, s := range sessions {
			assert.Equal(t, "grace", s.Account)
		}
	})

	t.Run("ListForAccount returns empty for unknown account", func(t *testing.T) {
		sessions, err := sessStore.ListForAccount(ctx, "site-a", "no-such-account")
		require.NoError(t, err)
		assert.Empty(t, sessions)
	})

	t.Run("cross-site: wrong site cannot list or revoke another site's sessions", func(t *testing.T) {
		// grace's sessions live on site-a; a site-b admin must see nothing and
		// revoke nothing.
		sessions, err := sessStore.ListForAccount(ctx, "site-b", "grace")
		require.NoError(t, err)
		assert.Empty(t, sessions, "site-b admin must not see site-a sessions")

		n, err := sessStore.DeleteByID(ctx, "site-b", "grace", "hash-a")
		require.NoError(t, err)
		assert.Equal(t, int64(0), n, "site-b admin must not revoke a site-a session")

		n, err = sessStore.DeleteForAccount(ctx, "site-b", "grace")
		require.NoError(t, err)
		assert.Equal(t, int64(0), n, "site-b admin must not revoke site-a sessions in bulk")

		// hash-a survives the cross-site attempts.
		got, err := sessStore.FindByHash(ctx, "hash-a")
		require.NoError(t, err)
		assert.Equal(t, "grace", got.Account)
	})

	t.Run("DeleteByID account-scoped: wrong account does NOT delete", func(t *testing.T) {
		// Try to delete sessA using the wrong account.
		n, err := sessStore.DeleteByID(ctx, "site-a", "other", "hash-a")
		require.NoError(t, err)
		assert.Equal(t, int64(0), n, "session belonging to a different account must not be deleted")

		// Verify sessA still exists.
		got, err := sessStore.FindByHash(ctx, "hash-a")
		require.NoError(t, err)
		assert.Equal(t, userID, got.UserID)
	})

	t.Run("DeleteByID account-scoped: correct account deletes", func(t *testing.T) {
		n, err := sessStore.DeleteByID(ctx, "site-a", "grace", "hash-a")
		require.NoError(t, err)
		assert.Equal(t, int64(1), n)

		_, err = sessStore.FindByHash(ctx, "hash-a")
		assert.ErrorIs(t, err, session.ErrNotFound)
	})

	t.Run("DeleteForAccount removes all of the account's sessions", func(t *testing.T) {
		n, err := sessStore.DeleteForAccount(ctx, "site-a", "grace")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, n, int64(1)) // hash-b remains at this point

		sessions, err := sessStore.ListForAccount(ctx, "site-a", "grace")
		require.NoError(t, err)
		assert.Empty(t, sessions)
	})

	t.Run("DeleteForAccount removes the other account's sessions only", func(t *testing.T) {
		// hash-c belongs to the "other" account and is still present.
		n, err := sessStore.DeleteForAccount(ctx, "site-a", "other")
		require.NoError(t, err)
		assert.Equal(t, int64(1), n)

		_, err = sessStore.FindByHash(ctx, "hash-c")
		assert.ErrorIs(t, err, session.ErrNotFound)
	})
}

// -------------------------------------------------------------------------
// Audit: AppendAudit + ListAudit (newest-first, site-scoped, filtered, paged)
// -------------------------------------------------------------------------

func TestIntegration_Audit(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))

	ctx := context.Background()
	targetAccount := "grace"
	now := time.Now().UTC().UnixMilli()

	entries := []AuditEntry{
		{ID: idgen.GenerateUUIDv7(), ActorUserID: "admin1", ActorAccount: "p_alice", Action: "user.create", TargetAccount: targetAccount, SiteID: "site-a", Timestamp: now - 3000},
		{ID: idgen.GenerateUUIDv7(), ActorUserID: "admin1", ActorAccount: "p_alice", Action: "user.update", TargetAccount: targetAccount, SiteID: "site-a", Timestamp: now - 2000},
		{ID: idgen.GenerateUUIDv7(), ActorUserID: "admin2", ActorAccount: "p_bob", Action: "user.create", TargetAccount: "other-account", SiteID: "site-a", Timestamp: now - 1000},
		{ID: idgen.GenerateUUIDv7(), ActorUserID: "admin1", ActorAccount: "p_alice", Action: "user.create", TargetAccount: targetAccount, SiteID: "site-b", Timestamp: now},
	}
	for i := range entries {
		require.NoError(t, st.AppendAudit(ctx, &entries[i]))
	}

	t.Run("site-scoped returns only site-a entries", func(t *testing.T) {
		results, total, err := st.ListAudit(ctx, "site-a", AuditFilter{}, 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, results, 3)
		for _, e := range results {
			assert.Equal(t, "site-a", e.SiteID)
		}
	})

	t.Run("newest-first ordering", func(t *testing.T) {
		results, _, err := st.ListAudit(ctx, "site-a", AuditFilter{}, 1, 10)
		require.NoError(t, err)
		require.Len(t, results, 3)
		// Results must be descending by timestamp.
		assert.Greater(t, results[0].Timestamp, results[1].Timestamp)
		assert.Greater(t, results[1].Timestamp, results[2].Timestamp)
	})

	t.Run("filter by targetAccount", func(t *testing.T) {
		results, total, err := st.ListAudit(ctx, "site-a", AuditFilter{TargetAccount: targetAccount}, 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		for _, e := range results {
			assert.Equal(t, targetAccount, e.TargetAccount)
		}
	})

	t.Run("filter by actor (actorAccount)", func(t *testing.T) {
		results, total, err := st.ListAudit(ctx, "site-a", AuditFilter{Actor: "p_bob"}, 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Equal(t, "p_bob", results[0].ActorAccount)
	})

	t.Run("filter by action", func(t *testing.T) {
		results, total, err := st.ListAudit(ctx, "site-a", AuditFilter{Action: "user.update"}, 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Equal(t, "user.update", results[0].Action)
	})

	t.Run("pagination – page 1 limit 2", func(t *testing.T) {
		results, total, err := st.ListAudit(ctx, "site-a", AuditFilter{}, 1, 2)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, results, 2)
	})

	t.Run("pagination – page 2 limit 2", func(t *testing.T) {
		results, total, err := st.ListAudit(ctx, "site-a", AuditFilter{}, 2, 2)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, results, 1)
	})

	t.Run("no match returns empty with total 0", func(t *testing.T) {
		results, total, err := st.ListAudit(ctx, "site-a", AuditFilter{Action: "no.such.action"}, 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(0), total)
		assert.Empty(t, results)
	})
}

// -------------------------------------------------------------------------
// EnsureIndexes
// -------------------------------------------------------------------------

func TestIntegration_EnsureIndexes_Keys(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))

	userKeys := testutil.IndexSpecs(t, db.Collection("users"))
	// No owned users index: account_1 is user-service's, and the old
	// {siteId, account} compound died with SearchUsers' site filter (R7).
	require.NotContains(t, userKeys, "siteId:1,account:1")

	auditKeys := testutil.IndexSpecs(t, db.Collection("admin_audit"))
	require.Contains(t, auditKeys, "siteId:1,timestamp:-1")
	require.Contains(t, auditKeys, "siteId:1,targetAccount:1,timestamp:-1")
}

func TestIntegration_EnsureIndexes_Idempotent(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)

	// First call.
	require.NoError(t, st.EnsureIndexes(context.Background()), "first EnsureIndexes must succeed")
	// Second call must also succeed (idempotent).
	require.NoError(t, st.EnsureIndexes(context.Background()), "second EnsureIndexes must be idempotent")
}

// -------------------------------------------------------------------------
// Permission grants: RecordPermissionChange (transactional ledger + state) +
// GetUserPermissions (materialized state, site-unfiltered) + ListPermissionGrants
// (company-wide ledger browse) + FindAccountStates (company-wide) + AppendAuditMany
// -------------------------------------------------------------------------

func TestIntegration_RecordPermissionChange_AllOrNothing(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	preexisting := &model.PermissionGrant{
		ID: "dup-id", Permission: model.PermissionExternalImageView,
		SubjectAccount: "zoe", Granted: false, ApplicantAccount: "carol", ApproverAccount: "dave",
		Reason: "pre-existing row", RecordedBy: "p_admin", RecordedAt: time.Now().UTC(),
	}
	preexistingState := model.PermissionState{Granted: false, UpdatedAt: preexisting.RecordedAt}
	require.NoError(t, st.RecordPermissionChange(ctx, []*model.PermissionGrant{preexisting}, preexistingState))

	now := time.Now().UTC()
	batch := []*model.PermissionGrant{
		{ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView, SubjectAccount: "alice", Granted: true, EffectiveFrom: timePtr(now), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row 1", RecordedBy: "p_admin", RecordedAt: now},
		{ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView, SubjectAccount: "bob", Granted: true, EffectiveFrom: timePtr(now), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row 2", RecordedBy: "p_admin", RecordedAt: now},
		{ID: "dup-id", Permission: model.PermissionExternalImageView, SubjectAccount: "carl", Granted: true, EffectiveFrom: timePtr(now), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row 3 — collides with preexisting _id", RecordedBy: "p_admin", RecordedAt: now},
	}
	batchState := model.PermissionState{Granted: true, EffectiveFrom: timePtr(now), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), UpdatedAt: now}

	err := st.RecordPermissionChange(ctx, batch, batchState)
	require.Error(t, err, "a duplicate _id inside the batch must fail the whole transaction")

	count, err := st.permGrants.CountDocuments(ctx, bson.M{"subjectAccount": bson.M{"$in": []string{"alice", "bob"}}})
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "the transaction must roll back alice and bob too, not just the colliding row")

	total, err := st.permGrants.CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total, "only the pre-existing row should remain")

	t.Run("empty batch is a no-op, not an error", func(t *testing.T) {
		require.NoError(t, st.RecordPermissionChange(ctx, nil, model.PermissionState{}))
	})
}

// TestRecordPermissionChange_TransactionAppliesLedgerAndState covers the
// $exists:false guard arm: brand-new user docs with no permissions.externalImageView
// field yet must still receive the state (the guard only blocks a STORED newer
// watermark, not an absent one).
func TestIntegration_RecordPermissionChange_TransactionAppliesLedgerAndState(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "harry", SiteID: "site-a"}))
	require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "ivy", SiteID: "site-a"}))

	// Truncate to millisecond: Mongo stores time.Time as BSON datetime (int64 ms),
	// so an untruncated in-memory value never round-trips equal to what's stored.
	now := time.Now().UTC().Truncate(time.Millisecond)
	state := model.PermissionState{
		Granted:       true,
		EffectiveFrom: timePtr(now),
		ExpiresAt:     timePtr(now.AddDate(0, 1, 0)),
		UpdatedAt:     now,
	}
	grants := []*model.PermissionGrant{
		{ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView, SubjectAccount: "harry", Granted: true, EffectiveFrom: state.EffectiveFrom, ExpiresAt: state.ExpiresAt, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch", RecordedBy: "p_admin", RecordedAt: now},
		{ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView, SubjectAccount: "ivy", Granted: true, EffectiveFrom: state.EffectiveFrom, ExpiresAt: state.ExpiresAt, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch", RecordedBy: "p_admin", RecordedAt: now},
	}

	require.NoError(t, st.RecordPermissionChange(ctx, grants, state))

	count, err := st.permGrants.CountDocuments(ctx, bson.M{"subjectAccount": bson.M{"$in": []string{"harry", "ivy"}}})
	require.NoError(t, err)
	assert.Equal(t, int64(2), count, "both ledger rows must exist")

	for _, account := range []string{"harry", "ivy"} {
		var raw model.User
		err := db.Collection("users").FindOne(ctx, bson.M{"account": account},
			options.FindOne().SetProjection(bson.M{"permissions": 1}),
		).Decode(&raw)
		require.NoError(t, err)
		require.NotNil(t, raw.Permissions, "account %s must carry a permissions snapshot", account)
		got := raw.Permissions.ExternalImageView
		require.NotNil(t, got, "account %s must carry externalImageView", account)
		assert.Equal(t, state.Granted, got.Granted)
		require.NotNil(t, got.EffectiveFrom)
		require.NotNil(t, got.ExpiresAt)
		assert.True(t, state.EffectiveFrom.Equal(*got.EffectiveFrom))
		assert.True(t, state.ExpiresAt.Equal(*got.ExpiresAt))
		assert.True(t, state.UpdatedAt.Equal(got.UpdatedAt))
	}
}

// TestRecordPermissionChange_GuardKeepsNewerState covers the $lte guard arm from
// both sides of the boundary: a strictly newer stored watermark blocks the write
// (ledger row still lands), an equal watermark does not (spec's half-open $lte).
func TestIntegration_RecordPermissionChange_GuardKeepsNewerState(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	// Truncate to millisecond: Mongo stores time.Time as BSON datetime (int64 ms),
	// so an untruncated in-memory value never round-trips equal to what's stored.
	now := time.Now().UTC().Truncate(time.Millisecond)
	state := model.PermissionState{
		Granted:       true,
		EffectiveFrom: timePtr(now),
		ExpiresAt:     timePtr(now.AddDate(0, 1, 0)),
		UpdatedAt:     now,
	}

	t.Run("stored state is newer — guard blocks the update, ledger still gets the row", func(t *testing.T) {
		require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "jack", SiteID: "site-a"}))

		newerState := model.PermissionState{Granted: false, UpdatedAt: now.Add(time.Second)}
		_, err := db.Collection("users").UpdateOne(ctx,
			bson.M{"account": "jack"},
			bson.M{"$set": bson.M{"permissions.externalImageView": newerState}},
		)
		require.NoError(t, err)

		grant := &model.PermissionGrant{
			ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView,
			SubjectAccount: "jack", Granted: true, EffectiveFrom: state.EffectiveFrom, ExpiresAt: state.ExpiresAt,
			ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r", RecordedBy: "p_admin", RecordedAt: now,
		}
		require.NoError(t, st.RecordPermissionChange(ctx, []*model.PermissionGrant{grant}, state))

		count, err := st.permGrants.CountDocuments(ctx, bson.M{"_id": grant.ID})
		require.NoError(t, err)
		assert.Equal(t, int64(1), count, "ledger row must be inserted regardless of the guard")

		var raw model.User
		require.NoError(t, db.Collection("users").FindOne(ctx, bson.M{"account": "jack"},
			options.FindOne().SetProjection(bson.M{"permissions": 1}),
		).Decode(&raw))
		require.NotNil(t, raw.Permissions)
		require.NotNil(t, raw.Permissions.ExternalImageView)
		assert.False(t, raw.Permissions.ExternalImageView.Granted, "user doc must keep the newer stored state, unchanged by the older write")
		assert.True(t, newerState.UpdatedAt.Equal(raw.Permissions.ExternalImageView.UpdatedAt))
	})

	t.Run("stored state has an equal updatedAt — state is applied ($lte)", func(t *testing.T) {
		require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "kate", SiteID: "site-a"}))

		equalState := model.PermissionState{Granted: false, UpdatedAt: state.UpdatedAt}
		_, err := db.Collection("users").UpdateOne(ctx,
			bson.M{"account": "kate"},
			bson.M{"$set": bson.M{"permissions.externalImageView": equalState}},
		)
		require.NoError(t, err)

		grant := &model.PermissionGrant{
			ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView,
			SubjectAccount: "kate", Granted: true, EffectiveFrom: state.EffectiveFrom, ExpiresAt: state.ExpiresAt,
			ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r", RecordedBy: "p_admin", RecordedAt: now,
		}
		require.NoError(t, st.RecordPermissionChange(ctx, []*model.PermissionGrant{grant}, state))

		var raw model.User
		require.NoError(t, db.Collection("users").FindOne(ctx, bson.M{"account": "kate"},
			options.FindOne().SetProjection(bson.M{"permissions": 1}),
		).Decode(&raw))
		require.NotNil(t, raw.Permissions)
		require.NotNil(t, raw.Permissions.ExternalImageView)
		assert.True(t, raw.Permissions.ExternalImageView.Granted, "equal updatedAt must still apply — $lte, not strictly less-than")
		assert.True(t, state.UpdatedAt.Equal(raw.Permissions.ExternalImageView.UpdatedAt))
	})
}

// TestRecordPermissionChange_MissingUserIsNoop covers a subject account that
// exists in the ledger's world but has no local user document (e.g. homed at
// another site with no cached doc here yet) — the ledger append must still
// succeed and the state-apply UpdateMany must not fabricate a user doc.
func TestIntegration_RecordPermissionChange_MissingUserIsNoop(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	now := time.Now().UTC()
	state := model.PermissionState{Granted: true, EffectiveFrom: timePtr(now), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), UpdatedAt: now}
	grant := &model.PermissionGrant{
		ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView,
		SubjectAccount: "ghost-account-no-doc", Granted: true, EffectiveFrom: state.EffectiveFrom, ExpiresAt: state.ExpiresAt,
		ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r", RecordedBy: "p_admin", RecordedAt: now,
	}

	require.NoError(t, st.RecordPermissionChange(ctx, []*model.PermissionGrant{grant}, state))

	count, err := st.permGrants.CountDocuments(ctx, bson.M{"_id": grant.ID})
	require.NoError(t, err)
	assert.Equal(t, int64(1), count, "ledger row must be inserted even though no user doc exists")

	userCount, err := db.Collection("users").CountDocuments(ctx, bson.M{"account": "ghost-account-no-doc"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), userCount, "RecordPermissionChange must never create a user document")
}

func TestIntegration_GetUserPermissions(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	t.Run("account does not exist → (nil, nil)", func(t *testing.T) {
		got, err := st.GetUserPermissions(ctx, "no-such-account")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("account exists with no permissions snapshot → (nil, nil)", func(t *testing.T) {
		require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "liam", SiteID: "site-a"}))

		got, err := st.GetUserPermissions(ctx, "liam")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("account with a materialized permission state round-trips", func(t *testing.T) {
		require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "mia", SiteID: "site-a"}))

		// Truncate to millisecond: Mongo stores time.Time as BSON datetime (int64 ms),
		// so an untruncated in-memory value never round-trips equal to what's stored.
		now := time.Now().UTC().Truncate(time.Millisecond)
		state := model.PermissionState{
			Granted:       true,
			EffectiveFrom: timePtr(now),
			ExpiresAt:     timePtr(now.AddDate(0, 1, 0)),
			UpdatedAt:     now,
		}
		grant := &model.PermissionGrant{
			ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView,
			SubjectAccount: "mia", Granted: true, EffectiveFrom: state.EffectiveFrom, ExpiresAt: state.ExpiresAt,
			ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r", RecordedBy: "p_admin", RecordedAt: now,
		}
		require.NoError(t, st.RecordPermissionChange(ctx, []*model.PermissionGrant{grant}, state))

		got, err := st.GetUserPermissions(ctx, "mia")
		require.NoError(t, err)
		require.NotNil(t, got)
		require.NotNil(t, got.ExternalImageView)
		assert.Equal(t, state.Granted, got.ExternalImageView.Granted)
		require.NotNil(t, got.ExternalImageView.EffectiveFrom)
		require.NotNil(t, got.ExternalImageView.ExpiresAt)
		assert.True(t, state.EffectiveFrom.Equal(*got.ExternalImageView.EffectiveFrom))
		assert.True(t, state.ExpiresAt.Equal(*got.ExternalImageView.ExpiresAt))
		assert.True(t, state.UpdatedAt.Equal(got.ExternalImageView.UpdatedAt))
	})

	t.Run("site-unfiltered: account homed at a different site still returns its materialized state", func(t *testing.T) {
		require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "noah", SiteID: "site-b"}))

		now := time.Now().UTC().Truncate(time.Millisecond)
		state := model.PermissionState{
			Granted:       true,
			EffectiveFrom: timePtr(now),
			ExpiresAt:     timePtr(now.AddDate(0, 1, 0)),
			UpdatedAt:     now,
		}
		grant := &model.PermissionGrant{
			ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView,
			SubjectAccount: "noah", Granted: true, EffectiveFrom: state.EffectiveFrom, ExpiresAt: state.ExpiresAt,
			ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r", RecordedBy: "p_admin", RecordedAt: now,
		}
		require.NoError(t, st.RecordPermissionChange(ctx, []*model.PermissionGrant{grant}, state))

		got, err := st.GetUserPermissions(ctx, "noah")
		require.NoError(t, err)
		require.NotNil(t, got, "a site-filtered query would find nothing for a site-b account")
		require.NotNil(t, got.ExternalImageView)
		assert.True(t, got.ExternalImageView.Granted)
	})
}

// TestIntegration_GetUserPermissionsForAccounts covers the resync endpoint's batch read:
// a mix of an account with a materialized state, an account whose doc exists but never had
// the permission recorded, and an account with no doc at all.
func TestIntegration_GetUserPermissionsForAccounts(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	// oscar: materialized state for the permission key, via RecordPermissionChange.
	require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "oscar", SiteID: "site-a"}))
	now := time.Now().UTC().Truncate(time.Millisecond)
	state := model.PermissionState{
		Granted:       true,
		EffectiveFrom: timePtr(now),
		ExpiresAt:     timePtr(now.AddDate(0, 1, 0)),
		UpdatedAt:     now,
	}
	grant := &model.PermissionGrant{
		ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView,
		SubjectAccount: "oscar", Granted: true, EffectiveFrom: state.EffectiveFrom, ExpiresAt: state.ExpiresAt,
		ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r", RecordedBy: "p_admin", RecordedAt: now,
	}
	require.NoError(t, st.RecordPermissionChange(ctx, []*model.PermissionGrant{grant}, state))

	// petra: user doc exists, but no permission was ever recorded for her.
	require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "petra", SiteID: "site-a"}))

	// "ghost-user": no CreateUser call — no doc at all.

	got, err := st.GetUserPermissionsForAccounts(ctx, []string{"oscar", "petra", "ghost-user"})
	require.NoError(t, err)

	assert.Len(t, got, 2, "only accounts with a user doc appear — oscar and petra, not ghost-user")
	_, hasGhost := got["ghost-user"]
	assert.False(t, hasGhost, "an account with no user doc must be absent from the map, not present with a nil value")

	require.Contains(t, got, "oscar")
	require.NotNil(t, got["oscar"])
	require.NotNil(t, got["oscar"].ExternalImageView)
	assert.Equal(t, state.Granted, got["oscar"].ExternalImageView.Granted)
	require.NotNil(t, got["oscar"].ExternalImageView.EffectiveFrom)
	require.NotNil(t, got["oscar"].ExternalImageView.ExpiresAt)
	assert.True(t, state.EffectiveFrom.Equal(*got["oscar"].ExternalImageView.EffectiveFrom))
	assert.True(t, state.ExpiresAt.Equal(*got["oscar"].ExternalImageView.ExpiresAt))
	assert.True(t, state.UpdatedAt.Equal(got["oscar"].ExternalImageView.UpdatedAt))

	require.Contains(t, got, "petra", "a user doc with nothing ever recorded must still appear in the map")
	assert.Nil(t, got["petra"], "no permissions ever recorded for petra — nil UserPermissions, not a zero-value struct")
}

func TestIntegration_FindAccountStates(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	activeFlag := false
	require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "active-user", SiteID: "site-a"}))
	require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "inactive-user", SiteID: "site-a", Active: &activeFlag}))
	require.NoError(t, st.CreateUser(ctx, &model.User{ID: idgen.GenerateUUIDv7(), Account: "other-site-user", SiteID: "site-b"}))

	states, err := st.FindAccountStates(ctx, []string{"active-user", "inactive-user", "other-site-user", "ghost-user"})
	require.NoError(t, err)

	assert.Equal(t, map[string]bool{"active-user": true, "inactive-user": false, "other-site-user": true}, states,
		"ghost-user must be absent, not false; other-site-user (a different site) must still be found — lookup is company-wide")

	_, exists := states["ghost-user"]
	assert.False(t, exists, "an account that doesn't exist must not appear in the map at all")
}

func TestIntegration_ListPermissionGrants(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	now := time.Now().UTC()
	otherPermission := model.PermissionKey("other.permission")
	grants := []*model.PermissionGrant{
		{ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView, SubjectAccount: "faye", Granted: true, EffectiveFrom: timePtr(now.Add(-3 * time.Hour)), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r1", RecordedBy: "p_admin", RecordedAt: now.Add(-3 * time.Hour)},
		{ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView, SubjectAccount: "faye", Granted: false, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r2", RecordedBy: "p_admin", RecordedAt: now.Add(-2 * time.Hour)},
		{ID: idgen.GenerateUUIDv7(), Permission: otherPermission, SubjectAccount: "faye", Granted: true, EffectiveFrom: timePtr(now.Add(-1 * time.Hour)), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r3 — different permission", RecordedBy: "p_admin", RecordedAt: now.Add(-1 * time.Hour)},
		{ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView, SubjectAccount: "other-subject", Granted: true, EffectiveFrom: timePtr(now), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r4 — different subject", RecordedBy: "p_admin", RecordedAt: now},
	}
	// Ledger-fixture seeding only — no user docs exist for these accounts, so the
	// state-apply half of RecordPermissionChange is an inert no-op here.
	state := model.PermissionState{Granted: true, EffectiveFrom: timePtr(now.Add(-3 * time.Hour)), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), UpdatedAt: now}
	require.NoError(t, st.RecordPermissionChange(ctx, grants, state))

	t.Run("filters by subjectAccount+permission, newest first", func(t *testing.T) {
		results, total, err := st.ListPermissionGrants(ctx, "faye", model.PermissionExternalImageView, 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		require.Len(t, results, 2)
		assert.Equal(t, "r2", results[0].Reason, "revoke row (recorded later) must come first")
		assert.Equal(t, "r1", results[1].Reason)
	})

	t.Run("permission empty means all permissions for the subject", func(t *testing.T) {
		results, total, err := st.ListPermissionGrants(ctx, "faye", "", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, results, 3)
	})

	t.Run("pagination – page 1 limit 1", func(t *testing.T) {
		results, total, err := st.ListPermissionGrants(ctx, "faye", "", 1, 1)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, results, 1)
	})

	t.Run("pagination – page 2 limit 1 returns a disjoint row from page 1", func(t *testing.T) {
		page1, total1, err := st.ListPermissionGrants(ctx, "faye", "", 1, 1)
		require.NoError(t, err)
		require.Len(t, page1, 1)

		page2, total2, err := st.ListPermissionGrants(ctx, "faye", "", 2, 1)
		require.NoError(t, err)
		require.Len(t, page2, 1)

		assert.Equal(t, int64(3), total1)
		assert.Equal(t, int64(3), total2)
		assert.NotEqual(t, page1[0].ID, page2[0].ID, "page 2 must return a different row than page 1 — skip must actually advance")
	})

	t.Run("no match returns empty with total 0", func(t *testing.T) {
		results, total, err := st.ListPermissionGrants(ctx, "no-such-subject", "", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(0), total)
		assert.Empty(t, results)
	})

	t.Run("every field round-trips through the explicit projection", func(t *testing.T) {
		results, _, err := st.ListPermissionGrants(ctx, "other-subject", model.PermissionExternalImageView, 1, 10)
		require.NoError(t, err)
		require.Len(t, results, 1)
		g := results[0]
		assert.NotEmpty(t, g.ID)
		assert.Equal(t, model.PermissionExternalImageView, g.Permission)
		assert.Equal(t, "other-subject", g.SubjectAccount)
		assert.True(t, g.Granted)
		require.NotNil(t, g.EffectiveFrom)
		require.NotNil(t, g.ExpiresAt)
		assert.Equal(t, "carol", g.ApplicantAccount)
		assert.Equal(t, "dave", g.ApproverAccount)
		assert.Equal(t, "r4 — different subject", g.Reason)
		assert.Equal(t, "p_admin", g.RecordedBy)
		assert.False(t, g.RecordedAt.IsZero())
	})

	t.Run("no filters (subjectAccount and permission both empty) returns every row, newest first", func(t *testing.T) {
		results, total, err := st.ListPermissionGrants(ctx, "", "", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(4), total)
		require.Len(t, results, 4)
		// newest first across ALL subjects: r4 (other-subject, now), r3 (faye, now-1h),
		// r2 (faye, now-2h), r1 (faye, now-3h).
		assert.Equal(t, "r4 — different subject", results[0].Reason)
		assert.Equal(t, "r3 — different permission", results[1].Reason)
		assert.Equal(t, "r2", results[2].Reason)
		assert.Equal(t, "r1", results[3].Reason)
	})

	t.Run("permission only (no subjectAccount) filters across every subject, newest first", func(t *testing.T) {
		results, total, err := st.ListPermissionGrants(ctx, "", model.PermissionExternalImageView, 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		require.Len(t, results, 3)
		for _, g := range results {
			assert.Equal(t, model.PermissionExternalImageView, g.Permission)
		}
		// newest first across faye + other-subject, otherPermission's row excluded:
		// r4 (other-subject, now), r2 (faye revoke, now-2h), r1 (faye grant, now-3h).
		assert.Equal(t, "r4 — different subject", results[0].Reason)
		assert.Equal(t, "r2", results[1].Reason)
		assert.Equal(t, "r1", results[2].Reason)
	})

	t.Run("same recordedAt ties break on _id desc", func(t *testing.T) {
		shared := time.Now().UTC()
		tieGrants := []*model.PermissionGrant{
			{ID: "tie-a", Permission: model.PermissionExternalImageView, SubjectAccount: "tie-subject", Granted: true, EffectiveFrom: timePtr(shared), ExpiresAt: timePtr(shared.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row a", RecordedBy: "p_admin", RecordedAt: shared},
			{ID: "tie-b", Permission: model.PermissionExternalImageView, SubjectAccount: "tie-subject", Granted: false, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row b", RecordedBy: "p_admin", RecordedAt: shared},
		}
		require.NoError(t, st.RecordPermissionChange(ctx, tieGrants, model.PermissionState{Granted: false, UpdatedAt: shared}))

		results, total, err := st.ListPermissionGrants(ctx, "tie-subject", "", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		require.Len(t, results, 2)
		assert.Equal(t, "tie-b", results[0].ID, "identical recordedAt must tie-break on the larger _id")
		assert.Equal(t, "tie-a", results[1].ID)
	})
}

func TestIntegration_AppendAuditMany(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	entries := []*AuditEntry{
		{ID: idgen.GenerateUUIDv7(), ActorUserID: "admin1", ActorAccount: "p_admin", Action: "permission.grant", TargetAccount: "alice", Details: map[string]string{"permission": "external.image.view"}, SiteID: "site-a", Timestamp: time.Now().UTC().UnixMilli()},
		{ID: idgen.GenerateUUIDv7(), ActorUserID: "admin1", ActorAccount: "p_admin", Action: "permission.grant", TargetAccount: "bob", Details: map[string]string{"permission": "external.image.view"}, SiteID: "site-a", Timestamp: time.Now().UTC().UnixMilli()},
	}
	require.NoError(t, st.AppendAuditMany(ctx, entries))

	results, total, err := st.ListAudit(ctx, "site-a", AuditFilter{Action: "permission.grant"}, 1, 10)
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, results, 2)

	t.Run("empty batch is a no-op, not an error", func(t *testing.T) {
		require.NoError(t, st.AppendAuditMany(ctx, nil))
	})
}

// planStage captures one node of a MongoDB explain winningPlan tree. Decoded
// via bson.Raw so nesting depth doesn't have to be known up front.
type planStage struct {
	Stage      string   `bson:"stage"`
	IndexName  string   `bson:"indexName"`
	InputStage bson.Raw `bson:"inputStage"`
}

// collectStages walks a winningPlan tree and returns every stage found,
// outermost first, keeping each stage's indexName (empty for non-IXSCAN
// stages) so callers can assert not just THAT an index was used but WHICH
// one — two different indexes can produce an identical stage-name shape
// (IXSCAN, no SORT) while serving the query very differently.
func collectStages(t *testing.T, raw bson.Raw) []planStage {
	t.Helper()
	if len(raw) == 0 {
		return nil
	}
	var s planStage
	require.NoError(t, bson.Unmarshal(raw, &s))
	return append([]planStage{s}, collectStages(t, s.InputStage)...)
}

// indexNameForKeys resolves the auto-generated name MongoDB assigned to the
// index with exactly the given key spec, by listing the real indexes on the
// collection rather than hand-predicting mongod's naming convention (field
// order is significant for a compound index's identity, so this is an
// ordered comparison, not a set comparison).
func indexNameForKeys(ctx context.Context, t *testing.T, iv mongo.IndexView, want bson.D) string {
	t.Helper()
	specs, err := iv.ListSpecifications(ctx)
	require.NoError(t, err)
	for _, spec := range specs {
		var got bson.D
		require.NoError(t, bson.Unmarshal(spec.KeysDocument, &got))
		if indexKeysEqual(got, want) {
			return spec.Name
		}
	}
	require.Fail(t, "no index found matching the expected key spec — did EnsureIndexes change?")
	return ""
}

// indexKeysEqual compares two index key specs field-by-field, in order.
func indexKeysEqual(got, want bson.D) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i].Key != want[i].Key || indexDirection(got[i].Value) != indexDirection(want[i].Value) {
			return false
		}
	}
	return true
}

// indexDirection normalizes a decoded BSON numeric (int32/int64/float64) index
// direction to int64 so driver-decoded values compare cleanly against Go int literals.
func indexDirection(v any) int64 {
	switch n := v.(type) {
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case int:
		return int64(n)
	}
	return 0
}

func TestIntegration_ListPermissionGrants_UsesIndexNoInMemorySort(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)
	require.NoError(t, st.EnsureIndexes(context.Background()))
	ctx := context.Background()

	now := time.Now().UTC()
	require.NoError(t, st.RecordPermissionChange(ctx, []*model.PermissionGrant{
		{ID: idgen.GenerateUUIDv7(), Permission: model.PermissionExternalImageView, SubjectAccount: "gina", Granted: true, EffectiveFrom: timePtr(now), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "r1", RecordedBy: "p_admin", RecordedAt: now},
	}, model.PermissionState{Granted: true, EffectiveFrom: timePtr(now), ExpiresAt: timePtr(now.AddDate(0, 1, 0)), UpdatedAt: now}))

	tests := []struct {
		name      string
		filter    bson.D
		indexKeys bson.D // the compound index that must serve the query
	}{
		{
			name: "subjectAccount+permission lookup",
			filter: bson.D{
				{Key: "permission", Value: string(model.PermissionExternalImageView)},
				{Key: "subjectAccount", Value: "gina"},
			},
			indexKeys: bson.D{
				{Key: "permission", Value: 1},
				{Key: "subjectAccount", Value: 1},
				{Key: "recordedAt", Value: -1},
				{Key: "_id", Value: -1},
			},
		},
		{
			// The default landing view: GET with no subjectAccount/permission filters.
			// Ties on recordedAt are guaranteed (createPermissions stamps one `now`
			// across a multi-subject batch), so the browse index must also carry the
			// _id tiebreaker or every page load pays a blocking in-memory SORT.
			name:   "no-filter browse",
			filter: bson.D{},
			indexKeys: bson.D{
				{Key: "recordedAt", Value: -1},
				{Key: "_id", Value: -1},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Resolve the compound index's real auto-generated name at runtime
			// (rather than hardcoding mongod's naming convention), so the assertion
			// below can prove WHICH index served the query, not just that some index did.
			wantIndexName := indexNameForKeys(ctx, t, st.permGrants.Indexes(), tc.indexKeys)
			t.Logf("compound index resolved to name %q", wantIndexName)

			cmd := bson.D{
				{Key: "explain", Value: bson.D{
					{Key: "find", Value: "permission_grants"},
					{Key: "filter", Value: tc.filter},
					{Key: "sort", Value: bson.D{{Key: "recordedAt", Value: -1}, {Key: "_id", Value: -1}}},
				}},
				{Key: "verbosity", Value: "queryPlanner"},
			}

			var result struct {
				QueryPlanner struct {
					WinningPlan bson.Raw `bson:"winningPlan"`
				} `bson:"queryPlanner"`
			}
			require.NoError(t, db.RunCommand(ctx, cmd).Decode(&result))

			stages := collectStages(t, result.QueryPlanner.WinningPlan)

			var ixscanNames []string
			sawSort := false
			for _, s := range stages {
				switch s.Stage {
				case "IXSCAN":
					ixscanNames = append(ixscanNames, s.IndexName)
				case "SORT":
					sawSort = true
				}
			}

			require.NotEmpty(t, ixscanNames, "the hot query must use an index, not a collection scan")
			assert.Equal(t, []string{wantIndexName}, ixscanNames,
				"the query must be served by the expected compound index; a different index could still "+
					"IXSCAN with no SORT stage — identical shape, wrong index — which is exactly what this "+
					"assertion (as opposed to a stage-names-only check) catches")
			assert.False(t, sawSort, "recordedAt desc,_id desc must come free from the index — no in-memory sort")
		})
	}
}

// -------------------------------------------------------------------------
// TestLoginAndChangePasswordEndToEnd
// -------------------------------------------------------------------------

func TestLoginAndChangePasswordEndToEnd(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminlogin")
	sessions := session.NewMongoStore(db)
	require.NoError(t, sessions.EnsureIndexes(context.Background()))
	store := newStoreMongo(db)
	require.NoError(t, store.EnsureIndexes(context.Background()))

	cfg := Config{SiteID: "site-a", BcryptCost: 4, SessionsMaxPerAccount: 100}
	h := newHandler(store, sessions, cfg, nil, nil)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, h, sessions, cfg.SiteID)

	// Seed one admin
	hash, err := pwhash.Hash("s3cret", cfg.BcryptCost)
	require.NoError(t, err)
	require.NoError(t, store.CreateUser(context.Background(), &model.User{
		ID: "u-alice", Account: "p_alice", SiteID: "site-a",
		Roles:    []model.UserRole{model.UserRoleAdmin},
		Services: model.Services{Password: model.PasswordCredentials{Bcrypt: hash}},
	}))

	// 1. Login
	w := postJSON(t, r, "/v1/login", map[string]string{"username": "p_alice", "password": "s3cret"})
	require.Equal(t, http.StatusOK, w.Code)
	var login loginResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &login))
	require.NotEmpty(t, login.AuthToken)

	// Sanity: session row exists
	_, err = sessions.FindByHash(context.Background(), sessiontoken.Hash(login.AuthToken))
	require.NoError(t, err)

	// 2. Seed a second session for the same account (represents a chat-frontend session)
	other := &session.Session{
		ID: "other-session-hash", UserID: "u-alice", Account: "p_alice", SiteID: "site-a",
		Roles: []string{string(model.UserRoleAdmin)}, IssuedAt: 1,
	}
	require.NoError(t, sessions.Insert(context.Background(), other))

	// 3. Change password using the login session
	body := map[string]string{"oldPassword": "s3cret", "newPassword": "newp@ss"}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/password/change", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+login.AuthToken)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNoContent, w.Code)

	// Caller session still valid
	_, err = sessions.FindByHash(context.Background(), sessiontoken.Hash(login.AuthToken))
	require.NoError(t, err)
	// Sibling session gone
	_, err = sessions.FindByHash(context.Background(), "other-session-hash")
	require.Error(t, err)

	// Old password no longer works
	w = postJSON(t, r, "/v1/login", map[string]string{"username": "p_alice", "password": "s3cret"})
	require.Equal(t, http.StatusUnauthorized, w.Code)

	// New password does
	w = postJSON(t, r, "/v1/login", map[string]string{"username": "p_alice", "password": "newp@ss"})
	require.Equal(t, http.StatusOK, w.Code)
}
