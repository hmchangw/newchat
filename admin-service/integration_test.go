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
	testutil.RunTestsWithPrewarm(m, testutil.EnsureMongoReplicaSet)
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

	t.Run("filter by siteId – excludes other sites", func(t *testing.T) {
		results, total, err := st.SearchUsers(ctx, "site-a", "", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		assert.Len(t, results, 2)
	})

	t.Run("filter by q matches account", func(t *testing.T) {
		results, total, err := st.SearchUsers(ctx, "site-a", "alice", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Equal(t, "alice", results[0].Account)
	})

	t.Run("filter by q matches engName", func(t *testing.T) {
		_, total, err := st.SearchUsers(ctx, "site-a", "Smith", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
	})

	t.Run("filter by q matches chineseName", func(t *testing.T) {
		results, total, err := st.SearchUsers(ctx, "site-a", "阿鮑", 1, 10)
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Equal(t, "bob", results[0].Account)
	})

	t.Run("pagination – page 1 limit 1", func(t *testing.T) {
		results, total, err := st.SearchUsers(ctx, "site-a", "", 1, 1)
		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		assert.Len(t, results, 1)
	})

	t.Run("pagination – page 2 limit 1", func(t *testing.T) {
		results, total, err := st.SearchUsers(ctx, "site-a", "", 2, 1)
		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		assert.Len(t, results, 1)
	})

	t.Run("no match returns empty slice", func(t *testing.T) {
		results, total, err := st.SearchUsers(ctx, "site-a", "zzznomatch", 1, 10)
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
	}
	require.NoError(t, st.CreateUser(ctx, u))

	t.Run("update roles", func(t *testing.T) {
		newRoles := []model.UserRole{model.UserRoleAdmin}
		err := st.UpdateUser(ctx, "site-a", u.Account, UserUpdate{Roles: &newRoles})
		require.NoError(t, err)

		got, err := st.GetUserByAccount(ctx, "site-a", u.Account)
		require.NoError(t, err)
		assert.Equal(t, []model.UserRole{model.UserRoleAdmin}, got.Roles)
	})

	t.Run("update deactivated on this path does not touch sessions (handler routes Deactivated=true to DeactivateAndRevoke instead)", func(t *testing.T) {
		sessStore := session.NewMongoStore(db)
		seedSession(t, db, session.Session{ID: "eve-sess-1", UserID: u.ID, Account: u.Account, SiteID: "site-a", IssuedAt: 1})

		deact := true
		err := st.UpdateUser(ctx, "site-a", u.Account, UserUpdate{Deactivated: &deact})
		require.NoError(t, err)

		got, err := st.GetUserByAccount(ctx, "site-a", u.Account)
		require.NoError(t, err)
		assert.True(t, got.Deactivated)

		sessions, err := sessStore.ListForAccount(ctx, "site-a", u.Account)
		require.NoError(t, err)
		assert.Len(t, sessions, 1, "store.UpdateUser must leave sessions alone — revocation is the handler's job now")
	})

	t.Run("update names", func(t *testing.T) {
		eng := "Eve Updated"
		cn := "更新伊芙"
		err := st.UpdateUser(ctx, "site-a", u.Account, UserUpdate{EngName: &eng, ChineseName: &cn})
		require.NoError(t, err)

		got, err := st.GetUserByAccount(ctx, "site-a", u.Account)
		require.NoError(t, err)
		assert.Equal(t, "Eve Updated", got.EngName)
		assert.Equal(t, "更新伊芙", got.ChineseName)
	})

	t.Run("no-op when all fields nil", func(t *testing.T) {
		err := st.UpdateUser(ctx, "site-a", u.Account, UserUpdate{})
		require.NoError(t, err)
	})

	t.Run("nonexistent id returns ErrUserNotFound", func(t *testing.T) {
		eng := "Ghost"
		err := st.UpdateUser(ctx, "site-a", "nonexistent-account", UserUpdate{EngName: &eng})
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

	t.Run("sets deactivated=true and kills every session for the account atomically", func(t *testing.T) {
		sessStore := session.NewMongoStore(db)
		seedSession(t, db, session.Session{ID: "gwen-sess-1", UserID: u.ID, Account: u.Account, SiteID: "site-a", IssuedAt: 1})
		seedSession(t, db, session.Session{ID: "gwen-sess-2", UserID: u.ID, Account: u.Account, SiteID: "site-a", IssuedAt: 2})

		err := st.DeactivateAndRevoke(ctx, "site-a", u.Account)
		require.NoError(t, err)

		var raw struct {
			Deactivated bool `bson:"deactivated"`
		}
		err = db.Collection("users").FindOne(ctx, bson.M{"_id": u.ID},
			options.FindOne().SetProjection(bson.M{"deactivated": 1}),
		).Decode(&raw)
		require.NoError(t, err)
		assert.True(t, raw.Deactivated)

		sessions, err := sessStore.ListForAccount(ctx, "site-a", u.Account)
		require.NoError(t, err)
		assert.Empty(t, sessions, "deactivate must kill every session for the account")
	})

	t.Run("nonexistent account returns ErrUserNotFound", func(t *testing.T) {
		err := st.DeactivateAndRevoke(ctx, "site-a", "ghost-account")
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
// EnsureIndexes idempotent
// -------------------------------------------------------------------------

func TestIntegration_EnsureIndexes_Idempotent(t *testing.T) {
	db := testutil.MongoDBReplicaSet(t, "adminsvc")
	st := newStoreMongo(db)

	// First call.
	require.NoError(t, st.EnsureIndexes(context.Background()), "first EnsureIndexes must succeed")
	// Second call must also succeed (idempotent).
	require.NoError(t, st.EnsureIndexes(context.Background()), "second EnsureIndexes must be idempotent")
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
	h := newHandler(store, sessions, cfg)

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
