package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/pwhash"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessioncache"
	"github.com/hmchangw/chat/pkg/valkeyfake"
)

func newRevokeHandler(t *testing.T, sessions *fakeSessionStore) (*Handler, *valkeyfake.Client) {
	t.Helper()
	store := NewMockAdminStore(gomock.NewController(t))
	store.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	h := newHandler(store, sessions, Config{SiteID: "site-A"}, nil, nil)
	bust := valkeyfake.New()
	h.valkey = bust
	return h, bust
}

// Revocation must reach the cache, not just Mongo. Every request resolves
// through sessioncache without touching Mongo until the entry's refresh window
// elapses (~67 min at the 90m default), and during a source outage the TTL
// slide re-arms it on every read — so a Mongo-only delete can leave a revoked
// token working indefinitely. A session's _id IS its token hash, so the ids the
// delete reports are exactly the cache keys.
func TestRevokeAllSessions_BustsEveryRevokedHash(t *testing.T) {
	sessions := emptySessionStore()
	sessions.DeleteForAccountFn = func(_ context.Context, _, _ string) ([]string, error) {
		return []string{"hash-a", "hash-b"}, nil
	}
	h, bust := newRevokeHandler(t, sessions)

	w := httptest.NewRecorder()
	setupSessionRouter(h).ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/sessions?account=alice", nil))
	require.Equal(t, http.StatusOK, w.Code)

	// Subset, not equality: a bust also clears the pre-Box generation, which is
	// sessioncache's own bookkeeping (pinned by its TestBust_DropsTheEntry) and
	// will be deleted once no such binary can run.
	assert.Subset(t, bust.DeletedKeys(), []string{sessioncache.Key("hash-a"), sessioncache.Key("hash-b")},
		"every revoked session must be evicted from the cache that authorizes it")
}

func TestRevokeSession_BustsTheSessionHash(t *testing.T) {
	sessions := emptySessionStore()
	sessions.DeleteByIDFn = func(_ context.Context, _, _, id string) (int64, error) {
		assert.Equal(t, "hash-a", id)
		return 1, nil
	}
	h, bust := newRevokeHandler(t, sessions)

	w := httptest.NewRecorder()
	setupSessionRouter(h).ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/sessions/hash-a?account=alice", nil))
	require.Equal(t, http.StatusOK, w.Code)

	assert.Subset(t, bust.DeletedKeys(), []string{sessioncache.Key("hash-a")},
		"the path parameter IS the token hash, so it is the cache key")
}

// The client is optional and this service only ever writes to it, so a revoke
// must still succeed with no cache configured.
func TestRevoke_WithoutValkeyStillSucceeds(t *testing.T) {
	sessions := emptySessionStore()
	sessions.DeleteForAccountFn = func(_ context.Context, _, _ string) ([]string, error) {
		return []string{"hash-a"}, nil
	}
	h, _ := newRevokeHandler(t, sessions)
	h.valkey = nil

	w := httptest.NewRecorder()
	require.NotPanics(t, func() {
		setupSessionRouter(h).ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/sessions?account=alice", nil))
	})
	assert.Equal(t, http.StatusOK, w.Code)
}

// newTxRevokeHandler builds a handler whose store expectations the caller sets,
// mirroring newRevokeHandler for the two paths that revoke inside a Mongo
// transaction rather than through session.Store.
func newTxRevokeHandler(t *testing.T) (*Handler, *MockAdminStore, *valkeyfake.Client) {
	t.Helper()
	store := NewMockAdminStore(gomock.NewController(t))
	store.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	h := newHandler(store, emptySessionStore(), Config{SiteID: "site-A"}, nil, nil)
	bust := valkeyfake.New()
	h.valkey = bust
	return h, store, bust
}

// Deactivation revokes inside a transaction, so its session ids never passed
// through session.Store and nothing bust the cache — a deactivated account kept
// authenticating from cache for the whole refresh window, and indefinitely
// while Mongo was down. Same invariant as TestRevokeAllSessions_BustsEveryRevokedHash.
func TestDeactivateUser_BustsEveryRevokedHash(t *testing.T) {
	h, store, bust := newTxRevokeHandler(t)
	store.EXPECT().DeactivateAndRevoke(gomock.Any(), "site-A", "u2").
		Return(&model.User{Account: "u2"}, []string{"hash-a", "hash-b"}, nil)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/users/u2", bodyBytes(t, map[string]any{"active": false}))
	setupRouter(h).ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	assert.Subset(t, bust.DeletedKeys(), []string{sessioncache.Key("hash-a"), sessioncache.Key("hash-b")},
		"deactivation must evict the sessions it revoked")
}

// Admin-forced password reset revokes every session for the account; those ids
// are cache keys for the same reason.
func TestSetPassword_BustsEveryRevokedHash(t *testing.T) {
	h, store, bust := newTxRevokeHandler(t)
	store.EXPECT().UpdateUserPasswordAndRevoke(gomock.Any(), "site-A", "u1", gomock.Any(), true, "").
		Return([]string{"hash-c"}, nil)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/users/u1/password",
		bodyBytes(t, map[string]any{"password": "newSecret123", "requirePasswordChange": true}))
	setupRouter(h).ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	assert.Subset(t, bust.DeletedKeys(), []string{sessioncache.Key("hash-c")},
		"an admin password reset must evict the sessions it revoked")
}

// Self-service change-password keeps the caller's own session alive, so that id
// must NOT be bust even though the others are.
func TestChangePassword_KeepsCallerSessionOutOfTheBust(t *testing.T) {
	h, store, bust := newTxRevokeHandler(t)
	store.EXPECT().UpdateUserPasswordAndRevoke(gomock.Any(), "site-A", "p_admin", gomock.Any(), false, "sess-1").
		Return([]string{"hash-other"}, nil)

	hash, err := pwhash.Hash("old-pw", 4)
	require.NoError(t, err)
	u := &model.User{Account: "p_admin"}
	u.Services.Password.Bcrypt = hash
	store.EXPECT().GetUserForAuth(gomock.Any(), "site-A", "p_admin").Return(u, nil)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/password/change",
		bodyBytes(t, map[string]any{"oldPassword": "old-pw", "newPassword": "newSecret123"}))
	selfServiceRouter(h).ServeHTTP(w, req)
	require.Equal(t, http.StatusNoContent, w.Code)

	assert.Subset(t, bust.DeletedKeys(), []string{sessioncache.Key("hash-other")})
	assert.NotContains(t, bust.DeletedKeys(), sessioncache.Key("sess-1"),
		"the caller's own session survives the reset, so it must stay cached")
}

// selfServiceRouter injects the caller principal directly, so the test exercises
// handleChangePassword rather than requireAdmin's token plumbing.
func selfServiceRouter(h *Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxPrincipal, session.Session{
			ID: "sess-1", UserID: "admin-user-id", Account: "p_admin",
			SiteID: "site-A", Roles: []string{"admin"},
		})
		c.Next()
	})
	r.POST("/password/change", h.handleChangePassword)
	return r
}
