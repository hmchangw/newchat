package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/sessioncache"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// bustRecorder records the keys a revoke deletes from the session cache.
type bustRecorder struct {
	valkeyutil.Client
	dels []string
}

func (b *bustRecorder) Del(_ context.Context, keys ...string) error {
	b.dels = append(b.dels, keys...)
	return nil
}

func (b *bustRecorder) Expire(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

func newRevokeHandler(t *testing.T, sessions *fakeSessionStore) (*Handler, *bustRecorder) {
	t.Helper()
	store := NewMockAdminStore(gomock.NewController(t))
	store.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	h := newHandler(store, sessions, Config{SiteID: "site-A"}, nil, nil)
	bust := &bustRecorder{}
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

	assert.ElementsMatch(t,
		[]string{sessioncache.Key("hash-a"), sessioncache.Key("hash-b")}, bust.dels,
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

	assert.Equal(t, []string{sessioncache.Key("hash-a")}, bust.dels,
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
