//go:build integration

package session_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func newStore(t *testing.T) session.Store {
	db := testutil.MongoDB(t, "sess")
	s := session.NewMongoStore(db)
	require.NoError(t, s.EnsureIndexes(context.Background()))
	return s
}

func TestInsertAndFindByHash(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	sess := &session.Session{
		ID: "hash-a", UserID: "u1", Account: "alice", SiteID: "site-a",
		Roles: []string{"admin"}, IssuedAt: 100,
	}
	require.NoError(t, s.Insert(ctx, sess))

	got, err := s.FindByHash(ctx, "hash-a")
	require.NoError(t, err)
	assert.Equal(t, sess, got)
}

func TestFindByHash_NotFound(t *testing.T) {
	s := newStore(t)
	_, err := s.FindByHash(context.Background(), "missing")
	require.Error(t, err)
}

func TestDeleteBeyondCap_EvictsOldest(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	for i, ts := range []int64{100, 200, 300, 400, 500} {
		require.NoError(t, s.Insert(ctx, &session.Session{
			ID: string(rune('a' + i)), UserID: "u1", Account: "alice", SiteID: "site-a",
			Roles: []string{"admin"}, IssuedAt: ts,
		}))
	}

	deleted, err := s.DeleteBeyondCap(ctx, "alice", 2)
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted)

	// Only the two newest survive.
	for _, id := range []string{"a", "b", "c"} {
		_, err := s.FindByHash(ctx, id)
		require.Error(t, err, "expected %q evicted", id)
	}
	for _, id := range []string{"d", "e"} {
		_, err := s.FindByHash(ctx, id)
		require.NoError(t, err, "expected %q kept", id)
	}
}

func TestDeleteBeyondCap_NoOp_UnderCap(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.Insert(ctx, &session.Session{
		ID: "only", UserID: "u1", Account: "alice", SiteID: "site-a", IssuedAt: 1,
	}))
	deleted, err := s.DeleteBeyondCap(ctx, "alice", 5)
	require.NoError(t, err)
	assert.Zero(t, deleted)
}

// TestDeleteBeyondCap_ConcurrentLogins locks in the "keep newest N" invariant
// under simultaneous inserts for the same account — the race Fix 4 narrows
// (deterministic (issuedAt, _id) tie-break) but does not eliminate.
func TestDeleteBeyondCap_ConcurrentLogins(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	const n = 5
	const maxSessions = 3

	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := range n {
		id := fmt.Sprintf("concurrent-%d", i)
		ids[i] = id
		wg.Add(1)
		go func(id string, ts int64) {
			defer wg.Done()
			assert.NoError(t, s.Insert(ctx, &session.Session{
				ID: id, UserID: "u-concurrent", Account: "concurrent-acct", SiteID: "site-a",
				Roles: []string{"admin"}, IssuedAt: ts,
			}))
		}(id, int64(100*(i+1)))
	}
	wg.Wait()

	_, err := s.DeleteBeyondCap(ctx, "concurrent-acct", maxSessions)
	require.NoError(t, err)

	remaining, err := s.ListForAccount(ctx, "site-a", "concurrent-acct")
	require.NoError(t, err)
	assert.Len(t, remaining, maxSessions, "exactly maxSessions sessions must remain")

	knownIDs := make(map[string]bool, n)
	for _, id := range ids {
		knownIDs[id] = true
	}
	for _, r := range remaining {
		assert.True(t, knownIDs[r.ID], "remaining session %q must be one of the originally inserted sessions", r.ID)
	}
}

func TestDeleteForAccountExcept(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	for _, id := range []string{"keep", "kill-1", "kill-2"} {
		require.NoError(t, s.Insert(ctx, &session.Session{
			ID: id, UserID: "u1", Account: "alice", SiteID: "site-a", IssuedAt: 1,
		}))
	}

	deleted, err := s.DeleteForAccountExcept(ctx, "site-a", "alice", "keep")
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)

	_, err = s.FindByHash(ctx, "keep")
	require.NoError(t, err)
	for _, id := range []string{"kill-1", "kill-2"} {
		_, err := s.FindByHash(ctx, id)
		require.Error(t, err)
	}
}

func TestEnsureIndexes_Idempotent(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	// second call must not error
	require.NoError(t, s.EnsureIndexes(ctx))
}
