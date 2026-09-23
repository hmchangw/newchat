//go:build integration

package session_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

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
	// The evicted ids come back so the caller can bust their cache entries;
	// a session _id IS its token hash.
	assert.Len(t, deleted, 3)

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
	assert.Empty(t, deleted)
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
	assert.ElementsMatch(t, []string{"kill-1", "kill-2"}, deleted)

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

// TestEnsureIndexes_DeleteBeyondCapPlanIsCoveredAndUnsorted pins the query
// plan behind DeleteBeyondCap. Its sort key is (issuedAt, _id); an index
// carrying only (account, issuedAt) satisfies the account equality but not the
// sort, so the planner fetched every session document for the account and
// sorted them in memory on every login. On a busy primary that blocking SORT
// plus per-document FETCH was the heaviest query in the slow-query log. The
// index must carry _id as well, so the walk is index-order and, since the
// query projects only _id, fully covered: no SORT stage, no FETCH stage, no
// documents examined.
func TestEnsureIndexes_DeleteBeyondCapPlanIsCoveredAndUnsorted(t *testing.T) {
	db := testutil.MongoDB(t, "sessplan")
	s := session.NewMongoStore(db)
	ctx := context.Background()
	require.NoError(t, s.EnsureIndexes(ctx))

	for i, ts := range []int64{100, 200, 300, 400, 500} {
		require.NoError(t, s.Insert(ctx, &session.Session{
			ID: fmt.Sprintf("alice-%d", i), UserID: "u1", Account: "alice", SiteID: "site-a",
			Roles: []string{"bot"}, IssuedAt: ts,
		}))
	}
	require.NoError(t, s.Insert(ctx, &session.Session{
		ID: "bob-0", UserID: "u2", Account: "bob", SiteID: "site-a", IssuedAt: 100,
	}))

	// The same find DeleteBeyondCap issues, wrapped in explain.
	var raw bson.Raw
	err := db.RunCommand(ctx, bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: session.Collection},
			{Key: "filter", Value: bson.D{{Key: "account", Value: "alice"}}},
			{Key: "sort", Value: bson.D{{Key: "issuedAt", Value: -1}, {Key: "_id", Value: -1}}},
			{Key: "skip", Value: 2},
			{Key: "projection", Value: bson.D{{Key: "_id", Value: 1}}},
		}},
		{Key: "verbosity", Value: "executionStats"},
	}).Decode(&raw)
	require.NoError(t, err)

	plan := explainToMap(t, raw)
	stages := planStages(plan["queryPlanner"].(map[string]any)["winningPlan"])
	assert.Contains(t, stages, "IXSCAN", "plan must use the index: %v", stages)
	assert.NotContains(t, stages, "SORT", "sort must come from index order, not a blocking SORT: %v", stages)
	assert.NotContains(t, stages, "FETCH", "projection is _id only, so the index must cover it: %v", stages)

	stats := plan["executionStats"].(map[string]any)
	assert.EqualValues(t, 0, stats["totalDocsExamined"], "covered plan must examine no documents")
	assert.EqualValues(t, 3, stats["nReturned"], "skip 2 of alice's 5 sessions leaves 3 to evict")
}

// explainToMap round-trips an explain reply through extended JSON so the nested
// plan tree decodes as plain maps regardless of the driver's default
// document type.
func explainToMap(t *testing.T, raw bson.Raw) map[string]any {
	t.Helper()
	ext, err := bson.MarshalExtJSON(raw, false, false)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(ext, &out))
	return out
}

// planStages collects every "stage" name in a winning plan. Mongo 8 nests the
// classic stage tree under winningPlan.queryPlan when the SBE engine runs the
// query, so walk every nested document rather than a fixed inputStage chain.
func planStages(node any) []string {
	var out []string
	switch v := node.(type) {
	case map[string]any:
		if st, ok := v["stage"].(string); ok {
			out = append(out, st)
		}
		for _, child := range v {
			out = append(out, planStages(child)...)
		}
	case []any:
		for _, child := range v {
			out = append(out, planStages(child)...)
		}
	}
	return out
}
