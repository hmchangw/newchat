//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/user-presence-service/presencestore"
)

// newTestStore returns a store over a per-test cluster. Per CLAUDE.md, the
// per-test cluster is used because the store wraps a client whose Close() it
// owns; the test never calls store.Close() (StartValkeyCluster's t.Cleanup
// closes the client).
func newTestStore(t *testing.T, stale, connsTTL time.Duration) *presencestore.Store {
	t.Helper()
	c := testutil.StartValkeyCluster(t)
	return presencestore.NewValkeyStoreFromClient(c, stale, connsTTL)
}

func TestValkeyStore_ConnectionLifecycle(t *testing.T) {
	st := newTestStore(t, 45*time.Second, 5*time.Minute)
	ctx := context.Background()

	// Hello brings the first connection online.
	changed, eff, err := st.SetActivity(ctx, "alice", "c1", false)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, model.StatusOnline, eff)

	// Ping refreshes a known connection: no status change.
	changed, _, err = st.Ping(ctx, "alice", "c1")
	require.NoError(t, err)
	assert.False(t, changed, "ping of a known connection must not flip status")

	// Activity marks the only connection inactive -> away.
	changed, eff, err = st.SetActivity(ctx, "alice", "c1", true)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, model.StatusAway, eff)

	// Bye drops the last connection -> offline.
	changed, eff, err = st.RemoveConnection(ctx, "alice", "c1")
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, model.StatusOffline, eff)
}

func TestValkeyStore_PingCreatesConnection(t *testing.T) {
	st := newTestStore(t, 45*time.Second, 5*time.Minute)
	ctx := context.Background()

	// A ping for a not-yet-seen connection upserts it and flips offline->online.
	changed, eff, err := st.Ping(ctx, "erin", "c1")
	require.NoError(t, err)
	assert.True(t, changed, "first-sight ping must bring the user online")
	assert.Equal(t, model.StatusOnline, eff)
}

func TestValkeyStore_MultipleConnections(t *testing.T) {
	st := newTestStore(t, 45*time.Second, 5*time.Minute)
	ctx := context.Background()

	_, eff, err := st.SetActivity(ctx, "alice", "c1", false)
	require.NoError(t, err)
	assert.Equal(t, model.StatusOnline, eff)

	// Second connection inactive: still online (c1 active).
	_, eff, err = st.SetActivity(ctx, "alice", "c2", true)
	require.NoError(t, err)
	assert.Equal(t, model.StatusOnline, eff)

	// Active connection goes inactive: now both inactive -> away.
	changed, eff, err := st.SetActivity(ctx, "alice", "c1", true)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, model.StatusAway, eff)
}

func TestValkeyStore_ManualOverride(t *testing.T) {
	st := newTestStore(t, 45*time.Second, 5*time.Minute)
	ctx := context.Background()
	_, _, err := st.SetActivity(ctx, "bob", "c1", false)
	require.NoError(t, err)

	changed, eff, err := st.SetManual(ctx, "bob", model.StatusAppearOffline)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, model.StatusOffline, eff, "appear_offline wins over online")

	changed, eff, err = st.SetManual(ctx, "bob", model.StatusBusy)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, model.StatusBusy, eff)

	changed, eff, err = st.SetManual(ctx, "bob", model.StatusNone)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, model.StatusOnline, eff, "clearing override restores availability")
}

func TestValkeyStore_SweepDecay(t *testing.T) {
	st := newTestStore(t, 10*time.Millisecond, time.Minute)
	ctx := context.Background()

	_, _, err := st.SetActivity(ctx, "carol", "c1", false)
	require.NoError(t, err)
	time.Sleep(40 * time.Millisecond) // let the connection go stale

	changes, err := st.Sweep(ctx, time.Now())
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Equal(t, "carol", changes[0].Account)
	assert.Equal(t, model.StatusOffline, changes[0].Effective)

	// A second sweep is a no-op (already offline, account removed from index).
	changes, err = st.Sweep(ctx, time.Now())
	require.NoError(t, err)
	assert.Empty(t, changes)
}

func TestValkeyStore_PrecedenceLadder(t *testing.T) {
	st := newTestStore(t, 45*time.Second, 5*time.Minute)
	ctx := context.Background()

	// Manual busy with an active connection -> busy.
	_, _, err := st.SetActivity(ctx, "frank", "c1", false)
	require.NoError(t, err)
	_, eff, err := st.SetManual(ctx, "frank", model.StatusBusy)
	require.NoError(t, err)
	assert.Equal(t, model.StatusBusy, eff)

	// Disconnecting the last connection beats the standing manual busy: a fully
	// disconnected user is offline regardless of any override (precedence rung 1).
	_, eff, err = st.RemoveConnection(ctx, "frank", "c1")
	require.NoError(t, err)
	assert.Equal(t, model.StatusOffline, eff, "no connections must beat manual busy")

	// Manual away with an active connection -> away (rung 2).
	_, _, err = st.SetActivity(ctx, "grace", "c1", false)
	require.NoError(t, err)
	_, eff, err = st.SetManual(ctx, "grace", model.StatusAway)
	require.NoError(t, err)
	assert.Equal(t, model.StatusAway, eff)

	// Manual online overrides the all-inactive -> away derivation (rung 3 beats 5).
	_, _, err = st.SetActivity(ctx, "heidi", "c1", false)
	require.NoError(t, err)
	_, eff, err = st.SetManual(ctx, "heidi", model.StatusOnline)
	require.NoError(t, err)
	assert.Equal(t, model.StatusOnline, eff)
	_, eff, err = st.SetActivity(ctx, "heidi", "c1", true) // connection goes inactive
	require.NoError(t, err)
	assert.Equal(t, model.StatusOnline, eff, "manual online suppresses the away derivation")
}

func TestValkeyStore_BatchGet(t *testing.T) {
	st := newTestStore(t, 45*time.Second, 5*time.Minute)
	ctx := context.Background()
	_, _, err := st.SetActivity(ctx, "dave", "c1", false)
	require.NoError(t, err)

	got, err := st.BatchGet(ctx, []string{"dave", "neverseen"})
	require.NoError(t, err)
	assert.Equal(t, model.StatusOnline, got["dave"])
	assert.Equal(t, model.StatusOffline, got["neverseen"])
}
