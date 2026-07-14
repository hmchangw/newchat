//go:build integration

package presencestore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

const extTTL = 2 * time.Minute

func newStore(t *testing.T) *Store {
	t.Helper()
	client := testutil.StartValkeyCluster(t)
	return NewValkeyStoreFromClient(client, 45*time.Second, 5*time.Minute)
}

func TestSetExternal_InCall_LiveUser(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, eff, err := s.SetActivity(ctx, "alice", "c1", false)
	require.NoError(t, err)
	require.Equal(t, model.StatusOnline, eff)

	changed, eff, err := s.SetExternal(ctx, "alice", model.StatusInCall, extTTL)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, model.StatusInCall, eff)
}

func TestSetExternal_InCall_OfflineStaysOffline(t *testing.T) {
	s := newStore(t)
	// No connection at all: the no-live-connection invariant beats in-call.
	// (changed may be true here — it's the first materialization of :status.)
	_, eff, err := s.SetExternal(context.Background(), "bob", model.StatusInCall, extTTL)
	require.NoError(t, err)
	assert.Equal(t, model.StatusOffline, eff)
}

func TestPrecedence_ManualAwayBeatsInCall(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, _, err := s.SetActivity(ctx, "carol", "c1", false)
	require.NoError(t, err)
	_, _, err = s.SetExternal(ctx, "carol", model.StatusInCall, extTTL)
	require.NoError(t, err)
	_, eff, err := s.SetManual(ctx, "carol", model.StatusAway)
	require.NoError(t, err)
	assert.Equal(t, model.StatusAway, eff)
}

func TestPrecedence_InCallBeatsManualBusy(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, _, err := s.SetActivity(ctx, "dave", "c1", false)
	require.NoError(t, err)
	_, _, err = s.SetManual(ctx, "dave", model.StatusBusy)
	require.NoError(t, err)
	_, eff, err := s.SetExternal(ctx, "dave", model.StatusInCall, extTTL)
	require.NoError(t, err)
	assert.Equal(t, model.StatusInCall, eff)
}

func TestPrecedence_AppearOfflineBeatsInCall(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, _, err := s.SetActivity(ctx, "erin", "c1", false)
	require.NoError(t, err)
	_, _, err = s.SetExternal(ctx, "erin", model.StatusInCall, extTTL)
	require.NoError(t, err)
	_, eff, err := s.SetManual(ctx, "erin", model.StatusAppearOffline)
	require.NoError(t, err)
	assert.Equal(t, model.StatusOffline, eff)
}

func TestSetExternal_Clear_RestoresConnectionDerived(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, _, err := s.SetActivity(ctx, "frank", "c1", false)
	require.NoError(t, err)
	_, _, err = s.SetExternal(ctx, "frank", model.StatusInCall, extTTL)
	require.NoError(t, err)
	_, eff, err := s.SetExternal(ctx, "frank", model.StatusNone, extTTL)
	require.NoError(t, err)
	assert.Equal(t, model.StatusOnline, eff)
}
