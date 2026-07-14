//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestValkeyIDMap_StoreResolve(t *testing.T) {
	client := testutil.StartValkeyCluster(t)
	ctx := context.Background()
	idm := newValkeyIDMap(client)

	// Nothing stored yet -> empty resolution.
	got, err := idm.Resolve(ctx, []string{"alice"})
	require.NoError(t, err)
	assert.Empty(t, got)

	require.NoError(t, idm.Store(ctx, map[string]string{"alice": "ida", "bob": "idb"}))

	got, err = idm.Resolve(ctx, []string{"alice", "carol"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"alice": "ida"}, got) // carol absent -> omitted
}

func TestValkeyInCallIndex_AddMembersRemove(t *testing.T) {
	client := testutil.StartValkeyCluster(t)
	ctx := context.Background()
	idx := newValkeyInCallIndex(client)

	require.NoError(t, idx.Add(ctx, "alice"))
	require.NoError(t, idx.Add(ctx, "bob"))
	m, err := idx.Members(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"alice", "bob"}, m)

	require.NoError(t, idx.Remove(ctx, "alice"))
	m, err = idx.Members(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"bob"}, m)
}
