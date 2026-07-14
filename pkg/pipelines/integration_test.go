//go:build integration

package pipelines

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestSubscribedAccounts(t *testing.T) {
	subs := testutil.MongoDB(t, "pipelines_test").Collection("subscriptions")
	ctx := context.Background()

	// alice/bob subscribed to r1; carol subscribed to a different room.
	_, err := subs.InsertMany(ctx, []any{
		bson.M{"_id": "s1", "roomId": "r1", "u": bson.M{"account": "alice"}},
		bson.M{"_id": "s2", "roomId": "r1", "u": bson.M{"account": "bob"}},
		bson.M{"_id": "s3", "roomId": "r2", "u": bson.M{"account": "carol"}},
	})
	require.NoError(t, err)

	t.Run("returns only the subset subscribed to the room", func(t *testing.T) {
		got, err := SubscribedAccounts(ctx, subs, "r1", []string{"alice", "bob", "carol", "dave"})
		require.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"alice": {}, "bob": {}}, got)
	})

	t.Run("empty when none of the candidates are subscribed", func(t *testing.T) {
		got, err := SubscribedAccounts(ctx, subs, "r1", []string{"carol", "dave"})
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("does not match a different room", func(t *testing.T) {
		got, err := SubscribedAccounts(ctx, subs, "r2", []string{"alice", "bob"})
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("empty when the candidate set is empty", func(t *testing.T) {
		got, err := SubscribedAccounts(ctx, subs, "r1", []string{})
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}
