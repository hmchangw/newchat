//go:build integration

package pipelines

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/orgdisplay"
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

func TestOrgDisplayUsers(t *testing.T) {
	users := testutil.MongoDB(t, "pipelines_orgdisplay_test").Collection("users")
	ctx := context.Background()

	_, err := users.InsertMany(ctx, []any{
		bson.M{"_id": "u1", "account": "carol", "sectId": "eng", "sectName": "Engineering",
			"sectTCName": "工程", "sectDescription": "Builds the product"},
		bson.M{"_id": "u2", "account": "dave", "deptId": "eng", "deptName": "Engineering Dept",
			"deptTCName": "工程部", "deptDescription": "The whole department"},
		bson.M{"_id": "u3", "account": "erin", "sectId": "ops", "sectName": "Operations"},
	})
	require.NoError(t, err)

	t.Run("returns dept and sect matches with the full display projection", func(t *testing.T) {
		got, err := OrgDisplayUsers(ctx, users, []string{"eng"})
		require.NoError(t, err)
		assert.ElementsMatch(t, []orgdisplay.User{
			{SectID: "eng", SectName: "Engineering", SectTCName: "工程", SectDescription: "Builds the product"},
			{DeptID: "eng", DeptName: "Engineering Dept", DeptTCName: "工程部", DeptDescription: "The whole department"},
		}, got)
	})

	t.Run("empty for an org no user belongs to", func(t *testing.T) {
		got, err := OrgDisplayUsers(ctx, users, []string{"ghost"})
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("multiple orgs resolve in one batch", func(t *testing.T) {
		got, err := OrgDisplayUsers(ctx, users, []string{"eng", "ops"})
		require.NoError(t, err)
		assert.Len(t, got, 3)
	})
}
