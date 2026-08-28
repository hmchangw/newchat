//go:build integration

package histdegrade

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestStore_SetGetClear(t *testing.T) {
	db := testutil.MongoDB(t, "histdegrade")
	s := NewStore(db)
	ctx := context.Background()

	t.Run("get returns nil for a healthy site", func(t *testing.T) {
		got, err := s.Get(ctx, "site-healthy")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("set then get returns the marker", func(t *testing.T) {
		require.NoError(t, s.Set(ctx, "site-a", 1700000000000))
		got, err := s.Get(ctx, "site-a")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "site-a", got.SiteID)
		assert.Equal(t, int64(1700000000000), got.DegradedSince)
	})

	t.Run("repeated set keeps the first degradedSince and advances updatedAt", func(t *testing.T) {
		require.NoError(t, s.Set(ctx, "site-b", 100))
		require.NoError(t, s.Set(ctx, "site-b", 999))
		got, err := s.Get(ctx, "site-b")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, int64(100), got.DegradedSince, "first failure timestamp must win")
		assert.Equal(t, int64(999), got.UpdatedAt)
	})

	t.Run("clear removes the marker", func(t *testing.T) {
		require.NoError(t, s.Set(ctx, "site-c", 1))
		require.NoError(t, s.Clear(ctx, "site-c"))
		got, err := s.Get(ctx, "site-c")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("clearing a healthy site is not an error", func(t *testing.T) {
		require.NoError(t, s.Clear(ctx, "site-never-set"))
	})

	t.Run("markers are isolated per site", func(t *testing.T) {
		require.NoError(t, s.Set(ctx, "site-d", 5))
		got, err := s.Get(ctx, "site-e")
		require.NoError(t, err)
		assert.Nil(t, got)
	})
}
