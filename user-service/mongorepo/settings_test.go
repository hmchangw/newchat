//go:build integration

package mongorepo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

func ptrBool(b bool) *bool    { return &b }
func ptrStr(s string) *string { return &s }

func TestGetUserSettings_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users",
		bson.M{"_id": "u-alice", "account": "alice", "active": true},
		bson.M{"_id": "u-bob", "account": "bob", "active": true,
			"settings": bson.M{"fullWidth": true, "translateMessageInto": "en-US"}},
		bson.M{"_id": "u-ghost", "account": "ghost", "active": false,
			"settings": bson.M{"fullWidth": true}},
	)

	t.Run("never-set settings come back nil", func(t *testing.T) {
		u, err := r.GetUserSettings(ctx, "alice")
		require.NoError(t, err)
		require.NotNil(t, u)
		assert.Nil(t, u.Settings)
	})

	t.Run("stored sub-document round-trips", func(t *testing.T) {
		u, err := r.GetUserSettings(ctx, "bob")
		require.NoError(t, err)
		require.NotNil(t, u)
		require.NotNil(t, u.Settings)
		assert.Equal(t, ptrBool(true), u.Settings.FullWidth)
		assert.Equal(t, ptrStr("en-US"), u.Settings.TranslateMessageInto)
		assert.Nil(t, u.Settings.MuteAllNotifications)
	})

	t.Run("inactive user dropped", func(t *testing.T) {
		u, err := r.GetUserSettings(ctx, "ghost")
		require.NoError(t, err)
		assert.Nil(t, u)
	})

	t.Run("missing user not found", func(t *testing.T) {
		u, err := r.GetUserSettings(ctx, "nobody")
		require.NoError(t, err)
		assert.Nil(t, u)
	})
}

func TestUpdateUserSettings_PartialSet_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users",
		bson.M{"_id": "u-alice", "account": "alice", "active": true,
			"settings": bson.M{"fullWidth": true, "muteAllNotifications": true}},
	)

	// Partial update: only translateMessageInto + fullWidth sent.
	u, err := r.UpdateUserSettings(ctx, "alice", &model.UserSettings{
		FullWidth:            ptrBool(false),
		TranslateMessageInto: ptrStr("ja"),
	})
	require.NoError(t, err)
	require.NotNil(t, u)
	require.NotNil(t, u.Settings)
	assert.Equal(t, ptrBool(false), u.Settings.FullWidth, "sent field updated")
	assert.Equal(t, ptrStr("ja"), u.Settings.TranslateMessageInto, "sent field created")
	assert.Equal(t, ptrBool(true), u.Settings.MuteAllNotifications, "unsent field keeps stored value")
	assert.Nil(t, u.Settings.ScrollToBottomInChat, "unsent absent field stays absent")
}

func TestUpdateUserSettings_FirstSetCreatesSubDocument_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users", bson.M{"_id": "u-alice", "account": "alice", "active": true})

	u, err := r.UpdateUserSettings(ctx, "alice", &model.UserSettings{ScrollToBottomInChat: ptrBool(true)})
	require.NoError(t, err)
	require.NotNil(t, u)
	require.NotNil(t, u.Settings)
	assert.Equal(t, ptrBool(true), u.Settings.ScrollToBottomInChat)
	assert.Nil(t, u.Settings.FullWidth)
}

func TestUpdateUserSettings_NoActiveUser_Integration(t *testing.T) {
	r, db := newTestUserRepo(t)
	ctx := context.Background()
	seed(t, db, "users", bson.M{"_id": "u-ghost", "account": "ghost", "active": false})

	u, err := r.UpdateUserSettings(ctx, "ghost", &model.UserSettings{FullWidth: ptrBool(true)})
	require.NoError(t, err)
	assert.Nil(t, u)
}
