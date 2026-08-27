//go:build integration

package mongorepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// --- SubscriptionRepo integration tests ---

func TestSubscriptionRepo_GetSubscription(t *testing.T) {
	db := setupMongo(t)
	repo := NewSubscriptionRepo(db)
	ctx := context.Background()

	joinTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID:     "s1",
		User:   model.SubscriptionUser{ID: "u1", Account: "u1"},
		RoomID: "r1", SiteID: "site-local",
		Roles: []model.Role{model.RoleMember}, HistorySharedSince: &joinTime, JoinedAt: joinTime,
	})
	require.NoError(t, err)

	sub, err := repo.GetSubscription(ctx, "u1", "r1")
	require.NoError(t, err)
	require.NotNil(t, sub)
	assert.Equal(t, "u1", sub.User.ID)
	assert.Equal(t, "u1", sub.User.Account)
	assert.Equal(t, []model.Role{model.RoleMember}, sub.Roles)
}

// TestSubscriptionRepo_GetSubscription_ProjectionFields pins subscriptionReadProjection
// against a real Mongo decode: the fields service/pin.go reads come back populated,
// and the ones it does not are absent from the wire rather than merely unused.
// A widened projection is a silent regression the unit tests cannot see.
func TestSubscriptionRepo_GetSubscription_ProjectionFields(t *testing.T) {
	db := setupMongo(t)
	repo := NewSubscriptionRepo(db)
	ctx := context.Background()

	joinTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID:     "sproj",
		User:   model.SubscriptionUser{ID: "u9", Account: "carol", IsBot: true},
		RoomID: "rproj", SiteID: "site-local",
		Roles:              []model.Role{model.RoleOwner, model.RoleMember},
		Name:               "Room Name",
		RoomType:           model.RoomTypeChannel,
		ThreadUnread:       []string{"t1", "t2"},
		HistorySharedSince: &joinTime,
		LastSeenAt:         &joinTime,
		JoinedAt:           joinTime,
	})
	require.NoError(t, err)

	sub, err := repo.GetSubscription(ctx, "carol", "rproj")
	require.NoError(t, err)
	require.NotNil(t, sub)

	// Read by canBypassLargeRoomPin and the PinnedBy participant.
	assert.Equal(t, "u9", sub.User.ID)
	assert.Equal(t, "carol", sub.User.Account)
	assert.True(t, sub.User.IsBot)
	assert.Equal(t, []model.Role{model.RoleOwner, model.RoleMember}, sub.Roles)

	// Not read by any call site — must not be fetched. threadUnread is the
	// expensive one: an unbounded parent-ID list on an uncached hot path.
	assert.Empty(t, sub.ThreadUnread, "threadUnread must stay out of the projection")
	assert.Empty(t, sub.ID, "_id must stay out of the projection")
	assert.Empty(t, sub.RoomID)
	assert.Empty(t, sub.SiteID)
	assert.Empty(t, sub.Name)
	assert.Empty(t, sub.RoomType)
	assert.Nil(t, sub.HistorySharedSince, "GetHistorySharedSince is the projected accessor for this field")
	assert.Nil(t, sub.LastSeenAt)
	assert.True(t, sub.JoinedAt.IsZero())
}

func TestSubscriptionRepo_GetSubscription_NotFound(t *testing.T) {
	db := setupMongo(t)
	repo := NewSubscriptionRepo(db)
	ctx := context.Background()

	sub, err := repo.GetSubscription(ctx, "nonexistent", "r1")
	require.NoError(t, err)
	assert.Nil(t, sub)
}

func TestSubscriptionRepo_GetHistorySharedSince_NilHSS(t *testing.T) {
	db := setupMongo(t)
	repo := NewSubscriptionRepo(db)
	ctx := context.Background()

	// Insert subscription with no HistorySharedSince (owner — full history access)
	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID:     "s2",
		User:   model.SubscriptionUser{ID: "owner", Account: "owner"},
		RoomID: "r1", SiteID: "site-local",
		Roles: []model.Role{model.RoleOwner}, JoinedAt: time.Now(),
	})
	require.NoError(t, err)

	accessSince, subscribed, err := repo.GetHistorySharedSince(ctx, "owner", "r1")
	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Nil(t, accessSince) // nil = no lower-bound restriction (full history access)
}

func TestSubscriptionRepo_GetHistorySharedSince_WithHSS(t *testing.T) {
	db := setupMongo(t)
	repo := NewSubscriptionRepo(db)
	ctx := context.Background()

	joinTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID:     "s3",
		User:   model.SubscriptionUser{ID: "u2", Account: "u2"},
		RoomID: "r2", SiteID: "site-local",
		Roles: []model.Role{model.RoleMember}, HistorySharedSince: &joinTime, JoinedAt: joinTime,
	})
	require.NoError(t, err)

	accessSince, subscribed, err := repo.GetHistorySharedSince(ctx, "u2", "r2")
	require.NoError(t, err)
	assert.True(t, subscribed)
	require.NotNil(t, accessSince)
	assert.Equal(t, joinTime.UTC(), accessSince.UTC())
}

func TestSubscriptionRepo_GetHistorySharedSince_NotSubscribed(t *testing.T) {
	db := setupMongo(t)
	repo := NewSubscriptionRepo(db)
	ctx := context.Background()

	accessSince, subscribed, err := repo.GetHistorySharedSince(ctx, "nobody", "r1")
	require.NoError(t, err)
	assert.False(t, subscribed)
	assert.Nil(t, accessSince)
}
