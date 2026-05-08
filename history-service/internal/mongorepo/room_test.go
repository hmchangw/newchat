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

func TestRoomRepo_GetMinUserLastSeenAt_Set(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)
	ctx := context.Background()

	floor := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID:                "r1",
		Name:              "general",
		Type:              model.RoomTypeChannel,
		SiteID:            "site-local",
		MinUserLastSeenAt: &floor,
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	})
	require.NoError(t, err)

	got, err := repo.GetMinUserLastSeenAt(ctx, "r1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.WithinDuration(t, floor, *got, time.Second)
}

func TestRoomRepo_GetMinUserLastSeenAt_Unset(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, model.Room{
		ID:        "r2",
		Name:      "no-floor",
		Type:      model.RoomTypeChannel,
		SiteID:    "site-local",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	got, err := repo.GetMinUserLastSeenAt(ctx, "r2")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestRoomRepo_GetMinUserLastSeenAt_MissingDocument(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)
	ctx := context.Background()

	got, err := repo.GetMinUserLastSeenAt(ctx, "no-such-room")
	require.NoError(t, err)
	assert.Nil(t, got)
}
