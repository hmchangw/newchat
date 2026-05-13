//go:build integration

package mongorepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
)

func TestRoomRepo_GetRoomTimes(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)

	createdAt := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	lastMsgAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	room := model.Room{
		ID:        "room-times-1",
		SiteID:    "site-A",
		Type:      model.RoomTypeChannel,
		CreatedAt: createdAt,
		LastMsgAt: &lastMsgAt,
	}
	_, err := db.Collection("rooms").InsertOne(context.Background(), room)
	require.NoError(t, err)

	gotLast, gotCreated, err := repo.GetRoomTimes(context.Background(), "room-times-1")
	require.NoError(t, err)
	assert.Equal(t, lastMsgAt.UTC(), gotLast.UTC())
	assert.Equal(t, createdAt.UTC(), gotCreated.UTC())
}

func TestRoomRepo_GetRoomTimes_NoLastMsg(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)

	createdAt := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("rooms").InsertOne(context.Background(), bson.M{
		"_id":       "room-no-lastmsg",
		"siteId":    "site-A",
		"type":      "channel",
		"createdAt": createdAt,
	})
	require.NoError(t, err)

	gotLast, gotCreated, err := repo.GetRoomTimes(context.Background(), "room-no-lastmsg")
	require.NoError(t, err)
	assert.True(t, gotLast.IsZero(), "lastMsgAt absent → zero time")
	assert.Equal(t, createdAt.UTC(), gotCreated.UTC())
}

func TestRoomRepo_GetRoomTimes_NotFound(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)

	_, _, err := repo.GetRoomTimes(context.Background(), "no-such-room")
	require.ErrorIs(t, err, mongo.ErrNoDocuments)
}

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
