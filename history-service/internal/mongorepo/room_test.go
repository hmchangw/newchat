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

// An explicit BSON null lastMsgAt (as opposed to the field being absent) must
// decode identically: nil pointer → zero time → "unknown" downstream.
func TestRoomRepo_GetRoomTimes_NullLastMsg(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)

	createdAt := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("rooms").InsertOne(context.Background(), bson.M{
		"_id":       "room-null-lastmsg",
		"siteId":    "site-A",
		"type":      "channel",
		"createdAt": createdAt,
		"lastMsgAt": nil,
	})
	require.NoError(t, err)

	gotLast, gotCreated, err := repo.GetRoomTimes(context.Background(), "room-null-lastmsg")
	require.NoError(t, err)
	assert.True(t, gotLast.IsZero(), "lastMsgAt null → zero time, same as absent")
	assert.Equal(t, createdAt.UTC(), gotCreated.UTC())
}

func TestRoomRepo_GetRoomTimes_NotFound(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)

	_, _, err := repo.GetRoomTimes(context.Background(), "no-such-room")
	require.ErrorIs(t, err, mongo.ErrNoDocuments)
}

func TestRoomRepo_GetRoomTimesByIDs(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)

	createdAt1 := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	lastMsgAt1 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	createdAt2 := time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC)
	lastMsgAt2 := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)

	rooms := []model.Room{
		{
			ID:        "batch-room-1",
			SiteID:    "site-A",
			Type:      model.RoomTypeChannel,
			CreatedAt: createdAt1,
			LastMsgAt: &lastMsgAt1,
		},
		{
			ID:        "batch-room-2",
			SiteID:    "site-A",
			Type:      model.RoomTypeChannel,
			CreatedAt: createdAt2,
			LastMsgAt: &lastMsgAt2,
		},
	}
	for _, room := range rooms {
		_, err := db.Collection("rooms").InsertOne(context.Background(), room)
		require.NoError(t, err)
	}

	got, err := repo.GetRoomTimesByIDs(context.Background(), []string{"batch-room-1", "batch-room-2", "missing-room"})
	require.NoError(t, err)
	require.Len(t, got, 2)

	assert.Equal(t, lastMsgAt1.UTC(), got["batch-room-1"].LastMsgAt.UTC())
	assert.Equal(t, createdAt1.UTC(), got["batch-room-1"].CreatedAt.UTC())
	assert.Equal(t, lastMsgAt2.UTC(), got["batch-room-2"].LastMsgAt.UTC())
	assert.Equal(t, createdAt2.UTC(), got["batch-room-2"].CreatedAt.UTC())

	_, missingPresent := got["missing-room"]
	assert.False(t, missingPresent, "missing room should be omitted, not an error")
}

func TestRoomRepo_GetRoomTimesByIDs_NoLastMsg(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)

	createdAt := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("rooms").InsertOne(context.Background(), bson.M{
		"_id":       "batch-room-no-lastmsg",
		"siteId":    "site-A",
		"type":      "channel",
		"createdAt": createdAt,
	})
	require.NoError(t, err)

	got, err := repo.GetRoomTimesByIDs(context.Background(), []string{"batch-room-no-lastmsg"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.True(t, got["batch-room-no-lastmsg"].LastMsgAt.IsZero(), "lastMsgAt absent -> zero time")
	assert.Equal(t, createdAt.UTC(), got["batch-room-no-lastmsg"].CreatedAt.UTC())
}

func TestRoomRepo_GetRoomTimesByIDs_Empty(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)

	got, err := repo.GetRoomTimesByIDs(context.Background(), []string{})
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
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

func TestRoomRepo_GetRoomUserCount(t *testing.T) {
	db := setupMongo(t)
	_, err := db.Collection("rooms").InsertOne(context.Background(),
		bson.M{"_id": "room-uc-1", "userCount": 42})
	require.NoError(t, err)

	repo := NewRoomRepo(db)
	count, err := repo.GetRoomUserCount(context.Background(), "room-uc-1")

	require.NoError(t, err)
	assert.Equal(t, 42, count)
}

func TestRoomRepo_GetRoomUserCount_RoomMissing(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)

	_, err := repo.GetRoomUserCount(context.Background(), "missing")

	require.ErrorIs(t, err, mongo.ErrNoDocuments)
}
