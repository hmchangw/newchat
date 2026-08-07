//go:build integration

package mongorepo

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/preview"
)

func TestRoomRepo_GetRoomTimes(t *testing.T) {
	db := setupMongo(t)
	repo := newRoomRepoAtEpoch(db, 1)

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
	repo := newRoomRepoAtEpoch(db, 1)

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
	repo := newRoomRepoAtEpoch(db, 1)

	_, _, err := repo.GetRoomTimes(context.Background(), "no-such-room")
	require.ErrorIs(t, err, mongo.ErrNoDocuments)
}

func TestRoomRepo_GetRoomTimesByIDs(t *testing.T) {
	db := setupMongo(t)
	repo := newRoomRepoAtEpoch(db, 1)

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
	repo := newRoomRepoAtEpoch(db, 1)

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
	repo := newRoomRepoAtEpoch(db, 1)

	got, err := repo.GetRoomTimesByIDs(context.Background(), []string{})
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
}

func TestRoomRepo_GetMinUserLastSeenAt_Set(t *testing.T) {
	db := setupMongo(t)
	repo := newRoomRepoAtEpoch(db, 1)
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
	repo := newRoomRepoAtEpoch(db, 1)
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
	repo := newRoomRepoAtEpoch(db, 1)
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

	repo := newRoomRepoAtEpoch(db, 1)
	count, err := repo.GetRoomUserCount(context.Background(), "room-uc-1")

	require.NoError(t, err)
	assert.Equal(t, 42, count)
}

func TestRoomRepo_GetRoomUserCount_RoomMissing(t *testing.T) {
	db := setupMongo(t)
	repo := newRoomRepoAtEpoch(db, 1)

	_, err := repo.GetRoomUserCount(context.Background(), "missing")

	require.ErrorIs(t, err, mongo.ErrNoDocuments)
}

// The watermark guard is shared with the clear path (pkg/preview.guardedFields):
// a first write lands on an empty doc, and a lower asOf is rejected outright.
func TestRoomRepo_SetPreviewMessage_WatermarkGuard(t *testing.T) {
	db := setupMongo(t)
	repo := newRoomRepoAtEpoch(db, 1)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{"_id": "warmback-r1", "name": "room"})
	require.NoError(t, err)

	p1 := model.PreviewMessage{MessageID: "m1", Content: "one", CreatedAt: time.UnixMilli(1000).UTC()}
	p2 := model.PreviewMessage{MessageID: "m2", Content: "two", CreatedAt: time.UnixMilli(2000).UTC()}

	// First write lands on an empty doc (no previewAsOf => guard treats it as 0).
	require.NoError(t, repo.SetPreviewMessage(ctx, "warmback-r1", p2, "m2", 200))
	got := readRoomDoc(t, db, "warmback-r1")
	require.NotNil(t, got.PreviewMeta)
	assert.Equal(t, "m2", got.PreviewMeta.MessageID)
	assert.Equal(t, int64(200), got.PreviewAsOf)

	// A lower asOf is rejected — stored preview and watermark both unchanged.
	require.NoError(t, repo.SetPreviewMessage(ctx, "warmback-r1", p1, "m1", 100))
	got = readRoomDoc(t, db, "warmback-r1")
	require.NotNil(t, got.PreviewMeta)
	assert.Equal(t, "m2", got.PreviewMeta.MessageID)
	assert.Equal(t, int64(200), got.PreviewAsOf)
}

// The room doc must never hold a message body in the clear — that is the whole
// reason the preview is sealed rather than stored as a plaintext subdocument.
func TestRoomRepo_SetPreviewMessage_StoresNoPlaintextBody(t *testing.T) {
	db := setupMongo(t)
	repo := newRoomRepoAtEpoch(db, 1)
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{"_id": "sealed-r1", "name": "room"})
	require.NoError(t, err)

	pvw := model.PreviewMessage{
		MessageID: "m1",
		Sender:    model.Participant{Account: "alice"},
		Content:   "the-secret-body",
		CreatedAt: time.UnixMilli(1000).UTC(),
		Attachments: []cassandra.Attachment{
			{ID: "f1", Title: "secret-attachment.png", Type: "image"},
		},
	}
	require.NoError(t, repo.SetPreviewMessage(ctx, "sealed-r1", pvw, "m1", 100))

	var raw bson.M
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "sealed-r1"}).Decode(&raw))
	dump, err := json.Marshal(raw)
	require.NoError(t, err)
	assert.NotContains(t, string(dump), "the-secret-body")
	assert.NotContains(t, string(dump), "secret-attachment.png")
	// The plaintext half is still there — it is what the freshness check needs.
	assert.Contains(t, string(dump), "previewMeta")
}

// A stored preview is served back only when it is current: previewForMsgId must
// still equal the room's lastMsgId.
func TestRoomRepo_GetRoomTimesByIDs_ServesFreshStoredPreview(t *testing.T) {
	db := setupMongo(t)
	repo := newRoomRepoAtEpoch(db, 1)
	ctx := context.Background()

	lastMsgAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	insertPreviewRoom(t, db, "fresh-r1", lastMsgAt, "m9")

	pvw := model.PreviewMessage{
		MessageID: "m1",
		Sender:    model.Participant{Account: "alice"},
		Content:   "hello",
		CreatedAt: lastMsgAt,
		Mentions:  []model.Participant{{Account: "bob"}},
	}
	// Freshness key is the newest OBSERVED id (m9), not the preview's own (m1).
	require.NoError(t, repo.SetPreviewMessage(ctx, "fresh-r1", pvw, "m9", 100))

	got, err := repo.GetRoomTimesByIDs(ctx, []string{"fresh-r1"})
	require.NoError(t, err)
	require.NotNil(t, got["fresh-r1"].Preview)
	assert.Equal(t, "hello", got["fresh-r1"].Preview.Content)
	assert.Equal(t, "m1", got["fresh-r1"].Preview.MessageID)
	assert.Equal(t, []model.Participant{{Account: "bob"}}, got["fresh-r1"].Preview.Mentions)
}

// A new message advances lastMsgId past the key the preview was stored under, so
// the memo is stale and the caller must walk instead.
func TestRoomRepo_GetRoomTimesByIDs_StalePreviewNotServed(t *testing.T) {
	db := setupMongo(t)
	repo := newRoomRepoAtEpoch(db, 1)
	ctx := context.Background()

	lastMsgAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	insertPreviewRoom(t, db, "stale-r1", lastMsgAt, "m9")
	pvw := model.PreviewMessage{MessageID: "m1", Content: "old", CreatedAt: lastMsgAt}
	require.NoError(t, repo.SetPreviewMessage(ctx, "stale-r1", pvw, "m9", 100))

	// A newer message lands: lastMsgId moves on, previewForMsgId does not.
	_, err := db.Collection("rooms").UpdateOne(ctx,
		bson.M{"_id": "stale-r1"}, bson.M{"$set": bson.M{"lastMsgId": "m10"}})
	require.NoError(t, err)

	got, err := repo.GetRoomTimesByIDs(ctx, []string{"stale-r1"})
	require.NoError(t, err)
	assert.Nil(t, got["stale-r1"].Preview, "a superseded preview must not be served")
}

// Rotation: a reader on a different epoch treats the stored preview as absent
// rather than reaching for a retired DEK.
func TestRoomRepo_GetRoomTimesByIDs_EpochMismatchNotServed(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	lastMsgAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	insertPreviewRoom(t, db, "epoch-r1", lastMsgAt, "m9")
	pvw := model.PreviewMessage{MessageID: "m1", Content: "old", CreatedAt: lastMsgAt}
	require.NoError(t, newRoomRepoAtEpoch(db, 1).SetPreviewMessage(ctx, "epoch-r1", pvw, "m9", 100))

	// Same doc, read by a process configured one epoch ahead.
	got, err := newRoomRepoAtEpoch(db, 2).GetRoomTimesByIDs(ctx, []string{"epoch-r1"})
	require.NoError(t, err)
	assert.Nil(t, got["epoch-r1"].Preview)

	// The original epoch still reads it, so rotation is lazy, not destructive.
	got, err = newRoomRepoAtEpoch(db, 1).GetRoomTimesByIDs(ctx, []string{"epoch-r1"})
	require.NoError(t, err)
	require.NotNil(t, got["epoch-r1"].Preview)
}

// A room that never had a preview simply reports none.
func TestRoomRepo_GetRoomTimesByIDs_NoStoredPreview(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()

	insertPreviewRoom(t, db, "bare-r1", time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), "m9")

	got, err := newRoomRepoAtEpoch(db, 1).GetRoomTimesByIDs(ctx, []string{"bare-r1"})
	require.NoError(t, err)
	assert.Nil(t, got["bare-r1"].Preview)
}

// ClearPreview must remove EVERY preview field together — leaving a nonce or an
// epoch behind would strand a fragment describing a ciphertext that is gone —
// while still advancing the watermark so an older redelivery can't resurrect it.
func TestRoomRepo_ClearPreview_RemovesEveryFieldAndHoldsWatermark(t *testing.T) {
	db := setupMongo(t)
	repo := newRoomRepoAtEpoch(db, 1)
	ctx := context.Background()

	lastMsgAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	insertPreviewRoom(t, db, "clear-r1", lastMsgAt, "m9")
	pvw := model.PreviewMessage{MessageID: "m1", Content: "gone soon", CreatedAt: lastMsgAt}
	require.NoError(t, repo.SetPreviewMessage(ctx, "clear-r1", pvw, "m9", 100))

	require.NoError(t, repo.ClearPreview(ctx, "clear-r1", 200))

	var raw bson.M
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "clear-r1"}).Decode(&raw))
	for _, key := range []string{
		"previewMeta", "previewCiphertext", "previewNonce", "previewKeyEpoch", "previewForMsgId",
	} {
		assert.NotContains(t, raw, key, "%s must be removed by a clear", key)
	}
	assert.EqualValues(t, 200, raw["previewAsOf"], "the watermark must advance on clear")

	// A redelivered older write cannot resurrect the cleared preview.
	require.NoError(t, repo.SetPreviewMessage(ctx, "clear-r1", pvw, "m9", 150))
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "clear-r1"}).Decode(&raw))
	assert.NotContains(t, raw, "previewMeta")
}

// With the cipher disabled (ATREST_ENABLED=false) preview storage is off entirely
// rather than degrading to a plaintext body on the room doc.
func TestRoomRepo_NoCipher_StoresNothing(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	repo := NewRoomRepo(db, nil, preview.Key{SiteID: testPreviewSite, Epoch: 1})

	insertPreviewRoom(t, db, "nocipher-r1", time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), "m9")
	pvw := model.PreviewMessage{MessageID: "m1", Content: "plaintext", CreatedAt: time.UnixMilli(1000).UTC()}
	require.NoError(t, repo.SetPreviewMessage(ctx, "nocipher-r1", pvw, "m9", 100))
	require.NoError(t, repo.ClearPreview(ctx, "nocipher-r1", 200))

	var raw bson.M
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "nocipher-r1"}).Decode(&raw))
	assert.NotContains(t, raw, "previewMeta")
	assert.NotContains(t, raw, "previewAsOf")
}

func insertPreviewRoom(t *testing.T, db *mongo.Database, roomID string, lastMsgAt time.Time, lastMsgID string) {
	t.Helper()
	_, err := db.Collection("rooms").InsertOne(context.Background(), model.Room{
		ID:        roomID,
		SiteID:    testPreviewSite,
		Type:      model.RoomTypeChannel,
		CreatedAt: lastMsgAt.AddDate(0, -1, 0),
		LastMsgAt: &lastMsgAt,
		LastMsgID: lastMsgID,
	})
	require.NoError(t, err)
}

func readRoomDoc(t *testing.T, db *mongo.Database, roomID string) *model.Room {
	t.Helper()
	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(context.Background(), bson.M{"_id": roomID}).Decode(&room))
	return &room
}
