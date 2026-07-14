//go:build integration

package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func emojiFixture(siteID, shortcode, by string, at int64) *model.CustomEmoji {
	return &model.CustomEmoji{
		ID:          siteID + ":" + shortcode,
		SiteID:      siteID,
		Shortcode:   shortcode,
		ImageURL:    "/api/v1/emoji/" + shortcode,
		CreatedBy:   by,
		CreatedAt:   at,
		UpdatedBy:   by,
		UpdatedAt:   at,
		MinioKey:    "emoji/" + siteID + "/" + shortcode,
		ContentType: "image/png",
		Size:        128,
		ETag:        "etag-" + shortcode,
	}
}

func TestMongoStore_EmojiCRUD(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "emoji")
	st := newMongoStore(db)
	require.NoError(t, st.EnsureEmojiIndexes(ctx))

	// Insert.
	require.NoError(t, st.UpsertEmoji(ctx, emojiFixture("s1", "party", "alice", 1000)))

	// EmojiDoc projects minioKey + etag.
	doc, found, err := st.EmojiDoc(ctx, "s1", "party")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "emoji/s1/party", doc.MinioKey)
	assert.Equal(t, "etag-party", doc.ETag)

	// Miss.
	_, found, err = st.EmojiDoc(ctx, "s1", "nope")
	require.NoError(t, err)
	assert.False(t, found)

	// Overwrite preserves createdBy/createdAt, updates the rest, no dup docs.
	over := emojiFixture("s1", "party", "bob", 2000)
	over.ContentType = "image/gif"
	require.NoError(t, st.UpsertEmoji(ctx, over))
	n, err := db.Collection("custom_emojis").CountDocuments(ctx, bson.M{"siteId": "s1", "shortcode": "party"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
	var full model.CustomEmoji
	require.NoError(t, db.Collection("custom_emojis").
		FindOne(ctx, bson.M{"_id": "s1:party"}).Decode(&full))
	assert.Equal(t, "alice", full.CreatedBy)
	assert.EqualValues(t, 1000, full.CreatedAt)
	assert.Equal(t, "bob", full.UpdatedBy)
	assert.EqualValues(t, 2000, full.UpdatedAt)
	assert.Equal(t, "image/gif", full.ContentType)

	// List is sorted by shortcode and projects the wire fields.
	require.NoError(t, st.UpsertEmoji(ctx, emojiFixture("s1", "aaa_first", "alice", 1000)))
	require.NoError(t, st.UpsertEmoji(ctx, emojiFixture("s2", "other_site", "alice", 1000)))
	list, err := st.ListEmojis(ctx, "s1")
	require.NoError(t, err)
	require.Len(t, list, 2)
	assert.Equal(t, "aaa_first", list[0].Shortcode)
	assert.Equal(t, "party", list[1].Shortcode)
	assert.Empty(t, list[0].CreatedBy, "list must not project createdBy")
	assert.Empty(t, list[0].MinioKey, "list must not project minioKey")

	// Delete returns the minioKey; second delete reports not found.
	key, found, err := st.DeleteEmoji(ctx, "s1", "party")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "emoji/s1/party", key)
	_, found, err = st.DeleteEmoji(ctx, "s1", "party")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestMongoStore_EnsureEmojiIndexes_Idempotent(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "emojiidx")
	st := newMongoStore(db)
	require.NoError(t, st.EnsureEmojiIndexes(ctx))
	require.NoError(t, st.EnsureEmojiIndexes(ctx))
}

func TestMinioBlobStore_Delete(t *testing.T) {
	ctx := context.Background()
	client, bucket := testutil.MinIO(t, "emoji")
	bs := newMinioBlobStore(client, bucket)

	_, err := bs.Put(ctx, "emoji/party", bytesReader("PNG"), 3, "image/png")
	require.NoError(t, err)
	require.NoError(t, bs.Delete(ctx, "emoji/party"))
	_, _, err = bs.Get(ctx, "emoji/party")
	assert.ErrorIs(t, err, errBlobNotFound)

	// Deleting a missing key is a no-op (S3 semantics).
	assert.NoError(t, bs.Delete(ctx, "emoji/missing"))
}

func bytesReader(s string) *strings.Reader { return strings.NewReader(s) }

func TestEmojiNATS_EndToEnd(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "emojirpc")
	client, bucket := testutil.MinIO(t, "emojirpc")
	st := newMongoStore(db)
	require.NoError(t, st.EnsureEmojiIndexes(ctx))
	bs := newMinioBlobStore(client, bucket)

	nc, err := otelnats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	h := newHandler(st, st, bs, &config{
		SiteID:             "s1",
		EIDCacheCapacity:   10,
		EIDCacheTTL:        time.Minute,
		EmojiDeleteEnabled: true,
	})
	router := natsrouter.New(nc, "media-service-"+t.Name())
	router.Use(natsrouter.Recovery(), natsrouter.RequestID(), natsrouter.Logging())
	registerEmojiNATS(router, h, "s1")
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = router.Shutdown(sctx)
	})

	// Seed one emoji through the store (doc + blob).
	_, err = bs.Put(ctx, "emoji/s1/party", bytesReader("GIF!"), 4, "image/gif")
	require.NoError(t, err)
	require.NoError(t, st.UpsertEmoji(ctx, emojiFixture("s1", "party", "alice", 1000)))

	// List over real NATS request-reply.
	msg, err := nc.Request(ctx, subject.EmojiList("alice", "s1"), nil, 5*time.Second)
	require.NoError(t, err)
	var list model.EmojiListResponse
	require.NoError(t, json.Unmarshal(msg.Data, &list))
	require.Len(t, list.Emojis, 1)
	assert.Equal(t, "party", list.Emojis[0].Shortcode)
	assert.Equal(t, "/api/v1/emoji/party", list.Emojis[0].ImageURL)
	assert.Equal(t, time.UnixMilli(1000).UTC(), list.Emojis[0].UpdatedAt,
		"updatedAt must cross the wire as RFC3339")

	// Delete over real NATS request-reply.
	body, err := json.Marshal(model.EmojiDeleteRequest{Shortcode: "party"})
	require.NoError(t, err)
	msg, err = nc.Request(ctx, subject.EmojiDelete("alice", "s1"), body, 5*time.Second)
	require.NoError(t, err)
	var del model.EmojiDeleteResponse
	require.NoError(t, json.Unmarshal(msg.Data, &del))
	assert.True(t, del.Deleted)

	// Doc and blob are both gone.
	_, found, err := st.EmojiDoc(ctx, "s1", "party")
	require.NoError(t, err)
	assert.False(t, found)
	_, _, err = bs.Get(ctx, "emoji/s1/party")
	assert.ErrorIs(t, err, errBlobNotFound)

	// Second delete returns the not_found envelope.
	msg, err = nc.Request(ctx, subject.EmojiDelete("alice", "s1"), body, 5*time.Second)
	require.NoError(t, err)
	assert.Contains(t, string(msg.Data), `"not_found"`)
}
