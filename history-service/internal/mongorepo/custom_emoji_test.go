//go:build integration

package mongorepo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
)

func insertCustomEmoji(t *testing.T, db *mongo.Database, ce model.CustomEmoji) {
	t.Helper()
	_, err := db.Collection("custom_emojis").InsertOne(context.Background(), ce)
	require.NoError(t, err)
}

func TestCustomEmojiRepo_CustomEmojiExists_Found(t *testing.T) {
	db := setupMongo(t)
	repo := NewCustomEmojiRepo(db)
	ctx := context.Background()

	insertCustomEmoji(t, db, model.CustomEmoji{
		ID:        "ce-1",
		SiteID:    "site-a",
		Shortcode: "acme_party",
		ImageURL:  "https://cdn.example/acme.png",
		CreatedBy: "admin",
		CreatedAt: 1000,
	})

	got, err := repo.CustomEmojiExists(ctx, "site-a", "acme_party")
	require.NoError(t, err)
	assert.True(t, got)
}

func TestCustomEmojiRepo_CustomEmojiExists_NotFound(t *testing.T) {
	db := setupMongo(t)
	repo := NewCustomEmojiRepo(db)
	ctx := context.Background()

	got, err := repo.CustomEmojiExists(ctx, "site-a", "never_registered")
	require.NoError(t, err)
	assert.False(t, got, "ErrNoDocuments must translate to (false, nil)")
}

func TestCustomEmojiRepo_CustomEmojiExists_PerSiteIsolation(t *testing.T) {
	db := setupMongo(t)
	repo := NewCustomEmojiRepo(db)
	ctx := context.Background()

	insertCustomEmoji(t, db, model.CustomEmoji{
		ID: "ce-a", SiteID: "site-a", Shortcode: "party", CreatedAt: 1000,
	})
	insertCustomEmoji(t, db, model.CustomEmoji{
		ID: "ce-b", SiteID: "site-b", Shortcode: "other", CreatedAt: 1000,
	})

	gotAA, err := repo.CustomEmojiExists(ctx, "site-a", "party")
	require.NoError(t, err)
	assert.True(t, gotAA)

	// Same shortcode, different site → not found.
	gotBA, err := repo.CustomEmojiExists(ctx, "site-b", "party")
	require.NoError(t, err)
	assert.False(t, gotBA, "shortcode on site-a must not be visible to site-b")

	gotBB, err := repo.CustomEmojiExists(ctx, "site-b", "other")
	require.NoError(t, err)
	assert.True(t, gotBB)
}

func TestCustomEmojiRepo_EnsureIndexes_CreatesUniqueCompoundIndex(t *testing.T) {
	db := setupMongo(t)
	repo := NewCustomEmojiRepo(db)
	ctx := context.Background()

	require.NoError(t, repo.EnsureIndexes(ctx))

	cur, err := db.Collection("custom_emojis").Indexes().List(ctx)
	require.NoError(t, err)
	var indexes []bson.M
	require.NoError(t, cur.All(ctx, &indexes))

	var found bson.M
	for _, ix := range indexes {
		if name, _ := ix["name"].(string); name == "siteId_shortcode_unique" {
			found = ix
			break
		}
	}
	require.NotNil(t, found, "siteId_shortcode_unique index must exist")
	assert.Equal(t, true, found["unique"])
}

func TestCustomEmojiRepo_EnsureIndexes_Idempotent(t *testing.T) {
	db := setupMongo(t)
	repo := NewCustomEmojiRepo(db)
	ctx := context.Background()

	require.NoError(t, repo.EnsureIndexes(ctx))
	require.NoError(t, repo.EnsureIndexes(ctx), "calling EnsureIndexes twice must be a no-op")
}

func TestCustomEmojiRepo_EnsureIndexes_RejectsDuplicateSiteShortcode(t *testing.T) {
	db := setupMongo(t)
	repo := NewCustomEmojiRepo(db)
	ctx := context.Background()

	require.NoError(t, repo.EnsureIndexes(ctx))

	insertCustomEmoji(t, db, model.CustomEmoji{
		ID: "ce-1", SiteID: "site-a", Shortcode: "party", CreatedAt: 1000,
	})

	// Same (siteId, shortcode) with a different _id must violate the unique
	// index — this is the only dedup mechanism the codebase relies on.
	_, err := db.Collection("custom_emojis").InsertOne(ctx, model.CustomEmoji{
		ID: "ce-2", SiteID: "site-a", Shortcode: "party", CreatedAt: 2000,
	})
	require.Error(t, err)
	assert.True(t, mongo.IsDuplicateKeyError(err), "expected duplicate key error, got %v", err)

	// Same shortcode on a different site must still be allowed.
	_, err = db.Collection("custom_emojis").InsertOne(ctx, model.CustomEmoji{
		ID: "ce-3", SiteID: "site-b", Shortcode: "party", CreatedAt: 3000,
	})
	require.NoError(t, err)
}
