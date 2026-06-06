package mongorepo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const customEmojisCollection = "custom_emojis"

// CustomEmojiRepo is the read-only adapter the reaction handler uses to
// resolve whether a custom shortcode exists for the site. Admin CRUD lives
// elsewhere; this repo only exposes existence checks.
type CustomEmojiRepo struct {
	col *mongo.Collection
}

func NewCustomEmojiRepo(db *mongo.Database) *CustomEmojiRepo {
	return &CustomEmojiRepo{col: db.Collection(customEmojisCollection)}
}

// EnsureIndexes creates the (siteId, shortcode) unique index; idempotent.
func (r *CustomEmojiRepo) EnsureIndexes(ctx context.Context) error {
	_, err := r.col.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "siteId", Value: 1}, {Key: "shortcode", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("siteId_shortcode_unique"),
	})
	if err != nil {
		return fmt.Errorf("ensure custom_emojis indexes: %w", err)
	}
	return nil
}

// CustomEmojiExists reports whether a custom emoji is registered for the
// given site and bare shortcode. A missing document returns (false, nil) —
// the caller treats that as ErrUnknownShortcode at the validator layer.
func (r *CustomEmojiRepo) CustomEmojiExists(ctx context.Context, siteID, shortcode string) (bool, error) {
	err := r.col.FindOne(ctx, bson.M{"siteId": siteID, "shortcode": shortcode},
		options.FindOne().SetProjection(bson.M{"_id": 1}),
	).Err()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, fmt.Errorf("lookup custom emoji %q for site %q: %w", shortcode, siteID, err)
	}
	return true, nil
}
