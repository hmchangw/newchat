package mongorepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const (
	settingsCollection     = "user_settings"
	maxDuplicateKeyRetries = 1
)

var settingsProjection = bson.M{
	"_id":       0,
	"account":   1,
	"siteId":    1,
	"data":      1,
	"version":   1,
	"updatedAt": 1,
}

// SettingsRepo is the Mongo implementation of service.SettingsRepository.
type SettingsRepo struct {
	settings *mongoutil.Collection[model.UserSettings]
}

// NewSettingsRepo builds a SettingsRepo over db.
func NewSettingsRepo(db *mongo.Database) *SettingsRepo {
	return &SettingsRepo{
		settings: mongoutil.NewCollection[model.UserSettings](db.Collection(settingsCollection)),
	}
}

// EnsureIndexes creates the unique logical key for one settings document per account and site.
func (r *SettingsRepo) EnsureIndexes(ctx context.Context) error {
	_, err := r.settings.Raw().Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "account", Value: 1}, {Key: "siteId", Value: 1}},
		Options: options.Index().SetName("user_settings_account_site_idx").SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("ensure user settings index: %w", err)
	}
	return nil
}

// GetUserSettings returns the settings document for account and site, or (nil, nil) when missing.
func (r *SettingsRepo) GetUserSettings(ctx context.Context, account, siteID string) (*model.UserSettings, error) {
	settings, err := r.settings.FindOne(ctx, bson.M{"account": account, "siteId": siteID}, mongoutil.WithProjection(settingsProjection))
	if err != nil {
		return nil, fmt.Errorf("get user settings: %w", err)
	}
	return settings, nil
}

// SetUserSettings writes opaque settings data and returns the post-write document.
// Conditional writes use the version in the filter so MongoDB's atomic FindOneAndUpdate
// makes a stale compare-and-set fail without changing the document.
func (r *SettingsRepo) SetUserSettings(ctx context.Context, account, siteID string, data json.RawMessage, ifVersion *int64) (*model.UserSettings, error) {
	now := time.Now().UTC()
	set := bson.M{"data": data, "updatedAt": now}
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetProjection(settingsProjection)

	if ifVersion != nil {
		res := r.settings.Raw().FindOneAndUpdate(ctx,
			bson.M{"account": account, "siteId": siteID, "version": *ifVersion},
			bson.M{"$set": set, "$inc": bson.M{"version": 1}},
			opts,
		)
		return decodeSettingsResult(res, true)
	}

	// The pipeline computes version from the current document, using 1 for an upsert
	// and incrementing an existing version. The whole operation remains atomic.
	update := mongo.Pipeline{bson.D{{Key: "$set", Value: bson.D{
		{Key: "account", Value: account},
		{Key: "siteId", Value: siteID},
		{Key: "data", Value: data},
		{Key: "version", Value: bson.D{{Key: "$add", Value: bson.A{
			bson.D{{Key: "$ifNull", Value: bson.A{"$version", int64(0)}}},
			int64(1),
		}}}},
		{Key: "updatedAt", Value: now},
	}}}}
	// Two first writes can race to upsert the same unique account/site key. The
	// loser retries once after the winner has created the document; that retry
	// follows the matched-document branch and atomically increments its version.
	for attempt := 0; attempt <= maxDuplicateKeyRetries; attempt++ {
		res := r.settings.Raw().FindOneAndUpdate(ctx,
			bson.M{"account": account, "siteId": siteID},
			update,
			opts.SetUpsert(true),
		)
		if err := res.Err(); err != nil {
			if mongo.IsDuplicateKeyError(err) && attempt < maxDuplicateKeyRetries {
				continue
			}
			return nil, fmt.Errorf("update user settings: %w", err)
		}
		var settings model.UserSettings
		if err := res.Decode(&settings); err != nil {
			return nil, fmt.Errorf("decode updated user settings: %w", err)
		}
		return &settings, nil
	}

	// Unreachable: the loop body always returns or continues.
	// Kept to satisfy the compiler's return-path analysis.
	return nil, fmt.Errorf("update user settings: unreachable: duplicate-key retry loop exited without returning")
}

func decodeSettingsResult(res *mongo.SingleResult, conditional bool) (*model.UserSettings, error) {
	if err := res.Err(); err != nil {
		if conditional && errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errcode.Conflict("user settings version conflict")
		}
		return nil, fmt.Errorf("update user settings: %w", err)
	}
	var settings model.UserSettings
	if err := res.Decode(&settings); err != nil {
		return nil, fmt.Errorf("decode updated user settings: %w", err)
	}
	return &settings, nil
}
