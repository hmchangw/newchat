package roomkeystore

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// indexEnsureTimeout makes a hung createIndexes fail startup rather than stall it.
const indexEnsureTimeout = 10 * time.Second

// OpenMongo builds the store every service touching retired keys needs: both
// handles primary-pinned, archive on, TTL index created. Returns errors, never exits.
func OpenMongo(ctx context.Context, db *mongo.Database, gracePeriod, retiredTTL time.Duration) (RoomKeyStore, error) {
	if gracePeriod <= 0 {
		return nil, fmt.Errorf("room key grace period must be a positive duration, got %s", gracePeriod)
	}
	if retiredTTL <= 0 {
		return nil, fmt.Errorf("retired room key TTL must be a positive duration, got %s", retiredTTL)
	}

	// Rotation and its confirming read-back run in a different service from the
	// key.get that resolves the archived version; a stale secondary breaks both.
	primary := options.Collection().SetReadPreference(readpref.Primary())
	store := NewMongoStore(
		db.Collection("rooms", primary),
		gracePeriod,
		WithRetiredKeys(db.Collection(RetiredKeysCollection, primary), retiredTTL),
	)

	idxCtx, cancel := context.WithTimeout(ctx, indexEnsureTimeout)
	defer cancel()
	if err := store.EnsureIndexes(idxCtx); err != nil {
		return nil, fmt.Errorf("ensure room key indexes: %w", err)
	}
	return store, nil
}
