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

// OpenConfig carries OpenMongo's optional settings.
type OpenConfig struct {
	readPref *readpref.ReadPref
}

// OpenOption customizes OpenMongo.
type OpenOption func(*OpenConfig)

// WithKeyReadPreference routes both key handles to rp. A nil rp keeps the
// default rather than unsetting the preference, so a caller that forgets to
// wire config cannot silently drop back to the driver default.
func WithKeyReadPreference(rp *readpref.ReadPref) OpenOption {
	return func(c *OpenConfig) {
		if rp != nil {
			c.readPref = rp
		}
	}
}

// newOpenConfig applies opts over the default preference.
//
// primaryPreferred, not primary: broadcast-worker encrypts against its own
// handle, and key.get resolves the stamped version through this store. If the
// two disagree about falling back, the producer keeps delivering messages whose
// keys the consumer cannot fetch — strictly worse than both failing together.
// Falling back is safe because no rotation can commit without a primary, so the
// two sides read the same replicated state; a version evicted by a rotation
// during a brief election still resolves via GetByVersion's archive fallback.
func newOpenConfig(opts ...OpenOption) OpenConfig {
	c := OpenConfig{readPref: readpref.PrimaryPreferred()}
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// OpenMongo builds the store every service touching retired keys needs: both
// handles on the configured read preference, archive on, TTL index created.
// Returns errors, never exits.
func OpenMongo(ctx context.Context, db *mongo.Database, gracePeriod, retiredTTL time.Duration, opts ...OpenOption) (RoomKeyStore, error) {
	if gracePeriod <= 0 {
		return nil, fmt.Errorf("room key grace period must be a positive duration, got %s", gracePeriod)
	}
	if retiredTTL <= 0 {
		return nil, fmt.Errorf("retired room key TTL must be a positive duration, got %s", retiredTTL)
	}

	// Rotation and its confirming read-back run in a different service from the
	// key.get that resolves the archived version, so both handles share one
	// preference — they must not disagree about falling back.
	pref := options.Collection().SetReadPreference(newOpenConfig(opts...).readPref)
	store := NewMongoStore(
		db.Collection("rooms", pref),
		gracePeriod,
		WithRetiredKeys(db.Collection(RetiredKeysCollection, pref), retiredTTL),
	)

	idxCtx, cancel := context.WithTimeout(ctx, indexEnsureTimeout)
	defer cancel()
	if err := store.EnsureIndexes(idxCtx); err != nil {
		return nil, fmt.Errorf("ensure room key indexes: %w", err)
	}
	return store, nil
}
