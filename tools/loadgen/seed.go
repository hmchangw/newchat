package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/roomkeystore"
)

// roomKeyStore is the narrow consumer interface for room-key seeding.
type roomKeyStore interface {
	Set(ctx context.Context, roomID string, pair roomkeystore.RoomKeyPair) (int, error)
	Delete(ctx context.Context, roomID string) error
}

func insertDocs[T any](ctx context.Context, coll *mongo.Collection, items []T) error {
	if len(items) == 0 {
		return nil
	}
	docs := make([]interface{}, len(items))
	for i := range items {
		docs[i] = items[i]
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("insert into %s: %w", coll.Name(), err)
	}
	return nil
}

// seededCollections are the collections the loadgen owns the data for. Indexes
// on them are owned by the services' EnsureIndexes, not by the seeder.
var seededCollections = []string{"users", "rooms", "subscriptions"}

// resetData drops coll to reclaim its storage in constant time, but first
// captures the collection's existing index specs and recreates them on the
// now-empty collection. This lets the seeder reset data cheaply without owning
// any index definitions: whatever indexes the services' EnsureIndexes created
// (keys, key order, unique/partial/etc.) are preserved verbatim. The implicit
// _id_ index is skipped. A not-yet-created collection has no indexes to
// preserve and is left as a no-op for Drop to handle.
func resetData(ctx context.Context, coll *mongo.Collection) error {
	specs, err := captureIndexSpecs(ctx, coll)
	if err != nil {
		return fmt.Errorf("capture index specs for %s: %w", coll.Name(), err)
	}
	if err := coll.Drop(ctx); err != nil {
		return fmt.Errorf("drop %s: %w", coll.Name(), err)
	}
	if len(specs) == 0 {
		return nil
	}
	cmd := bson.D{{Key: "createIndexes", Value: coll.Name()}, {Key: "indexes", Value: specs}}
	if err := coll.Database().RunCommand(ctx, cmd).Err(); err != nil {
		return fmt.Errorf("restore indexes on %s: %w", coll.Name(), err)
	}
	return nil
}

// captureIndexSpecs returns coll's non-_id index specs as createIndexes-ready
// documents, passed through verbatim minus the server-managed fields that
// createIndexes rejects (index version, legacy namespace). Returns no specs
// when the collection does not yet exist.
func captureIndexSpecs(ctx context.Context, coll *mongo.Collection) ([]interface{}, error) {
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		var cmdErr mongo.CommandError
		if errors.As(err, &cmdErr) && cmdErr.Code == 26 { // NamespaceNotFound
			return nil, nil
		}
		return nil, fmt.Errorf("list indexes on %s: %w", coll.Name(), err)
	}
	var existing []bson.Raw
	if err := cur.All(ctx, &existing); err != nil {
		return nil, fmt.Errorf("read indexes on %s: %w", coll.Name(), err)
	}

	specs := make([]interface{}, 0, len(existing))
	for _, ix := range existing {
		if name, _ := ix.Lookup("name").StringValueOK(); name == "_id_" {
			continue // implicit, recreated automatically
		}
		elems, err := ix.Elements()
		if err != nil {
			return nil, fmt.Errorf("decode index spec on %s: %w", coll.Name(), err)
		}
		spec := bson.D{}
		for _, e := range elems {
			switch e.Key() {
			case "v", "ns": // server-managed; createIndexes rejects them
				continue
			}
			spec = append(spec, bson.E{Key: e.Key(), Value: e.Value()})
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

// Seed repopulates users/rooms/subscriptions in db from fixtures, replacing any
// existing data while preserving the services' indexes (see resetData).
// Idempotent: safe to rerun.
func Seed(ctx context.Context, db *mongo.Database, f *Fixtures) error {
	for _, c := range seededCollections {
		if err := resetData(ctx, db.Collection(c)); err != nil {
			return fmt.Errorf("reset %s before seed: %w", c, err)
		}
	}
	if err := insertDocs(ctx, db.Collection("users"), f.Users); err != nil {
		return err
	}
	if err := insertDocs(ctx, db.Collection("rooms"), f.Rooms); err != nil {
		return err
	}
	if err := insertDocs(ctx, db.Collection("subscriptions"), f.Subscriptions); err != nil {
		return err
	}
	return nil
}

// Teardown clears the three seeded collections without repopulating, preserving
// their service-owned indexes so a following Seed starts indexed.
func Teardown(ctx context.Context, db *mongo.Database) error {
	for _, c := range seededCollections {
		if err := resetData(ctx, db.Collection(c)); err != nil {
			return fmt.Errorf("teardown %s: %w", c, err)
		}
	}
	return nil
}

// SeedRoomKeys writes each room keypair into the keystore. Harmless when
// broadcast-worker runs with ENCRYPTION_ENABLED=false.
func SeedRoomKeys(ctx context.Context, keys roomKeyStore, roomKeys map[string]roomkeystore.RoomKeyPair) error {
	for roomID, pair := range roomKeys {
		if _, err := keys.Set(ctx, roomID, pair); err != nil {
			return fmt.Errorf("set room key %s: %w", roomID, err)
		}
	}
	return nil
}

// TeardownRoomKeys deletes the keypairs written by SeedRoomKeys.
func TeardownRoomKeys(ctx context.Context, keys roomKeyStore, roomIDs []string) error {
	for _, roomID := range roomIDs {
		if err := keys.Delete(ctx, roomID); err != nil {
			return fmt.Errorf("delete room key %s: %w", roomID, err)
		}
	}
	return nil
}
