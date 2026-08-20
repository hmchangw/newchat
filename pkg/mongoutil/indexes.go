package mongoutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// WarnMissingIndexes warns (never errors) for each named index absent from coll.
// For a service that DEPENDS on an index another owns: creating the shared index
// with a divergent spec crashloops whichever service starts second, and a
// not-yet-built index must not take the dependent down. names are the owner's
// resolved names (e.g. "account_1").
func WarnMissingIndexes(ctx context.Context, coll *mongo.Collection, names ...string) {
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		slog.WarnContext(ctx, "mongo: cannot list indexes to verify dependencies",
			"collection", coll.Name(), "error", err)
		return
	}
	defer func() { _ = cur.Close(ctx) }()

	have := make(map[string]bool)
	for cur.Next(ctx) {
		var idx struct {
			Name string `bson:"name"`
		}
		if err := cur.Decode(&idx); err == nil {
			have[idx.Name] = true
		}
	}
	for _, n := range names {
		if !have[n] {
			slog.WarnContext(ctx, "mongo: depended-on index missing; its owner service must create it",
				"collection", coll.Name(), "index", n)
		}
	}
}

// EnsureIndexWithRepair creates model on coll and self-heals a pre-existing index
// with the SAME keys but a conflicting spec (e.g. a non-unique index where model
// is unique): it drops the conflicting index and recreates it from model.
//
// If the recreate fails — typically E11000 when the DATA holds duplicate values a
// unique index can't be built over — the old index is restored so the collection
// is never left without one, and the E11000 is returned so the caller can surface
// the dedupe-preflight guidance.
func EnsureIndexWithRepair(ctx context.Context, coll *mongo.Collection, model mongo.IndexModel) error {
	_, err := coll.Indexes().CreateOne(ctx, model)
	if err == nil || !isIndexSpecConflict(err) {
		return err
	}
	name := indexNameByKeys(ctx, coll, model.Keys)
	if name == "" {
		return err // can't identify the conflicting index; surface the original error
	}
	// Best-effort: a peer replica may have already dropped it — the recreate below
	// is the real check.
	_ = coll.Indexes().DropOne(ctx, name) //nolint:errcheck
	if _, cerr := coll.Indexes().CreateOne(ctx, model); cerr != nil {
		// Recreate failed (e.g. E11000: duplicate values block a unique index).
		// Restore the old index so the collection is never left without one; the
		// returned error still carries the cause so the caller surfaces its guidance.
		if _, rerr := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: model.Keys, Options: options.Index().SetName(name),
		}); rerr != nil {
			slog.WarnContext(ctx, "mongo: index repair failed and the old index could not be restored",
				"collection", coll.Name(), "index", name, "error", rerr)
		}
		return fmt.Errorf("repair index %q on %s: %w", name, coll.Name(), cerr)
	}
	slog.WarnContext(ctx, "mongo: repaired a conflicting index (dropped and recreated to the expected spec)",
		"collection", coll.Name(), "droppedIndex", name)
	return nil
}

// isIndexSpecConflict reports whether err is Mongo's IndexOptionsConflict (85) or
// IndexKeySpecsConflict (86) — a same-keys index that differs in options or name.
func isIndexSpecConflict(err error) bool {
	var se mongo.ServerError
	return errors.As(err, &se) && (se.HasErrorCode(85) || se.HasErrorCode(86))
}

// indexNameByKeys returns the name of the existing index on coll whose key spec
// matches keys (order-sensitive), or "" if none/unreadable.
func indexNameByKeys(ctx context.Context, coll *mongo.Collection, keys any) string {
	want := keySpec(keys)
	if want == "" {
		return ""
	}
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return ""
	}
	defer func() { _ = cur.Close(ctx) }()
	for cur.Next(ctx) {
		var idx struct {
			Name string `bson:"name"`
			Key  bson.D `bson:"key"`
		}
		if cur.Decode(&idx) == nil && keySpec(idx.Key) == want {
			return idx.Name
		}
	}
	return ""
}

// keySpec renders an index key document as "field:dir,..." preserving order, so
// two key specs compare equal iff they are the same compound index.
func keySpec(keys any) string {
	d, ok := keys.(bson.D)
	if !ok {
		return ""
	}
	parts := make([]string, 0, len(d))
	for _, e := range d {
		parts = append(parts, fmt.Sprintf("%s:%v", e.Key, e.Value))
	}
	return strings.Join(parts, ",")
}
