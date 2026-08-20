package mongoutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// WarnMissingIndexes logs a warning for each named index absent from coll and
// never returns an error. It is for services that DEPEND on an index another
// service owns: a dependent must not create the shared index (a divergent spec
// crashloops whichever service starts second) nor die when the owner has not
// built it yet. names are the owner's resolved index names (e.g. "account_1",
// "assistant_name_idx"); a name that drifts from the owner's spec only costs a
// spurious warning, never a failure.
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
// that has the SAME keys but a conflicting spec/options — e.g. a non-unique index
// where model is unique (the IndexOptionsConflict/IndexKeySpecsConflict, codes
// 85/86, that used to crashloop the service). On that conflict it drops the
// conflicting index and recreates it from model, so an environment carrying the
// old wrong index converges to the expected spec on startup.
//
// A duplicate-key error (E11000: the DATA holds duplicate values) is returned
// unchanged — dropping an index cannot fix duplicate data, and recreating a
// unique index over it would fail the same way; the caller surfaces the
// dedupe-preflight guidance. The drop+recreate runs at startup before the service
// serves traffic; the drop is best-effort so a concurrent replica that already
// dropped the index doesn't abort the repair.
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
