package mongoutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

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

// indexRestoreTimeout bounds the best-effort restore of the old index after a
// failed repair, on a context detached from the (possibly already-expired) caller.
const indexRestoreTimeout = 10 * time.Second

// EnsureIndexWithRepair creates model on coll and self-heals a pre-existing index
// with the SAME keys but a conflicting spec (e.g. a non-unique index where model
// is unique): it drops the conflicting index and recreates it from model.
//
// If the recreate fails — typically E11000 when the DATA holds duplicate values a
// unique index can't be built over — the old index is restored faithfully (its
// full spec, under a fresh context since the caller's may be the one that just
// expired) so the collection is never left without one, and the error is returned
// so the caller can surface the dedupe-preflight guidance.
//
// ponytail: concurrent repair across replicas on the same dirty index is narrowed
// (a retry before the destructive drop skips it once a peer has repaired, and a
// drop of an already-absent index is tolerated) but not fully serialized — a
// distributed lock is deliberately avoided for a one-time startup convergence; the
// write-gating follow-up closes the residual window.
func EnsureIndexWithRepair(ctx context.Context, coll *mongo.Collection, model mongo.IndexModel) error {
	if _, err := coll.Indexes().CreateOne(ctx, model); err == nil || !isIndexSpecConflict(err) {
		return err
	}
	// A peer replica may have repaired it since the first attempt — retry before the
	// destructive drop so we never drop a peer's freshly created correct index.
	if _, err := coll.Indexes().CreateOne(ctx, model); err == nil || !isIndexSpecConflict(err) {
		return err
	}
	old := existingIndexByKeys(ctx, coll, model.Keys)
	if old.name == "" {
		return fmt.Errorf("repair index on %s: conflicting index not found", coll.Name())
	}
	if err := coll.Indexes().DropOne(ctx, old.name); err != nil && !IsIndexNotFound(err) {
		return fmt.Errorf("repair index %q on %s: drop conflicting index: %w", old.name, coll.Name(), err)
	}
	if _, cerr := coll.Indexes().CreateOne(ctx, model); cerr != nil {
		// Restore the old index — its full spec, on a fresh context — so the
		// collection keeps an index; the returned error still carries the cause.
		restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), indexRestoreTimeout)
		defer cancel()
		if _, rerr := coll.Indexes().CreateOne(restoreCtx, old.model()); rerr != nil {
			slog.WarnContext(ctx, "mongo: index repair failed and the old index could not be restored",
				"collection", coll.Name(), "index", old.name, "error", rerr)
		}
		return fmt.Errorf("repair index %q on %s: %w", old.name, coll.Name(), cerr)
	}
	slog.WarnContext(ctx, "mongo: repaired a conflicting index (dropped and recreated to the expected spec)",
		"collection", coll.Name(), "droppedIndex", old.name)
	return nil
}

// isIndexSpecConflict reports whether err is Mongo's IndexOptionsConflict (85) or
// IndexKeySpecsConflict (86) — a same-keys index that differs in options or name.
func isIndexSpecConflict(err error) bool {
	var se mongo.ServerError
	return errors.As(err, &se) && (se.HasErrorCode(85) || se.HasErrorCode(86))
}

// IsIndexNotFound reports whether err is Mongo's IndexNotFound (27), so dropping an
// index a peer already removed is not treated as a failure.
func IsIndexNotFound(err error) bool {
	var se mongo.ServerError
	return errors.As(err, &se) && se.HasErrorCode(27)
}

// existingIndex is a pre-existing index captured for a faithful restore.
type existingIndex struct {
	name string
	keys bson.D
	spec bson.M
}

// model reconstructs an IndexModel reproducing the captured index, preserving the
// correctness-relevant options (unique, sparse, hidden, TTL, partial filter) so a
// restore never silently weakens the constraint. Collation is not reconstructed —
// no repaired index in this repo uses it.
func (e existingIndex) model() mongo.IndexModel {
	opts := options.Index().SetName(e.name)
	if v, _ := e.spec["unique"].(bool); v {
		opts = opts.SetUnique(true)
	}
	if v, _ := e.spec["sparse"].(bool); v {
		opts = opts.SetSparse(true)
	}
	if v, _ := e.spec["hidden"].(bool); v {
		opts = opts.SetHidden(true)
	}
	switch v := e.spec["expireAfterSeconds"].(type) {
	case int32:
		opts = opts.SetExpireAfterSeconds(v)
	case int64:
		opts = opts.SetExpireAfterSeconds(int32(v))
	}
	if v, ok := e.spec["partialFilterExpression"]; ok {
		opts = opts.SetPartialFilterExpression(v)
	}
	return mongo.IndexModel{Keys: e.keys, Options: opts}
}

// existingIndexByKeys returns the index on coll whose key spec matches keys
// (order-sensitive), captured for restore, or a zero value if none/unreadable.
func existingIndexByKeys(ctx context.Context, coll *mongo.Collection, keys any) existingIndex {
	want := keySpec(keys)
	if want == "" {
		return existingIndex{}
	}
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return existingIndex{}
	}
	defer func() { _ = cur.Close(ctx) }()
	for cur.Next(ctx) {
		var idx struct {
			Name string `bson:"name"`
			Key  bson.D `bson:"key"`
		}
		if cur.Decode(&idx) != nil || keySpec(idx.Key) != want {
			continue
		}
		var spec bson.M
		_ = bson.Unmarshal(cur.Current, &spec)
		return existingIndex{name: idx.Name, keys: idx.Key, spec: spec}
	}
	return existingIndex{}
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
