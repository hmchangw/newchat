package mongoutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// IndexEnsureTimeout bounds a service's startup index work, so a MongoDB that answers hello but
// stalls commands cannot hang startup. A degradable service keeps the attempt best-effort.
const IndexEnsureTimeout = 30 * time.Second

// WarnMissingIndexes warns (never errors) for each named index absent from coll.
// For a service that DEPENDS on an index another owns: creating the shared index
// with a divergent spec crashloops whichever service starts second, and a
// not-yet-built index must not take the dependent down. names are the owner's
// resolved names (e.g. "account_1").
func WarnMissingIndexes(ctx context.Context, coll *mongo.Collection, names ...string) {
	warnMissingIndexes(ctx, coll, false, names...)
}

// WarnMissingUniqueIndexes also warns when a named index exists but lost its unique option, which the
// name-only WarnMissingIndexes passes silently. Warn-only: a degradable service cannot list indexes mid-outage.
func WarnMissingUniqueIndexes(ctx context.Context, coll *mongo.Collection, names ...string) {
	warnMissingIndexes(ctx, coll, true, names...)
}

func warnMissingIndexes(ctx context.Context, coll *mongo.Collection, requireUnique bool, names ...string) {
	have, err := listIndexUniqueness(ctx, coll)
	if err != nil {
		slog.WarnContext(ctx, "mongo: cannot list indexes to verify dependencies",
			"collection", coll.Name(), "error", err)
		return
	}
	absent, nonUnique := missingUniqueIndexes(have, names...)
	for _, n := range absent {
		slog.WarnContext(ctx, "mongo: depended-on index missing; its owner service must create it",
			"collection", coll.Name(), "index", n)
	}
	if !requireUnique {
		return
	}
	for _, n := range nonUnique {
		slog.WarnContext(ctx, "mongo: depended-on index exists but is not unique; its owner service must repair it",
			"collection", coll.Name(), "index", n)
	}
}

// listIndexUniqueness maps index name to unique; the option is absent on a non-unique index, so false is honest.
func listIndexUniqueness(ctx context.Context, coll *mongo.Collection) (map[string]bool, error) {
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list indexes: %w", err)
	}
	defer func() { _ = cur.Close(ctx) }()

	have := make(map[string]bool)
	for cur.Next(ctx) {
		var idx struct {
			Name   string `bson:"name"`
			Unique bool   `bson:"unique"`
		}
		if err := cur.Decode(&idx); err != nil {
			// Skipped deliberately: the caller reads an undecodable index as absent; the cause stays at debug.
			slog.DebugContext(ctx, "mongo: skipping undecodable index document",
				"collection", coll.Name(), "error", err)
			continue
		}
		have[idx.Name] = idx.Unique
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("iterate indexes: %w", err)
	}
	return have, nil
}

// missingUniqueIndexes partitions names into absent and present-but-not-unique, in names order.
func missingUniqueIndexes(have map[string]bool, names ...string) (absent, nonUnique []string) {
	for _, n := range names {
		unique, ok := have[n]
		switch {
		case !ok:
			absent = append(absent, n)
		case !unique:
			nonUnique = append(nonUnique, n)
		}
	}
	return absent, nonUnique
}

// ErrIndexSpecConflict reports a same-keys index with a different spec that EnsureIndex may not repair.
var ErrIndexSpecConflict = errors.New("conflicting index exists; its owner service must repair it")

// EnsureIndex creates model without ever dropping anything: a no-op on an identical index, a wrapped
// ErrIndexSpecConflict on a same-keys conflict. Only the owner repairs (two repairers can drop each other's rebuild).
func EnsureIndex(ctx context.Context, coll *mongo.Collection, model mongo.IndexModel) error {
	_, err := coll.Indexes().CreateOne(ctx, model)
	switch {
	case err == nil:
		return nil
	case isIndexSpecConflict(err):
		return fmt.Errorf("ensure index on %s: %w (%v)", coll.Name(), ErrIndexSpecConflict, err)
	default:
		return fmt.Errorf("ensure index on %s: %w", coll.Name(), err)
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
		if _, rerr := coll.Indexes().CreateOne(restoreCtx, old.model(ctx)); rerr != nil {
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
// index a peer already removed is not treated as a failure. NamespaceNotFound (26)
// is deliberately NOT folded in: callers drop only after ensuring an index on the
// same collection, so the collection exists — a 26 there means an external actor
// dropped it, which must surface rather than pass as "nothing to drop".
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
func (e existingIndex) model(ctx context.Context) mongo.IndexModel {
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
	if raw, ok := e.spec["expireAfterSeconds"]; ok {
		if ttl, ok := ttlSeconds(raw); ok {
			opts = opts.SetExpireAfterSeconds(ttl)
		} else {
			slog.WarnContext(ctx, "mongo: captured index TTL is not a usable expireAfterSeconds; restoring the index without it",
				"index", e.name, "expireAfterSeconds", raw)
		}
	}
	if v, ok := e.spec["partialFilterExpression"]; ok {
		opts = opts.SetPartialFilterExpression(v)
	}
	return mongo.IndexModel{Keys: e.keys, Options: opts}
}

// ttlSeconds narrows a captured expireAfterSeconds to the int32 the driver takes.
// Mongo hands it back as int32 or int64, and a live TTL always fits in [0,
// MaxInt32]; anything outside that is reported unusable rather than narrowed,
// because a wrapped value would restore an index expiring documents on a
// schedule the original never had.
func ttlSeconds(raw any) (int32, bool) {
	switch v := raw.(type) {
	case int32:
		return v, v >= 0
	case int64:
		if v < 0 || v > math.MaxInt32 {
			return 0, false
		}
		return int32(v), true
	}
	return 0, false
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
