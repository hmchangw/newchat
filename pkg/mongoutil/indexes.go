package mongoutil

import (
	"context"
	"log/slog"

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
