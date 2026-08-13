package mongoutil

import (
	"context"
	"log/slog"
	"time"
)

// EnsureIndexesTimeout bounds a single startup index step. It sits well under
// the driver's 30s server-selection timeout so an unreachable MongoDB does not
// turn each step into a 30s stall — a service with several stores would
// otherwise spend minutes booting during the outage this exists to survive.
const EnsureIndexesTimeout = 10 * time.Second

// EnsureIndexesBestEffort runs a startup index-creation step under
// EnsureIndexesTimeout and downgrades failure from fatal to a WARN, so a
// service still boots when MongoDB is unreachable or degraded to read-only.
// name identifies the step in logs (e.g. "user-service subscriptions").
//
// Index existence is an ops/IaC steady-state concern, in the same family as
// stream bootstrap; createIndexes is a write, so it also fails against a
// read-only primary. Returning nothing is deliberate — callers must not treat
// a missing index as a reason to exit.
//
// There is no background retry: the step is idempotent and re-runs on every
// boot, so a skipped attempt self-heals on the next restart. A missing
// non-unique index costs query performance. A missing *unique* index is the
// real caveat — until a later boot creates it, duplicates it would have
// rejected can be written. Alert on this log line rather than letting it sit.
func EnsureIndexesBestEffort(ctx context.Context, name string, ensure func(context.Context) error) {
	if ensure == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, EnsureIndexesTimeout)
	defer cancel()
	if err := ensure(ctx); err != nil {
		slog.Warn("ensure indexes failed; continuing without them",
			"indexes", name,
			"error", err.Error(),
			"impact", "degraded query performance; unique constraints unenforced until a later start succeeds",
		)
	}
}
