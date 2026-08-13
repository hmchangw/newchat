package mongoutil

import (
	"context"
	"log/slog"
)

// EnsureIndexesBestEffort runs a startup index-creation step and downgrades
// failure from fatal to a WARN log, so a service can still boot when MongoDB
// is unreachable or degraded to read-only. It deliberately returns nothing:
// the whole point is that callers must not treat this as a reason to exit.
//
// Rationale: index existence is an ops/IaC steady-state concern, in the same
// family as stream bootstrap (which services already must not do in
// production). createIndexes is a write, so it fails against a read-only
// primary as well as a down one — refusing to boot on that is strictly worse
// than booting without it. The step is idempotent and runs on every start, so
// a boot that skips it self-heals on the next restart.
//
// There is no background retry, on purpose. The step already retries on every
// boot; a retry loop would add a goroutine (and a termination path) per
// service for a condition that a restart fixes anyway. A missing non-unique
// index costs query performance, not correctness. A missing *unique* index is
// the real caveat — between MongoDB becoming writable again and the next
// restart, duplicate documents could be written that the index would have
// rejected. Alert on this log line rather than letting it sit.
//
// name identifies the step in logs (e.g. "user-service subscriptions") so the
// warning is actionable and alertable.
func EnsureIndexesBestEffort(ctx context.Context, name string, ensure func(context.Context) error) {
	if ensure == nil {
		return
	}
	if err := ensure(ctx); err != nil {
		slog.Warn("ensure indexes failed; continuing without them",
			"indexes", name,
			"error", err.Error(),
			"impact", "degraded query performance; unique constraints unenforced until a later start succeeds",
		)
	}
}
