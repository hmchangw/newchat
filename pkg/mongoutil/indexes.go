package mongoutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"
)

// EnsureIndexesTimeout bounds one EnsureIndexes call. Steps inside it run
// concurrently, so this is the whole startup index budget for a service, not a
// per-step cost — an unreachable MongoDB stalls a boot once, not once per
// store. It stays at 30s rather than something tighter because createIndexes
// blocks until the build finishes, and a first rollout of an index over a
// populated collection is legitimately slow.
const EnsureIndexesTimeout = 30 * time.Second

// IndexStep is one named startup index-creation step. Name identifies it in
// logs (e.g. "user-service subscriptions") so a warning is actionable.
type IndexStep struct {
	Name   string
	Ensure func(context.Context) error
}

// Step builds an IndexStep.
func Step(name string, ensure func(context.Context) error) IndexStep {
	return IndexStep{Name: name, Ensure: ensure}
}

// EnsureIndexes runs startup index steps concurrently under one
// EnsureIndexesTimeout budget, splitting failures by whether a restart can fix
// them.
//
// A transient failure — MongoDB unreachable, or degraded to read-only, since
// createIndexes is a write — is downgraded to a WARN and reported as nil, so a
// service still boots during an outage. The step is idempotent and re-runs on
// every boot, so it self-heals on the next restart; that is also why there is
// no background retry. A missing non-unique index costs query performance, a
// missing unique one lets duplicates through until a later boot creates it, so
// alert on the warning rather than letting it sit.
//
// A permanent failure — duplicate keys, or an index-options conflict — is
// returned. No restart fixes those, and the stores raising them carry operator
// remediation text ("drop the old non-unique account_1 index…"), so callers
// must keep failing fast on them.
func EnsureIndexes(ctx context.Context, steps ...IndexStep) error {
	ctx, cancel := context.WithTimeout(ctx, EnsureIndexesTimeout)
	defer cancel()

	var g errgroup.Group
	for _, step := range steps {
		if step.Ensure == nil {
			continue
		}
		g.Go(func() error {
			err := step.Ensure(ctx)
			if err == nil {
				return nil
			}
			if isPermanentIndexError(err) {
				return fmt.Errorf("ensure indexes %s: %w", step.Name, err)
			}
			slog.Warn("ensure indexes failed; continuing without them",
				"indexes", step.Name,
				"error", err.Error(),
				"impact", "degraded query performance; unique constraints unenforced until a later start succeeds",
			)
			return nil
		})
	}
	return g.Wait()
}

// isPermanentIndexError reports whether a failed createIndexes describes state
// an operator must fix, rather than a MongoDB that is merely unreachable:
// E11000 (existing duplicates block a unique index) and 85/86
// (IndexOptionsConflict / IndexKeySpecsConflict — an incompatible index exists
// and Mongo will not upgrade it in place).
func isPermanentIndexError(err error) bool {
	if mongo.IsDuplicateKeyError(err) {
		return true
	}
	if se := mongo.ServerError(nil); errors.As(err, &se) {
		return se.HasErrorCode(85) || se.HasErrorCode(86)
	}
	return false
}
