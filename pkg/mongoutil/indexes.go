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

// EnsureIndexesTimeout is how long one EnsureIndexes call gets in total. Its
// steps run at the same time as each other, so this is the whole index budget
// for a service's startup, not a cost per store: with MongoDB unreachable a
// service waits this long once, however many stores it has.
//
// It is 30s rather than something shorter because asking MongoDB to create an
// index does not return until the index is built, and building one over a
// collection that already holds a lot of data genuinely takes a while.
const EnsureIndexesTimeout = 30 * time.Second

// IndexStep is one index-creation step to run at startup. The name is what
// shows up in the log if it fails, so make it say which service and which
// collection — for example "user-service subscriptions".
type IndexStep struct {
	Name   string
	Ensure func(context.Context) error
}

// Step builds an IndexStep.
func Step(name string, ensure func(context.Context) error) IndexStep {
	return IndexStep{Name: name, Ensure: ensure}
}

// EnsureIndexes creates a service's indexes at startup. It sorts failures into
// two kinds: ones a restart will clear, and ones only a person can.
//
// If MongoDB is unreachable, or is up but refusing writes, that is the first
// kind. Creating an index is a write, so both look the same here. EnsureIndexes
// logs a warning and reports success, so the service still starts. Nothing
// retries in the background: the whole step runs again on the next start
// anyway, and asking twice for an index that already exists costs nothing, so a
// start that skipped it fixes itself later.
//
// That leaves a gap worth alerting on. Until some later start succeeds, a
// missing ordinary index only makes queries slower, but a missing *unique*
// index means nothing is stopping duplicate rows being written.
//
// The second kind — MongoDB rejecting the index because of data or an index
// that is already there — is returned to the caller, which should exit. No
// number of restarts will change MongoDB's mind, and the stores that produce
// these errors attach instructions for whoever has to fix it (for example,
// "drop the old non-unique account_1 index"). Starting up anyway would bury
// those instructions in a warning nobody reads.
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

// MongoDB's two ways of saying there is nothing to drop: the collection was
// never created, or the index is already gone.
const (
	codeNamespaceNotFound = 26
	codeIndexNotFound     = 27
)

// DropIndexIfExists removes an old index we no longer want. "Not there" counts
// as done, because that is the normal answer on a brand new deployment and on
// every start after the first one that removed it.
//
// Anything else is reported. If the drop never reached MongoDB, or MongoDB
// turned it down, the old index is still in place — and treating that as
// success would let startup believe an old unique index is gone while it is
// still rejecting writes the new index would have accepted.
func DropIndexIfExists(ctx context.Context, col *mongo.Collection, name string) error {
	if err := col.Indexes().DropOne(ctx, name); err != nil && !isIndexAbsentError(err) {
		return fmt.Errorf("drop legacy index %q: %w", name, err)
	}
	return nil
}

// isIndexAbsentError tells "there was nothing to drop" apart from "the drop did
// not happen".
func isIndexAbsentError(err error) bool {
	if se := mongo.ServerError(nil); errors.As(err, &se) {
		return se.HasErrorCode(codeNamespaceNotFound) || se.HasErrorCode(codeIndexNotFound)
	}
	return false
}

// isPermanentIndexError reports whether MongoDB refused to create an index for
// a reason someone has to go and fix, rather than just being unreachable:
//
//   - E11000: the collection already holds rows that a unique index would
//     reject, so the duplicates have to be sorted out first.
//   - 85 and 86: an index on those fields already exists with different
//     settings. MongoDB will not change one in place, so the old one has to be
//     dropped by hand.
func isPermanentIndexError(err error) bool {
	if mongo.IsDuplicateKeyError(err) {
		return true
	}
	if se := mongo.ServerError(nil); errors.As(err, &se) {
		return se.HasErrorCode(85) || se.HasErrorCode(86)
	}
	return false
}
