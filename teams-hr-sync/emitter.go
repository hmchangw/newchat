package main

import (
	"context"
	"fmt"

	"github.com/hmchangw/chat/pkg/hrstore"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

// emitter is the seam between a computed diff and where it lands: the
// JetStream feed (stream mode, default) or a direct write into the target
// Mongo (direct mode, one-shot migration/backfill). The return count is
// informational (batches sent/written) for the run-summary log line.
type emitter interface {
	emit(ctx context.Context, diff diffResult) (int, error)
}

// streamEmitter wraps the existing publisher — stream-mode behavior is
// unchanged.
type streamEmitter struct{ pub *publisher }

func (e streamEmitter) emit(ctx context.Context, diff diffResult) (int, error) {
	return e.pub.publishSync(ctx, diff)
}

// directEmitter writes a diff straight to the target Mongo via the shared
// hrstore.Store, skipping the JetStream feed + hr-sync-worker entirely.
type directEmitter struct {
	store     hrstore.Store
	converter transform.EmployeeUserConverter
}

func (e directEmitter) emit(ctx context.Context, diff diffResult) (int, error) {
	written := 0
	if len(diff.Upserts) > 0 {
		if err := e.store.UpsertEmployees(ctx, diff.Upserts); err != nil {
			return written, fmt.Errorf("direct upsert employees: %w", err)
		}
		written++

		users := make([]model.UserWithChange, 0, len(diff.Upserts))
		for i := range diff.Upserts {
			users = append(users, model.UserWithChange{
				User:       e.converter.UserFromEmployee(&diff.Upserts[i].Employee),
				ChangeType: diff.Upserts[i].ChangeType,
			})
		}
		if err := e.store.UpsertUserIdentities(ctx, users); err != nil {
			return written, fmt.Errorf("direct upsert user identities: %w", err)
		}
		written++
	}

	// Symmetry with stream mode. A diff-vs-empty-baseline run (direct mode's
	// normal path) never produces quits, but honor them if ever passed one.
	for _, accounts := range diff.Quits {
		if len(accounts) == 0 {
			continue
		}
		if err := e.store.QuitTeamsEmployees(ctx, accounts); err != nil {
			return written, fmt.Errorf("direct quit employees: %w", err)
		}
		written++
	}
	return written, nil
}
