package main

import (
	"context"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

// emitResult breaks the run's written counts out by kind for the summary log.
// Employees/Users are record counts (they move 1:1 — users derive from the same
// upserts); Quits is the per-site batch count.
type emitResult struct {
	EmployeesWritten int
	UsersWritten     int
	QuitsWritten     int
}

// emitter is the seam between a computed diff and where it lands: the
// JetStream feed (stream mode, default) or a direct write into the target
// Mongo (direct mode, one-shot migration/backfill). The counts are
// informational for the run-summary log line.
type emitter interface {
	emit(ctx context.Context, diff diffResult) (emitResult, error)
}

// streamEmitter wraps the existing publisher — stream-mode behavior is
// unchanged.
type streamEmitter struct{ pub *publisher }

func (e streamEmitter) emit(ctx context.Context, diff diffResult) (emitResult, error) {
	return e.pub.publishSync(ctx, diff)
}

// directEmitter writes a diff straight to the target Mongo via this service's
// own WriteStore, skipping the JetStream feed + hr-sync-worker entirely.
type directEmitter struct {
	store     WriteStore
	converter transform.EmployeeUserConverter
}

func (e directEmitter) emit(ctx context.Context, diff diffResult) (emitResult, error) {
	var res emitResult
	if len(diff.Upserts) > 0 {
		if err := e.store.UpsertEmployees(ctx, diff.Upserts); err != nil {
			return res, fmt.Errorf("direct upsert employees: %w", err)
		}
		res.EmployeesWritten = len(diff.Upserts)

		users := make([]model.IUserWithChange, 0, len(diff.Upserts))
		for i := range diff.Upserts {
			users = append(users, model.IUserWithChange{
				User:       e.converter.UserFromEmployee(&diff.Upserts[i].IEmployee),
				ChangeType: diff.Upserts[i].ChangeType,
			})
		}
		if err := e.store.UpsertUserIdentities(ctx, users); err != nil {
			return res, fmt.Errorf("direct upsert user identities: %w", err)
		}
		res.UsersWritten = len(users)
	}

	// Symmetry with stream mode. A diff-vs-empty-baseline run (direct mode's
	// normal path) never produces quits, but honor them if ever passed one.
	for _, accounts := range diff.Quits {
		if len(accounts) == 0 {
			continue
		}
		if err := e.store.QuitTeamsEmployees(ctx, accounts); err != nil {
			return res, fmt.Errorf("direct quit employees: %w", err)
		}
		res.QuitsWritten++
	}
	return res, nil
}
