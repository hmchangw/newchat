// Package hrstore is the write surface an HR feed consumer persists into —
// shared by hr-sync-worker (stream mode) and teams-hr-sync's direct-write
// migration mode. All writes are idempotent — the feed is at-least-once.
package hrstore

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store.go -package=hrstore

// Store is the write surface an HR feed persists into.
type Store interface {
	// UpsertEmployees replaces hr_employee docs keyed by account.
	UpsertEmployees(ctx context.Context, employees []model.EmployeeWithChange) error
	// UpsertUserIdentities upserts users by account, writing IDENTITY FIELDS
	// ONLY (account, siteId, engName, chineseName, employeeId). It must never
	// touch roles/services/password/deactivated/status fields — users is the
	// live auth store.
	UpsertUserIdentities(ctx context.Context, users []model.UserWithChange) error
	// QuitTeamsEmployees deletes hr_employee rows for the given accounts.
	// users stays untouched (the user lifecycle is not the HR feed's to end).
	QuitTeamsEmployees(ctx context.Context, accounts []string) error
}
