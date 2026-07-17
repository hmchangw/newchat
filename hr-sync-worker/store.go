package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// Store is the write surface the HR feed persists into. All writes are
// idempotent — the feed is at-least-once.
type Store interface {
	// UpsertEmployees replaces hr_employee docs keyed by account.
	UpsertEmployees(ctx context.Context, employees []model.EmployeeWithChange) error
	// UpsertUserIdentities upserts users by account, writing IDENTITY FIELDS
	// ONLY (account, siteId, engName, chineseName, employeeId). It must never
	// touch roles/services/password/deactivated/status fields — users is the
	// live auth store.
	UpsertUserIdentities(ctx context.Context, users []model.UserWithChange) error
	// QuitTeamsEmployees deletes hr_employee rows for the accounts, scoped to
	// source "teams" — never another feed's rows. users stays untouched.
	QuitTeamsEmployees(ctx context.Context, accounts []string) error
}
