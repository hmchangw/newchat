package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// hrUser is the HR data resolved for an account from hr_employee.
type hrUser struct {
	SiteID  string
	EngName string
	Mail    string
}

// Store is the persistence surface updateUsers needs. Reads (ExistingIDs,
// HRUsers) are served by the read client; UpsertTeamsUsers by the write
// client.
type Store interface {
	// ExistingIDs returns which of ids already exist in teams_user.
	ExistingIDs(ctx context.Context, ids []string) (map[string]struct{}, error)
	// HRUsers resolves accounts to their HR data from the hr_employee
	// collection (keyed by hr_employee.account); accounts without a match are
	// absent.
	HRUsers(ctx context.Context, accounts []string) (map[string]hrUser, error)
	// UpsertTeamsUsers bulk-upserts merged records into teams_user, keyed on _id.
	UpsertTeamsUsers(ctx context.Context, users []model.TeamsUser) error
}
