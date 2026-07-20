package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// hrUser is the raw HR data resolved for an account; siteID derivation from
// LocationURL happens in the handler.
type hrUser struct {
	LocationURL string
	EngName     string
	Mail        string
}

// Store is the persistence surface updateUsers needs. Reads (ExistingIDs,
// HRUsers) are served by the read client; UpsertTeamsUsers by the write
// client.
type Store interface {
	// ExistingIDs returns which of ids already exist in teams_user.
	ExistingIDs(ctx context.Context, ids []string) (map[string]struct{}, error)
	// HRUsers resolves accounts to their HR data from the hr collection
	// (keyed by hr.accountName); accounts without a match are absent.
	HRUsers(ctx context.Context, accounts []string) (map[string]hrUser, error)
	// UpsertTeamsUsers bulk-upserts merged records into teams_user, keyed on _id.
	UpsertTeamsUsers(ctx context.Context, users []model.TeamsUser) error
}
