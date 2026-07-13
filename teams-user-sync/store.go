package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// Store is the persistence surface updateUsers needs. Reads (ExistingIDs,
// HRSiteIDs) are served by the read client; UpsertTeamsUsers by the write
// client.
type Store interface {
	// ExistingIDs returns which of ids already exist in teams_user.
	ExistingIDs(ctx context.Context, ids []string) (map[string]struct{}, error)
	// HRSiteIDs resolves accounts to siteIDs from the hr collection
	// (hr.accountName -> hr.siteID); accounts without a match are absent.
	HRSiteIDs(ctx context.Context, accounts []string) (map[string]string, error)
	// UpsertTeamsUsers bulk-upserts merged records into teams_user, keyed on _id.
	UpsertTeamsUsers(ctx context.Context, users []model.TeamsUser) error
}
