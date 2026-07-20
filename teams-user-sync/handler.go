package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// Syncer runs updateUsers: walk Graph /users page by page, insert the users
// missing from teams_user, joined with their HR data (siteID derived from the
// HR locationURL) when an hr row exists.
type Syncer struct {
	store    Store
	graph    msgraph.UserLister
	pageSize int
	logger   *slog.Logger
}

// NewSyncer builds a Syncer. pageSize is Graph's $top.
func NewSyncer(store Store, graph msgraph.UserLister, pageSize int, logger *slog.Logger) *Syncer {
	return &Syncer{store: store, graph: graph, pageSize: pageSize, logger: logger}
}

// RunStats summarizes one UpdateUsers run for the end-of-run log line.
type RunStats struct {
	Pages       int // Graph pages walked
	Seen        int // users returned by Graph
	Existing    int // already present in teams_user, untouched
	InvalidUPN  int // UPN without a local part and domain; never syncable
	HRUnmatched int // no hr.accountName match; upserted with empty HR fields
	Upserted    int // written to teams_user
}

// UpdateUsers performs one full sync run. Any Graph or store error aborts the
// run; the next scheduled run retries from scratch (writes are idempotent
// upserts, so partial progress is kept).
func (s *Syncer) UpdateUsers(ctx context.Context) (RunStats, error) {
	var stats RunStats
	if err := s.graph.ListUsers(ctx, s.pageSize, func(users []msgraph.GraphUser) error {
		return s.syncPage(ctx, users, &stats)
	}); err != nil {
		return stats, fmt.Errorf("walk graph users: %w", err)
	}
	return stats, nil
}

func (s *Syncer) syncPage(ctx context.Context, users []msgraph.GraphUser, stats *RunStats) error {
	stats.Pages++
	stats.Seen += len(users)
	if len(users) == 0 {
		return nil
	}

	ids := make([]string, 0, len(users))
	for _, u := range users {
		ids = append(ids, u.ID)
	}
	existing, err := s.store.ExistingIDs(ctx, ids)
	if err != nil {
		return fmt.Errorf("diff teams_user ids: %w", err)
	}
	stats.Existing += len(existing)

	candidates := make([]model.TeamsUser, 0, len(users)-len(existing))
	for _, u := range users {
		if _, ok := existing[u.ID]; ok {
			continue
		}
		account, ok := splitUPN(u.UserPrincipalName)
		if !ok {
			stats.InvalidUPN++
			continue
		}
		candidates = append(candidates, model.TeamsUser{ID: u.ID, UPN: u.UserPrincipalName, Account: account})
	}
	if len(candidates) == 0 {
		return nil
	}

	accounts := make([]string, 0, len(candidates))
	for _, c := range candidates {
		accounts = append(accounts, c.Account)
	}
	hrUsers, err := s.store.HRUsers(ctx, accounts)
	if err != nil {
		return fmt.Errorf("resolve hr users: %w", err)
	}
	matched := 0
	for _, c := range candidates {
		if _, ok := hrUsers[c.Account]; ok {
			matched++
		}
	}
	s.logger.Info("hr site ids lookup result",
		"requested", len(accounts), "matched", matched, "unmatched", len(accounts)-matched)

	merged := make([]model.TeamsUser, 0, len(candidates))
	for _, c := range candidates {
		hr, ok := hrUsers[c.Account]
		if !ok {
			stats.HRUnmatched++
			s.logger.Info("hr id not found", "account", c.Account, "userId", c.ID)
			merged = append(merged, c)
			continue
		}
		c.EngName = hr.EngName
		c.Mail = hr.Mail
		if hr.LocationURL == "" {
			s.logger.Warn("hr locationURL is empty", "account", c.Account)
		} else {
			c.SiteID = extractSiteIDFromLocationURL(hr.LocationURL)
			if c.SiteID == "" {
				s.logger.Warn("extract siteID from locationURL returned empty",
					"account", c.Account, "locationURL", hr.LocationURL)
			}
		}
		merged = append(merged, c)
	}
	if err := s.store.UpsertTeamsUsers(ctx, merged); err != nil {
		return fmt.Errorf("upsert teams users: %w", err)
	}
	stats.Upserted += len(merged)
	return nil
}

// splitUPN extracts a userPrincipalName's lowercased local part (the account).
// ok is false when there is no non-empty local part (no '@', or '@' first).
func splitUPN(upn string) (account string, ok bool) {
	at := strings.Index(upn, "@")
	if at <= 0 {
		return "", false
	}
	return strings.ToLower(upn[:at]), true
}

// extractSiteIDFromLocationURL returns the substring after "://" and before
// ".mysite" (pattern https://{siteID}.mysite.com); "" when either marker is
// absent.
func extractSiteIDFromLocationURL(locationURL string) string {
	_, rest, ok := strings.Cut(locationURL, "://")
	if !ok {
		return ""
	}
	siteID, _, ok := strings.Cut(rest, ".mysite")
	if !ok {
		return ""
	}
	return siteID
}
