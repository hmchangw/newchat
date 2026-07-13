package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// Syncer runs updateUsers: walk Graph /users page by page, insert the users
// missing from teams_user that have an HR site assignment.
type Syncer struct {
	store    Store
	graph    msgraph.UserLister
	domain   string
	pageSize int
}

// NewSyncer builds a Syncer. emailDomain scopes which UPNs are synced;
// pageSize is Graph's $top.
func NewSyncer(store Store, graph msgraph.UserLister, emailDomain string, pageSize int) *Syncer {
	return &Syncer{store: store, graph: graph, domain: emailDomain, pageSize: pageSize}
}

// RunStats summarizes one UpdateUsers run for the end-of-run log line.
type RunStats struct {
	Pages         int // Graph pages walked
	Seen          int // users returned by Graph
	Existing      int // already present in teams_user, untouched
	DomainSkipped int // UPN outside the configured domain (or malformed)
	HRUnmatched   int // no hr.accountName match; retried next run
	Upserted      int // written to teams_user
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
		account, domain, ok := splitUPN(u.UserPrincipalName)
		if !ok || !strings.EqualFold(domain, s.domain) {
			stats.DomainSkipped++
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
	siteIDs, err := s.store.HRSiteIDs(ctx, accounts)
	if err != nil {
		return fmt.Errorf("resolve hr site ids: %w", err)
	}

	merged := make([]model.TeamsUser, 0, len(candidates))
	for _, c := range candidates {
		siteID, ok := siteIDs[c.Account]
		if !ok {
			stats.HRUnmatched++
			continue
		}
		c.SiteID = siteID
		merged = append(merged, c)
	}
	if len(merged) == 0 {
		return nil
	}
	if err := s.store.UpsertTeamsUsers(ctx, merged); err != nil {
		return fmt.Errorf("upsert teams users: %w", err)
	}
	stats.Upserted += len(merged)
	return nil
}

// splitUPN splits a userPrincipalName into its lowercased local part and its
// domain. ok is false when there is no non-empty local part and domain.
func splitUPN(upn string) (account, domain string, ok bool) {
	at := strings.Index(upn, "@")
	if at <= 0 || at == len(upn)-1 {
		return "", "", false
	}
	return strings.ToLower(upn[:at]), upn[at+1:], true
}
