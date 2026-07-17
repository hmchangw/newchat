package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// mapEmployee maps one Graph group member + its group profile to an Employee.
// ok is false when the UPN has no local part (never syncable). Account is the
// lowercased UPN local part — the same account rule as teams-user-sync.
func mapEmployee(u msgraph.GraphUser, group *msgraph.GroupProfile, siteID, orgType string) (model.Employee, bool) {
	account, ok := splitUPN(u.UserPrincipalName)
	if !ok {
		return model.Employee{}, false
	}
	return model.Employee{
		EmployeeID:  u.EmployeeID,
		Account:     account,
		EngName:     strings.TrimSpace(u.GivenName + " " + u.Surname),
		ChineseName: u.DisplayName,
		SiteID:      siteID,
		Source:      sourceTeams,
		Org: model.Org{
			ID:          group.ID,
			Name:        group.DisplayName,
			Description: group.Description,
			Type:        orgType,
		},
	}, true
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

// collectStats counts the Graph walk for the run-summary log line.
type collectStats struct {
	Groups     int // groups walked
	Members    int // user-typed members returned by Graph
	SkippedObj int // non-user member objects (nested groups, devices)
	InvalidUPN int // members without a usable UPN local part
	DupAccount int // accounts already claimed by an earlier group (first wins)
}

// collectEmployees walks every configured group and maps its members to
// Employees. A member appearing in multiple groups keeps its first mapping
// (config order wins) so the diff sees one row per account.
func collectEmployees(ctx context.Context, graph msgraph.GroupReader, groups []syncGroup, orgType string, pageSize int) ([]model.Employee, collectStats, error) {
	var stats collectStats
	var out []model.Employee
	seen := make(map[string]struct{})
	for _, sg := range groups {
		profile, err := graph.GetGroup(ctx, sg.GroupID)
		if err != nil {
			return nil, stats, fmt.Errorf("get group %s: %w", sg.GroupID, err)
		}
		skipped, err := graph.ListGroupMembers(ctx, sg.GroupID, pageSize, func(users []msgraph.GraphUser) error {
			stats.Members += len(users)
			for _, u := range users {
				e, ok := mapEmployee(u, profile, sg.SiteID, orgType)
				if !ok {
					stats.InvalidUPN++
					continue
				}
				if _, dup := seen[e.Account]; dup {
					stats.DupAccount++
					continue
				}
				seen[e.Account] = struct{}{}
				out = append(out, e)
			}
			return nil
		})
		stats.SkippedObj += skipped
		if err != nil {
			return nil, stats, fmt.Errorf("list members of group %s: %w", sg.GroupID, err)
		}
		stats.Groups++
	}
	return out, stats, nil
}
