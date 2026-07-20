package main

import (
	"context"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

// collectStats counts the Graph walk for the run-summary log line.
type collectStats struct {
	Groups     int // groups walked
	Members    int // user-typed members returned by Graph
	SkippedObj int // non-user member objects (nested groups, devices)
	InvalidUPN int // members the mapper couldn't derive an account for
	DupAccount int // accounts already claimed by an earlier group (first wins)
	Overridden int // accounts whose site came from a SITE_OVERRIDES entry
}

// collectEmployees walks every configured group and maps its members to
// Employees via the injected mapper. A member appearing in multiple groups
// keeps its first mapping (config order wins) so the diff sees one row per
// account.
func collectEmployees(ctx context.Context, graph msgraph.GroupReader, mapper transform.Mapper, groups []syncGroup, siteOverrides map[string]string, pageSize int) ([]model.Employee, collectStats, error) {
	var stats collectStats
	var out []model.Employee
	seen := make(map[string]int) // account -> index in out
	for _, sg := range groups {
		profile, err := graph.GetGroup(ctx, sg.GroupID)
		if err != nil {
			return nil, stats, fmt.Errorf("get group %s: %w", sg.GroupID, err)
		}
		org := mapper.OrgFromGroup(*profile)
		skipped, err := graph.ListGroupMembers(ctx, sg.GroupID, pageSize, func(users []msgraph.GraphUser) error {
			stats.Members += len(users)
			for i := range users {
				e := mapper.EmployeeFromMember(&users[i], &org, sg.SiteID)
				if e.Account == "" {
					stats.InvalidUPN++
					continue
				}
				// per-account override wins over the group default
				if site := siteOverrides[e.Account]; site != "" {
					e.SiteID = site
					stats.Overridden++
				}
				if idx, dup := seen[e.Account]; dup {
					stats.DupAccount++
					// First mapping wins for the row; merge the extra group so a
					// member in several groups keeps every membership.
					out[idx].Groups = append(out[idx].Groups, e.Groups...)
					continue
				}
				seen[e.Account] = len(out)
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
