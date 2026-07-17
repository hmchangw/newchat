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
}

// collectEmployees walks every configured group and maps its members to
// Employees via the injected mapper. A member appearing in multiple groups
// keeps its first mapping (config order wins) so the diff sees one row per
// account.
func collectEmployees(ctx context.Context, graph msgraph.GroupReader, mapper transform.Mapper, groups []syncGroup, pageSize int) ([]model.Employee, collectStats, error) {
	var stats collectStats
	var out []model.Employee
	seen := make(map[string]struct{})
	for _, sg := range groups {
		profile, err := graph.GetGroup(ctx, sg.GroupID)
		if err != nil {
			return nil, stats, fmt.Errorf("get group %s: %w", sg.GroupID, err)
		}
		org := mapper.OrgFromGroup(*profile)
		skipped, err := graph.ListGroupMembers(ctx, sg.GroupID, pageSize, func(users []msgraph.GraphUser) error {
			stats.Members += len(users)
			for _, u := range users {
				e := mapper.EmployeeFromMember(u, org, sg.SiteID)
				if e.Account == "" {
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
