package main

import (
	"sort"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

// diffResult is one run's delta: rows to upsert (ChangeType new_hire/update)
// and departed accounts grouped by the stored row's siteId.
type diffResult struct {
	Upserts []model.EmployeeWithChange
	Quits   map[string][]string
}

// diffEmployees diffs the current Graph set against the persisted rows,
// keyed by account. Absent in store → created; present but any field differs
// (incl. Org) → updated; equal → omitted. Store-present-but-Graph-absent →
// quit. Stored rows from another source are ignored defensively (the store
// query already filters, but a false quit is destructive downstream).
// Output is sorted by account for deterministic publishes.
func diffEmployees(current, stored []model.Employee) diffResult {
	storedByAccount := make(map[string]*model.Employee, len(stored))
	for i := range stored {
		if stored[i].Source != transform.SourceTeams {
			continue
		}
		storedByAccount[stored[i].Account] = &stored[i]
	}

	res := diffResult{Quits: make(map[string][]string)}
	for i := range current {
		c := &current[i]
		prev, exists := storedByAccount[c.Account]
		delete(storedByAccount, c.Account)
		switch {
		case !exists:
			res.Upserts = append(res.Upserts, model.EmployeeWithChange{Employee: *c, ChangeType: model.ChangeTypeNewHire})
		case *prev != *c:
			res.Upserts = append(res.Upserts, model.EmployeeWithChange{Employee: *c, ChangeType: model.ChangeTypeUpdate})
		}
	}
	for _, s := range storedByAccount {
		res.Quits[s.SiteID] = append(res.Quits[s.SiteID], s.Account)
	}

	sort.Slice(res.Upserts, func(i, j int) bool { return res.Upserts[i].Account < res.Upserts[j].Account })
	for _, accounts := range res.Quits {
		sort.Strings(accounts)
	}
	return res
}
