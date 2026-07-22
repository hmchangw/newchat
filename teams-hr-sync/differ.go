package main

import (
	"sort"

	"github.com/hmchangw/chat/pkg/model"
)

// diffResult is one run's delta: rows to upsert (ChangeType new_hire/update)
// and departed accounts grouped by the stored row's siteId.
type diffResult struct {
	Upserts []model.IEmployeeWithChange
	Quits   map[string][]string
}

// diffEmployees diffs the current Graph set against the persisted rows,
// keyed by account. Absent in store → created; present but any field differs
// (incl. Org) → updated; equal → omitted. Store-present-but-Graph-absent →
// quit. Output is sorted by account for deterministic publishes.
func diffEmployees(current, stored []model.IEmployee) diffResult {
	storedByAccount := make(map[string]*model.IEmployee, len(stored))
	for i := range stored {
		storedByAccount[stored[i].Account] = &stored[i]
	}

	res := diffResult{Quits: make(map[string][]string)}
	for i := range current {
		c := &current[i]
		prev, exists := storedByAccount[c.Account]
		delete(storedByAccount, c.Account)
		switch {
		case !exists:
			res.Upserts = append(res.Upserts, model.IEmployeeWithChange{IEmployee: *c, ChangeType: model.IChangeTypeNewHire})
		case *prev != *c:
			res.Upserts = append(res.Upserts, model.IEmployeeWithChange{IEmployee: *c, ChangeType: model.IChangeTypeUpdate})
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
