package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

// publishFunc publishes one JetStream message; injected so unit tests capture
// payloads without a NATS connection.
type publishFunc func(ctx context.Context, subj string, data []byte) error

// publisher emits one run's diff as up to three batch kinds: employees.upsert
// + users.upsert on the central site, employees.quit per site. Empty batches
// are skipped. Payloads are plain JSON (the subject contract permits
// uncompressed).
type publisher struct {
	publish   publishFunc
	central   string
	converter transform.EmployeeUserConverter
	now       func() time.Time
}

func newPublisher(publish publishFunc, central string, converter transform.EmployeeUserConverter) *publisher {
	return &publisher{publish: publish, central: central, converter: converter, now: time.Now}
}

// publishSync publishes the diff and returns the number of messages sent.
func (p *publisher) publishSync(ctx context.Context, d diffResult) (int, error) {
	published := 0
	ts := p.now().UTC().UnixMilli()

	if len(d.Upserts) > 0 {
		if err := p.publishJSON(ctx, subject.OrgSyncEmployeesUpsert(p.central),
			model.EmployeesUpsertBatch{Timestamp: ts, Employees: d.Upserts}); err != nil {
			return published, fmt.Errorf("publish employees.upsert: %w", err)
		}
		published++

		users := make([]model.UserWithChange, 0, len(d.Upserts))
		for i := range d.Upserts {
			users = append(users, model.UserWithChange{User: p.converter.UserFromEmployee(&d.Upserts[i].Employee), Change: d.Upserts[i].Change})
		}
		if err := p.publishJSON(ctx, subject.OrgSyncUsersUpsert(p.central),
			model.UsersUpsertBatch{Timestamp: ts, Users: users}); err != nil {
			return published, fmt.Errorf("publish users.upsert: %w", err)
		}
		published++
	}

	// deterministic site order so a partial failure is reproducible
	siteIDs := make([]string, 0, len(d.Quits))
	for siteID := range d.Quits {
		siteIDs = append(siteIDs, siteID)
	}
	sort.Strings(siteIDs)
	for _, siteID := range siteIDs {
		if err := p.publishJSON(ctx, subject.EmployeesQuit(siteID),
			model.HRSyncEmployeeQuitBatch{Timestamp: ts, SiteID: siteID, Accounts: d.Quits[siteID]}); err != nil {
			return published, fmt.Errorf("publish employees.quit for site %s: %w", siteID, err)
		}
		published++
	}
	return published, nil
}

func (p *publisher) publishJSON(ctx context.Context, subj string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	return p.publish(ctx, subj, data)
}
