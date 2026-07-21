package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

const encodingZstd = "zstd"

// zstdEncoder is a process-wide encoder reused across every publish; klauspost's
// EncodeAll is safe for concurrent use.
var zstdEncoder = mustNewZstdEncoder()

func mustNewZstdEncoder() *zstd.Encoder {
	e, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		panic(fmt.Sprintf("init zstd encoder: %v", err))
	}
	return e
}

// publishFunc publishes one JetStream message with a Nats-Encoding value;
// injected so unit tests capture payloads without a NATS connection.
type publishFunc func(ctx context.Context, subj string, data []byte, encoding string) error

// publisher emits one run's diff as up to three message kinds: employees.upsert
// + users.upsert on the central site (bare zstd arrays), employees.quit per
// site. Empty batches are skipped.
type publisher struct {
	publish   publishFunc
	central   string
	converter transform.EmployeeUserConverter
}

func newPublisher(publish publishFunc, central string, converter transform.EmployeeUserConverter) *publisher {
	return &publisher{publish: publish, central: central, converter: converter}
}

// publishSync publishes the diff and returns the number of messages sent.
func (p *publisher) publishSync(ctx context.Context, d diffResult) (int, error) {
	published := 0

	if len(d.Upserts) > 0 {
		if err := p.publishZstd(ctx, subject.OrgSyncEmployeesUpsert(p.central), d.Upserts); err != nil {
			return published, fmt.Errorf("publish employees.upsert: %w", err)
		}
		published++

		users := make([]model.UserWithChange, 0, len(d.Upserts))
		for i := range d.Upserts {
			users = append(users, model.UserWithChange{
				User:       p.converter.UserFromEmployee(&d.Upserts[i].Employee),
				ChangeType: d.Upserts[i].ChangeType,
			})
		}
		if err := p.publishZstd(ctx, subject.OrgSyncUsersUpsert(p.central), users); err != nil {
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
		if err := p.publishZstd(ctx, subject.EmployeesQuit(siteID),
			model.HRSyncEmployeeQuitBatch{Timestamp: time.Now().UTC().UnixMilli(), SiteID: siteID, Accounts: d.Quits[siteID]}); err != nil {
			return published, fmt.Errorf("publish employees.quit for site %s: %w", siteID, err)
		}
		published++
	}
	return published, nil
}

func (p *publisher) publishZstd(ctx context.Context, subj string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	return p.publish(ctx, subj, zstdEncoder.EncodeAll(data, nil), encodingZstd)
}
