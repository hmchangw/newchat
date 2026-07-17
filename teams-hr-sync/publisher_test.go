package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

type captured struct {
	subj string
	data []byte
}

func newCapturingPublisher(t *testing.T, sink *[]captured) *publisher {
	t.Helper()
	p := newPublisher(func(_ context.Context, subj string, data []byte) error {
		*sink = append(*sink, captured{subj: subj, data: data})
		return nil
	}, "central", transform.DefaultConverter{})
	p.now = func() time.Time { return time.UnixMilli(1735689600001).UTC() }
	return p
}

func TestPublishSync_AllThreeBatches(t *testing.T) {
	var got []captured
	p := newCapturingPublisher(t, &got)

	d := diffResult{
		Upserts: []model.EmployeeWithChange{{
			Employee: teamsEmployee("alice", "site-a"),
			Change:   "created",
		}},
		Quits: map[string][]string{
			"site-b": {"bob"},
			"site-a": {"eve"},
		},
	}
	n, err := p.publishSync(context.Background(), d)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	require.Len(t, got, 4)

	assert.Equal(t, "chat.hr.central.employees.upsert", got[0].subj)
	var eb model.EmployeesUpsertBatch
	require.NoError(t, json.Unmarshal(got[0].data, &eb))
	assert.Equal(t, int64(1735689600001), eb.Timestamp)
	require.Len(t, eb.Employees, 1)
	assert.Equal(t, "alice", eb.Employees[0].Account)
	assert.Equal(t, "created", eb.Employees[0].Change)
	assert.Equal(t, "g1", eb.Employees[0].Org.ID)

	assert.Equal(t, "chat.hr.central.users.upsert", got[1].subj)
	var ub model.UsersUpsertBatch
	require.NoError(t, json.Unmarshal(got[1].data, &ub))
	require.Len(t, ub.Users, 1)
	assert.Equal(t, "alice", ub.Users[0].Account)
	assert.Equal(t, "site-a", ub.Users[0].SiteID)
	assert.Equal(t, "created", ub.Users[0].Change)

	// quit batches in sorted site order
	assert.Equal(t, "chat.hr.site-a.employees.quit", got[2].subj)
	assert.Equal(t, "chat.hr.site-b.employees.quit", got[3].subj)
	var qb model.HRSyncEmployeeQuitBatch
	require.NoError(t, json.Unmarshal(got[2].data, &qb))
	assert.Equal(t, model.HRSyncEmployeeQuitBatch{
		Timestamp: 1735689600001, SiteID: "site-a", Accounts: []string{"eve"},
	}, qb)
}

func TestPublishSync_SkipsEmptyBatches(t *testing.T) {
	var got []captured
	p := newCapturingPublisher(t, &got)

	n, err := p.publishSync(context.Background(), diffResult{})
	require.NoError(t, err)
	assert.Zero(t, n)
	assert.Empty(t, got, "nothing to publish on an empty diff")
}

func TestPublishSync_QuitsOnlySkipsUpserts(t *testing.T) {
	var got []captured
	p := newCapturingPublisher(t, &got)

	n, err := p.publishSync(context.Background(), diffResult{Quits: map[string][]string{"site-a": {"eve"}}})
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.Len(t, got, 1)
	assert.Equal(t, "chat.hr.site-a.employees.quit", got[0].subj)
}

func TestPublishSync_PublishErrorAborts(t *testing.T) {
	boom := errors.New("nats down")
	p := newPublisher(func(context.Context, string, []byte) error { return boom },
		"central", transform.DefaultConverter{})

	_, err := p.publishSync(context.Background(), diffResult{
		Upserts: []model.EmployeeWithChange{{Employee: teamsEmployee("alice", "site-a"), Change: "created"}},
	})
	require.ErrorIs(t, err, boom)
}
