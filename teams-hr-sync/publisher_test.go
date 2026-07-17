package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

var zstdTestDecoder, _ = zstd.NewReader(nil)

type captured struct {
	subj     string
	data     []byte
	encoding string
}

// decode decompresses the captured zstd payload into v.
func (c captured) decode(t *testing.T, v any) {
	t.Helper()
	assert.Equal(t, "zstd", c.encoding, "every publish carries Nats-Encoding: zstd")
	raw, err := zstdTestDecoder.DecodeAll(c.data, nil)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, v))
}

func newCapturingPublisher(_ *testing.T, sink *[]captured) *publisher {
	return newPublisher(func(_ context.Context, subj string, data []byte, encoding string) error {
		*sink = append(*sink, captured{subj: subj, data: data, encoding: encoding})
		return nil
	}, "central", transform.DefaultConverter{})
}

func TestPublishSync_AllThreeBatches(t *testing.T) {
	var got []captured
	p := newCapturingPublisher(t, &got)

	d := diffResult{
		Upserts: []model.EmployeeWithChange{{
			Employee:   teamsEmployee("alice", "site-a"),
			ChangeType: model.ChangeTypeNewHire,
		}},
		Quits: map[string][]string{"site-b": {"bob"}, "site-a": {"eve"}},
	}
	n, err := p.publishSync(context.Background(), d)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	require.Len(t, got, 4)

	// employees.upsert — bare array, no wrapper
	assert.Equal(t, "chat.hr.central.employees.upsert", got[0].subj)
	var employees []model.EmployeeWithChange
	got[0].decode(t, &employees)
	require.Len(t, employees, 1)
	assert.Equal(t, "alice", employees[0].Account)
	assert.Equal(t, model.ChangeTypeNewHire, employees[0].ChangeType)
	assert.Equal(t, "g1", employees[0].Org.SectID)

	// users.upsert — bare array
	assert.Equal(t, "chat.hr.central.users.upsert", got[1].subj)
	var users []model.UserWithChange
	got[1].decode(t, &users)
	require.Len(t, users, 1)
	assert.Equal(t, "alice", users[0].Account)
	assert.Equal(t, "site-a", users[0].SiteID)
	assert.Equal(t, model.ChangeTypeNewHire, users[0].ChangeType)

	// quit batches in sorted site order
	assert.Equal(t, "chat.hr.site-a.employees.quit", got[2].subj)
	assert.Equal(t, "chat.hr.site-b.employees.quit", got[3].subj)
	var qb model.HRSyncEmployeeQuitBatch
	got[2].decode(t, &qb)
	assert.Equal(t, "site-a", qb.SiteID)
	assert.Equal(t, []string{"eve"}, qb.Accounts)
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
	p := newPublisher(func(context.Context, string, []byte, string) error { return boom },
		"central", transform.DefaultConverter{})

	_, err := p.publishSync(context.Background(), diffResult{
		Upserts: []model.EmployeeWithChange{{Employee: teamsEmployee("alice", "site-a"), ChangeType: model.ChangeTypeNewHire}},
	})
	require.ErrorIs(t, err, boom)
}
