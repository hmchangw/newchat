//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

// fakeGraph is a mutable single-group Graph tenant served over httptest.
type fakeGraph struct {
	mu      sync.Mutex
	members []map[string]any
}

func (f *fakeGraph) setMembers(members []map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.members = members
}

func newFakeGraphServer(t *testing.T, f *fakeGraph) (tokenURL, baseURL string) {
	t.Helper()
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "tok", "expires_in": 3600})
	}))
	t.Cleanup(tokenSrv.Close)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/groups/g1":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "g1", "displayName": "Engineering", "description": "eng dept"})
		case "/groups/g1/members":
			_ = json.NewEncoder(w).Encode(map[string]any{"value": f.members})
		default:
			t.Errorf("unexpected graph path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(graphSrv.Close)
	return tokenSrv.URL, graphSrv.URL
}

func graphUser(id, upn, displayName, employeeID string) map[string]any {
	return map[string]any{
		"@odata.type": "#microsoft.graph.user", "id": id, "userPrincipalName": upn,
		"displayName": displayName, "employeeId": employeeID,
	}
}

// TestRunSync_EndToEnd drives two full runs against a real Mongo + JetStream
// and a fake Graph: first run publishes the full set, second run publishes
// only the delta + quit; a legacy-source row is never quit.
func TestRunSync_EndToEnd(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "teams_hr_sync_e2e")
	natsURL := testutil.NATS(t)

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "HR_SYNC_E2E", Subjects: []string{"chat.hr.>"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), "HR_SYNC_E2E") })

	fg := &fakeGraph{}
	tokenURL, baseURL := newFakeGraphServer(t, fg)
	graph := msgraph.NewGroupReaderClient(
		msgraph.Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		msgraph.WithBaseURL(baseURL), msgraph.WithTokenURL(tokenURL),
	)
	store := newMongoStore(db)
	pub := newPublisher(jetStreamPublish(js), "central", transform.DefaultConverter{})
	groups := []syncGroup{{GroupID: "g1", SiteID: "site-a"}}

	// --- first run: empty store -> everything created
	fg.setMembers([]map[string]any{
		graphUser("u1", "alice@corp.com", "愛麗絲", "EMP1"),
		graphUser("u2", "bob@corp.com", "鮑伯", "EMP2"),
	})
	stats, err := runSync(ctx, graph, transform.DefaultMapper{}, store, pub, groups, nil, 100)
	require.NoError(t, err)
	assert.Equal(t, 2, stats.Created)
	assert.Zero(t, stats.Quits)
	assert.Equal(t, 2, stats.EmployeesPublished) // 2 employee records
	assert.Equal(t, 2, stats.UsersPublished)     // 2 user records
	assert.Zero(t, stats.QuitsPublished)

	msgs := drainStream(t, stream)
	require.Len(t, msgs, 2)
	var employees []model.IEmployeeWithChange
	require.NoError(t, json.Unmarshal(msgs["chat.hr.central.employees.upsert"], &employees))
	require.Len(t, employees, 2)
	assert.Equal(t, "alice", employees[0].Account)
	assert.Equal(t, model.IChangeTypeNewHire, employees[0].ChangeType)
	assert.Equal(t, model.IOrg{SectID: "g1", SectName: "Engineering", SectDescription: "eng dept"}, employees[0].IOrg)
	var users []model.IUserWithChange
	require.NoError(t, json.Unmarshal(msgs["chat.hr.central.users.upsert"], &users))
	require.Len(t, users, 2)
	assert.Equal(t, "alice", users[0].Account)

	// persist what the downstream consumer would have written, so run 2 diffs
	// against ground truth. The consumer keys _id on employeeId (the wire
	// strips Employee.ID), so stamp it here too.
	docs := make([]any, 0, len(employees))
	for _, e := range employees {
		row := e.IEmployee
		row.ID = row.EmployeeID
		docs = append(docs, row)
	}
	_, err = db.Collection(hrEmployeeCollection).InsertMany(ctx, docs)
	require.NoError(t, err)

	// --- second run: bob renamed (updated), alice gone (quit), carol new (created)
	fg.setMembers([]map[string]any{
		graphUser("u2", "bob@corp.com", "鮑伯二世", "EMP2"),
		graphUser("u3", "carol@corp.com", "卡蘿", "EMP3"),
	})
	stats, err = runSync(ctx, graph, transform.DefaultMapper{}, store, pub, groups, nil, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Created)
	assert.Equal(t, 1, stats.Updated)
	assert.Equal(t, 1, stats.Quits)
	assert.Equal(t, 2, stats.EmployeesPublished) // 1 created + 1 updated
	assert.Equal(t, 2, stats.UsersPublished)
	assert.Equal(t, 1, stats.QuitsPublished)

	msgs = drainStream(t, stream)
	require.Len(t, msgs, 3)
	require.NoError(t, json.Unmarshal(msgs["chat.hr.central.employees.upsert"], &employees))
	require.Len(t, employees, 2)
	assert.Equal(t, "bob", employees[0].Account)
	assert.Equal(t, model.IChangeTypeUpdate, employees[0].ChangeType)
	assert.Equal(t, "鮑伯二世", employees[0].ChineseName)
	assert.Equal(t, "carol", employees[1].Account)
	assert.Equal(t, model.IChangeTypeNewHire, employees[1].ChangeType)
	var qb model.IHRSyncEmployeeQuitBatch
	require.NoError(t, json.Unmarshal(msgs["chat.hr.site-a.employees.quit"], &qb))
	assert.Equal(t, "site-a", qb.SiteID)
	assert.Equal(t, []string{"alice"}, qb.Accounts, "only the teams-sourced departure quits; the legacy row never does")
}

func TestMain(m *testing.M) { testutil.RunTests(m) }

// drainStream consumes every pending message, purges the stream, and returns
// the messages keyed by subject (at most one message per subject per run).
func drainStream(t *testing.T, stream jetstream.Stream) map[string][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
	require.NoError(t, err)
	out := make(map[string][]byte)
	for {
		batch, err := cons.Fetch(10, jetstream.FetchMaxWait(time.Second))
		require.NoError(t, err)
		n := 0
		for m := range batch.Messages() {
			data := m.Data()
			if m.Headers().Get("Nats-Encoding") == "zstd" {
				dec, err := zstd.NewReader(nil)
				require.NoError(t, err)
				data, err = dec.DecodeAll(data, nil)
				require.NoError(t, err)
				dec.Close()
			}
			out[m.Subject()] = data
			require.NoError(t, m.Ack())
			n++
		}
		if n == 0 {
			require.NoError(t, stream.Purge(ctx))
			return out
		}
	}
}
