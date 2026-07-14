//go:build integration

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestHistoryWorkload_EndToEnd exercises the full history workload:
// real Cassandra + Mongo seeding via the production seed helpers, then a
// short sustained-run round-trip against a stub history-service that mirrors
// natsrouter's subject layout. Asserts seeded rows are present and the
// generator reports non-zero per-endpoint samples on exit.
func TestHistoryWorkload_EndToEnd(t *testing.T) {
	ctx := context.Background()

	// --- Cassandra: keyspace + schema (mirror docker-local/cassandra/init).
	keyspace, admin, host := testutil.CassandraKeyspace(t, "loadgen_history")
	provisionHistorySchema(t, admin, keyspace)

	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.One
	cluster.DisableInitialHostLookup = true
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(session.Close)

	// --- Mongo.
	db := testutil.MongoDB(t, "loadgen_history")

	// --- Build fixtures + seed.
	preset, ok := BuiltinHistoryPreset("history-small")
	require.True(t, ok)
	siteID := "site-test"
	now := time.Now().UTC()
	res := BuildHistoryFixtures(&preset, 42, siteID, now)

	require.NoError(t, Seed(ctx, db, &res.Fixtures))
	require.NoError(t, SeedThreadRooms(ctx, db, &res, siteID))
	sizer := msgbucket.New(72 * time.Hour)
	totalRows, err := SeedHistoryCassandra(ctx, session, sizer, &res, siteID)
	require.NoError(t, err)

	// Cross-check row counts. history-small fits in memory so FullPlan is OK.
	plan := res.FullPlan()
	expectedTopLevel := 0
	for i := range plan.Messages {
		if plan.Messages[i].ThreadParentID == "" {
			expectedTopLevel++
		}
	}
	require.Equal(t, len(plan.Messages), totalRows, "seed reported row count")
	var byRoomCount int
	require.NoError(t, session.Query(
		fmt.Sprintf("SELECT count(*) FROM %s.messages_by_room", keyspace),
	).Scan(&byRoomCount))
	assert.Equal(t, expectedTopLevel, byRoomCount, "messages_by_room row count")

	var byIDCount int
	require.NoError(t, session.Query(
		fmt.Sprintf("SELECT count(*) FROM %s.messages_by_id", keyspace),
	).Scan(&byIDCount))
	// messages_by_id receives every row (top-level + replies).
	assert.Equal(t, len(plan.Messages), byIDCount, "messages_by_id row count")

	// --- NATS: stub history-service that responds with empty pages.
	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	historySub, err := nc.Subscribe(subject.MsgHistoryWildcard(siteID), func(m *nats.Msg) {
		_ = m.Respond([]byte(`{"messages":[]}`))
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = historySub.Unsubscribe() })

	threadSub, err := nc.Subscribe(subject.MsgThreadWildcard(siteID), func(m *nats.Msg) {
		_ = m.Respond([]byte(`{"messages":[]}`))
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = threadSub.Unsubscribe() })

	// --- Drive the generator briefly.
	collector := NewHistoryCollector()
	requester := newNATSHistoryRequester(nc)
	genCfg := HistoryGeneratorConfig{
		Preset:          &preset,
		Fixtures:        &res,
		SiteID:          siteID,
		Rate:            50,
		Mix:             EndpointMix{History: 80, Thread: 20},
		BeforeMode:      BeforeMode{Open: 100},
		ScrollbackPages: 3,
		PageLimit:       20,
		RequestTimeout:  2 * time.Second,
		Requester:       requester,
		Collector:       collector,
		MaxInFlight:     16,
	}
	gen := NewHistoryGenerator(&genCfg, 42)

	runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	require.NoError(t, gen.Run(runCtx))
	time.Sleep(500 * time.Millisecond) // drain trailing replies

	historySamples := collector.HistorySamples()
	threadSamples := collector.ThreadSamples()
	assert.NotEmpty(t, historySamples, "generator produced zero history samples")
	// history-small has zero thread parents; every thread pick should fall back.
	assert.Empty(t, threadSamples, "history-small has no thread parents; expected zero thread samples")
	assert.Greater(t, collector.NoThreadParentsCount(), 0)
	assert.Equal(t, 0, collector.TimeoutErrors(), "no requests should time out against the stub")
	assert.Equal(t, 0, collector.ReplyErrors(), "stub never returns an error")
}

// provisionHistorySchema applies the UDT + table DDL the history workload
// needs. Lifted from history-service/internal/cassrepo/integration_test.go so
// the loadgen test doesn't depend on internal test helpers from another
// package.
func provisionHistorySchema(t *testing.T, session *gocql.Session, keyspace string) {
	t.Helper()
	cql := func(format string) string { return fmt.Sprintf(format, keyspace) }
	udts := []string{
		cql(`CREATE TYPE IF NOT EXISTS %s."Participant" (id TEXT, eng_name TEXT, company_name TEXT, app_id TEXT, app_name TEXT, is_bot BOOLEAN, account TEXT)`),
		cql(`CREATE TYPE IF NOT EXISTS %s."File" (id TEXT, name TEXT, type TEXT)`),
		cql(`CREATE TYPE IF NOT EXISTS %s."Card" (template TEXT, data BLOB)`),
		cql(`CREATE TYPE IF NOT EXISTS %s."CardAction" (verb TEXT, text TEXT, card_id TEXT, display_text TEXT, hide_exec_log BOOLEAN, card_tmid TEXT, data BLOB)`),
		cql(`CREATE TYPE IF NOT EXISTS %s."QuotedParentMessage" (message_id TEXT, room_id TEXT, sender FROZEN<"Participant">, created_at TIMESTAMP, msg TEXT, mentions SET<FROZEN<"Participant">>, attachments LIST<BLOB>, message_link TEXT, thread_parent_id TEXT, thread_parent_created_at TIMESTAMP)`),
	}
	for _, stmt := range udts {
		require.NoError(t, session.Query(stmt).Exec())
	}
	tables := []string{
		cql(`CREATE TABLE IF NOT EXISTS %s.messages_by_room (
			room_id TEXT, bucket BIGINT, created_at TIMESTAMP, message_id TEXT,
			thread_room_id TEXT, sender FROZEN<"Participant">, msg TEXT,
			mentions SET<FROZEN<"Participant">>, attachments LIST<BLOB>,
			file FROZEN<"File">, card FROZEN<"Card">, card_action FROZEN<"CardAction">,
			tshow BOOLEAN, tcount INT, thread_parent_id TEXT,
			thread_parent_created_at TIMESTAMP, quoted_parent_message FROZEN<"QuotedParentMessage">,
			visible_to TEXT, reactions MAP<TEXT, FROZEN<SET<FROZEN<"Participant">>>>,
			deleted BOOLEAN, type TEXT, sys_msg_data BLOB, site_id TEXT,
			edited_at TIMESTAMP, updated_at TIMESTAMP,
			PRIMARY KEY ((room_id, bucket), created_at, message_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`),
		cql(`CREATE TABLE IF NOT EXISTS %s.messages_by_id (
			message_id TEXT, room_id TEXT, thread_room_id TEXT,
			sender FROZEN<"Participant">, msg TEXT,
			mentions SET<FROZEN<"Participant">>, attachments LIST<BLOB>,
			file FROZEN<"File">, card FROZEN<"Card">, card_action FROZEN<"CardAction">,
			tshow BOOLEAN, tcount INT, thread_parent_id TEXT,
			thread_parent_created_at TIMESTAMP, quoted_parent_message FROZEN<"QuotedParentMessage">,
			visible_to TEXT, reactions MAP<TEXT, FROZEN<SET<FROZEN<"Participant">>>>,
			deleted BOOLEAN, type TEXT, sys_msg_data BLOB, site_id TEXT,
			edited_at TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP,
			pinned_at TIMESTAMP, pinned_by FROZEN<"Participant">,
			PRIMARY KEY (message_id, created_at)
		) WITH CLUSTERING ORDER BY (created_at DESC)`),
		cql(`CREATE TABLE IF NOT EXISTS %s.thread_messages_by_thread (
			thread_room_id TEXT, created_at TIMESTAMP, message_id TEXT, room_id TEXT,
			sender FROZEN<"Participant">, msg TEXT,
			mentions SET<FROZEN<"Participant">>, attachments LIST<BLOB>,
			file FROZEN<"File">, card FROZEN<"Card">, card_action FROZEN<"CardAction">,
			thread_parent_id TEXT, tshow BOOLEAN, quoted_parent_message FROZEN<"QuotedParentMessage">,
			visible_to TEXT, reactions MAP<TEXT, FROZEN<SET<FROZEN<"Participant">>>>,
			deleted BOOLEAN, type TEXT, sys_msg_data BLOB, site_id TEXT,
			edited_at TIMESTAMP, updated_at TIMESTAMP,
			PRIMARY KEY ((thread_room_id), created_at, message_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`),
	}
	for _, stmt := range tables {
		require.NoError(t, session.Query(stmt).Exec())
	}
}
