//go:build integration

package cassrepo

import (
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func setupCassandra(t testing.TB) *gocql.Session {
	t.Helper()
	keyspace, adminSession, host := testutil.CassandraKeyspace(t, "history_service_test")
	cql := func(format string) string { return fmt.Sprintf(format, keyspace) }

	for _, stmt := range []string{
		cql(`CREATE TYPE IF NOT EXISTS %s."Participant" (id TEXT, eng_name TEXT, company_name TEXT, app_id TEXT, app_name TEXT, is_bot BOOLEAN, account TEXT)`),
		cql(`CREATE TYPE IF NOT EXISTS %s."File" (id TEXT, name TEXT, type TEXT)`),
		cql(`CREATE TYPE IF NOT EXISTS %s."Card" (template TEXT, data BLOB)`),
		cql(`CREATE TYPE IF NOT EXISTS %s."CardAction" (verb TEXT, text TEXT, card_id TEXT, display_text TEXT, hide_exec_log BOOLEAN, card_tmid TEXT, data BLOB)`),
		cql(`CREATE TYPE IF NOT EXISTS %s."QuotedParentMessage" (message_id TEXT, room_id TEXT, sender FROZEN<"Participant">, created_at TIMESTAMP, msg TEXT, mentions SET<FROZEN<"Participant">>, attachments LIST<BLOB>, message_link TEXT, thread_parent_id TEXT, thread_parent_created_at TIMESTAMP)`),
		cql(`CREATE TYPE IF NOT EXISTS %s.reaction_key (emoji TEXT, user_account TEXT)`),
		cql(`CREATE TYPE IF NOT EXISTS %s.reactor_info (user_id TEXT, eng_name TEXT, chn_name TEXT, account TEXT, reacted_at TIMESTAMP)`),
	} {
		require.NoError(t, adminSession.Query(stmt).Exec())
	}

	require.NoError(t, adminSession.Query(cql(`CREATE TABLE IF NOT EXISTS %s.messages_by_room (
		room_id TEXT,
		bucket BIGINT,
		created_at TIMESTAMP,
		message_id TEXT,
		thread_room_id TEXT,
		sender FROZEN<"Participant">,
		msg TEXT,
		mentions SET<FROZEN<"Participant">>,
		attachments LIST<BLOB>,
		file FROZEN<"File">,
		card FROZEN<"Card">,
		card_action FROZEN<"CardAction">,
		tshow BOOLEAN,
		tcount INT,
		thread_parent_id TEXT,
		thread_parent_created_at TIMESTAMP,
		quoted_parent_message FROZEN<"QuotedParentMessage">,
		visible_to TEXT,
		reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
		deleted BOOLEAN,
		type TEXT,
		sys_msg_data BLOB,
		site_id TEXT,
		edited_at TIMESTAMP,
		updated_at TIMESTAMP,
		PRIMARY KEY ((room_id, bucket), created_at, message_id)
	) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`)).Exec())

	require.NoError(t, adminSession.Query(cql(`CREATE TABLE IF NOT EXISTS %s.messages_by_id (
		message_id TEXT,
		room_id TEXT,
		thread_room_id TEXT,
		sender FROZEN<"Participant">,
		msg TEXT,
		mentions SET<FROZEN<"Participant">>,
		attachments LIST<BLOB>,
		file FROZEN<"File">,
		card FROZEN<"Card">,
		card_action FROZEN<"CardAction">,
		tshow BOOLEAN,
		tcount INT,
		thread_parent_id TEXT,
		thread_parent_created_at TIMESTAMP,
		quoted_parent_message FROZEN<"QuotedParentMessage">,
		visible_to TEXT,
		reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
		deleted BOOLEAN,
		type TEXT,
		sys_msg_data BLOB,
		site_id TEXT,
		edited_at TIMESTAMP,
		created_at TIMESTAMP,
		updated_at TIMESTAMP,
		pinned_at TIMESTAMP,
		pinned_by FROZEN<"Participant">,
		PRIMARY KEY (message_id, created_at)
	) WITH CLUSTERING ORDER BY (created_at DESC)`)).Exec())

	require.NoError(t, adminSession.Query(cql(`CREATE TABLE IF NOT EXISTS %s.thread_messages_by_thread (
		thread_room_id TEXT,
		created_at TIMESTAMP,
		message_id TEXT,
		room_id TEXT,
		sender FROZEN<"Participant">,
		msg TEXT,
		mentions SET<FROZEN<"Participant">>,
		attachments LIST<BLOB>,
		file FROZEN<"File">,
		card FROZEN<"Card">,
		card_action FROZEN<"CardAction">,
		thread_parent_id TEXT,
		quoted_parent_message FROZEN<"QuotedParentMessage">,
		visible_to TEXT,
		reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
		deleted BOOLEAN,
		type TEXT,
		sys_msg_data BLOB,
		site_id TEXT,
		edited_at TIMESTAMP,
		updated_at TIMESTAMP,
		PRIMARY KEY ((thread_room_id), created_at, message_id)
	) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`)).Exec())

	require.NoError(t, adminSession.Query(cql(`CREATE TABLE IF NOT EXISTS %s.pinned_messages_by_room (
		room_id TEXT,
		pinned_at TIMESTAMP,
		message_id TEXT,
		sender FROZEN<"Participant">,
		msg TEXT,
		mentions SET<FROZEN<"Participant">>,
		attachments LIST<BLOB>,
		file FROZEN<"File">,
		card FROZEN<"Card">,
		card_action FROZEN<"CardAction">,
		quoted_parent_message FROZEN<"QuotedParentMessage">,
		visible_to TEXT,
		reactions MAP<FROZEN<reaction_key>, FROZEN<reactor_info>>,
		deleted BOOLEAN,
		type TEXT,
		sys_msg_data BLOB,
		site_id TEXT,
		edited_at TIMESTAMP,
		updated_at TIMESTAMP,
		pinned_by FROZEN<"Participant">,
		created_at TIMESTAMP,
		tshow BOOLEAN,
		thread_parent_id TEXT,
		thread_parent_created_at TIMESTAMP,
		PRIMARY KEY ((room_id), pinned_at, message_id)
	) WITH CLUSTERING ORDER BY (pinned_at DESC, message_id DESC)`)).Exec())

	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.One
	cluster.DisableInitialHostLookup = true
	cluster.Keyspace = keyspace
	ksSession, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(func() { ksSession.Close() })
	return ksSession
}
