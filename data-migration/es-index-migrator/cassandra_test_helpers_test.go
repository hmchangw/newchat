//go:build integration

package main

import (
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

// newTestCassandraSession creates an isolated keyspace (via
// testutil.CassandraKeyspace) with the Participant/Card UDTs and a
// messages_by_room table scoped to exactly the columns this service reads
// (see messageColumns — including enc_payload, which the encryption guard
// needs to be present in every SELECT this service issues). Adding a column
// to messageColumns without adding it here fails every read with "Undefined
// column name". Returns a
// session pinned to that keyspace so production queries (which use
// unqualified table names) work unmodified. Shared by every integration test
// in this package that needs a real Cassandra table — a second, drifted copy
// of this schema previously caused messagesource_integration_test.go and
// integration_test.go to disagree on whether enc_payload existed, which would
// have made any query against the older schema fail outright.
func newTestCassandraSession(t *testing.T, prefix string) (keyspace, hostAddr string, session *gocql.Session) {
	t.Helper()
	keyspace, adminSession, hostAddr := testutil.CassandraKeyspace(t, prefix)
	cql := func(format string) string { return fmt.Sprintf(format, keyspace) }

	stmts := []string{
		cql(`CREATE TYPE IF NOT EXISTS %s."Participant" (
			id           TEXT,
			eng_name     TEXT,
			company_name TEXT,
			app_id       TEXT,
			app_name     TEXT,
			is_bot       BOOLEAN,
			account      TEXT
		)`),
		cql(`CREATE TYPE IF NOT EXISTS %s."Card" (
			template TEXT,
			data     BLOB
		)`),
		cql(`CREATE TABLE IF NOT EXISTS %s.messages_by_room (
			room_id                  TEXT,
			bucket                   BIGINT,
			created_at               TIMESTAMP,
			message_id               TEXT,
			sender                   FROZEN<"Participant">,
			msg                      TEXT,
			attachments              LIST<BLOB>,
			card                     FROZEN<"Card">,
			tshow                    BOOLEAN,
			thread_parent_id         TEXT,
			thread_parent_created_at TIMESTAMP,
			deleted                  BOOLEAN,
			site_id                  TEXT,
			edited_at                TIMESTAMP,
			updated_at               TIMESTAMP,
			enc_payload              BLOB,
			type                     TEXT,
			PRIMARY KEY ((room_id, bucket), created_at, message_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`),
	}
	for _, stmt := range stmts {
		require.NoError(t, adminSession.Query(stmt).Exec())
	}

	// adminSession is keyspace-unscoped; open a session pinned to our isolated
	// keyspace so production queries (unqualified table names) work as-is.
	cluster := gocql.NewCluster(hostAddr)
	cluster.Consistency = gocql.One
	cluster.DisableInitialHostLookup = true
	cluster.Keyspace = keyspace
	ksSession, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(func() { ksSession.Close() })
	return keyspace, hostAddr, ksSession
}
