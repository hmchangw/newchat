//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// setupCassandra creates the isolated keyspace's Participant/Card UDTs and a
// messages_by_room table scoped to exactly the columns this service reads
// (see messageColumns), then returns a session pinned to that keyspace so
// production queries (which use unqualified table names) work unmodified.
func setupCassandra(t *testing.T) *gocql.Session {
	t.Helper()
	keyspace, adminSession, host := testutil.CassandraKeyspace(t, "esmig")
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
			PRIMARY KEY ((room_id, bucket), created_at, message_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`),
	}
	for _, stmt := range stmts {
		require.NoError(t, adminSession.Query(stmt).Exec())
	}

	// adminSession is keyspace-unscoped; open a session pinned to our isolated
	// keyspace so production queries (unqualified table names) work as-is.
	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.One
	cluster.DisableInitialHostLookup = true
	cluster.Keyspace = keyspace
	ksSession, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(func() { ksSession.Close() })
	return ksSession
}

func insertTestMessage(t *testing.T, session *gocql.Session, roomID string, bucket int64, createdAt time.Time, msgID, msg string, deleted bool) {
	t.Helper()
	err := session.Query(
		"INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, deleted, site_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		roomID, bucket, createdAt, msgID, cassandra.Participant{ID: "u1", Account: "alice"}, msg, deleted, "site-a",
	).Exec()
	require.NoError(t, err)
}

func TestCassandraMessageSource_StreamMessages_MultiBucketWindow(t *testing.T) {
	session := setupCassandra(t)
	sizer := msgbucket.New(72 * time.Hour)
	source := newCassandraMessageSource(session, sizer)

	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(200 * time.Hour)
	inFirstBucket := from.Add(time.Hour)
	inThirdBucket := from.Add(150 * time.Hour)
	outsideWindow := to.Add(time.Hour)

	insertTestMessage(t, session, "room1", sizer.Of(inFirstBucket), inFirstBucket, "m1", "hello", false)
	insertTestMessage(t, session, "room1", sizer.Of(inThirdBucket), inThirdBucket, "m2", "world", false)
	insertTestMessage(t, session, "room1", sizer.Of(outsideWindow), outsideWindow, "m3", "too late", false)
	insertTestMessage(t, session, "room1", sizer.Of(inFirstBucket), inFirstBucket, "m4", "gone", true)

	var got []cassandra.Message
	err := source.StreamMessages(context.Background(), "site-a", "room1", from, to, func(m cassandra.Message) error {
		got = append(got, m)
		return nil
	})

	require.NoError(t, err)
	require.Len(t, got, 2, "expects m1 and m2 only: m3 is outside the window, m4 is deleted")
	ids := []string{got[0].MessageID, got[1].MessageID}
	require.ElementsMatch(t, []string{"m1", "m2"}, ids)
}

func TestCassandraMessageSource_StreamMessages_CallbackErrorAborts(t *testing.T) {
	session := setupCassandra(t)
	sizer := msgbucket.New(72 * time.Hour)
	source := newCassandraMessageSource(session, sizer)

	from := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	insertTestMessage(t, session, "room2", sizer.Of(from), from, "m1", "hello", false)

	callCount := 0
	err := source.StreamMessages(context.Background(), "site-a", "room2", from, to, func(m cassandra.Message) error {
		callCount++
		return errBoom
	})

	require.ErrorIs(t, err, errBoom)
	require.Equal(t, 1, callCount)
}

var errBoom = errors.New("boom")
