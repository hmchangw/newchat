//go:build integration

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// setupCassandra returns a session on an isolated keyspace with the schema
// this service reads (see messageColumns), via the shared helper in
// cassandra_test_helpers_test.go.
func setupCassandra(t *testing.T) *gocql.Session {
	t.Helper()
	_, _, session := newTestCassandraSession(t, "esmig")
	return session
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

func TestCassandraMessageSource_StreamMessages_EncryptedRowAbortsStream(t *testing.T) {
	session := setupCassandra(t)
	sizer := msgbucket.New(72 * time.Hour)
	source := newCassandraMessageSource(session, sizer)

	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	insertTestMessage(t, session, "room3", sizer.Of(from), from, "plain1", "hello", false)
	err := session.Query(
		"INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, enc_payload, site_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		"room3", sizer.Of(from), from.Add(time.Minute), "encrypted1", cassandra.Participant{ID: "u1", Account: "alice"},
		[]byte{0xDE, 0xAD, 0xBE, 0xEF}, "site-a",
	).Exec()
	require.NoError(t, err)

	var got []cassandra.Message
	streamErr := source.StreamMessages(context.Background(), "site-a", "room3", from, to, func(m cassandra.Message) error {
		got = append(got, m)
		return nil
	})

	require.ErrorIs(t, streamErr, errEncryptedMessage,
		"this migrator only supports plaintext-column data; an encrypted row must hard-fail rather than silently index blank content")
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
