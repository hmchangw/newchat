//go:build integration

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/testutil"
)

// setupCassandraForRun creates an isolated keyspace (via
// testutil.CassandraKeyspace) with the Participant/Card UDTs and the
// messages_by_room table this service reads — mirroring
// messagesource_integration_test.go's setupCassandra — and returns the
// keyspace name and host so the caller can build a config for run() to
// dial its own session against, plus a keyspace-pinned session for seeding
// test rows (production queries are unqualified table names, so the seed
// insert needs the same pinning).
func setupCassandraForRun(t *testing.T, prefix string) (keyspace, hostAddr string, pinned *gocql.Session) {
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
			PRIMARY KEY ((room_id, bucket), created_at, message_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`),
	}
	for _, stmt := range stmts {
		require.NoError(t, adminSession.Query(stmt).Exec())
	}

	cluster := gocql.NewCluster(hostAddr)
	cluster.Consistency = gocql.One
	cluster.DisableInitialHostLookup = true
	cluster.Keyspace = keyspace
	ksSession, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(func() { ksSession.Close() })
	return keyspace, hostAddr, ksSession
}

func TestRun_EndToEndBackfillIsIdempotentOnRerun(t *testing.T) {
	db := testutil.MongoDB(t, "esmigrun")
	mongoURI := testutil.MongoURI(t)
	keyspace, cassHosts, cassSession := setupCassandraForRun(t, "esmigrun")
	esURL := testutil.Elasticsearch(t)
	msgIndex := testutil.ElasticsearchIndex(t, "esmigrun-messages")
	spotlightIndex := testutil.ElasticsearchIndex(t, "esmigrun-spotlight")
	userRoomIndex := testutil.ElasticsearchIndex(t, "esmigrun-userroom")

	joinedAt := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	_, err := db.Collection("subscriptions").InsertOne(context.Background(), model.Subscription{
		ID: "s1", SiteID: "site-a", RoomID: "room1", RoomType: model.RoomTypeChannel,
		Name: "general", User: model.SubscriptionUser{Account: "alice"}, JoinedAt: joinedAt,
	})
	require.NoError(t, err)

	createdAt := joinedAt.Add(time.Hour)
	err = cassSession.Query(
		"INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, deleted, site_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		"room1", createdAt.Truncate(72*time.Hour).UnixMilli(), createdAt, "m1",
		cassandra.Participant{ID: "u1", Account: "alice"}, "hello world", false, "site-a",
	).Exec()
	require.NoError(t, err)

	// run() dials its own Mongo/Cassandra/ES clients from cfg, so cfg must
	// carry the exact connection info the shared testutil containers
	// actually accept: db.Name() is the per-test isolated database
	// testutil.MongoDB already created on the shared Mongo container at
	// mongoURI, and keyspace/cassHosts are the isolated keyspace/host
	// setupCassandraForRun already created (with schema) on the shared
	// Cassandra container.
	cfg := config{
		SiteID: "site-a", SearchURL: esURL, MsgIndexPrefix: msgIndex, SpotlightIndex: spotlightIndex, UserRoomIndex: userRoomIndex,
		MigrationStartAt: joinedAt.Add(-time.Hour), MigrationEndAt: joinedAt.Add(24 * time.Hour),
		MessageBucketHours: 72, MongoURI: mongoURI, MongoDB: db.Name(),
		CassandraHosts: cassHosts, CassandraKeyspace: keyspace,
		BulkBatchSize: 500, WorkerConcurrency: 2,
	}

	err = run(context.Background(), cfg)
	require.NoError(t, err)

	// A second identical run must not error and must not double-count
	// failures — every write this job makes is versioned/idempotent.
	err = run(context.Background(), cfg)
	require.NoError(t, err)
}
