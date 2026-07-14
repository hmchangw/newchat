//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestSeedThreadParents_Integration seeds thread parents into a real Cassandra
// keyspace and asserts each parent is readable by message ID — the lookup
// message-gatekeeper performs via history-service's GetMessageByID before
// accepting a thread reply.
func TestSeedThreadParents_Integration(t *testing.T) {
	ctx := context.Background()

	keyspace, admin, host := testutil.CassandraKeyspace(t, "loadgen_thread")
	provisionHistorySchema(t, admin, keyspace)

	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.One
	cluster.DisableInitialHostLookup = true
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(session.Close)

	preset, ok := BuiltinPreset("small")
	require.True(t, ok)
	siteID := "site-test"
	tf := BuildThreadFixtures(&preset, 42, 2, siteID)

	sizer := msgbucket.New(72 * time.Hour)
	now := time.Now().UTC()
	count, err := SeedThreadParents(ctx, session, sizer, &tf, siteID, now)
	require.NoError(t, err)
	assert.Greater(t, count, 0)

	// Total parents written must equal what the fixtures contain.
	expected := 0
	for _, list := range tf.ParentsByRoom {
		expected += len(list)
	}
	assert.Equal(t, expected, count, "all fixture parents written")

	// Every seeded parent must be readable by ID (scalar columns only — avoid
	// scanning the sender UDT).
	for roomID, list := range tf.ParentsByRoom {
		for _, pm := range list {
			var gotID, gotRoom string
			var gotCreated time.Time
			err := session.Query(
				`SELECT message_id, room_id, created_at FROM messages_by_id WHERE message_id = ?`,
				pm.MessageID,
			).WithContext(ctx).Scan(&gotID, &gotRoom, &gotCreated)
			require.NoErrorf(t, err, "parent %s must be readable by id", pm.MessageID)
			assert.Equal(t, pm.MessageID, gotID)
			assert.Equal(t, roomID, gotRoom)
			assert.False(t, gotCreated.IsZero(), "created_at must be set for parent %s", pm.MessageID)
		}
	}
}
