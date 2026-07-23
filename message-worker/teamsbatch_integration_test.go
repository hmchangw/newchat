//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/teamsmigrate"
	"github.com/hmchangw/chat/pkg/userstore"
)

// TestTeamsBatch_Integration drives the batch through the real transform + the real
// message-worker persist pipeline (isMigration=true suppresses thread side-effects),
// asserting persistence and an idempotent re-run (no dup row from the deterministic id).
func TestTeamsBatch_Integration(t *testing.T) {
	ctx := context.Background()
	cass := setupCassandra(t)
	mongoDB := setupMongo(t)

	// Seed a user whose display name the resolver reuses (no create needed).
	userCol := mongoDB.Collection("users")
	_, err := userCol.InsertOne(ctx, bson.M{
		"_id": "u-1", "account": "alice", "siteId": "site-a",
		"engName": "Alice", "chineseName": "Alice", "employeeId": "EMP001",
	})
	require.NoError(t, err)

	store := NewCassandraStore(cass, msgbucket.New(24*time.Hour), nil)
	us := userstore.NewMongoStore(userCol)
	threadStore := newThreadStoreMongo(mongoDB)
	persister := NewHandler(store, us, threadStore, "site-a",
		func(context.Context, string, []byte, string) error { return nil })

	teams := newTeamsBatchHandler(newMongoHRIdentityStore(mongoDB), "site-a", persister.processMessage)

	raw := mustJSON(teamsmigrate.Message{
		ID: "tm-1", RoomID: "room-1", MessageType: "message",
		From:            teamsmigrate.User{ID: "graph-1", DisplayName: "Alice"},
		Body:            teamsmigrate.Body{ContentType: "text", Content: "history line"},
		CreatedDateTime: time.Now().UTC().Truncate(time.Millisecond),
	})
	req := model.TeamsBatchRequest{Messages: []json.RawMessage{raw}}
	wantID := teamsmigrate.DeterministicMessageID("room-1", "tm-1")

	// Run twice: same batch → same deterministic id → idempotent.
	for i := 0; i < 2; i++ {
		require.NoError(t, teams.handleBatch(ctx, req))
	}

	var gotMsg string
	require.NoError(t, cass.Query(
		`SELECT msg FROM messages_by_id WHERE message_id = ?`, wantID,
	).Scan(&gotMsg))
	assert.Equal(t, "history line", gotMsg)

	var count int
	require.NoError(t, cass.Query(
		`SELECT COUNT(*) FROM messages_by_id WHERE message_id = ?`, wantID,
	).Scan(&count))
	assert.Equal(t, 1, count, "idempotent re-run leaves a single row")
}
