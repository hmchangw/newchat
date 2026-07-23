package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/teamsmigrate"
)

func teamsBatch(t *testing.T, msgs ...teamsmigrate.Message) []byte {
	t.Helper()
	raws := make([]json.RawMessage, 0, len(msgs))
	for i := range msgs {
		b, err := json.Marshal(msgs[i])
		require.NoError(t, err)
		raws = append(raws, b)
	}
	data, err := json.Marshal(model.TeamsBatchRequest{Messages: raws})
	require.NoError(t, err)
	return data
}

func TestTeamsMigrationCollection_Metadata(t *testing.T) {
	c := newTeamsMigrationCollection("messages-site-a-v1", "site-a", false)
	assert.Equal(t, "message-sync-teams", c.ConsumerName())
	assert.Equal(t, []string{"chat.msg.canonical.site-a.teams.batch"}, c.FilterSubjects("site-a"))
	// Same MESSAGES_CANONICAL stream + message template as messageCollection.
	assert.Equal(t, "MESSAGES_CANONICAL_site-a", c.StreamConfig("site-a").Name)
	assert.Equal(t, "messages-site-a_template", c.TemplateName())
}

func TestTeamsMigrationCollection_BuildAction(t *testing.T) {
	c := newTeamsMigrationCollection("messages-site-a-v1", "site-a", false)
	ts := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)

	data := teamsBatch(t,
		teamsmigrate.Message{
			ID: "tm-1", RoomID: "room-1", MessageType: "message",
			From: teamsmigrate.User{ID: "graph-1"},
			Body: teamsmigrate.Body{ContentType: "text", Content: "one"}, CreatedDateTime: ts,
		},
		teamsmigrate.Message{
			ID: "tm-2", RoomID: "room-1", MessageType: "message",
			From: teamsmigrate.User{ID: "graph-2"},
			Body: teamsmigrate.Body{ContentType: "html", Content: "<b>two</b>"}, CreatedDateTime: ts,
		},
	)
	actions, err := c.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 2)

	wantEmp := teamsmigrate.EmployeeIDFromGraphID("graph-1")
	assert.Equal(t, teamsmigrate.DeterministicMessageID("room-1", "tm-1"), actions[0].DocID)

	var doc MessageSearchIndex
	require.NoError(t, json.Unmarshal(actions[0].Doc, &doc))
	assert.Equal(t, wantEmp, doc.UserID)      // author key = employeeId hash, no Mongo read
	assert.Equal(t, wantEmp, doc.UserAccount) // best-effort reuse
	assert.Equal(t, "room-1", doc.RoomID)
	assert.Equal(t, "site-a", doc.SiteID)
	assert.Equal(t, "one", doc.Content)
	assert.Equal(t, ts, doc.CreatedAt)

	// html body renders to markdown
	var doc2 MessageSearchIndex
	require.NoError(t, json.Unmarshal(actions[1].Doc, &doc2))
	assert.Equal(t, "**two**", doc2.Content)
}

func TestTeamsMigrationCollection_BuildAction_Skips(t *testing.T) {
	c := newTeamsMigrationCollection("messages-site-a-v1", "site-a", false)
	ts := time.Now().UTC()

	data := teamsBatch(t,
		teamsmigrate.Message{ID: "", RoomID: "room-1", MessageType: "message", CreatedDateTime: ts},                                       // no id
		teamsmigrate.Message{ID: "tm-2", RoomID: "", MessageType: "message", CreatedDateTime: ts},                                         // no roomId
		teamsmigrate.Message{ID: "tm-3", RoomID: "room-1", MessageType: "systemEventMessage", CreatedDateTime: ts},                        // system
		teamsmigrate.Message{ID: "tm-4", RoomID: "room-1", MessageType: "message", From: teamsmigrate.User{ID: "g"}, CreatedDateTime: ts}, // kept
	)
	actions, err := c.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 1)
	assert.Equal(t, teamsmigrate.DeterministicMessageID("room-1", "tm-4"), actions[0].DocID)
}
