package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

func TestBuildMessageAction(t *testing.T) {
	createdAt := time.Date(2026, 3, 9, 7, 0, 0, 0, time.UTC)
	msg := cassandra.Message{
		RoomID: "room1", MessageID: "msg1", SiteID: "site-a", CreatedAt: createdAt,
		Sender: cassandra.Participant{ID: "u1", Account: "alice"}, Msg: "hello",
	}

	action, err := buildMessageAction(msg, "messages-a-v1")

	require.NoError(t, err)
	assert.Equal(t, searchengine.ActionIndex, action.Action)
	assert.Equal(t, "messages-a-v1-2026-03", action.Index)
	assert.Equal(t, "msg1", action.DocID)
	assert.Equal(t, createdAt.UnixMilli(), action.Version)

	var doc searchindex.MessageDoc
	require.NoError(t, json.Unmarshal(action.Doc, &doc))
	assert.Equal(t, "alice", doc.UserAccount)
	assert.Equal(t, "hello", doc.Content)
}

func TestBuildMessageAction_MissingMessageIDIsAnError(t *testing.T) {
	_, err := buildMessageAction(cassandra.Message{RoomID: "room1"}, "messages-a-v1")
	require.Error(t, err)
}

func TestBuildMessageAction_ZeroCreatedAtIsAnError(t *testing.T) {
	_, err := buildMessageAction(cassandra.Message{MessageID: "m1", RoomID: "room1"}, "messages-a-v1")
	require.Error(t, err)
}
