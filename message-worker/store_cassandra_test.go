package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

func TestBuildCassandraMessage_ReencodesQuotedParentAttachments(t *testing.T) {
	msg := &model.Message{
		ID: "m1", RoomID: "r1",
		QuotedParentMessage: &cassandra.QuotedParentMessage{
			MessageID:          "p1",
			DecodedAttachments: []cassandra.Attachment{{ID: "f1", Title: "a.png", Type: "file"}},
		},
	}

	cm := buildCassandraMessage(msg)

	require.NotNil(t, cm.QuotedParentMessage)
	// Raw re-encoded for the LIST<BLOB> column (gocql binds the raw Attachments field).
	require.Len(t, cm.QuotedParentMessage.Attachments, 1)
	got, skipped := cassandra.DecodeAttachments(cm.QuotedParentMessage.Attachments)
	assert.Zero(t, skipped)
	require.Len(t, got, 1)
	assert.Equal(t, "f1", got[0].ID)

	// Caller's *msg not mutated (fresh-struct copy).
	assert.Nil(t, msg.QuotedParentMessage.Attachments)
}
