package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

func TestSetDecodedAttachments(t *testing.T) {
	good, err := json.Marshal(cassandra.Attachment{ID: "f1", Title: "a.png", Type: "file"})
	require.NoError(t, err)

	msgs := []models.Message{
		{MessageID: "m1", Attachments: [][]byte{good}},
		{MessageID: "m2", Attachments: [][]byte{[]byte("{not json")}}, // malformed → skipped
		{MessageID: "m3"}, // no attachments → nil
		{MessageID: "m4", QuotedParentMessage: &cassandra.QuotedParentMessage{Attachments: [][]byte{good}}},
	}
	setDecodedAttachments(context.Background(), msgs)

	require.Len(t, msgs[0].DecodedAttachments, 1)
	assert.Equal(t, "f1", msgs[0].DecodedAttachments[0].ID)
	assert.Empty(t, msgs[1].DecodedAttachments) // malformed blob dropped, not fatal
	assert.Nil(t, msgs[2].DecodedAttachments)
	require.Len(t, msgs[3].QuotedParentMessage.DecodedAttachments, 1)
	assert.Equal(t, "f1", msgs[3].QuotedParentMessage.DecodedAttachments[0].ID)
}
