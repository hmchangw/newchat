package wire

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSoakWireContract_OmitsOptionalFieldsButKeepsRequiredZeroValues(t *testing.T) {
	encoded, err := json.Marshal(LoadHistoryRequest{Limit: 0})
	require.NoError(t, err)
	assert.JSONEq(t, `{"limit":0}`, string(encoded))

	encoded, err = json.Marshal(SubscriptionListRequest{Type: SubscriptionListType})
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"rooms"}`, string(encoded))

	encoded, err = json.Marshal(UserPageRequest{})
	require.NoError(t, err)
	assert.JSONEq(t, `{}`, string(encoded))
}

func TestSoakWireContract_DecodesMessageWithAdditionalReactions(t *testing.T) {
	payload := []byte(`{
		"roomId":"room-1",
		"createdAt":"2026-07-24T10:00:00Z",
		"messageId":"message-1",
		"sender":{"id":"user-1","account":"alice"},
		"msg":"hello",
		"reactions":{"wave":[{"account":"bob"}]}
	}`)

	var decoded Message
	require.NoError(t, json.Unmarshal(payload, &decoded))
	assert.Equal(t, "message-1", decoded.MessageID)
}

func TestSoakWireContract_UsesItemsForThreadPages(t *testing.T) {
	payload := []byte(`{"items":[{"threadRoomId":"thread-1"}],"hasNext":false}`)

	var decoded UserThreadListResponse
	require.NoError(t, json.Unmarshal(payload, &decoded))
	require.Len(t, decoded.Items, 1)
	assert.Equal(t, "thread-1", decoded.Items[0].ThreadRoomID)
}
