package model

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// toMap converts bson.D or bson.M to bson.M for consistent type handling
func toMap(v interface{}) bson.M {
	switch val := v.(type) {
	case bson.M:
		return val
	case bson.D:
		result := make(bson.M)
		for _, elem := range val {
			result[elem.Key] = elem.Value
		}
		return result
	default:
		return nil
	}
}

func samplePreviewRoom() Room {
	created := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)
	return Room{
		ID: "room-1",
		PreviewMeta: &PreviewMeta{
			MessageID: "m1",
			Sender:    Participant{Account: "alice", EngName: "Alice", DisplayName: "Alice A"},
			CreatedAt: created,
			Mentions:  []Participant{{Account: "bob"}},
		},
		PreviewCiphertext: []byte{0xde, 0xad, 0xbe, 0xef},
		PreviewNonce:      []byte{0x01, 0x02, 0x03},
		PreviewKeyEpoch:   2,
		PreviewForMsgID:   "m2",
		PreviewAsOf:       1754388000000,
	}
}

// The stored preview must persist under camelCase BSON keys — the doc shape
// history-service's projection reads back — and round-trip losslessly.
func TestRoom_PreviewFields_BSONRoundTrip(t *testing.T) {
	room := samplePreviewRoom()

	raw, err := bson.Marshal(room)
	require.NoError(t, err)

	var doc bson.M
	require.NoError(t, bson.Unmarshal(raw, &doc))

	metaVal, ok := doc["previewMeta"]
	require.True(t, ok, "previewMeta key missing: %v", doc)
	meta := toMap(metaVal)
	require.NotNil(t, meta, "previewMeta could not be converted to map")

	assert.Equal(t, "m1", meta["messageId"])
	assert.Contains(t, meta, "createdAt")
	assert.Contains(t, meta, "sender")
	assert.NotContains(t, meta, "visibleTo",
		"a restricted message is preview-ineligible, so no scope can reach the stored meta")

	// The sealed body is never stored in the clear: no content/attachments key
	// may appear anywhere in the plaintext meta subdocument.
	assert.NotContains(t, meta, "content")
	assert.NotContains(t, meta, "attachments")

	assert.Contains(t, doc, "previewCiphertext")
	assert.Contains(t, doc, "previewNonce")
	assert.EqualValues(t, 2, doc["previewKeyEpoch"])
	assert.Equal(t, "m2", doc["previewForMsgId"])
	assert.EqualValues(t, 1754388000000, doc["previewAsOf"])

	var back Room
	require.NoError(t, bson.Unmarshal(raw, &back))
	assert.Equal(t, room.PreviewMeta, back.PreviewMeta)
	assert.Equal(t, room.PreviewCiphertext, back.PreviewCiphertext)
	assert.Equal(t, room.PreviewNonce, back.PreviewNonce)
	assert.Equal(t, room.PreviewKeyEpoch, back.PreviewKeyEpoch)
	assert.Equal(t, room.PreviewForMsgID, back.PreviewForMsgID)
	assert.Equal(t, room.PreviewAsOf, back.PreviewAsOf)
}

// Every stored preview field is storage-only: clients receive a preview through
// the rooms.get reply and message events, never through Room.
func TestRoom_PreviewFields_NotInJSON(t *testing.T) {
	b, err := json.Marshal(samplePreviewRoom())
	require.NoError(t, err)

	for _, key := range []string{
		"previewMeta", "previewCiphertext", "previewNonce",
		"previewKeyEpoch", "previewForMsgId", "previewAsOf",
	} {
		assert.NotContains(t, string(b), key, "%s must not be serialized to clients", key)
	}
}

// An unwarmed room stores no preview fragment at all — omitempty must keep
// every key out of the document rather than writing zero values.
func TestRoom_PreviewFields_OmittedWhenUnset(t *testing.T) {
	raw, err := bson.Marshal(Room{ID: "room-1"})
	require.NoError(t, err)

	var doc bson.M
	require.NoError(t, bson.Unmarshal(raw, &doc))

	for _, key := range []string{
		"previewMeta", "previewCiphertext", "previewNonce",
		"previewKeyEpoch", "previewForMsgId", "previewAsOf",
	} {
		assert.NotContains(t, doc, key)
	}
}

// PreviewMeta carries precisely the fields Cassandra leaves unencrypted.
func TestPreviewMeta_BSONRoundTrip(t *testing.T) {
	meta := PreviewMeta{
		MessageID: "m1",
		Sender:    Participant{Account: "alice"},
		CreatedAt: time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC),
		Mentions:  []Participant{{Account: "bob"}, {Account: "carol"}},
		VisibleTo: "u1,u2",
	}

	raw, err := bson.Marshal(meta)
	require.NoError(t, err)

	var back PreviewMeta
	require.NoError(t, bson.Unmarshal(raw, &back))
	assert.Equal(t, meta, back)
}
