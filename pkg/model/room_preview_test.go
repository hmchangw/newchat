package model

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model/cassandra"
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

// The denormalized preview persists on the room doc; its BSON keys must be
// camelCase (matching the JSON shape) and must round-trip losslessly.
func TestRoom_PreviewMessage_BSONRoundTrip(t *testing.T) {
	created := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)
	room := Room{
		ID: "room-1",
		PreviewMessage: &PreviewMessage{
			MessageID: "m1",
			Sender:    Participant{Account: "alice", EngName: "Alice", DisplayName: "Alice A"},
			Content:   "hello",
			CreatedAt: created,
			Attachments: []cassandra.Attachment{{
				ID: "f1", Title: "pic.png", Type: "image", TitleLink: "/f1",
				FileType: "image/png", ImageURL: "/f1/img", ImageSize: 42,
				ImageDimensions: &cassandra.ImageDimensions{Width: 10, Height: 20},
			}},
			Mentions:  []Participant{{Account: "bob"}},
			VisibleTo: "alice",
		},
		PreviewAsOf: 1754388000000,
	}

	raw, err := bson.Marshal(room)
	require.NoError(t, err)

	// Stored keys are camelCase — the doc shape the $lookup projection reads.
	var doc bson.M
	require.NoError(t, bson.Unmarshal(raw, &doc))

	// Extract previewMessage and convert to consistent map type
	pmVal, ok := doc["previewMessage"]
	require.True(t, ok, "previewMessage key missing: %v", doc)
	pm := toMap(pmVal)
	require.NotNil(t, pm, "previewMessage could not be converted to map")

	assert.Equal(t, "m1", pm["messageId"])
	assert.Equal(t, "hello", pm["content"])
	assert.Contains(t, pm, "createdAt")

	// Check attachments array
	attsVal, ok := pm["attachments"]
	require.True(t, ok, "attachments missing from previewMessage")
	atts := attsVal.(bson.A)
	require.Len(t, atts, 1)

	// Check first attachment with nested imageDimensions
	attVal := atts[0]
	att := toMap(attVal)
	require.NotNil(t, att, "attachment could not be converted to map")

	assert.Equal(t, "pic.png", att["title"])
	assert.Equal(t, "image/png", att["fileType"])

	// Check imageDimensions nested in attachment
	dimsVal, ok := att["imageDimensions"]
	require.True(t, ok, "imageDimensions missing from attachment")
	dims := toMap(dimsVal)
	require.NotNil(t, dims, "imageDimensions could not be converted to map")
	assert.Contains(t, dims, "width")

	// Check previewAsOf
	assert.EqualValues(t, 1754388000000, doc["previewAsOf"])

	var back Room
	require.NoError(t, bson.Unmarshal(raw, &back))
	assert.Equal(t, room.PreviewMessage, back.PreviewMessage)
	assert.Equal(t, room.PreviewAsOf, back.PreviewAsOf)
}

// PreviewAsOf is a storage-only watermark: never serialized to clients.
func TestRoom_PreviewAsOf_NotInJSON(t *testing.T) {
	room := Room{ID: "r", PreviewAsOf: 123}
	b, err := json.Marshal(room) // encoding/json
	require.NoError(t, err)
	assert.NotContains(t, string(b), "previewAsOf")
}

// EnrichedSubscription.PreviewMessage is the denormalized room preview projected
// from the rooms $lookup $addFields. It must round-trip through BSON and be
// excluded from JSON (internal field).
func TestEnrichedSubscription_PreviewMessage_BSONRoundTrip(t *testing.T) {
	created := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)
	sub := EnrichedSubscription{
		Subscription: Subscription{
			ID:       "s1",
			User:     SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID:   "r1",
			SiteID:   "site-a",
			JoinedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		PreviewMessage: &PreviewMessage{
			MessageID: "m1",
			Sender:    Participant{Account: "alice", EngName: "Alice", DisplayName: "Alice A"},
			Content:   "hello",
			CreatedAt: created,
		},
	}

	// BSON round-trip: previewMessage must be present with camelCase keys
	raw, err := bson.Marshal(sub)
	require.NoError(t, err)

	var doc bson.M
	require.NoError(t, bson.Unmarshal(raw, &doc))
	pmVal, ok := doc["previewMessage"]
	require.True(t, ok, "previewMessage key missing from EnrichedSubscription BSON")

	pm := toMap(pmVal)
	require.NotNil(t, pm, "previewMessage could not be converted to map")
	assert.Equal(t, "m1", pm["messageId"])
	assert.Equal(t, "hello", pm["content"])

	// JSON round-trip: previewMessage must be excluded (json:"-")
	jsonData, err := json.Marshal(sub)
	require.NoError(t, err)
	assert.NotContains(t, string(jsonData), "previewMessage",
		"previewMessage must be excluded from JSON (json:\"-\")")

	// Verify struct round-trip
	var back EnrichedSubscription
	require.NoError(t, bson.Unmarshal(raw, &back))
	assert.Equal(t, sub.PreviewMessage, back.PreviewMessage)
}
