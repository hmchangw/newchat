package mongorepo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

func marshalSubRaw(t *testing.T, sub *model.Subscription) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(sub)
	require.NoError(t, err)
	return bson.Raw(b)
}

// decodeSubLites must surface exactly the fields the pre-page phases read
// (roomId for sort-key resolution, roomType+name for the self-DM pin and the
// name tiebreak) while carrying the raw document through untouched, so the
// full model.Subscription decode can be deferred to the page rows.
func TestDecodeSubLites_ExtractsLiteFieldsAndKeepsRaw(t *testing.T) {
	joined := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	subs := []model.Subscription{
		{
			ID:       "sub-1",
			User:     model.SubscriptionUser{Account: "alice"},
			RoomID:   "room-1",
			SiteID:   "siteA",
			Name:     "general",
			RoomType: model.RoomTypeChannel,
			JoinedAt: joined,
			Favorite: true,
			Open:     true,
		},
		{
			ID:       "sub-2",
			User:     model.SubscriptionUser{Account: "alice"},
			RoomID:   "room-2",
			SiteID:   "siteA",
			Name:     "alice",
			RoomType: model.RoomTypeDM,
			JoinedAt: joined,
			Open:     true,
		},
	}
	raws := make([]bson.Raw, 0, len(subs))
	for i := range subs {
		raws = append(raws, marshalSubRaw(t, &subs[i]))
	}

	docs, err := decodeSubLites(raws)
	require.NoError(t, err)
	require.Len(t, docs, len(subs))

	for i := range subs {
		assert.Equal(t, subs[i].RoomID, docs[i].lite.RoomID)
		assert.Equal(t, subs[i].RoomType, docs[i].lite.RoomType)
		assert.Equal(t, subs[i].Name, docs[i].lite.Name)

		// The raw document must still decode to the complete subscription —
		// this is what the page enrichment relies on.
		var full model.Subscription
		require.NoError(t, bson.Unmarshal(docs[i].raw, &full))
		assert.Equal(t, subs[i], full)
	}
}

func TestDecodeSubLites_Empty(t *testing.T) {
	docs, err := decodeSubLites(nil)
	require.NoError(t, err)
	assert.Empty(t, docs)
}

func TestDecodeSubLites_MalformedDoc(t *testing.T) {
	valid := marshalSubRaw(t, &model.Subscription{ID: "sub-1", RoomID: "room-1"})
	// Truncated document: declared length exceeds the actual bytes.
	malformed := bson.Raw{0xFF, 0x00, 0x00, 0x00, 0x01}

	_, err := decodeSubLites([]bson.Raw{valid, malformed})
	assert.Error(t, err)
}
