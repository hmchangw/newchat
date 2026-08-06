package mongorepo

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

// The pre-page phases read only the sort/filter fields, so phase 1 must fetch
// exactly those — the full subscription documents are refetched page-sized in
// the enrich step. Pinning the set keeps the fetch from silently regrowing.
func TestSubscriptionLiteProjection_ExactFields(t *testing.T) {
	assert.Equal(t, bson.M{
		"_id":      1,
		"roomId":   1,
		"roomType": 1,
		"name":     1,
	}, subscriptionLiteProjection())
}

// subLite must decode from a full subscription document (unknown fields
// ignored) — the lite projection returns a subset of the same shape.
func TestSubLite_DecodesFromFullDocument(t *testing.T) {
	sub := model.Subscription{
		ID:       "sub-1",
		User:     model.SubscriptionUser{Account: "alice"},
		RoomID:   "room-1",
		SiteID:   "siteA",
		Name:     "general",
		RoomType: model.RoomTypeChannel,
		JoinedAt: time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC),
		Favorite: true,
		Open:     true,
	}
	b, err := bson.Marshal(&sub)
	require.NoError(t, err)

	var lite subLite
	require.NoError(t, bson.Unmarshal(b, &lite))
	assert.Equal(t, sub.ID, lite.ID)
	assert.Equal(t, sub.RoomID, lite.RoomID)
	assert.Equal(t, sub.RoomType, lite.RoomType)
	assert.Equal(t, sub.Name, lite.Name)
}

// bsonTagPaths flattens a struct's bson tags into Mongo field paths (one level
// of nesting, matching roomBaseline's encKey sub-document), excluding _id.
func bsonTagPaths(t *testing.T, typ reflect.Type, prefix string) []string {
	t.Helper()
	var paths []string
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := f.Tag.Get("bson")
		require.NotEmpty(t, tag, "field %s must carry a bson tag", f.Name)
		if tag == "_id" {
			continue
		}
		if f.Type.Kind() == reflect.Struct && f.Type != reflect.TypeOf(time.Time{}) && f.Type.Name() == "" {
			paths = append(paths, bsonTagPaths(t, f.Type, prefix+tag+".")...)
			continue
		}
		paths = append(paths, prefix+tag)
	}
	return paths
}

// The enrich read's projection and the roomBaseline struct are the two halves
// of one contract: a field present in one but not the other silently decodes
// as a zero value. Deriving the expected set from the struct tags makes the
// drift a test failure instead of a data bug.
func TestRoomBaselineProjection_MatchesStructTags(t *testing.T) {
	proj := roomBaselineProjection()

	want := bsonTagPaths(t, reflect.TypeOf(roomBaseline{}), "")
	assert.Len(t, proj, len(want))
	for _, path := range want {
		assert.Contains(t, proj, path, "projection must include %q", path)
	}
}
