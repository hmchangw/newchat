package mongorepo

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

// The steps before paging read only the fields they sort and filter on, so the
// first read fetches exactly those; full documents come later, a page at a time.
// Pinning the set keeps that read from quietly growing back.
func TestSubscriptionLiteProjection_ExactFields(t *testing.T) {
	assert.Equal(t, bson.M{
		"_id":      1,
		"roomId":   1,
		"roomType": 1,
		"name":     1,
	}, subscriptionLiteProjection())
}

// subLite must decode from a full subscription document, ignoring fields it
// doesn't name, since the small projection returns part of the same shape.
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

// bsonTagPaths turns a struct's stored bson tags into Mongo field paths. It
// recurses into anonymous sub-structs, which covers roomBaseline's encKey, and
// skips _id and never-stored bson:"-" fields. Options like ",omitempty" are
// stripped so a differently written tag still matches.
func bsonTagPaths(t *testing.T, typ reflect.Type, prefix string) []string {
	t.Helper()
	var paths []string
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := f.Tag.Get("bson")
		require.NotEmpty(t, tag, "field %s must carry a bson tag", f.Name)
		name, _, _ := strings.Cut(tag, ",")
		require.NotEmpty(t, name, "field %s must carry a bson field name", f.Name)
		if name == "_id" || name == "-" {
			continue
		}
		ft := f.Type
		if ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}
		if ft.Kind() == reflect.Struct && ft.Name() == "" {
			paths = append(paths, bsonTagPaths(t, ft, prefix+name+".")...)
			continue
		}
		paths = append(paths, prefix+name)
	}
	return paths
}

// subscriptionFieldsProjection must cover every stored model.Subscription field
// (bson:"-" ones are filled at read time). A field added to the model but not
// the projection comes back zero-valued on the list path while every other read
// returns it — which is what once happened to origin.
func TestSubscriptionFieldsProjection_MatchesModelTags(t *testing.T) {
	proj := subscriptionFieldsProjection()

	assert.Contains(t, proj, "_id")
	want := bsonTagPaths(t, reflect.TypeOf(model.Subscription{}), "")
	assert.Len(t, proj, len(want)+1, "projection and model field counts must match (+1 for _id)")
	for _, path := range want {
		assert.Contains(t, proj, path, "projection must include %q", path)
	}
}

// The page read's projection and the roomBaseline struct must agree: a field in
// one but not the other quietly comes back zero-valued. Deriving the expected
// set from the struct's tags makes that a failing test, not a data bug.
func TestRoomBaselineProjection_MatchesStructTags(t *testing.T) {
	proj := roomBaselineProjection()

	want := bsonTagPaths(t, reflect.TypeOf(roomBaseline{}), "")
	assert.Len(t, proj, len(want))
	for _, path := range want {
		assert.Contains(t, proj, path, "projection must include %q", path)
	}
}
