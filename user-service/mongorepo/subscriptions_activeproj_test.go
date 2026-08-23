package mongorepo

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/models"
)

// bsonTagKeys returns the stored bson field names of a flat struct, sorted.
// Tag options like ",omitempty" are stripped so a differently written tag still
// matches, and bson:"-" fields are skipped as never stored. A field with no
// bson tag at all fails the test outright rather than being skipped: an
// untagged field would still be stored under a driver-derived name, would not
// be named in the projection, and would silently decode as a zero value — the
// exact wrong-badge hazard this guard exists to catch.
func bsonTagKeys(t *testing.T, typ reflect.Type) []string {
	t.Helper()
	keys := []string{}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("bson")
		if tag == "-" {
			continue
		}
		require.NotEmpty(t, tag, "field %s.%s has no bson tag", typ.Name(), field.Name)
		keys = append(keys, strings.Split(tag, ",")[0])
	}
	sort.Strings(keys)
	return keys
}

// The badge path decodes into models.ActiveSubscription, so the terminal
// $project must name exactly that struct's fields. Adding a field to the struct
// without projecting it would decode as a zero value — a wrong badge with no
// error anywhere — so the two are pinned to each other here.
func TestActiveSubscriptionProjection_MatchesRowType(t *testing.T) {
	proj := activeSubscriptionProjection()

	idVal, hasID := proj["_id"]
	require.True(t, hasID, "_id must be named explicitly (Mongo returns it by default)")
	assert.Equal(t, 0, idVal, "_id must be excluded, not included")

	got := []string{}
	for k, v := range proj {
		if k == "_id" {
			continue
		}
		assert.Equal(t, 1, v, "projection %q must be an inclusion", k)
		got = append(got, k)
	}
	sort.Strings(got)

	assert.Equal(t, bsonTagKeys(t, reflect.TypeOf(models.ActiveSubscription{})), got,
		"activeSubscriptionProjection and models.ActiveSubscription must name the same fields")
}

// Confirms the marshalled badge row is smaller than the marshalled enriched
// row — a coarse size-direction check only. It cannot prove no key material
// reaches the badge path, since both structs here are hardcoded literals, not
// a real query result; that guarantee is what
// TestGetActiveSubscriptions_ProjectsBadgeFields_Integration's raw-BSON
// assertion in subscriptions_test.go actually covers.
func TestActiveSubscriptionRow_IsLean(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)

	lean, err := bson.Marshal(models.ActiveSubscription{
		RoomID:       "01970a4f8c2d7c9aabcde01234567890",
		SiteID:       "site-a",
		LastSeenAt:   &now,
		ThreadUnread: []string{"01970a4f8c2d7c9aabcde01234567891"},
		LastMsgAt:    &now,
	})
	require.NoError(t, err)

	fat, err := bson.Marshal(model.EnrichedSubscription{
		Subscription: model.Subscription{
			ID:       "01970a4f8c2d7c9aabcde01234567892",
			User:     model.SubscriptionUser{ID: "01970a4f8c2d7c9aabcde01234567893", Account: "alice"},
			RoomID:   "01970a4f8c2d7c9aabcde01234567890",
			SiteID:   "site-a",
			Roles:    []model.Role{model.RoleMember},
			Name:     "engineering-general",
			RoomType: model.RoomTypeChannel,
			JoinedAt: now, LastSeenAt: &now,
			ThreadUnread:      []string{"01970a4f8c2d7c9aabcde01234567891"},
			Alert:             true,
			Open:              true,
			FavoriteUpdatedAt: &now, MuteUpdatedAt: &now, RolesUpdatedAt: &now,
			NameUpdatedAt: &now, RestrictUpdatedAt: &now, SectionUpdatedAt: &now,
		},
		UserCount: 42, LastMsgAt: &now, LastMsgID: "01970a4f8c2d7c9aabcde0123456789a",
		LastMentionAllAt: &now, MinUserLastSeenAt: &now, AppCount: 2,
		RoomName:    "engineering-general",
		RoomKeyPriv: make([]byte, 120),
		RoomKeyVer:  3,
	})
	require.NoError(t, err)

	// The number goes in the PR description; the assertion just guards the direction.
	t.Logf("ActiveSubscription=%dB EnrichedSubscription=%dB ratio=%.1fx",
		len(lean), len(fat), float64(len(fat))/float64(len(lean)))
	assert.Less(t, len(lean), len(fat), "the badge row must be smaller than the enriched row")
}

// The row type's bson tags must match what the subscriptions collection actually
// stores. The projection guard above only proves the projection and the struct
// agree with each other — decoding a marshalled Subscription proves both name
// real stored fields.
func TestActiveSubscription_DecodesFromFullSubscriptionDocument(t *testing.T) {
	seen := time.Date(2026, 8, 23, 9, 0, 0, 0, time.UTC)
	sub := model.Subscription{
		ID:           "01970a4f8c2d7c9aabcde01234567892",
		User:         model.SubscriptionUser{Account: "alice"},
		RoomID:       "01970a4f8c2d7c9aabcde01234567890",
		SiteID:       "site-a",
		RoomType:     model.RoomTypeChannel,
		LastSeenAt:   &seen,
		ThreadUnread: []string{"pm-1"},
	}
	b, err := bson.Marshal(&sub)
	require.NoError(t, err)

	var row models.ActiveSubscription
	require.NoError(t, bson.Unmarshal(b, &row))
	assert.Equal(t, sub.RoomID, row.RoomID)
	assert.Equal(t, sub.SiteID, row.SiteID)
	require.NotNil(t, row.LastSeenAt)
	assert.Equal(t, seen.UTC(), row.LastSeenAt.UTC())
	assert.Equal(t, sub.ThreadUnread, row.ThreadUnread)
	assert.Nil(t, row.LastMsgAt, "lastMsgAt is added by the rooms join, never stored on the subscription")
}
