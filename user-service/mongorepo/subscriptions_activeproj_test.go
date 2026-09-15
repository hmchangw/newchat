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

// bsonTagKeys returns a flat struct's stored bson field names, sorted, skipping
// bson:"-". An untagged field fails outright — it would decode as a silent zero.
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

// $project and the struct must name the same fields, or a missing one decodes as a silent zero.
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

// Coarse size-direction check; the raw-BSON integration guard proves the no-leak claim.
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

// Proves the tags name real stored fields, not just each other.
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

// lookupSubPipeline returns the sub-pipeline of the single $lookup in pipeline.
func lookupSubPipeline(t *testing.T, pipeline bson.A) bson.A {
	t.Helper()
	for _, stage := range pipeline {
		m, ok := stage.(bson.M)
		if !ok {
			continue
		}
		spec, ok := m["$lookup"].(bson.M)
		if !ok {
			continue
		}
		sub, ok := spec["pipeline"].(bson.A)
		require.True(t, ok, "$lookup must use a sub-pipeline, not a plain foreignField join")
		return sub
	}
	t.Fatal("no $lookup stage in pipeline")
	return nil
}

// projectedKeys returns the inclusion keys of the first $project in pipeline, sorted.
func projectedKeys(t *testing.T, pipeline bson.A) []string {
	t.Helper()
	for _, stage := range pipeline {
		m, ok := stage.(bson.M)
		if !ok {
			continue
		}
		proj, ok := m["$project"].(bson.M)
		if !ok {
			continue
		}
		keys := []string{}
		for k, v := range proj {
			if k == "_id" && v == 0 {
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return keys
	}
	t.Fatal("no $project stage in pipeline")
	return nil
}

// The badge pipeline keeps exactly one room field (lastMsgAt), so its rooms join
// must fetch exactly that. Reusing the list path's 11-field enrichment
// materializes ten fields per joined room — including the encKey blob — that the
// terminal $project then discards, once per account in a notification batch.
func TestActiveSubscriptionPipeline_JoinsOnlyLastMsgAt(t *testing.T) {
	r := &SubscriptionRepo{}

	sub := lookupSubPipeline(t, r.activeSubscriptionPipeline("alice", 100))

	assert.Equal(t, []string{"lastMsgAt"}, projectedKeys(t, sub),
		"the badge join must project only the room field the terminal $project keeps")
}

// The join must not carry the E2E key into the working set: it is the largest
// field in the enrichment set and the badge count never reads it.
func TestActiveSubscriptionPipeline_JoinOmitsRoomKey(t *testing.T) {
	r := &SubscriptionRepo{}

	sub := lookupSubPipeline(t, r.activeSubscriptionPipeline("alice", 100))

	for _, k := range projectedKeys(t, sub) {
		assert.NotContains(t, k, "encKey", "the badge join must not fetch the room E2E key")
	}
}
