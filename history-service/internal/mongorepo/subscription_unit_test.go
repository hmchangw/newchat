package mongorepo

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// subscriptionBSONFields reads model.Subscription's bson field names off the struct tags.
func subscriptionBSONFields(t *testing.T) map[string]bool {
	t.Helper()
	fields := map[string]bool{}
	rt := reflect.TypeOf(model.Subscription{})
	for i := range rt.NumField() {
		tag := rt.Field(i).Tag.Get("bson")
		if tag == "" || tag == "-" {
			continue
		}
		fields[strings.Split(tag, ",")[0]] = true
	}
	return fields
}

// Mongo ignores an unknown projection field, so "role" for "roles" would silently strip pin-bypass.
func TestSubscriptionReadProjection_NamesRealFields(t *testing.T) {
	schema := subscriptionBSONFields(t)
	require.NotEmpty(t, schema, "reflection over model.Subscription found no bson tags")

	for field := range subscriptionReadProjection {
		assert.True(t, schema[field],
			"projection names %q, which is not a bson field on model.Subscription", field)
	}
}

// Dropping either yields a zero value, not an error — pin-bypass would stop honoring owners.
func TestSubscriptionReadProjection_CoversCallSiteReads(t *testing.T) {
	// Read by canBypassLargeRoomPin and the PinnedBy participant in service/pin.go.
	for _, field := range []string{"u", "roles"} {
		assert.Equal(t, 1, subscriptionReadProjection[field],
			"call sites read %q, so it must be included in the projection", field)
	}
}

// knownUnreadSubscriptionFields is exhaustive rather than sampled: a field added to
// model.Subscription then fails PartitionsTheSchema until someone classifies it.
var knownUnreadSubscriptionFields = []string{
	"roomId", "siteId", "name", "roomType", "isSubscribed", "historySharedSince",
	"joinedAt", "lastSeenAt", "hasMention", "threadUnread", "alert", "muted",
	"favorite", "sectionId", "sectionOrder", "open", "restricted", "externalAccess",
	"favoriteUpdatedAt", "muteUpdatedAt", "rolesUpdatedAt", "nameUpdatedAt",
	"restrictUpdatedAt", "sectionUpdatedAt", "origin",
}

func TestSubscriptionReadProjection_PartitionsTheSchema(t *testing.T) {
	unread := map[string]bool{}
	for _, f := range knownUnreadSubscriptionFields {
		unread[f] = true
	}

	for field := range subscriptionBSONFields(t) {
		_, projected := subscriptionReadProjection[field]
		assert.True(t, projected || unread[field],
			"model.Subscription field %q is neither projected nor listed in "+
				"knownUnreadSubscriptionFields; if a call site now reads it, add it to "+
				"the projection, otherwise list it as unread", field)
	}

	for _, field := range knownUnreadSubscriptionFields {
		_, present := subscriptionReadProjection[field]
		assert.False(t, present,
			"no call site reads %q; leave it out so the pin hot path does not decode it", field)
	}

	assert.Equal(t, 0, subscriptionReadProjection["_id"], "_id must be explicitly excluded")
}

func TestSubscriptionReadProjection_IsInclusionProjection(t *testing.T) {
	for field, value := range subscriptionReadProjection {
		if field == "_id" {
			continue
		}
		assert.Equal(t, 1, value,
			"field %q must be an inclusion (1); mixing exclusions into an inclusion projection is a query-time error", field)
	}
}
