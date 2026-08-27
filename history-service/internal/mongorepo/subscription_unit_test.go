package mongorepo

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// subscriptionBSONFields returns the top-level bson field names on
// model.Subscription, so a projection can be checked against the real schema
// rather than against a hand-copied list.
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

// TestSubscriptionReadProjection_NamesRealFields catches the silent failure
// mode of a projection typo: Mongo does not reject an unknown field name, it
// just returns nothing for it, so "role" instead of "roles" would strip the
// pin-bypass roles with no error anywhere.
func TestSubscriptionReadProjection_NamesRealFields(t *testing.T) {
	schema := subscriptionBSONFields(t)
	require.NotEmpty(t, schema, "reflection over model.Subscription found no bson tags")

	for field := range subscriptionReadProjection {
		if field == "_id" {
			continue // _id is implicit on every document, not a struct-tag field
		}
		assert.True(t, schema[field],
			"projection names %q, which is not a bson field on model.Subscription", field)
	}
}

// TestSubscriptionReadProjection_CoversCallSiteReads pins the projection to the
// fields GetSubscription's callers actually read. Dropping one of these returns
// a zero value with no error — canBypassLargeRoomPin would silently stop
// honoring owner/admin/bot bypass, and PinnedBy would carry an empty user.
func TestSubscriptionReadProjection_CoversCallSiteReads(t *testing.T) {
	// Read at internal/service/pin.go: canBypassLargeRoomPin (sub.Roles,
	// sub.User.Account) and the PinnedBy participant (sub.User.ID,
	// sub.User.Account).
	for _, field := range []string{"u", "roles"} {
		assert.Equal(t, 1, subscriptionReadProjection[field],
			"call sites read %q, so it must be included in the projection", field)
	}
}

// TestSubscriptionReadProjection_ExcludesUnreadFields keeps the projection from
// drifting back toward the whole document. model.Subscription carries ~30
// fields; these are the expensive ones no call site reads.
func TestSubscriptionReadProjection_ExcludesUnreadFields(t *testing.T) {
	for _, field := range []string{"threadUnread", "name", "roomType", "historySharedSince", "lastSeenAt", "joinedAt"} {
		_, present := subscriptionReadProjection[field]
		assert.False(t, present,
			"no call site reads %q; leave it out so the pin hot path does not decode it", field)
	}

	// _id must be suppressed explicitly: an inclusion projection returns it by
	// default, and no call site reads sub.ID.
	assert.Equal(t, 0, subscriptionReadProjection["_id"], "_id must be explicitly excluded")
}

// TestSubscriptionReadProjection_IsInclusionProjection guards the shape itself:
// mixing 0s and 1s (beyond the _id special case) is a Mongo error at query
// time, which would break pin/unpin at runtime rather than in CI.
func TestSubscriptionReadProjection_IsInclusionProjection(t *testing.T) {
	for field, value := range subscriptionReadProjection {
		if field == "_id" {
			continue
		}
		assert.Equal(t, 1, value,
			"field %q must be an inclusion (1); mixing exclusions into an inclusion projection is a query-time error", field)
	}
}
