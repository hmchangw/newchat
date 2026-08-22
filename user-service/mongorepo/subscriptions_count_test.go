package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

// The count's deleted-room filter reads the subscription's OWN denormalized
// name, so CountActiveSubscriptions needs no rooms $lookup. subscriptions.name
// is kept in step with the room name by UpdateSubscriptionNamesForRoom
// (room-worker for in-stack renames, inbox-worker for oplog/cross-site ones).
func TestActiveSubscriptionFilter_ExcludesDeletedRoomNames(t *testing.T) {
	f := activeSubscriptionFilter("alice")

	got, ok := f["name"]
	require.True(t, ok, "filter must constrain `name`")
	assert.Equal(t, bson.M{"$not": bson.Regex{Pattern: deletedRoomNameRegex}}, got,
		"$not with a regex literal also matches docs with no name field, so a nameless sub still counts")
}

func TestCountActiveFilter_Composition(t *testing.T) {
	tests := []struct {
		name          string
		repo          *SubscriptionRepo
		account       string
		wantOriginKey bool
	}{
		{
			name:          "default hides Teams rooms",
			repo:          &SubscriptionRepo{},
			account:       "alice",
			wantOriginKey: true,
		},
		{
			name:          "showTeamsRoom drops the origin predicate",
			repo:          &SubscriptionRepo{showTeamsRoom: true},
			account:       "alice",
			wantOriginKey: false,
		},
		{
			name:          "allowlisted account drops the origin predicate",
			repo:          &SubscriptionRepo{showTeamsAccounts: map[string]bool{"alice": true}},
			account:       "alice",
			wantOriginKey: false,
		},
		{
			name:          "non-allowlisted account keeps the origin predicate",
			repo:          &SubscriptionRepo{showTeamsAccounts: map[string]bool{"bob": true}},
			account:       "alice",
			wantOriginKey: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.repo.countActiveFilter(tc.account)

			assert.Equal(t, tc.account, f["u.account"])
			assert.Equal(t, bson.M{"$ne": true}, f["muted"])
			assert.Equal(t, bson.M{"$ne": false}, f["open"])
			assert.Contains(t, f, "$or", "roomType/isSubscribed selection must survive the merge")
			assert.Contains(t, f, "name", "deleted-room filter must survive the merge")

			if tc.wantOriginKey {
				assert.Equal(t, bson.M{"$ne": model.OriginTeams}, f["origin"])
			} else {
				assert.NotContains(t, f, "origin")
			}
		})
	}
}

// The filter must be a plain find filter — no aggregation stages — so the count
// runs as CountDocuments rather than a pipeline with a rooms join.
func TestCountActiveFilter_IsAPlainFindFilter(t *testing.T) {
	f := (&SubscriptionRepo{}).countActiveFilter("alice")

	for key := range f {
		assert.NotContains(t, key, "$lookup")
		assert.NotContains(t, key, "$match")
		assert.NotContains(t, key, "$unwind")
	}
}

// countActiveFilter must not alias the shared activeSubscriptionFilter map:
// mutating one call's result would otherwise corrupt the next.
func TestCountActiveFilter_DoesNotMutateSharedFilter(t *testing.T) {
	repo := &SubscriptionRepo{}

	first := repo.countActiveFilter("alice")
	first["injected"] = true

	second := repo.countActiveFilter("alice")
	assert.NotContains(t, second, "injected")
	assert.NotContains(t, activeSubscriptionFilter("alice"), "injected")
}
