package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

// The count no longer excludes soft-deleted (^Del-) rooms: the deployment
// guarantees no Del- rename ever reaches the database, so the rooms $lookup that
// existed only to read room.name is gone. This test pins that as a deliberate
// choice — if a Del- filter is ever reintroduced it must come back as a
// subscription-local predicate, never as a join.
func TestActiveSubscriptionFilter_DoesNotFilterRoomNames(t *testing.T) {
	f := activeSubscriptionFilter("alice")

	assert.NotContains(t, f, "name")
	assert.NotContains(t, f, "roomName")
}

func TestActiveSubscriptionFilter_KeepsTheActiveSetPredicates(t *testing.T) {
	f := activeSubscriptionFilter("alice")

	assert.Equal(t, "alice", f["u.account"])
	assert.Equal(t, bson.M{"$ne": true}, f["muted"])
	assert.Equal(t, bson.M{"$ne": false}, f["open"])
	assert.Contains(t, f, "$or")
}

func TestCountActiveFilter_Composition(t *testing.T) {
	tests := []struct {
		name          string
		repo          *SubscriptionRepo
		account       string
		wantOriginKey bool
	}{
		{"default hides Teams rooms", &SubscriptionRepo{}, "alice", true},
		{"showTeamsRoom drops the origin predicate", &SubscriptionRepo{showTeamsRoom: true}, "alice", false},
		{
			"allowlisted account drops the origin predicate",
			&SubscriptionRepo{showTeamsAccounts: map[string]bool{"alice": true}}, "alice", false,
		},
		{
			"non-allowlisted account keeps the origin predicate",
			&SubscriptionRepo{showTeamsAccounts: map[string]bool{"bob": true}}, "alice", true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.repo.countActiveFilter(tc.account)

			assert.Equal(t, tc.account, f["u.account"])
			assert.Equal(t, bson.M{"$ne": true}, f["muted"])
			assert.Equal(t, bson.M{"$ne": false}, f["open"])
			assert.Contains(t, f, "$or", "roomType/isSubscribed selection must survive the merge")

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
