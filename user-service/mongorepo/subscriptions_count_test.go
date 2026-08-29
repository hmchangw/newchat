package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

// activeFilter is the whole predicate behind both subscription counts, so the tests
// assert the map in full: equality proves the active-set predicates are present AND
// that nothing filters on a room name — no endpoint interprets one any more.
func TestActiveFilter(t *testing.T) {
	// The $or comes from listTypeMatch("current") rather than being re-spelled
	// here: the badge count and subscription.list MUST select the same rows, so
	// this test pins that they share one definition, not that the definition has
	// any particular shape.
	base := func() bson.M {
		return bson.M{
			"u.account": "alice",
			"muted":     bson.M{"$ne": true},
			"open":      bson.M{"$ne": false},
			"$or":       listTypeMatch("current")["$or"],
		}
	}
	withOrigin := func() bson.M {
		f := base()
		f["origin"] = bson.M{"$ne": model.OriginTeams}
		return f
	}

	tests := []struct {
		name string
		repo *SubscriptionRepo
		want bson.M
	}{
		{"default hides Teams rooms", &SubscriptionRepo{}, withOrigin()},
		{"showTeamsRoom drops the origin predicate", &SubscriptionRepo{showTeamsRoom: true}, base()},
		{
			"allowlisted account drops the origin predicate",
			&SubscriptionRepo{showTeamsAccounts: map[string]bool{"alice": true}}, base(),
		},
		{
			"non-allowlisted account keeps the origin predicate",
			&SubscriptionRepo{showTeamsAccounts: map[string]bool{"bob": true}}, withOrigin(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.repo.activeFilter("alice"))
		})
	}
}

// activeFilter must not alias the shared activeSubscriptionFilter map: mutating one
// call's result would otherwise corrupt the next.
func TestActiveFilter_DoesNotMutateSharedFilter(t *testing.T) {
	repo := &SubscriptionRepo{}

	first := repo.activeFilter("alice")
	first["injected"] = true

	second := repo.activeFilter("alice")
	assert.NotContains(t, second, "injected")
	assert.NotContains(t, activeSubscriptionFilter("alice"), "injected")
}
