package pipelines

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestGetNewMembersPipeline(t *testing.T) {
	t.Run("three stages returned", func(t *testing.T) {
		got := GetNewMembersPipeline([]string{"org1"}, []string{"alice"}, "room1")
		assert.Len(t, got, 3)
	})

	t.Run("$or filter with both orgIDs and directAccounts", func(t *testing.T) {
		got := GetNewMembersPipeline([]string{"org1", "org2"}, []string{"alice"}, "room1")
		stage0 := got[0].(bson.M)
		match := stage0["$match"].(bson.M)
		orFilter := match["$or"].(bson.A)

		assert.Len(t, orFilter, 2)
		assert.NotNil(t, orFilter[0])
		assert.NotNil(t, orFilter[1])
	})

	t.Run("bot exclusion via $not regex", func(t *testing.T) {
		got := GetNewMembersPipeline([]string{"org1"}, []string{"alice"}, "room1")
		stage0 := got[0].(bson.M)
		match := stage0["$match"].(bson.M)

		notFilter := match["account"]
		assert.NotNil(t, notFilter)
	})

	t.Run("$or filter contains orgIDs when provided", func(t *testing.T) {
		got := GetNewMembersPipeline([]string{"org1"}, nil, "room1")
		stage0 := got[0].(bson.M)
		match := stage0["$match"].(bson.M)
		orFilter := match["$or"].(bson.A)

		assert.Len(t, orFilter, 1)
		sectIdFilter := orFilter[0].(bson.M)
		assert.Contains(t, sectIdFilter, "sectId")
	})

	t.Run("$or filter contains directAccounts when provided", func(t *testing.T) {
		got := GetNewMembersPipeline(nil, []string{"alice"}, "room1")
		stage0 := got[0].(bson.M)
		match := stage0["$match"].(bson.M)
		orFilter := match["$or"].(bson.A)

		assert.Len(t, orFilter, 1)
		accountFilter := orFilter[0].(bson.M)
		assert.Contains(t, accountFilter, "account")
	})

	t.Run("$lookup stage correct", func(t *testing.T) {
		got := GetNewMembersPipeline([]string{"org1"}, []string{"alice"}, "room1")
		stage1 := got[1].(bson.M)
		lookup := stage1["$lookup"].(bson.M)

		assert.Equal(t, "subscriptions", lookup["from"])
		assert.Equal(t, "existingSub", lookup["as"])
		assert.NotNil(t, lookup["let"])
		assert.NotNil(t, lookup["pipeline"])
	})

	t.Run("$match stage filters empty existingSub array", func(t *testing.T) {
		got := GetNewMembersPipeline([]string{"org1"}, []string{"alice"}, "room1")
		stage2 := got[2].(bson.M)
		match := stage2["$match"].(bson.M)
		existingSub := match["existingSub"].(bson.M)
		eqA := existingSub["$eq"].(bson.A)
		assert.Len(t, eqA, 0, "compare against the empty array literal")
	})
}
