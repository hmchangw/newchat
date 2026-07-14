package pipelines

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMatchCandidatesFilter(t *testing.T) {
	t.Run("orgs and accounts produce three $or branches", func(t *testing.T) {
		f := MatchCandidatesFilter([]string{"org1", "org2"}, []string{"alice", "bob"}, "")
		or, ok := f["$or"].(bson.A)
		require.True(t, ok, "$or must be a bson.A")
		assert.Equal(t, bson.A{
			bson.M{"sectId": bson.M{"$in": []string{"org1", "org2"}}},
			bson.M{"deptId": bson.M{"$in": []string{"org1", "org2"}}},
			bson.M{"account": bson.M{"$in": []string{"alice", "bob"}}},
		}, or)
	})

	t.Run("orgs only omits the account branch", func(t *testing.T) {
		f := MatchCandidatesFilter([]string{"org1"}, nil, "")
		assert.Len(t, f["$or"], 2)
	})

	t.Run("accounts only omits the org branches", func(t *testing.T) {
		f := MatchCandidatesFilter(nil, []string{"alice"}, "")
		assert.Equal(t, bson.A{bson.M{"account": bson.M{"$in": []string{"alice"}}}}, f["$or"])
	})

	t.Run("always excludes bots via a $not regex on account", func(t *testing.T) {
		f := MatchCandidatesFilter(nil, []string{"alice"}, "")
		acct, ok := f["account"].(bson.M)
		require.True(t, ok)
		rx, ok := acct["$not"].(bson.Regex)
		require.True(t, ok, "account.$not must be a regex")
		assert.Equal(t, botOrPseudoAccountRegex, rx.Pattern)
		_, hasNe := acct["$ne"]
		assert.False(t, hasNe, "no $ne when excludeAccount is empty")
	})

	t.Run("excludeAccount adds a $ne on account", func(t *testing.T) {
		f := MatchCandidatesFilter([]string{"org1"}, nil, "exclude-me")
		acct := f["account"].(bson.M)
		assert.Equal(t, "exclude-me", acct["$ne"])
		_, hasNot := acct["$not"]
		assert.True(t, hasNot, "bot filter is retained alongside $ne")
	})
}
