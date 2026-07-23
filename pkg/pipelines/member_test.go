package pipelines

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

// TestBotOrPseudoAccountRegex pins the wire-side residual filter to the model
// taxonomy: it must exclude ".bot" bots and the "p_tchatadmin_" platform-admin
// pseudo-account, but ADMIT plain "p_" QA test accounts (ordinary users now).
func TestBotOrPseudoAccountRegex(t *testing.T) {
	assert.Equal(t, `(\.bot$|^p_tchatadmin_)`, botOrPseudoAccountRegex())
	rx := regexp.MustCompile(botOrPseudoAccountRegex())
	cases := []struct {
		account string
		match   bool
	}{
		{"weather.bot", true},
		{"p_tchatadmin_siteA", true},
		{"p_tchatadmin_", true},
		{"p_qa1", false},
		{"p_webhook", false},
		{"p_", false},
		{"alice", false},
	}
	for _, c := range cases {
		assert.Equalf(t, c.match, rx.MatchString(c.account), "account %q", c.account)
	}
}

// TestBotOrPseudoAccountRegex_HonorsOverriddenPrefix verifies the wire-side
// filter tracks a deployment override of the platform-admin prefix (ADMIN_ACCT_PREFIX)
// so it stays the mirror of model.IsPlatformAdminAccount. Restores the default
// via t.Cleanup; not parallel (mutates process-global model state).
func TestBotOrPseudoAccountRegex_HonorsOverriddenPrefix(t *testing.T) {
	orig := model.PlatformAdminAccountPrefix()
	t.Cleanup(func() { require.NoError(t, model.SetPlatformAdminAccountPrefix(orig)) })

	require.NoError(t, model.SetPlatformAdminAccountPrefix("admin_"))
	assert.Equal(t, `(\.bot$|^admin_)`, botOrPseudoAccountRegex())
	rx := regexp.MustCompile(botOrPseudoAccountRegex())
	assert.True(t, rx.MatchString("admin_siteA"), "overridden-prefix admin must match")
	assert.True(t, rx.MatchString("weather.bot"), "bots still match")
	assert.False(t, rx.MatchString("p_tchatadmin_siteA"), "old default prefix no longer matches")
}

// TestBotOrPseudoAccountRegex_QuoteMetaEscapesPrefix verifies a prefix carrying
// regex metacharacters is QuoteMeta-escaped so it is matched literally rather
// than being interpreted as a pattern.
func TestBotOrPseudoAccountRegex_QuoteMetaEscapesPrefix(t *testing.T) {
	orig := model.PlatformAdminAccountPrefix()
	t.Cleanup(func() { require.NoError(t, model.SetPlatformAdminAccountPrefix(orig)) })

	require.NoError(t, model.SetPlatformAdminAccountPrefix("p.admin["))
	assert.Equal(t, `(\.bot$|^p\.admin\[)`, botOrPseudoAccountRegex())
	rx := regexp.MustCompile(botOrPseudoAccountRegex())
	assert.True(t, rx.MatchString("p.admin[siteA"), "literal metachar prefix must match")
	assert.False(t, rx.MatchString("pXadmin[siteA"), "'.' must not be treated as a wildcard")
}

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
		assert.Equal(t, botOrPseudoAccountRegex(), rx.Pattern)
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

func TestMatchCandidatesFilterWithDirectBots(t *testing.T) {
	botNot := bson.M{"$not": bson.Regex{Pattern: botOrPseudoAccountRegex(), Options: ""}}

	t.Run("org arms keep the bot exclusion, direct arm does not", func(t *testing.T) {
		f := MatchCandidatesFilterWithDirectBots([]string{"org1"}, []string{"alice", "weather.bot"}, "")
		or, ok := f["$or"].(bson.A)
		require.True(t, ok, "$or must be a bson.A")
		assert.Equal(t, bson.A{
			bson.M{"sectId": bson.M{"$in": []string{"org1"}}, "account": botNot},
			bson.M{"deptId": bson.M{"$in": []string{"org1"}}, "account": botNot},
			bson.M{"account": bson.M{"$in": []string{"alice", "weather.bot"}}},
		}, or)
		_, hasTopAccount := f["account"]
		assert.False(t, hasTopAccount, "no top-level account filter without excludeAccount")
	})

	t.Run("accounts only produces a single unfiltered arm", func(t *testing.T) {
		f := MatchCandidatesFilterWithDirectBots(nil, []string{"helper.bot"}, "")
		assert.Equal(t, bson.A{bson.M{"account": bson.M{"$in": []string{"helper.bot"}}}}, f["$or"])
	})

	t.Run("excludeAccount adds a top-level $ne only", func(t *testing.T) {
		f := MatchCandidatesFilterWithDirectBots([]string{"org1"}, nil, "exclude-me")
		acct, ok := f["account"].(bson.M)
		require.True(t, ok)
		assert.Equal(t, bson.M{"$ne": "exclude-me"}, acct)
	})
}
