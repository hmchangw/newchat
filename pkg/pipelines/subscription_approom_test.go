package pipelines

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// The two fragments must partition botDM rows exactly as model.IsBot does, so a
// query and the Go render path can never disagree about which rows are apps.
func TestAppRoomFilterShape(t *testing.T) {
	app := AppRoomFilter()
	assert.Equal(t, "botDM", app["roomType"], "app rooms are botDM rows")

	nonApp := NonAppRoomFilter()
	assert.Equal(t, "botDM", nonApp["roomType"], "non-app rooms are botDM rows too")

	name, ok := nonApp["name"].(bson.M)
	require.True(t, ok, "the name clause must be a bson.M")
	_, negated := name["$not"]
	assert.True(t, negated, "NonAppRoomFilter must negate the bot-suffix match")
}

// Callers mutate the returned map (the list bucket adds isSubscribed), so each
// call must hand back a fresh one.
func TestAppRoomFilterReturnsFreshMaps(t *testing.T) {
	first := AppRoomFilter()
	first["isSubscribed"] = true
	assert.NotContains(t, AppRoomFilter(), "isSubscribed",
		"a caller's mutation must not leak into the next call")
}

// The regex the fragments embed must agree with model.IsBot on every account
// shape the system produces.
func TestAppRoomFilterRegexMatchesIsBot(t *testing.T) {
	re := regexp.MustCompile(botSuffixRegex())
	for _, tc := range []struct {
		account string
		isBot   bool
	}{
		{"weather.bot", true},
		{"weather.site-a.bot", true},
		{"alice", false},
		{"p_admin_ops", false},
		{"p_qa_bob", false},
		{"bot", false},
		{"robot", false},
		{"a.bot.b", false},
		{"", false},
	} {
		t.Run(tc.account, func(t *testing.T) {
			assert.Equal(t, tc.isBot, re.MatchString(tc.account))
		})
	}
}
