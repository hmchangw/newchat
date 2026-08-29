package pipelines

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

// The whole map, not a structural walk: equality proves the predicates are
// present AND that nothing else narrows the filter.
func TestAppRoomFilter(t *testing.T) {
	assert.Equal(t, bson.M{
		"roomType": "botDM",
		"name":     bson.Regex{Pattern: botSuffixRegex},
	}, AppRoomFilter())
}

func TestUnsubscribedAppFilter(t *testing.T) {
	assert.Equal(t, bson.M{
		"roomType":     "botDM",
		"name":         bson.Regex{Pattern: botSuffixRegex},
		"isSubscribed": bson.M{"$ne": true},
	}, UnsubscribedAppFilter(),
		"$ne:true, so a legacy row with no isSubscribed field counts as unsubscribed")
}

// Callers mutate the returned map (UnsubscribedAppFilter adds a key to it), so
// each call must hand back a fresh one.
func TestAppRoomFilterReturnsFreshMaps(t *testing.T) {
	first := AppRoomFilter()
	first["isSubscribed"] = true
	assert.NotContains(t, AppRoomFilter(), "isSubscribed",
		"a caller's mutation must not leak into the next call")
}

// The regex is the wire-side twin of model.IsBot, so assert against IsBot
// itself — hardcoding a second truth table here would let the two drift, which
// is the exact failure this test exists to catch.
func TestBotSuffixRegexMatchesIsBot(t *testing.T) {
	re := regexp.MustCompile(botSuffixRegex)
	for _, account := range []string{
		"weather.bot", "weather.site-a.bot", "alice", "p_admin_ops",
		"p_qa_bob", "bot", "robot", "a.bot.b", "weather.BOT", "",
	} {
		t.Run(account, func(t *testing.T) {
			assert.Equal(t, model.IsBot(account), re.MatchString(account))
		})
	}
}

// The sibling regex must keep matching ".bot" now that it composes the shared
// literal, and must still additionally match the platform-admin prefix.
func TestBotOrPseudoAccountRegexComposesBotSuffix(t *testing.T) {
	re := regexp.MustCompile(botOrPseudoAccountRegex())
	assert.True(t, re.MatchString("weather.bot"))
	assert.True(t, re.MatchString(model.PlatformAdminAccountPrefix()+"ops"))
	assert.False(t, re.MatchString("alice"))
	assert.False(t, re.MatchString("p_qa_bob"))
}
