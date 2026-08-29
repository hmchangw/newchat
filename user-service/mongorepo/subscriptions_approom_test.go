package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// hasNonAppBranch reports whether an $or branch list admits non-app botDM rows
// (the bot's own side of a human DM, and p_admin DMs) with no isSubscribed gate.
func hasNonAppBranch(t *testing.T, branches bson.A) bool {
	t.Helper()
	for _, b := range branches {
		m, ok := b.(bson.M)
		if !ok || m["roomType"] != "botDM" {
			continue
		}
		name, ok := m["name"].(bson.M)
		if !ok {
			continue
		}
		if _, negated := name["$not"]; negated {
			_, gated := m["isSubscribed"]
			assert.False(t, gated, "non-app botDM rows must not be gated on isSubscribed")
			return true
		}
	}
	return false
}

func TestListTypeMatch_AdmitsNonAppBotDMs(t *testing.T) {
	for _, listType := range []string{"current", "rooms"} {
		t.Run(listType, func(t *testing.T) {
			m := listTypeMatch(listType)
			branches, ok := m["$or"].(bson.A)
			require.True(t, ok, "%s must use an $or over room-type branches", listType)
			assert.True(t, hasNonAppBranch(t, branches),
				"%s must admit the bot's own botDM rows, which carry isSubscribed=false", listType)
		})
	}
}

// The App section must keep holding only real apps: an unsubscribed .bot app
// stays hidden, and a bot's human DM never appears there.
func TestListTypeMatch_AppsKeepsSubscribedGate(t *testing.T) {
	m := listTypeMatch("apps")
	assert.Equal(t, true, m["isSubscribed"], "apps must still require isSubscribed")
	assert.Equal(t, "botDM", m["roomType"])
	name, ok := m["name"].(bson.M)
	require.True(t, ok, "apps must constrain the counterpart name")
	assert.Equal(t, `\.bot$`, name["$regex"], "apps admit only .bot counterparts")
}

// The badge count and the list must select identical rows, or a client folding
// its badge from the list can never reconcile with the server's count.
func TestActiveSubscriptionFilter_MatchesCurrentBucket(t *testing.T) {
	branches, ok := activeSubscriptionFilter("alice")["$or"].(bson.A)
	require.True(t, ok)
	assert.True(t, hasNonAppBranch(t, branches),
		"the badge filter must admit the same non-app botDM rows the list does")
}
