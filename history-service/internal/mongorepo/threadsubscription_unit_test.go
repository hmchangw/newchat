package mongorepo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// countStages returns how many pipeline stages carry the given operator key.
func countStages(t *testing.T, pipeline bson.A, op string) int {
	t.Helper()
	n := 0
	for _, raw := range pipeline {
		stage, ok := raw.(bson.D)
		if !ok {
			continue
		}
		for _, e := range stage {
			if e.Key == op {
				n++
			}
		}
	}
	return n
}

func TestUserThreadSubscriptionsPipeline_FirstPageHasNoCursorMatch(t *testing.T) {
	p := userThreadSubscriptionsPipeline("alice", nil, "", 20)
	// First page: userAccount $match + membership $match (sub != []), no value-cursor $match.
	assert.Equal(t, 2, countStages(t, p, "$match"))
	// Two joins: thread_rooms (sort/cursor) and subscriptions (membership). roomName
	// and roomType both ride in on the membership join, so there is no rooms lookup.
	assert.Equal(t, 2, countStages(t, p, "$lookup"))
	// Only thread_rooms is unwound; the membership join uses {$ne: []}, not $unwind.
	assert.Equal(t, 1, countStages(t, p, "$unwind"))
	assert.Equal(t, 1, countStages(t, p, "$sort"))
	// Only the outer page $limit is top-level; the membership join's inner $limit:1
	// is nested inside the $lookup pipeline and not counted here.
	assert.Equal(t, 1, countStages(t, p, "$limit"))
}

func TestUserThreadSubscriptionsPipeline_NextPageAddsCursorMatch(t *testing.T) {
	ts := time.Date(2026, 1, 1, 5, 0, 0, 0, time.UTC)
	p := userThreadSubscriptionsPipeline("alice", &ts, "thr-9", 20)
	// Next page: userAccount $match + value-cursor $match + membership $match.
	assert.Equal(t, 3, countStages(t, p, "$match"))
}

// A bot's own subscription row carries isSubscribed=false, so gating every
// botDM on it hides the bot's threads in its DMs with humans. Only real apps —
// botDM rows facing a ".bot" counterpart — keep the soft-unsubscribe gate.
func TestThreadMembershipGate_KeepsNonAppBotDMs(t *testing.T) {
	branches, ok := threadMembershipGate()["$or"].(bson.A)
	require.True(t, ok, "the gate must be an $or")
	require.Len(t, branches, 2)

	notApp, ok := branches[0].(bson.M)
	require.True(t, ok)
	nor, ok := notApp["$nor"].(bson.A)
	require.True(t, ok, "the first branch must exclude app rooms via $nor")
	require.Len(t, nor, 1)

	app, ok := nor[0].(bson.M)
	require.True(t, ok)
	assert.Equal(t, "botDM", app["roomType"])
	name, ok := app["name"].(bson.M)
	require.True(t, ok)
	assert.Equal(t, `\.bot$`, name["$regex"])

	subscribed, ok := branches[1].(bson.M)
	require.True(t, ok)
	assert.Equal(t, true, subscribed["isSubscribed"],
		"an app room still has to be subscribed to contribute threads")
}
