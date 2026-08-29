package mongorepo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// The gate drops threads in apps the user unsubscribed from, but must not drop
// a bot's threads in its own DMs with humans (those rows carry isSubscribed=false).
func TestThreadRoomGate_KeepsNonAppBotDMs(t *testing.T) {
	branches, ok := threadRoomGate()["$or"].(bson.A)
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
