//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
)

// The unit tests run against a fake source, so the query itself — the filter
// mirroring user-service's own subscription.list match, and the sort that
// clientsim's sharding depends on — is only ever exercised here.
func TestIntegration_MongoPoolSource_SelectsChannelSubscribers(t *testing.T) {
	db := testutil.MongoDB(t, "poolexport")
	ctx := context.Background()

	docs := []any{
		// Wanted: channel subscriptions on the site under test.
		bson.M{"_id": "s1", "siteId": "site-a", "roomType": "channel", "u": bson.M{"account": "carol"}},
		bson.M{"_id": "s2", "siteId": "site-a", "roomType": "channel", "u": bson.M{"account": "anna"}},
		// Same account, second room: the account must appear once.
		bson.M{"_id": "s3", "siteId": "site-a", "roomType": "channel", "u": bson.M{"account": "anna"}},
		// open:true is still open — only an explicit false is excluded.
		bson.M{"_id": "s4", "siteId": "site-a", "roomType": "channel", "open": true, "u": bson.M{"account": "bob"}},

		// Unwanted, each for its own reason.
		bson.M{"_id": "x1", "siteId": "site-b", "roomType": "channel", "u": bson.M{"account": "otherSite"}},
		bson.M{"_id": "x2", "siteId": "site-a", "roomType": "dm", "u": bson.M{"account": "dmOnly"}},
		bson.M{"_id": "x3", "siteId": "site-a", "roomType": "botDM", "u": bson.M{"account": "botOnly"}},
		bson.M{"_id": "x4", "siteId": "site-a", "roomType": "channel", "open": false, "u": bson.M{"account": "closed"}},
		bson.M{"_id": "x5", "siteId": "site-a", "roomType": "channel", "u": bson.M{"account": ""}},
		// Teams-origin: hidden from subscription.list under the default
		// SHOW_TEAMS_ROOM=false, so this account would walk to an empty plan
		// and report ready while measuring nothing.
		bson.M{"_id": "x6", "siteId": "site-a", "roomType": "channel",
			"origin": "teams", "u": bson.M{"account": "teamsOnly"}},
		// A bot that owns a room holds a real channel subscription
		// (bot-room-service/handler.go:213-216 writes IsBot:true with
		// RoomTypeChannel, and its $setOnInsert never sets `open`, so the
		// open filter passes it through). clientsim cannot connect as one:
		// bots authenticate over HTTP through pkg/botauth, not the user JWT
		// path, and a dotted ".bot" account spans subject tokens — it panics
		// subject.UserSubscriptionList before any request is made.
		bson.M{"_id": "x7", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "weather.site-a.bot", "isBot": true}},
		// isBot without the suffix, and the suffix without the flag: either
		// alone disqualifies, so neither filter carries the rule by itself.
		bson.M{"_id": "x8", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "p_admin", "isBot": true}},
		bson.M{"_id": "x9", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "legacy.site-a.bot"}},
	}
	_, err := db.Collection("subscriptions").InsertMany(ctx, docs)
	require.NoError(t, err)

	got, err := mongoPoolSource{db: db}.channelSubscriberAccounts(ctx, "site-a")
	require.NoError(t, err)

	// Sorted and de-duplicated. The order is the contract shardSlice relies
	// on: every pod slices the same array, so an unstable one would overlap
	// or skip accounts across pods.
	assert.Equal(t, []string{"anna", "bob", "carol"}, got)
}

// An account with BOTH a Teams room and an ordinary channel is still a valid
// pool member: it has something to walk. Only Teams-only accounts drop out.
func TestIntegration_MongoPoolSource_KeepsAccountsWithANonTeamsChannel(t *testing.T) {
	db := testutil.MongoDB(t, "poolexport-mixed")
	ctx := context.Background()
	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "m1", "siteId": "site-a", "roomType": "channel",
			"origin": "teams", "u": bson.M{"account": "mixed"}},
		bson.M{"_id": "m2", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "mixed"}},
	})
	require.NoError(t, err)

	got, err := mongoPoolSource{db: db}.channelSubscriberAccounts(ctx, "site-a")
	require.NoError(t, err)
	assert.Equal(t, []string{"mixed"}, got)
}

// A site with nobody must come back empty rather than erroring, so the caller
// can report the population it found — exportPool is what turns that into the
// loud failure.
func TestIntegration_MongoPoolSource_EmptySite(t *testing.T) {
	db := testutil.MongoDB(t, "poolexport-empty")
	got, err := mongoPoolSource{db: db}.channelSubscriberAccounts(context.Background(), "site-none")
	require.NoError(t, err)
	assert.Empty(t, got)
}
