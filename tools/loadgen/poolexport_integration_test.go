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

// A site with nobody must come back empty rather than erroring, so the caller
// can report the population it found — exportPool is what turns that into the
// loud failure.
func TestIntegration_MongoPoolSource_EmptySite(t *testing.T) {
	db := testutil.MongoDB(t, "poolexport-empty")
	got, err := mongoPoolSource{db: db}.channelSubscriberAccounts(context.Background(), "site-none")
	require.NoError(t, err)
	assert.Empty(t, got)
}
