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
		// The case a pre-group filter cannot see: ONE account, two rows, only
		// one carrying the flag. Filtering rows before the group drops the
		// flagged row and lets the account through on the other — and this
		// name has no ".bot" suffix, so the Go pass cannot catch it either.
		bson.M{"_id": "x10", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "mixedbot", "isBot": true}},
		bson.M{"_id": "x11", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "mixedbot"}},
	}
	_, err := db.Collection("subscriptions").InsertMany(ctx, docs)
	require.NoError(t, err)

	got, err := mongoPoolSource{db: db}.channelSubscriberAccounts(ctx, "site-a", 0)
	require.NoError(t, err)

	// Sorted and de-duplicated. The order is the contract shardSlice relies
	// on: every pod slices the same array, so an unstable one would overlap
	// or skip accounts across pods.
	//
	// The query drops x7 and x8 on the stored u.isBot flag, and mixedbot on
	// the grouped $max of it; those never leave the server and are the
	// population's definition, not skipped accounts. legacy.site-a.bot and the
	// empty account carry no flag, so the cursor walks them — and counts them.
	assert.Equal(t, []string{"anna", "bob", "carol"}, got.Accounts)
	assert.Equal(t, 2, got.Skipped)
	assert.Equal(t, []string{"legacy.site-a.bot"}, got.Sample,
		"the empty account is counted, but naming it would tell an operator nothing")

	// dropUnusable is the belt, and on this path it must find nothing:
	// composing it cannot change what the source already returned.
	kept, beltSkipped := dropUnusable(got.Accounts)
	assert.Equal(t, got.Accounts, kept)
	assert.Empty(t, beltSkipped)
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

	got, err := mongoPoolSource{db: db}.channelSubscriberAccounts(ctx, "site-a", 0)
	require.NoError(t, err)
	assert.Equal(t, []string{"mixed"}, got.Accounts)
}

// A site with nobody must come back empty rather than erroring, so the caller
// can report the population it found — exportPool is what turns that into the
// loud failure.
func TestIntegration_MongoPoolSource_EmptySite(t *testing.T) {
	db := testutil.MongoDB(t, "poolexport-empty")
	got, err := mongoPoolSource{db: db}.channelSubscriberAccounts(context.Background(), "site-none", 0)
	require.NoError(t, err)
	assert.Empty(t, got.Accounts)
	assert.Zero(t, got.Skipped)
}

// The $limit bounds whatever reaches it, so anything the Go pass would remove
// has to be excluded in the pipeline first. The empty account sorts before
// every real one, so with limit=1 a pipeline that leaves it in returns the
// empty string, the Go pass drops it, and a site full of eligible accounts
// reports none.
func TestIntegration_MongoPoolSource_LimitSkipsUnusableAccounts(t *testing.T) {
	db := testutil.MongoDB(t, "poolexportlimit")
	ctx := context.Background()

	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		// Sorts first, and is unusable.
		bson.M{"_id": "e1", "siteId": "site-a", "roomType": "channel", "u": bson.M{"account": ""}},
		// Sorts second, and is unusable for the other reason.
		bson.M{"_id": "e2", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": ".bot"}},
		bson.M{"_id": "e3", "siteId": "site-a", "roomType": "channel", "u": bson.M{"account": "zoe"}},
	})
	require.NoError(t, err)

	got, err := mongoPoolSource{db: db}.channelSubscriberAccounts(ctx, "site-a", 1)
	require.NoError(t, err)
	assert.Equal(t, []string{"zoe"}, got.Accounts,
		"limit=1 must return the first USABLE account, not the first row")
	assert.Equal(t, 2, got.Skipped, "the two it walked past are counted, not forgotten")
}

// The failure that motivated this: a staging site holds "k6.test-1.user", a
// leftover subscription row from another load tool. No isBot flag, no ".bot"
// suffix — nothing marks it as anything but a user — but its dots span subject
// tokens, so poolartifact refused the artifact and the whole export died on one
// stale row.
//
// The rule runs in Go, on the rows this cursor walks, so the same layer that
// bounds --limit is the one that counts what it passed over. A regex in the
// pipeline would filter them where nothing can count them — and would be a
// second dialect of subject.IsValidAccountToken, evaluated by another engine.
func TestIntegration_MongoPoolSource_SkipsAndCountsUnusableAccounts(t *testing.T) {
	db := testutil.MongoDB(t, "poolexporttokens")
	ctx := context.Background()

	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "k1", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "k6.test-1.user"}},
		bson.M{"_id": "k2", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "has space"}},
		bson.M{"_id": "k3", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "wild*card"}},
		bson.M{"_id": "k4", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "tail>token"}},
		// The two a Mongo-side regex would have missed: \s in a pattern is the
		// ASCII spaces, and neither engine spells out every control rune.
		// Here they are just accounts the validator refuses, like any other.
		bson.M{"_id": "k5", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "nbsp\u00a0user"}},
		bson.M{"_id": "k6", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "ctrl\x07user"}},
		// A row with no account at all. It groups under null, which the walk
		// reads as a candidate it cannot connect as — counted like the rest
		// rather than filtered where nothing could count it.
		bson.M{"_id": "k7", "siteId": "site-a", "roomType": "channel", "u": bson.M{}},
		bson.M{"_id": "k8", "siteId": "site-a", "roomType": "channel", "u": bson.M{"account": "anna"}},
		bson.M{"_id": "k9", "siteId": "site-a", "roomType": "channel", "u": bson.M{"account": "zoe"}},
	})
	require.NoError(t, err)

	got, err := mongoPoolSource{db: db}.channelSubscriberAccounts(ctx, "site-a", 0)
	require.NoError(t, err)
	assert.Equal(t, []string{"anna", "zoe"}, got.Accounts)
	assert.Equal(t, 7, got.Skipped,
		"every unusable candidate is counted — every rune class, and the row with no account at all")
	assert.Contains(t, got.Sample, "k6.test-1.user")

	// The bound counts usable accounts. All six unusable rows sort between
	// "anna" and "zoe", so a bound applied to ROWS would stop at "ctrl\x07user"
	// and hand back one account for a --limit of two.
	bounded, err := mongoPoolSource{db: db}.channelSubscriberAccounts(ctx, "site-a", 2)
	require.NoError(t, err)
	assert.Equal(t, []string{"anna", "zoe"}, bounded.Accounts,
		"--limit 2 must deliver 2 usable accounts, not 2 rows minus the unusable ones")
}

// The reviewer's question, asked of the real thing: does what Mongo returns
// reach the artifact, the result and the manifest consistently? Every unit
// test above this line runs against a fake source that could agree with a
// wrong implementation.
func TestIntegration_ExportPool_FromMongo_PublishesAndAccountsForEverySkip(t *testing.T) {
	db := testutil.MongoDB(t, "poolexportend2end")
	ctx := context.Background()

	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "e1", "siteId": "site-a", "roomType": "channel", "u": bson.M{"account": "anna"}},
		bson.M{"_id": "e2", "siteId": "site-a", "roomType": "channel", "u": bson.M{"account": "bob"}},
		bson.M{"_id": "e3", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "k6.test-1.user"}},
		bson.M{"_id": "e4", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "weather.site-a.bot", "isBot": true}},
	})
	require.NoError(t, err)

	pub := newFakePublisher()
	res, err := exportPool(ctx, mongoPoolSource{db: db}, pub, poolExportOptions{
		RunID: "run-1", SiteID: "site-a",
	})
	require.NoError(t, err, "one leftover row must not fail an export the rest of the site can serve")

	art := pub.artifacts[res.ArtifactKey]
	require.NotNil(t, art)
	assert.Equal(t, []string{"anna", "bob"}, art.Accounts)
	assert.Equal(t, 2, res.Accounts)

	// The evidence, end to end: the flagged bot never leaves the server, so it
	// is the query's business and not a skip; "k6.test-1.user" is walked past
	// by the cursor, counted there, and surfaces in all three places.
	assert.Equal(t, 1, res.Skipped)
	assert.Equal(t, []string{"k6.test-1.user"}, res.SkippedSample)
	man, ok := pub.blobs[res.ManifestKey].(poolManifest)
	require.True(t, ok)
	assert.Equal(t, 1, man.SkippedAccounts)
	assert.Equal(t, 2, man.Accounts)
}

// A site whose channel subscribers are all leftovers fails — and names one, so
// the operator knows it is junk data to clean rather than a site nobody uses.
func TestIntegration_ExportPool_FromMongo_FailsNamingAnUnusableSite(t *testing.T) {
	db := testutil.MongoDB(t, "poolexportjunk")
	ctx := context.Background()
	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "j1", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "k6.test-1.user"}},
		bson.M{"_id": "j2", "siteId": "site-a", "roomType": "channel",
			"u": bson.M{"account": "k6.test-2.user"}},
	})
	require.NoError(t, err)

	_, err = exportPool(ctx, mongoPoolSource{db: db}, newFakePublisher(), poolExportOptions{
		RunID: "run-1", SiteID: "site-a",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "k6.test-1.user")
}
