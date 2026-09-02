//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/user-service/mongorepo"
)

// The whole workload rests on one assumption the unit tests can only approximate:
// that fixtures written by the seeder survive user-service's own match filter.
// Three zero-valued fields (open, roomType, name) each make the query return an
// empty page rather than an error, so a regression here is silent — the ramp
// would report excellent latency for the cost of finding nothing. This drives
// the real repository against real Mongo.
func TestSubscriptionListFixtures_ProduceNonEmptyPagesThroughUserService(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := testutil.MongoDB(t, "sublist")
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	fixtures := BuildSubscriptionListFixtures(&p, 42, "site-a", time.Now().UTC())
	require.NoError(t, Seed(ctx, db, &fixtures))

	repo := mongorepo.NewSubscriptionRepo(db, 1024, time.Minute)
	require.NoError(t, repo.EnsureIndexes(ctx))

	accounts := map[string]struct{}{}
	for i := range fixtures.Subscriptions {
		accounts[fixtures.Subscriptions[i].User.Account] = struct{}{}
	}
	require.NotEmpty(t, accounts)

	page := mongoutil.OffsetPageRequest{Offset: 0, Limit: 200}
	for account := range accounts {
		res, err := repo.AggregateSubscriptions(ctx, account, "current", false, nil, page)
		require.NoError(t, err, "account %s", account)
		assert.NotEmpty(t, res.Data, "account %s must get a non-empty page: an empty one means the seeded fixtures no longer match user-service's filter", account)
	}
}

// The generator picks accounts uniformly from the same fixture set, so every
// account it can address must resolve. This pins the list type the workload
// defaults to; a type the fixtures cannot satisfy is a silent empty ramp.
func TestSubscriptionListFixtures_ListTypesBehaveAsTheWorkloadAssumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := testutil.MongoDB(t, "sublisttypes")
	p, ok := BuiltinPreset("realistic")
	require.True(t, ok)
	fixtures := BuildSubscriptionListFixtures(&p, 42, "site-a", time.Now().UTC())
	require.NoError(t, Seed(ctx, db, &fixtures))

	repo := mongorepo.NewSubscriptionRepo(db, 1024, time.Minute)
	require.NoError(t, repo.EnsureIndexes(ctx))

	account := fixtures.Subscriptions[0].User.Account
	page := mongoutil.OffsetPageRequest{Offset: 0, Limit: 200}

	t.Run("current returns rows", func(t *testing.T) {
		res, err := repo.AggregateSubscriptions(ctx, account, "current", false, nil, page)
		require.NoError(t, err)
		assert.NotEmpty(t, res.Data)
	})

	t.Run("rooms returns rows", func(t *testing.T) {
		res, err := repo.AggregateSubscriptions(ctx, account, "rooms", false, nil, page)
		require.NoError(t, err)
		assert.NotEmpty(t, res.Data)
	})

	// The seeder writes no botDM rows, so apps is legitimately empty. Pinned so a
	// future --list-type=apps ramp is understood as measuring an empty page
	// rather than a fast endpoint.
	t.Run("apps is empty because the seeder writes no botDM rows", func(t *testing.T) {
		res, err := repo.AggregateSubscriptions(ctx, account, "apps", false, nil, page)
		require.NoError(t, err)
		assert.Empty(t, res.Data)
	})
}

// hasMore drives nothing in the ramp's verdict but is recorded per sample, so a
// page smaller than the account's subscription count must report it.
func TestSubscriptionListFixtures_HasMoreReflectsPaging(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := testutil.MongoDB(t, "sublistpaging")
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	fixtures := BuildSubscriptionListFixtures(&p, 42, "site-a", time.Now().UTC())
	require.NoError(t, Seed(ctx, db, &fixtures))

	repo := mongorepo.NewSubscriptionRepo(db, 1024, time.Minute)
	require.NoError(t, repo.EnsureIndexes(ctx))

	byAccount := map[string]int{}
	for i := range fixtures.Subscriptions {
		byAccount[fixtures.Subscriptions[i].User.Account]++
	}
	var account string
	for a, n := range byAccount {
		if n > 1 {
			account = a
			break
		}
	}
	require.NotEmpty(t, account, "the preset must give some account more than one subscription")

	res, err := repo.AggregateSubscriptions(ctx, account, "current", false, nil,
		mongoutil.OffsetPageRequest{Offset: 0, Limit: 1})
	require.NoError(t, err)
	assert.Len(t, res.Data, 1)
	assert.True(t, res.HasMore, "a truncated page must report hasMore")
}
