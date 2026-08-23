package mongorepo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// lazyDB returns a database handle without contacting a server (mongo.Connect is
// lazy), so read-preference wiring is unit-testable with no container.
func lazyDB(t *testing.T) *mongo.Database {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Disconnect(context.Background())) })
	return client.Database("testdb")
}

func TestSubscriptionRepo_ReadPreferenceRouting(t *testing.T) {
	db := lazyDB(t)

	t.Run("no option aliases the primary handles", func(t *testing.T) {
		r := NewSubscriptionRepo(db)
		assert.Same(t, r.enriched, r.enrichedSecondary)
		assert.Same(t, r.subscriptions, r.subscriptionsSecondary)
	})

	t.Run("secondary preference clones read handles but not the primary ones", func(t *testing.T) {
		r := NewSubscriptionRepo(db, WithReadPreference(readpref.SecondaryPreferred()))
		assert.NotSame(t, r.enriched.Raw(), r.enrichedSecondary.Raw())
		assert.NotSame(t, r.subscriptions.Raw(), r.subscriptionsSecondary.Raw())
		// GetAppSubscription (dedup guard) must keep using the primary handle.
		assert.NotSame(t, r.subscriptions.Raw(), r.subscriptionsSecondary.Raw())
	})
}

func TestAppRepo_ReadPreferenceRouting(t *testing.T) {
	db := lazyDB(t)

	t.Run("no option aliases the primary handles", func(t *testing.T) {
		r := NewAppRepo(db)
		assert.Same(t, r.items, r.itemsSecondary)
		assert.Same(t, r.categories, r.categoriesSecondary)
		assert.Same(t, r.apps, r.appsSecondary)
	})

	t.Run("secondary preference clones the safe-read handles", func(t *testing.T) {
		r := NewAppRepo(db, WithReadPreference(readpref.SecondaryPreferred()))
		assert.NotSame(t, r.items.Raw(), r.itemsSecondary.Raw())
		assert.NotSame(t, r.categories.Raw(), r.categoriesSecondary.Raw())
		assert.NotSame(t, r.apps.Raw(), r.appsSecondary.Raw())
	})
}

func TestUserRepo_ReadPreferenceRouting(t *testing.T) {
	db := lazyDB(t)

	base := NewUserRepo(db)
	assert.Same(t, base.users, base.usersSecondary, "no option: usersSecondary aliases users")

	r := NewUserRepo(db, WithReadPreference(readpref.SecondaryPreferred()))
	assert.NotSame(t, r.users.Raw(), r.usersSecondary.Raw())
}
