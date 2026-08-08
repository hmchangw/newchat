package main

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
// lazy), so the store's read-preference wiring is unit-testable with no container.
func lazyDB(t *testing.T) *mongo.Database {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Disconnect(context.Background())) })
	return client.Database("testdb")
}

func TestNewMongoStore_ReadPreferenceRouting(t *testing.T) {
	db := lazyDB(t)

	t.Run("no option: every secondary handle aliases its primary", func(t *testing.T) {
		s := NewMongoStore(db)
		assert.Same(t, s.rooms, s.roomsSecondary)
		assert.Same(t, s.subscriptions, s.subscriptionsSecondary)
		assert.Same(t, s.threadSubscriptions, s.threadSubscriptionsSecondary)
		assert.Same(t, s.threadRooms, s.threadRoomsSecondary)
		assert.Same(t, s.users, s.usersSecondary)
		assert.Same(t, s.apps, s.appsSecondary)
		assert.Same(t, s.botCmdMenus, s.botCmdMenusSecondary)
	})

	t.Run("secondary preference: safe handles are distinct, primary handles untouched", func(t *testing.T) {
		s := NewMongoStore(db, WithReadPreference(readpref.SecondaryPreferred()))
		// Secondary handles diverge from the primaries...
		assert.NotSame(t, s.rooms, s.roomsSecondary)
		assert.NotSame(t, s.subscriptions, s.subscriptionsSecondary)
		assert.NotSame(t, s.threadSubscriptions, s.threadSubscriptionsSecondary)
		assert.NotSame(t, s.threadRooms, s.threadRoomsSecondary)
		assert.NotSame(t, s.users, s.usersSecondary)
		assert.NotSame(t, s.apps, s.appsSecondary)
		assert.NotSame(t, s.botCmdMenus, s.botCmdMenusSecondary)
		// ...but they target the same collections.
		assert.Equal(t, s.subscriptions.Name(), s.subscriptionsSecondary.Name())
		assert.Equal(t, s.users.Name(), s.usersSecondary.Name())
		// roomMembers and teamsMeetings have no secondary handle — they are read only
		// on must-primary paths (membership authz, meeting dedup/read-back).
		assert.NotNil(t, s.roomMembers)
		assert.NotNil(t, s.teamsMeetings)
	})
}
