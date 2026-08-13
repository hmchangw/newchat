//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/testutil"
)

// EnsureIndexes must not conflict with the canonical UNIQUE users.account_1 index
// (owned by user-service). Regression for the non-unique index that crashed the
// service on boot with IndexKeySpecsConflict.
func TestEnsureIndexes_UsersAccountMatchesCanonicalUnique(t *testing.T) {
	db := testutil.MongoDB(t, "search_service_index_test")
	ctx := context.Background()

	// Pre-create the canonical indexes owned by other services, as they do:
	// users.account UNIQUE (user-service) + apps.assistant.name named (user/room-service).
	_, err := db.Collection("users").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "account", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)
	_, err = db.Collection("apps").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "assistant.name", Value: 1}},
		Options: options.Index().SetName("assistant_name_idx"),
	})
	require.NoError(t, err)

	require.NoError(t, newMongoStore(db).ensureIndexes(ctx),
		"search-service ensureIndexes must match the canonical index specs, not conflict")
}
