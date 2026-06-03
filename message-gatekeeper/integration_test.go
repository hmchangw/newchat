//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/userstore"
)

// TestProcessMessage_PopulatesDisplayName_Integration walks the wiring that
// gatekeeper's main.go does in prod against real Mongo:
//
//	users coll  →  userstore.NewMongoStore  →  userstore.NewCache  →  Handler
//
// and asserts the published canonical event has UserDisplayName composed via
// displayfmt.CombineWithFallback.
//
// Scope is deliberately narrow: it proves the Mongo wiring (collection name,
// BSON field tags, projection drift) and the end-to-end composition. The
// composition *rule itself* (eng+zh dedupe, account fallback) is exhaustively
// covered by pkg/displayfmt/combine_test.go and Handler unit tests; re-testing
// every variant here would just duplicate that coverage with slow tests.
func TestProcessMessage_PopulatesDisplayName_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "message_gatekeeper_test")
	ctx := context.Background()

	user := model.User{ID: "u-alice", Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲"}
	seedUserAndSubscription(t, ctx, db, user, "r1")

	handler, getCaptured := buildHandlerWithCapture(t, db)

	req := model.SendMessageRequest{
		ID:        idgen.GenerateMessageID(),
		Content:   "hello",
		RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
	}

	_, perr := handler.processMessage(ctx, user.Account, "r1", "site-a", &req)
	require.NoError(t, perr)

	captured := getCaptured()
	require.NotNil(t, captured, "canonical event was never published")
	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal(captured.Data, &evt))
	assert.Equal(t, "Alice Wang 愛麗絲", evt.Message.UserDisplayName,
		"gatekeeper must compose display name via displayfmt.CombineWithFallback")
	assert.Equal(t, user.Account, evt.Message.UserAccount)
	assert.Equal(t, user.ID, evt.Message.UserID)
}

// seedUserAndSubscription inserts the minimal docs gatekeeper needs to accept a
// message from u into room roomID: a users row (read by userstore.Cache for
// display-name composition), a subscription so GetSubscription returns it, and
// a room doc so room-meta lookup succeeds.
func seedUserAndSubscription(t *testing.T, ctx context.Context, db *mongo.Database, u model.User, roomID string) {
	t.Helper()
	_, err := db.Collection("users").InsertOne(ctx, u)
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID: "sub-" + u.ID, RoomID: roomID, SiteID: "site-a",
		User: model.SubscriptionUser{ID: u.ID, Account: u.Account},
	})
	require.NoError(t, err)
	_, err = db.Collection("rooms").InsertOne(ctx, model.Room{
		ID: roomID, Name: "general", Type: model.RoomTypeChannel,
		SiteID: "site-a", UserCount: 1,
	})
	require.NoError(t, err)
}

// buildHandlerWithCapture mirrors main.go's gatekeeper wiring against the test
// Mongo and returns the Handler plus a getter that yields the canonical event
// published by processMessage (nil until the publish fires).
func buildHandlerWithCapture(t *testing.T, db *mongo.Database) (*Handler, func() *nats.Msg) {
	t.Helper()
	users, err := userstore.NewCache(userstore.NewMongoStore(db.Collection("users")), 100, time.Minute)
	require.NoError(t, err)

	var captured *nats.Msg
	pub := func(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
		captured = msg
		return &jetstream.PubAck{}, nil
	}
	reply := func(_ context.Context, _ *nats.Msg) error { return nil }
	return NewHandler(NewMongoStore(db), users, pub, reply, "site-a", nil, 500),
		func() *nats.Msg { return captured }
}
