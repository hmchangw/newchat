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
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
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
	return NewHandler(NewMongoStore(db, nil, 0), users, pub, reply, "site-a", nil, 500),
		func() *nats.Msg { return captured }
}

// TestGatekeeper_DebugBreadcrumbsAndPropagation_Integration walks a real
// Mongo-backed processMessage and asserts the two end-to-end debug behaviors
// that are only verifiable past the package boundary: (1) a flagged request
// emits the flow breadcrumb, and (2) the X-Debug rung rides onto the outbound
// MESSAGES_CANONICAL message so it propagates to downstream workers. The
// unflagged control proves zero added output and no header bleed.
func TestGatekeeper_DebugBreadcrumbsAndPropagation_Integration(t *testing.T) {
	db := testutil.MongoDB(t, "message_gatekeeper_test")
	ctx := context.Background()

	user := model.User{ID: "u-bob", Account: "bob", EngName: "Bob"}
	seedUserAndSubscription(t, ctx, db, user, "r-flow")

	rec := installRecorder(t)
	handler, getCaptured := buildHandlerWithCapture(t, db)

	newReq := func() model.SendMessageRequest {
		return model.SendMessageRequest{
			ID:        idgen.GenerateMessageID(),
			Content:   "hi",
			RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
		}
	}

	t.Run("flagged: flow breadcrumb emitted and X-Debug propagates onto canonical", func(t *testing.T) {
		rec.reset()
		req := newReq()
		_, perr := handler.processMessage(admitRung("flow"), user.Account, "r-flow", "site-a", &req)
		require.NoError(t, perr)

		assert.True(t, rec.has(logctx.LevelFlow, "gatekeeper published to canonical"),
			"flagged request must emit the flow breadcrumb")
		captured := getCaptured()
		require.NotNil(t, captured, "canonical event was never published")
		assert.Equal(t, "flow", captured.Header.Get(natsutil.DebugHeader),
			"X-Debug rung must ride onto the canonical message for downstream propagation")
	})

	t.Run("unflagged control: no flow breadcrumb, no X-Debug header", func(t *testing.T) {
		rec.reset()
		req := newReq()
		_, perr := handler.processMessage(context.Background(), user.Account, "r-flow", "site-a", &req)
		require.NoError(t, perr)

		assert.False(t, rec.hasLevel(logctx.LevelFlow), "unflagged traffic must emit no flow lines")
		captured := getCaptured()
		require.NotNil(t, captured)
		assert.Empty(t, captured.Header.Get(natsutil.DebugHeader), "no X-Debug header on unflagged traffic")
	})
}
