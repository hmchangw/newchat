//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestSoakRunA_SeedFrontDoorReadBackAndTeardown(t *testing.T) {
	ctx := context.Background()
	const siteID = "site-soak-run-a"
	db := testutil.MongoDB(t, "loadgen_soak_run_a")
	store := &mongoSoakStore{db: db}
	keyStore := roomkeystore.NewMongoStore(db.Collection("rooms"), time.Hour)
	t.Cleanup(func() { require.NoError(t, keyStore.Close()) })

	users := makeSoakUsers(8, siteID)
	userDocs := make([]any, len(users))
	for i := range users {
		userDocs[i] = users[i]
	}
	_, err := db.Collection("users").InsertMany(ctx, userDocs)
	require.NoError(t, err)

	cfg := validSoakConfig(t)
	cfg.RunID = "integration-run-a"
	cfg.MaxUsers = 8
	cfg.ActiveUsers = 6
	cfg.RoomCount = 4
	cfg.ChannelRatio = 0.5
	cfg.ChannelMembers = 3
	cfg.PersistGrace = 0
	cfg.ReactionsPerHotMessage = 3
	seededTopology, err := seedSoak(
		ctx,
		store,
		keyStore,
		&soakSeedInput{
			RunID: cfg.RunID, SiteID: siteID, MongoDatabase: db.Name(),
			CassandraKeyspace: "chat", Seed: 42, Config: &cfg,
		},
		newProductionSoakIDs(),
	)
	require.NoError(t, err)

	topology, err := store.LoadTopology(ctx, cfg.RunID, siteID)
	require.NoError(t, err)
	require.Len(t, topology.Rooms, cfg.RoomCount)
	require.NotEmpty(t, topology.Subscriptions)
	assert.Equal(t, soakUserIDs(seededTopology.ActiveUsers), soakUserIDs(topology.ActiveUsers))

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })
	accepted := make(chan model.Message, 1)
	gatekeeper, err := nc.Subscribe(subject.MsgSendWildcard(siteID), func(message *nats.Msg) {
		var request model.SendMessageRequest
		if unmarshalErr := json.Unmarshal(message.Data, &request); unmarshalErr != nil {
			return
		}
		account, roomID, _, ok := subject.ParseUserRoomSiteSubject(message.Subject)
		if !ok {
			return
		}
		var userID string
		for i := range topology.Subscriptions {
			subscription := &topology.Subscriptions[i]
			if subscription.RoomID == roomID &&
				subscription.User.Account == account {
				userID = subscription.User.ID
				break
			}
		}
		response := model.Message{
			ID: request.ID, RoomID: roomID, UserID: userID,
			UserAccount: account, Content: request.Content,
			ThreadParentMessageID: request.ThreadParentMessageID,
			CreatedAt:             time.Now().UTC(),
		}
		_, _ = db.Collection(atrest.CollectionName).InsertOne(
			ctx,
			bson.D{
				{Key: "_id", Value: roomID},
				{Key: "wrappedDEK", Value: []byte("vault:v1:test")},
				{Key: "createdAt", Value: time.Now().UTC()},
			},
		)
		data, _ := json.Marshal(response)
		_ = nc.Publish(subject.UserResponse(account, request.RequestID), data)
		accepted <- response
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gatekeeper.Unsubscribe() })

	selector, err := newSoakRuntimeSelector(&topology, &cfg, 42)
	require.NoError(t, err)
	catalog := newSoakCatalog(16, 64, 0, nil)
	sender := newSoakSender(
		soakSendConfig{SiteID: siteID, ReplyTimeout: time.Second},
		catalog,
		newNatsCorePublisher(nc, InjectFrontdoor, nil),
		nil,
		nil,
		nil,
	)
	replies := make(chan soakSendObservation, 1)
	responseSubscription, err := startSoakSendResponsesWithObserver(
		newNATSSoakResponseSource(nc),
		sender,
		func(result soakSendReplyResult) {
			replies <- soakSendObservation{result: result}
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = responseSubscription.Unsubscribe() })

	preflightCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, runSoakEncryptionPreflight(
		preflightCtx,
		store,
		sender,
		selector,
		replies,
		0,
	))
	message := <-accepted

	getSubject := subject.MsgGetWildcard(siteID)
	history, err := nc.Subscribe(getSubject, func(request *nats.Msg) {
		response := soakVerifyMessage{
			RoomID: message.RoomID, MessageID: message.ID,
			Sender: cassandra.Participant{
				ID: message.UserID, Account: message.UserAccount,
			},
			Msg: message.Content, CreatedAt: message.CreatedAt,
		}
		data, _ := json.Marshal(response)
		_ = request.Respond(data)
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = history.Unsubscribe() })
	require.NoError(t, nc.Flush())

	verifier := newSoakVerifier(
		&soakVerifyConfig{
			SiteID: siteID, RequestTimeout: time.Second,
		},
		catalog,
		newSoakRPCClient(
			newNATSHistoryRequester(nc),
			soakRetryConfig{MaxAttempts: 1},
			nil,
			nil,
		),
		nil,
		nil,
	)
	result := verifier.VerifyByID(ctx, message.RoomID, message.ID)
	assert.Equal(t, soakVerifyOK, result.Class)

	cfg.CassandraCleanup = "none"
	found, err := teardownSoak(ctx, store, nil, &cfg, "chat")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(len(users)), countDocuments(
		t,
		db.Collection("users"),
		bson.D{},
	))
	assert.Equal(t, int64(0), countDocuments(
		t,
		db.Collection("rooms"),
		bson.D{{Key: "soakRunId", Value: cfg.RunID}},
	))
}
