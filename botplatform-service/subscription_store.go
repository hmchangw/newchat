package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

// Reuses model.model.ErrSubscriptionNotFound as the miss sentinel — same shape user room-service already uses (errors.Is friendly).

// BotSub is the routing-relevant projection of a subscription row. Deliberately minimal: BP
// needs only what it takes to decide which site's downstream service the RPC forwards to.
type BotSub struct {
	RoomID   string
	SiteID   string
	RoomType model.RoomType
}

// subscriptionStore is the read-only surface BP uses to route bot RPCs.
type subscriptionStore interface {
	// FindForBot returns the bot's subscription to a channel room, or model.ErrSubscriptionNotFound when the bot is not a member.
	FindForBot(ctx context.Context, botID, roomID string) (*BotSub, error)
	// FindDMForBot returns the bot's DM subscription with otherAccount, or model.ErrSubscriptionNotFound on first-time DM (caller triggers ensure).
	FindDMForBot(ctx context.Context, botID, otherID string) (*BotSub, error)
}

type mongoSubscriptionStore struct {
	subscriptions *mongo.Collection
}

func newMongoSubscriptionStore(db *mongo.Database) *mongoSubscriptionStore {
	return &mongoSubscriptionStore{subscriptions: db.Collection("subscriptions")}
}

var subRoutingProjection = bson.M{"roomId": 1, "siteId": 1, "roomType": 1}

func (s *mongoSubscriptionStore) FindForBot(ctx context.Context, botID, roomID string) (*BotSub, error) {
	filter := bson.M{"u._id": botID, "roomId": roomID}
	return s.findOne(ctx, filter)
}

func (s *mongoSubscriptionStore) FindDMForBot(ctx context.Context, botID, otherID string) (*BotSub, error) {
	// Deterministic DM room ID: same sorted-concat both sides use, matching bot-room-service.dm.ensure and user-pipeline SyncCreateDMRequest.
	roomID := idgen.BuildDMRoomID(botID, otherID)
	filter := bson.M{"u._id": botID, "roomId": roomID, "roomType": string(model.RoomTypeDM)}
	return s.findOne(ctx, filter)
}

func (s *mongoSubscriptionStore) findOne(ctx context.Context, filter bson.M) (*BotSub, error) {
	var row struct {
		RoomID   string         `bson:"roomId"`
		SiteID   string         `bson:"siteId"`
		RoomType model.RoomType `bson:"roomType"`
	}
	err := s.subscriptions.FindOne(ctx, filter,
		options.FindOne().SetProjection(subRoutingProjection)).Decode(&row)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, model.ErrSubscriptionNotFound
		}
		return nil, fmt.Errorf("find subscription: %w", err)
	}
	return &BotSub{RoomID: row.RoomID, SiteID: row.SiteID, RoomType: row.RoomType}, nil
}

var _ subscriptionStore = (*mongoSubscriptionStore)(nil)
