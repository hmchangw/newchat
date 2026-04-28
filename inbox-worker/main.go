package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"
	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
)

type config struct {
	NatsURL       string          `env:"NATS_URL"        envDefault:"nats://localhost:4222"`
	NatsCredsFile string          `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string          `env:"SITE_ID"         envDefault:"default"`
	MongoURI      string          `env:"MONGO_URI"       envDefault:"mongodb://localhost:27017"`
	MongoDB       string          `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername string          `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword string          `env:"MONGO_PASSWORD"  envDefault:""`
	Bootstrap     bootstrapConfig `envPrefix:"BOOTSTRAP_"`
}

// mongoInboxStore implements InboxStore using MongoDB.
type mongoInboxStore struct {
	subCol  *mongo.Collection
	roomCol *mongo.Collection
	userCol *mongo.Collection
}

func (s *mongoInboxStore) CreateSubscription(ctx context.Context, sub *model.Subscription) error {
	_, err := s.subCol.InsertOne(ctx, sub)
	return err
}

func (s *mongoInboxStore) UpsertRoom(ctx context.Context, room *model.Room) error {
	filter := bson.M{"_id": room.ID}
	update := bson.M{"$set": room}
	opts := options.UpdateOne().SetUpsert(true)
	_, err := s.roomCol.UpdateOne(ctx, filter, update, opts)
	return err
}

func (s *mongoInboxStore) UpdateSubscriptionRoles(ctx context.Context, account, roomID string, roles []model.Role) error {
	filter := bson.M{"u.account": account, "roomId": roomID}
	update := bson.M{"$set": bson.M{"roles": roles}}
	res, err := s.subCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update subscription roles for %q in room %q: %w", account, roomID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("subscription not found for %q in room %q", account, roomID)
	}
	return nil
}

func (s *mongoInboxStore) DeleteSubscriptionsByAccounts(ctx context.Context, roomID string, accounts []string) error {
	_, err := s.subCol.DeleteMany(ctx, bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}})
	if err != nil {
		return fmt.Errorf("delete subscriptions in room %q: %w", roomID, err)
	}
	return nil
}

func (s *mongoInboxStore) FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error) {
	if len(accounts) == 0 {
		return nil, nil
	}
	cursor, err := s.userCol.Find(ctx, bson.M{"account": bson.M{"$in": accounts}})
	if err != nil {
		return nil, fmt.Errorf("find users by accounts: %w", err)
	}
	var users []model.User
	if err := cursor.All(ctx, &users); err != nil {
		return nil, fmt.Errorf("decode users: %w", err)
	}
	return users, nil
}

func (s *mongoInboxStore) BulkCreateSubscriptions(ctx context.Context, subs []*model.Subscription) error {
	if len(subs) == 0 {
		return nil
	}
	docs := make([]interface{}, len(subs))
	for i, sub := range subs {
		docs[i] = sub
	}
	opts := options.InsertMany().SetOrdered(false)
	_, err := s.subCol.InsertMany(ctx, docs, opts)
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("bulk create subscriptions: %w", err)
	}
	return nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "inbox-worker")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	db := mongoClient.Database(cfg.MongoDB)
	store := &mongoInboxStore{
		subCol:  db.Collection("subscriptions"),
		roomCol: db.Collection("rooms"),
		userCol: db.Collection("users"),
	}

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	js, err := oteljetstream.New(nc)
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	inboxCfg := stream.Inbox(cfg.SiteID)

	cons, err := js.CreateOrUpdateConsumer(ctx, inboxCfg.Name, jetstream.ConsumerConfig{
		Durable:   "inbox-worker",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	publisher := &natsPublisher{nc: nc}
	handler := NewHandler(store, publisher)

	cctx, err := cons.Consume(func(m oteljetstream.Msg) {
		if err := handler.HandleEvent(m.Context(), m.Data()); err != nil {
			slog.Error("handle event failed", "error", err)
			if err := m.Nak(); err != nil {
				slog.Error("failed to nak message", "error", err)
			}
			return
		}
		if err := m.Ack(); err != nil {
			slog.Error("failed to ack message", "error", err)
		}
	})
	if err != nil {
		slog.Error("consume failed", "error", err)
		os.Exit(1)
	}

	slog.Info("inbox-worker started", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error {
			cctx.Stop()
			return nil
		},
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
	)
}

// natsPublisher adapts *otelnats.Conn to the Publisher interface.
type natsPublisher struct {
	nc *otelnats.Conn
}

func (p *natsPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	return p.nc.Publish(ctx, subject, data)
}
