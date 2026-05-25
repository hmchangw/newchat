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

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

type config struct {
	NatsURL       string                  `env:"NATS_URL"        envDefault:"nats://localhost:4222"`
	NatsCredsFile string                  `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string                  `env:"SITE_ID"         envDefault:"default"`
	MongoURI      string                  `env:"MONGO_URI"       envDefault:"mongodb://localhost:27017"`
	MongoDB       string                  `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername string                  `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword string                  `env:"MONGO_PASSWORD"  envDefault:""`
	Consumer      stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap     bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
}

// mongoInboxStore implements InboxStore using MongoDB.
type mongoInboxStore struct {
	subCol       *mongo.Collection
	roomCol      *mongo.Collection
	userCol      *mongo.Collection
	threadSubCol *mongo.Collection
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

// BulkCreateSubscriptions inserts the supplied subs idempotently. Each is
// keyed by (roomId, u.account) and written via $setOnInsert so an existing
// sub (from a previous delivery, or with read-state already accumulated) is
// preserved. Redelivered cross-site events become no-ops on Mongo.
func (s *mongoInboxStore) BulkCreateSubscriptions(ctx context.Context, subs []*model.Subscription) error {
	if len(subs) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, len(subs))
	for i, sub := range subs {
		models[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.M{"roomId": sub.RoomID, "u.account": sub.User.Account}).
			SetUpdate(bson.M{"$setOnInsert": sub}).
			SetUpsert(true)
	}
	opts := options.BulkWrite().SetOrdered(false)
	if _, err := s.subCol.BulkWrite(ctx, models, opts); err != nil {
		return fmt.Errorf("bulk upsert subscriptions: %w", err)
	}
	return nil
}

// UpdateSubscriptionMute sets muted by (roomID, account); missing is a silent no-op.
func (s *mongoInboxStore) UpdateSubscriptionMute(ctx context.Context, roomID, account string, muted bool) error {
	_, err := s.subCol.UpdateOne(ctx,
		bson.M{"roomId": roomID, "u.account": account},
		bson.M{"$set": bson.M{"muted": muted}},
	)
	if err != nil {
		return fmt.Errorf("update subscription mute for %q in room %q: %w", account, roomID, err)
	}
	return nil
}

func (s *mongoInboxStore) UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) error {
	filter := bson.M{
		"roomId":    roomID,
		"u.account": account,
		"$or": bson.A{
			bson.M{"lastSeenAt": bson.M{"$exists": false}},
			bson.M{"lastSeenAt": bson.M{"$lt": lastSeenAt}},
		},
	}
	update := bson.M{"$set": bson.M{"lastSeenAt": lastSeenAt, "alert": alert}}
	if _, err := s.subCol.UpdateOne(ctx, filter, update); err != nil {
		return fmt.Errorf("update subscription read for %q in room %q: %w", account, roomID, err)
	}
	return nil
}

// ensureIndexes creates the unique index on (threadRoomId, userId) used by
// UpsertThreadSubscription. The index name and shape match what message-worker
// creates in its own threadStoreMongo so both services agree on the natural
// key for thread subscriptions.
func (s *mongoInboxStore) ensureIndexes(ctx context.Context) error {
	if _, err := s.threadSubCol.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "threadRoomId", Value: 1}, {Key: "userId", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("ensure thread_subscriptions (threadRoomId,userId) index: %w", err)
	}
	return nil
}

// UpsertThreadSubscription inserts the subscription on first event for a
// (threadRoomId, userId) pair, and on subsequent events updates only
// updatedAt and (monotonically) hasMention. $setOnInsert pins the immutable
// fields on insert; $set always refreshes updatedAt; $max on hasMention
// guarantees a non-mention event never clears a prior mention=true.
//
// $max on a bool works because BSON encodes false (0x00) < true (0x01), so
// $max(existing, incoming) for a bool is equivalent to a monotonic OR.
//
// $setOnInsert and $max operate on disjoint fields (hasMention is set by $max
// only — never by $setOnInsert) so MongoDB doesn't reject the update with a
// "conflicting update operators" error.
func (s *mongoInboxStore) UpsertThreadSubscription(ctx context.Context, sub *model.ThreadSubscription) error {
	filter := bson.M{"threadRoomId": sub.ThreadRoomID, "userId": sub.UserID}
	update := bson.M{
		"$setOnInsert": bson.M{
			"_id":             sub.ID,
			"parentMessageId": sub.ParentMessageID,
			"roomId":          sub.RoomID,
			"threadRoomId":    sub.ThreadRoomID,
			"userId":          sub.UserID,
			"userAccount":     sub.UserAccount,
			"siteId":          sub.SiteID,
			"lastSeenAt":      sub.LastSeenAt,
			"createdAt":       sub.CreatedAt,
		},
		"$set": bson.M{"updatedAt": sub.UpdatedAt},
		"$max": bson.M{"hasMention": sub.HasMention},
	}
	if _, err := s.threadSubCol.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("upsert thread subscription (threadRoomID %q, userID %q): %w",
			sub.ThreadRoomID, sub.UserID, err)
	}
	return nil
}

func (s *mongoInboxStore) ApplyThreadRead(ctx context.Context, roomID, threadRoomID, account string, newThreadUnread []string, alert bool, lastSeenAt time.Time) error {
	// Guarded thread-sub update first; same gate then protects the Subscription overwrite.
	tsFilter := bson.M{
		"threadRoomId": threadRoomID,
		"userAccount":  account,
		"$or": bson.A{
			bson.M{"lastSeenAt": nil},
			bson.M{"lastSeenAt": bson.M{"$lt": lastSeenAt}},
		},
	}
	tsUpdate := bson.M{"$set": bson.M{
		"lastSeenAt": lastSeenAt,
		"updatedAt":  lastSeenAt,
		"hasMention": false,
	}}
	tsRes, err := s.threadSubCol.UpdateOne(ctx, tsFilter, tsUpdate)
	if err != nil {
		return fmt.Errorf("apply thread read on thread subscription for %q in thread room %q: %w",
			account, threadRoomID, err)
	}
	if tsRes.MatchedCount == 0 {
		return nil
	}

	subFilter := bson.M{"roomId": roomID, "u.account": account}
	var subUpdate bson.M
	if len(newThreadUnread) == 0 {
		subUpdate = bson.M{
			"$set":   bson.M{"alert": alert},
			"$unset": bson.M{"threadUnread": ""},
		}
	} else {
		subUpdate = bson.M{"$set": bson.M{"threadUnread": newThreadUnread, "alert": alert}}
	}
	if _, err := s.subCol.UpdateOne(ctx, subFilter, subUpdate); err != nil {
		return fmt.Errorf("apply thread read on subscription for %q in room %q: %w", account, roomID, err)
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
		subCol:       db.Collection("subscriptions"),
		roomCol:      db.Collection("rooms"),
		userCol:      db.Collection("users"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
	if err := store.ensureIndexes(ctx); err != nil {
		slog.Error("ensure indexes failed", "error", err)
		os.Exit(1)
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

	// Local lane is reserved for search-sync-worker; scope to aggregate.> only.
	cons, err := js.CreateOrUpdateConsumer(ctx, inboxCfg.Name, buildConsumerConfig(cfg.Consumer, cfg.SiteID))
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	handler := NewHandler(store)

	cctx, err := cons.Consume(func(m oteljetstream.Msg) {
		handlerCtx := natsutil.ContextWithRequestIDFromHeaders(m.Context(), m.Headers())
		if err := handler.HandleEvent(handlerCtx, m.Data()); err != nil {
			slog.Error("handle event failed", "error", err, "request_id", natsutil.RequestIDFromContext(handlerCtx))
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

// buildConsumerConfig returns the durable consumer config for
// inbox-worker. The site-scoped FilterSubjects keeps inbox-worker on the
// federated `aggregate.>` lane only; same-site direct publishes are
// reserved for search-sync-worker.
func buildConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "inbox-worker"
	cc.FilterSubjects = []string{subject.InboxAggregateAll(siteID)}
	return cc
}
