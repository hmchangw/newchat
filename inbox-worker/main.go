package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/errcode"
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
	MaxWorkers    int                     `env:"MAX_WORKERS"     envDefault:"100"`
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

// UpsertRoom replicates room metadata, guarded by the incoming room's
// UpdatedAt so out-of-order federated delivery cannot regress it. The guard
// is in the filter, so an event whose UpdatedAt is not strictly newer than the
// stored one fails to match; with upsert enabled that falls back to an insert
// which collides on _id (the room already exists) — a duplicate-key error we
// treat as a no-op. A genuinely new room (no stored doc) is inserted normally.
func (s *mongoInboxStore) UpsertRoom(ctx context.Context, room *model.Room) error {
	filter := bson.M{
		"_id": room.ID,
		"$or": bson.A{
			bson.M{"updatedAt": bson.M{"$exists": false}},
			bson.M{"updatedAt": bson.M{"$lt": room.UpdatedAt}},
		},
	}
	update := bson.M{"$set": room}
	opts := options.UpdateOne().SetUpsert(true)
	if _, err := s.roomCol.UpdateOne(ctx, filter, update, opts); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Guard rejected a stale/duplicate room_sync; the existing doc is
			// newer-or-equal, so dropping this event is correct.
			return nil
		}
		return fmt.Errorf("upsert room %q: %w", room.ID, err)
	}
	return nil
}

// UpdateSubscriptionRoles applies roles under a rolesUpdatedAt guard so an
// out-of-order or duplicate role_updated cannot regress roles. A MatchedCount
// of 0 is ambiguous — either the subscription is missing (federation race:
// surface an error so the event is redelivered until member_added lands) or
// the guard rejected a stale event (the sub exists with rolesUpdatedAt >= the
// incoming one — a silent no-op). One existence check on this cold path
// disambiguates the two.
func (s *mongoInboxStore) UpdateSubscriptionRoles(ctx context.Context, account, roomID string, roles []model.Role, rolesUpdatedAt time.Time) error {
	filter := bson.M{
		"u.account": account,
		"roomId":    roomID,
		"$or": bson.A{
			bson.M{"rolesUpdatedAt": bson.M{"$exists": false}},
			bson.M{"rolesUpdatedAt": bson.M{"$lt": rolesUpdatedAt}},
		},
	}
	update := bson.M{"$set": bson.M{"roles": roles, "rolesUpdatedAt": rolesUpdatedAt}}
	res, err := s.subCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update subscription roles for %q in room %q: %w", account, roomID, err)
	}
	if res.MatchedCount > 0 {
		return nil
	}
	exists, err := s.subscriptionExists(ctx, account, roomID)
	if err != nil {
		return fmt.Errorf("check subscription exists for %q in room %q: %w", account, roomID, err)
	}
	if !exists {
		return fmt.Errorf("subscription not found for %q in room %q", account, roomID)
	}
	return nil
}

// subscriptionExists reports whether a subscription for (account, roomID) is
// present, used to distinguish a missing sub from a guard rejection.
func (s *mongoInboxStore) subscriptionExists(ctx context.Context, account, roomID string) (bool, error) {
	err := s.subCol.FindOne(ctx,
		bson.M{"u.account": account, "roomId": roomID},
		options.FindOne().SetProjection(bson.M{"_id": 1}),
	).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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

// UpdateSubscriptionMute sets muted by (roomID, account) under a muteUpdatedAt
// guard so an out-of-order or duplicate toggle cannot regress mute state.
// Missing-sub and guard-rejected events both leave MatchedCount 0 and are
// silent no-ops.
func (s *mongoInboxStore) UpdateSubscriptionMute(ctx context.Context, roomID, account string, muted bool, muteUpdatedAt time.Time) error {
	filter := bson.M{
		"roomId":    roomID,
		"u.account": account,
		"$or": bson.A{
			bson.M{"muteUpdatedAt": bson.M{"$exists": false}},
			bson.M{"muteUpdatedAt": bson.M{"$lt": muteUpdatedAt}},
		},
	}
	update := bson.M{"$set": bson.M{"muted": muted, "muteUpdatedAt": muteUpdatedAt}}
	if _, err := s.subCol.UpdateOne(ctx, filter, update); err != nil {
		return fmt.Errorf("update subscription mute for %q in room %q: %w", account, roomID, err)
	}
	return nil
}

// UpdateSubscriptionFavorite sets favorite by (roomID, account) under a
// favoriteUpdatedAt guard so an out-of-order or duplicate toggle cannot regress
// favorite state. Missing-sub and guard-rejected events both leave MatchedCount
// 0 and are silent no-ops.
func (s *mongoInboxStore) UpdateSubscriptionFavorite(ctx context.Context, roomID, account string, favorite bool, favoriteUpdatedAt time.Time) error {
	filter := bson.M{
		"roomId":    roomID,
		"u.account": account,
		"$or": bson.A{
			bson.M{"favoriteUpdatedAt": bson.M{"$exists": false}},
			bson.M{"favoriteUpdatedAt": bson.M{"$lt": favoriteUpdatedAt}},
		},
	}
	update := bson.M{"$set": bson.M{"favorite": favorite, "favoriteUpdatedAt": favoriteUpdatedAt}}
	if _, err := s.subCol.UpdateOne(ctx, filter, update); err != nil {
		return fmt.Errorf("update subscription favorite for %q in room %q: %w", account, roomID, err)
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

// UpdateSubscriptionNamesForRoom sets name on every subscription in the room,
// each guarded by its own nameUpdatedAt ($lt) so an out-of-order rename cannot
// regress a sub to a stale name. UpdateMany applies the guard per document, so
// subs already carrying a newer rename are skipped while the rest advance.
func (s *mongoInboxStore) UpdateSubscriptionNamesForRoom(ctx context.Context, roomID, newName string, nameUpdatedAt time.Time) error {
	filter := bson.M{
		"roomId": roomID,
		"$or": bson.A{
			bson.M{"nameUpdatedAt": bson.M{"$exists": false}},
			bson.M{"nameUpdatedAt": bson.M{"$lt": nameUpdatedAt}},
		},
	}
	update := bson.M{"$set": bson.M{"name": newName, "nameUpdatedAt": nameUpdatedAt}}
	if _, err := s.subCol.UpdateMany(ctx, filter, update); err != nil {
		return fmt.Errorf("update subscription names for room %s: %w", roomID, err)
	}
	return nil
}

// ApplySubscriptionVisibility writes {restricted, externalAccess, roles} to all
// subs in the room, each guarded by its own visibilityUpdatedAt ($lt) so an
// out-of-order visibility change cannot regress the flags/roles. The guard lives
// in the filter for both the restrict-with-owner pipeline branch and the
// flags-only branch.
func (s *mongoInboxStore) ApplySubscriptionVisibility(ctx context.Context, roomID string, restricted, externalAccess bool, ownerAccount string, visibilityUpdatedAt time.Time) error {
	filter := bson.M{
		"roomId": roomID,
		"$or": bson.A{
			bson.M{"visibilityUpdatedAt": bson.M{"$exists": false}},
			bson.M{"visibilityUpdatedAt": bson.M{"$lt": visibilityUpdatedAt}},
		},
	}
	if restricted && ownerAccount != "" {
		pipeline := mongo.Pipeline{
			bson.D{{Key: "$set", Value: bson.M{
				"restricted":          true,
				"externalAccess":      externalAccess,
				"visibilityUpdatedAt": visibilityUpdatedAt,
				"roles": bson.M{"$cond": bson.M{
					"if":   bson.M{"$eq": bson.A{"$u.account", ownerAccount}},
					"then": bson.A{string(model.RoleOwner)},
					"else": bson.A{string(model.RoleMember)},
				}},
			}}},
		}
		if _, err := s.subCol.UpdateMany(ctx, filter, pipeline); err != nil {
			return fmt.Errorf("apply visibility (restrict+rewrite): %w", err)
		}
		return nil
	}
	if _, err := s.subCol.UpdateMany(ctx, filter, bson.M{
		"$set": bson.M{"restricted": restricted, "externalAccess": externalAccess, "visibilityUpdatedAt": visibilityUpdatedAt},
	}); err != nil {
		return fmt.Errorf("apply visibility (flags only): %w", err)
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

	// Two-lane pull pattern over the single INBOX aggregate consumer:
	//
	//   - Membership events (member_added/member_removed) run on ONE
	//     sequential lane. They are NOT individually order-safe — a physical
	//     delete carries no high-water mark, so a stale add could otherwise
	//     resurrect a removed membership (and vice versa). Serializing them
	//     restores in-order processing within this instance and keeps the
	//     add/remove resurrection race at its pre-fan-out baseline.
	//   - Everything else (the high-volume subscription_read/thread_read
	//     receipts, plus role/mute/room_sync) fans out across a bounded
	//     worker pool. Those handlers are idempotent and order-safe (Mongo
	//     $lt/$max/$setOnInsert guards), so concurrent processing is correct.
	//
	// Membership traffic is a tiny fraction of the lane, so serializing it
	// costs negligible throughput while the read-receipt path keeps its full
	// MaxWorkers concurrency.
	iter, err := cons.Messages(jetstream.PullMaxMessages(2 * cfg.MaxWorkers))
	if err != nil {
		slog.Error("messages failed", "error", err)
		os.Exit(1)
	}

	sem := make(chan struct{}, cfg.MaxWorkers)
	membershipCh := make(chan oteljetstream.Msg, cfg.MaxWorkers)
	var wg sync.WaitGroup

	process := func(msg oteljetstream.Msg) {
		handlerCtx, _ := natsutil.StampRequestID(msg.Context(), msg.Headers(), msg.Subject())
		if err := handler.HandleEvent(handlerCtx, msg.Data()); err != nil {
			// Permanent failures (poison messages) Ack so JetStream stops
			// redelivering; transient infra errors Nak for redelivery.
			if _, isPermanent := errcode.IsPermanent(err); isPermanent {
				slog.Warn("permanent event failure — dropping (Ack)", "error", err, "request_id", natsutil.RequestIDFromContext(handlerCtx))
				if err := msg.Ack(); err != nil {
					slog.Error("failed to ack permanent message", "error", err)
				}
				return
			}
			slog.Error("handle event failed", "error", err, "request_id", natsutil.RequestIDFromContext(handlerCtx))
			if err := msg.Nak(); err != nil {
				slog.Error("failed to nak message", "error", err)
			}
			return
		}
		if err := msg.Ack(); err != nil {
			slog.Error("failed to ack message", "error", err)
		}
	}

	// Membership lane: a single worker drains membershipCh in FIFO order, so
	// add/remove for the same (room, account) are applied in arrival order.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range membershipCh {
			process(msg)
		}
	}()

	go func() {
		defer close(membershipCh)
		for {
			msgCtx, msg, err := iter.Next()
			if err != nil {
				return
			}
			m := oteljetstream.Msg{Msg: msg, Ctx: msgCtx}
			if isMembershipSubject(msg.Subject(), cfg.SiteID) {
				membershipCh <- m
				continue
			}
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() {
					<-sem
					wg.Done()
				}()
				process(m)
			}()
		}
	}()

	slog.Info("inbox-worker started", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error {
			iter.Stop()
			return nil
		},
		func(ctx context.Context) error {
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("worker drain timed out: %w", ctx.Err())
			}
		},
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
	)
}

// isMembershipSubject reports whether an INBOX aggregate-lane subject carries a
// membership event (member_added/member_removed) for this site. Those events
// are routed to a single sequential lane because, unlike the read-receipt and
// role/mute/room_sync handlers, they have no per-document high-water-mark guard
// and so must be applied in order to avoid the add/remove resurrection race.
func isMembershipSubject(subj, siteID string) bool {
	return subj == subject.InboxMemberAddedAggregate(siteID) ||
		subj == subject.InboxMemberRemovedAggregate(siteID)
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
