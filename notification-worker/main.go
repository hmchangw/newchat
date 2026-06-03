package main

import (
	"context"
	"encoding/json"
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

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type config struct {
	NatsURL                string                  `env:"NATS_URL"                  envDefault:"nats://localhost:4222"`
	NatsCredsFile          string                  `env:"NATS_CREDS_FILE"           envDefault:""`
	SiteID                 string                  `env:"SITE_ID"                   envDefault:"default"`
	MongoURI               string                  `env:"MONGO_URI"                 envDefault:"mongodb://localhost:27017"`
	MongoDB                string                  `env:"MONGO_DB"                  envDefault:"chat"`
	MongoUsername          string                  `env:"MONGO_USERNAME"            envDefault:""`
	MongoPassword          string                  `env:"MONGO_PASSWORD"            envDefault:""`
	MaxWorkers             int                     `env:"MAX_WORKERS"               envDefault:"100"`
	LargeRoomThreshold     int                     `env:"LARGE_ROOM_THRESHOLD"      envDefault:"500"`
	PushRecipientBatchSize int                     `env:"PUSH_RECIPIENT_BATCH_SIZE" envDefault:"100"`
	RoomMetaCacheSize      int                     `env:"ROOM_META_CACHE_SIZE"      envDefault:"10000"`
	RoomMetaCacheTTL       time.Duration           `env:"ROOM_META_CACHE_TTL"       envDefault:"2m"`
	ValkeyAddrs            []string                `env:"VALKEY_ADDRS"              envSeparator:","`
	ValkeyPassword         string                  `env:"VALKEY_PASSWORD"           envDefault:""`
	RoomSubCacheTTL        time.Duration           `env:"ROOMSUBCACHE_TTL"          envDefault:"5m"`
	PresenceBatchSize      int                     `env:"PRESENCE_BATCH_SIZE"       envDefault:"512"`
	PresenceRPCTimeout     time.Duration           `env:"PRESENCE_RPC_TIMEOUT"      envDefault:"2s"`
	PresenceEnabled        bool                    `env:"PRESENCE_RPC_ENABLED"      envDefault:"false"`  // false → noopPresenceSnapshotter; set true once presence service is available
	NatsMaxPayloadBytes    int                     `env:"NATS_MAX_PAYLOAD_BYTES"    envDefault:"262144"` // must match broker max_payload; emitter rejects any gzipped batch exceeding this
	Consumer               stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap              bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
}

type mongoMemberLoader struct {
	col *mongo.Collection
}

func (m *mongoMemberLoader) Load(ctx context.Context, roomID string) ([]roomsubcache.Member, error) {
	projection := bson.M{
		"u._id":              1,
		"u.account":          1,
		"u.isBot":            1,
		"roomType":           1,
		"muted":              1,
		"historySharedSince": 1,
	}
	cur, err := m.col.Find(ctx, bson.M{"roomId": roomID}, options.Find().SetProjection(projection))
	if err != nil {
		return nil, fmt.Errorf("find subscriptions for room %s: %w", roomID, err)
	}
	defer cur.Close(ctx)

	var out []roomsubcache.Member
	for cur.Next(ctx) {
		var doc struct {
			User struct {
				ID      string `bson:"_id"`
				Account string `bson:"account"`
				IsBot   bool   `bson:"isBot"`
			} `bson:"u"`
			RoomType           model.RoomType `bson:"roomType"`
			Muted              bool           `bson:"muted"`
			HistorySharedSince *time.Time     `bson:"historySharedSince"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode subscription: %w", err)
		}
		var hssMs *int64
		if doc.HistorySharedSince != nil {
			ms := doc.HistorySharedSince.UnixMilli()
			hssMs = &ms
		}
		out = append(out, roomsubcache.Member{
			ID:                 doc.User.ID,
			Account:            doc.User.Account,
			RoomType:           doc.RoomType,
			IsBot:              doc.User.IsBot,
			Muted:              doc.Muted,
			HistorySharedSince: hssMs,
		})
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("iterate subscriptions: %w", err)
	}
	return out, nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	if len(cfg.ValkeyAddrs) == 0 {
		slog.Error("VALKEY_ADDRS required")
		os.Exit(1)
	}

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "notification-worker")
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
	subCol := db.Collection("subscriptions")
	threadRoomCol := db.Collection("thread_rooms")
	roomsCol := db.Collection("rooms")

	roomMetaCache, err := roommetacache.New(cfg.RoomMetaCacheSize, cfg.RoomMetaCacheTTL,
		func(ctx context.Context, roomID string) (roommetacache.Meta, error) {
			return roommetacache.FetchFromMongo(ctx, roomsCol, roomID)
		})
	if err != nil {
		slog.Error("init room-meta cache failed", "error", err)
		os.Exit(1)
	}

	valkeyClient, err := valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword)
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}
	cache := roomsubcache.NewValkeyCache(valkeyClient)
	loader := &mongoMemberLoader{col: subCol}
	memberLookup := newCachedMemberLookup(cache, loader.Load, cfg.RoomSubCacheTTL)

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	otelJS, err := oteljetstream.New(nc)
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, otelJS, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	canonicalCfg := stream.MessagesCanonical(cfg.SiteID)
	cons, err := otelJS.CreateOrUpdateConsumer(ctx, canonicalCfg.Name, buildConsumerConfig(cfg.Consumer))
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	emitter := newMobileEmitter(&jsPublisher{js: otelJS}, cfg.SiteID, cfg.NatsMaxPayloadBytes)

	var presence PresenceSnapshotter = noopPresenceSnapshotter{}
	if cfg.PresenceEnabled {
		presence = newBulkPresenceSource(
			&natsPresenceRequester{nc: nc.NatsConn()},
			cfg.SiteID,
			cfg.PresenceBatchSize,
			cfg.PresenceRPCTimeout,
		)
	}

	handler := NewHandler(HandlerDeps{
		Members:            memberLookup,
		Followers:          newMongoThreadFollowers(threadRoomCol),
		Presence:           presence,
		Hook:               noopVetoer{},
		Emitter:            emitter,
		RoomMeta:           roomMetaCache,
		LargeRoomThreshold: cfg.LargeRoomThreshold,
		RecipientBatchSize: cfg.PushRecipientBatchSize,
	})

	// Bounded worker drains the channel so slow Valkey doesn't block NATS dispatch;
	// drops are safe because TTLs reconcile staleness.
	invalCtx, invalCancel := context.WithCancel(ctx)
	invalCh := make(chan string, 256)
	var invalWG sync.WaitGroup
	invalWG.Add(1)
	go func() {
		defer invalWG.Done()
		for roomID := range invalCh {
			memberLookup.Invalidate(invalCtx, roomID)
		}
	}()

	// Mute is the only canonical member event still on this stream; add/remove invalidation rides on MESSAGES_CANONICAL sys-messages.
	// DeliverNewPolicy: skip history on restart; roomsubcache TTL reconciles any boundary staleness.
	roomsCfg := stream.Rooms(cfg.SiteID)
	invalCons, err := otelJS.CreateOrUpdateConsumer(ctx, roomsCfg.Name, jetstream.ConsumerConfig{
		Durable:       "notification-worker-room-event-invalidate",
		FilterSubject: subject.RoomCanonicalMemberEvent(cfg.SiteID, model.CanonicalMemberEventMuted),
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})
	if err != nil {
		slog.Error("create canonical member event consumer failed", "error", err)
		os.Exit(1)
	}
	invalIter, err := invalCons.Messages(jetstream.PullMaxMessages(64))
	if err != nil {
		slog.Error("canonical member event iterator failed", "error", err)
		os.Exit(1)
	}
	go func() {
		for {
			_, msg, err := invalIter.Next()
			if err != nil {
				return
			}
			var evt model.CanonicalMemberEvent
			if err := json.Unmarshal(msg.Data(), &evt); err != nil {
				slog.Warn("canonical member event decode failed", "error", err)
				_ = msg.Ack()
				continue
			}
			if evt.RoomID != "" {
				select {
				case invalCh <- evt.RoomID:
				default:
					slog.Warn("invalidation queue full, dropping (TTL will reconcile)", "roomId", evt.RoomID)
				}
			}
			_ = msg.Ack()
		}
	}()

	iter, err := cons.Messages(jetstream.PullMaxMessages(2 * cfg.MaxWorkers))
	if err != nil {
		slog.Error("messages failed", "error", err)
		os.Exit(1)
	}

	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup

	go func() {
		for {
			msgCtx, msg, err := iter.Next()
			if err != nil {
				return
			}
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() {
					<-sem
					wg.Done()
				}()
				handlerCtx, _ := natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())
				if err := handler.HandleMessage(handlerCtx, msg.Data()); err != nil {
					slog.Error("handle message failed", "error", err, "request_id", natsutil.RequestIDFromContext(handlerCtx))
					if err := msg.Nak(); err != nil {
						slog.Error("failed to nak message", "error", err)
					}
					return
				}
				if err := msg.Ack(); err != nil {
					slog.Error("failed to ack message", "error", err)
				}
			}()
		}
	}()

	slog.Info("notification-worker started",
		"site", cfg.SiteID,
		"large_room_threshold", cfg.LargeRoomThreshold,
		"push_recipient_batch_size", cfg.PushRecipientBatchSize,
		"valkey_addrs", cfg.ValkeyAddrs,
		"presence_enabled", cfg.PresenceEnabled,
	)

	shutdown.Wait(ctx, 25*time.Second,
		func(_ context.Context) error {
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
		func(_ context.Context) error {
			invalIter.Stop()
			return nil
		},
		func(stepCtx context.Context) error {
			close(invalCh) // stop accepting work; worker drains the buffer
			done := make(chan struct{})
			go func() { invalWG.Wait(); close(done) }()
			select {
			case <-done:
			case <-stepCtx.Done():
				invalCancel() // unblock an in-flight Valkey DEL so the worker exits
				<-done
			}
			invalCancel() // always release the context (idempotent)
			return nil
		},
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(_ context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(_ context.Context) error { valkeyutil.Disconnect(valkeyClient); return nil },
	)
}

// buildConsumerConfig returns the durable consumer config for notification-worker.
func buildConsumerConfig(s stream.ConsumerSettings) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "notification-worker"
	return cc
}
