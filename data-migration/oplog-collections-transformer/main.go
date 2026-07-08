package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

func main() {
	cfg, err := parseConfig()
	if err != nil {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	// Surface an empty ALL_SITE_IDS once at startup: user statusText changes won't propagate
	// (publishUserStatus skips with a per-event metric). Legitimate for a rooms/subs-only partial
	// deployment; a misconfig otherwise. (Future: promote to a hard-fail once the modes are known.)
	if !hasDestinationSite(cfg.AllSiteIDs) {
		slog.Warn("ALL_SITE_IDS is empty — user status fan-out is disabled (intentional only for a partial deployment)")
	}

	m, err := newMetrics()
	if err != nil {
		slog.Error("init metrics failed", "error", err)
		os.Exit(1)
	}

	// Bind synchronously so a port conflict fails startup loudly. Metrics are
	// owned by the o11y SDK's Prometheus endpoint; this is health-only.
	healthStop, err := health.Serve(cfg.HealthAddr, 5*time.Second)
	if err != nil {
		slog.Error("health server failed to start", "addr", cfg.HealthAddr, "error", err)
		os.Exit(1)
	}

	// Source legacy Mongo: re-read full current docs by _id on update events.
	source, err := mongoutil.Connect(ctx, cfg.SourceMongoURI, cfg.SourceUsername, cfg.SourcePassword, mongoutil.WithObservability(sdk))
	if err != nil {
		slog.Error("source mongo connect failed", "error", err)
		os.Exit(1)
	}
	rp, err := readPreference(cfg.SourceReadPreference)
	if err != nil {
		slog.Error("read preference invalid", "error", err)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}
	sourceDB := source.Database(cfg.SourceDB)
	lookups := map[string]migration.SourceLookup{
		cfg.RoomsCollection:         migration.NewMongoSourceLookup(sourceDB.Collection(cfg.RoomsCollection, options.Collection().SetReadPreference(rp))),
		cfg.SubscriptionsCollection: migration.NewMongoSourceLookup(sourceDB.Collection(cfg.SubscriptionsCollection, options.Collection().SetReadPreference(rp))),
		cfg.ThreadSubsCollection:    migration.NewMongoSourceLookup(sourceDB.Collection(cfg.ThreadSubsCollection, options.Collection().SetReadPreference(rp))),
		cfg.UsersCollection:         migration.NewMongoSourceLookup(sourceDB.Collection(cfg.UsersCollection, options.Collection().SetReadPreference(rp))),
		cfg.RoomMembersCollection:   migration.NewMongoSourceLookup(sourceDB.Collection(cfg.RoomMembersCollection, options.Collection().SetReadPreference(rp))),
	}

	// Target new-stack per-site Mongo: user insert-if-absent + thread_room/user FK resolution.
	targetClient, err := mongoutil.Connect(ctx, cfg.TargetMongoURI, cfg.TargetUsername, cfg.TargetPassword, mongoutil.WithObservability(sdk))
	if err != nil {
		slog.Error("target mongo connect failed", "error", err)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}
	target := NewMongoTargetStore(targetClient.Database(cfg.TargetDB))
	if err := target.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure target indexes failed", "error", err)
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}
	js, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}

	h := &handler{
		siteID:          cfg.SiteID,
		allSiteIDs:      cfg.AllSiteIDs,
		roomsColl:       cfg.RoomsCollection,
		subsColl:        cfg.SubscriptionsCollection,
		threadSubsColl:  cfg.ThreadSubsCollection,
		usersColl:       cfg.UsersCollection,
		roomMembersColl: cfg.RoomMembersCollection,
		pub:             &jetstreamPublisher{publish: js.PublishMsg},
		target:          target,
		lookups:         lookups,
		metrics:         m,
		now:             nowMs,
	}

	streamName := stream.MigrationOplog(cfg.SiteID).Name
	// The connector owns MIGRATION_OPLOG and may bootstrap it slightly after we start.
	// Wait for the stream to appear rather than crash-loop on "stream not found".
	cons, err := createConsumerWithRetry(ctx, js, streamName, jetstream.ConsumerConfig{
		Durable:       cfg.ConsumerDurable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxDeliver:    cfg.MaxDeliver,
		FilterSubjects: []string{
			subject.MigrationOplog(cfg.SiteID, cfg.RoomsCollection, "*"),
			subject.MigrationOplog(cfg.SiteID, cfg.SubscriptionsCollection, "*"),
			subject.MigrationOplog(cfg.SiteID, cfg.ThreadSubsCollection, "*"),
			subject.MigrationOplog(cfg.SiteID, cfg.UsersCollection, "*"),
			subject.MigrationOplog(cfg.SiteID, cfg.RoomMembersCollection, "*"),
		},
	})
	if err != nil {
		slog.Error("create consumer failed", "stream", streamName, "error", err)
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}

	cc, err := cons.Consume(ctx, func(msgCtx context.Context, msg jetstream.Msg) {
		processOne(msgCtx, h, msg, m, cfg.MaxDeliver, cfg.DeleteMaxDeliver)
	})
	if err != nil {
		slog.Error("consume failed", "stream", streamName, "error", err)
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}

	slog.Info("oplog-collections-transformer started", "site", cfg.SiteID, "stream", streamName)

	// Ordered, timeout-bounded cleanup: stop consume → health → NATS drain →
	// Mongo (target then source) → flush observability LAST so all teardown
	// telemetry exports.
	shutdown.Wait(ctx, 25*time.Second,
		func(context.Context) error { cc.Stop(); return nil },
		func(ctx context.Context) error { return healthStop(ctx) },
		func(context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, targetClient); return nil },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, source); return nil },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// deliverCapFor selects the redelivery cap. Deletes get the short deleteMaxDeliver (their race
// converges in seconds) — except room-member deletes, which really write and must survive a target outage.
func (h *handler) deliverCapFor(op, collection string, maxDeliver, deleteMaxDeliver int) int {
	if op == "delete" && collection != h.roomMembersColl {
		return deleteMaxDeliver
	}
	return maxDeliver
}

// processOne decodes one event and maps its outcome to a JetStream disposition: Ack on success,
// Term on poison, Nak-with-delay on transient up to maxDeliver, then Term-with-metric (not silent drop).
func processOne(ctx context.Context, h *handler, m jetstream.Msg, mtr *metrics, maxDeliver, deleteMaxDeliver int) {
	// Stamp a correlation id once at entry; it flows via ctx into the inbox publish
	// (read from ctx through natsutil.NewMsg), so transformer→inbox-worker shares one request_id.
	ctx, reqID := natsutil.StampRequestID(ctx, m.Headers(), m.Subject())
	// dispose runs a JetStream ack/term/nak and logs (rather than silently drops) any failure —
	// the message will redeliver, but a failing disposition signals a NATS-health problem worth seeing.
	dispose := func(action string, fn func() error) {
		if derr := fn(); derr != nil {
			slog.Error("jetstream disposition failed", "action", action, "error", derr, "request_id", reqID)
		}
	}
	var ev oplogEvent
	if err := json.Unmarshal(m.Data(), &ev); err != nil {
		slog.Error("decode oplog event — term", "error", err, "request_id", reqID)
		mtr.onTerm(ctx, "unknown", "unknown")
		dispose("term", m.Term)
		return
	}
	deliverCap := h.deliverCapFor(ev.Op, ev.Collection, maxDeliver, deleteMaxDeliver)
	// Resolve delivery count; a Metadata error prefers Nak over a premature Term.
	var numDelivered uint64
	if meta, err := m.Metadata(); err == nil {
		numDelivered = meta.NumDelivered
	}
	isFinal := migration.IsFinalDelivery(numDelivered, deliverCap)
	err := h.handle(ctx, ev)
	switch migration.Classify(err, isFinal) {
	case migration.ActionAck:
		mtr.onProcessed(ctx, ev.Op, ev.Collection)
		dispose("ack", m.Ack)
	case migration.ActionTerm:
		slog.Error("poison event — term (skipping)", "eventId", ev.EventID, "error", err, "request_id", reqID)
		mtr.onTerm(ctx, ev.Op, ev.Collection)
		dispose("term", m.Term)
	case migration.ActionAckSkip:
		// A deliberate skip — already metered via onSkipped by the handler. Ack but DON'T count
		// it as processed (that would double-count the same event).
		dispose("ack", m.Ack)
	case migration.ActionTermExhausted:
		// A further Nak would hit the cap and be silently dropped by JetStream.
		// Term it explicitly so the give-up is logged + metered instead of vanishing.
		slog.Error("delivery limit reached — terming (dropping)", "eventId", ev.EventID, "op", ev.Op, "cap", deliverCap, "error", err, "request_id", reqID)
		mtr.onExhausted(ctx, ev.Op, ev.Collection)
		dispose("term", m.Term)
	default:
		slog.Error("transient failure — nak", "eventId", ev.EventID, "error", err, "request_id", reqID)
		mtr.onNak(ctx, ev.Op, ev.Collection)
		dispose("nak", func() error { return m.NakWithDelay(2 * time.Second) })
	}
}

func nowMs() int64 { return time.Now().UTC().UnixMilli() }

// hasDestinationSite reports whether sites has at least one non-empty entry (a real fan-out target).
func hasDestinationSite(sites []string) bool {
	for _, s := range sites {
		if s != "" {
			return true
		}
	}
	return false
}

// streamWaitTimeout bounds how long startup waits for the connector to bootstrap MIGRATION_OPLOG.
const streamWaitTimeout = 60 * time.Second

// createConsumerWithRetry creates the durable consumer, retrying while the stream does not yet exist
// (the connector creates it independently); other errors and streamWaitTimeout are returned.
//
//nolint:gocritic // hugeParam: cfg is passed by value to match jetstream.CreateOrUpdateConsumer's signature.
func createConsumerWithRetry(ctx context.Context, js o11ynats.JetStream, streamName string, cfg jetstream.ConsumerConfig) (o11ynats.Consumer, error) {
	deadline := time.Now().Add(streamWaitTimeout)
	for {
		cons, err := js.CreateOrUpdateConsumer(ctx, streamName, cfg)
		if err == nil {
			return cons, nil
		}
		if !errors.Is(err, jetstream.ErrStreamNotFound) || time.Now().After(deadline) {
			return nil, err
		}
		slog.Warn("waiting for stream to be created by the connector", "stream", streamName)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func readPreference(s string) (*readpref.ReadPref, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "primary":
		return readpref.Primary(), nil
	case "primarypreferred", "":
		return readpref.PrimaryPreferred(), nil
	case "secondary":
		return readpref.Secondary(), nil
	case "secondarypreferred":
		return readpref.SecondaryPreferred(), nil
	case "nearest":
		return readpref.Nearest(), nil
	default:
		return nil, fmt.Errorf("invalid SOURCE_READ_PREFERENCE: %s", s)
	}
}
