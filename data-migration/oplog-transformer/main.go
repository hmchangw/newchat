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

	client, err := mongoutil.Connect(ctx, cfg.SourceMongoURI, cfg.SourceUsername, cfg.SourcePassword, mongoutil.WithObservability(sdk))
	if err != nil {
		slog.Error("source mongo connect failed", "error", err)
		os.Exit(1)
	}

	rp, err := readPreference(cfg.SourceReadPreference)
	if err != nil {
		slog.Error("read preference invalid", "error", err)
		mongoutil.Disconnect(ctx, client)
		os.Exit(1)
	}
	sourceColl := client.Database(cfg.SourceDB).
		Collection(cfg.SourceMessageCollection, options.Collection().SetReadPreference(rp))

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		mongoutil.Disconnect(ctx, client)
		os.Exit(1)
	}
	js, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, client)
		os.Exit(1)
	}

	h := &handler{
		collection:     cfg.SourceMessageCollection,
		softDeleteType: cfg.SoftDeleteType,
		publisher:      &canonicalPublisher{siteID: cfg.SiteID, publish: js.PublishMsg, now: nowMs},
		history:        &natsHistoryClient{nc: nc.NatsConn(), siteID: cfg.SiteID, timeout: cfg.HistoryRequestTimeout, metrics: m},
		lookup:         migration.NewMongoSourceLookup(sourceColl),
		metrics:        m,
	}

	streamName := stream.MigrationOplog(cfg.SiteID).Name
	// The connector owns MIGRATION_OPLOG and may bootstrap it slightly after we start.
	// Wait for the stream to appear rather than crash-loop on "stream not found".
	cons, err := createConsumerWithRetry(ctx, js, streamName, jetstream.ConsumerConfig{
		Durable:       cfg.ConsumerDurable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxDeliver:    cfg.MaxDeliver,
		FilterSubject: subject.MigrationOplog(cfg.SiteID, cfg.SourceMessageCollection, "*"),
	})
	if err != nil {
		slog.Error("create consumer failed", "stream", streamName, "error", err)
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, client)
		os.Exit(1)
	}

	cc, err := cons.Consume(ctx, func(msgCtx context.Context, msg jetstream.Msg) {
		processOne(msgCtx, h, msg, m, cfg.MaxDeliver, cfg.DeleteMaxDeliver)
	})
	if err != nil {
		slog.Error("consume failed", "stream", streamName, "error", err)
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, client)
		os.Exit(1)
	}

	slog.Info("oplog-transformer started",
		"site", cfg.SiteID, "stream", streamName, "collection", cfg.SourceMessageCollection)

	// Ordered, timeout-bounded cleanup: stop consume → health → NATS drain →
	// Mongo → flush observability LAST so all teardown telemetry exports.
	shutdown.Wait(ctx, 25*time.Second,
		func(context.Context) error { cc.Stop(); return nil },
		func(ctx context.Context) error { return healthStop(ctx) },
		func(context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, client); return nil },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// processOne decodes one event and dispatches it, mapping the outcome to a JetStream disposition:
// Ack on success, Term on poison (never redelivered), Nak-with-delay on transient up to maxDeliver
// — then Termed with a distinct metric instead of JetStream's silent drop (see migration.IsFinalDelivery).
func processOne(ctx context.Context, h *handler, m jetstream.Msg, mtr *metrics, maxDeliver, deleteMaxDeliver int) {
	// Stamp a correlation id once at entry; it flows via ctx into the history RPC and canonical publish
	// (both read it from ctx through natsutil.NewMsg), so transformer→history→worker shares one request_id.
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
		mtr.onTerm(ctx, "unknown")
		dispose("term", m.Term)
		return
	}
	// Hard deletes get a shorter cap: a foreign-origin one can't be recognised (no doc) and would
	// otherwise churn to the global MaxDeliver; the local race needs only seconds to converge.
	deliverCap := maxDeliver
	if ev.Op == "delete" {
		deliverCap = deleteMaxDeliver
	}
	// Resolve delivery count; a Metadata error prefers Nak over a premature Term.
	var numDelivered uint64
	if meta, err := m.Metadata(); err == nil {
		numDelivered = meta.NumDelivered
	}
	isFinal := migration.IsFinalDelivery(numDelivered, deliverCap)
	err := h.handle(ctx, ev)
	switch migration.Classify(err, isFinal) {
	case migration.ActionAck:
		mtr.onProcessed(ctx, ev.Op)
		dispose("ack", m.Ack)
	case migration.ActionTerm:
		slog.Error("poison event — term (skipping)", "eventId", ev.EventID, "error", err, "request_id", reqID)
		mtr.onTerm(ctx, ev.Op)
		dispose("term", m.Term)
	case migration.ActionAckSkip:
		// A deliberate skip — already metered via onSkipped by the handler. Ack but DON'T count
		// it as processed (that would double-count the same event).
		dispose("ack", m.Ack)
	case migration.ActionTermExhausted:
		// A further Nak would hit the cap and be silently dropped by JetStream.
		// Term it explicitly so the give-up is logged + metered instead of vanishing.
		slog.Error("delivery limit reached — terming (dropping)", "eventId", ev.EventID, "op", ev.Op, "cap", deliverCap, "error", err, "request_id", reqID)
		mtr.onExhausted(ctx, ev.Op)
		dispose("term", m.Term)
	default:
		slog.Error("transient failure — nak", "eventId", ev.EventID, "error", err, "request_id", reqID)
		mtr.onNak(ctx, ev.Op)
		dispose("nak", func() error { return m.NakWithDelay(2 * time.Second) })
	}
}

func nowMs() int64 { return time.Now().UTC().UnixMilli() }

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
