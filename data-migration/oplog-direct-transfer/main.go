package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
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
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(cfg.LogLevel)})))

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "oplog-direct-transfer")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}
	meterShutdown, err := otelutil.InitMeter("oplog-direct-transfer")
	if err != nil {
		slog.Error("init meter failed", "error", err)
		os.Exit(1)
	}
	m, err := newMetrics()
	if err != nil {
		slog.Error("init metrics failed", "error", err)
		os.Exit(1)
	}

	metricsServer := newMetricsServer()
	ln, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		slog.Error("metrics listen failed", "addr", cfg.MetricsAddr, "error", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("metrics+health server listening", "addr", cfg.MetricsAddr)
		if err := metricsServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", "error", err)
		}
	}()

	source, err := mongoutil.Connect(ctx, cfg.SourceMongoURI, cfg.SourceUsername, cfg.SourcePassword)
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

	targetClient, err := mongoutil.Connect(ctx, cfg.TargetMongoURI, cfg.TargetUsername, cfg.TargetPassword)
	if err != nil {
		slog.Error("target mongo connect failed", "error", err)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}

	collections := make(map[string]struct{}, len(cfg.DirectCollections))
	lookups := make(map[string]migration.SourceLookup, len(cfg.DirectCollections))
	filterSubjects := make([]string, 0, len(cfg.DirectCollections))
	for _, coll := range cfg.DirectCollections {
		collections[coll] = struct{}{}
		lookups[coll] = migration.NewMongoSourceLookup(sourceDB.Collection(coll, options.Collection().SetReadPreference(rp)))
		filterSubjects = append(filterSubjects, subject.MigrationOplog(cfg.SiteID, coll, "*"))
	}

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}
	js, err := oteljetstream.New(nc)
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
		collections: collections,
		lookups:     lookups,
		target:      NewMongoTargetStore(targetClient.Database(cfg.TargetDB)),
		metrics:     m,
	}

	streamName := stream.MigrationOplog(cfg.SiteID).Name
	cons, err := createConsumerWithRetry(ctx, js, streamName, jetstream.ConsumerConfig{
		Durable:        cfg.ConsumerDurable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		MaxDeliver:     cfg.MaxDeliver,
		FilterSubjects: filterSubjects,
	})
	if err != nil {
		slog.Error("create consumer failed", "stream", streamName, "error", err)
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}

	cc, err := cons.Consume(func(msg oteljetstream.Msg) {
		processOne(msg.Context(), h, msg, m, cfg.MaxDeliver)
	})
	if err != nil {
		slog.Error("consume failed", "stream", streamName, "error", err)
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}

	slog.Info("oplog-direct-transfer started", "site", cfg.SiteID, "stream", streamName, "collections", cfg.DirectCollections)

	shutdown.Wait(ctx, 25*time.Second,
		func(context.Context) error { cc.Stop(); return nil },
		func(ctx context.Context) error { return metricsServer.Shutdown(ctx) },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(ctx context.Context) error { return meterShutdown(ctx) },
		func(context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, targetClient); return nil },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, source); return nil },
	)
}

// processOne decodes one event and maps its handler outcome to a JetStream disposition.
func processOne(ctx context.Context, h *handler, m jetstream.Msg, mtr *metrics, maxDeliver int) {
	ctx, reqID := natsutil.StampRequestID(ctx, m.Headers(), m.Subject())
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
	var numDelivered uint64
	if meta, err := m.Metadata(); err == nil {
		numDelivered = meta.NumDelivered
	}
	isFinal := migration.IsFinalDelivery(numDelivered, maxDeliver)
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
		dispose("ack", m.Ack)
	case migration.ActionTermExhausted:
		slog.Error("delivery limit reached — terming (dropping)", "eventId", ev.EventID, "op", ev.Op, "cap", maxDeliver, "error", err, "request_id", reqID)
		mtr.onExhausted(ctx, ev.Op, ev.Collection)
		dispose("term", m.Term)
	default:
		slog.Error("transient failure — nak", "eventId", ev.EventID, "error", err, "request_id", reqID)
		mtr.onNak(ctx, ev.Op, ev.Collection)
		dispose("nak", func() error { return m.NakWithDelay(2 * time.Second) })
	}
}

// streamWaitTimeout bounds how long startup waits for the connector to bootstrap MIGRATION_OPLOG.
const streamWaitTimeout = 60 * time.Second

//nolint:gocritic // hugeParam: cfg passed by value to match jetstream.CreateOrUpdateConsumer's signature.
func createConsumerWithRetry(ctx context.Context, js oteljetstream.JetStream, streamName string, cfg jetstream.ConsumerConfig) (oteljetstream.Consumer, error) {
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

func newMetricsServer() *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
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

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
