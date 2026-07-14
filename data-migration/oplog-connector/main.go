package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"
	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
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

	tracerShutdown, err := otelutil.InitTracer(ctx, "oplog-connector")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}
	meterShutdown, err := otelutil.InitMeter("oplog-connector")
	if err != nil {
		slog.Error("init meter failed", "error", err)
		os.Exit(1)
	}
	// role distinguishes the two split deployments in logs and metrics (PR #482 review).
	role := "collections"
	if cfg.watchesMessages() {
		role = "messages"
	}
	m, err := newMetrics(role)
	if err != nil {
		slog.Error("init metrics failed", "error", err)
		os.Exit(1)
	}

	// Bind synchronously so a port conflict fails startup loudly rather than
	// running blind — observability is the stall signal for this single-replica pump.
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

	conn, err := start(ctx, &cfg, m)
	if err != nil {
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
	slog.Info("oplog-connector started", "site", cfg.SiteID, "role", role, "collections", cfg.WatchCollections)
	if cfg.watchesMessages() {
		slog.Info("federation-origin filter active", "role", role, "message_collection", cfg.MessageCollection)
	} else {
		// Warn, not Info: legitimate for a collections-role pod, but conspicuous when a MESSAGE_COLLECTION
		// typo leaves a message pod forwarding foreign-origin messages unfiltered (double-deliver).
		slog.Warn("no message collection watched — federation-origin filter inactive",
			"role", role, "message_collection", cfg.MessageCollection)
	}

	// A fatal watcher error (e.g. lost resume token) exits non-zero without waiting
	// for a signal — recovery is operator-driven. Also exits on Done(), so no leak.
	go func() {
		select {
		case err := <-conn.Fatal():
			if err != nil {
				slog.Error("fatal watcher error — exiting", "error", err)
				_ = tracerShutdown(context.Background())
				_ = meterShutdown(context.Background())
				conn.Close()
				os.Exit(1)
			}
		case <-conn.Done():
		}
	}()

	// Ordered, timeout-bounded cleanup:
	// stop readers → drain watchers → metrics/health → observability → NATS → Mongo.
	shutdown.Wait(ctx, 25*time.Second,
		func(context.Context) error { conn.beginShutdown(); return nil },
		func(ctx context.Context) error { return conn.awaitWatchers(ctx) },
		func(ctx context.Context) error { return metricsServer.Shutdown(ctx) },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(ctx context.Context) error { return meterShutdown(ctx) },
		func(context.Context) error { return conn.nc.Drain() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, conn.client); return nil },
	)
}

// newMetricsServer builds the /metrics + /healthz HTTP server with timeouts that guard against hung scrapers tying up goroutines.
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

// connector owns the running watchers and the connections they share. Close stops watchers (flushing final checkpoints), then drains NATS, then Mongo.
type connector struct {
	client *mongo.Client
	nc     *otelnats.Conn
	cancel context.CancelFunc
	wg     sync.WaitGroup
	fatal  chan error
	done   chan struct{}
	once   sync.Once
}

// start connects Mongo + NATS, bootstraps the stream, and launches one watcher per collection. Returns a running connector driven via Fatal()/Close().
func start(ctx context.Context, cfg *config, m *metrics) (*connector, error) {
	if cfg.StartResumeToken != "" || cfg.StartAtTime != "" {
		// One-off seed overrides: left set, they force a reseed (ignoring the checkpoint)
		// on every restart — so warn loudly. Prefer a seed checkpoint doc.
		slog.Warn("START_RESUME_TOKEN/START_AT_TIME is set — ignoring any stored checkpoint and reseeding; unset after first start to resume from the checkpoint",
			"startResumeTokenSet", cfg.StartResumeToken != "", "startAtTime", cfg.StartAtTime)
	}

	client, err := mongoutil.Connect(ctx, cfg.SourceMongoURI, cfg.SourceUsername, cfg.SourcePassword)
	if err != nil {
		return nil, fmt.Errorf("source mongo connect: %w", err)
	}

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		mongoutil.Disconnect(ctx, client)
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	js, err := oteljetstream.New(nc)
	if err != nil {
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, client)
		return nil, fmt.Errorf("jetstream init: %w", err)
	}
	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, client)
		return nil, fmt.Errorf("bootstrap streams: %w", err)
	}

	rp, err := readPreference(cfg.ReadPreference)
	if err != nil {
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, client)
		return nil, err
	}

	store := NewMongoCheckpointStore(client.Database(cfg.CheckpointDB).Collection(checkpointCollection), cfg.SiteID)
	sourceDB := client.Database(cfg.SourceDB)

	watchCtx, cancel := context.WithCancel(context.Background())
	c := &connector{
		client: client,
		nc:     nc,
		cancel: cancel,
		fatal:  make(chan error, len(cfg.WatchCollections)),
		done:   make(chan struct{}),
	}
	checkpointMaxAge := time.Duration(cfg.CheckpointMaxAgeSeconds) * time.Second

	for _, coll := range cfg.WatchCollections {
		cp, err := store.Load(ctx, coll)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("load checkpoint %q: %w", coll, err)
		}
		sp, err := resolveStartPoint(cfg, cp)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("resolve start point %q: %w", coll, err)
		}
		mongoColl := sourceDB.Collection(coll,
			options.Collection().SetReadPreference(rp).SetReadConcern(readconcern.Majority()))
		src, err := openMongoChangeSource(watchCtx, mongoColl, sp, coll == cfg.MessageCollection)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("open change stream %q: %w", coll, err)
		}

		w := newWatcher(cfg.SiteID, coll, src, js, store, cfg.CheckpointEvery, checkpointMaxAge)
		w.metrics = m
		c.wg.Add(1)
		go func(w *watcher) {
			defer c.wg.Done()
			if err := w.run(watchCtx); err != nil {
				c.fatal <- err
				cancel() // one fatal watcher tears the whole connector down
			}
		}(w)
	}

	return c, nil
}

// Fatal delivers the first fatal watcher error, if any.
func (c *connector) Fatal() <-chan error { return c.fatal }

// Done is closed on shutdown so a Fatal() watcher can stop instead of blocking forever.
func (c *connector) Done() <-chan struct{} { return c.done }

// beginShutdown signals every watcher to stop (idempotent); each flushes its final checkpoint.
func (c *connector) beginShutdown() {
	c.once.Do(func() {
		close(c.done)
		c.cancel()
	})
}

// awaitWatchers blocks until every watcher has exited and flushed, or ctx is done.
func (c *connector) awaitWatchers(ctx context.Context) error {
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("watcher drain timed out: %w", ctx.Err())
	}
}

// Close runs the full teardown in order — used by the fatal-exit path and
// integration tests. main()'s signal path runs the same steps via shutdown.Wait.
func (c *connector) Close() {
	c.beginShutdown()
	wctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.awaitWatchers(wctx); err != nil {
		slog.Warn("watcher drain incomplete", "error", err)
	}
	_ = c.nc.Drain()
	mongoutil.Disconnect(context.Background(), c.client)
}

func readPreference(s string) (*readpref.ReadPref, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "primary":
		return readpref.Primary(), nil
	case "primarypreferred":
		return readpref.PrimaryPreferred(), nil
	case "secondary", "":
		return readpref.Secondary(), nil
	case "secondarypreferred":
		return readpref.SecondaryPreferred(), nil
	case "nearest":
		return readpref.Nearest(), nil
	default:
		return nil, fmt.Errorf("invalid READ_PREFERENCE: %s", s)
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
