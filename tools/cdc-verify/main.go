package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/gocql/gocql"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

type config struct {
	SiteID    string `env:"SITE_ID,required"`
	NATSURL   string `env:"NATS_URL,required"`
	CredsFile string `env:"NATS_CREDS_FILE" envDefault:""`

	SourceMongoURI      string `env:"SOURCE_MONGO_URI,required"`
	SourceMongoUsername string `env:"SOURCE_MONGO_USERNAME" envDefault:""`
	SourceMongoPassword string `env:"SOURCE_MONGO_PASSWORD" envDefault:""`
	SourceDB            string `env:"SOURCE_DB" envDefault:"rocketchat"`

	TargetMongoURI      string `env:"TARGET_MONGO_URI,required"`
	TargetMongoUsername string `env:"TARGET_MONGO_USERNAME" envDefault:""`
	TargetMongoPassword string `env:"TARGET_MONGO_PASSWORD" envDefault:""`
	TargetDB            string `env:"TARGET_DB" envDefault:"chat"`

	// Cassandra is required only when the mapping references it — enforced after mapping load.
	CassandraHosts    string `env:"CASSANDRA_HOSTS" envDefault:""`
	CassandraKeyspace string `env:"CASSANDRA_KEYSPACE" envDefault:""`
	CassandraUsername string `env:"CASSANDRA_USERNAME" envDefault:""`
	CassandraPassword string `env:"CASSANDRA_PASSWORD" envDefault:""`

	MappingFile        string          `env:"MAPPING_FILE,required"`
	Bootstrap          bootstrapConfig `envPrefix:"BOOTSTRAP_"`
	MessageBucketHours int             `env:"MESSAGE_BUCKET_HOURS" envDefault:"72"`
	TrackConsumers     []string        `env:"TRACK_CONSUMERS" envDefault:""`
	StartAtTime        string          `env:"START_AT_TIME" envDefault:""`
	VerifyPoll         time.Duration   `env:"VERIFY_POLL" envDefault:"2s"`
	VerifyTimeout      time.Duration   `env:"VERIFY_TIMEOUT" envDefault:"60s"`
	MaxChecks          int             `env:"MAX_CHECKS" envDefault:"32"`
	SamplePercent      int             `env:"SAMPLE_PERCENT" envDefault:"100"`
	RecentCap          int             `env:"RECENT_CAP" envDefault:"200"`
	FailedCap          int             `env:"FAILED_CAP" envDefault:"1000"`
	StatsInterval      time.Duration   `env:"STATS_INTERVAL" envDefault:"5s"`
	Port               int             `env:"PORT" envDefault:"8091"`
}

func (c *config) validate() error {
	if c.SamplePercent < 0 || c.SamplePercent > 100 {
		return fmt.Errorf("SAMPLE_PERCENT must be 0..100, got %d", c.SamplePercent)
	}
	if c.VerifyPoll <= 0 {
		return fmt.Errorf("VERIFY_POLL must be positive, got %s", c.VerifyPoll)
	}
	if c.VerifyTimeout < c.VerifyPoll {
		return fmt.Errorf("VERIFY_TIMEOUT (%s) must be >= VERIFY_POLL (%s)", c.VerifyTimeout, c.VerifyPoll)
	}
	if c.MaxChecks <= 0 {
		return fmt.Errorf("MAX_CHECKS must be positive, got %d", c.MaxChecks)
	}
	if c.RecentCap <= 0 || c.FailedCap <= 0 {
		return fmt.Errorf("RECENT_CAP and FAILED_CAP must be positive")
	}
	if c.MessageBucketHours <= 0 {
		return fmt.Errorf("MESSAGE_BUCKET_HOURS must be positive, got %d", c.MessageBucketHours)
	}
	if c.StartAtTime != "" {
		if _, err := time.Parse(time.RFC3339, c.StartAtTime); err != nil {
			return fmt.Errorf("START_AT_TIME must be RFC3339: %w", err)
		}
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
	if err := cfg.validate(); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}
	if cfg.CredsFile != "" {
		if _, err := os.Stat(cfg.CredsFile); err != nil {
			slog.Error("nats creds file not accessible", "path", cfg.CredsFile, "error", err)
			os.Exit(1)
		}
	}

	mapping, err := loadMapping(cfg.MappingFile)
	if err != nil {
		slog.Error("load mapping", "error", err)
		os.Exit(1)
	}
	if mapping.NeedsCassandra() && (cfg.CassandraHosts == "" || cfg.CassandraKeyspace == "") {
		slog.Error("mapping references cassandra targets but CASSANDRA_HOSTS/CASSANDRA_KEYSPACE are unset")
		os.Exit(1)
	}

	// cancel is deferred only after the last os.Exit-guarded block: os.Exit skips
	// defers, so an earlier defer is dead cleanup (gocritic exitAfterDefer).
	ctx, cancel := context.WithCancel(context.Background())

	// --- connections (fail fast, read-only use) ---
	natsOpts := []nats.Option{nats.Name("cdc-verify")}
	if cfg.CredsFile != "" {
		natsOpts = append(natsOpts, nats.UserCredentials(cfg.CredsFile))
	}
	nc, err := nats.Connect(cfg.NATSURL, natsOpts...)
	if err != nil {
		slog.Error("connect nats", "error", err)
		os.Exit(1)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("create jetstream context", "error", err)
		os.Exit(1)
	}
	streamName := stream.MigrationOplog(cfg.SiteID).Name
	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams", "error", err)
		os.Exit(1)
	}
	s, err := js.Stream(ctx, streamName)
	if err != nil {
		slog.Error("open stream", "stream", streamName, "error", err)
		os.Exit(1)
	}

	srcClient, err := mongoutil.Connect(ctx, cfg.SourceMongoURI, cfg.SourceMongoUsername, cfg.SourceMongoPassword,
		mongoutil.WithReadPreference(readpref.PrimaryPreferred()))
	if err != nil {
		slog.Error("connect source mongo", "error", err)
		os.Exit(1)
	}
	tgtClient, err := mongoutil.Connect(ctx, cfg.TargetMongoURI, cfg.TargetMongoUsername, cfg.TargetMongoPassword)
	if err != nil {
		slog.Error("connect target mongo", "error", err)
		os.Exit(1)
	}

	var cass CassStore
	var cassSession *gocql.Session
	if mapping.NeedsCassandra() {
		cassSession, err = cassutil.Connect(cassutil.Config{
			Hosts: cfg.CassandraHosts, Keyspace: cfg.CassandraKeyspace,
			Username: cfg.CassandraUsername, Password: cfg.CassandraPassword,
		})
		if err != nil {
			slog.Error("connect cassandra", "error", err)
			os.Exit(1)
		}
		cass = newCassStore(cassSession)
	}

	defer cancel()

	// --- pipeline ---
	sizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)
	reg := newTransformRegistry(sizer)
	sseHub := newHub()
	results := newResultsStore(cfg.RecentCap, cfg.FailedCap, sseHub.broadcastResult)
	v := newVerifier(mapping,
		newMongoStore(srcClient.Database(cfg.SourceDB)),
		newMongoStore(tgtClient.Database(cfg.TargetDB)),
		cass, reg, results, verifierConfig{
			Poll: cfg.VerifyPoll, Timeout: cfg.VerifyTimeout,
			MaxChecks: cfg.MaxChecks, SamplePercent: cfg.SamplePercent,
		})

	var startAt time.Time
	if cfg.StartAtTime != "" {
		startAt, _ = time.Parse(time.RFC3339, cfg.StartAtTime) // validated already
	}
	w := newWatcher(js, streamName, startAt, v)
	go func() {
		if err := w.Run(ctx); err != nil {
			slog.Error("watcher stopped", "error", err)
			os.Exit(1) // a dead feed makes the dashboard lie; die loudly
		}
	}()

	filter := subject.MigrationOplogWildcard(cfg.SiteID)
	poller := newStatsPoller(streamName,
		func(ctx context.Context) (*jetstream.StreamInfo, error) {
			return s.Info(ctx, jetstream.WithSubjectFilter(filter))
		},
		func(ctx context.Context, name string) (*jetstream.ConsumerInfo, error) {
			c, err := s.Consumer(ctx, name)
			if err != nil {
				return nil, fmt.Errorf("open consumer %s: %w", name, err)
			}
			return c.Info(ctx)
		},
		cfg.TrackConsumers, cfg.StatsInterval, w.Live, sseHub.broadcastStats)
	go poller.Run(ctx)

	// --- HTTP ---
	h := newHandler(sseHub, results, poller, cfg.RecentCap, mapping.TargetPairs(), v)
	mux := http.NewServeMux()
	h.registerRoutes(mux)
	srv := &http.Server{
		Addr:        fmt.Sprintf(":%d", cfg.Port),
		Handler:     mux,
		ReadTimeout: 30 * time.Second,
		// WriteTimeout deliberately omitted — SSE connections are long-lived.
		IdleTimeout: 60 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "error", err)
			os.Exit(1)
		}
	}()

	slog.Info("cdc-verify started", "port", cfg.Port, "stream", streamName,
		"site", cfg.SiteID, "sample_percent", cfg.SamplePercent)

	shutdown.Wait(context.Background(), 25*time.Second,
		func(sctx context.Context) error { return srv.Shutdown(sctx) },
		func(sctx context.Context) error { cancel(); v.Shutdown(sctx); return nil },
		func(_ context.Context) error { return nc.Drain() },
		func(sctx context.Context) error {
			mongoutil.Disconnect(sctx, srcClient)
			mongoutil.Disconnect(sctx, tgtClient)
			if cassSession != nil {
				cassutil.Close(cassSession)
			}
			return nil
		},
	)
}
