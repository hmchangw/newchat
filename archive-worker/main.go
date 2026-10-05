package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/drive"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/loopguard"
	"github.com/hmchangw/chat/pkg/minioutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// Durable consumer names, one per lane.
const (
	eventsDurable  = "archive-worker-events"
	membersDurable = "archive-worker-members"
	blobsDurable   = "archive-worker-blobs"
)

func main() {
	if err := run(); err != nil {
		slog.Error("archive-worker exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	if w := checkBatchAckCoupling(cfg.BatchEvents, cfg.Consumer.MaxAckPending, cfg.Replicas); w != "" {
		slog.Warn("batch/ack-pending coupling", "detail", w)
	}

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}
	m, err := newMetrics()
	if err != nil {
		return fmt.Errorf("create metrics: %w", err)
	}

	engine, err := searchengine.New(ctx, searchengine.Config{
		Backend:       cfg.SearchBackend,
		URL:           cfg.SearchURL,
		Username:      cfg.SearchUsername,
		Password:      cfg.SearchPassword,
		TLSSkipVerify: cfg.SearchTLSSkipVerify,
	}, searchengine.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect archive search: %w", err)
	}
	if err := bootstrapIndex(ctx, engine, cfg.SiteID, cfg.IndexRetention, cfg.DevMode); err != nil {
		return fmt.Errorf("bootstrap archive index: %w", err)
	}

	// The object-lock check needs the raw minio client; the sink itself goes
	// through minioutil so its calls are traced.
	lockClient, err := minio.New(cfg.S3Endpoint, &minio.Options{Creds: credentials.NewStaticV4(cfg.S3AccessKey, cfg.S3SecretKey, ""), Secure: cfg.S3UseSSL})
	if err != nil {
		return fmt.Errorf("create archive bucket client: %w", err)
	}
	if err := checkObjectLock(ctx, lockClient, cfg.Bucket, cfg.RequireObjectLock); err != nil {
		return fmt.Errorf("check archive bucket object lock: %w", err)
	}
	objClient, err := minioutil.Connect(ctx, cfg.S3Endpoint, cfg.S3UseSSL, cfg.S3AccessKey, cfg.S3SecretKey, minioutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect archive bucket: %w", err)
	}
	objects := newBucketSink(objClient, cfg.Bucket)

	wrapper, err := atrest.NewVaultKeyWrapper(ctx, cfg.Vault)
	if err != nil {
		return fmt.Errorf("create vault key wrapper: %w", err)
	}
	dek, err := loadOrCreateDEK(ctx, engine, wrapper, cfg.SiteID, time.Now)
	if err != nil {
		return fmt.Errorf("load archive DEK: %w", err)
	}
	cipher, err := auditarchive.NewCipher(dek)
	if err != nil {
		return fmt.Errorf("create archive cipher: %w", err)
	}

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("create jetstream context: %w", err)
	}
	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		return fmt.Errorf("bootstrap streams: %w", err)
	}

	// Armed before any lane starts: a lane that dies raises SIGTERM on this
	// process, and that must reach WaitOn rather than the default handler.
	sig := shutdown.Signals()

	// mkConsumer creates one durable and returns the config the server was
	// given, so callers wire MaxDeliver from the applied value.
	mkConsumer := func(streamName, durable string, filters []string, settings stream.ConsumerSettings) (msgFetcher, jetstream.ConsumerConfig, error) {
		cc := consumerConfig(durable, filters, settings)
		cons, err := js.CreateOrUpdateConsumer(ctx, streamName, cc)
		if err != nil {
			return nil, cc, fmt.Errorf("create consumer %s on %s: %w", durable, streamName, err)
		}
		return o11yConsumerAdapter{c: cons}, cc, nil
	}

	eventsGuard := loopguard.New("events-lane", loopguard.SelfShutdown)
	membersGuard := loopguard.New("members-lane", loopguard.SelfShutdown)
	blobsGuard := loopguard.New("blobs-lane", loopguard.SelfShutdown)

	// Events and members drop poison themselves (Term), so redelivery of a
	// failing write is unlimited rather than capped by a delivery count.
	laneSettings := stream.WithUnlimitedRedelivery(cfg.Consumer)
	eventsFetcher, _, err := mkConsumer(stream.MessagesCanonical(cfg.SiteID).Name, eventsDurable, []string{subject.MsgCanonicalMessageWildcard(cfg.SiteID)}, laneSettings)
	if err != nil {
		return err
	}
	membersFetcher, _, err := mkConsumer(stream.Inbox(cfg.SiteID).Name, membersDurable, subject.InboxMemberEventSubjects(cfg.SiteID), laneSettings)
	if err != nil {
		return err
	}

	flushCfg := flushConfig{putTimeout: cfg.PutTimeout, bulkTimeout: cfg.BulkTimeout, attempts: cfg.WriteAttempts}
	laneCfg := func(name string) laneConfig {
		return laneConfig{site: cfg.SiteID, name: name, fetchBatch: cfg.FetchBatch, fillInterval: cfg.FillInterval, now: time.Now, metrics: m}
	}
	events := newLane(laneCfg("events"), eventsFetcher, buildEventItem, cipher,
		newBatcher(cfg.BatchEvents, cfg.BatchBytes, cfg.FillInterval), newFlusher(objects, engine, flushCfg, m), eventsGuard)
	members := newLane(laneCfg("members"), membersFetcher, buildMemberItem, cipher,
		newBatcher(cfg.BatchEvents, cfg.BatchBytes, cfg.FillInterval), newFlusher(objects, engine, flushCfg, m), membersGuard)

	lanes := newLaneGroup()
	lanes.start(ctx, events.run)
	lanes.start(ctx, members.run)

	checks := []health.Check{natsutil.HealthCheck(nc), eventsGuard.Check(), membersGuard.Check()}
	guards := []*loopguard.Guard{eventsGuard, membersGuard}
	if cfg.BlobsEnabled {
		blobSettings := blobConsumerSettings(&cfg)
		blobsFetcher, blobCC, err := mkConsumer(stream.MessagesCanonical(cfg.SiteID).Name, blobsDurable, []string{subject.MsgCanonicalCreated(cfg.SiteID)}, blobSettings)
		if err != nil {
			lanes.stopAll()
			return err
		}
		cfg.Drive.LoadBaseURLs()
		blobs := newBlobLane(newBlobLaneConfig(&cfg, blobSettings, &blobCC), blobsFetcher, &driveSource{client: drive.NewClient(&cfg.Drive)}, objects, engine, cipher, blobsGuard, m)
		lanes.start(ctx, blobs.run)
		checks = append(checks, blobsGuard.Check())
		guards = append(guards, blobsGuard)
	}

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled, checks...)
	if err != nil {
		lanes.stopAll()
		return fmt.Errorf("start health server: %w", err)
	}
	slog.Info("archive-worker started", "site", cfg.SiteID, "bucket", cfg.Bucket, "blobs", cfg.BlobsEnabled)

	shutdown.WaitOn(ctx, sig, 25*time.Second,
		// First: the deliberate stop below must not read as a lane death.
		func(context.Context) error {
			for _, g := range guards {
				g.BeginShutdown()
			}
			return nil
		},
		func(context.Context) error { lanes.stopAll(); return nil },
		lanes.wait,
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(context.Context) error { return wrapper.Close() },
		func(ctx context.Context) error { return healthStop(ctx) },
		// Last, so spans and logs from the drain window are exported.
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
	return nil
}

// consumerConfig builds a durable's config; backoff is derived from settings
// by stream.DurableConsumerDefaults, never hardcoded here.
func consumerConfig(durable string, filters []string, settings stream.ConsumerSettings) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(settings)
	cc.Durable = durable
	cc.FilterSubjects = filters
	return cc
}

// blobConsumerSettings is the blob lane's consumer: its own AckWait (a Drive
// transfer is slow) and a bounded outage retry budget rather than unlimited
// redelivery. The budget is derived against jsretry.DefaultBackoff because
// that is the schedule the blob lane settles with.
func blobConsumerSettings(cfg *config) stream.ConsumerSettings {
	s := cfg.Consumer
	s.AckWait = cfg.BlobAckWait
	return stream.WithOutageRetryBudget(s, jsretry.DefaultBackoff)
}

// newBlobLaneConfig wires the blob lane from the consumer actually created:
// MaxDeliver is the applied value, and the heartbeat paces off the deadline
// the server enforces, not the configured field.
func newBlobLaneConfig(cfg *config, settings stream.ConsumerSettings, cc *jetstream.ConsumerConfig) blobLaneConfig {
	return blobLaneConfig{
		site:         cfg.SiteID,
		maxBytes:     cfg.BlobMaxBytes,
		chunkBytes:   auditarchive.DefaultChunkBytes,
		workers:      cfg.BlobWorkers,
		ackWait:      settings.EffectiveAckWait(),
		heartbeatMax: cfg.Consumer.HeartbeatMax,
		maxDeliver:   cc.MaxDeliver,
		now:          time.Now,
	}
}

// laneGroup owns the lifecycle of the consume loops: every started lane is
// told to stop through one channel and reports completion on its own.
type laneGroup struct {
	stop     chan struct{}
	stopOnce sync.Once
	mu       sync.Mutex
	done     []chan struct{}
}

func newLaneGroup() *laneGroup { return &laneGroup{stop: make(chan struct{})} }

func (g *laneGroup) start(ctx context.Context, run func(context.Context, <-chan struct{}, chan<- struct{})) {
	done := make(chan struct{})
	g.mu.Lock()
	g.done = append(g.done, done)
	g.mu.Unlock()
	go run(ctx, g.stop, done)
}

// stopAll asks every lane to drain and return; safe to call more than once.
func (g *laneGroup) stopAll() { g.stopOnce.Do(func() { close(g.stop) }) }

// wait blocks until every started lane has returned or ctx expires.
func (g *laneGroup) wait(ctx context.Context) error {
	g.mu.Lock()
	done := append([]chan struct{}(nil), g.done...)
	g.mu.Unlock()
	for _, ch := range done {
		select {
		case <-ch:
		case <-ctx.Done():
			return fmt.Errorf("lane drain timed out: %w", ctx.Err())
		}
	}
	return nil
}
