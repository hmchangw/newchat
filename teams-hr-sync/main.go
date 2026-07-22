package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/hrstore"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

// run executes exactly one sync pass and exits — this binary is triggered by
// a Kubernetes CronJob, which owns the schedule and the skip-if-still-running
// semantics (concurrencyPolicy: Forbid). A non-nil return exits 1 so the Job
// records the failure; the next fire re-diffs against the store, so a lost
// publish self-heals.
func run() error {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	// Graph rejects $top outside 1..999; fail fast on a bad knob.
	if cfg.GraphPageSize <= 0 || cfg.GraphPageSize > 999 {
		return fmt.Errorf("GRAPH_PAGE_SIZE must be in 1..999, got %d", cfg.GraphPageSize)
	}
	if cfg.HRSyncMode != modeStream && cfg.HRSyncMode != modeDirect {
		return fmt.Errorf("HR_SYNC_MODE must be %q or %q, got %q", modeStream, modeDirect, cfg.HRSyncMode)
	}
	// DIRECT_WRITE_URI has no env "required" tag because it's conditional on
	// mode — env doesn't support cross-field required, so enforce it here.
	if cfg.HRSyncMode == modeDirect && cfg.DirectWriteURI == "" {
		return fmt.Errorf("DIRECT_WRITE_URI is required when HR_SYNC_MODE=%s", modeDirect)
	}
	groups, err := parseSyncGroups(cfg.SyncGroups)
	if err != nil {
		return fmt.Errorf("parse sync groups: %w", err)
	}
	siteOverrides, err := parseSiteOverrides(cfg.SiteOverrides)
	if err != nil {
		return fmt.Errorf("parse site overrides: %w", err)
	}

	// SIGTERM/SIGINT (pod deletion, Job activeDeadlineSeconds) cancels the run
	// so it aborts between operations instead of being killed mid-batch.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var opts []msgraph.Option
	if cfg.GraphBaseURL != "" {
		opts = append(opts, msgraph.WithBaseURL(cfg.GraphBaseURL))
	}
	if cfg.GraphTokenURL != "" {
		opts = append(opts, msgraph.WithTokenURL(cfg.GraphTokenURL))
	}
	graph := msgraph.NewGroupReaderClient(msgraph.Config{
		TenantID:              cfg.TeamsTenantID,
		ClientID:              cfg.TeamsClientID,
		ClientSecret:          cfg.TeamsClientSecret,
		TLSInsecureSkipVerify: cfg.GraphTLSInsecureSkipVerify,
	}, opts...)
	// Injection point: swap DefaultMapper / DefaultConverter for custom
	// naming or derivation conventions (see teams-hr-sync/README.md).
	mapper := transform.DefaultMapper{}

	requestID := idgen.GenerateRequestID()
	// Stamp ctx so outbound messages (natsutil.NewMsg) carry the same X-Request-ID as the logs.
	ctx = natsutil.WithRequestID(ctx, requestID)
	logger := slog.With("requestId", requestID)
	logger.Info("teams hr sync started", "mode", cfg.HRSyncMode)
	start := time.Now()

	var stats runStats
	if cfg.HRSyncMode == modeDirect {
		stats, err = runDirectMode(ctx, &cfg, graph, mapper, groups, siteOverrides)
	} else {
		stats, err = runStreamMode(ctx, &cfg, graph, mapper, groups, siteOverrides)
	}
	logger.Info("teams hr sync finished",
		"groups", stats.Groups,
		"members", stats.Members,
		"skippedNonUser", stats.SkippedObj,
		"invalidUpn", stats.InvalidUPN,
		"dupAccounts", stats.DupAccount,
		"overridden", stats.Overridden,
		"created", stats.Created,
		"updated", stats.Updated,
		"quits", stats.Quits,
		"published", stats.Published,
		"durationMs", time.Since(start).Milliseconds(),
		"succeeded", err == nil,
	)
	if err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	return nil
}

// runStreamMode connects the diff-state Mongo read + JetStream, then runs the
// existing publish-a-delta pipeline. Behavior unchanged from pre-mode-flag.
func runStreamMode(ctx context.Context, cfg *config, graph msgraph.GroupReader, mapper transform.Mapper, groups []syncGroup, siteOverrides map[string]string) (runStats, error) {
	readClient, err := mongoutil.ConnectRead(ctx, cfg.MongoReadURI, cfg.MongoReadUsername, cfg.MongoReadPassword)
	if err != nil {
		return runStats{}, fmt.Errorf("connect mongo read client: %w", err)
	}
	defer disconnect(readClient)

	// One-shot job: no obs.Init — a noop tracer keeps natsutil's wiring happy
	// without dragging the full observability stack into a CronJob binary.
	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, noop.NewTracerProvider(), propagation.TraceContext{})
	if err != nil {
		return runStats{}, fmt.Errorf("connect nats: %w", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc.NatsConn())
	if err != nil {
		return runStats{}, fmt.Errorf("init jetstream: %w", err)
	}

	store := newMongoStore(readClient.Database(cfg.MongoReadDB))
	pub := newPublisher(jetStreamPublish(js), cfg.CentralSiteID, transform.DefaultConverter{})
	return runSync(ctx, graph, mapper, store, pub, groups, siteOverrides, cfg.GraphPageSize)
}

// runDirectMode connects only the migration target Mongo (never the
// diff-state store, never NATS) and writes the full collected set straight
// through hrstore.
func runDirectMode(ctx context.Context, cfg *config, graph msgraph.GroupReader, mapper transform.Mapper, groups []syncGroup, siteOverrides map[string]string) (runStats, error) {
	writeClient, err := mongoutil.Connect(ctx, cfg.DirectWriteURI, cfg.DirectWriteUsername, cfg.DirectWritePassword)
	if err != nil {
		return runStats{}, fmt.Errorf("connect mongo direct-write client: %w", err)
	}
	defer disconnect(writeClient)

	emit := directEmitter{
		store:     hrstore.NewMongoStore(writeClient.Database(cfg.DirectWriteDB)),
		converter: transform.DefaultConverter{},
	}
	return runDirectSync(ctx, graph, mapper, emit, groups, siteOverrides, cfg.GraphPageSize)
}

// jetStreamPublish is the JetStream publishFunc main wires in. natsutil.NewMsg
// returns a nil Header when ctx carries no request-id (the one-shot-job case),
// so guard before setting the encoding header.
func jetStreamPublish(js jetstream.JetStream) publishFunc {
	return func(ctx context.Context, subj string, data []byte, encoding string) error {
		msg := natsutil.NewMsg(ctx, subj, data)
		if encoding != "" {
			if msg.Header == nil {
				msg.Header = nats.Header{}
			}
			msg.Header.Set("Nats-Encoding", encoding)
		}
		_, err := js.PublishMsg(ctx, msg)
		return err
	}
}

// runStats summarizes one sync run for the end-of-run log line.
type runStats struct {
	collectStats
	Created   int // rows absent from the store
	Updated   int // rows whose fields changed
	Quits     int // departed accounts across all sites
	Published int // JetStream messages sent
}

// runSync performs one full stream-mode sync: walk the configured groups,
// diff against the persisted teams-sourced rows, publish the delta.
func runSync(ctx context.Context, graph msgraph.GroupReader, mapper transform.Mapper, store Store, pub *publisher, groups []syncGroup, siteOverrides map[string]string, pageSize int) (runStats, error) {
	stored, err := store.ListTeamsEmployees(ctx)
	if err != nil {
		return runStats{}, fmt.Errorf("list stored employees: %w", err)
	}
	return runSyncCore(ctx, graph, mapper, stored, streamEmitter{pub}, groups, siteOverrides, pageSize)
}

// runDirectSync performs one direct-mode migration pass: walk the configured
// groups and write the FULL collected set via emit — no read of the diff-state
// Store, so every collected employee becomes an upsert (diffEmployees against
// an empty baseline) and Quits is always empty.
func runDirectSync(ctx context.Context, graph msgraph.GroupReader, mapper transform.Mapper, emit emitter, groups []syncGroup, siteOverrides map[string]string, pageSize int) (runStats, error) {
	return runSyncCore(ctx, graph, mapper, nil, emit, groups, siteOverrides, pageSize)
}

// runSyncCore is the shared walk-diff-emit pipeline; stored is the diff
// baseline (persisted teams rows for stream mode, nil for direct mode).
func runSyncCore(ctx context.Context, graph msgraph.GroupReader, mapper transform.Mapper, stored []model.IEmployee, emit emitter, groups []syncGroup, siteOverrides map[string]string, pageSize int) (runStats, error) {
	var stats runStats
	current, cs, err := collectEmployees(ctx, graph, mapper, groups, siteOverrides, pageSize)
	stats.collectStats = cs
	if err != nil {
		return stats, fmt.Errorf("collect graph employees: %w", err)
	}
	diff := diffEmployees(current, stored)
	for i := range diff.Upserts {
		if diff.Upserts[i].ChangeType == model.IChangeTypeNewHire {
			stats.Created++
		} else {
			stats.Updated++
		}
	}
	for _, accounts := range diff.Quits {
		stats.Quits += len(accounts)
	}
	published, err := emit.emit(ctx, diff)
	stats.Published = published
	if err != nil {
		return stats, fmt.Errorf("emit sync batches: %w", err)
	}
	return stats, nil
}

// disconnect closes a client under its own timeout — the run context may
// already be canceled by the time the deferred cleanup executes.
func disconnect(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mongoutil.Disconnect(ctx, client)
}
