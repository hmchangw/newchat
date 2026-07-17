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
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/idgen"
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
	groups, err := parseSyncGroups(cfg.SyncGroups)
	if err != nil {
		return fmt.Errorf("parse sync groups: %w", err)
	}

	// SIGTERM/SIGINT (pod deletion, Job activeDeadlineSeconds) cancels the run
	// so it aborts between operations instead of being killed mid-batch.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	readClient, err := mongoutil.ConnectRead(ctx, cfg.MongoReadURI, cfg.MongoReadUsername, cfg.MongoReadPassword)
	if err != nil {
		return fmt.Errorf("connect mongo read client: %w", err)
	}
	defer disconnect(readClient)

	// One-shot job: no obs.Init — a noop tracer keeps natsutil's wiring happy
	// without dragging the full observability stack into a CronJob binary.
	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, noop.NewTracerProvider(), propagation.TraceContext{})
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc.NatsConn())
	if err != nil {
		return fmt.Errorf("init jetstream: %w", err)
	}

	var opts []msgraph.Option
	if cfg.GraphBaseURL != "" {
		opts = append(opts, msgraph.WithBaseURL(cfg.GraphBaseURL))
	}
	if cfg.GraphTokenURL != "" {
		opts = append(opts, msgraph.WithTokenURL(cfg.GraphTokenURL))
	}
	graph := msgraph.NewGroupReaderClient(msgraph.Config{
		TenantID:     cfg.TeamsTenantID,
		ClientID:     cfg.TeamsClientID,
		ClientSecret: cfg.TeamsClientSecret,
	}, opts...)

	store := newMongoStore(readClient.Database(cfg.MongoReadDB))
	// Injection point: swap DefaultMapper / DefaultConverter for custom
	// naming or derivation conventions (see teams-hr-sync/README.md).
	mapper := transform.DefaultMapper{OrgType: cfg.OrgType}
	pub := newPublisher(func(ctx context.Context, subj string, data []byte) error {
		_, err := js.Publish(ctx, subj, data)
		return err
	}, cfg.CentralSiteID, transform.DefaultConverter{})

	logger := slog.With("requestId", idgen.GenerateRequestID())
	logger.Info("teams hr sync started")
	start := time.Now()
	stats, err := runSync(ctx, graph, mapper, store, pub, groups, cfg.GraphPageSize)
	logger.Info("teams hr sync finished",
		"groups", stats.Groups,
		"members", stats.Members,
		"skippedNonUser", stats.SkippedObj,
		"invalidUpn", stats.InvalidUPN,
		"dupAccounts", stats.DupAccount,
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

// runStats summarizes one sync run for the end-of-run log line.
type runStats struct {
	collectStats
	Created   int // rows absent from the store
	Updated   int // rows whose fields changed
	Quits     int // departed accounts across all sites
	Published int // JetStream messages sent
}

// runSync performs one full sync: walk the configured groups, diff against
// the persisted teams-sourced rows, publish the delta.
func runSync(ctx context.Context, graph msgraph.GroupReader, mapper transform.Mapper, store Store, pub *publisher, groups []syncGroup, pageSize int) (runStats, error) {
	var stats runStats
	current, cs, err := collectEmployees(ctx, graph, mapper, groups, pageSize)
	stats.collectStats = cs
	if err != nil {
		return stats, fmt.Errorf("collect graph employees: %w", err)
	}
	stored, err := store.ListTeamsEmployees(ctx)
	if err != nil {
		return stats, fmt.Errorf("list stored employees: %w", err)
	}
	diff := diffEmployees(current, stored)
	for _, u := range diff.Upserts {
		if u.Change == transform.ChangeCreated {
			stats.Created++
		} else {
			stats.Updated++
		}
	}
	for _, accounts := range diff.Quits {
		stats.Quits += len(accounts)
	}
	published, err := pub.publishSync(ctx, diff)
	stats.Published = published
	if err != nil {
		return stats, fmt.Errorf("publish sync batches: %w", err)
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
