package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// runAllCollections runs the three collection functions and joins every
// error they return — a failure in one collection must not prevent the
// others from running, and the run's exit code must reflect all of them,
// not just the first.
func runAllCollections(ctx context.Context, runMsgs, runSpot, runUserRoom func(context.Context) error) error {
	return errors.Join(runMsgs(ctx), runSpot(ctx), runUserRoom(ctx))
}

// run is main's testable body: it returns an error instead of calling
// os.Exit, so its wiring can be exercised by an integration test.
//
//nolint:gocritic // hugeParam: cfg is passed by value to match runMessages/runSpotlight/runUserRoom's existing API contract; struct copy overhead is acceptable, not a hot path
func run(ctx context.Context, cfg config) error {
	cassSession, err := cassutil.Connect(cassutil.Config{
		Hosts: cfg.CassandraHosts, Keyspace: cfg.CassandraKeyspace,
		Username: cfg.CassandraUsername, Password: cfg.CassandraPassword, NumConns: cfg.CassandraNumConns,
	})
	if err != nil {
		return fmt.Errorf("cassandra connect: %w", err)
	}
	defer cassutil.Close(cassSession)

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("mongodb connect: %w", err)
	}
	defer func() { _ = mongoClient.Disconnect(ctx) }()
	db := mongoClient.Database(cfg.MongoDB)

	engine, err := searchengine.New(ctx, searchengine.Config{
		Backend: "elasticsearch", URL: cfg.SearchURL, Username: cfg.SearchUsername, Password: cfg.SearchPassword,
		TLSSkipVerify: cfg.SearchTLSSkipVerify,
	})
	if err != nil {
		return fmt.Errorf("elasticsearch connect: %w", err)
	}

	if err := bootstrapPrerequisites(ctx, engine, &cfg); err != nil {
		return fmt.Errorf("bootstrap prerequisites: %w", err)
	}

	bucketSizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)
	subs := newMongoSubscriptionSource(db)
	messages := newCassandraMessageSource(cassSession, bucketSizer)

	msgFlusher := newFlusher(engine, cfg.BulkBatchSize)
	spotlightFlusher := newFlusher(engine, cfg.BulkBatchSize)
	userRoomFlusher := newFlusher(engine, cfg.BulkBatchSize)

	runErr := runAllCollections(ctx,
		func(ctx context.Context) error { return runMessages(ctx, subs, messages, msgFlusher, cfg) },
		func(ctx context.Context) error { return runSpotlight(ctx, subs, spotlightFlusher, cfg) },
		func(ctx context.Context) error { return runUserRoom(ctx, subs, userRoomFlusher, cfg) },
	)

	failed := msgFlusher.FailedCount() + spotlightFlusher.FailedCount() + userRoomFlusher.FailedCount()
	slog.Info("migration run complete",
		"site", cfg.SiteID, "failedBulkItems", failed, "runError", runErr != nil)

	if runErr != nil {
		return fmt.Errorf("migration run: %w", runErr)
	}
	if failed > 0 {
		return fmt.Errorf("migration run: %d bulk items failed", failed)
	}
	return nil
}

// main has no defer of its own — os.Exit only runs from here, never past a
// live defer — so the cleanup in realMain (stop()) always fires before the
// process exits.
func main() {
	os.Exit(realMain())
}

// realMain is main's os.Exit-free body so its deferred signal-context
// cleanup always runs; it returns the process exit code instead of calling
// os.Exit directly (gocritic's exitAfterDefer would flag a defer followed
// by an in-function os.Exit as dead cleanup).
func realMain() int {
	logctx.SetupDefault(os.Stdout)

	cfg, err := loadConfig()
	if err != nil {
		slog.Error("load config", "error", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		slog.Error("es-index-migrator failed", "error", err)
		return 1
	}
	slog.Info("es-index-migrator completed successfully", "site", cfg.SiteID)
	return 0
}
