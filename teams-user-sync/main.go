package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/robfig/cron/v3"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/pkg/shutdown"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	// Graph rejects $top outside 1..999; fail fast on a bad knob.
	if cfg.GraphPageSize <= 0 || cfg.GraphPageSize > 999 {
		return fmt.Errorf("GRAPH_PAGE_SIZE must be in 1..999, got %d", cfg.GraphPageSize)
	}

	ctx := context.Background()

	readClient, err := mongoutil.ConnectRead(ctx, cfg.MongoReadURI, cfg.MongoReadUsername, cfg.MongoReadPassword)
	if err != nil {
		return fmt.Errorf("connect mongo read client: %w", err)
	}
	writeClient, err := mongoutil.Connect(ctx, cfg.MongoWriteURI, cfg.MongoWriteUsername, cfg.MongoWritePassword)
	if err != nil {
		return fmt.Errorf("connect mongo write client: %w", err)
	}

	store := newMongoStore(readClient.Database(cfg.MongoReadDB), writeClient.Database(cfg.MongoWriteDB))
	lister := msgraph.NewUserListerClient(&msgraph.Config{
		TenantID:     cfg.TeamsTenantID,
		ClientID:     cfg.TeamsClientID,
		ClientSecret: cfg.TeamsClientSecret,
	})
	syncer := NewSyncer(store, lister, cfg.GraphPageSize)

	// One guarded job shared by the schedule and the optional on-start run,
	// so "skip if the previous job is not yet finished" holds across both.
	job := guardedJob(func() { runSync(syncer) })

	c := cron.New(cron.WithLogger(cronSlogLogger{}))
	if _, err := c.AddJob(cfg.SyncCron, job); err != nil {
		return fmt.Errorf("register sync cron %q: %w", cfg.SyncCron, err)
	}
	c.Start()
	slog.Info("sync scheduled", "cron", cfg.SyncCron, "runOnStart", cfg.RunOnStart)

	// The on-start run is not tracked by cron's Stop context, so track it
	// ourselves and wait for it during shutdown.
	var startupRun sync.WaitGroup
	if cfg.RunOnStart {
		startupRun.Add(1)
		go func() {
			defer startupRun.Done()
			job.Run()
		}()
	}

	stopHealth, err := health.Serve(cfg.HealthAddr, 5*time.Second,
		health.Check{Name: "mongo-read", Probe: func(ctx context.Context) error { return readClient.Ping(ctx, nil) }},
		health.Check{Name: "mongo-write", Probe: func(ctx context.Context) error { return writeClient.Ping(ctx, nil) }},
	)
	if err != nil {
		return fmt.Errorf("start health listener: %w", err)
	}

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error {
			stopCtx := c.Stop() // done when the in-flight scheduled job (if any) finishes
			startupDone := make(chan struct{})
			go func() { startupRun.Wait(); close(startupDone) }()
			select {
			case <-stopCtx.Done():
			case <-ctx.Done():
				return fmt.Errorf("stop cron: in-flight sync did not finish before timeout")
			}
			select {
			case <-startupDone:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("stop cron: on-start sync did not finish before timeout")
			}
		},
		stopHealth,
		func(ctx context.Context) error {
			mongoutil.Disconnect(ctx, readClient)
			mongoutil.Disconnect(ctx, writeClient)
			return nil
		},
	)
	return nil
}
