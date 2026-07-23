package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
)

type config struct {
	NatsURL            string `env:"NATS_URL,required"`
	NatsCredsFile      string `env:"NATS_CREDS_FILE"`
	SiteID             string `env:"SITE_ID,required"`
	MaxWorkers         int    `env:"MAX_WORKERS" envDefault:"100"`
	MaxDeliver         int    `env:"MAX_DELIVER" envDefault:"5"`
	HealthAddr         string `env:"HEALTH_ADDR" envDefault:":8081"`
	PProfEnabled       bool   `env:"PPROF_ENABLED" envDefault:"false"`
	InputStream        string `env:"INPUT_STREAM,required"`
	InputSubjectFilter string `env:"INPUT_SUBJECT_FILTER,required"`
	ConsumerName       string `env:"CONSUMER_NAME" envDefault:"push-notification-service"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("push-notification-service exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("init jetstream: %w", err)
	}

	h := newHandler(LogDispatcher{})

	cons, err := js.CreateOrUpdateConsumer(ctx, cfg.InputStream, jetstream.ConsumerConfig{
		Durable:       cfg.ConsumerName,
		FilterSubject: cfg.InputSubjectFilter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    cfg.MaxDeliver,
		BackOff:       []time.Duration{1 * time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second},
	})
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(2*cfg.MaxWorkers))
	if err != nil {
		return fmt.Errorf("messages iter: %w", err)
	}

	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup
	go func() {
		for {
			mCtx, msg, err := iter.Next()
			if err != nil {
				return
			}
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() { <-sem; wg.Done() }()
				h.HandleJetStreamMsg(mCtx, msg)
			}()
		}
	}()

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		return fmt.Errorf("health server: %w", err)
	}

	slog.Info("push-notification-service running", "site", cfg.SiteID)
	shutdown.Wait(ctx, 25*time.Second,
		func(_ context.Context) error { iter.Stop(); return nil },
		func(dctx context.Context) error {
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				return nil
			case <-dctx.Done():
				return fmt.Errorf("worker drain: %w", dctx.Err())
			}
		},
		func(_ context.Context) error { return nc.Drain() },
		func(dctx context.Context) error { return healthStop(dctx) },
		func(dctx context.Context) error { return obsShutdown(dctx) },
	)
	return nil
}
