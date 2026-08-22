package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jsiter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
)

type config struct {
	NatsURL       string                  `env:"NATS_URL,required"`
	NatsCredsFile string                  `env:"NATS_CREDS_FILE"`
	SiteID        string                  `env:"SITE_ID,required"`
	MaxWorkers    int                     `env:"MAX_WORKERS" envDefault:"100"`
	Consumer      stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	HealthAddr    string                  `env:"HEALTH_ADDR" envDefault:":8081"`
	PProfEnabled  bool                    `env:"PPROF_ENABLED" envDefault:"false"`
	Mode          stream.Pipeline         `env:"MODE,required"` // user | bot; drives all stream/subject wiring via pkg/stream.Resolve

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

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("init jetstream: %w", err)
	}

	h := newHandler(LogDispatcher{})

	wiring := stream.Resolve(cfg.Mode, cfg.SiteID)

	consumerCfg := buildConsumerConfig(cfg.Consumer, cfg.Mode, wiring.PushInputWildcard)
	// The consumer is re-resolved on every iterator build, not captured once: an
	// iterator the server stopped usually means the durable itself is gone, and
	// rebuilding onto a stale handle would just stop again.
	newIter := func(ctx context.Context) (jsiter.Iterator, error) {
		cons, err := js.CreateOrUpdateConsumer(ctx, wiring.PushStream.Name, consumerCfg)
		if err != nil {
			return nil, fmt.Errorf("create push consumer: %w", err)
		}
		iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(2*cfg.MaxWorkers))
		if err != nil {
			return nil, fmt.Errorf("open push message iterator: %w", err)
		}
		return iter, nil
	}
	// jsiter.Pump absorbs the errors that leave the iterator usable (a missed idle
	// heartbeat) and rebuilds the ones that do not, so an error out of Next below
	// means consumption is genuinely over.
	iter, err := jsiter.New(ctx, consumerCfg.Durable, newIter)
	if err != nil {
		return fmt.Errorf("messages iter: %w", err)
	}

	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup
	go func() {
		for {
			mCtx, msg, err := iter.Next()
			if err != nil {
				// The pump only reports an error it could not recover from, so
				// this is the end of consumption — never a passing hiccup.
				if !errors.Is(err, jsiter.ErrStopped) {
					slog.ErrorContext(ctx, "consumer loop stopped", "error", err)
				}
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

	// A live NATS connection says nothing about whether this consumer is still
	// receiving — that gap is what let a dead pump keep reporting healthy — so
	// the pump's own state is probed alongside it.
	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
		iter.HealthCheck(),
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
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(dctx context.Context) error { return healthStop(dctx) },
		func(dctx context.Context) error { return obsShutdown(dctx) },
	)
	return nil
}

// buildConsumerConfig adds the durable name and filter; everything else comes
// from ConsumerSettings.
func buildConsumerConfig(s stream.ConsumerSettings, mode stream.Pipeline, filterSubject string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = mode.ConsumerName("push-notification-service")
	cc.FilterSubject = filterSubject
	return cc
}
