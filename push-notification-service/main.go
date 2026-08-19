package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
)

type config struct {
	NatsURL       string               `env:"NATS_URL,required"`
	NatsCredsFile string               `env:"NATS_CREDS_FILE"`
	SiteID        string               `env:"SITE_ID,required"`
	MaxWorkers    int                  `env:"MAX_WORKERS" envDefault:"100"`
	MaxDeliver    int                  `env:"MAX_DELIVER" envDefault:"5"`
	Buddy         natsutil.BuddyConfig `envPrefix:"BUDDY_"`
	HealthAddr    string               `env:"HEALTH_ADDR" envDefault:":8081"`
	PProfEnabled  bool                 `env:"PPROF_ENABLED" envDefault:"false"`
	Mode          stream.Pipeline      `env:"MODE,required"` // user | bot; drives all stream/subject wiring via pkg/stream.Resolve

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

	cons, err := js.CreateOrUpdateConsumer(ctx, wiring.PushStream.Name,
		buildConsumerConfig(cfg.Mode.ConsumerName("push-notification-service"),
			wiring.PushInputWildcard, cfg.MaxDeliver))
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(2*cfg.MaxWorkers))
	if err != nil {
		return fmt.Errorf("messages iter: %w", err)
	}

	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup
	natsutil.RunPool(iter, sem, &wg, h.HandleJetStreamMsg)

	// Buddy lane. BindBuddy never fails startup — on any failure buddyIter stays
	// nil and the service runs home-only. HasFailover gates the bot pipeline out
	// without a mode check here.
	//
	// APNs and FCM are external and unaffected by a site's NATS outage, so a
	// push built from a failover-lane request goes out exactly as usual.
	binder := natsutil.FailoverBinder{
		Service: "push-notification-service", SiteID: cfg.SiteID, Buddy: cfg.Buddy,
		MaxWorkers: cfg.MaxWorkers, Sem: sem, WG: &wg,
	}
	var buddyLane *natsutil.Lane
	buddyConn := natsutil.BindBuddy(ctx, cfg.Buddy.OnlyIf(wiring.HasFailover()), cfg.NatsCredsFile,
		sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace,
		func(ctx context.Context, bconn *o11ynats.Conn, bjs o11ynats.JetStream) error {
			var bErr error
			buddyLane, bErr = binder.Bind(ctx, bjs, &natsutil.LaneSpec{
				Stream: wiring.PushFailoverStream,
				// notification-worker owns the push stream and asserts its
				// placement; binding here is this service's existence check.
				Ownership: natsutil.BorrowsStreams,
				Consumer: buildConsumerConfig(cfg.Mode.FailoverConsumerName("push-notification-service"),
					wiring.PushFailoverInputWildcard, cfg.MaxDeliver),
			}, h.HandleJetStreamMsg)
			return bErr
		})

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		return fmt.Errorf("health server: %w", err)
	}

	slog.Info("push-notification-service running", "site", cfg.SiteID)
	shutdown.Wait(ctx, 25*time.Second,
		// Stop both iterators before draining, so neither lane pulls new work
		// while the other is still finishing. Both feed one WaitGroup.
		func(_ context.Context) error {
			iter.Stop()
			buddyLane.Stop()
			return nil
		},
		func(dctx context.Context) error { return natsutil.WaitPool(dctx, &wg) },
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		natsutil.DrainBuddy(buddyConn),
		func(dctx context.Context) error { return healthStop(dctx) },
		func(dctx context.Context) error { return obsShutdown(dctx) },
	)
	return nil
}

// buildConsumerConfig is the durable consumer config for a push lane. Extracted
// so the home and buddy lanes share identical ack policy, wait and retry
// backoff — the only differences are the durable name and the filter.
func buildConsumerConfig(durable, filterSubject string, maxDeliver int) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    maxDeliver,
		BackOff:       []time.Duration{1 * time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second},
	}
}
