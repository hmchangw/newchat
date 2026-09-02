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

	"github.com/hmchangw/chat/pkg/failoverlane"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

type config struct {
	NatsURL       string                  `env:"NATS_URL,required"`
	NatsCredsFile string                  `env:"NATS_CREDS_FILE"`
	SiteID        string                  `env:"SITE_ID,required"`
	MaxWorkers    int                     `env:"MAX_WORKERS" envDefault:"100"`
	Consumer      stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Buddy         natsutil.BuddyConfig    `envPrefix:"BUDDY_"`
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

	cons, err := js.CreateOrUpdateConsumer(ctx, wiring.PushStream.Name,
		buildConsumerConfig(cfg.Consumer, cfg.Mode.ConsumerName("push-notification-service"),
			wiring.PushInputWildcard))
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

	// Buddy lane. Never fails startup — on any failure buddyLane stays nil and
	// the service runs home-only. HasFailover gates the bot pipeline out.
	//
	// APNs and FCM are external and unaffected by a site's NATS outage, so the
	// one handler serves both lanes: nothing in it speaks NATS.
	binder := failoverlane.Binder{
		SiteID: cfg.SiteID, MaxWorkers: cfg.MaxWorkers, Sem: sem, WG: &wg,
		Dialer: &natsutil.BuddyDialer{
			Config: cfg.Buddy.OnlyIf(wiring.HasFailover()), CredsFile: cfg.NatsCredsFile,
			TracerProvider: sdk.TracerProvider(), Propagator: sdk.Propagator, TracingEnabled: sdk.Toggles.Trace,
		},
	}
	buddyLane, buddyConn := binder.BindLane(ctx, &failoverlane.LaneSpec{
		Stream: wiring.PushFailoverStream,
		// notification-worker owns the push stream and asserts its placement;
		// binding here is this service's existence check.
		Ownership: failoverlane.BorrowsStreams,
		Consumer: buildConsumerConfig(cfg.Consumer,
			cfg.Mode.FailoverConsumerName("push-notification-service"),
			wiring.PushFailoverInputWildcard),
	}, func(context.Context, *o11ynats.Conn, o11ynats.JetStream, subject.Lane) (func(context.Context, jetstream.Msg), error) {
		return h.HandleJetStreamMsg, nil
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

// buildConsumerConfig adds the durable name and filter; everything else comes
// from ConsumerSettings. The durable is a parameter rather than derived from the
// pipeline so the home and buddy lanes share one builder and differ only in the
// durable and filter — a shared durable would have them clobber each other's
// cursor on a single-server dev NATS.
func buildConsumerConfig(s stream.ConsumerSettings, durable, filterSubject string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = durable
	cc.FilterSubject = filterSubject
	return cc
}
