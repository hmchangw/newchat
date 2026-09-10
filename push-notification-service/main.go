package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
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

	wiring := stream.Resolve(cfg.Mode, cfg.SiteID)
	// HasFailover gates the bot pipeline out of the buddy lane; it also keeps
	// the home dial fail-fast there, since without a buddy a pod that cannot
	// reach home has nothing to do. With a buddy the dial is lazy, so a pod
	// that restarts while home is down still boots and serves the buddy lane.
	dialer := natsutil.NewBuddyDialer(cfg.Buddy.OnlyIf(wiring.HasFailover()), cfg.NatsCredsFile, sdk)
	nc, js, err := dialer.ConnectHomeJS(ctx, cfg.NatsURL, nil)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}

	h := newHandler(LogDispatcher{})

	// APNs and FCM are external and unaffected by a site's NATS outage, so the
	// one handler serves both lanes: nothing in it speaks NATS.
	lanes, err := failoverlane.BindLanes(ctx, nc, js, dialer, &failoverlane.LanesSpec{
		SiteID: cfg.SiteID, MaxWorkers: cfg.MaxWorkers,
		Home: failoverlane.HomeSpec{
			Stream: wiring.PushStream,
			Consumer: buildConsumerConfig(cfg.Consumer, cfg.Mode.ConsumerName("push-notification-service"),
				wiring.PushInputWildcard),
		},
		Buddy: &failoverlane.LaneSpec{
			Stream: wiring.PushFailoverStream,
			// notification-worker owns the push stream and asserts its
			// placement; binding here is this service's existence check.
			Ownership: failoverlane.BorrowsStreams,
			Consumer: buildConsumerConfig(cfg.Consumer,
				cfg.Mode.FailoverConsumerName("push-notification-service"),
				wiring.PushFailoverInputWildcard),
		},
	}, func(context.Context, *o11ynats.Conn, o11ynats.JetStream, subject.Lane) (func(context.Context, jetstream.Msg), error) {
		return h.HandleJetStreamMsg, nil
	})
	if err != nil {
		return fmt.Errorf("bind lanes: %w", err)
	}

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
		lanes.Check(),
	)
	if err != nil {
		return fmt.Errorf("health server: %w", err)
	}

	slog.Info("push-notification-service running", "site", cfg.SiteID)
	hooks := append(lanes.StopHooks(), lanes.DrainHooks()...)
	hooks = append(hooks,
		func(dctx context.Context) error { return healthStop(dctx) },
		func(dctx context.Context) error { return obsShutdown(dctx) },
	)
	shutdown.Wait(ctx, 25*time.Second, hooks...)
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
