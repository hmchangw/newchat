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
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsiter"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/outbox"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

type config struct {
	NatsURL       string `env:"NATS_URL"        envDefault:"nats://localhost:4222"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string `env:"SITE_ID"         envDefault:"site-local"`
	MaxWorkers    int    `env:"MAX_WORKERS"     envDefault:"100"`
	// AllSiteIDs is every federation site (same convention as user-service's
	// ALL_SITE_IDS; may include the local site, which is skipped). One
	// per-destination FIFO membership consumer is created per remote peer — a
	// peer missing from this list has NO membership lane and its membership
	// events sit in the OUTBOX stream unconsumed.
	AllSiteIDs   []string                `env:"ALL_SITE_IDS" envDefault:"" envSeparator:","`
	Consumer     stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap    bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	HealthAddr   string                  `env:"HEALTH_ADDR" envDefault:":8081"`
	PProfEnabled bool                    `env:"PPROF_ENABLED" envDefault:"false"`
}

func main() {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	// A non-positive MaxWorkers would deadlock dispatch (cap-0 channel) or panic
	// (negative make). Fail fast.
	if cfg.MaxWorkers <= 0 {
		slog.Error("MAX_WORKERS must be positive", "max_workers", cfg.MaxWorkers)
		os.Exit(1)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	outboxCfg := stream.Outbox(cfg.SiteID)

	// Every forward is JetStream-backed: it blocks on PubAck and the server honors
	// the msgID as Nats-Msg-Id for dedup. HandleEvent skips any target without a
	// DedupID, so msgID is always non-empty here.
	handler := NewHandler(func(ctx context.Context, subj string, data []byte, msgID string) error {
		msg := natsutil.NewMsg(ctx, subj, data)
		if _, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID)); err != nil {
			return fmt.Errorf("publish to %q: %w", subj, err)
		}
		return nil
	})

	// process is the one message disposition shared by every consumer:
	// jobguard Acks on panic (poison drop) — the callbacks run outside
	// natsrouter's Recovery middleware, so an unrecovered panic would crash the
	// worker and crash-loop on JetStream redelivery — and jsretry Ack-drops
	// permanent errors and Naks transient ones with backoff.
	process := func(msgCtx context.Context, msg jetstream.Msg) {
		jobguard.Run(msg, func() {
			handlerCtx, _ := natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())
			jsretry.Settle(handlerCtx, msg, jsretry.DefaultBackoff, handler.HandleEvent(handlerCtx, msg.Subject(), msg.Data()))
		})
	}

	// Shared bounded worker pool: every relay event is idempotent (dedup via
	// DedupID + the destination inbox-worker's high-water-mark guards), so
	// concurrent forwarding is order-safe. The pool caps total in-flight work
	// across every per-destination concurrent lane.
	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup

	// Both lanes are per remote peer (from ALL_SITE_IDS). Per destination, not a
	// single shared consumer, so a down peer's parked forwards (MaxDeliver=-1,
	// never Ack) fill only their own consumer's ack-pending budget instead of
	// stalling first-delivery to healthy peers. The ordered lane additionally
	// serializes (MaxAckPending=1) so its events cannot overtake each other; the
	// concurrent lane keeps the default budget and fans out into the shared pool.
	peers := federationPeers(cfg.SiteID, cfg.AllSiteIDs)
	if len(peers) == 0 {
		slog.Warn("no remote peers in ALL_SITE_IDS — federation events published to OUTBOX would sit unconsumed",
			"site", cfg.SiteID, "all_site_ids", cfg.AllSiteIDs)
	}
	iters := make([]*jsiter.Pump, 0, len(peers))
	orderedCtxs := make([]*jsiter.Supervisor, 0, len(peers))
	checks := []health.Check{natsutil.HealthCheck(nc)}
	for _, dest := range peers {
		concurrentLane, orderedLane := "outbox-concurrent-"+dest, "outbox-ordered-"+dest

		// Both lanes re-resolve their consumer on every rebuild rather than
		// capturing it: a stopped lane usually means the durable is gone, so
		// rebuilding onto a stale handle would just stop again.
		openConcurrent := jsiter.PullFrom(
			jsiter.Resolve(js, outboxCfg.Name, buildConcurrentConsumerConfig(cfg.Consumer, cfg.SiteID, dest)),
			jetstream.PullMaxMessages(2*cfg.MaxWorkers))
		iter, err := jsiter.NewPump(ctx, concurrentLane, openConcurrent)
		if err != nil {
			slog.Error("concurrent messages failed", "dest_site_id", dest, "error", err)
			os.Exit(1)
		}
		iters = append(iters, iter)
		checks = append(checks, iter.HealthCheck())
		drainPool(ctx, iter, sem, &wg, process)

		openOrdered := jsiter.ConsumeFrom(jsiter.Resolve(js, outboxCfg.Name, buildOrderedConsumerConfig(cfg.Consumer, cfg.SiteID, dest)), process)
		cc, err := jsiter.NewSupervisor(ctx, orderedLane, openOrdered)
		if err != nil {
			slog.Error("ordered consume failed", "dest_site_id", dest, "error", err)
			os.Exit(1)
		}
		orderedCtxs = append(orderedCtxs, cc)
		checks = append(checks, cc.HealthCheck())
	}

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled, checks...)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("outbox-worker running", "site", cfg.SiteID, "federation_peers", peers)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error {
			for _, it := range iters {
				it.Stop()
			}
			for _, cc := range orderedCtxs {
				cc.Stop()
			}
			return nil
		},
		func(ctx context.Context) error {
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("worker drain timed out: %w", ctx.Err())
			}
		},
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(ctx context.Context) error { return healthStop(ctx) },
		// obsShutdown LAST so all prior teardown telemetry is exported.
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// drainPool pumps one lane's messages into the shared worker pool, one
// goroutine per message bounded by sem. The pump goroutine is counted in wg
// too: iter.Stop() returns without waiting for it, so a message taken between
// Next() and the per-message Add(1) could otherwise slip past shutdown's
// wg.Wait() and race nc.Drain(). It exits when the iterator is stopped.
func drainPool(ctx context.Context, iter jsiter.Nexter, sem chan struct{}, wg *sync.WaitGroup, process func(context.Context, jetstream.Msg)) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			msgCtx, msg, err := iter.Next()
			if err != nil {
				return
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(msgCtx context.Context, msg jetstream.Msg) {
				defer func() {
					<-sem
					wg.Done()
				}()
				process(msgCtx, msg)
			}(msgCtx, msg)
		}
	}()
}

// buildLaneConsumerConfig is the shared body of the two per-destination lane
// configs: durable outbox-worker-{lane}-{dest}, one filter subject per event
// type, and MaxDeliver=-1 (retry forever — a destination outage delays a
// forward, never exhausts it).
//
// Per destination, NOT a single shared consumer: MaxDeliver=-1 keeps a forward
// to a down peer parked (delivered-but-unacked) for the whole outage, and a
// NAK'd-with-delay message holds its ack-pending slot until it finally Acks, so
// a shared consumer's finite MaxAckPending budget would fill with one down
// peer's parked events and stall first-delivery of every healthy peer's events.
// One consumer per peer gives each its own budget, so a down peer stalls only
// its own lane.
func buildLaneConsumerConfig(s stream.ConsumerSettings, siteID, destSiteID, lane string, eventTypes []model.InboxEventType) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "outbox-worker-" + lane + "-" + destSiteID
	filters := make([]string, 0, len(eventTypes))
	for _, et := range eventTypes {
		filters = append(filters, subject.Outbox(siteID, destSiteID, et))
	}
	cc.FilterSubjects = filters
	cc.MaxDeliver = -1
	return cc
}

// buildConcurrentConsumerConfig returns a per-destination consumer config for
// the order-insensitive event types (outbox.ConcurrentEventTypes); the
// order-sensitive types ride the per-destination FIFO lanes instead
// (buildOrderedConsumerConfig). The two filter sets partition the stream —
// pkg/outbox owns the partition and rejects publishes outside it. Concurrency
// within a peer is preserved (default MaxAckPending); only the isolation
// boundary is per-destination.
func buildConcurrentConsumerConfig(s stream.ConsumerSettings, siteID, destSiteID string) jetstream.ConsumerConfig {
	return buildLaneConsumerConfig(s, siteID, destSiteID, "concurrent", outbox.ConcurrentEventTypes)
}

// buildOrderedConsumerConfig returns the per-destination FIFO consumer config
// for the order-sensitive event types (outbox.OrderedEventTypes: membership +
// room rename). MaxAckPending=1 makes the server release message N+1 only after
// N is acked, so events on this lane cannot overtake each other end-to-end
// through a destination outage (no member_removed overtaken by a stale
// member_added, no room_renamed overtaken by the member_added it renames), and
// retry pressure on a down peer is bounded to one in-flight probe per backoff
// interval. These events are low-volume, so the serial ceiling (~1/RTT per
// destination) is far above the real rate.
func buildOrderedConsumerConfig(s stream.ConsumerSettings, siteID, destSiteID string) jetstream.ConsumerConfig {
	cc := buildLaneConsumerConfig(s, siteID, destSiteID, "ordered", outbox.OrderedEventTypes)
	cc.MaxAckPending = 1
	return cc
}

// federationPeers returns the remote destination sites that get federation
// lanes (both the concurrent and ordered consumers): allSiteIDs minus blanks
// (trailing-comma tokens), the local site, and duplicates, preserving order.
func federationPeers(siteID string, allSiteIDs []string) []string {
	var peers []string
	seen := make(map[string]struct{}, len(allSiteIDs))
	for _, id := range allSiteIDs {
		if id == "" || id == siteID {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		peers = append(peers, id)
	}
	return peers
}
