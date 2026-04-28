package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig groups every field that is ONLY meaningful when the worker
// is being stood up in dev or integration tests without its normal upstream
// services. In production Enabled must remain false — streams are owned by
// their publisher services (message-gatekeeper for MESSAGES_CANONICAL,
// inbox-worker for INBOX) and search-sync-worker only manages its own
// durable consumers.
//
// search-sync-worker NEVER bootstraps INBOX, even when Enabled=true; that
// stream's schema is owned by inbox-worker and its federation by ops/IaC.
//
// Env vars in this group are all prefixed `BOOTSTRAP_` so they're easy to
// spot in deployment manifests and obvious to grep.
type bootstrapConfig struct {
	// Enabled (BOOTSTRAP_STREAMS) toggles whether the worker calls
	// CreateOrUpdateStream at startup for each collection's stream. Leave
	// false in production. INBOX is intentionally excluded from this loop
	// — inbox-worker owns INBOX schema bootstrap.
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

type config struct {
	NatsURL        string `env:"NATS_URL,required"`
	NatsCredsFile  string `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID         string `env:"SITE_ID,required"`
	SearchURL      string `env:"SEARCH_URL,required"`
	SearchBackend  string `env:"SEARCH_BACKEND"  envDefault:"elasticsearch"`
	MsgIndexPrefix string `env:"MSG_INDEX_PREFIX,required"`
	SpotlightIndex string `env:"SPOTLIGHT_INDEX" envDefault:""`
	UserRoomIndex  string `env:"USER_ROOM_INDEX" envDefault:""`

	// FetchBatchSize is the maximum number of JetStream messages to pull
	// per Fetch() round-trip. Smaller values give lower latency per message
	// but more round-trips; larger values amortize the per-Fetch overhead.
	// This is a JetStream-client concern — it does NOT bound ES bulk
	// request size.
	FetchBatchSize int `env:"FETCH_BATCH_SIZE" envDefault:"100"`

	// BulkBatchSize is the soft cap on buffered ES bulk actions before the
	// worker flushes to Elasticsearch. This is counted in actions, not
	// messages: fan-out collections (bulk invites producing N actions per
	// JetStream message) can reach this threshold with far fewer messages
	// than the count suggests. The consumer loop checks handler.ActionCount()
	// against this value and triggers a flush mid-Fetch if a single fat
	// message pushes the buffer over the cap.
	BulkBatchSize int `env:"BULK_BATCH_SIZE" envDefault:"500"`

	// BulkFlushInterval is the maximum seconds between ES bulk flushes, even
	// if the action buffer hasn't hit BulkBatchSize. It's the time-based
	// counterpart to the size-based BulkBatchSize trigger — either
	// condition can fire a flush. Keeps write latency bounded during
	// idle / low-traffic periods.
	BulkFlushInterval int `env:"BULK_FLUSH_INTERVAL" envDefault:"5"`

	Bootstrap bootstrapConfig `envPrefix:"BOOTSTRAP_"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	if cfg.SpotlightIndex == "" {
		cfg.SpotlightIndex = fmt.Sprintf("spotlight-%s-v1-chat", cfg.SiteID)
	}
	if cfg.UserRoomIndex == "" {
		cfg.UserRoomIndex = fmt.Sprintf("user-room-%s", cfg.SiteID)
	}

	// Fail fast on non-positive batch/interval settings. Zero or negative
	// values degenerate runConsumer into busy loops (`Fetch(0)`, constant
	// flush checks) or stall it forever (`remaining <= 0` on every
	// iteration). Reject at startup so an operator gets a clear signal
	// instead of silent misbehavior. Matches the repo-wide "fail fast on
	// bad config" rule in CLAUDE.md.
	if cfg.FetchBatchSize <= 0 {
		slog.Error("invalid config", "name", "FETCH_BATCH_SIZE", "value", cfg.FetchBatchSize, "reason", "must be > 0")
		os.Exit(1)
	}
	if cfg.BulkBatchSize <= 0 {
		slog.Error("invalid config", "name", "BULK_BATCH_SIZE", "value", cfg.BulkBatchSize, "reason", "must be > 0")
		os.Exit(1)
	}
	if cfg.BulkFlushInterval <= 0 {
		slog.Error("invalid config", "name", "BULK_FLUSH_INTERVAL", "value", cfg.BulkFlushInterval, "reason", "must be > 0")
		os.Exit(1)
	}

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "search-sync-worker")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}

	engine, err := searchengine.New(ctx, cfg.SearchBackend, cfg.SearchURL)
	if err != nil {
		slog.Error("search engine connect failed", "error", err)
		os.Exit(1)
	}

	collections := []Collection{
		newMessageCollection(cfg.MsgIndexPrefix),
		newSpotlightCollection(cfg.SpotlightIndex),
		newUserRoomCollection(cfg.UserRoomIndex),
	}

	for _, coll := range collections {
		name := coll.TemplateName()
		body := coll.TemplateBody()
		if name == "" || body == nil {
			continue
		}
		if err := engine.UpsertTemplate(ctx, name, body); err != nil {
			slog.Error("upsert index template failed", "template", name, "error", err)
			os.Exit(1)
		}
		slog.Info("index template upserted", "name", name)
	}

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}
	js, err := oteljetstream.New(nc)
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	bulkFlushInterval := time.Duration(cfg.BulkFlushInterval) * time.Second
	stopCh := make(chan struct{})
	doneChs := make([]chan struct{}, 0, len(collections))

	// Multiple collections can share the same stream (spotlight + user-room
	// both consume INBOX). Track which streams have already been created so
	// we don't redundantly call CreateOrUpdateStream per collection.
	createdStreams := make(map[string]struct{}, len(collections))

	// INBOX is owned by inbox-worker — see the skip in the loop below.
	inboxName := stream.Inbox(cfg.SiteID).Name

	for _, coll := range collections {
		streamCfg := coll.StreamConfig(cfg.SiteID)
		// Skip INBOX bootstrap — inbox-worker owns its schema, ops/IaC
		// owns its federation. Consumer creation still runs for
		// INBOX-based collections (spotlight, user-room).
		if cfg.Bootstrap.Enabled && streamCfg.Name != inboxName {
			if _, alreadyCreated := createdStreams[streamCfg.Name]; !alreadyCreated {
				if _, err := js.CreateOrUpdateStream(ctx, streamCfg); err != nil {
					slog.Error("create stream failed", "stream", streamCfg.Name, "error", err)
					os.Exit(1)
				}
				createdStreams[streamCfg.Name] = struct{}{}
				slog.Info("stream bootstrapped", "stream", streamCfg.Name)
			}
		}

		consumerCfg := jetstream.ConsumerConfig{
			Durable:   coll.ConsumerName(),
			AckPolicy: jetstream.AckExplicitPolicy,
			BackOff:   []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second},
		}
		if filters := coll.FilterSubjects(cfg.SiteID); len(filters) > 0 {
			consumerCfg.FilterSubjects = filters
		}
		cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, consumerCfg)
		if err != nil {
			slog.Error("create consumer failed",
				"stream", streamCfg.Name,
				"consumer", coll.ConsumerName(),
				"error", err,
			)
			os.Exit(1)
		}

		handler := NewHandler(&engineAdapter{engine: engine}, coll, cfg.BulkBatchSize)
		doneCh := make(chan struct{})
		doneChs = append(doneChs, doneCh)

		slog.Info("collection wired",
			"stream", streamCfg.Name,
			"consumer", coll.ConsumerName(),
			"filters", consumerCfg.FilterSubjects,
		)

		go runConsumer(ctx, cons, handler, cfg.FetchBatchSize, cfg.BulkBatchSize, bulkFlushInterval, stopCh, doneCh)
	}

	slog.Info("search-sync-worker running",
		"site", cfg.SiteID,
		"msgPrefix", cfg.MsgIndexPrefix,
		"spotlightIndex", cfg.SpotlightIndex,
		"userRoomIndex", cfg.UserRoomIndex,
		"collections", len(collections),
	)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error {
			close(stopCh)
			return nil
		},
		func(ctx context.Context) error {
			for _, ch := range doneChs {
				select {
				case <-ch:
				case <-ctx.Done():
					return fmt.Errorf("consumer loop drain timed out: %w", ctx.Err())
				}
			}
			return nil
		},
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
	)
}

// runConsumer is the batch-flush consumer loop for a single collection.
//
// Two batch sizes apply at different layers:
//
//   - fetchBatchSize bounds how many JetStream messages are pulled per
//     `cons.Fetch(...)` round-trip. This is purely a JetStream-client tuning
//     knob — larger = fewer round-trips, smaller = lower per-message latency.
//
//   - bulkBatchSize is the soft cap on buffered ES bulk actions before a
//     flush is triggered. This is the real ES-side bound: a fan-out
//     collection (bulk invite producing N actions per message) can hit it
//     with far fewer messages than the count suggests, so the loop checks
//     handler.ActionCount() — not message count — against it.
//
// The two caps interact: the loop clamps the per-Fetch count to
// `min(fetchBatchSize, bulkBatchSize - ActionCount())` so we never pull
// more messages than the remaining bulk capacity can absorb under a 1:1
// assumption. Fan-out messages can still push the buffer past bulkBatchSize
// mid-loop (a single N-subscription event produces N actions on its own),
// which is handled by a mid-batch flush inside the message loop.
//
// Flushes happen on three triggers:
//  1. `stopCh` signalled (graceful shutdown): drain whatever is buffered.
//  2. `handler.ActionCount() >= bulkBatchSize`: size-based flush.
//  3. `time.Since(lastFlush) >= bulkFlushInterval` with a non-empty buffer:
//     time-based flush to bound write latency during idle periods.
func runConsumer(
	ctx context.Context,
	cons oteljetstream.Consumer,
	handler *Handler,
	fetchBatchSize, bulkBatchSize int,
	bulkFlushInterval time.Duration,
	stopCh <-chan struct{},
	doneCh chan<- struct{},
) {
	defer close(doneCh)
	lastFlush := time.Now()

	for {
		select {
		case <-stopCh:
			handler.Flush(ctx)
			return
		default:
		}

		// Bound the next Fetch by remaining bulk capacity so a steady stream
		// of 1:1 messages can't overshoot bulkBatchSize. Fan-out messages
		// may still push us over — that's handled mid-loop below.
		remaining := bulkBatchSize - handler.ActionCount()
		if remaining <= 0 {
			handler.Flush(ctx)
			lastFlush = time.Now()
			continue
		}
		fetchCount := fetchBatchSize
		if fetchCount > remaining {
			fetchCount = remaining
		}

		batch, err := cons.Fetch(fetchCount, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			select {
			case <-stopCh:
				handler.Flush(ctx)
				return
			default:
			}
			if handler.ActionCount() > 0 && time.Since(lastFlush) >= bulkFlushInterval {
				handler.Flush(ctx)
				lastFlush = time.Now()
			}
			continue
		}

		for msg := range batch.Messages() {
			handler.Add(msg.Msg)
			// Mid-batch flush: if a single fan-out message just pushed the
			// buffer over the bulk cap, flush immediately instead of waiting
			// for the outer loop — otherwise the next message's actions
			// would add to an already-oversized bulk request.
			if handler.ActionCount() >= bulkBatchSize {
				handler.Flush(ctx)
				lastFlush = time.Now()
			}
		}

		if handler.ActionCount() >= bulkBatchSize {
			handler.Flush(ctx)
			lastFlush = time.Now()
		} else if handler.ActionCount() > 0 && time.Since(lastFlush) >= bulkFlushInterval {
			handler.Flush(ctx)
			lastFlush = time.Now()
		}
	}
}

// engineAdapter adapts searchengine.SearchEngine to the Handler's Store interface.
type engineAdapter struct {
	engine searchengine.SearchEngine
}

func (a *engineAdapter) Bulk(ctx context.Context, actions []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
	return a.engine.Bulk(ctx, actions)
}
