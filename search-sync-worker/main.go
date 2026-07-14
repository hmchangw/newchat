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

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
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
	NatsURL             string `env:"NATS_URL,required"`
	NatsCredsFile       string `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID              string `env:"SITE_ID,required"`
	SearchURL           string `env:"SEARCH_URL,required"`
	SearchBackend       string `env:"SEARCH_BACKEND"         envDefault:"elasticsearch"`
	SearchUsername      string `env:"SEARCH_USERNAME"        envDefault:""`
	SearchPassword      string `env:"SEARCH_PASSWORD"        envDefault:""`
	SearchTLSSkipVerify bool   `env:"SEARCH_TLS_SKIP_VERIFY" envDefault:"false"`
	MsgIndexPrefix      string `env:"MSG_INDEX_PREFIX,required"`
	SpotlightIndex      string `env:"SPOTLIGHT_INDEX,required"`
	SpotlightOrgIndex   string `env:"SPOTLIGHT_ORG_INDEX,required"`
	HRCentralSiteID     string `env:"HR_CENTRAL_SITE_ID,required"`
	// HRJetStreamDomain, when set, is the JetStream domain of the remote NATS
	// cluster that owns OrgSyncStream (hr-syncer's HR stream). The spotlight-org
	// consumer is created against a domain-scoped JetStream context so a worker
	// at one site can consume the HR stream in another site's domain. Empty
	// (default) means the HR stream is in this worker's local domain and the
	// shared, otel-traced JetStream context is used.
	HRJetStreamDomain string `env:"HR_JETSTREAM_DOMAIN" envDefault:""`
	UserRoomIndex     string `env:"USER_ROOM_INDEX,required"`
	DevMode           bool   `env:"DEV_MODE" envDefault:"false"`
	HealthAddr        string `env:"HEALTH_ADDR" envDefault:":8081"`
	PProfEnabled      bool   `env:"PPROF_ENABLED" envDefault:"false"`

	// SyncMessagesFrom is an optional YYYY-MM-DD cutoff (UTC) that the
	// messages collection compares against Message.CreatedAt. Events
	// before the date are skipped — used to keep legacy-migration replays
	// of old chat messages out of the message index. Empty string
	// disables the filter. Spotlight and user-room are NOT filtered: a
	// user must still be able to discover and search rooms they joined
	// before the cutoff.
	SyncMessagesFrom string `env:"SYNC_MESSAGES_FROM" envDefault:""`

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

	Consumer  stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
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
	if _, _, ok := searchindex.StripVersion(cfg.MsgIndexPrefix); !ok {
		slog.Error("invalid config", "name", "MSG_INDEX_PREFIX", "value", cfg.MsgIndexPrefix, "reason", "must end with -v<N>, e.g. messages-site-a-v1")
		os.Exit(1)
	}
	if _, _, ok := searchindex.StripVersion(cfg.SpotlightIndex); !ok {
		slog.Error("invalid config", "name", "SPOTLIGHT_INDEX", "value", cfg.SpotlightIndex, "reason", "must end with -v<N>, e.g. spotlight-site-a-v1")
		os.Exit(1)
	}
	if _, _, ok := searchindex.StripVersion(cfg.SpotlightOrgIndex); !ok {
		slog.Error("invalid config", "name", "SPOTLIGHT_ORG_INDEX", "value", cfg.SpotlightOrgIndex, "reason", "must end with -v<N>, e.g. spotlightorg-site-a-v1")
		os.Exit(1)
	}
	syncMessagesFrom, err := parseSyncMessagesFrom(cfg.SyncMessagesFrom)
	if err != nil {
		slog.Error("invalid config", "name", "SYNC_MESSAGES_FROM", "value", cfg.SyncMessagesFrom, "error", err)
		os.Exit(1)
	}

	// Warn (don't fail) if the bulk batch size can't be reached under the
	// consumer's ack-pending ceiling — see checkBatchAckCoupling.
	if warning := checkBatchAckCoupling(cfg.BulkBatchSize, cfg.Consumer.MaxAckPending); warning != "" {
		slog.Warn("batch/ack-pending config coupling",
			"bulkBatchSize", cfg.BulkBatchSize,
			"maxAckPending", cfg.Consumer.MaxAckPending,
			"detail", warning,
		)
	}

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "search-sync-worker")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}

	engine, err := searchengine.New(ctx, searchengine.Config{
		Backend:       cfg.SearchBackend,
		URL:           cfg.SearchURL,
		Username:      cfg.SearchUsername,
		Password:      cfg.SearchPassword,
		TLSSkipVerify: cfg.SearchTLSSkipVerify,
	})
	if err != nil {
		slog.Error("search engine connect failed", "error", err)
		os.Exit(1)
	}

	msgColl := newMessageCollection(cfg.MsgIndexPrefix, syncMessagesFrom, cfg.DevMode)
	// search-service filters restricted-room access by threadParentMessageCreatedAt,
	// so re-resolve it from the parent's indexed createdAt (the event omits it).
	msgColl.parentResolver = newESParentResolver(engine, cfg.MsgIndexPrefix)

	collections := []Collection{
		msgColl,
		newSpotlightCollection(cfg.SpotlightIndex, cfg.DevMode),
		newSpotlightOrgCollection(cfg.SpotlightOrgIndex, cfg.SiteID, cfg.HRCentralSiteID, cfg.DevMode),
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

	// Register stored scripts before any consumer starts so the first
	// scripted update already resolves the script id. Idempotent across
	// pods (PUT _scripts is last-write-wins on identical bodies).
	for _, coll := range collections {
		for id, body := range coll.StoredScripts() {
			if err := engine.PutScript(ctx, id, body); err != nil {
				slog.Error("put stored script failed", "script", id, "error", err)
				os.Exit(1)
			}
			slog.Info("stored script registered", "script", id)
		}
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

	// When HR_JETSTREAM_DOMAIN is set, the HR stream (OrgSyncStream) lives in a
	// remote NATS domain. Build a raw domain-scoped JetStream context for the
	// spotlight-org consumer: oteljetstream has no domain variant, and this
	// worker already discards the per-message otel trace context on the consume
	// path, so the raw context loses nothing in use. NewWithDomain only sets the
	// API prefix (no I/O), so an error here is a config error, not a
	// reachability failure. Empty domain keeps the shared js for the HR consumer.
	var hrJS jetstream.JetStream
	if cfg.HRJetStreamDomain != "" {
		hrJS, err = jetstream.NewWithDomain(nc.NatsConn(), cfg.HRJetStreamDomain)
		if err != nil {
			slog.Error("jetstream HR-domain init failed",
				"domain", cfg.HRJetStreamDomain, "error", err)
			os.Exit(1)
		}
	}

	bulkFlushInterval := time.Duration(cfg.BulkFlushInterval) * time.Second
	stopCh := make(chan struct{})
	doneChs := make([]chan struct{}, 0, len(collections))

	// Multiple collections can share the same stream (spotlight + user-room
	// both consume INBOX). Track which streams have already been created so
	// we don't redundantly call CreateOrUpdateStream per collection.
	createdStreams := make(map[string]struct{}, len(collections))

	// INBOX is owned by inbox-worker; HR is owned by hr-syncer.
	// search-sync-worker is a pure consumer of both and must not create
	// their schemas.
	inboxName := stream.Inbox(cfg.SiteID).Name
	hrName := stream.OrgSyncStream(cfg.HRCentralSiteID).Name

	for _, coll := range collections {
		streamCfg := coll.StreamConfig(cfg.SiteID)
		// Skip INBOX and HR bootstrap — those streams are owned by other
		// services (inbox-worker / hr-syncer). Consumer creation still
		// runs for collections that read from them.
		if cfg.Bootstrap.Enabled && streamCfg.Name != inboxName && streamCfg.Name != hrName {
			if _, alreadyCreated := createdStreams[streamCfg.Name]; !alreadyCreated {
				if _, err := js.CreateOrUpdateStream(ctx, streamCfg); err != nil {
					slog.Error("create stream failed", "stream", streamCfg.Name, "error", err)
					os.Exit(1)
				}
				createdStreams[streamCfg.Name] = struct{}{}
				slog.Info("stream bootstrapped", "stream", streamCfg.Name)
			}
		}

		consumerCfg := buildConsumerConfig(cfg.Consumer, coll, cfg.SiteID)

		// The HR (spotlight-org) collection reads OrgSyncStream. When a remote HR
		// domain is configured, create its consumer against the domain-scoped
		// context; every other collection uses the shared otel-traced js.
		var fetcher msgFetcher
		if streamCfg.Name == hrName && hrJS != nil {
			cons, err := hrJS.CreateOrUpdateConsumer(ctx, streamCfg.Name, consumerCfg)
			if err != nil {
				slog.Error("create consumer failed",
					"stream", streamCfg.Name,
					"consumer", coll.ConsumerName(),
					"domain", cfg.HRJetStreamDomain,
					"error", err,
				)
				os.Exit(1)
			}
			fetcher = rawConsumerAdapter{cons}
			slog.Info("HR consumer bound to remote JetStream domain",
				"domain", cfg.HRJetStreamDomain,
				"stream", streamCfg.Name,
				"consumer", coll.ConsumerName(),
			)
		} else {
			cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, consumerCfg)
			if err != nil {
				slog.Error("create consumer failed",
					"stream", streamCfg.Name,
					"consumer", coll.ConsumerName(),
					"error", err,
				)
				os.Exit(1)
			}
			fetcher = otelConsumerAdapter{cons}
		}

		handler := NewHandler(&engineAdapter{engine: engine}, coll, cfg.BulkBatchSize)
		doneCh := make(chan struct{})
		doneChs = append(doneChs, doneCh)

		slog.Info("collection wired",
			"stream", streamCfg.Name,
			"consumer", coll.ConsumerName(),
			"filters", consumerCfg.FilterSubjects,
		)

		go runConsumer(ctx, fetcher, handler, cfg.FetchBatchSize, cfg.BulkBatchSize, bulkFlushInterval, stopCh, doneCh)
	}

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	syncMessagesFromLog := "disabled"
	if !syncMessagesFrom.IsZero() {
		syncMessagesFromLog = syncMessagesFrom.Format(time.RFC3339)
	}
	slog.Info("search-sync-worker running",
		"site", cfg.SiteID,
		"msgPrefix", cfg.MsgIndexPrefix,
		"spotlightIndex", cfg.SpotlightIndex,
		"userRoomIndex", cfg.UserRoomIndex,
		"syncMessagesFrom", syncMessagesFromLog,
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
		func(ctx context.Context) error { return healthStop(ctx) },
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
//
// Bulk flush spans many client requests, so per-message X-Request-ID is intentionally NOT propagated; mint a per-flush bulkID if per-batch traceability becomes a need.
func runConsumer(
	ctx context.Context,
	cons msgFetcher,
	handler *Handler,
	fetchBatchSize, bulkBatchSize int,
	bulkFlushInterval time.Duration,
	stopCh <-chan struct{},
	doneCh chan<- struct{},
) {
	defer close(doneCh)
	lastFlush := time.Now()

	// jobguard recovers panics from the batch handler so a single poison
	// message or a malformed bulk response can't crash this collection's
	// consumer goroutine for good. On a recovered panic the in-flight messages
	// are left un-acked and JetStream redelivers them after AckWait.
	flush := func() { jobguard.Guard("search-sync flush", func() { handler.Flush(ctx) }) }
	add := func(m jetstream.Msg) {
		jobguard.Guard("search-sync add: "+m.Subject(), func() { handler.Add(m) })
	}

	for {
		select {
		case <-stopCh:
			flush()
			return
		default:
		}

		// Bound the next Fetch by remaining bulk capacity so a steady stream
		// of 1:1 messages can't overshoot bulkBatchSize. Fan-out messages
		// may still push us over — that's handled mid-loop below.
		remaining := bulkBatchSize - handler.ActionCount()
		if remaining <= 0 {
			flush()
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
				flush()
				return
			default:
			}
			if handler.ActionCount() > 0 && time.Since(lastFlush) >= bulkFlushInterval {
				flush()
				lastFlush = time.Now()
			}
			continue
		}

		// Always drain batch.Messages() to completion. The otelBatch adapter
		// (consumer_source.go) re-channels via a goroutine that blocks on an
		// unbuffered send until the channel is drained; an early break here
		// would leak that goroutine and stall shutdown. Bound work via
		// fetchCount above, not by abandoning a batch mid-range.
		for msg := range batch.Messages() {
			add(msg)
			// Mid-batch flush: if a single fan-out message just pushed the
			// buffer over the bulk cap, flush immediately instead of waiting
			// for the outer loop — otherwise the next message's actions
			// would add to an already-oversized bulk request.
			if handler.ActionCount() >= bulkBatchSize {
				flush()
				lastFlush = time.Now()
			}
		}

		if handler.ActionCount() >= bulkBatchSize {
			flush()
			lastFlush = time.Now()
		} else if handler.ActionCount() > 0 && time.Since(lastFlush) >= bulkFlushInterval {
			flush()
			lastFlush = time.Now()
		}
	}
}

// checkBatchAckCoupling returns a non-empty warning when bulkBatchSize exceeds
// maxAckPending. In that regime a 1:1 collection (e.g. messages) stalls at
// maxAckPending un-acked messages — JetStream stops delivering — before
// ActionCount() can ever reach bulkBatchSize, so the size-based flush trigger
// (main loop) never fires and every flush falls back to the BulkFlushInterval
// timer. The result is undersized batches plus a fixed per-flush latency floor.
// Fan-out collections are unaffected (one message yields many actions), so this
// is a warning rather than a hard failure. Empty string = no coupling issue.
func checkBatchAckCoupling(bulkBatchSize, maxAckPending int) string {
	if bulkBatchSize > maxAckPending {
		return fmt.Sprintf(
			"BULK_BATCH_SIZE (%d) exceeds CONSUMER_MAX_ACK_PENDING (%d): "+
				"the size-based flush can never fire for 1:1 collections, so flushes "+
				"will wait the full BULK_FLUSH_INTERVAL and batches stay undersized. "+
				"Lower BULK_BATCH_SIZE to <= MAX_ACK_PENDING or raise MAX_ACK_PENDING.",
			bulkBatchSize, maxAckPending,
		)
	}
	return ""
}

// engineAdapter adapts searchengine.SearchEngine to the Handler's Store interface.
type engineAdapter struct {
	engine searchengine.SearchEngine
}

func (a *engineAdapter) Bulk(ctx context.Context, actions []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
	return a.engine.Bulk(ctx, actions)
}

// consumerSource is the subset of Collection that buildConsumerConfig
// needs. Narrowing keeps the helper unit-testable with a small fake.
type consumerSource interface {
	ConsumerName() string
	FilterSubjects(siteID string) []string
}

// buildConsumerConfig returns the durable consumer config for one
// search-sync-worker collection. Custom BackOff is intentional: ES
// indexing benefits from progressive retries on transient failures.
// With MaxDeliver=5 from defaults and 3 BackOff entries, NATS reuses
// the last entry (30s) for the 4th and 5th retries — do not extend
// BackOff to length 5 to "fix" this; the reuse is the intended pattern.
func buildConsumerConfig(s stream.ConsumerSettings, coll consumerSource, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = coll.ConsumerName()
	cc.BackOff = []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second}
	if filters := coll.FilterSubjects(siteID); len(filters) > 0 {
		cc.FilterSubjects = filters
	}
	return cc
}
