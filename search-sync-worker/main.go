package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
)

// bootstrapConfig groups fields only meaningful in dev/integration; in production Enabled must
// stay false — streams are owned by their publishers, and search-sync-worker NEVER bootstraps INBOX (owned by inbox-worker).
type bootstrapConfig struct {
	// Enabled (BOOTSTRAP_STREAMS) toggles CreateOrUpdateStream at startup for each collection's
	// stream; leave false in production. INBOX is always excluded — inbox-worker owns its schema.
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
	// HRJetStreamDomain, when set, is the remote NATS domain owning OrgSyncStream (hr-syncer's HR
	// stream), letting a worker at one site consume it in another's domain; empty means local domain.
	HRJetStreamDomain string `env:"HR_JETSTREAM_DOMAIN" envDefault:""`
	UserRoomIndex     string `env:"USER_ROOM_INDEX,required"`
	DevMode           bool   `env:"DEV_MODE" envDefault:"false"`
	HealthAddr        string `env:"HEALTH_ADDR" envDefault:":8081"`
	PProfEnabled      bool   `env:"PPROF_ENABLED" envDefault:"false"`

	// SyncMessagesFrom is an optional YYYY-MM-DD (UTC) cutoff for Message.CreatedAt, skipping
	// legacy-migration replays from the message index; empty disables it. Spotlight/user-room are NOT filtered.
	SyncMessagesFrom string `env:"SYNC_MESSAGES_FROM" envDefault:""`

	// FetchBatchSize is the max JetStream messages pulled per Fetch() round-trip (smaller = lower
	// latency, larger = amortized overhead); a JetStream-client concern that does NOT bound ES bulk size.
	FetchBatchSize int `env:"FETCH_BATCH_SIZE" envDefault:"100"`

	// BulkBatchSize is the soft cap on buffered ES bulk actions (counted in actions, not messages —
	// fan-out collections can reach it with far fewer messages); handler.ActionCount() triggers a mid-Fetch flush if exceeded.
	BulkBatchSize int `env:"BULK_BATCH_SIZE" envDefault:"500"`

	// BulkFlushInterval is the max seconds between ES bulk flushes even if BulkBatchSize isn't hit —
	// the time-based counterpart to the size trigger, bounding write latency during idle periods.
	BulkFlushInterval int `env:"BULK_FLUSH_INTERVAL" envDefault:"5"`

	Consumer  stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
}

func main() {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	// Fail fast on non-positive batch/interval settings — zero/negative values degenerate runConsumer
	// into busy loops (Fetch(0)) or stall it forever (remaining <= 0 every iteration); reject at startup for a clear signal.
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

	// Warn (don't fail) if the bulk batch size can't be reached under the consumer's ack-pending ceiling — see checkBatchAckCoupling.
	if warning := checkBatchAckCoupling(cfg.BulkBatchSize, cfg.Consumer.MaxAckPending); warning != "" {
		slog.Warn("batch/ack-pending config coupling",
			"bulkBatchSize", cfg.BulkBatchSize,
			"maxAckPending", cfg.Consumer.MaxAckPending,
			"detail", warning,
		)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	engine, err := searchengine.New(ctx, searchengine.Config{
		Backend:       cfg.SearchBackend,
		URL:           cfg.SearchURL,
		Username:      cfg.SearchUsername,
		Password:      cfg.SearchPassword,
		TLSSkipVerify: cfg.SearchTLSSkipVerify,
	}, searchengine.WithObservability(sdk))
	if err != nil {
		slog.Error("search engine connect failed", "error", err)
		os.Exit(1)
	}

	msgColl := newMessageCollection(cfg.MsgIndexPrefix, cfg.SiteID, syncMessagesFrom, cfg.DevMode)
	// search-service filters restricted-room access by threadParentMessageCreatedAt, so re-resolve it from the parent's indexed createdAt (the event omits it).
	msgColl.parentResolver = newESParentResolver(engine, cfg.MsgIndexPrefix)

	// Second consumer over messageCollection, bound to BOT_MESSAGES_CANONICAL. isBot is derived per-doc from model.IsBot(UserAccount) so bots reuse the same index.
	botMsgColl := newBotMessageCollection(cfg.MsgIndexPrefix, cfg.DevMode)
	botMsgColl.parentResolver = newESParentResolver(engine, cfg.MsgIndexPrefix)

	collections := []Collection{
		// msgColl also indexes migrated Teams history off .teams.batch (message-worker
		// persists it with no .created event) — one consumer covers both.
		msgColl,
		botMsgColl,
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

	if err := pushMappings(ctx, engine, collections); err != nil {
		slog.Error("update index mapping failed", "error", err)
		os.Exit(1)
	}

	// Register stored scripts before any consumer starts so the first scripted update already
	// resolves the script id; idempotent across pods (PUT _scripts is last-write-wins).
	for _, coll := range collections {
		for id, body := range coll.StoredScripts() {
			if err := engine.PutScript(ctx, id, body); err != nil {
				slog.Error("put stored script failed", "script", id, "error", err)
				os.Exit(1)
			}
			slog.Info("stored script registered", "script", id)
		}
	}

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}
	// Local JetStream consumers use the o11y facade so Fetch deliveries carry consumer spans;
	// the HR domain path below stays raw because the facade has no domain-scoped constructor.
	js, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	// When HR_JETSTREAM_DOMAIN is set, build a raw domain-scoped JetStream context for the
	// spotlight-org consumer (oteljetstream has no domain variant); NewWithDomain only sets the API prefix, so an error here is a config error, not reachability.
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

	// Multiple collections can share the same stream (spotlight + user-room both consume INBOX);
	// track which streams have already been created to avoid redundant CreateOrUpdateStream calls.
	createdStreams := make(map[string]struct{}, len(collections))

	// INBOX is owned by inbox-worker; HR is owned by hr-syncer. search-sync-worker is a pure consumer of both and must not create their schemas.
	inboxName := stream.Inbox(cfg.SiteID).Name
	hrName := stream.OrgSyncStream(cfg.HRCentralSiteID).Name

	for _, coll := range collections {
		streamCfg := coll.StreamConfig(cfg.SiteID)
		// Skip INBOX and HR bootstrap — those streams are owned by other services (inbox-worker /
		// hr-syncer); consumer creation still runs for collections that read from them.
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

		// The HR (spotlight-org) collection reads OrgSyncStream; when a remote HR domain is configured,
		// create its consumer against the domain-scoped context — every other collection uses the shared js.
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
			fetcher = o11yConsumerAdapter{cons}
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
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { return healthStop(ctx) },
		// obsShutdown LAST so drain-window flush spans/logs are exported.
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// pushMappings PUTs each collection's additive mapping onto existing indices;
// templates cover only new ones, so new fields stay unmapped until rollover.
func pushMappings(ctx context.Context, engine searchengine.SearchEngine, collections []Collection) error {
	for _, coll := range collections {
		pattern, body := coll.MappingUpdate()
		if pattern == "" || len(body) == 0 {
			continue
		}
		if err := engine.UpdateMapping(ctx, pattern, body); err != nil {
			return fmt.Errorf("update mapping %s: %w", pattern, err)
		}
		slog.Info("index mapping updated", "pattern", pattern)
	}
	return nil
}

// runConsumer is the batch-flush consumer loop: fetchBatchSize bounds JetStream Fetch() pulls
// (client-tuning only), bulkBatchSize caps buffered ES actions (flushed on stopCh, size, or bulkFlushInterval; the loop clamps fetch to remaining bulk capacity, but a fan-out message can still overshoot mid-loop and triggers its own flush).
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

	// jobguard recovers panics from the batch handler so a poison message or malformed bulk response
	// can't crash this collection's consumer goroutine; in-flight messages stay un-acked and JetStream redelivers after AckWait.
	flush := func() { jobguard.Guard("search-sync flush", func() { handler.Flush(ctx) }) }
	add := func(msgCtx context.Context, m jetstream.Msg) {
		jobguard.Guard("search-sync add: "+m.Subject(), func() { handler.AddWithContext(msgCtx, m) })
	}

	for {
		select {
		case <-stopCh:
			flush()
			return
		default:
		}

		// Bound the next Fetch by remaining bulk capacity so a steady stream of 1:1 messages can't
		// overshoot bulkBatchSize; fan-out messages may still push us over, handled mid-loop below.
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

		batch, err := cons.Fetch(ctx, fetchCount, jetstream.FetchMaxWait(time.Second))
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

		// Always drain batch.Messages() to completion — the raw domain adapter re-channels via a
		// goroutine blocked on an unbuffered send; an early break would leak it and stall shutdown.
		for msg := range batch.Messages() {
			add(msg.Ctx, msg.Msg)
			// Mid-batch flush: if a fan-out message just pushed the buffer over the bulk cap, flush
			// immediately — otherwise the next message's actions add to an already-oversized request.
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

// checkBatchAckCoupling warns when bulkBatchSize exceeds maxAckPending: a 1:1 collection then
// stalls at maxAckPending un-acked messages before the size-based flush can fire, undersizing every batch. Fan-out collections are unaffected. Empty string = no issue.
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

// consumerSource is the subset of Collection that buildConsumerConfig needs. Narrowing keeps the helper unit-testable with a small fake.
type consumerSource interface {
	ConsumerName() string
	FilterSubjects(siteID string) []string
}

// buildConsumerConfig returns the durable consumer config for one collection; custom BackOff gives
// progressive retries. With MaxDeliver=5 and 3 BackOff entries, NATS reuses the last (30s) for retries 4-5 — intended, don't extend to length 5.
func buildConsumerConfig(s stream.ConsumerSettings, coll consumerSource, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = coll.ConsumerName()
	cc.BackOff = []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second}
	if filters := coll.FilterSubjects(siteID); len(filters) > 0 {
		cc.FilterSubjects = filters
	}
	return cc
}
