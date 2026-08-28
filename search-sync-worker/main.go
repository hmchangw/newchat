package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
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
	// Mode selects which collections this pod binds: "default" runs the live
	// message/bot/spotlight/user-room consumers; "teams" runs only the
	// migrated-Teams-history consumer (MESSAGES-TEAMS).
	Mode          string `env:"MODE" envDefault:"default"`
	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string `env:"SITE_ID,required"`
	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB"      envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`
	// ReadPreference: primaryPreferred, not secondaryPreferred. A resolver miss is
	// durable — buildTeamsActions emits an index action with empty author fields and
	// handler.go Acks the source message once the bulk request succeeds, so nothing
	// retries the under-enriched write. The primary-offload win is not worth a
	// permanently mis-indexed document.
	ReadPreference      string `env:"MONGO_READ_PREFERENCE" envDefault:"primaryPreferred"`
	Pool                mongoutil.PoolConfig
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

	// PipelineDepth is how many ES bulk requests one collection keeps in flight while later
	// batches fetch and build. 1 serializes fetch/build behind every round-trip; higher trades
	// ack-pending headroom for throughput when ES latency dominates. Costs are per collection
	// AND per pod, so the cluster sees depth x collections x replicas concurrent bulk requests.
	// Needs (PipelineDepth+1) * BulkBatchSize <= CONSUMER_MAX_ACK_PENDING — see checkBatchAckCoupling.
	PipelineDepth int `env:"PIPELINE_DEPTH" envDefault:"2"`

	Consumer  stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap bootstrapConfig         `envPrefix:"BOOTSTRAP_"`

	// AdminAcctPrefix overrides the platform-admin account prefix (ADMIN_ACCT_PREFIX); keep it identical across services.
	AdminAcctPrefix string `env:"ADMIN_ACCT_PREFIX" envDefault:"p_admin"`
}

func main() {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	if cfg.Mode != "default" && cfg.Mode != "teams" {
		slog.Error("invalid config", "MODE", cfg.Mode, "reason", `must be "default" or "teams"`)
		os.Exit(1)
	}

	if err := model.SetPlatformAdminAccountPrefix(cfg.AdminAcctPrefix); err != nil {
		slog.Error("invalid ADMIN_ACCT_PREFIX", "error", err)
		os.Exit(1)
	}

	if err := cfg.Pool.Validate(); err != nil {
		slog.Error("invalid mongo pool config", "error", err)
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
	// A non-positive depth would make the pipeline's slot channel unbuffered and park the
	// consumer on its first flush forever.
	if cfg.PipelineDepth <= 0 {
		slog.Error("invalid config", "name", "PIPELINE_DEPTH", "value", cfg.PipelineDepth, "reason", "must be > 0")
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
	if warning := checkBatchAckCoupling(cfg.BulkBatchSize, cfg.Consumer.MaxAckPending, cfg.PipelineDepth); warning != "" {
		slog.Warn("batch/ack-pending config coupling",
			"bulkBatchSize", cfg.BulkBatchSize,
			"maxAckPending", cfg.Consumer.MaxAckPending,
			"pipelineDepth", cfg.PipelineDepth,
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

	// Mongo backs the migrated-Teams-history author lookup (teams_user → account → user _id).
	readPref, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	if err != nil {
		slog.Error("invalid mongo read preference", "value", cfg.ReadPreference, "error", err)
		os.Exit(1)
	}
	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk), mongoutil.WithReadPreference(readPref))
	if err != nil {
		slog.Error("mongodb connect failed", "error", err)
		os.Exit(1)
	}
	slog.Info("mongo read preference configured", "readPreference", readPref.Mode().String())
	db := mongoClient.Database(cfg.MongoDB)

	// obs.Init installed the SDK's MeterProvider as the OTel global, so this
	// resolves to real instruments when metrics are enabled and no-ops otherwise.
	esMetrics := newSyncMetrics(otel.Meter(metricsScope))

	// Mode gates which consumers this pod binds. "teams" runs only the
	// MESSAGES-TEAMS migrated-history consumer; "default" runs everything else.
	var collections []Collection
	if cfg.Mode == "teams" {
		// Bound to MESSAGES-TEAMS: message-worker's teams mode persists migrated Teams
		// history with no .created event on the canonical stream, so this indexes off
		// its own stream/subject rather than a filter on the live message collection.
		teamsMsgColl := newTeamsMessageCollection(cfg.MsgIndexPrefix, cfg.SiteID, cfg.DevMode)
		teamsMsgColl.teamsUsers = newMongoTeamsUserResolver(db)
		collections = []Collection{teamsMsgColl}
	} else {
		msgColl := newMessageCollection(cfg.MsgIndexPrefix, cfg.SiteID, syncMessagesFrom, cfg.DevMode)
		// search-service filters restricted-room access by threadParentMessageCreatedAt, so re-resolve it from the parent's indexed createdAt (the event omits it).
		msgResolver := newESParentResolver(engine, cfg.MsgIndexPrefix)
		msgResolver.metrics = esMetrics.forCollection(msgColl.ConsumerName())
		msgColl.parentResolver = msgResolver

		// Second consumer over messageCollection, bound to BOT-MESSAGES-CANONICAL. isBot is derived per-doc from model.IsBot(UserAccount) so bots reuse the same index.
		botMsgColl := newBotMessageCollection(cfg.MsgIndexPrefix, cfg.DevMode)
		botMsgResolver := newESParentResolver(engine, cfg.MsgIndexPrefix)
		botMsgResolver.metrics = esMetrics.forCollection(botMsgColl.ConsumerName())
		botMsgColl.parentResolver = botMsgResolver

		collections = []Collection{
			msgColl,
			botMsgColl,
			newSpotlightCollection(cfg.SpotlightIndex, cfg.DevMode),
			newSpotlightOrgCollection(cfg.SpotlightOrgIndex, cfg.SiteID, cfg.HRCentralSiteID, cfg.DevMode),
			newUserRoomCollection(cfg.UserRoomIndex, cfg.DevMode),
		}
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

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
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

	checks := make([]health.Check, 0, len(collections)+1)
	checks = append(checks, natsutil.HealthCheck(nc))

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

		// The HR (spotlight-org) collection reads OrgSyncStream; when a remote HR domain is
		// configured its consumer is created there — every other collection uses the shared js.
		useHRDomain := streamCfg.Name == hrName && hrJS != nil
		open := func(ctx context.Context) (msgFetcher, error) {
			if useHRDomain {
				cons, err := hrJS.CreateOrUpdateConsumer(ctx, streamCfg.Name, consumerCfg)
				if err != nil {
					return nil, fmt.Errorf("create %s consumer on domain %s: %w", streamCfg.Name, cfg.HRJetStreamDomain, err)
				}
				return rawConsumerAdapter{cons}, nil
			}
			cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, consumerCfg)
			if err != nil {
				return nil, fmt.Errorf("create %s consumer: %w", streamCfg.Name, err)
			}
			return o11yConsumerAdapter{cons}, nil
		}

		fetcher, err := newRecoveringFetcher(ctx, coll.ConsumerName(), open, stopCh)
		if err != nil {
			slog.Error("create consumer failed",
				"stream", streamCfg.Name,
				"consumer", coll.ConsumerName(),
				"error", err,
			)
			os.Exit(1)
		}
		checks = append(checks, fetcher.HealthCheck())
		if useHRDomain {
			slog.Info("HR consumer bound to remote JetStream domain",
				"domain", cfg.HRJetStreamDomain,
				"stream", streamCfg.Name,
				"consumer", coll.ConsumerName(),
			)
		}

		handler := NewHandler(&engineAdapter{engine: engine}, coll, cfg.BulkBatchSize)
		handler.metrics = esMetrics.forCollection(coll.ConsumerName())
		doneCh := make(chan struct{})
		doneChs = append(doneChs, doneCh)

		slog.Info("collection wired",
			"stream", streamCfg.Name,
			"consumer", coll.ConsumerName(),
			"filters", consumerCfg.FilterSubjects,
		)

		go runConsumer(ctx, fetcher, handler, consumerTuning{
			fetchBatchSize:    cfg.FetchBatchSize,
			bulkFlushInterval: bulkFlushInterval,
			pipelineDepth:     cfg.PipelineDepth,
		}, stopCh, doneCh)
	}

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled, checks...)
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
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
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

// flushPipeline overlaps up to `depth` ES bulk requests with the fetch and build of the
// batches behind them. The slot channel is the cap: unbounded concurrency would queue
// batches against ES without limit and blow through the consumer's ack-pending budget.
//
// Concurrent flushes can reach ES out of order, which is safe only because every
// collection carries an ordering guard — external versioning on messages and spotlight,
// a painless timestamp compare on user-room, a stream-sequence compare on spotlight-org.
// A collection added without one MUST run at depth 1.
//
// Only the consumer goroutine calls run/wait, so the struct needs no lock of its own.
type flushPipeline struct {
	slots chan struct{} // buffered to depth; a token is held for each in-flight flush
	wg    sync.WaitGroup
}

func newFlushPipeline(depth int) *flushPipeline {
	return &flushPipeline{slots: make(chan struct{}, depth)}
}

// wait blocks until every in-flight flush has finished.
func (p *flushPipeline) wait() {
	p.wg.Wait()
}

// run detaches whatever the handler has buffered and flushes it in the background,
// blocking for a slot once `depth` flushes are already in flight. Taking the batch first
// frees the buffer immediately, so ActionCount() reads 0 on return.
func (p *flushPipeline) run(ctx context.Context, h *Handler) {
	batch := h.Take()
	if batch == nil {
		return
	}
	p.slots <- struct{}{}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() { <-p.slots }()
		// jobguard recovers panics from the batch handler so a poison message or malformed
		// bulk response can't crash this collection's consumer; the batch's messages stay
		// un-acked and JetStream redelivers after AckWait.
		jobguard.Guard("search-sync flush", func() { h.FlushBatch(ctx, batch) })
	}()
}

// consumerTuning groups runConsumer's throughput knobs so call sites name them instead
// of passing a run of bare numbers.
type consumerTuning struct {
	fetchBatchSize    int
	bulkFlushInterval time.Duration
	pipelineDepth     int
}

// runConsumer is the batch-flush consumer loop: tune.fetchBatchSize bounds JetStream Fetch()
// pulls (client-tuning only), while the handler's own bulk size caps buffered ES actions
// (flushed on stopCh, size, or tune.bulkFlushInterval; the loop clamps fetch to remaining bulk
// capacity, but a fan-out message can still overshoot mid-loop and triggers its own flush).
//
// Each flush is handed to a flushPipeline holding up to pipelineDepth requests in flight, so
// ES round-trips overlap with the fetch and build behind them rather than serializing; the
// loop only stalls once every slot is busy, which is the backpressure we want.
func runConsumer(
	ctx context.Context,
	cons fetchSource,
	handler *Handler,
	tune consumerTuning,
	stopCh <-chan struct{},
	doneCh chan<- struct{},
) {
	defer close(doneCh)
	lastFlush := time.Now()

	// bulkBatchSize is the handler's own cap — read it from there rather than passing the
	// same number twice and letting the two drift.
	bulkBatchSize := handler.bulkSize

	pipe := newFlushPipeline(tune.pipelineDepth)
	// flushNow hands the buffered batch to the pipeline and restarts the interval clock.
	flushNow := func() {
		pipe.run(ctx, handler)
		lastFlush = time.Now()
	}
	// drain also waits on the in-flight bulk requests, so shutdown never abandons a write
	// that is already on its way to ES.
	drain := func() { flushNow(); pipe.wait() }
	add := func(msgCtx context.Context, m jetstream.Msg) {
		jobguard.Guard("search-sync add: "+m.Subject(), func() { handler.AddWithContext(msgCtx, m) })
	}
	// Flush before recovering, not on the usual interval: Recover can park for a whole
	// outage while it rebuilds, leaving built actions unindexed that long. It drains
	// rather than flushes, because a rebuild can easily outlast the in-flight bulk
	// requests the pipeline is still holding.
	flushBeforeRecover := func() {
		if handler.ActionCount() > 0 {
			drain()
		}
	}

	for {
		select {
		case <-stopCh:
			drain()
			return
		default:
		}

		// Bound the next Fetch by remaining bulk capacity so a steady stream of 1:1 messages can't
		// overshoot bulkBatchSize; fan-out messages may still push us over, handled mid-loop below.
		fetchCount := min(tune.fetchBatchSize, bulkBatchSize-handler.ActionCount())

		batch, err := cons.Fetch(ctx, fetchCount, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			select {
			case <-stopCh:
				drain()
				return
			default:
			}
			flushBeforeRecover()
			// Recover backs off and rebuilds; swallowing the error instead would
			// spin this loop against a consumer that will never answer again.
			if !cons.Recover(ctx, err) {
				drain()
				return
			}
			continue
		}

		// Always drain batch.Messages() to completion — the raw domain adapter re-channels via a
		// goroutine blocked on an unbuffered send; an early break would leak it and stall shutdown.
		for msg := range batch.Messages() {
			add(msg.Ctx, msg.Msg)
			// Size trigger, checked per message: a fan-out message can cross the cap on its own, and
			// flushing here keeps the next message's actions out of an already-oversized request.
			// Take() empties the buffer synchronously, so ActionCount stays under the cap after this.
			if handler.ActionCount() >= bulkBatchSize {
				flushNow()
			}
		}

		// Interval trigger: the size trigger above already fired for anything at the cap.
		if handler.ActionCount() > 0 && time.Since(lastFlush) >= tune.bulkFlushInterval {
			flushNow()
		}

		// Read only now that Messages is drained. This is the sole channel by
		// which a Fetch loop learns its consumer was deleted or its leader moved
		// — Fetch itself keeps returning empty batches and a nil error — so
		// skipping it turns a dead consumer into an indefinitely quiet one.
		batchErr := batch.Error()
		if batchErr != nil {
			flushBeforeRecover()
		}
		if !cons.Recover(ctx, batchErr) {
			drain()
			return
		}
	}
}

// checkBatchAckCoupling warns when the pipelined batches can't fit under maxAckPending.
// depth batches can be in flight while one more fills, so a 1:1 collection needs headroom
// for (depth+1)*bulkBatchSize un-acked messages; below that it stalls before the size-based
// flush can fire, undersizing every batch. Fan-out collections are unaffected. "" = no issue.
func checkBatchAckCoupling(bulkBatchSize, maxAckPending, pipelineDepth int) string {
	needed := (pipelineDepth + 1) * bulkBatchSize
	if needed > maxAckPending {
		return fmt.Sprintf(
			"BULK_BATCH_SIZE (%[1]d) at PIPELINE_DEPTH %[2]d needs CONSUMER_MAX_ACK_PENDING >= "+
				"%[3]d (%[2]d in flight plus one filling) but it is %[4]d: the size-based flush "+
				"can never fire for 1:1 collections, so flushes will wait the full "+
				"BULK_FLUSH_INTERVAL and batches stay undersized. Raise MAX_ACK_PENDING to "+
				"%[3]d, or lower BULK_BATCH_SIZE to %[5]d, or drop PIPELINE_DEPTH.",
			bulkBatchSize, pipelineDepth, needed, maxAckPending, maxAckPending/(pipelineDepth+1),
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

func (a *engineAdapter) UpdateByQuery(ctx context.Context, index string, body json.RawMessage) error {
	return a.engine.UpdateByQuery(ctx, index, body)
}

// consumerSource is the subset of Collection that buildConsumerConfig needs. Narrowing keeps the helper unit-testable with a small fake.
type consumerSource interface {
	ConsumerName() string
	FilterSubjects(siteID string) []string
}

// buildConsumerConfig adds the durable name and filters for one collection;
// everything else comes from ConsumerSettings.
func buildConsumerConfig(s stream.ConsumerSettings, coll consumerSource, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = coll.ConsumerName()
	if filters := coll.FilterSubjects(siteID); len(filters) > 0 {
		cc.FilterSubjects = filters
	}
	return cc
}
