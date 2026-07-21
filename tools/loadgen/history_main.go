package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/gocql/gocql"
	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/natsutil"
)

func runSeedHistory(ctx context.Context, cfg *config, preset string, seed int64) int {
	if cfg.CassandraHosts == "" {
		fmt.Fprintln(os.Stderr, "history workload requires CASSANDRA_HOSTS")
		return 2
	}
	p, ok := BuiltinHistoryPreset(preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown history preset: %s\n", preset)
		return 2
	}

	db, keyStore, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()

	session, err := connectCassandra(cfg)
	if err != nil {
		slog.Error("cassandra connect", "error", err)
		return 1
	}
	defer cassutil.Close(session)

	now := time.Now().UTC()
	res := BuildHistoryFixtures(&p, seed, cfg.SiteID, now)

	if err := Seed(ctx, db, &res.Fixtures); err != nil {
		slog.Error("seed mongo fixtures", "error", err)
		return 1
	}
	if err := SeedRoomKeys(ctx, keyStore, res.Fixtures.RoomKeys); err != nil {
		slog.Error("seed room keys", "error", err)
		return 1
	}
	if err := SeedThreadRooms(ctx, db, &res, cfg.SiteID); err != nil {
		slog.Error("seed thread rooms", "error", err)
		return 1
	}
	sizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)
	msgCount, err := SeedHistoryCassandra(ctx, session, sizer, &res, cfg.SiteID)
	if err != nil {
		slog.Error("seed cassandra messages", "error", err)
		return 1
	}

	slog.Info("seed complete (history)",
		"preset", p.Name,
		"users", len(res.Fixtures.Users),
		"rooms", len(res.Fixtures.Rooms),
		"subs", len(res.Fixtures.Subscriptions),
		"messages", msgCount,
		"threadParents", countThreadParents(res.ThreadParents),
		"bucketHours", cfg.MessageBucketHours)
	return 0
}

// runSeedReadReceipt seeds the same Mongo+Cassandra fixtures as the history
// workload, then stamps lastSeenAt on a readRatio fraction of each room's
// subscribers so the read-receipt RPC's ListReadReceipts query returns real
// readers. readRatio must be in (0, 1].
func runSeedReadReceipt(ctx context.Context, cfg *config, preset string, seed int64, readRatio float64) int {
	if readRatio <= 0 || readRatio > 1 {
		fmt.Fprintln(os.Stderr, "--read-ratio must be in (0, 1]")
		return 2
	}
	if cfg.CassandraHosts == "" {
		fmt.Fprintln(os.Stderr, "read-receipt workload requires CASSANDRA_HOSTS")
		return 2
	}
	p, ok := BuiltinHistoryPreset(preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown history preset: %s\n", preset)
		return 2
	}

	db, keyStore, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()

	session, err := connectCassandra(cfg)
	if err != nil {
		slog.Error("cassandra connect", "error", err)
		return 1
	}
	defer cassutil.Close(session)

	now := time.Now().UTC()
	res := BuildHistoryFixtures(&p, seed, cfg.SiteID, now)

	if err := Seed(ctx, db, &res.Fixtures); err != nil {
		slog.Error("seed mongo fixtures", "error", err)
		return 1
	}
	if err := SeedRoomKeys(ctx, keyStore, res.Fixtures.RoomKeys); err != nil {
		slog.Error("seed room keys", "error", err)
		return 1
	}
	if err := SeedThreadRooms(ctx, db, &res, cfg.SiteID); err != nil {
		slog.Error("seed thread rooms", "error", err)
		return 1
	}
	sizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)
	msgCount, err := SeedHistoryCassandra(ctx, session, sizer, &res, cfg.SiteID)
	if err != nil {
		slog.Error("seed cassandra messages", "error", err)
		return 1
	}
	plan := res.FullPlan()
	if err := SeedReadReceiptState(ctx, db, res.Fixtures.Subscriptions, &plan, readRatio, seed); err != nil {
		slog.Error("seed read-receipt reader state", "error", err)
		return 1
	}

	slog.Info("seed complete (read-receipt)",
		"preset", p.Name,
		"users", len(res.Fixtures.Users),
		"rooms", len(res.Fixtures.Rooms),
		"subs", len(res.Fixtures.Subscriptions),
		"messages", msgCount,
		"readRatio", readRatio,
		"bucketHours", cfg.MessageBucketHours)
	return 0
}

func runTeardownHistory(ctx context.Context, cfg *config, preset string, seed int64) int {
	if cfg.CassandraHosts == "" {
		fmt.Fprintln(os.Stderr, "history workload requires CASSANDRA_HOSTS")
		return 2
	}
	p, ok := BuiltinHistoryPreset(preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown history preset: %s\n", preset)
		return 2
	}

	db, keyStore, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()

	session, err := connectCassandra(cfg)
	if err != nil {
		slog.Error("cassandra connect", "error", err)
		return 1
	}
	defer cassutil.Close(session)

	now := time.Now().UTC()
	res := BuildHistoryFixtures(&p, seed, cfg.SiteID, now)
	roomIDs := roomIDsOf(res.Fixtures.Rooms)

	if err := Teardown(ctx, db); err != nil {
		slog.Error("teardown mongo", "error", err)
		return 1
	}
	if err := TeardownRoomKeys(ctx, keyStore, roomIDs); err != nil {
		slog.Error("teardown room keys", "error", err)
		return 1
	}
	if err := TeardownThreadRooms(ctx, db); err != nil {
		slog.Error("teardown thread rooms", "error", err)
		return 1
	}
	if err := TeardownHistoryCassandra(ctx, session); err != nil {
		slog.Error("teardown cassandra messages", "error", err)
		return 1
	}
	slog.Info("teardown complete (history)")
	return 0
}

func runHistorySustained(ctx context.Context, cfg *config, args []string) int {
	fs := flag.NewFlagSet("history-sustained", flag.ExitOnError)
	preset := fs.String("preset", "", "history preset name")
	seed := fs.Int64("seed", 42, "RNG seed")
	duration := fs.Duration("duration", 60*time.Second, "run duration")
	rate := fs.Int("rate", 200, "target req/sec")
	warmup := fs.Duration("warmup", 10*time.Second, "warmup window (samples discarded)")
	mixFlag := fs.String("mix", "history:80,thread:20", "endpoint mix")
	beforeModeFlag := fs.String("before-mode", "open:70,scrollback:30", "before-cursor mix")
	scrollbackPages := fs.Int("scrollback-pages", 5, "pages per scrollback chain before resetting")
	pageLimit := fs.Int("page-limit", 20, "LoadHistory/GetThreadMessages limit value")
	requestTimeout := fs.Duration("request-timeout", 5*time.Second, "per-request timeout")
	csvPath := fs.String("csv", "", "optional CSV output path")
	_ = fs.Parse(args)

	if *preset == "" {
		fmt.Fprintln(os.Stderr, "--preset required")
		return 2
	}
	p, ok := BuiltinHistoryPreset(*preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown history preset: %s\n", *preset)
		return 2
	}
	mix, err := ParseEndpointMix(*mixFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}
	beforeMode, err := ParseBeforeMode(*beforeModeFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}
	if *scrollbackPages <= 0 {
		fmt.Fprintln(os.Stderr, "--scrollback-pages must be > 0")
		return 2
	}

	nc, err := dialNATS(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect", "error", err)
		return 1
	}

	metrics := NewMetrics()
	metricsSrv := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           metrics.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("metrics server stopped", "error", err)
		}
	}()

	now := time.Now().UTC()
	res := BuildHistoryFixtures(&p, *seed, cfg.SiteID, now)
	collector := NewHistoryCollector()
	requester := newNATSHistoryRequester(nc.NatsConn())

	warmupDeadline := time.Now().Add(*warmup)
	genCfg := HistoryGeneratorConfig{
		Preset:          &p,
		Fixtures:        &res,
		SiteID:          cfg.SiteID,
		Rate:            *rate,
		Mix:             mix,
		BeforeMode:      beforeMode,
		ScrollbackPages: *scrollbackPages,
		PageLimit:       *pageLimit,
		RequestTimeout:  *requestTimeout,
		Requester:       requester,
		Collector:       collector,
		MaxInFlight:     cfg.MaxInFlight,
	}
	gen := NewHistoryGenerator(&genCfg, *seed)

	runCtx, cancelRun := context.WithTimeout(ctx, *duration)
	defer cancelRun()
	genErr := gen.Run(runCtx)
	// Drain trailing in-flight replies.
	time.Sleep(2 * time.Second)
	collector.DiscardBefore(warmupDeadline)

	shutCtx, cancelShut := context.WithTimeout(context.Background(), 5*time.Second)
	_ = metricsSrv.Shutdown(shutCtx)
	cancelShut()
	_ = nc.Drain()

	if genErr != nil {
		slog.Warn("generator returned error", "error", genErr)
	}

	bucketMs := int64(cfg.MessageBucketHours) * int64(time.Hour/time.Millisecond)
	single, multi := classifyBucketDepth(collector, bucketMs)
	measured := *duration - *warmup
	endpointStats := buildEndpointStats(collector)
	totalSamples := 0
	for i := range endpointStats {
		totalSamples += endpointStats[i].Count
	}
	actualRate := 0.0
	if measured > 0 {
		actualRate = float64(totalSamples) / measured.Seconds()
	}

	summary := HistorySummary{
		Preset: p.Name, Site: cfg.SiteID, Seed: *seed,
		TargetRate: *rate, ActualRate: actualRate,
		Duration: *duration, Warmup: *warmup,
		Sent:                totalSamples + collector.TimeoutErrors() + collector.ReplyErrors() + collector.BadReplyCount(),
		SentMeasured:        totalSamples,
		Mix:                 mix,
		BeforeMode:          beforeMode,
		PageLimit:           *pageLimit,
		Endpoints:           endpointStats,
		Timeouts:            collector.TimeoutErrors(),
		ReplyErrors:         collector.ReplyErrors(),
		BadReplies:          collector.BadReplyCount(),
		NoThreadParents:     collector.NoThreadParentsCount(),
		Saturation:          collector.SaturationCount(),
		SingleBucketReplies: single,
		MultiBucketReplies:  multi,
	}
	if err := PrintHistorySummary(os.Stdout, &summary); err != nil {
		slog.Warn("print summary", "error", err)
	}
	if *csvPath != "" {
		if err := writeHistoryCSVFile(*csvPath, collector); err != nil {
			slog.Error("csv export", "error", err)
		}
	}

	totalErrs := summary.Timeouts + summary.ReplyErrors + summary.BadReplies
	return DetermineExitCode(summary.SentMeasured, totalErrs)
}

func writeHistoryCSVFile(path string, c *HistoryCollector) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer func() { _ = f.Close() }()
	return writeHistoryCSV(f, c)
}

func connectCassandra(cfg *config) (*gocql.Session, error) {
	return cassutil.Connect(cassutil.Config{
		Hosts:    cfg.CassandraHosts,
		Keyspace: cfg.CassandraKeyspace,
		Username: cfg.CassandraUsername,
		Password: cfg.CassandraPassword,
	})
}

func countThreadParents(m map[string][]ThreadParentRef) int {
	total := 0
	for _, refs := range m {
		total += len(refs)
	}
	return total
}

// natsHistoryRequester is the production HistoryRequester. Each call performs
// nats.Conn.RequestMsgWithContext under a per-call timeout context, carrying the
// X-Request-ID from ctx on the message header (nil header when ctx has none, so
// callers that don't set a request ID are unaffected).
type natsHistoryRequester struct {
	nc *nats.Conn
}

func newNATSHistoryRequester(nc *nats.Conn) *natsHistoryRequester {
	return &natsHistoryRequester{nc: nc}
}

func (r *natsHistoryRequester) Request(ctx context.Context, subj string, data []byte, timeout time.Duration) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	msg, err := r.nc.RequestMsgWithContext(reqCtx, natsutil.NewMsg(ctx, subj, data))
	if err != nil {
		return nil, fmt.Errorf("nats request: %w", err)
	}
	return msg.Data, nil
}
