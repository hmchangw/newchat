package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	dto "github.com/prometheus/client_model/go"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

type config struct {
	NatsURL        string   `env:"NATS_URL,required"`
	NatsCredsFile  string   `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID         string   `env:"SITE_ID"         envDefault:"site-local"`
	MongoURI       string   `env:"MONGO_URI,required"`
	MongoDB        string   `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername  string   `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword  string   `env:"MONGO_PASSWORD"  envDefault:""`
	MetricsAddr    string   `env:"METRICS_ADDR"    envDefault:":9099"`
	MaxInFlight    int      `env:"MAX_IN_FLIGHT"   envDefault:"200"`
	PProfAddr      string   `env:"PPROF_ADDR"      envDefault:""`
	ValkeyAddrs    []string `env:"VALKEY_ADDRS,required" envSeparator:","`
	ValkeyPassword string   `env:"VALKEY_PASSWORD"       envDefault:""`
	// Cassandra is optional at startup so the existing messages/members
	// workloads keep working with no extra env. The history-* subcommands
	// fail-fast if CASSANDRA_HOSTS is empty.
	CassandraHosts     string `env:"CASSANDRA_HOSTS"        envDefault:""`
	CassandraKeyspace  string `env:"CASSANDRA_KEYSPACE"     envDefault:"chat"`
	CassandraUsername  string `env:"CASSANDRA_USERNAME"     envDefault:""`
	CassandraPassword  string `env:"CASSANDRA_PASSWORD"     envDefault:""`
	MessageBucketHours int    `env:"MESSAGE_BUCKET_HOURS"   envDefault:"72"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: loadgen <seed|run|teardown|members-sustained|members-capacity|history-sustained|max-rps> [flags]")
		os.Exit(2)
	}
	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	// SIGINT / SIGTERM cancel the base context. Each subcommand treats ctx
	// cancellation as "stop early but still run the end-of-run finalizers
	// (print summary, drain NATS, disconnect Mongo)".
	//
	// This deviates from CLAUDE.md's "use pkg/shutdown.Wait" guidance: that
	// helper blocks waiting for a signal and fires shutdown callbacks, which
	// doesn't fit a time-bounded CLI where the primary termination trigger is
	// the --duration timeout rather than an external signal. NotifyContext
	// gives us the same cleanup guarantee via context cancellation propagation.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	code := dispatch(ctx, &cfg)
	stop()
	os.Exit(code)
}

func dispatch(ctx context.Context, cfg *config) int {
	switch os.Args[1] {
	case "seed":
		return runSeed(ctx, cfg, os.Args[2:])
	case "run":
		return runRun(ctx, cfg, os.Args[2:])
	case "teardown":
		return runTeardown(ctx, cfg, os.Args[2:])
	case "members-sustained":
		return runMembersSustained(ctx, cfg, os.Args[2:])
	case "members-capacity":
		return runMembersCapacity(ctx, cfg, os.Args[2:])
	case "history-sustained":
		return runHistorySustained(ctx, cfg, os.Args[2:])
	case "max-rps":
		return runMaxRPS(ctx, cfg, os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", os.Args[1])
		return 2
	}
}

func runSeed(ctx context.Context, cfg *config, args []string) int {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	workload := fs.String("workload", "messages", "messages|members|history")
	preset := fs.String("preset", "", "preset name")
	seed := fs.Int64("seed", 42, "RNG seed")
	_ = fs.Parse(args)
	if *preset == "" {
		fmt.Fprintln(os.Stderr, "--preset required")
		return 2
	}
	switch *workload {
	case "messages":
		return runSeedMessages(ctx, cfg, *preset, *seed)
	case "members":
		return runSeedMembers(ctx, cfg, *preset, *seed)
	case "history":
		return runSeedHistory(ctx, cfg, *preset, *seed)
	default:
		fmt.Fprintf(os.Stderr, "unknown workload: %s\n", *workload)
		return 2
	}
}

func runSeedMessages(ctx context.Context, cfg *config, preset string, seed int64) int {
	p, ok := BuiltinPreset(preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown preset: %s\n", preset)
		return 2
	}
	db, keyStore, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()
	fixtures := BuildFixtures(&p, seed, cfg.SiteID)
	if err := Seed(ctx, db, &fixtures); err != nil {
		slog.Error("seed", "error", err)
		return 1
	}
	if err := SeedRoomKeys(ctx, keyStore, fixtures.RoomKeys); err != nil {
		slog.Error("seed room keys", "error", err)
		return 1
	}
	slog.Info("seed complete (messages)",
		"preset", p.Name,
		"users", len(fixtures.Users),
		"rooms", len(fixtures.Rooms),
		"subs", len(fixtures.Subscriptions),
		"roomKeys", len(fixtures.RoomKeys))
	return 0
}

func runSeedMembers(ctx context.Context, cfg *config, preset string, seed int64) int {
	p, ok := BuiltinMembersPreset(preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown members preset: %s\n", preset)
		return 2
	}
	db, keyStore, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()
	fixtures, pools := BuildMembersFixtures(&p, seed, cfg.SiteID)
	if err := Seed(ctx, db, &fixtures); err != nil {
		slog.Error("seed", "error", err)
		return 1
	}
	if err := SeedRoomKeys(ctx, keyStore, fixtures.RoomKeys); err != nil {
		slog.Error("seed room keys", "error", err)
		return 1
	}
	candCount := 0
	for _, ids := range pools {
		candCount += len(ids)
	}
	slog.Info("seed complete (members)",
		"preset", p.Name,
		"users", len(fixtures.Users),
		"rooms", len(fixtures.Rooms),
		"subs", len(fixtures.Subscriptions),
		"roomKeys", len(fixtures.RoomKeys),
		"candidatePoolTotal", candCount)
	return 0
}

func runTeardown(ctx context.Context, cfg *config, args []string) int {
	fs := flag.NewFlagSet("teardown", flag.ExitOnError)
	workload := fs.String("workload", "messages", "messages|members|history")
	preset := fs.String("preset", "", "preset name (required to identify which room keys to delete)")
	seed := fs.Int64("seed", 42, "RNG seed (must match the seed used at seed time)")
	_ = fs.Parse(args)
	if *preset == "" {
		fmt.Fprintln(os.Stderr, "--preset required")
		return 2
	}
	switch *workload {
	case "messages":
		return runTeardownMessages(ctx, cfg, *preset, *seed)
	case "members":
		return runTeardownMembers(ctx, cfg, *preset, *seed)
	case "history":
		return runTeardownHistory(ctx, cfg, *preset, *seed)
	default:
		fmt.Fprintf(os.Stderr, "unknown workload: %s\n", *workload)
		return 2
	}
}

func runTeardownMessages(ctx context.Context, cfg *config, preset string, seed int64) int {
	p, ok := BuiltinPreset(preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown preset: %s\n", preset)
		return 2
	}
	db, keyStore, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()
	fixtures := BuildFixtures(&p, seed, cfg.SiteID)
	roomIDs := roomIDsOf(fixtures.Rooms)
	if err := Teardown(ctx, db); err != nil {
		slog.Error("teardown", "error", err)
		return 1
	}
	if err := TeardownRoomKeys(ctx, keyStore, roomIDs); err != nil {
		slog.Error("teardown room keys", "error", err)
		return 1
	}
	slog.Info("teardown complete (messages)")
	return 0
}

func runTeardownMembers(ctx context.Context, cfg *config, preset string, seed int64) int {
	p, ok := BuiltinMembersPreset(preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown members preset: %s\n", preset)
		return 2
	}
	db, keyStore, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()
	fixtures, _ := BuildMembersFixtures(&p, seed, cfg.SiteID)
	roomIDs := roomIDsOf(fixtures.Rooms)
	if err := Teardown(ctx, db); err != nil {
		slog.Error("teardown", "error", err)
		return 1
	}
	if err := TeardownRoomKeys(ctx, keyStore, roomIDs); err != nil {
		slog.Error("teardown room keys", "error", err)
		return 1
	}
	slog.Info("teardown complete (members)")
	return 0
}

func runMembersSustained(ctx context.Context, cfg *config, args []string) int {
	fs := flag.NewFlagSet("members-sustained", flag.ExitOnError)
	preset := fs.String("preset", "", "members preset name")
	seed := fs.Int64("seed", 42, "RNG seed")
	duration := fs.Duration("duration", 60*time.Second, "run duration")
	rate := fs.Int("rate", 100, "target req/sec")
	warmup := fs.Duration("warmup", 10*time.Second, "warmup window (samples discarded)")
	inject := fs.String("inject", "frontdoor", "frontdoor|canonical")
	shapeFlag := fs.String("shape", "users", "users|orgs|channels|mixed (v1: users only)")
	usersPerAdd := fs.Int("users-per-add", 10, "users per add request")
	csvPath := fs.String("csv", "", "optional CSV output path")
	_ = fs.Parse(args)

	if *preset == "" {
		fmt.Fprintln(os.Stderr, "--preset required")
		return 2
	}
	p, ok := BuiltinMembersPreset(*preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown members preset: %s\n", *preset)
		return 2
	}
	injectMode, err := ParseInjectMode(*inject)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}
	shape, err := ParseShape(*shapeFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}
	if err := ValidateInjectShape(injectMode, shape); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}
	if *usersPerAdd <= 0 {
		fmt.Fprintln(os.Stderr, "--users-per-add must be > 0")
		return 2
	}

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect", "error", err)
		return 1
	}
	js, err := jetstream.New(nc.NatsConn())
	if err != nil {
		slog.Error("jetstream init", "error", err)
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

	fixtures, pools := BuildMembersFixtures(&p, *seed, cfg.SiteID)
	owners := OwnersByRoom(&fixtures)
	collector := NewMemberCollector(metrics, p.Name, injectMode)

	e2Sub, err := nc.NatsConn().Subscribe(subject.RoomMemberEventWildcard(), func(m *nats.Msg) {
		roomID, accounts, ok := ParseMemberAddBroadcast(m.Data)
		if !ok {
			return
		}
		collector.RecordBroadcast(roomID, accounts, time.Now())
	})
	if err != nil {
		slog.Error("subscribe e2", "error", err)
		return 1
	}
	defer func() { _ = e2Sub.Unsubscribe() }()

	var publisher MemberPublisher
	var frontdoor *frontdoorMemberPublisher
	switch injectMode {
	case InjectFrontdoor:
		frontdoor, err = newFrontdoorMemberPublisher(nc.NatsConn(), cfg.SiteID, func(corrID string, body []byte, at time.Time) {
			collector.RecordReply(corrID, string(body), at)
		})
		if err != nil {
			slog.Error("frontdoor publisher", "error", err)
			return 1
		}
		defer frontdoor.Close()
		publisher = frontdoor
	case InjectCanonical:
		publisher = newCanonicalMemberPublisher(js, cfg.SiteID)
	}

	samplerCtx, cancelSamplers := context.WithCancel(ctx)
	defer cancelSamplers()
	sampler := NewConsumerSampler(js, stream.Rooms(cfg.SiteID).Name, "room-worker", metrics, time.Second)
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		sampler.Run(samplerCtx)
	}()

	warmupDeadline := time.Now().Add(*warmup)
	genCfg := SustainedMembersConfig{
		Preset:         &p,
		Fixtures:       &fixtures,
		Pools:          pools,
		Owners:         owners,
		Rate:           *rate,
		UsersPerAdd:    *usersPerAdd,
		Inject:         injectMode,
		Shape:          shape,
		Publisher:      publisher,
		Metrics:        metrics,
		Collector:      collector,
		WarmupDeadline: warmupDeadline,
		MaxInFlight:    cfg.MaxInFlight,
	}
	gen := NewSustainedMembersGenerator(&genCfg, *seed)

	runCtx, cancelRun := context.WithTimeout(ctx, *duration)
	defer cancelRun()
	genErr := gen.Run(runCtx)
	time.Sleep(2 * time.Second) // drain trailing replies/broadcasts
	collector.DiscardBefore(warmupDeadline)
	missingReplies, missingBroadcasts := collector.Finalize()

	cancelSamplers()
	samplerWG.Wait()

	shutCtx, cancelShut := context.WithTimeout(context.Background(), 5*time.Second)
	_ = metricsSrv.Shutdown(shutCtx)
	cancelShut()
	_ = nc.Drain()

	if genErr != nil && !errors.Is(genErr, ErrPoolsExhausted) {
		slog.Error("generator error", "error", genErr)
	} else if errors.Is(genErr, ErrPoolsExhausted) {
		slog.Warn("aborted early", "reason", "pools exhausted")
	}

	mfs, _ := metrics.Registry.Gather()
	pubErrs := int(gatheredCounterValue(mfs, "loadgen_member_publish_errors_total", "reason", "publish"))
	rsErrs := collector.RoomServiceErrorCount()
	sentWarmup := int(gatheredCounterValue(mfs, "loadgen_member_published_total", "phase", "warmup"))
	sentMeasured := int(gatheredCounterValue(mfs, "loadgen_member_published_total", "phase", "measured"))
	sent := sentWarmup + sentMeasured
	measured := *duration - *warmup
	var actualRate float64
	if measured > 0 {
		actualRate = float64(sentMeasured) / measured.Seconds()
	}

	summary := MembersSummary{
		Preset: p.Name, Site: cfg.SiteID, Inject: string(injectMode), Shape: string(shape),
		Seed: *seed, TargetRate: *rate, ActualRate: actualRate,
		Duration: *duration, Warmup: *warmup, UsersPerAdd: *usersPerAdd,
		Sent: sent, SentMeasured: sentMeasured,
		PublishErrors: pubErrs, RoomServiceErrors: rsErrs,
		MissingReplies: missingReplies, MissingBroadcasts: missingBroadcasts,
		E1:      ComputePercentiles(collector.E1Samples()),
		E2:      ComputePercentiles(collector.E2Samples()),
		E1Count: collector.E1Count(), E2Count: collector.E2Count(),
		Consumers: []ConsumerStat{sampler.Snapshot()},
	}
	if err := PrintMembersSummary(os.Stdout, &summary); err != nil {
		slog.Warn("print summary", "error", err)
	}
	if *csvPath != "" {
		if err := writeMembersCSV(*csvPath, collector); err != nil {
			slog.Error("csv export", "error", err)
		}
	}
	totalErrs := summary.PublishErrors + summary.RoomServiceErrors + summary.MissingReplies + summary.MissingBroadcasts
	return DetermineExitCode(summary.SentMeasured, totalErrs)
}

func writeMembersCSV(path string, c *MemberCollector) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer func() { _ = f.Close() }()
	var rows []CSVSample
	for i, d := range c.E1Samples() {
		rows = append(rows, CSVSample{TimestampNs: int64(i), Metric: "E1", LatencyNs: d.Nanoseconds()})
	}
	for i, d := range c.E2Samples() {
		rows = append(rows, CSVSample{TimestampNs: int64(i), Metric: "E2", LatencyNs: d.Nanoseconds()})
	}
	return WriteCSV(f, rows)
}

func runMembersCapacity(ctx context.Context, cfg *config, args []string) int {
	fs := flag.NewFlagSet("members-capacity", flag.ExitOnError)
	preset := fs.String("preset", "", "members preset name")
	seed := fs.Int64("seed", 42, "RNG seed")
	inject := fs.String("inject", "frontdoor", "frontdoor|canonical")
	shapeFlag := fs.String("shape", "users", "users|orgs|channels|mixed (v1: users only)")
	usersPerAdd := fs.Int("users-per-add", 10, "users per add request")
	targetSize := fs.Int("target-size", 0, "stop each room when its member count >= target-size (required)")
	maxRate := fs.Int("max-rate", 0, "optional cap on per-room req/sec; 0 = sequential pacing only")
	e2Timeout := fs.Duration("e2-timeout", 30*time.Second, "max wait for broadcast per add")
	csvPath := fs.String("csv", "", "optional CSV output path")
	_ = fs.Parse(args)

	if *preset == "" {
		fmt.Fprintln(os.Stderr, "--preset required")
		return 2
	}
	if *targetSize <= 0 {
		fmt.Fprintln(os.Stderr, "--target-size required and must be > 0")
		return 2
	}
	p, ok := BuiltinMembersPreset(*preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown members preset: %s\n", *preset)
		return 2
	}
	injectMode, err := ParseInjectMode(*inject)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}
	shape, err := ParseShape(*shapeFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}
	if err := ValidateInjectShape(injectMode, shape); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}
	if *usersPerAdd <= 0 {
		fmt.Fprintln(os.Stderr, "--users-per-add must be > 0")
		return 2
	}

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect", "error", err)
		return 1
	}
	js, err := jetstream.New(nc.NatsConn())
	if err != nil {
		slog.Error("jetstream init", "error", err)
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

	fixtures, pools := BuildMembersFixtures(&p, *seed, cfg.SiteID)
	owners := OwnersByRoom(&fixtures)
	collector := NewMemberCollector(metrics, p.Name, injectMode)

	e2Sub, err := nc.NatsConn().Subscribe(subject.RoomMemberEventWildcard(), func(m *nats.Msg) {
		roomID, accounts, ok := ParseMemberAddBroadcast(m.Data)
		if !ok {
			return
		}
		collector.RecordBroadcast(roomID, accounts, time.Now())
	})
	if err != nil {
		slog.Error("subscribe e2", "error", err)
		return 1
	}
	defer func() { _ = e2Sub.Unsubscribe() }()

	var publisher MemberPublisher
	var frontdoor *frontdoorMemberPublisher
	switch injectMode {
	case InjectFrontdoor:
		frontdoor, err = newFrontdoorMemberPublisher(nc.NatsConn(), cfg.SiteID, func(corrID string, body []byte, at time.Time) {
			collector.RecordReply(corrID, string(body), at)
		})
		if err != nil {
			slog.Error("frontdoor publisher", "error", err)
			return 1
		}
		defer frontdoor.Close()
		publisher = frontdoor
	case InjectCanonical:
		publisher = newCanonicalMemberPublisher(js, cfg.SiteID)
	}

	genCfg := CapacityMembersConfig{
		Preset:      &p,
		Fixtures:    &fixtures,
		Pools:       pools,
		Owners:      owners,
		UsersPerAdd: *usersPerAdd,
		Inject:      injectMode,
		Shape:       shape,
		TargetSize:  *targetSize,
		MaxRate:     *maxRate,
		Publisher:   publisher,
		Metrics:     metrics,
		Collector:   collector,
		E2Timeout:   *e2Timeout,
	}
	gen := NewCapacityMembersGenerator(&genCfg)
	if err := gen.Run(ctx); err != nil {
		slog.Error("generator error", "error", err)
	}
	time.Sleep(2 * time.Second)
	collector.Finalize()

	shutCtx, cancelShut := context.WithTimeout(context.Background(), 5*time.Second)
	_ = metricsSrv.Shutdown(shutCtx)
	cancelShut()
	_ = nc.Drain()

	finals := map[string]int{}
	mfs, _ := metrics.Registry.Gather()
	for _, mf := range mfs {
		if mf.GetName() != "loadgen_member_room_size" {
			continue
		}
		for _, mt := range mf.GetMetric() {
			var rid string
			for _, l := range mt.GetLabel() {
				if l.GetName() == "room_id" {
					rid = l.GetValue()
				}
			}
			finals[rid] = int(mt.GetGauge().GetValue())
		}
	}
	pubErrs := int(gatheredCounterValue(mfs, "loadgen_member_publish_errors_total", "reason", "publish"))
	timeouts := int(gatheredCounterValue(mfs, "loadgen_member_publish_errors_total", "reason", "timeout"))

	edges := []int{0, *targetSize / 4, *targetSize / 2, (*targetSize * 3) / 4, *targetSize + 1}
	buckets := computeSizeBuckets(collector, finals, edges)

	summary := CapacitySummary{
		Preset: p.Name, Site: cfg.SiteID, Inject: string(injectMode), Shape: string(shape),
		Seed: *seed, UsersPerAdd: *usersPerAdd, TargetSize: *targetSize,
		PublishErrors: pubErrs, Timeouts: timeouts,
		Buckets: buckets, FinalSizes: finals,
	}
	if err := PrintCapacitySummary(os.Stdout, &summary); err != nil {
		slog.Warn("print summary", "error", err)
	}
	if *csvPath != "" {
		if err := writeMembersCSV(*csvPath, collector); err != nil {
			slog.Error("csv export", "error", err)
		}
	}
	return 0
}

// computeSizeBuckets is intentionally simple in v1 — it returns one row per
// bucket with the aggregate E1/E2 percentiles for samples whose source room's
// FINAL size fell in that bucket. (Per-sample size tracking is a v2
// enhancement; for now we treat each room's full latency tape as belonging
// to its final-size bucket.)
func computeSizeBuckets(c *MemberCollector, finals map[string]int, edges []int) []SizeBucket {
	out := make([]SizeBucket, 0, len(edges)-1)
	for i := 0; i < len(edges)-1; i++ {
		out = append(out, SizeBucket{Lower: edges[i], Upper: edges[i+1]})
	}
	for _, sz := range finals {
		idx := BucketIndex(sz, edges)
		if idx < 0 {
			continue
		}
		out[idx].Count++
	}
	e1 := ComputePercentiles(c.E1Samples())
	e2 := ComputePercentiles(c.E2Samples())
	for i := range out {
		if out[i].Count > 0 {
			out[i].E1 = e1
			out[i].E2 = e2
		}
	}
	return out
}

func roomIDsOf(rooms []model.Room) []string {
	out := make([]string, len(rooms))
	for i := range rooms {
		out[i] = rooms[i].ID
	}
	return out
}

// connectStores opens Mongo and Valkey. cleanup disconnects both; the caller
// must invoke it (typically via defer). On error, neither resource is leaked.
func connectStores(ctx context.Context, cfg *config) (*mongo.Database, roomkeystore.RoomKeyStore, func(), error) {
	client, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		slog.Error("mongo connect", "error", err)
		return nil, nil, nil, err
	}
	keyStore, err := connectKeyStore(cfg)
	if err != nil {
		mongoutil.Disconnect(ctx, client)
		slog.Error("valkey connect", "error", err)
		return nil, nil, nil, err
	}
	cleanup := func() {
		_ = keyStore.Close()
		mongoutil.Disconnect(ctx, client)
	}
	return client.Database(cfg.MongoDB), keyStore, cleanup, nil
}

func connectKeyStore(cfg *config) (roomkeystore.RoomKeyStore, error) {
	return roomkeystore.NewValkeyClusterStore(roomkeystore.ClusterConfig{
		Addrs:       cfg.ValkeyAddrs,
		Password:    cfg.ValkeyPassword,
		GracePeriod: time.Hour,
	})
}

func runRun(ctx context.Context, cfg *config, args []string) int {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	preset := fs.String("preset", "", "preset name")
	seed := fs.Int64("seed", 42, "RNG seed")
	duration := fs.Duration("duration", 60*time.Second, "run duration")
	rate := fs.Int("rate", 500, "target msgs/sec")
	warmup := fs.Duration("warmup", 10*time.Second, "warmup window (samples discarded)")
	inject := fs.String("inject", "frontdoor", "injection point: frontdoor|canonical")
	csvPath := fs.String("csv", "", "optional csv output path")
	_ = fs.Parse(args)
	if *preset == "" {
		fmt.Fprintln(os.Stderr, "--preset required")
		return 2
	}
	p, ok := BuiltinPreset(*preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown preset: %s\n", *preset)
		return 2
	}
	injectMode, err := ParseInjectMode(*inject)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 2
	}

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect", "error", err)
		return 1
	}
	js, err := jetstream.New(nc.NatsConn())
	if err != nil {
		slog.Error("jetstream init", "error", err)
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

	// pprof lives on a separate port, opt-in via PPROF_ADDR. Off by default
	// so the metrics endpoint (which Prometheus scrapes) doesn't
	// inadvertently expose profiling. Handlers are registered on a dedicated
	// mux rather than http.DefaultServeMux to avoid leaking debug endpoints
	// onto any other server that happens to use the default mux.
	var pprofSrv *http.Server
	if cfg.PProfAddr != "" {
		pprofMux := http.NewServeMux()
		pprofMux.HandleFunc("/debug/pprof/", pprof.Index)
		pprofMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		pprofMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		pprofMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		pprofMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		pprofSrv = &http.Server{
			Addr:              cfg.PProfAddr,
			Handler:           pprofMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			if err := pprofSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Warn("pprof server stopped", "error", err)
			}
		}()
		slog.Info("pprof server listening", "addr", cfg.PProfAddr)
	}

	fixtures := BuildFixtures(&p, *seed, cfg.SiteID)
	collector := NewCollector(metrics, p.Name)

	// E1 subscription: gatekeeper replies.
	e1Sub, err := nc.NatsConn().Subscribe(subject.UserResponseWildcard(), func(msg *nats.Msg) {
		reqID := lastToken(msg.Subject)
		var payload struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			// Malformed reply; count and drop per spec.
			metrics.PublishErrors.WithLabelValues(p.Name, "bad_reply").Inc()
			return
		}
		if payload.Error != "" {
			metrics.PublishErrors.WithLabelValues(p.Name, "gatekeeper").Inc()
		}
		collector.RecordReply(reqID, time.Now())
	})
	if err != nil {
		slog.Error("subscribe e1", "error", err)
		return 1
	}
	defer func() { _ = e1Sub.Unsubscribe() }()

	// E2 subscription: broadcast events.
	e2Handler := newE2Handler(collector)

	e2Sub, err := nc.NatsConn().Subscribe(subject.RoomEventWildcard(), e2Handler)
	if err != nil {
		slog.Error("subscribe e2", "error", err)
		return 1
	}
	defer func() { _ = e2Sub.Unsubscribe() }()

	// Broadcast-worker emits DM broadcasts on chat.user.{account}.event.room
	// (see pkg/subject.UserRoomEvent). Subscribe to both so E2 correlation
	// covers both group and DM rooms.
	e2DMSub, err := nc.NatsConn().Subscribe(subject.UserRoomEventWildcard(), e2Handler)
	if err != nil {
		slog.Error("subscribe e2 dm", "error", err)
		return 1
	}
	defer func() { _ = e2DMSub.Unsubscribe() }()

	canonical := stream.MessagesCanonical(cfg.SiteID)
	samplerCtx, cancelSamplers := context.WithCancel(ctx)
	defer cancelSamplers()
	samplers := []*ConsumerSampler{
		NewConsumerSampler(js, canonical.Name, "message-worker", metrics, 1*time.Second),
		NewConsumerSampler(js, canonical.Name, "broadcast-worker", metrics, 1*time.Second),
	}
	var samplerWG sync.WaitGroup
	for _, s := range samplers {
		samplerWG.Add(1)
		go func(s *ConsumerSampler) {
			defer samplerWG.Done()
			s.Run(samplerCtx)
		}(s)
	}

	publisher := newNatsCorePublisher(nc.NatsConn(), injectMode, js)

	warmupDeadline := time.Now().Add(*warmup)
	gen := NewGenerator(&GeneratorConfig{
		Preset:         &p,
		Fixtures:       fixtures,
		SiteID:         cfg.SiteID,
		Rate:           *rate,
		Inject:         injectMode,
		Publisher:      publisher,
		Metrics:        metrics,
		Collector:      collector,
		WarmupDeadline: warmupDeadline,
		MaxInFlight:    cfg.MaxInFlight,
	}, *seed)

	runCtx, cancelRun := context.WithTimeout(ctx, *duration)
	defer cancelRun()
	genErr := gen.Run(runCtx)
	// Wait up to 2 seconds for trailing replies and broadcasts to arrive.
	time.Sleep(2 * time.Second)
	collector.DiscardBefore(warmupDeadline)
	missingReplies, missingBroadcasts := collector.Finalize()

	cancelSamplers()
	samplerWG.Wait()

	shutCtx, cancelShut := context.WithTimeout(context.Background(), 5*time.Second)
	_ = metricsSrv.Shutdown(shutCtx)
	if pprofSrv != nil {
		_ = pprofSrv.Shutdown(shutCtx)
	}
	cancelShut()
	_ = nc.Drain()

	if genErr != nil {
		slog.Error("generator error", "error", genErr)
	}

	mfs, gerr := metrics.Registry.Gather()
	if gerr != nil {
		slog.Warn("metrics gather", "error", gerr)
		mfs = nil
	}
	publishErrs := gatheredCounterValue(mfs, "loadgen_publish_errors_total", "", "")
	gkErrs := gatheredCounterValue(mfs, "loadgen_publish_errors_total", "reason", "gatekeeper")
	sentWarmup := int(gatheredCounterValue(mfs, "loadgen_published_total", "phase", "warmup"))
	sentMeasured := int(gatheredCounterValue(mfs, "loadgen_published_total", "phase", "measured"))
	sent := sentWarmup + sentMeasured
	measured := *duration - *warmup
	actualRate := 0.0
	if measured > 0 {
		// In canonical mode, byReqID is never populated, so E1Count/missingReplies
		// are both 0. Fall back to sentMeasured to compute the true publish rate
		// for the measured window only.
		switch injectMode {
		case InjectCanonical:
			actualRate = float64(sentMeasured) / measured.Seconds()
		default:
			actualRate = float64(collector.E1Count()+missingReplies) / measured.Seconds()
		}
	}

	summary := Summary{
		Preset:            p.Name,
		Seed:              *seed,
		Site:              cfg.SiteID,
		TargetRate:        *rate,
		ActualRate:        actualRate,
		Duration:          *duration,
		Warmup:            *warmup,
		Inject:            *inject,
		Sent:              sent,
		SentMeasured:      sentMeasured,
		PublishErrors:     int(publishErrs - gkErrs),
		GatekeeperErrors:  int(gkErrs),
		MissingReplies:    missingReplies,
		MissingBroadcasts: missingBroadcasts,
		E1:                ComputePercentiles(collector.E1Samples()),
		E2:                ComputePercentiles(collector.E2Samples()),
		E1Count:           collector.E1Count(),
		E2Count:           collector.E2Count(),
		Consumers:         []ConsumerStat{samplers[0].Snapshot(), samplers[1].Snapshot()},
	}
	if err := PrintSummary(os.Stdout, &summary); err != nil {
		slog.Warn("print summary", "error", err)
	}

	if *csvPath != "" {
		if err := writeCSVFile(*csvPath, collector); err != nil {
			slog.Error("csv export", "error", err)
		}
	}

	totalErrs := summary.PublishErrors + summary.GatekeeperErrors + summary.MissingReplies + summary.MissingBroadcasts
	return DetermineExitCode(summary.SentMeasured, totalErrs)
}

// newE2Handler reads only LastMsgID from the cleartext envelope so the same
// handler works for plaintext and encrypted broadcasts (Message is nil under
// encryption). Decoding into a minimal struct avoids per-event allocation of
// the full RoomEvent's slices and pointers on this high-frequency path.
func newE2Handler(collector *Collector) func(*nats.Msg) {
	type envelope struct {
		LastMsgID string `json:"lastMsgId"`
	}
	return func(msg *nats.Msg) {
		var evt envelope
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			return
		}
		if evt.LastMsgID == "" {
			return
		}
		collector.RecordBroadcast(evt.LastMsgID, time.Now())
	}
}

type natsCorePublisher struct {
	nc           *nats.Conn
	useJetStream bool
	js           jetstream.JetStream
}

func newNatsCorePublisher(nc *nats.Conn, inject InjectMode, js jetstream.JetStream) *natsCorePublisher {
	return &natsCorePublisher{nc: nc, useJetStream: inject == InjectCanonical, js: js}
}

func (p *natsCorePublisher) Publish(ctx context.Context, subject string, data []byte) error {
	if p.useJetStream {
		if _, err := p.js.Publish(ctx, subject, data); err != nil {
			return fmt.Errorf("jetstream publish: %w", err)
		}
		return nil
	}
	if err := p.nc.Publish(subject, data); err != nil {
		return fmt.Errorf("core publish: %w", err)
	}
	return nil
}

func lastToken(subj string) string {
	i := strings.LastIndex(subj, ".")
	if i < 0 {
		return subj
	}
	return subj[i+1:]
}

func writeCSVFile(path string, c *Collector) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer func() { _ = f.Close() }()
	var rows []CSVSample
	for i, d := range c.E1Samples() {
		rows = append(rows, CSVSample{TimestampNs: int64(i), Metric: "E1", LatencyNs: d.Nanoseconds()})
	}
	for i, d := range c.E2Samples() {
		rows = append(rows, CSVSample{TimestampNs: int64(i), Metric: "E2", LatencyNs: d.Nanoseconds()})
	}
	return WriteCSV(f, rows)
}

func gatheredCounterValue(mfs []*dto.MetricFamily, name string, labelName, labelValue string) float64 {
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if labelName == "" {
				total += metric.GetCounter().GetValue()
				continue
			}
			for _, l := range metric.GetLabel() {
				if l.GetName() == labelName && l.GetValue() == labelValue {
					total += metric.GetCounter().GetValue()
				}
			}
		}
	}
	return total
}

func counterValue(m *Metrics, name string) float64 {
	mfs, err := m.Registry.Gather()
	if err != nil {
		slog.Warn("metrics gather", "error", err)
		return 0
	}
	return gatheredCounterValue(mfs, name, "", "")
}

func counterValueLabeled(m *Metrics, name, labelName, labelValue string) float64 {
	mfs, err := m.Registry.Gather()
	if err != nil {
		slog.Warn("metrics gather", "error", err)
		return 0
	}
	return gatheredCounterValue(mfs, name, labelName, labelValue)
}
