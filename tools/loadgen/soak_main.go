package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/catalog"
	collect "github.com/hmchangw/chat/tools/loadgen/internal/soak/collector"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/distribution"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/mutation"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/presence"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/read"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/run"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/search"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/send"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
	soakuserread "github.com/hmchangw/chat/tools/loadgen/internal/soak/userread"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/workload"
)

const (
	soakDefaultSeed          = int64(42)
	soakRequestTimeout       = 5 * time.Second
	soakEncryptionPollPeriod = 250 * time.Millisecond
)

type soakEncryptionStore interface {
	HasWrappedDEK(context.Context, string) (bool, error)
}

type soakRuntimeStore interface {
	soakLifecycleStore
	soakEncryptionStore
	LoadTopology(context.Context, string, string) (topology.Topology, error)
}

// soakDefaultPageLimit is how many messages a soak read asks for per page.
//
// The binding constraint is the broker's max_payload, not history-service's
// maxPageSize (100). A read reply carries pageLimit messages, each up to
// SOAK_PAYLOAD_MAX_BYTES — 10 KB by default, well under history-service's 20 KB
// content cap, which this workload never reaches. At those defaults a page of
// 15 is ~150 KB, inside the 256 KB budget; the old hardcoded 50 was ~500 KB and
// came back as pkg/natsutil's oversize envelope instead of data.
//
// Because SOAK_PAYLOAD_MAX_BYTES is configurable, no constant is safe on its
// own: validateSoakPageBudget checks the actual pair at startup.
const soakDefaultPageLimit = 15

// soakRoomInfoBatchSize is how many rooms a room-read batch asks about. It is
// far below the payload budget a message page needs, since a room record is
// metadata rather than message bodies.
const soakRoomInfoBatchSize = 20

// soakWalkRowBudget is how many clustered rows a paged walk must be able to
// reach, independent of how the pages are sized.
//
// MaxPages used to be hardcoded at 100, which was a 5,000-row reach at the old
// 50-row page. Lowering the page to fit the broker payload silently cut that to
// 1,500: once a partition grew past it, older rows became unreachable to the
// verifier and a mutation there would be missed or misreported as
// target-missing. Page size is a transport constraint and walk depth is a
// coverage requirement, so the two must not be the same number.
const soakWalkRowBudget = 5000

// soakMaxPages converts the row budget into a page count for the configured
// page size, so a smaller page buys more pages rather than less history.
func soakMaxPages(pageLimit int) int {
	if pageLimit <= 0 {
		return 1
	}
	return (soakWalkRowBudget + pageLimit - 1) / pageLimit
}

// soakOptions are the soak run's command-line knobs.
type soakOptions struct {
	Seed      int64
	PageLimit int
	// PoolOut, when non-empty, makes the seed phase write the clientsim
	// pool artifact (the borrowed active users' accounts) to this path.
	PoolOut string
}

func parseSoakArgs(args []string) (soakOptions, error) {
	fs := flag.NewFlagSet("soak", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	seed := fs.Int64("seed", soakDefaultSeed, "RNG seed")
	pageLimit := fs.Int("page-limit", soakDefaultPageLimit,
		"messages requested per read page; keep it under broker max_payload / message size")
	if err := fs.Parse(args); err != nil {
		return soakOptions{}, fmt.Errorf("parse soak arguments: %w", err)
	}
	if fs.NArg() != 0 {
		return soakOptions{}, fmt.Errorf("soak does not accept positional arguments")
	}
	if *pageLimit <= 0 {
		return soakOptions{}, fmt.Errorf("page-limit must be > 0, got %d", *pageLimit)
	}
	return soakOptions{Seed: *seed, PageLimit: *pageLimit}, nil
}

func waitForSoakWrappedDEK(
	ctx context.Context,
	store soakEncryptionStore,
	roomID string,
	pollInterval time.Duration,
) error {
	if store == nil {
		return fmt.Errorf("wrapped DEK store is required")
	}
	if roomID == "" {
		return fmt.Errorf("wrapped DEK room ID is required")
	}
	if pollInterval <= 0 {
		pollInterval = soakEncryptionPollPeriod
	}
	for {
		found, err := store.HasWrappedDEK(ctx, roomID)
		if err != nil {
			return fmt.Errorf("check wrapped DEK for room %q: %w", roomID, err)
		}
		if found {
			return nil
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf(
				"wait for wrapped DEK evidence in room %q: %w",
				roomID,
				ctx.Err(),
			)
		case <-timer.C:
		}
	}
}

type soakReadCollectorRecorder struct {
	collector *collect.Collector
	now       func() time.Time
}

func (r *soakReadCollectorRecorder) Record(sample *read.Sample) {
	outcome := collect.OutcomeSucceeded
	if sample.Skipped {
		outcome = collect.OutcomeSkipped
	} else if sample.ErrorClass != "" {
		outcome = collect.OutcomeFailed
	}
	recordSoakSample(r.collector.Record(&collect.Sample{
		Action: sample.Action, Outcome: outcome, At: r.now(),
		Latency: sample.Latency, Retries: sample.Retries,
		ErrorClass: sample.ErrorClass, ErrorReason: sample.ErrorReason,
		RowsCounted: sample.RowsCounted, ReplyBytes: sample.ReplyBytes, Rows: sample.Messages,
	}))
}

// recordSoakSample surfaces a rejected sample. The collector validates every
// label it is handed, so a value outside a bounded set drops the measurement —
// and a silently missing metric is indistinguishable from a healthy run.
func recordSoakSample(err error) {
	if err != nil {
		slog.Error("record Cassandra soak sample", "error", err)
	}
}

type soakMutationCollectorRecorder struct {
	collector *collect.Collector
	now       func() time.Time
}

func (r *soakMutationCollectorRecorder) Record(sample mutation.Sample) {
	outcome := collect.OutcomeSucceeded
	if sample.Skipped {
		outcome = collect.OutcomeSkipped
	} else if sample.ErrorClass != "" {
		outcome = collect.OutcomeFailed
	}
	recordSoakSample(r.collector.Record(&collect.Sample{
		Action: sample.Action, Outcome: outcome, At: r.now(),
		Latency: sample.Latency, Retries: sample.Retries,
		ErrorClass: sample.ErrorClass, ErrorReason: sample.ErrorReason,
		TargetMissing: sample.TargetMissing,
	}))
}

type soakVerifyCollectorRecorder struct {
	collector *collect.Collector
	now       func() time.Time
}

func (r *soakVerifyCollectorRecorder) Record(result *read.VerifyResult) {
	if result == nil {
		return
	}
	recordSoakSample(r.collector.RecordVerification(result))
	outcome := collect.OutcomeFailed
	switch result.Class {
	case read.VerifyOK:
		outcome = collect.OutcomeSucceeded
	case read.VerifySkipped:
		outcome = collect.OutcomeSkipped
	case read.VerifyMissing, read.VerifyMismatch, read.VerifyMalformed,
		read.VerifyRetryable, read.VerifyRPCError:
		outcome = collect.OutcomeFailed
	}
	recordSoakSample(r.collector.Record(&collect.Sample{
		Action: result.Action, Outcome: outcome, At: r.now(),
		Latency: result.Latency, Retries: result.Retries,
		ErrorClass: result.RPCErrorClass, ErrorReason: result.RPCErrorReason,
	}))
}

type soakCollectorRecorders struct {
	read     *soakReadCollectorRecorder
	mutation *soakMutationCollectorRecorder
	verify   *soakVerifyCollectorRecorder
}

func newSoakCollectorRecorders(
	collector *collect.Collector,
	now func() time.Time,
) soakCollectorRecorders {
	if now == nil {
		now = time.Now
	}
	return soakCollectorRecorders{
		read:     &soakReadCollectorRecorder{collector: collector, now: now},
		mutation: &soakMutationCollectorRecorder{collector: collector, now: now},
		verify:   &soakVerifyCollectorRecorder{collector: collector, now: now},
	}
}

type soakRuntimeSelector struct {
	mu      sync.Mutex
	rooms   []string
	members map[string][]send.Target
	picker  *distribution.RoomPicker
	sizer   *distribution.PayloadSizer
	rng     *rand.Rand
}

func newSoakRuntimeSelector(
	shape *topology.Topology,
	cfg *soakConfig,
	seed int64,
) (*soakRuntimeSelector, error) {
	if shape == nil || len(shape.Rooms) == 0 {
		return nil, fmt.Errorf("soak topology has no rooms")
	}
	if cfg == nil {
		return nil, fmt.Errorf("soak configuration is required")
	}
	picker, err := distribution.NewRoomPicker(
		seed, len(shape.Rooms), cfg.RoomZipfS, cfg.RoomZipfV,
	)
	if err != nil {
		return nil, fmt.Errorf("build soak room distribution: %w", err)
	}
	sizer, err := distribution.NewPayloadSizer(
		seed+1,
		cfg.PayloadMedianBytes,
		cfg.PayloadP95Bytes,
		cfg.PayloadMaxBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("build soak payload distribution: %w", err)
	}
	selector := &soakRuntimeSelector{
		rooms: make([]string, len(shape.Rooms)), picker: picker, sizer: sizer,
		rng:     rand.New(rand.NewSource(seed + 2)),
		members: make(map[string][]send.Target, len(shape.Rooms)),
	}
	for i := range shape.Rooms {
		selector.rooms[i] = shape.Rooms[i].ID
	}
	active := topology.ActiveUserIDs(shape)
	roomTypes := make(map[string]model.RoomType, len(shape.Rooms))
	for i := range shape.Rooms {
		roomTypes[shape.Rooms[i].ID] = shape.Rooms[i].Type
	}
	recipients := soakRecipientSets(shape)
	for i := range shape.Subscriptions {
		subscription := &shape.Subscriptions[i]
		if !topology.IsActiveSubscription(subscription, active) ||
			subscription.User.ID == "" ||
			subscription.User.Account == "" {
			continue
		}
		selector.members[subscription.RoomID] = append(
			selector.members[subscription.RoomID],
			send.Target{
				UserID: subscription.User.ID, Account: subscription.User.Account,
				RoomID: subscription.RoomID, RoomType: roomTypes[subscription.RoomID],
				Recipients:           append([]string(nil), recipients[subscription.RoomID]...),
				RecipientSetSource:   send.RecipientSourceTopology,
				RecipientSetComplete: true,
				RecipientRoute:       recipientRouteForRoomType(roomTypes[subscription.RoomID]),
			},
		)
	}
	for _, roomID := range selector.rooms {
		if len(selector.members[roomID]) == 0 {
			return nil, fmt.Errorf("soak room %q has no subscribed member", roomID)
		}
	}
	return selector, nil
}

func recipientRouteForRoomType(roomType model.RoomType) send.RecipientExpectedRoute {
	if roomType == model.RoomTypeChannel {
		return send.RecipientRouteRoom
	}
	return send.RecipientRouteUser
}

func soakRecipientSets(shape *topology.Topology) map[string][]string {
	if shape == nil {
		return nil
	}
	recipients := make(map[string][]string, len(shape.Rooms))
	// ActiveUsers bounds sender selection only. Broadcast evidence must retain
	// every subscribed human account, including borrowed non-senders that the
	// production broadcast worker is still required to reach.
	for i := range shape.Subscriptions {
		subscription := &shape.Subscriptions[i]
		if !topology.IsRoomMember(subscription) ||
			subscription.User.IsBot || subscription.User.Account == "" {
			continue
		}
		recipients[subscription.RoomID] = append(recipients[subscription.RoomID], subscription.User.Account)
	}
	for roomID := range recipients {
		slices.Sort(recipients[roomID])
		recipients[roomID] = slices.Compact(recipients[roomID])
	}
	return recipients
}

func (s *soakRuntimeSelector) nextRoom() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rooms[s.picker.Next()]
}

func (s *soakRuntimeSelector) nextSend() (send.Target, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	roomID := s.rooms[s.picker.Next()]
	targets := s.members[roomID]
	target := targets[s.rng.Intn(len(targets))]
	return target, distribution.ContentOfSize(s.sizer.NextContentBytes())
}

type soakSendObservation struct {
	result send.ReplyResult
}

func runSoakSeed(
	ctx context.Context,
	cfg *config,
	seed int64,
	poolOut string,
) int {
	db, keyStore, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()
	seeded, err := run.Seed(
		ctx,
		run.NewMongo(db),
		keyStore,
		soakSeedInputFrom(cfg, seed),
		topology.NewProductionIdentitySource(),
	)
	if err != nil {
		slog.Error("seed Cassandra soak topology", "runId", cfg.Soak.RunID, "error", err)
		return 1
	}
	if poolOut != "" {
		if err := writePoolArtifact(poolOut, cfg.Soak.RunID, cfg.SiteID,
			digestSoakConfig(&cfg.Soak), seeded.ActiveUsers); err != nil {
			slog.Error("write pool artifact", "error", err, "path", poolOut)
			return 1
		}
		slog.Info("pool artifact written", "path", poolOut, "accounts", len(seeded.ActiveUsers))
	}
	slog.Info(
		"Cassandra soak topology seeded",
		"runId", cfg.Soak.RunID,
		"borrowedUsers", len(seeded.BorrowedUsers),
		"activeUsers", len(seeded.ActiveUsers),
		"rooms", len(seeded.Rooms),
		"subscriptions", len(seeded.Subscriptions),
	)
	return 0
}

// soakShutdownExitCode folds a ledger close failure into the run's result. A
// close that could not make an invalidation durable means this run's evidence
// will not be recognised as disowned when it is replayed, which is a failed run
// however well the traffic went. An earlier failure keeps its own code: it is
// closer to the cause.
// errSoakLedgerNotDurable is the cancellation cause the durability watch uses.
// A run it stopped leaves through the same cancellation an operator's SIGTERM
// uses, so without a cause the two are indistinguishable and a truncated run
// reports success.
var errSoakLedgerNotDurable = errors.New(
	"failure ledger cannot record the verdict disqualifying its evidence")

// soakRunExitCode decides the run's verdict from how the workload ended and why
// its context was cancelled. Lost durability outranks a workload error: it is
// the reason the run stopped, and 2 is the code this command already uses for
// the evidence machinery failing rather than the system under test.
func soakRunExitCode(runErr, cause error) int {
	if errors.Is(cause, errSoakLedgerNotDurable) {
		return 2
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return 1
	}
	return 0
}

func soakShutdownExitCode(current int, closeErr error) int {
	if closeErr == nil || current != 0 {
		return current
	}
	return 2
}

func runSoakWorkload(
	ctx context.Context,
	cfg *config,
	opts soakOptions,
) (exitCode int) {
	seed := opts.Seed
	fastLeaseAbort := false
	// Zero until the run stops because its lease is at risk. The NATS drain is
	// registered before the ledger exists, so both are declared here and filled
	// in once the workload has returned.
	leaseShutdownBudget := time.Duration(0)
	invalidateEvidence := func(string) {}
	client, err := mongoutil.Connect(
		ctx,
		cfg.MongoURI,
		cfg.MongoUsername,
		cfg.MongoPassword,
	)
	if err != nil {
		slog.Error("connect Mongo for Cassandra soak", "error", err)
		return 1
	}
	defer func() {
		if !fastLeaseAbort {
			mongoutil.Disconnect(context.Background(), client)
		}
	}()
	store := run.NewMongo(client.Database(cfg.MongoDB))

	topology, err := store.LoadTopology(ctx, cfg.Soak.RunID, cfg.SiteID)
	if err != nil {
		slog.Error("load Cassandra soak topology", "runId", cfg.Soak.RunID, "error", err)
		return 1
	}
	slog.Info("Cassandra soak topology loaded",
		append([]any{"runId", cfg.Soak.RunID},
			summarizeSoakTopology(&topology, cfg.Soak.SendRate).LogValues()...)...)
	selector, err := newSoakRuntimeSelector(&topology, &cfg.Soak, seed)
	if err != nil {
		slog.Error("prepare Cassandra soak distributions", "error", err)
		return 1
	}

	metrics := NewMetrics()
	heartbeatStatus := newSoakHeartbeatStatus(metrics)
	shutdownMongoProbe := startSoakMongoProbe(ctx, client, metrics, time.Now)
	defer func() {
		if !fastLeaseAbort {
			shutdownMongoProbe()
		}
	}()
	reconcileBreaches := warnSoakReconcileConfig(&cfg.Soak)
	setSoakRunInfo(metrics, cfg.Soak.Environment)
	defer func() {
		if !fastLeaseAbort {
			metrics.stopNATSHealth()
		}
	}()
	nc, err := dialNATSWithMetrics(cfg.NatsURL, cfg.NatsCredsFile, metrics)
	if err != nil {
		slog.Error("connect NATS for Cassandra soak", "error", err)
		return 1
	}
	defer func() {
		if fastLeaseAbort {
			return
		}
		drainSoakNATS(nc.NatsConn(), leaseShutdownBudget, invalidateEvidence)
	}()
	// Fail before any load is generated: derive max_payload from the connected
	// server's INFO so non-default brokers are neither overrun nor needlessly
	// constrained by a local constant.
	if err := validateSoakPageBudget(
		opts.PageLimit,
		cfg.Soak.PayloadMaxBytes,
		nc.NatsConn().MaxPayload(),
	); err != nil {
		slog.Error("soak page budget rejected", "error", err)
		return 2
	}

	metricsServer := startSoakMetricsServer(cfg.MetricsAddr, metrics)
	defer func() {
		if fastLeaseAbort {
			return
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("stop Cassandra soak metrics server", "error", err)
		}
	}()

	pprofServer, pprofErr := startSoakPProfServer(cfg.PProfAddr)
	if pprofErr != nil {
		slog.Error("start Cassandra soak pprof server", "error", pprofErr)
		return 2
	}
	if pprofServer != nil {
		defer func() {
			if fastLeaseAbort {
				return
			}
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := pprofServer.Shutdown(shutdownCtx); err != nil {
				slog.Error("stop Cassandra soak pprof server", "error", err)
			}
		}()
	}
	heapProfiler, heapProfilerErr := newSoakHeapProfiler(soakHeapProfileConfig{
		Dir: cfg.Soak.HeapProfileDir, Keep: cfg.Soak.HeapProfileKeep,
	})
	if heapProfilerErr != nil {
		slog.Error("open Cassandra soak heap profiler", "error", heapProfilerErr)
		return 2
	}
	if heapProfiler != nil {
		profileCtx, stopProfiling := context.WithCancel(ctx)
		profileTicker := time.NewTicker(cfg.Soak.HeapProfileInterval)
		profileDone := make(chan struct{})
		go func() {
			defer close(profileDone)
			defer profileTicker.Stop()
			heapProfiler.Run(profileCtx, profileTicker.C)
		}()
		defer func() {
			if fastLeaseAbort {
				return
			}
			stopProfiling()
			<-profileDone
		}()
	}

	now := time.Now
	collectorDuration := cfg.Soak.RunDuration
	if cfg.Soak.RunMode == soakRunModeContinuous {
		collectorDuration = 0
	}
	collector := collect.New(
		&soakCollectorMetrics{metrics: metrics},
		now(),
		cfg.Soak.Warmup,
		collectorDuration,
	)
	recorders := newSoakCollectorRecorders(collector, now)
	ledger, degradedLedger, err := openSoakFailureObservationLedger(&cfg.Soak, metrics, now)
	if err != nil {
		slog.Error("open Cassandra soak failure ledger", "error", err)
		return 2
	}
	if degradedLedger {
		slog.Error("durable failure ledger unavailable; continuing with invalid in-memory observation")
	}
	if err := recordSoakStartupInvalidations(ledger, reconcileBreaches); err != nil {
		slog.Error("record Cassandra soak startup invalidation", "error", err)
		if closeErr := ledger.Close(); closeErr != nil {
			slog.Error("close Cassandra soak failure ledger", "error", closeErr)
		}
		return 2
	}
	if dropped := ledger.Snapshot().Dropped; dropped > 0 {
		metrics.FailureDropped.Add(float64(dropped))
	}
	invalidateEvidence = ledger.Invalidate
	defer func() {
		if fastLeaseAbort {
			return
		}
		err := ledger.Close()
		if err != nil {
			slog.Error("close Cassandra soak failure ledger", "error", err)
		}
		exitCode = soakShutdownExitCode(exitCode, err)
	}()
	observationRuntime := newSoakFailureObservationRuntime(
		cfg.Soak.RecipientObserverEnabled,
		ledger,
		metrics,
		cfg.Soak.RecipientObserverQueue,
		cfg.Soak.LedgerCapacity,
		cfg.Soak.LedgerDir,
		now,
	)
	// Collected at scrape time so a stalled expiry sweep cannot freeze the one
	// number that would reveal the stall.
	metrics.SetRecipientExpectationSource(observationRuntime.Evidence().Len)
	// The workload gets its own cancel so the run can be stopped from inside:
	// a ledger that can no longer record its own verdict has nothing to gain
	// from the hours it would otherwise keep running.
	workloadCtx, stopWorkload := context.WithCancelCause(ctx)
	defer stopWorkload(nil)
	durabilityTicker := time.NewTicker(soakFailureExpiryInterval(cfg.Soak.ReconcileDeadline))
	go func() {
		defer durabilityTicker.Stop()
		watchSoakLedgerDurability(
			workloadCtx, ledger, durabilityTicker.C,
			soakFailureExpiryInterval(cfg.Soak.ReconcileDeadline),
			func(reasons []string) {
				slog.Error(
					"failure ledger cannot record the verdict disqualifying its evidence",
					"reasons", reasons,
					"consequence", "stopping the run rather than accepting messages "+
						"whose evidence a restart would replay as trustworthy",
				)
				stopWorkload(fmt.Errorf(
					"%w: %s", errSoakLedgerNotDurable, strings.Join(reasons, ", ")))
			},
		)
	}()
	expiryCtx, stopExpiry := context.WithCancel(ctx)
	expiryTicker := time.NewTicker(soakFailureExpiryInterval(cfg.Soak.ReconcileDeadline))
	expiryDone := make(chan struct{})
	go func() {
		defer close(expiryDone)
		defer expiryTicker.Stop()
		runSoakFailureExpiry(
			expiryCtx,
			ledger,
			observationRuntime.Evidence(),
			expiryTicker.C,
			func(err error) {
				slog.Error("expire Cassandra soak failure evidence", "error", err)
			},
		)
	}()
	var expiryShutdownOnce sync.Once
	shutdownExpiry := func() {
		expiryShutdownOnce.Do(func() {
			stopExpiry()
			<-expiryDone
		})
	}
	defer func() {
		if !fastLeaseAbort {
			shutdownExpiry()
		}
	}()
	consumerSamplerCtx, stopConsumerSamplers := context.WithCancel(context.Background())
	var consumerSamplerWG sync.WaitGroup
	js, jetStreamErr := jetstream.New(nc.NatsConn())
	if jetStreamErr != nil {
		slog.Error("initialize JetStream consumer sampling", "error", jetStreamErr)
	} else {
		for _, target := range soakConsumerSamplerTargets(cfg.SiteID) {
			sampler, samplerErr := NewConsumerSampler(
				js,
				target.Stream,
				target.Durable,
				metrics,
				time.Second,
			)
			if samplerErr != nil {
				slog.Error(
					"configure soak consumer sampler",
					"stream", target.Stream,
					"durable", target.Durable,
					"error", samplerErr,
				)
				continue
			}
			consumerSamplerWG.Add(1)
			go func() {
				defer consumerSamplerWG.Done()
				sampler.Run(consumerSamplerCtx)
			}()
		}
	}
	var consumerShutdownOnce sync.Once
	shutdownConsumerSamplers := func() {
		consumerShutdownOnce.Do(func() {
			stopConsumerSamplers()
			consumerSamplerWG.Wait()
		})
	}
	defer func() {
		if !fastLeaseAbort {
			shutdownConsumerSamplers()
		}
	}()
	if cfg.Soak.RecipientObserverEnabled {
		recipientObserver := observationRuntime.Recipient()
		recipientSource := newPooledNATSFailureRecipientSource(
			cfg.Soak.RecipientObserverConnections,
			func(_ int) (failureRecipientConnection, error) {
				connection, connectErr := connectWithCredsHealth(
					cfg.NatsURL,
					"loadgen-recipient-observer",
					cfg.NatsCredsFile,
					"recipient_observer",
					metrics,
					recipientObserver.health,
				)
				if connectErr != nil {
					return nil, connectErr
				}
				return newNATSFailureRecipientConnection(connection), nil
			},
		)
		if err := observationRuntime.StartRecipient(recipientSource, &topology, ledger.ActiveOperations()); err != nil {
			ledger.Invalidate("recipient_observer")
			slog.Error("start recipient observer", "error", err)
		}
	}
	defer func() {
		if fastLeaseAbort {
			return
		}
		if err := observationRuntime.Close(); err != nil {
			slog.Error("shutdown recipient observer", "error", err)
		}
	}()
	trackerOptions := []soakFailureTrackerOption{
		withSoakFailureMetrics(metrics),
		withSoakFailureRunID(cfg.Soak.RunID),
	}
	if recipientObserver := observationRuntime.Recipient(); recipientObserver != nil {
		trackerOptions = append(trackerOptions, withSoakFailureRecipientObserver(recipientObserver))
	}
	trackerOptions = append(
		trackerOptions, withSoakFailureSearchObserver(cfg.Soak.SearchObserverEnabled),
	)
	failureTracker := newSoakFailureTracker(
		ledger,
		cfg.Soak.PersistGrace,
		cfg.Soak.ReconcileDeadline,
		now,
		trackerOptions...,
	)
	catalog := catalog.New(
		cfg.Soak.RecentPerRoom,
		cfg.Soak.RecentTotal,
		cfg.Soak.PersistGrace,
		nil,
	)
	catalog.RetainSearchTerms(cfg.Soak.SearchObserverEnabled)
	scheduler := mutation.NewScheduler(
		cfg.Soak.SoftDeleteRatio,
		rand.New(rand.NewSource(seed+3)),
	)
	threadBudgets := distribution.NewThreadBudgetSampler(seed + 7)
	sender := send.New(
		send.Config{
			SiteID: cfg.SiteID, ThreadShare: cfg.Soak.ThreadShare,
			ReplyTimeout:         10 * time.Second,
			NextThreadReplyLimit: threadBudgets.Next,
		},
		catalog,
		newNatsCorePublisher(nc.NatsConn(), InjectFrontdoor, nil),
		nil,
		rand.New(rand.NewSource(seed+4)),
		nil,
		send.WithLifecycle(failureTracker, func(error) {
			// The ledger reports the reason through its own invalidation and
			// untracked counters; logging per send would flood during an outage.
			metrics.FailureUntracked.WithLabelValues(failureUntrackedReasonStart).Inc()
		}),
	)
	sendReplies := make(chan soakSendObservation, cfg.MaxInFlight+1)
	responseSub, err := send.StartResponsesWithObserver(
		send.NewNATSResponseSource(nc.NatsConn()),
		sender,
		func(result send.ReplyResult) {
			action := rpc.ActionSend
			if result.Kind == send.KindThreadReply {
				action = rpc.ActionThreadReply
			}
			outcome := collect.OutcomeFailed
			errorClass := result.ErrorClass
			errorReason := result.ErrorReason
			if result.Status == send.ReplyAccepted {
				outcome = collect.OutcomeSucceeded
				errorClass = ""
				errorReason = ""
				scheduler.ObserveAcceptedSend()
			} else if errorClass == "" {
				errorClass = rpc.ErrorInternal
			}
			recordSoakSample(collector.Record(&collect.Sample{
				Action: action, Outcome: outcome, At: now(),
				Latency: result.Latency, ErrorClass: errorClass,
				ErrorReason: errorReason,
			}))
			if result.Status != send.ReplyUnmatched {
				if err := failureTracker.ObserveReply(&result); err != nil {
					slog.Error("record Cassandra soak send observation", "error", err)
				}
			}
			select {
			case sendReplies <- soakSendObservation{result: result}:
			default:
			}
		},
	)
	if err != nil {
		slog.Error("subscribe Cassandra soak send responses", "error", err)
		return 1
	}
	defer func() {
		if !fastLeaseAbort {
			_ = responseSub.Unsubscribe()
		}
	}()

	if err := runSoakEncryptionPreflight(
		ctx,
		soakEncryptionPreflightConfig{
			Enabled:  cfg.Soak.EncryptionPreflight,
			Timeout:  cfg.Soak.EncryptionPreflightTimeout,
			Verified: metrics.SoakEncryptionPreflight,
		},
		store,
		sender,
		selector,
		sendReplies,
	); err != nil {
		slog.Error("Cassandra soak encryption preflight", "error", err)
		return 1
	}

	rpcClient := rpc.NewClient(
		newNATSHistoryRequester(nc.NatsConn()),
		rpc.RetryConfig{
			MaxAttempts: cfg.Soak.MutationRetries + 1,
			MinBackoff:  cfg.Soak.RetryMinBackoff,
			MaxBackoff:  cfg.Soak.RetryMaxBackoff,
			Jitter:      0.2,
		},
		nil,
		nil,
	)
	failureVerifier := newSoakFailureRPCVerifier(
		cfg.SiteID,
		rpcClient,
		catalog,
		recorders.verify,
		now,
	)
	searchReader, searchReaderErr := search.New(
		search.Config{
			SiteID: cfg.SiteID, PageSize: opts.PageLimit,
			RequestTimeout: soakRequestTimeout, Settle: cfg.Soak.SearchSettle,
		},
		&topology, rpcClient, recorders.read,
		rand.New(rand.NewSource(seed+13)),
		now,
	)
	if searchReaderErr != nil {
		slog.Error("build Cassandra soak search reader", "error", searchReaderErr)
		return 1
	}
	reconcilerOptions := make([]soakFailureReconcilerOption, 0, 3)
	reconcilerOptions = append(reconcilerOptions, withSoakFailureReconcileMetrics(metrics))
	if cfg.Soak.SearchObserverEnabled {
		reconcilerOptions = append(reconcilerOptions, withSoakFailureSearchIndexProbe(
			newSoakSearchIndexProbe(searchReader, catalog),
		))
	}
	if recipientObserver := observationRuntime.Recipient(); recipientObserver != nil {
		reconcilerOptions = append(reconcilerOptions, withSoakFailureRecipientFinalizer(recipientObserver))
	}
	failureReconciler := newSoakFailureReconciler(
		ledger,
		failureVerifier,
		cfg.Soak.ReconcileRetryInterval,
		now,
		reconcilerOptions...,
	)
	reconcileGate := newSoakShareGate(cfg.Soak.ReconcileReadShare)
	warmReader := read.NewReader(
		read.Config{
			SiteID: cfg.SiteID, PageLimit: opts.PageLimit, MaxPages: soakMaxPages(opts.PageLimit),
			RequestTimeout: soakRequestTimeout,
		},
		&topology,
		catalog,
		rpcClient,
		nil,
		rand.New(rand.NewSource(seed+5)),
		now,
	)
	if err := warmSoakPinnedCatalog(
		ctx,
		warmReader,
		selector.rooms,
		cfg.MaxInFlight,
	); err != nil {
		slog.Error("warm Cassandra soak pinned catalog", "error", err)
		return 1
	}
	reader := read.NewReader(
		soakMeasuredReadConfig(cfg.SiteID, opts.PageLimit),
		&topology,
		catalog,
		rpcClient,
		recorders.read,
		rand.New(rand.NewSource(seed+5)),
		now,
	)
	mutator := mutation.New(
		&mutation.Config{
			SiteID: cfg.SiteID, MutationRetries: cfg.Soak.MutationRetries,
			RetryMinBackoff:        cfg.Soak.RetryMinBackoff,
			RetryMaxBackoff:        cfg.Soak.RetryMaxBackoff,
			ReactionsPerHotMessage: cfg.Soak.ReactionsPerHotMessage,
			ReactionRemoveShare:    cfg.Soak.ReactionRemoveShare,
			ReactionMessageScope:   cfg.Soak.ReactionMessageScope,
			RequestTimeout:         soakRequestTimeout,
		},
		&topology,
		catalog,
		rpcClient,
		recorders.mutation,
		rand.New(rand.NewSource(seed+6)),
		nil,
		nil,
	)
	verifier := read.NewVerifier(
		&read.VerifyConfig{
			SiteID: cfg.SiteID, PageLimit: opts.PageLimit, MaxPages: soakMaxPages(opts.PageLimit),
			RequestTimeout: soakRequestTimeout,
		},
		catalog,
		rpcClient,
		recorders.verify,
		now,
	)

	roomPool, err := newSoakRoomStatePool(
		&topology,
		cfg.Soak.MemberQuarantineMax,
		metrics,
		rand.New(rand.NewSource(seed+8)),
	)
	if err != nil {
		slog.Error("prepare soak room state pool", "error", err)
		return 1
	}
	roomReader := newSoakRoomReader(
		soakRoomReadConfigFrom(cfg.SiteID, &cfg.Soak),
		roomPool,
		rpcClient,
		recorders.read,
		rand.New(rand.NewSource(seed+9)),
		now,
	)
	// The read-receipt read needs a real message ID, which only the catalog has.
	roomReader.SetMessageSource(catalog)
	userReader, userReaderErr := soakuserread.New(
		soakuserread.Config{
			SiteID: cfg.SiteID, PageLimit: opts.PageLimit,
			RequestTimeout: soakRequestTimeout,
		},
		&topology, rpcClient, soakUserReadRecorderAdapter{recorder: recorders.read},
		rand.New(rand.NewSource(seed+11)),
		now,
	)
	if userReaderErr != nil {
		slog.Error("build Cassandra soak user reader", "error", userReaderErr)
		return 1
	}
	roomStateHealth := newFailureObserverHealth(failureObserverRoomState, now())
	roomVerifier := newSoakRoomStateVerifier(roomReader, store, cfg.SiteID, metrics, roomStateHealth, now)
	roomLanes := newSoakRoomLanes(
		soakRoomLaneConfig{
			RunID: cfg.Soak.RunID, SiteID: cfg.SiteID,
			PersistGrace: cfg.Soak.PersistGrace,
			Deadline:     cfg.Soak.ReconcileDeadline, RetryInterval: cfg.Soak.ReconcileRetryInterval,
			RoomCreateBudget: cfg.Soak.RoomCreateBudget, CreateRoomSize: cfg.Soak.RoomCreateSize,
		},
		roomPool,
		newSoakRoomMutator(cfg.SiteID, rpcClient, soakRequestTimeout, now),
		ledger,
		roomReader,
		store,
		metrics,
		recorders.mutation,
		now,
	)
	// A replacement process inherits the run's unresolved operations from the
	// WAL. Retake their pool reservations and subtract the rooms the run already
	// created, or the new process races its predecessor's in-flight mutations
	// and restarts the create cap from zero.
	if recovered := ledger.ActiveOperations(); len(recovered) > 0 {
		if inFlight := roomLanes.Rehydrate(recovered); inFlight > 0 {
			// Counted for visibility only, and that is only safe because
			// CountCreatedRooms matches on the run's name prefix alone: a
			// create whose room was made is counted below whether or not
			// ownership was ever recorded, and one whose room was not made
			// occupies no allowance. Narrowing that query to claimed rooms
			// would strand this count and let a crash loop re-spend the cap.
			slog.Info("recovered in-flight soak room creates", "count", inFlight)
		}
	}
	// An unknown count cannot be treated as zero: the lane would restart with the
	// whole allowance and the per-run cap would become per-process, which is the
	// unbounded MongoDB growth the budget exists to prevent.
	created, countErr := store.CountCreatedRooms(ctx, cfg.Soak.RunID)
	if countErr != nil {
		slog.Error("count rooms this soak run already created",
			"runId", cfg.Soak.RunID, "error", countErr)
		return 1
	}
	roomLanes.SpendCreateBudget(created)
	roomReconcileGate := newSoakShareGate(cfg.Soak.RoomReconcileReadShare)

	presenceLane, err := presence.New(
		presence.Config{
			SiteID: cfg.SiteID, Connections: cfg.Soak.PresenceConnections,
			QueryShare: cfg.Soak.PresenceQueryShare, Settle: cfg.Soak.PresenceSettle,
			TTL: cfg.Soak.PresenceTTL, QueryBatchSize: cfg.Soak.PresenceQueryBatch,
			RequestTimeout: soakRequestTimeout,
		},
		&topology,
		presence.NewNATSPublisher(nc.NatsConn()),
		rpcClient,
		&soakPresenceMetricsAdapter{metrics: metrics},
		recorders.read,
		rand.New(rand.NewSource(seed+10)),
		now,
	)
	if err != nil {
		slog.Error("prepare soak presence lane", "error", err)
		return 1
	}

	var verificationSequence atomic.Uint64
	actions := workload.Actions{
		Send: func(actionCtx context.Context, _ bool) error {
			// #nosec G601 -- go.mod requires go 1.25; since 1.22 each iteration has its own loop variable
			// nosemgrep: gosec.G601-1
			for _, result := range sender.ExpireResults() {
				if err := collector.Record(&collect.Sample{
					Action: rpc.ActionSend, Outcome: collect.OutcomeFailed, At: now(),
					ErrorClass: rpc.ErrorTimeout,
				}); err != nil {
					slog.Error("record expired Cassandra soak send timeout", "error", err)
				}
				if err := failureTracker.ObserveReply(&result); err != nil {
					slog.Error("record expired Cassandra soak send", "error", err)
				}
			}
			target, content := selector.nextSend()
			pending, publishErr := sender.Publish(actionCtx, target, content)
			if publishErr == nil {
				return nil
			}
			action := rpc.ActionSend
			if pending != nil && pending.Kind == send.KindThreadReply {
				action = rpc.ActionThreadReply
			}
			recordSoakSample(collector.Record(&collect.Sample{
				Action: action, Outcome: collect.OutcomeFailed, At: now(),
				ErrorClass:  rpc.ClassifyError(publishErr),
				ErrorReason: rpc.ClassifyReason(publishErr),
			}))
			// Publish classifies definite local rejections as not_sent. Ambiguous
			// failures remain active for admission timeout and downstream
			// reconciliation.
			return nil
		},
		Read: func(actionCtx context.Context, _ bool) error {
			// Reconciliation borrows read slots so verification adds no read RPS,
			// but it may only take its configured share: a large unresolved
			// backlog must not stop the production-like read mix mid-fault.
			if reconcileReadAction(actionCtx, failureReconciler, reconcileGate) {
				return nil
			}
			_, _ = reader.ReadMixed(actionCtx, selector.nextRoom())
			return nil
		},
		Mutation: func(actionCtx context.Context, _ bool) error {
			roomID := selector.nextRoom()
			switch scheduler.Next() {
			case mutation.KindDelete:
				_, _ = mutator.Delete(actionCtx, roomID)
			case mutation.KindPinFamily:
				_, _ = mutator.PinOrUnpin(actionCtx, roomID)
			default:
				_, _ = mutator.Edit(actionCtx, roomID, "soak-edited")
			}
			return nil
		},
		Reaction: func(actionCtx context.Context, _ bool) error {
			_, _ = mutator.React(actionCtx, selector.nextRoom())
			return nil
		},
		PinnedList: func(actionCtx context.Context, _ bool) error {
			_, _ = reader.ListPinnedMessages(actionCtx, selector.nextRoom())
			return nil
		},
		Verify: func(actionCtx context.Context, _ bool) error {
			roomID := selector.nextRoom()
			if verificationSequence.Add(1)%2 == 0 {
				candidate, ok := catalog.PickHistoryVerificationCandidate(roomID)
				if ok {
					verifier.VerifyHistory(actionCtx, roomID, candidate.ID)
					return nil
				}
			}
			verifier.Sample(actionCtx, roomID)
			return nil
		},
		MemberMutation: func(actionCtx context.Context, _ bool) error {
			if err := roomLanes.MemberMutation(actionCtx); err != nil {
				slog.Error("run Cassandra soak member mutation", rpc.ErrorAttrs(err)...)
			}
			return nil
		},
		RoomMutation: func(actionCtx context.Context, _ bool) error {
			if err := roomLanes.RoomMutation(actionCtx); err != nil {
				slog.Error("run Cassandra soak room mutation", rpc.ErrorAttrs(err)...)
			}
			return nil
		},
		RoomRead: func(actionCtx context.Context, _ bool) error {
			// Room and member reconciliation borrows read slots so verification
			// adds no unbudgeted request rate, capped by its share so a
			// fault-time backlog cannot starve the room read mix.
			if roomReconcileGate.Allow() {
				reconciled, err := roomLanes.Reconcile(actionCtx, roomVerifier)
				if err != nil {
					slog.Error("reconcile Cassandra soak room operation", rpc.ErrorAttrs(err)...)
				}
				if reconciled {
					return nil
				}
				probed, probeErr := roomLanes.ProbeQuarantine(actionCtx, roomVerifier)
				if probeErr != nil {
					slog.Error("probe quarantined soak member candidate", "error", probeErr)
				}
				if probed {
					return nil
				}
				// The expiry sweep can finalize an operation this lane never
				// claimed, which happens whenever a fault leaves more work than
				// the reconcile budget retires. Reclaim those reservations or
				// the affected rooms stop mutating for the rest of the run.
				if roomLanes.SettleFinalized() > 0 {
					return nil
				}
			}
			if err := roomReader.ReadMixed(actionCtx); err != nil {
				slog.Error("run Cassandra soak room read", rpc.ErrorAttrs(err)...)
			}
			return nil
		},
		UserRead: func(actionCtx context.Context, _ bool) error {
			if err := userReader.ReadMixed(actionCtx); err != nil {
				slog.Error("run Cassandra soak user read", rpc.ErrorAttrs(err)...)
			}
			return nil
		},
		SearchRead: func(actionCtx context.Context, _ bool) error {
			if err := searchReader.ReadMixed(actionCtx); err != nil {
				slog.Error("run Cassandra soak search read", rpc.ErrorAttrs(err)...)
			}
			return nil
		},
		RoomCreate: func(actionCtx context.Context, _ bool) error {
			if err := roomLanes.RoomCreate(actionCtx); err != nil {
				slog.Error("run Cassandra soak room create", rpc.ErrorAttrs(err)...)
			}
			return nil
		},
		ReadReceipt: func(actionCtx context.Context, _ bool) error {
			if err := roomLanes.ReadReceipt(actionCtx); err != nil {
				slog.Error("run Cassandra soak read receipt", rpc.ErrorAttrs(err)...)
			}
			return nil
		},
		Presence: func(actionCtx context.Context, _ bool) error {
			if err := presenceLane.Signal(actionCtx); err != nil {
				slog.Error("run Cassandra soak presence signal", rpc.ErrorAttrs(err)...)
			}
			return nil
		},
	}
	workload := workload.New(
		soakWorkloadConfigFrom(&cfg.Soak, cfg.MaxInFlight),
		run.NewLifecycle(store),
		&actions,
		dispatchSoakLane,
		now,
		nil,
		workload.WithPacingRecorder(newSoakPacingMetrics(metrics)),
		workload.WithHeartbeatObserver(heartbeatStatus),
		withSoakFailureInvalidation(ledger.Invalidate),
	)
	result, runErr := workload.Run(workloadCtx)
	if errors.Is(runErr, errSoakHeartbeatLeaseAtRisk) {
		// Half the margin went to draining the lanes; the rest is all the
		// shutdown may spend, and the NATS drain is the part of it that can
		// still put traffic on the wire.
		leaseShutdownBudget = cfg.Soak.HeartbeatInterval / 2
	}
	if result.LeaseAbort {
		fastLeaseAbort = true
		stopWorkload(runErr)
		slog.Error(
			"bypass graceful soak cleanup after lease abort",
			"consequence", "process must exit before the teardown lease becomes stale",
			"error", runErr,
		)
		return 1
	}
	shutdownExpiry()
	shutdownConsumerSamplers()
	if err := observationRuntime.Close(); err != nil {
		ledger.Invalidate("recipient_observer")
		slog.Error("shutdown recipient observer", "error", err)
	}
	snapshot := collector.Snapshot(now())
	report := BuildSoakReport(&snapshot, soakTargetRates(&cfg.Soak))
	if err := PrintSoakReport(os.Stdout, &report); err != nil {
		slog.Error("print Cassandra soak report", "error", err)
	}
	exit := soakRunExitCode(runErr, context.Cause(workloadCtx))
	switch exit {
	case 2:
		slog.Error(
			"stop Cassandra soak workload early",
			"completion", result.Completion,
			"error", context.Cause(workloadCtx),
		)
	case 1:
		slog.Error(
			"run Cassandra soak workload",
			"completion", result.Completion,
			"error", runErr,
		)
	}
	return exit
}

type soakConsumerSamplerTarget struct {
	Stream  string
	Durable string
}

// soakConsumerSamplerTargets lists every durable the lanes feed. A consumer
// with traffic and no sampler is a blind spot precisely where a fault window
// needs backlog evidence, so the room and member lanes' downstream consumers on
// ROOMS and INBOX belong here alongside the message hops.
func soakConsumerSamplerTargets(siteID string) []soakConsumerSamplerTarget {
	canonicalStream := stream.MessagesCanonical(siteID).Name
	roomsStream := stream.Rooms(siteID).Name
	inboxStream := stream.Inbox(siteID).Name
	return []soakConsumerSamplerTarget{
		{Stream: stream.Messages(siteID).Name, Durable: "message-gatekeeper"},
		{Stream: canonicalStream, Durable: "message-worker"},
		{Stream: canonicalStream, Durable: "broadcast-worker"},
		{Stream: canonicalStream, Durable: "notification-worker"},
		// search-sync-worker indexes messages off the same canonical stream.
		{Stream: canonicalStream, Durable: "message-sync"},
		{Stream: roomsStream, Durable: "room-worker"},
		// notification-worker runs a second consumer that invalidates its cache
		// on mute events, which the room mutation lane drives.
		{Stream: roomsStream, Durable: "notification-worker-room-event-invalidate"},
		// Both search-sync collections index subscription lifecycle events off
		// the INBOX internal lane that the member mutation lane produces.
		{Stream: inboxStream, Durable: "spotlight-sync"},
		{Stream: inboxStream, Durable: "user-room-sync"},
	}
}

func soakMeasuredReadConfig(siteID string, pageLimit int) read.Config {
	// Workload Model v1 defines the read rate in RPCs/second. Scheduled reads
	// therefore fetch one page; the independent verifier owns bucket-walks.
	return read.Config{
		SiteID: siteID, PageLimit: pageLimit, MaxPages: 1,
		RequestTimeout: soakRequestTimeout,
	}
}

func warmSoakPinnedCatalog(
	ctx context.Context,
	reader *read.Reader,
	roomIDs []string,
	maxInFlight int,
) error {
	if reader == nil {
		return fmt.Errorf("soak pinned-catalog reader is required")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(max(1, maxInFlight))
	for _, roomID := range roomIDs {
		roomID := roomID
		group.Go(func() error {
			if _, err := reader.ListPinnedMessages(groupCtx, roomID); err != nil {
				return fmt.Errorf(
					"list pinned messages for room %q: %w",
					roomID,
					err,
				)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return fmt.Errorf("warm pinned catalog: %w", err)
	}
	return nil
}

// soakEncryptionPreflightConfig is the preflight's own gate and budget. The
// timeout is deliberately not derived from SOAK_PERSIST_GRACE: that value also
// sets when every ledger verification begins, so borrowing it to survive a slow
// environment would silently delay the evidence the run exists to collect.
type soakEncryptionPreflightConfig struct {
	Enabled bool
	Timeout time.Duration
	// Verified is set to 1 when the check passed and 0 when it was skipped, so
	// the run's own metrics say whether at-rest encryption was ever proven. A
	// warning at t=0 of a multi-day run is not a durable record.
	Verified prometheus.Gauge
}

func runSoakEncryptionPreflight(
	ctx context.Context,
	cfg soakEncryptionPreflightConfig,
	store soakEncryptionStore,
	sender *send.Sender,
	selector *soakRuntimeSelector,
	replies <-chan soakSendObservation,
) error {
	if cfg.Verified != nil {
		cfg.Verified.Set(0)
	}
	if !cfg.Enabled {
		slog.Warn(
			"Cassandra soak encryption preflight skipped by configuration",
			"encryptionPreflight", false,
			"consequence", "this run does not verify the encrypted write path",
		)
		return nil
	}
	// The budget starts before the publish, not after it: the front-door outage
	// this check exists to notice is exactly the case where the send itself
	// stalls, and a deadline created afterwards would not bound it.
	probeCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	target, content := selector.nextSend()
	pending, err := sender.Publish(probeCtx, target, content)
	if err != nil {
		return fmt.Errorf("publish encrypted front-door probe: %w", err)
	}
	for {
		select {
		case <-probeCtx.Done():
			return fmt.Errorf(
				"wait for encrypted front-door probe response: %w",
				probeCtx.Err(),
			)
		case observation := <-replies:
			if observation.result.RequestID != pending.RequestID {
				continue
			}
			if observation.result.Status != send.ReplyAccepted {
				return fmt.Errorf(
					"encrypted front-door probe was %s",
					observation.result.Status,
				)
			}
			if err := waitForSoakWrappedDEK(
				probeCtx,
				store,
				target.RoomID,
				soakEncryptionPollPeriod,
			); err != nil {
				return err
			}
			if cfg.Verified != nil {
				cfg.Verified.Set(1)
			}
			slog.Info(
				"Cassandra soak encryption preflight passed",
				"roomId", target.RoomID,
				"messageId", pending.MessageID,
			)
			return nil
		}
	}
}

func startSoakMetricsServer(addr string, metrics *Metrics) *http.Server {
	server := &http.Server{
		Addr:              addr,
		Handler:           metrics.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			slog.Error("Cassandra soak metrics server", "error", err)
		}
	}()
	return server
}

func soakTargetRates(cfg *soakConfig) map[rpc.Action]float64 {
	rates := map[rpc.Action]float64{
		rpc.ActionSend:        cfg.SendRate * (1 - cfg.ThreadShare),
		rpc.ActionThreadReply: cfg.SendRate * cfg.ThreadShare,
		rpc.ActionLoadHistory: cfg.ReadRate * 0.75,
		rpc.ActionGetThread:   cfg.ReadRate * 0.15,
		rpc.ActionGetMessage:  cfg.ReadRate * 0.10,
		rpc.ActionReact:       cfg.ReactionRate,
		rpc.ActionPinnedList:  cfg.PinnedListRate,
		rpc.ActionReadBack:    cfg.VerifyRate,
		// The room mutation lane alternates rename and mute, so each shape gets
		// half of the configured rate.
		rpc.ActionMemberAdd:        cfg.MemberMutationRate / 2,
		rpc.ActionMemberRemove:     cfg.MemberMutationRate / 2,
		rpc.ActionRoomRename:       cfg.RoomMutationRate / 2,
		rpc.ActionMuteToggle:       cfg.RoomMutationRate / 2,
		rpc.ActionRoomCreate:       cfg.RoomCreateRate,
		rpc.ActionMemberList:       cfg.RoomReadRate * 0.45,
		rpc.ActionRoomsInfo:        cfg.RoomReadRate * 0.27,
		rpc.ActionSubscriptionList: cfg.RoomReadRate * 0.18,
		rpc.ActionReadReceiptList:  cfg.RoomReadRate * 0.10,
		rpc.ActionMessageRead:      cfg.ReadReceiptRate,
		rpc.ActionPresenceQuery:    cfg.PresenceRate * cfg.PresenceQueryShare,
		rpc.ActionSearchMessages:   cfg.SearchReadRate * 0.7,
		rpc.ActionSearchRooms:      cfg.SearchReadRate * 0.3,
	}
	// The user lane dispatches uniformly across its reads, so each carries an
	// equal share of the configured rate.
	userReadActions := rpc.UserReadActions()
	share := cfg.UserReadRate / float64(len(userReadActions))
	for _, action := range userReadActions {
		rates[action] = share
	}
	return rates
}

var _ soakRuntimeStore = (*run.Mongo)(nil)
