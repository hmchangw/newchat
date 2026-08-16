package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/stream"
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
	LoadTopology(context.Context, string, string) (soakTopology, error)
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
	collector *SoakCollector
	now       func() time.Time
}

func (r *soakReadCollectorRecorder) Record(sample soakReadSample) {
	outcome := soakOutcomeSucceeded
	if sample.Skipped {
		outcome = soakOutcomeSkipped
	} else if sample.ErrorClass != "" {
		outcome = soakOutcomeFailed
	}
	_ = r.collector.Record(&soakOperationSample{
		Action: sample.Action, Outcome: outcome, At: r.now(),
		Latency: sample.Latency, Retries: sample.Retries,
		ErrorClass: sample.ErrorClass,
	})
}

type soakMutationCollectorRecorder struct {
	collector *SoakCollector
	now       func() time.Time
}

func (r *soakMutationCollectorRecorder) Record(sample soakMutationSample) {
	outcome := soakOutcomeSucceeded
	if sample.Skipped {
		outcome = soakOutcomeSkipped
	} else if sample.ErrorClass != "" {
		outcome = soakOutcomeFailed
	}
	_ = r.collector.Record(&soakOperationSample{
		Action: sample.Action, Outcome: outcome, At: r.now(),
		Latency: sample.Latency, Retries: sample.Retries,
		ErrorClass: sample.ErrorClass, TargetMissing: sample.TargetMissing,
	})
}

type soakVerifyCollectorRecorder struct {
	collector *SoakCollector
	now       func() time.Time
}

func (r *soakVerifyCollectorRecorder) Record(result *soakVerifyResult) {
	if result == nil {
		return
	}
	_ = r.collector.RecordVerification(result)
	outcome := soakOutcomeFailed
	switch result.Class {
	case soakVerifyOK:
		outcome = soakOutcomeSucceeded
	case soakVerifySkipped:
		outcome = soakOutcomeSkipped
	case soakVerifyMissing, soakVerifyMismatch, soakVerifyMalformed,
		soakVerifyRetryable, soakVerifyRPCError:
		outcome = soakOutcomeFailed
	}
	_ = r.collector.Record(&soakOperationSample{
		Action: result.Action, Outcome: outcome, At: r.now(),
		Latency: result.Latency, Retries: result.Retries,
		ErrorClass: result.RPCErrorClass,
	})
}

type soakCollectorRecorders struct {
	read     *soakReadCollectorRecorder
	mutation *soakMutationCollectorRecorder
	verify   *soakVerifyCollectorRecorder
}

func newSoakCollectorRecorders(
	collector *SoakCollector,
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
	members map[string][]soakSendTarget
	picker  *soakRoomPicker
	sizer   *soakPayloadSizer
	rng     *rand.Rand
}

func newSoakRuntimeSelector(
	topology *soakTopology,
	cfg *soakConfig,
	seed int64,
) (*soakRuntimeSelector, error) {
	if topology == nil || len(topology.Rooms) == 0 {
		return nil, fmt.Errorf("soak topology has no rooms")
	}
	if cfg == nil {
		return nil, fmt.Errorf("soak configuration is required")
	}
	picker, err := newSoakRoomPicker(seed, len(topology.Rooms))
	if err != nil {
		return nil, fmt.Errorf("build soak room distribution: %w", err)
	}
	sizer, err := newSoakPayloadSizer(
		seed+1,
		cfg.PayloadMedianBytes,
		cfg.PayloadP95Bytes,
		cfg.PayloadMaxBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("build soak payload distribution: %w", err)
	}
	selector := &soakRuntimeSelector{
		rooms: make([]string, len(topology.Rooms)), picker: picker, sizer: sizer,
		rng:     rand.New(rand.NewSource(seed + 2)),
		members: make(map[string][]soakSendTarget, len(topology.Rooms)),
	}
	for i := range topology.Rooms {
		selector.rooms[i] = topology.Rooms[i].ID
	}
	active := activeSoakUserIDs(topology)
	roomTypes := make(map[string]model.RoomType, len(topology.Rooms))
	for i := range topology.Rooms {
		roomTypes[topology.Rooms[i].ID] = topology.Rooms[i].Type
	}
	recipients := soakRecipientSets(topology)
	for i := range topology.Subscriptions {
		subscription := &topology.Subscriptions[i]
		if !isActiveSoakSubscription(subscription, active) ||
			subscription.User.ID == "" ||
			subscription.User.Account == "" {
			continue
		}
		selector.members[subscription.RoomID] = append(
			selector.members[subscription.RoomID],
			soakSendTarget{
				UserID: subscription.User.ID, Account: subscription.User.Account,
				RoomID: subscription.RoomID, RoomType: roomTypes[subscription.RoomID],
				Recipients:           append([]string(nil), recipients[subscription.RoomID]...),
				RecipientSetSource:   recipientSetSourceTopology,
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

func recipientRouteForRoomType(roomType model.RoomType) recipientExpectedRoute {
	if roomType == model.RoomTypeChannel {
		return recipientExpectedRouteRoom
	}
	return recipientExpectedRouteUser
}

func soakRecipientSets(topology *soakTopology) map[string][]string {
	if topology == nil {
		return nil
	}
	recipients := make(map[string][]string, len(topology.Rooms))
	// ActiveUsers bounds sender selection only. Broadcast evidence must retain
	// every subscribed human account, including borrowed non-senders that the
	// production broadcast worker is still required to reach.
	for i := range topology.Subscriptions {
		subscription := &topology.Subscriptions[i]
		if !subscription.IsSubscribed || subscription.User.IsBot || subscription.User.Account == "" {
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

func (s *soakRuntimeSelector) nextSend() (soakSendTarget, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	roomID := s.rooms[s.picker.Next()]
	targets := s.members[roomID]
	target := targets[s.rng.Intn(len(targets))]
	return target, soakContentOfSize(s.sizer.NextContentBytes())
}

type soakSendObservation struct {
	result soakSendReplyResult
}

func runSoakSeed(
	ctx context.Context,
	cfg *config,
	seed int64,
) int {
	db, keyStore, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()
	topology, err := seedSoak(
		ctx,
		&mongoSoakStore{db: db},
		keyStore,
		&soakSeedInput{
			RunID: cfg.Soak.RunID, SiteID: cfg.SiteID,
			MongoDatabase: cfg.MongoDB, CassandraKeyspace: cfg.CassandraKeyspace,
			Seed: seed, Config: &cfg.Soak,
		},
		newProductionSoakIDs(),
	)
	if err != nil {
		slog.Error("seed Cassandra soak topology", "runId", cfg.Soak.RunID, "error", err)
		return 1
	}
	slog.Info(
		"Cassandra soak topology seeded",
		"runId", cfg.Soak.RunID,
		"borrowedUsers", len(topology.BorrowedUsers),
		"activeUsers", len(topology.ActiveUsers),
		"rooms", len(topology.Rooms),
		"subscriptions", len(topology.Subscriptions),
	)
	return 0
}

func runSoakWorkload(
	ctx context.Context,
	cfg *config,
	opts soakOptions,
) int {
	seed := opts.Seed
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
	defer mongoutil.Disconnect(context.Background(), client)
	store := &mongoSoakStore{db: client.Database(cfg.MongoDB)}

	topology, err := store.LoadTopology(ctx, cfg.Soak.RunID, cfg.SiteID)
	if err != nil {
		slog.Error("load Cassandra soak topology", "runId", cfg.Soak.RunID, "error", err)
		return 1
	}
	selector, err := newSoakRuntimeSelector(&topology, &cfg.Soak, seed)
	if err != nil {
		slog.Error("prepare Cassandra soak distributions", "error", err)
		return 1
	}

	metrics := NewMetrics()
	setSoakRunInfo(metrics, cfg.Soak.Environment)
	defer metrics.stopNATSHealth()
	nc, err := dialNATSWithMetrics(cfg.NatsURL, cfg.NatsCredsFile, metrics)
	if err != nil {
		slog.Error("connect NATS for Cassandra soak", "error", err)
		return 1
	}
	defer func() {
		if err := nc.Drain(); err != nil {
			slog.Error("drain Cassandra soak NATS connection", "error", err)
		}
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("stop Cassandra soak metrics server", "error", err)
		}
	}()

	now := time.Now
	collectorDuration := cfg.Soak.RunDuration
	if cfg.Soak.RunMode == soakRunModeContinuous {
		collectorDuration = 0
	}
	collector := NewSoakCollector(
		metrics,
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
	if dropped := ledger.Snapshot().Dropped; dropped > 0 {
		metrics.FailureDropped.Add(float64(dropped))
	}
	defer func() {
		if err := ledger.Close(); err != nil {
			slog.Error("close Cassandra soak failure ledger", "error", err)
		}
	}()
	expiryCtx, stopExpiry := context.WithCancel(ctx)
	expiryTicker := time.NewTicker(soakFailureExpiryInterval(cfg.Soak.ReconcileDeadline))
	expiryDone := make(chan struct{})
	go func() {
		defer close(expiryDone)
		defer expiryTicker.Stop()
		runSoakFailureExpiry(expiryCtx, ledger, expiryTicker.C, func(err error) {
			slog.Error("expire Cassandra soak failure evidence", "error", err)
		})
	}()
	var expiryShutdownOnce sync.Once
	shutdownExpiry := func() {
		expiryShutdownOnce.Do(func() {
			stopExpiry()
			<-expiryDone
		})
	}
	defer shutdownExpiry()
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
	defer shutdownConsumerSamplers()
	observationRuntime := newSoakFailureObservationRuntime(
		cfg.Soak.RecipientObserverEnabled,
		ledger,
		metrics,
		cfg.Soak.RecipientObserverQueue,
		cfg.Soak.LedgerDir,
		now,
	)
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
	catalog := newSoakCatalog(
		cfg.Soak.RecentPerRoom,
		cfg.Soak.RecentTotal,
		cfg.Soak.PersistGrace,
		nil,
	)
	scheduler := newSoakMutationScheduler(
		cfg.Soak.SoftDeleteRatio,
		rand.New(rand.NewSource(seed+3)),
	)
	threadBudgets := newSoakThreadBudgetSampler(seed + 7)
	sender := newSoakSender(
		soakSendConfig{
			SiteID: cfg.SiteID, ThreadShare: cfg.Soak.ThreadShare,
			ReplyTimeout:         10 * time.Second,
			NextThreadReplyLimit: threadBudgets.Next,
		},
		catalog,
		newNatsCorePublisher(nc.NatsConn(), InjectFrontdoor, nil),
		nil,
		rand.New(rand.NewSource(seed+4)),
		nil,
		withSoakSendLifecycle(failureTracker, func(error) {
			// The ledger reports the reason through its own invalidation and
			// untracked counters; logging per send would flood during an outage.
			metrics.FailureUntracked.WithLabelValues(failureUntrackedReasonStart).Inc()
		}),
	)
	sendReplies := make(chan soakSendObservation, cfg.MaxInFlight+1)
	responseSub, err := startSoakSendResponsesWithObserver(
		newNATSSoakResponseSource(nc.NatsConn()),
		sender,
		func(result soakSendReplyResult) {
			action := soakRPCSend
			if result.Kind == soakSendThreadReply {
				action = soakRPCThreadReply
			}
			outcome := soakOutcomeFailed
			errorClass := result.ErrorClass
			if result.Status == soakSendReplyAccepted {
				outcome = soakOutcomeSucceeded
				errorClass = ""
				scheduler.ObserveAcceptedSend()
			} else if errorClass == "" {
				errorClass = soakErrorInternal
			}
			_ = collector.Record(&soakOperationSample{
				Action: action, Outcome: outcome, At: now(),
				Latency: result.Latency, ErrorClass: errorClass,
			})
			if result.Status != soakSendReplyUnmatched {
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
	defer func() { _ = responseSub.Unsubscribe() }()

	if err := runSoakEncryptionPreflight(
		ctx,
		store,
		sender,
		selector,
		sendReplies,
		cfg.Soak.PersistGrace,
	); err != nil {
		slog.Error("Cassandra soak encryption preflight", "error", err)
		return 1
	}

	rpc := newSoakRPCClient(
		newNATSHistoryRequester(nc.NatsConn()),
		soakRetryConfig{
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
		rpc,
		catalog,
		recorders.verify,
		now,
	)
	searchReader, searchReaderErr := newSoakSearchReader(
		soakSearchConfig{
			SiteID: cfg.SiteID, PageSize: opts.PageLimit,
			RequestTimeout: soakRequestTimeout, Settle: cfg.Soak.SearchSettle,
		},
		&topology, rpc, recorders.read,
		rand.New(rand.NewSource(seed+13)),
		now,
	)
	if searchReaderErr != nil {
		slog.Error("build Cassandra soak search reader", "error", searchReaderErr)
		return 1
	}
	reconcilerOptions := make([]soakFailureReconcilerOption, 0, 2)
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
	warmReader := newSoakReader(
		soakReadConfig{
			SiteID: cfg.SiteID, PageLimit: opts.PageLimit, MaxPages: soakMaxPages(opts.PageLimit),
			RequestTimeout: soakRequestTimeout,
		},
		&topology,
		catalog,
		rpc,
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
	reader := newSoakReader(
		soakMeasuredReadConfig(cfg.SiteID, opts.PageLimit),
		&topology,
		catalog,
		rpc,
		recorders.read,
		rand.New(rand.NewSource(seed+5)),
		now,
	)
	mutator := newSoakMutator(
		&soakMutationConfig{
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
		rpc,
		recorders.mutation,
		rand.New(rand.NewSource(seed+6)),
		nil,
		nil,
	)
	verifier := newSoakVerifier(
		&soakVerifyConfig{
			SiteID: cfg.SiteID, PageLimit: opts.PageLimit, MaxPages: soakMaxPages(opts.PageLimit),
			RequestTimeout: soakRequestTimeout,
		},
		catalog,
		rpc,
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
		soakRoomReadConfig{
			SiteID: cfg.SiteID, BatchSize: soakRoomInfoBatchSize,
			RequestTimeout: soakRequestTimeout,
		},
		roomPool,
		rpc,
		recorders.read,
		rand.New(rand.NewSource(seed+9)),
		now,
	)
	// The read-receipt read needs a real message ID, which only the catalog has.
	roomReader.SetMessageSource(catalog)
	userReader, userReaderErr := newSoakUserReader(
		soakUserReadConfig{
			SiteID: cfg.SiteID, PageLimit: opts.PageLimit,
			RequestTimeout: soakRequestTimeout,
		},
		&topology, rpc, recorders.read,
		rand.New(rand.NewSource(seed+11)),
		now,
	)
	if userReaderErr != nil {
		slog.Error("build Cassandra soak user reader", "error", userReaderErr)
		return 1
	}
	roomStateHealth := newFailureObserverHealth(failureObserverRoomState, now())
	roomVerifier := newSoakRoomStateVerifier(roomReader, store, metrics, roomStateHealth, now)
	roomLanes := newSoakRoomLanes(
		soakRoomLaneConfig{
			RunID: cfg.Soak.RunID, PersistGrace: cfg.Soak.PersistGrace,
			Deadline: cfg.Soak.ReconcileDeadline, RetryInterval: cfg.Soak.ReconcileRetryInterval,
			RoomCreateBudget: cfg.Soak.RoomCreateBudget, CreateRoomSize: cfg.Soak.RoomCreateSize,
		},
		roomPool,
		newSoakRoomMutator(cfg.SiteID, rpc, soakRequestTimeout, now),
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
			slog.Info("recovered in-flight soak room creates", "count", inFlight)
			roomLanes.SpendCreateBudget(inFlight)
		}
	}
	if created, countErr := store.CountCreatedRooms(ctx, cfg.Soak.RunID); countErr != nil {
		slog.Error("count rooms this soak run already created", "error", countErr)
	} else {
		roomLanes.SpendCreateBudget(created)
	}
	roomReconcileGate := newSoakShareGate(cfg.Soak.RoomReconcileReadShare)

	presenceLane, err := newSoakPresenceLane(
		soakPresenceConfig{
			SiteID: cfg.SiteID, Connections: cfg.Soak.PresenceConnections,
			QueryShare: cfg.Soak.PresenceQueryShare, Settle: cfg.Soak.PresenceSettle,
			TTL: cfg.Soak.PresenceTTL, QueryBatchSize: cfg.Soak.PresenceQueryBatch,
			RequestTimeout: soakRequestTimeout,
		},
		&topology,
		newNATSSoakPresencePublisher(nc.NatsConn()),
		rpc,
		metrics,
		recorders.read,
		rand.New(rand.NewSource(seed+10)),
		now,
	)
	if err != nil {
		slog.Error("prepare soak presence lane", "error", err)
		return 1
	}

	var verificationSequence atomic.Uint64
	actions := soakWorkloadActions{
		Send: func(actionCtx context.Context, _ bool) error {
			for _, result := range sender.ExpireResults() {
				if err := collector.Record(&soakOperationSample{
					Action: soakRPCSend, Outcome: soakOutcomeFailed, At: now(),
					ErrorClass: soakErrorTimeout,
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
			action := soakRPCSend
			if pending != nil && pending.Kind == soakSendThreadReply {
				action = soakRPCThreadReply
			}
			_ = collector.Record(&soakOperationSample{
				Action: action, Outcome: soakOutcomeFailed, At: now(),
				ErrorClass: classifySoakRPCError(publishErr),
			})
			// Publish classifies definite local rejections as not_sent. Ambiguous
			// failures remain active for admission timeout and downstream
			// reconciliation.
			return nil
		},
		Read: func(actionCtx context.Context, _ bool) error {
			// Reconciliation borrows read slots so verification adds no read RPS,
			// but it may only take its configured share: a large unresolved
			// backlog must not stop the production-like read mix mid-fault.
			if reconcileGate.Allow() {
				reconciled, err := failureReconciler.Try(actionCtx)
				if err != nil {
					slog.Error("reconcile Cassandra soak operation", "error", err)
				}
				if reconciled {
					return nil
				}
			}
			_, _ = reader.ReadMixed(actionCtx, selector.nextRoom())
			return nil
		},
		Mutation: func(actionCtx context.Context, _ bool) error {
			roomID := selector.nextRoom()
			switch scheduler.Next() {
			case soakMutationDelete:
				_, _ = mutator.Delete(actionCtx, roomID)
			case soakMutationPinFamily:
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
				slog.Error("run Cassandra soak member mutation", "error", err)
			}
			return nil
		},
		RoomMutation: func(actionCtx context.Context, _ bool) error {
			if err := roomLanes.RoomMutation(actionCtx); err != nil {
				slog.Error("run Cassandra soak room mutation", "error", err)
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
					slog.Error("reconcile Cassandra soak room operation", "error", err)
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
				slog.Error("run Cassandra soak room read", "error", err)
			}
			return nil
		},
		UserRead: func(actionCtx context.Context, _ bool) error {
			if err := userReader.ReadMixed(actionCtx); err != nil {
				slog.Error("run Cassandra soak user read", "error", err)
			}
			return nil
		},
		SearchRead: func(actionCtx context.Context, _ bool) error {
			if err := searchReader.ReadMixed(actionCtx); err != nil {
				slog.Error("run Cassandra soak search read", "error", err)
			}
			return nil
		},
		RoomCreate: func(actionCtx context.Context, _ bool) error {
			if err := roomLanes.RoomCreate(actionCtx); err != nil {
				slog.Error("run Cassandra soak room create", "error", err)
			}
			return nil
		},
		ReadReceipt: func(actionCtx context.Context, _ bool) error {
			if err := roomLanes.ReadReceipt(actionCtx); err != nil {
				slog.Error("run Cassandra soak read receipt", "error", err)
			}
			return nil
		},
		Presence: func(actionCtx context.Context, _ bool) error {
			if err := presenceLane.Signal(actionCtx); err != nil {
				slog.Error("run Cassandra soak presence signal", "error", err)
			}
			return nil
		},
	}
	workload := newSoakWorkload(
		soakWorkloadConfigFrom(&cfg.Soak, cfg.MaxInFlight),
		store,
		&actions,
		nil,
		now,
		nil,
		withSoakPacingMetrics(newSoakPacingMetrics(metrics)),
	)
	result, runErr := workload.Run(ctx)
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
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		slog.Error(
			"run Cassandra soak workload",
			"completion", result.Completion,
			"error", runErr,
		)
		return 1
	}
	return 0
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

func soakMeasuredReadConfig(siteID string, pageLimit int) soakReadConfig {
	// Workload Model v1 defines the read rate in RPCs/second. Scheduled reads
	// therefore fetch one page; the independent verifier owns bucket-walks.
	return soakReadConfig{
		SiteID: siteID, PageLimit: pageLimit, MaxPages: 1,
		RequestTimeout: soakRequestTimeout,
	}
}

func warmSoakPinnedCatalog(
	ctx context.Context,
	reader *soakReader,
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

func runSoakEncryptionPreflight(
	ctx context.Context,
	store soakEncryptionStore,
	sender *soakSender,
	selector *soakRuntimeSelector,
	replies <-chan soakSendObservation,
	persistGrace time.Duration,
) error {
	target, content := selector.nextSend()
	pending, err := sender.Publish(ctx, target, content)
	if err != nil {
		return fmt.Errorf("publish encrypted front-door probe: %w", err)
	}
	timeout := max(30*time.Second, persistGrace+10*time.Second)
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
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
			if observation.result.Status != soakSendReplyAccepted {
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

func soakTargetRates(cfg *soakConfig) map[soakRPCAction]float64 {
	rates := map[soakRPCAction]float64{
		soakRPCSend:        cfg.SendRate * (1 - cfg.ThreadShare),
		soakRPCThreadReply: cfg.SendRate * cfg.ThreadShare,
		soakRPCLoadHistory: cfg.ReadRate * 0.75,
		soakRPCGetThread:   cfg.ReadRate * 0.15,
		soakRPCGetMessage:  cfg.ReadRate * 0.10,
		soakRPCReact:       cfg.ReactionRate,
		soakRPCPinnedList:  cfg.PinnedListRate,
		soakRPCReadBack:    cfg.VerifyRate,
		// The room mutation lane alternates rename and mute, so each shape gets
		// half of the configured rate.
		soakRPCMemberAdd:        cfg.MemberMutationRate / 2,
		soakRPCMemberRemove:     cfg.MemberMutationRate / 2,
		soakRPCRoomRename:       cfg.RoomMutationRate / 2,
		soakRPCMuteToggle:       cfg.RoomMutationRate / 2,
		soakRPCRoomCreate:       cfg.RoomCreateRate,
		soakRPCMemberList:       cfg.RoomReadRate * 0.45,
		soakRPCRoomsInfo:        cfg.RoomReadRate * 0.27,
		soakRPCSubscriptionList: cfg.RoomReadRate * 0.18,
		soakRPCReadReceiptList:  cfg.RoomReadRate * 0.10,
		soakRPCMessageRead:      cfg.ReadReceiptRate,
		soakRPCPresenceQuery:    cfg.PresenceRate * cfg.PresenceQueryShare,
		soakRPCSearchMessages:   cfg.SearchReadRate * 0.7,
		soakRPCSearchRooms:      cfg.SearchReadRate * 0.3,
	}
	// The user lane dispatches uniformly across its reads, so each carries an
	// equal share of the configured rate.
	share := cfg.UserReadRate / float64(len(soakUserReadActions))
	for _, action := range soakUserReadActions {
		rates[action] = share
	}
	return rates
}

var _ soakRuntimeStore = (*mongoSoakStore)(nil)
