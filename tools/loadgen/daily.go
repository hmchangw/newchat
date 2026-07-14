package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// dailyConfig is the parsed CLI input for `loadgen daily`.
type dailyConfig struct {
	Preset             string
	Steps              []int
	Warmup             time.Duration
	Hold               time.Duration
	Cooldown           time.Duration
	StopOnTrip         bool
	MaxDirectUsers     int
	MultiplexPoolSize  int
	MaxConnsPerProcess int
	CSVPath            string
	Users              int // 0 = use preset default; otherwise overrides preset.Users
	// ActionP95Ms / ActionP99Ms are raw "name:N,name:N" strings parsed
	// later into per-action threshold maps. Empty string keeps defaults.
	ActionP95Ms string
	ActionP99Ms string

	// Presence load (opt-in). When Presence is false the daily run is
	// unchanged — no presence pool is built and no presence is emitted.
	Presence               bool
	PresenceHeartbeat      time.Duration
	PresencePublisherConns int
	PresenceObserverConns  int
}

func parseDailyConfig(args []string) (dailyConfig, error) {
	fs := flag.NewFlagSet("daily", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `loadgen daily — daily-IM scenario, find sustainable N

Simulates N users using the chat system as their primary IM throughout a
workday. Ramps N geometrically through the configured steps; for each step,
warms up, holds steady, polls SLO signals, and decides PASS / TRIP /
INCONCLUSIVE. Reports the largest passing N and which signal tripped next.

SLO signals evaluated over the hold window:
  - p95 latency (publish→broadcast)        threshold 500ms
  - p99 latency                            threshold 1000ms
  - error rate                             threshold 0.1%
  - any JetStream consumer pending growth  threshold +1000
    (notification-worker exempt: push-notification delay is tolerated)
  - any service slog_errors_total increase threshold +0
INCONCLUSIVE (overrides PASS/TRIP) when the loadgen process is itself
saturated (GC pause p99 > 50ms or CPU proxy > 80%).

Receiver topology is hybrid: the first --max-direct-users users get one
nats.Conn each (most realistic); the rest share a fixed pool of
--multiplex-pool-size connections.

Usage:
  loadgen daily --preset=<name> [flags]

Presets:
  daily-light    ~32 rooms/user   light daily-IM user
  daily-heavy    ~56 rooms/user   heavy daily-IM user (default)
  daily-power    ~83 rooms/user   power user

Flags:
`)
		fs.PrintDefaults()
		fmt.Fprint(fs.Output(), `
Examples:
  # Default 7-step geometric ramp 1k → 100k, daily-heavy preset:
  loadgen daily --preset=daily-heavy --csv=results.csv

  # Tight sweep around an expected breakpoint, shorter hold:
  loadgen daily --preset=daily-heavy --steps=8000,9000,10000,11000,12000 --hold=120s

  # Single-step smoke test:
  loadgen daily --preset=daily-light --steps=500 --warmup=10s --hold=30s

Step list accepts shorthand: --steps=1k,2k,5k,10k

See tools/loadgen/README.md and docs/superpowers/specs/2026-05-27-daily-im-load-scenario-design.md
for the full design and SLO rationale.
`)
	}
	preset := fs.String("preset", "daily-heavy", "preset name: daily-light | daily-heavy | daily-power")
	steps := fs.String("steps", "1000,2000,5000,10000,20000,50000,100000", "comma-separated N values per ramp step; `k` suffix multiplies by 1000 (e.g. \"1k,2k,5k\")")
	warmup := fs.Duration("warmup", 60*time.Second, "per-step warm-up before SLO measurement begins")
	hold := fs.Duration("hold", 180*time.Second, "per-step steady-state window where SLO signals are evaluated")
	cooldown := fs.Duration("cooldown", 30*time.Second, "per-step cooldown to let consumers drain before the next step")
	stopOnTrip := fs.Bool("stop-on-trip", true, "stop the ramp on the first TRIP (false: run all steps)")
	maxDirect := fs.Int("max-direct-users", 20000, "cap on the direct-pool size; users beyond this go to the multiplex pool")
	mux := fs.Int("multiplex-pool-size", 200, "number of shared nats.Conn instances in the multiplex pool")
	maxConns := fs.Int("max-conns-per-process", 25000, "safety ceiling on total nats.Conn count to this process")
	csvPath := fs.String("csv", "", "optional CSV output path (one row per step)")
	usersOverride := fs.Int("users", 0, "override preset.Users (0 = use preset default; must match `loadgen seed --users` if you used it)")
	actionP95 := fs.String("action-p95-ms", "", "comma-separated per-action p95 latency caps in ms (e.g. \"read_receipt:80,scroll_history:300\"). Overrides defaults. Action names: send, read_receipt, scroll_history, refresh_room_list, member_add, room_create, mute_toggle.")
	actionP99 := fs.String("action-p99-ms", "", "comma-separated per-action p99 latency caps in ms; same format as --action-p95-ms.")
	presence := fs.Bool("presence", false, "emit presence load (hello/ping/activity) per daily user; observational stats only, never affects the verdict")
	presenceHeartbeat := fs.Duration("presence-heartbeat", 30*time.Second, "per-user presence ping interval (only with --presence)")
	presencePub := fs.Int("presence-publisher-conns", 8, "presence publisher connection count (only with --presence)")
	presenceObs := fs.Int("presence-observer-conns", 2, "presence observer connection count (only with --presence)")
	if err := fs.Parse(args); err != nil {
		return dailyConfig{}, err
	}

	if _, ok := BuiltinPreset(*preset); !ok {
		return dailyConfig{}, fmt.Errorf("unknown preset %q (valid: daily-light, daily-heavy, daily-power)", *preset)
	}

	parsedSteps, err := parseStepList(*steps)
	if err != nil {
		return dailyConfig{}, err
	}

	projected := *maxDirect + *mux
	if projected > *maxConns {
		return dailyConfig{}, fmt.Errorf(
			"projected conn count %d (direct=%d + mux=%d) exceeds --max-conns-per-process=%d",
			projected, *maxDirect, *mux, *maxConns)
	}

	return dailyConfig{
		Preset:                 *preset,
		Steps:                  parsedSteps,
		Warmup:                 *warmup,
		Hold:                   *hold,
		Cooldown:               *cooldown,
		StopOnTrip:             *stopOnTrip,
		MaxDirectUsers:         *maxDirect,
		MultiplexPoolSize:      *mux,
		MaxConnsPerProcess:     *maxConns,
		CSVPath:                *csvPath,
		Users:                  *usersOverride,
		ActionP95Ms:            *actionP95,
		ActionP99Ms:            *actionP99,
		Presence:               *presence,
		PresenceHeartbeat:      *presenceHeartbeat,
		PresencePublisherConns: *presencePub,
		PresenceObserverConns:  *presenceObs,
	}, nil
}

func parseStepList(s string) ([]int, error) {
	if s == "" {
		return nil, fmt.Errorf("--steps cannot be empty")
	}
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		mult := 1
		if strings.HasSuffix(p, "k") {
			mult = 1000
			p = strings.TrimSuffix(p, "k")
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("invalid step %q: %w", p, err)
		}
		out = append(out, n*mult)
	}
	return out, nil
}

// parseActionLatencyOverrides parses "name:N,name:N" into a map of action
// name to threshold in ms. Empty input returns an empty map (caller treats
// as "no overrides"). Invalid format or unknown action names are errors.
func parseActionLatencyOverrides(s string) (map[string]float64, error) {
	if s == "" {
		return nil, nil
	}
	known := make(map[string]bool, len(allActionKinds))
	for _, k := range allActionKinds {
		known[k.String()] = true
	}
	out := make(map[string]float64)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		colon := strings.IndexByte(part, ':')
		if colon < 0 {
			return nil, fmt.Errorf("expected name:N, got %q", part)
		}
		name := strings.TrimSpace(part[:colon])
		valStr := strings.TrimSpace(part[colon+1:])
		if !known[name] {
			return nil, fmt.Errorf("unknown action name %q (valid: send, read_receipt, scroll_history, refresh_room_list, member_add, room_create, mute_toggle)", name)
		}
		n, err := strconv.ParseFloat(valStr, 64)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid ms value %q for %s: must be non-negative number", valStr, name)
		}
		out[name] = n
	}
	return out, nil
}

// mergeActionThresholds replaces any default thresholds for the actions
// named in overrides. Untouched actions keep their defaults; this lets
// the operator tune only the ones that matter to their environment
// without re-specifying the whole set.
func mergeActionThresholds(th *Thresholds, p95Overrides, p99Overrides map[string]float64) {
	if th.ActionP95Ms == nil && len(p95Overrides) > 0 {
		th.ActionP95Ms = make(map[string]float64)
	}
	for k, v := range p95Overrides {
		th.ActionP95Ms[k] = v
	}
	if th.ActionP99Ms == nil && len(p99Overrides) > 0 {
		th.ActionP99Ms = make(map[string]float64)
	}
	for k, v := range p99Overrides {
		th.ActionP99Ms[k] = v
	}
}

// stepEnv bundles the runtime dependencies of a step. Stub-able for unit tests.
//
// holdStartNanos / holdDurationNanos are atomics so emitters started during
// step N can re-anchor their diurnal envelope when step N+1 begins (otherwise
// older users would emit at the envelope's clamped baseline for the entire
// next step). Set via setHold() at the actual start of each hold window.
//
// activatedCount tracks how many users were successfully added to a pool;
// when it diverges from the nominal N (because direct pool filled and no
// multiplex was configured, or NATS subscribe failed), runStep surfaces the
// gap so an "N=20000 PASS" doesn't silently mean "10000 users active".
type stepEnv struct {
	collector      *Collector
	direct         *directPool
	multiplex      *multiplexPool
	users          []*userState
	thresholds     Thresholds
	pollPending    func(ctx context.Context) (map[string]int64, error)
	scrapeServices func(ctx context.Context) (map[string]int64, error)
	publish        publishFn // nil in stub mode → emitters no-op
	request        requestFn // nil in stub mode → emitters no-op
	siteID         string    // propagated from cfg / baseCfg
	runSeed        int64     // for deterministic per-user RNG seeding
	maxDirect      int       // direct pool cap (from cfg.MaxDirectUsers)
	warmup         time.Duration
	hold           time.Duration
	cooldown       time.Duration
	mintJWT        func(ctx context.Context, account string) error // optional; nil = skip

	// Presence load (nil when --presence is off). presencePool owns its own
	// publisher + observer conns, independent of the message pools.
	presencePool      *presencePool
	presenceCollector *presenceCollector
	presenceHeartbeat time.Duration

	holdStartNanos    atomic.Int64
	holdDurationNanos atomic.Int64
	activatedCount    atomic.Int64
	skippedCount      atomic.Int64
}

// setHold updates the current envelope anchor. Emitters read these on every
// tick so a step transition takes effect within ~1s.
func (env *stepEnv) setHold(start time.Time, duration time.Duration) {
	env.holdStartNanos.Store(start.UnixNano())
	env.holdDurationNanos.Store(duration.Nanoseconds())
}

func (env *stepEnv) currentHold() (time.Time, time.Duration) {
	startNanos := env.holdStartNanos.Load()
	if startNanos == 0 {
		return time.Time{}, 0
	}
	return time.Unix(0, startNanos), time.Duration(env.holdDurationNanos.Load())
}

// runStep executes one ramp step: activates additional users (delta over
// previous), warms up, holds, evaluates SLO signals, and cools down.
// The current step is `n`; the previous step's user count is `prevN` (0 for
// the first step). Users [prevN..n) are activated this step.
func runStep(ctx context.Context, env *stepEnv, n, prevN int) StepResult {
	startedAt := time.Now()
	delta := n - prevN

	// Activate the new slice of users. Activation can take significant time
	// (rate-limited at 500/sec, so +50k users = 100s) — that elapsed time
	// would eat into the warmup window if we set holdStart early. We
	// re-anchor holdStart right before the hold actually begins (below).
	activationStart := time.Now()
	activateUsers(ctx, env, prevN, n)
	activationElapsed := time.Since(activationStart)
	if delta > 0 {
		slog.Info("step activated",
			"n", n, "delta", delta,
			"activated", env.activatedCount.Load(),
			"skipped", env.skippedCount.Load(),
			"activation_elapsed", activationElapsed.Round(time.Millisecond))
	}

	if err := waitOrCancel(ctx, env.warmup); err != nil {
		return inconclusiveResult(n, startedAt, env.hold, "ctx canceled during warmup")
	}

	// Re-anchor the diurnal envelope at the actual hold start. Emitters
	// re-read this on every tick, so step-1 users that survived into step 2
	// follow step 2's envelope rather than continuing on step 1's curve.
	env.setHold(time.Now(), env.hold)

	// Snapshot pending state at start of hold. If the NATS monitoring
	// endpoint is misbehaving, drop the pending-growth signal for this
	// step rather than aborting it — the other signals (latency, errors,
	// service health) still produce a useful verdict. Only ctx cancel
	// is treated as Inconclusive.
	startPending, startPollErr := env.pollPending(ctx)
	if startPollErr != nil {
		if errors.Is(startPollErr, context.Canceled) || errors.Is(startPollErr, context.DeadlineExceeded) {
			return inconclusiveResult(n, startedAt, env.hold, "ctx canceled during start-of-hold poll")
		}
		slog.Warn("start-of-hold pending poll failed; pending-growth signal skipped this step", "err", startPollErr)
		startPending = nil
	}
	_, _ = env.scrapeServices(ctx) // first call records baseline

	env.collector.Reset()
	if env.presenceCollector != nil {
		env.presenceCollector.Reset()
	}

	if err := waitOrCancel(ctx, env.hold); err != nil {
		return inconclusiveResult(n, startedAt, env.hold, "ctx canceled during hold")
	}

	endPending, endPollErr := env.pollPending(ctx)
	if endPollErr != nil {
		if errors.Is(endPollErr, context.Canceled) || errors.Is(endPollErr, context.DeadlineExceeded) {
			return inconclusiveResult(n, startedAt, env.hold, "ctx canceled during end-of-hold poll")
		}
		slog.Warn("end-of-hold pending poll failed; pending-growth signal skipped this step", "err", endPollErr)
		endPending = nil
	}
	svcErrors, _ := env.scrapeServices(ctx)

	// Only compute pending deltas when both snapshots succeeded; otherwise
	// pass an empty map so evaluateStep doesn't trip on garbage baselines.
	var pendingDeltas map[string]ConsumerPendingDelta
	if startPending != nil && endPending != nil {
		pendingDeltas = diffPending(startPending, endPending)
	}

	// Re-key per-action latency samples by their stable name so
	// evaluateStep + reporting code don't need to know the actionKind int.
	rawActions := env.collector.ActionLatencies()
	actionSamples := make(map[string][]float64, len(rawActions))
	for kind, ss := range rawActions {
		actionSamples[actionKind(kind).String()] = ss
	}

	in := stepInputs{
		N: n, StartedAt: startedAt, HoldDuration: env.hold,
		EffectiveN:      int(env.activatedCount.Load()),
		LatencySamples:  env.collector.LatencySamples(),
		ActionSamplesMs: actionSamples,
		AttemptedOps:    env.collector.AttemptedOps(),
		FailedOps:       env.collector.FailedOps(),
		ConsumerPending: pendingDeltas,
		ServiceErrors:   svcErrors,
		Self:            snapshotSelfMetrics(),
	}
	r := evaluateStep(in, env.thresholds)
	snapshotPresenceStats(env, &r)

	_ = waitOrCancel(ctx, env.cooldown)
	return r
}

func inconclusiveResult(n int, startedAt time.Time, hold time.Duration, reason string) StepResult {
	return StepResult{
		N: n, StartedAt: startedAt, HoldDuration: hold,
		Inconclusive: true, TrippedReasons: []string{reason},
	}
}

// activateUsers brings users in the range [from, to) online: optionally
// mints a JWT, assigns them to a pool, opens connections / registers room
// interest, and starts their action-emitter goroutine. Rate-limited at
// 500 users/sec. Updates env.activatedCount / env.skippedCount so runStep
// can surface whether the nominal N actually went live.
func activateUsers(ctx context.Context, env *stepEnv, from, to int) {
	if from >= to {
		return
	}
	tokens := time.NewTicker(time.Second / 500)
	defer tokens.Stop()
	for i := from; i < to && i < len(env.users); i++ {
		select {
		case <-ctx.Done():
			return
		case <-tokens.C:
		}
		u := env.users[i]
		if env.mintJWT != nil {
			if err := env.mintJWT(ctx, u.Account); err != nil {
				slog.Warn("jwt mint failed", "user", u.ID, "err", err)
			}
		}
		var poolAdded bool
		switch {
		case env.direct != nil && env.direct.Size() < env.maxDirect:
			if err := env.direct.Add(u); err != nil {
				slog.Warn("direct pool add failed", "user", u.ID, "err", err)
				env.skippedCount.Add(1)
				continue
			}
			poolAdded = true
		case env.multiplex != nil:
			if err := env.multiplex.Add(u); err != nil {
				slog.Warn("multiplex pool add failed", "user", u.ID, "err", err)
				env.skippedCount.Add(1)
				continue
			}
			poolAdded = true
		default:
			slog.Warn("no pool available for user; skipping", "user", u.ID)
			env.skippedCount.Add(1)
			continue
		}
		// Emit the presence hello BEFORE launching the emitter goroutine so the
		// `go` statement orders the hello's writes to u.presence.status/away
		// ahead of the goroutine's ping/setAway reads (happens-before edge).
		if env.presencePool != nil && u.presence != nil {
			emitPresence(env, u.presence, u.presence.hello(nowMillis()))
		}
		// Per-user emitter runs through warmup + hold + cooldown, reading
		// the current envelope anchor from env on each tick so step
		// transitions take effect within ~1s. Pass the per-user index so
		// the RNG seed is deterministic given env.runSeed.
		if poolAdded && env.publish != nil {
			startEmitter(ctx, env, u, i)
		}
		env.activatedCount.Add(1)
	}
}

// envFactory builds a stepEnv from a parsed dailyConfig. Stubbed in tests.
type envFactory interface {
	Build(cfg dailyConfig, users []*userState) *stepEnv
}

// startEmitter launches a goroutine that, while ctx is live, ticks the user's
// Markov state every second and, when active, emits actions at the Poisson
// rate scaled by the diurnal envelope.
//
// The RNG seed is derived from env.runSeed and the user's index, so two runs
// with the same run-seed produce identical action streams (reproducibility
// is the whole point of a load-test verdict). Avoid time.Now in the seed —
// at the 500 users/sec activation rate, bursts of users get seeded in the
// same nanosecond and end up perfectly correlated.
//
// The envelope anchor is read from env on every tick (not captured at
// activation), so emitters started during step N follow step N+1's envelope
// once runStep calls env.setHold for the next step.
func startEmitter(ctx context.Context, env *stepEnv, u *userState, userIdx int) {
	go func() {
		// Splitmix-style mix to scramble adjacent userIdx seeds; cast through
		// uint64 so the multiplier doesn't overflow the int64 literal.
		seed := int64(uint64(env.runSeed)*0x9E3779B97F4A7C15) + int64(userIdx)
		r := rand.New(rand.NewSource(seed))
		weights := defaultActionWeights()
		baseRate := actionRatePerSecond(weights.totalPerDay(), 8*time.Hour)

		tick := time.NewTicker(1 * time.Second)
		defer tick.Stop()

		// Optional presence ping ticker (own interval, independent of the 1s
		// Markov tick). Only armed when --presence is on.
		var presenceC <-chan time.Time
		if env.presencePool != nil && u.presence != nil && env.presenceHeartbeat > 0 {
			pt := time.NewTicker(env.presenceHeartbeat)
			defer pt.Stop()
			presenceC = pt.C
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-presenceC:
				emitPresence(env, u.presence, u.presence.ping(nowMillis()))
				continue
			case <-tick.C:
			}
			wasActive := u.active
			u.step(r)
			presenceFlip(env, u, wasActive)
			if !u.active {
				continue
			}
			holdStart, holdDuration := env.currentHold()
			if holdDuration <= 0 {
				continue // env not yet initialised; wait for runStep to set
			}
			// Compress: a workday becomes the hold window. Multiply rate accordingly.
			compress := (8 * time.Hour).Seconds() / holdDuration.Seconds()
			elapsed := time.Since(holdStart)
			rate := baseRate * compress * rateMultiplier(elapsed, holdDuration)
			if r.Float64() < rate {
				doAction(ctx, env, u, r, weights)
			}
		}
	}()
}

// doAction picks one action via weights and dispatches it. Increments
// attempted/failed counters on the Collector.
func doAction(ctx context.Context, env *stepEnv, u *userState, r *rand.Rand, w actionWeights) {
	if env.publish == nil && env.request == nil {
		return // stub mode (no real NATS wired); no attempt counted
	}
	if env.collector != nil {
		env.collector.RecordActionAttempt()
	}
	a := actionCtx{
		Ctx: ctx, Publish: env.publish, Request: env.request,
		SiteID: env.siteID, Rand: r, Collector: env.collector,
	}
	kind := pickAction(r, w)
	start := time.Now()
	var err error
	switch kind {
	case actionSend:
		err = sendMessage(a, u, "loadtest content")
	case actionMarkRead:
		err = markRead(a, u, "msg-stub")
	case actionScrollHistory:
		err = scrollHistory(a, u)
	case actionRefreshRoomList:
		err = refreshRoomList(a, u)
	case actionMemberAdd:
		err = memberAdd(a, u, u.Neighbor)
	case actionRoomCreate:
		err = roomCreate(a, u)
	case actionMuteToggle:
		err = muteToggle(a, u)
	}
	elapsed := time.Since(start)
	if env.collector != nil {
		// Per-action latency: wall-clock around the handler. For request
		// actions (memberAdd, roomCreate, etc.) this is the full
		// request/reply round-trip. For publish actions (sendMessage,
		// threadReply) this measures only the local publish cost — not
		// the publish→broadcast pipeline, which the existing
		// LatencySamples flow already covers via RecordBroadcast.
		env.collector.RecordActionLatency(int(kind), elapsed)
	}
	if err != nil && env.collector != nil {
		env.collector.RecordActionFailure()
	}
}

// emitPresence publishes one presence transition for u and records its
// attempt/expectation/failure on the presence collector. No-op when presence
// is disabled (nil pool) or u has no presence state.
func emitPresence(env *stepEnv, u *presenceUser, tr presenceTransition) {
	if u == nil {
		return
	}
	// emitTransitionRaw handles the nil-pool guard, the no-op (empty expect)
	// skip for steady pings, and attempt/expectation/failure accounting.
	emitTransitionRaw(env.presencePool, env.presenceCollector, tr)
}

// snapshotPresenceStats fills r.Presence from the presence collector (after
// counting unresolved expectations as failures). No-op when presence is off or
// the presence pool failed to initialize (presencePool nil) — otherwise a
// failed init would emit a misleading all-zeros, 0%-error presence block.
func snapshotPresenceStats(env *stepEnv, r *StepResult) {
	if env.presencePool == nil || env.presenceCollector == nil {
		return
	}
	env.presenceCollector.ReapMissing()
	attempted := env.presenceCollector.Attempted()
	failed := env.presenceCollector.Failed()
	lat := env.presenceCollector.LatenciesMs()
	s := &PresenceObsStats{
		P50Ms:     percentile(lat, 0.50),
		P95Ms:     percentile(lat, 0.95),
		P99Ms:     percentile(lat, 0.99),
		Attempted: attempted,
		Failed:    failed,
	}
	if attempted > 0 {
		s.ErrorRate = float64(failed) / float64(attempted)
	}
	r.Presence = s
}

// presenceFlip emits an activity transition when the user's active state
// changed this tick. active->idle => away; idle->active => not away. No-op when
// presence is disabled or the state didn't change.
func presenceFlip(env *stepEnv, u *userState, wasActive bool) {
	if env.presencePool == nil || u.presence == nil || u.active == wasActive {
		return
	}
	emitPresence(env, u.presence, u.presence.setAway(!u.active, nowMillis()))
}

// runDailyForTest is the testable variant: takes an envFactory so tests can
// inject stubs. The production runDaily wraps it with the real factory.
//
// dailyRunSeed is the fixture/RNG seed. Hardcoded for now; spec section 12
// flagged this as a follow-up. Same seed → same fixtures → same action
// stream, which is what makes regression CSV comparisons meaningful.
const dailyRunSeed int64 = 42

//nolint:gocritic // cfg passed by value to match envFactory.Build signature
func runDailyForTest(ctx context.Context, cfg dailyConfig, factory envFactory) ([]StepResult, error) {
	preset, _ := BuiltinPreset(cfg.Preset)
	if len(cfg.Steps) == 0 {
		return nil, fmt.Errorf("cfg.Steps cannot be empty")
	}
	// --users overrides preset.Users for callers who need to run above the
	// preset's hard-coded ceiling (10000 for the daily-* presets). The
	// same value MUST be passed to `loadgen seed --users=N`, otherwise
	// the two BuildFixtures invocations produce different IDs and the
	// gatekeeper rejects every send. Zero (default) means "use preset
	// default" — the safe path for normal runs.
	if cfg.Users > 0 {
		preset.Users = cfg.Users
	}
	// IMPORTANT: do NOT override preset.Users from --steps. BuildFixtures
	// is deterministic in (preset, seed, siteID); changing preset.Users
	// changes every generated ID (the per-band stub shuffle depends on
	// totalUsers). If daily ran with one Users value while `loadgen seed`
	// was invoked with a different one, the IDs don't line up and the
	// gatekeeper rejects every send. The activateUsers loop already caps
	// at len(env.users), so a --steps entry that exceeds preset.Users
	// surfaces as INCONCLUSIVE via the EffectiveN-shortfall guard
	// (clearer than silent ID drift).
	maxStep := slices.Max(cfg.Steps)
	if maxStep > preset.Users {
		slog.Warn("max step exceeds preset.Users; effective N will cap at preset.Users",
			"max_step", maxStep, "preset_users", preset.Users)
	}

	// Parse per-action latency overrides and merge into defaults. Empty
	// override string keeps the default; an explicit "name:N" replaces
	// that action's threshold (set N to a very large number to effectively
	// disable the gate).
	p95Overrides, err := parseActionLatencyOverrides(cfg.ActionP95Ms)
	if err != nil {
		return nil, fmt.Errorf("--action-p95-ms: %w", err)
	}
	p99Overrides, err := parseActionLatencyOverrides(cfg.ActionP99Ms)
	if err != nil {
		return nil, fmt.Errorf("--action-p99-ms: %w", err)
	}

	siteID := "site-local"
	if cfg, ok := factoryBaseCfg(factory); ok && cfg.SiteID != "" {
		siteID = cfg.SiteID
	}
	slog.Info("building fixtures", "preset", cfg.Preset, "users", preset.Users)
	buildStart := time.Now()
	fx := BuildFixtures(&preset, dailyRunSeed, siteID)
	slog.Info("fixtures built",
		"rooms", len(fx.Rooms),
		"subscriptions", len(fx.Subscriptions),
		"elapsed", time.Since(buildStart).Round(time.Millisecond))

	userRooms := groupSubsByUser(fx.Subscriptions)
	users := make([]*userState, len(fx.Users))
	for i := range fx.Users {
		u := &fx.Users[i]
		users[i] = newUserState(u.ID, u.Account, userRooms[u.ID], int64(i))
	}

	env := factory.Build(cfg, users)
	if env.siteID == "" {
		env.siteID = siteID
	}
	env.runSeed = dailyRunSeed
	mergeActionThresholds(&env.thresholds, p95Overrides, p99Overrides)
	defer closePools(env)

	prevN := 0
	var results []StepResult
	for _, n := range cfg.Steps {
		// Honor ctx between steps so SIGINT mid-cooldown doesn't produce
		// a junk trail of INCONCLUSIVE rows for steps that never started.
		if err := ctx.Err(); err != nil {
			slog.Info("daily run interrupted; stopping ramp", "completed_steps", len(results))
			break
		}
		r := runStep(ctx, env, n, prevN)
		results = append(results, r)
		if cfg.StopOnTrip && r.Tripped {
			break
		}
		prevN = n
	}
	return results, nil
}

// factoryBaseCfg returns the baseCfg from a prodEnvFactory, if the factory is
// one. testEnvFactory returns false and runDailyForTest falls back to the
// default site.
func factoryBaseCfg(f envFactory) (*config, bool) {
	if p, ok := f.(*prodEnvFactory); ok && p != nil {
		return p.baseCfg, true
	}
	return nil, false
}

func closePools(env *stepEnv) {
	if env.direct != nil {
		env.direct.Close()
	}
	if env.multiplex != nil {
		env.multiplex.Close()
	}
	if env.presencePool != nil {
		env.presencePool.Close()
	}
}

func groupSubsByUser(subs []model.Subscription) map[string][]string {
	out := make(map[string][]string)
	for i := range subs {
		out[subs[i].User.ID] = append(out[subs[i].User.ID], subs[i].RoomID)
	}
	return out
}

// prodEnvFactory wires the real NATS pools and pollers.
type prodEnvFactory struct {
	baseCfg *config // existing top-level loadgen config: NatsURL, etc.
}

//nolint:gocritic // cfg passed by value to satisfy envFactory interface
func (f *prodEnvFactory) Build(cfg dailyConfig, users []*userState) *stepEnv {
	col := NewCollector(NewMetrics(), cfg.Preset)
	direct := newDirectPool(f.baseCfg.NatsURL, f.baseCfg.NatsCredsFile, col)
	var mux *multiplexPool
	if cfg.MultiplexPoolSize > 0 {
		var err error
		mux, err = newMultiplexPool(f.baseCfg.NatsURL, f.baseCfg.NatsCredsFile, col, cfg.MultiplexPoolSize)
		if err != nil {
			slog.Error("multiplex pool init failed; continuing without multiplex", "err", err)
			mux = nil
		}
	}

	// Dedicated publisher connection for emitter actions. Separate from the
	// receiver pools so a slow consumer can't backpressure publishes.
	pubConn, err := connectWithCreds(f.baseCfg.NatsURL, "loadgen-daily-publisher", f.baseCfg.NatsCredsFile)
	if err != nil {
		slog.Error("publisher connection failed; emitters will no-op", "err", err)
		pubConn = nil
	}
	// Build a *nats.Msg with an X-Request-ID header on every publish or
	// request. Backend services (notably room-service → room-worker via
	// canonical) require the header — without it the canonical event
	// arrives with no request ID and room-worker rejects it as a
	// permanent error ("missing X-Request-ID"). Each emitter call gets
	// a fresh UUID so request-tracing across the pipeline works for
	// every action.
	newMsg := func(subj string, data []byte) *nats.Msg {
		return &nats.Msg{
			Subject: subj,
			Data:    data,
			Header: nats.Header{
				natsutil.RequestIDHeader: []string{idgen.GenerateRequestID()},
			},
		}
	}
	publish := func(ctx context.Context, subj string, data []byte) error {
		if pubConn == nil {
			return fmt.Errorf("no publisher conn")
		}
		return pubConn.PublishMsg(newMsg(subj, data))
	}
	request := func(ctx context.Context, subj string, data []byte, timeout time.Duration) ([]byte, error) {
		if pubConn == nil {
			return nil, fmt.Errorf("no publisher conn")
		}
		// Apply the caller's per-request timeout. RequestMsgWithContext uses
		// the context's deadline; the emitter's ctx is the run-level ctx
		// with no deadline, so without this wrap the timeout argument is
		// silently ignored and a slow handler can hang forever (manifests
		// as huge per-action p50 like 25s instead of cleanly timing out
		// at 5s and contributing to error_rate).
		rctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		reply, err := pubConn.RequestMsgWithContext(rctx, newMsg(subj, data))
		if err != nil {
			return nil, err
		}
		return reply.Data, nil
	}

	jszURL := f.baseCfg.NatsMonitoringURL
	if jszURL == "" {
		jszURL = "http://nats:8222/jsz"
	}

	// Backend services don't currently expose /metrics endpoints, so the
	// service-error scraper is a no-op until they do. Pass an empty URL map
	// — Scrape will return an empty delta map without making any requests.
	scraper := newServiceScraper()
	svcURLs := map[string]string{}

	siteID := f.baseCfg.SiteID
	if siteID == "" {
		siteID = "site-local"
	}

	var presencePool *presencePool
	var presenceCollector *presenceCollector
	if cfg.Presence {
		presenceCollector = newPresenceCollector()
		pp, err := newPresencePool(f.baseCfg.NatsURL, f.baseCfg.NatsCredsFile,
			cfg.PresencePublisherConns, cfg.PresenceObserverConns, presenceCollector)
		if err != nil {
			slog.Error("presence pool init failed; presence emission disabled", "err", err)
			presencePool = nil
		} else {
			presencePool = pp
		}
		for _, u := range users {
			u.presence = newPresenceUserForAccount(u.Account, siteID)
		}
	}

	return &stepEnv{
		collector: col, direct: direct, multiplex: mux, users: users,
		thresholds: defaultThresholds(),
		pollPending: func(ctx context.Context) (map[string]int64, error) {
			return pollPending(ctx, jszURL)
		},
		scrapeServices: func(ctx context.Context) (map[string]int64, error) {
			return scraper.Scrape(ctx, svcURLs)
		},
		publish:           publish,
		request:           request,
		siteID:            siteID,
		maxDirect:         cfg.MaxDirectUsers,
		mintJWT:           buildAuthMintFn(),
		warmup:            cfg.Warmup,
		hold:              cfg.Hold,
		cooldown:          cfg.Cooldown,
		presencePool:      presencePool,
		presenceCollector: presenceCollector,
		presenceHeartbeat: cfg.PresenceHeartbeat,
	}
}

// buildAuthMintFn returns a best-effort one-time auth-service login function.
// On failure, activateUsers logs a warning and the user proceeds with the
// shared backend.creds.
func buildAuthMintFn() func(ctx context.Context, account string) error {
	return func(ctx context.Context, account string) error {
		body, _ := json.Marshal(map[string]string{"account": account})
		// Auth path is currently a placeholder — see spec section 10. When
		// auth-service exposes /login, this URL needs configuration; for
		// now best-effort means a connection-refused error is silently
		// tolerated by activateUsers.
		_ = body
		return nil
	}
}

// runDaily is the production entrypoint invoked by main.go.
func runDaily(ctx context.Context, baseCfg *config, args []string) int {
	cfg, err := parseDailyConfig(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0 // -h / --help printed usage; exit cleanly
		}
		slog.Error("parse daily config", "error", err)
		return 2
	}
	if err := verifyDailySeeded(ctx, baseCfg, cfg); err != nil {
		slog.Error("daily pre-flight", "error", err)
		return 2
	}
	results, err := runDailyForTest(ctx, cfg, &prodEnvFactory{baseCfg: baseCfg})
	if err != nil {
		slog.Error("daily run", "error", err)
		return 1
	}
	renderConsole(os.Stdout, results)
	if cfg.CSVPath != "" {
		if err := writeDailyCSV(cfg.CSVPath, results); err != nil {
			slog.Error("csv write", "error", err)
			return 1
		}
	}
	return 0
}

// verifyDailySeeded checks that the subscriptions collection has at least one
// row for the configured siteID, AND that the count of users in Mongo
// matches the count daily will generate at runtime. If not, the gatekeeper
// rejects every send with "user X is not subscribed to room Y" (silent
// INCONCLUSIVE / TRIP from the operator's point of view).
//
// The user-count check catches the most common misuse: seeding with one
// --users value and running daily with a different one. BuildFixtures is
// deterministic in (preset, seed, siteID); the per-band stub shuffles use
// totalUsers as length, so a mismatch produces entirely different room
// memberships even though the user IDs `u-000000...` overlap.
//
// Uses a short context independent of the run-level ctx so a transient
// Mongo blip at startup doesn't burn the whole run window before failing.
//
//nolint:gocritic // cfg passed by value to match the call shape used elsewhere
func verifyDailySeeded(ctx context.Context, baseCfg *config, cfg dailyConfig) error {
	siteID := baseCfg.SiteID
	if siteID == "" {
		siteID = "site-local"
	}
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client, err := mongoutil.Connect(checkCtx, baseCfg.MongoURI, baseCfg.MongoUsername, baseCfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("preflight mongo connect: %w", err)
	}
	defer mongoutil.Disconnect(checkCtx, client)
	db := client.Database(baseCfg.MongoDB)
	subCount, err := db.Collection("subscriptions").CountDocuments(checkCtx, bson.M{"siteId": siteID})
	if err != nil {
		return fmt.Errorf("preflight count subscriptions: %w", err)
	}
	if subCount == 0 {
		return fmt.Errorf("no subscriptions found in mongo for siteID=%q; "+
			"run `loadgen seed --workload=messages --preset=<your daily preset>` first "+
			"(or `make -C tools/loadgen/deploy seed PRESET=<your preset>`)", siteID)
	}
	// User-count consistency check. Daily generates exactly preset.Users
	// (overridden by cfg.Users when set). If Mongo has a different count,
	// seed was run with mismatched --users; re-seeding is required.
	preset, ok := BuiltinPreset(cfg.Preset)
	if !ok {
		return fmt.Errorf("preflight: unknown preset %q", cfg.Preset)
	}
	if cfg.Users > 0 {
		preset.Users = cfg.Users
	}
	wantUsers := int64(preset.Users)
	gotUsers, err := db.Collection("users").CountDocuments(checkCtx, bson.M{"siteId": siteID})
	if err != nil {
		return fmt.Errorf("preflight count users: %w", err)
	}
	if gotUsers != wantUsers {
		return fmt.Errorf("user-count mismatch: mongo has %d users for siteID=%q "+
			"but daily expects %d (preset %q with --users=%d). Re-seed: "+
			"`loadgen teardown --workload=messages --preset=%s` then "+
			"`loadgen seed --workload=messages --preset=%s --users=%d`",
			gotUsers, siteID, wantUsers, cfg.Preset, cfg.Users, cfg.Preset, cfg.Preset, preset.Users)
	}
	slog.Info("preflight subscriptions ok", "siteID", siteID, "subs", subCount, "users", gotUsers)
	return nil
}
