package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"
)

type capacityConfig struct {
	Steps            []int
	Warmup           time.Duration
	Hold             time.Duration
	Cooldown         time.Duration
	Heartbeat        time.Duration
	ConnectP95Ms     float64
	ConnectP99Ms     float64
	FalseOfflineRate float64
	ErrorRate        float64
	PingTolerance    float64
	StopOnTrip       bool
	PublisherConns   int
	ObserverConns    int
	CSVPath          string
}

func parseCapacityConfig(args []string) (capacityConfig, error) {
	fs := flag.NewFlagSet("presence-capacity", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `loadgen presence-capacity — find the max concurrent online population N

Cumulatively ramps a synthetic population through --steps. Each step activates
the delta of new users (each sends hello, measuring connect-edge latency),
holds with every user online and heartbeating, and counts false offlines
(users the service wrongly swept offline) plus ping sustainability. Reports the
largest N held without tripping.

Flags:
`)
		fs.PrintDefaults()
	}
	steps := fs.String("steps", "10000,20000,50000,100000,200000", "comma-separated cumulative N per step; k suffix x1000")
	warmup := fs.Duration("warmup", 30*time.Second, "post-activation settle before snapshot")
	hold := fs.Duration("hold", 120*time.Second, "steady-state false-offline window")
	cooldown := fs.Duration("cooldown", 15*time.Second, "per-step cooldown before next step")
	heartbeat := fs.Duration("heartbeat", 30*time.Second, "per-user ping interval (matches PRESENCE_HEARTBEAT_INTERVAL)")
	connectP95 := fs.Float64("connect-p95-ms", 500, "connect-edge p95 latency cap (ms)")
	connectP99 := fs.Float64("connect-p99-ms", 1000, "connect-edge p99 latency cap (ms)")
	falseOff := fs.Float64("false-offline-rate", 0.001, "false-offline fraction cap (TRIP)")
	errRate := fs.Float64("error-rate", 0.01, "connect error-rate cap (fraction)")
	pingTol := fs.Float64("ping-tolerance", 0.10, "ping-sustainability shortfall band (INCONCLUSIVE)")
	stop := fs.Bool("stop-on-trip", true, "stop the ramp on the first TRIP")
	pub := fs.Int("publisher-conns", 16, "shared publisher connection count")
	obs := fs.Int("observer-conns", 4, "observer connection count (subscribe presence.state.*)")
	csv := fs.String("csv", "", "optional CSV output path")
	if err := fs.Parse(args); err != nil {
		return capacityConfig{}, err
	}
	parsedSteps, err := parseStepList(*steps)
	if err != nil {
		return capacityConfig{}, err
	}
	return capacityConfig{
		Steps: parsedSteps, Warmup: *warmup, Hold: *hold, Cooldown: *cooldown,
		Heartbeat: *heartbeat, ConnectP95Ms: *connectP95, ConnectP99Ms: *connectP99,
		FalseOfflineRate: *falseOff, ErrorRate: *errRate, PingTolerance: *pingTol,
		StopOnTrip: *stop, PublisherConns: *pub, ObserverConns: *obs, CSVPath: *csv,
	}, nil
}

// capacityEnv bundles a capacity run's deps. onActivated / afterReset are test
// seams (nil in prod -> real wiring used).
type capacityEnv struct {
	pool       *presencePool
	collector  *presenceCollector
	users      []*presenceUser
	thresholds capacityThresholds
	warmup     time.Duration
	hold       time.Duration
	cooldown   time.Duration
	heartbeat  time.Duration

	onActivated func(env *capacityEnv, idx int)
	afterReset  func(env *capacityEnv)

	holdDurationNanos atomic.Int64
	activated         atomic.Int64
	pingsSent         atomic.Int64
}

func (env *capacityEnv) holding() bool { return env.holdDurationNanos.Load() > 0 }

type capacityFactory interface {
	Build(cfg capacityConfig) *capacityEnv
}

// runStepCapacity activates the delta of new users (measuring connect-edge
// latency), warms up, snapshots connect stats, then holds while watching for
// false offlines, and grades the step.
func runStepCapacity(ctx context.Context, env *capacityEnv, n, prevN int) capacityStepResult {
	activateCapacityUsers(ctx, env, prevN, n)
	_ = waitOrCancel(ctx, env.warmup)

	// Connect-edge stats: the hello->online round-trips captured during
	// activation+warmup. ReapMissing counts hellos that never went online.
	connectLat := env.collector.LatenciesMs()
	env.collector.ReapMissing()
	connectAttempted := env.collector.Attempted()
	connectFailed := env.collector.Failed()

	startedAt := time.Now()
	env.collector.Reset()
	cohort := make([]string, 0, n)
	for i := 0; i < n && i < len(env.users); i++ {
		cohort = append(cohort, env.users[i].account)
	}
	env.collector.WatchOnline(cohort)
	env.pingsSent.Store(0)
	env.holdDurationNanos.Store(env.hold.Nanoseconds())
	if env.afterReset != nil {
		env.afterReset(env)
	}
	_ = waitOrCancel(ctx, env.hold)
	env.collector.StopWatchOnline()
	env.holdDurationNanos.Store(0)

	var pingsRequired int64
	if env.heartbeat > 0 {
		pingsRequired = int64(n) * int64(env.hold/env.heartbeat)
	}

	in := capacityStepInputs{
		N: n, EffectiveN: int(env.activated.Load()),
		StartedAt: startedAt, HoldDuration: env.hold,
		ConnectLatencyMs: connectLat,
		ConnectAttempted: connectAttempted,
		ConnectFailed:    connectFailed,
		FalseOfflines:    env.collector.FalseOfflines(),
		PingsSent:        env.pingsSent.Load(),
		PingsRequired:    pingsRequired,
		Self:             snapshotSelfMetrics(),
	}
	r := evaluateCapacityStep(in, env.thresholds)
	_ = waitOrCancel(ctx, env.cooldown)
	return r
}

// activateCapacityUsers brings users [from,to) online (hello) at a bounded rate
// and starts their steady-ping goroutine.
func activateCapacityUsers(ctx context.Context, env *capacityEnv, from, to int) {
	if to > len(env.users) {
		to = len(env.users)
	}
	tokens := time.NewTicker(time.Second / 1000) // 1000 activations/sec
	defer tokens.Stop()
	for i := from; i < to; i++ {
		select {
		case <-ctx.Done():
			return
		case <-tokens.C:
		}
		if env.onActivated != nil {
			env.onActivated(env, i) // test seam
		} else {
			startCapacityEmitter(ctx, env, env.users[i])
		}
		env.activated.Add(1)
	}
}

// startCapacityEmitter sends one user's hello (measured connect edge) and runs
// its steady-ping loop. No churn: no activity flips, no bye.
func startCapacityEmitter(ctx context.Context, env *capacityEnv, u *presenceUser) {
	emitCapacityHello(env, u)
	go func() {
		tick := time.NewTicker(env.heartbeat)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			// A ping has empty expect, so emitTransitionRaw publishes it
			// without recording a measurement.
			emitTransitionRaw(env.pool, env.collector, u.ping(nowMillis()))
			if env.holding() {
				env.pingsSent.Add(1)
			}
		}
	}()
}

// emitCapacityHello publishes a hello and registers its online expectation.
func emitCapacityHello(env *capacityEnv, u *presenceUser) {
	emitTransitionRaw(env.pool, env.collector, u.hello(nowMillis()))
}

//nolint:gocritic // cfg passed by value to satisfy capacityFactory interface
func runPresenceCapacityForTest(ctx context.Context, cfg capacityConfig, f capacityFactory) ([]capacityStepResult, error) {
	if len(cfg.Steps) == 0 {
		return nil, fmt.Errorf("cfg.Steps cannot be empty")
	}
	env := f.Build(cfg)
	if env.pool != nil {
		defer env.pool.Close()
	}
	prevN := 0
	var results []capacityStepResult
	for _, n := range cfg.Steps {
		if err := ctx.Err(); err != nil {
			slog.Info("presence-capacity interrupted", "completed_steps", len(results))
			break
		}
		r := runStepCapacity(ctx, env, n, prevN)
		results = append(results, r)
		if cfg.StopOnTrip && r.Kind == verdictTrip {
			break
		}
		prevN = n
	}
	return results, nil
}

// prodCapacityFactory wires the real pool.
type prodCapacityFactory struct{ baseCfg *config }

//nolint:gocritic // cfg by value to match interface
func (f *prodCapacityFactory) Build(cfg capacityConfig) *capacityEnv {
	c := newPresenceCollector()
	siteID := f.baseCfg.SiteID
	if siteID == "" {
		siteID = "site-local"
	}
	pool, err := newPresencePool(f.baseCfg.NatsURL, f.baseCfg.NatsCredsFile, cfg.PublisherConns, cfg.ObserverConns, c)
	if err != nil {
		slog.Error("presence pool init failed; emitters will no-op", "err", err)
	}
	users := make([]*presenceUser, slicesMaxInt(cfg.Steps))
	for i := range users {
		users[i] = newPresenceUser(i, siteID)
	}
	return &capacityEnv{
		pool: pool, collector: c, users: users,
		thresholds: capacityThresholds{
			ConnectP95Ms: cfg.ConnectP95Ms, ConnectP99Ms: cfg.ConnectP99Ms,
			FalseOfflineRate: cfg.FalseOfflineRate, ErrorRate: cfg.ErrorRate,
			PingTolerance: cfg.PingTolerance, GCPauseInconclusive: 50,
		},
		warmup: cfg.Warmup, hold: cfg.Hold, cooldown: cfg.Cooldown, heartbeat: cfg.Heartbeat,
	}
}

// runPresenceCapacity is the production entrypoint invoked by main.go.
func runPresenceCapacity(ctx context.Context, baseCfg *config, args []string) int {
	cfg, err := parseCapacityConfig(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		slog.Error("parse presence-capacity config", "error", err)
		return 2
	}
	results, err := runPresenceCapacityForTest(ctx, cfg, &prodCapacityFactory{baseCfg: baseCfg})
	if err != nil {
		slog.Error("presence-capacity run", "error", err)
		return 1
	}
	renderCapacityConsole(os.Stdout, results)
	if cfg.CSVPath != "" {
		if err := writeCapacityCSV(cfg.CSVPath, results); err != nil {
			slog.Error("write capacity csv", "error", err)
			return 1
		}
	}
	return presenceCapacityExitCode(results)
}

// presenceCapacityExitCode returns 0 if any step passed, else 1.
func presenceCapacityExitCode(results []capacityStepResult) int {
	for i := range results {
		if results[i].Kind == verdictPass {
			return 0
		}
	}
	return 1
}
