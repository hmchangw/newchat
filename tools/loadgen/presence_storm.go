package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type stormConfig struct {
	Users          int
	StormSteps     []float64
	Mode           string // "graceful" | "silent"
	Warmup         time.Duration
	Settle         time.Duration // cooldown between storm steps
	RecoverySLO    time.Duration
	Heartbeat      time.Duration
	SilentWait     time.Duration // wait for sweeper offline in silent mode
	ReconnectRate  int           // hellos/sec during the herd; 0 = unbounded burst
	StopOnTrip     bool
	PublisherConns int
	ObserverConns  int
	CSVPath        string
	P99Ms          float64
	ErrorRate      float64
}

func parseFractionList(s string) ([]float64, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("--storm-steps cannot be empty")
	}
	var out []float64
	for _, p := range strings.Split(s, ",") {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid fraction %q: %w", p, err)
		}
		if f <= 0 || f > 1 {
			return nil, fmt.Errorf("fraction %v out of range (0,1]", f)
		}
		out = append(out, f)
	}
	return out, nil
}

func stormUserCount(fraction float64, n int) int {
	c := int(math.Floor(fraction * float64(n)))
	if c < 1 && fraction > 0 {
		c = 1
	}
	if c > n {
		c = n
	}
	return c
}

func parseStormConfig(args []string) (stormConfig, error) {
	fs := flag.NewFlagSet("presence-storm", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `loadgen presence-storm — find the largest survivable reconnect storm

Warms --users online, then for each fraction in --storm-steps drops that
fraction and thundering-herd reconnects them, measuring recovery time, spike
latency, and error rate. Reports the largest fraction that recovered within
--recovery-slo.

Flags:
`)
		fs.PrintDefaults()
	}
	users := fs.Int("users", 10000, "fixed warmed population N")
	steps := fs.String("storm-steps", "0.10,0.25,0.50,1.0", "comma-separated storm fractions in (0,1]")
	mode := fs.String("storm-mode", "graceful", "graceful (bye+hello) | silent (stop ping, sweeper offline, hello)")
	warmup := fs.Duration("warmup", 30*time.Second, "warm-up before the first storm")
	settle := fs.Duration("settle", 15*time.Second, "cooldown between storm steps")
	slo := fs.Duration("recovery-slo", 10*time.Second, "max recovery time to PASS a storm")
	heartbeat := fs.Duration("heartbeat", 30*time.Second, "warmed-user ping interval")
	silentWait := fs.Duration("silent-wait", 50*time.Second, "silent mode: wait for sweeper offline (> stale threshold)")
	reconnect := fs.Int("reconnect-rate", 0, "hellos/sec during the herd (0 = unbounded burst)")
	stop := fs.Bool("stop-on-trip", true, "stop ramping fractions on the first TRIP")
	pub := fs.Int("publisher-conns", 16, "shared publisher connection count")
	obs := fs.Int("observer-conns", 4, "observer connection count")
	csv := fs.String("csv", "", "optional CSV output path")
	p99 := fs.Float64("p99-ms", 1000, "spike p99 latency cap (ms)")
	errRate := fs.Float64("error-rate", 0.05, "spike error-rate cap (fraction)")
	if err := fs.Parse(args); err != nil {
		return stormConfig{}, err
	}
	fractions, err := parseFractionList(*steps)
	if err != nil {
		return stormConfig{}, err
	}
	if *mode != "graceful" && *mode != "silent" {
		return stormConfig{}, fmt.Errorf("invalid --storm-mode %q (want graceful|silent)", *mode)
	}
	return stormConfig{
		Users: *users, StormSteps: fractions, Mode: *mode,
		Warmup: *warmup, Settle: *settle, RecoverySLO: *slo, Heartbeat: *heartbeat,
		SilentWait: *silentWait, ReconnectRate: *reconnect, StopOnTrip: *stop,
		PublisherConns: *pub, ObserverConns: *obs, CSVPath: *csv,
		P99Ms: *p99, ErrorRate: *errRate,
	}, nil
}

// stormConn is one warmed user whose steady-ping goroutine can be paused
// (silent drop) and resumed.
type stormConn struct {
	user   *presenceUser
	paused atomic.Bool
}

// stormEnv bundles a storm run's deps. The runStorm seam lets unit tests drive
// the ramp loop without a broker.
type stormEnv struct {
	pool       *presencePool
	collector  *presenceCollector
	conns      []*stormConn
	thresholds stormThresholds
	cfg        stormConfig
	siteID     string

	// runStorm executes one fraction step and returns its inputs. Prod uses
	// executeStorm; tests inject a stub.
	runStorm func(ctx context.Context, env *stormEnv, fraction float64) stormStepInputs
}

type stormFactory interface {
	Build(cfg stormConfig) *stormEnv
}

//nolint:gocritic // cfg by value to satisfy interface
func runPresenceStormForTest(ctx context.Context, cfg stormConfig, f stormFactory) ([]stormStepResult, error) {
	if len(cfg.StormSteps) == 0 {
		return nil, fmt.Errorf("cfg.StormSteps cannot be empty")
	}
	env := f.Build(cfg)
	if env.pool != nil {
		defer env.pool.Close()
	}
	return runStormSteps(ctx, cfg, env), nil
}

// runStormSteps drives the fraction ramp against an already-built env.
//
//nolint:gocritic // hugeParam: cfg passed by value to match the pattern of runPresenceSustainedForTest
func runStormSteps(ctx context.Context, cfg stormConfig, env *stormEnv) []stormStepResult {
	var results []stormStepResult
	for _, frac := range cfg.StormSteps {
		if err := ctx.Err(); err != nil {
			break
		}
		in := env.runStorm(ctx, env, frac)
		in.Self = snapshotSelfMetrics()
		r := evaluateStormStep(in, env.thresholds)
		results = append(results, r)
		if cfg.StopOnTrip && r.Kind == verdictTrip {
			break
		}
		_ = waitOrCancel(ctx, cfg.Settle)
	}
	return results
}

// executeStorm is the real one-fraction storm: drop, herd, measure recovery.
func executeStorm(ctx context.Context, env *stormEnv, fraction float64) stormStepInputs {
	count := stormUserCount(fraction, len(env.conns))
	victims := env.conns[:count]
	env.collector.Reset()

	switch env.cfg.Mode {
	case "silent":
		for _, sc := range victims {
			sc.paused.Store(true)
		}
		_ = waitOrCancel(ctx, env.cfg.SilentWait) // sweeper marks them offline
	default: // graceful
		for _, sc := range victims {
			emitTransitionRaw(env.pool, env.collector, sc.user.bye(nowMillis()))
		}
	}

	accounts := make([]string, count)
	for i, sc := range victims {
		accounts[i] = sc.user.account
	}
	start := time.Now()
	env.collector.BeginRecovery(accounts, start)
	herdHello(ctx, env, victims)

	wait := time.NewTicker(50 * time.Millisecond)
	defer wait.Stop()
	deadline := time.After(env.cfg.RecoverySLO)
waitLoop:
	for !env.collector.RecoveryComplete() {
		select {
		case <-ctx.Done():
			break waitLoop
		case <-deadline:
			break waitLoop
		case <-wait.C:
		}
	}

	for _, sc := range victims {
		sc.paused.Store(false) // resume steady ping
	}
	return stormStepInputs{
		Fraction: fraction, StormUsers: count,
		RecoveryComplete:  env.collector.RecoveryComplete(),
		RecoveryElapsed:   env.collector.RecoveryElapsed(),
		RecoveryRemaining: env.collector.RecoveryRemaining(),
		SpikeLatencyMs:    env.collector.LatenciesMs(),
		Attempted:         env.collector.Attempted(),
		Failed:            env.collector.Failed(),
	}
}

// herdHello re-hellos every victim, optionally paced at cfg.ReconnectRate/sec.
func herdHello(ctx context.Context, env *stormEnv, victims []*stormConn) {
	if env.cfg.ReconnectRate <= 0 {
		var wg sync.WaitGroup
		for _, sc := range victims {
			wg.Add(1)
			go func(sc *stormConn) {
				defer wg.Done()
				emitTransitionRaw(env.pool, env.collector, sc.user.hello(nowMillis()))
			}(sc)
		}
		wg.Wait()
		return
	}
	tick := time.NewTicker(time.Second / time.Duration(env.cfg.ReconnectRate))
	defer tick.Stop()
	for _, sc := range victims {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		emitTransitionRaw(env.pool, env.collector, sc.user.hello(nowMillis()))
	}
}

// emitTransitionRaw is the pool-backed emit used by storm (no test seam).
func emitTransitionRaw(pool *presencePool, c *presenceCollector, tr presenceTransition) {
	if pool == nil {
		return // publisher down; surfaces via zero attempts / incomplete recovery
	}
	sentAt := time.Now()
	err := pool.Publish(tr.subject, tr.payload)
	if tr.expect == "" {
		return
	}
	if err != nil {
		c.RecordEmit()
		c.RecordEmitFailure()
		return
	}
	c.Expect(accountFromSubject(tr.subject), tr.expect, sentAt)
}

// prodStormFactory wires the real pool and builds the population.
type prodStormFactory struct{ baseCfg *config }

//nolint:gocritic // cfg by value to satisfy interface
func (f *prodStormFactory) Build(cfg stormConfig) *stormEnv {
	c := newPresenceCollector()
	siteID := f.baseCfg.SiteID
	if siteID == "" {
		siteID = "site-local"
	}
	pool, err := newPresencePool(f.baseCfg.NatsURL, f.baseCfg.NatsCredsFile, cfg.PublisherConns, cfg.ObserverConns, c)
	if err != nil {
		slog.Error("presence pool init failed", "err", err)
	}
	conns := make([]*stormConn, cfg.Users)
	for i := range conns {
		conns[i] = &stormConn{user: newPresenceUser(i, siteID)}
	}
	return &stormEnv{
		pool: pool, collector: c, conns: conns,
		thresholds: stormThresholds{RecoverySLO: cfg.RecoverySLO, P99Ms: cfg.P99Ms, ErrorRate: cfg.ErrorRate, GCPauseInconclusive: 50},
		cfg:        cfg, siteID: siteID, runStorm: executeStorm,
	}
}

// warmStormPopulation hello's every conn and starts its pausable steady-ping
// goroutine. Pings reuse presenceUser.ping() (a no-op publish at the service).
func warmStormPopulation(ctx context.Context, env *stormEnv) {
	if env.pool == nil {
		return
	}
	for _, sc := range env.conns {
		emitTransitionRaw(env.pool, env.collector, sc.user.hello(nowMillis()))
		go func(sc *stormConn) {
			tick := time.NewTicker(env.cfg.Heartbeat)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
				if sc.paused.Load() {
					continue
				}
				tr := sc.user.ping(nowMillis())
				_ = env.pool.Publish(tr.subject, tr.payload)
			}
		}(sc)
	}
}

// runPresenceStorm is the production entrypoint.
func runPresenceStorm(ctx context.Context, baseCfg *config, args []string) int {
	cfg, err := parseStormConfig(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		slog.Error("parse presence-storm config", "error", err)
		return 2
	}
	env := (&prodStormFactory{baseCfg: baseCfg}).Build(cfg)
	if env.pool != nil {
		defer env.pool.Close()
	}
	warmStormPopulation(ctx, env)
	_ = waitOrCancel(ctx, cfg.Warmup)

	results := runStormSteps(ctx, cfg, env)
	renderStormConsole(os.Stdout, results)
	if cfg.CSVPath != "" {
		if err := writeStormCSV(cfg.CSVPath, results); err != nil {
			slog.Error("write storm csv", "error", err)
			return 1
		}
	}
	return presenceStormExitCode(results)
}

func presenceStormExitCode(results []stormStepResult) int {
	for i := range results {
		if results[i].Kind == verdictPass {
			return 0
		}
	}
	return 1
}
