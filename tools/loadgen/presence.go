package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"slices"
	"sync/atomic"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

type presenceConfig struct {
	Steps            []int
	Warmup           time.Duration
	Hold             time.Duration
	Cooldown         time.Duration
	Heartbeat        time.Duration // ping interval per user
	ActivityFlipRate float64       // activity flips per user per minute
	ReconnectRate    float64       // reconnects per user per minute
	StopOnTrip       bool
	PublisherConns   int
	ObserverConns    int
	CSVPath          string
	P95Ms            float64
	P99Ms            float64
	ErrorRate        float64
}

func slicesMaxInt(xs []int) int {
	if len(xs) == 0 {
		return 0
	}
	return slices.Max(xs)
}

func parsePresenceConfig(args []string) (presenceConfig, error) {
	fs := flag.NewFlagSet("presence-sustained", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `loadgen presence-sustained — find the sustainable presence population N

Ramps a synthetic user population through --steps. For each N: activates the
delta of new users (each sends hello), warms up, holds while users heartbeat
(ping) and churn (activity flips + reconnects), then grades state-publish
latency, error rate, and loadgen self-saturation. Reports the largest passing N.

Flags:
`)
		fs.PrintDefaults()
	}
	steps := fs.String("steps", "1000,2000,5000,10000,20000,50000,100000", "comma-separated N per step; `k` suffix x1000")
	warmup := fs.Duration("warmup", 30*time.Second, "per-step warm-up before measurement")
	hold := fs.Duration("hold", 120*time.Second, "per-step steady-state measurement window")
	cooldown := fs.Duration("cooldown", 15*time.Second, "per-step cooldown before next step")
	heartbeat := fs.Duration("heartbeat", 30*time.Second, "per-user ping interval (matches PRESENCE_HEARTBEAT_INTERVAL)")
	flip := fs.Float64("activity-flip-rate", 4, "activity flips per user per minute (churn that publishes)")
	reconnect := fs.Float64("reconnect-rate", 1, "bye+hello reconnects per user per minute")
	stop := fs.Bool("stop-on-trip", true, "stop the ramp on the first TRIP")
	pub := fs.Int("publisher-conns", 16, "shared publisher connection count")
	obs := fs.Int("observer-conns", 4, "observer connection count (subscribe presence.state.*)")
	csv := fs.String("csv", "", "optional CSV output path")
	p95 := fs.Float64("p95-ms", 200, "state-publish p95 latency cap (ms)")
	p99 := fs.Float64("p99-ms", 500, "state-publish p99 latency cap (ms)")
	errRate := fs.Float64("error-rate", 0.01, "error-rate cap (fraction)")
	if err := fs.Parse(args); err != nil {
		return presenceConfig{}, err
	}
	parsedSteps, err := parseStepList(*steps)
	if err != nil {
		return presenceConfig{}, err
	}
	return presenceConfig{
		Steps: parsedSteps, Warmup: *warmup, Hold: *hold, Cooldown: *cooldown,
		Heartbeat: *heartbeat, ActivityFlipRate: *flip, ReconnectRate: *reconnect,
		StopOnTrip: *stop, PublisherConns: *pub, ObserverConns: *obs, CSVPath: *csv,
		P95Ms: *p95, P99Ms: *p99, ErrorRate: *errRate,
	}, nil
}

// presenceEnv bundles a sustained run's deps. The onActivated / afterReset
// function fields are test seams (nil in prod → real wiring used).
type presenceEnv struct {
	pool       *presencePool
	collector  *presenceCollector
	users      []*presenceUser
	thresholds presenceThresholds
	siteID     string
	warmup     time.Duration
	hold       time.Duration
	cooldown   time.Duration
	heartbeat  time.Duration
	flipRate   float64
	reconnRate float64

	// Test seams; prod leaves them nil and uses the real loops.
	onActivated func(env *presenceEnv, idx int)
	afterReset  func(env *presenceEnv)

	holdStartNanos    atomic.Int64
	holdDurationNanos atomic.Int64
	activated         atomic.Int64
}

func (env *presenceEnv) setHold(start time.Time, d time.Duration) {
	env.holdStartNanos.Store(start.UnixNano())
	env.holdDurationNanos.Store(d.Nanoseconds())
}

func (env *presenceEnv) holding() bool { return env.holdDurationNanos.Load() > 0 }

type presenceFactory interface {
	Build(cfg presenceConfig) *presenceEnv
}

// runStepPresence activates the delta of new users, warms up, holds while
// measuring, then cools down and returns the graded step.
func runStepPresence(ctx context.Context, env *presenceEnv, n, prevN int) presenceStepResult {
	activatePresenceUsers(ctx, env, prevN, n)
	_ = waitOrCancel(ctx, env.warmup)

	startedAt := time.Now()
	env.setHold(startedAt, env.hold)
	env.collector.Reset()
	if env.afterReset != nil {
		env.afterReset(env)
	}
	_ = waitOrCancel(ctx, env.hold)
	env.collector.ReapMissing()
	env.holdDurationNanos.Store(0) // pause churn during cooldown

	in := presenceStepInputs{
		N: n, EffectiveN: int(env.activated.Load()),
		StartedAt: startedAt, HoldDuration: env.hold,
		LatencySamples: env.collector.LatenciesMs(),
		Attempted:      env.collector.Attempted(),
		Failed:         env.collector.Failed(),
		Self:           snapshotSelfMetrics(),
	}
	r := evaluatePresenceStep(in, env.thresholds)
	_ = waitOrCancel(ctx, env.cooldown)
	return r
}

// activatePresenceUsers brings users [from,to) online (hello) at a bounded
// rate and starts their per-user emitter goroutine.
func activatePresenceUsers(ctx context.Context, env *presenceEnv, from, to int) {
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
		u := env.users[i]
		if env.onActivated != nil {
			env.onActivated(env, i) // test seam
		} else {
			startPresenceEmitter(ctx, env, u)
		}
		env.activated.Add(1)
	}
}

// startPresenceEmitter runs one user's hello + heartbeat + churn loop until
// ctx cancels. At most one measured state-changing transition fires per tick
// so each maps 1:1 to the next publish for this account.
func startPresenceEmitter(ctx context.Context, env *presenceEnv, u *presenceUser) {
	emitTransition(env, u.hello(nowMillis())) // initial (pre-hold; cleared by Reset)
	go func() {
		seed := int64(uint64(u.idx)*0x9E3779B97F4A7C15 + 1) //nolint:gosec // wrapping arithmetic is intentional
		r := rand.New(rand.NewSource(seed))
		tick := time.NewTicker(env.heartbeat)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			// In the dropped window (a bye fired on a prior tick): reconnect
			// with a measured hello and resume pings next tick. Do NOT ping
			// while offline — a ping for the removed connection would itself
			// re-create it (first-sight online) and race the hello.
			if u.status == model.StatusOffline {
				emitTransition(env, u.hello(nowMillis()))
				continue
			}
			emitTransition(env, u.ping(nowMillis())) // steady-state no-op publish
			if !env.holding() {
				continue
			}
			frac := env.heartbeat.Minutes()
			switch {
			case r.Float64() < env.reconnRate*frac:
				emitTransition(env, u.bye(nowMillis())) // -> offline; hello next tick
			case r.Float64() < env.flipRate*frac:
				emitTransition(env, u.setAway(!u.away, nowMillis()))
			}
		}
	}()
}

// emitTransition publishes one transition, recording attempt/expectation/
// failure against the collector. No-op transitions (expect == StatusNone) are
// sent but not counted as measurable attempts.
func emitTransition(env *presenceEnv, tr presenceTransition) {
	if env.pool == nil {
		return // publisher down; surfaces as INCONCLUSIVE (zero attempts)
	}
	sentAt := time.Now()
	err := env.pool.Publish(tr.subject, tr.payload)
	if tr.expect == "" { // StatusNone: no publish expected, don't measure
		return
	}
	if err != nil {
		env.collector.RecordEmit()
		env.collector.RecordEmitFailure()
		return
	}
	env.collector.Expect(accountFromSubject(tr.subject), tr.expect, sentAt)
}

func nowMillis() int64 { return time.Now().UTC().UnixMilli() }

// accountFromSubject extracts {account} from chat.user.{account}.event... .
func accountFromSubject(subj string) string {
	const prefix = "chat.user."
	if len(subj) <= len(prefix) {
		return ""
	}
	rest := subj[len(prefix):]
	for i := 0; i < len(rest); i++ {
		if rest[i] == '.' {
			return rest[:i]
		}
	}
	return rest
}

//nolint:gocritic // cfg passed by value to satisfy presenceFactory interface
func runPresenceSustainedForTest(ctx context.Context, cfg presenceConfig, f presenceFactory) ([]presenceStepResult, error) {
	if len(cfg.Steps) == 0 {
		return nil, fmt.Errorf("cfg.Steps cannot be empty")
	}
	env := f.Build(cfg)
	if env.pool != nil {
		defer env.pool.Close()
	}
	prevN := 0
	var results []presenceStepResult
	for _, n := range cfg.Steps {
		if err := ctx.Err(); err != nil {
			slog.Info("presence-sustained interrupted", "completed_steps", len(results))
			break
		}
		r := runStepPresence(ctx, env, n, prevN)
		results = append(results, r)
		if cfg.StopOnTrip && r.Kind == verdictTrip {
			break
		}
		prevN = n
	}
	return results, nil
}

// prodPresenceFactory wires the real pool.
type prodPresenceFactory struct{ baseCfg *config }

//nolint:gocritic // cfg by value to match interface
func (f *prodPresenceFactory) Build(cfg presenceConfig) *presenceEnv {
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
	return &presenceEnv{
		pool: pool, collector: c, users: users,
		thresholds: presenceThresholds{P95Ms: cfg.P95Ms, P99Ms: cfg.P99Ms, ErrorRate: cfg.ErrorRate, GCPauseInconclusive: 50},
		siteID:     siteID,
		warmup:     cfg.Warmup, hold: cfg.Hold, cooldown: cfg.Cooldown,
		heartbeat: cfg.Heartbeat, flipRate: cfg.ActivityFlipRate, reconnRate: cfg.ReconnectRate,
	}
}

// runPresenceSustained is the production entrypoint invoked by main.go.
func runPresenceSustained(ctx context.Context, baseCfg *config, args []string) int {
	cfg, err := parsePresenceConfig(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		slog.Error("parse presence-sustained config", "error", err)
		return 2
	}
	results, err := runPresenceSustainedForTest(ctx, cfg, &prodPresenceFactory{baseCfg: baseCfg})
	if err != nil {
		slog.Error("presence-sustained run", "error", err)
		return 1
	}
	renderPresenceConsole(os.Stdout, results)
	if cfg.CSVPath != "" {
		if err := writePresenceCSV(cfg.CSVPath, results); err != nil {
			slog.Error("write presence csv", "error", err)
			return 1
		}
	}
	return presenceExitCode(results)
}

// presenceExitCode returns 0 if any step passed, else 1.
func presenceExitCode(results []presenceStepResult) int {
	for i := range results {
		if results[i].Kind == verdictPass {
			return 0
		}
	}
	return 1
}
