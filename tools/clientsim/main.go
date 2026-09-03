// Command clientsim holds real WSS client connections against a site: per
// simulated user it mints a JWT through the auth exchange, connects over
// NATS WebSocket, performs the production frontend's subscription walk, and
// counts deliveries. See docs/superpowers/specs/2026-08-29-clientsim-design.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/poolartifact"
)

type config struct {
	NATSWSURL string `env:"CLIENTSIM_NATS_WS_URL,required"`
	// AllowInsecureWS opts into a cleartext ws:// URL. Off by default because
	// the NATS user JWT crosses the wire on connect: against anything shared
	// that is a credential in the clear, and a warning is too easy to miss in
	// a soak's log stream. The local throwaway stack sets it explicitly.
	AllowInsecureWS bool    `env:"CLIENTSIM_ALLOW_INSECURE_WS" envDefault:"false"`
	AuthURL         string  `env:"CLIENTSIM_AUTH_URL,required"`
	PoolFile        string  `env:"CLIENTSIM_POOL_FILE,required"`
	SiteID          string  `env:"CLIENTSIM_SITE_ID,required"`
	TargetConns     int     `env:"CLIENTSIM_TARGET_CONNS" envDefault:"0"`
	ShardIndex      int     `env:"CLIENTSIM_SHARD_INDEX" envDefault:"0"`
	ShardCount      int     `env:"CLIENTSIM_SHARD_COUNT" envDefault:"1"`
	RampRate        float64 `env:"CLIENTSIM_RAMP_RATE" envDefault:"50"`
	ChurnRate       float64 `env:"CLIENTSIM_CHURN_RATE" envDefault:"0"`
	JWTMode         string  `env:"CLIENTSIM_JWT_MODE" envDefault:"proactive"`
	// MinReadyRatio is the fraction of the shard that must have reached
	// full readiness at some point for the run to count. 0 disables it.
	MinReadyRatio float64 `env:"CLIENTSIM_MIN_READY_RATIO" envDefault:"0.95"`
	// FailOnDegraded turns loss evidence into a non-zero exit. Off by
	// default: in a failure test that loss is the measurement, not a fault.
	FailOnDegraded bool `env:"CLIENTSIM_FAIL_ON_DEGRADED" envDefault:"false"`
	SubPendingMsgs int  `env:"CLIENTSIM_SUB_PENDING_MSGS" envDefault:"512"`
	// 128 KiB: payloads are counted and dropped, so the byte limit exists
	// only as a memory backstop — at 10k conns a 1 MiB default would budget
	// hundreds of GiB (review: performance §3).
	SubPendingBytes   int           `env:"CLIENTSIM_SUB_PENDING_BYTES" envDefault:"131072"`
	ReconnectBufBytes int           `env:"CLIENTSIM_RECONNECT_BUF_BYTES" envDefault:"65536"`
	PingInterval      time.Duration `env:"CLIENTSIM_PING_INTERVAL" envDefault:"2m"`
	MetricsAddr       string        `env:"CLIENTSIM_METRICS_ADDR" envDefault:":2112"`
}

const (
	jwtModeProactive = "proactive"
	jwtModeExpiry    = "expiry"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil {
		slog.Error("clientsim failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if err := validateConfig(&cfg); err != nil {
		return err
	}

	pool, err := poolartifact.Load(cfg.PoolFile, cfg.SiteID)
	if err != nil {
		return fmt.Errorf("load pool artifact: %w", err)
	}
	shard, err := shardSlice(pool.Accounts, cfg.TargetConns, cfg.ShardIndex, cfg.ShardCount)
	if err != nil {
		return fmt.Errorf("compute shard: %w", err)
	}
	slog.Info("clientsim starting",
		"runId", pool.RunID, "configDigest", pool.ConfigDigest,
		"shardIndex", cfg.ShardIndex, "shardCount", cfg.ShardCount,
		"shardAccounts", len(shard), "jwtMode", cfg.JWTMode)

	m := newMetrics()
	m.RunInfo.WithLabelValues(cfg.JWTMode,
		strconv.Itoa(cfg.ShardIndex), strconv.Itoa(cfg.ShardCount)).Set(1)
	// Bind before the swarm starts: a soak whose metrics endpoint never
	// came up would run blind for hours — that is a startup failure.
	lis, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		return fmt.Errorf("bind metrics endpoint %s: %w", cfg.MetricsAddr, err)
	}
	// ReadHeaderTimeout alone does not bound the body: a client can declare one
	// and then dribble it while the server drains it after the handler returns.
	metricsSrv := &http.Server{
		Handler:           m.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := metricsSrv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server stopped mid-run", "error", err)
		}
	}()

	mintClient := newAuthClient(cfg.AuthURL, devProvider{}, m)
	factory := func(account string) (runnable, error) {
		return newSimClient(account, pool.RunID, &cfg, mintClient, m)
	}

	// Snapshot readiness at the shutdown boundary, then propagate the caller's
	// cancellation into the swarm. Passing ctx directly would drain every
	// connection before the gate could distinguish a recovered fleet from one
	// that remained collapsed after a fault.
	swarmCtx, cancelSwarm := context.WithCancel(context.WithoutCancel(ctx))
	captured := make(chan struct{})
	go func() {
		<-ctx.Done()
		m.captureReadyAtDrain()
		cancelSwarm()
		close(captured)
	}()
	swarmErr := runSwarm(swarmCtx, shard, cfg.RampRate, cfg.ChurnRate, factory)
	<-captured

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx) // best-effort; the process is exiting either way
	summary, summaryErr := printSummary(m, pool.RunID, pool.ConfigDigest, len(shard))

	if swarmErr != nil {
		return swarmErr
	}
	if summaryErr != nil {
		return fmt.Errorf("summarize run: %w", summaryErr)
	}
	// The fleet gate runs even on a clean drain: a soak that never held the
	// connections it was asked to hold produced numbers about nothing.
	if err := readyGate(m, len(shard), cfg.MinReadyRatio); err != nil {
		return err
	}
	if cfg.FailOnDegraded && summary.Degraded {
		return errors.New("run marked degraded and CLIENTSIM_FAIL_ON_DEGRADED is set")
	}
	return nil
}

// validateConfig rejects value combinations that would silently void a
// spec-required control instead of limping with surprising semantics.
func validateConfig(cfg *config) error {
	// The NATS user JWT crosses the wire on connect, so cleartext has to be a
	// deliberate act rather than a typo in a URL. Opting in still warns: on a
	// throwaway local stack that is fine, against anything shared it is not.
	if !strings.HasPrefix(cfg.NATSWSURL, "wss://") {
		if !cfg.AllowInsecureWS {
			return fmt.Errorf("CLIENTSIM_NATS_WS_URL must use wss://, got %q — set CLIENTSIM_ALLOW_INSECURE_WS=true only for a throwaway local stack",
				cfg.NATSWSURL)
		}
		slog.Warn("connecting over cleartext WebSocket; the NATS user JWT is sent unencrypted — never do this against a shared cluster",
			"url", cfg.NATSWSURL)
	}
	if cfg.JWTMode != jwtModeProactive && cfg.JWTMode != jwtModeExpiry {
		return fmt.Errorf("CLIENTSIM_JWT_MODE must be proactive or expiry, got %q", cfg.JWTMode)
	}
	if cfg.MinReadyRatio < 0 || cfg.MinReadyRatio > 1 {
		return fmt.Errorf("CLIENTSIM_MIN_READY_RATIO must be within [0,1], got %v", cfg.MinReadyRatio)
	}
	// Non-positive pending values mean UNLIMITED in nats.go — the opposite
	// of the resource control spec §5.3 requires.
	if cfg.SubPendingMsgs <= 0 || cfg.SubPendingBytes <= 0 {
		return fmt.Errorf("CLIENTSIM_SUB_PENDING_MSGS/_BYTES must be positive, got %d/%d",
			cfg.SubPendingMsgs, cfg.SubPendingBytes)
	}
	if cfg.RampRate <= 0 {
		return fmt.Errorf("CLIENTSIM_RAMP_RATE must be positive, got %v", cfg.RampRate)
	}
	if cfg.ChurnRate < 0 {
		return fmt.Errorf("CLIENTSIM_CHURN_RATE must be >= 0, got %v", cfg.ChurnRate)
	}
	if cfg.ReconnectBufBytes <= 0 || cfg.PingInterval <= 0 {
		return fmt.Errorf("CLIENTSIM_RECONNECT_BUF_BYTES and CLIENTSIM_PING_INTERVAL must be positive")
	}
	return nil
}
