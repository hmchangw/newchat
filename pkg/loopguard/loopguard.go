// Package loopguard makes a dead consume loop visible and recoverable.
//
// A JetStream worker's only goroutine returns on a terminal iterator error and
// nothing else observes that: the NATS connection stays healthy, liveness
// always answers 200 by design, and the only wg.Wait() is the shutdown drain.
// The pod keeps reporting ready while processing nothing — the failure hardest
// to notice and slowest to diagnose. Readiness alone cannot recover it either:
// nothing routes traffic to a queue worker, so a 503 on /readyz makes the death
// visible without ever replacing the pod. A Guard therefore does both — it
// fails readiness with the cause, and on an unexpected stop it runs a hook
// (normally SelfShutdown) that asks the process to terminate so the supervisor
// starts a fresh one, the only actor able to rebuild the iterator.
package loopguard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"syscall"

	"github.com/hmchangw/chat/pkg/health"
)

// ErrConsumeClosed is the cause recorded when a watched consume context closes.
var ErrConsumeClosed = errors.New("consume context closed")

// Guard tracks one consume loop. Zero value is not usable; use New.
type Guard struct {
	name             string
	reason           atomic.Pointer[error]
	stopping         atomic.Bool
	fired            atomic.Bool
	onUnexpectedStop func()
}

// New returns a Guard whose readiness row is named name. onUnexpectedStop runs
// once, on the first stop that is not preceded by BeginShutdown; nil disables
// it (readiness-only mode, for tests).
func New(name string, onUnexpectedStop func()) *Guard {
	return &Guard{name: name, onUnexpectedStop: onUnexpectedStop}
}

// BeginShutdown marks the stop that follows as intended, so tearing the worker
// down does not look like a failure and re-signal a process already exiting.
// Call it as the first shutdown hook, before iter.Stop().
func (g *Guard) BeginShutdown() { g.stopping.Store(true) }

// Stopped records that the loop has exited with err. Every exit is recorded,
// including the deliberate one during shutdown: a worker that has stopped
// consuming is not ready either way, and during shutdown failing readiness is
// what drains it from the load balancer. Only an unexpected stop runs the hook,
// and only the first of them — a loop with two observers (its own exit and a
// WatchClosed watcher) may report the same death twice.
func (g *Guard) Stopped(err error) {
	g.reason.CompareAndSwap(nil, &err)
	// A graceful stop is the normal end of every pod's life — iter.Stop()
	// during shutdown is what makes Next return — and logging that at ERROR
	// trips error-rate alerting on every deploy.
	if g.stopping.Load() {
		slog.Info("consume loop stopped (shutdown)", "loop", g.name, "error", err)
		return
	}
	slog.Error("consume loop stopped; no further messages will be processed", "loop", g.name, "error", err)
	if g.onUnexpectedStop != nil && g.fired.CompareAndSwap(false, true) {
		g.onUnexpectedStop()
	}
}

// WatchClosed reports the loop stopped when closed is closed. It is for
// callback consumers (jetstream.Consume), whose only death signal is
// ConsumeContext.Closed(). The goroutine ends when the channel closes, which
// shutdown guarantees by stopping the consume context.
func (g *Guard) WatchClosed(closed <-chan struct{}) {
	go func() {
		<-closed
		g.Stopped(ErrConsumeClosed)
	}()
}

// Check reports not-ready once the loop has stopped. The readiness contract is
// "can this pod do its job", and a worker whose loop has exited cannot,
// whatever the reason.
func (g *Guard) Check() health.Check {
	return health.Check{Name: g.name, Probe: func(context.Context) error {
		if err := g.reason.Load(); err != nil {
			return fmt.Errorf("consume loop stopped: %w", *err)
		}
		return nil
	}}
}

// signaller is the one method SelfShutdown needs from a process, so the
// undeliverable-signal path is reachable from a test.
type signaller interface{ Signal(os.Signal) error }

// Swapped in tests; os.Exit and os.FindProcess in production.
var (
	findProcess = func(pid int) (signaller, error) { return os.FindProcess(pid) }
	exitProcess = os.Exit
)

// SelfShutdown raises SIGTERM on this process so shutdown.WaitOn runs the
// ordinary graceful teardown — draining in-flight work rather than abandoning
// it, which an immediate exit would do. The caller must have armed the signal
// (shutdown.Signals) BEFORE starting any loop able to call this, or the signal
// keeps its default disposition and kills the process outright.
//
// When the signal cannot be delivered, exit non-zero rather than return: the
// guard has already latched its hook, so nothing retries the restart, and a
// worker left running would sit ready-but-idle — the failure this package
// exists to eliminate. A drain lost to a hard exit is the lesser cost, and the
// non-zero status is what a restart-on-failure supervisor needs to see.
func SelfShutdown() {
	p, err := findProcess(os.Getpid())
	if err != nil {
		slog.Error("cannot find own process to signal after consume loop stopped; exiting non-zero so the supervisor replaces the pod", "error", err)
		exitProcess(1)
		return
	}
	if err := p.Signal(syscall.SIGTERM); err != nil {
		slog.Error("cannot signal self after consume loop stopped; exiting non-zero so the supervisor replaces the pod", "error", err)
		exitProcess(1)
	}
}
