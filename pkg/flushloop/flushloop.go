// Package flushloop drives a buffered writer's periodic drain and, once the
// process is shutting down, the one final drain that lands what is still
// buffered.
//
// Three services had written this loop independently — unread-worker's
// write-intent batch, broadcast-worker's room-list preview, and the oplog
// connector's checkpointer — and each got the final drain subtly different. The
// hard part is not the ticker: it is that the final drain runs precisely when
// the context that drove the loop has just been cancelled, so it cannot use
// that context. Two of the three reached for context.Background(), which works
// but drops the trace and every request-scoped value; the third used
// context.WithoutCancel with no timeout at all, so a hung final write could
// outlast the whole 25s shutdown budget.
//
// This does both correctly: WithoutCancel to keep the values, plus a deadline.
package flushloop

import (
	"context"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/jobguard"
)

// DefaultFinalTimeout bounds the post-cancellation drain. It is deliberately a
// constant rather than a Config field: that drain is the last thing between a
// buffered batch and losing it, and it also has to sit inside the process's
// shutdown budget, so there is one sensible answer and no reason for a caller
// to pick a different one. Comfortably inside the 25s budget every service
// allows, and far longer than a drain that is going to succeed needs.
const DefaultFinalTimeout = 5 * time.Second

// Config describes one buffered writer's drain cadence.
type Config struct {
	// Name labels the loop in panic and failure logs. It reads as the thing
	// being drained, e.g. "unread-state flush".
	Name string
	// Interval is the drain cadence. Must be positive; Run refuses to start
	// otherwise. Validate it at startup where you can name the env var.
	Interval time.Duration
	// PerFlush bounds one periodic drain. Zero leaves the drain on the loop's
	// own context, for a writer that already bounds itself.
	//
	// Worth setting when the loop drives the drain synchronously: an unbounded
	// write does not merely delay its own batch, it stops every later drain
	// while the buffer behind it keeps growing. Keep it well under the
	// consumer's AckWait, for the same reason the Mongo server-selection
	// timeout stays under it.
	PerFlush time.Duration
	// Logger receives drain failures. Nil uses the default logger.
	Logger *slog.Logger
}

func (c Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

// Run drains through flush on cfg.Interval until ctx is cancelled, then drains
// once more so a batch buffered at shutdown still lands. It returns after that
// final drain.
//
// Every drain is panic-recovered: these loops run user-derived data through a
// bulk write, and an unrecovered panic here takes the whole process down —
// which, for a worker holding un-acked messages, turns one poison batch into a
// crash loop. A drain that panics or errors is logged and the loop keeps
// ticking; flush owns whether a failure is worth retrying.
//
// A non-positive Interval logs and returns rather than panicking inside
// time.NewTicker, since that panic would land in a goroutine and kill the
// process. Validate at startup to catch it as a config error instead.
func Run(ctx context.Context, cfg Config, flush func(context.Context) error) {
	if cfg.Interval <= 0 {
		cfg.logger().ErrorContext(ctx, "flush loop not started: non-positive interval",
			"loop", cfg.Name, "interval", cfg.Interval)
		return
	}

	t := time.NewTicker(cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// WithoutCancel, then a deadline: the values (trace, request id)
			// survive so the final drain is still attributable, while the
			// deadline keeps it inside the shutdown budget.
			finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), DefaultFinalTimeout)
			run(finalCtx, cfg, flush, "final")
			cancel()
			return
		case <-t.C:
			flushCtx, cancel := bound(ctx, cfg.PerFlush)
			run(flushCtx, cfg, flush, "periodic")
			cancel()
		}
	}
}

// bound applies the per-drain deadline, or leaves the context alone when none is
// configured. The returned cancel is always safe to call.
func bound(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// run performs one guarded drain and logs a failure against phase, so an
// operator can tell a routine drain failure from losing the shutdown batch.
func run(ctx context.Context, cfg Config, flush func(context.Context) error, phase string) {
	jobguard.Guard(cfg.Name, func() {
		if err := flush(ctx); err != nil {
			cfg.logger().ErrorContext(ctx, "flush failed", "loop", cfg.Name, "phase", phase, "error", err)
		}
	})
}
