package shutdown

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Wait blocks until SIGINT or SIGTERM, then calls each shutdown function sequentially.
// If cleanup does not complete within the given timeout, Wait returns and logs a warning.
// The timeout context is passed to each shutdown function so they can respect the deadline.
//
// Wait arms the signal handler itself, which is right for a service whose
// shutdown is only ever triggered from outside. A service that can raise its
// own shutdown signal must use WaitOn instead — see the note there.
func Wait(ctx context.Context, timeout time.Duration, shutdownFuncs ...func(context.Context) error) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	WaitOn(ctx, sig, timeout, shutdownFuncs...)
}

// WaitOn is Wait with the signal channel supplied by the caller, already armed
// via signal.Notify.
//
// It exists for services that can signal themselves — a worker that raises
// SIGTERM when its consume loop dies, say. Those race Wait's own registration:
// until signal.Notify runs, SIGTERM keeps its default disposition and kills the
// process instead of running the graceful path, and a signal raised in that
// window is lost to a handler armed afterwards. Arming a buffered channel
// before starting anything able to raise it closes both halves — the signal is
// non-fatal from the start, and it waits in the buffer until this call reads it.
func WaitOn(ctx context.Context, sig <-chan os.Signal, timeout time.Duration, shutdownFuncs ...func(context.Context) error) {
	<-sig
	slog.Info("shutting down...")

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, fn := range shutdownFuncs {
			if err := fn(ctx); err != nil {
				slog.Error("shutdown error", "error", err)
			}
		}
	}()

	select {
	case <-done:
		slog.Info("shutdown complete")
	case <-ctx.Done():
		slog.Warn("shutdown timed out, forcing exit")
	}
}
