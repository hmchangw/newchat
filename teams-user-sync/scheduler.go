package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/hmchangw/chat/pkg/idgen"
)

// guardedJob wraps run with skip-if-still-running semantics: a fire that
// arrives while the previous run is executing is dropped (robfig logs the
// skip via cronSlogLogger), never queued.
func guardedJob(run func()) cron.Job {
	return cron.NewChain(cron.SkipIfStillRunning(cronSlogLogger{})).Then(cron.FuncJob(run))
}

// runSync executes one updateUsers run under a fresh request id and logs the
// outcome. The Syncer itself is silent; this is the run boundary where errors
// are logged exactly once.
func runSync(syncer *Syncer) {
	logger := slog.With("requestId", idgen.GenerateRequestID())
	logger.Info("teams user sync started")
	start := time.Now()

	stats, err := syncer.UpdateUsers(context.Background())
	fields := []any{
		"pages", stats.Pages,
		"seen", stats.Seen,
		"existing", stats.Existing,
		"domainSkipped", stats.DomainSkipped,
		"hrUnmatched", stats.HRUnmatched,
		"upserted", stats.Upserted,
		"durationMs", time.Since(start).Milliseconds(),
	}
	if err != nil {
		logger.Error("teams user sync failed", append(fields, "error", err)...)
		return
	}
	logger.Info("teams user sync finished", fields...)
}

// cronSlogLogger adapts robfig's cron.Logger to slog (JSON discipline).
type cronSlogLogger struct{}

func (cronSlogLogger) Info(msg string, kv ...any) {
	slog.Info("cron: "+msg, kv...)
}

func (cronSlogLogger) Error(err error, msg string, kv ...any) {
	slog.Error("cron: "+msg, append(kv, "error", err)...)
}
