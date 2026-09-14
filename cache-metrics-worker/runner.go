package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/cachescan"
)

// runner periodically walks the Valkey keyspace and publishes per-cache usage.
type runner struct {
	cluster cachescan.Cluster
	metrics *cachescan.Metrics
	opts    cachescan.Options
	// timeout bounds one scan so a stalled node cannot wedge the loop until
	// process shutdown; the next tick starts a fresh attempt.
	timeout time.Duration
}

// scanOnce performs a single scan and records the result.
func (r *runner) scanOnce(ctx context.Context) error {
	scanCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	start := time.Now()
	report, err := cachescan.Scan(scanCtx, r.cluster, r.opts)
	if err != nil {
		return fmt.Errorf("scan keyspace: %w", err)
	}
	r.metrics.Record(ctx, report)

	slog.InfoContext(ctx, "cache keyspace scanned",
		"keys", report.TotalKeys,
		"estimated_bytes", report.TotalBytes,
		"caches", len(report.Caches),
		"duration_ms", time.Since(start).Milliseconds())
	return nil
}

// run scans immediately, then on every tick until ctx is done. A failed scan is
// logged and the loop continues: ending it would freeze every gauge at its last
// value, which reads as a healthy steady state rather than an outage.
func (r *runner) run(ctx context.Context, interval time.Duration) {
	scan := func() {
		if err := r.scanOnce(ctx); err != nil {
			slog.ErrorContext(ctx, "cache keyspace scan failed", "error", err)
		}
	}

	scan()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scan()
		}
	}
}
