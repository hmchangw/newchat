package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// retentionVerdict is checkStreamRetention's answer: whether the stream can hold a
// backlog for as long as this service now needs it to, and if not, what to change.
type retentionVerdict struct {
	Sufficient bool
	Reasons    []string
}

// checkStreamRetention compares MESSAGES-CANONICAL's retention against the outage
// length the service is sized for.
//
// MaxDeliver=-1 removed the consumer's give-up boundary, which leaves the stream as
// the only one. During an outage the consumer stalls at MaxAckPending by design and
// the backlog accumulates here; if MaxAge is shorter than the outage, JetStream ages
// those messages off and the loss is worse than what this service set out to fix —
// a terminated delivery at least logged an ERROR, a retention discard logs nothing.
//
// MaxAge=0 means unlimited, which is the safest setting rather than the most
// suspicious one. A non-positive floor disables the check for operators who bound
// the stream by MaxBytes alone.
func checkStreamRetention(cfg *jetstream.StreamConfig, minMaxAge time.Duration) retentionVerdict {
	if minMaxAge <= 0 || cfg.MaxAge == 0 || cfg.MaxAge >= minMaxAge {
		return retentionVerdict{Sufficient: true}
	}
	return retentionVerdict{Reasons: []string{fmt.Sprintf(
		"stream MaxAge is %s, below the %s this service's retry policy assumes: "+
			"with MaxDeliver=-1 the stream is the only durability boundary, and a backlog "+
			"older than MaxAge is discarded without a log line",
		cfg.MaxAge, minMaxAge)}}
}

// streamInfoFunc reads the stream's current config. Injected so the startup check is
// exercised without a live NATS connection.
type streamInfoFunc func(ctx context.Context) (*jetstream.StreamInfo, error)

// reportStreamRetention logs the durability boundary at startup and flags a stream
// that cannot hold the backlog.
//
// Deliberately advisory: a bad verdict logs and sets a gauge, it does not exit.
// Refusing to boot would stop this pod persisting anything at all, which is a total
// outage traded for a partial one — the wrong direction for a service whose entire
// design principle is that the failure direction must be inert. The retention values
// are logged unconditionally so the boundary is on the record even when it passes,
// since pkg/stream sets only Name and Subjects and nothing in the repo can show it.
func reportStreamRetention(ctx context.Context, info streamInfoFunc, streamName string, minMaxAge time.Duration, m *metrics) {
	si, err := info(ctx)
	if err != nil || si == nil {
		// Not fatal, and not silent: an unverified boundary is not a verified-good one.
		slog.WarnContext(ctx, "could not read stream retention — the durability boundary is unverified",
			"error", err, "stream", streamName)
		m.setStreamRetentionUnknown()
		return
	}

	verdict := checkStreamRetention(&si.Config, minMaxAge)
	m.setStreamRetentionInsufficient(!verdict.Sufficient)
	attrs := []any{
		"stream", streamName,
		"max_age", si.Config.MaxAge.String(),
		"max_bytes", si.Config.MaxBytes,
		"max_msgs", si.Config.MaxMsgs,
		"discard", si.Config.Discard.String(),
		"min_max_age", minMaxAge.String(),
	}
	if verdict.Sufficient {
		slog.InfoContext(ctx, "stream retention checked", attrs...)
		return
	}
	slog.ErrorContext(ctx, "stream retention is below what the retry policy assumes — messages can age off the stream during an outage",
		append(attrs, "reasons", verdict.Reasons)...)
}
