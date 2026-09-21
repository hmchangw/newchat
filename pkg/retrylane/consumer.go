package retrylane

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// Settings is the retry lane's env surface. Embed in a service Config with
// envPrefix:"RETRY_".
//
// The surface exposes a split *index*, not a schedule. A raw []time.Duration
// env knob has no off-switch under caarlos0/env — an empty value falls back to
// envDefault — and would let an operator silently redefine the retry budget.
type Settings struct {
	// Enabled is the per-service opt-in, default false. Disabling stops new
	// escalations; the retry consumer keeps draining what is already parked.
	Enabled bool `env:"LANE_ENABLED" envDefault:"false"`

	// FastSteps is how many deliveries stay on the hot lane. The occupancy it
	// leaves in place is schedule-specific, not a universal 36s: on
	// jsretry.DefaultBackoff ({1s,5s,30s,2m,10m}) 3 steps leave 36s instead of
	// 756s; on jsretry.LowLatencyBackoff ({200ms,1s,5s,30s}) — which
	// broadcast-worker runs — they leave 6.2s instead of 66.2s.
	FastSteps int `env:"LANE_FAST_STEPS" envDefault:"3"`

	// Consumer tunes the retry lane's own durable, envPrefix RETRY_CONSUMER_.
	Consumer ConsumerSettings `envPrefix:"CONSUMER_"`
}

// ConsumerSettings is the retry lane's own tagged view of the durable
// consumer knobs, carrying the retry-lane numbers directly as envDefault
// tags so env.Parse produces the right values on the first pass.
//
// This can't be stream.ConsumerSettings: caarlos0/env resolves envDefault
// per field, not per embedding, so a nested stream.ConsumerSettings would
// always default to the hot lane's own numbers (MaxAckPending=1000,
// MaxDeliver=6) here regardless of what this struct wants — the retry lane
// would silently inherit the hot lane's budget instead of its own.
//
// MaxAckPending defaults high: this lane deliberately holds the long waits,
// sized for ~5 escalations/s against ~720s of slow-rung occupancy.
// MaxDeliver counts retry-lane attempts only.
//
// MaxWorkers is the mirror image, and deliberately small. The retry loop runs
// in the SAME process as the hot loop, so sizing it off the hot lane's
// MAX_WORKERS would make the process-wide in-flight cap 2×MaxWorkers — reached
// precisely during the incident that fills the retry lane. It also doubles as
// the recovery-herd damper: when a dependency comes back, the parked backlog
// drains at a bounded rate instead of stampeding it.
type ConsumerSettings struct {
	AckWait       time.Duration `env:"ACK_WAIT"        envDefault:"30s"`
	MaxWorkers    int           `env:"MAX_WORKERS"     envDefault:"10"`
	MaxDeliver    int           `env:"MAX_DELIVER"     envDefault:"3"`
	MaxWaiting    int           `env:"MAX_WAITING"     envDefault:"512"`
	MaxAckPending int           `env:"MAX_ACK_PENDING" envDefault:"4000"`
	BackOffSteps  int           `env:"BACKOFF_STEPS"  envDefault:"3"`
	BackOffFactor float64       `env:"BACKOFF_FACTOR" envDefault:"2"`
	BackOffMax    time.Duration `env:"BACKOFF_MAX"    envDefault:"8m"`
}

// streamSettings converts to stream.ConsumerSettings so ConsumerConfig can
// feed stream.DurableConsumerDefaults — the single place that derives
// BackOff from AckWait; a hand-rolled cc.BackOff is a blocking semgrep
// finding.
func (c ConsumerSettings) streamSettings() stream.ConsumerSettings {
	return stream.ConsumerSettings{
		AckWait:       c.AckWait,
		MaxDeliver:    c.MaxDeliver,
		MaxWaiting:    c.MaxWaiting,
		MaxAckPending: c.MaxAckPending,
		BackOffSteps:  c.BackOffSteps,
		BackOffFactor: c.BackOffFactor,
		BackOffMax:    c.BackOffMax,
	}
}

// DurableName is the retry consumer's durable for a service.
func DurableName(consumer string) string { return consumer + "-retry" }

// SlowBackoff returns the rungs the fast schedule left behind. The retry budget
// is relocated, not redefined: fast + slow equals the original schedule, so
// total patience per message is unchanged and only the occupancy moves — that
// additivity has to hold even at the edges, so fastSteps is clamped into
// range rather than special-cased, and the last rung is never counted twice.
//
// It never returns an empty slice — jsretry floors an empty schedule at a 1ms
// nak, which would burn the retry lane's MaxDeliver in milliseconds.
func SlowBackoff(fastSteps int, full []time.Duration) []time.Duration {
	if len(full) == 0 {
		return jsretryFallback()
	}
	if fastSteps < 0 {
		fastSteps = 0
	}
	if fastSteps > len(full)-1 {
		fastSteps = len(full) - 1
	}
	return full[fastSteps:]
}

// jsretryFallback is the schedule used when a caller supplies none.
func jsretryFallback() []time.Duration {
	return []time.Duration{2 * time.Minute, 10 * time.Minute}
}

// ConsumerConfig is the retry lane's durable consumer for one service, filtered
// to its own escalations. Built through stream.DurableConsumerDefaults so the
// derived BackOff and AckWait cannot disagree (a hardcoded cc.BackOff is a
// blocking semgrep finding). s is read-only, taken by pointer only because
// Settings is large enough to copy needlessly.
func ConsumerConfig(siteID, consumer string, s *Settings) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s.Consumer.streamSettings())
	cc.Durable = DurableName(consumer)
	cc.FilterSubjects = []string{subject.RetryConsumerWildcard(siteID, consumer)}
	return cc
}

// SkipMissingStream reports whether a retry-consumer bind failure is the benign
// "not provisioned yet" case, and logs it once when it is. Phase 1 ships dark:
// RETRY-{siteID} is ops/IaC-owned and production runs BOOTSTRAP_STREAMS=false,
// so a site that has not provisioned it yet would otherwise crash-loop every
// hot-path worker on a feature that is switched off.
//
// It cannot strand a message: if the stream does not exist, nothing can be
// parked on it. The tolerance is deliberately narrow — only ErrStreamNotFound,
// and only while the lane is disabled. With RETRY_LANE_ENABLED=true an operator
// has asked for the lane, and a missing stream must stay a loud startup failure.
func SkipMissingStream(ctx context.Context, streamName string, enabled bool, err error) bool {
	if err == nil || enabled || !errors.Is(err, jetstream.ErrStreamNotFound) {
		return false
	}
	slog.WarnContext(ctx, "retry lane disabled and its stream is not provisioned — running without the retry consumer",
		"stream", streamName, "retry_lane_enabled", enabled)
	return true
}
