package retrylane

import (
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

	// FastSteps is how many deliveries stay on the hot lane. 3 leaves
	// {1s,5s,30s} = 36s of ack-pending occupancy instead of 756s.
	FastSteps int `env:"LANE_FAST_STEPS" envDefault:"3"`

	// Consumer tunes the retry lane's own durable, envPrefix RETRY_CONSUMER_.
	// MaxAckPending defaults high: this lane deliberately holds the long waits,
	// sized for ~5 escalations/s against ~720s of slow-rung occupancy.
	Consumer stream.ConsumerSettings `envPrefix:"CONSUMER_"`
}

// DurableName is the retry consumer's durable for a service.
func DurableName(consumer string) string { return consumer + "-retry" }

// DefaultConsumerSettings are the retry lane's consumer defaults, which differ
// from a hot lane's. This lane deliberately parks the long waits, so its
// ack-pending budget is sized for ~5 escalations/s against ~720s of slow-rung
// occupancy (≈3,600 in flight), and MaxDeliver counts retry-lane attempts only.
//
// Services apply this when their parsed Settings carry no explicit override —
// struct-tag envDefaults cannot express a different default for an embedded
// stream.ConsumerSettings than the hot lane's.
func DefaultConsumerSettings() stream.ConsumerSettings {
	return stream.ConsumerSettings{
		AckWait:       30 * time.Second,
		MaxDeliver:    3,
		MaxWaiting:    512,
		MaxAckPending: 4000,
		BackOffSteps:  3,
		BackOffFactor: 2,
		BackOffMax:    8 * time.Minute,
	}
}

// ApplyDefaults fills in DefaultConsumerSettings when the operator set no
// retry-consumer env vars, leaving any explicit value untouched. Call it once
// after env parsing.
func (s Settings) ApplyDefaults() Settings {
	if s.Consumer.MaxAckPending == 0 {
		s.Consumer = DefaultConsumerSettings()
	}
	return s
}

// SlowBackoff returns the rungs the fast schedule left behind. The retry budget
// is relocated, not redefined: fast + slow equals the original schedule, so
// total patience per message is unchanged and only the occupancy moves.
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
	if fastSteps >= len(full) {
		// Everything is a fast rung; reuse the last entry so the lane still paces.
		return []time.Duration{full[len(full)-1]}
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
// blocking semgrep finding).
func ConsumerConfig(siteID, consumer string, s Settings) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s.Consumer)
	cc.Durable = DurableName(consumer)
	cc.FilterSubjects = []string{subject.RetryConsumerWildcard(siteID, consumer)}
	return cc
}
