package stream

import (
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ConsumerSettings holds the env-driven knobs for durable JetStream
// consumers. Embed in each service's Config with envPrefix:"CONSUMER_".
//
// Defaults are set on the struct tags so caarlos0/env supplies them when
// the env vars are unset. Operators tune per-service values via the
// service's deployment env (e.g. CONSUMER_MAX_ACK_PENDING).
type ConsumerSettings struct {
	AckWait       time.Duration `env:"ACK_WAIT"        envDefault:"30s"`
	MaxDeliver    int           `env:"MAX_DELIVER"     envDefault:"6"`
	MaxWaiting    int           `env:"MAX_WAITING"     envDefault:"512"`
	MaxAckPending int           `env:"MAX_ACK_PENDING" envDefault:"1000"`

	// BackOff schedule shape: entry i is AckWait * Factor^i capped at Max. Entry 0
	// is AckWait by construction — the server overwrites AckWait with BackOff[0]
	// (consumer.go:677-682), so the two can never disagree. Steps=0 disables it.
	BackOffSteps  int           `env:"BACKOFF_STEPS"  envDefault:"5"`
	BackOffFactor float64       `env:"BACKOFF_FACTOR" envDefault:"2"`
	BackOffMax    time.Duration `env:"BACKOFF_MAX"    envDefault:"8m"`
}

// DurableConsumerDefaults returns a ConsumerConfig populated from the
// supplied ConsumerSettings plus the project-wide architectural
// invariants (AckPolicy=Explicit, DeliverPolicy=All).
//
// Callers MUST set Durable. Callers MAY set FilterSubjects to scope the
// consumer to a subset of the stream's subjects.
//
// DeliverPolicy=All so a freshly-created durable (new deploy, new site,
// or a deleted-and-recreated durable) replays the stream from the start.
// search-sync-worker's MV rebuild and inbox-worker's federated catch-up
// both depend on this; for streams with no historical data (steady-state
// new sites) All and New are equivalent.
//
// DeliverPolicy is honored only at consumer creation. Updating an
// existing durable via js.CreateOrUpdateConsumer does not reset its
// cursor position.
//
// BackOff is derived from AckWait (see backOffSchedule) and governs redelivery
// only for messages un-acked past AckWait; a handler that Naks is spaced by
// pkg/jsretry instead.
func DurableConsumerDefaults(s ConsumerSettings) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       s.AckWait,
		MaxDeliver:    s.MaxDeliver,
		MaxWaiting:    s.MaxWaiting,
		MaxAckPending: s.MaxAckPending,
		BackOff:       s.backOffSchedule(),
	}
}

// backOffSchedule builds the redelivery schedule, nil when disabled (flat AckWait
// retry). Steps are clamped to MaxDeliver because the server rejects a longer
// schedule outright (consumer.go:807) — clamping beats failing at startup.
func (s ConsumerSettings) backOffSchedule() []time.Duration {
	steps := s.BackOffSteps
	if steps <= 0 || s.AckWait <= 0 {
		return nil
	}
	// Clamp only against a real cap: the server normalizes MaxDeliver 0 and < -1
	// to -1, meaning unlimited, before it checks the length (consumer.go:612-617).
	if s.MaxDeliver > 0 && steps > s.MaxDeliver {
		slog.Warn("consumer backoff steps exceed MaxDeliver — clamping",
			"backoffSteps", steps, "maxDeliver", s.MaxDeliver)
		steps = s.MaxDeliver
	}
	factor := s.BackOffFactor
	if factor < 1 {
		factor = 1
	}

	out := make([]time.Duration, steps)
	d := s.AckWait
	for i := range out {
		if s.BackOffMax > 0 && d > s.BackOffMax {
			d = s.BackOffMax
		}
		out[i] = d
		next := time.Duration(float64(d) * factor)
		if next < d {
			next = d // overflow guard
		}
		d = next
	}
	return out
}
