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

	// BackOff schedule shape. Entry i is AckWait * Factor^i capped at Max, so
	// entry 0 is AckWait by construction — nats-server overwrites AckWait with
	// BackOff[0] (server/consumer.go:677-682) and the two must never disagree.
	// Steps=0 disables the schedule entirely (flat AckWait retry).
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
// BackOff is derived from AckWait via the BackOff* knobs; see backOffSchedule.
// It governs redelivery only for messages that go un-acked past AckWait — a
// handler that Naks is spaced by pkg/jsretry instead, because a bare -NAK
// bypasses BackOff entirely (server/consumer.go:3308-3311).
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

// backOffSchedule builds the redelivery schedule: entry i is AckWait*Factor^i,
// capped at BackOffMax. Returns nil when disabled, leaving flat AckWait retry.
//
// Steps are clamped to MaxDeliver because nats-server rejects a schedule longer
// than MaxDeliver outright (server/consumer.go:807), and a clamp with a warning
// beats a consumer that fails to create at startup.
func (s ConsumerSettings) backOffSchedule() []time.Duration {
	steps := s.BackOffSteps
	if steps <= 0 || s.AckWait <= 0 {
		return nil
	}
	if s.MaxDeliver != -1 && steps > s.MaxDeliver {
		slog.Warn("consumer backoff steps exceed MaxDeliver — clamping",
			"backoffSteps", steps, "maxDeliver", s.MaxDeliver)
		steps = s.MaxDeliver
	}
	if steps <= 0 {
		return nil
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
