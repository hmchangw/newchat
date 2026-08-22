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
// maxBackOffSteps bounds an operator-supplied BackOffSteps. An unlimited
// MaxDeliver skips the clamp below, so nothing else caps the allocation.
const maxBackOffSteps = 32

func (s ConsumerSettings) backOffSchedule() []time.Duration {
	steps := s.BackOffSteps
	if steps > maxBackOffSteps {
		slog.Warn("consumer backoff steps above the bound — clamping",
			"backoffSteps", steps, "bound", maxBackOffSteps)
		steps = maxBackOffSteps
	}
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
	// A cap below AckWait would truncate entry 0, and the server then adopts that
	// shorter value as AckWait — exactly what deriving the schedule prevents.
	backOffMax := s.BackOffMax
	if backOffMax > 0 && backOffMax < s.AckWait {
		slog.Warn("consumer backoff max is below AckWait — raising it to AckWait",
			"backoffMax", backOffMax, "ackWait", s.AckWait)
		backOffMax = s.AckWait
	}

	out := make([]time.Duration, steps)
	d := s.AckWait
	for i := range out {
		if backOffMax > 0 && d > backOffMax {
			d = backOffMax
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

// DefaultMaxDeliver mirrors the MaxDeliver struct tag. A consumer left at this
// value drops a message after roughly 12.6 minutes of jsretry backoff, which is
// the right answer for work that is cheap to lose and wrong for work that must
// survive a dependency outage.
const DefaultMaxDeliver = 6

// OutageRetryMaxDeliver is the delivery budget for consumers that must ride out
// a dependency outage rather than drop the message.
//
// The window is set by jsretry.DefaultBackoff, whose last entry repeats. n
// deliveries means n-1 waits: the first four are 1s + 5s + 30s + 2m = 156s and
// the remaining n-5 are 10m each, so the window is 156s + 600s*(n-5). 12 gives
// about 72 minutes, comfortably past the one-hour MongoDB outage the
// outage-survival work targets.
//
// TestOutageRetryBudget_ExceedsOneHour walks the real schedule rather than this
// formula, so a change to DefaultBackoff re-checks the budget instead of
// quietly invalidating the arithmetic above — which is exactly what happened
// when the tail grew from 2m to 10m.
const OutageRetryMaxDeliver = 12

// WithOutageRetryBudget raises s.MaxDeliver to OutageRetryMaxDeliver when it is
// still at the package default, and leaves any other value alone.
//
// It exists because the default lives on a struct tag shared by every consumer
// in the repo, and only some of them should ride out an outage — a service that
// needs the longer budget has to opt in somewhere, and a deployment env var is
// not that place: per-service docker-compose files are local-dev only, so a
// budget set there is absent in production, which is exactly where it matters.
//
// An operator who deliberately sets MAX_DELIVER to the default value is
// indistinguishable from one who set nothing, and gets the outage budget. That
// ambiguity is deliberate: the failure it causes (a poison message retried for
// the outage window before dropping) is far cheaper than the one it avoids (a
// user's message accepted, then silently dropped mid-outage).
func WithOutageRetryBudget(s ConsumerSettings) ConsumerSettings {
	if s.MaxDeliver == DefaultMaxDeliver {
		s.MaxDeliver = OutageRetryMaxDeliver
	}
	return s
}
