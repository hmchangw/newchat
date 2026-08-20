package stream

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ConsumerSettings holds the env-driven knobs for durable JetStream
// consumers. Embed in each service's Config with envPrefix:"CONSUMER_".
//
// Defaults are set on the struct tags so caarlos0/env supplies them when
// the env vars are unset. Operators tune per-service values via the
// service's deployment env (e.g. CONSUMER_MAX_ACK_PENDING).
//
// MaxDeliver is sized by the outage it must ride out, not by an attempt count:
// paired with jsretry.DefaultBackoff (tail 2m) the retry window is
// 36s + (MaxDeliver-4) x 2m, so 20 deliveries park a message for ~32m before
// JetStream drops it — a node loss plus a rolling cluster restart, with
// headroom. Buy the window with attempts rather than a longer backoff tail:
// the tail is also the lag between the dependency recovering and the next
// retry. Two ceilings bound any further increase — the stream's MaxAge (a
// window past retention retries against a deleted message) and MaxAckPending
// (a Nak'd-with-delay message holds its slot, so a consumer stalls once the
// budget fills; backpressure, not loss).
type ConsumerSettings struct {
	AckWait       time.Duration `env:"ACK_WAIT"        envDefault:"30s"`
	MaxDeliver    int           `env:"MAX_DELIVER"     envDefault:"20"`
	MaxWaiting    int           `env:"MAX_WAITING"     envDefault:"512"`
	MaxAckPending int           `env:"MAX_ACK_PENDING" envDefault:"1000"`
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
func DurableConsumerDefaults(s ConsumerSettings) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       s.AckWait,
		MaxDeliver:    s.MaxDeliver,
		MaxWaiting:    s.MaxWaiting,
		MaxAckPending: s.MaxAckPending,
	}
}
