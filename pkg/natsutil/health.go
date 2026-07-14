package natsutil

import (
	"context"
	"fmt"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"
	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/health"
)

// connStatus is the subset of *nats.Conn used to probe NATS readiness.
type connStatus interface {
	Status() nats.Status
}

// HealthCheck returns a readiness check for the NATS connection. It reports
// healthy while CONNECTED or actively RECONNECTING — the client buffers and
// retries during a brief blip, so tolerating RECONNECTING keeps readiness from
// flapping on every reconnect. A sustained DISCONNECTED/CLOSED state reports
// not-ready. This is a per-pod signal (this pod's own connection), which is what
// readiness should reflect — unlike a shared datastore.
func HealthCheck(nc *otelnats.Conn) health.Check {
	return healthCheckFor(nc.NatsConn())
}

func healthCheckFor(s connStatus) health.Check {
	return health.Check{Name: "nats", Probe: func(context.Context) error {
		switch st := s.Status(); st {
		case nats.CONNECTED, nats.RECONNECTING:
			return nil
		default:
			return fmt.Errorf("nats connection %s", st)
		}
	}}
}
