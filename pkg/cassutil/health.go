package cassutil

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/health"
)

// healthQuery is served by the coordinator itself, so it exercises the
// connection pool without touching application data or a user keyspace.
const healthQuery = `SELECT key FROM system.local`

// prober runs one readiness query. The seam keeps the check's error mapping
// unit-testable without a live cluster.
type prober func(ctx context.Context) error

// HealthCheck returns a readiness check for the Cassandra session. Unlike the
// NATS check this probes a shared datastore, so a cluster-wide outage marks
// every pod not-ready at once — intended for a hard dependency: readiness (never
// liveness) sheds traffic without restarting pods. On an instrumented session
// each probe emits a query span, so keep the readiness interval coarse.
func HealthCheck(session *gocql.Session) health.Check {
	return healthCheckFor(func(ctx context.Context) error {
		return session.Query(healthQuery).WithContext(ctx).Consistency(gocql.One).Exec()
	})
}

func healthCheckFor(probe prober) health.Check {
	return health.Check{Name: "cassandra", Probe: func(ctx context.Context) error {
		if err := probe(ctx); err != nil {
			return fmt.Errorf("cassandra probe: %w", err)
		}
		return nil
	}}
}
