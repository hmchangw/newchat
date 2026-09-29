//go:build integration

package testutil

import (
	"testing"
)

// natsFailover is a SECOND JetStream server, independent of the primary — same
// startup path, own lifecycle.
var natsFailover = &natsInstance{name: "failover"}

func ensureNATSFailover() (string, error) { return natsFailover.ensure() }

// NATSPair returns the URLs of two process-shared JetStream servers, for tests
// that exercise a service holding both a home and a failover connection.
//
// FIDELITY LIMIT: these are two INDEPENDENT servers, not a supercluster. A
// publish on one is not routed to the other. The pair proves consumer binding
// and per-lane handling across two connections; it CANNOT prove gateway interest
// routing, stream placement, or that a down cluster yields no-responders. Those
// require a real supercluster in staging.
func NATSPair(t *testing.T) (home, failover string) {
	t.Helper()
	h := NATS(t)
	b, err := ensureNATSFailover()
	if err != nil {
		t.Fatalf("testutil.NATSPair: %v", err)
	}
	return h, b
}

// EnsureNATSFailover starts the shared failover server if not already started.
// No-t variant intended for TestMain pre-warming.
func EnsureNATSFailover() error { _, err := ensureNATSFailover(); return err }

// TerminateNATSFailover stops the shared failover server. Best-effort, idempotent.
func TerminateNATSFailover() { natsFailover.terminate() }
