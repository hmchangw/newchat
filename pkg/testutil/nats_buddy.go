//go:build integration

package testutil

import (
	"testing"
)

// natsBuddy is a SECOND JetStream server, independent of the primary — same
// startup path, own lifecycle.
var natsBuddy = &natsInstance{name: "buddy"}

func ensureNATSBuddy() (string, error) { return natsBuddy.ensure() }

// NATSPair returns the URLs of two process-shared JetStream servers, for tests
// that exercise a service holding both a home and a buddy connection.
//
// FIDELITY LIMIT: these are two INDEPENDENT servers, not a supercluster. A
// publish on one is not routed to the other. The pair proves consumer binding
// and per-lane handling across two connections; it CANNOT prove gateway interest
// routing, stream placement, or that a down cluster yields no-responders. Those
// require a real supercluster in staging.
func NATSPair(t *testing.T) (home, buddy string) {
	t.Helper()
	h := NATS(t)
	b, err := ensureNATSBuddy()
	if err != nil {
		t.Fatalf("testutil.NATSPair: %v", err)
	}
	return h, b
}

// EnsureNATSBuddy starts the shared buddy server if not already started.
// No-t variant intended for TestMain pre-warming.
func EnsureNATSBuddy() error { _, err := ensureNATSBuddy(); return err }

// TerminateNATSBuddy stops the shared buddy server. Best-effort, idempotent.
func TerminateNATSBuddy() { natsBuddy.terminate() }
