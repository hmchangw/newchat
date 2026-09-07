// This file is deliberately untagged, for the same reason as routesgolden.go:
// the tests that use it are ordinary unit tests, so it must stay
// outside the //go:build integration set that pulls in testcontainers.
package testutil

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"
)

// EmbeddedNATS runs a NATS server in-process and returns a connection to it, so
// a request/reply unit test needs no Docker.
//
// The registration tests cannot avoid a real connection: registering a route
// subscribes to it (natsrouter.addRoute → QueueSubscribe), so there is no way
// to build the table Routes() returns without one. The outbound client tests
// use it to stand up a fake callee.
//
// It lives here because ten packages carried a near-identical copy, which made
// any change to the setup — a connect option, an o11y wiring change, a
// ReadyForConnections bump for a slow runner — a ten-file edit.
func EmbeddedNATS(t *testing.T) *o11ynats.Conn {
	t.Helper()

	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	if err != nil {
		t.Fatalf("start in-process nats server: %v", err)
	}
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("in-process nats server did not become ready within 5s")
	}
	t.Cleanup(ns.Shutdown)

	nc, err := o11ynats.Connect(context.Background(), ns.ClientURL(),
		noop.NewTracerProvider(), propagation.TraceContext{})
	if err != nil {
		t.Fatalf("connect to in-process nats server: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}
