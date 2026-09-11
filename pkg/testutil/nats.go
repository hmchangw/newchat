//go:build integration

package testutil

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	natsmod "github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/hmchangw/chat/pkg/testutil/testimages"
)

// natsInstance is one process-shared JetStream server. Two exist: the primary
// and the buddy that NATSPair hands out, so a service holding a home and a
// buddy connection can be exercised end to end. They share this type so the
// image, flags, wait strategy and teardown cannot drift apart — a buddy that
// quietly kept the old startup behaviour would fail in ways that look like the
// failover code's fault.
type natsInstance struct {
	name      string // used in error text only
	once      sync.Once
	container testcontainers.Container
	stopProc  func()
	url       string
	initErr   error
}

// ensure starts the server once. It prefers a `nats-server -js` subprocess when
// the binary is on PATH and falls back to a testcontainers NATS container.
//
// JetStream is enabled unconditionally so consumers that publish/consume
// through streams (search-sync-worker, inbox-worker, etc.) Just Work. Consumers
// that only use core NATS request/reply pay nothing extra — JS is dormant until
// used.
func (n *natsInstance) ensure() (string, error) {
	n.once.Do(func() {
		if u, stop, err := startNATSBinary(); err == nil {
			n.url = u
			n.stopProc = stop
			return
		}
		ctx := context.Background()
		c, err := natsmod.Run(ctx, testimages.NATS,
			testcontainers.WithCmdArgs("--jetstream"),
			testcontainers.WithWaitStrategy(wait.ForLog("Server is ready").WithStartupTimeout(60*time.Second)),
		)
		if err != nil {
			n.initErr = fmt.Errorf("start %s nats: %w", n.name, err)
			return
		}
		url, err := c.ConnectionString(ctx)
		if err != nil {
			_ = c.Terminate(ctx)
			n.initErr = fmt.Errorf("get %s nats url: %w", n.name, err)
			return
		}
		n.container = c
		n.url = url
	})
	return n.url, n.initErr
}

// terminate stops the server (subprocess or container). Best-effort, idempotent.
func (n *natsInstance) terminate() {
	if n.stopProc != nil {
		n.stopProc()
		n.stopProc = nil
		return
	}
	if n.container == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := n.container.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "terminate %s nats: %v\n", n.name, err)
	}
	n.container = nil
}

var natsPrimary = &natsInstance{name: "shared"}

func ensureNATS() (string, error) { return natsPrimary.ensure() }

// NATS returns the URL of a process-shared NATS container with JetStream
// enabled.
func NATS(t *testing.T) string {
	t.Helper()
	u, err := ensureNATS()
	if err != nil {
		t.Fatalf("testutil.NATS: %v", err)
	}
	return u
}

// EnsureNATS starts the shared NATS container if not already started.
// No-t variant intended for TestMain pre-warming.
func EnsureNATS() error { _, err := ensureNATS(); return err }

// TerminateNATS stops the shared NATS instance (subprocess or
// container). Best-effort, idempotent.
func TerminateNATS() { natsPrimary.terminate() }
