package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// A Valkey outage must never stop this service starting: it is a shared
// datastore, so gating startup on it crashloops every replica at once. Runs
// main's own valkeyDial, not an approximation — a test that dials differently
// from the service proves nothing about the service.
func TestValkeyStartupSurvivesOutage(t *testing.T) {
	cfg := valkeyutil.Config{Addrs: []string{testutil.DeadValkeyAddr(t)}}
	client := valkeyDial(context.Background(), cfg, testutil.ValkeyObservability())
	require.NotNil(t, client, "a dead Valkey must not silently disable this service's tier")
	t.Cleanup(func() { _ = client.Close() })
}
