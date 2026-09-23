package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Presence has no fallback — Valkey IS its store — but that is a reason for its
// RPCs to fail, not for the pod to refuse to start. A crashlooping replica
// cannot recover when Valkey returns; a running one does.
func TestValkeyStartupSurvivesOutage(t *testing.T) {
	cfg := valkeyutil.Config{Addrs: []string{testutil.DeadValkeyAddr(t)}}
	client, err := valkeyDial(context.Background(), cfg, testutil.ValkeyObservability())
	require.NoError(t, err, "a dead Valkey must not fail this service's dial")
	require.NotNil(t, client)
	t.Cleanup(func() { _ = client.Close() })
}
