//go:build integration

package testutil_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestNATSPair_ReturnsTwoIndependentServers(t *testing.T) {
	home, buddy := testutil.NATSPair(t)
	assert.NotEqual(t, home, buddy, "home and buddy must be distinct servers")

	hc, err := nats.Connect(home)
	require.NoError(t, err)
	t.Cleanup(hc.Close)

	bc, err := nats.Connect(buddy)
	require.NoError(t, err)
	t.Cleanup(bc.Close)

	require.NoError(t, hc.Flush())
	require.NoError(t, bc.Flush())

	// Independent, not superclustered: a subject published on one is not
	// delivered on the other. This is the harness's documented fidelity limit,
	// asserted so nobody mistakes it for a supercluster and writes a routing
	// test that silently proves nothing.
	sub, err := bc.SubscribeSync("probe.subject")
	require.NoError(t, err)
	require.NoError(t, bc.Flush())

	require.NoError(t, hc.Publish("probe.subject", []byte("x")))
	require.NoError(t, hc.Flush())

	_, err = sub.NextMsg(500 * time.Millisecond)
	assert.ErrorIs(t, err, nats.ErrTimeout, "servers must not be linked")
}
