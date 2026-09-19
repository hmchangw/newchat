package natsutil_test

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
)

// Every service builds its dialer from the same three SDK values. One
// constructor, so the tuple cannot be copied wrong — or, as happened once, a
// service left on a fail-fast dial while its dialer said otherwise.
func TestNewBuddyDialer_TakesTheSDKTuple(t *testing.T) {
	sdk, shutdown, err := obs.Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	cfg := natsutil.BuddyConfig{SiteID: "site-b", NatsURL: "nats://b:4222"}
	d := natsutil.NewBuddyDialer(cfg, "/creds", sdk)

	assert.Equal(t, cfg, d.Config)
	assert.Equal(t, "/creds", d.CredsFile)
	assert.Equal(t, sdk.TracerProvider(), d.TracerProvider)
	assert.Equal(t, sdk.Propagator, d.Propagator)
	assert.Equal(t, sdk.Toggles.Trace, d.TracingEnabled)
}

// The connect-then-JetStream block was copied into every main with two
// log-and-exit branches each. One call, one error.
func TestBuddyDialer_ConnectHomeJS(t *testing.T) {
	t.Run("reachable home: connected, with JetStream", func(t *testing.T) {
		nc, js, err := dialer(natsutil.BuddyConfig{}).ConnectHomeJS(context.Background(), startTestNATSURL(t), nil)
		require.NoError(t, err)
		t.Cleanup(func() { nc.NatsConn().Close() })
		assert.Equal(t, nats.CONNECTED, nc.NatsConn().Status())
		assert.NotNil(t, js)
	})

	t.Run("home down with a buddy: dialing in the background, JetStream ready", func(t *testing.T) {
		url, _ := reservePort(t)
		nc, js, err := buddyEnabled().ConnectHomeJS(context.Background(), url, nil)
		require.NoError(t, err)
		t.Cleanup(func() { nc.NatsConn().Close() })
		assert.Equal(t, nats.RECONNECTING, nc.NatsConn().Status())
		assert.NotNil(t, js, "the JetStream context needs no live server to build")
	})

	t.Run("home down without a buddy: fails fast", func(t *testing.T) {
		url, _ := reservePort(t)
		_, _, err := dialer(natsutil.BuddyConfig{}).ConnectHomeJS(context.Background(), url, nil)
		require.Error(t, err)
	})
}
