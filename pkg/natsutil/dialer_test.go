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
func TestNewFailoverDialer_TakesTheSDKTuple(t *testing.T) {
	sdk, shutdown, err := obs.Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	cfg := natsutil.FailoverSiteConfig{SiteID: "site-b", NatsURL: "nats://b:4222"}
	d := natsutil.NewFailoverDialer(cfg, "site-a", "/creds", sdk)

	assert.Equal(t, cfg, d.Config)
	assert.Equal(t, "/creds", d.CredsFile)
	assert.Equal(t, sdk.TracerProvider(), d.TracerProvider)
	assert.Equal(t, sdk.Propagator, d.Propagator)
	assert.Equal(t, sdk.Toggles.Trace, d.TracingEnabled)
}

// The connect-then-JetStream block was copied into every main with two
// log-and-exit branches each. One call, one error.
func TestFailoverDialer_ConnectHomeJS(t *testing.T) {
	t.Run("reachable home: connected, with JetStream", func(t *testing.T) {
		nc, js, err := dialer(natsutil.FailoverSiteConfig{}).ConnectHomeJS(context.Background(), startTestNATSURL(t), nil)
		require.NoError(t, err)
		t.Cleanup(func() { nc.NatsConn().Close() })
		assert.Equal(t, nats.CONNECTED, nc.NatsConn().Status())
		assert.NotNil(t, js)
	})

	t.Run("home down with a failover: dialing in the background, JetStream ready", func(t *testing.T) {
		url, _ := reservePort(t)
		nc, js, err := standbyEnabled().ConnectHomeJS(context.Background(), url, nil)
		require.NoError(t, err)
		t.Cleanup(func() { nc.NatsConn().Close() })
		assert.Equal(t, nats.RECONNECTING, nc.NatsConn().Status())
		assert.NotNil(t, js, "the JetStream context needs no live server to build")
	})

	t.Run("home down without a failover: fails fast", func(t *testing.T) {
		url, _ := reservePort(t)
		_, _, err := dialer(natsutil.FailoverSiteConfig{}).ConnectHomeJS(context.Background(), url, nil)
		require.Error(t, err)
	})
}

// A shared failover site is one fleet-wide value, so the site that HOSTS it
// receives its own id in FAILOVER_SITE_ID along with every other site. Left
// alone that binds a "standby" lane on the very cluster the lane exists to
// outlive, and nothing downstream complains — CheckPlacement compares the
// stream's cluster against this same id, so the assertion passes and the lane
// reads as configured while being a no-op. The dialer is the one place every
// service builds its lane from, so the config dies here.
func TestNewFailoverDialer_DisablesAFailoverSiteEqualToHome(t *testing.T) {
	sdk, shutdown, err := obs.Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	d := natsutil.NewFailoverDialer(
		natsutil.FailoverSiteConfig{SiteID: "failover-1", NatsURL: "nats://f:4222"}, "failover-1", "/creds", sdk)

	assert.False(t, d.Config.Enabled(), "a failover site equal to the home site is not a lane")
	assert.Equal(t, natsutil.FailoverSiteConfig{}, d.Config)
}

func TestNewFailoverDialer_KeepsAFailoverSiteHostedElsewhere(t *testing.T) {
	sdk, shutdown, err := obs.Init(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	cfg := natsutil.FailoverSiteConfig{SiteID: "failover-1", NatsURL: "nats://f:4222"}
	d := natsutil.NewFailoverDialer(cfg, "site-a", "/creds", sdk)

	assert.True(t, d.Config.Enabled())
	assert.Equal(t, cfg, d.Config)
}
