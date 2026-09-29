package natsutil_test

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// startTestNATSURL runs an embedded server and returns its client URL. The
// sibling helper in reply_test.go hands back a *nats.Conn; ConnectFailoverSite needs
// the URL so it can dial for itself.
func startTestNATSURL(t *testing.T) string {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second))
	t.Cleanup(ns.Shutdown)
	return ns.ClientURL()
}

func TestConnectFailoverSite_UnreachableReturnsNil(t *testing.T) {
	conn := natsutil.ConnectFailoverSite(context.Background(), "nats://127.0.0.1:1", "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	assert.Nil(t, conn, "an unreachable failover must degrade to nil, never block startup")
}

func TestConnectFailoverSite_EmptyURLReturnsNil(t *testing.T) {
	conn := natsutil.ConnectFailoverSite(context.Background(), "", "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	assert.Nil(t, conn, "an unconfigured failover is not an error")
}

func TestConnectFailoverSite_ReachableReturnsConn(t *testing.T) {
	url := startTestNATSURL(t)

	conn := natsutil.ConnectFailoverSite(context.Background(), url, "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)

	require.NotNil(t, conn)
	t.Cleanup(func() { conn.NatsConn().Close() })
	assert.True(t, conn.NatsConn().IsConnected())
}

// A missing creds file is a config error, not a reachable-failover question — it
// must still degrade rather than fail startup, since the failover lane is optional.
func TestConnectFailoverSite_BadCredsFileReturnsNil(t *testing.T) {
	url := startTestNATSURL(t)

	conn := natsutil.ConnectFailoverSite(context.Background(), url, "/nonexistent/creds.json",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)

	assert.Nil(t, conn)
}

func TestFailoverSiteConfig_SameSiteAsHome(t *testing.T) {
	tests := []struct {
		name string
		cfg  natsutil.FailoverSiteConfig
		home string
		want bool
	}{
		{
			name: "shared failover site hosted by this very site",
			cfg:  natsutil.FailoverSiteConfig{SiteID: "site-a", NatsURL: "nats://a:4222"},
			home: "site-a",
			want: true,
		},
		{
			name: "failover site elsewhere, the normal case",
			cfg:  natsutil.FailoverSiteConfig{SiteID: "failover-1", NatsURL: "nats://f:4222"},
			home: "site-a",
			want: false,
		},
		{
			name: "unconfigured is not a misconfiguration",
			cfg:  natsutil.FailoverSiteConfig{},
			home: "site-a",
			want: false,
		},
		{
			name: "url without site id cannot be compared",
			cfg:  natsutil.FailoverSiteConfig{NatsURL: "nats://f:4222"},
			home: "",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.cfg.SameSiteAsHome(tt.home))
		})
	}
}
