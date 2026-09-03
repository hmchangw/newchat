package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/poolartifact"
)

func validTestConfig() config {
	return config{
		// Cleartext with the explicit opt-in: these cases are about the other
		// knobs, and TestValidateConfig_RequiresEncryptedTransportUnlessOptedIn
		// owns the transport rule.
		NATSWSURL: "ws://x", AllowInsecureWS: true,
		AuthURL: "http://x", PoolFile: "p", SiteID: "s",
		RampRate: 50, JWTMode: jwtModeProactive,
		SubPendingMsgs: 512, SubPendingBytes: 1 << 17,
		ReconnectBufBytes: 1 << 16, PingInterval: 2 * time.Minute,
	}
}

func TestValidateConfig(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*config)
		wantErr string
	}{
		{"valid", func(*config) {}, ""},
		{"valid expiry mode", func(c *config) { c.JWTMode = jwtModeExpiry }, ""},
		{"unknown jwt mode", func(c *config) { c.JWTMode = "sometimes" }, "JWT_MODE"},
		{"zero pending msgs would mean unlimited", func(c *config) { c.SubPendingMsgs = 0 }, "PENDING"},
		{"negative pending bytes would mean unlimited", func(c *config) { c.SubPendingBytes = -1 }, "PENDING"},
		{"zero ramp rate", func(c *config) { c.RampRate = 0 }, "RAMP_RATE"},
		{"negative churn rate", func(c *config) { c.ChurnRate = -1 }, "CHURN_RATE"},
		{"zero reconnect buffer", func(c *config) { c.ReconnectBufBytes = 0 }, "RECONNECT_BUF"},
		{"zero ping interval", func(c *config) { c.PingInterval = 0 }, "PING_INTERVAL"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			tt.mutate(&cfg)
			err := validateConfig(&cfg)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

// requestConn is a minimal simConn whose Request is scripted — for
// natsLister's error paths.
type requestConn struct {
	fakeConn
	reply []byte
	err   error
}

func (r *requestConn) Request(context.Context, string, []byte) (*nats.Msg, error) {
	if r.err != nil {
		return nil, r.err
	}
	return &nats.Msg{Data: r.reply}, nil
}

func TestNatsLister_ErrorPaths(t *testing.T) {
	t.Run("transport error", func(t *testing.T) {
		l := &natsLister{conn: &requestConn{err: errors.New("no responders")}, subject: "s", timeout: time.Second}
		_, err := l.List(context.Background(), subListRequest{Type: "rooms"})
		assert.ErrorContains(t, err, "subscription.list request")
	})
	t.Run("errcode rejection envelope", func(t *testing.T) {
		l := &natsLister{conn: &requestConn{reply: []byte(`{"code":"bad_request","error":"unknown subscription type"}`)}, subject: "s", timeout: time.Second}
		_, err := l.List(context.Background(), subListRequest{Type: "nope"})
		assert.ErrorContains(t, err, "rejected")
	})
	t.Run("malformed reply body", func(t *testing.T) {
		l := &natsLister{conn: &requestConn{reply: []byte("{nope")}, subject: "s", timeout: time.Second}
		_, err := l.List(context.Background(), subListRequest{Type: "rooms"})
		assert.ErrorContains(t, err, "decode")
	})
	t.Run("happy path", func(t *testing.T) {
		l := &natsLister{conn: &requestConn{reply: []byte(`{"subscriptions":[],"hasMore":false}`)}, subject: "s", timeout: time.Second}
		page, err := l.List(context.Background(), subListRequest{Type: "rooms"})
		require.NoError(t, err)
		assert.False(t, page.HasMore)
	})
}

func TestPrintSummary_LogsWithoutError(t *testing.T) {
	m := newMetrics()
	m.DecodeFailures.Inc()
	s, err := printSummary(m, "run-9", "digest", 10)
	require.NoError(t, err)
	assert.True(t, s.Degraded, "a decode failure marks the window degraded")
}

// setupUnreachableRun points a run at dead endpoints: every client fails
// auth, so no connection is ever held.
func setupUnreachableRun(t *testing.T) {
	t.Helper()
	pool := filepath.Join(t.TempDir(), "pool.json")
	require.NoError(t, poolartifact.Write(pool, &poolartifact.Artifact{
		RunID: "test-run", SiteID: "site-t", ConfigDigest: "d",
		Accounts: []string{"user-a", "user-b"},
	}))
	t.Setenv("CLIENTSIM_NATS_WS_URL", "ws://127.0.0.1:1")
	t.Setenv("CLIENTSIM_ALLOW_INSECURE_WS", "true")
	t.Setenv("CLIENTSIM_AUTH_URL", "http://127.0.0.1:1")
	t.Setenv("CLIENTSIM_POOL_FILE", pool)
	t.Setenv("CLIENTSIM_SITE_ID", "site-t")
	t.Setenv("CLIENTSIM_METRICS_ADDR", "127.0.0.1:0")
	t.Setenv("CLIENTSIM_RAMP_RATE", "1000")
}

// A soak where nothing ever connected must not report success — that run
// measured nothing, however cleanly it shut down.
func TestRun_EndToEnd_FailsWhenFleetNeverCameUp(t *testing.T) {
	setupUnreachableRun(t)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := run(ctx)
	require.Error(t, err)
	assert.ErrorContains(t, err, "readiness floor")
}

// With the gate switched off the same run drains cleanly and exits 0.
func TestRun_EndToEnd_StartsAndShutsDown(t *testing.T) {
	setupUnreachableRun(t)
	t.Setenv("CLIENTSIM_MIN_READY_RATIO", "0")
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	assert.NoError(t, run(ctx), "a cancelled run with failing clients still shuts down cleanly")
}

func TestRun_FailsFastOnBadPoolFile(t *testing.T) {
	t.Setenv("CLIENTSIM_NATS_WS_URL", "ws://127.0.0.1:1")
	t.Setenv("CLIENTSIM_ALLOW_INSECURE_WS", "true")
	t.Setenv("CLIENTSIM_AUTH_URL", "http://127.0.0.1:1")
	t.Setenv("CLIENTSIM_POOL_FILE", filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("CLIENTSIM_SITE_ID", "site-t")
	err := run(context.Background())
	assert.ErrorContains(t, err, "pool artifact")
}

func TestRun_FailsFastOnInvalidMode(t *testing.T) {
	t.Setenv("CLIENTSIM_NATS_WS_URL", "ws://127.0.0.1:1")
	t.Setenv("CLIENTSIM_ALLOW_INSECURE_WS", "true")
	t.Setenv("CLIENTSIM_AUTH_URL", "http://127.0.0.1:1")
	t.Setenv("CLIENTSIM_POOL_FILE", "x")
	t.Setenv("CLIENTSIM_SITE_ID", "site-t")
	t.Setenv("CLIENTSIM_JWT_MODE", "yolo")
	err := run(context.Background())
	assert.ErrorContains(t, err, "JWT_MODE")
}

func TestValidateConfig_ReadyRatioBounds(t *testing.T) {
	cases := []struct {
		name    string
		ratio   float64
		wantErr bool
	}{
		{"disabled", 0, false},
		{"default", 0.95, false},
		{"full fleet required", 1, false},
		{"negative", -0.1, true},
		{"over one", 1.5, true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config{NATSWSURL: "wss://x", JWTMode: jwtModeProactive,
				MinReadyRatio:  tt.ratio,
				SubPendingMsgs: 1, SubPendingBytes: 1, ShardCount: 1,
				RampRate: 1, ReconnectBufBytes: 1, PingInterval: time.Minute}
			err := validateConfig(cfg)
			if tt.wantErr {
				assert.ErrorContains(t, err, "MIN_READY_RATIO")
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestValidateConfig_RequiresEncryptedTransportUnlessOptedIn(t *testing.T) {
	base := func() *config {
		return &config{
			NATSWSURL: "wss://nats.example:443", JWTMode: jwtModeProactive,
			MinReadyRatio: 0.95, SubPendingMsgs: 512, SubPendingBytes: 1 << 17,
			RampRate: 50, ChurnRate: 0, ReconnectBufBytes: 1 << 16,
			PingInterval: 2 * time.Minute,
		}
	}
	tests := []struct {
		name    string
		url     string
		allow   bool
		wantErr string
	}{
		{"wss is always fine", "wss://nats.example:443", false, ""},
		{"ws without the opt-in is rejected", "ws://127.0.0.1:8080", false, "CLIENTSIM_ALLOW_INSECURE_WS"},
		{"ws with the opt-in is allowed", "ws://127.0.0.1:8080", true, ""},
		// Rejected for BEING the wrong protocol, not merely for lacking the
		// opt-in — TestValidateConfig_RejectsNonWebSocketSchemesEvenWithTheOptIn
		// owns the case where the opt-in is present.
		{"a non-websocket scheme is rejected too", "nats://127.0.0.1:4222", false, "must be a WebSocket URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base()
			cfg.NATSWSURL = tt.url
			cfg.AllowInsecureWS = tt.allow
			err := validateConfig(cfg)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// The tool's entire output is its metrics endpoint. A Serve failure mid-run
// means hours of fleet time nobody can read, so it has to reach run() rather
// than being logged past.
func TestServeMetrics_ReportsAServeFailure(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{Handler: http.NewServeMux(), ReadHeaderTimeout: time.Second}
	errCh := serveMetrics(srv, lis)

	require.NoError(t, lis.Close()) // pulled out from under Serve
	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.ErrorContains(t, err, "metrics server")
	case <-time.After(3 * time.Second):
		t.Fatal("a dead metrics server never reported")
	}
}

// A deliberate shutdown is not a failure: the channel closes with no error.
func TestServeMetrics_ShutdownIsNotAFailure(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{Handler: http.NewServeMux(), ReadHeaderTimeout: time.Second}
	errCh := serveMetrics(srv, lis)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))
	select {
	case err, ok := <-errCh:
		assert.False(t, ok, "a clean shutdown must not report an error, got %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("serveMetrics never finished after Shutdown")
	}
}

// The tool's premise is that it connects the way the real client does — over
// nats.ws. A nats:// URL would connect over plain TCP instead, quietly
// measuring a transport nobody ships. The insecure opt-in exists to allow
// cleartext WebSocket on a throwaway stack, not to allow a different protocol.
func TestValidateConfig_RejectsNonWebSocketSchemesEvenWithTheOptIn(t *testing.T) {
	base := func() *config {
		return &config{
			JWTMode: jwtModeProactive, MinReadyRatio: 0.95,
			SubPendingMsgs: 512, SubPendingBytes: 1 << 17,
			RampRate: 50, ReconnectBufBytes: 1 << 16, PingInterval: 2 * time.Minute,
		}
	}
	tests := []struct {
		name    string
		url     string
		allow   bool
		wantErr string
	}{
		{"wss is always fine", "wss://nats.example:443", false, ""},
		{"ws needs the opt-in", "ws://127.0.0.1:8080", false, "CLIENTSIM_ALLOW_INSECURE_WS"},
		{"ws with the opt-in", "ws://127.0.0.1:8080", true, ""},
		{"nats:// is not a WebSocket URL, opt-in or not", "nats://127.0.0.1:4222", true, "WebSocket"},
		{"tls:// is not either", "tls://127.0.0.1:4222", true, "WebSocket"},
		{"a bare host is not a URL at all", "127.0.0.1:4222", true, "WebSocket"},
		// A scheme with no authority parses cleanly and only fails later,
		// inside nats.Connect's own handshake, where the message is far less
		// obviously a config mistake.
		{"scheme with no authority", "wss:", true, "host"},
		{"empty authority with a path", "wss:///path", true, "host"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base()
			cfg.NATSWSURL, cfg.AllowInsecureWS = tt.url, tt.allow
			err := validateConfig(cfg)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
