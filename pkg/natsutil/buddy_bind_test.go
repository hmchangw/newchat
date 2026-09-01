package natsutil_test

import (
	"context"
	"errors"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/natsutil"
)

func bindArgs() (noop.TracerProvider, propagation.TraceContext) {
	return noop.NewTracerProvider(), propagation.TraceContext{}
}

// The env block is copied into nine services; parsing it here is what stops the
// tags from drifting apart again.
func TestBuddyConfig_ParsesUnderTheBuddyPrefix(t *testing.T) {
	type cfg struct {
		Buddy natsutil.BuddyConfig `envPrefix:"BUDDY_"`
	}

	t.Setenv("BUDDY_SITE_ID", "site-b")
	t.Setenv("BUDDY_NATS_URL", "nats://buddy:4222")

	c, err := env.ParseAs[cfg]()
	require.NoError(t, err)
	assert.Equal(t, "site-b", c.Buddy.SiteID)
	assert.Equal(t, "nats://buddy:4222", c.Buddy.NatsURL)
	assert.True(t, c.Buddy.Enabled())
}

func TestBuddyConfig_Enabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  natsutil.BuddyConfig
		want bool
	}{
		{"both set", natsutil.BuddyConfig{SiteID: "site-b", NatsURL: "nats://b:4222"}, true},
		{"unset is a normal single-site deployment", natsutil.BuddyConfig{}, false},
		{"site without url cannot be dialled", natsutil.BuddyConfig{SiteID: "site-b"}, false},
		{"url without site has no cluster to assert placement against", natsutil.BuddyConfig{NatsURL: "nats://b:4222"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.cfg.Enabled())
		})
	}
}

func TestBindBuddy_UnconfiguredSkipsTheBind(t *testing.T) {
	tp, prop := bindArgs()
	called := false

	conn := natsutil.BindBuddy(context.Background(), natsutil.BuddyConfig{}, "", tp, prop, false,
		func(context.Context, *o11ynats.Conn, o11ynats.JetStream) error { called = true; return nil })

	assert.Nil(t, conn)
	assert.False(t, called, "no buddy configured means nothing to bind")
}

func TestBindBuddy_UnreachableBuddySkipsTheBind(t *testing.T) {
	tp, prop := bindArgs()
	called := false

	conn := natsutil.BindBuddy(context.Background(),
		natsutil.BuddyConfig{SiteID: "site-b", NatsURL: "nats://127.0.0.1:1"}, "", tp, prop, false,
		func(context.Context, *o11ynats.Conn, o11ynats.JetStream) error { called = true; return nil })

	assert.Nil(t, conn, "an unreachable buddy must never block startup")
	assert.False(t, called)
}

func TestBindBuddy_ReachableBuddyRunsTheBind(t *testing.T) {
	url := startTestNATSURL(t)
	tp, prop := bindArgs()

	var gotJS o11ynats.JetStream
	conn := natsutil.BindBuddy(context.Background(),
		natsutil.BuddyConfig{SiteID: "site-b", NatsURL: url}, "", tp, prop, false,
		func(_ context.Context, _ *o11ynats.Conn, js o11ynats.JetStream) error { gotJS = js; return nil })

	require.NotNil(t, conn)
	t.Cleanup(func() { conn.NatsConn().Close() })
	assert.NotNil(t, gotJS, "the bind callback receives the buddy's JetStream context")
}

// A bind failure is a degraded lane, not a failed startup — and the connection
// still has to come back so shutdown drains it instead of leaking it.
func TestBindBuddy_BindErrorStillReturnsTheConnToDrain(t *testing.T) {
	url := startTestNATSURL(t)
	tp, prop := bindArgs()

	conn := natsutil.BindBuddy(context.Background(),
		natsutil.BuddyConfig{SiteID: "site-b", NatsURL: url}, "", tp, prop, false,
		func(context.Context, *o11ynats.Conn, o11ynats.JetStream) error { return errors.New("stream missing") })

	require.NotNil(t, conn)
	t.Cleanup(func() { conn.NatsConn().Close() })
}

func TestDrainBuddy_NilConnIsANoOp(t *testing.T) {
	require.NoError(t, natsutil.DrainBuddy(nil)(context.Background()))
}

func TestDrainBuddy_DrainsAConnectedBuddy(t *testing.T) {
	url := startTestNATSURL(t)
	tp, prop := bindArgs()

	conn := natsutil.BindBuddy(context.Background(),
		natsutil.BuddyConfig{SiteID: "site-b", NatsURL: url}, "", tp, prop, false,
		func(context.Context, *o11ynats.Conn, o11ynats.JetStream) error { return nil })
	require.NotNil(t, conn)

	require.NoError(t, natsutil.DrainBuddy(conn)(context.Background()))
	assert.True(t, conn.NatsConn().IsClosed())
}

// Five services gate their lane on a service-specific condition. OnlyIf keeps
// that intent explicit instead of each one blanking the struct, which would
// silently zero any field added to BuddyConfig later.
func TestBuddyConfig_OnlyIf(t *testing.T) {
	cfg := natsutil.BuddyConfig{SiteID: "site-b", NatsURL: "nats://b:4222"}

	assert.Equal(t, cfg, cfg.OnlyIf(true), "a satisfied condition leaves the config untouched")
	assert.False(t, cfg.OnlyIf(false).Enabled(), "a failed condition disables the lane")
	assert.True(t, cfg.OnlyIf(true).Enabled())
}

// OnlyIf narrows, never widens: it cannot switch on a lane the deployment did
// not configure.
func TestBuddyConfig_OnlyIfCannotEnableAnUnconfiguredBuddy(t *testing.T) {
	assert.False(t, natsutil.BuddyConfig{}.OnlyIf(true).Enabled())
}
