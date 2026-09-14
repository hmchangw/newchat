package valkeyutil

import (
	"context"
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// stubObs is a minimal Observability for asserting option wiring.
type stubObs struct{}

func (stubObs) TracerProvider() trace.TracerProvider { return tracenoop.NewTracerProvider() }
func (stubObs) MeterProvider() metric.MeterProvider  { return noop.NewMeterProvider() }

// withStubDial swaps the package dialer for the duration of one test.
func withStubDial(t *testing.T, fn func(context.Context, []string, string, ...Option) (Client, error)) {
	t.Helper()
	prev := dial
	dial = fn
	t.Cleanup(func() { dial = prev })
}

// withStubRawDial swaps the concrete-client dialer for the duration of one test.
func withStubRawDial(t *testing.T, fn func(context.Context, []string, string, ...Option) (*redis.ClusterClient, error)) {
	t.Helper()
	prev := dialRaw
	dialRaw = fn
	t.Cleanup(func() { dialRaw = prev })
}

func TestConfig_Enabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want bool
	}{
		{"zero value", Config{}, false},
		{"nil addrs", Config{Addrs: nil, Password: "p"}, false},
		{"empty addrs", Config{Addrs: []string{}}, false},
		{"one addr", Config{Addrs: []string{"host:6379"}}, true},
		{"many addrs", Config{Addrs: []string{"a:6379", "b:6379"}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.cfg.Enabled())
		})
	}
}

// Validate is how a service whose Valkey is a hard dependency states that,
// replacing the `env:",required"` tag the shared Config cannot carry.
func TestConfig_Validate(t *testing.T) {
	require.NoError(t, Config{Addrs: []string{"a:6379"}}.Validate())

	err := Config{}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "VALKEY_ADDRS")
}

func TestConnect_DisabledReturnsNilClientAndNoError(t *testing.T) {
	dialed := false
	withStubDial(t, func(context.Context, []string, string, ...Option) (Client, error) {
		dialed = true
		return nil, nil
	})

	client, err := Connect(context.Background(), Config{})

	require.NoError(t, err, "an unconfigured Valkey is not a failure")
	assert.Nil(t, client)
	assert.False(t, dialed, "an unconfigured Valkey must not be dialled")
}

func TestConnect_PassesAddressesAndPasswordThrough(t *testing.T) {
	var (
		gotAddrs []string
		gotPass  string
		gotOpts  int
	)
	want := &clusterClient{}
	withStubDial(t, func(_ context.Context, addrs []string, password string, opts ...Option) (Client, error) {
		gotAddrs, gotPass, gotOpts = addrs, password, len(opts)
		return want, nil
	})

	cfg := Config{Addrs: []string{"a:6379", "b:6379"}, Password: "secret"}
	client, err := Connect(context.Background(), cfg, Instrumented(stubObs{}))

	require.NoError(t, err)
	assert.Same(t, want, client)
	assert.Equal(t, []string{"a:6379", "b:6379"}, gotAddrs)
	assert.Equal(t, "secret", gotPass)
	assert.Equal(t, 1, gotOpts)
}

func TestConnect_PropagatesDialError(t *testing.T) {
	dialErr := errors.New("boom")
	withStubDial(t, func(context.Context, []string, string, ...Option) (Client, error) {
		return nil, dialErr
	})

	client, err := Connect(context.Background(), Config{Addrs: []string{"a:6379"}})

	require.ErrorIs(t, err, dialErr)
	assert.Nil(t, client)
}

func TestConnectOptional_DisabledReturnsNil(t *testing.T) {
	dialed := false
	withStubDial(t, func(context.Context, []string, string, ...Option) (Client, error) {
		dialed = true
		return nil, nil
	})

	assert.Nil(t, ConnectOptional(context.Background(), Config{}, "test"))
	assert.False(t, dialed)
}

// A failed dial must degrade to a nil client, never a fatal: an optional tier is
// not a startup dependency, and every bust and read already treats nil as off.
func TestConnectOptional_DialFailureReturnsNil(t *testing.T) {
	withStubDial(t, func(context.Context, []string, string, ...Option) (Client, error) {
		return nil, errors.New("unreachable")
	})

	assert.Nil(t, ConnectOptional(context.Background(), Config{Addrs: []string{"a:6379"}}, "test"))
}

func TestConnectOptional_SuccessReturnsClient(t *testing.T) {
	want := &clusterClient{}
	withStubDial(t, func(context.Context, []string, string, ...Option) (Client, error) {
		return want, nil
	})

	assert.Same(t, want, ConnectOptional(context.Background(), Config{Addrs: []string{"a:6379"}}, "test"))
}

func TestConnectRaw_DisabledReturnsNilClientAndNoError(t *testing.T) {
	dialed := false
	withStubRawDial(t, func(context.Context, []string, string, ...Option) (*redis.ClusterClient, error) {
		dialed = true
		return nil, nil
	})

	client, err := ConnectRaw(context.Background(), Config{})

	require.NoError(t, err)
	assert.Nil(t, client)
	assert.False(t, dialed, "an unconfigured Valkey must not be dialled")
}

func TestConnectRaw_PassesAddressesAndPasswordThrough(t *testing.T) {
	var (
		gotAddrs []string
		gotPass  string
	)
	want := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"a:6379"}})
	t.Cleanup(func() { _ = want.Close() })
	withStubRawDial(t, func(_ context.Context, addrs []string, password string, _ ...Option) (*redis.ClusterClient, error) {
		gotAddrs, gotPass = addrs, password
		return want, nil
	})

	client, err := ConnectRaw(context.Background(), Config{Addrs: []string{"a:6379"}, Password: "secret"})

	require.NoError(t, err)
	assert.Same(t, want, client)
	assert.Equal(t, []string{"a:6379"}, gotAddrs)
	assert.Equal(t, "secret", gotPass)
}

func TestConnectRaw_PropagatesDialError(t *testing.T) {
	dialErr := errors.New("boom")
	withStubRawDial(t, func(context.Context, []string, string, ...Option) (*redis.ClusterClient, error) {
		return nil, dialErr
	})

	client, err := ConnectRaw(context.Background(), Config{Addrs: []string{"a:6379"}})

	require.ErrorIs(t, err, dialErr)
	assert.Nil(t, client)
}

// Instrumented is the fleet-standard bundle: o11y providers plus request-scoped
// spans. Pinned so a service cannot half-adopt it and quietly emit startup noise.
func TestInstrumented_SetsObservabilityAndRequireParentSpan(t *testing.T) {
	obs := stubObs{}

	cfg := newConnectConfig(Instrumented(obs))

	assert.Equal(t, obs, cfg.obs)
	assert.Len(t, cfg.redisOpts, 1, "require-parent-span rides along with the providers")
}
