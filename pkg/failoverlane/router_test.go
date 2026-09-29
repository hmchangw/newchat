package failoverlane

import (
	"context"
	"errors"
	"testing"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// recordedBuild captures the lanes a builder was asked for, so a test can assert
// which lanes were actually built rather than only what came back.
type recordedBuild struct {
	lanes []subject.Lane
	err   error
}

func (r *recordedBuild) build(_ context.Context, _ *o11ynats.Conn, _ o11ynats.JetStream, lane subject.Lane) (*natsrouter.Router, error) {
	r.lanes = append(r.lanes, lane)
	if r.err != nil {
		return nil, r.err
	}
	// A nil router is enough for the paths under test: nothing here dispatches a
	// request, and ShutdownHooks tolerates one.
	return nil, nil
}

func dialer(cfg natsutil.FailoverSiteConfig) *natsutil.FailoverDialer {
	return &natsutil.FailoverDialer{Config: cfg, TracerProvider: noop.NewTracerProvider(), Propagator: propagation.TraceContext{}}
}

// bind runs BindRouters with no live connections — every lane the builder sees
// is recorded, so a test asserts which lanes were built rather than what came back.
func bind(cfg natsutil.FailoverSiteConfig, rec *recordedBuild) (*Routers, error) {
	return BindRouters(context.Background(), nil, nil, dialer(cfg), rec.build)
}

// The overwhelmingly common deployment is single-site: with no failover configured
// the home router must still be built, and nothing must be dialed.
func TestBindRouters_NoFailoverBuildsHomeOnly(t *testing.T) {
	rec := &recordedBuild{}

	routers, err := bind(natsutil.FailoverSiteConfig{}, rec)

	require.NoError(t, err)
	assert.Equal(t, []subject.Lane{subject.LaneHome}, rec.lanes)
	assert.Nil(t, routers.Failover)
}

// A home-lane build failure is fatal — the service cannot serve its own site —
// so it surfaces as an error rather than degrading silently the way the failover
// lane does.
func TestBindRouters_HomeBuildErrorIsReturned(t *testing.T) {
	rec := &recordedBuild{err: errors.New("register failed")}

	_, err := bind(natsutil.FailoverSiteConfig{}, rec)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "register failed")
}

// An unreachable failover must not fail startup: a failover that is already down is a
// double fault, and refusing to boot over a cluster only needed during an outage
// would turn it into an outage of its own.
func TestBindRouters_UnreachableFailoverDoesNotFailStartup(t *testing.T) {
	rec := &recordedBuild{}

	routers, err := bind(natsutil.FailoverSiteConfig{SiteID: "site-b", NatsURL: "nats://127.0.0.1:1"}, rec)

	require.NoError(t, err)
	assert.Equal(t, []subject.Lane{subject.LaneHome}, rec.lanes, "the failover lane is never reached")
	assert.Nil(t, routers.Failover)
}

// Shutdown must be safe on a service that never bound a failover — the single-site
// case — so every caller can list the hooks unconditionally.
func TestRouters_ShutdownHooks_ToleratesNoFailover(t *testing.T) {
	rec := &recordedBuild{}
	routers, err := bind(natsutil.FailoverSiteConfig{}, rec)
	require.NoError(t, err)

	hooks := routers.ShutdownHooks()
	require.NotEmpty(t, hooks)
	for i, hook := range hooks {
		require.NoError(t, hook(context.Background()), "hook %d", i)
	}
}
