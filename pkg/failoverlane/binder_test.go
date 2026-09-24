package failoverlane_test

import (
	"context"
	"testing"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/failoverlane"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// BindLane is the worker-side twin of BindRouters: the lane's handler is built
// by the same function that built the home one, on the buddy connection, and
// an unconfigured buddy binds nothing without touching the builder.
func TestBinder_BindLane_UnconfiguredBuddyBindsNothing(t *testing.T) {
	b := &failoverlane.Binder{SiteID: "site-a", MaxWorkers: 1}
	built := false

	lane, conn := b.BindLane(context.Background(), &failoverlane.BuddyLane{},
		func(context.Context, *o11ynats.Conn, o11ynats.JetStream, subject.Lane) (func(context.Context, jetstream.Msg), error) {
			built = true
			return nil, nil
		})

	assert.Nil(t, lane)
	assert.Nil(t, conn)
	assert.False(t, built, "no buddy means no failover handler is ever built")
}

// The returned Lane is nil-safe so a service's shutdown list needs no guard.
func TestBinder_BindLane_UnreachableBuddyDoesNotFailStartup(t *testing.T) {
	b := &failoverlane.Binder{
		SiteID: "site-a", MaxWorkers: 1,
		Dialer: &natsutil.BuddyDialer{
			Config:         natsutil.BuddyConfig{SiteID: "site-b", NatsURL: "nats://127.0.0.1:1"},
			TracerProvider: noop.NewTracerProvider(), Propagator: propagation.TraceContext{},
		},
	}

	lane, conn := b.BindLane(context.Background(), &failoverlane.BuddyLane{},
		func(context.Context, *o11ynats.Conn, o11ynats.JetStream, subject.Lane) (func(context.Context, jetstream.Msg), error) {
			t.Fatal("the builder must not run without a live buddy connection")
			return nil, nil
		})

	assert.Nil(t, conn)
	lane.Stop() // nil-safe
}
