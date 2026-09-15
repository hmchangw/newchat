package failoverlane

import (
	"context"
	"fmt"
	"sync"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// HomeSpec is a worker's home lane: the stream it consumes on its own cluster
// and the durable it binds there.
type HomeSpec struct {
	Stream   stream.Config
	Consumer jetstream.ConsumerConfig
	// Bootstrap creates the home stream in dev (BOOTSTRAP_STREAMS). Nil when
	// the service does not own the stream. It runs inside the deferred bind,
	// because it needs the home server exactly as the consumer does.
	Bootstrap func(context.Context, o11ynats.JetStream) error
}

// LanesSpec is everything a worker's two lanes need, in one place.
type LanesSpec struct {
	SiteID string
	Home   HomeSpec
	// Buddy is the standby lane; nil for a service that has none (the bot
	// pipeline, a migration mode). The Dialer's OnlyIf gate says the same thing
	// from the connection side; both are honoured.
	Buddy *LaneSpec
	// Bootstrap creates the standby streams instead of verifying them. Dev only.
	Bootstrap bool
	// MaxWorkers sizes the pool both lanes share and their pull batches.
	MaxWorkers int
	// Metrics is optional; nil binds the lanes uninstrumented.
	Metrics *natsmetrics.Metrics
}

// Lanes is a worker's home and buddy lanes bound as one: the worker twin of
// Routers. One HandlerFor builds both handlers, one pool bounds both, and one
// set of hooks stops and drains both — so a service cannot wire one lane
// without the other, which is how a worker was once left on a fail-fast home
// dial with its buddy lane in place.
//
// The home lane is bound through natsutil.BindWhenConnected: immediately when
// the home connection is up, on the first CONNECTED otherwise. The buddy lane
// binds at once and never fails startup.
type Lanes struct {
	home      *natsutil.DeferredBind
	buddy     *loop
	homeConn  *o11ynats.Conn
	buddyConn *o11ynats.Conn
	wg        sync.WaitGroup
}

// BindLanes builds both handlers up front (home first, so a builder may capture
// home-lane state) and binds the lanes. Only the home lane's handler build or
// an immediate home bind failure is fatal; the buddy degrades to home-only.
func BindLanes(ctx context.Context, home *o11ynats.Conn, homeJS o11ynats.JetStream,
	dialer *natsutil.BuddyDialer, spec *LanesSpec, build HandlerFor,
) (*Lanes, error) {
	l := &Lanes{homeConn: home}
	// One pool for both lanes: a buddy lane with its own semaphore would take
	// the service to 2×MAX_WORKERS in-flight handlers against the same stores.
	b := &Binder{
		SiteID: spec.SiteID, Dialer: dialer,
		Bootstrap: spec.Bootstrap, MaxWorkers: spec.MaxWorkers,
		Sem: make(chan struct{}, spec.MaxWorkers), WG: &l.wg, Metrics: spec.Metrics,
	}

	homeHandle, err := build(ctx, home, homeJS, subject.LaneHome)
	if err != nil {
		return nil, fmt.Errorf("build home handler: %w", err)
	}
	homeName := spec.Home.Stream.Name
	l.home, err = natsutil.BindWhenConnected(ctx, home, homeName, func(ctx context.Context) (func(), error) {
		if spec.Home.Bootstrap != nil {
			if err := spec.Home.Bootstrap(ctx, homeJS); err != nil {
				return nil, fmt.Errorf("bootstrap %s: %w", homeName, err)
			}
		}
		cons, err := homeJS.CreateOrUpdateConsumer(ctx, homeName, spec.Home.Consumer)
		if err != nil {
			return nil, fmt.Errorf("create consumer on %s: %w", homeName, err)
		}
		lp, err := b.startLoop(ctx, cons, homeName, &spec.Home.Consumer, homeHandle)
		if err != nil {
			return nil, err
		}
		return lp.stop, nil
	})
	if err != nil {
		return nil, err
	}

	if spec.Buddy != nil {
		l.buddyConn = b.Dialer.Bind(ctx, func(ctx context.Context, bconn *o11ynats.Conn, bjs o11ynats.JetStream) error {
			handle, err := build(ctx, bconn, bjs, subject.LaneFailover)
			if err != nil {
				return fmt.Errorf("build failover handler: %w", err)
			}
			cons, err := b.BindConsumer(ctx, bjs, spec.Buddy)
			if err != nil {
				return err
			}
			l.buddy, err = b.startLoop(ctx, cons, spec.Buddy.Stream.Name, &spec.Buddy.Consumer, handle)
			return err
		})
	}
	return l, nil
}

// HomeReady reports whether the home lane is bound and consuming.
func (l *Lanes) HomeReady() bool { return l.home.Ready() }

// Check is the readiness check: at least one lane serving.
func (l *Lanes) Check() health.Check {
	return natsutil.LanesCheck(l.home.Ready, func() bool { return l.buddy != nil })
}

// StopHooks stops both lanes and waits for their in-flight handlers, in the
// order shutdown.Wait should run them: both iterators stop before either lane
// finishes, so neither pulls new work while the other is still draining.
func (l *Lanes) StopHooks() []func(context.Context) error {
	return []func(context.Context) error{
		func(context.Context) error {
			l.home.Stop()
			l.buddy.stop()
			return nil
		},
		func(ctx context.Context) error { return natsutil.WaitPool(ctx, &l.wg) },
	}
}

// DrainHooks drains the home connection, then the buddy. Separate from
// StopHooks so a service can flush its own buffers between the two — after its
// handlers have finished, before its connections close.
func (l *Lanes) DrainHooks() []func(context.Context) error {
	return []func(context.Context) error{
		func(ctx context.Context) error { return natsutil.Drain(ctx, l.homeConn) },
		natsutil.DrainBuddy(l.buddyConn),
	}
}
