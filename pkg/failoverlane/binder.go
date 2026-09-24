// Package failoverlane binds a service's standby lane on its buddy cluster.
//
// It sits above pkg/natsutil and pkg/stream rather than inside either: the
// binder needs both, and pkg/stream already depends on pkg/natsutil through
// pkg/jsretry, so putting it in natsutil would close an import cycle.
package failoverlane

import (
	"context"
	"fmt"
	"sync"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// BuddyLane is what differs between one service's standby lane and another's:
// the stream it consumes, the consumer it binds, and any further streams that
// must exist on the buddy first.
type BuddyLane struct {
	// Stream is the standby stream this lane consumes.
	Stream stream.Config
	// PublishesTo lists standby streams this lane publishes to. They must exist
	// before the first failover message arrives, or the work would be consumed
	// and then have nowhere to go.
	PublishesTo []stream.Config
	// Consumer is the durable to bind on Stream. Its durable must differ from
	// the home lane's so the two keep independent cursors.
	Consumer jetstream.ConsumerConfig
	// BorrowStream means another service creates this stream and asserts its
	// placement, so this one must do neither — binding the consumer is its
	// existence check, and a missing stream fails the bind and leaves the home
	// lane running. Streams have a single owning service.
	BorrowStream bool
}

// streamsToEnsure lists every stream this lane needs on the buddy, the consumed
// one first — it is what the bind depends on, so its failure is the one worth
// reporting.
func (s *BuddyLane) streamsToEnsure() []stream.Config {
	if s.BorrowStream {
		return nil
	}
	return append([]stream.Config{s.Stream}, s.PublishesTo...)
}

// Binder readies and binds a standby lane on a buddy cluster.
//
// It holds the setup every standby lane needs, so a service states only what is
// different about its own lane. Before this, each service carried its own copy
// and they drifted apart as they were maintained.
//
// Build one per service from its config; call Bind once per lane.
type Binder struct {
	// SiteID labels the lane's metrics. The service itself is identified by the
	// meter the Metrics were built from.
	SiteID string
	// Dialer reaches the buddy cluster the standby streams must be hosted by.
	// Placement is asserted against its site in production — names are unique
	// supercluster-wide, so a standby stream sitting on the very cluster it
	// exists to outlive would pass an existence check and fail only during the
	// outage it was built for.
	Dialer *natsutil.BuddyDialer
	// Bootstrap creates the streams instead of verifying them. Dev only.
	Bootstrap bool
	// MaxWorkers sizes the pull batch.
	MaxWorkers int
	// Sem and WG are the service's own pool, shared with the home lane: a lane
	// that allocated its own would double the service's in-flight handlers
	// against the same databases the moment it bound.
	Sem chan struct{}
	WG  *sync.WaitGroup
	// Metrics is optional; a service that is not instrumented binds its lane
	// without consumer metrics rather than not at all.
	Metrics *natsmetrics.Metrics
	// OnLoopStop is told when this lane's pull loop exits. A lane shares its
	// pool with the home lane, so the service's WaitGroup keeps looking busy
	// after one of them dies — this is the only signal that it has. Services
	// point it at a pkg/loopguard Guard of the lane's own; nil ignores it.
	OnLoopStop func(error)
}

// buddySiteID is the cluster placement is asserted against; empty when no
// buddy is configured, which EnsureFailoverStream treats as nothing to verify.
func (b *Binder) buddySiteID() string {
	if b.Dialer == nil {
		return ""
	}
	return b.Dialer.Config.SiteID
}

// BindConsumer readies spec's streams on the buddy and returns the bound
// consumer, for a lane whose draining pattern is its own — inbox-worker
// serializes membership events on one worker while fanning the rest out, which
// no shared pool can express.
func (b *Binder) BindConsumer(ctx context.Context, bjs o11ynats.JetStream, spec *BuddyLane) (o11ynats.Consumer, error) {
	fjs := bjs
	for _, c := range spec.streamsToEnsure() {
		if err := stream.EnsureFailoverStream(ctx, fjs, c, b.Bootstrap, b.buddySiteID()); err != nil {
			return nil, err
		}
	}
	cons, err := bjs.CreateOrUpdateConsumer(ctx, spec.Stream.Name, spec.Consumer)
	if err != nil {
		return nil, fmt.Errorf("create failover consumer on %s: %w", spec.Stream.Name, err)
	}
	return cons, nil
}

// Bind readies spec's streams on the buddy, binds its consumer, and drains it
// into the binder's pool through handle. The returned Lane is what shutdown
// stops and waits on; it is nil-safe, so a service needs no guard of its own.
func (b *Binder) Bind(ctx context.Context, bjs o11ynats.JetStream, spec *BuddyLane,
	handle func(context.Context, jetstream.Msg),
) (*natsutil.Lane, error) {
	cons, err := b.BindConsumer(ctx, bjs, spec)
	if err != nil {
		return nil, err
	}
	loop, err := b.startLoop(ctx, cons, spec.Stream.Name, &spec.Consumer, handle)
	if err != nil {
		return nil, err
	}
	return loop.lane, nil
}

// loop is one running pull loop: the lane shutdown stops, and the metrics that
// record its start and stop.
type loop struct {
	lane    *natsutil.Lane
	metrics *natsmetrics.Consumer
}

// stop records the loop as stopped and stops its iterator. Nil-safe.
func (l *loop) stop() {
	if l == nil {
		return
	}
	l.metrics.LoopStopped(context.Background())
	l.lane.Stop()
}

// startLoop drains cons into the binder's pool through handle. It is the one
// loop start for both lanes: the home lane binds its consumer itself (it owns
// the stream), the failover lane through BindConsumer (placement asserted).
func (b *Binder) startLoop(ctx context.Context, cons o11ynats.Consumer, streamName string,
	consumerCfg *jetstream.ConsumerConfig, handle func(context.Context, jetstream.Msg),
) (*loop, error) {
	iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(2*b.MaxWorkers))
	if err != nil {
		return nil, fmt.Errorf("bind consumer messages on %s: %w", streamName, err)
	}
	// A nil Metrics yields a Consumer with no instruments, so the stop path
	// can record unconditionally.
	laneMetrics := b.Metrics.Consumer(natsmetrics.ConsumerConfig{
		Site: b.SiteID, Stream: streamName, Consumer: consumerCfg.Durable,
	})

	if b.Metrics == nil {
		natsutil.RunPool(iter, b.Sem, b.WG, handle, b.OnLoopStop)
		return &loop{lane: natsutil.NewLane(iter), metrics: laneMetrics}, nil
	}

	natsmetrics.StartInPool(ctx, iter, laneMetrics, b.Sem, consumerCfg.MaxDeliver, b.WG,
		func(msg jetstream.Msg) natsmetrics.EventType {
			return natsmetrics.EventTypeFromSubject(msg.Subject())
		},
		func(msgCtx context.Context, msg *natsmetrics.Message) { handle(msgCtx, msg) },
		b.OnLoopStop)
	laneMetrics.LoopStarted(ctx)
	return &loop{lane: natsutil.NewLane(iter), metrics: laneMetrics}, nil
}

// BuildHandler builds one lane's message handler from the connection that lane
// consumes on. The same function builds both lanes, so a failover handler
// cannot end up being a home handler that still publishes, replies or makes
// requests over the dead connection.
//
// It hands back a plain jetstream.Msg handler rather than a metrics one because
// the uninstrumented path passes the raw message straight through: a hand-built
// metrics Message with no consumer behind it panics on its first disposition,
// so only the tracking loop ever makes one.
type BuildHandler func(ctx context.Context, conn *o11ynats.Conn, js o11ynats.JetStream, lane subject.Lane) (func(context.Context, jetstream.Msg), error)

// BindLane dials the buddy and binds spec there with a handler built by build
// for the failover lane. It is the worker-side twin of BindRouters, holding the
// choreography every worker used to copy: dial, build the lane's handler on the
// buddy connection, Bind, capture the Lane for shutdown.
//
// It never fails startup (see BuddyDialer.Bind). The returned Lane is nil-safe
// and the Conn is nil exactly when no connection was established, so a service
// lists both in its shutdown hooks unconditionally.
func (b *Binder) BindLane(ctx context.Context, spec *BuddyLane, build BuildHandler) (*natsutil.Lane, *o11ynats.Conn) {
	if b.Dialer == nil {
		return nil, nil
	}
	var lane *natsutil.Lane
	conn := b.Dialer.Bind(ctx, func(ctx context.Context, bconn *o11ynats.Conn, bjs o11ynats.JetStream) error {
		handle, err := build(ctx, bconn, bjs, subject.LaneFailover)
		if err != nil {
			return fmt.Errorf("build failover handler: %w", err)
		}
		lane, err = b.Bind(ctx, bjs, spec, handle)
		return err
	})
	return lane, conn
}
