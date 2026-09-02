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

// StreamOwnership says whether this service is responsible for the standby
// streams its lane touches. Streams have a single owning service — the one that
// creates them and asserts their placement — and everyone else binds against
// what that owner put there.
type StreamOwnership int

const (
	// OwnsStreams creates the standby streams (in dev) and asserts in production
	// that they are hosted by the buddy cluster. The default.
	OwnsStreams StreamOwnership = iota
	// BorrowsStreams leaves them alone: another service owns them, and binding
	// the consumer is this service's existence check. A missing stream surfaces
	// as a failed bind, which leaves the home lane running.
	BorrowsStreams
)

// LaneSpec is what differs between one service's standby lane and another's:
// the stream it consumes, the consumer it binds, and any further streams that
// must exist on the buddy first.
type LaneSpec struct {
	// Stream is the standby stream this lane consumes.
	Stream stream.Config
	// AlsoEnsure lists standby streams this lane PUBLISHES to. They must exist
	// before the first failover message arrives, or the work would be consumed
	// and then have nowhere to go.
	AlsoEnsure []stream.Config
	// Consumer is the durable to bind on Stream. Its durable must differ from
	// the home lane's so the two keep independent cursors.
	Consumer jetstream.ConsumerConfig
	// Ownership defaults to OwnsStreams.
	Ownership StreamOwnership
}

// StreamsToEnsure lists every stream this lane needs on the buddy, the consumed
// one first — it is what the bind depends on, so its failure is the one worth
// reporting.
func (s *LaneSpec) StreamsToEnsure() []stream.Config {
	if s.Ownership == BorrowsStreams {
		return nil
	}
	return append([]stream.Config{s.Stream}, s.AlsoEnsure...)
}

// Binder readies and binds a standby lane on a buddy cluster.
//
// The six steps every failover lane performs — ensure the standby streams,
// assert their placement, create the durable, give the lane its own consumer
// metrics, drain it into the service's pool, mark the loop up — were copied into
// each service and drifted apart as they were maintained. Holding them here
// means a service states only what is different about its lane.
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
}

// buddySiteID is the cluster placement is asserted against; empty when no
// buddy is configured, which EnsureFailoverStream treats as nothing to verify.
func (b *Binder) buddySiteID() string {
	if b.Dialer == nil {
		return ""
	}
	return b.Dialer.Config.SiteID
}

// MetricsIdentity is the metric labelling for one lane. The attributes name the
// FAILOVER stream and durable: sharing the home lane's Consumer would attribute
// failover traffic to the live stream it did not come from.
func (b *Binder) MetricsIdentity(spec *LaneSpec) natsmetrics.ConsumerConfig {
	return natsmetrics.ConsumerConfig{
		Site:     b.SiteID,
		Stream:   spec.Stream.Name,
		Consumer: spec.Consumer.Durable,
	}
}

// BindConsumer readies spec's streams on the buddy and returns the bound
// consumer, for a lane whose draining pattern is its own — inbox-worker
// serializes membership events on one worker while fanning the rest out, which
// no shared pool can express.
func (b *Binder) BindConsumer(ctx context.Context, bjs o11ynats.JetStream, spec *LaneSpec) (o11ynats.Consumer, error) {
	fjs := stream.FailoverJS(bjs)
	for _, c := range spec.StreamsToEnsure() {
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
func (b *Binder) Bind(ctx context.Context, bjs o11ynats.JetStream, spec *LaneSpec,
	handle func(context.Context, jetstream.Msg),
) (*natsutil.Lane, error) {
	cons, err := b.BindConsumer(ctx, bjs, spec)
	if err != nil {
		return nil, err
	}
	iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(2*b.MaxWorkers))
	if err != nil {
		return nil, fmt.Errorf("bind failover consumer messages on %s: %w", spec.Stream.Name, err)
	}

	// handle takes a plain jetstream.Msg rather than a *natsmetrics.Message so
	// the uninstrumented path can pass the raw message straight through. A
	// hand-built Message with no Consumer behind it panics on the first
	// disposition, so one is only ever created by the tracking loop itself.
	if b.Metrics == nil {
		natsutil.RunPool(iter, b.Sem, b.WG, handle)
		return natsutil.NewLane(iter, b.WG), nil
	}

	laneMetrics := b.Metrics.Consumer(b.MetricsIdentity(spec))
	natsmetrics.StartInPool(ctx, iter, laneMetrics, b.Sem, spec.Consumer.MaxDeliver, b.WG,
		func(msg jetstream.Msg) natsmetrics.EventType {
			return natsmetrics.EventTypeFromSubject(msg.Subject())
		},
		func(msgCtx context.Context, msg *natsmetrics.Message) { handle(msgCtx, msg) })
	laneMetrics.LoopStarted(ctx)
	return natsutil.NewLane(iter, b.WG), nil
}

// HandlerFor builds one lane's message handler from the connection that lane
// consumes on. It is BindRouters' RouterFor for a worker: the same function
// builds both lanes, so the failover handler cannot be a home handler that
// still publishes, replies or does request/reply over the dead connection.
type HandlerFor func(ctx context.Context, conn *o11ynats.Conn, js o11ynats.JetStream, lane subject.Lane) (func(context.Context, jetstream.Msg), error)

// BindLane dials the buddy and binds spec there with a handler built by build
// for the failover lane. It is the worker-side twin of BindRouters, holding the
// choreography every worker used to copy: dial, build the lane's handler on the
// buddy connection, Bind, capture the Lane for shutdown.
//
// It never fails startup (see BuddyDialer.Bind). The returned Lane is nil-safe
// and the Conn is nil exactly when no connection was established, so a service
// lists both in its shutdown hooks unconditionally.
func (b *Binder) BindLane(ctx context.Context, spec *LaneSpec, build HandlerFor) (*natsutil.Lane, *o11ynats.Conn) {
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
