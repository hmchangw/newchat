package subject

import (
	"fmt"
	"time"
)

// Lane identifies which NATS connection a unit of work arrived on. Room-event
// routing depends on it: a failover-lane publish must reach clients scattered
// across peer clusters, which the site-local subject root cannot do — it is
// filtered at the leaf node and never crosses a gateway.
type Lane int

const (
	LaneHome     Lane = iota // the site's own cluster
	LaneFailover             // the buddy cluster hosting the standby lanes
)

// DefaultFailoverRevertGrace is how long after the home connection is restored
// a publisher keeps emitting to BOTH roots.
//
// Servers revert the instant their home lane delivers again; clients revert on
// their own reconnect backoff, up to five minutes later. In that gap some
// clients are home and some are still on a peer cluster, so both roots have to
// carry the traffic or the stragglers go silent. This value and the client's
// backoff cap are a coupled pair: raising the client cap without raising this
// reopens the silent recovery gap that dual-publishing exists to close.
//
// Distinct from roomLocalityGrace (7 days): that one covers permanent room
// reclassification, where clients learn the new locality from bootstrap. This
// one only has to outlast the client revert backoff.
const DefaultFailoverRevertGrace = 30 * time.Minute

// EffectiveRouteMode resolves the room-route mode for a single publish.
//
//   - Failover lane → RouteGlobal. Home is down, so every client of this site is
//     on some other cluster; chat.local.> has no legitimate subscribers there
//     and forcing global loses nothing.
//   - Home lane within grace of restoration → RouteDual, so clients that have
//     not yet reverted keep receiving.
//   - Otherwise → the configured mode, i.e. steady state is unchanged.
//
// A zero homeRestoredAt means the home connection was never lost, so there is no
// window to be inside. When the configured mode is already RouteGlobal there is
// no gap to cover and the grace window is skipped — dual would only add
// pointless local publishes.
func EffectiveRouteMode(configured RoomRouteMode, lane Lane, homeRestoredAt time.Time,
	grace time.Duration, now time.Time,
) RoomRouteMode {
	if lane == LaneFailover {
		return RouteGlobal
	}
	if configured.UsesLocal() && !homeRestoredAt.IsZero() && now.Before(homeRestoredAt.Add(grace)) {
		return RouteDual
	}
	return configured
}

// RouteResolver yields the room-route mode a publisher should use right now.
// Handlers hold one of these instead of a fixed RoomRouteMode, because the
// answer depends on the lane the work arrived on and on how recently home was
// restored.
type RouteResolver interface {
	Mode(now time.Time) RoomRouteMode
}

// ResolveMode reads a resolver that may be nil. An unconfigured resolver routes
// global — the fail-safe that reaches every client, matching how a nil crossSite
// flag is treated. Publish sites call this rather than r.Mode directly, so a
// handler assembled without a resolver degrades to over-delivery instead of
// panicking mid-broadcast.
func ResolveMode(r RouteResolver, now time.Time) RoomRouteMode {
	if r == nil {
		return RouteGlobal
	}
	return r.Mode(now)
}

// LaneRouter is the standard RouteResolver: one per lane, sharing the service's
// configured mode and grace window.
//
// The zero value resolves to RouteGlobal, the fail-safe that reaches every
// client — a router nobody configured must not silently narrow delivery.
type LaneRouter struct {
	configured RoomRouteMode
	lane       Lane
	restoredAt func() time.Time
	grace      time.Duration
}

// NewLaneRouter builds a resolver for one lane. restoredAt may be nil — the
// failover lane has no home-restoration concept, and a home lane in a service
// that does not track restores simply never enters the grace window.
func NewLaneRouter(configured RoomRouteMode, lane Lane, restoredAt func() time.Time,
	grace time.Duration,
) LaneRouter {
	return LaneRouter{configured: configured, lane: lane, restoredAt: restoredAt, grace: grace}
}

// Mode implements RouteResolver.
func (r LaneRouter) Mode(now time.Time) RoomRouteMode {
	var restored time.Time
	if r.restoredAt != nil {
		restored = r.restoredAt()
	}
	return EffectiveRouteMode(r.configured, r.lane, restored, r.grace, now)
}

// CanonicalEvent is the tail token of a per-message canonical subject. Typed so
// a lane-aware builder cannot be handed an arbitrary string that would land on a
// subject no consumer filters for.
type CanonicalEvent string

const (
	CanonicalCreated  CanonicalEvent = "created"
	CanonicalUpdated  CanonicalEvent = "updated"
	CanonicalDeleted  CanonicalEvent = "deleted"
	CanonicalPinned   CanonicalEvent = "pinned"
	CanonicalUnpinned CanonicalEvent = "unpinned"
	CanonicalReacted  CanonicalEvent = "reacted"
)

// canonicalRoot is the subject root each lane's canonical stream is built on.
// One place, so a new canonical event type gains its failover twin for free
// rather than needing a second named builder that someone must remember to add.
func (l Lane) canonicalRoot() string {
	if l == LaneFailover {
		return "chat.failover.msg.canonical"
	}
	return "chat.msg.canonical"
}

// MsgCanonical returns the canonical subject for a per-message event on this
// lane. A failover-lane event must not be published to the live canonical
// stream: that stream lives on the cluster that is down, so the event would be
// accepted by nothing and silently lost.
func (l Lane) MsgCanonical(siteID string, evt CanonicalEvent) string {
	return fmt.Sprintf("%s.%s.%s", l.canonicalRoot(), siteID, evt)
}

// outboxRoot is the subject root each lane's OUTBOX stream is built on.
func (l Lane) outboxRoot() string {
	if l == LaneFailover {
		return "chat.failover.outbox"
	}
	return "chat.outbox"
}

// Outbox returns the OUTBOX subject for a federation event on this lane. The
// failover form is how a site keeps federating outward while its own NATS is
// down: the live OUTBOX buffer lives on the cluster that is gone, so an event
// published there would go nowhere. Destination and event type ride the subject
// so outbox-worker's per-destination consumers filter on one peer exactly as
// they do on the live stream.
func (l Lane) Outbox(originSiteID, destSiteID, eventType string) string {
	return fmt.Sprintf("%s.%s.%s.%s", l.outboxRoot(), originSiteID, destSiteID, eventType)
}
