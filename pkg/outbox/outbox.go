// Package outbox is the cross-site federation relay contract shared by the
// producers (room-service, room-worker, message-worker, broadcast-worker) and the consumer
// (outbox-worker): which event types ride which OUTBOX consumer lane, and the
// one way to publish a relay event onto the stream.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// ConcurrentEventTypes are the OUTBOX event types forwarded by outbox-worker's
// shared concurrent consumer. They are order-insensitive at the destination
// (inbox-worker applies them under high-water-mark / idempotent-upsert guards),
// so parallel forwarding is safe.
var ConcurrentEventTypes = []model.InboxEventType{
	model.InboxRoleUpdated,
	model.InboxSubscriptionRead,
	model.InboxThreadRead,
	model.InboxThreadReadAll,
	model.InboxSubscriptionMuteToggled,
	model.InboxSubscriptionFavoriteToggled,
	model.InboxSubscriptionSectionMoved,
	model.InboxSubscriptionOpened,
	model.InboxRoomRestricted,
	// message-worker: thread-subscription federation. Order-insensitive —
	// inbox-worker's UpsertThreadSubscription is $setOnInsert (immutable identity)
	// + $max hasMention (monotonic), so out-of-order/duplicate applies converge.
	model.InboxThreadSubscriptionUpserted,
	// thread_unread_added $addToSets one parent ID and thread_read $pulls one —
	// per-ID set ops that commute across threads, so both ride this lane.
	model.InboxThreadUnreadAdded,
	// Teams migration joinedAt self-correction: sets joinedAt on an existing
	// replica ($set, missing sub = no-op), targeting members added in a prior run.
	// Idempotent + commutes with add/remove (a delete just makes it a no-op), so
	// it rides the concurrent lane.
	model.InboxMemberJoinedAtRefreshed,
	// broadcast-worker: room-level mention badge. The destination write is a
	// guarded idempotent $set, so duplicates and out-of-order applies converge.
	model.InboxSubscriptionMention,
}

// OrderedEventTypes are the OUTBOX event types forwarded by outbox-worker's
// per-destination FIFO lanes (MaxAckPending=1). They are order-sensitive and
// share one lane per destination so they cannot overtake each other: a stale
// member_added must not overtake a member_removed (resurrecting the member),
// and a room_renamed must not overtake the member_added that creates the
// subscription it renames (stranding a new member on the old name).
var OrderedEventTypes = []model.InboxEventType{
	model.InboxMemberAdded,
	model.InboxMemberRemoved,
	model.InboxRoomRenamed,
}

// The two sets partition the stream: an event type in neither has no consumer
// filter and would sit in OUTBOX unconsumed until retention deletes it, which
// is why Publish rejects unknown types instead of stranding them silently.
var knownEventTypes = func() map[model.InboxEventType]struct{} {
	m := make(map[model.InboxEventType]struct{}, len(ConcurrentEventTypes)+len(OrderedEventTypes))
	for _, et := range ConcurrentEventTypes {
		m[et] = struct{}{}
	}
	for _, et := range OrderedEventTypes {
		m[et] = struct{}{}
	}
	return m
}()

// Publish builds the cross-site InboxEvent envelope (so every producer relays
// byte-identical envelopes), wraps it in an OutboxEvent, and publishes it onto
// the local OUTBOX stream at chat.outbox.{origin}.{dest}.{eventType}. payload
// is the pre-marshaled inner event; ts is the envelope's event timestamp. A
// blank or local destination is a no-op — there is nothing to federate, and a
// local-destination subject would sit in the stream with no consumer lane.
// dedupID is the publish's Nats-Msg-Id (so a producer-side redelivery can't
// double-enqueue) AND the forward's Nats-Msg-Id at the destination —
// outbox-worker Ack-drops an event without one, so it is required here.
func Publish(ctx context.Context, publish func(ctx context.Context, subj string, data []byte, msgID string) error,
	originSiteID, roomID, destSiteID string, eventType model.InboxEventType, payload []byte, dedupID string, ts int64,
) error {
	return PublishTo(ctx, publish, subject.LaneHome, originSiteID, roomID, destSiteID, eventType, payload, dedupID, ts)
}

// PublishTo is Publish with an explicit lane. The failover lane targets the
// buddy-hosted OUTBOX-FAILOVER stream, which is how a site keeps federating
// outward while its own NATS is down: the live OUTBOX buffer lives on the
// cluster that is gone, so an event published there would go nowhere.
//
// The event-type partition guard applies on both lanes — a type in neither
// filter set has no consumer on either stream and would sit until retention
// deleted it.
func PublishTo(ctx context.Context, publish func(ctx context.Context, subj string, data []byte, msgID string) error,
	lane subject.Lane, originSiteID, roomID, destSiteID string, eventType model.InboxEventType,
	payload []byte, dedupID string, ts int64,
) error {
	if destSiteID == "" || destSiteID == originSiteID {
		return nil
	}
	if _, ok := knownEventTypes[eventType]; !ok {
		return fmt.Errorf("outbox publish for room %s: event type %q is in no outbox-worker filter set (add it to exactly one)", roomID, eventType)
	}
	if dedupID == "" {
		return fmt.Errorf("outbox publish for room %s (%s): missing dedup id", roomID, eventType)
	}
	envelope, err := json.Marshal(model.InboxEvent{
		Type:       eventType,
		SiteID:     originSiteID,
		DestSiteID: destSiteID,
		Payload:    payload,
		Timestamp:  ts,
	})
	if err != nil {
		return errcode.MarshalFailed("inbox event envelope", err)
	}
	data, err := json.Marshal(model.OutboxEvent{
		RoomID:    roomID,
		Envelope:  envelope,
		DedupID:   dedupID,
		Timestamp: time.Now().UTC().UnixMilli(),
	})
	if err != nil {
		return errcode.MarshalFailed("outbox event", err)
	}
	if err := publish(ctx, lane.Outbox(originSiteID, destSiteID, eventType), data, dedupID); err != nil {
		return fmt.Errorf("publish outbox event for %s: %w", destSiteID, err)
	}
	return nil
}
