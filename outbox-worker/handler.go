package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/outbox"
	"github.com/hmchangw/chat/pkg/subject"
)

// PublishFunc publishes data; non-empty msgID sets Nats-Msg-Id for JetStream
// stream-level dedup. Aliased to the pkg/outbox type so the handler and
// ForwardWithFailover cannot drift apart.
type PublishFunc = outbox.PublishFunc

// Handler forwards federation relay events off the OUTBOX stream to their
// destination sites' INBOX. It holds no store — it is a pure NATS→NATS relay.
type Handler struct {
	publish PublishFunc
}

func NewHandler(publish PublishFunc) *Handler {
	return &Handler{publish: publish}
}

// federationForwardTimeout bounds one forward attempt so a hung publish frees its
// worker slot promptly; on failure jsretry NakWithDelays for redelivery.
const federationForwardTimeout = 3 * time.Second

// HandleEvent forwards one OutboxEvent to its destination INBOX. Destination and
// event type come from the subject (chat.outbox.{origin}.{dest}.{eventType}); the
// body carries the pre-marshaled Envelope and the forward's Nats-Msg-Id. Forward
// failure -> transient error (jsretry Naks/redelivers, idempotent via DedupID);
// malformed subject/envelope -> permanent (jsretry Ack-poison).
func (h *Handler) HandleEvent(ctx context.Context, subj string, data []byte) error {
	_, destSiteID, eventType, ok := subject.ParseOutbox(subj)
	if !ok {
		return errcode.Permanent(errcode.BadRequest("malformed outbox subject"))
	}
	var evt model.OutboxEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return errcode.Permanent(errcode.BadRequest("unmarshal outbox event"))
	}
	// An empty DedupID would forward without a Nats-Msg-Id, so a redelivery would
	// double-apply at the destination; an empty Envelope has nothing to forward.
	// room-service never emits either — drop (Ack) rather than retry forever.
	if len(evt.Envelope) == 0 || evt.DedupID == "" {
		slog.WarnContext(ctx, "skipping malformed outbox event",
			"room_id", evt.RoomID, "dest_site_id", destSiteID, "event_type", eventType,
			"has_dedup_id", evt.DedupID != "", "request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
	// Redelivery is idempotent (DedupID), so a timed-out-but-delivered forward re-forwards safely.
	pubCtx, cancel := context.WithTimeout(ctx, federationForwardTimeout)
	defer cancel()
	if err := outbox.ForwardWithFailover(pubCtx, h.publish, destSiteID, eventType, evt.Envelope, evt.DedupID); err != nil {
		if errors.Is(err, nats.ErrMaxPayload) {
			// Rejected client-side before the wire, so every redelivery fails the
			// same way — on the failover lane too, since the payload is what is
			// oversized, not the destination. This lane is MaxDeliver=-1 with
			// MaxAckPending=1 per peer: a Nak would park every later event for
			// that peer behind this one forever, so drop the single oversized
			// event instead.
			return errcode.Permanent(errcode.Internal(
				fmt.Sprintf("outbox %s event for %s exceeds broker max_payload", eventType, destSiteID)))
		}
		return fmt.Errorf("forward outbox event to %s: %w", destSiteID, err)
	}
	return nil
}
