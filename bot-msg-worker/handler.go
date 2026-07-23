package main

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
)

// handler consumes bot canonical messages and writes them to Cassandra.
type handler struct {
	store  Store
	siteID string
}

func newHandler(store Store, siteID string) *handler {
	return &handler{store: store, siteID: siteID}
}

// HandleJetStreamMsg processes one canonical message. Ack on success, Nak on transient error, Ack-drop on permanent/unmarshal error.
func (h *handler) HandleJetStreamMsg(ctx context.Context, msg jetstream.Msg) {
	var evt model.MessageEvent
	if err := json.Unmarshal(msg.Data(), &evt); err != nil {
		slog.ErrorContext(ctx, "bot-msg-worker unmarshal failed — ack-drop",
			"subject", msg.Subject(), "error", err)
		_ = msg.Ack()
		return
	}
	m := evt.Message
	if err := h.write(ctx, &m); err != nil {
		if isPermanent(err) {
			permanentErrorTotal.Inc()
			slog.ErrorContext(ctx, "bot-msg-worker permanent error — ack-drop",
				"messageID", m.ID, "roomID", m.RoomID, "error", err)
			_ = msg.Ack()
			return
		}
		slog.WarnContext(ctx, "bot-msg-worker transient error — nak",
			"messageID", m.ID, "roomID", m.RoomID, "error", err)
		// NakWithDelay(0) defers to the consumer's BackOff schedule.
		if nakErr := msg.NakWithDelay(0); nakErr != nil {
			slog.WarnContext(ctx, "bot-msg-worker nak failed", "error", nakErr)
		}
		return
	}
	if err := msg.Ack(); err != nil {
		slog.WarnContext(ctx, "bot-msg-worker ack failed",
			"messageID", m.ID, "roomID", m.RoomID, "error", err)
	}
}

// write dispatches to SaveMessage or SaveThreadMessage based on the parent-thread fields.
// threadRoomID is the parent room's ID (partition key in thread_messages_by_thread).
func (h *handler) write(ctx context.Context, m *model.Message) error {
	if m.ThreadParentMessageID == "" {
		return h.store.SaveMessage(ctx, m, h.siteID)
	}
	threadRoomID := m.RoomID
	return h.store.SaveThreadMessage(ctx, m, h.siteID, threadRoomID)
}

// isPermanent treats non-errcode errors as transient (retry under Cassandra outage).
func isPermanent(err error) bool {
	if err == nil {
		return false
	}
	_, permanent := errcode.IsPermanent(err)
	return permanent
}
