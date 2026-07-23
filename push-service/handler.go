package main

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
)

// Dispatcher sends one push batch to the recipients' registered devices (APNs/FCM in prod).
type Dispatcher interface {
	Dispatch(ctx context.Context, evt *model.PushNotificationEvent) error
}

type handler struct {
	dispatcher Dispatcher
}

func newHandler(d Dispatcher) *handler {
	return &handler{dispatcher: d}
}

func (h *handler) HandleJetStreamMsg(ctx context.Context, msg jetstream.Msg) {
	var evt model.PushNotificationEvent
	if err := json.Unmarshal(msg.Data(), &evt); err != nil {
		slog.ErrorContext(ctx, "push-service unmarshal — ack-drop",
			"subject", msg.Subject(), "error", err)
		_ = msg.Ack()
		return
	}
	if err := h.dispatcher.Dispatch(ctx, &evt); err != nil {
		if _, permanent := errcode.IsPermanent(err); permanent {
			slog.WarnContext(ctx, "push-service permanent — ack-drop",
				"id", evt.ID, "roomID", evt.RoomID, "error", err)
			_ = msg.Ack()
			return
		}
		slog.WarnContext(ctx, "push-service transient — nak",
			"id", evt.ID, "roomID", evt.RoomID, "error", err)
		_ = msg.NakWithDelay(0)
		return
	}
	if err := msg.Ack(); err != nil {
		slog.WarnContext(ctx, "push-service ack failed",
			"id", evt.ID, "error", err)
	}
}

// LogDispatcher logs each push batch at INFO instead of hitting APNs/FCM.
type LogDispatcher struct{}

func (LogDispatcher) Dispatch(ctx context.Context, evt *model.PushNotificationEvent) error {
	slog.InfoContext(ctx, "push (log-only)",
		"id", evt.ID, "roomID", evt.RoomID, "accounts", len(evt.Accounts),
		"messageID", evt.Data.MessageID, "type", evt.Data.Type,
	)
	return nil
}
