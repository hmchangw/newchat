package main

import (
	"context"
	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
)

func processInvalidationMessage(ctx context.Context, msg jetstream.Msg, queue chan<- string) {
	var evt model.CanonicalMemberEvent
	if err := sonic.Unmarshal(msg.Data(), &evt); err != nil {
		// Acking a payload that will never decode drops it permanently, and Ack
		// records a success disposition. Mark the terminal reason so the drop is
		// visible as loss rather than as normal processing.
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalInvalidPayload)
		slog.WarnContext(ctx, "canonical member event decode failed", "error", err)
		ackInvalidation(ctx, msg)
		return
	}
	if evt.RoomID != "" {
		select {
		case queue <- evt.RoomID:
		default:
			slog.WarnContext(ctx, "invalidation queue full, dropping (TTL will reconcile)", "roomId", evt.RoomID)
		}
	}
	ackInvalidation(ctx, msg)
}

func ackInvalidation(ctx context.Context, msg jetstream.Msg) {
	if err := msg.Ack(); err != nil {
		slog.ErrorContext(ctx, "canonical member event ack failed", "error", err)
	}
}
