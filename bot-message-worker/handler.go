package main

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// handler consumes bot canonical messages and writes them to Cassandra.
type handler struct {
	store  Store
	siteID string
}

func newHandler(store Store, siteID string) *handler {
	return &handler{store: store, siteID: siteID}
}

// botSender is the bot a message is attributed to when it fails.
type botSender struct {
	ID      string
	Account string
}

// senderFromHeader reads the X-Bot-Identity bot-message-handler forwards onto
// the canonical publish. A missing or malformed header yields a zero sender
// rather than an error: attribution is diagnostic, never a reason to fail a
// message that would otherwise be written.
func senderFromHeader(h nats.Header) botSender {
	raw := h.Get(model.HeaderBotIdentity)
	if raw == "" {
		return botSender{}
	}
	var ident model.BotIdentity
	if err := json.Unmarshal([]byte(raw), &ident); err != nil {
		return botSender{}
	}
	return botSender{ID: ident.ID, Account: ident.Account}
}

// orElse fills empty fields from the decoded payload, so a message published
// before the header existed is still attributed.
func (s botSender) orElse(id, account string) botSender {
	if s.ID == "" {
		s.ID = id
	}
	if s.Account == "" {
		s.Account = account
	}
	return s
}

// label is the metric label for this sender; unattributable failures are still counted.
func (s botSender) label() string {
	if s.Account == "" {
		return unknownBot
	}
	return s.Account
}

// HandleJetStreamMsg processes one canonical message. Ack on success, Nak on transient error, Ack-drop on permanent/unmarshal error.
func (h *handler) HandleJetStreamMsg(ctx context.Context, msg jetstream.Msg) {
	ctx, _ = logctx.ConsumeContext(ctx, msg.Headers(), msg.Subject(), msg.Data())
	// Resolved before the unmarshal: the header is the only attribution left
	// when the body does not decode.
	sender := senderFromHeader(msg.Headers())

	var evt model.MessageEvent
	if err := json.Unmarshal(msg.Data(), &evt); err != nil {
		botFailureTotal.WithLabelValues(sender.label(), outcomeMalformed).Inc()
		slog.ErrorContext(ctx, "bot-message-worker unmarshal failed — ack-drop",
			"subject", msg.Subject(), "botID", sender.ID, "botAccount", sender.Account,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		_ = msg.Ack()
		return
	}
	m := evt.Message
	sender = sender.orElse(m.UserID, m.UserAccount)
	if err := h.write(ctx, &m); err != nil {
		if isPermanent(err) {
			permanentErrorTotal.Inc()
			botFailureTotal.WithLabelValues(sender.label(), outcomePermanent).Inc()
			slog.ErrorContext(ctx, "bot-message-worker permanent error — ack-drop",
				"messageID", m.ID, "roomID", m.RoomID, "botID", sender.ID, "botAccount", sender.Account,
				"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
			_ = msg.Ack()
			return
		}
		botFailureTotal.WithLabelValues(sender.label(), outcomeNak).Inc()
		slog.WarnContext(ctx, "bot-message-worker transient error — nak",
			"messageID", m.ID, "roomID", m.RoomID, "botID", sender.ID, "botAccount", sender.Account,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		jsretry.Nak(ctx, msg, jsretry.DefaultBackoff, "transient cassandra error")
		return
	}
	if err := msg.Ack(); err != nil {
		slog.WarnContext(ctx, "bot-message-worker ack failed",
			"messageID", m.ID, "roomID", m.RoomID, "botID", sender.ID, "botAccount", sender.Account,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
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
