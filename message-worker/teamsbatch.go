package main

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/model"
)

// messageProcessor persists a canonical MessageEvent through message-worker's own
// pipeline (satisfied by Handler.processMessage). isMigration=true suppresses the
// thread side-effects the source already produced.
type messageProcessor func(ctx context.Context, data []byte, isMigration bool) error

// teamsBatchHandler transforms Teams message-history batches and feeds each message
// through the existing persist pipeline with isMigration=true. It never re-publishes to
// canonical, so broadcast/notification/search-sync (all keyed on the .created event)
// stay silent — the no-fan-out migration by design (search is a phase-1 gap, see the design doc).
type teamsBatchHandler struct {
	store          HRIdentityStore
	siteID         string
	process        messageProcessor
	newTransformer func(identityResolver) MessageTransformer // injectable seam; default wraps DefaultTransformer
}

func newTeamsBatchHandler(store HRIdentityStore, siteID string, process messageProcessor) *teamsBatchHandler {
	return &teamsBatchHandler{
		store:          store,
		siteID:         siteID,
		process:        process,
		newTransformer: func(r identityResolver) MessageTransformer { return NewDefaultTransformer(r) },
	}
}

// consume decodes one batch message and settles it: Ack when every message was
// handled (per-message transform errors are logged, not redelivered), Nak on an
// infra failure so at-least-once redelivery re-runs the idempotent batch.
func (h *teamsBatchHandler) consume(ctx context.Context, msg jetstream.Msg) {
	var req model.TeamsBatchRequest
	if err := sonic.Unmarshal(msg.Data(), &req); err != nil {
		// A malformed batch can never parse on redelivery — drop it as poison.
		jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, errcode.Permanent(errcode.BadRequest("malformed teams batch")))
		return
	}
	jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, h.handleBatch(ctx, req))
}

// handleBatch runs each message; a fresh resolver per call scopes the sender cache to
// the batch. Returns the first infra error so the caller Naks — per-message transform
// errors are logged and don't block the batch.
func (h *teamsBatchHandler) handleBatch(ctx context.Context, req model.TeamsBatchRequest) error {
	resolver := newSenderResolver(h.store, h.siteID)
	tr := h.newTransformer(resolver)

	var persisted, skipped, failed int
	for _, raw := range req.Messages {
		res, err := h.migrateOne(ctx, tr, raw)
		if err != nil {
			return err // infra failure → Nak, the idempotent batch replays
		}
		switch res.Status {
		case model.TeamsBatchPersisted:
			persisted++
		case model.TeamsBatchSkipped:
			skipped++
		default:
			failed++
		}
	}
	// No RPC reply exists (consumer, not request/reply); the summary is how outcomes surface.
	slog.InfoContext(ctx, "teams batch migrated", "persisted", persisted, "skipped", skipped, "error", failed)
	return nil
}

func (h *teamsBatchHandler) migrateOne(ctx context.Context, tr MessageTransformer, raw json.RawMessage) (model.TeamsBatchResult, error) {
	head, ok := peekTeamsHead(raw)
	res := model.TeamsBatchResult{TeamsMsgID: head.ID}
	if !ok {
		slog.WarnContext(ctx, "teams batch: malformed message payload")
		res.Status, res.Error = model.TeamsBatchError, "malformed message payload"
		return res, nil
	}
	if head.ID == "" {
		slog.DebugContext(ctx, "teams batch: skip message with no id")
		res.Status = model.TeamsBatchSkipped
		return res, nil
	}
	// No target room → the deterministic id would collide across conversations and the
	// message would orphan; skip rather than persist it.
	if head.RoomID == "" {
		slog.DebugContext(ctx, "teams batch: skip message with no roomId", "teamsMsgId", head.ID)
		res.Status = model.TeamsBatchSkipped
		return res, nil
	}

	msg, err := tr.Transform(ctx, raw)
	if err != nil {
		// Per-message, deterministic → log the chain, record error, but don't Nak the batch.
		slog.ErrorContext(ctx, "teams batch: transform failed", "teamsMsgId", head.ID, "error", err)
		res.Status, res.Error = model.TeamsBatchError, "transform failed"
		return res, nil //nolint:nilerr // per-message transform error is isolated, not a batch Nak
	}
	// Teams ids are unique only per conversation → scope by roomId to avoid collisions.
	msg.ID = deterministicMessageID(head.RoomID, head.ID)

	evt := model.MessageEvent{Event: model.EventCreated, Message: msg, SiteID: h.siteID}
	data, err := sonic.Marshal(evt)
	if err != nil {
		slog.ErrorContext(ctx, "teams batch: marshal event failed", "teamsMsgId", head.ID, "error", err)
		res.Status, res.Error = model.TeamsBatchError, "marshal event failed"
		return res, nil //nolint:nilerr // marshalling our own struct is deterministic → per-message, not infra
	}
	if err := h.process(ctx, data, true); err != nil {
		// Persist-pipeline failure is infra: surface it so the batch Naks and replays.
		slog.ErrorContext(ctx, "teams batch: process failed", "teamsMsgId", head.ID, "error", err)
		res.Status, res.Error = model.TeamsBatchError, "process failed"
		return res, err
	}
	res.Status = model.TeamsBatchPersisted
	return res, nil
}

// teamsHead is the minimal envelope needed before transforming: the source id and
// its conversation scope (roomId).
type teamsHead struct {
	ID     string `json:"id"`
	RoomID string `json:"roomId"`
}

// peekTeamsHead reads the id + roomId; ok is false on malformed JSON (an error
// result), true with an empty id when the field is absent (a skip).
func peekTeamsHead(raw json.RawMessage) (h teamsHead, ok bool) {
	if err := json.Unmarshal(raw, &h); err != nil {
		return teamsHead{}, false
	}
	return h, true
}
