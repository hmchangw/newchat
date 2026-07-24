package main

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/bytedance/sonic"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/teamsmigrate"
)

// ponytail: fixed cap; an evicted sender just re-resolves (a Mongo point-read),
// so bump only if a migration's distinct-sender count routinely exceeds this.
const teamsSenderCacheSize = 10_000

// teamsBatchHandler transforms Teams message-history batches and writes each message
// straight into Cassandra via the Store. It never re-publishes to canonical, so
// broadcast/notification stay silent and thread/mention/quote side-effects are
// intentionally dropped — the no-fan-out migration by design (see the design doc).
// The resolver + transformer are built once; the sender cache is process-wide.
type teamsBatchHandler struct {
	store    Store
	siteID   string
	resolver identityResolver
	tr       MessageTransformer
}

func newTeamsBatchHandler(store Store, hrStore HRIdentityStore, siteID string) *teamsBatchHandler {
	cache, _ := lru.New[string, resolvedSender](teamsSenderCacheSize) // errors only on size<=0
	resolver := newSenderResolver(hrStore, siteID, cache)
	return &teamsBatchHandler{
		store:    store,
		siteID:   siteID,
		resolver: resolver,
		tr:       NewDefaultTransformer(resolver),
	}
}

// consume decodes one batch message and settles it: Ack when every message was
// handled (per-message errors are logged, not redelivered), Nak on an infra
// failure so at-least-once redelivery re-runs the idempotent batch.
func (h *teamsBatchHandler) consume(ctx context.Context, msg jetstream.Msg) {
	var req model.TeamsBatchRequest
	if err := sonic.Unmarshal(msg.Data(), &req); err != nil {
		// A malformed batch can never parse on redelivery — drop it as poison.
		jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, errcode.Permanent(errcode.BadRequest("malformed teams batch")))
		return
	}
	jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, h.handleBatch(ctx, req))
}

// handleBatch runs each message. Returns the first infra error so the caller Naks —
// per-message transform/resolve errors are logged and don't block the batch.
func (h *teamsBatchHandler) handleBatch(ctx context.Context, req model.TeamsBatchRequest) error {
	var persisted, skipped, failed int
	for _, raw := range req.Messages {
		res, err := h.migrateOne(ctx, raw)
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

func (h *teamsBatchHandler) migrateOne(ctx context.Context, raw json.RawMessage) (model.TeamsBatchResult, error) {
	head, ok := peekTeamsHead(raw)
	res := model.TeamsBatchResult{TeamsMsgID: head.ID}
	if !ok {
		slog.WarnContext(ctx, "teams batch: skip malformed message payload")
		res.Status, res.Error = model.TeamsBatchError, "malformed message payload"
		return res, nil
	}
	if head.ID == "" {
		slog.WarnContext(ctx, "teams batch: skip message with no id")
		res.Status = model.TeamsBatchSkipped
		return res, nil
	}
	if head.RoomID == "" {
		slog.WarnContext(ctx, "teams batch: skip message with no roomId", "teamsMsgId", head.ID)
		res.Status = model.TeamsBatchSkipped
		return res, nil
	}

	msg, err := h.tr.Transform(ctx, raw)
	if err != nil {
		slog.WarnContext(ctx, "teams batch: skip message, transform failed", "teamsMsgId", head.ID, "error", err)
		res.Status, res.Error = model.TeamsBatchError, "transform failed"
		return res, nil //nolint:nilerr // per-message transform error is isolated, not a batch Nak
	}
	// Teams ids are unique only per conversation → scope by roomId to avoid collisions.
	msg.ID = teamsmigrate.DeterministicMessageID(head.RoomID, head.ID)

	// Cache hit after Transform (same sender key), so it adds no store round trip.
	sender, err := h.resolver.resolve(ctx, head.From.ID, head.From.DisplayName)
	if err != nil {
		slog.WarnContext(ctx, "teams batch: skip message, resolve sender failed", "teamsMsgId", head.ID, "error", err)
		res.Status, res.Error = model.TeamsBatchError, "resolve sender failed"
		return res, nil //nolint:nilerr // per-message resolve error is isolated, not a batch Nak
	}
	cass := &cassParticipant{ID: sender.UserID, EngName: sender.EngName, CompanyName: sender.ChineseName, Account: sender.Account}

	if err := h.store.SaveMessage(ctx, &msg, cass, h.siteID); err != nil {
		// A persist failure is infra: surface it so the batch Naks and replays.
		slog.ErrorContext(ctx, "teams batch: save message failed", "teamsMsgId", head.ID, "error", err)
		res.Status, res.Error = model.TeamsBatchError, "save message failed"
		return res, err
	}
	res.Status = model.TeamsBatchPersisted
	return res, nil
}

// teamsHead is the minimal envelope needed before transforming: the source id, its
// conversation scope (roomId), and the sender identity (from) for the Cassandra write.
type teamsHead struct {
	ID     string `json:"id"`
	RoomID string `json:"roomId"`
	From   struct {
		ID          string `json:"id"`
		DisplayName string `json:"displayName"`
	} `json:"from"`
}

// peekTeamsHead reads the id + roomId + from; ok is false on malformed JSON (an error
// result), true with an empty id when the field is absent (a skip).
func peekTeamsHead(raw json.RawMessage) (h teamsHead, ok bool) {
	if err := json.Unmarshal(raw, &h); err != nil {
		return teamsHead{}, false
	}
	return h, true
}
