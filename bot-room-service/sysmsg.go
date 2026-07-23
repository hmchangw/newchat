package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// Sysmsg emission — LOCAL ONLY, never federated via OUTBOX.
// Remote members learn membership from the member_added event instead.
// Add/remove emit only when the diff is non-empty; create always emits (owner is the seed).
// Wire shape is a raw model.Message, matching bot-msg-handler on the same subject.

// jsPublishAdapter narrows o11ynats.JetStream to sysmsgPublisher.
type jsPublishAdapter struct {
	js interface {
		Publish(ctx context.Context, subj string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	}
}

func (a jsPublishAdapter) PublishWithMsgID(ctx context.Context, subj string, data []byte, msgID string) error {
	if _, err := a.js.Publish(ctx, subj, data, jetstream.WithMsgID(msgID)); err != nil {
		return fmt.Errorf("bot canonical publish: %w", err)
	}
	return nil
}

// emitSysmsg publishes a Type=msgType message with SysMsgData=payload to the local canonical stream.
// Failures are logged, not returned — the state write already succeeded (best-effort sysmsg).
func (h *handler) emitSysmsg(ctx context.Context, roomID string, caller *BotIdentity, msgType string, payload any, dedupSuffix string) {
	if h.sysmsgPub == nil {
		return
	}
	data, err := json.Marshal(payload)
	if err != nil {
		slog.WarnContext(ctx, "bot-room-service sysmsg marshal failed",
			"roomID", roomID, "type", msgType, "error", err)
		return
	}
	now := h.now()
	msg := model.Message{
		ID:          h.newMsgID(),
		RoomID:      roomID,
		UserID:      caller.ID,
		UserAccount: caller.Account,
		Type:        msgType,
		SysMsgData:  data,
		CreatedAt:   now,
	}
	envelope, err := json.Marshal(msg)
	if err != nil {
		slog.WarnContext(ctx, "bot-room-service sysmsg envelope marshal failed",
			"roomID", roomID, "type", msgType, "error", err)
		return
	}
	// Deterministic dedup key: room+type+caller-supplied suffix defeats double-emit on retry.
	pubCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	msgID := fmt.Sprintf("bot-sysmsg:%s:%s:%s", roomID, msgType, dedupSuffix)
	if err := h.sysmsgPub.PublishWithMsgID(pubCtx, subject.BotCanonicalCreated(h.siteID), envelope, msgID); err != nil {
		slog.WarnContext(ctx, "bot-room-service sysmsg publish failed",
			"roomID", roomID, "type", msgType, "error", err)
	}
}
