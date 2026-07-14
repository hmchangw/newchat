package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/subject"
)

// historyRequestTimeout matches the nats.go default request timeout.
const historyRequestTimeout = 2 * time.Second

// historyMessageReader resolves a message via history-service's GetMessageByID
// NATS handler. Message history is owned by history-service, so routing the
// read-receipt lookup through it lets room-service drop its direct Cassandra
// dependency entirely.
type historyMessageReader struct {
	nc     *otelnats.Conn
	siteID string
}

func newHistoryMessageReader(nc *otelnats.Conn, siteID string) *historyMessageReader {
	return &historyMessageReader{nc: nc, siteID: siteID}
}

// getMessageByIDRequest mirrors history-service's GetMessageByIDRequest wire
// shape (the source struct lives under internal/ and isn't importable).
type getMessageByIDRequest struct {
	MessageID string `json:"messageId"`
}

// GetMessageReadMeta issues a NATS request to history-service's
// GetMessageByID handler, scoped to (account, roomID). history-service looks the
// message up within roomID, so a message that does not exist there comes back as
// NotFound, which maps to found=false. A no-responder/timeout degrades to
// errcode.Unavailable so read receipts fail soft instead of erroring hard.
func (r *historyMessageReader) GetMessageReadMeta(
	ctx context.Context, account, roomID, messageID string,
) (MessageReadMeta, bool, error) {
	reqBytes, err := json.Marshal(getMessageByIDRequest{MessageID: messageID})
	if err != nil {
		return MessageReadMeta{}, false, fmt.Errorf("marshal get-message request: %w", err)
	}

	msg, err := r.nc.Request(ctx, subject.MsgGet(account, roomID, r.siteID), reqBytes, historyRequestTimeout)
	if err != nil {
		return MessageReadMeta{}, false, errcode.Unavailable("read receipts are temporarily unavailable",
			errcode.WithReason(errcode.RoomReadReceiptsUnavailable),
			errcode.WithCause(err))
	}

	// An errcode envelope (a real Message has no top-level "error" field, so this
	// cannot false-positive). NotFound maps to found=false so the handler returns
	// its canonical errMessageNotFound; other classifications propagate intact.
	if ee, ok := errcode.Parse(msg.Data); ok && ee.Code.Valid() {
		if ee.Code == errcode.CodeNotFound {
			return MessageReadMeta{}, false, nil
		}
		return MessageReadMeta{}, false, ee
	}

	// Decode a narrow projection, not the full cassandra.Message: that type embeds
	// the marshal-only Reactions map (struct-keyed, no UnmarshalJSON), so a reacted
	// message fails to decode. Mirrors message-gatekeeper's quotedParentProjection.
	var m messageProjection
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		return MessageReadMeta{}, false, fmt.Errorf("unmarshal message: %w", err)
	}
	return MessageReadMeta{
		RoomID:       m.RoomID,
		CreatedAt:    m.CreatedAt,
		Sender:       m.Sender.Account,
		ThreadRoomID: m.ThreadRoomID,
		// Thread-only = a reply (threadParentId set) not mirrored to the channel.
		ThreadOnly: m.ThreadParentID != "" && !m.TShow,
	}, true, nil
}

// messageProjection decodes only the fields the read-receipt lookup needs.
type messageProjection struct {
	RoomID         string                `json:"roomId"`
	CreatedAt      time.Time             `json:"createdAt"`
	Sender         cassandra.Participant `json:"sender"`
	ThreadParentID string                `json:"threadParentId"`
	ThreadRoomID   string                `json:"threadRoomId"`
	TShow          bool                  `json:"tshow"`
}
