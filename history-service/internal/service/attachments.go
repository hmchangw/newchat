package service

import (
	"context"
	"log/slog"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// setDecodedAttachments decodes each message's (and its quoted parent's)
// attachments in place. See decodeMessageAttachments.
func setDecodedAttachments(ctx context.Context, msgs []models.Message) {
	for i := range msgs {
		decodeMessageAttachments(ctx, &msgs[i])
	}
}

// decodeMessageAttachments fills m's DecodedAttachments (and its quoted parent's)
// from the raw LIST<BLOB> attachments. Lenient: malformed blobs are logged and
// skipped so one bad row can't fail a history load. Call it after redaction and
// just before returning — a redacted stub has its raw Attachments already nil'd,
// so it decodes to nil.
func decodeMessageAttachments(ctx context.Context, m *models.Message) {
	decode := func(raw [][]byte, what string) []cassandra.Attachment {
		out, skipped := cassandra.DecodeAttachments(raw)
		if skipped > 0 {
			slog.WarnContext(ctx, "skipped malformed "+what+" attachment blobs",
				"messageId", m.MessageID, "skipped", skipped)
		}
		return out
	}
	m.DecodedAttachments = decode(m.Attachments, "message")
	if qp := m.QuotedParentMessage; qp != nil {
		qp.DecodedAttachments = decode(qp.Attachments, "quoted-parent")
	}
}
