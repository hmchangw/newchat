package preview

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// DEKCollection holds the wrapped per-site preview DEKs, kept apart from the room DEKs:
// losing one is a cache miss, losing the other is permanent history loss.
const DEKCollection = "preview_deks"

// Key identifies the preview DEK: one per site per epoch, since a read enumerates up to
// 1000 rooms. Never seal message bodies with it — its GCM invocation count is far higher.
type Key struct {
	SiteID string
	Epoch  int
}

// ID is the sentinel atrest takes for a roomID; the colon cannot collide with a room id.
func (k Key) ID() string {
	return "preview:" + k.SiteID + ":" + strconv.Itoa(k.Epoch)
}

// Sealed is a stored room preview: clear metadata plus the sealed body. ForMsgID is the
// freshness key the caller supplies — the newest message id OBSERVED, not Meta.MessageID.
type Sealed struct {
	Meta       model.PreviewMeta
	Ciphertext []byte
	Nonce      []byte
	KeyEpoch   int
	ForMsgID   string
}

// Seal splits p into clear meta (already plaintext in Cassandra) and an encrypted body.
//
//nolint:gocritic // hugeParam: PreviewMessage is the wire shape itself; by-value keeps callers simple and the copy cost is negligible.
func Seal(ctx context.Context, c atrest.Cipher, k Key, forMsgID string, p model.PreviewMessage) (Sealed, error) {
	atts, err := encodeAttachments(p.Attachments)
	if err != nil {
		return Sealed{}, fmt.Errorf("encode preview attachments: %w", err)
	}

	ciphertext, meta, err := c.Encrypt(ctx, k.ID(), atrest.EncryptedFields{
		Msg:         p.Content,
		Attachments: atts,
	})
	if err != nil {
		return Sealed{}, fmt.Errorf("seal preview body: %w", err)
	}

	return Sealed{
		Meta: model.PreviewMeta{
			MessageID: p.MessageID,
			Sender:    p.Sender,
			CreatedAt: p.CreatedAt,
			Mentions:  p.Mentions,
			VisibleTo: p.VisibleTo,
		},
		Ciphertext: ciphertext,
		Nonce:      meta.Nonce,
		KeyEpoch:   k.Epoch,
		ForMsgID:   forMsgID,
	}, nil
}

// Open is the inverse of Seal; the DEK comes from s.KeyEpoch, never a retired one.
//
//nolint:gocritic // hugeParam: Sealed is the stored shape itself; by-value matches Seal's signature.
func Open(ctx context.Context, c atrest.Cipher, siteID string, s Sealed) (model.PreviewMessage, error) {
	k := Key{SiteID: siteID, Epoch: s.KeyEpoch}

	fields, err := c.Decrypt(ctx, k.ID(), s.Ciphertext, atrest.EncMeta{Nonce: s.Nonce})
	if err != nil {
		return model.PreviewMessage{}, fmt.Errorf("open preview body: %w", err)
	}

	atts, skipped := cassandra.DecodeAttachments(fields.Attachments)
	if skipped > 0 {
		// Costs a render detail, not the preview — but we sealed these, so a skip is rot.
		slog.WarnContext(ctx, "preview attachment blob undecodable",
			"messageId", s.Meta.MessageID, "skipped", skipped)
	}

	return model.PreviewMessage{
		MessageID:   s.Meta.MessageID,
		Sender:      s.Meta.Sender,
		Content:     fields.Msg,
		CreatedAt:   s.Meta.CreatedAt,
		Attachments: atts,
		Mentions:    s.Meta.Mentions,
		VisibleTo:   s.Meta.VisibleTo,
	}, nil
}

// encodeAttachments marshals attachments to the [][]byte atrest stores. The length check
// is the point: the shared encoder skips bad elements, which would silently drop one.
func encodeAttachments(atts []cassandra.Attachment) ([][]byte, error) {
	out := cassandra.EncodeAttachments(atts)
	if len(out) != len(atts) {
		return nil, fmt.Errorf("encode %d preview attachment(s): %d unmarshalable", len(atts), len(atts)-len(out))
	}
	return out, nil
}
