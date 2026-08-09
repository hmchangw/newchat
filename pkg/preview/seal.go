package preview

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// Key identifies the preview DEK: one per site per epoch, shared by every room.
// Per-room keys are the wrong granularity — preview reads enumerate up to 1000
// rooms at once, the cold-tail scan atrest's 2Q DEK cache resists rather than
// serves.
//
// This key must never seal message bodies: it sees far more GCM invocations than
// a per-room DEK, and a nonce collision would leak the GHASH subkey for
// everything under it.
type Key struct {
	SiteID string
	Epoch  int
}

// ID renders the sentinel atrest takes in place of a roomID. The colon cannot
// collide with a room id (base62, 32-char hex, or a concat of those).
func (k Key) ID() string {
	return "preview:" + k.SiteID + ":" + strconv.Itoa(k.Epoch)
}

// Sealed is a stored room preview: the plaintext half Cassandra also leaves
// unencrypted, plus the sealed body and the material to open it.
//
// ForMsgID is the freshness key, supplied by the caller — the newest message id
// the walk OBSERVED, not Meta.MessageID.
type Sealed struct {
	Meta       model.PreviewMeta
	Ciphertext []byte
	Nonce      []byte
	KeyEpoch   int
	ForMsgID   string
}

// marshalAttachment is a seam over json.Marshal keeping encodeAttachments' error
// path reachable in tests; marshalling a plain Attachment cannot fail in practice.
var marshalAttachment = json.Marshal

// Seal splits p into plaintext meta and an encrypted body, sealing the two fields
// atrest treats as user-authored (Content, Attachments) under k. The rest stays
// clear — message-worker already writes it to plaintext Cassandra columns.
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

// Open is the inverse of Seal. The DEK comes from s.KeyEpoch, so a reader on a
// different epoch never reaches a retired key; callers check the epoch first.
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
		// Derived data: a mangled blob costs a render detail, not the preview.
		// Worth logging — we sealed these, so a skip means storage corruption.
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

// encodeAttachments marshals each attachment into the [][]byte form atrest stores
// natively. Unlike cassandra.EncodeAttachments it fails the batch rather than
// skipping a bad element: a dropped attachment in a sealed preview is
// indistinguishable downstream from one that never existed.
func encodeAttachments(atts []cassandra.Attachment) ([][]byte, error) {
	if len(atts) == 0 {
		return nil, nil
	}
	out := make([][]byte, 0, len(atts))
	for i := range atts {
		b, err := marshalAttachment(atts[i])
		if err != nil {
			return nil, fmt.Errorf("attachment %d: %w", i, err)
		}
		out = append(out, b)
	}
	return out, nil
}
