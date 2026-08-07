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

// Key identifies the preview DEK. Scope is one key per site per epoch, shared
// by every room on the site: preview reads are driven by membership
// enumeration over up to MAX_SUBSCRIPTION_LIMIT rooms, which is the one-shot
// cold-tail scan atrest's 2Q DEK cache resists rather than serves. One site key
// is unwrapped once and cached for the process lifetime.
//
// The preview DEK must never be reused for message bodies: it sees far more
// GCM invocations than a per-room DEK, and a nonce collision would leak the
// GHASH subkey for everything sealed under it.
type Key struct {
	SiteID string
	Epoch  int
}

// ID renders the sentinel passed to atrest in place of a roomID. The colon
// cannot collide with a room id — those are base62, 32-char hex, or a concat of
// two of those, none of which contain one.
func (k Key) ID() string {
	return "preview:" + k.SiteID + ":" + strconv.Itoa(k.Epoch)
}

// Sealed is a room preview in its stored form: the plaintext half that mirrors
// what Cassandra leaves unencrypted, plus the sealed body and the crypto
// material needed to open it.
//
// ForMsgID is the freshness key and is supplied by the caller, not derived from
// the preview: it must be the newest message id the resolving walk OBSERVED in
// Cassandra, which differs from Meta.MessageID whenever the newest message was
// ineligible and skipped.
type Sealed struct {
	Meta       model.PreviewMeta
	Ciphertext []byte
	Nonce      []byte
	KeyEpoch   int
	ForMsgID   string
}

// marshalAttachment is an indirection over json.Marshal so the encode-failure
// path stays reachable in tests. Marshalling a plain Attachment cannot fail in
// practice, but the error is surfaced rather than swallowed — see
// encodeAttachments.
var marshalAttachment = json.Marshal

// Seal splits p into its plaintext meta and its encrypted body, sealing exactly
// the two fields atrest classifies as user-authored (Content, Attachments)
// under the site preview DEK for k. Sender, mentions, createdAt and visibleTo
// stay clear: message-worker already writes those to plaintext Cassandra
// columns, so storing them introduces no new classification.
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

// Open is the inverse of Seal, recomposing the wire preview from the stored
// halves. The DEK is selected by s.KeyEpoch, so a reader configured for a
// different epoch never reaches a retired key — callers check the epoch before
// calling and re-resolve on mismatch.
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
		// Not fatal: a preview is derived data, so a mangled attachment blob
		// costs a render detail rather than the whole preview. Log because we
		// sealed these ourselves — a skip here means storage corruption.
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

// encodeAttachments marshals each attachment into the [][]byte form atrest
// stores natively, so sealing is a round-trip rather than a re-encoding. Unlike
// cassandra.EncodeAttachments it fails the batch instead of skipping a bad
// element: a silently dropped attachment inside a sealed preview is
// indistinguishable downstream from a message that never had one.
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
