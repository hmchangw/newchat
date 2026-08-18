package cassrepo

import (
	"context"
	"errors"
	"fmt"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/atrest"
)

// ErrEncryptedRowCipherDisabled is returned when a reader encounters an
// encrypted row but the repository was constructed without a cipher
// (ATREST_ENABLED=false). Callers can detect this via errors.Is.
var ErrEncryptedRowCipherDisabled = errors.New("encrypted row encountered but cipher is disabled")

// decryptIfNeeded decrypts m's enc_payload in place when present. When
// enc_payload is nil the row is treated as legacy plaintext and m is
// returned unchanged. When the cipher is nil and enc_payload is non-nil,
// ErrEncryptedRowCipherDisabled is returned.
// decryptRows decrypts msgs in place. Rows carrying no enc_payload pass through
// untouched, so a bucket written across an at-rest rollout (some rows sealed,
// some not) needs no special casing. Used on the cached read path, where rows
// arrive in the sealed form loadSealedBucket stored.
func (r *Repository) decryptRows(ctx context.Context, msgs []models.Message) error {
	for i := range msgs {
		if err := r.decryptIfNeeded(ctx, &msgs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) decryptIfNeeded(ctx context.Context, m *models.Message) error {
	if len(m.EncPayload) == 0 {
		return nil
	}
	if r.cipher == nil {
		return fmt.Errorf("%w (room=%s message=%s)", ErrEncryptedRowCipherDisabled, m.RoomID, m.MessageID)
	}
	meta := atrest.EncMeta{}
	if m.EncMeta != nil {
		meta.Nonce = m.EncMeta.Nonce
	}
	fields, err := r.cipher.Decrypt(ctx, m.RoomID, m.EncPayload, meta)
	if err != nil {
		return fmt.Errorf("decrypt message %s in room %s: %w", m.MessageID, m.RoomID, err)
	}
	atrest.ApplyDecryptedFields(m, &fields)
	// Clear the on-row encrypted fields so callers above the cassrepo
	// layer never see them.
	m.EncPayload = nil
	m.EncMeta = nil
	return nil
}
