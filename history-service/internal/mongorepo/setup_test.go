//go:build integration

package mongorepo

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/preview"
	"github.com/hmchangw/chat/pkg/testutil"
)

func setupMongo(t *testing.T) *mongo.Database {
	return testutil.MongoDB(t, "history_service_test")
}

// testPreviewCipher is a real AES-GCM cipher keyed by sha256(keyID), standing in
// for the Vault-backed atrest.Cipher. Real crypto (not a passthrough) so these
// tests exercise the actual seal/open round-trip and a wrong-epoch read genuinely
// fails authentication.
type testPreviewCipher struct{}

func (testPreviewCipher) aead(keyID string) (cipher.AEAD, error) {
	sum := sha256.Sum256([]byte(keyID))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func (c testPreviewCipher) Encrypt(_ context.Context, keyID string, fields atrest.EncryptedFields) ([]byte, atrest.EncMeta, error) { //nolint:gocritic // hugeParam: matches the atrest.Cipher interface
	plaintext, err := json.Marshal(fields)
	if err != nil {
		return nil, atrest.EncMeta{}, err
	}
	gcm, err := c.aead(keyID)
	if err != nil {
		return nil, atrest.EncMeta{}, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, atrest.EncMeta{}, err
	}
	return gcm.Seal(nil, nonce, plaintext, nil), atrest.EncMeta{Nonce: nonce}, nil
}

func (c testPreviewCipher) Decrypt(_ context.Context, keyID string, payload []byte, meta atrest.EncMeta) (atrest.EncryptedFields, error) { //nolint:gocritic // hugeParam: matches the atrest.Cipher interface
	gcm, err := c.aead(keyID)
	if err != nil {
		return atrest.EncryptedFields{}, err
	}
	plaintext, err := gcm.Open(nil, meta.Nonce, payload, nil)
	if err != nil {
		return atrest.EncryptedFields{}, atrest.ErrAuthFailed
	}
	var out atrest.EncryptedFields
	if err := json.Unmarshal(plaintext, &out); err != nil {
		return atrest.EncryptedFields{}, atrest.ErrPayloadMalformed
	}
	return out, nil
}

func (testPreviewCipher) EnsureDEK(context.Context, string) error { return nil }

const testPreviewSite = "site-A"

// newRoomRepoAtEpoch builds a RoomRepo whose preview cipher is live at the given
// key epoch, so rotation behaviour can be exercised by reading a doc back through
// a repo on a different epoch.
func newRoomRepoAtEpoch(db *mongo.Database, epoch int) *RoomRepo {
	return NewRoomRepo(db, testPreviewCipher{}, preview.Key{SiteID: testPreviewSite, Epoch: epoch})
}
