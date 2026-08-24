package preview

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// fakeCipher is a real AES-GCM cipher keyed by sha256(keyID), so a wrong-key
// open fails authentication for the same reason the production cipher would.
type fakeCipher struct {
	encErr error
	decErr error
}

func aeadFor(keyID string) (cipher.AEAD, error) {
	sum := sha256.Sum256([]byte(keyID))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func (f fakeCipher) Encrypt(_ context.Context, keyID string, fields atrest.EncryptedFields) ([]byte, atrest.EncMeta, error) { //nolint:gocritic // hugeParam: matches the atrest.Cipher interface
	if f.encErr != nil {
		return nil, atrest.EncMeta{}, f.encErr
	}
	plaintext, err := json.Marshal(fields)
	if err != nil {
		return nil, atrest.EncMeta{}, err
	}
	gcm, err := aeadFor(keyID)
	if err != nil {
		return nil, atrest.EncMeta{}, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, atrest.EncMeta{}, err
	}
	return gcm.Seal(nil, nonce, plaintext, nil), atrest.EncMeta{Nonce: nonce}, nil
}

func (f fakeCipher) Decrypt(_ context.Context, keyID string, payload []byte, meta atrest.EncMeta) (atrest.EncryptedFields, error) { //nolint:gocritic // hugeParam: matches the atrest.Cipher interface
	if f.decErr != nil {
		return atrest.EncryptedFields{}, f.decErr
	}
	gcm, err := aeadFor(keyID)
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

func (f fakeCipher) EnsureDEK(context.Context, string) error { return nil }

func samplePreview() model.PreviewMessage {
	return model.PreviewMessage{
		MessageID: "msg-1",
		Sender:    model.Participant{Account: "alice", EngName: "Alice", DisplayName: "Alice A"},
		Content:   "the quick brown fox",
		CreatedAt: time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC),
		Attachments: []cassandra.Attachment{{
			ID: "f1", Title: "pic.png", Type: "image", TitleLink: "/f1",
			FileType: "image/png", ImageURL: "/f1/img", ImageSize: 42,
			ImageDimensions: &cassandra.ImageDimensions{Width: 10, Height: 20},
		}},
		Mentions: []model.Participant{{Account: "bob"}},
	}
}

func TestKey_ID(t *testing.T) {
	assert.Equal(t, "preview:site-a:1", Key{SiteID: "site-a", Epoch: 1}.ID())
	assert.Equal(t, "preview:site-a:7", Key{SiteID: "site-a", Epoch: 7}.ID())

	// A different epoch must select a different DEK, or rotation is a no-op.
	assert.NotEqual(t, Key{SiteID: "s", Epoch: 1}.ID(), Key{SiteID: "s", Epoch: 2}.ID())

	// The sentinel must not collide with a room id. Room ids are base62
	// (channel/message), 32-char hex (UUIDv7), or a concat of two of those —
	// none can contain a colon.
	assert.Contains(t, Key{SiteID: "s", Epoch: 1}.ID(), ":")
}

func TestSeal_Open_RoundTrip(t *testing.T) {
	ctx := context.Background()
	key := Key{SiteID: "site-a", Epoch: 1}
	pvw := samplePreview()

	sealed, err := Seal(ctx, fakeCipher{}, key, "msg-2", pvw)
	require.NoError(t, err)

	// Freshness key is what the caller observed, independent of the preview's
	// own message id.
	assert.Equal(t, "msg-2", sealed.ForMsgID)
	assert.Equal(t, 1, sealed.KeyEpoch)
	assert.NotEmpty(t, sealed.Ciphertext)
	assert.NotEmpty(t, sealed.Nonce)

	got, err := Open(ctx, fakeCipher{}, key.SiteID, sealed)
	require.NoError(t, err)
	assert.Equal(t, pvw, got)
}

func TestSeal_RoundTripWithoutAttachments(t *testing.T) {
	ctx := context.Background()
	key := Key{SiteID: "site-a", Epoch: 1}
	pvw := samplePreview()
	pvw.Attachments = nil

	sealed, err := Seal(ctx, fakeCipher{}, key, "msg-1", pvw)
	require.NoError(t, err)

	got, err := Open(ctx, fakeCipher{}, key.SiteID, sealed)
	require.NoError(t, err)
	assert.Equal(t, pvw, got)
	assert.Nil(t, got.Attachments)
}

// The body must leave the process sealed: neither the plaintext meta nor the
// ciphertext may expose Content or attachment titles.
func TestSeal_BodyNeverStoredInClear(t *testing.T) {
	sealed, err := Seal(context.Background(), fakeCipher{}, Key{SiteID: "site-a", Epoch: 1}, "msg-1", samplePreview())
	require.NoError(t, err)

	// Meta carries exactly the fields Cassandra leaves unencrypted.
	assert.Equal(t, "msg-1", sealed.Meta.MessageID)
	assert.Equal(t, "alice", sealed.Meta.Sender.Account)
	assert.Equal(t, []model.Participant{{Account: "bob"}}, sealed.Meta.Mentions)

	assert.False(t, bytes.Contains(sealed.Ciphertext, []byte("the quick brown fox")),
		"message body must not survive in the ciphertext")
	assert.False(t, bytes.Contains(sealed.Ciphertext, []byte("pic.png")),
		"attachment title must not survive in the ciphertext")
}

// Meta.MessageID is the preview's own id, which differs from ForMsgID whenever
// the newest message was ineligible and skipped by the walk.
func TestSeal_MetaMessageIDIsIndependentOfForMsgID(t *testing.T) {
	sealed, err := Seal(context.Background(), fakeCipher{}, Key{SiteID: "s", Epoch: 1}, "newest-99", samplePreview())
	require.NoError(t, err)

	assert.Equal(t, "msg-1", sealed.Meta.MessageID)
	assert.Equal(t, "newest-99", sealed.ForMsgID)
}

func TestOpen_WrongKey(t *testing.T) {
	ctx := context.Background()
	sealed, err := Seal(ctx, fakeCipher{}, Key{SiteID: "site-a", Epoch: 1}, "msg-1", samplePreview())
	require.NoError(t, err)

	t.Run("different epoch", func(t *testing.T) {
		wrong := sealed
		wrong.KeyEpoch = 2 // selects preview:site-a:2, a different DEK
		_, err := Open(ctx, fakeCipher{}, "site-a", wrong)
		require.Error(t, err)
		assert.ErrorIs(t, err, atrest.ErrAuthFailed)
	})

	t.Run("different site", func(t *testing.T) {
		_, err := Open(ctx, fakeCipher{}, "site-b", sealed)
		require.Error(t, err)
		assert.ErrorIs(t, err, atrest.ErrAuthFailed)
	})
}

// A mangled attachment blob costs a render detail, not the whole preview:
// previews are derived data, so Open degrades rather than failing.
func TestOpen_UndecodableAttachmentSkipped(t *testing.T) {
	ctx := context.Background()
	key := Key{SiteID: "site-a", Epoch: 1}

	ciphertext, meta, err := fakeCipher{}.Encrypt(ctx, key.ID(), atrest.EncryptedFields{
		Msg:         "hello",
		Attachments: [][]byte{[]byte("{not json")},
	})
	require.NoError(t, err)

	got, err := Open(ctx, fakeCipher{}, key.SiteID, Sealed{
		Meta:       model.PreviewMeta{MessageID: "m1"},
		Ciphertext: ciphertext,
		Nonce:      meta.Nonce,
		KeyEpoch:   key.Epoch,
	})
	require.NoError(t, err)
	assert.Equal(t, "hello", got.Content)
	assert.Empty(t, got.Attachments)
}

func TestSeal_CipherError(t *testing.T) {
	boom := errors.New("vault down")
	_, err := Seal(context.Background(), fakeCipher{encErr: boom}, Key{SiteID: "s", Epoch: 1}, "msg-1", samplePreview())
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
}

func TestOpen_CipherError(t *testing.T) {
	ctx := context.Background()
	sealed, err := Seal(ctx, fakeCipher{}, Key{SiteID: "s", Epoch: 1}, "msg-1", samplePreview())
	require.NoError(t, err)

	boom := errors.New("vault down")
	_, err = Open(ctx, fakeCipher{decErr: boom}, "s", sealed)
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
}
