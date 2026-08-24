//go:build integration

package mongorepo

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/preview"
)

// previewCipher is real AES-GCM keyed by sha256(keyID), so a wrong epoch fails
// authentication as a production per-epoch DEK would, rather than silently decrypting.
type previewCipher struct{}

func (previewCipher) aead(keyID string) (cipher.AEAD, error) {
	sum := sha256.Sum256([]byte(keyID))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}
	return gcm, nil
}

func (c previewCipher) Encrypt(_ context.Context, keyID string, fields atrest.EncryptedFields) ([]byte, atrest.EncMeta, error) { //nolint:gocritic // hugeParam: matches the atrest.Cipher interface
	gcm, err := c.aead(keyID)
	if err != nil {
		return nil, atrest.EncMeta{}, fmt.Errorf("create preview AEAD: %w", err)
	}
	plaintext, err := json.Marshal(fields)
	if err != nil {
		return nil, atrest.EncMeta{}, fmt.Errorf("marshal preview fields: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, atrest.EncMeta{}, fmt.Errorf("generate preview nonce: %w", err)
	}
	return gcm.Seal(nil, nonce, plaintext, nil), atrest.EncMeta{Nonce: nonce}, nil
}

func (c previewCipher) Decrypt(_ context.Context, keyID string, payload []byte, meta atrest.EncMeta) (atrest.EncryptedFields, error) { //nolint:gocritic // hugeParam: matches the atrest.Cipher interface
	gcm, err := c.aead(keyID)
	if err != nil {
		return atrest.EncryptedFields{}, fmt.Errorf("create preview AEAD: %w", err)
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

func (previewCipher) EnsureDEK(context.Context, string) error { return nil }

var previewKey = preview.Key{SiteID: "site-A", Epoch: 2}

// samplePreview carries one of every enriched field, so a round-trip proves both halves.
func samplePreview() model.PreviewMessage {
	return model.PreviewMessage{
		MessageID: "m-preview",
		Sender:    model.Participant{UserID: "u1", Account: "alice", EngName: "Alice", ChineseName: "愛麗絲"},
		Content:   "hello",
		CreatedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Attachments: []cassandra.Attachment{
			{ID: "f1", Title: "a.png", Type: "file"},
		},
		Mentions: []model.Participant{{UserID: "u2", Account: "bob", ChineseName: "小明"}},
	}
}

// seedRoomWithPreview seals a preview for forMsgID against lastMsgID; equal means current.
func seedRoomWithPreview(t *testing.T, repo *RoomRepo, roomID, forMsgID, lastMsgID string) {
	t.Helper()
	ctx := context.Background()

	sealed, err := preview.Seal(ctx, previewCipher{}, previewKey, forMsgID, samplePreview())
	require.NoError(t, err)

	_, err = repo.rooms.Raw().InsertOne(ctx, bson.M{
		"_id":               roomID,
		"siteId":            previewKey.SiteID,
		"type":              model.RoomTypeChannel,
		"createdAt":         time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		"lastMsgAt":         time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		"lastMsgId":         lastMsgID,
		"previewMeta":       sealed.Meta,
		"previewCiphertext": sealed.Ciphertext,
		"previewNonce":      sealed.Nonce,
		"previewKeyEpoch":   sealed.KeyEpoch,
		"previewForMsgId":   sealed.ForMsgID,
		"previewAsOf":       int64(1),
	})
	require.NoError(t, err)
}

// A current, same-epoch, decryptable preview round-trips with every field intact.
func TestRoomRepo_GetRoomTimesByIDs_ServesCurrentStoredPreview(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	seedRoomWithPreview(t, repo, "r-current", "m-preview", "m-preview")

	got, err := repo.GetRoomTimesByIDs(context.Background(), []string{"r-current"})
	require.NoError(t, err)
	require.Contains(t, got, "r-current")

	pvw := got["r-current"].Preview
	require.NotNil(t, pvw, "a current preview on the configured epoch must be served")
	assert.Equal(t, "m-preview", pvw.MessageID)
	assert.Equal(t, "hello", pvw.Content)
	assert.Equal(t, "愛麗絲", pvw.Sender.ChineseName)
	require.Len(t, pvw.Attachments, 1)
	assert.Equal(t, "a.png", pvw.Attachments[0].Title)
	require.Len(t, pvw.Mentions, 1)
	assert.Equal(t, "bob", pvw.Mentions[0].Account)
}

// The freshness key is an identity check against lastMsgId: a newer message this preview
// does not describe means it is withheld, not served as current.
func TestRoomRepo_GetRoomTimesByIDs_StalePreviewIsWithheld(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	seedRoomWithPreview(t, repo, "r-stale", "m-old", "m-new")

	got, err := repo.GetRoomTimesByIDs(context.Background(), []string{"r-stale"})
	require.NoError(t, err)
	require.Contains(t, got, "r-stale", "the row itself still comes back")
	assert.Nil(t, got["r-stale"].Preview, "previewForMsgId != lastMsgId must withhold the preview")
}

// A preview on a retired epoch reads as absent, so no reader ever needs a retired DEK.
func TestRoomRepo_GetRoomTimesByIDs_EpochMismatchIsWithheld(t *testing.T) {
	db := setupMongo(t)
	// Seed on the configured epoch, then read through a repo on a later one.
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	seedRoomWithPreview(t, repo, "r-epoch", "m-preview", "m-preview")

	rotated := NewRoomRepo(db, previewCipher{}, preview.Key{SiteID: previewKey.SiteID, Epoch: previewKey.Epoch + 1})
	got, err := rotated.GetRoomTimesByIDs(context.Background(), []string{"r-epoch"})
	require.NoError(t, err)
	require.Contains(t, got, "r-epoch")
	assert.Nil(t, got["r-epoch"].Preview, "a preview on a retired epoch must be withheld")
}

// A ciphertext that fails authentication drops the preview, not the whole read.
func TestRoomRepo_GetRoomTimesByIDs_UndecryptablePreviewIsWithheld(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	seedRoomWithPreview(t, repo, "r-corrupt", "m-preview", "m-preview")

	_, err := repo.rooms.Raw().UpdateByID(context.Background(), "r-corrupt",
		bson.M{"$set": bson.M{"previewCiphertext": []byte("not a valid ciphertext")}})
	require.NoError(t, err)

	got, err := repo.GetRoomTimesByIDs(context.Background(), []string{"r-corrupt"})
	require.NoError(t, err, "an undecryptable preview must not fail the batch")
	require.Contains(t, got, "r-corrupt")
	assert.Nil(t, got["r-corrupt"].Preview)
}

// With no cipher the preview fields are unreadable, so the row returns times and no preview.
func TestRoomRepo_GetRoomTimesByIDs_NoCipherWithholdsPreview(t *testing.T) {
	db := setupMongo(t)
	seeder := NewRoomRepo(db, previewCipher{}, previewKey)
	seedRoomWithPreview(t, seeder, "r-nocipher", "m-preview", "m-preview")

	repo := NewRoomRepo(db, nil, previewKey)
	got, err := repo.GetRoomTimesByIDs(context.Background(), []string{"r-nocipher"})
	require.NoError(t, err)
	require.Contains(t, got, "r-nocipher")
	assert.Nil(t, got["r-nocipher"].Preview)
}

// seedBareRoom stores a room with no preview — the pre-rollout shape.
func seedBareRoom(t *testing.T, repo *RoomRepo, roomID, lastMsgID string) {
	t.Helper()
	_, err := repo.rooms.Raw().InsertOne(context.Background(), bson.M{
		"_id":       roomID,
		"siteId":    previewKey.SiteID,
		"type":      model.RoomTypeChannel,
		"createdAt": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		"lastMsgAt": time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		"lastMsgId": lastMsgID,
	})
	require.NoError(t, err)
}

// The warm-back round-trip: a room with no preview gets one, and the next read serves it.
func TestRoomRepo_SetPreviewMessage_WarmsBackAndIsThenServed(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	ctx := context.Background()
	seedBareRoom(t, repo, "r-warm", "m-newest")

	before, err := repo.GetRoomTimesByIDs(ctx, []string{"r-warm"})
	require.NoError(t, err)
	require.Nil(t, before["r-warm"].Preview, "precondition: the room starts with no stored preview")

	// forMsgID is the newest message the walk observed, which is lastMsgId here.
	require.NoError(t, repo.SetPreviewMessage(ctx, "r-warm", samplePreview(), "m-newest", 100))

	after, err := repo.GetRoomTimesByIDs(ctx, []string{"r-warm"})
	require.NoError(t, err)
	pvw := after["r-warm"].Preview
	require.NotNil(t, pvw, "the warmed-back preview must read as current")
	assert.Equal(t, "m-preview", pvw.MessageID)
	assert.Equal(t, "hello", pvw.Content)
	assert.Equal(t, "愛麗絲", pvw.Sender.ChineseName)
}

// Stamped with a non-newest id: stored, but the identity guard withholds it.
func TestRoomRepo_SetPreviewMessage_StaleKeyIsStoredButWithheld(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	ctx := context.Background()
	seedBareRoom(t, repo, "r-raced", "m-newest")

	require.NoError(t, repo.SetPreviewMessage(ctx, "r-raced", samplePreview(), "m-older", 100))

	got, err := repo.GetRoomTimesByIDs(ctx, []string{"r-raced"})
	require.NoError(t, err)
	assert.Nil(t, got["r-raced"].Preview, "previewForMsgId != lastMsgId must withhold")
}

// The watermark is the only coordination between the writers: a late older write loses.
func TestRoomRepo_SetPreviewMessage_OlderAsOfDoesNotRegress(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	ctx := context.Background()
	seedBareRoom(t, repo, "r-guard", "m-newest")

	newer := samplePreview()
	newer.MessageID = "m-newer"
	newer.Content = "newer body"
	require.NoError(t, repo.SetPreviewMessage(ctx, "r-guard", newer, "m-newest", 500))

	older := samplePreview()
	older.Content = "older body"
	require.NoError(t, repo.SetPreviewMessage(ctx, "r-guard", older, "m-newest", 100),
		"a losing guarded write is a no-op, not an error")

	got, err := repo.GetRoomTimesByIDs(ctx, []string{"r-guard"})
	require.NoError(t, err)
	require.NotNil(t, got["r-guard"].Preview)
	assert.Equal(t, "newer body", got["r-guard"].Preview.Content, "the newer write must survive")
}

// A mutation reseals the body only: an edit does not move lastMsgId.
func TestRoomRepo_UpdatePreviewBody_ReplacesBodyKeepingFreshnessKey(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	ctx := context.Background()
	seedRoomWithPreview(t, repo, "r-edit", "m-preview", "m-preview")

	edited := samplePreview()
	edited.Content = "edited content"
	applied, err := repo.UpdatePreviewBody(ctx, "r-edit", edited, "m-preview", 200)
	require.NoError(t, err)
	assert.True(t, applied, "a write that lands must report applied")

	got, err := repo.GetRoomTimesByIDs(ctx, []string{"r-edit"})
	require.NoError(t, err)
	pvw := got["r-edit"].Preview
	require.NotNil(t, pvw, "the preview must still read as current after a body-only update")
	assert.Equal(t, "edited content", pvw.Content)

	var raw bson.M
	require.NoError(t, repo.rooms.Raw().FindOne(ctx, bson.M{"_id": "r-edit"}).Decode(&raw))
	assert.Equal(t, "m-preview", raw["previewForMsgId"], "a mutation must not restamp the freshness key")
}

// An insert is the sole creator: a doc minted by a mutation would carry no key.
func TestRoomRepo_UpdatePreviewBody_RefusesToCreate(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	ctx := context.Background()
	seedBareRoom(t, repo, "r-nopreview", "m-newest")

	applied, err := repo.UpdatePreviewBody(ctx, "r-nopreview", samplePreview(), "m-newest", 200)
	require.NoError(t, err)
	assert.False(t, applied, "refusing to create is a rejected write, not a silent success")

	var raw bson.M
	require.NoError(t, repo.rooms.Raw().FindOne(ctx, bson.M{"_id": "r-nopreview"}).Decode(&raw))
	assert.NotContains(t, raw, "previewCiphertext", "a mutation must not create a preview")
	assert.NotContains(t, raw, "previewForMsgId")
}

// The case the repair exists for: an insert moved the key between the mutation's walk and
// its write. previewAsOf advances on the watermark alone, so a modified document is not
// evidence the body landed.
func TestRoomRepo_UpdatePreviewBody_RejectsAKeyThatMoved(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	ctx := context.Background()
	seedRoomWithPreview(t, repo, "r-raced", "m-new", "m-new")

	edited := samplePreview()
	edited.Content = "edited content"
	applied, err := repo.UpdatePreviewBody(ctx, "r-raced", edited, "m-observed", 200)
	require.NoError(t, err)
	assert.False(t, applied, "a guard that rejected the write must not report applied")

	got, err := repo.GetRoomTimesByIDs(ctx, []string{"r-raced"})
	require.NoError(t, err)
	pvw := got["r-raced"].Preview
	require.NotNil(t, pvw)
	assert.Equal(t, "hello", pvw.Content, "the winning insert's body must survive")
}

// Clearing removes every field together: a nonce describing a gone ciphertext is the risk.
func TestRoomRepo_ClearPreview_RemovesEveryPreviewField(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	ctx := context.Background()
	seedRoomWithPreview(t, repo, "r-cleared", "m-preview", "m-preview")

	cleared, err := repo.ClearPreview(ctx, "r-cleared", 200)
	require.NoError(t, err)
	assert.True(t, cleared, "a clear that lands must report applied")

	var raw bson.M
	require.NoError(t, repo.rooms.Raw().FindOne(ctx, bson.M{"_id": "r-cleared"}).Decode(&raw))
	for _, f := range []string{"previewMeta", "previewCiphertext", "previewNonce", "previewKeyEpoch", "previewForMsgId"} {
		assert.NotContains(t, raw, f, "clearing must remove every preview field")
	}
	assert.Equal(t, int64(200), raw["previewAsOf"], "the watermark must advance so an older redelivery cannot resurrect it")

	got, err := repo.GetRoomTimesByIDs(ctx, []string{"r-cleared"})
	require.NoError(t, err)
	assert.Nil(t, got["r-cleared"].Preview)
}

// Previews off means no write on any path: a nil cipher cannot seal a body.
func TestRoomRepo_PreviewWrites_NoCipherIsANoOp(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, nil, previewKey)
	ctx := context.Background()
	seedBareRoom(t, repo, "r-off", "m-newest")

	require.NoError(t, repo.SetPreviewMessage(ctx, "r-off", samplePreview(), "m-newest", 100))
	updated, err := repo.UpdatePreviewBody(ctx, "r-off", samplePreview(), "m-any", 100)
	require.NoError(t, err)
	assert.False(t, updated, "previews off writes nothing, so nothing was applied")
	cleared, err := repo.ClearPreview(ctx, "r-off", 100)
	require.NoError(t, err)
	assert.False(t, cleared)
	require.NoError(t, repo.InvalidatePreviewKey(ctx, "r-off", "m-any"))

	var raw bson.M
	require.NoError(t, repo.rooms.Raw().FindOne(ctx, bson.M{"_id": "r-off"}).Decode(&raw))
	assert.NotContains(t, raw, "previewCiphertext")
	assert.NotContains(t, raw, "previewAsOf", "a disabled writer must not even advance the watermark")
}

// --- #226: withdrawing certification a mutation could not renew ---

// The shape #226 describes: the body is still stored and still keyed to the room's last
// message, so the reader serves it as current even though the message it describes has
// just been edited or deleted. Withdrawing the key is what makes the next read miss.
func TestRoomRepo_InvalidatePreviewKey_WithholdsThePreviewItNamed(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	ctx := context.Background()
	seedRoomWithPreview(t, repo, "r-withdraw", "m-preview", "m-preview")

	got, err := repo.GetRoomTimesByIDs(ctx, []string{"r-withdraw"})
	require.NoError(t, err)
	require.NotNil(t, got["r-withdraw"].Preview, "precondition: served as current before the repair")

	require.NoError(t, repo.InvalidatePreviewKey(ctx, "r-withdraw", "m-preview"))

	got, err = repo.GetRoomTimesByIDs(ctx, []string{"r-withdraw"})
	require.NoError(t, err)
	require.Contains(t, got, "r-withdraw", "the row itself still comes back")
	assert.Nil(t, got["r-withdraw"].Preview, "a preview whose key was withdrawn must be withheld")
}

// The body survives the repair: a degraded walk is not evidence the room is empty, and
// the read-time walk is what replaces it. The watermark goes with the key, so the
// warm-back that refills the room cannot be rejected by a watermark left behind.
func TestRoomRepo_InvalidatePreviewKey_KeepsTheBodyAndDropsTheWatermark(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	ctx := context.Background()
	seedRoomWithPreview(t, repo, "r-body", "m-preview", "m-preview")

	require.NoError(t, repo.InvalidatePreviewKey(ctx, "r-body", "m-preview"))

	var raw bson.M
	require.NoError(t, repo.rooms.Raw().FindOne(ctx, bson.M{"_id": "r-body"}).Decode(&raw))
	assert.NotContains(t, raw, "previewForMsgId", "the certification comes off")
	assert.NotContains(t, raw, "previewAsOf", "and the watermark protecting it, or the repair cannot land")
	assert.Contains(t, raw, "previewCiphertext", "the body itself must survive")
	assert.Contains(t, raw, "previewMeta")
}

// Keyed on the stored body, so a preview some newer write already replaced is left alone —
// otherwise a slow mutation would knock out a perfectly good preview it has nothing to do
// with, at the cost of a Cassandra walk on the next read.
func TestRoomRepo_InvalidatePreviewKey_LeavesAPreviewOfAnotherMessage(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	ctx := context.Background()
	seedRoomWithPreview(t, repo, "r-other", "m-preview", "m-preview")

	require.NoError(t, repo.InvalidatePreviewKey(ctx, "r-other", "m-someone-else"))

	got, err := repo.GetRoomTimesByIDs(ctx, []string{"r-other"})
	require.NoError(t, err)
	assert.NotNil(t, got["r-other"].Preview, "a body this mutation did not change stays current")
}

// The repair must survive the failure it follows. A seal is what breaks when Vault is
// down, so the invalidate does not seal — it must still land with the cipher unusable.
func TestRoomRepo_InvalidatePreviewKey_NeedsNoSeal(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db, previewCipher{}, previewKey)
	ctx := context.Background()
	seedRoomWithPreview(t, repo, "r-noseal", "m-preview", "m-preview")

	// A repo whose cipher cannot encrypt, standing in for a Vault outage on the seal path.
	broken := NewRoomRepo(db, failingCipher{}, previewKey)
	_, err := broken.UpdatePreviewBody(ctx, "r-noseal", samplePreview(), "m-preview", 200)
	require.Error(t, err, "precondition: the body write is what fails")
	require.NoError(t, broken.InvalidatePreviewKey(ctx, "r-noseal", "m-preview"),
		"the repair must not depend on the seal path that just failed")

	got, err := repo.GetRoomTimesByIDs(ctx, []string{"r-noseal"})
	require.NoError(t, err)
	assert.Nil(t, got["r-noseal"].Preview)
}

// failingCipher stands in for an unreachable Vault: every seal fails, decrypts included.
type failingCipher struct{}

func (failingCipher) Encrypt(context.Context, string, atrest.EncryptedFields) ([]byte, atrest.EncMeta, error) {
	return nil, atrest.EncMeta{}, fmt.Errorf("vault unreachable")
}

func (failingCipher) Decrypt(context.Context, string, []byte, atrest.EncMeta) (atrest.EncryptedFields, error) {
	return atrest.EncryptedFields{}, fmt.Errorf("vault unreachable")
}

func (failingCipher) EnsureDEK(context.Context, string) error { return nil }
