package auditarchive

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// DEKSize is the AES-256 key length every archive DEK has.
const DEKSize = 32

// ErrAuthFailed means the GCM tag did not validate: wrong key, wrong AAD, or
// altered bytes. Callers use errors.Is.
var ErrAuthFailed = errors.New("auditarchive: authentication failed")

// Cipher seals and opens frames with one site's archive DEK. Safe for
// concurrent use.
type Cipher struct {
	aead   cipher.AEAD
	rand   io.Reader
	macKey []byte // HKDF-derived from the DEK; never logged or exposed
}

// macInfo domain-separates the digest key from the DEK's encryption use.
const macInfo = "chat-audit-record-mac"

// NewCipher wraps a 32-byte DEK in AES-256-GCM.
func NewCipher(dek []byte) (*Cipher, error) {
	if len(dek) != DEKSize {
		return nil, fmt.Errorf("auditarchive: DEK must be %d bytes, got %d", DEKSize, len(dek))
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("auditarchive: aes cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("auditarchive: gcm: %w", err)
	}
	macKey, err := hkdf.Key(sha256.New, dek, nil, macInfo, sha256.Size)
	if err != nil {
		return nil, fmt.Errorf("auditarchive: derive mac key: %w", err)
	}
	return &Cipher{aead: aead, rand: rand.Reader, macKey: macKey}, nil
}

// Digest returns "hmac-sha256:<hex>" over b under a key derived from the DEK.
// It is keyed so that a reader of the plaintext index metadata cannot confirm
// a guessed message body offline against the stored digest.
func (c *Cipher) Digest(b []byte) string {
	m := hmac.New(sha256.New, c.macKey)
	m.Write(b) // hash.Hash.Write never returns an error
	return "hmac-sha256:" + hex.EncodeToString(m.Sum(nil))
}

// Seal returns nonce || ciphertext || tag with a fresh random nonce.
func (c *Cipher) Seal(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(c.rand, nonce); err != nil {
		return nil, fmt.Errorf("auditarchive: nonce: %w", err)
	}
	out := make([]byte, 0, len(nonce)+len(plaintext)+c.aead.Overhead())
	out = append(out, nonce...)
	return c.aead.Seal(out, nonce, plaintext, aad), nil
}

// Open reverses Seal. A tag or AAD mismatch is ErrAuthFailed.
func (c *Cipher) Open(sealed, aad []byte) ([]byte, error) {
	ns := c.aead.NonceSize()
	if len(sealed) < ns+c.aead.Overhead() {
		return nil, fmt.Errorf("auditarchive: sealed frame too short (%d bytes)", len(sealed))
	}
	pt, err := c.aead.Open(nil, sealed[:ns], sealed[ns:], aad)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrAuthFailed, err)
	}
	return pt, nil
}

// FrameAAD binds a segment frame to its site and stream sequence.
func FrameAAD(site string, seq uint64) []byte {
	return []byte("frame|" + site + "|" + strconv.FormatUint(seq, 10))
}

// BodyAAD binds an index document's encBody to its site and sequence.
func BodyAAD(site string, seq uint64) []byte {
	return []byte("body|" + site + "|" + strconv.FormatUint(seq, 10))
}

// ChunkAAD binds a blob chunk to its file, position and whether it is the
// terminating chunk, so chunks cannot be reordered, dropped or truncated.
func ChunkAAD(fileID string, index uint32, final bool) []byte {
	f := "0"
	if final {
		f = "1"
	}
	return []byte("chunk|" + fileID + "|" + strconv.FormatUint(uint64(index), 10) + "|" + f)
}
