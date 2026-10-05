package auditarchive

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlob_RoundTrip(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	for _, size := range []int{0, 1, 1000, 4096, 4097, 3*4096 + 5} {
		plain := bytes.Repeat([]byte{0xab}, size)
		var enc bytes.Buffer
		sum, n, err := EncryptBlob(&enc, bytes.NewReader(plain), c, "f1", "0123456789abcdef", 4096)
		require.NoError(t, err, "size %d", size)
		assert.Equal(t, int64(size), n)
		assert.Equal(t, c.Digest(plain), sum, "keyed digest of the plaintext")
		unkeyed := sha256.Sum256(plain)
		assert.NotContains(t, sum, hex.EncodeToString(unkeyed[:]), "never the unkeyed sha256")

		var dec bytes.Buffer
		sum2, n2, keyID, err := DecryptBlob(&dec, bytes.NewReader(enc.Bytes()), c, "f1")
		require.NoError(t, err, "size %d", size)
		assert.Equal(t, string(plain), dec.String(), "size %d", size) // string compare: an empty plaintext decodes to a nil slice
		assert.Equal(t, sum, sum2)
		assert.Equal(t, n, n2)
		assert.Equal(t, "0123456789abcdef", keyID)
	}
}

func TestBlob_Tampering(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	plain := bytes.Repeat([]byte{1}, 10000)
	var enc bytes.Buffer
	_, _, err = EncryptBlob(&enc, bytes.NewReader(plain), c, "f1", "k", 4096)
	require.NoError(t, err)

	t.Run("wrong file id", func(t *testing.T) {
		_, _, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(enc.Bytes()), c, "f2")
		assert.True(t, errors.Is(err, ErrAuthFailed))
	})
	t.Run("truncated before terminator", func(t *testing.T) {
		b := enc.Bytes()
		_, _, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(b[:len(b)-40]), c, "f1")
		assert.True(t, errors.Is(err, ErrBlobTruncated))
	})
	t.Run("flipped byte", func(t *testing.T) {
		b := append([]byte(nil), enc.Bytes()...)
		b[100] ^= 1
		_, _, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(b), c, "f1")
		assert.True(t, errors.Is(err, ErrAuthFailed))
	})
	t.Run("bad chunk size", func(t *testing.T) {
		_, _, err := EncryptBlob(&bytes.Buffer{}, bytes.NewReader(plain), c, "f1", "k", 0)
		assert.Error(t, err)
	})
}

func TestBlob_KeyIDHeader(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	t.Run("empty key id is tolerated", func(t *testing.T) {
		var enc bytes.Buffer
		_, _, err := EncryptBlob(&enc, strings.NewReader("hi"), c, "f1", "", 64)
		require.NoError(t, err)
		var dec bytes.Buffer
		_, _, keyID, err := DecryptBlob(&dec, bytes.NewReader(enc.Bytes()), c, "f1")
		require.NoError(t, err)
		assert.Empty(t, keyID)
		assert.Equal(t, "hi", dec.String())
	})
	t.Run("key id too long is rejected", func(t *testing.T) {
		_, _, err := EncryptBlob(&bytes.Buffer{}, strings.NewReader("hi"), c, "f1", strings.Repeat("k", 65536), 64)
		assert.True(t, errors.Is(err, ErrBadSegment), "got %v", err)
	})
	t.Run("ReadBlobHeader exposes the key id before any key is chosen", func(t *testing.T) {
		var enc bytes.Buffer
		_, _, err := EncryptBlob(&enc, strings.NewReader("hi"), c, "f1", "kid-1", 64)
		require.NoError(t, err)
		h, err := ReadBlobHeader(bytes.NewReader(enc.Bytes()))
		require.NoError(t, err)
		assert.Equal(t, BlobHeader{Version: FormatVersion, ChunkBytes: 64, KeyID: "kid-1"}, h)
	})
}
