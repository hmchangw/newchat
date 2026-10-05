package auditarchive

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDEK() []byte { return bytes.Repeat([]byte{0x4b}, DEKSize) }

func TestCipher_SealOpen(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	aad := FrameAAD("site-a", 7)
	sealed, err := c.Seal([]byte("hello"), aad)
	require.NoError(t, err)
	assert.Len(t, sealed, 12+5+16)

	got, err := c.Open(sealed, aad)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), got)

	t.Run("fresh nonce per call", func(t *testing.T) {
		again, err := c.Seal([]byte("hello"), aad)
		require.NoError(t, err)
		assert.NotEqual(t, sealed, again)
	})
	t.Run("wrong aad fails", func(t *testing.T) {
		_, err := c.Open(sealed, FrameAAD("site-a", 8))
		assert.True(t, errors.Is(err, ErrAuthFailed))
	})
	t.Run("tampered byte fails", func(t *testing.T) {
		bad := append([]byte(nil), sealed...)
		bad[len(bad)-1] ^= 1
		_, err := c.Open(bad, aad)
		assert.True(t, errors.Is(err, ErrAuthFailed))
	})
	t.Run("short input fails", func(t *testing.T) {
		_, err := c.Open([]byte{1, 2, 3}, aad)
		assert.Error(t, err)
	})
	t.Run("wrong key size", func(t *testing.T) {
		_, err := NewCipher([]byte("short"))
		assert.Error(t, err)
	})
}

func TestAADs(t *testing.T) {
	assert.Equal(t, "frame|site-a|7", string(FrameAAD("site-a", 7)))
	assert.Equal(t, "body|site-a|7", string(BodyAAD("site-a", 7)))
	assert.Equal(t, "chunk|f1|3|0", string(ChunkAAD("f1", 3, false)))
	assert.Equal(t, "chunk|f1|3|1", string(ChunkAAD("f1", 3, true)))
}

func TestCipher_Digest(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	d := c.Digest([]byte("record"))
	assert.Regexp(t, `^hmac-sha256:[0-9a-f]{64}$`, d)

	t.Run("deterministic for the same input and DEK", func(t *testing.T) {
		c2, err := NewCipher(testDEK())
		require.NoError(t, err)
		assert.Equal(t, d, c2.Digest([]byte("record")))
	})
	t.Run("differs across DEKs", func(t *testing.T) {
		c2, err := NewCipher(bytes.Repeat([]byte{0x4c}, DEKSize))
		require.NoError(t, err)
		assert.NotEqual(t, d, c2.Digest([]byte("record")))
	})
	t.Run("differs across inputs", func(t *testing.T) {
		assert.NotEqual(t, d, c.Digest([]byte("record2")))
	})
	t.Run("is not the unkeyed sha256", func(t *testing.T) {
		sum := sha256.Sum256([]byte("record"))
		assert.NotContains(t, d, hex.EncodeToString(sum[:]))
	})
}
