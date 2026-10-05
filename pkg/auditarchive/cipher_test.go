package auditarchive

import (
	"bytes"
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
