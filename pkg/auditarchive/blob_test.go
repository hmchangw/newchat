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

func TestBlob_RoundTrip(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	for _, size := range []int{0, 1, 1000, 4096, 4097, 3*4096 + 5} {
		plain := bytes.Repeat([]byte{0xab}, size)
		var enc bytes.Buffer
		sum, n, err := EncryptBlob(&enc, bytes.NewReader(plain), c, "f1", 4096)
		require.NoError(t, err, "size %d", size)
		assert.Equal(t, int64(size), n)
		want := sha256.Sum256(plain)
		assert.Equal(t, "sha256:"+hex.EncodeToString(want[:]), sum)

		var dec bytes.Buffer
		sum2, n2, err := DecryptBlob(&dec, bytes.NewReader(enc.Bytes()), c, "f1")
		require.NoError(t, err, "size %d", size)
		assert.Equal(t, string(plain), dec.String(), "size %d", size) // string compare: an empty plaintext decodes to a nil slice
		assert.Equal(t, sum, sum2)
		assert.Equal(t, n, n2)
	}
}

func TestBlob_Tampering(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	plain := bytes.Repeat([]byte{1}, 10000)
	var enc bytes.Buffer
	_, _, err = EncryptBlob(&enc, bytes.NewReader(plain), c, "f1", 4096)
	require.NoError(t, err)

	t.Run("wrong file id", func(t *testing.T) {
		_, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(enc.Bytes()), c, "f2")
		assert.True(t, errors.Is(err, ErrAuthFailed))
	})
	t.Run("truncated before terminator", func(t *testing.T) {
		b := enc.Bytes()
		_, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(b[:len(b)-40]), c, "f1")
		assert.True(t, errors.Is(err, ErrBlobTruncated))
	})
	t.Run("flipped byte", func(t *testing.T) {
		b := append([]byte(nil), enc.Bytes()...)
		b[100] ^= 1
		_, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(b), c, "f1")
		assert.True(t, errors.Is(err, ErrAuthFailed))
	})
	t.Run("bad chunk size", func(t *testing.T) {
		_, _, err := EncryptBlob(&bytes.Buffer{}, bytes.NewReader(plain), c, "f1", 0)
		assert.Error(t, err)
	})
}
