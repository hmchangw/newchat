package sessiontoken

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_LengthAndCharset(t *testing.T) {
	tok, err := New()
	require.NoError(t, err)
	assert.Len(t, tok, 43) // 32 bytes, RawURLEncoding
	_, err = base64.RawURLEncoding.DecodeString(tok)
	assert.NoError(t, err)
}

func TestNew_Unique(t *testing.T) {
	a, err := New()
	require.NoError(t, err)
	b, err := New()
	require.NoError(t, err)
	assert.NotEqual(t, a, b)
}

func TestHash_DeterministicAndKnownScheme(t *testing.T) {
	// Golden: base64.StdEncoding(sha256("token")) — must match botplatform's prior scheme.
	h1 := Hash("token")
	assert.Equal(t, "PEaenWxYddN6Q/NT1PiOYfz4EsZu7jRXRlpAsNpBU+A=", h1)
	h2 := Hash("token")
	assert.Equal(t, h1, h2)
	assert.Len(t, h1, 44)
}
