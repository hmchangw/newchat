package pwhash

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func TestHash_VerifyRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		plaintext string
	}{
		{"short password", "s3cr3t"},
		{"empty password", ""},
		{"long password over 72 bytes", strings.Repeat("a", 100)},
		{"unicode password", "密碼pass測試"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hash, err := Hash(tc.plaintext, bcrypt.MinCost)
			require.NoError(t, err)
			assert.NotEmpty(t, hash)
			assert.True(t, Verify(hash, tc.plaintext), "verify must accept the original plaintext")
		})
	}
}

func TestVerify_RejectsWrongPassword(t *testing.T) {
	hash, err := Hash("correct-password", bcrypt.MinCost)
	require.NoError(t, err)

	assert.False(t, Verify(hash, "wrong-password"))
}

func TestVerify_RejectsNonBcryptString(t *testing.T) {
	assert.False(t, Verify("not-a-bcrypt-hash", "anything"))
	assert.False(t, Verify("", "anything"))
}

func TestHash_LongPasswordsAreDistinguishable(t *testing.T) {
	// bcrypt truncates its input at 72 bytes; without the SHA-256 pre-hash,
	// two long passwords sharing a 72-byte prefix would collide. Confirm they
	// don't: two long passwords differing only after byte 72 must verify
	// distinctly.
	base := strings.Repeat("x", 100)
	a := base + "-one"
	b := base + "-two"

	hashA, err := Hash(a, bcrypt.MinCost)
	require.NoError(t, err)

	assert.True(t, Verify(hashA, a))
	assert.False(t, Verify(hashA, b), "passwords differing after byte 72 must not collide")
}

func TestHash_ErrorOnInvalidCost(t *testing.T) {
	_, err := Hash("password", bcrypt.MaxCost+1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hash password")
}
