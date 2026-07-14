// Package sessiontoken generates and hashes opaque session tokens shared by
// botplatform-service (issuer) and admin-service (validator). The hashing scheme
// is the stored sessions._id and MUST stay byte-identical across services.
package sessiontoken

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

// New returns a 43-char base64url (RawURLEncoding) token from 32 random bytes.
func New() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate session token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// Hash maps a raw token to its stored sessions._id: base64.StdEncoding(sha256(raw)).
func Hash(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return base64.StdEncoding.EncodeToString(sum[:])
}
