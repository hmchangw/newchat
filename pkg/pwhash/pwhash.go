// Package pwhash implements the shared bcrypt(sha256hex(password)) recipe
// used by admin-service and botplatform-service. The SHA-256 pre-hash keeps
// bcrypt's 72-byte input limit from silently truncating long passwords, and
// the recipe must stay byte-identical across services.
package pwhash

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

// Hash derives a bcrypt hash from the SHA-256 hex of plaintext at the given cost.
func Hash(plaintext string, cost int) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(sha256Hex(plaintext)), cost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(b), nil
}

// Verify reports whether plaintext matches the stored bcrypt hash.
func Verify(hash, plaintext string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(sha256Hex(plaintext))) == nil
}

func sha256Hex(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}
