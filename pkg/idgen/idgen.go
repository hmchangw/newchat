// Package idgen produces identifiers for the chat system:
// UUIDv7 hex (32 chars) for entity MongoDB _ids via GenerateUUIDv7,
// 20-char base62 for message IDs via GenerateMessageID,
// 17-char base62 for channel room IDs via GenerateID, and
// sorted-concat user IDs for DM rooms via BuildDMRoomID.
package idgen

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"math/big"

	"github.com/google/uuid"
)

// base62 alphabet (0-9A-Za-z).
const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

const (
	// idLength: 17-char base62, ~101 bits of entropy. Channel room IDs.
	idLength = 17
	// messageIDLength: 20-char base62, ~119 bits of entropy. Message.ID and JetStream Nats-Msg-Id.
	messageIDLength = 20
	// legacyMessageIDLength: pre-cutover 17-char base62 message IDs; honored by the validator only.
	legacyMessageIDLength = 17
	// uuidHyphenatedLength: standard hyphenated UUID for X-Request-ID.
	uuidHyphenatedLength = 36
)

// encodeBase62 renders n into a length-char base62 string. Mutates n.
func encodeBase62(n *big.Int, length int) string {
	base := big.NewInt(int64(len(alphabet)))
	mod := new(big.Int)
	buf := make([]byte, length)
	for i := length - 1; i >= 0; i-- {
		n.DivMod(n, base, mod)
		buf[i] = alphabet[mod.Int64()]
	}
	return string(buf)
}

// generateBase62 returns a uniformly-distributed random base62 string of the requested length via rejection sampling on bytes (rejects ≥248; ~3.1% rate).
func generateBase62(length int) string {
	out := make([]byte, length)
	bufSize := length + length/8 + 1
	buf := make([]byte, bufSize)
	written := 0
	for written < length {
		if _, err := rand.Read(buf); err != nil {
			panic("idgen: crypto/rand read failed: " + err.Error())
		}
		for _, b := range buf {
			if b >= 248 {
				continue
			}
			out[written] = alphabet[b%62]
			written++
			if written == length {
				break
			}
		}
	}
	return string(out)
}

// GenerateID returns a fresh random 17-char base62 identifier (channel room IDs).
func GenerateID() string {
	return generateBase62(idLength)
}

// GenerateMessageID returns a fresh random 20-char base62 identifier (Message.ID and Nats-Msg-Id).
func GenerateMessageID() string {
	return generateBase62(messageIDLength)
}

// MessageIDFromRequestID returns a deterministic 20-char base62 from SHA-256(requestID+":"+suffix); stable across redeliveries so JetStream dedup catches retries.
func MessageIDFromRequestID(requestID, suffix string) string {
	h := sha256.Sum256([]byte(requestID + ":" + suffix))
	return encodeBase62(new(big.Int).SetBytes(h[:16]), messageIDLength)
}

// DeterministicID renders a stable 17-char base62 id from seed (SHA-256 → base62),
// the same shape as GenerateID. Same seed → same id, so an external key (e.g. a
// Graph object id) maps to one identity without a lookup.
func DeterministicID(seed []byte) string {
	h := sha256.Sum256(seed)
	return encodeBase62(new(big.Int).SetBytes(h[:16]), idLength)
}

// IsValidMessageID reports whether s is a well-formed base62 message ID. Accepts
// both the current 20-char form and the legacy 17-char form so pre-cutover
// messages keep flowing through federation replays, JetStream redeliveries, and
// historical reads.
func IsValidMessageID(s string) bool {
	if len(s) != messageIDLength && len(s) != legacyMessageIDLength {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
		case c >= 'A' && c <= 'Z':
		case c >= 'a' && c <= 'z':
		default:
			return false
		}
	}
	return true
}

// IsValidUUIDv7 reports whether s is a 32-char lowercase hex UUIDv7 (no hyphens). Validates length, alphabet, version nibble (index 12 == '7'), and variant nibble (index 16 ∈ {8,9,a,b}).
func IsValidUUIDv7(s string) bool {
	const uuidv7HexLength = 32
	if len(s) != uuidv7HexLength {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	if s[12] != '7' {
		return false
	}
	switch s[16] {
	case '8', '9', 'a', 'b':
		return true
	}
	return false
}

// GenerateUUIDv7 returns a fresh UUIDv7 as 32-char lowercase hex without hyphens (entity Mongo _id and request IDs).
func GenerateUUIDv7() string {
	u, err := uuid.NewV7()
	if err != nil {
		panic("idgen: uuid.NewV7 failed: " + err.Error())
	}
	var buf [32]byte
	hex.Encode(buf[:], u[:])
	return string(buf[:])
}

// BuildDMRoomID returns the lexicographically-sorted concat of two user IDs; BuildDMRoomID(a,b) == BuildDMRoomID(b,a).
func BuildDMRoomID(userA, userB string) string {
	if userA <= userB {
		return userA + userB
	}
	return userB + userA
}

// GenerateRequestID returns a fresh UUIDv7 in standard hyphenated form (36 chars,
// e.g. "01970a4f-8c2d-7c9a-abcd-e0123456789f"). Used at HTTP/NATS entry points to
// mint X-Request-ID values when no inbound header is present. The hyphenated form
// matches the industry-standard UUID representation that frontends and tools expect.
func GenerateRequestID() string {
	u, err := uuid.NewV7()
	if err != nil {
		panic("idgen: uuid.NewV7 failed: " + err.Error())
	}
	return u.String()
}

// ResolveRequestID enforces the repo-wide "mint everywhere" policy on inbound
// X-Request-ID values: if inbound is a valid hyphenated UUID, it passes through
// unchanged; otherwise a fresh UUIDv7 is minted. replaced is true ONLY when
// inbound was non-empty-and-invalid (i.e., a malformed client value was
// swapped) — empty inbound returns (fresh, false) because "missing" is the
// benign common case, not a client bug. Callers should emit a Warn on
// replaced=true so a buggy client stays traceable.
//
// This is the transport-agnostic primitive. NATS callers wrap it in
// natsutil.StampRequestID, which also handles ctx-stamping and the warn log;
// HTTP callers (Gin middleware) call it directly with c.GetHeader(...).
func ResolveRequestID(inbound string) (id string, replaced bool) {
	if IsValidUUID(inbound) {
		return inbound, false
	}
	return GenerateRequestID(), inbound != ""
}

// IsValidUUID reports whether s is a well-formed hyphenated UUID of any version
// (case-insensitive). Used to validate inbound X-Request-ID headers — we don't
// care which UUID scheme the caller used (v4 or v7), only that the shape is
// well-formed. The strict v7 check (IsValidUUIDv7) is preserved for paths that
// need to assert the v7 shape on the 32-char no-hyphen entity-_id form.
func IsValidUUID(s string) bool {
	if len(s) != uuidHyphenatedLength {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			switch {
			case c >= '0' && c <= '9':
			case c >= 'a' && c <= 'f':
			case c >= 'A' && c <= 'F':
			default:
				return false
			}
		}
	}
	return true
}
