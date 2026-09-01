// Package emoji canonicalizes reaction emoji and holds the built-in
// standard-emoji set. Reactions accept ANY emoji (raw unicode, ZWJ sequences,
// ASCII shortcodes) via CanonicalizeReaction — there is no support check; the
// FE decides renderability. Canonicalize (the `[a-z0-9_+-]{1,32}` form) is kept
// for media-service's custom-emoji upload path, where shortcodes are named and
// IsStandard reserves standard names.
package emoji

//go:generate go run -C gen .

import (
	"errors"
	"fmt"
	"regexp"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"github.com/hmchangw/chat/pkg/errcode"
)

var shortcodeRe = regexp.MustCompile(`^[a-z0-9_+-]{1,32}$`)

// ErrInvalidShortcode is returned when the input fails the wire-format regex.
var ErrInvalidShortcode = errors.New("invalid reaction shortcode")

// Canonicalize returns the NFC-canonical form of a bare shortcode, or
// ErrInvalidShortcode when it fails the input-length cap or wire-format regex.
// Callers MUST use the returned string — not the raw input — for any storage
// key or wire echo, because storage-key equality is byte-exact.
func Canonicalize(shortcode string) (string, error) {
	// Cap input bytes before NFC so a pathological input can't allocate a large output buffer.
	const maxInputBytes = 256
	if len(shortcode) > maxInputBytes {
		return "", fmt.Errorf("canonicalize shortcode (%d bytes): %w", len(shortcode), ErrInvalidShortcode)
	}

	// IsNormalString skips the allocating transform on already-NFC inputs (ASCII always is).
	if !norm.NFC.IsNormalString(shortcode) {
		shortcode = norm.NFC.String(shortcode)
	}

	if !shortcodeRe.MatchString(shortcode) {
		return "", fmt.Errorf("canonicalize shortcode %q: %w", shortcode, ErrInvalidShortcode)
	}
	return shortcode, nil
}

// ReactionMaxBytes caps a reaction emoji — ~20× any real emoji, so it only ever
// rejects an abusive/buggy blob, never a legitimate reaction.
const ReactionMaxBytes = 64

// reactionMaxInputBytes bounds the pre-normalization input so an attacker-sized
// blob can't force O(n) NFC work before the post-NFC cap applies. NFC grows
// length only modestly, so this generous ceiling never rejects a valid emoji.
const reactionMaxInputBytes = 256

// CanonicalizeReaction normalizes a reaction emoji for use as a storage/wire key
// with NO support check: any emoji is accepted and the FE decides renderability.
// It keeps only structural invariants, none about "is this supported": a byte cap
// (so a client can't blob-bomb the reactions map), NFC (so the same emoji in two
// normalization forms is one storage key, not two reactions), and a content-rune
// check (so an invisible key can't be stored). Returns a client-facing BadRequest
// so callers can return the error as-is with a friendly message.
//
// It does NOT map shortcode↔unicode: `thumbsup` and `👍` are distinct keys, since
// there is no support/alias table (that would contradict "accept any emoji"). The
// FE is responsible for sending one consistent representation per emoji.
func CanonicalizeReaction(emojiKey string) (string, error) {
	if len(emojiKey) > reactionMaxInputBytes { // reject before paying for NFC
		return "", errcode.BadRequest("reaction emoji too large")
	}
	if !norm.NFC.IsNormalString(emojiKey) {
		emojiKey = norm.NFC.String(emojiKey)
	}
	if len(emojiKey) > ReactionMaxBytes { // check post-NFC — normalization can grow bytes.
		return "", errcode.BadRequest("reaction emoji too large")
	}
	// Require a visible rune. "Invisible" = whitespace, control (Cc), format
	// (Cf — U+200B ZWSP, U+200D ZWJ), or a combining mark (M — U+FE0F) — a key made
	// only of these stores as an invisible reaction. Everything else is visible,
	// including ASCII punctuation like `_`/`-` (valid shortcode chars), private-use
	// glyphs (Co), and not-yet-assigned code points (Cn) — a future emoji absent
	// from this Go build's Unicode tables must still be accepted ("FE decides
	// renderability"), so we deny only Cc/Cf/M, NOT the whole C category.
	if !hasVisibleRune(emojiKey) {
		return "", errcode.BadRequest("reaction emoji is required")
	}
	return emojiKey, nil
}

func hasVisibleRune(s string) bool {
	for _, r := range s {
		if !unicode.IsSpace(r) && !unicode.In(r, unicode.Cc, unicode.Cf, unicode.M) {
			return true
		}
	}
	return false
}

// IsStandard reports whether an already-canonical shortcode is one of the
// built-in standard emoji (gemoji set). media-service reserves these names
// from custom-emoji registration since a custom upload would be permanently
// shadowed by the standard set.
func IsStandard(shortcode string) bool {
	_, ok := standardEmoji[shortcode]
	return ok
}
