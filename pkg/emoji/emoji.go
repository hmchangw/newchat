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

// CanonicalizeReaction normalizes a reaction emoji for use as a storage/wire key
// with NO support check: any emoji is accepted and the FE decides renderability.
// It keeps only two invariants, neither about "is this supported": NFC (so the
// same emoji in two normalization forms is one storage key, not two reactions)
// and a byte cap (so a client can't blob-bomb the reactions map). The cap is the
// only reject; empty is caught upstream. Returns a client-facing BadRequest past
// the cap so callers can return the error as-is with a friendly message.
func CanonicalizeReaction(emojiKey string) (string, error) {
	if !norm.NFC.IsNormalString(emojiKey) {
		emojiKey = norm.NFC.String(emojiKey)
	}
	if len(emojiKey) > ReactionMaxBytes { // check post-NFC — normalization can grow bytes.
		return "", errcode.BadRequest("reaction emoji too large")
	}
	return emojiKey, nil
}

// IsStandard reports whether an already-canonical shortcode is one of the
// built-in standard emoji (gemoji set). media-service reserves these names
// from custom-emoji registration since a custom upload would be permanently
// shadowed by the standard set.
func IsStandard(shortcode string) bool {
	_, ok := standardEmoji[shortcode]
	return ok
}
