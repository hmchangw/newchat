// Package emoji provides reaction shortcode canonicalization and the
// built-in standard-emoji set. Shortcodes are bare (no colons),
// `[a-z0-9_+-]{1,32}`, and NFC-normalised; the canonical form is what
// callers bind into storage. There is no registration check here: reactions
// accept any well-formed shortcode. media-service's upload path still uses
// IsStandard to reserve standard names from custom-emoji registration.
package emoji

//go:generate go run -C gen .

import (
	"errors"
	"fmt"
	"regexp"

	"golang.org/x/text/unicode/norm"
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

// IsStandard reports whether an already-canonical shortcode is one of the
// built-in standard emoji (gemoji set). media-service reserves these names
// from custom-emoji registration since a custom upload would be permanently
// shadowed by the standard set.
func IsStandard(shortcode string) bool {
	_, ok := standardEmoji[shortcode]
	return ok
}
