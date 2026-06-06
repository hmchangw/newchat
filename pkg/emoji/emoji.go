// Package emoji validates reaction shortcodes against a site-scoped custom
// emoji store. Shortcodes are bare (no colons), `[a-z0-9_+-]{1,32}`, and
// NFC-normalised; the canonical form is what callers bind into storage.
package emoji

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"golang.org/x/text/unicode/norm"
)

var shortcodeRe = regexp.MustCompile(`^[a-z0-9_+-]{1,32}$`)

// ErrInvalidShortcode is returned when the input fails the wire-format regex.
var ErrInvalidShortcode = errors.New("invalid reaction shortcode")

// ErrUnknownShortcode is returned when a syntactically valid shortcode is not registered for the site.
var ErrUnknownShortcode = errors.New("unknown reaction shortcode")

// CustomEmojiLookup resolves a site-scoped custom emoji shortcode.
type CustomEmojiLookup interface {
	CustomEmojiExists(ctx context.Context, siteID, shortcode string) (bool, error)
}

// Validator validates reaction shortcodes against a site-scoped custom emoji store.
type Validator struct {
	lookup CustomEmojiLookup
}

// NewValidator returns a Validator backed by the given custom emoji lookup.
func NewValidator(lookup CustomEmojiLookup) *Validator {
	return &Validator{lookup: lookup}
}

// Validate reports whether shortcode is acceptable and returns the
// NFC-normalised canonical form. Callers MUST use the returned string —
// not the raw input — for any storage key or wire echo, because Cassandra
// map-key equality is byte-exact.
func (v *Validator) Validate(ctx context.Context, siteID, shortcode string) (string, error) {
	// Cap input bytes before NFC so a pathological input can't allocate a large output buffer.
	const maxInputBytes = 256
	if len(shortcode) > maxInputBytes {
		return "", fmt.Errorf("validate shortcode (%d bytes): %w", len(shortcode), ErrInvalidShortcode)
	}

	// IsNormalString skips the allocating transform on already-NFC inputs (ASCII always is).
	if !norm.NFC.IsNormalString(shortcode) {
		shortcode = norm.NFC.String(shortcode)
	}

	if !shortcodeRe.MatchString(shortcode) {
		return "", fmt.Errorf("validate shortcode %q: %w", shortcode, ErrInvalidShortcode)
	}
	ok, err := v.lookup.CustomEmojiExists(ctx, siteID, shortcode)
	if err != nil {
		return "", fmt.Errorf("lookup custom emoji %q for site %q: %w", shortcode, siteID, err)
	}
	if !ok {
		return "", fmt.Errorf("validate shortcode %q: %w", shortcode, ErrUnknownShortcode)
	}
	return shortcode, nil
}
