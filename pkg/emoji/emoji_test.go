package emoji_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/emoji"
)

type stubLookup struct {
	exists map[string]bool
	err    error
}

func (s *stubLookup) CustomEmojiExists(_ context.Context, _, shortcode string) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	return s.exists[shortcode], nil
}

func TestValidator_Validate_CustomShortcode_Found(t *testing.T) {
	lookup := &stubLookup{exists: map[string]bool{"acme_party": true}}
	v := emoji.NewValidator(lookup)

	got, err := v.Validate(context.Background(), "site-a", "acme_party")
	require.NoError(t, err)
	assert.Equal(t, "acme_party", got)
}

func TestValidator_Validate_CustomShortcode_NotFound(t *testing.T) {
	v := emoji.NewValidator(&stubLookup{exists: map[string]bool{}})

	got, err := v.Validate(context.Background(), "site-a", "not_a_real_emoji_xyz")
	require.Error(t, err)
	assert.ErrorIs(t, err, emoji.ErrUnknownShortcode)
	assert.Empty(t, got)
}

func TestValidator_Validate_InvalidFormat(t *testing.T) {
	v := emoji.NewValidator(&stubLookup{})

	cases := []struct {
		name      string
		shortcode string
	}{
		{"empty", ""},
		{"wrapped in colons", ":thumbsup:"},
		{"leading colon", ":thumbsup"},
		{"trailing colon", "thumbsup:"},
		{"uppercase letters", "ThumbsUp"},
		{"space", "thumbs up"},
		{"unicode literal", "👍"},
		{"trailing dot", "party."},
		{"slash", "party/time"},
		{"too long", strings.Repeat("a", 33)},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := v.Validate(context.Background(), "site-a", tc.shortcode)
			require.Error(t, err)
			assert.ErrorIs(t, err, emoji.ErrInvalidShortcode)
			assert.Empty(t, got)
		})
	}
}

// Leading `_`/`+`/`-` pass the regex and reach the lookup — covers +1/-1.
func TestValidator_Validate_LeadingSymbol(t *testing.T) {
	lookup := &stubLookup{exists: map[string]bool{
		"+1":     true,
		"-1":     true,
		"_party": true,
	}}
	v := emoji.NewValidator(lookup)

	for _, in := range []string{"+1", "-1", "_party"} {
		in := in
		t.Run(in, func(t *testing.T) {
			got, err := v.Validate(context.Background(), "site-a", in)
			require.NoError(t, err)
			assert.Equal(t, in, got)
		})
	}
}

func TestValidator_Validate_BoundaryLengths(t *testing.T) {
	lookup := &stubLookup{exists: map[string]bool{
		"a":                     true,
		strings.Repeat("a", 32): true,
	}}
	v := emoji.NewValidator(lookup)

	got, err := v.Validate(context.Background(), "site-a", "a")
	require.NoError(t, err)
	assert.Equal(t, "a", got)

	got, err = v.Validate(context.Background(), "site-a", strings.Repeat("a", 32))
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("a", 32), got)
}

func TestValidator_Validate_LookupError(t *testing.T) {
	sentinel := errors.New("mongo down")
	v := emoji.NewValidator(&stubLookup{err: sentinel})

	got, err := v.Validate(context.Background(), "site-a", "some_custom_emoji")
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.Empty(t, got)
}

// Pins the 256-byte pre-NFC guard so pathological input doesn't pay NFC cost.
func TestValidator_Validate_InputLengthCap(t *testing.T) {
	v := emoji.NewValidator(&stubLookup{})
	huge := strings.Repeat("a", 1024)

	got, err := v.Validate(context.Background(), "site-a", huge)
	require.Error(t, err)
	assert.ErrorIs(t, err, emoji.ErrInvalidShortcode)
	assert.Empty(t, got)
}

// Pins ASCII byte-stability under NFC so the canonical form can't drift.
func TestValidator_Validate_NFC_ASCIIStable(t *testing.T) {
	lookup := &stubLookup{exists: map[string]bool{
		"acme_party": true,
		"a0_-+":      true,
		"a":          true,
	}}
	v := emoji.NewValidator(lookup)

	cases := []string{"acme_party", "a", "a0_-+"}
	for _, in := range cases {
		in := in
		t.Run(in, func(t *testing.T) {
			got, err := v.Validate(context.Background(), "site-a", in)
			require.NoError(t, err)
			assert.Equal(t, in, got, "ASCII input must be byte-stable under NFC")
		})
	}
}

// TestValidator_Validate_NFC_CollapsesEquivalentForms documents that the NFC
// step is wired. Both forms fail today (regex is ASCII); when the regex
// broadens, both must yield byte-identical canonical strings.
func TestValidator_Validate_NFC_CollapsesEquivalentForms(t *testing.T) {
	v := emoji.NewValidator(&stubLookup{})

	// "é" precomposed (U+00E9) vs decomposed (U+0065 U+0301).
	precomposed := "é"
	decomposed := "é"

	gotPre, errPre := v.Validate(context.Background(), "site-a", precomposed)
	require.Error(t, errPre)
	assert.ErrorIs(t, errPre, emoji.ErrInvalidShortcode)
	assert.Empty(t, gotPre)

	gotDec, errDec := v.Validate(context.Background(), "site-a", decomposed)
	require.Error(t, errDec)
	assert.ErrorIs(t, errDec, emoji.ErrInvalidShortcode)
	assert.Empty(t, gotDec)
}
