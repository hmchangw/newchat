package emoji_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/emoji"
)

func TestCanonicalize(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{"valid ascii", "party_parrot", "party_parrot", false},
		{"leading underscore", "_party", "_party", false},
		{"leading plus", "+1", "+1", false},
		{"leading minus", "-1", "-1", false},
		{"boundary 1 char", "a", "a", false},
		{"boundary 32", strings.Repeat("a", 32), strings.Repeat("a", 32), false},
		{"empty", "", "", true},
		{"uppercase", "Party", "", true},
		{"wrapped in colons", ":party:", "", true},
		{"leading colon", ":party", "", true},
		{"trailing colon", "party:", "", true},
		{"space", "party time", "", true},
		{"unicode literal", "👍", "", true},
		{"trailing dot", "party.", "", true},
		{"slash", "party/time", "", true},
		{"too long 33", strings.Repeat("a", 33), "", true},
		{"over byte cap", strings.Repeat("a", 1024), "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := emoji.Canonicalize(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, emoji.ErrInvalidShortcode)
				assert.Empty(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestCanonicalize_NFC_ASCIIStable pins ASCII byte-stability under NFC so the
// canonical form can't drift.
func TestCanonicalize_NFC_ASCIIStable(t *testing.T) {
	cases := []string{"acme_party", "a", "a0_-+"}
	for _, in := range cases {
		in := in
		t.Run(in, func(t *testing.T) {
			got, err := emoji.Canonicalize(in)
			require.NoError(t, err)
			assert.Equal(t, in, got, "ASCII input must be byte-stable under NFC")
		})
	}
}

// TestCanonicalize_NFC_CollapsesEquivalentForms documents that the NFC step is
// wired. Both forms fail today (regex is ASCII); when the regex broadens, both
// must yield byte-identical canonical strings.
func TestCanonicalize_NFC_CollapsesEquivalentForms(t *testing.T) {
	// "é" precomposed (U+00E9) vs decomposed (U+0065 U+0301). The decomposed
	// form must be written as an explicit escape — a literal "é" in source is
	// silently precomposed by tooling/editors, which would make this test a
	// no-op that never exercises the norm.NFC.String transform branch.
	precomposed := "é"
	decomposed := "\u0065\u0301"

	gotPre, errPre := emoji.Canonicalize(precomposed)
	require.Error(t, errPre)
	assert.ErrorIs(t, errPre, emoji.ErrInvalidShortcode)
	assert.Empty(t, gotPre)

	gotDec, errDec := emoji.Canonicalize(decomposed)
	require.Error(t, errDec)
	assert.ErrorIs(t, errDec, emoji.ErrInvalidShortcode)
	assert.Empty(t, gotDec)
}

// TestCanonicalize_InputLengthCap pins the 256-byte pre-NFC guard so
// pathological input doesn't pay NFC cost.
func TestCanonicalize_InputLengthCap(t *testing.T) {
	huge := strings.Repeat("a", 1024)

	got, err := emoji.Canonicalize(huge)
	require.Error(t, err)
	assert.ErrorIs(t, err, emoji.ErrInvalidShortcode)
	assert.Empty(t, got)
}

func TestIsStandard(t *testing.T) {
	assert.True(t, emoji.IsStandard("thumbsup"))
	assert.True(t, emoji.IsStandard("+1"))
	assert.True(t, emoji.IsStandard("heart"))
	assert.False(t, emoji.IsStandard("acme_party"))
	assert.False(t, emoji.IsStandard(""))
}
