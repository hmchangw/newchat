package main

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsBot(t *testing.T) {
	assert.True(t, isBot("helper.bot"))
	// Platform-admin pseudo-account uses the bot avatar path (no employee photo).
	assert.True(t, isBot("p_tchatadmin_siteA"))
	// QA p_ accounts are ordinary users served via the user avatar path.
	assert.False(t, isBot("p_payroll"))
	assert.False(t, isBot("p_qa1"))
	assert.False(t, isBot("alice"))
}

func TestSanitizeInitial(t *testing.T) {
	cases := map[string]string{
		"alice":   "A",
		"張三":      "張",
		"7eleven": "7",
		"</text>": "?",
		"":        "?",
		" x":      "?", //nolint:gocritic // intentional: test sanitizeInitial with leading space
	}
	for in, want := range cases {
		assert.Equalf(t, want, sanitizeInitial(in), "sanitizeInitial(%q)", in)
	}
}

func TestRenderDefaultSVG_Deterministic(t *testing.T) {
	assert.Equal(t, renderDefaultSVG("room-1", "General"), renderDefaultSVG("room-1", "General"))
}

func TestRenderDefaultSVG_StableColourPerSeed(t *testing.T) {
	a := string(renderDefaultSVG("room-1", "Alpha"))
	b := string(renderDefaultSVG("room-1", "Beta"))
	fillA := strings.Split(strings.SplitN(a, `fill="`, 2)[1], `"`)[0]
	assert.Contains(t, b, `fill="`+fillA+`"`, "same seed → same colour regardless of name")
}

func TestRenderDefaultSVG_InjectionSafe(t *testing.T) {
	out := renderDefaultSVG("seed", `</text><script>alert(1)</script>`)
	require.NoError(t, xml.Unmarshal(out, new(struct{ XMLName xml.Name })), "must be well-formed XML")
	assert.NotContains(t, string(out), "<script>")
}

func TestDefaultETag_StableAndQuoted(t *testing.T) {
	e1 := defaultETag("room-1", "General")
	assert.Equal(t, e1, defaultETag("room-1", "General"))
	assert.True(t, strings.HasPrefix(e1, `"`) && strings.HasSuffix(e1, `"`))
}

func TestBotObjectKey(t *testing.T) {
	assert.Equal(t, "bot/helper.bot", botObjectKey("helper.bot"))
}

func TestEmployeePhotoURL(t *testing.T) {
	cases := []struct {
		name, base, eid, want string
	}{
		{"bare base", "https://photos.example.com", "E123", "https://photos.example.com/E123_120.JPG"},
		{"base carries the path", "https://host/realPhoto/dir", "E123", "https://host/realPhoto/dir/E123_120.JPG"},
		{"trailing slash trimmed", "https://host/p/", "E123", "https://host/p/E123_120.JPG"},
		{"eid escaped", "https://host", "a/b", "https://host/a%2Fb_120.JPG"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, employeePhotoURL(tc.base, tc.eid))
		})
	}
}
