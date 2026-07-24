package teamsmigrate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHTMLToMarkdown(t *testing.T) {
	cases := map[string]struct{ in, want string }{
		"bold":               {"<b>hi</b>", "**hi**"},
		"strong":             {"<strong>hi</strong>", "**hi**"},
		"italic":             {"<i>hi</i>", "*hi*"},
		"link":               {`<a href="http://x">t</a>`, "[t](http://x)"},
		"break":              {"a<br>b", "a\nb"},
		"unsupported to raw": {"<div><span>plain</span></div>", "plain"},
		"entities":           {"a &amp; b &lt;c&gt;", "a & b <c>"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, htmlToMarkdown(tc.in))
		})
	}
}

func TestBodyToContent_TextPassthrough(t *testing.T) {
	assert.Equal(t, "*raw* text", BodyToContent(Body{ContentType: "text", Content: "*raw* text"}))
	assert.Equal(t, "**hi**", BodyToContent(Body{ContentType: "html", Content: "<b>hi</b>"}))
}

func TestMessageType(t *testing.T) {
	assert.Equal(t, "", MessageType(""))
	assert.Equal(t, "", MessageType("message"))
	assert.Equal(t, "teams_system", MessageType("systemEventMessage"))
}

func TestEmployeeIDFromGraphID_Deterministic(t *testing.T) {
	a := EmployeeIDFromGraphID("graph-1")
	assert.Equal(t, a, EmployeeIDFromGraphID("graph-1"), "same graph id → same key")
	assert.NotEqual(t, a, EmployeeIDFromGraphID("graph-2"))
	assert.Len(t, a, 17, "17-char base62 (native-user id shape)")
}

func TestDeterministicMessageID_Stable(t *testing.T) {
	a := DeterministicMessageID("r1", "tm-1")
	assert.Equal(t, a, DeterministicMessageID("r1", "tm-1"), "same room+teams id → same message id")
	assert.NotEqual(t, a, DeterministicMessageID("r1", "tm-2"))
	assert.NotEqual(t, a, DeterministicMessageID("r2", "tm-1"), "same teams id in a different room → different id (no cross-room collision)")
	assert.True(t, isValidBase62MessageID(a), "valid message-id format: %q", a)
}

// isValidBase62MessageID mirrors idgen's 20-char base62 message-id shape.
func isValidBase62MessageID(s string) bool {
	if len(s) != 20 {
		return false
	}
	for _, c := range s {
		isBase62 := (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
		if !isBase62 {
			return false
		}
	}
	return true
}
