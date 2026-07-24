package main

import (
	"fmt"
	"hash/fnv"
	"html"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hmchangw/chat/pkg/model"
)

const svgTemplateVersion = "v1"

// isBot reports whether account uses the bot avatar path (bot subject type,
// no employee photo): real ".bot" bots and the "p_tchatadmin_" platform-admin
// pseudo-account. Routed through the model taxonomy so plain "p_" QA test
// accounts — ordinary users — are served via the user avatar path instead.
func isBot(account string) bool {
	return model.IsBot(account) || model.IsPlatformAdminAccount(account)
}

var palette = []string{
	"#1abc9c", "#2ecc71", "#3498db", "#9b59b6",
	"#e67e22", "#e74c3c", "#f39c12", "#16a085",
}

func stableHash(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// sanitizeInitial returns the first rune of name, uppercased, when it is a
// letter or digit; otherwise a neutral placeholder "?". The result never
// contains characters that need XML escaping.
func sanitizeInitial(name string) string {
	r, sz := utf8.DecodeRuneInString(name)
	if sz == 0 || r == utf8.RuneError {
		return "?"
	}
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return string(unicode.ToUpper(r))
	}
	return "?"
}

// defaultETag is a strong, deterministic validator over (seed, sanitized glyph).
func defaultETag(seed, name string) string {
	return fmt.Sprintf(`"%s-%x"`, svgTemplateVersion, stableHash(seed+sanitizeInitial(name)))
}

// renderDefaultSVG returns the same bytes for the same (seed, name) on every
// replica. seed picks the background colour; the first sanitized rune of name is
// the glyph. The glyph is html.EscapeString-escaped as defense-in-depth.
func renderDefaultSVG(seed, name string) []byte {
	// #nosec G115 -- palette is a fixed 8-element literal; len(palette) cannot overflow uint32
	bg := palette[stableHash(seed)%uint32(len(palette))]
	initial := html.EscapeString(sanitizeInitial(name))
	svg := fmt.Sprintf(
		`<svg xmlns="http://www.w3.org/2000/svg" width="120" height="120" viewBox="0 0 120 120">`+
			`<rect width="120" height="120" fill="%s"/>`+
			`<text x="60" y="60" font-family="sans-serif" font-size="60" fill="#ffffff" `+
			`text-anchor="middle" dominant-baseline="central">%s</text></svg>`,
		bg, initial)
	return []byte(svg)
}

// botObjectKey is the MinIO key chosen for a new bot upload; stored verbatim in
// the avatars doc and used as-is on reads.
func botObjectKey(account string) string { return "bot/" + account }

// employeePhotoURL builds the external employee-photo redirect target.
// base (EMPLOYEE_PHOTO_BASE_URL) owns the host and path — a trailing slash is
// tolerated — and this only appends the "{eid}_120.JPG" filename.
func employeePhotoURL(base, eid string) string {
	return fmt.Sprintf("%s/%s_120.JPG", strings.TrimRight(base, "/"), url.PathEscape(eid))
}
