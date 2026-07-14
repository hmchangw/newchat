package emoji

import "testing"

func TestStandardEmoji_PopulatedAndWireValid(t *testing.T) {
	if len(standardEmoji) < 100 {
		t.Fatalf("standardEmoji too small: %d", len(standardEmoji))
	}
	for _, want := range []string{"thumbsup", "+1", "heart"} {
		if _, ok := standardEmoji[want]; !ok {
			t.Errorf("expected standard shortcode %q to be present", want)
		}
	}
	for name := range standardEmoji {
		if !shortcodeRe.MatchString(name) {
			t.Errorf("standard shortcode %q does not match the wire regex", name)
		}
	}
}
