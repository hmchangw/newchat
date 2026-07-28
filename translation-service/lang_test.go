package main

import "testing"

func TestNormalizeTargetLang(t *testing.T) {
	cases := []struct {
		name   string
		tag    string
		want   string
		wantOK bool
	}{
		// English family — backend code == primary subtag.
		{"en bare", "en", "en", true},
		{"en-US region", "en-US", "en", true},
		{"en-GB region", "en-GB", "en", true},
		{"EN-us mixed case", "EN-us", "en", true},
		// German family.
		{"de bare", "de", "de", true},
		{"de-DE region", "de-DE", "de", true},
		// Japanese family.
		{"ja bare", "ja", "ja", true},
		{"ja-JP region", "ja-JP", "ja", true},
		// Traditional Chinese — script or region resolves to zhTW.
		{"zh-Hant-TW full", "zh-Hant-TW", "zhTW", true},
		{"zh-Hant script only", "zh-Hant", "zhTW", true},
		{"zh-TW region only", "zh-TW", "zhTW", true},
		{"zh-HK region", "zh-HK", "zhTW", true},
		{"zh-MO region", "zh-MO", "zhTW", true},
		{"zh-Hant-HK script wins", "zh-Hant-HK", "zhTW", true},
		{"zh-hant lowercase", "zh-hant-tw", "zhTW", true},
		// Simplified Chinese — script or region resolves to zhCN.
		{"zh-Hans-CN full", "zh-Hans-CN", "zhCN", true},
		{"zh-Hans script only", "zh-Hans", "zhCN", true},
		{"zh-CN region only", "zh-CN", "zhCN", true},
		{"zh-SG region", "zh-SG", "zhCN", true},
		{"zh-MY region", "zh-MY", "zhCN", true},
		// Ambiguous / unsupported.
		{"bare zh ambiguous", "zh", "", false},
		{"empty off", "", "", false},
		{"unsupported fr", "fr", "", false},
		{"unsupported ko", "ko", "", false},
		{"zh unknown region", "zh-XX", "", false},
		{"whitespace trimmed", "  en-US  ", "en", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := normalizeTargetLang(tc.tag)
			if ok != tc.wantOK {
				t.Fatalf("normalizeTargetLang(%q) ok = %v, want %v", tc.tag, ok, tc.wantOK)
			}
			if got != tc.want {
				t.Fatalf("normalizeTargetLang(%q) = %q, want %q", tc.tag, got, tc.want)
			}
		})
	}
}
