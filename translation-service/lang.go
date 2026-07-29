package main

import "strings"

// normalizeTargetLang maps a client-supplied BCP-47 language tag (the same shape
// stored in user settings.translateMessageInto, e.g. "en-US", "zh-Hant-TW") to the
// language code the translation backend expects (zhTW/zhCN/en/de/ja), reporting ok
// only when the tag resolves to a supported language.
//
// Matching is case-insensitive and tolerant of BCP-47 subtags: region and country
// variants collapse to their base language (en-GB→en), and Chinese resolves to
// Traditional (zhTW) or Simplified (zhCN) by script (Hant/Hans) or, absent a script,
// by region. A bare "zh" is ambiguous (no script or region to disambiguate) and is
// rejected, as is "" (translation off) and any language outside the supported set —
// the handler turns !ok into an unsupported_lang result.
//
// The supported set lives here as the single maintenance point; extending it means
// editing this function, not scattering codes across the handler.
func normalizeTargetLang(tag string) (backend string, ok bool) {
	subtags := strings.Split(strings.TrimSpace(tag), "-")
	lang := strings.ToLower(subtags[0])
	switch lang {
	case "en", "de", "ja":
		return lang, true
	case "zh":
		return normalizeChinese(subtags[1:])
	default:
		return "", false
	}
}

// normalizeChinese resolves the Traditional/Simplified split from the subtags after
// the "zh" primary subtag. A script subtag (Hant/Hans) is authoritative; otherwise a
// region subtag decides. Neither present ⇒ ambiguous ⇒ not ok.
func normalizeChinese(rest []string) (backend string, ok bool) {
	var region string
	for _, sub := range rest {
		low := strings.ToLower(sub)
		switch low {
		case "hant":
			return "zhTW", true
		case "hans":
			return "zhCN", true
		}
		if region == "" && len(low) == 2 {
			region = low
		}
	}
	switch region {
	case "tw", "hk", "mo":
		return "zhTW", true
	case "cn", "sg", "my":
		return "zhCN", true
	default:
		return "", false
	}
}
