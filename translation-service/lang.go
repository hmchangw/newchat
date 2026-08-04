package main

import "strings"

// normalizeTargetLang maps a client BCP-47 tag (the settings.translateMessageInto
// shape, e.g. "en-US", "zh-Hant-TW") to the backend's code (zhTW/zhCN/en/de/ja),
// ok=false for anything unsupported. A bare "zh" is rejected as ambiguous: with no
// script (Hant/Hans) or region there is no way to pick Traditional vs Simplified.
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
