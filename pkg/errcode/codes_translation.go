package errcode

// Translation-service client-facing reasons. Attached via WithReason so the
// frontend can branch on the specific validation failure.
const (
	TranslateUnsupportedLang     Reason = "unsupported_lang"     // 400: targetLang not in the allowed set
	TranslateEmptyText           Reason = "empty_text"           // 400: text is empty
	TranslateUpstreamUnavailable Reason = "upstream_unavailable" // 503: third-party translation backend returned 5XX or was unreachable
)
