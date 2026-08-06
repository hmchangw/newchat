package errcode

// Translation-service client-facing reasons. Attached via WithReason so the
// frontend can branch on the specific validation failure.
const (
	TranslateUnsupportedLang Reason = "unsupported_lang" // 400: targetLang not in the allowed set
	TranslateEmptyText       Reason = "empty_text"       // 400: text is empty
	// TranslateBackendUnavailable: 503 when the upstream translate backend is
	// temporarily unavailable (e.g. it returns HTTP 503). Signals the client to
	// retry, rather than the `internal` collapse other backend failures take.
	TranslateBackendUnavailable Reason = "backend_unavailable"
)
