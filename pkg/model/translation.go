package model

// TranslateRequest is the client→server payload for the synchronous translate
// RPC on chat.user.{account}.request.translate.{siteID}.text. It is a NATS
// request/reply call: the reply is a TranslateResult on success, or an errcode
// error envelope on failure, correlated by NATS via the _INBOX reply subject.
// TargetLang is a BCP-47 tag (the user's settings.translateMessageInto value);
// translation-service normalizes it to the backend's language code
// (zhTW/zhCN/en/de/ja). These are wire-only structs (json tags only) — the
// backend never persists translations.
type TranslateRequest struct {
	Text       string `json:"text"`
	TargetLang string `json:"targetLang"`
}

// TranslateResult is the server→client reply for a successful translate RPC.
// Failures are returned as a standard errcode error envelope instead, so this
// type carries only the success payload. TargetLang echoes the client's original
// BCP-47 tag, not the mapped backend code.
type TranslateResult struct {
	TranslatedText string `json:"translatedText"`
	TargetLang     string `json:"targetLang"`
}
