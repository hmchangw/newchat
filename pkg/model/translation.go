package model

// TranslateRequest is the client→server payload published to
// chat.user.{account}.request.translate.{siteID}. RequestID is the
// client-generated correlation key; the result is published to
// chat.user.{account}.response.{RequestID}. TargetLang is one of
// zhTW/zhCN/en/de/ja and is passed through to the backend unchanged.
type TranslateRequest struct {
	RequestID  string `json:"requestId"`
	Text       string `json:"text"`
	TargetLang string `json:"targetLang"`
}

// TranslateResult is the async server→client result delivered on
// chat.user.{account}.response.{requestID}. It mirrors AsyncJobResult's envelope:
// Status is TranslateStatusOK / TranslateStatusError; on error Error/Code/Reason
// carry the classified errcode envelope, typed as string so pkg/model does not
// import pkg/errcode. Timestamp is the event-level publish time (UTC ms).
type TranslateResult struct {
	RequestID      string `json:"requestId"`
	Status         string `json:"status"`
	TranslatedText string `json:"translatedText,omitempty"`
	TargetLang     string `json:"targetLang,omitempty"`
	Error          string `json:"error,omitempty"`
	Code           string `json:"code,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Timestamp      int64  `json:"timestamp"`
}

const (
	TranslateStatusOK    = "ok"
	TranslateStatusError = "error"
)
