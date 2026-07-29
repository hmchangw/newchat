package model

// TranslateRequest is the client→server payload published to
// chat.user.{account}.request.translate.{siteID}. RequestID is the
// client-generated correlation key; the result is published to
// chat.user.{account}.response.{RequestID}. TargetLang is a BCP-47 tag (the
// user's settings.translateMessageInto value); translation-service normalizes it
// to the backend's language code (zhTW/zhCN/en/de/ja). Timestamp is the
// client-side publish time (UTC ms); the service does not act on it.
type TranslateRequest struct {
	RequestID  string `json:"requestId"  bson:"requestId"`
	Text       string `json:"text"       bson:"text"`
	TargetLang string `json:"targetLang" bson:"targetLang"`
	Timestamp  int64  `json:"timestamp"  bson:"timestamp"`
}

// TranslateResult is the async server→client result delivered on
// chat.user.{account}.response.{requestID}. It mirrors AsyncJobResult's envelope:
// Status is TranslateStatusOK / TranslateStatusError; on error Error/Code/Reason
// carry the classified errcode envelope, typed as string so pkg/model does not
// import pkg/errcode. Timestamp is the event-level publish time (UTC ms).
type TranslateResult struct {
	RequestID      string `json:"requestId"                bson:"requestId"`
	Status         string `json:"status"                   bson:"status"`
	TranslatedText string `json:"translatedText,omitempty" bson:"translatedText,omitempty"`
	TargetLang     string `json:"targetLang,omitempty"     bson:"targetLang,omitempty"`
	Error          string `json:"error,omitempty"          bson:"error,omitempty"`
	Code           string `json:"code,omitempty"           bson:"code,omitempty"`
	Reason         string `json:"reason,omitempty"         bson:"reason,omitempty"`
	Timestamp      int64  `json:"timestamp"                bson:"timestamp"`
}

const (
	TranslateStatusOK    = "ok"
	TranslateStatusError = "error"
)
