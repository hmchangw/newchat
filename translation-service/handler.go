package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

var allowedLangs = map[string]bool{
	"zhTW": true, "zhCN": true, "en": true, "de": true, "ja": true,
}

// Handler validates translate requests, calls the backend, and publishes the
// TranslateResult on the requester's async response subject.
type Handler struct {
	translator Translator
	publish    func(ctx context.Context, subj string, data []byte) error
	now        func() int64
}

func NewHandler(t Translator, publish func(context.Context, string, []byte) error) *Handler {
	return &Handler{
		translator: t,
		publish:    publish,
		now:        func() int64 { return time.Now().UTC().UnixMilli() },
	}
}

// Translate is a natsrouter RegisterVoid handler: no synchronous reply. Success and
// failure are both delivered as a TranslateResult on
// chat.user.{account}.response.{requestID}.
func (h *Handler) Translate(c *natsrouter.Context, req model.TranslateRequest) error {
	account := c.Param("account")
	if req.RequestID == "" || account == "" {
		slog.WarnContext(c, "translate: missing requestId or account", "account", account)
		return nil
	}

	if req.Text == "" {
		h.publishResult(c, account, req.RequestID, req.TargetLang, "",
			errcode.BadRequest("text is empty", errcode.WithReason(errcode.TranslateEmptyText)))
		return nil
	}
	if !allowedLangs[req.TargetLang] {
		h.publishResult(c, account, req.RequestID, req.TargetLang, "",
			errcode.BadRequest("unsupported targetLang", errcode.WithReason(errcode.TranslateUnsupportedLang)))
		return nil
	}

	translated, err := h.translator.Translate(c, req.Text, req.TargetLang)
	if err != nil {
		h.publishResult(c, account, req.RequestID, req.TargetLang, "",
			fmt.Errorf("translate backend: %w", err))
		return nil
	}
	h.publishResult(c, account, req.RequestID, req.TargetLang, translated, nil)
	return nil
}

// publishResult builds and publishes the TranslateResult. On error it classifies
// once (Classify logs at a category-aware level) and fills the string envelope,
// mirroring room-worker's fillAsyncError.
func (h *Handler) publishResult(ctx context.Context, account, requestID, targetLang, translated string, resultErr error) {
	result := model.TranslateResult{
		RequestID:  requestID,
		Status:     model.TranslateStatusOK,
		TargetLang: targetLang,
		Timestamp:  h.now(),
	}
	if resultErr != nil {
		ctx = errcode.WithLogValues(ctx, "request_id", requestID)
		e := errcode.Classify(ctx, resultErr)
		result.Status = model.TranslateStatusError
		result.Error, result.Code, result.Reason = e.Message, string(e.Code), string(e.Reason)
	} else {
		result.TranslatedText = translated
	}

	data, err := json.Marshal(result)
	if err != nil {
		slog.ErrorContext(ctx, "translate: marshal result", "error", err, "request_id", requestID)
		return
	}
	if err := h.publish(ctx, subject.UserResponse(account, requestID), data); err != nil {
		slog.WarnContext(ctx, "translate: publish result failed", "error", err, "request_id", requestID)
	}
}
