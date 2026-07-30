package main

import (
	"fmt"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// Handler validates translate requests and returns the translated text as a
// synchronous request/reply response. natsrouter marshals the TranslateResult on
// success, or the errcode envelope when the handler returns a typed error.
type Handler struct {
	translator Translator
}

func NewHandler(t Translator) *Handler {
	return &Handler{translator: t}
}

// Translate is a natsrouter.Register handler: it returns the TranslateResult as a
// synchronous reply, or a typed errcode error that the router turns into the
// standard error envelope on the caller's _INBOX subject.
func (h *Handler) Translate(c *natsrouter.Context, req model.TranslateRequest) (*model.TranslateResult, error) {
	if req.Text == "" {
		return nil, errcode.BadRequest("text is empty", errcode.WithReason(errcode.TranslateEmptyText))
	}
	// targetLang is a BCP-47 tag (the client sends its settings.translateMessageInto
	// value unchanged); map it to the backend's language code. The reply still echoes
	// the client's original targetLang, not the mapped code.
	backendLang, ok := normalizeTargetLang(req.TargetLang)
	if !ok {
		return nil, errcode.BadRequest("unsupported targetLang", errcode.WithReason(errcode.TranslateUnsupportedLang))
	}

	translated, err := h.translator.Translate(c, req.Text, backendLang)
	if err != nil {
		return nil, fmt.Errorf("translate backend: %w", err)
	}
	return &model.TranslateResult{
		TranslatedText: translated,
		TargetLang:     req.TargetLang,
	}, nil
}
