package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

type capturedPublish struct {
	subj string
	data []byte
	n    int
}

func newTestHandler(tr Translator, cap *capturedPublish) *Handler {
	h := NewHandler(tr, func(_ context.Context, subj string, data []byte) error {
		cap.subj, cap.data, cap.n = subj, data, cap.n+1
		return nil
	})
	h.now = func() int64 { return 1_700_000_000_000 }
	return h
}

func decodeResult(t *testing.T, cap *capturedPublish) model.TranslateResult {
	t.Helper()
	var r model.TranslateResult
	require.NoError(t, json.Unmarshal(cap.data, &r))
	return r
}

func TestHandler_Translate_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	tr := NewMockTranslator(ctrl)
	// Client sends the BCP-47 tag from settings; the backend receives the mapped code.
	tr.EXPECT().Translate(gomock.Any(), "Hello", "zhTW").Return("你好", nil)

	var cap capturedPublish
	h := newTestHandler(tr, &cap)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})

	err := h.Translate(c, model.TranslateRequest{RequestID: "req-1", Text: "Hello", TargetLang: "zh-Hant-TW"})
	require.NoError(t, err)

	assert.Equal(t, "chat.user.alice.response.req-1", cap.subj)
	res := decodeResult(t, &cap)
	assert.Equal(t, model.TranslateStatusOK, res.Status)
	assert.Equal(t, "你好", res.TranslatedText)
	assert.Equal(t, "zh-Hant-TW", res.TargetLang) // echoes the client's BCP-47 value, not the backend code
	assert.Equal(t, "req-1", res.RequestID)
	assert.Equal(t, int64(1_700_000_000_000), res.Timestamp)
}

func TestHandler_Translate_MapsRegionTagToBaseLang(t *testing.T) {
	ctrl := gomock.NewController(t)
	tr := NewMockTranslator(ctrl)
	tr.EXPECT().Translate(gomock.Any(), "Hello", "en").Return("Hello", nil) // en-US → en

	var cap capturedPublish
	h := newTestHandler(tr, &cap)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})

	require.NoError(t, h.Translate(c, model.TranslateRequest{RequestID: "req-1a", Text: "Hello", TargetLang: "en-US"}))

	res := decodeResult(t, &cap)
	assert.Equal(t, model.TranslateStatusOK, res.Status)
	assert.Equal(t, "en-US", res.TargetLang) // client value echoed verbatim
}

func TestHandler_Translate_AmbiguousChineseRejected(t *testing.T) {
	var cap capturedPublish
	h := newTestHandler(mockTranslator{}, &cap)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})

	// Bare "zh" cannot be resolved to Traditional or Simplified → unsupported.
	require.NoError(t, h.Translate(c, model.TranslateRequest{RequestID: "req-3a", Text: "hi", TargetLang: "zh"}))

	res := decodeResult(t, &cap)
	assert.Equal(t, model.TranslateStatusError, res.Status)
	assert.Equal(t, "bad_request", res.Code)
	assert.Equal(t, "unsupported_lang", res.Reason)
	assert.Equal(t, "zh", res.TargetLang)
}

func TestHandler_Translate_EmptyText(t *testing.T) {
	var cap capturedPublish
	h := newTestHandler(mockTranslator{}, &cap)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})

	require.NoError(t, h.Translate(c, model.TranslateRequest{RequestID: "req-2", Text: "", TargetLang: "en"}))

	res := decodeResult(t, &cap)
	assert.Equal(t, model.TranslateStatusError, res.Status)
	assert.Equal(t, "bad_request", res.Code)
	assert.Equal(t, "empty_text", res.Reason)
}

func TestHandler_Translate_UnsupportedLang(t *testing.T) {
	var cap capturedPublish
	h := newTestHandler(mockTranslator{}, &cap)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})

	require.NoError(t, h.Translate(c, model.TranslateRequest{RequestID: "req-3", Text: "hi", TargetLang: "fr"}))

	res := decodeResult(t, &cap)
	assert.Equal(t, model.TranslateStatusError, res.Status)
	assert.Equal(t, "bad_request", res.Code)
	assert.Equal(t, "unsupported_lang", res.Reason)
}

func TestHandler_Translate_BackendError(t *testing.T) {
	ctrl := gomock.NewController(t)
	tr := NewMockTranslator(ctrl)
	tr.EXPECT().Translate(gomock.Any(), "hi", "en").Return("", errors.New("upstream down"))

	var cap capturedPublish
	h := newTestHandler(tr, &cap)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})

	require.NoError(t, h.Translate(c, model.TranslateRequest{RequestID: "req-4", Text: "hi", TargetLang: "en"}))

	res := decodeResult(t, &cap)
	assert.Equal(t, model.TranslateStatusError, res.Status)
	assert.Equal(t, "internal", res.Code)             // raw error collapses to internal
	assert.NotContains(t, res.Error, "upstream down") // internal cause never leaks
}

func TestHandler_Translate_MissingRequestID_NoPublish(t *testing.T) {
	var cap capturedPublish
	h := newTestHandler(mockTranslator{}, &cap)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})

	require.NoError(t, h.Translate(c, model.TranslateRequest{RequestID: "", Text: "hi", TargetLang: "en"}))
	assert.Equal(t, 0, cap.n) // cannot address a response subject without requestId
}

func TestHandler_Translate_PublishErrorIsLoggedNotReturned(t *testing.T) {
	ctrl := gomock.NewController(t)
	tr := NewMockTranslator(ctrl)
	tr.EXPECT().Translate(gomock.Any(), "hi", "en").Return("hola", nil)

	h := NewHandler(tr, func(_ context.Context, _ string, _ []byte) error {
		return errors.New("nats down")
	})
	h.now = func() int64 { return 1 }
	c := natsrouter.NewContext(map[string]string{"account": "alice"})

	// A publish failure is best-effort: logged, never returned to the router.
	require.NoError(t, h.Translate(c, model.TranslateRequest{RequestID: "req-5", Text: "hi", TargetLang: "en"}))
}
