package main

import (
	"context"
	"errors"
)

//go:generate mockgen -source=translator.go -destination=mock_translator_test.go -package=main

// errBackendUnavailable marks a transient upstream failure — the translation
// backend is temporarily unavailable (e.g. it returned HTTP 503). A Translator
// wraps it so the handler maps it to errcode.Unavailable (a retryable reply),
// instead of the `internal` collapse other backend errors take.
var errBackendUnavailable = errors.New("translate backend unavailable")

// Translator turns source text into targetLang text. Implementations may call an
// external service; callers pass a context with a deadline.
type Translator interface {
	Translate(ctx context.Context, text, targetLang string) (string, error)
}

// mockTranslator returns deterministic output without any network call. It is the
// default backend until the third-party endpoint is configured.
type mockTranslator struct{}

func (mockTranslator) Translate(_ context.Context, text, targetLang string) (string, error) {
	return "[" + targetLang + "] " + text, nil
}
