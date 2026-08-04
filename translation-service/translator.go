package main

import "context"

//go:generate mockgen -source=translator.go -destination=mock_translator_test.go -package=main

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
