package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sseServer writes each line verbatim with the SSE framing the client expects.
func sseServer(t *testing.T, lines []string, captured *map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if captured != nil {
			body, _ := io.ReadAll(r.Body)
			require.NoError(t, json.Unmarshal(body, captured))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, ln := range lines {
			_, _ = io.WriteString(w, ln+"\n")
		}
	}))
}

func TestStreamTranslator_MergePreservesWhitespace(t *testing.T) {
	var body map[string]any
	srv := sseServer(t, []string{
		`data: {"returnCode":96200,"returnMessage":"success","returnData":{"translation":"Hel"}}`,
		`data: {"returnCode":96200,"returnMessage":"success","returnData":{"translation":"lo "}}`,
		`data: {"returnCode":96200,"returnMessage":"success","returnData":{"translation":" world"}}`,
		`data: [DONE]`,
	}, &body)
	defer srv.Close()

	tr := newStreamTranslator(srv.URL, "secret", 5*time.Second)
	got, err := tr.Translate(context.Background(), "Hello  world", "de")
	require.NoError(t, err)
	assert.Equal(t, "Hello  world", got) // "Hel"+"lo "+" world", whitespace preserved verbatim
	assert.Equal(t, "Hello  world", body["text"])
	assert.Equal(t, "de", body["targetLang"])
	assert.Equal(t, false, body["applyWiki"])
}

func TestStreamTranslator_NonSuccessCodeErrors(t *testing.T) {
	srv := sseServer(t, []string{
		`data: {"returnCode":96500,"returnMessage":"boom","returnData":{"translation":""}}`,
		`data: [DONE]`,
	}, nil)
	defer srv.Close()

	tr := newStreamTranslator(srv.URL, "", 5*time.Second)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "96500")
}

func TestStreamTranslator_MissingDoneErrors(t *testing.T) {
	srv := sseServer(t, []string{
		`data: {"returnCode":96200,"returnMessage":"success","returnData":{"translation":"partial"}}`,
	}, nil) // stream ends (EOF) with no [DONE]
	defer srv.Close()

	tr := newStreamTranslator(srv.URL, "", 5*time.Second)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "[DONE]")
}
