package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read request body: %v", err)
				return
			}
			if err := json.Unmarshal(body, captured); err != nil {
				t.Errorf("decode request body: %v", err)
				return
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, ln := range lines {
			_, _ = io.WriteString(w, ln+"\n")
		}
	}))
}

// streamTranslatorTo wires a streamTranslator to translateURL, backed by an
// accessToken server that hands out a valid J2 token.
func streamTranslatorTo(t *testing.T, translateURL string) *streamTranslator {
	t.Helper()
	at := accessTokenServer(t, "J2-test", rfc3339In(time.Hour), nil)
	t.Cleanup(at.Close)
	return newStreamTranslator(translateURL, at.URL, "J1-test", 5*time.Second, time.Minute)
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

	tr := streamTranslatorTo(t, srv.URL)
	got, err := tr.Translate(context.Background(), "Hello  world", "de")
	require.NoError(t, err)
	assert.Equal(t, "Hello  world", got) // "Hel"+"lo "+" world", whitespace preserved verbatim
	assert.Equal(t, "Hello  world", body["text"])
	assert.Equal(t, "de", body["targetLang"])
	assert.Equal(t, false, body["applyWiki"])
}

func TestStreamTranslator_SendsAuthorizationHeader(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"returnCode":96200,"returnMessage":"success","returnData":{"translation":"ok"}}`+"\n")
		_, _ = io.WriteString(w, "data: [DONE]\n")
	}))
	defer srv.Close()

	tr := streamTranslatorTo(t, srv.URL)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.NoError(t, err)
	assert.Equal(t, "J2-test", gotAuth) // raw J2 token, no Bearer prefix
}

func TestStreamTranslator_NonSuccessCodeErrors(t *testing.T) {
	srv := sseServer(t, []string{
		`data: {"returnCode":96500,"returnMessage":"boom","returnData":{"translation":""}}`,
		`data: [DONE]`,
	}, nil)
	defer srv.Close()

	tr := streamTranslatorTo(t, srv.URL)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "96500")
}

func TestStreamTranslator_MissingDoneErrors(t *testing.T) {
	srv := sseServer(t, []string{
		`data: {"returnCode":96200,"returnMessage":"success","returnData":{"translation":"partial"}}`,
	}, nil) // stream ends (EOF) with no [DONE]
	defer srv.Close()

	tr := streamTranslatorTo(t, srv.URL)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "[DONE]")
}

func TestStreamTranslator_MalformedChunkErrors(t *testing.T) {
	srv := sseServer(t, []string{
		`data: {"returnCode":96200,"returnMessage":"success","returnData":{"translation":"ok"}}`,
		`data: {not-json}`,
		`data: [DONE]`,
	}, nil)
	defer srv.Close()

	tr := streamTranslatorTo(t, srv.URL)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode stream chunk")
}

// On a "failed to verify jwt" response, the client force-refreshes J2 and retries once.
func TestStreamTranslator_RefreshAndRetryOnJWTFailure(t *testing.T) {
	var translateCalls int32
	tSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&translateCalls, 1) == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"message": "failed to verify jwt"}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"returnCode":96200,"returnMessage":"success","returnData":{"translation":"你好"}}`+"\n")
		_, _ = io.WriteString(w, "data: [DONE]\n")
	}))
	defer tSrv.Close()

	var atCalls int32
	atSrv := accessTokenServer(t, "J2-fresh", rfc3339In(time.Hour), &atCalls)
	defer atSrv.Close()

	tr := newStreamTranslator(tSrv.URL, atSrv.URL, "J1", 5*time.Second, time.Minute)
	got, err := tr.Translate(context.Background(), "hi", "zhTW")
	require.NoError(t, err)
	assert.Equal(t, "你好", got)
	assert.Equal(t, int32(2), atomic.LoadInt32(&translateCalls)) // retried once after refresh
	assert.Equal(t, int32(2), atomic.LoadInt32(&atCalls))        // initial fetch + forced refresh
}

// A jwt failure that persists after refresh returns an error (no infinite retry).
func TestStreamTranslator_JWTFailurePersistsAfterRefresh(t *testing.T) {
	tSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"message": "failed to verify jwt"}`)
	}))
	defer tSrv.Close()
	atSrv := accessTokenServer(t, "J2", rfc3339In(time.Hour), nil)
	defer atSrv.Close()

	tr := newStreamTranslator(tSrv.URL, atSrv.URL, "J1", 5*time.Second, time.Minute)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to verify jwt")
}

// An accessToken fetch failure surfaces before any translate call is made.
func TestStreamTranslator_TokenFetchError(t *testing.T) {
	atSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer atSrv.Close()

	// The translate endpoint is never reached because the token fetch fails first.
	tr := newStreamTranslator("http://translate.invalid", atSrv.URL, "J1", 5*time.Second, time.Minute)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get access token")
}

// A jwt failure whose forced refresh also fails returns the refresh error, not a retry.
func TestStreamTranslator_RefreshErrorAfterJWTFailure(t *testing.T) {
	tSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"message": "failed to verify jwt"}`)
	}))
	defer tSrv.Close()

	var atCalls int32
	atSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&atCalls, 1) == 1 {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"token":"J2","expiresAt":"`+rfc3339In(time.Hour)+`","username":"u","jwtRequestId":"j"}`)
			return
		}
		w.WriteHeader(http.StatusInternalServerError) // the forced refresh fails
	}))
	defer atSrv.Close()

	tr := newStreamTranslator(tSrv.URL, atSrv.URL, "J1", 5*time.Second, time.Minute)
	_, err := tr.Translate(context.Background(), "hi", "zhTW")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refresh access token after jwt failure")
}

// A non-JWT error body with no SSE data surfaces a sanitized backend error.
func TestStreamTranslator_BackendErrorNonJWT(t *testing.T) {
	tSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `<html>internal server error</html>`) // not SSE, not JWT JSON
	}))
	defer tSrv.Close()

	tr := streamTranslatorTo(t, tSrv.URL)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "translate backend error")
	// A 500 is not a transient-unavailable signal — it must NOT be tagged
	// errBackendUnavailable (it stays an internal-collapsing backend error).
	assert.NotErrorIs(t, err, errBackendUnavailable)
}

// A 503 from the translate backend is a transient outage: it is tagged
// errBackendUnavailable so the handler replies `unavailable` (retryable)
// instead of collapsing to `internal`.
func TestStreamTranslator_ServiceUnavailable(t *testing.T) {
	tSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `<html>503 Service Unavailable</html>`) // not SSE, not JWT JSON
	}))
	defer tSrv.Close()

	tr := streamTranslatorTo(t, tSrv.URL)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	assert.ErrorIs(t, err, errBackendUnavailable)
	assert.Contains(t, err.Error(), "503")
}
