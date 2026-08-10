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

	"github.com/hmchangw/chat/pkg/errcode"
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
	return newStreamTranslator(translateURL, at.URL, staticJ1("J1-test"), 5*time.Second, time.Minute)
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
	// A returnCode rejection is a normal backend reply, not an upstream outage.
	var ec *errcode.Error
	assert.NotErrorAs(t, err, &ec)
}

// A 429 from the backend is a rate limit: return a too_many_requests errcode
// (reason rate_limited) and do NOT retry (unlike a jwt failure).
func TestStreamTranslator_RateLimitedReturnsTooManyRequests(t *testing.T) {
	var translateCalls int32
	tSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&translateCalls, 1)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"message":"rate limit exceeded"}`)
	}))
	defer tSrv.Close()

	tr := streamTranslatorTo(t, tSrv.URL)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)

	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeTooManyRequests, ec.Code)
	assert.Equal(t, errcode.TranslateRateLimited, ec.Reason)
	assert.Equal(t, int32(1), atomic.LoadInt32(&translateCalls)) // not retried
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

	tr := newStreamTranslator(tSrv.URL, atSrv.URL, staticJ1("J1"), 5*time.Second, time.Minute)
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

	tr := newStreamTranslator(tSrv.URL, atSrv.URL, staticJ1("J1"), 5*time.Second, time.Minute)
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
	tr := newStreamTranslator("http://translate.invalid", atSrv.URL, staticJ1("J1"), 5*time.Second, time.Minute)
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

	tr := newStreamTranslator(tSrv.URL, atSrv.URL, staticJ1("J1"), 5*time.Second, time.Minute)
	_, err := tr.Translate(context.Background(), "hi", "zhTW")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refresh access token after jwt failure")
}

// A non-JWT 4XX error body with no SSE data surfaces a sanitized backend error
// (not Unavailable — a 4XX is a caller/backend-contract issue, not an outage).
func TestStreamTranslator_BackendErrorNonJWT(t *testing.T) {
	tSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `<html>bad request</html>`) // not SSE, not JWT JSON
	}))
	defer tSrv.Close()

	tr := streamTranslatorTo(t, tSrv.URL)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "translate backend error")
	var ec *errcode.Error
	assert.NotErrorAs(t, err, &ec)
}

// An upstream 5XX with no SSE data classifies as errcode.Unavailable so the
// client can show a "third-party unavailable" message instead of a generic error.
func TestStreamTranslator_Upstream5xxUnavailable(t *testing.T) {
	tSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `<html>upstream down</html>`) // not SSE, not JWT JSON
	}))
	defer tSrv.Close()

	tr := streamTranslatorTo(t, tSrv.URL)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeUnavailable, ec.Code)
	assert.Equal(t, errcode.TranslateUpstreamUnavailable, ec.Reason)
}

// A transport failure (backend unreachable) classifies as errcode.Unavailable.
func TestStreamTranslator_TransportErrorUnavailable(t *testing.T) {
	tSrv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := tSrv.URL
	tSrv.Close() // connections to url are now refused

	tr := streamTranslatorTo(t, url)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeUnavailable, ec.Code)
	assert.Equal(t, errcode.TranslateUpstreamUnavailable, ec.Reason)
}

// A 5XX carrying valid-looking SSE data must still classify as Unavailable — the
// status is checked before the body is parsed, so no translation leaks out.
func TestStreamTranslator_Upstream5xxWithDataUnavailable(t *testing.T) {
	tSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "data: {\"returnCode\":0,\"returnData\":{\"translation\":\"leak\"}}\ndata: [DONE]\n")
	}))
	defer tSrv.Close()

	tr := streamTranslatorTo(t, tSrv.URL)
	out, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	assert.Empty(t, out) // the 5XX must not surface the "translation"
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeUnavailable, ec.Code)
	assert.Equal(t, errcode.TranslateUpstreamUnavailable, ec.Reason)
}

// A connection drop mid-stream (a non-EOF read error after headers arrive)
// classifies as Unavailable, same as an up-front transport failure.
func TestStreamTranslator_MidStreamDropUnavailable(t *testing.T) {
	tSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		// 200 promising more body than we send, then abrupt close → the client's
		// stream read fails with io.ErrUnexpectedEOF mid-stream.
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 4096\r\n\r\n")
		_, _ = io.WriteString(conn, ": keep-alive partial line without a newline")
		_ = conn.Close()
	}))
	defer tSrv.Close()

	tr := streamTranslatorTo(t, tSrv.URL)
	_, err := tr.Translate(context.Background(), "hi", "en")
	require.Error(t, err)
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeUnavailable, ec.Code)
	assert.Equal(t, errcode.TranslateUpstreamUnavailable, ec.Reason)
}
