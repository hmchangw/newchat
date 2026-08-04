package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/restyutil"
)

// successReturnCode is the third-party "success" sentinel for a stream chunk.
const successReturnCode = 96200

// jwtFailureMessage is the message the translate API returns when the J2 token is
// rejected. On this response the client refreshes J2 and retries once.
const jwtFailureMessage = "failed to verify jwt"

type backendRequest struct {
	Text       string `json:"text"`
	TargetLang string `json:"targetLang"`
	ApplyWiki  bool   `json:"applyWiki"`
}

type streamChunk struct {
	ReturnCode    int    `json:"returnCode"`
	ReturnMessage string `json:"returnMessage"`
	ReturnData    struct {
		Translation string `json:"translation"`
	} `json:"returnData"`
}

// streamTranslator calls the third-party streaming translation API and merges the
// SSE chunks into one string. It authenticates with a J2 token obtained from
// tokens (J1 → accessToken API → J2).
type streamTranslator struct {
	client *resty.Client
	tokens *tokenProvider
}

func newStreamTranslator(endpoint, accessTokenURL, j1Token string, timeout, skew time.Duration) *streamTranslator {
	return &streamTranslator{
		client: restyutil.New(endpoint, restyutil.WithTimeout(timeout)),
		tokens: newTokenProvider(accessTokenURL, j1Token, timeout, skew),
	}
}

// Translate fetches a J2 token and calls the translate API. If the token is
// rejected ("failed to verify jwt"), it force-refreshes J2 and retries once.
func (t *streamTranslator) Translate(ctx context.Context, text, targetLang string) (string, error) {
	token, err := t.tokens.Token(ctx)
	if err != nil {
		return "", fmt.Errorf("get access token: %w", err)
	}

	result, jwtFailed, err := t.translateOnce(ctx, text, targetLang, token)
	if err != nil {
		return "", err
	}
	if jwtFailed {
		token, err = t.tokens.Refresh(ctx, token)
		if err != nil {
			return "", fmt.Errorf("refresh access token after jwt failure: %w", err)
		}
		result, jwtFailed, err = t.translateOnce(ctx, text, targetLang, token)
		if err != nil {
			return "", err
		}
		if jwtFailed {
			return "", fmt.Errorf("translate rejected: %s (after token refresh)", jwtFailureMessage)
		}
	}
	return result, nil
}

// translateOnce performs a single translate call. It returns jwtFailed=true (with
// nil error) when the response is a "failed to verify jwt" auth rejection, so the
// caller can refresh and retry.
func (t *streamTranslator) translateOnce(ctx context.Context, text, targetLang, token string) (result string, jwtFailed bool, err error) {
	resp, err := t.client.R().
		SetContext(ctx).
		SetHeader("Accept", "text/event-stream").
		SetHeader("Authorization", token).
		SetBody(backendRequest{Text: text, TargetLang: targetLang, ApplyWiki: false}).
		SetDoNotParseResponse(true).
		Post("")
	if err != nil {
		return "", false, fmt.Errorf("translate request: %w", err)
	}
	body := resp.RawBody()
	defer body.Close()

	reader := bufio.NewReader(body)
	var merged strings.Builder
	var nonSSE strings.Builder // accumulates a non-SSE body (potential error JSON)
	sawData := false
	sawDone := false
readLoop:
	for {
		line, readErr := reader.ReadString('\n')
		trimmed := strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(trimmed, "data:"):
			sawData = true
			payload := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
			if payload == "[DONE]" {
				sawDone = true
				break readLoop
			}
			var chunk streamChunk
			if uerr := json.Unmarshal([]byte(payload), &chunk); uerr != nil {
				return "", false, fmt.Errorf("decode stream chunk: %w", uerr)
			}
			if chunk.ReturnCode != successReturnCode {
				// ReturnMessage is third-party-controlled — do not propagate it into
				// caller-visible errors; keep only the trusted numeric returnCode.
				return "", false, fmt.Errorf("translate backend rejected request (returnCode %d)", chunk.ReturnCode)
			}
			merged.WriteString(chunk.ReturnData.Translation) // verbatim, whitespace preserved
		case trimmed != "":
			nonSSE.WriteString(trimmed)
		}
		// Check readErr after the switch: ReadString returns the last line together
		// with io.EOF (no trailing newline), so that line must be processed first.
		if readErr != nil {
			if readErr == io.EOF {
				break readLoop
			}
			return "", false, fmt.Errorf("read stream: %w", readErr)
		}
	}

	if !sawData {
		// No SSE payload — the response is an error body, possibly a JWT rejection.
		if isJWTFailure(nonSSE.String()) {
			return "", true, nil
		}
		return "", false, fmt.Errorf("translate backend error (status %d)", resp.StatusCode())
	}
	if !sawDone {
		return "", false, fmt.Errorf("stream ended without [DONE]")
	}
	return merged.String(), false, nil
}

// isJWTFailure reports whether body is a `{"message": "...failed to verify jwt..."}`
// auth-rejection response from the translate API.
func isJWTFailure(body string) bool {
	var e struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		return false
	}
	return strings.Contains(strings.ToLower(e.Message), jwtFailureMessage)
}
