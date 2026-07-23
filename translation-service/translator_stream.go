package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/restyutil"
)

// successReturnCode is the third-party "success" sentinel for a stream chunk.
const successReturnCode = 96200

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
// SSE chunks into one string.
type streamTranslator struct {
	client   *resty.Client
	endpoint string
}

func newStreamTranslator(endpoint, apiKey string, timeout time.Duration) *streamTranslator {
	opts := []restyutil.Option{restyutil.WithTimeout(timeout)}
	if apiKey != "" {
		opts = append(opts, restyutil.WithBearerToken(apiKey))
	}
	return &streamTranslator{client: restyutil.New(endpoint, opts...), endpoint: endpoint}
}

func (t *streamTranslator) Translate(ctx context.Context, text, targetLang string) (string, error) {
	resp, err := t.client.R().
		SetContext(ctx).
		SetHeader("Accept", "text/event-stream").
		SetBody(backendRequest{Text: text, TargetLang: targetLang, ApplyWiki: false}).
		SetDoNotParseResponse(true).
		Post("")
	if err != nil {
		return "", fmt.Errorf("translate request: %w", err)
	}
	body := resp.RawBody()
	defer body.Close()
	if resp.StatusCode() != http.StatusOK {
		return "", fmt.Errorf("translate backend status %d", resp.StatusCode())
	}

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var merged strings.Builder
	sawDone := false
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue // skip blank lines / other SSE fields
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			sawDone = true
			break
		}
		var chunk streamChunk
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			return "", fmt.Errorf("decode stream chunk: %w", err)
		}
		if chunk.ReturnCode != successReturnCode {
			return "", fmt.Errorf("translate backend returnCode %d: %s", chunk.ReturnCode, chunk.ReturnMessage)
		}
		merged.WriteString(chunk.ReturnData.Translation) // verbatim, whitespace preserved
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("read stream: %w", err)
	}
	if !sawDone {
		return "", fmt.Errorf("stream ended without [DONE]")
	}
	return merged.String(), nil
}
