# Translation API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a `translation-service` that translates client text on demand over NATS pub/sub — client publishes a `TranslateRequest`, service publishes a `TranslateResult` (success or failure) on the client's async response subject.

**Architecture:** Stateless NATS consumer (no DB, core NATS, no JetStream). A `natsrouter.RegisterVoid` handler validates the request, calls a pluggable `Translator` backend, and publishes the result to `chat.user.{account}.response.{requestID}` (the existing async-result channel). The backend is a **mock** now; a third-party SSE-streaming implementation is built and unit-tested behind the same interface, enabled later by config.

**Tech Stack:** Go 1.25, `nats.go`, `pkg/natsrouter`, `pkg/subject`, `pkg/model`, `pkg/errcode`, `pkg/obs`, `pkg/shutdown`, `pkg/restyutil` (resty v2), `go.uber.org/mock`, `testify`.

## Global Constraints

- Go 1.25; module `github.com/hmchangw/chat`; single root `go.mod`.
- Use `make` targets, never raw `go`: `make test SERVICE=translation-service`, `make generate SERVICE=translation-service`, `make build SERVICE=translation-service`, `make lint`.
- TDD Red-Green-Refactor for all code; ≥80% coverage (target 90%+ on handler/translator).
- All NATS payloads are typed structs from `pkg/model`, JSON via `encoding/json` (this service is not a hot-path sonic worker).
- Errors: named `errcode` constructors + `WithReason`; infra failures raw-wrapped `fmt.Errorf("...: %w", err)`.
- Every NATS event struct in `pkg/model` carries `Timestamp int64 json:"timestamp"`, set at publish via `time.Now().UTC().UnixMilli()`.
- Subject builders live in `pkg/subject`, never raw `fmt.Sprintf` at call sites.
- Config via `caarlos0/env` typed struct; `SCREAMING_SNAKE_CASE`; secrets `required`, never defaulted, never logged.
- Service layout: flat `package main` at repo root; `deploy/` with Dockerfile (`golang:1.25.12-alpine` → `alpine:3.21`), docker-compose.yml, azure-pipelines.yml.
- Client-facing wire changes update `docs/client-api.md` **and** derived views `docs/client-api/request-reply.md` + `docs/client-api/events.md`.
- Target languages (pass through unchanged, validate at handler): `zhTW`, `zhCN`, `en`, `de`, `ja`.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `pkg/subject/subject.go` | `TranslateRequest`, `TranslateRequestPattern` builders |
| `pkg/model/translation.go` | `TranslateRequest`, `TranslateResult`, status constants |
| `pkg/errcode/codes_translation.go` | `TranslateUnsupportedLang`, `TranslateEmptyText` reasons |
| `translation-service/translator.go` | `Translator` interface + `mockTranslator` + `//go:generate` |
| `translation-service/translator_stream.go` | `streamTranslator` (third-party SSE client + merge) |
| `translation-service/handler.go` | request validation, translate, publish result |
| `translation-service/main.go` | config, backend selection, wiring, shutdown |
| `translation-service/deploy/*` | Dockerfile, docker-compose.yml, azure-pipelines.yml |
| `docs/client-api.md` (+ derived views) | client-facing documentation |

---

## Task 1: Frontend interface — subjects, model types, reason codes

Delivers the wire contract the frontend codes against: the two subjects, the request/result payloads, and the error reasons.

**Files:**
- Modify: `pkg/subject/subject.go`
- Test: `pkg/subject/subject_test.go`
- Create: `pkg/model/translation.go`
- Test: `pkg/model/model_test.go`
- Create: `pkg/errcode/codes_translation.go`

**Interfaces:**
- Produces: `subject.TranslateRequest(account, siteID string) string`; `subject.TranslateRequestPattern(siteID string) string`; `model.TranslateRequest{RequestID, Text, TargetLang string}`; `model.TranslateResult{RequestID, Status, TranslatedText, TargetLang, Error, Code, Reason string; Timestamp int64}`; `model.TranslateStatusOK`, `model.TranslateStatusError`; `errcode.TranslateUnsupportedLang`, `errcode.TranslateEmptyText`.

- [ ] **Step 1: Write the failing subject test**

Add to `pkg/subject/subject_test.go`:

```go
func TestTranslateSubjects(t *testing.T) {
	assert.Equal(t, "chat.user.alice.request.translate.site-a",
		subject.TranslateRequest("alice", "site-a"))
	assert.Equal(t, "chat.user.{account}.request.translate.site-a",
		subject.TranslateRequestPattern("site-a"))
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `go test ./pkg/subject/ -run TestTranslateSubjects -v`. Expected: FAIL (undefined `subject.TranslateRequest`).

- [ ] **Step 3: Add the subject builders**

Append to `pkg/subject/subject.go` (near the other `chat.user.{account}.request.*` builders):

```go
// TranslateRequest is the subject a client publishes a TranslateRequest to.
// The service registers TranslateRequestPattern; the async result is published
// to UserResponse(account, requestID).
func TranslateRequest(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.translate.%s", account, siteID)
}

// TranslateRequestPattern is the natsrouter registration pattern; {account} is a
// named token read via c.Param("account").
func TranslateRequestPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.translate.%s", siteID)
}
```

- [ ] **Step 4: Run subject test to confirm it passes**

Run: `go test ./pkg/subject/ -run TestTranslateSubjects -v`. Expected: PASS.

- [ ] **Step 5: Write the failing model round-trip test**

Add to `pkg/model/model_test.go`:

```go
func TestTranslateRequestJSON(t *testing.T) {
	r := model.TranslateRequest{
		RequestID:  "01970a4f-8c2d-7c9a-abcd-e0123456789f",
		Text:       "Hello world",
		TargetLang: "zhTW",
	}
	roundTrip(t, &r, &model.TranslateRequest{})
}

func TestTranslateResultJSON_OK(t *testing.T) {
	r := model.TranslateResult{
		RequestID:      "01970a4f-8c2d-7c9a-abcd-e0123456789f",
		Status:         model.TranslateStatusOK,
		TranslatedText: "你好 世界",
		TargetLang:     "zhTW",
		Timestamp:      1_700_000_000_000,
	}
	roundTrip(t, &r, &model.TranslateResult{})
}

func TestTranslateResultJSON_Error(t *testing.T) {
	r := model.TranslateResult{
		RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
		Status:    model.TranslateStatusError,
		Error:     "unsupported targetLang",
		Code:      "bad_request",
		Reason:    "unsupported_lang",
		Timestamp: 1_700_000_000_000,
	}
	roundTrip(t, &r, &model.TranslateResult{})
}
```

- [ ] **Step 6: Run it to confirm it fails**

Run: `go test ./pkg/model/ -run TestTranslate -v`. Expected: FAIL (undefined `model.TranslateRequest`).

- [ ] **Step 7: Create the model types**

Create `pkg/model/translation.go`:

```go
package model

// TranslateRequest is the client→server payload published to
// chat.user.{account}.request.translate.{siteID}. RequestID is the
// client-generated correlation key; the result is published to
// chat.user.{account}.response.{RequestID}. TargetLang is one of
// zhTW/zhCN/en/de/ja and is passed through to the backend unchanged.
type TranslateRequest struct {
	RequestID  string `json:"requestId"`
	Text       string `json:"text"`
	TargetLang string `json:"targetLang"`
}

// TranslateResult is the async server→client result delivered on
// chat.user.{account}.response.{requestID}. It mirrors AsyncJobResult's envelope:
// Status is TranslateStatusOK / TranslateStatusError; on error Error/Code/Reason
// carry the classified errcode envelope, typed as string so pkg/model does not
// import pkg/errcode. Timestamp is the event-level publish time (UTC ms).
type TranslateResult struct {
	RequestID      string `json:"requestId"`
	Status         string `json:"status"`
	TranslatedText string `json:"translatedText,omitempty"`
	TargetLang     string `json:"targetLang,omitempty"`
	Error          string `json:"error,omitempty"`
	Code           string `json:"code,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Timestamp      int64  `json:"timestamp"`
}

const (
	TranslateStatusOK    = "ok"
	TranslateStatusError = "error"
)
```

- [ ] **Step 8: Run the model test to confirm it passes**

Run: `go test ./pkg/model/ -run TestTranslate -v`. Expected: PASS.

- [ ] **Step 9: Create the reason codes**

Create `pkg/errcode/codes_translation.go`:

```go
package errcode

// Translation-service client-facing reasons. Attached via WithReason so the
// frontend can branch on the specific validation failure.
const (
	TranslateUnsupportedLang Reason = "unsupported_lang" // 400: targetLang not in the allowed set
	TranslateEmptyText       Reason = "empty_text"       // 400: text is empty
)
```

- [ ] **Step 10: Verify the whole interface layer builds and tests pass**

Run: `go build ./pkg/... && go test ./pkg/subject/ ./pkg/model/ ./pkg/errcode/ -v`. Expected: PASS. Then `make lint`.

- [ ] **Step 11: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go pkg/model/translation.go pkg/model/model_test.go pkg/errcode/codes_translation.go
git commit -m "feat(translation): add frontend wire interface — subjects, model, reasons"
```

---

## Task 2: Translator interface + mock backend

**Files:**
- Create: `translation-service/translator.go`
- Test: `translation-service/translator_mock_test.go`
- Generated: `translation-service/mock_translator_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `Translator` interface `Translate(ctx context.Context, text, targetLang string) (string, error)`; `mockTranslator` struct; generated `NewMockTranslator(ctrl *gomock.Controller) *MockTranslator`.

- [ ] **Step 1: Write the failing mock-backend test**

Create `translation-service/translator_mock_test.go`:

```go
package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockTranslator_Translate(t *testing.T) {
	got, err := mockTranslator{}.Translate(context.Background(), "Hello", "zhTW")
	require.NoError(t, err)
	assert.Equal(t, "[zhTW] Hello", got)
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `go test ./translation-service/ -run TestMockTranslator -v`. Expected: FAIL (undefined `mockTranslator`).

- [ ] **Step 3: Create the interface + mock implementation**

Create `translation-service/translator.go`:

```go
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
```

- [ ] **Step 4: Run the mock test to confirm it passes**

Run: `go test ./translation-service/ -run TestMockTranslator -v`. Expected: PASS.

- [ ] **Step 5: Generate the mock**

Run: `make generate SERVICE=translation-service`. Expect `translation-service/mock_translator_test.go` created with `MockTranslator` / `NewMockTranslator`.

- [ ] **Step 6: Commit**

```bash
git add translation-service/translator.go translation-service/translator_mock_test.go translation-service/mock_translator_test.go
git commit -m "feat(translation): add Translator interface and mock backend"
```

---

## Task 3: Streaming third-party backend (SSE parse + merge)

**Files:**
- Create: `translation-service/translator_stream.go`
- Test: `translation-service/translator_stream_test.go`

**Interfaces:**
- Consumes: `Translator` (Task 2).
- Produces: `newStreamTranslator(endpoint, apiKey string, timeout time.Duration) *streamTranslator` implementing `Translator`.

- [ ] **Step 1: Write the failing stream tests**

Create `translation-service/translator_stream_test.go`:

```go
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
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `go test ./translation-service/ -run TestStreamTranslator -v`. Expected: FAIL (undefined `newStreamTranslator`).

- [ ] **Step 3: Implement the streaming backend**

Create `translation-service/translator_stream.go`:

```go
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/restyutil"

	"github.com/go-resty/resty/v2"
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
```

- [ ] **Step 4: Run the stream tests to confirm they pass**

Run: `go test ./translation-service/ -run TestStreamTranslator -v`. Expected: PASS (all three).

- [ ] **Step 5: Commit**

```bash
git add translation-service/translator_stream.go translation-service/translator_stream_test.go
git commit -m "feat(translation): add third-party SSE streaming backend with merge"
```

---

## Task 4: Request handler

**Files:**
- Create: `translation-service/handler.go`
- Test: `translation-service/handler_test.go`

**Interfaces:**
- Consumes: `Translator` (Task 2); `MockTranslator` (Task 2); `model.TranslateRequest`, `model.TranslateResult` (Task 1); `errcode.TranslateEmptyText`, `errcode.TranslateUnsupportedLang` (Task 1); `subject.UserResponse` (existing).
- Produces: `NewHandler(t Translator, publish func(context.Context, string, []byte) error) *Handler`; method `Translate(c *natsrouter.Context, req model.TranslateRequest) error`.

- [ ] **Step 1: Write the failing handler tests**

Create `translation-service/handler_test.go`:

```go
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

func newTestHandler(t Translator, cap *capturedPublish) *Handler {
	h := NewHandler(t, func(_ context.Context, subj string, data []byte) error {
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
	tr.EXPECT().Translate(gomock.Any(), "Hello", "zhTW").Return("你好", nil)

	var cap capturedPublish
	h := newTestHandler(tr, &cap)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})

	err := h.Translate(c, model.TranslateRequest{RequestID: "req-1", Text: "Hello", TargetLang: "zhTW"})
	require.NoError(t, err)

	assert.Equal(t, "chat.user.alice.response.req-1", cap.subj)
	res := decodeResult(t, &cap)
	assert.Equal(t, model.TranslateStatusOK, res.Status)
	assert.Equal(t, "你好", res.TranslatedText)
	assert.Equal(t, "zhTW", res.TargetLang)
	assert.Equal(t, "req-1", res.RequestID)
	assert.Equal(t, int64(1_700_000_000_000), res.Timestamp)
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
	assert.Equal(t, "internal", res.Code) // raw error collapses to internal
	assert.NotContains(t, res.Error, "upstream down") // internal cause never leaks
}

func TestHandler_Translate_MissingRequestID_NoPublish(t *testing.T) {
	var cap capturedPublish
	h := newTestHandler(mockTranslator{}, &cap)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})

	require.NoError(t, h.Translate(c, model.TranslateRequest{RequestID: "", Text: "hi", TargetLang: "en"}))
	assert.Equal(t, 0, cap.n) // cannot address a response subject without requestId
}
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `make test SERVICE=translation-service`. Expected: FAIL (undefined `Handler`, `NewHandler`).

- [ ] **Step 3: Implement the handler**

Create `translation-service/handler.go`:

```go
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
```

- [ ] **Step 4: Run the handler tests to confirm they pass**

Run: `make test SERVICE=translation-service`. Expected: PASS (all handler + translator tests).

- [ ] **Step 5: Commit**

```bash
git add translation-service/handler.go translation-service/handler_test.go
git commit -m "feat(translation): add request handler with async result publish"
```

---

## Task 5: main wiring, config, backend selection

**Files:**
- Create: `translation-service/main.go`
- Test: `translation-service/main_test.go`

**Interfaces:**
- Consumes: `Config`; `newTranslator`; `NewHandler` (Task 4); `mockTranslator` (Task 2); `newStreamTranslator` (Task 3); `subject.TranslateRequestPattern` (Task 1).
- Produces: `Config` struct; `newTranslator(cfg Config) (Translator, error)`.

- [ ] **Step 1: Write the failing backend-selection test**

Create `translation-service/main_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTranslator_Mock(t *testing.T) {
	tr, err := newTranslator(Config{Backend: "mock"})
	require.NoError(t, err)
	_, ok := tr.(mockTranslator)
	assert.True(t, ok)
}

func TestNewTranslator_StreamRequiresEndpoint(t *testing.T) {
	_, err := newTranslator(Config{Backend: "stream", Endpoint: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TRANSLATION_ENDPOINT")
}

func TestNewTranslator_Stream(t *testing.T) {
	tr, err := newTranslator(Config{Backend: "stream", Endpoint: "http://x", HTTPTimeout: time.Second})
	require.NoError(t, err)
	_, ok := tr.(*streamTranslator)
	assert.True(t, ok)
}

func TestNewTranslator_Unknown(t *testing.T) {
	_, err := newTranslator(Config{Backend: "bogus"})
	require.Error(t, err)
}
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `make test SERVICE=translation-service`. Expected: FAIL (undefined `Config`, `newTranslator`).

- [ ] **Step 3: Implement main.go**

Create `translation-service/main.go`:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/subject"
)

type NATSConfig struct {
	URL       string `env:"URL,required"`
	CredsFile string `env:"CREDS_FILE" envDefault:""`
}

type Config struct {
	SiteID      string        `env:"SITE_ID,required"`
	Backend     string        `env:"TRANSLATION_BACKEND"      envDefault:"mock"`
	Endpoint    string        `env:"TRANSLATION_ENDPOINT"     envDefault:""`
	APIKey      string        `env:"TRANSLATION_API_KEY"      envDefault:""`
	HTTPTimeout time.Duration `env:"TRANSLATION_HTTP_TIMEOUT" envDefault:"30s"`
	NATS        NATSConfig    `envPrefix:"NATS_"`
}

// newTranslator selects the backend. The stream backend fails fast without an
// endpoint so a misconfigured production deploy dies at startup, not per-request.
func newTranslator(cfg Config) (Translator, error) {
	switch cfg.Backend {
	case "mock":
		return mockTranslator{}, nil
	case "stream":
		if cfg.Endpoint == "" {
			return nil, fmt.Errorf("TRANSLATION_ENDPOINT is required when TRANSLATION_BACKEND=stream")
		}
		return newStreamTranslator(cfg.Endpoint, cfg.APIKey, cfg.HTTPTimeout), nil
	default:
		return nil, fmt.Errorf("unknown TRANSLATION_BACKEND %q (want mock|stream)", cfg.Backend)
	}
}

func main() {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	translator, err := newTranslator(cfg)
	if err != nil {
		slog.Error("init translator failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(ctx, cfg.NATS.URL, cfg.NATS.CredsFile, sdk.TracerProvider(), sdk.Propagator)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	publish := func(ctx context.Context, subj string, data []byte) error {
		return nc.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data))
	}

	handler := NewHandler(translator, publish)
	router := natsrouter.Default(nc, "translation-service")
	natsrouter.RegisterVoid(router, subject.TranslateRequestPattern(cfg.SiteID), handler.Translate)

	slog.Info("translation-service running", "site", cfg.SiteID, "backend", cfg.Backend)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `make test SERVICE=translation-service`. Expected: PASS. Then `make build SERVICE=translation-service`. Expected: builds clean.

- [ ] **Step 5: Verify coverage ≥ 80%**

Run: `go test ./translation-service/ -coverprofile=cover.out && go tool cover -func=cover.out | tail -1`. Expected: total ≥ 80%. Then remove `cover.out`.

- [ ] **Step 6: Commit**

```bash
git add translation-service/main.go translation-service/main_test.go
git commit -m "feat(translation): add service main, config, and backend selection"
```

---

## Task 6: Deploy artifacts

**Files:**
- Create: `translation-service/deploy/Dockerfile`
- Create: `translation-service/deploy/docker-compose.yml`
- Create: `translation-service/deploy/azure-pipelines.yml`

- [ ] **Step 1: Create the Dockerfile**

Create `translation-service/deploy/Dockerfile`:

```dockerfile
FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY translation-service/ translation-service/
RUN CGO_ENABLED=0 go build -o /translation-service ./translation-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /translation-service /translation-service
USER app
ENTRYPOINT ["/translation-service"]
```

- [ ] **Step 2: Create docker-compose.yml**

Create `translation-service/deploy/docker-compose.yml`:

```yaml
name: translation-service

services:
  translation-service:
    build:
      context: ../..
      dockerfile: translation-service/deploy/Dockerfile
    environment:
      - OTEL_SERVICE_NAME=translation-service
      - OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://otel-collector:4318}
      - NATS_URL=nats://nats:4222
      - NATS_CREDS_FILE=/etc/nats/backend.creds
      - SITE_ID=site-local
      - TRANSLATION_BACKEND=mock
    volumes:
      - ../../docker-local/backend.creds:/etc/nats/backend.creds:ro
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

- [ ] **Step 3: Create azure-pipelines.yml**

Create `translation-service/deploy/azure-pipelines.yml`:

```yaml
trigger:
  branches:
    include:
      - main
      - develop
  paths:
    include:
      - translation-service/
      - pkg/

pr:
  branches:
    include:
      - main
  paths:
    include:
      - translation-service/
      - pkg/

variables:
  GO_VERSION: '1.25.12'
  SERVICE_NAME: translation-service
  REGISTRY: '$(containerRegistry)'

stages:
  - stage: Validate
    displayName: 'Lint & Test'
    jobs:
      - job: LintAndTest
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: GoTool@0
            inputs:
              version: '$(GO_VERSION)'
            displayName: 'Install Go $(GO_VERSION)'

          - script: go vet ./$(SERVICE_NAME)/... ./pkg/...
            displayName: 'Go Vet'

          - script: go test ./pkg/... -v -race -coverprofile=coverage-pkg.out
            displayName: 'Test shared packages'

          - script: go test ./$(SERVICE_NAME)/... -v -race -coverprofile=coverage-$(SERVICE_NAME).out
            displayName: 'Test $(SERVICE_NAME)'

          - script: go build -o /dev/null ./$(SERVICE_NAME)/
            displayName: 'Build $(SERVICE_NAME)'

  - stage: Build
    displayName: 'Build & Push Image'
    dependsOn: Validate
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - job: BuildImage
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: Docker@2
            inputs:
              containerRegistry: '$(containerRegistry)'
              repository: 'chat/$(SERVICE_NAME)'
              command: 'buildAndPush'
              Dockerfile: '$(SERVICE_NAME)/deploy/Dockerfile'
              buildContext: '.'
              tags: |
                $(Build.BuildId)
                latest
            displayName: 'Build & push $(SERVICE_NAME)'
```

- [ ] **Step 4: Verify the build context works**

Run: `docker build -f translation-service/deploy/Dockerfile -t translation-service:local .` (if Docker available). Expected: image builds. If Docker is unavailable, skip and rely on `make build SERVICE=translation-service`.

- [ ] **Step 5: Commit**

```bash
git add translation-service/deploy/
git commit -m "chore(translation): add deploy Dockerfile, compose, pipeline"
```

---

## Task 7: Client API documentation

**Files:**
- Modify: `docs/client-api.md`
- Modify: `docs/client-api/request-reply.md`
- Modify: `docs/client-api/events.md`

- [ ] **Step 1: Read the surrounding conventions**

Read the `msg.send` section and the `AsyncJobResult` schema section in `docs/client-api.md` (search for `AsyncJobResult` and `response.{requestID}`) to match the exact table + example style, then mirror it.

- [ ] **Step 2: Document the request in `docs/client-api.md`**

Add a "Translate text" subsection under the user-RPC area. Include:

- **Publish subject:** `chat.user.{account}.request.translate.{siteID}` (client publishes; no synchronous reply).
- **Request body** field table:

| Field | Type | Notes |
|-------|------|-------|
| `requestId` | string | 36-char hyphenated UUID; correlation key for the async result |
| `text` | string | text to translate (no length cap) |
| `targetLang` | string | one of `zhTW`, `zhCN`, `en`, `de`, `ja` |

- **Async result subject:** `chat.user.{account}.response.{requestID}` (client must already be subscribed to `chat.user.{account}.>`).
- **`TranslateResult`** field table:

| Field | Type | Notes |
|-------|------|-------|
| `requestId` | string | echoes the request `requestId` |
| `status` | string | `ok` or `error` |
| `translatedText` | string | present when `status = ok` |
| `targetLang` | string | echoes the request |
| `error` | string | user-facing message when `status = error` |
| `code` | string | errcode category when `status = error` (e.g. `bad_request`, `internal`) |
| `reason` | string | domain reason when present (`unsupported_lang`, `empty_text`) |
| `timestamp` | int64 | publish time (UTC ms) |

- **Success example:**

```json
{
  "requestId": "01970a4f-8c2d-7c9a-abcd-e0123456789f",
  "status": "ok",
  "translatedText": "你好 世界",
  "targetLang": "zhTW",
  "timestamp": 1700000000000
}
```

- **Error cases:** `bad_request` / `empty_text` (empty `text`); `bad_request` / `unsupported_lang` (targetLang not in the set); `internal` (backend failure). A request with an empty `requestId` yields no result (undeliverable).
- **Triggered events:** none.

- [ ] **Step 3: Update the derived views**

Mirror the same request/result contract into `docs/client-api/request-reply.md` (the translate publish + async result) and note the `TranslateResult` async delivery in `docs/client-api/events.md`, matching how `AsyncJobResult` is presented there. Keep them byte-consistent with the canonical section.

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md docs/client-api/events.md
git commit -m "docs(client-api): document translate RPC and TranslateResult"
```

---

## Task 8 (optional): End-to-end integration test

Only if you want a NATS-level regression; the unit suite already covers logic.

**Files:**
- Create: `translation-service/integration_test.go`

- [ ] **Step 1: Write the integration test**

Create `translation-service/integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// Note: this drives the handler in-process (publishing via a real NATS conn) rather
// than standing up the full router, which is enough to exercise the publish path.

func TestTranslate_EndToEnd(t *testing.T) {
	url := testutil.NATS(t)
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(func() { nc.Drain() })

	sub, err := nc.SubscribeSync(subject.UserResponse("alice", "req-e2e"))
	require.NoError(t, err)

	h := NewHandler(mockTranslator{}, func(_ context.Context, subj string, data []byte) error {
		return nc.Publish(subj, data)
	})
	// Drive the handler directly with a router-shaped context carrying the account token.
	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	require.NoError(t, h.Translate(c, model.TranslateRequest{RequestID: "req-e2e", Text: "Hello", TargetLang: "en"}))

	msg, err := sub.NextMsg(2 * time.Second)
	require.NoError(t, err)
	var res model.TranslateResult
	require.NoError(t, json.Unmarshal(msg.Data, &res))
	require.Equal(t, model.TranslateStatusOK, res.Status)
	require.Equal(t, "[en] Hello", res.TranslatedText)
}
```

Note: `newE2EContext` builds a `*natsrouter.Context` with the `account` param set — reuse `natsrouter.NewContext(map[string]string{"account": account})`. Replace the placeholder helper call accordingly.

- [ ] **Step 2: Run it**

Run: `make test-integration SERVICE=translation-service` (requires Docker). Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add translation-service/integration_test.go
git commit -m "test(translation): add NATS end-to-end integration test"
```

---

## Final Verification

- [ ] `make generate SERVICE=translation-service` — mocks current
- [ ] `make test SERVICE=translation-service` — all green, race-clean
- [ ] `go test ./pkg/subject/ ./pkg/model/ ./pkg/errcode/` — green
- [ ] `make build SERVICE=translation-service` — builds
- [ ] `make lint` — clean
- [ ] `make sast` — no medium+ findings
- [ ] Coverage ≥ 80% on `translation-service`
- [ ] `docs/client-api.md` + derived views reflect the RPC
