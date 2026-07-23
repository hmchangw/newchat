# Translation Service — Async Text Translation API Design

## Summary

A new `translation-service` exposes an on-demand text-translation API to clients
over NATS. The client **publishes** a translate request; the service performs the
translation and **publishes** the result (success or failure) back on the client's
async response subject. The translation backend is a third-party streaming API; for
this iteration the backend is a **mock** (no network), behind a pluggable
`Translator` interface so the real streaming client can be swapped in later without
touching the handler.

The flow is pure pub/sub (no synchronous reply), reusing the codebase's existing
async-result convention (`chat.user.{account}.response.{requestID}`, the same
channel `msg.send` and `AsyncJobResult` already use).

## Scope

**In scope**
- New `translation-service` (flat `package main` service at repo root).
- Client-facing async translate RPC: request subject + result on the response subject.
- `pkg/model` request/result types; `pkg/subject` subject builders; `pkg/errcode`
  reason codes.
- Pluggable `Translator` backend: a **mock** implementation (default, used now) and a
  **streaming** implementation that speaks the third-party SSE contract (built and
  unit-tested now, enabled in production only once an endpoint is configured).
- `docs/client-api.md` (+ derived views) documentation.
- Unit tests (TDD) and an optional integration test.

**Out of scope (YAGNI)**
- Result caching (future: Valkey keyed on `(hash(text), targetLang)`).
- Batch translation of multiple texts in one request.
- Auto-translation of the message hot path (gatekeeper/broadcast untouched).
- Source-language detection surfaced to the client (the backend contract carries no
  detected-source field).
- Exposing `applyWiki` to the client (fixed `false` for now).

## Architecture

`translation-service` is a stateless NATS consumer. No MongoDB, no Cassandra, no
JetStream — the request/result flow is core NATS pub/sub. It does not participate in
message federation and does not touch any stream.

```
client ──publish TranslateRequest──▶ chat.user.{account}.request.translate.{siteID}
                                             │  (queue group: translation-service)
                                             ▼
                                     translation-service handler
                                             │  Translator.Translate(ctx, text, targetLang)
                                             ▼
                                     mock | streaming third-party (SSE, merged)
                                             │
client ◀─TranslateResult (ok|error)── chat.user.{account}.response.{requestID}
   (client already subscribed to chat.user.{account}.> )
```

## NATS Subjects

Added to `pkg/subject/subject.go`:

```go
// TranslateRequest is the concrete subject a client publishes a TranslateRequest to.
func TranslateRequest(account, siteID string) string {
    return fmt.Sprintf("chat.user.%s.request.translate.%s", account, siteID)
}

// TranslateRequestPattern is the wildcard the service QueueSubscribes to (raw NATS,
// account is a wildcard token extracted from the delivered subject).
func TranslateRequestPattern(siteID string) string {
    return fmt.Sprintf("chat.user.*.request.translate.%s", siteID)
}
```

The result is published to the **existing** `UserResponse(account, requestID)`
builder — `chat.user.{account}.response.{requestID}` — no new result subject is
introduced. The account is extracted from the delivered request subject (token index
2, `chat.user.<account>.request.translate.<siteID>`).

## Request / Result Types (`pkg/model/translation.go`)

```go
// TranslateRequest is the client→server payload published to
// chat.user.{account}.request.translate.{siteID}. RequestID is the client-generated
// correlation key; the result is published to chat.user.{account}.response.{RequestID}.
type TranslateRequest struct {
    RequestID  string `json:"requestId"`
    Text       string `json:"text"`
    TargetLang string `json:"targetLang"`
}

// TranslateResult is the async server→client result delivered on
// chat.user.{account}.response.{requestID}. It mirrors AsyncJobResult's envelope:
// Status is TranslateStatusOK / TranslateStatusError; on error Error/Code/Reason
// carry the classified errcode envelope, typed as string so pkg/model does not
// import pkg/errcode. Timestamp is set at publish via time.Now().UTC().UnixMilli().
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

`model_test.go` gains round-trip coverage for both types via the existing generic
`roundTrip` helper.

## Target Language

The client sends one of a fixed set; the value is passed through to the backend
**unchanged** (the frontend already sends the backend's expected codes):

```
zhTW | zhCN | en | de | ja
```

Validation lives in the handler. Unknown values are rejected before any backend call.

## Handler Flow

`translation-service/handler.go`:

1. Unmarshal `TranslateRequest` from the message payload. Malformed JSON →
   `BadRequest` result.
2. Extract `account` from the delivered subject.
3. Validate:
   - `RequestID` empty → drop (cannot address a response subject); log at warn. No
     result can be delivered without a correlation key.
   - `Text` empty → `BadRequest`, reason `TranslateEmptyText`.
   - `TargetLang` not in the allowed set → `BadRequest`, reason
     `TranslateUnsupportedLang`.
4. Call `Translator.Translate(ctx, text, targetLang)`.
5. On success → publish `TranslateResult{Status: ok, TranslatedText, TargetLang}`.
6. On error → classify via `errcode.Classify` and fill `Error/Code/Reason`
   (same shape as `AsyncJobResult`), publish `TranslateResult{Status: error, ...}`.
7. Publish to `subject.UserResponse(account, req.RequestID)`; best-effort (log on
   publish failure).

No length cap on `Text`.

### Error handling

Because delivery is async pub/sub (no synchronous reply), errors are **not** returned
through `errnats.Reply`. Instead the handler classifies the error once
(`errcode.Classify`) and embeds the envelope in the `TranslateResult` — mirroring
`room-worker`'s `fillAsyncError`. Validation errors use `errcode.BadRequest(...,
errcode.WithReason(...))`; backend failures are raw-wrapped `fmt.Errorf` and collapse
to `internal` at classification.

New reasons in `pkg/errcode/codes_translation.go`:

```go
const (
    TranslateUnsupportedLang Reason = "unsupported_lang" // 400: targetLang not in allowed set
    TranslateEmptyText       Reason = "empty_text"       // 400: text is empty
)
```

## Translator Backend

Interface defined in the consumer (`translation-service/translator.go`), with a
`//go:generate mockgen` directive producing `mock_translator_test.go`:

```go
// Translator turns source text into targetLang text. Implementations may call an
// external service; callers pass a context with a deadline.
type Translator interface {
    Translate(ctx context.Context, text, targetLang string) (string, error)
}
```

Selected at startup by config `TRANSLATION_BACKEND`:

### `mock` (default — used this iteration)

Returns deterministic mock output without any network call, e.g.
`"[<targetLang>] " + text` (exact form finalized in the plan). No credentials
required. Lets the whole service run end-to-end in dev/CI immediately.

### `stream` (third-party, built + tested now, enabled later)

Speaks the third-party streaming contract:

- **Request** (Resty POST, timeout set): `{"text": <text>, "targetLang": <targetLang>, "applyWiki": false}`.
- **Response**: Server-Sent Events. Each line is
  `data: {"returnCode":96200,"returnMessage":"success","returnData":{"translation":"..."}}`.
- **Merge rule**: for each chunk with `returnCode == 96200`, append
  `returnData.translation` **verbatim, in arrival order, preserving all whitespace**
  (no trim). The concatenation of all fragments is the final translated string.
- **Termination**: a line `data: [DONE]` ends the stream; the merged string is
  returned.
- **Errors**: any chunk with `returnCode != 96200`, or the stream ending (EOF /
  timeout / connection drop) **without** a `[DONE]`, is a backend failure (raw-wrapped
  error → `internal` at the boundary).

Constructor fails fast if `TRANSLATION_ENDPOINT` (and any required credential) is
empty when `TRANSLATION_BACKEND=stream`.

## Configuration (`caarlos0/env`)

| Env | Default | Notes |
|-----|---------|-------|
| `NATS_URL` | — | required |
| `SITE_ID` | — | required |
| `LOG_LEVEL` | `info` | |
| `TRANSLATION_BACKEND` | `mock` | `mock` \| `stream` |
| `TRANSLATION_ENDPOINT` | `` | required when backend=`stream` (checked at construction) |
| `TRANSLATION_API_KEY` | `` | secret; required when backend=`stream`; never logged |
| `TRANSLATION_HTTP_TIMEOUT` | `30s` | Resty client timeout |
| `MAX_WORKERS` | `100` | in-flight handler concurrency (semaphore) |

o11y wired once via `pkg/obs.Init`. Graceful shutdown via `pkg/shutdown.Wait`
(drain subscription, wait in-flight, `nc.Drain()`).

## Testing (TDD)

### Unit

- `pkg/subject/subject_test.go` — `TranslateRequest` / `TranslateRequestPattern`
  concrete strings.
- `pkg/model/model_test.go` — round-trip `TranslateRequest`, `TranslateResult`.
- `translation-service/translator_stream_test.go` — an `httptest` server emitting a
  scripted SSE stream: asserts multi-chunk merge preserves whitespace exactly;
  non-`96200` chunk → error; stream without `[DONE]` (EOF) → error; request body
  carries `applyWiki:false` and the pass-through `targetLang`.
- `translation-service/translator_mock_test.go` — mock output shape.
- `translation-service/handler_test.go` — table-driven with a mocked `Translator`:
  valid translate → `ok` result on `UserResponse`; empty text → `BadRequest`/
  `empty_text`; unsupported lang → `BadRequest`/`unsupported_lang`; backend error →
  `error` result with `internal` code; malformed payload → `BadRequest`; missing
  `requestId` → no publish. The publish function is injected as a field so tests
  capture published subject + payload without a real NATS connection.

### Integration (optional, `//go:build integration`)

- `translation-service/integration_test.go` — `testutil.NATS(t)` + an `httptest`
  fake SSE backend; publish a `TranslateRequest`, subscribe to the response subject,
  assert the merged `TranslateResult`. `TestMain` calls `testutil.RunTests(m)`.

Coverage ≥ 80% (target 90%+ on handler and translator).

## File Change Surface

| File | Change |
|------|--------|
| `pkg/errcode/codes_translation.go` | new — `TranslateUnsupportedLang`, `TranslateEmptyText` |
| `pkg/subject/subject.go` | add `TranslateRequest`, `TranslateRequestPattern` |
| `pkg/subject/subject_test.go` | add subject tests |
| `pkg/model/translation.go` | new — `TranslateRequest`, `TranslateResult`, status consts |
| `pkg/model/model_test.go` | add round-trip cases |
| `translation-service/main.go` | new — config, wiring, subscribe, shutdown |
| `translation-service/handler.go` | new — request handler |
| `translation-service/handler_test.go` | new |
| `translation-service/translator.go` | new — `Translator` iface + mock + stream + `//go:generate` |
| `translation-service/translator_stream_test.go` | new |
| `translation-service/translator_mock_test.go` | new |
| `translation-service/mock_translator_test.go` | generated (mockgen) |
| `translation-service/integration_test.go` | new (optional) |
| `translation-service/deploy/Dockerfile` | new (multi-stage `golang:1.25.12-alpine` → `alpine:3.21`) |
| `translation-service/deploy/docker-compose.yml` | new (NATS only) |
| `translation-service/deploy/azure-pipelines.yml` | new |
| `docs/client-api.md` | document the translate RPC (request + async result + errors) |
| `docs/client-api/request-reply.md` | derived view update |
| `docs/client-api/events.md` | derived view update (async result) |

## Design Decisions

- **Pub/sub over request/reply.** The client publishes and receives the result on
  `chat.user.{account}.response.{requestID}` — the existing async-result channel used
  by `msg.send` and `AsyncJobResult`. No new event subject is invented; the frontend's
  existing `chat.user.{account}.>` subscription already receives it.
- **`TranslateResult` mirrors `AsyncJobResult`.** Same `Status` + `Error/Code/Reason`
  string-typed envelope, so pkg/model stays free of a pkg/errcode import and the
  frontend parses one error shape everywhere.
- **Pluggable `Translator`, mock first.** The SSE parse-and-merge logic is real and
  unit-tested now against an `httptest` stream; only the live third-party endpoint is
  deferred. Switching to production is config-only.
- **Pass targetLang through unchanged.** The frontend sends the backend's codes
  (`zhTW/zhCN/en/de/ja`); no mapping layer to drift.
- **No length cap.** Per product requirement; the backend owns any size limits.
- **Whitespace-preserving merge.** Fragments are concatenated verbatim so
  spacing/formatting from the backend survives intact.
