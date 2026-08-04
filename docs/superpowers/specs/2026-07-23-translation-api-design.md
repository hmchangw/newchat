# Translation Service — Text Translation API Design

> **Revision (transport):** the RPC is now **synchronous NATS request/reply**
> (`natsrouter.Register`), not the fire-and-forget publish + async-response-subject
> shape this document originally described. A data-returning RPC gets an immediate,
> correlated reply, and handler saturation surfaces as an `unavailable` reply instead
> of a silent drop. Sections below have been updated; the canonical client contract is
> [client-api.md §3.6](../../client-api.md#36-translation-service).

## Summary

A new `translation-service` exposes an on-demand text-translation API to clients
over NATS. The client sends a translate request via **NATS request/reply**; the
service performs the translation and **replies** — a `TranslateResult` on success, or
the standard errcode envelope on failure — on the auto-generated `_INBOX` reply
subject. The translation backend is a third-party streaming API; for this iteration
the backend is a **mock** (no network), behind a pluggable `Translator` interface so
the real streaming client can be swapped in later without touching the handler.

The flow is synchronous request/reply — the same shape every other data-returning RPC
in the codebase uses (`natsrouter.Register`). The service fully merges the backend's
SSE stream into one string before replying, so the client-facing hop is unary.

## Scope

**In scope**
- New `translation-service` (flat `package main` service at repo root).
- Client-facing synchronous translate RPC: request subject + reply on the caller's `_INBOX`.
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
JetStream — the request/reply flow is core NATS. It does not participate in message
federation and does not touch any stream.

```text
client ──request TranslateRequest──▶ chat.user.{account}.request.translate.{siteID}.text
                                             │  (queue group: translation-service)
                                             ▼
                                     translation-service handler
                                             │  Translator.Translate(ctx, text, targetLang)
                                             ▼
                                     mock | streaming third-party (SSE, merged)
                                             │
client ◀── TranslateResult | errcode envelope ──  reply on _INBOX (NATS request/reply)
```

## NATS Subjects

Added to `pkg/subject/subject.go`:

```go
// TranslateRequest is the concrete subject a client sends a TranslateRequest to.
// The trailing `.text` action segment matches the repo's `<resource>.{siteID}.<action>`
// family (search.{siteID}.messages, …) and leaves room for a future `.batch`.
func TranslateRequest(account, siteID string) string {
    return fmt.Sprintf("chat.user.%s.request.translate.%s.text", account, siteID)
}

// TranslateRequestPattern is the natsrouter registration pattern; {account} is a
// named token that scopes the subject to the caller.
func TranslateRequestPattern(siteID string) string {
    return fmt.Sprintf("chat.user.{account}.request.translate.%s.text", siteID)
}
```

The reply travels on the auto-generated `_INBOX` subject that NATS request/reply
provides — no result subject is introduced and the handler does not read `{account}`
(the token only scopes the subject to the caller via their NATS-JWT permissions).

`{siteID}` is the caller's **own (local) site ID** — the local `translation-service`
registers `TranslateRequestPattern(cfg.SiteID)`. Translation is stateless and not
federated, so clients always address their local site; there is no origin-site rule
like `msg.send`.

## Request / Result Types (`pkg/model/translation.go`)

```go
// TranslateRequest is the client→server request/reply payload on
// chat.user.{account}.request.translate.{siteID}.text. TargetLang is a BCP-47 tag
// normalized to a backend code (zhTW/zhCN/en/de/ja).
type TranslateRequest struct {
    Text       string `json:"text"`
    TargetLang string `json:"targetLang"`
}

// TranslateResult is the server→client reply for a SUCCESSFUL translate RPC.
// Failures are returned as a standard errcode error envelope instead, so this type
// carries only the success payload. TargetLang echoes the client's original tag.
type TranslateResult struct {
    TranslatedText string `json:"translatedText"`
    TargetLang     string `json:"targetLang"`
}
```

`model_test.go` gains round-trip coverage for both types via the existing generic
`roundTrip` helper.

## Target Language

The client sends a **BCP-47 tag** — the user's `settings.translateMessageInto` value
sent unchanged, so translate aligns with the settings representation (see issue #2).
`translation-service` normalizes it to the backend's language code
(`zhTW | zhCN | en | de | ja`) at the boundary:

- `en*` / `de*` / `ja*` → `en` / `de` / `ja` (region/variant subtags dropped).
- `zh` with script `Hant`/`Hans`, or region `TW/HK/MO` vs `CN/SG/MY` → `zhTW` / `zhCN`.
- bare `zh`, `""` (translation off), or any other language → rejected as `unsupported_lang`.

Matching is case-insensitive. The mapping lives in `translation-service/lang.go`
(`normalizeTargetLang`) as the single maintenance point for extending the supported
set. The `TranslateResult` echoes the client's original tag, not the mapped code.
Validation lives in the handler; unresolvable tags are rejected before any backend call.

## Handler Flow

`translation-service/handler.go` — registered via `natsrouter.Register`, so the
handler signature is `func(*natsrouter.Context, TranslateRequest) (*TranslateResult, error)`:

1. `natsrouter` decodes the JSON payload into `TranslateRequest` before the handler
   runs. A malformed payload is rejected by the router with a `bad_request` reply on
   the caller's `_INBOX` — the handler never sees it.
2. Validate:
   - `Text` empty → return `errcode.BadRequest(..., WithReason(TranslateEmptyText))`.
   - `TargetLang` does not resolve via `normalizeTargetLang` → return
     `errcode.BadRequest(..., WithReason(TranslateUnsupportedLang))`.
3. Call `Translator.Translate(ctx, text, backendLang)` with the normalized code.
4. On success → return `&TranslateResult{TranslatedText, TargetLang}`; the router
   marshals it as the reply.
5. On backend error → return `fmt.Errorf("translate backend: %w", err)`; it collapses
   to `internal` at the boundary.

No length cap on `Text`.

### Error handling

Delivery is synchronous request/reply, so the handler simply returns a typed error and
the router (`natsrouter.Register` → `errnats.Marshal`/`errcode.Classify`) replies with
the `{code, reason?, error}` envelope on the caller's `_INBOX`, logging the cause once
server-side. Validation errors use `errcode.BadRequest(..., errcode.WithReason(...))`;
backend failures are raw-wrapped `fmt.Errorf` and collapse to `internal` at
classification. Handler saturation (the `WithMaxConcurrency` semaphore) is replied as
`errcode.Unavailable("service busy")` — a structured retry signal, never a silent drop.

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

### `mock` (used this iteration for dev/CI; must be selected explicitly)

Returns deterministic mock output without any network call, e.g.
`"[<targetLang>] " + text` (exact form finalized in the plan). No credentials
required. Lets the whole service run end-to-end in dev/CI immediately.

### `stream` (third-party, built + tested now, enabled later)

Speaks the third-party streaming contract:

- **Authentication (J1 → J2)**: a `tokenProvider` obtains the **J1** token from a
  `j1Source` — `TRANSLATION_J1_TOKEN` (local/dev) when set, otherwise the file at
  `TRANSLATION_J1_TOKEN_FILE` (default: the projected Kubernetes ServiceAccount token
  mount), **re-read on each exchange** so a rotated token is picked up without a restart.
  It POSTs the J1 to the accessToken API (`TRANSLATION_ACCESS_TOKEN_URL`)
  with `Content-Type: application/json` and the JSON body `{"key": <J1>}`, and receives
  `{token, expiresAt, username, jwtRequestId}`. The **J2** token (`token`) is cached until `expiresAt` minus a skew
  (`TRANSLATION_TOKEN_SKEW`, default 60s) and sent on the translate call as
  `Authorization: <J2>` (raw, no `Bearer`). The provider is mutex-guarded (safe for
  concurrent translate calls).
- **Reactive refresh + retry**: if the translate API replies
  `{"message": "failed to verify jwt"}` (token rejected mid-use), the client
  force-refreshes J2 and retries the translate call **once**; a second failure is a
  backend error.
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

`newTranslator` fails fast when `TRANSLATION_BACKEND=stream` if `TRANSLATION_ENDPOINT`
or `TRANSLATION_ACCESS_TOKEN_URL` is empty, or if neither `TRANSLATION_J1_TOKEN` nor a
readable `TRANSLATION_J1_TOKEN_FILE` yields a J1 token (the source is probed once at
startup). See the follow-up design
`2026-08-04-translation-j1-service-account-design.md`.

## Configuration (`caarlos0/env`)

| Env | Default | Notes |
|-----|---------|-------|
| `NATS_URL` | — | required |
| `SITE_ID` | — | required |
| `LOG_LEVEL` | `info` | |
| `TRANSLATION_BACKEND` | — | required (no default); `mock` \| `stream` |
| `TRANSLATION_ENDPOINT` | `` | translate API URL; required when backend=`stream` |
| `TRANSLATION_ACCESS_TOKEN_URL` | `` | accessToken (J1→J2) API URL; required when backend=`stream` |
| `TRANSLATION_J1_TOKEN` | `` | J1 token literal; local/dev + test only; wins when set; never logged |
| `TRANSLATION_J1_TOKEN_FILE` | `/var/run/secrets/kubernetes.io/serviceaccount/token` | file read for J1 when the literal is empty (prod: ServiceAccount token); re-read each exchange; never logged |
| `TRANSLATION_TOKEN_SKEW` | `60s` | refresh J2 this long before `expiresAt` |
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
- `translation-service/handler_test.go` — table-driven with a mocked `Translator`,
  calling the handler directly and asserting the returned `(*TranslateResult, error)`:
  valid translate → result with the translated text; empty text → `BadRequest`/
  `empty_text`; unsupported lang → `BadRequest`/`unsupported_lang`; backend error →
  raw (untyped) error that collapses to `internal`.

### Integration (optional, `//go:build integration`)

- `translation-service/integration_test.go` — registers the handler on a
  `natsrouter` against `testutil.NATS(t)` and drives it via `nc.Request`, asserting
  both a success `TranslateResult` reply and a `bad_request` envelope reply.
  `TestMain` calls `testutil.RunTests(m)`.

Coverage ≥ 80% (target 90%+ on handler and translator).

## File Change Surface

| File | Change |
|------|--------|
| `pkg/errcode/codes_translation.go` | new — `TranslateUnsupportedLang`, `TranslateEmptyText` |
| `pkg/subject/subject.go` | add `TranslateRequest`, `TranslateRequestPattern` |
| `pkg/subject/subject_test.go` | add subject tests |
| `pkg/model/translation.go` | new — `TranslateRequest`, `TranslateResult` |
| `pkg/model/model_test.go` | add round-trip cases |
| `translation-service/main.go` | new — config, wiring, register, shutdown |
| `translation-service/handler.go` | new — request/reply handler |
| `translation-service/handler_test.go` | new |
| `translation-service/translator.go` | new — `Translator` iface + mock + stream + `//go:generate` |
| `translation-service/translator_stream_test.go` | new |
| `translation-service/translator_mock_test.go` | new |
| `translation-service/mock_translator_test.go` | generated (mockgen) |
| `translation-service/integration_test.go` | new (optional) |
| `translation-service/deploy/Dockerfile` | new (multi-stage `golang:1.25.12-alpine` → `alpine:3.21`) |
| `translation-service/deploy/docker-compose.yml` | new (NATS only) |
| `translation-service/deploy/azure-pipelines.yml` | new |
| `docs/client-api.md` | document the translate RPC (request + reply + errors) |
| `docs/client-api/request-reply.md` | derived view update |
| `docs/client-api/events.md` | derived view update (remove async result) |

## Design Decisions

- **Synchronous request/reply.** A data-returning RPC replies on the caller's `_INBOX`
  the same way every other `natsrouter.Register` RPC does. The caller gets an immediate,
  correlated result; validation errors surface at once; and handler saturation replies
  `unavailable` instead of the silent drop a fire-and-forget publish would incur.
- **Errors via the standard errcode envelope.** The handler returns a typed error and
  the router replies `{code, reason?, error}`, so success and failure are two distinct
  shapes (a `TranslateResult` vs. an envelope) rather than one union with a `status`
  discriminator — matching the rest of the API.
- **Pluggable `Translator`, mock first.** The SSE parse-and-merge logic is real and
  unit-tested now against an `httptest` stream; only the live third-party endpoint is
  deferred. Switching to production is config-only.
- **Client sends BCP-47, service maps to backend codes.** The frontend sends its
  `settings.translateMessageInto` value unchanged; `normalizeTargetLang` maps it to
  the backend's `zhTW/zhCN/en/de/ja` at one boundary, so the two services share one
  representation instead of the frontend maintaining a lossy lookup table (issue #2).
- **No length cap.** Per product requirement; the backend owns any size limits.
- **Whitespace-preserving merge.** Fragments are concatenated verbatim so
  spacing/formatting from the backend survives intact.
