# nats-debug: X-Debug send controls

**Date:** 2026-06-26
**Status:** Approved — ready for plan
**Relationship:** Front-end affordance for two existing server-side capabilities — the metadata `X-Debug` verbosity ladder (`docs/superpowers/specs/2026-06-12-nats-debug-header-design.md`) and `X-Debug-Payload` payload capture (`docs/superpowers/specs/2026-06-14-debug-payload-capture-design.md`). This adds **no** new server behavior; it lets the `tools/nats-debug` UI *stamp* those headers on the messages it sends.

## Problem

`tools/nats-debug` can publish (source conn), subscribe (dest conn), and request/reply (request conn), but it sets **no NATS headers** on outbound messages. The services already honor `X-Debug: flow|debug|trace` (per-request verbose server-side logging) and `X-Debug-Payload: 1` (dev-only full payload capture, gated by `DEBUG_LOG_PAYLOADS`). A developer using the debug tool today has no way to flag a single message and exercise that machinery — they would have to hand-craft a message elsewhere. This closes that gap.

## Goals

- Stamp `X-Debug: flow|debug|trace` on outbound **Publish** and **Request** messages from the tool's UI.
- Stamp `X-Debug-Payload: 1` on the same two paths via a toggle.
- Reuse the wire-name constants from `pkg/natsutil` so the tool stays bound to the same contract as the services.
- Byte-identical to today's behavior when neither control is set.

## Non-Goals

- Displaying NATS headers on **received** (subscribed) messages.
- An arbitrary key/value header editor — the controls are limited to the `X-Debug` pair.
- Sending/surfacing `X-Request-ID`. For fire-and-forget publishes, the trace is joined server-side by the server-generated `request_id`, which the tool will not show. The **Request** path returns the reply directly. Correlation-ID plumbing is a possible future add-on, deliberately out of scope here.
- Any change to server behavior, the honor decision, or the rate limiter.

## Architecture

Three existing layers, one new value threaded through them:

```
UI publish/request form  ──(debug, debugPayload in JSON body)──▶  handler (handler.go)
handler  ──(DebugHeaders{Level, Payload})──▶  hub.Publish / hub.Request (hub.go)
hub      ──▶  nats.Msg with Header{ "X-Debug": <token>, "X-Debug-Payload": "1" }  ──▶  NATS (hub_nats.go)
```

No new endpoints, connections, or SSE changes.

## Section 1 — Hub interface & header construction (`hub.go`, `hub_nats.go`)

A small value type carries the two optional fields together:

```go
// DebugHeaders are the optional X-Debug headers stamped on an outbound message.
type DebugHeaders struct {
    Level   string // canonical token: "" (none) | "flow" | "debug" | "trace"
    Payload bool   // sets X-Debug-Payload: 1 when true
}
```

The two `Hub` methods gain it as a trailing param:

- `Publish(subject, payload string, dbg DebugHeaders) error`
- `Request(subject, payload string, timeoutMs int, dbg DebugHeaders) (string, error)`

`hub_nats.go` builds a `nats.Header` via a shared unexported helper and sends with `conn.PublishMsg` / `conn.RequestMsg`:

```go
func debugHeader(dbg DebugHeaders) nats.Header {
    h := nats.Header{}
    if dbg.Level != "" {
        h.Set(natsutil.DebugHeader, dbg.Level)   // "X-Debug"
    }
    if dbg.Payload {
        h.Set(natsutil.DebugPayloadHeader, "1")  // "X-Debug-Payload"
    }
    if len(h) == 0 {
        return nil // no headers → identical to today's Publish/Request
    }
    return h
}
```

When both fields are empty/false, `debugHeader` returns `nil` and the message is byte-identical to the current behavior. Changing the `Hub` interface requires regenerating `mock_hub_test.go` via `make generate SERVICE=tools/nats-debug`.

## Section 2 — Handler normalization (`handler.go`)

`publishRequest` and `natsRequestBody` each gain two optional fields:

```go
Debug        string `json:"debug"`        // "" | "flow" | "debug" | "trace"
DebugPayload bool   `json:"debugPayload"`
```

The handler normalizes the level through `natsutil.ParseDebugLevel(req.Debug).String()` before passing `DebugHeaders` to the hub. This reuses the exact server-side parse: `off`/`0`/empty/unknown collapse to `""` (no header emitted — matching the server's strict no-footgun rule), and `1`/`true`/`on` canonicalize to `debug`. No new error path — an unrecognized value simply means "no `X-Debug`". The dropdown only offers valid tokens; normalization guards against a malformed body sending a stray header.

## Section 3 — UI (`static/index.html`)

Both the Publish panel and the Request panel gain the same two compact controls above their send button:

- a `<select>`: `Off` (default) / `flow` / `debug` / `trace`
- an `X-Debug-Payload` checkbox

`doPublish()` and `doRequest()` include `debug` and `debugPayload` in the JSON body they already POST. A short helper note states that `X-Debug-Payload` only produces output where the target service has `DEBUG_LOG_PAYLOADS` enabled, so no body logging is expected in prod.

## Section 4 — Content safety

The tool only sends header *tokens* — it never logs message bodies itself, and the `X-Debug` ladder is metadata-only by construction server-side. `X-Debug-Payload` remains inert unless a target service opts in via `DEBUG_LOG_PAYLOADS`; the tool cannot override that gate. No content-safety invariant changes.

## Testing (TDD)

- **`handler_test.go`** — table-driven over each level string (`""`/`off`/`flow`/`debug`/`trace`/`1`/`true`/garbage/mixed-case) × payload flag; assert the mock hub's `Publish`/`Request` receives the expected normalized `DebugHeaders`. Cover both send paths.
- **`hub_nats_test.go`** — `debugHeader` builds the right `nats.Header` for: level only, payload only, both, neither (→ `nil`). If the file carries integration coverage, add a publish-with-header round-trip asserting a subscriber sees `X-Debug`/`X-Debug-Payload`.
- Coverage stays ≥80% for the tool package; new branches (normalization, header construction) covered including the empty/garbage cases.

## Files

Changed:
- `tools/nats-debug/hub.go` — `DebugHeaders` type; `Publish`/`Request` signatures.
- `tools/nats-debug/hub_nats.go` — `debugHeader` helper; `PublishMsg`/`RequestMsg`.
- `tools/nats-debug/handler.go` — request-body fields + normalization.
- `tools/nats-debug/static/index.html` — controls + JS body fields.
- `tools/nats-debug/mock_hub_test.go` — regenerated (`make generate`).
- `tools/nats-debug/handler_test.go`, `tools/nats-debug/hub_nats_test.go` — new tests.
- `tools/nats-debug/README.md` — document the controls and the `DEBUG_LOG_PAYLOADS` caveat.

No `docs/client-api.md` change: the `X-Debug` transport header is already documented there, and this is a dev-tool affordance, not a client-facing RPC change.
