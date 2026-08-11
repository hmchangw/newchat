# NATS request-failure classification and payload-cap drift

**Date:** 2026-08-11
**Status:** Approved
**Scope:** PR 2 of 2 from the NATS connection-handling review. PR 1 (blocking
drain + slow-consumer diagnostics) is `hmchangw/newchat#223` and is independent
of this work — neither PR depends on the other and they may merge in either
order.

## Problem

### A. A missing upstream is reported as a bug

Fourteen call sites issue NATS request/reply and all share one shape:

```go
msg, err := c.nc.Request(ctx, subject.RoomsInfoBatch(siteID), req, roomRPCTimeout)
if err != nil {
    return nil, fmt.Errorf("rooms-info rpc: %w", err)
}
if e, ok := errcode.Parse(msg.Data); ok {
    return nil, e
}
```

The third step is careful: a typed error *returned by the remote handler* is
relayed with its classification intact. The second step is not. A raw wrapped
error collapses to `internal` at the boundary, so every transport failure —
`nats.ErrNoResponders` (nothing is subscribed to that subject) and
`nats.ErrTimeout` (nobody answered in time) — surfaces to the client as a 500
and to the operator as a generic "request failed".

Those two failures are not bugs. They mean an upstream service is down,
not yet started, or unreachable across the supercluster. Reporting them as
`internal` costs twice over: the client cannot tell "retry in a moment" from
"this request will never work", and the on-call engineer starts debugging
application code for what is a deployment or routing problem.

The subject is not in the error either, so the log does not record which
upstream disappeared.

### B. A hand-synced payload cap can silently drop notifications

Three places deal with the NATS `max_payload` limit, by three different means:

| Site | Source of the limit |
|---|---|
| `room-service/main.go:273` | `nc.NatsConn().MaxPayload()` — server-advertised |
| `notification-worker/main.go:62` | `NATS_MAX_PAYLOAD_BYTES` env, `envDefault:"262144"` |
| `pkg/natsutil/reply.go:43` | post-hoc `errors.Is(err, nats.ErrMaxPayload)` |

The middle one is a real defect. `notification-worker/emit.go:43` rejects any
push batch larger than that env-sourced cap *before* publishing. The env value
carries the comment "must match broker max_payload", which is precisely the
problem: nothing enforces the match. If the broker's real `max_payload` is
larger than the env value, the worker rejects batches the broker would have
accepted, and mobile notifications are dropped by a configuration drift that
produces no error at the broker and no alert anywhere. If it is smaller, the
publish fails at the broker instead — caught, but with a worse error than the
pre-flight guard would have given.

This is not a silent-loss bug in the general case: `nats.go` already enforces
`max_payload` client-side before writing (`nats.go@v1.50.0:4345`), returning
`ErrMaxPayload`. The defect is specifically the second source of truth.

## Non-goals

- **No shared payload-check helper.** Two of the three sites are already
  correct; `reply.go`'s post-hoc fallback solves a different problem (turning an
  oversize *reply* into an error envelope rather than a client timeout) and is
  orthogonal to where the cap comes from. Building an abstraction over three
  callers to unify two lines is not worth the indirection.
- **No change to `reply.go`.** Its `ErrMaxPayload` fallback stays exactly as is.
- **No change to `room-service`.** It already reads the server-advertised value.
- **No retry or circuit-breaking.** Classifying the failure is this PR; deciding
  what a caller does about it is not.
- **No change to timeout *values*.** The per-call timeouts
  (`roomRPCTimeout`, `historyRequestTimeout`, …) are untouched.

## Design

### A1. `natsutil.RequestFailure`

A new file `pkg/natsutil/request_failure.go`:

```go
// RequestFailure classifies a NATS request/reply transport error. Two failures
// are availability problems rather than defects and are reported as such:
// nothing is subscribed to the subject, and nobody answered in time. Everything
// else is wrapped raw and collapses to internal at the boundary, which is the
// correct treatment for a genuine fault.
//
// op describes what the caller was doing ("rooms-info rpc"), matching the
// fmt.Errorf convention it replaces.
func RequestFailure(op string, err error) error
```

Behaviour:

| Input | Result |
|---|---|
| `nats.ErrNoResponders` | `errcode.Unavailable`, reason `NatsNoResponders`, `WithCause(err)` |
| `nats.ErrTimeout` | `errcode.Unavailable`, reason `NatsRequestTimeout`, `WithCause(err)` |
| `context.DeadlineExceeded` | `errcode.Unavailable`, reason `NatsRequestTimeout`, `WithCause(err)` |
| any other non-nil error | `fmt.Errorf("%s: %w", op, err)` |
| `nil` | `nil` |

Matching uses `errors.Is`, never string comparison.

**Why `Unavailable` for a timeout.** A timeout means no answer arrived, which is
closer to 503 than 500. Three properties keep this from being a regression:
`Classify` already groups `CodeUnavailable` with `CodeInternal` at
`slog.LevelError` (`pkg/errcode/classify.go:46`), so log severity and alert
volume do not move; `CodeUnavailable` maps to HTTP 503 (`category.go:44`), which
is the honest status; and `errcode.IsPermanent` stays false, so JetStream
workers Nak-and-retry rather than Ack-poisoning (verified at
`pkg/jsretry/jsretry.go:85` and `inbox-worker/main.go:698` — `IsPermanent`
requires an explicit `*PermanentError` wrapper, which this helper never
produces).

**The subject goes in the cause, not the metadata.** `errcode.Error.Metadata` is
explicitly client-visible (`pkg/errcode/options.go:44`) and serialises to the
wire envelope. Putting a NATS subject there would leak internal topology to
clients. `WithCause` is server-side only — never serialised, logged once by
`Classify` — which is exactly the right channel for "which upstream failed".
Callers already name the operation in `op`; that string must not contain a
token, account, room ID, or message body.

**Placement.** `pkg/natsutil` already imports `pkg/errcode`
(`request_id.go:14`), so there is no import cycle, and the helper sits beside
the other request-path helpers. It is not a fit for `pkg/errcode/errnats`,
which is the *reply*-side adapter.

### A2. Two new platform reasons

In `pkg/errcode/codes_platform.go`, which exists for reasons emitted by
cross-cutting middleware rather than one domain service:

```go
// NatsNoResponders marks a request whose subject had no subscriber — the
// upstream service is down, not yet started, or not routed to this site.
// Retryable once the upstream returns.
NatsNoResponders Reason = "no_responders"

// NatsRequestTimeout marks a request that was delivered but not answered
// within the caller's timeout. Retryable.
NatsRequestTimeout Reason = "upstream_timeout"
```

Distinct string values from the existing per-domain `upstream_unavailable`
reasons in `codes_botplatform.go` and `codes_translation.go`, which describe an
HTTP backend rather than a NATS peer.

### A3. Call-site conversion (14 sites)

Each replaces its raw wrap with the helper, preserving the existing `op` text:

```go
-		return nil, fmt.Errorf("rooms-info rpc: %w", err)
+		return nil, natsutil.RequestFailure("rooms-info rpc", err)
```

| File | Line |
|---|---|
| `room-service/reader_history.go` | 51 |
| `message-gatekeeper/fetcher_history.go` | 75 |
| `broadcast-worker/parent_fetcher.go` | 54 |
| `search-service/room_client.go` | 32 |
| `notification-worker/parent_fetcher.go` | 68 |
| `notification-worker/presence.go` | 76 |
| `user-service/roomclient/client.go` | 34, 57, 79, 99 |
| `user-service/presenceclient/client.go` | 35 |
| `user-service/historyclient/client.go` | 37, 63 |
| `admin-service/room_onduty.go` | 87 |

Nothing else in those functions changes — the `errcode.Parse` relay below each
call is already correct and stays.

### B. Payload cap from the broker

`notification-worker` reads the server-advertised limit at startup instead of
the environment:

- Delete `NatsMaxPayloadBytes` from the config struct (`main.go:62`).
- At the `newMobileEmitter` call (`main.go:216`), pass
  `nc.NatsConn().MaxPayload()`.
- Delete `NATS_MAX_PAYLOAD_BYTES` from
  `notification-worker/deploy/user/docker-compose.yml:46` and
  `notification-worker/deploy/bot/docker-compose.yml:39`.

`MaxPayload()` returns `int64` and `mobileEmitter.maxPayloadBytes` is `int`. The
conversion must be explicit and bounded rather than a bare cast — `gosec` G115
flags unchecked integer narrowing, and `make sast` is a blocking gate. Clamp to
`math.MaxInt` before narrowing.

`MaxPayload()` is only meaningful after the connection handshake, since the
value comes from the server INFO. The call site is already after
`natsutil.Connect`, so this is satisfied; the emitter's existing `> 0` guard
means a zero value disables the check rather than rejecting everything.

## Testing

Following TDD — tests first, confirmed failing, then implementation.

**`pkg/natsutil/request_failure_test.go`** — table-driven over: `ErrNoResponders`
→ `Unavailable` + `NatsNoResponders`; `ErrTimeout` → `Unavailable` +
`NatsRequestTimeout`; `context.DeadlineExceeded` → same; a wrapped
`ErrNoResponders` (via `fmt.Errorf("%w")`) → still matched, proving `errors.Is`
rather than equality; an unrelated error → stays a raw wrap; `nil` → `nil`.

Classification is asserted with `errors.As(err, &target)` against
`*errcode.Error` — **not** `errcode.Parse`, which takes the marshalled `[]byte`
envelope rather than a Go error and is the wrong tool for inspecting a returned
value. Assert the `op` text reaches the message, and separately marshal the
error and assert the cause does *not* appear in the wire envelope.

**Integration** (`//go:build integration`, `pkg/natsutil`) — against
`testutil.NATS(t)`, issue a request to a subject with no subscriber and assert
the result classifies as `Unavailable` with reason `no_responders`. This is the
test that proves the mapping fires on a real client error rather than on a
hand-constructed sentinel.

**`notification-worker`** — the emitter's existing tests take the cap as a
constructor parameter and are unaffected. Add a case asserting the clamp helper
narrows `int64` to `int` without overflow at the boundary.

Coverage floor is 80%; `pkg/` targets 90%+. `RequestFailure` is a pure function
with five branches and should reach 100%.

## Documentation impact

`NatsNoResponders` and `NatsRequestTimeout` are client-observable reasons, so
per CLAUDE.md the same PR must update:

- `docs/client-api.md` §6 (error envelope reference)
- `docs/client-api/request-reply.md` and `docs/client-api/events.md` — the
  derived views, which must not drift from the canonical document

`docs/error-handling.md` also needs a note: the Tier-1 rule says infra failures
return a raw wrapped error and collapse to `internal`. This PR carves out a
deliberate exception for two NATS transport errors, and the guide should say so
rather than leaving the helper looking like a violation.

No wire schema, event struct, or config schema changes otherwise. The only
config change is a removal (`NATS_MAX_PAYLOAD_BYTES`).

## Risks

1. **A 500 becomes a 503 for existing clients.** Any client branching on the
   numeric status for these failures sees a different code. This is the intended
   correction — the previous value was wrong — but it is a wire-visible change
   and belongs in the PR description.
2. **Removing `NATS_MAX_PAYLOAD_BYTES` is a deployment-visible change.** Any
   environment setting it will find the variable ignored. Since the new source
   is the broker's own advertised value, the setting was only ever able to make
   things worse; the PR description must call the removal out explicitly.
3. **`admin-service/room_onduty.go:87` uses `RequestMsg`, not `Request`.** The
   helper classifies the returned error and is agnostic to which call produced
   it, but the site should be verified individually rather than assumed
   identical to the other thirteen.

## Verification

- `make test` (full repo, `-race`), `make lint`, `make build` for every touched
  service
- `make test-integration SERVICE=pkg/natsutil` for the no-responder test
- `make sast` — note `govulncheck` and semgrep's registry rulesets are blocked
  by egress policy in the development sandbox; `gosec` and the in-repo
  `.semgrep/` rules run locally and CI must run the full gate
- `grep` sweep confirming no `nc.Request`/`RequestMsg` site still wraps its
  transport error with a bare `fmt.Errorf`
