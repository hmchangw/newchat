# NATS Request-Failure Classification and Payload-Cap Drift — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Report a missing or unresponsive NATS upstream as `unavailable`/503 instead of `internal`/500, and stop `notification-worker` from enforcing a hand-synced copy of the broker's `max_payload`.

**Architecture:** One pure helper, `natsutil.RequestFailure(op string, err error) error`, classifies two transport errors into typed `errcode` values and leaves everything else as a raw wrap. Eleven request/reply call sites adopt it; three that already classify correctly are deliberately left alone. Separately, `notification-worker` sources its payload cap from the server-advertised `nc.NatsConn().MaxPayload()` instead of an environment variable.

**Tech Stack:** Go 1.25, `github.com/nats-io/nats.go` v1.50.0, `pkg/errcode`, `stretchr/testify`, and `nats-server/v2` run in-process for tests that need a live broker (no Docker).

**Spec:** `docs/superpowers/specs/2026-08-11-nats-payload-noresponders-design.md`

## Global Constraints

- All commands go through `make` — never run raw `go` commands. Tests: `make test`. Lint: `make lint`. Build: `make build SERVICE=<name>`.
- All tests run with `-race` (the Makefile handles this).
- TDD is mandatory: write the failing test, run it and confirm it fails, then implement.
- Minimum 80% coverage per package; `pkg/` targets 90%+.
- Errors wrap with context: `fmt.Errorf("short description: %w", err)`. Never a bare `err`.
- Never compare errors by string — use `errors.Is` / `errors.As`.
- Logging is `log/slog` only, structured key-value fields. Never log tokens, passwords, or full message bodies.
- `errcode.WithCause` wraps an infra error, never another `*errcode.Error` (it panics otherwise, and semgrep guards it).
- `errcode.Error.Metadata` is **client-visible** and serialises to the wire. Never put a NATS subject, account, or internal topology in it.
- No new third-party dependencies.
- Keep changes minimal and focused — do not refactor unrelated code.
- There is **one** `TestMain` per Go package. `pkg/natsutil` already has one at `continuity_integration_test.go:21`. Do not add another.

---

## File Structure

**Created:**
- `pkg/natsutil/request_failure.go` — the classifier. One exported function, no state.
- `pkg/natsutil/request_failure_test.go` — unit tests (no network).
(no integration-test file — the real-server test is a unit test using the package's existing embedded-server helper)

**Modified:**
- `pkg/errcode/codes_platform.go` — two new `Reason` constants.
- Seven call-site files (Task 2) — 11 sites, since roomclient carries 4 and historyclient 2.
- `notification-worker/main.go`, `notification-worker/emit.go`, and two `docker-compose.yml` files (Task 3).
- `docs/client-api.md`, `docs/client-api/request-reply.md`, `docs/client-api/events.md`, `docs/error-handling.md` (Task 4).

---

### Task 1: `RequestFailure` helper and platform reasons

**Files:**
- Create: `pkg/natsutil/request_failure.go`
- Create: `pkg/natsutil/request_failure_test.go` (unit tests **and** the embedded-server test — no separate integration file)
- Modify: `pkg/errcode/codes_platform.go`

**Interfaces:**
- Consumes: `errcode.Unavailable`, `errcode.WithReason`, `errcode.WithCause` (existing).
- Produces: `natsutil.RequestFailure(op string, err error) error`, and the reasons `errcode.NatsNoResponders` (`"no_responders"`) and `errcode.NatsRequestTimeout` (`"upstream_timeout"`). Task 2 calls the function; Task 4 documents the reasons.

- [ ] **Step 1: Add the two reason constants**

Append to `pkg/errcode/codes_platform.go`, inside the existing `const` block:

```go
	// NatsNoResponders marks a request/reply whose subject had no subscriber —
	// the upstream service is down, not yet started, or not routed to this
	// site. Retryable once the upstream returns.
	NatsNoResponders Reason = "no_responders"

	// NatsRequestTimeout marks a request that was delivered but not answered
	// within the caller's timeout. Retryable.
	NatsRequestTimeout Reason = "upstream_timeout"
```

- [ ] **Step 2: Write the failing unit tests**

Create `pkg/natsutil/request_failure_test.go`:

```go
package natsutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
)

func TestRequestFailure(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantTyped  bool
		wantCode   errcode.Code
		wantReason errcode.Reason
	}{
		{
			name:       "no responders is unavailable",
			err:        nats.ErrNoResponders,
			wantTyped:  true,
			wantCode:   errcode.CodeUnavailable,
			wantReason: errcode.NatsNoResponders,
		},
		{
			name:       "timeout is unavailable",
			err:        nats.ErrTimeout,
			wantTyped:  true,
			wantCode:   errcode.CodeUnavailable,
			wantReason: errcode.NatsRequestTimeout,
		},
		{
			name:       "deadline exceeded is unavailable",
			err:        context.DeadlineExceeded,
			wantTyped:  true,
			wantCode:   errcode.CodeUnavailable,
			wantReason: errcode.NatsRequestTimeout,
		},
		{
			// Proves matching is errors.Is, not equality — the nats client
			// wraps its sentinels on some paths.
			name:       "wrapped no responders still matches",
			err:        fmt.Errorf("outer: %w", nats.ErrNoResponders),
			wantTyped:  true,
			wantCode:   errcode.CodeUnavailable,
			wantReason: errcode.NatsNoResponders,
		},
		{
			name:      "unrelated error stays a raw wrap",
			err:       errors.New("connection reset"),
			wantTyped: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RequestFailure("rooms-info rpc", tt.err)
			require.Error(t, got)
			require.Contains(t, got.Error(), "rooms-info rpc")

			var typed *errcode.Error
			ok := errors.As(got, &typed)
			require.Equal(t, tt.wantTyped, ok)
			if !tt.wantTyped {
				// The original error must remain unwrappable for callers
				// that inspect it further.
				require.ErrorIs(t, got, tt.err)
				return
			}
			require.Equal(t, tt.wantCode, typed.Code)
			require.Equal(t, tt.wantReason, typed.Reason)
		})
	}
}

func TestRequestFailure_NilReturnsNil(t *testing.T) {
	require.NoError(t, RequestFailure("rooms-info rpc", nil))
}

// The cause is server-side only. If it ever reaches the wire envelope it would
// leak internal detail to clients, which is the one thing errcode guarantees
// against.
func TestRequestFailure_CauseNeverSerialised(t *testing.T) {
	err := RequestFailure("rooms-info rpc", fmt.Errorf("dial 10.1.2.3:4222: %w", nats.ErrNoResponders))

	var typed *errcode.Error
	require.True(t, errors.As(err, &typed))

	data, mErr := json.Marshal(typed)
	require.NoError(t, mErr)
	require.NotContains(t, string(data), "10.1.2.3")
	require.NotContains(t, string(data), "no responders available")
}
```

- [ ] **Step 3: Run the tests and confirm they FAIL**

Run: `make test SERVICE=pkg/natsutil`
Expected: FAIL — `undefined: RequestFailure` (and `undefined: errcode.NatsNoResponders` if Step 1 was skipped).

- [ ] **Step 4: Write the implementation**

Create `pkg/natsutil/request_failure.go`:

```go
package natsutil

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
)

// RequestFailure classifies a NATS request/reply transport error.
//
// Two failures are availability problems rather than defects and are reported
// as such: nothing is subscribed to the subject (ErrNoResponders), and nobody
// answered in time (ErrTimeout / context.DeadlineExceeded). Both mean an
// upstream service is down, not yet started, or unreachable — not that this
// service is broken — so they classify as unavailable (HTTP 503) and tell the
// caller a retry is worthwhile.
//
// Every other error is wrapped raw and collapses to internal at the boundary,
// which is the correct treatment for a genuine fault.
//
// op describes what the caller was doing ("rooms-info rpc"), matching the
// fmt.Errorf convention it replaces. It reaches the client in the message, so
// it must not contain a token, account, or message body.
//
// The underlying error is attached with WithCause: server-side only, logged
// once by Classify, never serialised into the wire envelope. It deliberately
// does NOT go into Metadata, which is client-visible.
//
// context.Canceled is not handled here. Only admin-service maps it today, with
// a rationale specific to that idempotent call; see the spec's "Sites
// deliberately excluded".
func RequestFailure(op string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, nats.ErrNoResponders):
		return errcode.Unavailable(op+": no service responding",
			errcode.WithReason(errcode.NatsNoResponders),
			errcode.WithCause(err))
	case errors.Is(err, nats.ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		return errcode.Unavailable(op+": upstream did not respond in time",
			errcode.WithReason(errcode.NatsRequestTimeout),
			errcode.WithCause(err))
	default:
		return fmt.Errorf("%s: %w", op, err)
	}
}
```

- [ ] **Step 5: Run the tests and confirm they PASS**

Run: `make test SERVICE=pkg/natsutil`
Expected: PASS.

- [ ] **Step 6: Write the real-server test — as a UNIT test, not an integration test**

This test needs a live NATS server, but it must **not** be an integration test. `pkg/natsutil` already runs an **embedded, in-process** server in its ordinary unit tests via the helper `startTestNATSWithMaxPayload(t, maxPayload int32) *nats.Conn` in `reply_test.go:39`. It calls `natsserver.NewServer(&natsserver.Options{Port: -1})`, starts it, waits for readiness and registers both server and connection cleanups. No Docker, no testcontainers, no build tag.

Using it means this test runs under plain `make test` — locally, in CI, and in sandboxes with no Docker daemon — instead of being skipped everywhere the daemon is absent.

Append to `pkg/natsutil/request_failure_test.go` (the existing unit-test file — do **not** create a new file, and do **not** add a `TestMain`; the package already has one at `continuity_integration_test.go:21`):

```go
// Proves the mapping fires on an error the real client produces, not just on a
// hand-constructed sentinel. A request to a subject nobody subscribes to
// returns ErrNoResponders, because responder detection is on by default.
//
// Uses the package's existing embedded-server helper, so this is a unit test:
// no Docker, and it runs everywhere make test runs.
func TestRequestFailure_RealNoResponders(t *testing.T) {
	nc := startTestNATSWithMaxPayload(t, 0) // 0 = leave the server default

	_, reqErr := nc.Request("nobody.is.listening.here", []byte("{}"), 2*time.Second)
	require.Error(t, reqErr)

	got := RequestFailure("probe rpc", reqErr)

	var typed *errcode.Error
	require.True(t, errors.As(got, &typed), "expected a typed errcode, got %v", got)
	require.Equal(t, errcode.CodeUnavailable, typed.Code)
	require.Equal(t, errcode.NatsNoResponders, typed.Reason)
}
```

Add `"time"` to the file's imports.

- [ ] **Step 7: Run it**

Run: `make test SERVICE=pkg/natsutil`
Expected: PASS, with no Docker required.

If the request returns `nats: timeout` rather than `no responders`, responder detection was disabled on the connection — report it rather than relaxing the assertion to accept either error.

- [ ] **Step 8: Verify coverage of the new function**

Run: `go test -coverprofile=/tmp/c.out ./pkg/natsutil/... && go tool cover -func=/tmp/c.out | grep request_failure`
Expected: `RequestFailure` at 100.0% — it is a pure function with five branches, all covered by the table above.

(This is the one place a raw `go` command is acceptable, because the Makefile has no per-function coverage target. Do not substitute it for `make test`.)

- [ ] **Step 9: Lint and commit**

```bash
make lint
git add pkg/natsutil/request_failure.go pkg/natsutil/request_failure_test.go pkg/errcode/codes_platform.go
git commit -m "feat(natsutil): classify no-responders and request timeout as unavailable

A raw wrapped transport error collapses to internal at the boundary, so a
service that is down, unstarted, or unrouted reaches the client as a 500
and reads to an operator as an application bug. RequestFailure maps the
two availability failures to unavailable with a distinguishable reason,
and leaves every other error exactly as it was.

The cause is attached with WithCause (server-side, logged once) rather
than Metadata, which is client-visible."
```

---

### Task 2: Adopt the helper at 11 call sites

**Files:**
- Modify: `message-gatekeeper/fetcher_history.go:77`
- Modify: `broadcast-worker/parent_fetcher.go:56`
- Modify: `search-service/room_client.go:34`
- Modify: `notification-worker/parent_fetcher.go:70`
- Modify: `user-service/roomclient/client.go:36,59,81,101`
- Modify: `user-service/presenceclient/client.go:37`
- Modify: `user-service/historyclient/client.go:39,65`

**Interfaces:**
- Consumes: `natsutil.RequestFailure(op string, err error) error` from Task 1.
- Produces: nothing new.

**Do NOT touch these three sites.** They already classify correctly and converting them loses behaviour:

| Site | Why |
|---|---|
| `room-service/reader_history.go:53` | Already returns `errcode.Unavailable` with the **domain** reason `RoomReadReceiptsUnavailable`. The generic reason is less useful to the client. |
| `admin-service/room_onduty.go:87` | Already handles no-responders, timeout, deadline **and** `context.Canceled`, which the helper does not. |
| `notification-worker/presence.go:76` | Fire-and-forget goroutine; logs and returns, no error return path to classify. |

- [ ] **Step 1: Convert the eight single-line sites**

Each is a one-line replacement preserving the existing `op` text verbatim. Line numbers drift as you edit — locate by content.

```go
// message-gatekeeper/fetcher_history.go
-		return nil, fmt.Errorf("history request: %w", err)
+		return nil, natsutil.RequestFailure("history request", err)

// broadcast-worker/parent_fetcher.go
-		return nil, fmt.Errorf("history request for parent %s: %w", messageID, err)
+		return nil, natsutil.RequestFailure(fmt.Sprintf("history request for parent %s", messageID), err)

// search-service/room_client.go
-		return nil, fmt.Errorf("rooms-info rpc: %w", err)
+		return nil, natsutil.RequestFailure("rooms-info rpc", err)

// notification-worker/parent_fetcher.go
-		return nil, fmt.Errorf("history request for parent %s: %w", messageID, err)
+		return nil, natsutil.RequestFailure(fmt.Sprintf("history request for parent %s", messageID), err)

// user-service/presenceclient/client.go
-		return nil, fmt.Errorf("presence-query rpc: %w", err)
+		return nil, natsutil.RequestFailure("presence-query rpc", err)
```

- [ ] **Step 2: Convert the four `user-service/roomclient` sites**

```go
-		return nil, fmt.Errorf("rooms-info rpc: %w", err)
+		return nil, natsutil.RequestFailure("rooms-info rpc", err)

-		return nil, fmt.Errorf("thread-room-info rpc: %w", err)
+		return nil, natsutil.RequestFailure("thread-room-info rpc", err)

-		return fmt.Errorf("clear-all-thread-unread rpc: %w", err)
+		return natsutil.RequestFailure("clear-all-thread-unread rpc", err)

-		return model.Subscription{}, fmt.Errorf("create-dm rpc: %w", err)
+		return model.Subscription{}, natsutil.RequestFailure("create-dm rpc", err)
```

- [ ] **Step 3: Convert the two `user-service/historyclient` sites**

```go
-		return model.ThreadSubscriptionListResponse{}, fmt.Errorf("thread-list rpc: %w", err)
+		return model.ThreadSubscriptionListResponse{}, natsutil.RequestFailure("thread-list rpc", err)

-		return nil, fmt.Errorf("rooms-get rpc: %w", err)
+		return nil, natsutil.RequestFailure("rooms-get rpc", err)
```

- [ ] **Step 4: Fix imports**

Each modified file needs `github.com/hmchangw/chat/pkg/natsutil` imported. Several already import it; add it only where missing. Two files (`broadcast-worker/parent_fetcher.go`, `notification-worker/parent_fetcher.go`) still need `fmt` for the `Sprintf`; the others may no longer use `fmt` at all — if `make lint` reports an unused import, remove it.

Run: `make fmt`

- [ ] **Step 5: Verify no conversion was missed and no exclusion was touched**

```bash
grep -rn "\.Request(\|\.RequestMsg(" --include="*.go" . --exclude="*_test.go" | grep -v "^./tools/"
```

Expected: 14 lines. Then confirm by reading each that exactly 11 are followed by `natsutil.RequestFailure` and the 3 documented exclusions are unchanged.

```bash
git diff --name-only
```

Expected: exactly **7** files — 11 sites, since `roomclient` carries 4 and `historyclient` 2 (1+1+1+1+4+1+2 = 11). Do NOT convert anything extra to reach a higher count. If `room-service/`, `admin-service/` or `notification-worker/presence.go` appear, an exclusion was converted — revert that file.

- [ ] **Step 6: Build every touched service**

```bash
make build SERVICE=message-gatekeeper
make build SERVICE=broadcast-worker
make build SERVICE=search-service
make build SERVICE=notification-worker
make build SERVICE=user-service
```

Expected: all succeed.

- [ ] **Step 7: Run tests and lint**

Run: `make test` then `make lint`
Expected: all packages ok, 0 issues.

Some existing tests may assert on the exact error string of a transport failure. If one fails, the assertion is testing the old wrap format — update the assertion to match the new message, and say so in the report. Do not weaken an assertion to make it pass.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: classify NATS transport failures at 11 request/reply sites

Each site wrapped its transport error raw, so a missing upstream reached
the client as a 500. They now route through natsutil.RequestFailure.

Three sites are deliberately left alone: room-service already returns a
domain-specific reason for this failure, admin-service already handles
these errors plus context.Canceled, and notification-worker's presence
fetch is fire-and-forget with no error return to classify."
```

---

### Task 3: Source `notification-worker`'s payload cap from the broker

**Files:**
- Modify: `notification-worker/main.go:62` (config field), `:216` (emitter construction)
- Modify: `notification-worker/emit.go` (add the clamp helper)
- Modify: `notification-worker/deploy/user/docker-compose.yml:46`
- Modify: `notification-worker/deploy/bot/docker-compose.yml:39`
- Modify: `notification-worker/emit_test.go` (add clamp tests)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: nothing later tasks rely on.

**Why:** `NATS_MAX_PAYLOAD_BYTES` is a second source of truth for a value the broker already advertises. Its own comment says "must match broker max_payload" — nothing enforces that. If it drifts *below* the broker's real limit, `emit.go` rejects push batches the broker would have accepted, dropping mobile notifications with no error at the broker and no alert anywhere.

- [ ] **Step 1: Write the failing clamp test**

`MaxPayload()` returns `int64`; `mobileEmitter.maxPayloadBytes` is `int`. A bare cast is unchecked narrowing — `gosec` G115 flags it and `make sast` is a blocking gate. Add to `notification-worker/emit_test.go`:

```go
func TestClampPayloadCap(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		want int
	}{
		{name: "typical broker value", in: 1048576, want: 1048576},
		{name: "zero disables the guard", in: 0, want: 0},
		{name: "negative clamps to zero", in: -1, want: 0},
		{name: "above MaxInt clamps to MaxInt", in: math.MaxInt64, want: math.MaxInt},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, clampPayloadCap(tt.in))
		})
	}
}
```

Add `"math"` to that file's imports.

- [ ] **Step 2: Run it and confirm it FAILS**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — `undefined: clampPayloadCap`.

- [ ] **Step 3: Implement the clamp**

Add to `notification-worker/emit.go`:

```go
// clampPayloadCap narrows the broker's advertised max_payload to int without
// an unchecked conversion (gosec G115). A non-positive value disables the
// pre-flight guard, matching the emitter's existing `> 0` check.
func clampPayloadCap(n int64) int {
	if n <= 0 {
		return 0
	}
	if n > math.MaxInt {
		return math.MaxInt
	}
	return int(n)
}
```

Add `"math"` to `emit.go`'s imports.

- [ ] **Step 4: Run it and confirm it PASSES**

Run: `make test SERVICE=notification-worker`
Expected: PASS.

- [ ] **Step 5: Read the cap from the connection**

In `notification-worker/main.go`, replace the emitter construction:

```go
-	emitter := newMobileEmitter(&jsPublisher{js: otelJS}, wiring.PushSendSubject, cfg.NatsMaxPayloadBytes)
+	// The broker advertises max_payload in its INFO on connect, so this is
+	// always in step with the server. An env var was a second source of truth
+	// that silently dropped batches whenever it drifted below the real limit.
+	emitter := newMobileEmitter(&jsPublisher{js: otelJS}, wiring.PushSendSubject, clampPayloadCap(nc.NatsConn().MaxPayload()))
```

The connection variable at that point is `nc`. Confirm by reading the surrounding function — if it is named differently, use the actual name.

- [ ] **Step 6: Delete the config field**

Remove this line from the config struct in `notification-worker/main.go`:

```go
-	NatsMaxPayloadBytes    int                     `env:"NATS_MAX_PAYLOAD_BYTES"    envDefault:"262144"` // must match broker max_payload; emitter rejects any batch exceeding this
```

- [ ] **Step 7: Delete the compose entries**

```yaml
# notification-worker/deploy/user/docker-compose.yml
-      - NATS_MAX_PAYLOAD_BYTES=${NATS_MAX_PAYLOAD_BYTES:-262144}

# notification-worker/deploy/bot/docker-compose.yml
-      - NATS_MAX_PAYLOAD_BYTES=${NATS_MAX_PAYLOAD_BYTES:-262144}
```

- [ ] **Step 8: Verify the variable is gone repo-wide**

```bash
grep -rn "NATS_MAX_PAYLOAD_BYTES\|NatsMaxPayloadBytes" --include="*.go" --include="*.yml" --include="*.yaml" --include="*.md" .
```

Expected: no output. If a `.md` file documents the variable, delete that row too and note it in the report.

- [ ] **Step 9: Build, test, lint, commit**

```bash
make build SERVICE=notification-worker
make test
make lint
git add -A
git commit -m "fix(notification-worker): take max_payload from the broker, not an env var

NATS_MAX_PAYLOAD_BYTES was a second source of truth for a value the
broker advertises on connect. Its own comment said it must match
max_payload, but nothing enforced that: drift below the real limit made
the emitter reject push batches the broker would have accepted, dropping
mobile notifications with no error at the broker and no alert anywhere.

The narrowing from int64 is clamped rather than cast, since gosec flags
unchecked integer conversions and SAST is a blocking gate."
```

---

### Task 4: Documentation and final verification

**Files:**
- Modify: `docs/client-api.md` (§6 reason catalog)
- Modify: `docs/client-api/request-reply.md`
- Modify: `docs/client-api/events.md`
- Modify: `docs/error-handling.md`

**Interfaces:**
- Consumes: the reason constants from Task 1.
- Produces: nothing.

**Why this is mandatory, not optional:** CLAUDE.md requires that any change to a client-facing error surface updates `docs/client-api.md` in the same PR, and that the two derived views never drift from it.

- [ ] **Step 1: Add both reasons to the §6 catalog**

`docs/client-api.md` §6 has a "`reason` catalog (present today)" section. Add rows in that section's existing table format, matching the surrounding style (status, `code`, `reason`, example body):

```markdown
| 503 | `unavailable` | `no_responders` | `{ "code": "unavailable", "reason": "no_responders", "error": "rooms-info rpc: no service responding" }` — the upstream service is down, not yet started, or not routed to this site. Retry. |
| 503 | `unavailable` | `upstream_timeout` | `{ "code": "unavailable", "reason": "upstream_timeout", "error": "rooms-info rpc: upstream did not respond in time" }` — the request was delivered but not answered within the caller's timeout. Retry. |
```

Read the surrounding rows first and match their exact column layout — the table format in that section is the authority, not this snippet.

- [ ] **Step 2: Update both derived views**

Apply the same two rows to whichever error-reference sections exist in `docs/client-api/request-reply.md` and `docs/client-api/events.md`. Read each file's existing error section first; if a view has no reason catalog, it needs no change — say so explicitly in the report rather than inventing a section.

- [ ] **Step 3: Record the Tier-1 exception in `docs/error-handling.md`**

That guide's Tier 1 says infra failures return a raw wrapped error and collapse to `internal`. This PR carves out a deliberate exception, and without a note the helper looks like a violation of the guide. Add to the Tier 1 discussion:

```markdown
**One exception, at the NATS request/reply boundary.** `natsutil.RequestFailure`
classifies two transport errors — `nats.ErrNoResponders` and
`nats.ErrTimeout`/`context.DeadlineExceeded` — as `errcode.Unavailable` rather
than letting them collapse to `internal`. They mean an upstream is down or
unresponsive, not that this service is faulty, and a client can act on that
difference. Every other transport error still returns a raw wrap. Call it in
place of `fmt.Errorf` on the error returned by `nc.Request`/`RequestMsg`.
```

- [ ] **Step 3b: Correct a comment this PR made inaccurate**

`message-gatekeeper/handler.go` has a doc comment on `quoteFetchErrIsTerminal` (around lines 497-503) stating that "non-errcode infra failures (NATS timeout, no-responders, unmarshal) are transient". After Task 2, NATS timeout and no-responders no longer arrive as non-errcode failures — they arrive as `errcode.CodeUnavailable` and are handled by that function's explicit `case errcode.CodeUnavailable`. The classification outcome is unchanged (still transient, still degrades to a placeholder); only the comment's description of *how* is now wrong.

Read the current comment and correct just the inaccurate clause. Do not change any logic — `quoteFetchErrIsTerminal`'s behaviour is verified by `message-gatekeeper/handler_test.go:1435` and must stay as it is.

- [ ] **Step 4: Full verification sweep**

```bash
make test
make lint
make sast
```

`make sast` is a blocking CI gate that fails on medium-or-higher findings.

**Known environment limitation:** `govulncheck` and semgrep's registry rulesets (`p/golang`, `p/security-audit`) require egress to `vuln.go.dev` and `semgrep.dev`, which the sandbox's policy blocks with 403. `gosec` runs locally and must pass. Semgrep's in-repo rules can be run alone:

```bash
semgrep --config .semgrep/ --metrics=off --error .
```

Report exactly which scanners ran and which were blocked. Do **not** disable TLS verification, unset `HTTPS_PROXY`, or add a suppression to work around a blocked scanner.

- [ ] **Step 5: Confirm coverage did not regress**

```bash
go test -coverprofile=/tmp/c.out ./pkg/natsutil/... ./notification-worker/... && go tool cover -func=/tmp/c.out | tail -1
```

Expected: both packages at or above 80%. Report the actual numbers.

- [ ] **Step 6: Commit**

```bash
git add docs/
git commit -m "docs: document no_responders and upstream_timeout reasons

Both are client-observable, so client-api.md and its derived views carry
them. error-handling.md records why the NATS request boundary is a
deliberate exception to the Tier-1 rule that infra failures collapse to
internal."
```

---

## Self-Review

**Spec coverage:** A1 helper → Task 1 Steps 4-5. A2 reasons → Task 1 Step 1. A3 eleven conversions → Task 2 Steps 1-3. A4 three exclusions → Task 2's do-not-touch table plus the Step 5 guard. B payload cap → Task 3. Testing section → Task 1 Steps 2/6, Task 3 Step 1. Documentation impact → Task 4. Risks 1 and 2 (503 change, env removal) are PR-description items, carried in Task 3's commit message and the final PR body. Risk 3 (admin-service `Canceled`) is explicitly out of scope and needs no task.

**Placeholder scan:** No TBD/TODO. Every code step carries real code. The two doc steps that say "read the surrounding rows first" are deliberate — the table format in those files is the authority and inventing a layout from this plan would corrupt it.

**Type consistency:** `RequestFailure(op string, err error) error` is used identically in Tasks 1 and 2. `clampPayloadCap(int64) int` is defined in Task 3 Step 3 and used in Step 5. `errcode.NatsNoResponders` / `errcode.NatsRequestTimeout` are defined in Task 1 Step 1 and referenced in Task 1's tests and Task 4's docs, spelled the same everywhere.
