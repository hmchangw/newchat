# Design: User-Agent header on the app-only Microsoft Graph client

**Date:** 2026-07-20
**Status:** Approved (pending implementation)
**Area:** `pkg/msgraph`, `room-service`

## Problem

The shared app-only Microsoft Graph client in `pkg/msgraph/msgraph.go` (constructed
by `msgraph.New`) sets no explicit `User-Agent` header on any of its outbound
requests. It relies on Go's `net/http` default, `Go-http-client/1.1`.

room-service's Teams meeting RPC (`teamsMeeting` in `room-service/handler_teams.go`)
calls `graphClient.CreateOnlineMeeting`, so its Graph traffic goes out with that
default agent. In deployments where Microsoft Graph is fronted by a corporate
proxy/WAF that rejects non-browser agents, those requests can be blocked.

The presence client (`pkg/msgraph/presence.go`, an ROPC-grant client) already
solves this: it sends a configurable `User-Agent`, defaulting to a desktop-browser
string (`defaultUserAgent`), because it is expected to sit behind such a proxy/WAF.
The app-only client should get the same capability so the room-service Teams path
(and the other app-only surfaces) can pass the same fronting infrastructure.

## Goal

Every request the app-only `graphClient` makes carries a `User-Agent` header that:

- Is overridable per-environment via existing `Config.UserAgent`.
- Falls back to the shared browser-string `defaultUserAgent` when the override is empty.

## Non-goals

- No `ProxyURL` support for the app-only client — `Config.ProxyURL` remains honored
  only by the presence client (`NewPresenceClient`).
- No change to the `teamsMeeting` NATS RPC request/response contract. This is an
  outbound HTTP header to Microsoft Graph, not a client-facing (`chat.user.`)
  schema change, so **`docs/client-api.md` is not touched**.
- No new default behavior when Graph is reached directly: the browser string is a
  value Graph already accepts, so direct-to-Graph deployments are unaffected.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Scope | Whole app-only client | The header is set on every `graphClient` request (token fetch, `CreateOnlineMeeting`, `ResolveAccountIDs`, `ListUsers`), not just the meeting call, so all consumers benefit and there is one consistent code path. |
| Default value | Reuse browser `defaultUserAgent` | Matches presence behavior; the browser string is the value most likely to pass both Graph and a fronting proxy/WAF. |
| Env var name | `GRAPH_USER_AGENT` | Cross-service consistency with `user-presence-service` (which already uses `GRAPH_USER_AGENT`), even though room-service otherwise uses the `TEAMS_` prefix. |
| Injection style | Explicit `req.Header.Set` per request site (Approach A) | Mirrors the existing `presence.go` pattern (resolved `userAgent` field + explicit set); lowest risk; CLAUDE.md requires following existing patterns. Rejected: a `RoundTripper` wrapper (composes awkwardly with the existing `TLSInsecureSkipVerify` transport cloning) and a shared `newRequest` helper (broader refactor than needed). |

## Changes

### 1. `pkg/msgraph/msgraph.go` — the client

- Add a `userAgent string` field to the `graphClient` struct.
- In `New`, resolve it once, mirroring `NewPresenceClient`:
  ```go
  ua := cfg.UserAgent
  if ua == "" {
      ua = defaultUserAgent
  }
  g.userAgent = ua
  ```
  `UserAgent` is not an `Option`, so the value depends only on `cfg`; resolve it
  when `g` is constructed (before returning), no ordering constraint against the
  `opts` loop.
- Add `req.Header.Set("User-Agent", g.userAgent)` at all four request sites:
  - `accessToken` — token fetch to `login.microsoftonline.com`.
  - `CreateOnlineMeeting` — the room-service Teams meeting path.
  - `resolveChunk` — backs `ResolveAccountIDs`.
  - `fetchUsersPage` — backs `ListUsers`.
- Move the `defaultUserAgent` constant from `presence.go` into `msgraph.go` and
  generalize its doc comment (it is no longer presence-specific; both clients use
  it). `presence.go` keeps referencing `defaultUserAgent` unchanged — same package.
- Update the `Config.UserAgent` doc comment: it is now honored by both the app-only
  client (`New`) and the presence client (`NewPresenceClient`). `Config.ProxyURL`
  documentation stays presence-only.

### 2. `room-service/main.go` — wiring

- Add a config field near the `Teams*` fields:
  ```go
  GraphUserAgent string `env:"GRAPH_USER_AGENT" envDefault:""`
  ```
- Pass it through in the existing `msgraph.New(msgraph.Config{...})` block:
  ```go
  UserAgent: cfg.GraphUserAgent,
  ```

### 3. Tests — TDD, Red first

Per CLAUDE.md, write failing tests before implementation. Add to
`pkg/msgraph/msgraph_test.go`, using the existing `httptest.Server` +
`WithBaseURL`/`WithTokenURL` pattern and asserting `r.Header.Get("User-Agent")`:

- `CreateOnlineMeeting` sends `defaultUserAgent` when `Config.UserAgent` is empty.
- `CreateOnlineMeeting` sends the custom value when `Config.UserAgent` is set.
- The OAuth token request carries the `User-Agent` header.
- A directory (`ResolveAccountIDs`) or list-users (`ListUsers`) test asserts the
  header, covering `resolveChunk` / `fetchUsersPage`.

These mirror the existing presence UA tests in `presence_test.go`
(`TestGetPresencesByUserId_*`, including the default and override cases).

room-service needs no new unit test for the wiring itself (a struct-tag/env pass-
through), consistent with how other `Config` fields are treated.

## Risks

Minimal. The only behavioral change is the outbound `User-Agent` value flipping
from `Go-http-client/1.1` to the browser string (or a configured override).
Microsoft Graph accepts both, so no regression for direct-to-Graph deployments;
proxy/WAF-fronted deployments gain the ability to pass. The change is one struct
field, one resolution block, four one-line header sets, one config field, and one
wiring line, plus tests.

## Affected files

- `pkg/msgraph/msgraph.go` (client + moved constant + doc comments)
- `pkg/msgraph/presence.go` (remove the moved `defaultUserAgent` constant)
- `pkg/msgraph/msgraph_test.go` (new UA assertions)
- `room-service/main.go` (config field + wiring)
