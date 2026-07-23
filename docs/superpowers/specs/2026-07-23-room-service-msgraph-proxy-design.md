# room-service Graph proxy — design

**Date:** 2026-07-23
**Status:** Approved (pending spec review)

## Problem

room-service builds its Microsoft Graph client via `msgraph.New(...)` — the
**meetings** client, backing the `onlineMeeting` create RPC. By design, `New`
(and `NewDirectoryClient`) **ignore** `Config.ProxyURL` and rely on the ambient
`HTTPS_PROXY`/`HTTP_PROXY` env vars (see the `Config.ProxyURL` docstring in
`pkg/msgraph/msgraph.go`). Only the error-returning constructors
(`NewUserListerClient`, `NewPresenceClient`, `NewChatsClient`,
`NewChatMembersClient`) call `applyProxyURL` and honor an explicit proxy.

In environments where Graph egress must go through a specific corporate proxy
(overriding the ambient env vars), room-service has no way to configure it. We
want a `GRAPH_PROXY_URL` config on room-service that is passed through to the
meetings client.

## Goal

Allow room-service to route its Graph meetings calls through an explicit proxy,
configured via `GRAPH_PROXY_URL`, overriding `HTTPS_PROXY`/`HTTP_PROXY`. Match
the convention already used by `teams-user-sync` and `user-presence-service`
(both expose `GRAPH_PROXY_URL` → `Config.ProxyURL`).

## Approach

Chosen approach: **new error-returning constructor** `msgraph.NewMeetingsClient`.
This mirrors the existing `NewUserListerClient` / `NewPresenceClient` pattern —
constructors that honor `ProxyURL` return an error so a malformed proxy URL
fails fast at construction rather than surfacing as an opaque per-request error.
`New` keeps its documented proxy-unaware behavior (it returns no error and so
cannot fail-fast).

Rejected alternatives:
- *Make `New` honor `ProxyURL` directly* — `New` returns no error, so a bad URL
  could only surface per-request; also silently changes `NewDirectoryClient`
  behavior.
- *A `WithProxyURL` functional Option* — Options can't return errors, so an
  invalid proxy URL would be silently swallowed, inconsistent with the
  fail-fast pattern used everywhere else in the package.

## Changes

### 1. `pkg/msgraph/msgraph.go` — `NewMeetingsClient`

```go
// NewMeetingsClient returns an app-only meetings client that honors
// cfg.ProxyURL (unlike New, which ignores it and relies on the standard proxy
// env vars). It mirrors NewUserListerClient: it applies the proxy after
// construction and reports an invalid value at construction rather than
// surfacing an opaque per-request error.
func NewMeetingsClient(cfg Config, opts ...Option) (Client, error) {
	g := New(cfg, opts...).(*graphClient)
	if err := applyProxyURL(g.httpClient, cfg.ProxyURL); err != nil {
		return nil, err
	}
	return g, nil
}
```

- Reuses the existing `applyProxyURL` helper — no new proxy logic.
- `applyProxyURL` → `mutableTransport` *clones* the transport, so a
  `TLSInsecureSkipVerify` transport installed by `New` is preserved; TLS-insecure
  and proxy compose. An empty `ProxyURL` is a no-op. A custom
  (non-`*http.Transport`) RoundTripper causes a fail-fast error.
- Update the `Config.ProxyURL` docstring: add `NewMeetingsClient` to the list of
  constructors that honor it; `New` / `NewDirectoryClient` still ignore it.

### 2. `room-service/main.go` — config + wiring

- Add config field, docstring matching the two sibling services:

  ```go
  // GraphProxyURL, when set, routes the meetings Graph client through this
  // proxy explicitly (overriding HTTPS_PROXY/HTTP_PROXY). Must include a scheme
  // and host, e.g. "http://proxy.corp:8080". Empty falls back to the standard
  // proxy env vars.
  GraphProxyURL string `env:"GRAPH_PROXY_URL" envDefault:""`
  ```

- In the Graph-client construction block, swap `msgraph.New(...)` for
  `msgraph.NewMeetingsClient(...)`, pass `ProxyURL: cfg.GraphProxyURL`, and
  handle the returned error with the existing fail-fast startup style
  (`slog.Error(...)` + `os.Exit(1)`):

  ```go
  graphClient, err = msgraph.NewMeetingsClient(msgraph.Config{
      TenantID:              cfg.TeamsTenantID,
      ClientID:              cfg.TeamsClientID,
      ClientSecret:          cfg.TeamsClientSecret,
      TLSInsecureSkipVerify: cfg.TeamsTLSInsecure,
      ProxyURL:              cfg.GraphProxyURL,
      UserAgent:             cfg.GraphUserAgent,
  })
  if err != nil {
      slog.Error("build graph meetings client", "error", err)
      os.Exit(1)
  }
  ```

  (`graphClient` is declared as `var graphClient msgraph.Client` outside the
  block; introduce the `err` binding as needed without shadowing it.)

## Tests (TDD — Red first)

### `pkg/msgraph/msgraph_test.go`

- `NewMeetingsClient` routes requests through a configured proxy: assert
  `g.httpClient.Transport.(*http.Transport).Proxy` is non-nil when `ProxyURL` is
  set, and returns the configured proxy URL. Mirror
  `TestNewChatsClient_MaxIdleConnsSurvivesProxy` (options survive proxy
  application) for the meetings constructor.
- Empty `ProxyURL` is a no-op (proxy falls back to `ProxyFromEnvironment` /
  transport default; construction succeeds).
- TLS-insecure + proxy compose: `TLSInsecureSkipVerify: true` + a `ProxyURL`
  yields a transport with both `TLSClientConfig.InsecureSkipVerify` and `Proxy`
  set.
- Custom RoundTripper + `ProxyURL` fails fast (mirror
  `TestNewChatsClient_ProxyRejectsCustomRoundTripper`).
- Extend `TestGraphClients_InvalidProxyURL` to include `NewMeetingsClient` in the
  set of constructors asserted to fail on a malformed proxy value.

### `room-service`

room-service's `main_test.go` is integration-tagged and has no config-parse unit
test today. If a lightweight config-parse assertion is warranted, add a
`config`-parsing test that confirms `GRAPH_PROXY_URL` maps to `GraphProxyURL`
(guarding the env tag), following whatever unit-test pattern fits without a
container. Otherwise the field is exercised via the msgraph constructor tests
plus a compile-time wiring check; no forced integration test.

## Docs

- `docs/msgraph-client.md`: note `NewMeetingsClient` honors `ProxyURL` (add it to
  the constructor list alongside the presence/chats/user-lister clients).
- No `docs/client-api.md` change — no client-facing NATS/HTTP handler request or
  response schema changes.

## Out of scope (YAGNI)

- Changing `New` / `NewDirectoryClient` proxy behavior.
- The deep-link RPCs (they use only `EmailDomain`, never call Graph).
- Any change to the presence/chats/members/user-lister proxy paths.
