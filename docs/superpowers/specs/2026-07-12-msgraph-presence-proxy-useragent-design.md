# msgraph presence: configurable proxy URL + User-Agent header

**Date:** 2026-07-12
**Branch:** `claude/msgraph-presence-proxy-ua-c27imi`
**Status:** Design — awaiting review

## Problem

Two independent gaps in the Microsoft Graph presence integration
(`pkg/msgraph`, consumed by `user-presence-service/sync`):

1. **No explicit proxy configuration.** The Graph client only routes through a
   proxy when the standard `HTTPS_PROXY`/`HTTP_PROXY` environment variables are
   set (via `http.ProxyFromEnvironment` on the default transport). There is no
   way to give the presence client a proxy URL directly through service config.

2. **Missing `User-Agent` header.** Presence requests (both the ROPC token
   acquisition and `getPresencesByUserId`) are sent without a `User-Agent`
   header. Microsoft Graph is rejecting these requests because a `User-Agent`
   is required.

## Scope

**Presence path only.** `msgraph.New` (app-only meetings, used by
`room-service`) and `msgraph.NewDirectoryClient` are intentionally left
unchanged. The new proxy field lives on the shared `msgraph.Config` but is
honored only by the presence client; the other constructors ignore it.

Decisions (confirmed with the requester):

| Question | Decision |
|----------|----------|
| Which clients get proxy + UA? | Presence client only |
| How is the User-Agent set? | Env-configurable (`GRAPH_USER_AGENT`); defaults to a desktop-browser string in the msgraph package |
| Which requests carry them? | Both the OAuth token request and the Graph presence request |

> **Revision (post-review):** the User-Agent was originally a static
> app-identifier constant. Because the presence client sits behind a corporate
> proxy/WAF (the same reason `GRAPH_PROXY_URL` exists), which commonly rejects
> non-browser agents, the value is now env-configurable with a desktop-browser
> default so it can be tuned per environment without a code change.

## Design

### 1. Config field (`pkg/msgraph/msgraph.go`)

Add to `msgraph.Config`:

```go
// ProxyURL, when non-empty, routes the presence client's HTTP requests
// through this proxy (overriding HTTPS_PROXY/HTTP_PROXY). Honored only by the
// presence client (NewPresenceClient); the app-only and directory clients
// ignore it. Must include a scheme (e.g. "http://proxy.corp:8080").
ProxyURL string
```

### 2. Proxy-aware presence client (`pkg/msgraph/presence.go`)

`NewPresenceClient` gains an error return so a malformed proxy URL fails fast at
construction rather than surfacing as an opaque per-request error:

```go
func NewPresenceClient(cfg Config, creds ROPCCredentials, opts ...Option) (PresenceReader, error)
```

Behavior:

- `cfg.ProxyURL == ""` → unchanged. The presence client inherits the transport
  built by `New` (default transport, which still honors `HTTPS_PROXY`; or the
  TLS-skip transport when `TLSInsecureSkipVerify` is set).
- `cfg.ProxyURL != ""` →
  1. `url.Parse(cfg.ProxyURL)`; return a wrapped error on parse failure or when
     the parsed URL has an empty scheme.
  2. Obtain a mutable `*http.Transport`: reuse the throwaway client's transport
     when it is already `*http.Transport` (preserving any TLS-skip settings),
     otherwise clone `http.DefaultTransport`.
  3. Set `tr.Proxy = http.ProxyURL(parsed)` and assign it back to the client.

Because the proxy is set at the transport level, it covers **both** the token
host (`login.microsoftonline.com`) and the Graph host (`graph.microsoft.com`)
with no per-host wiring.

### 3. Configurable User-Agent (`pkg/msgraph/presence.go`)

`Config.UserAgent` overrides the header; when empty the presence client falls
back to a desktop-browser default constant kept in the package (single source of
truth — the long string is not duplicated in service config):

```go
const defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
	"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
```

`NewPresenceClient` resolves `ua := cfg.UserAgent` (or `defaultUserAgent`) once
and stores it on the client. It is set on both presence requests:

- `accessToken` — the ROPC `password`-grant token POST.
- `fetch` — the `getPresencesByUserId` POST.

```go
req.Header.Set("User-Agent", p.userAgent)
```

### 4. Service wiring (`user-presence-service/sync/main.go`)

- New config fields:
  ```go
  GraphProxyURL  string `env:"GRAPH_PROXY_URL" envDefault:""`
  GraphUserAgent string `env:"GRAPH_USER_AGENT" envDefault:""`
  ```
- Populate `graphCfg.ProxyURL = cfg.GraphProxyURL` and
  `graphCfg.UserAgent = cfg.GraphUserAgent`.
- Handle the new `NewPresenceClient` error (fail-fast):
  ```go
  pres, err := msgraph.NewPresenceClient(graphCfg, msgraph.ROPCCredentials{...})
  if err != nil {
      return fmt.Errorf("build presence client: %w", err)
  }
  ```

## Testing (TDD — Red → Green → Refactor)

`pkg/msgraph/presence_test.go`:

1. **User-Agent present** — extend the existing ROPC test so both the token
   server and the graph server assert
   `r.Header.Get("User-Agent") == "chat-user-presence/1.0"`.
2. **Proxy traversal** — a recording forwarding-proxy `httptest` server set as
   `ProxyURL`; assert both the token request and the presence request pass
   through it and the presences still decode correctly.
3. **Invalid proxy URL** — `NewPresenceClient` with a malformed `ProxyURL`
   returns a non-nil error and a nil reader.
4. Update the two existing `NewPresenceClient` call sites for the new
   `(PresenceReader, error)` signature.

Coverage target: keep `pkg/msgraph` at/above the 80% floor; the presence
construction and error path are both exercised.

## Non-goals / out of scope

- No proxy/UA changes to the app-only meetings client or directory client.
- User-Agent is not env-configurable (static constant by decision).
- No `docs/client-api.md` change — `pkg/msgraph` is an internal outbound Graph
  integration, not a client-facing NATS/HTTP handler.

## Files touched

- `pkg/msgraph/msgraph.go` — `Config.ProxyURL` and `Config.UserAgent` fields + docs.
- `pkg/msgraph/presence.go` — proxy-aware `NewPresenceClient`, `defaultUserAgent`
  browser constant, per-client resolved UA on both requests.
- `pkg/msgraph/presence_test.go` — default + override UA assertions,
  proxy-traversal test, invalid-URL test, signature updates.
- `user-presence-service/sync/main.go` — `GRAPH_PROXY_URL` + `GRAPH_USER_AGENT`
  config, wiring, error handling.
