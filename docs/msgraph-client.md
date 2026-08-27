# Microsoft Graph client (`pkg/msgraph`)

A minimal, app-only Microsoft Graph client used by `room-service` to create
Teams **online meetings** for the `teams.meeting` RPC. It exposes only the
surface room-service needs and sits behind a `Client` interface so callers can
be unit-tested against a mock without reaching Azure.

## What it does

- One operation: `CreateOnlineMeeting` — creates (or returns the existing)
  Teams online meeting and yields its `joinUrl` + meeting id.
- Authenticates with the **client-credentials (app-only) OAuth2 flow** and
  caches the token until it expires.

## Configuration

The client takes a `Config{TenantID, ClientID, ClientSecret}`. `room-service`
populates it from these environment variables (plus the email domain it uses to
derive organizer/attendee addresses):

| Env var | Purpose |
|---|---|
| `TEAMS_TENANT_ID` | Azure AD tenant id (path segment of the token URL) |
| `TEAMS_CLIENT_ID` | App registration (client) id |
| `TEAMS_CLIENT_SECRET` | App registration client secret |
| `TEAMS_EMAIL_DOMAIN` | Domain appended to an `account` to form an email (`account@domain`); defaults to `dev.local` for local/dev. Used only by the deep-link call RPCs — meetings resolve real object IDs (below). |
| `GRAPH_PROXY_URL` | Optional. Routes the meetings Graph client through this proxy (scheme+host, e.g. `http://proxy.corp:8080`), overriding `HTTPS_PROXY`/`HTTP_PROXY`. Empty falls back to the standard proxy env vars. |
| `GRAPH_PROXY_USERNAME` | Optional. Basic-auth user for `GRAPH_PROXY_URL`. Overrides any userinfo embedded in the URL. |
| `GRAPH_PROXY_PASSWORD` | Optional. Basic-auth password for `GRAPH_PROXY_URL`. **Secret** — store it in a k8s Secret, never a ConfigMap. |

When the Teams credentials are unset, the deep-link call RPCs still work (they
need only `TEAMS_EMAIL_DOMAIN`); the meetings RPC returns a not-configured
error until the credentials are set.

## Auth flow

1. `POST https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token` with
   `grant_type=client_credentials`, the client id/secret, and
   `scope=https://graph.microsoft.com/.default`.
2. The access token is cached and reused until shortly before expiry.

## Creating a meeting (idempotent)

The request carries Azure AD **object IDs**, not emails:
`CreateOnlineMeetingRequest{ExternalID, Subject, OrganizerID, AttendeeIDs}`. The
organizer object ID is the path segment; attendees are added as
`participants.attendees[].identity.user.id`.

The client calls Graph's **`createOrGet`** endpoint with a required
`externalId`:

- App-only: `POST {base}/users/{organizerId}/onlineMeetings/createOrGet`
- Delegated fallback: `POST {base}/me/onlineMeetings/createOrGet`

`createOrGet` is idempotent at the source of truth: for a given
`(organizer, externalId)` it returns the **existing** meeting if one exists,
otherwise creates one. `room-service` sets `externalId` to a stable per-room key
(`siteID:roomID`), so repeated or concurrent `teams.meeting` calls for the same
room return the same meeting. `externalId` is required — the client rejects an
empty value.

room-service constructs this client via `NewMeetingsClient(cfg)`, which honors
`Config.ProxyURL` (from `GRAPH_PROXY_URL`) and fails fast on a malformed proxy
value at startup.

### Authenticating proxies

Set `GRAPH_PROXY_USERNAME`/`GRAPH_PROXY_PASSWORD` alongside `GRAPH_PROXY_URL`
to authenticate to the proxy. How they travel depends on the scheme:

- **`http` / `https`** — `Proxy-Authorization: Basic` on every hop, the token
  request and the Graph request alike, through the CONNECT tunnel.
- **`socks5` / `socks5h`** — the RFC 1929 username/password sub-negotiation
  during the SOCKS handshake. No HTTP proxy-auth header is sent.

Credentials embedded in the URL (`http://user:pass@proxy:8080`) still work and
stay supported, but the separate vars are preferred:

- A password containing `@ : / ? # %` needs no percent-encoding.
- Only the password is a secret, so `GRAPH_PROXY_URL` can stay in a ConfigMap.
  A `GRAPH_PROXY_URL` that *does* embed userinfo is itself a secret and belongs
  in a Secret — that is the second reason to prefer the separate vars.
- Rotating the password touches one value, not a connection string.

The explicit vars win over embedded userinfo. These fail fast at construction
rather than silently egressing unauthenticated or breaking on the first request:

- credentials with no `GRAPH_PROXY_URL`;
- a password with no username — from the settings *or* from URL userinfo
  (`http://:secret@proxy:8080`), since Basic would send `:secret` and draw a 407;
- a malformed URL, or one missing a scheme or hostname (`http://:8080` has a
  port but no host to dial);
- a scheme `net/http` cannot proxy through — only `http`, `https`, `socks5` and
  `socks5h` work;
- a `:` in the username of an `http`/`https` proxy. Basic sends `user:password`,
  so the proxy would split at the wrong colon and answer 407 on the first
  request; RFC 7617 forbids it. SOCKS5 fields are length-prefixed, so a colon is
  accepted there.

**No rejection quotes the value.** Every one of those errors is static (bar the
scheme name), because there is no safe way to echo a bad proxy URL: a failed
parse quotes the whole input, its underlying error quotes fragments (`invalid
URL escape "%zz"`), and `Redacted()` masks userinfo only when `url.Parse`
populated it — a scheme-less `user:pw@host` lands entirely in `Opaque` with
`User` nil, so `Redacted()` would return the password verbatim.

For `http`/`https` proxies, only **Basic** auth is supported — Go's transport
has no NTLM/Kerberos/Digest proxy auth.

Every error-returning constructor applies it: `NewMeetingsClient`,
`NewMeetingsDirectoryClient`, `NewUserListerClient`, `NewChatsClient`,
`NewChatMembersClient`, `NewPresenceClient`, `NewDirectoryClient` and
`NewGroupReaderClient`. Only the bare `New` ignores proxy config and relies on
`HTTPS_PROXY`/`HTTP_PROXY` — every service-facing constructor returns an error
precisely so a bad proxy value stops the pod at startup instead of failing the
first Graph call.

The two settings are alternatives, not layers. When `GRAPH_PROXY_URL` is set it
**overrides** `HTTPS_PROXY`/`HTTP_PROXY` for Graph traffic; when it is empty the
client keeps the default transport, so those ambient vars stay the fallback
(they also remain the fallback for a service's non-Graph egress either way).
Set `GRAPH_PROXY_URL` on every Graph service and no Graph egress depends on the
ambient vars.

## Resolving object IDs (app-only directory reader)

Because the organizer path and attendee identities are object IDs — not the
guessed `account@TEAMS_EMAIL_DOMAIN` email — `room-service` first resolves them
through the **app-only** directory reader that holds the **`User.Read.All`**
*application* permission — the same Service Principal (`TEAMS_CLIENT_ID`/
`TEAMS_CLIENT_SECRET`) that creates the meeting, so no resource-owner
credentials are involved. Construct both surfaces together via
`NewMeetingsDirectoryClient(cfg)`, which returns the meetings `Client` and the
`DirectoryReader` backed by one client (one token cache). `ResolveAccountIDs(ctx,
accounts) → map[account]objectID` matches `startsWith(userPrincipalName,'account@')`
so any domain resolves. The organizer must resolve or the `teams.meeting` request
fails; an attendee that does not resolve is dropped from the invite.

## Listing users (paginated)

`UserLister.ListUsers(ctx, pageSize, fn)` walks `GET /users` with
`$select=id,userPrincipalName&$top={pageSize}`, following `@odata.nextLink`
and invoking `fn` once per page. Used by `teams-user-sync` to enumerate the
tenant. Requires the **`User.Read.All`** application permission. Construct
via `NewUserListerClient(cfg)`.

## Production requirement (the live gate)

App-only `onlineMeetings` access is **not** granted by the application
permission alone. Before live use the tenant must have:

1. The **`OnlineMeetings.ReadWrite.All`** application permission, admin-consented
   for the app registration; and
2. A **Teams application access policy** (`New-CsApplicationAccessPolicy` +
   `Grant-CsApplicationAccessPolicy`) that authorizes the app to create meetings
   **on behalf of the organizer user**.

Without the access policy, `createOrGet` returns `403`. This is the one piece
that cannot be exercised by the unit tests and must be validated against the
real tenant.

## Testing without credentials

The client is built to be tested with **no Azure credentials**. The constructor
takes options that point it at local stub servers:

- `WithTokenURL(url)` — override the OAuth token endpoint.
- `WithBaseURL(url)` — override the Graph API base URL.
- `WithHTTPClient(c)` — inject a custom `*http.Client`.

`pkg/msgraph/msgraph_test.go` uses `httptest.NewServer` to stub **both** the
token endpoint and the Graph API, covering: success, idempotent-same-externalId,
required-externalId, token error, Graph error, and missing-joinURL. Because the
client is behind the `Client` interface, the `room-service` meetings handler is
also unit-tested against a generated mock (including a concurrent test that
asserts exactly one meeting + one system message under parallel calls).

Run them with:

```bash
go test ./pkg/msgraph/... ./room-service/...
```

No secrets, no network to Azure — only the live end-to-end smoke (above) needs
the real tenant.
