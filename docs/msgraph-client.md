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
| `TEAMS_EMAIL_DOMAIN` | Domain appended to an `account` to form an email (`account@domain`); defaults to `dev.local` for local/dev |
| `GRAPH_PROXY_URL` | Optional. Routes the meetings Graph client through this proxy (scheme+host, e.g. `http://proxy.corp:8080`), overriding `HTTPS_PROXY`/`HTTP_PROXY`. Empty falls back to the standard proxy env vars. |

When the Teams credentials are unset, the deep-link call RPCs still work (they
need only `TEAMS_EMAIL_DOMAIN`); the meetings RPC returns a not-configured
error until the credentials are set.

## Auth flow

1. `POST https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token` with
   `grant_type=client_credentials`, the client id/secret, and
   `scope=https://graph.microsoft.com/.default`.
2. The access token is cached and reused until shortly before expiry.

## Creating a meeting (idempotent)

The client calls Graph's **`createOrGet`** endpoint with a required
`externalId`:

- App-only: `POST {base}/users/{organizerObjectId}/onlineMeetings/createOrGet`
- Delegated fallback: `POST {base}/me/onlineMeetings/createOrGet`

The app-only path segment **must be the organizer's Azure AD object id (a GUID)**,
not their userPrincipalName/email — the `onlineMeetings` endpoints reject a UPN
with `400` *"The userId in request URL is not a valid GUID"*. `room-service`
resolves the requester's object id from the `teams_user` mapping (populated by
`teams-user-sync`) and passes it as `CreateOnlineMeetingRequest.OrganizerID`;
attendee UPNs still ride the request body, which does accept UPNs.

`createOrGet` is idempotent at the source of truth: for a given
`(organizer, externalId)` it returns the **existing** meeting if one exists,
otherwise creates one. `room-service` sets `externalId` to a stable per-room key
(`siteID:roomID`), so repeated or concurrent `teams.meeting` calls for the same
room return the same meeting. `externalId` is required — the client rejects an
empty value.

room-service constructs this client via `NewMeetingsClient(cfg)`, which honors
`Config.ProxyURL` (from `GRAPH_PROXY_URL`) and fails fast on a malformed proxy
value at startup.

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
