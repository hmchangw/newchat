# Room-service Teams meeting: use Azure AD object IDs (ROPC directory lookup)

**Date:** 2026-07-24
**Service:** room-service, pkg/msgraph
**Status:** Approved design

## Problem

`room-service`'s `teamsMeeting` RPC (`handler_teams.go`) creates a Microsoft
Teams `onlineMeeting` via Graph's `createOrGet` endpoint. It currently derives
every identity — organizer and attendees — as `account@TEAMS_EMAIL_DOMAIN`, a
*guessed* email:

- Organizer goes into the path: `POST /users/{account@domain}/onlineMeetings/createOrGet`.
- Attendees go into the body as `{"upn": "account@domain"}`.

When a user's real `userPrincipalName` differs from `account@TEAMS_EMAIL_DOMAIN`
(different domain, or a local-part that isn't the chat account), Graph rejects
the request — the organizer path 404s and the whole create fails.

## Goal

Resolve real **Azure AD object IDs** for the room members and use those in the
meeting request instead of `account@domain`:

- Organizer object ID in the `createOrGet` path.
- Attendee object IDs in the body via `identity.user.id`.

Object IDs are resolved through a **ROPC (`grant_type=password`) service
account** that holds the `User.Read.All` Graph permission, matching the pattern
already used by `user-presence-service`.

## Decisions

| # | Decision | Choice |
|---|----------|--------|
| D1 | ROPC app registration | **Reuse `TEAMS_CLIENT_ID`/`TEAMS_CLIENT_SECRET`** as the confidential client; add `TEAMS_ROPC_USERNAME`/`TEAMS_ROPC_PASSWORD` for the service-account resource owner. The existing meetings Azure app is also granted `User.Read.All` (delegated). |
| D2 | Scope of object-ID substitution | **Organizer + attendees.** One batched directory lookup resolves every individual member. |
| D3 | Organizer resolution failure | **Fail with a clear error** (`errcode.Internal`). No meeting is created under a guessed organizer. |
| D4 | Attendee resolution failure | **Best-effort.** An unresolved attendee is dropped from the invite and logged; the meeting is still created as long as the organizer resolves. Prevents one directory-missing member from failing Graph's whole create. |
| D5 | Config var names | `TEAMS_ROPC_USERNAME` / `TEAMS_ROPC_PASSWORD` (grouped with the other `TEAMS_*` config). |
| D6 | msgraph request field names | Rename `OrganizerEmail`→`OrganizerID`, `AttendeeEmails`→`AttendeeIDs`; values are Azure object IDs. |
| D7 | Meetings requires ROPC | When ROPC creds are absent the meetings RPC returns `errTeamsNotConfigured` (object-ID resolution is now mandatory for meetings). Deep-link call RPCs are unaffected. |

## Components

### 1. `pkg/msgraph` — ROPC-backed directory reader

**Shared resolve helper.** Extract the token-agnostic account→objectID lookup
(currently inside `graphClient.ResolveAccountIDs` / `resolveChunk`) into a
package-level helper:

```go
func resolveAccountIDs(ctx context.Context, hc *http.Client, baseURL, userAgent, token string, accounts []string) (map[string]string, error)
```

It keeps the existing semantics verbatim: chunked `$filter` with
`startsWith(userPrincipalName,'account@')`, lower/upper cased variants,
`ConsistencyLevel: eventual`, `$count=true`, first-match-wins, result keyed by
lowercased UPN local-part. `graphClient.ResolveAccountIDs` becomes a thin
wrapper that acquires its app-only token and delegates — behavior unchanged, so
existing app-only tests stay green.

**New ROPC directory client.** Mirror `presenceClient` (its own token cache,
`grant_type=password`, reuses `Config.ClientID`/`ClientSecret` + username/
password, honors `ProxyURL`/`UserAgent`):

```go
func NewDirectoryROPCClient(cfg Config, creds ROPCCredentials, opts ...Option) (DirectoryReader, error)
```

It satisfies the existing `DirectoryReader` interface and delegates the actual
resolution to `resolveAccountIDs` with a ROPC token. Reuses the existing
`ROPCCredentials` type from `presence.go`.

**Meeting payload → object IDs.** Change `CreateOnlineMeetingRequest`:

```go
type CreateOnlineMeetingRequest struct {
    ExternalID  string
    Subject     string
    OrganizerID string   // was OrganizerEmail; Azure object ID, path segment
    AttendeeIDs []string // was AttendeeEmails; Azure object IDs
}
```

`CreateOnlineMeeting` builds the organizer path from `OrganizerID` (Graph
`/users/{id}` accepts an object ID) and the attendee body from `identity.user.id`
instead of `upn`:

```go
type meetingAttendee struct {
    Identity meetingIdentitySet `json:"identity"`
}
type meetingIdentitySet struct {
    User meetingIdentity `json:"user"`
}
type meetingIdentity struct {
    ID string `json:"id"`
}
```

Blast radius of the rename + payload change is contained to `pkg/msgraph`
(implementation + tests) and `room-service` (the one caller).

### 2. `room-service` config + wiring

`main.go` config additions:

```go
TeamsROPCUsername string `env:"TEAMS_ROPC_USERNAME" envDefault:""`
TeamsROPCPassword string `env:"TEAMS_ROPC_PASSWORD" envDefault:""`
```

In `main.go`, the directory client is constructed only when the meetings Graph
client is built **and** both ROPC creds are present (reusing the same
`msgraph.Config` — tenant, client id/secret, proxy, user-agent, TLS). It is
injected as a new handler field `directoryClient msgraph.DirectoryReader`
(field injection, mirroring `graphClient`/`teamsMeetingStore`, so no
`NewHandler` signature churn).

### 3. `room-service/handler_teams.go` — `teamsMeeting`

- Not-configured gate becomes
  `h.graphClient == nil || h.teamsMeetingStore == nil || h.directoryClient == nil`
  → `errTeamsNotConfigured`.
- After the membership check and member-limit gate, build the set of accounts to
  resolve: the organizer (`requesterAccount`) plus every individual member
  account. One batched `h.directoryClient.ResolveAccountIDs(ctx, accounts)` call.
- Organizer object ID **must** be present in the result → otherwise return the
  new sentinel `errTeamsOrganizerUnresolved` (`errcode.Internal`, collapses to
  the already-documented `internal` case). Infra failures from
  `ResolveAccountIDs` return a raw wrapped `fmt.Errorf` (also `internal`).
- Attendee object IDs: for each individual member (organizer included is
  harmless — Graph dedups the organizer), append its resolved object ID; skip +
  count members that didn't resolve, and emit a single `slog.Warn` with the
  dropped count (never the account names/ids as sensitive values — count only).
- Call `CreateOnlineMeeting` with `OrganizerID` + `AttendeeIDs`.
- Remove `membersToAttendeeEmails` (meetings-only, now unused). Keep
  `teamsEmail` and `membersToCallEmails` — the deep-link call RPCs
  (`teamsRoomCall`/`teamsUserCall`) still legitimately need emails and are out
  of scope.

New sentinel in `helper.go`:

```go
errTeamsOrganizerUnresolved = errcode.Internal("could not resolve meeting organizer identity")
```

## Error handling

| Condition | Result |
|-----------|--------|
| ROPC / directory client not configured | `errTeamsNotConfigured` (`internal`) — same wire category as today. |
| Organizer account not in directory | `errTeamsOrganizerUnresolved` (`internal`). |
| `ResolveAccountIDs` infra failure (ROPC token, Graph 5xx) | raw wrapped error → `internal`. |
| Attendee(s) not in directory | dropped + `slog.Warn(count)`; meeting still created. |

No new client-facing `Reason` is introduced — all failure modes collapse to the
existing documented `internal` case, so the frontend contract is unchanged.

## Testing (TDD)

**`pkg/msgraph`:**
- ROPC directory client: `grant_type=password` / `username` asserted on the
  token request; resolves account→objectID against an `httptest` Graph; password
  never appears in returned errors/logs; proxy/user-agent honored.
- App-only `ResolveAccountIDs` regression tests continue to pass unchanged
  (delegation refactor).
- `CreateOnlineMeeting`: attendee body serializes as `identity.user.id` (not
  `upn`); organizer object ID in the path; renamed fields.

**`room-service/handler_teams_test.go`** (mock `DirectoryReader`):
- Happy path: organizer + attendees resolved; `CreateOnlineMeeting` receives the
  expected `OrganizerID` + `AttendeeIDs`.
- Organizer unresolved → `errTeamsOrganizerUnresolved`.
- One attendee unresolved → dropped; meeting created with the rest.
- `directoryClient == nil` → `errTeamsNotConfigured`.
- Directory infra error → wrapped internal error, no meeting/publish.

Coverage stays ≥80% (target 90% for the handler + msgraph paths).

## Docs

- `docs/client-api.md` → Start Teams Meeting: replace the "attendee emails
  derived as `account@TEAMS_EMAIL_DOMAIN`" sentence with object-ID resolution
  via the ROPC `User.Read.All` service account; keep the `internal` error row
  (organizer-unresolved and not-configured both collapse there). Request/reply
  and event structs are unchanged, so `docs/client-api/request-reply.md` and
  `events.md` need no edits.
- `docs/msgraph-client.md` → document the ROPC directory reader, the reused
  `TEAMS_CLIENT_ID/SECRET` + `TEAMS_ROPC_USERNAME/PASSWORD`, and the object-ID
  meeting payload.
- `room-service/deploy/docker-compose.yml` → add `TEAMS_ROPC_USERNAME` /
  `TEAMS_ROPC_PASSWORD` passthrough.

## Out of scope

- Deep-link call RPCs (`teamsRoomCall`, `teamsUserCall`) — they build
  `teams.microsoft.com/l/call` deep links that require emails.
- Caching resolved object IDs — resolution only happens on the meeting
  slow-path (no existing `teams_meetings` record), so it is infrequent; a cache
  is unnecessary (YAGNI).
