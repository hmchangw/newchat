# Room-service Teams meeting: resolve object IDs app-only (drop ROPC)

**Date:** 2026-07-28
**Service:** room-service, pkg/msgraph
**Status:** Proposed design (supersedes the ROPC directory lookup from
`2026-07-24-room-service-teams-meeting-azure-objectid-design.md`)

## Problem

The Teams meeting RPC resolves organizer/attendee Azure AD object IDs through a
**ROPC** (`grant_type=password`) directory client (`pkg/msgraph/directory_ropc.go`,
`NewDirectoryROPCClient`), gated on `TEAMS_ROPC_USERNAME`/`TEAMS_ROPC_PASSWORD`.

That ROPC dependency is unnecessary. The lookup is a plain directory read:

```
GET /users?$filter=startsWith(userPrincipalName,'account@')&$select=id,userPrincipalName&$count=true
```

which is served by the **`User.Read.All` *application* permission** and works
with an app-only client-credentials token. The resolution logic
(`resolveAccountIDs`) is already token-agnostic — the app-only `graphClient` and
the ROPC `directoryClient` call the *same* helper, differing only in how they
mint the token (`client_credentials` vs `password`).

`user-presence-service` proves the split: it resolves object IDs with the
app-only Service Principal (`NewDirectoryClient`, no username/password) and uses
ROPC **only** for its presence call, because Graph's `getPresencesByUserId`
requires `Presence.Read.All`, which is delegated-only. Directory reads have no
such restriction.

The `2026-07-24` design justified ROPC as "matching the pattern used by
`user-presence-service`" — a mis-read: user-presence uses ROPC only for the
delegated-only presence endpoint, not for the directory lookup.

## Goal

Resolve meeting organizer/attendee object IDs with the **app-only Service
Principal room-service already builds for meetings**, and remove the ROPC
directory client, its config, and its service-account password. Meeting behavior
(object-ID organizer path, `identity.user.id` attendees, organizer-fatal /
attendee-best-effort) is unchanged.

## Decisions

| # | Decision | Choice |
|---|----------|--------|
| D1 | Directory token | **App-only client-credentials.** Reuse the meetings app's `TEAMS_CLIENT_ID`/`TEAMS_CLIENT_SECRET`; no resource-owner creds. |
| D2 | Client instances | **One shared app-only `*graphClient`** serves both the meetings (`Client`) and directory (`DirectoryReader`) surfaces — a single token cache. |
| D3 | Proxy parity | The new constructor honors `cfg.ProxyURL` (like `NewMeetingsClient`). The bare `NewDirectoryClient` ignores `ProxyURL`, so reusing the meetings client avoids a proxy regression. |
| D4 | Not-configured gate | Unchanged shape (`graphClient == nil \|\| teamsMeetingStore == nil \|\| directoryClient == nil`). `directoryClient` is now non-nil whenever `graphClient` is, since both come from the same constructor. |
| D5 | Azure permission | The meetings app must hold **`User.Read.All` as an application permission** (admin-consented), replacing the delegated grant the `2026-07-24` design chose. Ops/IaC concern, no code. |
| D6 | Scope | room-service + `pkg/msgraph` only. `user-presence-service` (presence ROPC is correct) and the deep-link call RPCs (email-based) are untouched. |

## Components

### 1. `pkg/msgraph`

- **Delete** `directory_ropc.go` (the `directoryClient` type + `NewDirectoryROPCClient`).
- **Delete** its tests in `msgraph_test.go` (`TestNewDirectoryROPCClient_ResolvesWithPasswordGrant`,
  `TestNewDirectoryROPCClient_TokenErrorDoesNotLeakPassword`).
- **Add** a constructor returning both surfaces from one proxy-honoring app-only client:

  ```go
  // NewMeetingsDirectoryClient returns the meetings (Client) and directory
  // (DirectoryReader) surfaces backed by a single app-only graphClient (one
  // token cache). Honors cfg.ProxyURL. Both return values are the same instance.
  func NewMeetingsDirectoryClient(cfg Config, opts ...Option) (Client, DirectoryReader, error)
  ```

- **Unchanged:** `resolveAccountIDs`, `graphClient.ResolveAccountIDs`,
  `CreateOnlineMeeting`, the object-ID meeting payload (`OrganizerID`,
  `AttendeeIDs`, `identity.user.id`), the `DirectoryReader` interface.

### 2. `room-service/main.go`

- Remove `TeamsROPCUsername`/`TeamsROPCPassword` config fields and their doc comment.
- Build both clients with `NewMeetingsDirectoryClient(graphCfg)` inside the same
  `TeamsTenantID/ClientID/ClientSecret` guard; assign `graphClient` +
  `directoryClient`. Drop the ROPC branch and its "unset → not-configured" warn.

### 3. `room-service/handler_teams.go` / `handler.go`

- Keep the `h.directoryClient == nil` gate (correct; now always satisfied
  alongside `graphClient`).
- Update comments: "ROPC directory (`User.Read.All`)" → "app-only
  `User.Read.All`". Handler logic unchanged.

### 4. Tests

`handler_teams_test.go` injects a `fakeDirectory`, so all handler behavior tests
(happy path, organizer-unresolved, attendee-dropped, not-configured, infra
error) are unaffected — only ROPC-referencing comments change. Add/adjust a
`pkg/msgraph` test asserting `NewMeetingsDirectoryClient` returns a working
`DirectoryReader` (resolves account→objectID against an httptest Graph using an
app-only token) and a usable `Client`. Coverage stays ≥80%.

## Docs & config

- `docs/msgraph-client.md` — rewrite "Resolving object IDs" from ROPC to app-only
  `User.Read.All`; drop the `TEAMS_ROPC_USERNAME`/`TEAMS_ROPC_PASSWORD` rows.
- `docs/client-api.md` (Start Teams Meeting) — replace the "ROPC `User.Read.All`
  service account (`TEAMS_ROPC_USERNAME`/`PASSWORD`)" sentence with app-only
  `User.Read.All`; keep the `internal` error row unchanged. Request/reply and
  event structs are unchanged, so `docs/client-api/request-reply.md` and
  `events.md` need no edits.
- `room-service/deploy/docker-compose.yml` — remove the two `TEAMS_ROPC_*`
  passthroughs.

## Error handling

No change. Not-configured, organizer-unresolved, `ResolveAccountIDs` infra
failure, and attendee-dropped all behave exactly as today and collapse to the
documented `internal` wire case. No new client-facing `Reason`.

## Out of scope

- `user-presence-service` — its presence ROPC targets a delegated-only endpoint
  and is correct.
- Deep-link call RPCs (`teamsRoomCall`, `teamsUserCall`) — email-based by design.
- Caching resolved object IDs — resolution only runs on the meeting slow-path
  (no existing `teams_meetings` record); a cache is unnecessary (YAGNI).
