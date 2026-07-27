# graphmock

Fixture-driven mock of the Microsoft Graph surface the Teams sync stages use
(HR sync, user sync, chat sync, member sync). Dev/e2e only — accepts any
credentials.

## Endpoints

| Endpoint | Serves |
|---|---|
| `POST /{tenant}/oauth2/v2.0/token` | Static fake app token |
| `GET /v1.0/groups/{id}` | Group profile (404 on unknown id) |
| `GET /v1.0/groups/{id}/members` | Group members, paged (`$top` + `@odata.nextLink`) |
| `GET /v1.0/users` | Directory users, paged (`$top` + `@odata.nextLink`) |
| `GET /v1.0/users/{id}/chats` | Chats the user is a member of (members inline), paged |
| `GET /v1.0/chats/{id}/members` | Chat members, paged (404 on unknown id) |
| `PUT /__fixtures` / `GET /__fixtures` | Replace / read the in-memory dataset at runtime |
| `GET /healthz` | 200 |

All list endpoints honor `$top` and emit a self-pointing `@odata.nextLink` so
real Graph clients exercise their pager.

Point the sync at it: `GRAPH_BASE_URL=http://host:8080/v1.0`,
`GRAPH_TOKEN_URL=http://host:8080/t/oauth2/v2.0/token`.

## Config

`PORT` (default `8080`), `FIXTURES_PATH` (optional startup JSON).

## Fixture schema

See `fixtures.sample.json`. Three top-level arrays:

- `groups[]` — with raw member objects; user members carry
  `"@odata.type": "#microsoft.graph.user"` plus the identity `$select` fields,
  non-user members (nested groups, devices) exercise the skip path.
- `users[]` — directory users (`id`, `userPrincipalName`, `displayName`, …).
- `chats[]` — `id`, `chatType`, `topic`, `createdDateTime`,
  `lastUpdatedDateTime`, and `members[]` (`userId` + optional
  `visibleHistoryStartDateTime`). `GET /users/{id}/chats` returns chats whose
  `members` include that user; `GET /chats/{id}/members` returns a chat's members.

```json
{"groups": [{"id": "g1", "displayName": "…", "description": "…",
  "members": [{"@odata.type": "#microsoft.graph.user", "id": "u1",
    "userPrincipalName": "…", "displayName": "…", "givenName": "…",
    "surname": "…", "employeeId": "…"}]}],
 "users": [{"id": "u1", "userPrincipalName": "…", "displayName": "…"}],
 "chats": [{"id": "19:c1", "chatType": "group", "topic": "…",
   "createdDateTime": "2026-01-02T03:04:05Z", "lastUpdatedDateTime": "2026-06-01T00:00:00Z",
   "members": [{"userId": "u1", "visibleHistoryStartDateTime": "2026-01-02T03:04:05Z"}]}]}
```

`PUT /__fixtures` with the same schema swaps the dataset mid-run (e.g. to
simulate a member leaving between sync runs).
