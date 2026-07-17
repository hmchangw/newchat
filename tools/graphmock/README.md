# graphmock

Fixture-driven mock of the Microsoft Graph surface the HR sync uses. Dev/e2e
only — accepts any credentials.

## Endpoints

| Endpoint | Serves |
|---|---|
| `POST /{tenant}/oauth2/v2.0/token` | Static fake app token |
| `GET /v1.0/groups/{id}` | Group profile (404 on unknown id) |
| `GET /v1.0/groups/{id}/members` | Members, honoring `$top` + emitting self-pointing `@odata.nextLink` pages |
| `PUT /__fixtures` / `GET /__fixtures` | Replace / read the in-memory dataset at runtime |
| `GET /healthz` | 200 |

Point the sync at it: `GRAPH_BASE_URL=http://host:8080/v1.0`,
`GRAPH_TOKEN_URL=http://host:8080/t/oauth2/v2.0/token`.

## Config

`PORT` (default `8080`), `FIXTURES_PATH` (optional startup JSON).

## Fixture schema

See `fixtures.sample.json` — groups with raw member objects; user members
carry `"@odata.type": "#microsoft.graph.user"` plus the identity `$select`
fields, non-user members (nested groups, devices) exercise the skip path:

```json
{"groups": [{"id": "g1", "displayName": "…", "description": "…",
  "members": [{"@odata.type": "#microsoft.graph.user", "id": "u1",
    "userPrincipalName": "…", "displayName": "…", "givenName": "…",
    "surname": "…", "employeeId": "…"}]}]}
```

`PUT /__fixtures` with the same schema swaps the dataset mid-run (e.g. to
simulate a member leaving between sync runs).
