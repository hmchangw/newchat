# teams-hr-sync

One-shot HR-feed producer (K8s CronJob owns schedule + overlap prevention).
Walks the configured Teams/Graph groups, diffs their user members against the
persisted `hr_employee` rows (`source:"teams"` only), and publishes the delta
to JetStream: `employees.upsert` + `users.upsert` on the central site,
`employees.quit` per site. It never writes the store — a downstream consumer
persists the batches, so a lost publish self-heals on the next run.

## Config

| Env | Required | Default | Notes |
|---|---|---|---|
| `TEAMS_TENANT_ID` / `TEAMS_CLIENT_ID` / `TEAMS_CLIENT_SECRET` | ✔ | — | Graph app-only credentials |
| `SYNC_GROUPS` | ✔ | — | JSON `[{"groupId":"…","siteId":"…"}]`; unique groupIds. Each group's `siteId` is the DEFAULT site for its members |
| `SITE_OVERRIDES` | | `[]` | JSON `[{"account":"…","siteId":"…"}]`; per-account site that WINS over the group default (an override for an account in no group is unused) |
| `CENTRAL_SITE_ID` | ✔ | — | Scopes the two upsert subjects |
| `MONGO_READ_URI` | ✔ | — | + optional `MONGO_READ_USERNAME/PASSWORD/DB` (db `chat`) |
| `NATS_URL` | ✔ | — | + optional `NATS_CREDS_FILE` |
| `GRAPH_PAGE_SIZE` | | `500` | Graph `$top`, 1..999 |
| `GRAPH_BASE_URL` / `GRAPH_TOKEN_URL` | | public Graph | Test/on-prem overrides |
| `GRAPH_TLS_INSECURE_SKIP_VERIFY` | | `false` | Opt-in; skips Graph TLS verify for on-prem/self-signed |

## Injecting your own Mapper / Converter

All Graph→domain shaping lives behind two interfaces in
[`transform`](transform/transform.go) — the service only calls the interfaces:

- **`transform.Mapper`** — owns name mapping and org placement (a group maps
  to the section level). `OrgFromGroup` shapes the org node from the group
  profile; `EmployeeFromMember` derives the Employee (account from the UPN,
  names, site, `Source`). Returning an Employee with an empty `Account` marks
  the member unmappable — the service skips it.
- **`transform.EmployeeUserConverter`** — derives the `users.upsert` row from
  an Employee. `DefaultConverter` copies identity fields only.

Change labels the differ stamps are `model.ChangeTypeNewHire` /
`model.ChangeTypeUpdate`; the ownership tag is `transform.SourceTeams`.

Example — different English-name convention:

```go
type surnameFirstMapper struct{ transform.DefaultMapper }

func (m surnameFirstMapper) EmployeeFromMember(u *msgraph.GraphUser, org *model.Org, siteID string) model.Employee {
	e := m.DefaultMapper.EmployeeFromMember(u, org, siteID)
	e.EngName = strings.TrimSpace(u.Surname + " " + u.GivenName)
	return e
}
```

Wire it at the single injection point in `main.go`:

```go
mapper := surnameFirstMapper{transform.DefaultMapper{}}
stats, err := runSync(ctx, graph, mapper, store, pub, groups, siteOverrides, cfg.GraphPageSize)
```

The converter is injected the same way via `newPublisher(..., yourConverter)`.

## Dev e2e with graphmock

`tools/graphmock` mocks the whole Graph surface this service touches. Run it
with `FIXTURES_PATH=tools/graphmock/fixtures.sample.json`, then point the sync
at it:

```
GRAPH_BASE_URL=http://localhost:8080/v1.0
GRAPH_TOKEN_URL=http://localhost:8080/t/oauth2/v2.0/token
SYNC_GROUPS=[{"groupId":"g-eng","siteId":"site-a"},{"groupId":"g-sales","siteId":"site-b"}]
```

`PUT /__fixtures` between runs to simulate joins/leaves/renames; pair with
`hr-sync-worker` consuming the published batches for a full loop.
