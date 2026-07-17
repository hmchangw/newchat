# teams-hr-sync — phased plan

See `docs/superpowers/specs/2026-07-14-teams-hr-sync-design.md` for the design.

## Phase 1 — `pkg/model` types + `pkg/subject` builders

- `pkg/model/employee.go`: `Org` (`{id, description, name, type}`, the
  group-shaped org node), `Employee` (source of truth for `User`; nested single
  `org`; carries `EngName`/`ChineseName` + `Source`), `EmployeeWithChange`,
  `EmployeesUpsertBatch`, `UserWithChange`, `UsersUpsertBatch`,
  `HRSyncEmployeeQuitBatch`. `Change` is a plain wire string.
- `pkg/subject`: `OrgSyncUsersUpsert(central)`, `EmployeesQuit(siteID)`.
- `employee_test.go`: nested-org shape + round-trips.

## Phase 2 — Graph group surface + service

- `pkg/msgraph`: `GroupReader` — group profile getter (`GET /groups/{id}`) and
  group-members lister (`GET /groups/{id}/members`, identity `$select`,
  nextLink paging pinned to the configured origin, non-user objects
  skipped + counted).
- `teams-hr-sync/transform`: one-way `EmployeeUserConverter`
  (`DefaultConverter`, identity fields only).
- `teams-hr-sync/`: one-shot CronJob binary — env config (`SYNC_GROUPS`
  per-group siteId map, `ORG_TYPE`, `CENTRAL_SITE_ID`, read Mongo, NATS),
  mapper (Graph member+group → Employee), publisher (3 batches, empty
  batches skipped, plain JSON), run-summary log line.

## Phase 3 — query-first diff + quit detection

- Read-only `hr_employee` reader filtered `{source:"teams"}` with an explicit
  Employee-field projection.
- Differ keyed by account: created/updated/omitted; store-present-but-absent →
  quit grouped per stored row's `siteId`; other-source rows never quit.
- Unit matrix (created/updated/unchanged/quit/empty-store-first-run/
  legacy-source-never-quit) + integration (seeded read + two full one-shot
  runs vs httptest Graph + real Mongo/NATS).

## Follow-up

- search-sync-worker's spotlight-org consumer decodes a **flat** org subset;
  the nested single-org employees.upsert shape doesn't feed it — reconcile the
  consumer (or its own feed) separately.
