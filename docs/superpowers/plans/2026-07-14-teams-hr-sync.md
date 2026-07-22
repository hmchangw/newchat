# teams-hr-sync — phased plan

See `docs/superpowers/specs/2026-07-14-teams-hr-sync-design.md` for the design.

## Phase 1 — `pkg/model` types + `pkg/subject` builders

- `pkg/model/employee.go`: `Org` (nine section/dept/division fields, tag-identical to
  `SpotlightOrgIndex`, embedded inline in `Employee`), `Employee` (source of
  truth for `User`; carries `EngName`/`ChineseName`), `ChangeType`
  (`new_hire`/`update`), `EmployeeWithChange`, `UserWithChange`,
  `HRSyncEmployeeQuitBatch` (the only wrapper).
- `pkg/subject`: `OrgSyncUsersUpsert(central)`, `EmployeesQuit(siteID)`.
- `pkg/model/employee_test.go`: flat-org shape + bare-array round-trips.

## Phase 2 — Graph group surface + service

- `pkg/msgraph`: `GroupReader` — group profile getter (`GET /groups/{id}`) and
  group-members lister (`GET /groups/{id}/members`, identity `$select`,
  nextLink paging pinned to the configured origin, non-user objects
  skipped + counted).
- `teams-hr-sync/transform`: one-way `EmployeeUserConverter`
  (`DefaultConverter`, identity fields only).
- `teams-hr-sync/`: one-shot CronJob binary — env config (`SYNC_GROUPS`
  per-group siteId map + `SITE_OVERRIDES`, `CENTRAL_SITE_ID`, read Mongo, NATS),
  mapper (Graph member+group → Employee; group → section), publisher (bare
  zstd arrays for the two upserts + quit wrapper, `Nats-Encoding: zstd`, empty
  arrays skipped, reflection-derived read projection), run-summary log line.

## Phase 3 — query-first diff + quit detection

- Read-only `hr_employee` reader with an explicit Employee-field projection.
- Differ keyed by account: created/updated/omitted; store-present-but-absent →
  quit grouped per stored row's `siteId`.
- Unit matrix (created/updated/unchanged/quit/empty-store-first-run) +
  integration (seeded read + two full one-shot runs vs httptest Graph + real
  Mongo/NATS).

## Phase 4 — dev e2e mock + reference consumer

- `tools/graphmock`: fixture-driven Graph mock (token grant, group profile,
  `$top`/nextLink-paged members, runtime fixture swap) so the full loop runs
  without a tenant.
- `hr-sync-worker`: reference persister for the three subjects (in-repo
  contract; an external consumer can replace it). Identity-only `users`
  writes; delete-by-account quits; zstd-aware decode; idempotent.

## Phase 5 — unify org shape + search-sync consume

- `model.Org` becomes the nine-field `SpotlightOrgIndex` shape (inline embed),
  published as bare zstd arrays with `ChangeType`.
- search-sync-worker decodes the bare `[]EmployeeWithChange` and copies each
  element's inline org fields into `SpotlightOrgIndex` (1:1) — the earlier
  flat-vs-nested follow-up is resolved by making the producer shape match.

## Phase 6 — direct-write migration mode

- `pkg/hrstore`: extracted `hr-sync-worker`'s write `Store` interface +
  `MongoStore` impl (`UpsertEmployees`/`UpsertUserIdentities`/
  `QuitTeamsEmployees`) into a shared package; `hr-sync-worker` keeps a thin
  local `Store`/`MockStore` alias.
- `teams-hr-sync`: `HR_SYNC_MODE` (`stream`|`direct`) + `emitter` seam
  (`streamEmitter` wraps the existing `publisher`; `directEmitter` writes via
  `hrstore.Store`). `direct` diffs against an empty baseline (all-upsert,
  no-quit) and writes straight to `DIRECT_WRITE_*` Mongo, skipping JetStream +
  the worker — a one-shot idempotent backfill, no diff-state store touched.
