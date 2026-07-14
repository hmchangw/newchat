# teams-hr-sync — phased plan

See `docs/superpowers/specs/2026-07-14-teams-hr-sync-design.md` for the design.

## Phase 1 — `pkg/model` types + `pkg/subject` builders (this PR)

Additive, no behavior change. Foundation the producer publishes.

- `pkg/model/hremployee.go`: `OrgTaxonomy`, `Employee` (+ `Source`),
  `EmployeeChange` + consts, `EmployeeWithChange`, `EmployeesUpsertBatch`,
  `UserWithChange`, `UsersUpsertBatch`, `HRSyncEmployeeQuitBatch`.
- `pkg/subject`: `OrgSyncUsersUpsert(central)`, `EmployeesQuit(siteID)`.
- `model_test.go` round-trips + a consumer-decode-compat test (marshal
  `EmployeesUpsertBatch`, decode into the org-only subset, org fields survive).
- **Deferred:** reusing `Employee` in portal-service / search-sync-worker (a fit
  question surfaced separately) — not in this PR.

## Phase 2 — service skeleton + Graph + full-publish

New `teams-hr-sync` service (one-shot CronJob, `signal.NotifyContext`), config,
a group-members lister added to `pkg/msgraph`, Graph→Employee/User mapping with a
**stubbed** `resolveOrgTaxonomy` + `source="teams"`. Publish full batches every
run (no diff yet). Unit + integration.

## Phase 3 — query-first diff + quit detection

Read `hr_employee` `{source:"teams"}` via the typed Mongo reader, diff → emit only
changed + quits (siteID from the stored row), empty-store first-run handling. Unit
(diff matrix incl. `source` scoping) + integration (seed → delta+quit). **Confirm
the downstream `Change`/quit contract before wiring.**

## Follow-up

Fill `resolveOrgTaxonomy` once the Graph org-taxonomy source is confirmed
(extensionAttributes vs mapping table vs nested groups).
