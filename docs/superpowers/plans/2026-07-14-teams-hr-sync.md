# teams-hr-sync — phased plan

See `docs/superpowers/specs/2026-07-14-teams-hr-sync-design.md` for the design.

## Phase 1 — `pkg/model` types + `pkg/subject` builders (this PR)

Additive, no behavior change. Foundation the producer publishes.

- `pkg/model/employee.go`: `Org`, `Employee` (source of truth for `User`;
  carries `EngName`/`ChineseName` + `Source`), `EmployeeWithChange`,
  `EmployeesUpsertBatch`, `UserWithChange`, `UsersUpsertBatch`,
  `HRSyncEmployeeQuitBatch`. `Change` is a plain wire string — the typed enum +
  diff logic land in the producer's `transform` layer (Phase 3), not `pkg/model`.
- `pkg/subject`: `OrgSyncUsersUpsert(central)`, `EmployeesQuit(siteID)`.
- `employee_test.go` round-trips + a consumer-decode-compat test (marshal
  `EmployeesUpsertBatch`, decode into the org-only subset, org fields survive).
- **Deferred:** reusing `Employee` in search-sync-worker (a fit question surfaced
  separately) — not in this PR. portal-service keeps its own employee type
  (different purpose), no reuse there.

## Phase 2 — service skeleton + Graph + full-publish

New `teams-hr-sync` service (one-shot CronJob, `signal.NotifyContext`), config,
a group-members lister added to `pkg/msgraph`, Graph→Employee mapping (Employee is
the source of truth; a downstream out-of-repo service derives `User` from it) with
a **stubbed** `resolveOrg` + `source="teams"`. Publish full batches every run (no
diff yet). Unit + integration.

## Phase 3 — query-first diff + quit detection

Read `hr_employee` `{source:"teams"}` via the typed Mongo reader, diff → emit only
changed + quits (siteID from the stored row), empty-store first-run handling. Unit
(diff matrix incl. `source` scoping) + integration (seed → delta+quit). **Confirm
the downstream `Change`/quit contract before wiring.**

## Follow-up

Fill `resolveOrg` once the Graph org-taxonomy source is confirmed
(extensionAttributes vs mapping table vs nested groups).
