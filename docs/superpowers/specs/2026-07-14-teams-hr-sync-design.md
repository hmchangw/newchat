# teams-hr-sync — design

**Precedent:** teams-user-sync (reuse `pkg/msgraph`, the read-side Mongo client, and the K8s-CronJob one-shot model).

## Overview

A standalone service that walks configured Teams/Graph **groups**, diffs their
user members against the persisted HR state, and persists via one of two modes
(`HR_SYNC_MODE`):

- **`stream`** (default) — publishes the HR sync feed to JetStream on three
  subjects; a consumer persists the batches. A **reference consumer ships
  in-repo** (`hr-sync-worker`, see its README) and an external persister can
  replace it 1:1.
- **`direct`** — a one-shot migration/backfill: writes `hr_employee` + `users`
  straight to the target Mongo via the shared `pkg/hrstore`, skipping JetStream.

`hr_employee` is the unified HR directory — the legacy `hr` collection retires
and `teams-user-sync` reads `hr_employee`, so a quit deletes by account.

## Group = Org (section level)

`Org` is the nine-field section/department/division shape — json+bson tags
**identical** to search-sync-worker's `SpotlightOrgIndex`, embedded **inline**
in `Employee` so the org fields serialize flat alongside the identity fields.
One wire row feeds both the ES org index and the `hr_employee` store.

A configured Graph group maps to the **section** level: `Org.SectID` = group
id, `Org.SectName` = displayName, `Org.SectDescription` = description. The
dept/division and `*TCName` fields stay empty — the org-taxonomy source is an
open stub. There is no `Org.Type` / `ORG_TYPE` (the new shape has no type
field).

Site resolution is two-tier: a member's site defaults to its group's
`SYNC_GROUPS.siteId`, and an optional `SITE_OVERRIDES` JSON
(`[{"account":"…","siteId":"…"}]`) pins specific accounts to a site that
**wins** over the group default (an override for an account in no synced group
is simply unused).

## Publishes (bare zstd arrays)

| Method | Subject builder | Payload |
|---|---|---|
| employees.upsert | `subject.OrgSyncEmployeesUpsert(central)` | bare `[]EmployeeWithChange` |
| users.upsert | `subject.OrgSyncUsersUpsert(central)` | bare `[]UserWithChange` |
| employees.quit | `subject.EmployeesQuit(siteID)` (per-site) | `HRSyncEmployeeQuitBatch` |

All three are zstd-compressed with a `Nats-Encoding: zstd` header (the
framework decompresses on the consumer side). The two upserts are **bare
arrays** — no wrapper, no timestamp; quit keeps its wrapper. Empty arrays are
skipped. search-sync-worker consumes employees.upsert 1:1 (decode the bare
array → copy each element's inline org fields into `SpotlightOrgIndex`).

## Wire types (`pkg/model/employee.go`)

- `Org` — the nine flat section/dept/division fields (SpotlightOrgIndex shape).
- `Employee` — inline `Org` + `employeeId/account/engName/chineseName/siteId/source`.
- `ChangeType` — `new_hire` / `update`.
- `EmployeeWithChange` / `UserWithChange` — embed `Employee` / `User` + `ChangeType`.
- `HRSyncEmployeeQuitBatch` — the only remaining wrapper (quit).

## Graph mapping (`transform.DefaultMapper`)

Per member (`GET /groups/{id}/members`, `$select=id,userPrincipalName,
displayName,givenName,surname,employeeId`, non-user objects skipped):
`Account` = lowercased UPN local part (same rule as teams-user-sync),
`EngName` = `TrimSpace(givenName + " " + surname)`, `ChineseName` =
`displayName`, `EmployeeID` = `employeeId`, `SiteID` = the group's configured
site, `Org` = the group profile at section level
(`SectID`/`SectName`/`SectDescription`). An account appearing in multiple
groups keeps its first mapping (config order wins).

## Injectable seams (`teams-hr-sync/transform` + `emitter`)

Three seams, injected in `main.go` (see `teams-hr-sync/README.md` for a worked
replacement example): `transform.Mapper` (Graph group/member →
`Org`/`Employee`; owns name mapping + org placement, `DefaultMapper{}`),
`transform.EmployeeUserConverter` (one-way; `DefaultConverter` copies identity
fields only — `Account/SiteID/EngName/ChineseName/EmployeeID`; every other
`User` field stays zero, the downstream persister owns defaults/merging), and
`emitter` (one method consuming the run's diff — `streamEmitter` publishes to
JetStream via the existing `publisher`; `directEmitter` writes through the
shared `hrstore.Store` instead, see § Output modes). The change-label
constants (`ChangeCreated`/`ChangeUpdated`) live in `transform` too.

## Output modes (`HR_SYNC_MODE`)

Two modes select the emitter and the diff baseline in `main.go`:

- **`stream`** (default, unchanged behavior) — diffs the Graph walk against
  the persisted `hr_employee` rows (§ Change detection) and emits via
  `streamEmitter`.
- **`direct`** — a one-shot migration/backfill. Diffs against an **empty**
  baseline (`diffEmployees(current, nil)`), so every collected employee is a
  `new_hire` upsert and `Quits` is always empty — a full idempotent write, not
  a delta. Emits via `directEmitter`, which writes straight to the
  `DIRECT_WRITE_*` Mongo through the shared `pkg/hrstore.Store` (the same
  interface + Mongo impl `hr-sync-worker` writes from the consumer side —
  extracted there in this change). Never reads or writes the diff-state store;
  runs once and exits, no daemon loop.

## Change detection (query-first)

Diff the Graph walk against `hr_employee` filtered `{source:"teams"}` (ground
truth, not a self-held snapshot), keyed by account: absent-in-store → `created`;
any field differs (incl. `Org`) → `updated`; equal → omitted;
store-present-but-Graph-absent → quit, grouped per the stored row's `siteId`.
A previously-lost publish self-heals on the next run. Legacy-`source` rows are
filtered out at the store query (and defensively in the differ), so no false
quits.

## Service shape

Flat one-shot `teams-hr-sync/` (K8s CronJob owns schedule + overlap
prevention): env config via caarlos0/env (fail fast), read-only Mongo client,
`msgraph.GroupReader` (group profile getter + members lister with nextLink
paging), JetStream publish via an injected publish func, run-summary log line
with counts (groups, members, created, updated, quits, published), non-zero
exit on failure.

## Reference consumer (`hr-sync-worker`)

One durable sequential consumer per site's `HR_{siteID}` stream. Persists
employees.upsert → `hr_employee` (replace by `{account, source}`),
users.upsert → `users` (**identity fields only** — never roles/services/
password; live auth store), employees.quit → source-scoped `hr_employee`
delete. Idempotent; malformed = poison-Ack, transient = Nak-backoff.

## Dev e2e (`tools/graphmock`)

Fixture-driven Graph mock (token, group profile, paged members with real
nextLinks, runtime `PUT /__fixtures` swap) — see `tools/graphmock/README.md`
and the "Dev e2e with graphmock" section in `teams-hr-sync/README.md`.
