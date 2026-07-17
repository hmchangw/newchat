# teams-hr-sync — design

**Precedent:** teams-user-sync (reuse `pkg/msgraph`, the read-side Mongo client, and the K8s-CronJob one-shot model).

## Overview

A standalone producer that walks configured Teams/Graph **groups**, diffs their
user members against the persisted HR state, and publishes the HR sync feed to
JetStream on three subjects. It does **not** persist employees/users — a
consumer persists the batches. A **reference consumer ships in-repo**
(`hr-sync-worker`, see its README): the subject contract is now defined here,
and an external persister can replace it 1:1. It **coexists**
with the legacy HR syncer (different systems feeding the same `hr_employee`
store, distinguished by a `source` field; this producer stamps `source:"teams"`
and never quits another source's rows).

## Group = Org

Each configured Graph group maps to one **site** (`SYNC_GROUPS` JSON:
`[{"groupId":"…","siteId":"…"}]`) and becomes the member's org node: `Org`
carries the group's `id`/`displayName`/`description` plus a configured `type`
(`ORG_TYPE`, default `"group"`). `Org` nests under `Employee` as a **single
node** (json key `org`) — not flattened fields.

Site resolution is two-tier: a member's site defaults to its group's
`SYNC_GROUPS.siteId`, and an optional `SITE_OVERRIDES` JSON
(`[{"account":"…","siteId":"…"}]`) pins specific accounts to a site that
**wins** over the group default (an override for an account in no synced group
is simply unused).

> Consequence (flagged follow-up): search-sync-worker's spotlight-org consumer
> decodes each employees.upsert element into a **flat** org-field subset; the
> nested single-org shape does not feed that decode. Reconciling the consumer is
> tracked separately — not in this service's scope.

## Publishes (plain JSON in v1 — the header contract permits uncompressed)

| Method | Subject builder | Payload |
|---|---|---|
| employees.upsert | `subject.OrgSyncEmployeesUpsert(central)` | `EmployeesUpsertBatch` |
| users.upsert | `subject.OrgSyncUsersUpsert(central)` | `UsersUpsertBatch` |
| employees.quit | `subject.EmployeesQuit(siteID)` (per-site) | `HRSyncEmployeeQuitBatch` |

Empty batches are skipped. `Timestamp` = publish time (UTC millis).

## Wire types (`pkg/model`)

- `Org` — `{id, description, name, type}`, the group-shaped org node.
- `Employee` — the source of truth a downstream service maps into `model.User`:
  `employeeId/account/engName/chineseName/siteId/source` + nested `org`.
- `EmployeeWithChange` / `UserWithChange` — embed `Employee` / `User` + a
  `Change` wire string (`created`/`updated`).
- `EmployeesUpsertBatch` / `UsersUpsertBatch` / `HRSyncEmployeeQuitBatch` — the
  three batch shapes, each carrying `Timestamp`.

## Graph mapping (`transform.DefaultMapper`)

Per member (`GET /groups/{id}/members`, `$select=id,userPrincipalName,
displayName,givenName,surname,employeeId`, non-user objects skipped):
`Account` = lowercased UPN local part (same rule as teams-user-sync),
`EngName` = `TrimSpace(givenName + " " + surname)`, `ChineseName` =
`displayName`, `EmployeeID` = `employeeId`, `SiteID` = the group's configured
site, `Source` = `"teams"`, `Org` = the group profile + `ORG_TYPE`. An account
appearing in multiple groups keeps its first mapping (config order wins).

## Injectable seams (`teams-hr-sync/transform`)

Two interfaces, injected in `main.go` (see `teams-hr-sync/README.md` for a
worked replacement example): `Mapper` (Graph group/member → `Org`/`Employee`;
owns name mapping + `Org.Type`, `DefaultMapper{OrgType}`) and
`EmployeeUserConverter` (one-way; `DefaultConverter` copies identity fields
only — `Account/SiteID/EngName/ChineseName/EmployeeID`; every other `User`
field stays zero, the downstream persister owns defaults/merging). The
change-label constants (`ChangeCreated`/`ChangeUpdated`) and `SourceTeams`
live here too.

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
