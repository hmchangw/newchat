# teams-hr-sync — design

**Target repo:** `hmchangw/newchat`
**Precedent:** teams-user-sync (reuse `pkg/msgraph`, the read/write Mongo split, and the K8s-CronJob one-shot model).

## Overview

A standalone producer that reads the Teams/Graph directory, diffs against the
current persisted HR state, and publishes the HR sync feed to JetStream on three
subjects. It does **not** persist employees/users — a downstream microservice
consumes the subjects and writes them. It **coexists** with the legacy HR syncer
(different systems feeding the same `hr_employee` store, distinguished by a
`source` field).

## Publishes (JSON, zstd via `Nats-Encoding: zstd`)

| Method | Subject builder | Payload |
|---|---|---|
| employees.upsert | `subject.OrgSyncEmployeesUpsert(central)` (exists) | `EmployeesUpsertBatch` |
| users.upsert | `subject.OrgSyncUsersUpsert(central)` (new) | `UsersUpsertBatch` |
| employees.quit | `subject.EmployeesQuit(siteID)` (new, per-site) | `HRSyncEmployeeQuitBatch` |

Only `employees.upsert` exists in-repo today (consumed by search-sync-worker's
spotlight-org collection, which decodes each element's flat 9-field org subset).

## Wire types (`pkg/model`, this phase)

- `Org` — the 9 org fields, json tags **identical** to search-sync-worker's
  org index row so one wire row feeds both the ES index and `hr_employee`. `bson`
  tags added (search-specific struct tags deliberately excluded from `pkg/model`).
- `Employee` — the source of truth a downstream out-of-repo service maps into
  `model.User`. Embeds `Org` inline (org fields stay flat on the wire) +
  `employeeId/account/engName/chineseName/siteId/source`. `engName`/`chineseName`
  mirror `model.User` so the derive is lossless; `source` is the coexistence tag.
- `EmployeeWithChange` / `UserWithChange` — embed `Employee` / `User` + a `Change`
  wire string (`created`/`updated`). Flat embedding preserved so the consumer's
  org-subset decode still works.
- `EmployeesUpsertBatch` / `UsersUpsertBatch` / `HRSyncEmployeeQuitBatch` — the
  three batch shapes, each carrying `Timestamp`.

## Change detection (later phase)

Query-first against `hr_employee` scoped to `{source:"teams"}` (not a self-held
snapshot): diff the Graph walk against ground truth → changed/new → `*WithChange`;
present-in-store-but-absent → quits (siteID from the stored row). A previously-lost
publish self-heals on the next run. Legacy-`source` rows are never seen as absent,
so no false quits.

## Change semantics — downstream contract (open)

`EmployeeWithChange.Change` / `UserWithChange.Change` (a plain wire string) and the
quit-batch shape are the downstream **persister's** contract, not defined in this
repo. The typed change enum + diff logic live in the producer's `transform` layer
(Phase 3), keeping `pkg/model` a leaf. Confirm the exact semantics before wiring.
