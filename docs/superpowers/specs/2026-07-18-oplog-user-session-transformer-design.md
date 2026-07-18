# oplog-user-session-transformer — users, sessions, hr_employee CDC lanes (Design)

> **Status:** DESIGN — fourth member of the `data-migration` CDC suite (`oplog-connector`,
> `oplog-transformer`, `oplog-collections-transformer`, `oplog-direct-transfer`).
> Branch: `claude/data-migration-transformer-y92grr`.
> Prerequisite context: the users lane was removed from `oplog-collections-transformer`
> (commit `ee124f6`) — this service is its replacement, with expanded scope.

A new service that consumes the collections connector's CDC feed for three source collections —
**`users`**, the **legacy sessions collection**, and **`company_hr_acct_org`** — and applies them to
the new stack: user upserts + status changes routed through a **new internal user-service RPC**,
resume-token lists upserted into the **`sessions`** collection, and HR acct/org records mapped into
**`hr_employee`**.

---

## 0. Where this sits

```text
 (per site) source Mongo ──change streams──▶ oplog-connector-collections ──▶ MIGRATION_OPLOG_{site}
            users · <sessions> · company_hr_acct_org       │   chat.migration.oplog.{site}.{coll}.{op}
            (federation $match now list-driven)            │
                                                           ▼
                                       oplog-user-session-transformer
                                          │            │           │
                users insert/replace ─────┤            │           └─ company_hr_acct_org → map →
                  upsert (overwrite       │            │              upsert hr_employee by account
                  mapped fields) ─────────┼──▶ target per-site Mongo
                                          │    (users · sessions · hr_employee)
                users statusText/IsShow ──┘
                  change → RPC chat.migration.internal.{site}.user.status
                            └─▶ user-service: $set statusText/statusIsShow/updatedAt
                                 + existing direct INBOX fan-out (user_status_updated → all sites)
```

**Scope boundary — live CDC tail only.** As with the sibling transformers, a separate owner
bulk-migrates state ≤ checkpoint; this service applies only tailed changes.

**Deployment is per-site.** One replica per site (sequential durable consumer — do **not** scale
horizontally), alongside the existing collections/messages/direct-transfer deployments.

---

## 1. Scope

### 1.1 In scope
- **`users`** — insert/replace upsert; `statusText`/`statusIsShow` updates via user-service RPC.
- **legacy sessions collection** — resume-token lists upserted into the target `sessions`
  collection (upsert-only; schema section to be added to `SOURCE_DATA.md` — see §9 Open confirmations).
- **`company_hr_acct_org`** — mapped into target `hr_employee` rows.
- **Connector change** — generalize the federation `$match` from one hard-wired collection to a
  configurable list (§6).
- **user-service change** — new internal migration status RPC (§5).

### 1.2 Out of scope
- User deactivation/deletion (source sets `active:false`; no destination apply-path — unchanged
  deferral from the removed lane).
- Session revocation mirroring (upsert-only, §3.2): a token revoked in legacy stays valid in the
  new stack until FIFO-cap eviction or admin revoke. **Documented gap, accepted for a short
  cutover window.**
- Per-account session FIFO cap enforcement — migration upserts may exceed
  `SESSIONS_MAX_PER_ACCOUNT`; the next real login evicts (botplatform-service owns the cap).
- OUTBOX-buffering of the status fan-out — user-service keeps its existing **direct INBOX
  publish** (fire-and-forget, last-write-wins; decision recorded 2026-07-18).
- Bulk backfill of users/sessions/hr_employee ≤ checkpoint (separate owner).

---

## 2. Users lane

### 2.1 insert / replace — upsert, overwrite mapped fields
Decode the full doc (`bson.UnmarshalExtJSON`, relaxed). Foreign-origin docs
(`federation.origin` set) are skipped — defense-in-depth behind the connector `$match` (§6).

Write: `UpdateOne(filter: {account}, update, upsert: true)` where
- `$setOnInsert`: `_id` ← **source `_id` adopted** (no ID generation; an existing destination doc
  keeps its `_id`),
- `$set` (source wins on every insert/replace event): `account` ← `username`, `engName`,
  `chineseName` ← `customFields.companyName`, `deptId`/`deptName`/`sectId`/`sectName`, `roles`
  (mapped: `admin` → `UserRoleAdmin`, else `UserRoleUser`), `statusText`, `statusIsShow` (source
  field name pending confirmation, §9), `siteId` ← `siteIDFromOrigin(federation.origin, SITE_ID)`,
  bot marker (`type == "bot"`), `updatedAt` ← source op time (`clusterTime`, ms).

No fan-out on insert/replace — remote sites learn about users from their own sync/migration.

### 2.2 update — status only, delta-first
1. Inspect `updateDescription` **before** any lookup. Delta touches neither `statusText` nor
   `statusIsShow` → **ack-skip** (`user_update_no_status`). HR fields, roles, presence, resume
   arrays on the user doc — all deliberately not propagated (company-wide sync owns them).
2. Delta touches status → source-lookup the full current doc by `_id`; vanished → ack-skip;
   foreign (`federation.origin` set) → ack-skip (the `$match` cannot filter updates — no
   `fullDocument` — so this is the real filter, mirroring the messages lane).
3. Sync request/reply to `chat.migration.internal.{site}.user.status` (§5) with
   `{account, statusText, statusIsShow, updatedAt: clusterTime}`.
4. Reply classification (`errcode.Parse`, cross-site-consumer tier):
   - success → Ack;
   - `not_found` (user absent **or deactivated** — the RPC matches active users only) →
     **ack-skip + metric** (`status_user_missing`), never Nak — retrying a deactivated user's
     status to `MAX_DELIVER` helps nobody;
   - any other category / transport error → Nak (transient).

### 2.3 delete — skip
Deactivation is `active:false` (an update); a hard user delete is un-actionable (only source
`_id`) → ack-skip + metric.

---

## 3. Sessions lane

### 3.1 Source shape (pending SOURCE_DATA.md section)
A separate legacy sessions collection (name via `SESSIONS_COLLECTION` env). Working assumption
pending the source schema section: docs carry an account/user reference and a **resume-token
list** (`[{hashedToken, when}, …]`, Meteor-style base64-SHA256 **already-hashed** tokens — same
scheme as the target `sessions._id`). The mapper lives in one file (`sessions.go`) so field-name
corrections from the schema section stay local.

### 3.2 Op handling — upsert-only
| Op | Handling |
|---|---|
| `insert` / `replace` | upsert **every token** in the doc's resume-token list as one target `sessions` row each |
| `update` | delta-first: resume-token-list field changed → source-lookup full doc → upsert all tokens; any other field → ack-skip (`session_update_no_tokens`) |
| `delete` | ack-skip (upsert-only; revocation gap accepted, §1.2) |

Per-token row (target `sessions`, botplatform-service schema):
`_id` ← hashed token · `issuedAt` ← token `when` (ms) · `userId`/`account`/`siteId`/`roles` ←
resolved from the **target users collection by account**.

- Target user **absent** → **Nak-retry** (bounded by `MAX_DELIVER`) — the users lane or the
  company-wide sync will land it (thread-subs FK precedent).
- Target user's `siteId` ≠ ours → **ack-skip** (`session_foreign_user`) — second federation guard,
  independent of whatever origin marker the session doc itself carries.
- Upsert is `ReplaceOne(_id, row, upsert)` — idempotent under redelivery; re-upserting an existing
  token refreshes the same row, never duplicates.

---

## 4. hr_employee lane

Source: **`company_hr_acct_org`** (already in `WATCH_COLLECTIONS`). Mapped — not verbatim — into
target `hr_employee` `{account, employeeId, siteId}` rows; **upsert keyed by `account`**
(destination unique index on `account` already exists; portal-service reads it).

| Op | Handling |
|---|---|
| `insert` / `replace` | decode → map → upsert by `account` |
| `update` | source-lookup full doc → map → upsert (vanished → ack-skip) |
| `delete` | **ack-skip + metric** until the source mutation pattern is confirmed (SOURCE_DATA §7 Q5 is still open) |

Field mapping acct/org-doc → `{account, employeeId, siteId}` is an **open confirmation** (§9) —
the mapper file (`hremployee.go`) isolates it. Ownership note: the HR cron also writes
`hr_employee`; during the window writes are last-write-wins between cron and migration — accepted,
both derive from the same HR truth.

---

## 5. user-service — internal migration status RPC

- **Subject:** `chat.migration.internal.{siteID}.user.status` — new builder
  `subject.MigrationUserStatus(siteID)` (+ pattern func). **Server identities only** — same NATS
  account-permission requirement as `chat.migration.internal.{site}.msg.*` (no client publish).
- **Request** (`pkg/model`): `MigrationUserStatusRequest{Account string, StatusText string,
  StatusIsShow *bool, UpdatedAt int64}` (all `json` camelCase; `UpdatedAt` unix ms = source op time).
- **Handler** (user-service, `nc.QueueSubscribe`, queue group `user-service`, `errnats.Reply`):
  one `FindOneAndUpdate` on the **active** user by account, `$set` `statusText`, `statusIsShow`
  (only when non-nil), `updatedAt`; no match → `errcode.NotFound`; success → reuse the existing
  `publishStatus` fan-out (direct INBOX publish to every remote site — **unchanged**).
- Repo: new `SetUserStatusMigration(ctx, account, text, isShow, updatedAtMs)` beside
  `SetUserStatus` — the live client path is untouched (it does not stamp `updatedAt`; changing
  that is out of scope).
- **Not client-facing** → `docs/client-api.md` untouched; ops note added to
  `data-migration/README.md` instead.

---

## 6. Connector change — list-driven federation filter

`MESSAGE_COLLECTION` (single) is **replaced** by `FILTERED_COLLECTIONS` (comma list, default
`rocketchat_message`). Every watched collection named in the list gets the existing `$match` stage
— drop `insert`/`replace` where `fullDocument.federation.origin` is set. Semantics per collection
are identical to today's message filter, including:

- collections whose docs lack `federation.origin` are unaffected (absent field → kept);
- `update`/`delete` carry no doc → filtered downstream by the consuming transformer;
- the **checkpoint-stall-under-`$match`** caveat (README) now applies to each listed collection —
  same accepted replay-not-loss trade-off, still connector-internal.

Ops config after this change (collections deployment):
`WATCH_COLLECTIONS=…,users,<sessions>,company_hr_acct_org` and
`FILTERED_COLLECTIONS=users,<sessions>` (messages deployment keeps
`FILTERED_COLLECTIONS=rocketchat_message`). Whether the sessions collection carries
`federation.origin` is an open confirmation (§9); listing it is harmless either way, and the
transformer's target-user site guard (§3.2) filters foreign sessions regardless.

---

## 7. Service skeleton, config, error handling

**Layout** (flat `package main`, mirrors `oplog-collections-transformer`):
`main.go` · `config.go` · `handler.go` (dispatch + shared resolveDoc/documentKeyID) · `users.go` ·
`sessions.go` · `hremployee.go` · `statusclient.go` (RPC client) · `targetstore.go` ·
`metrics.go` · `*_test.go` · `deploy/{Dockerfile,docker-compose.yml,azure-pipelines.yml}`.

**Consumer:** durable `oplog-user-session-transformer`, `AckExplicitPolicy`, `DeliverAllPolicy`,
`FilterSubjects` = the three collections' subjects, created via the shared
wait-for-stream-then-create retry. Sequential `cons.Consume`; single replica.

**Dispositions** (shared `pkg/migration.Classify` skeleton, identical to siblings): nil → Ack;
`ErrSkipped` → ack-without-count (reason metered); `ErrPoison` (undecodable doc/documentKey,
non-degraded missing `fullDocument`, unmapped collection in lookups) → Term;
other → Nak +2s up to `MAX_DELIVER`, then Term + `exhausted` metric. Degraded events recover via
source lookup (never Term). **No `DELETE_MAX_DELIVER`** — every delete in this service is an
immediate ack-skip, so the short cap has nothing to bound.

**Config** (env, fail-fast, whitespace-validated like the sibling):

| Env | Req | Default | Purpose |
|-----|-----|---------|---------|
| `SITE_ID` | ✓ | — | site scope |
| `NATS_URL` / `NATS_CREDS_FILE` | ✓ / – | — / `""` | consume + RPC |
| `SOURCE_MONGO_URI` (+`_USERNAME`/`_PASSWORD`) | ✓ | — | update lookups + degraded recovery |
| `SOURCE_DB` | | `rocketchat` | source DB |
| `TARGET_MONGO_URI` (+`_USERNAME`/`_PASSWORD`) | ✓ | — | users/sessions/hr_employee writes + user FK reads |
| `TARGET_DB` | | `chat` | target DB |
| `USERS_COLLECTION` | | `users` | source users |
| `SESSIONS_COLLECTION` | | *(set with SOURCE_DATA section)* | source sessions |
| `HR_ACCT_ORG_COLLECTION` | | `company_hr_acct_org` | source HR acct/org |
| `SOURCE_READ_PREFERENCE` | | `primaryPreferred` | lookups must not read a lagging secondary |
| `CONSUMER_DURABLE` | | `oplog-user-session-transformer` | durable name |
| `MAX_DELIVER` | | `1000` | redelivery cap |
| `STATUS_RPC_TIMEOUT` | | `5s` | user-service request timeout |
| `BOOTSTRAP_STREAMS` | | `false` | dev-only (connector owns MIGRATION_OPLOG) |
| `HEALTH_ADDR` | | `:9090` | `/healthz` |

**Observability** (o11y SDK, nil-safe metrics): `oplog_user_session_transformer_…`
`events_processed_total` / `naks_total` / `terms_total` / `exhausted_total` (by op+collection),
`events_skipped_total` (by reason), `writes_total` (by collection+action: users upsert, sessions
upsert count, hr_employee upsert), `status_rpc_total` (by outcome: ok / not_found / error),
`resolve_miss_total` (session user FK). Request-ID stamped at consume, propagated through lookup,
RPC, and writes.

---

## 8. Testing (TDD)

- **Unit** (fakes for lookup / target store / RPC client / clock): per-lane table-driven tests —
  users upsert mapping + overwrite semantics, delta-first status gate (no-status delta never
  touches the lookup), RPC `not_found` → ack-skip vs infra → Nak, foreign skips per lane, session
  token-list expansion (N tokens → N rows), missing-user Nak, hr mapping + delete-skip, dispatch,
  poison/degraded paths, config parsing.
- **user-service unit:** new handler — set + fan-out, `NotFound` on absent/deactivated,
  `statusIsShow` nil vs set, `updatedAt` stamped.
- **connector unit:** `FILTERED_COLLECTIONS` parsing; `$match` applied to each listed watcher and
  no others.
- **Integration** (`pkg/testutil` Mongo ×2 + NATS; `TestMain` = `testutil.RunTests`): users
  insert→upsert e2e (then replace overwrites mapped fields, preserves destination `_id`); status
  update → RPC → target doc updated + `user_status_updated` on the remote INBOX; sessions
  Nak-then-resolve (user absent → error, seed user → rows land); hr_employee upsert e2e.
- Coverage: ≥80% floor, ≥90% target on handlers/mappers.

---

## 9. Open confirmations (source engineers)

Recorded SOURCE_DATA-style; blocking only where marked:

1. **Sessions collection name + doc shape** (§3.1) — **blocks implementation of `sessions.go`**.
   To be added as a new `SOURCE_DATA.md` section: token list field path, whether tokens are
   already base64-SHA256 hashes, `when` semantics, account/user reference field.
2. **Sessions federation marker** — does the sessions doc carry `federation.origin`? (Only
   affects whether the connector `$match` helps; the transformer's site guard filters regardless.)
3. **`company_hr_acct_org` → `hr_employee` field mapping** (§4) — account / employeeId / site
   fields. **Blocks `hremployee.go`.**
4. **`company_hr_acct_org` mutation pattern** (SOURCE_DATA §7 Q5, still open) — decides whether
   delete stays a skip.
5. **Source field for the status show-flag** — SOURCE_DATA §6 lists `statusText`/`status` but no
   show-flag; confirm the legacy field name backing destination `statusIsShow` (the §2.2 delta
   gate and §2.1 mapping both key on it). If none exists, `statusIsShow` drops out of the users
   lane and the RPC only ever receives it as nil.

---

## 10. Ops & cleanup

- **NATS permissions (required):** `chat.migration.internal.{site}.user.*` restricted to server
  identities — extend the existing `msg.*` restriction note in `data-migration/README.md`.
- **Connector env:** add the sessions collection to the collections deployment's
  `WATCH_COLLECTIONS`; set `FILTERED_COLLECTIONS` per §6 (replaces `MESSAGE_COLLECTION`) — update
  both deployments' manifests.
- **Docs in the implementation PR:** `data-migration/README.md` (component table + at-a-glance
  section), `CDC_COVERAGE.md` (users rows revived under this owner; new sessions + hr_employee
  sections; `user_status_updated` re-marked ✅ emitted), `SOURCE_DATA.md` sessions section.
- **Cleanup PR at source retirement** (extends the existing list): delete this service; remove the
  user-service migration handler + `SetUserStatusMigration`; drop the extra connector env.
