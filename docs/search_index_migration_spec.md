# ES Index Migrator Design Spec

## Purpose

`data-migration/es-index-migrator` is a standalone, **one-time, pre-live** job that backfills a site's three Elasticsearch indexes — **messages** (monthly), **spotlight**, and **user-room** — directly from that site's own current-stack Cassandra and MongoDB, as part of that site's cutover *before* it starts taking live traffic. It exists for situations where a site's ES data needs to be built or reconstructed from scratch during that pre-live window:

- Populating search for a site as part of its pre-live migration cutover, once the bulk Cassandra/MongoDB migration for that site has completed.
- An index mapping/template change discovered before go-live that requires a full reindex.
- ES data loss or corruption that happened during the pre-live migration window itself.

**Scope boundary — pre-live only.** This job normally runs once per site, after that site's bulk data migration into Cassandra/MongoDB finishes and before the site goes live; pre-live reruns (e.g. to pick up a template/mapping fix) are supported, but only after clearing the target indices first — see "Rerunning this job" below. External versioning makes a duplicate write of the *same* data safe, but it does not remove a stale deleted-document or update an already-equal-version document, so it is not on its own a substitute for clearing indices before a rerun. Once a site is live, `oplog-connector`'s CDC tail and `search-sync-worker`'s live event consumption take over as the system of record for search indexing — this job is not a general "rebuild ES for an already-live site" tool and must not be re-run against a site that has gone live. It has no mechanism to catch up on writes made after its own run; see "No at-rest decryption dependency" below for the concrete failure mode if it's pointed at live data anyway.

It is **not** an onboarding tool. Migrating a brand-new site off the legacy RocketChat source system is a separate, already-solved problem owned by `oplog-connector` + `oplog-collections-transformer` (also under `data-migration/`) — this job never reads legacy `rocketchat_*` collections and has no relationship to that pipeline.

## Why not route through the live pipeline

Replaying history through `MESSAGES_CANONICAL`/`INBOX` would also fire `notification-worker`'s push notifications and `broadcast-worker`'s live delivery — both wrong for data that could be months or years old. This job bypasses those streams entirely and writes straight to Elasticsearch via `_bulk`, reusing the exact document shapes, field mappings, index names, and template/script builders `search-sync-worker` uses on its live path (via the shared `pkg/searchindex`/`pkg/searchengine` packages — see "Shared code" below), so a document's *content* is indistinguishable from one the live worker would have produced. Versioning is the one intentional exception: this job versions messages by `CreatedAt` rather than the live path's event-publish timestamp — see "Write model" below for why that's fine under this job's pre-live-only contract.

## Site scoping

Run once per site, against that site's own Cassandra/MongoDB/ES — no cross-site reads:

- **Messages**: a room's history lives exclusively in the Cassandra of the site that owns the room. Room IDs are enumerated from every distinct `roomId` in the site-scoped `subscriptions` collection — the same federated-subscriber-inclusive set described below for Spotlight/user-room, not just locally-owned rooms — rather than a `SELECT DISTINCT` scan of Cassandra or a `rooms` collection query. A room a foreign site's user is subscribed to but this site doesn't own is a harmless empty-result Cassandra query — the room's messages simply don't exist in this site's keyspace.
- **Spotlight / user-room**: every subscription document in the site's own `subscriptions` collection is processed, whether the subscribing user is local to this site or federated in from elsewhere — `subscriptions` is already scoped by the subscribing user's `siteId`, so no extra filter is needed.

Index **names** are identical across sites; isolation comes from each site's job pointing at its own `SEARCH_URL`.

## Source of truth (and why this differs from the design's earlier draft)

An earlier version of this design (built for a now-retired sibling repo) read room/subscription data from legacy RocketChat Mongo collections (`rocketchat_subscriptions`, `rocketchat_rooms`), because at the time those were the only available source for sites still being migrated off the old system. **This repo's own MongoDB no longer has those collections at all** — every site's rooms and subscriptions already live in the current schema (`rooms`, `subscriptions`), and a subscription document already carries everything this job needs (`roomId`, `roomType`, `name`, `joinedAt`, `historySharedSince`) with no separate room lookup or type-classification step required. Concretely, this job reads only:

- Cassandra: `messages_by_room` (bucketed message history).
- MongoDB: `subscriptions` (one document per user-room membership).

It does not read `rooms`, `room_members`, or any legacy collection.

## Additive-only limitation

Subscriptions are **hard-deleted** the moment a user leaves a room (`DeleteOne`/`DeleteMany` in `room-worker`) — there is no soft-delete/closed-row state to read. Because of this, a point-in-time read of `subscriptions` can only tell this job about **current** memberships, never about memberships that ended. This means:

- Spotlight and user-room backfills are **additive reconstructions of current state**. They will correctly (re)populate ES with every room a user is currently in.
- They **cannot** detect or evict a stale ES entry for a membership that ended and was never re-added, if that stale entry predates this job's run (e.g., from an earlier partial ES loss). This is an accepted limitation of any rebuild tool operating on current-state data, not a bug — closing it would require replaying membership history, which the current `subscriptions` schema doesn't retain.

Messages are read from Cassandra's append-only history table and are unaffected by this limitation — a message row exists for as long as `messages_by_room` retains it, deleted rows are explicitly flagged (`deleted = true`) and skipped, never physically removed early.

## Write model: every write is versioned / idempotent

Every document this job writes is written using the same `version_type: external` (or scripted last-write-wins, for user-room) mechanism the live worker (`search-sync-worker`) uses — a re-run of this job always converges instead of racing or duplicating:

- **Messages** — `ActionIndex`, `Version = Message.CreatedAt.UnixMilli()`, `version_type: external`. ES only accepts a write whose version is strictly greater than what's stored; a duplicate write with the same timestamp 409s, which `pkg/searchengine.IsBulkItemSuccess` already treats as a benign, expected outcome for `ActionIndex`. Note this job versions by `CreatedAt`, while `search-sync-worker`'s live path versions by the event's publish timestamp (`evt.Timestamp`, which advances on edits — see CLAUDE.md's Event Timestamps convention) — the two are **not** version-comparable. That's fine under this job's pre-live-only scope (see "Scope boundary" above: no live-worker writes exist yet to converge against), but it means the "converges instead of racing" property holds only for reruns of this job itself, not for an "overlapping live write from `search-sync-worker`" — that scenario is out of scope by design, not something this versioning scheme was built to handle.
- **Spotlight** — `ActionIndex`, `Version = Subscription.JoinedAt.UnixMilli()`. (There is no delete path here — see Additive-only limitation above: every subscription this job reads is a current, active membership.)
- **User-room** — `ActionUpdate` against the same stored Painless script `search-sync-worker` registers at startup (`search-sync-user-room-add-v1`), with **no external ES version** — the script's own `roomTimestamps`-keyed last-write-wins guard (`params.ts > stored`, else `ctx.op = 'none'`) is the ordering mechanism, exactly as it is on the live path.

## Shared code

To guarantee this job can never drift from what `search-sync-worker` actually indexes, the ES document shapes, Painless scripts, template/mapping builders, and bulk-result classification are extracted out of `search-sync-worker` (where they were previously private) into two importable packages both services depend on:

- **`pkg/searchindex`** — `MessageDoc`/`SpotlightDoc`/`UserRoomUpsertDoc` struct definitions and their `New*Doc` builders, the two user-room Painless scripts and their stored-script IDs, the `BuildAddRoomUpdateBody`/`BuildRemoveRoomUpdateBody` update-body builders, and the three index template/mapping builders (`MessageTemplateBody`, `SpotlightTemplateBody`, `UserRoomTemplateBody`).
- **`pkg/searchengine`** — `IsBulkItemSuccess`, the classifier that decides whether a `_bulk` response item counts as success or failure per action type (correctly treating a benign external-versioning 409 on `ActionIndex`/`ActionDelete` as success, while still failing a real conflict on `ActionUpdate`).

`search-sync-worker` becomes a thin caller of both packages; this job is the second, independent caller — see the implementation plan (`docs/superpowers/plans/2026-07-24-es-index-migrator.md`) for the exact extraction steps.

## No at-rest decryption dependency

`message-worker`'s live write path can store message content encrypted in Cassandra's `enc_payload`/`enc_meta` columns when a site has `ATREST_ENABLED=true` (envelope encryption via `pkg/atrest`, DEKs wrapped by Vault's transit engine), and `history-service` decrypts those columns on its live read path. This job is different: the `messages_by_room` rows it reads were themselves written directly into the plaintext `msg`/`attachments`/`card` columns by the pre-live bulk migration that populated the table for this site — never through the live at-rest-encryption write path. So this job reads those columns as-is, never touches `enc_payload`/`enc_meta` for indexing purposes, and has no dependency on `pkg/atrest` or Vault at all — no Vault connectivity, no `room_data_keys` read access, no `ATREST_*`/`VAULT_*` config.

This is an operational assumption tied directly to the job's pre-live scope (see "Scope boundary" above), not something the schema enforces on its own. `messagesource_cassandra.go` does not rely on that assumption blindly: it also reads `enc_payload` (the column, not its content) purely to detect whether it's populated, and `rejectEncryptedMessage` hard-fails the affected room's stream the moment a row carries a non-empty encrypted payload, rather than silently indexing blank content. In normal pre-live use this should never trigger. If it does, it means this run has reached data written through the live at-rest-encryption path — i.e. the site is no longer in the pre-live window this job assumes — and the failure should be treated as a signal to abort and investigate, not retried or ignored.

## Collection 1 — Messages

**Source:** Cassandra `messages_by_room`, partition key `(room_id, bucket)`, clustering `(created_at DESC, message_id DESC)`.

**Read strategy:**
1. Enumerate `roomId`s from the site's own `subscriptions` collection (`Distinct("roomId", {siteId})`).
2. Per room, walk every bucket touching `[MIGRATION_START_AT, MIGRATION_END_AT)` via `pkg/msgbucket.Sizer` (bucket window = `MESSAGE_BUCKET_HOURS`, must match the value configured on `message-worker`/`history-service` for this site).
3. Stream rows per bucket (never materializing a whole room's history in memory) via `SELECT ... FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at >= ? AND created_at < ?`.
4. Skip rows where `deleted = true`.

Both this `Distinct("roomId", ...)` and Collection 2/3's `subscriptions.find({siteId})` filter on `siteId` — a `{siteId: 1, roomId: 1}` index on `subscriptions` keeps both queries index-covered instead of falling back to a collection scan on a large site.

**Field mapping** (Cassandra `Message` → `searchindex.MessageDoc`, via `searchindex.MessageFields`):

| ES field | Cassandra column |
|---|---|
| `messageId` | `message_id` |
| `roomId` | `room_id` |
| `siteId` | `site_id` (fallback to configured `SITE_ID` if empty) |
| `userId` | `sender.id` |
| `userAccount` | `sender.account` |
| `content` | `msg` |
| `createdAt` | `created_at` |
| `editedAt` | `edited_at` |
| `updatedAt` | `updated_at` |
| `threadParentMessageId` | `thread_parent_id` |
| `threadParentMessageCreatedAt` | `thread_parent_created_at` |
| `tshow` | `tshow` |
| `isBot` | derived from `sender.account`'s `.bot` suffix |
| `attachments` / `attachmentText` | `attachments` (decoded via `cassandra.DecodeAttachments`) |
| `cardData` | `card.data` |

**Index name & doc ID:** index `{MSG_INDEX_PREFIX}-{createdAt UTC "2006-01"}`; doc ID = `message_id`.

**Bulk action:** `ActionIndex` always (a historical scan never deletes); `Version = CreatedAt.UnixMilli()`, `version_type: external`.

## Collection 2 — Spotlight

**Source:** every subscription document for the site (`subscriptions.find({siteId})`) — no time window (see Additive-only limitation: subscriptions represent current state, not append-only history, so a rebuild backfills all of them unconditionally). Per CLAUDE.md's "always project precisely" rule, this query uses an explicit projection covering only the top-level fields Collections 2/3 need — `_id`, `u` (the embedded participant, source of `account`/`isBot`), `roomId`, `siteId`, `name`, `roomType`, `historySharedSince`, `joinedAt` — never a whole-document fetch.

**Field mapping** (`model.Subscription` → `searchindex.SpotlightDoc`, via `searchindex.SpotlightFields`):

| ES field | Subscription field |
|---|---|
| `userAccount` | `u.account` |
| `roomId` | `roomId` |
| `roomName` | `name` |
| `roomType` | `roomType` |
| `siteId` | `siteId` |
| `joinedAt` | `joinedAt` |

**Index name & doc ID:** index = `SPOTLIGHT_INDEX` (flat); doc ID = `{userAccount}_{roomId}`.

**Bulk action:** `ActionIndex` always; `Version = JoinedAt.UnixMilli()`. One doc per subscription row (already 1:1, no fan-out needed).

## Collection 3 — User-Room

**Source:** same subscriptions query as Spotlight, same "no window" reasoning. Iterate per subscription row — no full-doc-rebuild mode (matches the live worker's own per-event scripted-delta approach).

**Bulk action, per subscription row:** `ActionUpdate` against `search-sync-user-room-add-v1`, doc ID = `u.account`, `params = {rid: roomId, ts: joinedAt.UnixMilli(), hss: historySharedSince != nil ? historySharedSince.UnixMilli() : 0}`. No external version — the script's own `roomTimestamps` guard is the ordering mechanism. **Bot subscriptions (`u.isBot == true`) are skipped** — bots don't search, and including them would only inflate the per-user access-control view (matches `search-sync-worker`'s live behavior).

Index: `USER_ROOM_INDEX` (flat).

## Configuration

| Var | Required | Default | Description |
|---|---|---|---|
| `SITE_ID` | yes | — | Site this run migrates. Scopes all reads and stamps `siteId`. |
| `SEARCH_URL` | yes | — | Target ES for this site. |
| `SEARCH_USERNAME` / `SEARCH_PASSWORD` | no | `""` | ES auth, if required. |
| `SEARCH_TLS_SKIP_VERIFY` | no | `false` | Opt-in only, for self-signed/internal clusters. |
| `MSG_INDEX_PREFIX` / `SPOTLIGHT_INDEX` / `USER_ROOM_INDEX` | yes | — | Same names `search-sync-worker` uses for this site. |
| `MIGRATION_START_AT` | yes | — | RFC3339, inclusive lower bound for the **messages** window. |
| `MIGRATION_END_AT` | yes | — | RFC3339, exclusive upper bound for the **messages** window. Must be after `MIGRATION_START_AT`. |
| `MESSAGE_BUCKET_HOURS` | yes | — | Must match the value configured on `message-worker`/`history-service` for this site. |
| `MONGO_URI` | yes | — | Site's own MongoDB. |
| `MONGO_DB` | no | `chat` | |
| `MONGO_USERNAME` / `MONGO_PASSWORD` | no | `""` | |
| `CASSANDRA_HOSTS` | yes | — | Site's own Cassandra. |
| `CASSANDRA_KEYSPACE` | no | `chat` | |
| `CASSANDRA_USERNAME` / `CASSANDRA_PASSWORD` | no | `""` | |
| `CASSANDRA_NUM_CONNS` | no | `8` | |
| `BULK_BATCH_SIZE` | no | `500` | Soft cap on buffered ES actions per `_bulk` call. |
| `WORKER_CONCURRENCY` | no | `4` | Parallel room/subscription workers. |

Only `MIGRATION_START_AT`/`MIGRATION_END_AT` bound anything — spotlight and user-room always backfill the site's full current subscription set. There are no `ATREST_*`/`VAULT_*` vars — see "No at-rest decryption dependency" above.

## Execution shape

1. Parse config, fail fast on missing required vars.
2. Connect to Cassandra (`cassutil.Connect` — `LocalQuorum` consistency, 10-second timeout, per repo convention), MongoDB, Elasticsearch.
3. Idempotently **ensure** (not just verify) the three ES index templates and two user-room stored scripts exist, using the exact same builders `search-sync-worker` uses at its own startup — so this job can run standalone against a fresh site that has never run `search-sync-worker`.
4. Run the three collections sequentially — messages, then spotlight, then user-room — to avoid the three phases contending for the same Cassandra/MongoDB connection pools; each collection internally parallelizes across rooms/subscriptions with a bounded worker pool (`WORKER_CONCURRENCY`).
5. Buffer ES bulk actions per collection, flushing at `BULK_BATCH_SIZE`. A flush always runs at the end of each collection's pass, even if a worker in that collection failed — partial progress is never silently discarded.
6. `slog` JSON progress/error logs throughout, per repo convention.
7. On a `_bulk` item failure, log the failing doc ID + error and continue — don't abort the whole run for a handful of bad rows — but track a failure count.
8. Exit non-zero if any collection's read failed, or if any bulk item failed across any collection; exit 0 only on a fully clean run.

## Rerunning this job

This job has no delete path for messages (see "Additive-only limitation" above) — a message deleted in Cassandra between two runs is simply skipped on the later run, not removed from an index that already indexed it on an earlier run. So a rerun (e.g. to pick up a fixed index template/mapping, or a bug found in an earlier run) must always start from empty indices, not write on top of a prior run's data:

1. Delete the site's existing indices before rerunning:
   - **Messages** are split into monthly indices (`{MSG_INDEX_PREFIX}-2026-01`, `{MSG_INDEX_PREFIX}-2026-02`, ...) — delete all of them for this site with a wildcard, e.g. `DELETE /{MSG_INDEX_PREFIX}-*` against the site's own `SEARCH_URL`.
   - **Spotlight** and **user-room** are each a single flat index — `DELETE /{SPOTLIGHT_INDEX}` and `DELETE /{USER_ROOM_INDEX}`.
2. Just run the job again. Index **templates** and the two user-room Painless **scripts** do not need to be recreated manually — step 3 of "Execution shape" idempotently re-ensures them on every run, and Elasticsearch auto-creates each index from its template the moment this job's first write lands.

This is safe pre-live (no one is searching the site yet, so the brief window with no data during the delete-then-rerun isn't user-visible). It is not a safe procedure once a site is live — see "Scope boundary — pre-live only" above.

## Related documents

- `docs/superpowers/plans/2026-07-24-es-index-migrator.md` — the task-by-task implementation plan (TDD steps, exact file layout, test cases).
