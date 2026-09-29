# Message audit access — design

**Date:** 2026-09-29
**Branch:** `claude/bold-knuth-sitjie`
**Status:** approved design, not implemented

## Goal

Let an authorized investigator read every message in every room that one or more
user accounts belong to, after a sign-off by a second person, with every operation
recorded in a tamper-evident log. The capability must be hard to reach for a developer
with production access to the platform.

## Decisions summary

| Question | Decision |
|---|---|
| Sign-off | Two-person: a requester submits, a different approver grants |
| Threat model | Developers with production access (kubectl, MongoDB, Cassandra, Vault policies) |
| Identity and roles | OIDC SSO, roles from an IdP group claim, no platform-stored auditor role |
| Grant shape | One or more target accounts, one message date range, time-limited validity |
| Multi-site | Grants and reads are per site, no federation; remote rooms are listed, not readable |
| Consumption | Interactive console; export is a follow-up |
| Message source | A new archive fed from MESSAGES-CANONICAL, not live Cassandra |
| Archive record | Sealed segments in an S3-compatible bucket under Object Lock, encrypted with an audit-only Vault key |
| Archive index | Dedicated Elasticsearch cluster, metadata plus encrypted body, no plaintext search |
| Operation log | Hash-chained collection in a dedicated MongoDB deployment, mirrored to an Object Lock bucket |
| Membership history | Recorded by the archive from rollout on; rooms the target left stay readable for the membership interval |

## Non-goals

- Export bundles.
- Plaintext full-text search over message bodies. The index shape allows it later, but
  no mode flag or migration tool is designed here.
- Cross-site federated reads. An investigation touching two sites needs a grant at each.
- Backfill of messages sent before the archive worker was deployed.
- Attachment blob retrieval through `media-service`.
- Legal hold on live deletes in Cassandra. The archive keeps deleted content; the live
  store is unchanged.
- Changes to the chat clients or to how users see their own messages.

## Background: what exists and what is wrong with it

The survey of the repo found four facts that shape this design.

1. **Any backend credential can already read any user's history.** `history-service`
   trusts the `{account}` segment of the NATS subject and checks only that the named
   account is subscribed to the room. Backend services share one NATS user allowed to
   publish and subscribe on `>`. `auth-service` in `DEV_MODE` mints a JWT for any
   account with no validation. An audit front door is pointless while this side door
   stays open, so §2 closes it.
2. **One Vault key decrypts everything.** At-rest encryption wraps every room DEK with
   one transit key, `chat-kek`, and at least seven services hold the decrypt policy.
   The archive therefore uses a separate key that no chat service holds.
3. **`admin_audit` is not evidence-grade.** It logs mutations only, best-effort after the
   action, in a mutable collection on the shared cluster.
4. **History is lost.** Leaving a room hard-deletes the subscription, editing overwrites
   in place, and deleting a message nulls its content. Live Cassandra cannot answer
   "what did this person write last year in a room they since left".

The existing `permission_grants` collection (applicant, approver, reason, time window)
and the `search-sync-worker` batching pipeline are the two patterns this design builds
on.

## 1. Architecture

Four deliverables, in dependency order.

1. **Prerequisite hardening** (§2). Small code changes plus ops configuration.
2. **`archive-worker`** (§4). A new JetStream worker that copies every canonical message
   and membership event into the archive bucket and the archive index.
3. **`audit-service`** (§3, §5, §6). A new flat Gin service owning grants, the operation
   log, room enumeration, and message reads.
4. **`audit-frontend`** (§8). A separate Vite and React SPA.

### Stores

All stores below belong to the audit plane and are reachable only with credentials the
audit plane holds (§7).

| Store | Purpose | Written by | Read by |
|---|---|---|---|
| Archive bucket (S3-compatible, Object Lock compliance mode) | System of record: sealed, encrypted segments of canonical events | `archive-worker` | `audit-service` (verification only) |
| Archive Elasticsearch cluster | Query index: message metadata with encrypted body, membership intervals | `archive-worker` | `audit-service` |
| Audit MongoDB deployment | `audit_requests`, `audit_ops`, `audit_chain_checkpoints`, `archive_data_keys` | `audit-service`, `archive-worker` (keys only) | `audit-service`, `archive-worker` |
| Operation log sink (S3-compatible, Object Lock) | Immutable copy of every `audit_ops` entry | `audit-service` | Ops, on demand |

`audit-service` also holds a read-only credential on the chat MongoDB for the `users`
collection, used to resolve target accounts and for the distinctness checks in §3. It
has no connection to NATS, Cassandra, the live Elasticsearch cluster, or any other chat
collection.

### Data flow

```
MESSAGES-CANONICAL ─┐
                    ├─► archive-worker ─► segment PUT ─► archive bucket
INBOX (member evts) ┘        │
                             └─► bulk index ─► archive Elasticsearch
                                                      ▲
audit-frontend ─► audit-service ─► grant check ─► ops log (Mongo + sink) ─┘ query + decrypt
```

## 2. Prerequisite hardening

These changes are required before the audit plane is meaningful. They are small and
ship first.

- **`auth-service` dev-mode guard.** `DEV_MODE=true` is refused at startup unless a
  second flag, `DEV_MODE_LOCAL_ONLY_ACK=true`, is also set. Local docker-compose sets
  both. A production deployment that copies `DEV_MODE=true` by mistake exits instead of
  minting unauthenticated JWTs.
- **Per-service NATS credentials.** The shared `backend` NATS user with `>` on publish
  and subscribe is replaced by per-service users. Only the services that legitimately
  call history on a user's behalf keep `chat.user.>` publish rights: `room-service`,
  `message-gatekeeper`, `broadcast-worker`, `notification-worker`, `room-worker`,
  `search-sync-worker`. Every other service gets the subjects it actually uses. This is
  an ops change in the NATS account configuration, documented in
  `docker-local/setup.sh` for local parity, with no application code change.

## 3. Identity, roles, and the grant workflow

### Identity

`audit-frontend` performs the OIDC authorization-code flow with PKCE against the
existing issuer and sends the ID token as a bearer on every call. `audit-service`
validates it per request with `pkg/oidc` and reads a groups claim. The claim name and
the two group names are configuration:

| Env | Default | Meaning |
|---|---|---|
| `AUDIT_OIDC_GROUPS_CLAIM` | `groups` | Claim holding the user's groups |
| `AUDIT_AUDITOR_GROUP` | required | Group that grants the auditor role |
| `AUDIT_APPROVER_GROUP` | required | Group that grants the approver role |

There is no session collection and no platform-stored role. Token lifetime is the
revocation window, so the IdP must issue short-lived tokens for this client; this is a
deployment precondition. A token from a user in neither group is refused and logged.

Every identity is recorded as the OIDC `sub` claim (stable, opaque) plus the
`preferred_username` claim (the platform account name). Distinctness checks compare
`sub`. Auditors and approvers do not need a chat account.

### Grant record

`audit_requests`, one document per request, never deleted.

| Field | Type | Meaning |
|---|---|---|
| `_id` | string | UUIDv7 hex via `idgen.GenerateUUIDv7()` |
| `siteId` | string | The service's own site |
| `targetAccounts` | string[] | 1 to `AUDIT_MAX_TARGETS` (default 20) accounts, each existing in `users` |
| `requesterSubject`, `requesterAccount` | string | From the token |
| `reason` | string | 1 to 1000 runes |
| `caseRef` | string | Optional external reference |
| `messageRange` | `{from, to}` | Half-open UTC window the auditor may read; `to` may not exceed submission time |
| `requestedValidity` | duration | Capped by `AUDIT_MAX_VALIDITY` (default 30d); default `AUDIT_DEFAULT_VALIDITY` (7d) |
| `status` | enum | `pending`, `approved`, `denied`, `revoked`, `expired` |
| `approverSubject`, `approverAccount`, `decidedAt`, `decisionNote` | | Set on approve or deny |
| `validUntil` | time | Set on approve: `decidedAt + validity`, approver may shorten |
| `revokedBySubject`, `revokedAt` | | Set on revoke |
| `createdAt`, `updatedAt` | time | |

### Rules

- On approve, the approver may remove accounts from `targetAccounts`, shorten
  `validity`, and narrow `messageRange`. Never add, extend, or widen.
- The approver's `sub` must differ from the requester's.
- Neither requester nor approver may resolve, via `users`, to any account in
  `targetAccounts`. Requester, approver, and every target are pairwise distinct on one
  request. An auditor's own account may be a target of a different request approved by
  someone else.
- Transitions: `pending` to `approved` or `denied`; `approved` to `revoked`. Anything
  else is a conflict. Nobody edits a request after submission.
- A grant is usable only while `status = approved` and `now < validUntil`, evaluated on
  every read. A periodic sweep marks stale rows `expired` for listing only.
- Only the requester reads under a grant. A second auditor on the same case submits
  their own request.
- Requesters see their own requests. Approvers see all requests at the site.

### Endpoints

All under `/v1/audit`, bearer-authenticated, JSON.

| Method and path | Role | Action |
|---|---|---|
| `POST /requests` | auditor | Create |
| `GET /requests`, `GET /requests/:id` | auditor (own), approver (all) | List, detail |
| `POST /requests/:id/approve`, `/deny`, `/revoke` | approver | Decide |
| `GET /grants/:id/rooms` | requester | Rooms in scope (§5) |
| `GET /grants/:id/rooms/:roomId/messages` | requester | Page a room |
| `GET /grants/:id/rooms/:roomId/threads/:parentId/messages` | requester | Page a thread |
| `GET /grants/:id/messages` | requester | Cross-room metadata query |
| `GET /grants/:id/messages/:messageId/verify` | requester | Verify against the bucket |
| `GET /ops` | approver | Operation log, filterable |
| `GET /ops/verify` | approver | Walk the hash chain |
| `GET /healthz`, `GET /readyz` | none | Probes |

Errors use `pkg/errcode` with a new `codes_audit.go`: `AuditGrantNotActive`,
`AuditNotRequester`, `AuditSelfApproval`, `AuditTargetIsParty`,
`AuditRoomOutOfScope`, `AuditLogUnavailable`, `AuditRemoteRoom`,
`AuditInvalidTransition`.

## 4. `archive-worker`

### Sources

Two durable pull consumers in one process, each guarded by `pkg/loopguard`:

- **MESSAGES-CANONICAL**, filter on the per-message event subjects: created, updated,
  deleted, pinned, unpinned, reacted. The canonical event carries the full plaintext
  message, so no Cassandra read and no chat-side decrypt is needed.
- **INBOX**, filter on `subject.InboxMemberEventSubjects(siteID)`: member added, member
  removed, joined-at refreshed, room renamed, on both lanes. Remote-site rooms a local
  user joins arrive on the external lane.

### Batching

Events are accumulated per pod and sealed into one segment when the first of three
bounds trips.

| Env | Default | Bound |
|---|---|---|
| `ARCHIVE_FILL_INTERVAL` | `10s` | Time since the first event in the batch |
| `ARCHIVE_BATCH_EVENTS` | `2000` | Event count |
| `ARCHIVE_BATCH_BYTES` | `8MiB` | Encrypted size |
| `ARCHIVE_PUT_TIMEOUT` | `10s` | One upload attempt |
| `ARCHIVE_BULK_TIMEOUT` | `10s` | One index attempt |
| `ARCHIVE_WRITE_ATTEMPTS` | `2` | Upload and index attempts before a NAK |
| `CONSUMER_ACK_WAIT` | `60s` | Must exceed fill + attempts × (put + bulk timeouts); 10 + 2 × 20 = 50s at defaults |
| `CONSUMER_MAX_ACK_PENDING` | `12000` | Must be ≥ replicas × 2 × `ARCHIVE_BATCH_EVENTS` |

At the projected 200 events a second the count bound trips every 10 seconds, giving
about 8,600 objects a day of roughly 4 MiB each. Startup refuses a configuration where
the worst-case batch time exceeds the ack wait, and warns when the ack-pending ceiling
is below the formula, using the same shape of check as `search-sync-worker`. Production
runs three replicas.

The consumer uses `stream.WithUnlimitedRedelivery`. Poison events that fail to parse
are logged, counted, and terminated.

### Segment format

Key: `{site}/{yyyy}/{mm}/{dd}/{hh}/{firstSeq}-{lastSeq}.seg`. Time-ordered by
construction; room is not in the key because the index answers by-room questions.

Body:

1. Plaintext header: format version, site, first and last stream sequence, record count.
2. One frame per event: 4-byte length prefix, then the archive record encrypted
   individually with the site's archive DEK and a fresh nonce.
3. Trailer: SHA-256 over everything before it.

An archive record is the canonical event plus stream name, stream sequence, and a
SHA-256 of its canonical JSON. The manifest of which messages a segment holds is inside
the encrypted frames, so the bucket exposes only site and time.

### Encryption

A new transit key `chat-audit-kek`. Each site has one archive DEK, generated by the
worker through `atrest.KeyWrapper.GenerateDataKey`, stored wrapped in
`archive_data_keys` in the audit MongoDB, and cached unwrapped in process. `chat-kek` is
never involved. The worker's Vault role has `datakey` and `encrypt` on the audit key;
the service's role has `decrypt` only. No chat-side role has any policy on the audit key.

### Write order and acknowledgement

1. Seal the segment and compute the trailer.
2. PUT the segment. Up to `ARCHIVE_WRITE_ATTEMPTS` attempts in process with a short
   jittered wait between them. A 200 means durable.
3. Bulk-index the batch, each document pointing at `segmentKey` and `frameOffset`.
   Retry the same way. This order means the index never points at a missing segment.
4. Ack every message whose bulk item succeeded. NAK any item the bulk rejected as
   retryable, through `jsretry` with the default jittered backoff.
5. If the PUT fails after all attempts, nothing has been written anywhere: NAK the whole
   batch through `jsretry` and drop it from memory. The batch is rebuilt from
   redelivery, never from memory.

A redelivered event lands in a later segment and is upserted again in the index. The
bucket gains a duplicate frame, the index keeps the last write, and a rebuild dedups by
stream sequence. Duplicates cost storage, never correctness.

### Behaviour when the bucket is unavailable

In plain terms: the pod tries the upload twice over about twenty seconds while
holding the batch in memory and its messages un-acked. If that fails it hands the whole
batch back to NATS with a jittered delay of about a minute, and drops it. It keeps
pulling, filling, failing, and handing back, with the delay growing to a cap of a few
minutes, so a long outage produces a slow trickle of redeliveries rather than a storm.
Nothing is acked, nothing is indexed, nothing is lost, and there is no delivery cap to
exhaust. Readiness stays green because NATS is healthy; the alert is on the
`archive_write_failures_total` and `archive_lag_seconds` metrics. When the bucket
returns, the next redelivered batch succeeds and the backlog drains with no operator
action. A pod that dies mid-upload leaves its messages un-acked until the 60-second ack
wait passes, after which NATS redelivers them to another pod.

### Object Lock

Retention comes from the bucket's default Object Lock rule in compliance mode. The
worker sets no per-object retention. At startup it reads the bucket's lock
configuration and refuses to run if lock is absent or the mode is not compliance. The
worker's bucket credential has `PutObject` only.

### Index shape

Two indices on the archive cluster, templates pushed by the worker's `bootstrap.go`
when `BOOTSTRAP_STREAMS=true`, following the repo convention. Daily indices under an
index lifecycle policy: hot, warm, cold, then searchable snapshots on the same S3
service. Retention is a lifecycle policy.

**`audit-messages-{site}-{yyyy.mm.dd}`**, one document per message ID:

| Field | ES type | Notes |
|---|---|---|
| `messageId`, `roomId`, `siteId`, `roomType` | keyword | |
| `senderAccount`, `senderId` | keyword | |
| `createdAt`, `editedAt`, `deletedAt` | date | |
| `eventType`, `lastSeq` | keyword, long | Last event applied |
| `threadParentId` | keyword | |
| `attachmentCount`, `attachmentTypes` | integer, keyword | |
| `pinned`, `reactionCount` | boolean, integer | |
| `segmentKey`, `frameOffset`, `contentHash` | keyword, long, keyword | Pointer to the sealed record |
| `encBody` | binary, not indexed | Ciphertext of the current body, cards, quoted parent |
| `versions` | nested | Prior `{editedAt, encBody, segmentKey, frameOffset}` entries |

**`audit-memberships-{site}`**, one document per room and account:

| Field | ES type | Notes |
|---|---|---|
| `roomId`, `account`, `siteId`, `roomType`, `roomName` | keyword | |
| `intervals` | nested `{joinedAt, leftAt}` | `leftAt` null while a member |
| `firstSeenSeq` | long | For "history begins at rollout" flagging |

`encBody` is kept for `ARCHIVE_INDEX_BODY_RETENTION` (default 180d), then stripped by
a lifecycle step. Older messages are read from the segment by ranged GET at the stored
offset.

## 5. Read path

Every read follows the same order: validate token and role, load the grant and check
status, expiry, site, and that the caller is its requester, check scope, append the
operation entry and confirm it is durable (§6), then query and decrypt. A failure after
the log entry writes a second entry with a `.failed` action.

**Room enumeration** queries the memberships index for the grant's targets and merges by
room. Each room carries the targets who are members and their membership intervals. A
room is in scope if at least one target's interval overlaps the grant's message range.
Rooms on other sites carry `remote: true` and are not readable here. Rooms whose
`firstSeenSeq` is the archive's first sequence are flagged so the auditor knows
visible history begins at rollout.

**Room messages** take grant, room, optional cursor, page size (default 100, max 500).
The query is clamped to the intersection of the grant's message range and the union of
the targets' intervals in that room, sorted by `createdAt` then `messageId`, paged with
search-after. The cursor does not carry the grant ID, so it cannot be replayed under a
different grant. Each hit's `encBody` and `versions` are decrypted in memory with the
cached archive DEK. Deleted messages return `deletedAt` and their last body. Thread
replies read the same way, filtered by parent.

**Cross-room query** takes grant plus metadata filters: sender in the target set, date
range, has attachment, deleted, edited, room type, thread only. It runs over every room
in scope and returns the same page shape. There is no body filter.

**Verify** takes grant and message ID, fetches the segment by key, decrypts the frame at
the offset, recomputes the record hash, and reports whether it matches `contentHash`.

Plaintext exists only inside the `audit-service` process and the auditor's browser.
Responses set `Cache-Control: no-store`.

## 6. Operation log

`audit_ops` in the audit MongoDB, one document per operation, never updated or deleted.

| Field | Meaning |
|---|---|
| `_id` | UUIDv7 hex |
| `seq` | Site-local monotonic sequence |
| `prevHash`, `hash` | SHA-256 of the previous entry, and of this entry's canonical JSON including `prevHash` |
| `actorSubject`, `actorAccount`, `actorRoles` | From the token |
| `action` | `login`, `request.create`, `request.approve`, `request.deny`, `request.revoke`, `rooms.list`, `messages.read`, `messages.query`, `message.verify`, `ops.list`, `ops.verify`, `auth.denied`, each with a `.failed` variant |
| `grantId`, `targetAccounts`, `roomId`, `messageIds` | Scope; `messageIds` only on reads, capped at the page size |
| `query` | Filter or cursor used, never message content |
| `requestId`, `clientIp`, `userAgent`, `timestamp` | Context |

One serialized appender per process holds the last hash in memory and reloads it on
start. Two replicas would fork the chain, so `audit-service` runs as one replica per
site. The insert uses majority write concern. After it is acknowledged the same entry
is uploaded to the sink at `{site}/{yyyy-mm-dd}/{seq}.json`. If the insert fails the
operation is refused with `AuditLogUnavailable`. If the sink upload fails the operation
proceeds, the entry is queued for retry, and `audit_ops_sink_backlog` turns readiness
red until it drains, so a sink outage is visible but does not stop an investigation.

`audit_chain_checkpoints` stores `{seq, hash, at}` every `AUDIT_CHECKPOINT_EVERY`
(default 1000) entries. `GET /ops/verify` walks from the latest checkpoint before a
given sequence and reports the first break.

## 7. Hardening and deployment

- **Namespace.** `archive-worker` and `audit-service` run in a dedicated Kubernetes
  namespace whose RBAC excludes the developer group: no exec, no secret reads, no
  port-forward. Network policy allows the audit MongoDB, the archive Elasticsearch, the
  buckets, Vault, and the chat MongoDB (for `users`) only from that namespace. The
  deployment pipeline for the namespace has a non-developer approver.
- **Vault.** Two roles bound to the two ServiceAccounts. Worker: `datakey`, `encrypt` on
  `chat-audit-kek`. Service: `decrypt` on `chat-audit-kek`. Neither has any policy on
  `chat-kek`.
- **Credentials.** Worker: NATS user limited to its two consumers, bucket `PutObject`
  only, Elasticsearch write role on the two indices, audit MongoDB user with write on
  `archive_data_keys` only. Service: bucket `GetObject` only, Elasticsearch read role,
  audit MongoDB user, sink `PutObject` only, chat MongoDB user with read on `users`
  only. All from secrets in the audit namespace.
- **No dev bypass.** `audit-service` has no dev mode. Local docker-compose runs an OIDC
  issuer container with group claims on test users.
- **Buckets.** Object Lock compliance mode with a default retention rule. Bucket admin
  credentials stay with ops. Versioning is on, as Object Lock requires.
- **Replicas.** `audit-service` one per site. `archive-worker` three, scaling with the
  ack-pending formula in §4.

## 8. Frontend

`audit-frontend` mirrors `admin-frontend`'s toolchain (Vite, React 19, Vitest) and
shell, sharing nothing at runtime. Pages: OIDC login redirect, my requests, new request,
request detail, approvals queue (approver), rooms for a grant, room messages with
thread expansion, cross-room query, verify message, operation log (approver). Message
rendering reuses `admin-frontend`'s shared display components where they exist. No
client-side persistence of message bodies beyond the current page. The search box is
absent, since there is no body filter.

## 9. Configuration

`audit-service`:

| Env | Default | Meaning |
|---|---|---|
| `PORT` | `8083` | |
| `SITE_ID` | required | |
| `OIDC_ISSUER_URL`, `OIDC_AUDIENCES` | required | Validator |
| `AUDIT_OIDC_GROUPS_CLAIM`, `AUDIT_AUDITOR_GROUP`, `AUDIT_APPROVER_GROUP` | §3 | |
| `AUDIT_MONGO_URI`, `AUDIT_MONGO_DB` | required, `audit` | Audit deployment |
| `MONGO_URI`, `MONGO_DB` | required, `chat` | Read-only, `users` only |
| `ARCHIVE_SEARCH_URL`, `ARCHIVE_SEARCH_USERNAME`, `ARCHIVE_SEARCH_PASSWORD` | required | Archive cluster |
| `ARCHIVE_BUCKET`, `ARCHIVE_S3_ENDPOINT`, credentials | required | Verification reads |
| `AUDIT_SINK_BUCKET`, `AUDIT_SINK_S3_ENDPOINT`, credentials | required | Ops log sink |
| `VAULT_*`, `ATREST_VAULT_TRANSIT_KEY=chat-audit-kek` | required | Decrypt |
| `AUDIT_MAX_TARGETS`, `AUDIT_DEFAULT_VALIDITY`, `AUDIT_MAX_VALIDITY` | 20, 7d, 30d | |
| `AUDIT_PAGE_SIZE_DEFAULT`, `AUDIT_PAGE_SIZE_MAX` | 100, 500 | |
| `AUDIT_CHECKPOINT_EVERY` | 1000 | |

`archive-worker`: the §4 batching table plus `NATS_URL`, `NATS_CREDS_FILE`, `SITE_ID`,
`AUDIT_MONGO_URI`, `ARCHIVE_SEARCH_*`, `ARCHIVE_BUCKET`, `ARCHIVE_S3_*`, `VAULT_*`,
`ARCHIVE_INDEX_BODY_RETENTION`, `CONSUMER_*`, `BOOTSTRAP_STREAMS`, `MAX_WORKERS`.

Both services follow the repo layout: `main.go`, `handler.go`, `routes.go` (service
only), `store.go` with mockgen, `store_mongo.go`, `store_search.go`, `store_bucket.go`,
`bootstrap.go` (worker only), `deploy/` with Dockerfile, docker-compose, and
azure-pipelines.

## 10. Observability

Through `pkg/obs`. Spans never carry message bodies or tokens.

Metrics: `audit_ops_total{action,outcome}`, `audit_ops_sink_backlog`,
`audit_chain_verify_total{result}`, `archive_events_total{source,outcome}`,
`archive_segments_total`, `archive_segment_bytes`, `archive_write_failures_total{store}`,
`archive_lag_seconds` (event timestamp to ack), `archive_redeliveries_total`,
`audit_decrypt_failures_total`.

The alerting relationship to state in the runbook: a steady non-zero
`archive_redeliveries_total` with no pod restarts means batches are exceeding the ack
wait and either the fill window or the ack wait needs adjusting.

## 11. Testing

Unit tests per handler with mocked stores, table-driven, covering the state machine,
every distinctness rule, expiry at the boundary, range clamping, remote rooms, the
log-before-read ordering, and a refused read when the log insert fails. Chain tests
with a tampered middle entry and a truncated tail. Segment format round-trip tests
with a corrupted trailer and a corrupted frame. Batching tests for each of the three
bounds and for the ack-wait startup check.

Integration tests with `pkg/testutil` containers: MongoDB, Elasticsearch, MinIO, NATS.
The worker test publishes canonical and member events, kills the bucket mid-batch, and
asserts nothing is acked, then restores it and asserts one segment per batch and
idempotent index state after a forced redelivery. The service test runs a request
through approval and reads a real archived room. OIDC is validated against a test
issuer with group claims.

Coverage floor 80%, target 90% on handlers and stores, per the repo rule.

## 12. Rollout

1. Prerequisite hardening (§2).
2. Buckets, archive Elasticsearch, audit MongoDB, Vault key and roles, namespace.
3. `archive-worker`. The archive starts accumulating; nothing reads it yet.
4. `audit-service` and `audit-frontend`.
5. IdP groups populated, first request exercised end to end by the security owner.

## 13. Follow-ups

- Export bundles on the same grant model.
- Plaintext full-text search, gated by its own sign-off and a reindex tool.
- Cross-site federated reads.
- Attachment blob retrieval through `media-service` under the same grant check.
- Backfill of pre-rollout history from Cassandra.
- Legal hold on live deletes.
- A write-ahead-log variant of the worker that acks per message, if the NATS team
  rules out batch acknowledgement. Not designed here.

## 14. Risks

- **A single-replica `audit-service`** is an availability trade for chain integrity. An
  outage blocks investigations, not the archive.
- **The archive index holds ciphertext for 180 days** and metadata forever. Metadata
  alone reveals who talked to whom and when; the cluster's isolation is the control.
- **The archive DEK is one key per site.** Compromise of the audit Vault role exposes
  the whole site's archive. Rotation follows the same re-wrap procedure as `chat-kek`.
- **Object counts.** About 3 million segments a year per site at the default batching.
  Within MinIO's comfortable range for objects of this size; the fill interval is the
  knob if that changes.
- **Vault unavailability** fails audit reads closed and stalls the worker, which NAKs
  rather than drops.
