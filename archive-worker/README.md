# archive-worker

Per-site JetStream worker that copies canonical message and membership events into
an Object Lock bucket and an append-only Elasticsearch index, for the audit access
plane. Deployed once per site (`SITE_ID`); production runs three replicas.

## What it archives

| Lane | Source | Writes |
|---|---|---|
| Messages | `MESSAGES-CANONICAL-{site}`: created, updated, deleted, pinned, unpinned, reacted | Sealed segments in the bucket, `audit-events-{site}` documents |
| Members | `INBOX-{site}`: member added/removed, room renamed, both lanes (joined-at refreshed is delivered but acked without being archived) | Sealed segments in the bucket, `audit-members-{site}` documents |
| Attachments | `MESSAGES-CANONICAL-{site}` created events with attachments | Chunk-encrypted blob at `{site}/blobs/{fileId}`, `audit-blobs-{site}` document |

Each archive record is encrypted with the site's archive DEK. Documents are only ever
created (`op_type: create`); the worker never updates or deletes. The daily
`audit-events-{site}-{yyyy.mm.dd}` and `audit-members-…` index a document lands in is
named for the day it is archived, not the day of the event: a late event, or one a NAK
redelivers after midnight, is rebuilt into the current day's index, which is still
writable (each daily index turns read-only a day after creation). `eventAt` stays the
event's own time (or, when the event has none, the stream's store time, so a
redelivery rebuilds the identical record). A redelivery that crosses midnight
therefore leaves one document per day for the same event; readers dedup by `seq`.

## Key management

The wrapped DEK is the single document of `audit-keys-{site}`, and the bucket holds an
escrow copy beside the data it protects: `{site}/keys/current.json` and
`{site}/keys/{keyId}.json`, the same KeyDoc JSON. The index copy is the fast path; the
bucket copy survives the loss of the index. `keyId` is the first 16 hex characters of
SHA-256 over the wrapped DEK, and every segment and blob header carries it, so the
escrow object for any archived object can be found by name. On start the worker:

- finds the index document: unwraps it, and writes both bucket objects if
  `current.json` is missing;
- finds no index document but finds `current.json` in the bucket: refuses to start.
  Minting a new key would orphan everything sealed under the escrowed one. Restore the
  index document from `current.json` (same JSON, id `current`), then start again;
- finds neither: mints a key through Vault, writes both bucket objects, then creates
  the index document. A replica that loses the create race re-reads the winner and
  writes its escrow again, so the newest `current.json` version always names the key
  the index holds.

## Batching

Events are sealed into one segment when the first bound trips.

| Env | Default | Bound |
|---|---|---|
| `ARCHIVE_FILL_INTERVAL` | `10s` | Time since the first event in the batch |
| `ARCHIVE_BATCH_EVENTS` | `2000` | Event count |
| `ARCHIVE_BATCH_BYTES` | `8388608` | Encrypted size |
| `ARCHIVE_PUT_TIMEOUT` | `10s` | One upload attempt |
| `ARCHIVE_BULK_TIMEOUT` | `10s` | One index attempt |
| `ARCHIVE_WRITE_ATTEMPTS` | `2` | Upload and index attempts before a NAK |
| `CONSUMER_ACK_WAIT` | none, must be set | Must exceed fill + 2 x (attempts x (put + bulk) + bulk + retry waits); 113s at defaults |
| `CONSUMER_MAX_ACK_PENDING` | none, must be set | Should be >= replicas x 2 x `ARCHIVE_BATCH_EVENTS` (`ARCHIVE_REPLICAS` feeds this check) |

`CONSUMER_ACK_WAIT` and `CONSUMER_MAX_ACK_PENDING` have no worker-specific defaults: the
shared `CONSUMER_*` defaults (30s and 1000) are below what this worker needs, so a
deployment that leaves them unset fails startup validation on the ack wait and warns on
the ack-pending ceiling. Production sets `CONSUMER_ACK_WAIT=120s` and
`CONSUMER_MAX_ACK_PENDING=12000` with `ARCHIVE_REPLICAS=3` (3 x 2 x 2000). The local
compose file sets `120s`, `4000` and `ARCHIVE_REPLICAS=1`.

Startup refuses a configuration where the worst-case batch time reaches the ack wait,
and prints that time. A message can wait one fill interval, then two flushes back to
back (the one in flight when its batch sealed, then its own). One flush is every
upload and index attempt, one bulk timeout of conflict read-back (below), and the
pauses between attempts (0.5s, 1s, ..., summed over the attempt count):
`ARCHIVE_FILL_INTERVAL + 2 x (ARCHIVE_WRITE_ATTEMPTS x (ARCHIVE_PUT_TIMEOUT +
ARCHIVE_BULK_TIMEOUT) + ARCHIVE_BULK_TIMEOUT + retry waits)`, 10 + 2 x (40 + 10 + 1.5)
= 113s at the defaults. It warns when the ack-pending ceiling is below the formula.
The timeouts and the fill interval must be positive, `ARCHIVE_REPLICAS` at least 1,
`ARCHIVE_BLOB_MAX_BYTES` positive, and `SITE_ID` lowercase letters, digits, `-` or `_`,
since it names Elasticsearch indices.

## Create conflicts

A document id is `{site}-{streamSeq}` (members add the account position), so a
redelivered event's create conflicts with the document an earlier delivery wrote. A
conflict is not taken on trust: the worker reads the existing document back and
compares its `contentHash` with the record it just built. Equal means the event is
already archived and the message is acked. Different means the id belongs to another
record; the message is terminated, logged at error with its `seq`, document id and
index, and counted as `archive_events_total{outcome="conflict"}`. A failed read-back is
retried through a NAK. All read-backs of one flush share one `ARCHIVE_BULK_TIMEOUT`.

## Outage behaviour

If the bucket is unavailable the pod tries the upload twice over about twenty seconds,
holding the batch in memory with its messages un-acked. If that fails it hands the whole
batch back to NATS with a jittered delay and drops it, then keeps pulling, filling and
handing back with the delay growing to a cap of a few minutes. Nothing is acked, nothing
is indexed, nothing is lost, and there is no delivery cap to exhaust. Readiness stays
green because NATS is healthy; alert on `archive_write_failures_total` and
`archive_redeliveries_total`. When the bucket returns the backlog drains with no operator
action. A pod that dies mid-upload leaves its messages un-acked until the ack wait
passes, after which another pod receives them.

## Object Lock precondition

Retention comes from the bucket's default Object Lock rule in compliance mode; the
worker sets no per-object retention. With `ARCHIVE_REQUIRE_OBJECT_LOCK=true` (the
default) it refuses to start when the bucket has no lock configuration or the mode is
not compliance. The worker's bucket credential needs `PutObject` on the bucket, and
`GetObject` (which also answers the existence check) on `{site}/keys/*` for the key
escrow.

## Vault

One transit key per site, `chat-audit-kek-{site}` (`ATREST_VAULT_TRANSIT_KEY`). The
worker's role holds `datakey/wrapped` and `decrypt` on its own site's key only, and no
`encrypt`: it mints its DEK through `transit/datakey/wrapped` and unwraps it with
`decrypt`, and uses Vault for nothing else. The central audit service holds `decrypt`
on every site's key. One shared key would let any site's worker unwrap every site's
DEK, since each site's wrapped DEK is readable through the archive index and the
bucket escrow; a key per site confines a stolen worker role to its own site.
`chat-kek` is never involved.

## NATS permissions

Run the worker on its own NATS user, never the all-subjects backend credential. It
needs no `chat.user.>` rights (it never calls history). Production set, per site
(`S1` = `MESSAGES-CANONICAL-{site}`, `S2` = `INBOX-{site}`); a JetStream API subject
token cannot carry a partial wildcard, so spell the stream names out or use `*`:

| Direction | Subject | Used for |
|---|---|---|
| publish | `$JS.API.STREAM.INFO.S1`, `$JS.API.STREAM.INFO.S2` | startup stream verification |
| publish | `$JS.API.CONSUMER.CREATE.S1.archive-worker-events` | events durable |
| publish | `$JS.API.CONSUMER.CREATE.S1.archive-worker-blobs` | attachment durable (only with `ARCHIVE_BLOBS_ENABLED`) |
| publish | `$JS.API.CONSUMER.CREATE.S2.archive-worker-members` | members durable |
| publish | `$JS.API.CONSUMER.MSG.NEXT.S1.archive-worker-events`, `...S1.archive-worker-blobs`, `...S2.archive-worker-members` | pull requests |
| publish | `$JS.ACK.S1.archive-worker-events.>`, `$JS.ACK.S1.archive-worker-blobs.>`, `$JS.ACK.S2.archive-worker-members.>` | ack, nak, in-progress and term of delivered messages; the server addresses acks as `$JS.ACK.<stream>.<consumer>.<tail>` |
| subscribe | `_INBOX.>` | API replies and pulled messages |

The durables are created with a filter list, so the create subject has no filter
suffix. Add `$JS.API.STREAM.CREATE.S1` and `$JS.API.STREAM.UPDATE.S1` only where
`BOOTSTRAP_STREAMS=true` (local dev); production streams belong to ops. The worker
never deletes a consumer or a stream. `docker-local/setup.sh` generates this user
as `archive-worker.creds` for both local-dev sites.

## Streams and indexes

`BOOTSTRAP_STREAMS=true` (dev) creates `MESSAGES-CANONICAL-{site}`; otherwise the
worker verifies it exists. `INBOX-{site}` belongs to `inbox-worker` and is only ever
verified. On every start the worker creates the `audit-archive` lifecycle policy if it
is absent (operator edits survive) and upserts the four index templates.

## Elasticsearch write role (ops step)

The worker only ever creates documents, so its steady-state identity should not be
able to overwrite or delete archived ones. Provision a role such as this through the
security API and bind the worker's `ARCHIVE_SEARCH_USERNAME` user to it:

```
PUT /_security/role/audit-writer
{"indices":[{"names":["audit-*"],"privileges":["create_doc","read","auto_configure"]}]}
```

`create_doc` permits `op_type=create` (every bulk action the worker sends) and refuses
index-over-existing and delete. `read` is for the documents the worker looks up while
running: a create conflict is read back to compare its `contentHash`, and an
attachment's `audit-blobs` document is looked up before any download; it grants no
write. Startup also upserts index templates and the lifecycle policy and reads the key
document, which need `manage_index_templates` and `manage_ilm`; grant those to a
separate bootstrap identity or add them to the role if one identity is used. The integration suite exercises the `create_doc`
refusal only when the test cluster has security enabled, which it does not today, so it
skips that case.

## Metrics

Recorded through the OpenTelemetry meter `archive-worker`: `archive_segments_total`,
`archive_segment_bytes` (histogram), `archive_events_total{source,outcome}` (`source`
is the lane, `events` or `members`; `outcome` is `archived`, `failed`, `nak`,
`conflict`, `poison` or `skip`), `archive_write_failures_total{store}` (`bucket` or
`index`), `archive_blobs_total{outcome}` (`archived`, `exists`, `skipped_size`,
`skipped_missing`, `skipped_legacy`, `skipped_host`), `archive_blob_bytes_total`,
`archive_redeliveries_total` (a message delivered more than once),
`archive_lag_seconds{source}` (histogram, event time to ack) and
`archive_blob_lag_seconds` (histogram, created event time to the ack of its
attachments). Every label is a closed set; the contract is in
`docs/specs/o11y/nats-metrics-contract.md`.

## Operations / shutdown

On SIGTERM the worker stops its lanes first (the loop guards are told the stop is
deliberate, each lane is told to drain, and shutdown waits for every lane to return),
and only then drains NATS, closes Vault, the health server and observability. The
whole sequence has a 25s budget, which must stay below the pod's
`terminationGracePeriodSeconds` (30s). Anything unfinished when the budget runs out is
not acked, so NATS redelivers it after the ack wait; nothing is lost, it is only
archived later.

Two things can outlast the 25s budget:

- **A segment flush.** A lane drains by flushing what it holds after any flush already
  in flight, so up to two flushes run back to back. One flush is bounded by
  `ARCHIVE_WRITE_ATTEMPTS x (ARCHIVE_PUT_TIMEOUT + ARCHIVE_BULK_TIMEOUT)` plus the short
  pause between attempts (0.5s, then 1s, ...), plus one bulk timeout of conflict
  read-back. At the defaults that is about 51s per flush in the worst case, an
  unreachable bucket and index, and well under a second when both are healthy. To make the worst case fit, lower `ARCHIVE_PUT_TIMEOUT`,
  `ARCHIVE_BULK_TIMEOUT` or `ARCHIVE_WRITE_ATTEMPTS`; `CONSUMER_ACK_WAIT` must keep
  exceeding the figure in the batching table.
- **A blob transfer.** The blob lane waits for in-flight attachments, and a Drive
  download can run up to the Drive client's 5 minute request timeout because the
  download call takes no context and so cannot be cancelled early. A pod stopped
  mid-transfer is force-exited at 25s and the message is redelivered after
  `ARCHIVE_BLOB_ACK_WAIT`. `ARCHIVE_BLOB_WORKERS` bounds how many transfers can be in
  flight, and `ARCHIVE_BLOB_MAX_BYTES` bounds how long one can run.

A fetch error that is neither benign nor known-terminal (a deleted durable can surface
to a pull as a generic error such as "no responders") makes the lane
ask the server for the consumer before it pauses. When the answer is "consumer not
found" (or "stream not found") the lane drains, its loop guard fails readiness with
that cause and raises SIGTERM, so the supervisor replaces the pod and startup recreates
the durable; any other answer is retried after the pause.

The events and members consumers redeliver without a cap (`MaxDeliver` unlimited); the
blob consumer uses the shared outage retry budget on top of `CONSUMER_MAX_DELIVER`, and
logs and terminates a message on its last delivery. `ARCHIVE_BLOB_ACK_WAIT` is the ack
wait of the blob consumer, and the heartbeat that holds a slow transfer open is bounded
by `CONSUMER_HEARTBEAT_MAX`.

`VAULT_ADDR` is required; startup fails without it.

Turning `ARCHIVE_BLOBS_ENABLED` off after it was on leaves the `archive-worker-blobs`
durable on the server, collecting pending messages; operators should delete it.

## Attachment links

`upload-service` writes only the relative form
`api/v1/file/rooms/{room}/file/{file}?drive_host={host}` for new attachments. Legacy
`file-upload` links come from rows written by the old stack; the Cassandra attachment
decoder rewrites those, absolute URLs included, to the relative
`api/v1/file-upload/{id}/{name}` form before the worker sees them, and the blob lane
records them as skipped (`legacy`) instead of archiving them.

Each attachment is archived once per file id: the lane looks up the `audit-blobs`
document first and, when it exists, makes no Drive call and no upload. A skipped
attachment still gets a document with `skipped` set: `size` (over
`ARCHIVE_BLOB_MAX_BYTES`), `missing` (no longer at the source), `legacy` (above) or
`host` (the link's `drive_host` is not one of the configured Drive base URLs; logged
once at warn with the host and attachment id). The document's `plainDigest` is the
keyed `hmac-sha256:` digest of the plaintext.
