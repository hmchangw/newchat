# archive-worker

Per-site JetStream worker that copies canonical message and membership events into
an Object Lock bucket and an append-only Elasticsearch index, for the audit access
plane. Deployed once per site (`SITE_ID`); production runs three replicas.

## What it archives

| Lane | Source | Writes |
|---|---|---|
| Messages | `MESSAGES-CANONICAL-{site}`: created, updated, deleted, pinned, unpinned, reacted | Sealed segments in the bucket, `audit-events-{site}` documents |
| Members | `INBOX-{site}`: member added/removed, joined-at refreshed, room renamed, both lanes | Sealed segments in the bucket, `audit-members-{site}` documents |
| Attachments | `MESSAGES-CANONICAL-{site}` created events with attachments | Chunk-encrypted blob at `{site}/blobs/{fileId}`, `audit-blobs-{site}` document |

Each archive record is encrypted with the site's archive DEK. The wrapped DEK is the
single document of `audit-keys-{site}`; the worker creates it on first start through
Vault and unwraps it on later starts. Documents are only ever created (`op_type:
create`); the worker never updates or deletes.

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
| `CONSUMER_ACK_WAIT` | `60s` | Must exceed fill + attempts x (put + bulk timeouts); 50s at defaults |
| `CONSUMER_MAX_ACK_PENDING` | `12000` | Should be >= replicas x 2 x `ARCHIVE_BATCH_EVENTS` (`ARCHIVE_REPLICAS` feeds this check) |

Startup refuses a configuration where the worst-case batch time reaches the ack wait
and warns when the ack-pending ceiling is below the formula.

## Outage behaviour

If the bucket is unavailable the pod tries the upload twice over about twenty seconds,
holding the batch in memory with its messages un-acked. If that fails it hands the whole
batch back to NATS with a jittered delay and drops it, then keeps pulling, filling and
handing back with the delay growing to a cap of a few minutes. Nothing is acked, nothing
is indexed, nothing is lost, and there is no delivery cap to exhaust. Readiness stays
green because NATS is healthy; alert on `archive_write_failures_total` and
`archive_lag_seconds`. When the bucket returns the backlog drains with no operator
action. A pod that dies mid-upload leaves its messages un-acked until the ack wait
passes, after which another pod receives them.

## Object Lock precondition

Retention comes from the bucket's default Object Lock rule in compliance mode; the
worker sets no per-object retention. With `ARCHIVE_REQUIRE_OBJECT_LOCK=true` (the
default) it refuses to start when the bucket has no lock configuration or the mode is
not compliance. The worker's bucket credential needs `PutObject` only.

## Vault

Transit key `chat-audit-kek` (`ATREST_VAULT_TRANSIT_KEY`), shared by every site's
worker and the central audit service. The worker's role needs `datakey` and `encrypt`
on it and uses Vault only to create or unwrap its site's DEK. `chat-kek` is never
involved.

## Streams and indexes

`BOOTSTRAP_STREAMS=true` (dev) creates `MESSAGES-CANONICAL-{site}`; otherwise the
worker verifies it exists. `INBOX-{site}` belongs to `inbox-worker` and is only ever
verified. On every start the worker creates the `audit-archive` lifecycle policy if it
is absent (operator edits survive) and upserts the four index templates.
