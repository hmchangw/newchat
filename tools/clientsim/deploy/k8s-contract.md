# clientsim on Kubernetes — the chart contract

The Helm chart is ops-owned and lives outside this repo. This file is the
contract it has to satisfy: the objects, the env each one needs, and the
reasons behind the parts that look optional but are not.

## The chain

```text
  ┌──────────────────────────────┐
  │ Job (PreSync / pre-install)  │   loadgen pool-export --run-id=<runId>
  │ image: loadgen               │
  └───────────┬──────────────────┘
              │  MongoDB: accounts with a channel subscription on this site
              ▼
      object store (MinIO)
      <prefix>/<siteId>/<runId>/pool.json.gz         ← the fleet reads this
      <prefix>/<siteId>/<runId>/pool-manifest.json   ← how it was produced
              │
              ▼
  ┌──────────────────────────────┐
  │ StatefulSet: clientsim       │   every pod fetches the SAME object
  │ replicas = SHARD_COUNT       │   at startup — no initContainer
  └──────────────────────────────┘
```

Run the Job before the StatefulSet.

**A run ID names one immutable population.** Re-running `pool-export` with the
same `--run-id` succeeds only while the population is unchanged — a retried
Job is safe. If the accounts have changed, it **fails** and tells you to use a
new run ID, because overwriting a pool a fleet may already be reading is the
one thing that breaks the invariant below: pods that started before and pods
that restart after would slice different arrays.

`--run-id` becomes a path segment, so it must match
`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$` and be neither `.` nor `..`.

## Why not each pod querying MongoDB itself

`shardSlice` hands every pod the same array and slices it by ordinal. Two
pods whose queries returned slightly different sets — staging churns while
the run is live — would claim overlapping or disjoint ranges: accounts
connected twice or not at all, with every pod still reporting ready. One
querier, one snapshot, one object is what makes the sharding safe.

## Why an object store rather than a PVC or a ConfigMap

| | why not |
|---|---|
| shared PVC | needs `ReadWriteMany`; SAN-backed block storage cannot give that to a multi-replica StatefulSet |
| ConfigMap | caps at 1 MiB — 30k real account names are 1.14 MiB uncompressed. In a GitOps repo it also commits a list of real identities to git history, permanently |

## Object 1 — the export Job

Image: **loadgen**. Command: `loadgen pool-export --run-id=<runId>`.

| flag | default | meaning |
|---|---|---|
| `--run-id` | required | the run the artifact is filed under |
| `--limit` | `0` | cap the exported accounts (`0` = the whole population) |
| `--out` | — | also write the artifact to a local path |

Env — all of these already exist in the loadgen half of the chart except the
`POOL_S3_*` group:

| env | required | meaning |
|---|---|---|
| `MONGO_URI` | ✅ | operational MongoDB; a read-only user is enough |
| `MONGO_DB` | | database (default `chat`) |
| `MONGO_USERNAME` / `MONGO_PASSWORD` | | when the URI does not carry them |
| `SITE_ID` | ✅ | the site to export; must match clientsim's |
| `NATS_URL` | ✅ | loadgen requires it at startup even though `pool-export` does not use it |
| `POOL_S3_ENDPOINT` | ✅ | object store host:port |
| `POOL_S3_ACCESS_KEY` / `POOL_S3_SECRET_KEY` | ✅ | credentials |
| `POOL_S3_BUCKET` | ✅ | bucket |
| `POOL_S3_PREFIX` | | key prefix (default `clientsim`) |
| `POOL_S3_USE_SSL` | | default `true` |

### What it exports

```text
subscriptions: {siteId, roomType: "channel", open: {$ne: false},
                origin: {$ne: "teams"}}
             → distinct u.account, sorted ascending
```

Mirrors user-service's own `subscription.list` match, narrowed to `channel`:
clientsim opens room lanes only for channels, because DM traffic arrives on
the user lane instead. An account with no channel subscription would connect
cleanly and then measure nothing.

The **`origin` exclusion matters for the same reason**. user-service hides
Teams-origin rooms from `subscription.list` unless `SHOW_TEAMS_ROOM` (default
`false`) or the account is allowlisted. Without it, an account whose only
channel subscriptions are Teams rooms is exported, walks to an *empty* plan,
and reports ready while subscribing to nothing. It is applied unconditionally
rather than mirroring the per-account allowlist: for a load pool,
under-selecting costs a few connections and over-selecting costs silent
measurement loss.

The sort is load-bearing, not cosmetic — see the sharding note above.

### Index

The query starts on `siteId + roomType + open`, and the subscriptions
collection carries no index for that shape (`u.account + roomType` and
`name + roomType` today). Note that an index would help here by being
**covering**, not by being selective: on a single-site deployment the query
legitimately needs most of the channel subscriptions, so the win is skipping
the document fetches, not the scan.

A covering index would be `{siteId: 1, roomType: 1, open: 1, "u.account": 1}`.
It is **not** created by this tool — a load tool must not mutate the schema of
the database under test, and the write amplification on a hot collection is
the subscriptions owner's call. Until it exists, expect the Job to do a
collection scan; measure with `explain("executionStats")` against a
production-sized collection before deciding.

An empty result **fails the Job**. A fleet started against nobody reports a
healthy zero, and this is the last place that can say why.

### The manifest

`pool-manifest.json` records `runId`, `siteId`, `configDigest`, the account
count, the limit, the query, and the export time. The artifact says *who*
connected; the manifest says how that set was chosen, which is what makes a
run reproducible months later rather than merely identifiable.

`configDigest` fingerprints the **population** (a Mongo export has no preset
or RNG seed to hash), so two runs against the same accounts share a digest
and one changed account is visible.

## Object 2 — the clientsim StatefulSet

Image: **clientsim**. Replicas = `CLIENTSIM_SHARD_COUNT`.

| env | required | meaning |
|---|---|---|
| `CLIENTSIM_POOL_URL` | ✅ | `s3://<bucket>/<prefix>/<siteId>/<runId>/pool.json.gz` |
| `POOL_S3_ENDPOINT` / `_ACCESS_KEY` / `_SECRET_KEY` | ✅ | same store the Job wrote to. Read-only credentials are enough |
| `POOL_S3_BUCKET` | | the URL's bucket wins; set it only if the URL omits one |
| `CLIENTSIM_NATS_WS_URL` | ✅ | `wss://…` |
| `CLIENTSIM_AUTH_URL` | ✅ | `http://dev-auth-service.<ns>.svc.cluster.local:8080` |
| `CLIENTSIM_SITE_ID` | ✅ | must match the artifact, or startup fails |
| `CLIENTSIM_SHARD_INDEX` | ✅ | pod ordinal, via the downward API |
| `CLIENTSIM_SHARD_COUNT` | ✅ | = replicas |

Everything else has a default — see the Configuration table in the README.

`CLIENTSIM_POOL_FILE` and `CLIENTSIM_POOL_URL` are mutually exclusive: set
exactly one. A **partly** set `POOL_S3_*` group fails startup even on the file
path, because it means someone meant to use the object store and mistyped.

### Pod ordinal → shard index

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef: { fieldPath: metadata.name }
  # CLIENTSIM_SHARD_INDEX = the ordinal suffix of POD_NAME
```

The downward API exposes the pod name, not the ordinal, so the chart has to
split the suffix (an initContainer, a shell prefix in `command`, or whatever
the chart already does for other StatefulSets).

## Sizing

- **Room-queue memory** is the one that bites: worst case retained per pod is
  `CLIENTSIM_TARGET_CONNS × CLIENTSIM_SUB_PENDING_MSGS × event size`
  (10k × 512 × ~2 KiB ≈ 10 GB if every pump stalled at once).
  `CLIENTSIM_SUB_PENDING_BYTES` does **not** bound this lane.
- **`ulimit -n`** well above `conns × (2 + rooms)`.
- One `(srcIP → dstIP:port)` tuple caps at ~60k ephemeral ports — beyond that
  add replicas (each pod has its own IP) or NATS endpoint IPs.

## Operational rules

- **Never change `CLIENTSIM_SHARD_COUNT` under a running fleet.** Ownership is
  `index mod count` with no fencing, so a rolling rescale runs both
  partitionings at once and double-connects the overlap. Scale to zero first.
- **The pool bucket holds real account names.** Give it its own bucket — or at
  least its own prefix and credentials — rather than sharing one with
  application uploads, and set a lifecycle rule: soak runs are frequent and
  nothing prunes old artifacts on its own.
- **The dev-mode auth-service mints a JWT for any account it is asked for.**
  ClusterIP, no ingress, NetworkPolicy limited to the clientsim and loadgen
  pods, test/staging only. A namespace is not a network boundary by default,
  and `kubectl port-forward` bypasses network controls entirely.
