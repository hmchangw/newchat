# Cassandra Run A Helm release

This Chart packages the Cassandra-focused Run A harness for an Argo CD
release pipeline. Production operation is fully declarative: operators change
the run values, publish a release, and let Argo CD reconcile the selected
phase. Direct production `kubectl apply`, image patching, and Helm hooks are
not part of the workflow.

The Chart deploys only loadgen. NATS, MongoDB, Cassandra,
message-gatekeeper, message-worker, and history-service must already exist.

## Resource model

A run advances through four desired states:

```text
seed Job -> soak Deployment -> stopped -> teardown Job
```

- `phase=seed` renders one bounded Job. It borrows existing Mongo users
  read-only, creates run-owned room/subscription topology, seeds room
  transport keys, and writes the ownership manifest.
- `phase=soak` renders one single-replica Deployment. It runs continuously
  through the real service path until the release changes phase. Its
  operation ledger uses a run-specific PVC and resumes unresolved Cassandra
  message reconciliation after pod replacement.
- `phase=stopped` renders no workload controller. Argo CD removes the
  Deployment and Kubernetes sends SIGTERM, allowing loadgen to drain
  in-flight work and mark the run stopped.
- `phase=teardown` renders one bounded Job. It deletes only run-owned Mongo
  topology and performs Cassandra cleanup only under the guarded,
  dedicated-keyspace option.

The Seed and Teardown Jobs default to a two-hour
`activeDeadlineSeconds`, below the platform's one-day Job limit. The Soak
Deployment has no duration or Kubernetes deadline.

The phase resources are ordinary Argo CD managed resources, not
`PreSync`/`Sync`/`PostSync` hooks. In particular, teardown can never run
automatically after soak completion.

The ledger PVC is annotated with `helm.sh/resource-policy: keep` and
`argocd.argoproj.io/sync-options: Prune=false`. It intentionally survives
`phase=stopped`, `phase=teardown`, and release removal until an operator has
retained the evidence and explicitly deletes the claim.

Failure observation is continuous and never injects or schedules faults. The
versioned WAL remains on the retained ledger PVC and resumes unresolved
admission/history reconciliation after replacement. Optional exact-recipient
observation is enabled independently with `recipientObserver.enabled=true`;
its subscriptions are flushed before they are marked healthy. WAL, observer,
or queue failures remain visible while production-shaped traffic continues.
Dashboard query time defines the fault window and evaluates evidence validity,
impact, and correctness as independent dimensions.

## Upgrading an existing release

**Allocate a new `runId`.** The observer contract and the scenario label changed,
so a WAL written by an earlier image is rejected on replay. The pod does not
crash: it falls back to an in-memory ledger and marks the whole run's evidence
invalid (`loadgen_failure_invalidations_total{reason="wal"}`), which is easy to
miss. The WAL file is named after the run ID, so a new ID starts a clean journal.

**Check the run ID format.** `SOAK_RUN_ID` is now validated against
`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`. A non-conforming ID exits with status 2 at
startup instead of running with a degraded journal.

**Set `soak.environment`.** Allowed values are `local`, `test`, `staging`, and
`production`. It defaults to `local`, which is valid but mislabels
`loadgen_run_info` on any shared dashboard.

**Update dashboards before the first run.** Three series changed:

| Before | After |
|---|---|
| `loadgen_soak_saturation_total` | `loadgen_soak_lane_saturation_total` and `loadgen_soak_global_saturation_total` |
| `loadgen_member_room_size{room_id}` | `loadgen_member_room_size{size_bucket}` |
| `scenario="cassandra_soak"` | `scenario="message_soak"` |

The retained PVC needs no resizing. At `sendRate=100` and a ten-minute
reconcile deadline the WAL holds roughly 68 MB, peaking near 220 MB while a
compaction keeps the current, temporary, and backup files together.

## Required release inputs

Create one values file per run in the deployment configuration repository.
Do not commit credentials.

```yaml
phase: seed
runId: cassandra-run-a-20260725-01
siteId: site-staging

image:
  repository: registry.example/newchat-loadgen
  digest: sha256:<immutable-digest>

existingSecret: cassandra-soak-loadgen-secrets

mongo:
  database: chat

cassandra:
  keyspace: chat_soak_20260725
  cleanup: none
  confirmKeyspace: chat_soak_20260725
  messageBucketHours: 72

ledger:
  enabled: true
  existingClaim: ""
  storageClassName: ""
  accessMode: ReadWriteOnce
  size: 20Gi
  mountPath: /var/lib/loadgen/ledger
  capacity: "200000"
  reconcileDeadline: 10m
  reconcileRetryInterval: 1s

soak:
  environment: staging
```

### Staging Mongo failure campaign settings

For the planned two-minute Mongo outage, add the following block to the same
per-run values file. If the release system uses ordered values files instead,
apply this as the last overlay so these campaign values win:

```yaml
soak:
  heartbeatInterval: 30s
  heartbeatStaleAfter: 30m
  roomReadRate: "30"

ledger:
  reconcileDeadline: 10m
  roomReconcileReadShare: 0.5

recipientObserver:
  enabled: true
```

This is the complete Mongo-campaign values block; this change does not add
another operator setting. The five-second heartbeat attempt timeout and the
five-second room-state Mongo read budget are fixed safety bounds in loadgen.

| Chart value | Environment variable | Staging value | Why |
|---|---|---:|---|
| `soak.heartbeatInterval` | `SOAK_HEARTBEAT_INTERVAL` | `30s` | Normal manifest lease-renewal cadence. Keep it explicit so the lease calculation is reviewable. |
| `soak.heartbeatStaleAfter` | `SOAK_HEARTBEAT_STALE_AFTER` | `30m` | Destructive-operation lease, sized well beyond the planned outage and recovery. |
| `soak.roomReadRate` | `SOAK_ROOM_READ_RATE` | `30` | Leaves room-state catch-up capacity after Mongo recovers. |
| `ledger.reconcileDeadline` | `SOAK_RECONCILE_DEADLINE` | `10m` | Pending room operations must recover before this deadline; do not raise it to manufacture capacity. |
| `ledger.roomReconcileReadShare` | `SOAK_ROOM_RECONCILE_READ_SHARE` | `0.5` | Reserves half of the room-read lane for reconciliation. |
| `recipientObserver.enabled` | `SOAK_RECIPIENT_OBSERVER_ENABLED` | `true` | Retains exact-recipient evidence for the campaign. |

Loadgen validates this lease relationship at startup:

```text
heartbeatStaleAfter >= 2 * heartbeatInterval + 5s
```

The extra interval is the shutdown margin; the five seconds cover a heartbeat
attempt that begins at the normal cadence. With the values above, an
unrecoverable heartbeat outage stops dispatch around 29 minutes 30 seconds
after the last persisted heartbeat, before seed or teardown may treat the run
as abandoned. The room recovery sizing assumes the outage is much shorter than
the ten-minute reconciliation deadline. This relationship is checked for the
`soak` workload; teardown has no heartbeat loop and validates its emergency
lease override separately. See
[`docs/load-testing/failure/mongodb.md`](../../../../docs/load-testing/failure/mongodb.md)
for the demand and recovery math and
[`docs/load-testing/loadgen/dashboard-contract.md`](../../../../docs/load-testing/loadgen/dashboard-contract.md)
for the evidence-validity queries.

The stop boundary is computed from the timestamp loadgen persisted and its own
clock, while seed and teardown judge staleness against theirs, so the margin
also has to cover the clock difference between those nodes. One heartbeat
interval of margin therefore assumes they agree to within `30s`; ordinary NTP
keeps them orders of magnitude closer. A server-side heartbeat timestamp would
remove that assumption rather than size around it, and is listed as follow-up
work on the pull request.

Staging releases must use an immutable digest. A mutable tag is accepted only
when `image.allowMutableTag=true`, which is reserved for local kind
validation.

The referenced Secret is operator-owned and must provide:

| Key | Required by | Purpose |
|---|---|---|
| `nats-url` | all phases | NATS endpoint |
| `backend.creds` | all phases | Mounted NATS credentials |
| `mongo-uri` | all phases | MongoDB endpoint |
| `mongo-username` | optional | MongoDB authentication |
| `mongo-password` | optional | MongoDB authentication |
| `cassandra-hosts` | truncate teardown only | Direct cleanup connection |
| `cassandra-username` | optional truncate auth | Cassandra authentication |
| `cassandra-password` | optional truncate auth | Cassandra authentication |

## Operational gates

Implementation completion does not authorize a staging run. Before releasing
the seed phase, record approval for:

1. Namespace, site ID, NATS account, Mongo database, and unique run ID.
2. Borrowed-user filter/count, room count, and MongoDB write budget.
3. A dedicated Cassandra keyspace if cleanup will use `truncate`.
4. Matching `MESSAGE_BUCKET_HOURS` on loadgen, message-worker, and
   history-service.
5. At-rest encryption enabled with working Vault/KMS dependencies.
6. Cassandra free disk for projected growth plus safety margin.
7. Prometheus discovery and access to Cassandra/service dashboards.
8. Evidence-retention window and the named teardown approver.
9. Acceptance that NATS, Elasticsearch, and Valkey side effects are not
   automatically removed.

## Release sequence

### 1. Seed

Publish the run values with:

```yaml
phase: seed
teardown:
  approved: false
```

Wait for the Argo CD Job health to become complete. Before promotion, inspect
the `loadgen_soak_runs` manifest, owned room/subscription counts, borrowed-user
count, room keys, and the absence of changes to borrowed user documents.
Capture Job status and logs before changing phase if the Argo application
prunes resources that disappear from the next render.

### 2. Start and sustain load

Promote the same run values to:

```yaml
phase: soak
```

Argo CD replaces the Seed Job with a single-replica Deployment. The
Deployment uses `strategy.type=Recreate`, so a configuration or image rollout
cannot briefly double the generated rate.

The first front-door probe must log:

```text
Cassandra soak encryption preflight passed
```

The Deployment then runs until another release changes its phase. Pod
replacement reloads the Mongo manifest, increments the restart count, starts
a fresh warm-up, rebuilds the in-memory recent-message catalog, and reloads
unresolved operations from the PVC-backed WAL. Prometheus retains aggregate
cross-restart evidence while the WAL retains unresolved per-operation
evidence. The interrupted traffic/SLO interval remains inconclusive.

Freeze the image and workload configuration while collecting a comparable
run. A values change restarts the Deployment because the Pod template carries
a ConfigMap checksum.

### 3. Stop load

Promote to:

```yaml
phase: stopped
```

Argo CD removes the Deployment. SIGTERM causes loadgen to stop dispatching,
drain in-flight operations and NATS, print its process-local report, and mark
the run stopped. Confirm that no loadgen Pod remains and that the Mongo
heartbeat no longer advances.

Do not move directly from `soak` to `teardown`.

### 4. Preserve evidence

Before teardown, retain:

- immutable image digest and complete non-secret values;
- manifest state, restart count, first-start/stop times, and heartbeat;
- Deployment/Pod status, events, node identity, and restart history;
- logs and process-local reports;
- Prometheus rate, error, retry, latency, saturation, and verification data;
- operation outcome, inflight, recovery, invalidation, WAL-size, NATS
  connection, Go runtime, and process metrics;
- the versioned WAL, observer health, exact-recipient anomaly journals, and
  their retained SHA-256 content hashes;
- Cassandra service metrics, disk usage, compaction, timeout, and latency
  evidence;
- Mongo owned-object counts before and after cleanup.

Never export Secret values, NATS credentials, plaintext message bodies, or
wrapped key material into the evidence bundle.

The controller intentionally remains a single-replica `Deployment` with
`Recreate`, not a StatefulSet. Do not approve teardown until the retained PVC
has been copied and its evidence digests verified.

### 5. Teardown

After explicit approval, promote to:

```yaml
phase: teardown

teardown:
  approved: true
  batchRooms: "250"
  batchDelay: 100ms
  batchTimeout: 30s
```

The Chart refuses to render `phase=teardown` without the approval flag.
Loadgen additionally refuses teardown while the manifest has a fresh active
heartbeat.

For a Mongo fault campaign, set `soak.heartbeatStaleAfter` well beyond the
longest planned outage and recovery window (for example `30m` for a planned
two-minute outage). This value is a destructive-operation lease; it is not the
dashboard heartbeat freshness threshold. If renewal never recovers, loadgen
stops dispatching one `soak.heartbeatInterval` before this lease can become
stale. With the example values (`30m` and the default `30s` interval), the
workload therefore stops after approximately 29 minutes 30 seconds without a
successful persisted heartbeat. The staging overlay and required validation
relationship are listed under "Staging Mongo failure campaign settings" above.

If loadgen actually crashed, there is no force flag. The emergency escape hatch
is a teardown-only lease override. First promote to `phase=stopped`, confirm no
loadgen Pod exists, and verify across at least two normal heartbeat intervals
that the manifest heartbeat no longer advances. Only then render the teardown
Job with a shorter still-valid lease:

```bash
helm upgrade cassandra-soak tools/loadgen/deploy/k8s \
  -n loadgen-smoke -f tools/loadgen/deploy/k8s/values-local.yaml \
  --set phase=teardown --set teardown.approved=true \
  --set soak.heartbeatStaleAfter=1m \
  --wait --wait-for-jobs --timeout 10m
```

The teardown-only `1m` value is intentionally shorter than the workload lease
minimum. It is accepted only because `phase=teardown` has no heartbeat cadence
to protect. Never shorten this lease while a loadgen Pod may still be running.
Doing so permits teardown to delete a live run's topology.

Mongo cleanup pages the run's ownership room IDs and deletes serial batches.
Tune `batchRooms` and `batchDelay` downward/upward to reduce or increase
cleanup pressure; `batchTimeout` limits one batch without imposing a deadline
on the whole cleanup. Each room is rechecked against the selected run before
its dependent artifacts are deleted. The queries use existing service indexes,
and the ownership ledger uses its run-prefixed `_id` range, so the Chart does
not require a teardown-only index on shared collections.

The safe Cassandra default is:

```yaml
cassandra:
  cleanup: none
```

Only a disposable dedicated keyspace may use:

```yaml
cassandra:
  keyspace: chat_soak_20260725
  cleanup: truncate
  confirmKeyspace: chat_soak_20260725
```

The Chart and binary both require exact keyspace confirmation. A shared
staging keyspace must never use `truncate`.

After the Teardown Job completes and cleanup evidence is retained, remove the
run release from desired state. Delete the retained ledger PVC only through a
separate, explicitly targeted operator action.

## Validation

Run the portable Chart checks locally:

```bash
make validate-loadgen-k8s
```

This lints the Chart and renders the Seed Job, Soak Deployment, stopped state,
and approved Teardown Job. When a reachable Kubernetes context exists:

```bash
make validate-loadgen-k8s KUBE_DRY_RUN=true
```

The optional command adds kubectl API discovery. A real release remains the
final validation of company admission policies, Argo CD configuration,
ExternalSecret integration, quotas, and network policy.

## Local kind service-path smoke

`values-local.yaml` is a non-secret, low-rate profile for validating the same
Chart lifecycle against a local kind cluster. It is not a capacity benchmark
and must not be promoted to staging. On Windows, run the POSIX Make recipes
from Git Bash.

For a first-time Windows setup, keep the temporary NATS credential output on a
Docker-bindable workspace path:

```bash
export NATS_SETUP_TMP_ROOT="$PWD/tmp"
```

Start the local dependencies, seed the existing development users, and start
the three services that define Run A:

```bash
make deps-up
make seed
make up-detached SERVICE=message-gatekeeper
make up-detached SERVICE=message-worker
make up-detached SERVICE=history-service
```

The local Valkey cluster announces the Docker-network hostname `valkey`. If a
Windows host cannot resolve that redirect while running `make seed`, run the
same Make target in an ephemeral container attached to `chat-local`; do not
change production Valkey discovery:

```bash
docker run --rm --network chat-local \
  -v "$PWD:/src" -w /src \
  -e MONGO_URI=mongodb://mongodb:27017 \
  -e VALKEY_ADDRS=valkey:6379 \
  golang:1.25.13 make seed
```

Create the cluster, build the exact loadgen image, and load it into kind:

```bash
kind create cluster --name newchat-loadgen
make -C tools/loadgen/deploy image IMAGE=newchat-loadgen:local
kind load docker-image newchat-loadgen:local --name newchat-loadgen
make validate-loadgen-k8s KUBE_DRY_RUN=true
```

Create only local credentials. These commands are for the disposable kind
cluster; production Secret delivery remains part of the release platform:

```bash
kubectl create namespace loadgen-smoke
kubectl -n loadgen-smoke create secret generic cassandra-soak-credentials \
  --from-literal=nats-url=nats://host.docker.internal:4222 \
  --from-literal=mongo-uri=mongodb://host.docker.internal:27017 \
  --from-file=backend.creds=docker-local/backend.creds
```

Exercise the same desired-state transitions that Argo CD will reconcile:

```bash
helm upgrade --install cassandra-soak tools/loadgen/deploy/k8s \
  -n loadgen-smoke -f tools/loadgen/deploy/k8s/values-local.yaml \
  --set phase=seed --wait --wait-for-jobs --timeout 10m

helm upgrade cassandra-soak tools/loadgen/deploy/k8s \
  -n loadgen-smoke -f tools/loadgen/deploy/k8s/values-local.yaml \
  --set phase=soak --wait --timeout 5m

helm upgrade cassandra-soak tools/loadgen/deploy/k8s \
  -n loadgen-smoke -f tools/loadgen/deploy/k8s/values-local.yaml \
  --set phase=stopped --wait --timeout 5m

helm upgrade cassandra-soak tools/loadgen/deploy/k8s \
  -n loadgen-smoke -f tools/loadgen/deploy/k8s/values-local.yaml \
  --set phase=teardown --set teardown.approved=true \
  --wait --wait-for-jobs --timeout 10m
```

During soak, confirm the encryption preflight log, Prometheus operation/error
counters, increasing Cassandra row counts, and ciphertext in `enc_payload`
with a null plaintext `msg`. Delete the soak Pod once and confirm that its
replacement reloads the manifest, rebuilds pinned state through the real
pinned-list RPC, performs a fresh warm-up, and resumes the continuous run.
After `phase=stopped`, confirm no loadgen Pod remains and its heartbeat is no
longer advancing. After teardown, confirm borrowed `users` remain and only
run-owned Mongo topology was removed.

This smoke proves container startup, Kubernetes API acceptance, lifecycle
replacement, host-to-kind connectivity, the real NATS service path,
Cassandra encrypted persistence, history read-back, graceful termination,
and restart recovery. It does not reproduce staging topology, admission
controllers, network policies, Argo CD pruning, ExternalSecret behavior,
multi-node Cassandra, or production performance.
