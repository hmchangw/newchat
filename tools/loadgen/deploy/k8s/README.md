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
  through the real service path until the release changes phase.
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
```

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
a fresh warm-up, and rebuilds the in-memory recent-message catalog. Prometheus
counters are the authoritative cross-restart evidence.

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
- Cassandra service metrics, disk usage, compaction, timeout, and latency
  evidence;
- Mongo owned-object counts before and after cleanup.

Never export Secret values, NATS credentials, plaintext message bodies, or
wrapped key material into the evidence bundle.

### 5. Teardown

After explicit approval, promote to:

```yaml
phase: teardown

teardown:
  approved: true
```

The Chart refuses to render `phase=teardown` without the approval flag.
Loadgen additionally refuses teardown while the manifest has a fresh active
heartbeat.

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
run release from desired state.

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
