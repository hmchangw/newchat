# Cassandra Run A on Kubernetes

These manifests run the Cassandra-focused Run A harness in three explicit
phases. They contain no namespace, staging endpoint, or credential.

- `kustomization.yaml` installs only the shared non-secret ConfigMap and
  metrics Service.
- `seed-job.yaml` borrows real users read-only and creates run-owned Mongo
  topology.
- `soak-job.yaml` drives the real gatekeeper, message-worker, and
  history-service paths.
- `teardown-job.yaml` is manual and is never created by the shared
  Kustomization.

Never apply all YAML files as a directory. Doing so would start teardown at
the same time as the soak.

## Prerequisites and operational gate

Implementation completion does not authorize a staging run. Before seed,
record approval for:

1. The namespace, site ID, NATS account, Mongo database, and unique run ID.
2. The borrowed-user filter/count, room count, and MongoDB write budget.
3. A Cassandra keyspace dedicated to the test if Cassandra cleanup will use
   `truncate`.
4. Matching `MESSAGE_BUCKET_HOURS` on loadgen, message-worker, and
   history-service.
5. `ATREST_ENABLED=true` and working Vault/KMS dependencies on
   message-worker and history-service.
6. Cassandra free disk for projected growth plus safety margin.
7. Prometheus discovery of this Job and access to the Cassandra/service
   dashboards.
8. The evidence-retention window and named operator responsible for manual
   teardown.
9. Acceptance that NATS, Elasticsearch, and Valkey side effects are not
   automatically removed by this harness.

Build and publish the existing `tools/loadgen/deploy/Dockerfile`. Record the
immutable image digest:

```bash
export NAMESPACE='<approved-namespace>'
export LOADGEN_IMAGE='<registry>/newchat-loadgen@sha256:<digest>'
```

Do not use a mutable tag for a 72-hour result.

## Create operator-owned configuration

The committed ConfigMap holds only portable workload defaults. Create a
run-specific ConfigMap separately. `SOAK_CONFIRM_KEYSPACE` is required even
when cleanup is `none`, so the evidence bundle records the intended target.
Use `truncate` only for a disposable, dedicated keyspace.

```bash
kubectl -n "$NAMESPACE" create configmap cassandra-soak-run \
  --from-literal=SOAK_RUN_ID='<unique-run-id>' \
  --from-literal=SITE_ID='<site-id>' \
  --from-literal=CASSANDRA_KEYSPACE='<dedicated-keyspace>' \
  --from-literal=SOAK_CASSANDRA_CLEANUP='none' \
  --from-literal=SOAK_CONFIRM_KEYSPACE='<dedicated-keyspace>' \
  --dry-run=client -o yaml |
  kubectl -n "$NAMESPACE" apply -f -
```

Create the connection Secret. The NATS credential is mounted read-only; URIs
and credentials are injected as individual environment variables. Optional
Mongo authentication keys may be omitted. Cassandra connection keys are
needed only when `SOAK_CASSANDRA_CLEANUP=truncate`.

```bash
kubectl -n "$NAMESPACE" create secret generic cassandra-soak-loadgen-secrets \
  --from-literal=nats-url='<nats-url>' \
  --from-file=backend.creds='<path-to-backend.creds>' \
  --from-literal=mongo-uri='<mongo-uri>' \
  --from-literal=mongo-username='<mongo-user>' \
  --from-literal=mongo-password='<mongo-password>' \
  --dry-run=client -o yaml |
  kubectl -n "$NAMESPACE" apply -f -
```

For guarded Cassandra truncation, recreate/apply the Secret with these
additional keys:

```text
cassandra-hosts
cassandra-username
cassandra-password
```

Do not commit either generated object or its rendered YAML.

Apply shared configuration and verify that the intended workload values are
present:

```bash
kubectl -n "$NAMESPACE" apply -k tools/loadgen/deploy/k8s
kubectl -n "$NAMESPACE" get configmap cassandra-soak-loadgen-config -o yaml
kubectl -n "$NAMESPACE" get configmap cassandra-soak-run -o yaml
```

Override any provisional rate or population value by editing the
`cassandra-soak-loadgen-config` object in the cluster before seed. Keep the
rendered, non-secret ConfigMaps in the evidence bundle.

## Phase 1: seed

Patch the immutable image locally before creating the Job. This avoids a race
where the placeholder image could start before `kubectl set image`.

```bash
kubectl -n "$NAMESPACE" delete job cassandra-soak-seed --ignore-not-found
kubectl set image --local \
  -f tools/loadgen/deploy/k8s/seed-job.yaml \
  loadgen="$LOADGEN_IMAGE" -o yaml |
  kubectl -n "$NAMESPACE" apply -f -

kubectl -n "$NAMESPACE" wait \
  --for=condition=complete job/cassandra-soak-seed --timeout=30m
kubectl -n "$NAMESPACE" logs job/cassandra-soak-seed
```

Before proceeding, inspect the `loadgen_soak_runs` manifest, owned room and
subscription counts, borrowed-user count, and the absence of changes to
borrowed user documents.

## Phase 2: soak

The soak Job has exactly one completion and one pod in parallel. A successful
Job is not restarted. A failed container or evicted pod can retry under the
Job's bounded backoff. The process reloads its Mongo manifest and resumes the
original wall-clock deadline; its recent-message catalog intentionally starts
empty and warms again.

```bash
kubectl -n "$NAMESPACE" delete job cassandra-soak-run --ignore-not-found
kubectl set image --local \
  -f tools/loadgen/deploy/k8s/soak-job.yaml \
  loadgen="$LOADGEN_IMAGE" -o yaml |
  kubectl -n "$NAMESPACE" apply -f -

kubectl -n "$NAMESPACE" get job cassandra-soak-run --watch
kubectl -n "$NAMESPACE" get pods \
  -l app.kubernetes.io/component=cassandra-soak,loadgen.newchat/phase=soak
kubectl -n "$NAMESPACE" logs -f job/cassandra-soak-run
```

The first front-door probe must log
`Cassandra soak encryption preflight passed`. This means the gatekeeper
accepted the message and message-worker created a non-empty
`room_data_keys.wrappedDEK`. A room's seeded `rooms.encKey` is not equivalent
evidence.

Verify Prometheus discovery:

```bash
kubectl -n "$NAMESPACE" get service,endpoints cassandra-soak-loadgen
kubectl -n "$NAMESPACE" port-forward service/cassandra-soak-loadgen 9099:9099
```

Confirm `/metrics` exposes `loadgen_soak_operations_total`,
`loadgen_soak_rpc_latency_seconds`, `loadgen_soak_errors_total`,
`loadgen_soak_retries_total`, `loadgen_soak_verifications_total`, and
`loadgen_soak_mutation_target_missing_total`. The port-forward is for
inspection only; the Service and pod annotations are the scrape discovery
contract.

## Evidence retention

Before teardown, retain at minimum:

- the immutable loadgen image digest and Git commit;
- both non-secret ConfigMaps and the Job/Pod specs;
- complete seed and soak logs, including the final per-RPC report;
- Job status, pod restart count, events, node identity, and start/end times;
- Prometheus snapshots or exported query results for achieved rate, errors,
  retries, latency drift, saturation, and correctness classes;
- Cassandra capacity, compaction, tombstone, latency, and error dashboards;
- the Mongo run manifest and ownership counts;
- the accepted preflight message ID and wrapped-DEK existence check;
- the approved environment assumptions and any deviations during the run.

Do not export Secret values, NATS credentials, message bodies, or plaintext
DEKs.

## Phase 3: manual teardown

Teardown is intentionally absent from the soak Job and Kustomization. Run it
only after evidence retention and explicit approval. Check the run-specific
ConfigMap immediately beforehand:

```bash
kubectl -n "$NAMESPACE" get configmap cassandra-soak-run -o yaml
```

For Mongo-only ownership cleanup, keep
`SOAK_CASSANDRA_CLEANUP=none`. For a dedicated disposable keyspace, set
`SOAK_CASSANDRA_CLEANUP=truncate` and ensure
`SOAK_CONFIRM_KEYSPACE` exactly equals `CASSANDRA_KEYSPACE`; the binary
rejects any mismatch.

```bash
kubectl -n "$NAMESPACE" delete job cassandra-soak-teardown --ignore-not-found
kubectl set image --local \
  -f tools/loadgen/deploy/k8s/teardown-job.yaml \
  loadgen="$LOADGEN_IMAGE" -o yaml |
  kubectl -n "$NAMESPACE" apply -f -

kubectl -n "$NAMESPACE" wait \
  --for=condition=complete job/cassandra-soak-teardown --timeout=2h
kubectl -n "$NAMESPACE" logs job/cassandra-soak-teardown
```

Verify that borrowed users and unrelated runs remain, then remove the
operational objects when the retention window ends:

```bash
kubectl -n "$NAMESPACE" delete job \
  cassandra-soak-seed cassandra-soak-run cassandra-soak-teardown \
  --ignore-not-found
kubectl -n "$NAMESPACE" delete service cassandra-soak-loadgen
kubectl -n "$NAMESPACE" delete configmap \
  cassandra-soak-loadgen-config cassandra-soak-run
kubectl -n "$NAMESPACE" delete secret cassandra-soak-loadgen-secrets
```

If Cassandra cleanup remains `none`, its test rows remain by design and need a
separately approved retention/deletion procedure.

## Local validation

This requires only `kubectl`. It renders the safe shared Kustomization and a
validation-only aggregate containing all three phase Jobs. When the current
Kubernetes context is reachable, opt in to a client-side dry run using that
cluster's API discovery. Offline runs report that final server schema
validation is deferred to the real apply:

```bash
make validate-loadgen-k8s
make validate-loadgen-k8s KUBE_DRY_RUN=true
```
