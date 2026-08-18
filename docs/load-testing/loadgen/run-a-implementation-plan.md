# Cassandra Run A Soak Harness Implementation Plan

**Status:** Approved and implemented. The Kubernetes execution model was
amended after confirming that company Jobs are limited to one day and
production releases are reconciled by Argo CD.

**Authoritative specification:**
[`soak-test-plan.md`](../soak/cassandra-soak-plan.md)

**Goal:** Extend the existing `tools/loadgen` binary with a Kubernetes-ready
Run A harness that drives the real message and history service paths for a
continuous, operator-stopped Cassandra soak. The harness must build an isolated,
owned room topology from real staging users, sustain the workload model from
the specification, verify a sample of persisted data, survive transient
dependency failures and pod restarts, and report bounded per-RPC metrics.

**Primary path under test:**

```text
loadgen
  -> message-gatekeeper
  -> MESSAGES-CANONICAL
  -> message-worker
  -> Cassandra

loadgen
  -> history-service
  -> Cassandra
```

**Technology:** Go 1.25, NATS and JetStream, MongoDB, Cassandra, Prometheus,
Kubernetes Jobs and Deployment, Helm, Argo CD, `go.uber.org/mock`,
`stretchr/testify`, and the existing repository Make targets.

---

## Scope

### In scope

- Run A only, through `message-gatekeeper`, `message-worker`, and
  `history-service`.
- Borrowing up to 20,000 real MongoDB users without modifying or deleting
  them.
- Test-owned channel and DM rooms, subscriptions, room keys, thread metadata,
  messages, reactions, and pins.
- A realistic, configurable action mix:
  - top-level sends;
  - thread replies;
  - LoadHistory;
  - GetThreadMessages;
  - GetMessageByID;
  - reaction add/remove;
  - edit;
  - soft-delete;
  - pin/unpin;
  - pinned-list.
- Gatekeeper-success plus `persist_grace` eligibility.
- Transient retry, NATS reconnect, and post-restart warm-up.
- Correctness sampling against data produced by the current run.
- Bounded in-process metrics and Prometheus export.
- Kubernetes Seed Job, single-replica Soak Deployment, and Teardown Job.

### Explicitly out of scope

- Run B/C and every pathological direct-CQL injection experiment.
- Historical Cassandra backfill.
- A full newchat end-to-end capacity verdict.
- User-facing SLI/SLO definition or certification.
- Service code changes.
- The optional o11y domain metric mentioned in the specification.
- Cross-process checkpointing of the recent-message catalog.
- Automatic cleanup of NATS, Elasticsearch, or Valkey side effects.

---

## Repository Facts That Constrain the Design

1. `tools/loadgen` already has reusable open-loop pacing, publishers,
   collectors, reports, fixture builders, Mongo seed helpers, and history RPC
   patterns. Run A must extend these rather than create a second load-test
   binary.
2. `natsutil.Connect` already configures unlimited reconnect attempts with a
   two-second reconnect wait. Run A needs reconnect-aware workload behavior,
   not another NATS dialer.
3. A successful gatekeeper response contains the accepted `model.Message`.
   The asynchronous response subject is the authoritative admission event for
   the recent-message catalog.
4. Gatekeeper success only proves publication to `MESSAGES-CANONICAL`; it does
   not prove that `message-worker` has persisted the message. The configured
   `persist_grace` and read-back sampler cover that gap.
5. Edit and soft-delete are sender-only. The catalog must retain the original
   account and route these mutations as that account.
6. Reaction is a toggle, not an idempotent "set" operation. An ambiguous
   timeout cannot be retried blindly because a successful add followed by a
   retry would remove the same reaction.
7. LoadHistory paginates with `before`, while GetThreadMessages and pinned-list
   use cursors. The implementation must follow the real wire contracts.
8. `SeedRoomKeys` writes the room document's `encKey`. Cassandra at-rest
   encryption uses wrapped DEKs in `room_data_keys`, created lazily through the
   real `message-worker` Vault/KMS path. The harness must not fabricate wrapped
   DEKs.
9. The root Makefile expands `SERVICE` as `./$(SERVICE)/...`; the correct
   loadgen value is `SERVICE=tools/loadgen`, not `SERVICE=loadgen`.
10. The repository currently has a production-capable loadgen Dockerfile but
    no Kubernetes manifests.

---

## Proposed CLI

```text
loadgen seed --workload=soak
loadgen soak
loadgen teardown --workload=soak
```

`SOAK_RUN_ID` identifies all three phases. Seed is safe to retry for the same
run ID, soak loads the topology from the run manifest, and teardown removes
only data owned by that run.

The soak command supports a bounded `duration` mode for local/CI use and a
`continuous` mode for the Kubernetes Deployment. It records lifecycle metadata
such as first start, optional deadline, restart count, stop time, and heartbeat
lease in the manifest. This is not a recent-message checkpoint: after a pod
restart the message catalog is empty and must be rebuilt from fresh successful
sends.

---

## Kubernetes Execution Model

The company platform limits Jobs to one day, while the soak must run until an
operator publishes a stop release. Seed and teardown are bounded batch work;
the load phase is a continuous single-replica Deployment.

```text
seed Job (manual)
  -> validates environment
  -> borrows real users
  -> writes owned Mongo topology and room keys

soak Deployment (one replica, Recreate strategy)
  -> loads the run manifest
  -> exposes Prometheus metrics
  -> renews a Mongo heartbeat lease
  -> runs until Argo CD removes it
  -> re-warms after a pod restart

teardown Job (manual, after evidence retention)
  -> removes owned Mongo data
  -> truncates only an explicitly confirmed dedicated Cassandra keyspace
```

Kubernetes requirements:

- `parallelism: 1` and `completions: 1` for both Jobs.
- Exactly one Soak Deployment replica with `strategy.type=Recreate`; two soak
  pods would double the target rate and invalidate the run.
- Job-level deadlines below the platform's one-day limit.
- Continuous mode has no process or Kubernetes deadline. Argo CD removal sends
  SIGTERM and the process drains before exit.
- NATS credentials mounted read-only from a Secret.
- MongoDB and Cassandra credentials sourced from Secret keys, never committed.
- Non-secret workload settings sourced from a ConfigMap.
- Explicit CPU and memory requests/limits so loadgen self-saturation is
  observable and scheduling is stable.
- A sufficiently long termination grace period for in-flight request drain
  and the final per-pod summary.
- A ClusterIP Service and Prometheus scrape annotations for port 9099.
- `automountServiceAccountToken: false`, non-root execution, and a read-only
  root filesystem.
- No automatic teardown chained to Deployment removal. Evidence must remain for
  the agreed 24-72 hour review window.

Prometheus is the cross-pod source of record if Kubernetes restarts the soak
pod. In-process percentiles and the final console report cover one process
lifetime; the harness does not checkpoint its catalog or collector.

---

## Configuration Contract

All soak behavior is configured by environment variables. Existing shared
loadgen settings remain unchanged.

| Variable | Proposed default | Purpose |
|---|---:|---|
| `SOAK_RUN_ID` | required | Stable ownership and correlation ID |
| `SOAK_RUN_MODE` | `duration` | `duration` for bounded runs or `continuous` until SIGTERM |
| `SOAK_RUN_DURATION` | `72h` | Total run duration in `duration` mode |
| `SOAK_WARMUP` | `30s` | Per-process warm-up excluded from steady-state reporting |
| `SOAK_HEARTBEAT_INTERVAL` | `30s` | Active-run Mongo lease renewal |
| `SOAK_HEARTBEAT_STALE_AFTER` | `2m` | Teardown active-run guard threshold |
| `SOAK_SEND_RATE` | `100` | Total sends per second |
| `SOAK_READ_RATE` | `700` | Main history RPCs per second; each scheduled read fetches one page |
| `SOAK_THREAD_SHARE` | `0.10` | Thread replies as a share of sends |
| `SOAK_MUTATION_RATE` | `5` | Edit/delete/pin family operations per second |
| `SOAK_SOFT_DELETE_RATIO` | `0.001` | Soft-deletes per accepted message |
| `SOAK_REACTION_RATE` | `100` | Independent reaction operations per second |
| `SOAK_REACTIONS_PER_HOT_MESSAGE` | `30` | Target map width for popular messages |
| `SOAK_REACTION_MESSAGE_SCOPE` | `hot_only` | Provisional I8 interpretation |
| `SOAK_REACTION_REMOVE_SHARE` | `0.20` | Share of reaction operations that remove |
| `SOAK_PINNED_LIST_RATE` | `1` | Pinned-list requests per second |
| `SOAK_VERIFY_RATE` | `1` | Read-back samples per second |
| `SOAK_MAX_USERS` | `20000` | Maximum borrowed user count |
| `SOAK_ACTIVE_USERS` | `2000` | Active load-driving users |
| `SOAK_ROOM_COUNT` | `10000` | Safe staging default; production forecast remains configurable |
| `SOAK_CHANNEL_RATIO` | `0.30` | Channel share of generated rooms |
| `SOAK_CHANNEL_MEMBERS` | `100` | Target channel membership |
| `SOAK_LARGE_ROOM_THRESHOLD` | `500` | Gatekeeper large-room threshold topology must not exceed |
| `SOAK_RATE_SCOPE` | `site` | Provisional I10 interpretation |
| `SOAK_MESSAGES_PER_ACTIVE_USER_PER_DAY` | `0` | `0` means derive and report the implied I12 value |
| `SOAK_PAYLOAD_MEDIAN_BYTES` | `1024` | Post-encryption target median |
| `SOAK_PAYLOAD_P95_BYTES` | `2048` | Post-encryption target p95 |
| `SOAK_PAYLOAD_MAX_BYTES` | `10240` | Post-encryption hard cap |
| `SOAK_PERSIST_GRACE` | `10s` | Minimum age after gatekeeper success |
| `SOAK_MUTATION_RETRIES` | `3` | Not-found retry limit |
| `SOAK_RETRY_MIN_BACKOFF` | `100ms` | Initial transient retry delay |
| `SOAK_RETRY_MAX_BACKOFF` | `5s` | Maximum transient retry delay |
| `SOAK_RECENT_PER_ROOM` | `128` | Per-room catalog capacity |
| `SOAK_RECENT_TOTAL` | `200000` | Global catalog memory bound |
| `SOAK_CASSANDRA_CLEANUP` | `none` | Must be set to `truncate` for Cassandra cleanup |
| `SOAK_CONFIRM_KEYSPACE` | empty | Must exactly equal `CASSANDRA_KEYSPACE` before truncate |
| `SOAK_TEARDOWN_BATCH_ROOMS` | `250` | Maximum owned room IDs per Mongo deletion batch |
| `SOAK_TEARDOWN_BATCH_DELAY` | `100ms` | Cancellable delay between Mongo deletion batches |
| `SOAK_TEARDOWN_BATCH_TIMEOUT` | `30s` | Per-batch Mongo cleanup timeout |

The values for I8, I10, and I12 are deliberately configurable and must be
called out as provisional in operator output and documentation.

---

## Planned File Layout

```text
pkg/subject/
  subject.go
  subject_test.go

tools/loadgen/
  main.go
  main_test.go
  metrics.go
  collector.go
  soak_config.go
  soak_config_test.go
  soak_topology.go
  soak_topology_test.go
  soak_store.go
  soak_seed.go
  soak_seed_test.go
  soak_seed_integration_test.go
  soak_teardown.go
  soak_teardown_test.go
  soak_teardown_integration_test.go
  soak_distribution.go
  soak_distribution_test.go
  soak_catalog.go
  soak_catalog_test.go
  soak_wire.go
  soak_wire_test.go
  soak_rpc.go
  soak_rpc_test.go
  soak_send.go
  soak_send_test.go
  soak_read.go
  soak_read_test.go
  soak_mutation.go
  soak_mutation_test.go
  soak_verify.go
  soak_verify_test.go
  soak_workload.go
  soak_workload_test.go
  soak_collector.go
  soak_collector_test.go
  soak_report.go
  soak_report_test.go
  soak_main.go
  soak_main_test.go
  soak_integration_test.go
  mock_soak_store_test.go
  README.md

tools/loadgen/deploy/k8s/
  Chart.yaml
  README.md
  values.yaml
  values-local.yaml
  values-validation.yaml
  values.schema.json
  templates/
    _helpers.tpl
    configmap.yaml
    service.yaml
    seed-job.yaml
    soak-deployment.yaml
    teardown-job.yaml
```

No new third-party Go dependency is planned.

---

## TDD and Commit Protocol

Every task follows the same sequence:

1. Write the task's tests first.
2. Run the relevant Make target and confirm that the new test fails for the
   intended reason.
3. Implement the minimum behavior required to pass.
4. Refactor while the tests remain green.
5. Run `make lint` and `make test`.
6. Commit only that task with the commit message listed below.

When an interface changes:

```text
make generate SERVICE=tools/loadgen
```

Focused loadgen tests:

```text
make test SERVICE=tools/loadgen
```

The full repository tests remain the task-level regression gate:

```text
make lint
make test
```

---

## Task 1: Add Concrete History Mutation Subject Builders

**Files**

- Modify `pkg/subject/subject.go`.
- Modify `pkg/subject/subject_test.go`.

**Red**

- Add exact-subject tests for edit, delete, pin, unpin, pinned-list, and
  reaction.
- Add invalid account-token tests matching the existing concrete history
  builders.
- Run `make test SERVICE=pkg/subject` and confirm the builders are missing.

**Green and refactor**

- Add `MsgEdit`, `MsgDelete`, `MsgPin`, `MsgUnpin`, `MsgPinnedList`, and
  `MsgReact`.
- Keep concrete builders adjacent to their matching natsrouter patterns.

**Acceptance**

- No soak action constructs a NATS subject with a local `fmt.Sprintf`.
- Concrete subjects exactly match the handlers registered by
  `history-service`.

**Commit**

`feat(subject): add concrete message mutation subjects`

---

## Task 2: Define Soak Configuration and CLI Contracts

**Files**

- Create `tools/loadgen/soak_config.go`.
- Create `tools/loadgen/soak_config_test.go`.
- Modify `tools/loadgen/main.go`.
- Modify `tools/loadgen/main_test.go`.

**Red**

- Test all defaults in the configuration table.
- Test required `SOAK_RUN_ID`.
- Test invalid rates, ratios, durations, retry bounds, catalog limits, user
  bounds, and cleanup confirmations.
- Test dispatch for `soak`, `seed --workload=soak`, and
  `teardown --workload=soak`.
- Test that no Run B/C or direct-CQL workload value is accepted.

**Green and refactor**

- Add an env-prefixed `soakConfig`.
- Add validation that fails before external connections are opened.
- Add CLI routing stubs without implementing workload behavior.
- Print the provisional I8/I10/I12 assumptions at startup.

**Acceptance**

- Every unconfirmed input is configurable.
- Unsafe cleanup configuration fails closed.
- Existing loadgen subcommands and config remain backward-compatible.

**Commit**

`feat(loadgen): define Cassandra soak configuration`

---

## Task 3: Build Topology from Borrowed Real Users

**Files**

- Create `tools/loadgen/soak_topology.go`.
- Create `tools/loadgen/soak_topology_test.go`.

**Red**

- Test exclusion of inactive users, empty accounts, invalid NATS account
  tokens, and ineligible bot/platform accounts.
- Test the 20,000-user ceiling and deterministic active-user selection.
- Test the 3:7 channel/DM split, exact two-person DMs, unique DM pairs, channel
  membership, and role assignment.
- Test that every active user has at least one writable room.
- Test that the input user values are unchanged.

**Green and refactor**

- Build topology from a projected `[]model.User`.
- Use `pkg/idgen` for room and subscription identities.
- Assign an owner to each channel and member roles to the remaining
  subscribers.
- Construct DM IDs with `idgen.BuildDMRoomID`.

**Acceptance**

- No synthetic user is created.
- Borrowed users are never mutated.
- The generated topology is deterministic for the same input ordering and
  seed.
- Reducing room count scales topology only; it does not change Cassandra
  message rates.

**Commit**

`feat(loadgen): build soak topology from borrowed users`

---

## Task 4: Add Mongo Ownership Store and Safe Seed

**Files**

- Create `tools/loadgen/soak_store.go`.
- Create `tools/loadgen/soak_seed.go`.
- Create `tools/loadgen/soak_seed_test.go`.
- Create `tools/loadgen/soak_seed_integration_test.go`.
- Generate `tools/loadgen/mock_soak_store_test.go`.

**Red**

- Test the exact user filter and projection.
- Test bulk insertion of rooms and subscriptions with a run ownership tag.
- Test chunked ownership records so large room sets do not approach MongoDB's
  single-document size limit.
- Test that generated room IDs cannot overwrite an existing non-owned room.
- Test that the exact active-user IDs are persisted and restored.
- Test retry after a partially completed seed.
- Integration-test preservation of borrowed users, unrelated rooms,
  subscriptions, and service-owned indexes.

**Green and refactor**

- Define narrow consumer-owned store interfaces and generate mocks.
- Read only the user fields required by topology, gatekeeper identity, and
  reaction display data.
- Write test rooms/subscriptions in bounded batches.
- Generate independent cryptographically random room key pairs and persist
  them through the existing room-key store.
- Persist chunked ownership before room writes so a partial seed remains
  recoverable.
- Persist manifest state transitions: `seeding`, `seeded`, `running`,
  `completed`, and `cleaned`.

**Acceptance**

- Seed never drops a collection.
- Seed never inserts, updates, or deletes a user.
- Re-running seed for the same run ID can clean or resume only that run's
  partial data.
- The manifest records counts, configuration digest, site, Mongo database,
  Cassandra keyspace, and timestamps.

**Commit**

`feat(loadgen): seed owned soak topology without touching users`

---

## Task 5: Implement Guarded Mongo and Cassandra Teardown

**Files**

- Create `tools/loadgen/soak_teardown.go`.
- Create `tools/loadgen/soak_teardown_test.go`.
- Create `tools/loadgen/soak_teardown_integration_test.go`.
- Modify `tools/loadgen/main.go`.

**Red**

- Test that an unknown run ID changes nothing.
- Test deletion of only the selected run's subscriptions, thread rooms,
  thread subscriptions, room keys, wrapped DEK rows, rooms, and ownership
  records.
- Test that users and another run's data survive.
- Test that Cassandra cleanup is refused unless cleanup mode is `truncate` and
  `SOAK_CONFIRM_KEYSPACE` exactly matches the connected keyspace.
- Test repeat teardown as an idempotent no-op.

**Green and refactor**

- Page through chunked room ownership records.
- Resolve each batch's rooms by `_id` and verify `soakRunId` before deleting
  any dependent artifacts.
- Use existing `_id`, `roomId`, and `threadRoomId` indexes rather than adding
  a teardown-only index to shared service collections.
- Page and delete ownership chunks through their run-prefixed `_id` range.
- Delete Mongo data in dependency-safe, configurable paced batches with an
  independent timeout per batch.
- Truncate exactly:
  - `messages_by_room`;
  - `messages_by_id`;
  - `thread_messages_by_thread`;
  - `pinned_messages_by_room`.
- Mark the manifest cleaned only after every selected cleanup stage succeeds.

**Acceptance**

- There is no Cassandra row-by-row delete path.
- There is no wildcard or unverified keyspace target.
- Teardown remains a manual command and is never a soak finalizer.

**Commit**

`feat(loadgen): add guarded soak teardown`

---

## Task 6: Model Room Heat, Payload Size, and Thread Length

**Files**

- Create `tools/loadgen/soak_distribution.go`.
- Create `tools/loadgen/soak_distribution_test.go`.

**Red**

- Test reproducible Zipf room selection.
- Test a clipped lognormal size distribution near the configured median and
  p95 and never above the maximum.
- Test content-size adjustment for JSON serialization plus the AES-GCM
  authentication tag so targets describe Cassandra `enc_payload`, not raw
  client text.
- Test thread-length targets with p99 50 and hard cap 500.

**Green and refactor**

- Reuse the standard library RNG and `rand.Zipf`.
- Model content-only `atrest.EncryptedFields` overhead without performing
  encryption in loadgen.
- Give each eligible thread parent a bounded reply budget.

**Acceptance**

- Generated client content stays below gatekeeper's 20 KiB limit.
- Hot rooms receive a stable, reproducible majority of operations.
- No thread parent is selected after reaching 500 replies.

**Commit**

`feat(loadgen): model soak heat and encrypted payload sizes`

---

## Task 7: Add the Bounded Recent-Message Catalog

**Files**

- Create `tools/loadgen/soak_catalog.go`.
- Create `tools/loadgen/soak_catalog_test.go`.

**Red**

- Test that JetStream publish success alone does not admit a message.
- Test admission only after a successful gatekeeper reply.
- Test that mutation and thread-parent selection are blocked before
  `persist_grace`.
- Test author-only edit/delete selection.
- Test state changes for edit, delete, pin, unpin, reaction, and thread reply
  count.
- Test per-room eviction, global eviction, and concurrent access with a fake
  clock.

**Green and refactor**

- Store message ID, room, author, accepted content, gatekeeper reply time,
  created time, thread relation, mutation state, and reaction state.
- Use sharded synchronization so hot-room access does not serialize the whole
  workload.
- Allocate per-room rings lazily and enforce both memory bounds.

**Acceptance**

- Catalog memory is bounded independently of run duration.
- No test uses `time.Sleep` for catalog synchronization.
- A process restart may discard the catalog without corrupting owned
  topology.

**Commit**

`feat(loadgen): track persistence-eligible soak messages`

---

## Task 8: Add Wire Mirrors, Error Classification, and Retry

**Files**

- Create `tools/loadgen/soak_wire.go`.
- Create `tools/loadgen/soak_wire_test.go`.
- Create `tools/loadgen/soak_rpc.go`.
- Create `tools/loadgen/soak_rpc_test.go`.

**Red**

- Test JSON compatibility for every history-service request and response used
  by Run A.
- Test canonical error envelope parsing through `errcode.Parse`.
- Test timeout, no-responder, disconnected, unavailable, internal,
  not-found, forbidden, bad-request, conflict, decode, and assertion classes.
- Test exponential backoff bounds, jitter bounds, retry exhaustion, and
  immediate context cancellation.
- Test that ambiguous reaction timeouts are not blindly repeated.

**Green and refactor**

- Define local wire mirrors because loadgen cannot import
  `history-service/internal/models`.
- Build a request/reply adapter around the existing o11y NATS connection.
- Retry only safe transient operations.
- For reaction ambiguity, read the message state before deciding whether a
  retry is still required; otherwise record and skip.

**Acceptance**

- Terminal client errors are never retried.
- Mutation not-found follows its separate K-attempt policy.
- Retry metrics carry only bounded action and error-class labels.

**Commit**

`feat(loadgen): add resilient soak RPC transport`

---

## Task 9: Drive Top-Level and Thread Sends Through Gatekeeper

**Files**

- Create `tools/loadgen/soak_send.go`.
- Create `tools/loadgen/soak_send_test.go`.

**Red**

- Test top-level and thread payloads and subjects.
- Test the configured 90:10 selection.
- Test asynchronous correlation on `UserResponseWildcard`.
- Test success response admission and error response rejection.
- Test thread-parent room matching, grace, and cap enforcement.
- Test publish failure with stable message and request IDs.

**Green and refactor**

- Reuse the existing frontdoor publisher and JetStream pattern.
- Generate IDs with `pkg/idgen`.
- Subscribe and flush the response wildcard before starting the send lane.
- Decode the successful gatekeeper `model.Message` and admit it to the
  catalog.

**Acceptance**

- Run A has no canonical injection path.
- A thread reply never references another room.
- A missing gatekeeper response is reported but does not create a second
  logical message with a new ID.

**Commit**

`feat(loadgen): drive soak sends through message gatekeeper`

---

## Task 10: Implement History and Pinned-List Reads

**Files**

- Create `tools/loadgen/soak_read.go`.
- Create `tools/loadgen/soak_read_test.go`.

**Red**

- Test the 75/15/10 LoadHistory, GetThreadMessages, and GetMessageByID mix.
- Test LoadHistory `before` pagination across pages and empty buckets.
- Test thread and pinned-list cursor pagination.
- Test empty catalog behavior as a warm-up skip rather than a malformed
  request.
- Test payload decoding and endpoint-specific latency recording.

**Green and refactor**

- Reuse the existing history requester pattern.
- Select an account that is subscribed to the target room.
- Advance LoadHistory with the oldest returned `created_at` and a strict
  boundary to prevent duplicate pages.
- Detect repeated cursors and non-progressing pages.

**Acceptance**

- All four read endpoints are measured separately.
- Bucket-walk pagination cannot loop forever.
- Normal empty responses are not counted as service errors.

**Commit**

`feat(loadgen): add Cassandra soak read actions`

---

## Task 11: Implement State-Aware Mutations and Reactions

**Files**

- Create `tools/loadgen/soak_mutation.go`.
- Create `tools/loadgen/soak_mutation_test.go`.

**Red**

- Test edit and soft-delete as the original sender.
- Test delete scheduling at 0.1% of accepted messages.
- Test pin/unpin state transitions and pin-limit avoidance.
- Test reaction actor membership, unique actor targeting, add/remove balance,
  hot-message scope, and member-count clamp.
- Test not-found retry K times, final skip, and
  `mutation_target_missing` increment.
- Test that deleted messages are excluded from invalid future actions.

**Green and refactor**

- Split the configured mutation budget across edit, delete, and pin/unpin
  while keeping delete density tied to accepted sends.
- Keep reaction scheduling independent of send rate.
- Bound `reactions_per_hot_message` by available room members.
- Reconcile catalog state only after successful replies.

**Acceptance**

- The implementation never computes reaction rate as send rate multiplied by
  reactions per message.
- A retry cannot accidentally toggle a reaction twice.
- Expected target misses remain observable as a dedicated finding.

**Commit**

`feat(loadgen): execute state-aware soak mutations`

---

## Task 12: Add Read-Back Correctness Sampling

**Files**

- Create `tools/loadgen/soak_verify.go`.
- Create `tools/loadgen/soak_verify_test.go`.

**Red**

- Test GetMessageByID presence, room, author, and expected-content checks.
- Test edited-content verification.
- Test soft-deleted state verification.
- Test LoadHistory search across multiple `before` pages.
- Test missing, mismatched, malformed, and retryable responses as distinct
  result classes.

**Green and refactor**

- Sample only catalog entries old enough to be persisted.
- Prefer immutable messages for exact-content sampling while still sampling
  explicitly tracked edit/delete states.
- Bound the number of LoadHistory pages per sample.

**Acceptance**

- Read-back failures are not merged with normal RPC failures.
- A content mismatch identifies action, room, and message ID in structured
  output but never logs the full message body.
- Sampling never changes application data.

**Commit**

`feat(loadgen): verify persisted soak messages`

---

## Task 13: Orchestrate the Long-Running Weighted Workload

**Files**

- Create `tools/loadgen/soak_workload.go`.
- Create `tools/loadgen/soak_workload_test.go`.

**Red**

- Test independent send, read, mutation, reaction, pinned-list, and
  verification lanes.
- Test weighted selection within each lane.
- Test warm-up exclusion and catalog-not-ready skips.
- Test bounded in-flight work, cancellation, graceful drain, and no goroutine
  leaks.
- Test manifest deadline reuse for bounded mode and deadline-free continuous
  restart/stop behavior.

**Green and refactor**

- Reuse `pacedDispatch` rather than writing another ticker.
- Share a global in-flight budget.
- Continue sends and safe history reads while stateful actions re-warm.
- Distinguish configured-duration completion, graceful SIGTERM stop, and
  dependency failure so the Deployment can resume appropriately.
- Renew a Mongo-backed heartbeat lease and make teardown reject a fresh lease.

**Acceptance**

- Configured rates remain independent.
- A bounded pod restart does not double the total run window; a continuous
  restart has no deadline.
- The recent-message catalog is rebuilt only from fresh gatekeeper successes.
- Graceful shutdown stops new work before draining in-flight work.

**Commit**

`feat(loadgen): orchestrate the Cassandra Run A soak`

---

## Task 14: Extend Metrics, Collector, and Report

**Files**

- Modify `tools/loadgen/metrics.go`.
- Modify `tools/loadgen/collector.go`.
- Create `tools/loadgen/soak_collector.go`.
- Create `tools/loadgen/soak_collector_test.go`.
- Create `tools/loadgen/soak_report.go`.
- Create `tools/loadgen/soak_report_test.go`.

**Red**

- Test attempted, succeeded, failed, skipped, and retried counts per RPC.
- Test achieved-rate calculation after warm-up.
- Test bounded latency histograms and p50/p95/p99 calculation.
- Test early-window versus late-window p99 drift.
- Test error classes, read-back failures, and
  `mutation_target_missing`.
- Test stable Prometheus metric names and bounded labels.
- Test collector memory shape independent of theoretical event count.

**Green and refactor**

- Add action-labeled counters and latency histograms.
- Use fixed buckets or bounded rolling snapshots rather than retaining every
  sample for three days.
- Print target rate, achieved rate, percentiles, errors, retries, missing
  targets, correctness results, and early/late drift.
- Keep the current collectors and reports backward-compatible.

**Acceptance**

- React, edit, delete, pin, unpin, and pinned-list are distinct RPC rows.
- No metric label contains account, user ID, room ID, message ID, run ID, or
  raw error text.
- Prometheus remains the aggregate source if the pod restarts.

**Commit**

`feat(loadgen): report bounded per-RPC soak metrics`

---

## Task 15: Wire CLI, Integration Tests, and Operator Documentation

**Files**

- Create `tools/loadgen/soak_main.go`.
- Create `tools/loadgen/soak_main_test.go`.
- Create `tools/loadgen/soak_integration_test.go`.
- Modify `tools/loadgen/main.go`.
- Modify `tools/loadgen/README.md`.
- Modify `tools/loadgen/deploy/docker-compose.yml`.
- Modify `tools/loadgen/deploy/Makefile`.

**Red**

- Test full CLI argument and environment wiring.
- Integration-test seed, a short action mix, response correlation,
  read-back, and teardown with testcontainers and fake NATS handlers.
- Test restart re-warm with an existing manifest and an empty catalog.
- Test the runtime encryption preflight against present and missing
  `room_data_keys` evidence for an accepted test-room message. The actual
  wrapped-DEK creation path remains a staging smoke check through the real
  message-worker, not a fake loadgen integration test.

**Green and refactor**

- Connect Mongo and NATS for seed/run; connect Cassandra only where teardown
  requires it.
- Start and stop the metrics server with the soak lifecycle.
- Add local Compose commands for soak seed/run/teardown.
- Document every environment variable, safety precondition, known blind spot,
  and the difference between room `encKey` and at-rest wrapped DEK.

**Acceptance**

- The harness can be started without any service code change.
- The normal workload issues no direct CQL.
- The staging smoke procedure proves that a real accepted send creates wrapped
  DEK evidence and that history-service returns the decrypted content.
- Existing loadgen workloads remain green.
- Run B/C, historical backfill, end-to-end SLOs, and o11y domain metrics are
  clearly marked deferred.

**Commit**

`feat(loadgen): wire the Cassandra Run A harness`

---

## Task 16: Add GitOps Kubernetes Deployment Assets

**Files**

- Create `tools/loadgen/deploy/k8s/README.md`.
- Create a Helm Chart rooted at `tools/loadgen/deploy/k8s`.
- Add templates for the ConfigMap, metrics Service, Seed Job, Soak
  Deployment, and Teardown Job.
- Add values schema and non-secret validation values.
- Modify the root `Makefile` with a manifest validation target if no suitable
  target exists.

**Red**

- Add a validation check that lints the Chart and renders every lifecycle
  phase through a Make target.
- Confirm the check fails while the Chart is absent.

**Green and refactor**

- Render exactly one workload phase for `seed`, `soak`, `stopped`, or
  `teardown`.
- Use bounded Jobs for seed/teardown and a single-replica, `Recreate`
  Deployment for continuous load.
- Reference an operator-created Secret; do not add credentials or encoded
  credentials to the repository.
- Add the non-secret ConfigMap, metrics Service, scrape annotations, resource
  settings, security context, read-only credential mount, and termination
  grace period.
- Enforce an immutable image digest for non-local releases.
- Require an explicit teardown approval value and exact keyspace confirmation.
- Keep teardown out of Argo hooks and out of the soak Deployment lifecycle.
- Document values promotion, Argo health gates, status inspection, log
  collection, Prometheus verification, evidence retention, and manual
  cleanup.

**Acceptance**

- The Docker image starts directly with the expected subcommand.
- The Chart passes the repository's Kubernetes validation Make target.
- Seed and Teardown Jobs stay below the one-day platform limit.
- The Soak Deployment runs until its phase changes to `stopped`.
- A failed/evicted pod restarts with an empty catalog and resumes the same
  continuous run.
- Teardown requires both the run ID and exact keyspace confirmation.
- No Chart template contains a staging hostname, credential, or
  environment-specific namespace.

**Commit**

`feat(loadgen): add Kubernetes assets for Cassandra soak`

---

## Task 17: Validate the Hybrid Local Service Path

**Files**

- Add `tools/loadgen/deploy/k8s/values-local.yaml`.
- Extend `tools/loadgen/deploy/k8s/README.md`.
- Update Run A catalog, mutation, read, verification, and runtime wiring where
  the real service-path smoke exposes contract drift.
- Make `docker-local/setup.sh` accept a Windows-safe temporary output root
  without changing its Linux/macOS default.

**Red**

- Lint the Chart with the local values profile.
- Confirm kind accepts the Seed Job, Soak Deployment, stopped state, and
  Teardown Job through Kubernetes API discovery.
- Run the real message-gatekeeper → message-worker → Cassandra path and the
  history-service read path at a low local rate.
- Add failing tests for every runtime contract mismatch found by the smoke;
  do not hide real RPC failures by lowering assertions.

**Green and refactor**

- Keep the local profile non-secret and explicitly allow its mutable image tag
  only for kind.
- Preserve pinned run-owned messages in the bounded catalog and rebuild pin
  state through pinned-list RPCs after a Pod restart.
- Use a reaction shortcode accepted by the history-service wire contract.
- Reconcile reaction toggles from the successful server response after a
  process-local catalog restart.
- Decode history responses through a narrow wire projection instead of the
  Cassandra storage-only reaction map.
- Send a current `lastMsgAt` room hint so zero-history synthetic Mongo rooms do
  not collapse the history-service bucket walk to the Unix epoch.
- Restrict LoadHistory verification to top-level messages and begin its
  timestamp walk at the selected message boundary.
- Keep scheduled reads to one page so `SOAK_READ_RATE` remains an RPC rate;
  reserve multi-page bucket walks for the independent verifier.
- Retain failed Seed Job Pods so Argo operators can inspect application logs.
- Document the exact local lifecycle and the boundary between what kind proves
  and what remains staging-only.

**Acceptance**

- The Seed Job borrows the expected local users and creates only run-owned
  topology.
- The Soak Deployment passes the encryption preflight, persists ciphertext
  with no plaintext `msg`, and exposes per-action Prometheus counters.
- Reaction, pin/unpin, and history verification operate without systematic
  contract errors after the corrected image is deployed.
- The measured read RPC rate is not multiplied by pagination depth.
- SIGTERM prints a process-local report and a replacement Pod resumes the same
  continuous run after a fresh warm-up.
- The stopped phase removes the Deployment; approved teardown preserves
  borrowed users and removes only run-owned Mongo topology.
- No result from this low-rate Docker Desktop run is presented as a Cassandra
  capacity conclusion.

**Commit**

`test(loadgen): validate the local Cassandra soak lifecycle`

---

## Final Validation

Run the following after the last task:

```text
make generate SERVICE=tools/loadgen
make lint
make test SERVICE=pkg/subject
make test SERVICE=tools/loadgen
make test-integration SERVICE=tools/loadgen
make test
make coverage-loadgen-soak
```

The scoped coverage target must report at least 80% statement coverage across
the Run A `soak_*.go` files using unit and integration tests. It must also
report at least 90% meaningful coverage for the Run A core after excluding
the CLI/environment boundary (`soak_main.go`) and Mongo adapter
(`soak_store.go`). The existing loadgen modes share the same large
`package main`; their package-wide percentage is tracked as a no-regression
baseline rather than requiring this Cassandra-only change to repay unrelated
legacy coverage debt.

Also run the Helm/Kubernetes validation Make target added in Task 16 and
build the existing loadgen Dockerfile.

---

## Pre-Run Operational Gate

Implementation completion does not itself authorize a staging run. Before
creating the seed Job, the operator must confirm:

- the exact staging site and NATS account;
- the borrowed-user filter and count;
- room count and MongoDB write budget;
- a Cassandra keyspace dedicated to this test and configured identically on
  `message-worker` and `history-service`;
- `MESSAGE_BUCKET_HOURS` consistency;
- `ATREST_ENABLED=true` and working Vault/KMS dependencies;
- enough free Cassandra disk for the approved maximum observation window plus
  safety margin;
- Prometheus scraping of loadgen and access to L2/L3 dashboards;
- the evidence-retention and teardown time;
- awareness that NATS, Elasticsearch, and Valkey may receive test side
  effects that this teardown does not automatically remove.

If any gate is missing, seed or soak should fail before generating load.
