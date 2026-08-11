# NATS / JetStream Failure Testing and Loadgen Coverage Plan

> Inventory date: 2026-08-10. This document treats the current code, `pkg/stream`, service consumer configurations, and `loadgen` implementation as authoritative. Where an existing traffic-estimation document differs from the code, the code takes precedence.

## 1. Executive Summary

Loadgen can currently drive traffic through the following primary paths:

- User messages: `MESSAGES -> message-gatekeeper -> MESSAGES_CANONICAL`, including a subset of downstream message, broadcast, and notification outcomes.
- Room membership: `ROOMS -> room-worker`.
- Synchronous request/reply: history, read receipts, room/thread reads, and selected room/user operations.
- Presence: hello, ping, activity, bye, and an application-level presence storm.
- Long-running data consistency: soak mode reads through the history API and verifies edit/delete/pin/reaction/thread outcomes.

However, the current implementation is **not sufficient to claim complete NATS / JetStream failover validation**:

1. Loadgen has no federation / OUTBOX / remote INBOX, real bot pipeline, push delivery, search, HR, migration, or Teams pipeline scenarios.
2. Loadgen has no per-operation outcome ledger. Some modes calculate latency from successful replies only, so timed-out or permanently missing outcomes may not be included in the failure rate.
3. The `daily` NATS connection pool uses the default reconnection behavior of raw `nats.Connect` and does not expose disconnect/reconnect/closed metrics. During a long outage, loadgen may lose its own connection first and produce incorrect fault attribution.
4. Loadgen does not currently inject faults. Kubernetes, Chaos Mesh, network policies, traffic control, or NATS management tooling must inject faults while loadgen continues generating traffic and validating outcomes.
5. The local `docker-local` environment runs a single NATS node. It can test a complete outage or restart, but it cannot validate JetStream RAFT leader failover, quorum loss, rolling node failover, or a cross-site gateway partition.

The recommended approach has two stages: first complete the P0 loadgen observability and scenario work in Section 8, then execute the campaigns in Section 7 in staging with at least three NATS nodes per site and at least two sites.

## 2. Shared Connection, Retry, and Health Behavior

| Area | Current behavior | Failure-test implication |
|---|---|---|
| Initial connection | Most services exit when the initial NATS connection, JetStream context, stream verification, or consumer creation fails | A deployment or restart during a NATS outage can crash-loop. Test an outage during steady state separately from startup during an outage |
| Core NATS reconnect | `pkg/natsutil.Connect` sets `MaxReconnects(-1)` with a two-second reconnect interval | A running connection keeps reconnecting, but request/reply and publish operations are not guaranteed to succeed |
| Core publish | There is no server PubAck; publishes during a disconnect rely on the client reconnect buffer | A full buffer fails synchronously. Publish success only means that the operation entered the client/server path, not that a subscriber processed it |
| Request/reply | Most calls have one timeout and no application-level retry | Failover can produce timeouts or no responders. Side-effecting RPCs must also verify idempotency when the request ran but the reply was lost |
| Readiness | `RECONNECTING` is considered ready; only `DISCONNECTED/CLOSED` is considered not ready | A pod can remain ready indefinitely while reconnecting and unable to provide NATS-backed functionality |
| Default JetStream consumer | AckWait 30 seconds, MaxDeliver 5, MaxAckPending 1000 | Most transient failures still exhaust after five deliveries; retries are not unlimited |
| Shared JS retry | Default backoff: 1s, 5s, 30s, 2m. Low-latency backoff: 200ms, 1s, 5s, 30s | With MaxDeliver 5, a final failure stops delivery. Max-delivery advisories and unresolved outcomes must be monitored |
| Consumer iterator | The NATS client can continue through ordinary reconnects and leader changes. On terminal errors such as consumer deletion, most workers exit the goroutine | The process and readiness can stay green while nobody consumes the durable. Consumer deletion and recreation must be tested |
| Panic guard | Most per-message handler panics Ack and drop the message; a search batch panic leaves messages unacknowledged | Panic/drop outcomes must be part of event-integrity and terminal-outcome checks, not only service uptime |

### Identified High-Risk Retry and Loss Behavior

| Priority | Location/path | Current behavior and risk |
|---|---|---|
| P0 | message-gatekeeper | A failed canonical publish uses immediate `Nak()` while the consumer has the default MaxDeliver 5. A NATS fault can exhaust deliveries in a very short time |
| P0 | inbox-worker | Transient failures use immediate `Nak()` with the default MaxDeliver 5. Cross-site events can exhaust rapidly, and the ordered membership lane can stop progressing |
| P0 | Most workers | Comments or design semantics imply retry-until-recovered, but consumer MaxDeliver is still 5. Message, broadcast, notification, room, search-sync, and bot paths all need an explicit exhaustion outcome |
| P0 | broadcast-worker DM and partial thread fanout | Individual Core NATS publish failures are logged while processing continues and the canonical event is Acked. A partially successful fanout may also Ack, leaving failed recipients without retry and causing silent loss |
| P0 | room-worker post-write side effects | Some subscription/client/local INBOX publish failures are logged and swallowed. Mongo may be updated while the corresponding event is missing |
| P0 | Consumer-loop terminal error | Most workers return from `Next()` terminal errors without recreating the consumer. The process remains alive and readiness may still pass |
| P0 | Loadgen daily connection pool | It uses the raw default reconnect policy without connection-state telemetry, which can misclassify a generator failure as a system-under-test failure |
| P1 | Request/reply side effects | Calls have a single timeout and no uniform retry/idempotency contract. A client retry after reply loss can duplicate side effects |
| P1 | Startup | Startup during a fault usually fails fast. Orchestrator backoff, recovery time, and cascading restarts require validation |
| P1 | Observability | There are no uniform max-delivery, consumer-loop-stopped, reconnect-buffer-full, publish-retry, or publish-exhausted metrics and alerts |

Exceptions: the primary `outbox-worker` and `hr-sync-worker` consumers use `MaxDeliver=-1`. Migration consumers use 1000 deliveries for most events and 60 for deletes, then `Term` and record metrics on the last attempt. These paths still require recovery, backlog, and terminal-outcome testing, but they do not carry the default five-delivery risk.

## 3. JetStream Path Inventory and Loadgen Coverage

Coverage labels:

- **Covered**: loadgen can directly generate traffic for the path and observe at least one business outcome.
- **Partial**: loadgen can trigger the path but lacks complete outcome, backlog, cross-site, or downstream validation.
- **Not covered**: loadgen has no corresponding scenario.

| Stream/path | Primary producer | Primary consumer | Retry/Ack considerations | Loadgen coverage |
|---|---|---|---|---|
| `MESSAGES_{site}` | User Core NATS publish | message-gatekeeper | Gatekeeper transient errors use immediate Nak; MaxDeliver 5 | **Covered**: `run`, `max-rps messages`, `daily`, `soak`, `max-room-size` |
| `MESSAGES_CANONICAL_{site}` | message-gatekeeper, history-service mutations, room-worker system events, migration transformer | message-worker, broadcast-worker, notification-worker, search-sync-worker | Most consumers use MaxDeliver 5 and redeliver independently; downstream side effects must be idempotent | **Partial**: message/soak verifies message persistence and some broadcasts; only message-worker and broadcast-worker backlog is sampled |
| `MESSAGES_TEAMS_{site}` | Teams message lane | message-worker, search-sync-worker | Finite retries by default | **Not covered** |
| `ROOMS_{site}` | room-service | room-worker | JS PubAck; room-worker MaxDeliver 5; multiple side effects follow the DB write | **Covered/partial**: members and daily add/create; not all room-event outcomes are verified |
| `ROOMS_TEAMS_{site}` | Teams room creation | room-worker | Same as above | **Not covered** |
| `PUSH_NOTIFICATION_{site}` | notification-worker | push-notification-service | Producer uses synchronous PubAck; consumer MaxDeliver 5; current dispatcher is a log dispatcher | **Not covered**: no recipient-correlated push observer |
| `INBOX_{site}` external | Direct remote-site publish; outbox-worker forwarding | inbox-worker, search-sync-worker depending on event lane | inbox-worker uses immediate Nak plus MaxDeliver 5; membership has a sequential lane | **Not covered**: no cross-site traffic or verification |
| `INBOX_{site}` internal | Same-site search feeds | search-sync-worker | MaxDeliver 5; batch Fetch retries, but a deleted consumer is not recreated | **Not covered**: no search eventual-consistency verifier |
| `OUTBOX_{site}` | room-service, room-worker, bot-room-service | Per-peer concurrent and ordered outbox-worker consumers | MaxDeliver=-1; ordered lane MaxAckPending=1; Ack after remote PubAck | **Not covered**: no remote peer, healthy-peer isolation, or ordering verification |
| `BOT_MESSAGES_CANONICAL_{site}` | bot-message-handler | bot-message-worker, broadcast-worker, notification-worker, search-sync-worker | Most consumers use MaxDeliver 5 | **Not covered**: `max-room-size` uses the normal user-message front door, not the bot pipeline |
| `BOT_PUSH_NOTIFICATION_{site}` | Bot notification path | push-notification-service | MaxDeliver 5 | **Not covered** |
| `HR_{site}` | HR producer | hr-sync-worker, search-sync-worker | hr-sync uses MaxDeliver=-1; search-sync remains finite | **Not covered** |
| `MIGRATION_OPLOG_{site}` | oplog-connector | oplog transformer, direct transfer, collections transformer | Most events use 1000 deliveries, deletes use 60, with a 2s delay and final `Term` plus metric | **Not covered** |

## 4. Service-Level NATS Scenarios and Loadgen Coverage

### 4.1 Messages, Notifications, and Search

| Service | NATS / JetStream scenario | Loadgen coverage | Primary gap |
|---|---|---|---|
| message-gatekeeper | Consumes MESSAGES, validates, publishes to canonical with PubAck, replies to sender | **Covered** | Gatekeeper durable is not monitored; immediate Nak exhaustion and reply loss are not verified |
| message-worker | Consumes user/bot/Teams canonical events, writes Cassandra, and emits thread/outbox side effects | **Partial** | Message backlog sampling and soak read-back exist; bot/Teams/outbox side effects are not verified |
| broadcast-worker | Consumes canonical and fans out Core NATS events to rooms/users | **Partial** | E2 covers only part of broadcasting; DM/partial fanout silent loss and mutation/reaction/thread-badge outcomes are incomplete |
| notification-worker | Consumes canonical, publishes to the push stream with PubAck, consumes ROOMS mute invalidation | **Partial** | Message load triggers it indirectly, but backlog/outcome and mute invalidation are not monitored |
| push-notification-service | Consumes push streams and dispatches user/bot notifications | **Not covered** | No push observer or recipient correlation; the current implementation is not a real provider |
| search-sync-worker | Consumes canonical, Teams, INBOX, and HR events and performs bulk indexing | **Partial/not covered** | Traffic may trigger it indirectly, but loadgen does not query search correctness or sample the durable |
| search-service | Core NATS request/reply for message/room/app/user/org search | **Not covered** | No query-availability or index-convergence SLO |
| history-service | Multiple Core request/reply reads and mutations; some mutations publish canonical events | **Covered/partial** | Soak and focused modes cover the core, but not the full RPC surface or systematic post-timeout side-effect idempotency |

### 4.2 Rooms, Users, and Federation

| Service | NATS / JetStream scenario | Loadgen coverage | Primary gap |
|---|---|---|---|
| room-service | Multiple room Core request/reply handlers, PubAck to ROOMS/OUTBOX, Core client updates | **Partial** | Members/daily cover only subsets such as add/create/mute; OUTBOX and the full RPC surface are not verified |
| room-worker | Consumes ROOMS/Teams, writes Mongo, publishes canonical/Core/OUTBOX/INBOX events | **Partial** | Members covers the primary path; post-write side effects and Teams are not verified |
| outbox-worker | Per-peer concurrent/ordered consumers forwarding to remote INBOX | **Not covered** | Peer outage, healthy-peer isolation, FIFO ordering, duplicate handling, and convergence are not verified |
| inbox-worker | Consumes remote external events with a sequential membership lane | **Not covered** | Immediate Nak, MaxDeliver exhaustion, cross-site convergence, and ordering are not verified |
| user-service | Multiple user/subscription/app/thread Core request/reply handlers, Core events, and cross-site event publishing | **Partial** | Daily covers a small subset of user/subscription operations; full request/reply, cross-site, and reply-loss idempotency are not verified |
| user-presence-service | hello/ping/activity/bye/manual/query and state broadcast | **Covered** | Sustained/storm/capacity modes exist; storm models application-level silent clients, not an actual TCP/NATS failover |
| user-presence-service/sync | Cross-site/state reconciliation over Core NATS | **Partial** | Presence traffic can trigger it indirectly, but loadgen has no dedicated sync-convergence, peer-partition, or replay assertion |
| admin-service | HTTP handler calls the NATS restricted-room request/reply endpoint | **Not covered** | HTTP behavior, retry, and error classification for NATS timeouts are not verified |

### 4.3 Bot, Teams, HR, Migration, and Other Request/Reply Services

| Service | NATS / JetStream scenario | Loadgen coverage | Primary gap |
|---|---|---|---|
| botplatform-service | Bot platform request/reply and bot workflow | **Not covered** | No bot request/reply or callback traffic |
| bot-message-handler | Receives bot-message requests and publishes to bot canonical with PubAck | **Not covered** | No bot front-door or reply verifier |
| bot-message-worker | Consumes bot canonical events | **Not covered** | No persistence or side-effect verifier |
| bot-room-service | Bot room request/reply; publishes bot system events and OUTBOX events | **Not covered** | No room/federation bot scenario |
| teams-room-creation | Teams room creation and NATS path | **Not covered** | The complete Teams room lane is missing |
| teams-hr-sync | HR/Teams NATS workflow | **Not covered** | No HR producer/result verifier |
| hr-sync-worker | Consumes HR events and synchronizes domain/search state | **Not covered** | Backlog and recovery for MaxDeliver=-1 are not verified |
| oplog-connector / transformers | Publish/consume MIGRATION_OPLOG, call history request/reply, publish canonical/INBOX | **Not covered** | Long retries, Term, checkpoints, and replay consistency are not verified |
| media-service | Emoji Core request/reply | **Not covered** | Failover availability is not verified |
| translation-service | Translation Core request/reply | **Not covered** | Failover availability is not verified |

### 4.4 Paths Without a Direct NATS Data Connection

- auth-service issues and validates NATS user JWTs but does not directly connect as a normal data-path client. NATS callout/auth-node failures and new connection behavior still require testing. Loadgen currently uses an auth stub and does not cover real per-user JWT connections.
- portal-service returns the NATS URL and related connection information to clients but does not directly connect to NATS.
- upload-service, tcard-service, and other unlisted inspect/verify services currently have no identified direct NATS / JetStream client path. They may still be indirectly affected by failures in upstream NATS-dependent services.

## 5. What Existing Loadgen Modes Can Answer

| Mode | Traffic generated | Validation available | Failure-test limitation |
|---|---|---|---|
| `run` | Message front door or canonical injection | Gatekeeper reply, partial broadcast, message/broadcast pending samples | Canonical injection bypasses gatekeeper; other durables and missing outcomes are absent |
| `max-rps messages` | Stepped message throughput | Successful latency, message/broadcast pending | Successful-sample bias; missing results may not be counted as failures |
| `max-rps thread` | Thread pressure | Partial thread results | Downstream/federation/search/push validation is incomplete |
| `daily` | Mixed send/read/history/subscription-list/member-add/room-create/mute traffic, optionally presence | Aggregate operation errors and before/after pending deltas for all durables | A thread-reply helper exists but is not in the action mix; notification pending growth is exempted; no outcome ledger; insufficient connection-pool resilience |
| `soak` | Continuous messages plus edit/delete/pin/unpin/reaction/thread operations | Data consistency through history/Cassandra read-back | Single-site only; no push/search/federation; some mutation retries alter the original failure distribution |
| `members-*` | room-service front door or canonical ROOMS injection | room-worker backlog and member throughput | Not all side effects are verified; canonical mode bypasses room-service |
| `history-*` / read modes | History/read-receipt/room-read/thread-read request/reply | RPC latency and error | Only selected endpoints; no side-effect ledger for reply loss |
| `presence-*` | Sustained presence, capacity, and a thundering herd after bye/silent timeout | Presence operation outcomes | Does not inject an actual NATS network disconnect; it must run with an external fault injector |
| `max-room-size` | Normal user messages to a large room | Large-fanout pressure | Despite the bot-related implementation name, it uses MESSAGES/gatekeeper rather than the BOT canonical pipeline |

### Recommended Combination of Existing Modes

Before new scenarios are implemented, the most useful baseline combination is:

1. `daily` to model the general product operation mix.
2. `soak` for message mutations and historical data-consistency read-back.
3. `presence-sustained` to maintain long-lived connections and presence traffic.
4. Low-rate `members-sustained` traffic through ROOMS and room-worker.
5. A separate `run` observer for gatekeeper replies and broadcast E2.

These modes must use separate site/tenant/account scopes so their teardown operations do not interfere. Actual ratios should be calibrated from production telemetry rather than copied from default weights.

## 6. Test Environment and Observability Prerequisites

### 6.1 Environment

- At least three NATS JetStream nodes per site, with stream replicas matching production.
- At least two sites with production-equivalent gateway/supercluster routing.
- Dedicated test tenant/site IDs and a fixed seed that supports post-test data reconciliation.
- Separate loadgen and fault-injector deployments. Loadgen must not share a single failure domain with the NATS node or service pod being terminated.
- Synchronized clocks so fault events, loadgen operations, and service logs share one time base.
- The single-node `docker-local` environment is only a smoke-test target and is not accepted for failover validation.

### 6.2 Required Observability

NATS / JetStream:

- Server connections, routes/gateways, leader/quorum, storage, memory, slow consumers, reconnect buffer, and publish errors.
- Per-stream and per-consumer leader, replicas, num_pending, num_ack_pending, redelivered, ack floor, and last active.
- Oldest pending age, with queries deduplicated by the current-leader label to avoid counting replicas multiple times.
- Max-delivery advisories, consumer deletion, stream unavailable, RAFT election, and quorum events.

Services:

- Connect/disconnect/reconnect/closed counts and durations.
- JetStream publish attempts, retries, exhaustion, Ack/Nak/Term, and a consumer-loop-running gauge.
- Separate request/reply timeout, no-responders, and reconnect-buffer-full classifications instead of one aggregate error counter.
- Per-consumer processing latency, business success, duplicate/idempotent replay, and terminal drop.
- Readiness that distinguishes connected, reconnecting, consumer-loop-dead, and dependency-unavailable states.

Loadgen:

- A unique operation ID, start time, deadline, and Ack/reply/event/read-back outcome for every operation.
- Four mutually exclusive outcomes: `eligible`, `good`, `bad`, and `missing_after_deadline`, plus run-window deltas.
- Loadgen's own NATS connection state, reconnect count, buffer errors, CPU, memory, and socket errors.
- Separate statistics for warmup, measurement, fault, and recovery/settle phases.

If any loadgen shard closes, saturates its resources, or cannot reconnect, the affected interval must be marked **inconclusive** rather than attributed directly to a service.

## 7. Failure Campaigns

Every campaign uses four phases: stable warmup for at least two maximum retry windows, measurement baseline, fault injection, and recovery/settle followed by data reconciliation. Fault duration must cover both a short election and a long outage exceeding the two-minute backoff and 30-second AckWait.

| ID | Fault injection | Concurrent traffic | Primary expectations and validation |
|---|---|---|---|
| F01 | Stop follower NATS nodes one at a time | daily + soak + presence + members | No data loss, no unnecessary leader change, and no closed service or loadgen connections |
| F02 | Stop the current stream leader | Same mix with elevated traffic for the selected stream | Publish/consume resumes after RAFT election; temporary timeouts are attributable; backlog returns to baseline |
| F03 | Stop the consumer leader or the node hosting the consumer | Path-specific traffic | Consumer rebinds, ack floor advances, and the handler goroutine does not stop permanently |
| F04 | Rolling restart of the complete NATS cluster | Production-like mix | Services reconnect, no reconnect-buffer overflow, and pods started during the fault eventually recover |
| F05 | Network partition between one service pod and NATS | Isolate one service role per run | Precisely validate that service's publish/consume retries; no cascade to other services; readiness and alerts are correct |
| F06 | Loss of majority / JetStream quorum | Separate high- and low-rate runs | Writes fail or wait explicitly rather than reporting false success; redelivery, deduplication, and data converge after recovery |
| F07 | Short and long complete site-level NATS outage | Steady production-like mix | Short fault crosses reconnect buffering; long fault has no silent loss; loadgen remains attributable; recovery meets RTO |
| F08 | Gateway partition between site A and site B | Cross-site membership/rename/subscription/message traffic after loadgen enhancement | Durable OUTBOX retry, per-peer isolation, ordered-lane FIFO, and eventual remote INBOX convergence |
| F09 | Site B down while site C remains healthy | Multi-peer federation after loadgen enhancement | Parked forwards for B do not block C; pending and oldest age remain independent per peer |
| F10 | NATS storage latency, disk pressure, or full disk | JetStream-heavy mix | PubAck latency/errors are explicit; services do not OOM or spin; backlog drains after recovery; no false Ack |
| F11 | Delete a durable consumer, then recreate it through IaC/operator workflow | Corresponding worker traffic | Consumer-loop-dead alerts; service self-heals or clearly requires restart; no permanently idle consumer behind a green process |
| F12 | Delete or temporarily remove stream availability in an isolated environment | Producer and consumer traffic | Producer errors, startup/recovery, and stream ownership/IaC behavior match expectations; non-owners do not silently create production streams |
| F13 | Restart a service while NATS is still unavailable | Rotate through each service role | Crash-loop behavior is controlled; after NATS recovery the pod becomes ready and recreates its durable/iterator |
| F14 | Break the reply path after the request has executed | Side-effecting create/add/mute/edit/delete request/reply after ledger enhancement | Client retries do not create unintended duplicates; final state is unique and observable |

### Per-Campaign Procedure

1. Record NATS topology, stream/consumer leaders, server and pod versions, and loadgen seed.
2. Establish a pre-test data baseline and confirm all durable pending counts are near normal steady state.
3. After warmup, freeze the measurement-start counters and continuously record per-operation outcomes.
4. Inject exactly one fault and record precise start/end timestamps and the target node, link, or service.
5. Keep loadgen running during the fault and label SUT errors separately from loadgen connection errors.
6. Remove the fault and wait for settle: all target operations reach their deadlines, backlog returns to baseline, and oldest pending age returns to zero or steady state.
7. Reconcile independently through history, Mongo, Cassandra, search, the remote site, and the push observer. JetStream pending=0 alone is insufficient.
8. Export a report containing the fault timeline, SLOs, missing IDs, redeliveries/duplicates, terminal advisories, and recovery time.

## 8. Loadgen Enhancement Backlog

### P0: Required Before Formal Failure Campaigns

Implementation status on 2026-08-12: Cassandra soak now exposes its NATS
connected state, lifecycle events, and outage duration, and its user-message
lane has a PVC-backed admission-to-history operation ledger. These are shared
building blocks, but the daily connection pool, other loadgen modes, complete
durable sampler, terminal advisories, and federation observers below remain
open. See [Loadgen Failure Observation Runtime](loadgen-failure-observation.md).

1. **Shared resilient connection and self-monitoring**
   - Give every loadgen pool a consistent, explicit reconnect policy.
   - Expose disconnect/reconnect/closed, buffer-full, last-connected-server, and reconnect-duration telemetry.
   - Automatically mark an interval inconclusive when the generator fails.

2. **Per-operation outcome ledger / assertion mode**
   - Persist operation ID, lane, deadline, expected event, and final read-back for each operation.
   - Count eligible/good/bad/missing-after-deadline outcomes. Percentiles must not use successful samples only.
   - Keep deadline-based reconciliation running continuously so late recovery
     is not lost when an externally injected fault ends.

3. **Complete durable sampler**
   - Include all enabled gatekeeper, message, broadcast, notification, push, room, inbox, outbox, search-sync, bot, HR, and migration durables.
   - Sample pending, ack pending, redelivery, ack floor, and oldest pending age. Treat a missing consumer as a failure rather than skipping it.

4. **Federation scenarios**
   - Add two-site and three-site modes for member add/remove/rename, subscription state, and messages.
   - Verify OUTBOX per-peer isolation, ordered FIFO behavior, remote INBOX outcomes, and deduplication.

5. **Service outcome observers**
   - Broadcast: per-recipient outcomes covering DM/channel/thread/reaction/mutation.
   - Search: message/room/user queries that verify eventual convergence.
   - Push: a fake dispatcher or observer that records recipient-correlated delivery.
   - Monitor the actual bot canonical pipeline rather than substituting normal user messages.

6. **Real authentication and connection churn**
   - Connect with per-user JWT/NKeys issued by auth-service.
   - Test NATS auth callout, token expiry, reconnect re-authentication, and large-scale client reconnects.

### P1: Broader Product Surface

- Add the existing but unused thread-reply helper to the daily action mix.
- Add Teams, HR, and migration traffic with checkpoint and terminal-outcome verification.
- Add the complete admin/media/translation/search and room/user/history request/reply surface.
- Load action-rate profiles from production telemetry rather than using only fixed built-in ratios.
- Produce a machine-readable run manifest and fault timeline so Grafana, traces, and logs can align on the run ID.

### Responsibility Boundary Between Loadgen and the Fault Injector

Loadgen should not receive node-deletion privileges for a production-like cluster. An external orchestrator should execute faults and send events to the shared report through a run ID and timestamp API or file. This prevents the traffic generator from also becoming the fault-control plane and reduces operational risk and attribution ambiguity.

## 9. Acceptance Criteria

The following are common hard gates for every campaign. Exact latency and error-budget values continue to come from `docs/load-testing/system/sli-slo.md`. New federation/search/push metrics require a baseline before final thresholds are approved.

- Every operation accepted by the system must produce success, an explicit failure, or a queryable terminal outcome. `missing_after_deadline` must be zero unless the campaign explicitly permits it under an approved error budget.
- No silent drop may exist only in a log without a counter or advisory. MaxDeliver exhaustion must be enumerable by operation/event ID.
- After fault removal, target stream/consumer pending, ack pending, and oldest pending age return to pre-fault steady state, with recovery time recorded.
- Final state converges across Mongo, Cassandra, history, search, the remote site, and push. At-least-once duplicate delivery is acceptable, but business state must be idempotent with no duplicate membership, notification, or side effect.
- OUTBOX ordered lanes must not allow add/remove/rename overtaking. A failed peer must not block a healthy peer.
- Every consumer loop remains active after recovery. A missing consumer, terminated iterator, or falsely green readiness is a failure.
- Loadgen itself has no closed connection, resource saturation, or outcome-recorder loss. Otherwise, the interval is inconclusive.
- Startup during a fault and restart after a fault both become ready within the approved RTO without manual message deletion or data repair.

## 10. Recommended Execution Order

1. Implement P0 loadgen connection telemetry, the outcome ledger, and the complete durable sampler.
2. Run an outage/restart smoke test in the local single-node environment and confirm that reports and missing outcomes fail correctly.
3. Run F01-F07 and F10-F13 in a single-site, three-node staging environment.
4. After implementing federation observers, run F08-F09 in two-site and three-site environments.
5. After implementing the side-effect ledger, run F14 for create/add/mute/edit/delete operations.
6. Assign an owner and remediation or explicit risk acceptance to every failure, then rerun the same seed and fault timeline as a regression test.

## 11. Minimum Contents of Every Test Report

- Git SHA, image/tag, NATS version and topology, stream replicas, site IDs, and loadgen seed/profile.
- Baseline/fault/recovery timeline and injection target.
- Per-lane eligible/good/bad/missing, p50/p95/p99, and maximum recovery latency.
- Per-consumer pending/ack-pending/redelivery/oldest-age/ack-floor charts.
- Disconnect/reconnect/closed, publish retry/exhausted, and MaxDeliver advisories.
- Duplicate-event and data-reconciliation results with all missing/duplicate operation IDs.
- Pass/fail/inconclusive conclusion, service owner, and follow-up issue.

## 12. Primary Code Inventory Sources

- Connection and health: `pkg/natsutil/connect.go`, `pkg/natsutil/health.go`.
- Consumer defaults and retries: `pkg/stream/consumer.go`, `pkg/jsretry/jsretry.go`.
- Stream and lane definitions: `pkg/stream/stream.go`, `pkg/stream/pipeline.go`, `pkg/outbox/outbox.go`.
- Immediate Nak paths: `message-gatekeeper/handler.go`, `inbox-worker/main.go`.
- Federation retries: `outbox-worker/main.go`, `hr-sync-worker/main.go`.
- Core fanout loss semantics: `broadcast-worker/handler.go`, `room-worker/handler.go`.
- Loadgen consumer sampling and connection pools: `tools/loadgen/consumerlag.go`, `tools/loadgen/maxrps_messages.go`, `tools/loadgen/daily_pool.go`.
- Loadgen action mix and botroom path: `tools/loadgen/daily_user.go`, `tools/loadgen/daily_actions.go`, `tools/loadgen/botroom.go`.
- Current SLO assertion design: `docs/load-testing/system/sli-slo.md`.
- Local single-node NATS: `docker-local/nats.conf`, `docker-local/compose.deps.yaml`.
