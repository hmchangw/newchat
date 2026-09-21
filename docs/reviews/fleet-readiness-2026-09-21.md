# Fleet production readiness — 2026-09-21

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `d22004d` (base `main`)  
**Scope:** all 35 Go services in the monorepo  
**Fleet score:** **3.09 / 5** (2026-09-01 baseline: 3.18, Δ −0.09)  
**Method:** 210 independent expert reviews — six dimensions × 35 services, each reviewer
working alone against `CLAUDE.md` and Go-at-scale practice, every finding citing
`file:line` on this tree. Per-service reports are the `*-readiness-2026-09-21.md`
files beside this one.

## Executive summary

This is the second full-fleet production-readiness pass, three weeks after the
2026-09-01 baseline. Every service was reviewed by six independent expert
subagents — one per dimension, none seeing another's work — against `CLAUDE.md`
first and Go-at-scale practice second, with every finding required to cite
`file:line` on this tree.

**The fleet's engineering is solid and its verification is not.** Code quality
averages 3.6, architecture 3.5 and performance 3.4; test coverage averages
**1.5**. That spread is the single most important number in this report, and the
per-service evidence says it is a *measurement and CI* failure more than a
testing failure: correctly-written, correctly-tagged integration tests exist
across the fleet and **no service pipeline runs them**, while every pipeline
writes a coverage profile that nothing reads. Several services move from the
mid-50s to comfortably past the 80% floor on counting alone.

**Most of the codebase is in good shape.** `gosec` at medium+ is clean repo-wide
and the 20 repo-owned semgrep rules are clean across 1,978 files with their
fixture tests passing. The house patterns hold where they matter: `pkg/jsretry`
settle paths with no bare `Nak()`, `pkg/stream.DurableConsumerDefaults` rather
than hardcoded backoff, opt-in stream bootstrap, consumer-defined store
interfaces, explicit Mongo projections, `pkg/subject` builders on the hot paths.
Mocks are not stale anywhere.

**Where things break, they break quietly.** The dominant severity-raising
pattern this cycle is not a crash — it is work that fails and reports success. A
push lane wired to a logger instead of a push provider. A room-membership
pipeline that treats an empty API response as an instruction to unsubscribe
everyone. An HR feed that one duplicate key stalls indefinitely with no DLQ. A
CronJob that exits 0 when every batch failed. Subject routing on string literals
whose `default` branch Ack-drops an entire feed. These are the findings to act
on first, because none of them will page anyone.

**The batch/CronJob tier is the weakest cohort.** The seven `teams-*` jobs plus
`hr-sync-worker` carry most of the critical findings, and they share their
defects rather than each having their own: a Graph config block copy-pasted six
times with a TLS-verification default that has already diverged, a SIGTERM
handling bug repeated in six jobs, index ownership that sits with readers
instead of writers, and no per-operation timeouts anywhere.

**On the score movement**: 17 services score lower than on 2026-09-01, 10 higher
and 8 flat. Read that with care. Very little code got worse in three weeks; what
changed is that this pass went deeper — several regressions are a reviewer
finding a defect that existed in August and was missed, and the coverage
dimension is now applied mechanically against the CLAUDE.md floor where the
first pass was more lenient. The two movements that do reflect real change are
`bot-message-worker` (−0.7), where the thread-partition bug was newly traced end
to end, and `media-service`/`upload-service` (+0.2 each), which absorbed real
fixes. Treat the deltas as a change in visibility, not a trend line.

## Scores

| Service | Overall | Δ vs 09-01 | CQ | Arch | Test | Maint | Integ | Perf | crit | high |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| `roomlist-worker` | **3.7** | 0.0 | 4 | 4 | 2 | 4 | 4 | 4 | 0 | 1 |
| `translation-service` | **3.7** | 0.0 | 4 | 4 | 4 | 4 | 3 | 3 | 0 | 7 |
| `media-service` | **3.5** | +0.2 | 4 | 4 | 2 | 4 | 4 | 3 | 0 | 2 |
| `teams-room-inspector` | **3.5** | +0.2 | 4 | 4 | 1 | 4 | 4 | 4 | 1 | 2 |
| `teams-room-verify` | **3.5** | 0.0 | 4 | 4 | 2 | 4 | 3 | 4 | 0 | 5 |
| `admin-service` | **3.3** | +0.1 | 3 | 3 | 2 | 4 | 4 | 4 | 0 | 4 |
| `auth-service` | **3.3** | +0.1 | 4 | 4 | 2 | 3 | 3 | 4 | 0 | 5 |
| `broadcast-worker` | **3.3** | -0.2 | 4 | 4 | 2 | 3 | 3 | 4 | 0 | 4 |
| `history-service` | **3.3** | +0.1 | 4 | 4 | 1 | 3 | 4 | 4 | 1 | 2 |
| `message-gatekeeper` | **3.3** | 0.0 | 4 | 4 | 2 | 3 | 3 | 4 | 0 | 4 |
| `message-worker` | **3.3** | +0.1 | 4 | 4 | 1 | 3 | 4 | 4 | 1 | 1 |
| `portal-service` | **3.3** | +0.1 | 4 | 4 | 1 | 4 | 3 | 4 | 1 | 3 |
| `teams-chat-member-sync` | **3.3** | +0.1 | 3 | 4 | 2 | 4 | 3 | 4 | 0 | 7 |
| `teams-user-sync` | **3.3** | +0.1 | 4 | 4 | 1 | 4 | 3 | 4 | 1 | 7 |
| `client-update-service` | **3.2** | -0.1 | 4 | 3 | 2 | 4 | 3 | 3 | 0 | 10 |
| `notification-worker` | **3.2** | 0.0 | 4 | 4 | 1 | 3 | 3 | 4 | 1 | 3 |
| `room-worker` | **3.2** | -0.1 | 4 | 3 | 2 | 3 | 4 | 3 | 0 | 5 |
| `tcard-service` | **3.2** | -0.3 | 4 | 3 | 2 | 4 | 3 | 3 | 0 | 6 |
| `teams-chat-sync` | **3.2** | -0.3 | 3 | 4 | 2 | 4 | 3 | 3 | 0 | 11 |
| `teams-room-creation` | **3.2** | -0.3 | 4 | 4 | 1 | 4 | 2 | 4 | 2 | 4 |
| `upload-service` | **3.2** | +0.2 | 3 | 4 | 2 | 4 | 3 | 3 | 0 | 10 |
| `user-service` | **3.2** | -0.1 | 4 | 4 | 1 | 3 | 3 | 4 | 1 | 6 |
| `outbox-worker` | **3.0** | -0.2 | 4 | 3 | 1 | 4 | 3 | 3 | 1 | 4 |
| `room-service` | **3.0** | -0.2 | 3 | 4 | 1 | 3 | 3 | 4 | 1 | 6 |
| `search-service` | **3.0** | -0.3 | 3 | 3 | 2 | 3 | 3 | 4 | 0 | 5 |
| `search-sync-worker` | **3.0** | -0.2 | 4 | 3 | 2 | 3 | 3 | 3 | 0 | 7 |
| `bot-message-handler` | **2.8** | -0.4 | 4 | 3 | 1 | 3 | 3 | 3 | 1 | 8 |
| `botplatform-service` | **2.8** | 0.0 | 4 | 3 | 1 | 3 | 3 | 3 | 3 | 14 |
| `user-presence-service` | **2.8** | 0.0 | 4 | 3 | 1 | 3 | 3 | 3 | 1 | 17 |
| `inbox-worker` | **2.7** | -0.1 | 3 | 3 | 1 | 3 | 3 | 3 | 1 | 10 |
| `push-notification-service` | **2.7** | 0.0 | 3 | 3 | 1 | 3 | 3 | 3 | 1 | 11 |
| `bot-room-service` | **2.5** | -0.3 | 3 | 3 | 1 | 3 | 2 | 3 | 1 | 10 |
| `hr-sync-worker` | **2.5** | -0.3 | 3 | 3 | 1 | 3 | 2 | 3 | 2 | 14 |
| `teams-hr-sync` | **2.5** | -0.3 | 3 | 3 | 1 | 3 | 2 | 3 | 1 | 15 |
| `bot-message-worker` | **1.8** | -0.7 | 2 | 2 | 1 | 2 | 2 | 2 | 1 | 17 |

**Dimension means across the fleet**

| Dimension | Mean | Range |
|---|--:|---|
| Code quality | 3.63 | 2–4 |
| Performance | 3.46 | 2–4 |
| Architecture | 3.51 | 2–4 |
| Maintainability | 3.40 | 2–4 |
| Integration | 3.06 | 2–4 |
| **Test coverage** | **1.51** | 1–4 |

**Severity totals:** 23 critical · 247 high · 618 medium · 604 low · 239 nitpick.

**Movement vs 2026-09-01:** 10 improved, 17 regressed, 8 flat. See the
executive summary for why the regressions are mostly a change in visibility
rather than in the code.

## Top fleet-wide actions

Ranked by expected harm if left alone, not by effort. Each cites the service
report that carries the full evidence.

1. **`push-notification-service` sends no notifications.** The only dispatcher
   wired into production is `LogDispatcher{}`; no APNs or FCM client exists
   anywhere in the repo, so every push event is acked and discarded. The
   service's retry semantics also contradict its own ops contract, which
   mandates ack-on-receipt and `MaxDeliver=1`. Either ship a dispatcher or make
   the gap explicit — today the lane looks healthy and delivers nothing.
2. **An empty Teams roster mass-unsubscribes a room.** `teams-chat-sync` writes a
   zero-member or truncated Graph `$expand=members` response as a complete
   roster; `teams-chat-member-sync` writes `Account: ""` for members missing from
   `teams_user`; `teams-room-creation` forwards both unguarded; and
   `room-worker/teamsroomcreate.go:153-179` reconciles subscriptions to that list
   with a `DeleteMany`. `teams-room-verify` cannot catch it — 0 subs against 0
   accounts verifies as converged — and an integration test pins the empty-roster
   publish as intended. Fix both ends: skip a chat with no resolvable accounts,
   and refuse to delete when `len(wantAccounts) == 0`.
3. **`bot-message-worker` writes bot thread replies to the wrong partition.** It
   binds `thread_room_id` to the parent *room* id while `history-service` reads by
   the Mongo thread-room id, so bot replies are invisible in threads and
   `countAndSetParentTcount` scans the wrong partition and clobbers the user
   pipeline's count. The service is the fleet's worst at 1.8 and also lacks an
   outage retry budget, `jobguard` panic containment, and `USING TIMESTAMP`
   pinning.
4. **One re-homed employee stalls a site's entire HR feed, forever.**
   `hr-sync-worker` keys `hr_employee` by `employeeId` against a unique index on
   `account` that a read-only service owns. The resulting `E11000` is classified
   transient, and with `MaxDeliver=-1` plus `MaxAckPending=1` it redelivers
   indefinitely while every later HR message queues behind it. There is no DLQ
   and no `mongo.IsDuplicateKeyError` branch in the service.
5. **A deactivated account keeps authenticating for ~67 minutes.**
   `admin-service`'s `DeactivateAndRevoke` and `UpdateUserPasswordAndRevoke`
   delete sessions inside the transaction and return only an error, so they never
   bust `sessioncache` — indefinitely while Mongo is down.
6. **Collapse the Graph config block into `pkg/msgraph` and default TLS
   verification on.** Six services re-declare the same knobs; three now default
   `GRAPH_TLS_INSECURE_SKIP_VERIFY` to `true`, sending the OAuth client secret
   over an unverified connection whenever the variable is unset — the Kubernetes
   case. One env-tagged struct fixes all six.
7. **`teams-hr-sync`'s quit batch has no floor.** One group returning HTTP 200
   with an empty `value` — revoked permission, emptied group, `SYNC_GROUPS` typo —
   is diffed as a mass departure and `DeleteMany`'d out of `hr_employee`, which
   `portal-service` left-joins. Add a max-quit ratio and a zero-member abort.
8. **`upload-service` trusts the client's declared `Content-Type`,** defeating the
   default `image/svg+xml` blacklist; the images endpoint validates by filename
   extension only; and neither endpoint caps the request body before spooling to
   disk.
9. **Make the coverage gate real.** Add `-tags integration` to the service
   pipelines and assert the merged profile with the `tools/coveragecheck` the
   Makefile already wires. Today no pipeline runs integration tests and no
   pipeline reads the profile it writes, which is why the fleet's worst dimension
   went unnoticed. Several services jump from ~55% to ~80%+ on counting alone.
10. **`notification-worker` requests a subject no service registers.**
    `subject.PresenceSnapshot` has no handler anywhere, with an incompatible
    payload and batch limit — unfixed across two audit cycles.
11. **`bot-room-service` publishes a bare `model.Message`** onto
    BOT-MESSAGES-CANONICAL where every consumer decodes a `MessageEvent`, and
    writes rooms with legacy `t`/`u` keys and subscriptions missing
    `joinedAt`/`open`/`roles`.
12. **Teach the CronJobs to stop.** Six of seven `teams-*` jobs drain their whole
    backlog after SIGTERM and exit non-zero, making every pod eviction look like
    an outage. One `select` per job.

## Cross-cutting findings

These are the patterns that showed up in three or more services, ordered by what
they would cost to leave alone. Each was found independently by reviewers who
did not see each other's work.

### 1. Test coverage is a structural failure, not a discipline failure

Test coverage is the worst dimension in the fleet by a wide margin — a mean of
**1.55 / 5** against 3.62 for code quality — and exactly one service
(`translation-service`, 81.8%) clears the 80% floor CLAUDE.md §4 declares a
MUST-NOT-MERGE gate. That reads like neglect, but the per-service evidence says
otherwise, and the distinction matters because it changes the fix.

Three structural causes account for most of the gap:

- **No pipeline runs integration tests.** `grep -l "tags integration" */deploy/azure-pipelines.yml`
  returns nothing. Every service's `store_*.go` is therefore dark in the measured
  profile even where it has correct, well-structured testcontainer coverage —
  right build tag, `package main`, `TestMain` → `testutil.RunTests`, containers
  from `pkg/testutil`. `teams-chat-sync` is 67.6% measured and ~80.1% with its
  own integration tests counted; `teams-chat-member-sync` is 60.3% measured and
  ~98% on the surface its tests actually reach.
- **Coverage profiles are produced and then discarded.** Pipelines write
  `coverage-<svc>.out` and assert nothing on it, although `tools/coveragecheck`
  exists and the root `Makefile` already wires it. The drop to 55–60% in several
  services was invisible because nothing was looking.
- **`main.go`'s `run()` is untestable as written and is 20–40% of statements in
  the small services.** In `hr-sync-worker`, `main()` alone is 38% of the package,
  so a unit-only profile caps at 29.7% with perfect tests. Services that extracted
  a `validateConfig` seam (`teams-chat-sync`, `teams-room-verify`,
  `admin-service`) show it costs very little.

Where coverage is *genuinely* missing, it clusters on the riskiest code rather
than the dull code — `teams-hr-sync`'s `WriteStore`, which hand-builds a `$set`
specifically so a full-document replace cannot wipe `roles`/`services`/`password`
on the live auth store, has no test of any kind, unit or integration.

### 2. One shared Graph knob, six declarations, and a split TLS default

`pkg/msgraph.Config` carries no `env` tags, so six services each re-declare
`GRAPH_TENANT_ID`, `GRAPH_CLIENT_ID`, `GRAPH_CLIENT_SECRET`,
`GRAPH_TLS_INSECURE_SKIP_VERIFY` and the three `GRAPH_PROXY_*` knobs — the exact
pattern CLAUDE.md §Configuration forbids, with the exact consequence it predicts.
`GRAPH_TLS_INSECURE_SKIP_VERIFY` now defaults **`true`** in `teams-chat-sync`,
`teams-chat-member-sync` and `teams-user-sync`, and **`false`** in
`teams-hr-sync` and `user-presence-service/sync`.

This is security-relevant rather than merely untidy. `pkg/msgraph` installs
`InsecureSkipVerify` on the single `http.Client` that also POSTs the OAuth
client-secret to the public `login.microsoftonline.com`, so a deployment that
simply omits the variable — which is the Kubernetes CronJob case, since no
manifest in this repo sets it — sends `GRAPH_CLIENT_SECRET` over an unverified
connection. Each service's own compose file ships `false`, so the insecure value
only ever reaches production. gosec is clean because the `#nosec G402` sits in
`pkg/msgraph`, where the default is not visible.

One env-tagged struct in the owning package, defaulted `false` and mounted as a
named field, closes all six at once. It is the highest value-per-line change in
this audit.

### 3. Every CronJob mis-handles SIGTERM the same way

Six of the seven `teams-*` jobs dispatch their entire remaining backlog *after*
the context is cancelled, because no dispatch loop selects on `ctx.Done()`. Each
remaining item then fails fast on the cancelled context, increments a `Failed`
counter, and emits an error line — so the job exits non-zero and a routine pod
eviction is indistinguishable from a Graph or Mongo outage, with an O(backlog)
error storm that buries any real failure. Several of these services carry a
comment in `main.go` claiming the opposite ("aborts between operations instead
of being killed mid-batch"). The fix is one `select` per job plus classifying
`context.Canceled` apart from failure.

### 4. Index ownership is inverted, absent, or borrowed

CLAUDE.md puts index creation in the store constructor or a startup
`EnsureIndexes`. Across the batch services it lands almost anywhere else:

- `hr-sync-worker` **writes** `hr_employee` and declares no indexes at all; the
  only index on the collection — a *unique* one on `account`, which its
  `_id = employeeId` upsert can violate — is created by `portal-service`, a
  read-only consumer whose `EnsureIndexWithRepair` may drop and recreate it.
- `teams-user-sync`'s per-page `$in` on `hr_employee.account` is index-backed
  only because `portal-service` ran first, and its config explicitly allows the
  read lane to point at a different cluster.
- `teams-chat-member-sync`'s only scan relies on a partial index `teams-chat-sync`
  creates — best-effort, warn-and-continue.
- `teams-hr-sync`'s direct-mode filter on `users.employeeId` is backed by no
  index anywhere in the repo, making a 20k-employee migration O(N·M).

The writer should own the constraint it has to satisfy.

### 5. No run deadline and no per-operation timeout on the batch jobs

The MongoDB v2 driver applies no default operation timeout — `pkg/mongoutil`
says so in a comment — and none of the CronJobs set one. `MONGO_SERVER_SELECTION_TIMEOUT`
bounds selection, not an in-flight query. Several services delegate the run
deadline to a Kubernetes CronJob `activeDeadlineSeconds` that exists in no
manifest in this repository, so the bound is unverifiable. A stalled primary
parks every worker indefinitely, and under `concurrencyPolicy: Forbid` one hung
run silently suppresses every later sync — the failure looks like nothing
happening rather than like an error.

### 6. One contract, two hand-written implementations, already diverged

`hr-sync-worker` and `teams-hr-sync` implement the same documented `Store`
contract in two copies that now disagree on every detail that matters: match key
(`account` vs `employeeId`), `_id` rule (minted UUIDv7 vs `= employeeId`), and
which field's emptiness causes a skip. Both write `users`, whose `account` is
uniquely indexed, so the winning `_id` depends on which path inserted first and a
direct-mode backfill can collide mid-run. `teams-hr-sync`'s README points at a
shared `pkg/hrstore` that would have prevented this; the package does not exist,
and the doc now conceals the duplication rather than recording it. The same shape
recurs at smaller scale: `splitUPN` — the key joining `teams_user`,
`hr_employee` and `users` — is duplicated byte-for-byte across two services, and
`EmployeeIDFromGraphID` is duplicated with a "must match" comment and no test
binding the two.

### 7. The silent-failure class

The single most common severity-raising pattern is work that fails without
producing a signal:

- `push-notification-service`'s only dispatcher wired into production is
  `LogDispatcher{}` — no APNs or FCM client exists anywhere in the repo, so every
  push event is acked and discarded.
- `teams-chat-member-sync`'s optimistic-write token is a whole-document write
  stamp rather than a membership token, so a busy chat can lose the compare
  forever; the loss is counted as `Superseded`, excluded from failures, and the
  run exits 0.
- `teams-chat-sync` treats an empty or truncated Graph roster as authoritative,
  and `room-worker` then reconciles subscriptions to it — mass-unsubscribing a
  room from one degraded API response.
- `teams-hr-sync`'s quit batch has no floor: one group returning HTTP 200 with an
  empty `value` wipes a site's HR directory.
- `teams-room-creation` exits 0 and logs "done" even when every batch failed.
- Several services route NATS subjects with `strings.HasSuffix` literals instead
  of `pkg/subject` builders, with a `default` branch that Ack-drops permanently —
  so renaming a builder still compiles and silently discards an entire feed.

### 8. Security scanning is partly unverified this cycle

`gosec` at medium+ is clean repo-wide, and the 20 repo-owned semgrep rules are
clean across 1,978 tracked files with their own fixture tests passing.
**`govulncheck` and the semgrep registry packs could not run**: `vuln.go.dev` and
`semgrep.dev` are both blocked by the sandbox egress policy (HTTP 403).
Dependency-vulnerability status and registry-rule coverage are therefore
explicitly unverified for this audit — not clean, unknown. Re-run both from an
environment with egress before treating this cycle's security result as complete.

## Audit limitations

- **`govulncheck` and the semgrep registry packs did not run.** `vuln.go.dev` and
  `semgrep.dev` are both blocked by this environment's egress policy (HTTP 403).
  Dependency-vulnerability status and registry-rule coverage are unverified for
  this cycle, not clean. Re-run both from an environment with egress.
- **Coverage figures are unit-only.** The measured profile comes from
  `go test -race -covermode=atomic ./...` with no `-tags integration`, matching
  what CI actually runs. Where a service's real tested surface is materially
  higher, the per-service report says so and gives both numbers.
- **No load testing or profiling.** Every performance finding is derived from
  reading call sites, configured timeouts and query shapes; latency and memory
  claims are reasoned, not measured.
- **Reviews are point-in-time.** Findings cite line numbers on this branch's
  tree; they will drift as the code moves.
