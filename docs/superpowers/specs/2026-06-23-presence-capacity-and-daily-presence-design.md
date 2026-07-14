# Presence capacity mode + daily presence load — design

Date: 2026-06-23
Status: Draft
Area: `tools/loadgen`

## 1. Summary

Two additive load-generation features for the presence service, both built on
the presence primitives already in `tools/loadgen` (`presence_subjects.go`,
`presence_user.go`, `presence_pool.go`, `presence_collector.go`,
`presence_verdict.go`):

- **Part 1 — `presence-capacity`**: a new subcommand that answers *"how many
  users can be simultaneously online?"* It cumulatively ramps a synthetic
  population through `--steps`, holding all activated users online, and reports
  the largest N the service holds without falsely sweeping users offline.

- **Part 2 — `daily --presence`**: an opt-in flag on the existing `daily`
  subcommand that makes each simulated daily user *also* maintain presence
  (hello on activation, ping per heartbeat, activity flip on each Markov
  active↔idle transition). Presence latency/errors are reported as
  **observational** stats and never affect the daily PASS/TRIP verdict. Off by
  default — absent the flag, `daily` is byte-for-byte unchanged.

The two parts are independent and independently shippable. They share the
presence building blocks but touch disjoint code paths.

## 2. Motivation

`presence-sustained` measures latency/error SLOs of a churning population.
`presence-storm` measures recovery from reconnect herds. Neither answers the
simplest capacity question an operator asks before a launch: *how many
concurrent online sessions will the presence service hold before it starts
dropping users?* That ceiling is governed by TTL-sweeper throughput, memory,
and connect-path cost — none of which the existing modes isolate.

Separately, the `daily` scenario simulates realistic IM behaviour but emits no
presence traffic, so a daily run under-represents production load on the
presence service. Adding opt-in presence emission lets a single daily run
exercise the messaging *and* presence pipelines together, with presence kept
observational so it never muddies the existing daily verdict.

## 3. Background — reused components

| Component | File | Reused as-is? |
|-----------|------|---------------|
| `presenceUser` (hello/ping/setAway/bye transitions) | `presence_user.go` | Part 2 adds one constructor; otherwise as-is |
| `presencePool` (publisher RR + `presence.state.*` observer) | `presence_pool.go` | as-is |
| `presenceCollector` (expectation/latency/recovery tracker) | `presence_collector.go` | Part 1 adds a false-offline watcher |
| `evaluatePresenceStep` / `presenceThresholds` | `presence_verdict.go` | pattern reused, new capacity variant added |
| `verdictKind`, `percentile`, `snapshotSelfMetrics`, `waitOrCancel`, `parseStepList`, `slicesMaxInt`, `accountFromSubject` | various | as-is |
| renderer/CSV pattern | `presence_report.go` | pattern reused, new capacity variant added |

The `presence-storm` subcommand (`presence_storm.go`) is the structural
precedent for Part 1: a second presence subcommand with its own config parser,
factory, env, verdict, renderer, and `main.go` dispatch entry.

## 4. Part 1 — `presence-capacity`

### 4.1 Goal & headline

Find the maximum number of users the presence service holds **simultaneously
online**. Headline output: `MAX CONCURRENT ONLINE: N` = the largest passing
step.

### 4.2 Model

Cumulative ramp, mirroring `daily`'s activation model: each step activates only
the *delta* of new users `[prevN, n)`; users activated in prior steps stay
online and keep pinging. At step `n`, exactly `n` users are online with `n`
steady-ping goroutines running. **No churn** — capacity mode never flips
activity and never sends `bye`; the only transitions are the per-step `hello`s
at activation and any unexpected `offline` the service emits under stress.

### 4.3 Two measurement phases per step

Because a steady-state hold with no churn produces **no state-changing
transitions** (a `ping` for a known connection is a service-side no-op → no
`presence.state.*` publish → nothing to time), the two signals are captured in
different phases:

| Phase | Activity | Signal measured |
|-------|----------|-----------------|
| **Activation** `[prevN, n)` | each new user sends `hello`, expecting `online` | **connect-edge latency** — hello→online round-trip p50/p95/p99 at the current population size |
| **Hold** (all `n` online, pinging) | pings are no-ops; watch the wildcard for `offline` publishes | **false-offline count** (primary ceiling) + **ping sustainability** |

- **False offlines** are the ceiling signal. The collector arms a
  "should-be-online" cohort of all `n` active accounts at hold start; any
  `offline` state publish observed for a cohort account during the hold is a
  false offline — the service swept a user that the loadgen kept alive.
- **Ping sustainability** guards against the *loadgen* being the bottleneck: it
  compares pings actually sent during the hold against the required count
  (`n × floor(hold / heartbeat)`).

### 4.4 Verdict — `evaluateCapacityStep`

Precedence: **INCONCLUSIVE → TRIP → PASS** (matching `evaluatePresenceStep`).

1. **INCONCLUSIVE** (verdict cannot be trusted), checked first, when any of:
   - loadgen GC pause p99 > `GCPauseInconclusive` (50ms) — load box saturated.
   - activation shortfall: `EffectiveN / N < 0.95` — fewer than 95% of users
     came online (the population under test isn't really `n`).
   - **ping-sustainability shortfall**: `pingsSent < pingsRequired × (1 − pingTolerance)`
     — the loadgen couldn't keep `n` users heartbeating, so any offline is
     loadgen-induced, not a service limit. *Checked before false-offline TRIP*
     so a load-box-induced sweep never reads as a service failure.
2. **TRIP** (service hit its limit), when any of:
   - false-offline rate `falseOfflines / N > --false-offline-rate` (default 0.001 = 0.1%).
   - connect p95 > `--connect-p95-ms` or connect p99 > `--connect-p99-ms`.
   - connect error rate (activation hellos that never observed their `online`)
     > `--error-rate`.
3. **PASS** otherwise.

### 4.5 New collector capability — false-offline watcher

Add to `presenceCollector` (same single-`Observe`-callback path as the existing
recovery tracker):

```go
// WatchOnline arms a cohort of accounts that must remain online. Any offline
// observed for a watched account during the watch window is a false offline.
func (c *presenceCollector) WatchOnline(accounts []string)
func (c *presenceCollector) FalseOfflines() int
func (c *presenceCollector) StopWatchOnline()   // disarm at end of hold
```

`Observe(account, status, at)` gains a branch: when watching and
`status == StatusOffline` and `account` is in the cohort, increment a
`falseOfflines` counter (dedup per account so a flapping user counts once).
`Reset()` clears the watcher state alongside the existing fields. The latency
and recovery paths are untouched.

Connect-edge latency reuses the existing `Expect`/`Observe`/`LatenciesMs`
machinery: each activation `hello` calls `Expect(account, StatusOnline, sentAt)`;
the `online` publish resolves it into a latency sample. The capacity runner
**snapshots `LatenciesMs()` after activation** (these are the connect samples),
then `Reset()`s before the hold so the hold starts with empty latency state.

### 4.6 Step sequence (`runStepCapacity`)

```
activateCapacityUsers(ctx, env, prevN, n)   // hello each new user (Expect online); start steady-ping goroutine
wait(warmup)
connectLat := collector.LatenciesMs()       // snapshot connect-edge samples
connectAttempted, connectFailed := collector.Attempted(), collector.Failed() + reapPendingHellos
collector.Reset()
collector.WatchOnline(allActiveAccounts[:n])
holdStart := now; env.setHold(holdStart, hold)
wait(hold)
collector.StopWatchOnline()
falseOfflines := collector.FalseOfflines()
pingsSent := env.pingsSent.Load()           // atomic, incremented by ping goroutines during hold
env.holdDurationNanos.Store(0)
evaluateCapacityStep(capacityStepInputs{...})
wait(cooldown)
```

Steady-ping goroutines (one per activated user, started at activation and
living until ctx cancel) tick every `heartbeat`; while `holding()` they
`atomic.Add` the ping counter on each publish. No activity flips, no bye.

### 4.7 Config & flags (`parseCapacityConfig`)

`flag.NewFlagSet("presence-capacity", ContinueOnError)`, mirroring
`parsePresenceConfig`:

| Flag | Default | Meaning |
|------|---------|---------|
| `--steps` | `10000,20000,50000,100000,200000` | cumulative N per step; `k` suffix |
| `--warmup` | `30s` | post-activation settle before snapshot |
| `--hold` | `120s` | steady-state false-offline window |
| `--cooldown` | `15s` | inter-step gap |
| `--heartbeat` | `30s` | per-user ping interval |
| `--connect-p95-ms` | `500` | connect-edge p95 cap |
| `--connect-p99-ms` | `1000` | connect-edge p99 cap |
| `--false-offline-rate` | `0.001` | false-offline fraction cap (TRIP) |
| `--error-rate` | `0.01` | connect error-rate cap |
| `--ping-tolerance` | `0.10` | ping-sustainability shortfall band (INCONCLUSIVE) |
| `--stop-on-trip` | `true` | stop ramp on first TRIP |
| `--publisher-conns` | `16` | shared publisher conns |
| `--observer-conns` | `4` | observer conns (`presence.state.*`) |
| `--csv` | `""` | optional CSV path |

### 4.8 New types & files

- `presence_capacity.go`: `capacityConfig`, `parseCapacityConfig`,
  `capacityEnv` (+ `setHold`/`holding`/atomics), `capacityFactory` interface,
  `prodCapacityFactory`, `runStepCapacity`, `activateCapacityUsers`,
  `startCapacityEmitter`, `runPresenceCapacityForTest`, `runPresenceCapacity`
  (prod entrypoint), `presenceCapacityExitCode`.
- `capacityStepInputs` / `capacityStepResult` / `capacityThresholds` /
  `evaluateCapacityStep` (in `presence_capacity.go` or a
  `presence_capacity_verdict.go` — keep verdict logic in its own pure file to
  match `presence_verdict.go`).
- Renderer/CSV: `renderCapacityConsole` + `writeCapacityCSV`. Console columns:
  `N  connect_p50  connect_p95  connect_p99  false_off  ping_sustain%  verdict`,
  ending with `MAX CONCURRENT ONLINE: N` and the next limit.
- `main.go`: add `case "presence-capacity": return runPresenceCapacity(...)`
  and extend the usage string.

### 4.9 Test seams

Follow `presenceEnv`'s seam style: `capacityEnv` exposes `onActivated` /
`afterReset` function fields (nil in prod → real wiring) so unit tests drive
the step loop without a broker. `capacityFactory` is stubbed in tests
(`prodCapacityFactory` is the real wiring). `evaluateCapacityStep` is a pure
function unit-tested across every PASS/TRIP/INCONCLUSIVE branch and precedence
ordering.

## 5. Part 2 — `daily --presence`

### 5.1 Goal

When `--presence` is set, every active daily user also generates presence
traffic, and the daily report shows observational presence latency/error stats.
Default off; the daily verdict is never affected by presence.

### 5.2 Emission points (reusing `presenceUser`)

Hooked into the existing daily user lifecycle (`daily.go`, `daily_user.go`):

| Daily event | Presence transition | Expected publish |
|-------------|--------------------|------------------|
| User activation (`activateUsers`) | `hello` | `online` |
| Per heartbeat tick | `ping` | none (no-op) |
| Markov flip `idle→active` (`userState.step`) | `setAway(false)` | `online` |
| Markov flip `active→idle` | `setAway(true)` | `away` |

Daily users don't disconnect mid-run, so no `bye` (consistent with the existing
emitter, which never tears down a user). The heartbeat ping uses a dedicated
interval (`--presence-heartbeat`, default `30s`) — independent of the 1s Markov
tick — so presence ping rate matches production rather than the action tick.

### 5.3 Per-user presence state

A daily `userState` carries a real fixture account (`user-N`), not a synthetic
index, so `newPresenceUser(idx, siteID)` (which derives `u-%06d` from the
index) can't be reused directly. Add:

```go
// newPresenceUserForAccount builds a presenceUser bound to an explicit account
// (connID = "c-"+account) rather than the index-derived synthetic identity.
func newPresenceUserForAccount(account, siteID string) *presenceUser
```

`presenceAccount`/`newPresenceUser` stay for Parts 1 and storm/sustained.
`newPresenceUser` is refactored to delegate to `newPresenceUserForAccount` to
avoid divergence. Each daily `userState` gets an optional `presence *presenceUser`
field, populated only when `--presence` is set.

### 5.4 Wiring into the daily env

`stepEnv` gains optional, nil-when-disabled fields:

```go
presencePool      *presencePool       // observer + publisher; nil when --presence off
presenceCollector *presenceCollector  // observational only
presenceHeartbeat time.Duration
```

`prodEnvFactory.Build`, when `cfg.Presence`:
- constructs a `presenceCollector` and a `presencePool` (its own publisher +
  `presence.state.*` observer conns — independent of the daily message pools so
  presence backpressure can't perturb message latency).
- attaches `newPresenceUserForAccount(u.Account, siteID)` to each `userState`.

`activateUsers`: after a user is pool-added, if presence is enabled, emit the
user's `hello` (Expect online) via the presence pool/collector.

`startEmitter`: add a per-user presence ping ticker (own `time.Ticker` at
`presenceHeartbeat`) and, inside the existing `u.step(r)` flip handling, emit
`setAway` when `active` changes. Presence emission goes through a small helper
(`emitPresence(env, u.presence, transition)`) modelled on `emitTransitionRaw`
— records attempt/expectation/failure on the presence collector. **All presence
emission is guarded by `env.presencePool != nil`** so the disabled path is a
no-op.

### 5.5 Observational reporting (no verdict impact)

`StepResult` gains an optional `Presence *PresenceObsStats` field:

```go
type PresenceObsStats struct {
    P50Ms, P95Ms, P99Ms float64 // combined connect + activity latency
    Attempted, Failed   int64
    ErrorRate           float64
}
```

`runStep` populates it from the presence collector when enabled: `Reset()` at
hold start (alongside the daily collector's reset), then snapshot
`LatenciesMs()`/`Attempted()`/`Failed()` at end of hold (after `ReapMissing()`).
`evaluateStep` is **unchanged** — it never reads `Presence`. `renderConsole`
prints an extra `presence:` line per step when present; `writeDailyCSV` appends
presence columns (always written, zero-valued when disabled, to keep a fixed
schema — matching the existing per-action column convention).

Latency is a single combined figure over all measured presence transitions
(connect `hello→online` plus activity `setAway→away`/`→online`). The collector
keys latency by account into one slice and both connect and the
return-to-online activity resolve to the same `online` status, so a clean
connect-vs-activity split would require restructuring the shared collector;
combined latency keeps the collector untouched and is sufficient for an
observational signal. Pings are service-side no-ops and contribute no latency —
only to attempted/error accounting if a publish errors at send time.

### 5.6 Config & flags

`parseDailyConfig` gains:

| Flag | Default | Meaning |
|------|---------|---------|
| `--presence` | `false` | enable presence load + observational stats |
| `--presence-heartbeat` | `30s` | per-user presence ping interval |
| `--presence-publisher-conns` | `8` | presence publisher conns (only when `--presence`) |
| `--presence-observer-conns` | `2` | presence observer conns (only when `--presence`) |

`dailyConfig` gains `Presence bool`, `PresenceHeartbeat time.Duration`,
`PresencePublisherConns int`, `PresenceObserverConns int`.

### 5.7 Shutdown

`closePools(env)` also drains `env.presencePool` when non-nil (its own
`Close()` drains pub+obs conns).

## 6. Non-goals

- No change to the presence service itself.
- No cross-site presence load.
- Part 1 does not measure activity-flip latency (no churn by design); Part 2
  does not gate any verdict on presence.
- No new third-party dependencies.

## 7. Testing

Per CLAUDE.md TDD: tests first, table-driven, same `package main`.

- **Part 1**: `presence_capacity_test.go` — `parseCapacityConfig` (defaults,
  `k`-suffix, errors); `evaluateCapacityStep` (every branch + precedence:
  GC-inconclusive, activation-shortfall, ping-shortfall-before-false-offline,
  false-offline TRIP, connect-latency TRIP, connect-error TRIP, PASS);
  `runStepCapacity` via stubbed `capacityEnv` seams (activation snapshot →
  reset → watch → false-offline accounting); renderer/CSV golden-ish assertions.
  `presence_collector_test.go` — `WatchOnline`/`FalseOfflines`/dedup/`Reset`.
- **Part 2**: extend `daily_test.go` / `daily_verdict_test.go` — `--presence`
  off leaves behaviour and CSV schema additions zero-valued; flip-driven
  `setAway` emission; `evaluateStep` ignores `Presence`; `newPresenceUserForAccount`
  account/connID correctness; `renderConsole`/`writeDailyCSV` presence line/columns.
- Existing presence integration test (`presence_e2e_test.go`) is the model for
  an optional capacity smoke test against a real broker (small N, short hold).
- Coverage ≥80% (target 90% for verdict + collector additions).

## 8. Rollout / sequencing

Two plan phases, each independently shippable:

1. **Phase 1 — `presence-capacity`**: collector watcher + capacity files +
   verdict + renderer + dispatch + tests. Fully standalone.
2. **Phase 2 — `daily --presence`**: `newPresenceUserForAccount` +
   `stepEnv`/`prodEnvFactory` wiring + emitter hooks + observational
   `StepResult` field + report/CSV + config flags + tests. Depends only on the
   pre-existing presence primitives, not on Phase 1.

## 9. Open questions (resolved)

- *Connect latency timing* — measured during activation, not the hold (the hold
  has no state transitions). Confirmed.
- *daily observer overhead* — keep the `presence.state.*` observer subscription
  (cheap, enables latency stats). Confirmed.
- *daily presence gating* — observational only; never affects daily verdict.
  Confirmed.
</content>
</invoke>
