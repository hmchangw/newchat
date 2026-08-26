# loadgen refactor design

Status: proposed · Base: `c00644a` · 2026-08-26

## 1. Why

`tools/loadgen` is 74 168 lines across 263 files in a single `package main`.
It grew feature by feature under time pressure, largely AI-written and never
human-reviewed. It works — `go vet` is clean and `go test -race` passes in
319s — but it has no enforced internal boundaries, so 1 410 top-level
declarations share one namespace and `soak_`/`maxrps_`/`presence_` filename
prefixes stand in for packages the compiler does not know about.

The cost is not aesthetic. Ten verified defects are recorded in
[`docs/load-testing/loadgen-findings.md`](../../load-testing/loadgen-findings.md),
and eight are the same shape: **a correct implementation exists somewhere in
the tree and was never propagated to its siblings** — the ceil-percentile fix
that landed only in `daily`, the dropped-delivery accounting present in four
run paths and absent from `max-room-size`, the seed/run manifest validation
solved in `soak` and in no other workload. These survive because no single
place defines the behaviour.

Two goals, in order:

1. **Comprehension.** The author needs to understand code they did not write.
   The refactor is the vehicle: moving a unit forces reading it.
2. **Maintainability.** Give each behaviour one definition site so the
   propagation failures above stop recurring.

## 2. Constraints

### Frozen contracts

| Frozen | Detail |
|---|---|
| CLI subcommands and flags | All names, defaults, and `--workload` values unchanged |
| Prometheus metric names and labels | Grafana dashboards and `failure_dashboard_contract_test.go` depend on them |
| `SOAK_*` environment variables | Including `envPrefix` grouping |

### Explicitly not frozen

Terminal report layout, CSV columns, exit-code semantics. This is what makes
collapsing the duplicated report and verdict layers possible.

### Behaviour preservation

No commit in this refactor changes behaviour. Defects found while reading go
to the findings ledger and are fixed in separate PRs. Mixing the two destroys
the only signal that matters: if a contract test goes red, it must mean the
move broke something.

## 3. Target architecture

Packages under `tools/loadgen/internal/`, dependencies strictly downward.
CLAUDE.md's flat-service rule targets services at the repo root; its sanctioned
sub-package exception (`user-service`, `history-service`) applies here, and
`internal/` under `tools/loadgen/` is compiler-enforced privacy for exactly the
boundary we want.

```
  tools/loadgen/
    main.go              ~40 lines: base config -> signal ctx -> dispatch
    cmd_*.go             one file per subcommand, 50-80 lines each
─────────────────────────────────────────────────────────────────────
L4  internal/soak/  ->  internal/failure/        internal/attribution/
    internal/workload/{messages,thread,history,readreceipt,roomread,
                       threadread,login,search,members,botroom,daily}
─────────────────────────────────────────────────────────────────────
L3  internal/ramp/                    step-ladder engine (outer only)
─────────────────────────────────────────────────────────────────────
L2  internal/collect/  internal/verdict/  internal/report/
    internal/presence/                 pool, user, subjects
─────────────────────────────────────────────────────────────────────
L1  internal/harness/   internal/store/
─────────────────────────────────────────────────────────────────────
L0  internal/preset/  internal/metric/  internal/stat/  internal/pacer/
```

The CLI stays as flat files in `package main` rather than an `internal/cli`
package — matching vegeta's layout and this repo's own service convention, and
removing a hop. `main.go` drops from 1 509 lines to roughly 40.

## 4. Sequencing: soak first

soak plus failure is 20 418 lines — **54% of all non-test code**. It is also
the only part under active development (eight of the last eleven commits touching it are soak work),
and it has a single author, so conflict risk is self-managed rather than
external. The outer CLI is frozen by comparison: across the 61 commits between
`a96389f` and `c00644a`, `main.go` changed by one line and `ramp.go`,
`report.go`, `verdict.go`, `preset.go`, `collector.go`, `presence.go` and
`botroom.go` were not touched at all.

So the work starts where the knowledge and the volume are, and the frozen
outer layer waits.

**But the order inside that is forced by the compiler.** `internal/soak`
cannot import `package main`, and soak currently reaches outward for:

| Symbol | Lives in | Target package |
|---|---|---|
| `Metrics`, `NewMetrics`, `Set*Source` | `metrics.go` | `metric` |
| `NewConsumerSampler` | `consumerlag.go` | `harness` |
| `connectWithCredsHealth` | `daily_pool.go` | `harness` |
| `connectStores`, `newNatsCorePublisher`, `lastToken` | `main.go` | `harness` / `store` |
| `newNATSHistoryRequester` | `history_main.go` | `harness` |
| `pacedDispatchRate` | `pacer.go` | `pacer` |
| `percentile` | `daily_verdict.go` | `stat` |
| `waitOrCancel` | `ramp.go` | `stat` |
| `SeedRoomKeys`, `roomKeyStore`, `Teardown` | `seed.go`, `history_seed.go` | `store` |
| `presence*Subject` | `presence_subjects.go` | `presence` |

The foundation is roughly 2 000 lines and almost entirely mechanical. It is
Phase 1 whichever end you start from, so nothing here is paid twice.

## 5. Phases

Each phase is independently reviewable and mergeable, ends with `make lint`
and `make test` green, and leaves CLI behaviour unchanged.

| # | Content | ~Lines | Nature |
|---|---|---|---|
| 0 | Contract snapshot tests: `SOAK_*` env parsing, all metric names + labels, all CLI flag names + defaults | small | safety net |
| 1 | Foundation: `metric`, `stat`, `pacer`, `harness`, `store`, `presence` (subjects) | 2 000 | mechanical |
| 2 | `internal/failure` | 5 118 | soak's direct dependency |
| 3 | `internal/soak` and its sub-packages | 15 300 | **the main event** |
| — | Outer CLI, ramp, verdict, report, workloads | 22 000 | deferred |

Phase 0 is not optional. Every later phase's claim of "behaviour unchanged"
rests on it, and it is cheap — `failure_dashboard_contract_test.go` is already
a working precedent.

Phases 1 to 3 are also the correct reading order: the 14-lane registry in
`soak_workload.go` cannot be understood before `Metrics` and `pacer` are.

### 5.1 Phase 1 detail — `metric`

`metrics.go` is 766 lines defining one struct with **84 Prometheus fields**,
imported by 33 files. Split it into per-domain sets — `metric.Core`,
`metric.Members`, `metric.BotRoom`, `metric.Soak`, `metric.Failure` —
registered into one shared `*prometheus.Registry`. **Every metric name and
label string is copied verbatim.** Phase 0's snapshot test is what proves it.

This is the one unavoidable collision point with in-flight soak work, since
`metrics.go` gained 329 lines over those same 61 commits. Doing it in Phase 1,
before Phase 3 touches the same files again, keeps it to one collision.

### 5.2 Phase 3 detail — splitting soak

15 300 lines is too large for one package. soak already has real internal
seams that the split can follow rather than invent:

- a **lane registry** — `soakWorkloadActions` names 14 `soakWorkloadAction`
  fields, dispatched through `soakLaneDispatcher` with per-lane pacing metrics
  (`soak_workload.go:19`, `:585`)
- **24 consumer-defined interfaces** (`soakReadSampleRecorder`,
  `soakRoomStateStore`, `soakRPCTransport`, …) — dependency inversion is
  already present

Proposed sub-packages:

| Package | Files | ~Lines |
|---|---|---|
| `soak/` (engine) | `workload`, `config`, `topology`, `distribution`, `collector`, `report`, `failure`, `reconcile_backoff`, and the orchestration currently in `soak_main.go` | 5 500 |
| `soak/lane/` | `send`, `read`, `roomread`, `userread`, `search`, `presence`, `roommember`, `roomops`, `roomstate`, `mutation`, `verify`, `roomverify` | 6 144 |
| `soak/store/` | `store`, `catalog`, `seed`, `teardown` | 2 400 |
| `soak/rpc/` | `rpc` transport | 500 |
| `soak/wire/` | `wire` DTOs — zero dependencies | 360 |
| `soak/probe/` | `mongo_probe`, `pprof` | 400 |

Dependency direction: `wire` and `rpc` at the bottom, then `store`, then
`lane`, then the engine. Verified as achievable: `soak_failure.go` and the
twelve lane files reference **none** of each other's symbols — they are wired
together only in the engine — so the lane layer and the failure bridge can sit
side by side without a cycle.

`Action` and `Actions` stay defined in the engine package as plain function
types; `soak/lane` supplies constructors and the engine assembles them. No
cycle, because a lane never needs the engine's type.

`soak_main.go` is 1 464 lines of seed/run/teardown orchestration and does not
survive as one file. It becomes the engine's entry points plus `cmd_soak.go`
at the CLI layer.

## 6. Seams the refactor creates

**`harness.Harness` — kills the startup boilerplate.** Ten hand-rolled metrics
HTTP servers, 13 `dialNATS*` sites, and repeated subscribe/sampler/drain/shutdown
sequences collapse into one type:

```go
h, err := harness.Open(ctx, harness.Options{Base: cfg.Base, PoolTag: "soak"})
defer h.Close()   // sampler stop -> wg.Wait -> nats drain -> store disconnect
```

About 400 duplicated lines disappear, and the shutdown order has one
implementation. (The four existing sequences do agree on ordering today; the
duplication is the problem, not divergence.)

**Config splits by owner.** The `config` god struct mixes NATS, Mongo,
Cassandra, auth, Prometheus and soak fields and is passed to 17 files. It
becomes `harness.BaseConfig` plus one config per subsystem. **Env var names
and prefixes are unchanged.**

**Two engines stay two engines.** The outer `ramp` drives a step ladder —
increase RPS, evaluate, stop on trip. soak drives 14 concurrent lanes at fixed
rates, continuously. These are different concepts and merging them would make
both worse. What they legitimately share is `waitOrCancel`, which moves to a
neutral package in Phase 1.

**Deferred to the outer phase:** collapsing four step-loop implementations into
one `ramp`, six verdict implementations into one `verdict.Evaluate` plus
per-domain gates, and ten report renderers into one table/CSV writer driven by
per-workload column specs.

## 7. Hazards

**The Makefile coverage gates fail silently.** `make coverage-loadgen-soak` and
`coverage-loadgen-failure` select files by path prefix:

```
coveragecheck -profile … -include tools/loadgen/soak_    -min 80
coveragecheck -profile … -include tools/loadgen/failure_ -min 80
```

After Phases 2 and 3 those prefixes match nothing, and a gate that inspects
zero files passes. Both phases must update the prefixes **and** add a
"fail if the include pattern matched no files" guard to `tools/coveragecheck`.
Without the guard the gate is worse than removed — it reports success.

**Every new package with integration tests needs its own `TestMain`.** One
`testutil.RunTests(m)` in `integration_test.go` currently serves the whole
package. Thirteen `//go:build integration` files will spread across roughly
eight packages, each requiring its own, per CLAUDE.md.

**Test migration is the bulk of the work.** Roughly half of all files are
`_test.go` and they test unexported symbols. They move with their
implementation into the same package, keeping white-box access; test bodies do
not change. Only genuinely cross-package cases need exporting.

**PR #217 overlap is one file.** `claude/loadgen-automation-test-w4p5ac` adds a
`verify` subcommand — 7 new files plus `main.go`, `daily.go`, `daily_pool.go`,
`deploy/Makefile`, `README.md`. It touches **zero soak files**, so Phases 2 and
3 are unaffected. Phase 1 moves `connectWithCredsHealth` out of `daily_pool.go`,
which is the single point of contact. When #217 lands it will add an eleventh
report renderer and a seventh verdict — more fuel for the deferred outer phase,
not a blocker for this one.

**Export decisions must be deliberate.** 122 identifiers are currently exported
inside `package main`, where export means nothing. Splitting forces a real
choice for each. Default to unexported; export only what another package
demonstrably consumes.

## 8. Verification

Per phase, in order:

1. `go build ./tools/loadgen/...` — the compiler finds every missed reference.
2. Phase 0 contract tests — metric names, CLI flags, `SOAK_*` parsing.
3. `make test` — the full race-enabled suite (319s at time of writing; it gets
   faster per-package after the split).
4. `make lint`.
5. `make test-integration SERVICE=loadgen` for phases that move integration
   tests.
6. A manual `loadgen soak` smoke run against the local stack for Phase 3.

Item 1 carries more weight than usual: a mechanical move that compiles and
keeps a green race-enabled suite is close to provably behaviour-preserving.

## 9. Open decisions

- **When to split soak internally.** Phase 3 could stop at "one
  `internal/soak` package" and defer the sub-package split. That halves the
  Phase 3 diff at the cost of leaving a 15 300-line package. Recommendation:
  do the sub-split in Phase 3, because the seams already exist and a second
  pass would re-read the same code.
- **Whether the outer phase happens at all.** After Phases 0-3 the tool is 22 000
  lines of frozen, untouched outer code plus a properly structured soak. That
  may be a reasonable stopping point until the outer code next needs work.
- **F1 timing.** `max-room-size` is blind to dropped broadcasts
  (findings ledger F1). It is in the deferred outer layer, but any existing
  `ANSWER: max room size = N` from a large-room step is suspect until fixed.
  Independent of this refactor; needs its own scheduling decision.
