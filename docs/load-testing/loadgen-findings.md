# loadgen findings ledger

Living record of defects and smells found while reading `tools/loadgen`.

**Rules for this ledger**

1. Nothing here is fixed as part of a refactor commit. Refactors must be
   behaviour-preserving so that "contract tests still green" means something.
   Each finding gets its own PR.
2. A finding is only listed once it has been verified against the source, with
   file:line evidence. Suspicions go in "Open questions", not in the table.
3. When a finding is fixed, keep the row and mark it `FIXED (#PR)`. The ledger
   is also the record of what the code used to get wrong.

## Provenance

F1-F10 come from the pre-refactor audit; F11-F15 from reviewing the soak
extraction (PR #474) on 2026-09-07, where both review bots surfaced them. All
of F11-F15 describe behaviour that predates that PR — it moved the code into
packages without changing it — which is why none was fixed there.

## Baseline at audit time

Commit `a96389f`, 2026-08-20.

| Check | Result |
|---|---|
| `go vet ./tools/loadgen/...` | clean |
| `go test -race -count=1 ./tools/loadgen/...` | **pass**, 244.9s |
| Non-test source | 210 files, 52 846 lines, one `package main` |
| Top-level declarations | 1 410 (`func`/`type`), 122 of them exported |

The test suite passes and the race detector is clean. Whatever else is wrong
here, there is a working safety net to refactor against.

## Findings

| # | Sev | Area | Summary |
|---|---|---|---|
| F1 | **High** | `max-room-size` | Blind to dropped broadcasts — the one failure it exists to find |
| F2 | **High** | statistics | Three percentile implementations; two systematically under-report |
| F3 | **High** | seed/run coupling | No guard against seed/run parameter mismatch outside `soak` |
| F4 | Medium | `members-capacity` | Size-bucket latency table reports the same number in every row |
| F5 | Medium | `max-rps` | Generator errors discarded in all six ramp workloads |
| F6 | Medium | concurrency | `time.Sleep` used as a synchronisation primitive, 7 sites |
| F7 | Low-Med | `Collector` | `MissingInWindow` is dead code with four tests giving false confidence |
| F8 | Low-Med | `presence` | Verdict thresholds duplicated between tests and production, unlinked |
| F9 | Low | `daily` | `buildAuthMintFn` is a no-op stub wired into the production path |
| F10 | Low | CLI / hygiene | Undocumented workload alias; test-only helper in production code |
| F11 | **High** | `soak` teardown | No atomic claim between reading the manifest and deleting the run's data |
| F12 | Low | `soak/run` | `Teardown` does not defend its own `BatchRooms` precondition |
| F13 | Medium | `soak` search | A capped search page reports an indexed message as missing |
| F14 | Medium | `soak` catalog | `SearchTerm` retains the whole message body, against its own doc comment |
| F15 | Low-Med | `soak` mutation | Concurrent reaction toggles can leave the catalog disagreeing with the server |

---

### F1 — `max-room-size` is blind to dropped broadcasts · **High**

`runMaxRoomSize` is the only run path that never calls `Collector.Finalize()`
(compare `main.go:689`, `main.go:909`, `main.go:1162` — `main.go:1357-1509` has
no such call). `botroom.go` has no reference to `Finalize`, `MissingInWindow`,
or any missing-delivery count at all.

What the step actually reports (`botroom.go:236-244`):

- `Failed` = publish errors + gatekeeper rejections only (`botroom.go:223,229`)
- `E2Samples` = `Collector.LatencySamples()`

`Collector.RecordBroadcast` (`collector.go:184-199`) appends a latency sample
**only on a successful match** and deletes the entry. A message that the
gatekeeper accepted but that was never broadcast therefore:

- does not increment `Failed` — it is invisible to the `--slo-error-rate` gate
- contributes no latency sample — so dropping messages makes p95/p99 *better*

Net effect: at the room size where broadcast fan-out starts shedding
deliveries, the error rate stays flat and the latency improves. The step can
return **PASS**. Finding that size is the entire purpose of the subcommand.

Every other measured path counts this: `run` and `members-*` via `Finalize()`,
`daily` via `BroadcastStatsInWindow` (`daily.go:416`), `max-rps messages` via
`missCounts` (`maxrps_messages.go:88`).

### F2 — Three percentile implementations, two under-report · **High**

| Site | Formula | Used by |
|---|---|---|
| `report.go:28` | `int(float64(len-1) * q)` — floor | `run`, `members-sustained`, `members-capacity`, `max-room-size`, `max-rps` |
| `history_report.go:148` | `int(p * float64(len-1))` — floor | `history-sustained` |
| `daily_verdict.go:211` | `int(math.Ceil(p*len)) - 1` — ceil | `daily` |

`daily_verdict.go:200-204` documents the defect in the other two by name:

> *"Floor-based indexing systematically under-reports for small sample counts —
> e.g. p99 of 50 samples with floor → cp[48] (true p98), with ceil → cp[49]
> (true p99)."*

The fix was applied in one file and never propagated. Consequences: reported
p95/p99 are biased low by up to one rank on most sample counts, and the same
latency tape yields a different p99 depending on which subcommand printed it.

### F3 — No guard against seed/run parameter mismatch · **High**

`--seed`, `--users` and `--parents-per-room` must be identical between
`loadgen seed` and the run subcommand, because fixture IDs are derived from
them. The code says so in flag help (`main.go:164`, `main.go:165`,
`main.go:318`, `main.go:1360`, `daily.go:117`, `maxrps.go:104`) but **nothing
verifies it at runtime** — no fingerprint is written at seed time and nothing
is compared at run time.

Mismatch produces a run where every generated room and subscription ID differs
from what was seeded, so the gatekeeper rejects 100% of sends. The operator
sees a total-failure run with no indication that the cause is their own flag.

`soak` already solves this properly: it persists a manifest and validates run
identity (`soak_seed.go:72`). None of that was applied to the other twelve
workloads.

### F4 — `members-capacity` size buckets are not per-bucket · Medium

`computeSizeBuckets` (`main.go:950-971`) computes `ComputePercentiles` once
over **all** samples, then assigns those same `e1`/`e2` values to every bucket
whose `Count > 0`. Every non-empty row of the size-bucket table therefore
prints identical latency figures.

"How does latency vary with room size" is the only question this subcommand
exists to answer. The comment at `main.go:945-949` concedes the design
("intentionally simple in v1", per-sample size tracking deferred), but the
report does not mark the column as unpopulated — it prints plausible numbers.

### F5 — `max-rps` discards generator errors · Medium

`_ = gen.Run(genCtx)` in all six ramp workloads: `maxrps_history.go:118`,
`maxrps_messages.go:262`, `maxrps_readreceipt.go:114`, `maxrps_roomread.go:112`,
`maxrps_thread.go:179`, `maxrps_threadread.go:116`.

The error is not logged, not surfaced, and not fed into the verdict. By
contrast `runRun` (`main.go:1175`) and `runMembersSustained` (`main.go:699`) at
least `slog.Error` it. A generator that fails mid-step still yields a step
result; the shortfall shows up only as low achieved RPS routed to
INCONCLUSIVE, with the actual reason discarded.

### F6 — `time.Sleep` as a synchronisation primitive · Medium

CLAUDE.md §3 Concurrency: *"Never use `time.Sleep` for goroutine
synchronization."* Seven sites in non-test code:

`main.go:687`, `main.go:905`, `main.go:1160`, `maxrps_history.go:139`,
`maxrps_readreceipt.go:102`, `history_main.go:279`, `botroom.go:233`

All are "sleep 2s so trailing replies land before we snapshot". This couples
the measurement to host load: on a slow or loaded box, in-flight replies that
arrive at 2.1s are counted as missing. The drain should be driven by the
correlation map emptying or an explicit deadline on the collector, not a
constant.

### F7 — `Collector.MissingInWindow` is dead code with four tests · Low-Med

`collector.go:256`. Production callers: **none**. Tests:
`collector_test.go:210,224,236` and `daily_actions_test.go:102`.

Its 15-line doc comment (`collector.go:240-255`) explains subtle
start/end-boundary semantics written specifically for `daily` — but `daily`
uses `BroadcastStatsInWindow` instead (`daily.go:416`). The old function was
superseded and never deleted; its tests still pass, so nothing flagged it. Four
green tests currently certify a function no shipped code path executes.

### F8 — `presence` thresholds duplicated between test and production · Low-Med

`defaultPresenceThresholds()` (`presence_verdict.go:16`),
`defaultStormThresholds()` (`presence_verdict.go:107`) and
`defaultCapacityThresholds()` (`presence_capacity_verdict.go:18`) are
referenced only from `_test.go` files. Production builds its thresholds from
flag values instead (`presence.go:67-69`, `presence_capacity.go:51-55`,
`presence.go:295`, `presence_capacity.go:249-254`).

The two sets of numbers currently agree (200/500/0.01 and
500/1000/0.001/0.01/0.10). Nothing enforces that. Changing a flag default
leaves the verdict tests asserting against the stale copy, still green.

### F9 — `buildAuthMintFn` is a no-op wired into production · Low

`daily.go:962-970` marshals a request body, discards it (`_ = body`) and
returns `nil` unconditionally. It is wired into the production env at
`daily.go:945` and invoked per activated user at `daily.go:463-464`, so every
`daily` run reports a successful JWT mint for every user without making a
request.

This is consistent with a documented non-goal (README: *"Not an auth
benchmark. Uses shared `backend.creds`"*), so it is not a hidden measurement
gap — but it is dead weight on a hot path that reads like a working feature.

### F10 — CLI and hygiene · Low

- `--workload=thread-read` silently dispatches to the `history` seed and
  teardown (`main.go:188`, `main.go:339`). Undocumented alias.
- `runPresenceStormForTest` (`presence_storm.go:140`) lives in production code
  and is called only from `presence_storm_test.go:75`; production
  `runPresenceStorm` calls `runStormSteps` directly (`presence_storm.go:339`).
  Its siblings `runPresenceSustainedForTest` / `runPresenceCapacityForTest`
  *are* on the production path, so the storm variant diverged. CLAUDE.md §4:
  test helpers belong in `_test.go` only.
- `_ = fs.Parse(args)` discards parse errors at 8 sites; flag sets mix
  `ExitOnError` (8) and `ContinueOnError` (5), so a mistyped flag behaves
  differently per subcommand.
- 122 exported identifiers in a `package main`, where export is meaningless.
  Not a defect today, but every one is an unreviewed decision that the package
  split will have to make deliberately.

### F11 — Teardown has no atomic claim on the run · **High**

`Teardown` reads the manifest (`internal/soak/run/teardown.go:88`), refuses when
it is `StateRunning` with a fresh heartbeat, and otherwise proceeds straight
into the deletion loop. Nothing claims the run in between.

`Lifecycle.Prepare` writes `manifest.State = StateRunning` through an
unconditional `PutManifest` (`internal/soak/run/lifecycle.go:86-89`), so a run
starting after teardown's read — but before its deletes — is dispatching
against rooms teardown is removing. Teardown then deletes ownership, may
truncate Cassandra, and finishes with an unconditional `MarkCleaned`
(`teardown.go:137-149`), overwriting the live run's state.

`Prepare` also accepts a manifest that is already `StateRunning`, so two
workers can hold the same run and each one's terminal transition can hide the
other's.

The fix is an owner/epoch claim shared by seed, prepare and teardown, with
heartbeats and terminal transitions conditioned on the epoch. Both review bots
raised this independently on PR #474 and rated it their top finding.

Exposure today is low — the soak has one operator — but the failure is
silent and destructive, and it gets worse the moment a second person or a
scheduled run exists.

### F12 — `run.Teardown` does not defend its own precondition · Low

`ChunkRoomIDs(page.RoomIDs, cfg.BatchRooms)` (`teardown.go:121`) returns no
batches when `BatchRooms <= 0`. The loop body then never runs, so no room is
deleted — but the flow continues to `DeleteOwnership` and `MarkCleaned`
(`teardown.go:137-149`), leaving the rooms behind with the record that could
identify them gone.

Not reachable through the CLI: `validateSoakConfig` rejects a non-positive
`TEARDOWN_BATCH_ROOMS` (`soak_config.go:524-525`). It is listed because the
extraction turned an invariant that used to be local into a public package
API's unguarded precondition — `run.Teardown` now trusts a check that lives in
another package. A three-line guard closes it before PR 3 or 4 adds a second
caller.

### F13 — A capped search page reports an indexed message as missing · Medium

`Reader.IndexedAt` requests `PageSize` hits and returns `IndexMissing` when the
target is not among them (`internal/soak/search/search.go:194-199`). It never
pages further.

The query term comes from `catalog.SearchTerm`, and generated bodies are
`strings.Repeat("x", size)` (`internal/soak/distribution/distribution.go:137`)
— so every message of the same generated size carries the same term. Sizes are
drawn from a bounded log-normal, so collisions are the norm rather than the
exception in a hot room. Once matches exceed `PageSize`, a fully indexed older
message is simply not on the page, and the run records it as loss.

The soak's output is evidence. A false loss claim costs someone a hunt for a
bug that does not exist, which is the same class of defect as F1 — the tool
reporting something other than what happened.

### F14 — `SearchTerm` retains the whole body, against its own comment · Medium

`SearchTerm` walks `strings.FieldsSeq(content)` and returns the first field of
three characters or more, cloned (`internal/soak/catalog/catalog.go:99-106`).
Its doc comment states the clone means "retaining it does not retain the
complete message body".

For production payloads that is exactly inverted. `ContentOfSize` generates
`strings.Repeat("x", size)` with no whitespace, so `FieldsSeq` yields one field
— the entire body — and the clone copies all of it. Every accepted catalog
entry then holds a full-payload copy while `RetainSearchTerms` is on, up to
`RecentTotal`.

The catalog's own memory test does not reach this: it builds
`CatalogProbeID(i) + " " + strings.Repeat("ab ", …)`
(`internal/soak/catalog/content_test.go:158`), a whitespace-delimited body
whose first field is the short probe ID. It measures a shape production never
produces, and its comment already records 351 MB for a full default catalog at
that lighter shape.

F13 and F14 are one root cause — the body is a single unbroken token — seen
from two sides. A bounded, message-unique searchable token in every payload
fixes both.

### F15 — Concurrent reaction toggles can overwrite newer catalog state · Low-Med

Two reaction slots can select the same hot message and actor from identical
catalog snapshots. Each applies its response with an unconditional
`catalog.SetReaction` (`internal/soak/mutation/mutation.go:497-504`), so if the
service processes add-then-remove but the goroutines return in the other order,
the catalog ends with the reaction present where the server has removed it.

Reaction targets are hot-scoped, so this needs only reaction latency to exceed
the dispatch interval. The cost is skewed later actor selection and
reconciliation evidence that disagrees with the server.

Needs either serialised selection-through-update per target, or a version on
the target so a stale update is rejected.

## What is in good shape

Reported so the ledger is not read as a verdict on the whole tool.

- `go vet` clean; `go test -race` green.
- **Divide-by-zero discipline is complete** — all 15 ratio computations are
  guarded (`verdict.go:204,207,210,242`, `daily_verdict.go:238,241`,
  `presence_verdict.go:60,143`, `presence_capacity_verdict.go:71,74,77`,
  `botroom_verdict.go:67`, `daily.go:654`, `soak_config.go:324`,
  `history_report.go:112`).
- **No dead unexported code** — all 438 unexported functions have callers.
- No `fmt.Println`/`log.Print`; no token, password or message-body logging.
- NATS payloads use typed structs from `pkg/model`; the two `map[string]any`
  sites are in `daily_actions.go:148,169` only.
- `Collector` uses sharded mutexes with a documented rationale — the
  concurrency design is deliberate, not accidental.
- The `soak` subsystem's manifest and ownership-ledger design is visibly
  higher quality than the rest of the tool. F3 is the case for propagating it.

## Pattern behind these findings

Eight of the first ten are the same shape: **a fix or a good design exists
somewhere in the tree and was never propagated to its siblings.**

- F2 — ceil percentile fixed in `daily`, not in the other two
- F1 — drop accounting present in four paths, absent in `max-room-size`
- F3 — manifest validation solved in `soak`, absent in twelve workloads
- F7 — new window function added, old one left behind with its tests
- F10 — the `ForTest` seam used by two presence commands, orphaned in the third

This is the signature of features written one at a time with no cross-feature
review. It also means the refactor's package boundaries are worth more than
usual: most of these survive because there is no single place where the
behaviour is defined.

The later findings add a second shape: **a test that measures a case production
never produces, and so certifies nothing.**

- F8 — verdict thresholds duplicated between test and production, unlinked
- F14 — the catalog memory test uses a whitespace-delimited body; production
  bodies have no whitespace, which is the whole reason the retention is large
- F4 — the size-bucket table prints per-bucket percentiles it never computed

A green suite is the safety net this refactor leans on, so a test pointed at a
shape that cannot occur is worse than a missing one: it reports safety that was
never checked.

## Open questions

- Do per-step emitter goroutines in `presence.go:178` / `presence_capacity.go:179`
  need a `WaitGroup`? They terminate on ctx and the collector is mutex-guarded,
  so this is not a race — but step results are read while emitters may still be
  publishing. Needs a decision on whether that is intended.
- `ObserveEvent` / `ObserveMismatch` have no direct callers. Likely satisfied
  through an interface; not yet confirmed.
