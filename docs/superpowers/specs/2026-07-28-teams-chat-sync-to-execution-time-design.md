# teams-chat-sync: per-user window end at execution time — Design

**Date:** 2026-07-28
**Status:** Approved
**Amends:** `2026-07-14-teams-chat-sync-design.md` (supersedes its §"Window",
lines 157-161, which specify a single run-level `to`)

Move the sync window's end boundary `to` from a run-level value computed once at
startup to a per-user value computed when a worker picks the user up. Scope is
`teams-chat-sync/syncer.go` and its tests — no config, store, model, or `pkg/`
changes.

## 1. Problem

`run()` computes `to := startOfDayUTC(s.cfg.Now())` once (`syncer.go:147`) and
every user in the run shares it: it is the Graph filter's upper bound, and it is
the value written to `teams_user.from` on success.

A full run over the federation takes about **seven days**, paced by Graph
throttling. So a user the fan-out reaches on day 5 is queried with a `to` from
day 0 and has its watermark recorded as day 0 — five days behind the moment it
was actually processed. The next run starts on day 7 and re-fetches from day 0,
and itself takes seven days. Steady-state staleness is therefore **one to two
run lengths**, and the lag compounds: work done on day 5 of a run is credited as
if it happened on day 0.

The watermark is not wrong — the window really did end at day 0, so no chat is
lost — but it is needlessly conservative, and the cost is a permanent extra run
length of lag for every user not processed early in the run.

Throughout this document `D0` is the day a run starts and `D5` a day five days
into that same run.

## 2. Change

`to` is computed inside `syncUser`, once per user, from a single `Now()` call
that also stamps `UpdatedAt`:

```
BEFORE                                  AFTER
run():                                  run():
  cache := ListUsers()                    cache := ListUsers()
  to := startOfDayUTC(Now())                (no run-level `to`)
  spawn MaxWorkers workers                spawn MaxWorkers workers
  for u in users:                         for u in users:
    if from >= to: Skipped++; skip          jobs <- u
    jobs <- u                             close(jobs); wg.Wait()
  close(jobs); wg.Wait()
                                        syncUser(u, cache, sum):
syncUser(u, to, cache, sum):              now := Now()
  ListUserChats(u, from, to)              to  := startOfDayUTC(now)
  buildChat(gc, cache, Now(), …)          if from >= to: return skipped
  SetFrom(u, to)                          ListUserChats(u, from, to)
                                          buildChat(gc, cache, now, …)
                                          SetFrom(u, to)
```

The skip gate moves with `to`. It has to: the gate and the window must agree on
where the day boundary is, and after this change that boundary is per-user. One
`Now()` call per user feeds the gate, the Graph filter, the `UpdatedAt` stamp,
and the watermark write — today it is two calls (`syncer.go:147` and `:210`)
serving one boundary.

**Rejected alternative — gate stays in the dispatch loop, recomputing `Now()`
per iteration.** This works today only because `jobs` is unbuffered
(`syncer.go:151`), so the dispatch loop advances in lockstep with the workers
and the gate's `now` lands within milliseconds of the worker's. The agreement is
load-bearing but invisible: buffering `jobs` to smooth throughput would silently
drift the gate from the window by however long the buffer holds, with nothing
failing loudly. Rejected for that reason, not for correctness today.

**Rejected alternative — keep the run-level `to` for the Graph query, write the
watermark at execution time.** Data loss. The watermark would advance past the
window actually queried, so `[run-to, exec-time)` is fetched by no run: this run
stopped at `run-to`, and the next run starts after it. Every chat updated in
that span is dropped permanently.

## 3. Components

| Symbol | Change |
|---|---|
| `syncer.run(ctx) error` | Signature unchanged. Loses the run-level `to` and the dispatch skip gate. |
| `syncer.syncUser(...)` | Drops the `to time.Time` parameter; returns `(skipped bool, err error)`. |
| `summary.Skipped` | `int` → `atomic.Int64` — workers write it now, not the dispatcher. |
| `startOfDayUTC`, `effectiveFrom`, `syncConfig` | Untouched. `Now` is already injectable. |

### Run accounting

Today the gate sits outside the worker, so a skipped user is counted in
`Skipped` and reaches no other counter. Once the gate moves inside `syncUser`, a
skip that returns a bare `nil` would fall through to the worker's
`sum.Succeeded.Add(1)` and be counted **twice** — once skipped, once succeeded —
and the run summary would silently misreport.

`syncUser` therefore returns its outcome rather than having the worker infer it:

```go
func (s *syncer) syncUser(...) (skipped bool, err error)

// worker:
switch skipped, err := s.syncUser(ctx, u, cache, &sum); {
case err != nil:
    sum.Failed.Add(1)
    slog.Error("teams chat sync: user failed", "userID", u.ID, "error", err)
case skipped:
    sum.Skipped.Add(1)
default:
    sum.Succeeded.Add(1)
}
```

All three counter writes stay in one place and
`Succeeded + Failed + Skipped == Total` holds by construction — which today
holds only as a side effect of the gate living outside the worker. A sentinel
`errSkipped` matched with `errors.Is` was considered and rejected: it routes a
non-error through the error channel and reads worse at the call site.

## 4. Semantics

**Preserved.** The window stays half-open `[from, to)` and the watermark is set
to exactly the `to` that was queried, so the seven midnights a run crosses
cannot open a gap or destructively double-fetch (upserts are idempotent). The
gate guarantees `from < to` before any Graph call, so `SetFrom` never moves a
watermark backwards and a negative window is structurally impossible rather than
something to reason about. Per-user failure still holds that user's watermark
for the next run; the run still exits non-zero if any user failed.

**Intended behavior change.** A user whose watermark already sits at the run's
start day is no longer skipped. Run starts D0; the user's `from` is D0 from the
previous run; a worker reaches it on D5 → `to = D5`, `from = D0 < D5`, so it
syncs `[D0, D5)` instead of being gated out. That is the fix working — under the
current code that user waits until the *next* run to see those five days.

**Staleness bound.** From up to two run lengths down to at most one.

**Single writer.** The CronJob uses `concurrencyPolicy: Forbid` (or is triggered
manually), so only one run is ever in flight and a given `teams_user.from` has
exactly one writer. No cross-run watermark coordination is needed. If overlapping
runs are ever allowed, this section must be revisited.

## 5. Error handling

Unchanged. The skip returns before any I/O and cannot fail. Graph failure, upsert
failure, and `SetFrom` failure all behave exactly as today: log, hold the
watermark, continue with other users, exit non-zero at the end.

One nuance to carry in a code comment: `Skipped` no longer means "no worker
touched this user" — it means "a worker evaluated it and returned early".

## 6. Testing

Red-Green-Refactor per CLAUDE.md §4.

**The existing tests cannot drive the Red phase.** `fixedNow` returns a constant
(`worker_test.go:24`), so `wtTo` is identical for every user and every current
assertion stays true after the change. They are the regression net proving the
single-day path is untouched; they need only the mechanical `syncUser` signature
update.

New tests, each written to fail first:

1. **`TestRun_ToTracksExecutionTime`** — a mutex-guarded `Now` fake returning D0
   on the first call and D5 on the second. Assert `ListUserChats` and `SetFrom`
   see `to=D0` for the first user and `to=D5` for the second. Fails today: both
   receive D0.
2. **`TestRun_GateEvaluatedAtProcessingTime`** — a user with `from = D0`
   processed when `Now()` returns D5. Assert it is not skipped and is queried
   with `[D0, D5)`. Fails today: gated out at dispatch.
3. **`TestSyncUser_SkipReturnsSkippedNotSuccess`** — call `syncUser` directly,
   assert `skipped == true`, `err == nil`, and that no Graph or store call is
   made. This is what the `(bool, error)` return buys: the outcome is directly
   assertable without exposing `summary`.

The advancing `Now` fake must be mutex-guarded — `make test` runs `-race` and
workers call it concurrently.

Coverage: `syncer.go` is core business logic, so the 90% target applies
(CLAUDE.md §4 Coverage), against an 80% hard floor.

## 7. Docs

This spec supersedes step 2 of the "Sync flow" section in
`2026-07-14-teams-chat-sync-design.md`. That document is amended in place: an
"Amended by" pointer in its header, and an inline superseded note on step 2
itself, so the stale run-level `to` cannot be read without the correction.

No `docs/client-api.md` change: `teams-chat-sync` is a CronJob that registers no
NATS handler and no HTTP route, so the client-facing-handler rule in CLAUDE.md
§5 does not apply.

## 8. Explicitly out of scope

Both were raised and deferred; neither is blocked by this change.

- **Run-start user cache.** `cache` is built once from `ListUsers`
  (`syncer.go:142-145`). On day 5 of a run, member accounts and siteID votes are
  resolved against a five-day-old `teams_user` snapshot, and `siteId` is written
  `$setOnInsert`, so a stale vote is permanent for that chat. Users onboarded
  mid-run are invisible to it.
- **Graph settle margin.** `startOfDayUTC(now)` yields 0-24h of Graph indexing
  lag tolerance depending on when a worker lands. A user processed at 00:00:30
  UTC gets a `to` thirty seconds old, and anything Graph has not yet indexed is
  skipped permanently once the watermark advances past it. This exists in the
  current code, but per-user `to` crosses that boundary once per midnight rather
  than once per run, so it fires more often.
