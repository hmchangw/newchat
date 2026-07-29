# teams-chat-sync: per-user window end at execution time — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compute the sync window's end boundary `to` per user, when a worker
picks that user up, instead of once for the whole run at startup.

**Architecture:** `teams-chat-sync` is a run-to-completion Kubernetes CronJob. A
full run over the federation takes ~7 days, paced by Microsoft Graph throttling.
Today `run()` computes `to := startOfDayUTC(Now())` once and shares it with every
user, so a user processed on day 5 records a day-0 watermark and the next run
re-fetches five days it already had. This moves `to` — and the skip gate that
must agree with it — into `syncUser`, derived from a single `Now()` call that
also stamps `UpdatedAt`. All changes are in one file.

**Tech Stack:** Go 1.25, `go.uber.org/mock` (gomock), `stretchr/testify`.

**Spec:** `docs/superpowers/specs/2026-07-28-teams-chat-sync-to-execution-time-design.md`

## Global Constraints

- All commands go through the root `Makefile` — never run raw `go` commands
  (CLAUDE.md §2). **One carve-out:** the coverage check in Task 1 Step 7. The
  Makefile has no general coverage target (only the scoped
  `coverage-loadgen-soak`), so that step invokes `go test -coverprofile` and the
  repo's own `tools/coveragecheck` directly. This is the only permitted raw `go`
  invocation in this plan.
- TDD is mandatory: Red → Green → Refactor → Commit. Never write implementation
  before its test exists, and never skip confirming the test fails first
  (CLAUDE.md §4).
- Tests always run with `-race` (the Makefile handles this). Any test double
  shared across worker goroutines must be mutex-guarded.
- Minimum 80% coverage, 90% target for core business logic. `syncer.go` is core.
- Error wrapping: `fmt.Errorf("short description: %w", err)` describing what the
  current function was doing. Never a bare `err`.
- Structured logging via `log/slog` with key-value fields, never interpolated
  strings.
- Do not reformat, re-comment, or refactor code this plan does not name. Keep the
  diff minimal and focused (CLAUDE.md §5).
- Preserve every existing `//nolint:` directive on lines you touch — they
  suppress `gocritic hugeParam` warnings that will otherwise fail `make lint`.

## Background: what the code does today

`teams-chat-sync/syncer.go`, current shape (abridged to the lines that change):

```go
type summary struct {
	Total, Skipped    int
	Succeeded, Failed atomic.Int64
	Upserted          atomic.Int64
}

func (s *syncer) run(ctx context.Context) error {
	// ... loads users, builds cache ...
	to := startOfDayUTC(s.cfg.Now())        // ← one boundary for the whole run
	// ... spawns workers ...
	//     for u := range jobs {
	//         if err := s.syncUser(ctx, u, to, cache, &sum); err != nil { ... }
	//         sum.Succeeded.Add(1)
	//     }
	for _, u := range users {
		if !s.effectiveFrom(u).Before(to) {  // ← gate, outside the worker
			sum.Skipped++
			continue
		}
		jobs <- u
	}
	// ...
}

func (s *syncer) syncUser(ctx context.Context, u model.TeamsUser, to time.Time,
	cache map[string]cachedUser, sum *summary) error {
	graphChats, err := s.graph.ListUserChats(ctx, u.ID, s.effectiveFrom(u), to)
	// ...
	now := s.cfg.Now()                      // ← second Now() call, same boundary
	// ... buildChat(gc, cache, now, ...) ...
	// ... s.users.SetFrom(ctx, u.ID, to) ...
}
```

Two facts that shape the tests below:

1. **Today `Now()` is called once in `run()` and once per user in `syncUser`.**
   After the change it is called exactly once per user, in `syncUser`, and not at
   all in `run()`. Tests that advance the clock across calls therefore see a
   different call ordering before and after — this is what makes them fail first.
2. **Every existing test in `worker_test.go` drives `run()`, never `syncUser`
   directly**, and `fixedNow` returns a constant. They all compile and pass
   unchanged after this change, and serve as the regression net for the
   single-day path. `integration_test.go` only exercises the Mongo store and is
   untouched.

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `teams-chat-sync/syncer.go` | Orchestration, per-user worker, window math | Modified — the whole change lives here |
| `teams-chat-sync/worker_test.go` | Unit tests driving `run()` with mocked stores | Modified — three tests added, existing ones untouched |
| `teams-chat-sync/syncer_test.go` | Pure-function tests (`startOfDayUTC`, `voteSiteID`, `buildChat`) | Unchanged |
| `teams-chat-sync/integration_test.go` | Mongo store integration tests | Unchanged |
| `teams-chat-sync/main.go`, `store*.go` | Config, wiring, persistence | Unchanged |

---

### Task 1: Move `to` and the skip gate into `syncUser`

**Files:**
- Modify: `teams-chat-sync/syncer.go:125-231` (the `summary` struct, `run`, and `syncUser`)
- Test: `teams-chat-sync/worker_test.go` (add helper + three tests at end of file)

**Interfaces:**
- Consumes: nothing from earlier tasks — this is the first task.
- Produces:
  - `func (s *syncer) syncUser(ctx context.Context, u model.TeamsUser, cache map[string]cachedUser, sum *summary) (bool, error)` — the `to time.Time` parameter is gone; the first return is `skipped`, true when the user's watermark already reached the current UTC day and no work was done.
  - `summary.Skipped` becomes `atomic.Int64` (was `int`).
  - `func advancingNow(times ...time.Time) func() time.Time` — test helper in `worker_test.go`.

- [ ] **Step 1: Write the failing tests**

Append to `teams-chat-sync/worker_test.go`. Note `wtNow` / `wtTo` / `wtDefaultFrom`
already exist at the top of that file; `wtD5` and `wtToD5` are new.

```go
// wtD5 is wtNow advanced five days — the clock a worker sees when it reaches a
// user late in a multi-day run. wtToD5 is its startOfDayUTC.
var (
	wtD5   = time.Date(2026, 7, 19, 10, 30, 0, 0, time.UTC)
	wtToD5 = time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
)

// advancingNow returns a Now func that yields each time in order and then sticks
// on the last one. Mutex-guarded: workers call it concurrently under -race.
func advancingNow(times ...time.Time) func() time.Time {
	var mu sync.Mutex
	i := 0
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		t := times[i]
		if i < len(times)-1 {
			i++
		}
		return t
	}
}

// TestRun_ToTracksExecutionTime pins the core of this change: each user's window
// end comes from the clock when its worker picks it up, not from run start. With
// one worker the users are processed in order, so u1 sees day 0 and u2 sees day 5.
func TestRun_ToTracksExecutionTime(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	chats := NewMockTeamsChatStore(ctrl)
	graph := NewMockchatsFetcher(ctrl)
	s := newSyncer(users, chats, graph, syncConfig{
		MaxWorkers: 1, DefaultFrom: wtDefaultFrom,
		Now: advancingNow(wtNow, wtD5), DefaultSiteID: "site-default",
	})

	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
		{ID: "u2", SiteID: "site-b", Account: "bob"},
	}, nil)
	// No chats returned, so buildChat never runs and each user consumes exactly
	// one Now() call — keeping the clock sequence aligned with the user order.
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).Return(nil, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u2", wtDefaultFrom, wtToD5).Return(nil, nil)
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(nil)
	users.EXPECT().SetFrom(gomock.Any(), "u2", wtToD5).Return(nil)

	require.NoError(t, s.run(context.Background()))
}

// TestRun_GateEvaluatedAtProcessingTime is the intended behavior change: a user
// whose watermark already sits at the run's start day is no longer skipped, on
// the strength of the clock having moved on by the time a worker reaches it.
func TestRun_GateEvaluatedAtProcessingTime(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	chats := NewMockTeamsChatStore(ctrl)
	graph := NewMockchatsFetcher(ctrl)
	s := newSyncer(users, chats, graph, syncConfig{
		MaxWorkers: 1, DefaultFrom: wtDefaultFrom,
		Now: advancingNow(wtNow, wtD5), DefaultSiteID: "site-default",
	})

	from := wtTo // u2's watermark is already at the run's start day
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
		{ID: "u2", SiteID: "site-b", Account: "bob", From: &from},
	}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).Return(nil, nil)
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(nil)
	// The assertion: u2 is synced for [D0, D5), not gated out at dispatch.
	graph.EXPECT().ListUserChats(gomock.Any(), "u2", wtTo, wtToD5).Return(nil, nil)
	users.EXPECT().SetFrom(gomock.Any(), "u2", wtToD5).Return(nil)

	require.NoError(t, s.run(context.Background()))
}

// TestSyncUser_SkipReturnsSkippedNotSuccess guards the run accounting: a skipped
// user must report skipped=true with no error, so the worker counts it once as
// skipped rather than falling through to the success counter.
func TestSyncUser_SkipReturnsSkippedNotSuccess(t *testing.T) {
	s, _, _, _ := newTestSyncer(t, 1)
	from := wtTo // watermark already at startOfDayUTC(wtNow): empty window
	var sum summary

	// No mock expectations are set, so any Graph or store call fails the test.
	skipped, err := s.syncUser(context.Background(),
		model.TeamsUser{ID: "u1", SiteID: "site-a", Account: "alice", From: &from},
		map[string]cachedUser{}, &sum)

	require.NoError(t, err)
	assert.True(t, skipped, "a user with an empty window reports skipped")
	assert.Equal(t, int64(0), sum.Upserted.Load(), "a skipped user upserts nothing")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=teams-chat-sync`

Expected: FAIL. `TestSyncUser_SkipReturnsSkippedNotSuccess` fails to **compile**
(`too many arguments in call to s.syncUser` / `assignment mismatch: 2 variables
but 1 value`) — a compile failure is a valid Red here, since the signature is
part of what the test pins. Once you get past compilation, the other two fail on
gomock's "missing call(s)" for the `wtToD5` expectations, because today every
user shares the run-level `to` of `wtTo`.

- [ ] **Step 3: Change the `summary` struct**

In `teams-chat-sync/syncer.go`, replace the struct and its doc comment
(currently at lines 125-132):

```go
// summary is the per-run outcome reported in the final log line. Total is set
// once by the dispatching goroutine before fan-out; every other field is an
// atomic written by workers.
type summary struct {
	Total                      int
	Succeeded, Failed, Skipped atomic.Int64
	Upserted                   atomic.Int64
}
```

- [ ] **Step 4: Drop the run-level `to` and the dispatch gate**

In `run()`, delete the line `to := startOfDayUTC(s.cfg.Now())`, replace the
worker body, and reduce the dispatch loop to a bare send:

```go
	var sum summary
	sum.Total = len(users)

	jobs := make(chan model.TeamsUser)
	var wg sync.WaitGroup
	for i := 0; i < s.cfg.MaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range jobs {
				switch skipped, err := s.syncUser(ctx, u, cache, &sum); {
				case err != nil:
					sum.Failed.Add(1)
					slog.Error("teams chat sync: user failed", "userID", u.ID, "error", err)
				case skipped:
					sum.Skipped.Add(1)
				default:
					sum.Succeeded.Add(1)
				}
			}
		}()
	}
	for _, u := range users {
		jobs <- u
	}
	close(jobs)
	wg.Wait()
```

Then update the final log line to read `Skipped` atomically:

```go
	slog.Info("teams chat sync: run complete",
		"usersTotal", sum.Total, "usersSucceeded", sum.Succeeded.Load(),
		"usersFailed", sum.Failed.Load(), "usersSkipped", sum.Skipped.Load(),
		"chatsUpserted", sum.Upserted.Load())
```

Note the `switch` has an init statement and no tag expression — valid Go, and it
keeps all three counter writes in one place so
`Succeeded + Failed + Skipped == Total` holds by construction.

- [ ] **Step 5: Move `to` and the gate into `syncUser`**

Replace `syncUser` (currently lines 199-231) in full. Keep the existing
`//nolint:gocritic` directive — `make lint` fails without it.

```go
// syncUser fetches one user's chat window, upserts every chat it lists, and
// advances the user's watermark only after everything succeeded — a failed user
// keeps its old watermark and is retried next run. The window end is derived
// here, from a single Now() call taken when this worker picks the user up, so a
// run spanning several days credits each user with the day it was actually
// processed rather than the day the run started. That same Now() stamps
// UpdatedAt on every chat built below.
//
// It reports skipped=true when the user's watermark already reached the current
// UTC day, which means no Graph call and no watermark write. Note this is
// evaluated inside the worker, so "skipped" means a worker looked at the user
// and returned early — not that nothing ever touched it.
//
//nolint:gocritic // hugeParam: u is consumed once per user on a batch path; passing by value keeps the worker pure.
func (s *syncer) syncUser(ctx context.Context, u model.TeamsUser, cache map[string]cachedUser, sum *summary) (bool, error) {
	now := s.cfg.Now()
	to := startOfDayUTC(now)
	from := s.effectiveFrom(u)
	// Empty window: the watermark already reached today. Gating here, against the
	// same `to` the window and the watermark write use, is what makes a negative
	// window structurally impossible.
	if !from.Before(to) {
		return true, nil
	}

	graphChats, err := s.graph.ListUserChats(ctx, u.ID, from, to)
	if err != nil {
		return false, fmt.Errorf("list user chats: %w", err)
	}
	batch := make([]model.TeamsChat, 0, len(graphChats))
	for _, gc := range graphChats {
		built := buildChat(gc, cache, now, s.cfg.DefaultSiteID)
		// Defensive: DefaultSiteID is required,notEmpty in production, so this
		// only fires if the syncer is built with an empty default (tests).
		if built.SiteID == "" {
			slog.Warn("teams chat sync: siteID vote empty, skipping chat", "chatID", gc.ID, "userID", u.ID)
			continue
		}
		batch = append(batch, built)
	}
	if len(batch) > 0 {
		if err := s.chats.UpsertChats(ctx, batch); err != nil {
			return false, fmt.Errorf("upsert chats: %w", err)
		}
		sum.Upserted.Add(int64(len(batch)))
	}
	if err := s.users.SetFrom(ctx, u.ID, to); err != nil {
		return false, fmt.Errorf("advance watermark: %w", err)
	}
	return false, nil
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `make test SERVICE=teams-chat-sync`

Expected: PASS — all three new tests plus all eleven pre-existing ones. If any
pre-existing test fails, stop: they encode behavior this change is required to
preserve, so a failure means the implementation drifted, not that the test is
stale.

- [ ] **Step 7: Confirm coverage holds**

This is the plan's one permitted raw `go` invocation (see Global Constraints).
It uses `tools/coveragecheck`, the same gate the `coverage-loadgen-soak` target
uses — it exits non-zero below the threshold, so these are real assertions, not
output to eyeball:

```bash
cd /home/user/newchat && \
  go test -race -coverprofile=cov.out ./teams-chat-sync/ && \
  go run ./tools/coveragecheck -profile cov.out -include teams-chat-sync/ \
    -exclude main.go -exclude store_mongo.go -min 80 && \
  go run ./tools/coveragecheck -profile cov.out -include teams-chat-sync/syncer.go -min 90
```

The `-exclude` flags mirror `coverage-loadgen-soak`, which excludes
`soak_main.go`/`soak_store.go` for the same reason: `main.go` (process wiring)
and `store_mongo.go` (the Mongo adapter) are covered by `integration_test.go`
under the `integration` build tag, so a unit-only profile reports them at 0% and
would drag the package figure below the floor for reasons unrelated to this
change. Measuring them here would be measuring the wrong thing.

Expected: both checks exit 0 — the unit-tested surface clears the 80% floor and
`syncer.go` clears the 90% target for core business logic.

Then delete the profile so it is never committed:

```bash
rm -f /home/user/newchat/cov.out
```

If the 90% check fails, inspect which lines are uncovered with
`go tool cover -func=cov.out | grep syncer.go`. The likely culprit is
`syncUser`'s skip branch, which means `TestSyncUser_SkipReturnsSkippedNotSuccess`
is not reaching it — fix the test rather than lowering the threshold.

- [ ] **Step 8: Lint**

Run: `make lint`

Expected: clean. The likely failure is `gocritic hugeParam` on `syncUser` if the
`//nolint` directive was dropped in Step 5, or `unused` on `startOfDayUTC` if
Step 4 removed its last caller by mistake (it should still be called from
`syncUser`).

- [ ] **Step 9: Commit**

```bash
git add teams-chat-sync/syncer.go teams-chat-sync/worker_test.go
git commit -m "$(cat <<'EOF'
fix(teams-chat-sync): derive the window end per user at execution time

A full run takes ~7 days, but `to` was computed once at run start and shared by
every user, so a user processed on day 5 was queried with a day-0 boundary and
recorded a day-0 watermark. The next run re-fetched those five days, leaving
steady-state staleness at one-to-two run lengths.

`to` is now derived inside syncUser from a single Now() call taken when a worker
picks the user up, and the skip gate moves with it so the gate, the Graph filter,
and the watermark write share one boundary. syncUser reports skipped separately
from success, keeping Succeeded + Failed + Skipped == Total.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Repo-level verification and push

**Files:**
- Modify: none — this task only runs gates and pushes.

**Interfaces:**
- Consumes: the committed change from Task 1.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Run the full unit suite**

Run: `make test`

Expected: PASS across every service. `syncer.go` is not imported by any other
package (services are flat `package main` directories), so a failure elsewhere is
pre-existing — verify by stashing and re-running before investigating.

- [ ] **Step 2: Run the SAST gate**

Run: `make sast`

Expected: clean, no medium-or-above findings. This is a blocking CI gate
(CLAUDE.md §5), so run it before pushing rather than discovering it in the
pipeline. The change adds no new I/O, parsing, or conversions, so a new finding
here is unexpected — read it rather than suppressing it.

- [ ] **Step 3: Run the store integration tests**

Run: `make test-integration SERVICE=teams-chat-sync`

Expected: PASS. These only exercise the Mongo store and are unaffected by this
change, so this is a smoke check that nothing in the package broke at the build
level. Requires Docker; if Docker is unavailable in this environment, say so
explicitly rather than reporting the step as passed.

- [ ] **Step 4: Push**

```bash
git push -u origin claude/teams-chat-sync-user-to-timing-9ongjq
```

On network failure, retry up to 4 times with exponential backoff (2s, 4s, 8s,
16s). Do not open a pull request — the user has not asked for one.

---

## Verification checklist

Confirm each before reporting the work complete, with the command output in hand:

- [ ] `make test SERVICE=teams-chat-sync` passes — 3 new tests, 11 pre-existing ones
- [ ] `make test` passes repo-wide
- [ ] `make lint` clean
- [ ] `make sast` clean
- [ ] `run` and `syncUser` at or above 90% coverage
- [ ] `git log` shows the change committed and pushed to `claude/teams-chat-sync-user-to-timing-9ongjq`

## Already done — no task needed

Spec §7 (docs) is complete as of commits `d2629d5` and `a17f335`: the spec itself
is committed, and `2026-07-14-teams-chat-sync-design.md` carries an "Amended by"
header plus an inline superseded note on Sync flow step 2. No `docs/client-api.md`
change applies — `teams-chat-sync` is a CronJob registering no NATS handler and
no HTTP route.

## Out of scope

Named in spec §8, deliberately not addressed here. Do not fold them in:

- **Run-start user cache staleness** — `cache` is built once from `ListUsers`
  (`syncer.go:142-145`), so day-5 member resolution and siteID votes use a
  five-day-old snapshot, and `siteId` is `$setOnInsert`.
- **Graph settle margin** — `startOfDayUTC(now)` gives 0-24h of indexing lag
  tolerance depending on when a worker lands.
