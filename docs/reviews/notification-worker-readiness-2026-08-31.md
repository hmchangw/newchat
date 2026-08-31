# notification-worker — Production Readiness Review

**Service:** `notification-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The hot path is well-engineered — sonic with pretouch, narrowed-candidate batching, L1/L2 room caches, a bounded worker semaphore, precise Mongo projections, clean `jsretry` discipline. Three things pull the score down, and all three were found independently by more than one expert. **A shutdown-time send-on-closed-channel race can panic the process during a routine rolling restart**: the member-event invalidation goroutine is tracked by no `WaitGroup`, yet shutdown stops the iterator and closes the channel it sends into two steps later — a message already returned by `Next()` sends after the close, and a `select` with a `default` does not save it. **The dev bootstrap narrows a shared stream**: it creates `MESSAGES-CANONICAL-{site}` with only the `.created` leaf instead of the declared `.>` wildcard, silently constraining a stream four services consume. And **the mute gate fails open at remote sites** — the user-settings replica this worker reads has no creation or backfill path, so a muted user at a site added after their last settings write gets pushed anyway. Coverage at 59.0% is under the 60% critical line, driven by a 196-statement `main()` at 2.0%.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 2 | 12 | 18 | 13 | 9 | **54** |

---

## 2. Go code quality — 4 / 5

Idiomatic, carefully-reasoned Go with disciplined error wrapping and correct `errcode`/`jsretry` tiering; marred by one shutdown-time send-on-closed-channel race and a cluster of logging-discipline drifts (double-log, non-context `slog`, missing request IDs).

**Message-body privacy: verified clean.** No log line in the service carries `msg.Content` or a payload; `handler.go:479` logs only counts. The single body-logging path is `logctx.CapturePayload` via `logctx.ConsumeContext` (`main.go:363`), which is inert here because the service never calls `logctx.Configure` (`pkg/logctx/limiter.go:27,87` — `capturePayloads` defaults false).

### Findings
- `high` — the canonical-member-event reader goroutine is untracked by any WaitGroup (`notification-worker/main.go:304-325`), yet shutdown step 4 closes the channel it sends on (`notification-worker/main.go:420`) after only `invalIter.Stop()` (`main.go:416`). `Stop()` does not wait for a message already returned by `Next()`, so a decode in flight can reach `invalCh <- evt.RoomID` (`main.go:318`) after the close and panic the process during graceful shutdown. Every other goroutine here has a tracked termination path; this one does not.
- `medium` — log-and-return double-logging on the emit path: `slog.Error("emit push batch failed", …)` (`notification-worker/handler.go:332`) is followed by returning the same error, which `jsretry.Settle` (`notification-worker/main.go:375`) logs again (`pkg/jsretry/jsretry.go:129`). `SettleQuiet` exists precisely for already-logged paths; CLAUDE.md's "never log AND return" is the same rule one transport over.
- `medium` — the per-message path uses non-context `slog` and so loses o11y trace correlation: `handler.go:247`, `handler.go:332`, `handler.go:387`, `handler.go:497`, `presence.go:76,83,87,95`, `usersettings.go:107`, `main.go:312,320`. Only `handler.go:479` uses `WarnContext`. Sibling hot-path worker `broadcast-worker/handler.go:205,219,228,234,255` is uniformly `*Context`.
- `medium` — four warn lines in the presence fan-out carry no request/correlation ID at all (`notification-worker/presence.go:76,83,87,95`), against "include in all log lines"; the handler and `usersettings.go:107` do attach it, so a presence failure is the one event that cannot be joined to its message.
- `medium` — `logctx.ConsumeContext` is called (`notification-worker/main.go:363`) but the service never declares `logctx.Config` nor calls `logctx.Configure`/`SetupDefault` (contrast `broadcast-worker/main.go:99,125,133`). The admitted `X-Debug` rung is therefore dead on arrival: the package limiter stays `denyAll` (`pkg/logctx/limiter.go:24`) and the default handler installed by `obs` (`pkg/obs/obs.go:229`) is not the logctx wrapper, so sub-INFO records never emit. Half the helper's contract is wired.
- `low` — bare `return err` from the JetStream publish adapter (`notification-worker/emit.go:91`), against "never return bare `err`". The caller re-wraps (`emit.go:63`), so impact is cosmetic, but it is the one unwrapped return in the service.
- `low` — sonic marshals `model.PushNotificationEvent`, which carries a `map[string]int` (`pkg/model/push.go:17`), on the emit hot path (`notification-worker/emit.go:41`) with no sonic wire-compat test in this service (only `pretouch_test.go` mentions sonic). CLAUDE.md names map fields as one of two triggers for confirming compat before adopting sonic, with `broadcast-worker`/`message-gatekeeper` as the pattern. Dedup keys off `evt.ID`, not a payload hash, so the actual risk is small — the missing verification is the gap.
- `low` — audit-coverage gap, not a service defect: gosec and the repo-owned semgrep rules are clean repo-wide; `govulncheck` and the semgrep registry packs could not run (blocked egress).
- `nitpick` — `ch := ch` loop-variable copy (`notification-worker/presence.go:70`) is obsolete under `go 1.25.13` (`go.mod:3`); the newer `errgroup` fan-out in `handler.go:379` already omits it.
- `nitpick` — 16 identifiers are exported from `package main` (e.g. `MemberCache`, `EligibleForPush`, `ParentMessageInfo`), which nothing can import; `badgeClient` (`badge_client.go:23`) and `notifSettings` (`usersettings.go:33`) show the intended unexported form.

### Recommendations
- `high` — track the invalidation reader with `invalWG` (or a dedicated one) and wait on it *before* `close(invalCh)`; alternatively drop the close entirely and let `invalCancel()` plus a `select` on `invalCtx.Done()` terminate the drain worker.
- `medium` — switch `main.go:375` to `jsretry.SettleQuiet` (handler already logs) or delete `handler.go:332` and let `Settle` own the log; pick one owner.
- `medium` — mechanically convert the per-message `slog.Warn/Error` calls listed above to `WarnContext`/`ErrorContext`, and add `"request_id", natsutil.RequestIDFromContext(ctx)` to the four `presence.go` lines (thread `ctx` into the goroutine closure, which it already captures).
- `medium` — add `DebugLog logctx.Config \`envPrefix:"DEBUG_LOG_"\`` to `config` and call `logctx.SetupDefault(os.Stdout)` + `logctx.Configure(cfg.DebugLog)` at startup, matching the other canonical-stream workers — or drop `ConsumeContext` for plain `natsutil.StampRequestID` so the code stops implying a capability it lacks.
- `low` — wrap the adapter error at `emit.go:91` (`fmt.Errorf("jetstream publish: %w", err)`), and add a sonic-vs-stdlib wire-compat test for `PushNotificationEvent` (including a populated `UnreadCounts`) modelled on `broadcast-worker`'s.
