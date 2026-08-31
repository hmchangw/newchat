# message-worker — Production Readiness Review

**Service:** `message-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The sole persister of message history, and it gets the genuinely dangerous part exactly right: **every one of `CLAUDE.md`'s `USING TIMESTAMP` pinning rules is correctly implemented and test-pinned** — plaintext creates pin, encrypted creates do not (a fresh nonce per attempt would make a same-timestamp per-cell conflict permanently undecryptable), tombstones and derived SETs ride the client clock. `handler.go` is 95.1% covered. The federation lane is correct and fully closed end to end. What holds it back: the **thread-reply path is O(N²) per thread** (a full partition rescan for `tcount` on every reply, plus two LWTs and ~10 serial Mongo round-trips), coverage is **56.8%** with `main()` alone accounting for ~46% of the deficit, and **the negative half of the timestamp rule is untested** — adding `USING TIMESTAMP` to either derived SET today passes the whole suite while silently corrupting data.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 12 | 17 | 15 | 6 | **51** |

---

## 2. Go code quality — 4 / 5

Idiomatic, lint-clean Go with disciplined `%w` wrapping, sentinel errors via `errors.Is`, and correct worker-tier `errcode` usage — undercut by dead code, a mock with no `go:generate` hook, one double-log, and three poison paths that discard the underlying parse error.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | `mock_hridentity_test.go` is a `DO NOT EDIT` MockGen file with **no `//go:generate` directive anywhere in the service**, so `make generate` never regenerates it. A change to `HRIdentityStore`/`identityResolver` silently leaves a stale hand-frozen mock. (The repo-wide zero-diff `make generate` is consistent with this — the file is simply **never visited**, not verified up to date) | `mock_hridentity_test.go:6`; `store.go:11` |
| medium | Log-and-return double-log: `migrateOne` logs the save failure at ERROR and returns the same `err`, which `handleBatch` propagates to `jsretry.Settle`, which logs it again. `SettleQuiet` exists precisely for the already-logged case | `teamsbatch.go:140-142`; `pkg/jsretry/jsretry.go:138` |
| medium | All three poison-message paths construct the `errcode` with a literal string and **silently drop the decode error**, so the reason a batch or event is unparseable is never recorded anywhere. Peers wrap it via `errcode.WithCause`, which keeps it server-side only — exactly the intent here | `handler.go:94-98`; `teamsbatch.go:64`, `:71` |
| medium | `reactionShortcode` is **dead across the whole repo** — referenced only by a prose comment and its own test. Because a `_test.go` file calls it, `unused` never fires, so it will rot indefinitely. `_ = tm.Forwarded` is the same shape | `teamstransform.go:113`, `:69` |
| low | `slog.Error` (not `ErrorContext`) on two paths that **have** a live `ctx` and hand-copy `request_id` out of it. Every other log site uses the `…Context` form | `store_cassandra.go:475`, `:490` |
| low | `Mode` is a stringly-typed enum with `"teams"`/`"default"` literals repeated across three files and four decision sites, validated only by an inline string comparison. One typo routes a pod to the wrong stream with no compile-time signal — while the service models closed enums correctly elsewhere | `main.go:84`, `:231`, `:361`; `bootstrap.go:46` |
| low | Two competing optional-dependency idioms in one service: a proper functional option for `NewHandler`, but `newTeamsBatchHandler` takes `injectedMetrics ...*persistenceMetrics` and reads `[0]` — a variadic used as an optional arg, silently accepting two and ignoring the second | `handler.go:49` vs `teamsbatch.go:38-44` |
| low | The publish closure selects its metric labels by branching on `msgID == ""`, coupling the label to a **transport detail** rather than the caller's intent, so a third publish site inherits whichever label its msgID happens to imply | `main.go:205-223` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Mojibake in two doc comments — em-dashes rendered as a stray `â` | `teamstransform.go:72`, `:94` |

### Recommendations
- `medium` — Add `//go:generate mockgen -source=teamssender.go -destination=mock_hridentity_test.go -package=main` so the third mock joins `make generate`.
- `medium` — Replace the log-then-return at `teamsbatch.go:140-142` with a bare return (Settle logs it), or switch `consume` to `jsretry.SettleQuiet`.
- `medium` — Attach `errcode.WithCause(err)` to the three `errcode.BadRequest` poison constructions so the parse failure is classified and logged once server-side.
- `medium` — Delete `reactionShortcode`, its test, and the `_ = tm.Forwarded` placeholder; the existing comments already carry the intent without dead symbols.
- `low` — Introduce `type mode string` with `modeDefault`/`modeTeams` constants and a `parseMode` validator; convert `newTeamsBatchHandler` to the existing option type for one DI idiom service-wide.
- `nitpick` — Switch the two `store_cassandra.go` logs to `ErrorContext`; fix the mojibake.

