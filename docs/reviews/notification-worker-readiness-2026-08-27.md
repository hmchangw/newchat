# Production Readiness Review — `notification-worker`

| | |
|---|---|
| **Service** | `notification-worker` |
| **Date** | 2026-08-27 |
| **Branch** | `claude/notification-worker-prod-ready-bupzgf` |
| **Overall score** | **2.7 / 5** (mean of six dimensions) |
| **Method** | Six independent expert passes (code quality, architecture, test coverage, maintainability, integration, performance), findings cross-verified against the source |

## Executive summary

`notification-worker` is a well-crafted service carrying a small number of genuinely
serious defects. The craftsmanship is visible and consistent: every NATS subject is
built through `pkg/subject` (no raw `fmt.Sprintf` anywhere), the primary consumer
follows the `Messages()` + semaphore pattern exactly, ack discipline runs through
`jsretry.Settle` with `errcode.Permanent` for poison messages, Mongo reads carry
precise projections and batch `$in` fetches, and `HandleMessage` — the actual business
logic — sits at 97.4% unit coverage with strong fail-open error-path tests. The
integration suite is textbook-compliant with CLAUDE.md Section 4.

What holds it back is concentrated in three places. **First, a shutdown race**: the
cache-invalidation reader goroutine (`main.go:378-399`) is tracked by no `WaitGroup`,
and shutdown closes the channel it sends on one step after stopping its iterator —
a send on a closed channel panics the pod. Four of the six experts found this
independently. **Second, a bootstrap defect**: `main.go:268` passes the `.created`
leaf subject as the *stream's* subject set, so a dev boot with `BOOTSTRAP_STREAMS=true`
narrows `MESSAGES-CANONICAL-{site}` and strips `.edited/.deleted/.reacted/.pinned`
from every other publisher, last-writer-wins. **Third, the hot path serializes work
that is independent**: settings, presence, and badge lookups run back-to-back
(`handler.go:243-259`) for ~13 s of worst-case serial wall time against a 30 s
`AckWait`, and the badge RPC is the one fan-out call that is never chunked, so a
5 000-member room becomes a single 5 000-account request per message.

Separately, the service deviates from the mandated per-service layout — there is no
`store.go` / `store_mongo.go` / `//go:generate mockgen`, and a full Mongo store
implementation lives inside `main.go` — which is both a convention violation and the
direct cause of the coverage number below.

**Coverage is 55.6%, against a repo floor of 80%**, which floors that dimension at 1
per CLAUDE.md Section 4. This number needs an honest caveat: `func main()` alone is
184 statements (27.5% of the package) of untested wiring, and Docker was unavailable
in the audit environment so the `//go:build integration` suite — which does exercise
the uncovered Mongo adapters — could not run. Excluding `main()`, coverage is 76.9%.
The floor is still missed, and the fix (extracting the store out of `main()`) is the
same fix the architecture and maintainability dimensions independently call for.

Nothing here is unshippable in the sense of "wrong notifications get delivered". The
shutdown race and the bootstrap narrowing are the two items that would bite in
production and both are small, local fixes.

## Dimension scores

| # | Dimension | Score |
|---|---|---|
| 1 | Code quality | 3 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |
| | **Overall** | **2.7 / 5** |

## Findings by severity

Counts are **deduplicated** across dimensions — the shutdown race was reported by four
experts and the missing `store.go` by three; each is counted once, at the highest
severity any expert assigned it.

| Severity | Count |
|---|---|
| `critical` | 1 |
| `high` | 10 |
| `medium` | 24 |
| `low` | 13 |
| `nitpick` | 9 |
| **Total unique** | **57** |

### Verification notes

- Coverage independently re-run and confirmed: **55.6%** total, `HandleMessage` 97.4%.
- `gosec` runs clean (exit 0, no medium+ findings). `govulncheck` and `semgrep` could
  **not** be executed in this environment (`vuln.go.dev` blocked by the proxy; `make tools`
  aborts on a `pipx`/`uv` version conflict). Two of three blocking SAST gates are
  therefore unverified here, not passed — see Chapter 2.
- `make test SERVICE=notification-worker` passes with `-race`.
- `make generate` produces no diff — mocks are not stale. This service uses hand-written
  stubs rather than mockgen, and has no `//go:generate` directives.

---

# Chapter 2 — Code quality

**Score: 3 / 5**

Precise error wrapping, correct `errcode` tiering (`Permanent` in a worker, `Parse` for
remote envelopes), consumer-defined interfaces, and thoughtfully documented fail-open
behaviour. Held back by one shutdown-crash defect, one latent correctness bug, a layout
deviation, and logging that drops trace correlation.

## Findings

### `high` — Send on a closed channel during shutdown

The cache-invalidation goroutine at `notification-worker/main.go:378-399` is tracked by
no `WaitGroup`. Shutdown step 3 (`main.go:490`) calls `invalIter.Stop()`; step 4
(`main.go:494`) immediately runs `close(invalCh)`. `Stop()` is asynchronous, so a
goroutine already past `invalIter.Next()` will execute `invalCh <- evt.RoomID`
(`main.go:392`) on a closed channel and panic the process. The main consumer loop is
deliberately `wg`-tracked for exactly this hazard (`main.go:411-416`); this loop is not.
Violates CLAUDE.md Section 3 ("never launch goroutines without a clear termination path").

*Verified directly against the source; independently reported by four of six experts.*

### `medium` — Two of three SAST gates unverifiable in this environment

`gosec` ran clean (exit 0, no medium+ findings). `govulncheck` failed with
`Get "https://vuln.go.dev/index/modules.json.gz": Forbidden` (agent proxy). `semgrep`
could not be installed — `make tools` aborts with `pipx needs uv>=0.9.17, but
/root/.local/bin/uv reports 0.8.17`. SAST is a blocking CI gate per CLAUDE.md Section 5,
so this must be re-run somewhere with network and working tooling before the gate can be
called green. **No medium+ SAST finding touching `notification-worker/` was detected by
the one scanner that did run.**

### `medium` — Mention matching is case-sensitive against lowercased input

`pkg/mention/mention.go:47` lowercases every parsed account. `handler.go:199` then indexes
`mentionedAccounts[m.Account]` using the raw `subscriptions.u.account` value loaded at
`main.go:107`, with no normalization. Any stored account containing an uppercase character
silently loses its mention — no push in large rooms (`handler.go:228`) and no delivery on a
thread-only reply (`handler.go:209`). `mentionnames.go:72` gets this right and lowercases
its map keys; the handler does not. Latent — it only fires if account data is not already
uniformly lowercase.

### `medium` — Log-and-return double-logs

`handler.go:302` logs the emit failure and `handler.go:309` returns it; `main.go:449` then
passes it to `jsretry.Settle`, which logs it again at `pkg/jsretry/jsretry.go:122`.
CLAUDE.md Section 6: "Never log AND return."

### `medium` — Mandated per-service layout not followed

No `store.go`, no `store_mongo.go`, no `//go:generate mockgen`, no `mock_store_test.go`.
Mongo access is scattered across `main.go:83-178` (`mongoMemberLoader`, `fillHomeSites`),
`threads.go:25-55`, and `usersettings.go:71-145`. Siblings comply
(`broadcast-worker/store.go`, `message-worker/store.go`).

### `medium` — Trace correlation lost in most log lines

Non-`Context` `slog` calls at `handler.go:220,302,346,456`; `members.go:46,62,80`;
`presence.go:76,83,87,94`; `main.go:386,394,441,443` — with `request_id` hand-threaded as a
field. Only `handler.go:438` uses `WarnContext`. This defeats the o11y SDK's
trace-correlated logging described in CLAUDE.md Section 1.

### `medium` — `HandleMessage` is a 220-line function

`handler.go:93-313` performs decode, cache invalidation, thread resolution, three filter
passes, settings/presence/badge fan-out, and batched emit in one body.

### `low` — Bare error returns

`emit.go:86` returns `err` unwrapped; `members.go:73` returns `ctx.Err()` unwrapped.
CLAUDE.md Section 3 requires `fmt.Errorf("...: %w", err)` with context.

### `low` — Inconsistent `errcode.Parse` guard

`badge_client.go:53` and `parent_fetcher.go:82` gate on `ee.Code.Valid()`; `presence.go:86`
does not, so any reply carrying an `error` field is misread as a failure.

### `low` — Secrets carry `envDefault` instead of `required`

`main.go:40` `NatsCredsFile`, `main.go:45` `MongoPassword`, `main.go:57` `ValkeyPassword`
all default to `""`. CLAUDE.md Section 6: "never default secrets or connection strings."

### `low` — Unchecked type assertion

`members.go:71` asserts `res.Val.([]roomsubcache.Member)` without the comma-ok form.

### `low` — Production test seams

`presence.go:132-135` declares `isDND` / `isPresenting` as package-level mutable func vars
that always return `false`, swapped by `presence_test.go:34-49`. Shared mutable global
state in production code; blocks `t.Parallel()`.

### `nitpick` — Mixed log-key casing

`messageId`/`roomId`/`siteId` alongside `request_id` in the same call
(`handler.go:303-304,347`; `main.go:465-466`).

### `nitpick` — Pre-Go-1.22 idioms

`ch := ch` loop-variable copy (`presence.go:70`), `sort.Strings` (`handler.go:8,255`),
manual clamps where `min()` fits (`handler.go:288`, `presence.go:116`). `main.go:420`
records `LoopFailed` on the normal shutdown-induced iterator-closed error.

## Recommendations

1. **`high`** — Track the invalidation goroutine with a `sync.WaitGroup` and join it *before*
   `close(invalCh)`; or drop the close and let `invalCancel()` terminate the drain.
2. **`medium`** — Normalize `Member.Account` to lowercase in `mongoMemberLoader.Load`, or
   lowercase at the `handler.go:199` lookup, and add a table case pinning mixed-case mentions.
3. **`medium`** — Switch `main.go:449` to `jsretry.SettleQuiet`, or remove the `slog.Error` at
   `handler.go:302`.
4. **`medium`** — Extract `store.go` (interface + `//go:generate mockgen`) and `store_mongo.go`;
   move `mongoMemberLoader` out of `main.go`.
5. **`medium`** — Convert request-path `slog.X` calls to `slog.XContext`, drop the hand-threaded
   `request_id`, and settle on one key casing.
6. **`medium`** — Split `HandleMessage` into `resolveThreadContext`, `selectRecipients`, and
   `emitBatches`.
7. **`low`** — Wrap the bare returns, add the comma-ok assertion, mark secrets `required`, and
   re-run `make sast` where `vuln.go.dev` and `pipx`/`uv` are reachable.
