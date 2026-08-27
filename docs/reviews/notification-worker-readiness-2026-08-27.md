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
