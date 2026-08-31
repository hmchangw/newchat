# teams-chat-member-sync — Production Readiness Review

**Service:** `teams-chat-member-sync` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

Idiomatic and carefully built — consumer-defined interfaces, constructor DI, `Now` injected for testability, a genuine `-race` exercise on the user cache, batched user resolution memoised run-wide, and correct optimistic-concurrency handling against `teams-chat-sync`'s `updatedAt` watermark. Coverage reads 60.3% only because the unit profile cannot see the integration tests that do cover the store; every business-logic function is at 100%.

The integration dimension is where this service is genuinely weak, and the reason is what sits downstream. `room-worker` treats the `members` list this job writes as **authoritative and deletes every subscription not in it**. Two unguarded paths feed it: **an empty Graph roster is written verbatim and advances the chat**, so one degraded response silently empties a room; and **a member missing from `teams_user` is persisted with an empty `Account`** and the chat is marked done, so that person is permanently omitted with no log, no counter and no retry. Reading `teams_user` through a secondary-preferred client makes the second case more likely, on collections a sibling deliberately keeps on the primary for exactly this reason.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 2 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 4 | 6 | 12 | 2 | **24** |

---

## 2. Go code quality — 4 / 5

Idiomatic, tightly-wrapped errors and disciplined structured logging throughout; the one real defect is a security knob that ships insecure-by-default.

### Findings
- `high` — `GraphTLSInsecureSkipVerify` defaults to **true**, so a deployment that forgets the env var silently disables TLS verification on the connection that POSTs `client_secret` — `teams-chat-member-sync/main.go:44`
  Sibling services disagree: `teams-hr-sync/config.go:28` and `user-presence-service/sync/main.go:43` default `false`; only `teams-chat-sync/main.go:51` and `teams-user-sync/config.go:20` share the true default. The underlying `#nosec G402 … //nolint:gosec` double-suppression in `pkg/msgraph/msgraph.go:257-258` is correct — the defect is the *default*, not the mechanism.
- `low` — SAST audit coverage is incomplete: gosec (0 findings) and the 18 repo-owned semgrep rules (0 findings, 2/2 fixture tests pass) are clean, but `govulncheck` and the semgrep registry packs could not run (egress blocked, per GLOBAL_PREP). Environmental, not a service defect.
- `nitpick` — `errSuperseded` is a syncer-level control-flow concept declared in `store.go:18`; it reads as a store detail but is consumed only by `syncer.go:144`.

Verified clean: every wrap names what *this* function was doing (`store_mongo.go:39,56,85`; `syncer.go:72,95,127,177,181,185`); no bare `err`, no `error: %w`; `errors.Is(err, errSuperseded)` at `syncer.go:144`, never a string compare; JSON `slog` set once at `main.go:53`; no `fmt.Println`/`log.Print`/`os.Getenv`/`panic` anywhere in the service. **Secrets rule holds**: the service never logs config; `pkg/msgraph/msgraph.go:406` surfaces only the OAuth error code, and `pkg/msgraph/chats.go:156` logs status/Retry-After but explicitly never the token or endpoint. `pkg/errcode` is correctly absent — this is a CronJob with no client boundary.

### Recommendations
- `high` — Flip `envDefault` to `"false"` at `main.go:44` and set `GRAPH_TLS_INSECURE_SKIP_VERIFY=true` only in the on-prem overlay; align the three outlier services in one PR.
- `medium` — Log once at startup at WARN level when `GraphTLSInsecureSkipVerify` is true, so an accidentally-insecure prod run is visible in logs.
- `low` — Move `errSuperseded` to `syncer.go` (or a shared `errors.go`), leaving `store.go` purely the consumer-side contract.
- `low` — Re-run `make sast-vuln` from an environment with `vuln.go.dev` reachable before release sign-off.

---
