# teams-chat-sync — Production Readiness Review

**Service:** `teams-chat-sync` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.5 / 5

The best-instrumented of the six `teams-*` CronJobs. It owns the three partial indexes that three downstream jobs scan on and creates them at startup; its `siteId` immutability via `$setOnInsert` is pinned by both a unit and an integration test; and it carries a named regression guard for a real past data-loss bug (the shared-chat claim). Error wrapping, secret hygiene and mock discipline are all clean.

One finding would block a release. **`ListUserChats` follows the server-supplied `@odata.nextLink` with the bearer token attached and no origin pin** — the sibling users pager in the same package *does* pin it, with a comment saying exactly why ("a tampered nextLink must not exfiltrate the token to another host"). Paired with the second defect — **`GRAPH_TLS_INSECURE_SKIP_VERIFY` defaulting to `true`**, which removes the TLS barrier that would otherwise make injection hard — an app-only `Chat.Read.All` token can be sent to an arbitrary host. Two further items are structural: `EnsureIndexes` failure only warns and the run continues, so three downstream jobs can silently degrade to collection scans with no alert; and SIGTERM produces a failure storm rather than a graceful stop.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 3 | 8 | 7 | 2 | **20** |

---

## 2. Go code quality — 4 / 5

Idiomatic, consistently context-wrapped, secret-hygiene rule verifiably upheld — but the Graph client is configured insecure-by-default in code while the shipped compose says otherwise.

### Findings
- `high` — `GRAPH_TLS_INSECURE_SKIP_VERIFY` defaults to **`true`**, so any deployment that simply omits the var (the k8s CronJob manifest is not in-repo) runs Graph calls with TLS verification disabled — `teams-chat-sync/main.go:51`. The service's own compose sets `:-false` (`teams-chat-sync/deploy/docker-compose.yml:18`) and `main_test.go:32` pins the `true` default, so the two disagree deliberately. Fleet is split: `teams-hr-sync/config.go:28` and `user-presence-service/sync/main.go:43` default `false`; `teams-user-sync/config.go:20` and `teams-chat-member-sync/main.go:44` default `true`. A fail-open default for a credential-bearing client is the wrong direction, and it is the enabling half of the D5 nextLink finding.
- `low` — bare `return err` from `validateConfig` — `teams-chat-sync/main.go:98`. CLAUDE.md §3 says never return bare `err`; the error is already self-describing (`"invalid config: …"`), so this is cosmetic, but it is a literal rule violation.
- `low` — audit-coverage gap, not a service defect: gosec (0 findings) and the 18 repo-owned semgrep rules (0 findings, 2/2 fixture tests pass) are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (egress 403). No dependency-CVE signal exists for this service.

**Secret-handling rule — verified clean.** `GraphClientSecret` is read at `main.go:43`, passed by value into `msgraph.Config` at `main.go:121`, and never touched again. No log site in the service takes a credential: the only fields logged are `userID`/`chatID`/`error` (`syncer.go:163`, `syncer.go:219`) and counters (`syncer.go:180-183`). The `pkg/msgraph` boundary is likewise disciplined — the token endpoint surfaces only status + OAuth error code, never the form body (`pkg/msgraph/msgraph.go:405-406`), and throttle logs explicitly exclude the token and endpoint (`pkg/msgraph/chats.go:141-151`). `slog` JSON handler set once at `main.go:60`; no `fmt.Println`/`log.Println` anywhere. No `errcode` usage, correctly — this job has no client-facing boundary.

### Recommendations
- `high` — Flip `main.go:51` to `envDefault:"false"` and make the on-prem proxy deployment set it explicitly; align `teams-user-sync` and `teams-chat-member-sync` in the same PR so the fleet has one default.
- `low` — Wrap at `main.go:98`: `return fmt.Errorf("validate config: %w", err)`.
- `low` — Port `teams-chat-member-sync/log_test.go`'s `recordingHandler` pattern here as a regression guard asserting no emitted record contains the client secret or a bearer token.

---
