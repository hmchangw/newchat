# history-service — Production Readiness Review

**Service:** `history-service`
**Date:** 2026-08-31
**Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents (code quality, architecture, test coverage, maintainability, integration, performance), each judging against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The largest service in the repo (~25.7k lines) and, on the evidence, one of the better-engineered ones: error wrapping, `errcode` tiering, projection discipline, goroutine termination and WHY-comment quality are all genuinely strong, and the bucket-walk read path is deliberately and thoughtfully bounded. Three things hold it back. First, **test coverage at 55.0% sits below the repo's 60% critical line**, with the entire store layer (`cassrepo` 32.1%, `mongorepo` 3.5%) and `cmd/` (11.3%) effectively unexercised outside integration builds. Second, a **real correctness defect in reaction mirroring**: reactions on a `TShow=true` thread reply never reach `messages_by_room`, so the channel timeline permanently loses them on reload. Third, the service is the repo's **only** user of a `cmd/` + `internal/` layout, which `CLAUDE.md` §1 forbids and which blocks reuse of genuinely reusable code. None of these is a shipping blocker on its own; the reaction-mirroring bug is the one that silently corrupts user-visible state.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 4 / 5 |

### Findings by severity

| Severity | Count |
|----------|-------|
| critical | 1 |
| high | 6 |
| medium | 17 |
| low | 16 |
| nitpick | 7 |
| **Total** | **47** |

> **Audit-coverage caveat.** `gosec` and the 18 repo-owned `semgrep` rules ran clean repo-wide (0 findings; rule fixtures 2/2). `govulncheck` and the `semgrep` registry packs could **not** run — `vuln.go.dev` and `semgrep.dev` are blocked by this environment's egress policy (403). Dependency-CVE coverage is therefore unverified and must be re-run on a network-permitted runner before shipping.

---

## 2. Go code quality — 4 / 5

Error wrapping, `errcode` tiering, sentinel/`errors.Is` discipline and goroutine bounding are exemplary across ~8k lines of production code. The deductions are a genuine log-and-return violation, inconsistent `slog` context propagation, and store-interface naming drift.

### Evidence

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | Logs **and** returns the same error, double-logging once the boundary classifies it (§3 "Never log AND return") | `internal/cassrepo/utils.go:161` |
| medium | Nine request-path log sites use the non-`Context` `slog` form, dropping ctx-carried trace/span correlation | `internal/service/threads.go:65`, `:121`, `:374`; `internal/service/migration.go:139`, `:145`; `internal/service/messages.go:405`, `:737`, `:742`; `internal/cassrepo/utils.go:162` |
| medium | Three of those carry no `request_id` at all — a publish/marshal failure cannot be tied to a request | `internal/service/messages.go:737`, `:742`; `internal/service/threads.go:374` |
| low | Store interfaces named `*Repository`, not `<Domain>Store` per §3 — and inconsistent with `UserStore`/`AppStore` declared in the same file | `internal/service/service.go:52`, `:57`, `:68`, `:105`, `:120` |
| low | Interpolated log message instead of a structured key: `"skipped malformed "+what+" attachment blobs"` | `internal/service/attachments.go:28` |
| low | Dynamic attribute key — `slog.Error("invalid config", c.name, c.value)` emits a different JSON field per knob, so the field is unqueryable | `cmd/main.go:56` |
| low | 9 of 19 `service` test files sit in external `package service_test`, forcing `UnavailableQuoteMsg` to stay exported for tests only | `internal/service/utils.go:97`, `internal/service/threads_test.go:607` |
| low | `getOrLoad`'s doc comment is separated from its function by `remove`, so godoc misattributes it | `internal/readcache/readcache.go:56` |
| low | Audit-coverage gap (environmental, not a service defect): `govulncheck` + semgrep registry packs blocked by egress | — |
| nitpick | `ErrEncryptedRowCipherDisabled` exported and documented for cross-package `errors.Is`, but no external referencer | `internal/cassrepo/decrypt.go:15` |
| nitpick | Mixed log-key casing: `messageID`/`gotRoom`/`wantRoom` beside the repo's `room_id`/`request_id` | `internal/service/threads.go:374` |
| nitpick | `ThreadSubRow` carries `bson` tags but no `json` tags | `internal/mongorepo/threadsubscription.go:21` |

The nine bare-`slog` sites are **drift, not house style** — the rest of the service correctly uses `WarnContext` (e.g. `internal/service/rooms.go:142`), which is what makes them worth fixing rather than accepting.

### Recommendations

- `medium` — Delete the `slog.Warn` at `internal/cassrepo/utils.go:162` and keep only the returned error; the boundary logs it once at the right level with the request ID.
- `medium` — Convert the nine bare `slog.Warn`/`slog.Error` request-path calls to `WarnContext(ctx, …)`. Where the context is a `*natsrouter.Context`, pass it directly — that also removes the hand-written `"request_id", natsutil.RequestIDFromContext(...)` pairs at `internal/service/threads.go:66`, `:121`.
- `low` — Rename the store interfaces in `internal/service/service.go` to `<Domain>Store` (`MessageStore`, `SubscriptionStore`, …), matching `UserStore`/`AppStore` already there, and regenerate `internal/service/mocks/` in the same change.
- `low` — Replace the concatenated message at `internal/service/attachments.go:28` with a constant message plus a `"kind"` attribute; normalize `messageID`→`message_id`, `gotRoom`→`got_room_id`.
- `low` — Move the nine `package service_test` files into `package service`, then unexport `UnavailableQuoteMsg`.
- `low` — Re-run `make sast-vuln` from an environment with egress to `vuln.go.dev` before shipping.
- `nitpick` — Move `getOrLoad`'s doc block back above it; either unexport `ErrEncryptedRowCipherDisabled` or add the cross-package `errors.Is` check its comment promises.

