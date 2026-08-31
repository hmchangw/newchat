# user-service — Production Readiness Review

**Service:** `user-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

The best-behaved service in the fleet on code discipline: **zero Section-3 violations found** across ~18.8k lines, no token/password/body ever logged, `errors.Is` throughout, every `fmt.Errorf` wrapping with context. The sub-package decomposition is coherent and the `subscription.list` hot path is genuinely well engineered (TTL'd sort-key LRU, page-sized batch refetches, bounded per-site fan-out, precise projections). Three things hold it back. The **cross-site fan-out is synchronous, sequential and best-effort inside the request handler** — a down peer permanently loses that site's replica of settings that a remote `notification-worker` reads to decide whether to push. The **per-site fan-out pattern is hand-copied five times and has already drifted** — one copy lost its semaphore. And **coverage is 53.2%**, with all six client-facing chatlist RPCs handler-untested.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 8 | 18 | 12 | 5 | **44** |

> **Audit-coverage caveat.** `gosec` and the 18 repo-owned `semgrep` rules are clean repo-wide. `govulncheck` and the semgrep registry packs could not run (egress blocks `vuln.go.dev` / `semgrep.dev`); dependency-CVE coverage is unverified.

---

## 2. Go code quality — 4 / 5

Exemplary error-wrapping, logging and `errcode` discipline across ~18.8k lines with **zero Section-3 violations found**; only minor idiom and startup-robustness nits keep it off 5.

### Verified clean — and this is the finding that matters

Zero `fmt.Println`/`log.Println`. All 58 `slog` sites use scalar structured k/v with `request_id` and `WarnContext`. **No token, refresh token, password or request body is ever logged** — the two SSO failure paths deliberately route the verification error through `WithCause` (server-log only) and never the token bytes (`service/sso.go:41-47`, `:82`; `middleware.go:93-98`). No `err.Error()` string comparison — `errors.Is` throughout, including the `pkgoidc.ErrTokenExpired` and `ErrShuttingDown`/`context.DeadlineExceeded` branches. Every `fmt.Errorf` wraps with a this-function description and `%w`. No silently discarded errors, no `time.Sleep`, no `os.Getenv`, no `map[string]interface{}` payloads, no `panic`. `errcode` tiering respected: Tier-1 constructors with `WithReason` only on branchable cases, `errcode.Parse` confined to the four remote-envelope RPC clients, no `WithCause` wrapping another `*errcode.Error`, no log-and-return double-logging. The `#nosec G101` on `ssoTokenHeader` is a correctly-placed, justified false positive.

| Sev | Finding | Evidence |
|-----|---------|----------|
| low | All five `EnsureIndexes` failures are `slog.Warn` + continue, so a pod can serve the whole aggregation surface against an unindexed collection with no fail-fast and no readiness signal — index absence degrades silently to collscans exactly under the load that caused the failure | `main.go:177-190` |
| low | `cappedUnion(ids []string, trigger string, cap int)` shadows the predeclared `cap` builtin | `service/badge.go:67` |
| low | DTOs in `models/` carry only `json` tags where §3 requires both. Materially harmless today — none is bson-decoded, and `models.ActiveSubscription` does carry both, so the rule is honoured exactly where decoding happens — but a future direct `Decode` into one of these would silently zero the fields | `models/prioritycontacts.go:19-22`, `models/app.go:39-41`, `models/status.go:17-21` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Thread-unread fan-out spawns one unbounded goroutine per site, bypassing the `MAX_SITE_FANOUT` semaphore that `fanOutChunks` enforces for the same class of RPC | `service/threadunread.go:58-61`, `:150-160` |
| nitpick | `main.go` hand-copies `service`'s unexported `badgeCache` interface because it cannot name it, with a 4-line comment explaining the workaround | `main.go:62-71` |
| nitpick | `fmt.Errorf` with no format verbs where `errors.New` is the idiom | `config/config.go:262`, `:265` |

### Recommendations
- `low` — Make `EnsureIndexes` failures fatal, or gate readiness on them so an unindexed pod never takes traffic.
- `low` — Rename `cappedUnion`'s `cap` parameter and enable the `predeclared` linter so this class cannot recur.
- `low` — Add `bson` tags to the remaining `models/` DTOs, or mark the package wire-only in a comment.
- `low` — Re-run `make sast-vuln` and the registry packs from a network-permitted runner before shipping.
- `nitpick` — Route the two `threadunread.go` fan-outs through `fanOutChunks` so all per-site RPC concurrency obeys one knob; export `service.BadgeCache` and delete `main.go`'s structural copy.

---

## 3. Architecture — 4 / 5

Boundaries are unusually disciplined — every interface is consumer-defined, compile-time assertions guard drift, subjects come from `pkg/subject`, shutdown order is correct — but a re-declared shared Mongo pool knob, a duplicated unexported interface, and a synchronous non-durable cross-site fan-out keep it off 5.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | The HTTP Mongo client **re-declares `mongoutil.PoolConfig`'s knobs as three hand-written fields** instead of mounting the shared type with `envPrefix`. The justifying comment is *factually wrong*: it claims the tags "are fixed to the `MONGO_` prefix and cannot be reused", but `pkg/mongoutil/poolconfig.go:11-13` states the tags carry the full prefix precisely so a prefixed field works, and `CLAUDE.md` gives `Breaker mongoutil.BreakerConfig` + `envPrefix:"HISTORY_"` as the sanctioned form. **The concrete cost is a dropped knob**: `ServerSelectionTimeout` is only applied via `WithPool`, so the HTTP pool silently keeps the driver's 30 s default — the exact hang `poolconfig.go:33-40` exists to prevent, on the transport that serves the client-facing sidebar | `config/config.go:47-55`, `:196-198`; `main.go:137-144` |
| medium | Cross-site fan-out is a **synchronous, sequential, best-effort** JetStream publish inside the request handler: one blocking `PublishMsg` PubAck per destination, errors logged and swallowed. N remote sites add N × cross-gateway RTT to every mutation under a 15 s `HANDLER_TIMEOUT`, and a down peer permanently loses that site's replica. `CLAUDE.md` sanctions user-service as a direct publisher, so this is a documented gap rather than a rule break — but **the settings replica drives remote `notification-worker` push decisions**, so a lost event means a remote site keeps pushing muted notifications until the user next touches settings. This is the case OUTBOX was introduced for | `service/status.go:106-123`; `service/settings.go:143-160`; `service/chatlist.go:202-220`; `publisher/publisher.go:26-31` |
| medium | `badgeCache` is declared unexported in `service` yet is a constructor parameter of the exported `service.New`, forcing `main` to keep a hand-maintained structural copy whose own comment admits the workaround. Nothing fails the build if the two drift | `service/service.go:91-104`, `:159`; `main.go:63-70` |
| medium | `service.New` takes 14 positional dependencies plus variadic options, and `main` calls it **twice** with the argument list duplicated — the second call silently omits `WithPageBudget`. No compiler help on ordering between same-typed params (`pub, clientPub EventPublisher`) | `main.go:210-211`, `:217-222` |
| medium | `ALL_SITE_IDS` defaults to empty with **no validation and no startup log**, and `Load` never checks it or that `SiteID` is a member. An empty value makes every federation loop a silent no-op: federation is off with zero signal, while `SITE_ID` is correctly `notEmpty` | `config/config.go:70`, `:210-280` |
| low | No `bootstrap.go` / `Bootstrap` field despite publishing to JetStream. Behaviourally **correct** — it only ever publishes to remote INBOX lanes, which `inbox-worker` owns and this service must never create — but the gap deserves an explicit comment | `main.go:126` |
| low | Cohesion drift: `service/enrich_test.go` (846 lines) has no `enrich.go`; the fan-out/enrichment engine it tests lives inside `service/subscriptions.go:225-544` | — |
| nitpick | `startHTTPServer` (~60 lines of listener wiring) sits in `main.go` while `httpserver.go` holds only the 12-line `newHTTPServer` | `main.go:328-404` |

### Recommendations
- `high` — Replace `HTTPConfig`'s three pool fields and `validateMongoPool` with `Pool mongoutil.PoolConfig \`envPrefix:"HTTP_"\``; env names are unchanged, `Validate()` replaces the local copy, and the HTTP client picks up the 2 s `ServerSelectionTimeout` it currently lacks.
- `medium` — Route the settings and chatlist inbox events through the local OUTBOX (`outbox.Publish`), adding each type to exactly one `pkg/outbox` partition set; keep status direct if the LWW argument is judged sufficient. At minimum, fan out concurrently under the existing `MaxSiteFanout` semaphore instead of serially.
- `medium` — Export `service.BadgeCache` and delete the structural copy; collapse `service.New`'s dependency list into a named `service.Deps` struct and build the two instances by overriding one `Deps`.
- `medium` — Validate in `config.Load` that `SiteID` appears in `AllSiteIDs` when non-empty, and log the resolved peer list at startup so a federation-disabled deploy is visible.
- `low` — Extract `service/subscriptions.go`'s enrichment engine into `service/enrich.go`; add a comment at `main.go:126` recording why no `bootstrapStreams` exists.

