# teams-user-sync — Production Readiness Review

**Service:** `teams-user-sync` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

A small, well-built CronJob — textbook consumer-defined store, constructor DI, typed config with `required,notEmpty` on both credentials and both URIs, correctly mounted shared knobs, and business logic at **100% coverage** with every error path covered as a distinct subtest. The headline number (53.4%) understates it badly: the entire gap is `main()` wiring plus a store layer that *is* fully tested — behind `//go:build integration`, which the service's own pipeline never builds.

Three defects matter. **TLS verification of the Microsoft Graph and Azure token endpoints is disabled by default** — `GRAPH_TLS_INSECURE_SKIP_VERIFY` ships `envDefault:"true"`, the credential-bearing `client_credentials` POST rides that transport, and neither the compose file nor the pipeline sets the var, so every environment inherits the insecure default. **A user inserted before their HR row exists is permanently orphaned**: existing ids are skipped before any HR lookup, so the join is never re-attempted and `siteId`/`engName`/`mail` stay empty forever — five downstream consumers read those fields, and `message-worker` errors outright on an HR-less row. And **the directory walk has no 429 handling**, so one throttled response discards the whole run; the chats surface on the same client has a full retry loop and a tenant-wide throttle gate for exactly this.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 5 | 5 | 9 | 1 | **21** |

---

## 2. Go code quality — 4 / 5

Idiomatic, correctly-wrapped errors and disciplined secret-free structured logging throughout; the one real defect is a security-critical config knob that defaults to the insecure value.

### Findings
- `high` — `GraphTLSInsecureSkipVerify` defaults to **true**, so TLS verification of the Microsoft Graph / Azure token endpoint is disabled unless an operator explicitly opts back in — `teams-user-sync/config.go:20`
  The credential-bearing `client_credentials` POST (`pkg/msgraph/msgraph.go:379`) rides that transport (`pkg/msgraph/msgraph.go:252-259`). `deploy/docker-compose.yml` never sets the var, and neither does the pipeline, so every environment inherits the insecure default. `pkg/msgraph/msgraph.go:124-127` itself says "Never enable in production". Fail-safe requires `envDefault:"false"`.
- `medium` — one `Info` log line per HR-unmatched user, unbounded by directory size — `teams-user-sync/handler.go:106`
  The aggregate counter two lines later (`handler.go:117-118`) already carries the same information. A tenant with a large guest population emits one line per guest per run.
- `low` — a failed run logs twice: `run` emits the finished line with `succeeded:false` (`teams-user-sync/main.go:80-89`), then `main` logs `fatal error` (`teams-user-sync/main.go:24`). Both are needed for different reasons, but a reader sees two records per failure.
- `low` — audit-coverage gap, not a service defect: gosec and the 18 repo-owned semgrep rules are clean repo-wide; `govulncheck` and the semgrep registry packs could not run (blocked egress, per `GLOBAL_PREP.md`), so third-party CVE exposure of this service's dependency set is unverified.

**Verified clean:** no secret ever reaches a log — the client secret is only form-encoded (`pkg/msgraph/msgraph.go:379`), token errors surface code-only (`pkg/msgraph/msgraph.go:404`), Graph non-200s surface status-only (`pkg/msgraph/msgraph.go:717`), and Mongo URIs are sanitized before logging (`pkg/mongoutil/mongo.go:66`). Every error is wrapped with what *this* function was doing (`handler.go:46,64,96,120`; `store_mongo.go:60,77,89`); no bare `err`, no string comparison, no `fmt.Println`, no interpolated log fields.

### Recommendations
- `high` — flip `config.go:20` to `envDefault:"false"` and set `GRAPH_TLS_INSECURE_SKIP_VERIFY=true` explicitly in the on-prem deployment manifest that actually needs it.
- `medium` — drop `handler.go:106` to `Debug`, or cap it (log the first N ids per run) and rely on the `hrUnmatched` counter.
- `low` — have `run` log the finished line only on success and let `main` own the failure record, or add the error to the finished line and drop `main.go:24`.
- `low` — re-run `make sast-vuln` from an egress-permitted environment before release.

---
