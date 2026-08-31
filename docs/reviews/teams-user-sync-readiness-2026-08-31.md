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
