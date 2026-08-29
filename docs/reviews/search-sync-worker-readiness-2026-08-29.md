# Production-Readiness Review — `search-sync-worker`

| | |
|---|---|
| **Service** | `search-sync-worker` |
| **Date** | 2026-08-29 |
| **Branch** | `claude/search-sync-service-prod-ready-cj09nf` |
| **Overall score** | **3.2 / 5** |
| **Method** | Six parallel expert audits (code quality, architecture, test coverage, maintainability, integration, performance) against `CLAUDE.md` and industry practice for Go microservices at scale |

> Requested as `search-sync-service`; no such directory exists. Audited `search-sync-worker`, the closest match and the service the branch name refers to, as confirmed by the requester.

## Executive summary

`search-sync-worker` is a competently built ingestion worker with real engineering behind its hot path — a genuine ES `_bulk` pipeline with slot-bounded concurrency, per-item 429 classification, precomputed OTel attribute sets, disciplined `jsretry`/`errcode` poison-vs-retry tiering, zero raw `fmt.Sprintf` subject construction, and correct federation hygiene (it never bootstraps INBOX, leaving `inbox-worker` as sole owner). Unit-test craft is above the repo average: ~130 descriptively named subtests, error paths covered deliberately, metrics asserted against a real `ManualReader`. It is not, however, production-ready as it stands. One `critical` defect will crashloop the pod: the bot-message collection binds `BOT-MESSAGES-CANONICAL-{siteID}` but filters on `chat.msg.canonical.{siteID}.*` (`messages.go:129`), a subject that stream does not carry — consumer creation fails and `main.go:348` exits. A second contract gap silently under-restricts search results: `room_restricted` never reaches the user-room ACL index, so a room restricted after members joined leaves their docs unrestricted and full-history message search is granted. Structurally, the service is the only JetStream worker in the repo without a `bootstrap.go`, inlining stream creation at `main.go:310` with no production existence check; `main()` has grown to 319 lines and absorbed the entire consumer runtime; dependencies are injected by post-construction field mutation; `Collection.BuildAction` takes no `ctx`, forcing two `context.Background()` network calls (one an unbounded Mongo query) that sever tracing and outlive shutdown. Package coverage is **66.8%**, below the repo's 80% floor, concentrated almost entirely in an untestable `main.go` — excluding it the package sits at ~84.8%. `gosec` passes clean repo-wide; `govulncheck` and `semgrep` could not run in this sandbox (proxy 403 / tool absent) and remain unverified here.

## Dimension scores

| # | Dimension | Score | One-line verdict |
|---|---|---|---|
| 1 | Go code quality | **4 / 5** | Disciplined idioms and worker tiering; context propagation and one swallowed `Fetch` error are the gaps |
| 2 | Architecture | **3 / 5** | Clean federation and subject hygiene, undermined by the missing `bootstrap.go` and a `main.go` that became the runtime |
| 3 | Test coverage | **2 / 5** | Floored by the 80% rule at 66.8%; craft would otherwise merit 4 |
| 4 | Maintainability | **3 / 5** | Good `Collection` abstraction; wiring-by-mutation and a 319-line `main()` fight it |
| 5 | Integration | **3 / 5** | Excellent subject/stream discipline with one crashlooping filter mismatch and one ACL gap |
| 6 | Performance | **4 / 5** | Strong bulk design; missing deadlines and an uncached N+1 are the real risks |

**Average: 3.2 / 5**

## Findings by severity

| Severity | Count |
|---|---|
| `critical` | 1 |
| `high` | 15 |
| `medium` | 24 |
| `low` | 6 |
| `nitpick` | 6 |
| **Total** | **52** |

Counts are per-dimension and overlap by design: four experts independently flagged the missing `ctx` on `Collection.BuildAction`, three flagged the un-backed-off `Fetch` error loop, and two each flagged the absent `bootstrap.go` and the size of `main.go`. Convergence from independent lenses is the strongest signal in this report — those four items are the structural core of the remediation list in Chapter 8.
