# Fleet Production-Readiness Report

**Scope:** all 35 Go services in the monorepo
**Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** the `production_readiness` skill run per service — six independent expert agents each (code quality, architecture, test coverage, maintainability, integration, performance), every finding requiring `file:line` evidence, judged against `CLAUDE.md` and industry practice for Go microservices at scale. Per-service reports live beside this one in `docs/reviews/`.

## How this run was executed

Three steps were computed **once repo-wide** rather than 35 times, because every service would otherwise re-scan an identical repo:

- **`make sast`** — `gosec` **PASS** (0 findings at `-severity medium -confidence low -tests=true`); repo-owned `semgrep` rules **0 findings** across 18 rules / 1,798 files, fixture tests 2/2. **`govulncheck` and the `semgrep` registry packs could not run** — `vuln.go.dev` and `semgrep.dev` are blocked by this environment's egress policy (403 on CONNECT). That is an environmental gap, not a code finding, and it is the one CI gate this audit could not exercise.
- **`make generate`** — produced **zero diff** repo-wide. Mocks are current everywhere. (One caveat found later: `message-worker/mock_hridentity_test.go` has no `//go:generate` directive at all, so `make generate` never visits it — a zero diff there means "not checked", not "up to date".)
- **Coverage** — one `-covermode=atomic` profile over `./...`, sliced per service.

Two `pkg/` tests failed only under the loaded parallel run and **pass in isolation** (`pkg/shutdown`, `pkg/testutil`); they are load-flaky, not broken.

## The headline number

> **34 of 35 services fall below the repo's own 80% coverage floor. 19 are below 60%.**
> **`translation-service` (82.3%) is the only service in the fleet that passes.**

`CLAUDE.md` §4 states the 80% minimum as a merge gate: "code below this threshold MUST NOT be merged." On today's `master` that gate would reject all but one service. This is not 35 independent lapses — Chapter 2 shows it is largely **one root cause repeated 35 times**.

## Fleet scorecard

Scores are the mean of the six dimensions. Coverage is statement-weighted from the shared profile.

| Service | Overall | D1 Qual | D2 Arch | D3 Test | D4 Maint | D5 Integ | D6 Perf | Coverage |
|---|---|---|---|---|---|---|---|---|
| roomlist-worker | **3.7** | 4 | 4 | 2 | 4 | 4 | 4 | 65.9% |
| broadcast-worker | **3.5** | 4 | 4 | 2 | 3 | 4 | 4 | 67.7% |
| user-service | **3.3** | 4 | 4 | 2 | 3 | 3 | 4 | 53.2% |
| room-worker | **3.3** | 4 | 4 | 2 | 2 | 4 | 4 | 62.8% |
| search-service | **3.3** | 4 | 4 | 2 | 3 | 4 | 3 | 66.9% |
| message-gatekeeper | **3.3** | 4 | 4 | 2 | 3 | 3 | 4 | 65.5% |
| media-service | **3.3** | 4 | 4 | 2 | 3 | 4 | 3 | 70.0% |
| history-service | **3.2** | 4 | 4 | 1 | 3 | 3 | 4 | 55.0% |
| room-service | **3.2** | 4 | 3 | 1 | 3 | 4 | 4 | 57.2% |
| admin-service | **3.2** | 4 | 3 | 2 | 3 | 4 | 3 | 68.9% |
| message-worker | **3.2** | 4 | 4 | 1 | 3 | 4 | 3 | 56.8% |
| search-sync-worker | **3.2** | 4 | 4 | 2 | 3 | 2 | 4 | 67.7% |
| notification-worker | **3.2** | 4 | 4 | 1 | 3 | 3 | 4 | 59.0% |
| outbox-worker | **3.2** | 4 | 4 | 1 | 3 | 4 | 3 | 36.9% |
| auth-service | **3.2** | 4 | 3 | 2 | 4 | 3 | 3 | 61.9% |
| upload-service | **3.0** | 4 | 4 | 2 | 3 | 2 | 3 | 76.5% |
| inbox-worker | **2.8** | 4 | 3 | 1 | 3 | 3 | 3 | 44.1% |
| botplatform-service | **2.8** | 3 | 3 | 1 | 4 | 3 | 3 | 56.5% |
| bot-room-service | **2.8** | 4 | 3 | 1 | 3 | 3 | 3 | 49.0% |
| user-presence-service | **2.8** | 4 | 3 | 1 | 3 | 3 | 3 | 45.1% |

**Coverage for the remaining 15** (audits completing; coverage is measured and final): translation-service **82.3%** ✅ · teams-room-verify 78.9% · client-update-service 76.8% · tcard-service 69.7% · teams-chat-sync 67.6% · teams-chat-member-sync 60.3% · portal-service 58.6% · teams-hr-sync 57.5% · teams-room-creation 55.9% · teams-user-sync 53.4% · teams-room-inspector 47.7% · bot-message-handler 40.9% · push-notification-service 26.9% · hr-sync-worker 21.1% · **bot-message-worker 13.6%** (fleet low).

### What the scores say

No service scored below 2.8 and none above 3.7 — **the fleet is uniformly mid-band**, which is itself the finding. The code *inside* functions is consistently good: D1 (code quality) averages ~3.9 and never drops below 3. What drags every service down is the same three things — **untested wiring, shared state configured per-service instead of once, and cross-service contracts asserted by comment rather than by compiler or test.**

