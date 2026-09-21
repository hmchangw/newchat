# hr-sync-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `60ba808` (base `main`)  
**Overall score:** 2.5 / 5 (baseline 2026-09-01: 2.8, Δ -0.3)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`hr-sync-worker` is a small, tidy daily consumer — four files, clean WHY-comments, correct `jsretry`/`jobguard` settle path, opt-in stream bootstrap, no hardcoded `cc.BackOff`, shutdown in the documented order. Every reviewer said the Go craftsmanship is fine. It scores 2.5 because the contract around it is wrong in ways the code cannot detect, and because almost none of the risky code is tested.

The critical finding is a head-of-line stall that one ordinary HR event can trigger. The upsert keys `hr_employee` by `_id = employeeId` (`store.go:58`), but the only index on that collection is a **unique** index on `account`, created by the read-only `portal-service` (`portal-service/store_mongo.go:30`). The producer, `teams-hr-sync`, diffs by account, so a person whose Graph id changes is emitted as an upsert, never a quit — the insert collides on `account` and returns a `BulkWriteException`. Nothing in the service calls `mongo.IsDuplicateKeyError`, so `handler.go:32` wraps it raw, `jsretry.Settle` classifies it transient and NAKs, and with `MaxDeliver=-1` plus `MaxAckPending=1` that one batch redelivers forever at the 10-minute backoff tail while every later HR message for the site queues behind it. There is no DLQ. Index ownership is inverted the same way: the writer declares no `EnsureIndexes` at all, so the constraint it must satisfy is owned — and, via `EnsureIndexWithRepair`'s drop path, can be silently rewritten — by a service that never writes the collection.

Second, this service and `teams-hr-sync` are two implementations of one documented `Store` contract, copy-pasted and already diverged. `UpsertUserIdentities` here filters on `account` and mints `idgen.GenerateUUIDv7()`; the twin filters on `employeeId` and sets `_id = employeeId` explicitly, with a comment explaining that search-sync depends on the id being derivable from the Teams id. Both write the same `users` collection, whose `account` is unique — so which `_id` a user ends up with depends on which path inserted first, and the derivability the twin documents is lost for every user arriving over the feed. `teams-hr-sync/README.md:48` points at a shared `pkg/hrstore` that does not exist; the de-duplication was written up and never done, and the doc now conceals the duplication.

Third, the README is not a description of the code. It claims employees are replaced by `{account, source}` and that quits are scoped `source: "teams"` so "legacy-source rows survive" — but `model.IEmployee` has no `source` field, the upsert keys on `employeeId`, and `DeleteMany` filters on `account` alone (and discards the batch's decoded `SiteID`). This file calls itself the reference contract for anyone replacing the persister. Subject routing is the same class of hazard: `strings.HasSuffix` against literals that duplicate `pkg/subject`, with a `default` branch that Ack-drops permanently — so a rename in the builders still compiles, and the entire HR feed is discarded with a WARN.

Coverage is 21.1%, the lowest audited this cycle. `store.go` — the dup-key, `omitempty`-merge and blank-`_id` code above — is at 0% in the unit profile, `bootstrap.go` is 0% despite a `streamManager` seam whose comment says it exists for tests, and the integration test re-implements the consume path by hand instead of calling `startSiteConsumer`, so `jobguard`, `logctx` and the decode-poison branch are executed by no test at all. `main()` is 38% of the package's statements, so the 80% floor is arithmetically unreachable from `make test` as the package is shaped today.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 2 |
| Performance | 3 |

**Findings by severity:** 2 critical, 14 high, 21 medium, 9 low, 4 nitpick (50 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

