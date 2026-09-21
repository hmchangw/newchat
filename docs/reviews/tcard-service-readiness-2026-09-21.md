# tcard-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `5a5dab1` (base `main`)  
**Overall score:** 3.2 / 5 (baseline 2026-09-01: 3.5, Δ -0.3)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`tcard-service` is a small, clean, read-mostly HTTP service: it loads AdaptiveCard templates from a MongoDB `cards` collection into an in-process snapshot and serves them by `{path}@{version}`. Code quality and maintainability are the best of any service audited in this cycle (4 and 4) — tight files, honest WHY-comments, shared knobs mounted as named `mongoutil`/`ginutil` fields with nothing re-declared, and a semver gate with real tests. What holds it at 3.2 is that everything around the edges of that core is unfinished.

The sharpest problem is the trust boundary. `POST /api/v1/cards/validate` and `POST /api/v1/cards/refresh` carry no authentication (`routes.go:8-9`), while `docs/client-api.md:8827` documents `/validate` as "Admin only … Not for end-user browsers" and the accepted design defers enforcement to a network policy that does not exist in this repo. `ginutil.CORS()` sets `Access-Control-Allow-Origin: *` and allows `POST` preflight, so `/refresh` — an unbounded full-collection `Find(ctx, bson.D{})` — is a cross-origin scan amplifier reachable from any browser that can reach the read route. The repo already has the pattern to fix it (`client-update-service/routes.go:16` gates its privileged route with `requireServiceAccount`).

Second, the cache has no multi-replica story. Each pod holds a private snapshot refreshed at startup, once daily, or by a POST a load balancer delivers to exactly one replica — so a newly published card is servable on one pod and `404`s on the others for up to 24 hours, and `/validate`'s `409` ordering verdict depends on which pod answers. Compounding this, nothing in the repo writes `cards`: the publisher is out-of-repo, yet the service runs `EnsureIndexWithRepair`, which will silently drop and recreate the external owner's index if its options differ, and then warns-and-continues when the ensure fails — the exact silent behaviour the accepted design's Decision 11 rejected.

Third, two contract gaps that only show up in production. The read path emits relaxed extended JSON (`store_mongo.go:92`), so any BSON date/ObjectID/Decimal128 inside a card reaches the client as `{"$date":…}`, which no AdaptiveCard renderer accepts — while the docs promise the document verbatim. And immutable, content-addressed templates are served with no `ETag`, `Cache-Control` or `If-None-Match` handling (`handler.go:100`), so every client launch re-downloads every card in full; `media-service/handler.go:42-76` already implements exactly the pattern that would remove essentially all steady-state traffic. Twelve client-facing 400/409 cases also carry no machine-readable `reason` — there is no `codes_tcard.go` — so card authors must branch on prose.

Coverage is 69.3%, and the deficit is almost entirely one monolithic `run()` (68 statements, 0%) plus the Mongo layer; the tested logic sits at 96%+. Extracting `buildRouter`/`validateConfig` the way `admin-service` does would close most of it.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 3 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 0 critical, 6 high, 21 medium, 14 low, 6 nitpick (47 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

