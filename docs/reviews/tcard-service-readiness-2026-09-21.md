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


## 2. Code quality — score 4

### Evidence

- [medium] Bare `err` returned from a store method — `tcard-service/store_mongo.go:58` — `ListCards` returns `docToCard`'s error unwrapped (`return nil, err`), while every sibling path in the same function wraps (`find cards`, `decode card document`, `iterate cards`). CLAUDE.md §3 "Never return bare `err`"; the resulting chain loses the "was listing cards" frame.
- [medium] Request-scoped logs bypass the context-aware slog handler — `tcard-service/cache.go:105`, `cache.go:116`, `tcard-service/store_mongo.go:60` — `pkg/obs` installs the o11y SDK handler as the slog default specifically so contextual calls gain trace correlation (`pkg/obs/obs.go:175-176`), and `cache.Load` already receives the request-id-bearing ctx from `handler.go:56-58`. Using plain `slog.Info/Warn` drops `request_id`/`trace_id` from exactly the lines an operator needs after a failed `POST /api/v1/cards/refresh`; peer Gin services use `slog.InfoContext` (e.g. `botplatform-service/handler.go:159`).
- [medium] Load timeout is started before the write lock, contradicting its own contract — `tcard-service/cache.go:96-99` — `context.WithTimeout` is applied, then `writeMu.Lock()` blocks. The comment at `cache.go:15-16` claims `cacheLoadTimeout` "bounds a single full scan", but a second concurrent `Load` (the refresh endpoint is a plain unauthenticated POST, `routes.go:9`) spends its 60s budget queued behind the first and can enter `store.ListCards` with an already-expired ctx, turning a slow refresh into a cascade of `context deadline exceeded` 500s. Swap the two lines so the timeout starts after the lock is held.
- [low] Sentinel error compared with `!=` instead of `errors.Is` — `tcard-service/main.go:154` — `err != http.ErrServerClosed`. It happens to work because `ListenAndServe` returns the sentinel unwrapped, but §3 mandates `errors.Is`/`errors.As` for error identity, and it breaks silently if the server is ever wrapped in middleware that decorates the return.
- [low] Unactionable skip warning — `tcard-service/store_mongo.go:60` — `slog.Warn("card document missing a string path or _tcardVersion, skipping")` carries zero structured fields, so a silently dropped card cannot be located in Mongo. The doc is already decoded into `bson.D`; logging the `_id` (or whichever of `path`/`_tcardVersion` did parse) costs nothing and is what §3 "structured fields as key-value pairs" is for.
- [low] No domain `reason` codes despite a branch-heavy validation API — `tcard-service/handler.go:174-215` — `validateCard` returns eleven distinct `errcode.BadRequest`s that a client can only tell apart by prose message, plus the `409` at `handler.go:166`. There is no `pkg/errcode/codes_tcard.go`; the convention exists elsewhere (`codes_auth.go`, used at `auth-service/handler.go:141`). Any UI that must highlight the offending field is forced into string matching.
- [nitpick] Loop variable shadows a package type — `tcard-service/cache.go:113` — `for _, card := range cards` hides the `card` struct type declared in `store.go:12` for the body of `replace`.
- [nitpick] Pre-generics sorting on a Go 1.25 module — `tcard-service/cache.go:197`, `cache.go:218` — `sort.Slice`/`sort.Strings` where `slices.SortFunc`/`slices.Sort` are the current idiom (and `slices.SortFunc` is type-safe and faster here).
- [nitpick] Wrap text names the callee, not the caller — `tcard-service/cache.go:102` (`"list cards: %w"` around `store.ListCards`) and `store_mongo.go:45` (`"find cards: %w"` around `Find`) — §3 asks the frame to describe what the current function was doing; the chain reads "list cards: find cards: …".

### Recommendations

- [medium] Wrap the `docToCard` error — `tcard-service/store_mongo.go:58` → `fmt.Errorf("convert card document: %w", err)` — removes the only bare-`err` return in the service and restores the frame §3 requires.
- [medium] Convert the three request-reachable log sites to the `*Context` variants — `tcard-service/cache.go:105`, `cache.go:116`, `store_mongo.go:60` — `Load`/`replace`/`ListCards` all have a ctx in scope (thread ctx into `replace`), which restores trace and `request_id` correlation on the refresh path for free.
- [medium] Move `context.WithTimeout` below `writeMu.Lock()` — `tcard-service/cache.go:96-99` — makes the 60s budget bound the scan as documented, so queued refreshes each get a full budget instead of failing on arrival.
- [low] Use `errors.Is(err, http.ErrServerClosed)` — `tcard-service/main.go:154` — §3 compliance and robustness against future wrapping of the listener error.
- [low] Add `_id` (and the field that did parse) as structured fields to the skip warning — `tcard-service/store_mongo.go:60` — makes a dropped card findable in Mongo without a full re-scan.
- [low] Introduce `pkg/errcode/codes_tcard.go` with reasons for the validate failures and attach them via `errcode.WithReason` — `tcard-service/handler.go:174-215`, `handler.go:166` — lets the card-authoring UI branch on a stable code rather than on error prose.
- [nitpick] Rename the shadowing loop variable and switch to `slices.SortFunc`/`slices.Sort` — `tcard-service/cache.go:113`, `cache.go:197`, `cache.go:218` — small readability/idiom cleanups on an otherwise well-written file.
