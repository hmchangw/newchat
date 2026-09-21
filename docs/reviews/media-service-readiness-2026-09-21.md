# media-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `032ec71` (base `main`)  
**Overall score:** 3.5 / 5 (baseline 2026-09-01: 3.3, Δ +0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

media-service is well built for what it does — errcode discipline is textbook in every handler bar one, eight of its nine Mongo reads project precisely, blobs stream rather than buffer, all three image paths honour conditional requests, and its boundary with upload-service is genuinely clean (separate buckets, separate collections, no shared key convention). Two defects stand out. The bot-avatar upload decodes an image with no header or dimension pre-check, so a one-megabyte file declaring huge dimensions allocates gigabytes and kills the pod — the emoji path guards exactly this case a few files away, and the comment there calls it decompression-bomb hardening. And error responses inherit a six-hour public cache lifetime, because the cache headers are set before the blob fetch: a transient storage blip gets pinned in browsers and any shared CDN for hours. Around those, the hot read path is under-protected: the avatar lookup is the one unprojected find in the service, the two highest-volume reads are uncached and even a conditional-request hit pays the database round trip, redirects carry no cache directive so every render of a normal user's avatar re-hits the service, and nothing bounds in-flight HTTP requests or puts a deadline on the metadata hops. The `drive.members` route is a third sub-domain with its own hand-rolled error envelope, no inline justification and no entry in the client API doc; a `wrong_cluster` reason reaches the wire without being in the reason table, and can ship a dangling "upload to " when the owning site is missing from the domain map. Coverage reads 70% but that understates it — the store and blob tiers are container-tested and simply invisible to the unit profile; the real gaps are `run`'s validation gates and two store methods untested at any tier.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 4 |
| Performance | 3 |

**Findings by severity:** 0 critical, 2 high, 26 medium, 16 low, 5 nitpick (49 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

