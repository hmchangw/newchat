# upload-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `4c07ee6` (base `main`)  
**Overall score:** 3.2 / 5 (baseline 2026-09-01: 3.0, Δ +0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

upload-service handles untrusted user files and its input-validation boundary does not hold. The media-type allow/deny filter trusts the client's declared `Content-Type` whenever it is set and not the generic fallback, so a client posting SVG bytes labelled as a PNG defeats the default SVG blacklist entirely — and the code comment two functions away asserts the opposite invariant. The images endpoint is worse: it validates by filename extension only, with no sniffing and no filter, so the same file renamed passes. Neither endpoint caps the request body, so the whole upload is spooled to the container's temp directory before any size check runs, and the images path can spool a quarter of a gigabyte per request; nothing bounds concurrent uploads either, although the service's own comment says ephemeral storage must fit the concurrent-upload total. Exploitability is capped, not closed, by the download path always sending an attachment disposition and a locked-down content policy. On the integration side two contract breaks would make real data unreachable: the collection the download path reads has no producer anywhere in the repository (the migration pipeline writes a differently-named one), and legacy attachment URLs of one shape are rewritten onto a route the service does not serve, contradicting both the code's own comment and the client API doc. The Drive client carries no request context, so that leg loses request-ID logging, trace parentage and cancellation, and it runs on a bare transport with two idle connections per host. Coverage is 76.7%, three points under the floor, and the gap is concentrated: `run` is untested, every error branch of the file-upload handler is uncovered while the equivalents on the images handler are tested, and the config test covers one field of thirty-five.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 0 critical, 10 high, 17 medium, 18 low, 4 nitpick (49 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

