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


## 2. Code quality — score 3

### Evidence

- [high] The media-type allow/deny gate is bypassable with a client-supplied `Content-Type` — `upload-service/mediatype.go:217` — `resolveMediaType` returns the declared type verbatim whenever it is non-empty and not `application/octet-stream`, without reading a byte (asserted by `mediatype_test.go:163` and `:282`). A client posting SVG bytes as `Content-Type: image/png` gets `mime = "image/png"`, so `h.mimeFilter.allowed` (`handler.go:277`) never sees the real type and the default `FILE_UPLOAD_MEDIA_TYPE_BLACKLIST=image/svg+xml` (`main.go:75`) is defeated; a whitelist is defeated the same way. The comment at `handler.go:268-272` asserts the opposite invariant ("the real type comes from the bytes and the name … Filtering THAT … is what stops a blacklisted upload arriving under a generic label"), so code and stated contract disagree. Exposure is capped, not closed, by the download path always sending `Content-Disposition: attachment` + `Content-Security-Policy: default-src 'none'` (`handler.go:377-381`, `:430-436`).
- [high] The images endpoint validates untrusted files by filename extension only — `upload-service/handler.go:469` — `preprocessFiles` gates on `drive.AllowedImageFileTypes[filepath.Ext(...)]` (png/jpeg/jpg/heic, `pkg/drive/images_file.go:9`) with no sniff and no `mimeFilter`. `evil.svg` renamed `evil.png` is accepted and stored as an image. Same class as the finding above; both should route through `resolveMediaType` + `mimeFilter`.
- [high] No cap on the request body, so the size check runs after the upload is already on disk — `upload-service/handler.go:237` / `:254` — `c.MultipartForm()` parses and spools the *entire* body (anything over `MaxMultipartMemory = 1 MiB`, `main.go:32/181`) to the OS temp dir before `fh.Size > h.maxFileSize` is evaluated; with `FILE_UPLOAD_MAX_FILE_SIZE=-1` the check is skipped entirely. Nothing calls `http.MaxBytesReader` anywhere in the service. Sibling `media-service/upload.go:58` does exactly this ("Size cap before reading the body"), so the pattern exists in-repo. An authenticated client can exhaust the pod's ephemeral storage inside one 15-minute `ReadTimeout` window.
- [high] A knob shared by two services is re-declared with its own env tag and default — `upload-service/main.go:101` — `MinioDownloadTimeout time.Duration \`env:"MINIO_DOWNLOAD_TIMEOUT" envDefault:"5m"\`` is declared identically and independently in `client-update-service/config.go:24`. CLAUDE.md §6 Configuration: a knob used by more than one service is declared once in the owning package (`pkg/minioutil`) and mounted as a named field, never re-declared per service. Values agree today; the drift the rule exists to prevent is one edit away.
- [low] Sentinel error compared with `!=` instead of `errors.Is` — `upload-service/main.go:228` — `if err != nil && err != http.ErrServerClosed`. Works today only because `srv.Shutdown` returns the unwrapped sentinel. The repo majority (`media-service/main.go:143`, `admin-service/main.go:174`, `botplatform-service/main.go:160`) uses `errors.Is`.
- [low] A missing MinIO object is reported as 503, not 404 — `upload-service/handler.go:419-422` — every `s3.Open` failure, including `NoSuchKey` from the `Stat` probe (`store_minio.go:136`), becomes `errcode.Unavailable`. The Mongo-metadata-present / object-absent case is a genuine 404; as written it burns the error budget and invites client retries that can never succeed.
- [nitpick] Request-ID log plumbing duplicated — `upload-service/middleware.go:140` re-inlines the exact body of `logCtx` (`handler.go:91`) instead of calling it.
- [nitpick] Bare `return err` — `upload-service/store_minio.go:55` — `cancelReadCloser.Close` passes the inner Close error through unwrapped. Defensible for an `io.Closer`, but it is the one un-annotated error return in the service.

### Recommendations

- [high] Make `resolveMediaType` sniff-authoritative for the *filter* decision — `mediatype.go:216` — either drop the declared-type shortcut, or keep the declared value as the recorded type while filtering on the sniffed/extension-derived type. Add a table case for "declared image/png + SVG bytes → rejected".
- [high] Run `preprocessFiles` through the same resolver + `mimeFilter` — `handler.go:463` — so both endpoints share one validation path for untrusted bytes.
- [high] Wrap the body before parsing: `c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)` at the top of `HandleUploadFile`/`HandleUploadImages` (`handler.go:212`, `:135`), mirroring `media-service/upload.go:58`; give `FILE_UPLOAD_MAX_FILE_SIZE=-1` a hard ceiling rather than "unbounded".
- [medium] Move `MINIO_DOWNLOAD_TIMEOUT` into a `minioutil.Config` mounted as a named field in both services — `main.go:101`, `client-update-service/config.go:24`.
- [low] `errors.Is(err, http.ErrServerClosed)` at `main.go:228`; map `minio.ToErrorResponse(err).Code == "NoSuchKey"` to `errcode.NotFound` at `handler.go:421`.
