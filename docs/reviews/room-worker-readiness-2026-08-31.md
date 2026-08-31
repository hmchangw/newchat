# room-worker — Production Readiness Review

**Service:** `room-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

The federation plumbing is correct and unusually well-reasoned — every OUTBOX type is correctly partitioned onto the FIFO lane, subjects all come from `pkg/subject`, `bootstrapStreams` is *stricter* than the spec, and the high-throughput consumer pattern is textbook. The problems are elsewhere. **A rename can permanently diverge `rooms.name` from `subscriptions.name`**: the room-name `$set` is unguarded and commits before a NAK-able federate, while the subscription write *is* high-water-mark guarded and refuses to follow it back. **The teams-mode deploy silently serves live client DM-create RPCs** on the shared queue group. And structurally the service is the hardest in the fleet to change safely: a 476-line function inside a 2,625-line `handler.go`, a 7,920-line test file, a 31-method store interface, and five copy-pasted federation blocks.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 2 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 12 | 22 | 15 | 6 | **55** |

> **Audit-coverage caveat.** `gosec` and the repo-owned `semgrep` rules are clean repo-wide; `govulncheck` and the registry packs could not run (blocked egress), so dependency-CVE coverage is unverified.

---

## 2. Go code quality — 4 / 5

Disciplined, idiomatic Go — correct `%w` wrapping, `errors.Is` never string comparison, clean `errcode` Tier-1 usage and `jsretry.SettleQuiet` — held back by 22 systematically-discarded marshal errors and context-less log calls.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **22 `json.Marshal` errors discarded to `_`** with no justifying comment (§3: "never ignore errors silently — comment if intentionally discarded"). A nil `data` is published as an **empty body**, so a marshal regression ships a malformed event instead of failing. The same file does it right at `handler.go:1977`, which makes this stylistic drift rather than policy | `handler.go:187`, `:452`, `:467`, `:480`, `:504`, `:506`, `:532`, `:668`, `:682`, `:693`, `:708`, `:731`, `:1188`, `:1195`, `:1210`, `:1234`, `:1259`, `:1296`, `:1348`, `:1868`, `:1876`, `:1898` |
| medium | `mustMarshal` violates the Go `must*` convention: it **swallows the error and returns `nil`** rather than panicking. The name promises a guarantee the body does not provide; every caller reads it as infallible | `handler.go:1347` |
| medium | Non-`Context` `slog` variants used inside functions holding a `ctx`, dropping the correlation ID §3 requires. These are the publish-failure and rename paths — precisely the lines an operator needs to join to a request — while 20+ sibling sites in the same file use `*Context` correctly | `handler.go:112`, `:2305`, `:2426`, `:2434`, `:2449`, `:2462`, `:2584` |
| medium | Raw decoder error text interpolated into a **client-facing** `errcode.BadRequest` — §3: "Never expose raw internal errors to clients". The other three unmarshal sites use static strings, so this is an outlier | `handler.go:2302` |
| low | `SubscriptionStore` declares 30 methods spanning rooms, users, apps, orgs, threads, room-members and cross-site flags; the `<Domain>Store` name no longer names a domain | `store.go:64` |
| low | 14 bare `return err`. Mostly benign (the callee wraps), but at `handler.go:417` the surviving text is only `pkg/outbox`'s "publish outbox event for {dest}" — **the caller's room operation is lost** | `handler.go:354`, `:417`, `:545`, `:626`, `:758`, `:861`, `:1014`, `:1300`, `:1566`, `:1643`, `:1901`, `:2408`, `:2546`; `teamsroomcreate.go:85` |
| low | `loadAddMemberInputs` uses a plain `errgroup.Group`, not `errgroup.WithContext`, so a failing branch leaves up to four sibling Mongo queries running to completion | `handler.go:780-783` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Inconsistent log-key casing: `room_id` (24 sites) vs `roomId` vs `roomID`; `request_id` (22) vs `requestID` — breaks dashboard filters keyed on the dominant form | `handler.go:2308`, `:2309`, `:2584`, `:2610` |
| nitpick | Store result DTOs carry only `bson` tags, no `json` | `store.go:30`, `:37`, `:55` |
| nitpick | `errors.New("chat has no id")` states no operation, unlike every other error in the file | `teamsroomcreate.go:47` |

### Recommendations
- `medium` — Replace the 22 discarded marshals with the existing `publishCanonical` shape on error-returning paths, and a single logged-and-skipped branch on best-effort fan-outs. The payloads are all `pkg/model` structs, so the errors are unreachable — **that is the argument for a one-line comment, not for `_`**.
- `medium` — Either make `mustMarshal` actually panic (matching `text/template.Must`) or rename it `marshalOrEmpty` and document that callers may publish an empty body.
- `medium` — Convert the seven non-`Context` `slog` calls to `*Context`; add a `forbidigo`/semgrep rule so the plain variants cannot be reintroduced in `package main` handlers.
- `medium` — Drop `err.Error()` from `handler.go:2302`; use the static `errcode.BadRequest` form the other three sites use.
- `low` — Rename `SubscriptionStore` to `RoomWorkerStore` or split off the thread-cleanup and org-display groups; switch `loadAddMemberInputs` to `errgroup.WithContext`; normalize log keys to `snake_case`.

