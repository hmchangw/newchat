# message-gatekeeper — Production Readiness Review

**Service:** `message-gatekeeper` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

The first hop on every user message, and the hot path is genuinely well-engineered: precomputed metric attribute sets, L1+L2 caches with singleflight, precise Mongo projections, correct semaphore consumer pattern, sonic + `Pretouch`, clean `jsretry` discipline. Excluding `main.go` the package is ~91% covered. Three things stand out. **The consumer binds MESSAGES with no `FilterSubjects`, so every verb under `msg.>` is processed as a create** — a client publishing `msg.edit` today is validated as a send and republished to the canonical `.created` subject. **The parent-resolution path has no overall deadline** — a reply that quotes a different parent can hold a worker slot for ~6 s. And the derived client-API view **contradicts the canonical doc** on bot-DM fan-out.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 8 | 20 | 15 | 9 | **52** |

---

## 2. Go code quality — 4 / 5

Disciplined error tiering, typed `errcode` usage and zero string-matching on errors; marred by four ctx-less `slog` calls on the per-request path, two "log AND return" violations, and two unwrapped error returns.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **Log-and-return double-logs the large-room rejection**: `slog.Info("send blocked")` fires, then the same error reaches `errcode.Classify`, which logs `"request failed"` at Info for a `Forbidden`. **Two Info lines per blocked send** | `handler.go:425`, returned `:432`, marshalled `:211`; `pkg/errcode/classify.go:40`, `:49` |
| medium | Same double-log on the invalid-subject path | `handler.go:160`, marshal at `:164` |
| medium | **Four per-request logs use the ctx-less `slog` variants**, dropping the request/trace correlation the surrounding code just built — `ctx` is enriched with `request_id`/`account` at `:145` and `room_id` at `:173`. Two of them land in Loki with **no `request_id` at all** — precisely the two lines an operator would want to join to a failing send. Sibling calls in the same file use the `*Context` forms, so this is inconsistency, not house style | `handler.go:160`, `:298`, `:425`, `:472` |
| low | **Unwrapped error escapes `processMessage`**: `return sonic.Marshal(msg)` hands the caller a bare sonic error, which is then classified as an *infra* failure and **NAKed for redelivery — even though a marshal failure is permanent.** The immediately preceding marshal wraps correctly. This is the only such tail-return in any service handler repo-wide | `handler.go:519` vs `:507` |
| low | Bare `return nil, err` from the cache constructor — the message survives only because the caller happens to supply one | `metacache.go:21` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Unchecked type assertion inside a JetStream worker goroutine — safe today, but a panic here takes down a `MaxWorkers` goroutine; `interface{}` instead of `any` on a Go 1.25 module; an empty `default:` clause; `accountFromSubject` computed twice on the rejection path | `subcache.go:111`, `:81`; `handler.go:251`, `:147`, `:164` |

**Verified clean:** no `fmt.Println`/`log.Println`; no `errors.Is`-by-string; no `WithCause` chaining an `*errcode.Error`; **no token/password/body logging** — the flow breadcrumbs carry sizes and coarse tags only, and `:180` **deliberately declines `WithCause(parseErr)` to keep the offending substring out of the log**; `WithReason` used only where the frontend must branch; infra failures returned as raw `fmt.Errorf`; `errCanonicalPublish` is a sentinel matched by `errors.Is`, not text. **The documented sonic exception at `fetcher_history.go:53` is correctly scoped** — the projection omits `Reactions` and is correspondingly excluded from `pretouch.go`.

### Recommendations
- `medium` — Delete the two `slog` calls that precede a returned error; move any field worth keeping into `errcode.WithLogValues(ctx, …)` so `Classify` emits them on its single line.
- `medium` — Convert `handler.go:298` and `:472` to the `*Context` variants so the enriched `request_id`/`room_id`/trace ride the line.
- `low` — Wrap the tail marshal as a typed `errcode.Internal(…, WithCause(err))` so a permanently unmarshalable message **Acks instead of burning `MaxDeliver` NAKs**; wrap `metacache.go:21`.
- `nitpick` — Make the singleflight assertion comma-ok with a defensive fallback; switch `interface{}` → `any`.

---

## 3. Architecture — 4 / 5

Boundaries, DI, bootstrap gating and the high-throughput consumer pattern are all correct and well-documented; the deductions are an unfiltered client-facing consumer, per-service re-declaration of shared cache knobs, and a constructor that has outgrown positional DI.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **The consumer binds MESSAGES with no `FilterSubjects`**, so every verb under `chat.user.*.room.*.{siteID}.msg.>` is processed as a create. `buildConsumerConfig` sets only `Durable`; the stream captures `msg.>`, and `subject.ParseUserRoomSiteSubject` returns `ok` from the first four tokens, **never inspecting the trailing verb**. A client publishing `…msg.edit` today is validated as a send and republished to `chat.msg.canonical.{siteID}.created`. `CLAUDE.md` explicitly reserves `.edited`/`.deleted` "for future", so **the gate will silently mis-route them the day they exist.** Fix is one line | `main.go:269-275`; `pkg/stream/stream.go:18`; `pkg/subject/subject.go:113-118` |
| medium | **L1 cache knobs re-declared per service** — `ROOM_META_CACHE_TTL`, `USER_CACHE_SIZE`, `USER_CACHE_TTL` carry their own tag + `envDefault` here and again in four sibling services. **The L2 siblings already do it right** (`RoomMetaL2 roommetacache.TTLConfig`, `UserL2 userstore.TTLConfig`); the L1 tier never got it. The consequence is the documented one: two services reading the same data drift apart on a default nobody notices | `main.go:52-53`, `:58-59` |
| medium | `NewHandler` takes **10 positional parameters**, four of which are policy scalars (`largeRoomThreshold, maxAttachments, maxAttachmentBytes, chatBaseURL`) — call-site-indistinguishable ints and strings. The options pattern is **already present** in the same file | `handler.go:107`, `:82-102` |
| medium | The gate performs a **synchronous cross-service RPC plus a timed re-check inside the JetStream worker slot**: quote and thread-parent resolution issue a 2 s NATS request to history-service, and a missing thread parent adds a 150 ms retry. **Ingest availability is therefore coupled to history-service latency** — each in-flight send holds a `MaxAckPending` slot for up to ~4.2 s. Architecturally this is *enrichment* work living in the *validation* gate | `fetcher_history.go:82`; `handler.go:547-561` |
| low | MESSAGES-CANONICAL is **not exclusively produced by this service**, so the invariants enforced here (20-char message ID, 20 KB content cap, attachment caps) apply only to the client lane. Legitimate for system messages, but "gatekeeper validates → canonical" is not the whole truth | `room-worker/handler.go:533`, `:732`, `:1260`, `:1981`; `room-service/handler_teams.go:269` |
| low | Subject-shape knowledge duplicated outside `pkg/subject`: `accountFromSubject` re-implements the `chat.user.{account}.…` split with a raw `strings.Split`, existing only because `ParseUserRoomSiteSubject` is all-or-nothing | `handler.go:305-311` |
| nitpick | Dangling reference to a file that does not exist (`see doc.go`) | `handler.go:179` |

**Verified correct:** `Store` is consumer-defined with exactly the two methods used; `bootstrapStreams` sets only `Name + Subjects` from `pkg/stream`, verifies-and-fails-fast when disabled, matching the repo-wide shape; the high-throughput pattern is intact (`cons.Messages()` + `PullMaxMessages(2*MaxWorkers)` + semaphore/WaitGroup), never mixed with `Consume()`; shutdown order is correct under `shutdown.Wait`; publish/reply injected as fields; zero `os.Getenv`.

### Recommendations
- `high` — **Set `FilterSubjects` to the `msg.send` pattern on the durable** (or reject non-`send` verbs before `processMessage`), and add a `pkg/subject` parser that returns the verb.
- `medium` — Move the `USER_CACHE_*` / `ROOM_META_CACHE_*` L1 knobs into `userstore` / `roommetacache` as mounted config structs, mirroring the existing `TTLConfig` fields.
- `medium` — Collapse the four policy scalars into a `sendPolicy` struct or handler options.
- `medium` — Extract quote/thread-parent resolution behind a `parentResolver` type so the gate is validate-and-publish and the enrichment coupling is isolated and separately testable.
- `low` — Add `subject.AccountFromUserSubject` and delete the local helper; fix the `doc.go` reference.

