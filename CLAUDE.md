# Project Guidelines

## Section 1: Project Context

**What:** Distributed multi-site chat system in Go. Users send messages in rooms with real-time delivery, federated across independent sites.

**Architecture:** Event-driven microservices — NATS JetStream for async event processing, NATS request/reply for sync operations.

| Component | Technology |
|-----------|-----------|
| Language | Go 1.25 |
| Messaging | NATS + JetStream |
| Operational DB | MongoDB (rooms, subscriptions, messages) |
| History DB | Cassandra (message history / time-series) |
| Auth | NATS callout service with JWT + NKeys |
| HTTP Framework | Gin |
| HTTP Client | Resty |
| Config | Environment variables via `caarlos0/env` |
| Observability | `flywindy/o11y` SDK — OpenTelemetry traces, Prometheus metrics, `log/slog` JSON logs (all trace-correlated); each service wires it once via `pkg/obs.Init` |
| Testing | `go.uber.org/mock` (mockgen), `stretchr/testify` (assertions), `testcontainers-go` (integration) |
| Containers | Docker multi-stage builds, Docker Compose |

**Event flow:** User publishes message to MESSAGES stream → `message-gatekeeper` validates and publishes to MESSAGES-CANONICAL → `message-worker` persists to Cassandra, `broadcast-worker` delivers to room members, `roomlist-worker` applies room/subscription state (`lastMsgAt`, `hasMention`, sender `lastSeenAt`), `notification-worker` sends notifications → cross-site events are published directly into remote sites' INBOX streams.

**Multi-site federation:** Each site runs independently with its own NATS, MongoDB, and Cassandra. Cross-site events cross the NATS supercluster as a direct JetStream publish into the destination site's INBOX stream (`chat.inbox.{destSiteID}.external.{eventType}`, no sourcing/SubjectTransform). Origin-side, `room-service`'s request/reply federation, `room-worker`'s order-sensitive events (`member_added`/`member_removed`/`room_renamed`), `message-worker`'s thread-subscription events and `broadcast-worker`'s mention badges are buffered through a local per-site `OUTBOX` stream that `outbox-worker` drains and forwards, so a failed cross-gateway publish is durably retried rather than lost. Both lanes are per remote peer (from `ALL_SITE_IDS`) so a down peer's parked forwards (`MaxDeliver=-1`, never Ack) fill only their own consumer's ack-pending budget instead of stalling delivery to healthy peers: the order-sensitive events ride per-destination FIFO lanes (`MaxAckPending=1`, so they can't overtake each other — e.g. a rename can't overtake the add that creates the subscription it renames; one in-flight probe per down peer); the order-insensitive subscription-state events ride a per-destination concurrent consumer (default budget). Other consumer-originated events (messages) publish to the remote INBOX directly. User subscriptions and room metadata are scoped by `siteID`.

**Repo structure:** Monorepo with single `go.mod` at root. Services are flat `package main` directories at the repo root — no `cmd/` or `internal/`. Shared code lives in `pkg/`. Each service has a `deploy/` subdirectory with Dockerfile, docker-compose.yml, and azure-pipelines.yml. Claude discovers services by exploring the repo.

**Per-service file organization:**
- `main.go` — Config parsing, dependency wiring, startup, graceful shutdown
- `handler.go` — Request/message handling logic
- `routes.go` — HTTP route registration (Gin services only)
- `store.go` — Store interface definition + `//go:generate mockgen` directive
- `store_mongo.go` / `store_cassandra.go` — Store implementation
- `handler_test.go` — Unit tests with mocked store
- `integration_test.go` — Integration tests with testcontainers (tagged `//go:build integration`)
- `mock_store_test.go` — Generated mocks (never edit manually)

All services follow this layout, including `message-gatekeeper` (validates messages and publishes to MESSAGES-CANONICAL).

**Note:** request/reply services with a larger surface (e.g. `user-service`, `history-service`) MAY instead use a sub-package layout under the service directory (`config/`, `models/`, `mongorepo/`, `service/`, `service/mocks/`) — this is a sanctioned exception, not a deviation. The store interface still lives with its consumer (`service/`), and generated mocks still go in a dedicated mocks package (`service/mocks/`).

## Section 2: Common Commands

All commands are wrapped in the root Makefile. Always use `make` targets — never run raw `go` commands directly.

| Command | Description |
|---------|-------------|
| `make lint` | Run `golangci-lint` (includes `go vet`, `staticcheck`, `errcheck`, `goimports`, etc.) |
| `make fmt` | Run `goimports` via `golangci-lint fmt` to format all `.go` files |
| `make test` | Run all unit tests with race detector |
| `make test SERVICE=<name>` | Run unit tests for a single service |
| `make test-integration` | Run all integration tests (requires Docker) |
| `make test-integration SERVICE=<name>` | Run integration tests for a single service |
| `make generate` | Regenerate all mocks |
| `make generate SERVICE=<name>` | Regenerate mocks for a single service |
| `make build SERVICE=<name>` | Build a single service binary |
| `make tools` | Install pinned dev/SAST tooling (`golangci-lint`, `gosec`, `govulncheck`, `semgrep`) |
| `make sast` | Run all SAST scans (`gosec`, `govulncheck`, `semgrep`) plus the repo-owned semgrep rule tests; fails on medium+ |
| `make sast-gosec` / `make sast-vuln` / `make sast-semgrep` | Run a single SAST scan |
| `make sast-semgrep-test` | Run the repo-owned semgrep rules against their fixtures. A rule file `.semgrep/X.yml` is tested when `.semgrep/X.go` exists beside it; annotate lines a rule must flag with `// ruleid: <id>[, <id>]`, and leave lines it must not flag unannotated. Add fixtures when adding or editing a rule — an unverified rule can be disabled by a pattern edit without any scan failing |

## Section 3: Coding Rules

### Naming
- Packages: short, lowercase, single-word — no underscores or mixedCaps
- Interfaces: `-er` suffix for single-method; `<Domain>Store` for store interfaces
- Constructors: `New<Type>` pattern
- Export only what other packages consume; keep handler/store implementations unexported within services
- NEVER name packages `utils`, `helpers`, `common`, or `base` — use descriptive names that convey specific functionality

### Error Handling
- Always wrap with context: `fmt.Errorf("short description: %w", err)` — describe what the current function was doing, not what failed underneath
- Never return bare `err` or `fmt.Errorf("error: %w", err)`
- Never ignore errors silently — comment if intentionally discarded
- Use `pkg/errcode` for ALL client-facing errors; reply via `errnats.Reply` (NATS) / `errhttp.Write` (Gin). Construct with the named constructors (`errcode.NotFound`, `errcode.Forbidden`, …), attach a domain `reason` from `codes_<service>.go` where the frontend must distinguish cases, and return raw `fmt.Errorf("…: %w", err)` for infra failures (they collapse to `internal` at the boundary). Full guide: `docs/error-handling.md`. Wire-side reference for clients: `docs/client-api.md` §6.
- Never compare errors by string — use `errors.Is` and `errors.As`
- Never expose raw internal errors to clients — the unexported `errcode.Error.cause` is never serialized; `Classify` logs it once server-side. Never wrap raw message bodies/tokens into a cause.

### Interfaces & Dependency Injection
- Define interfaces in the consumer, not the implementer
- Each service defines its own store interface in `store.go` with only the methods it needs
- Accept interfaces, return structs
- Handler structs hold dependencies injected via constructor

### Struct Tags
- All model structs get both `json` and `bson` tags
- Use `bson:"_id"` for MongoDB primary keys mapped to the `ID` field
- `camelCase` for both `json` and `bson` tags, except `_id`

### Logging
- Always use `log/slog` with JSON format — never `fmt.Println`, `log.Println`, or text-format loggers
- Structured fields as key-value pairs, never interpolated strings
- Never log tokens, passwords, or full message bodies

### Request Logging & Tracing
- HTTP services (Gin): use middleware that logs method, path, status code, latency, and request ID per request
- Generate or extract a unique request/correlation ID at the entry point (HTTP middleware or NATS message handler), propagate via `context.Context`, include in all log lines
- **Request ID format**: 36-char hyphenated UUID (industry-standard form, e.g. `01970a4f-8c2d-7c9a-abcd-e0123456789f`). Generated server-side via `idgen.GenerateRequestID()` (UUIDv7 hyphenated) when no inbound `X-Request-ID` header is present. Inbound IDs are accepted as long as they are valid UUIDs in standard hyphenated form (v4 or v7, case-insensitive) — validated via `idgen.IsValidUUID`. The 32-char no-hyphen form is reserved for Mongo entity `_id`s only and is NOT used for request IDs.

### Concurrency
- Never use `time.Sleep` for goroutine synchronization — use proper sync primitives (channels, `sync.WaitGroup`, `sync.Mutex`)
- Never launch goroutines without a clear termination path — avoid goroutine leaks

## Section 4: Testing Rules

### Unit Tests
- Use standard `testing` package with `github.com/stretchr/testify/assert` and `testify/require` for assertions
- Mock with `go.uber.org/mock` (mockgen) — generated mocks go in `mock_store_test.go`, never edit manually
- Test files live in the same package (`package main`) to access unexported types
- Naming: `Test<Type>_<Method>` or `Test<Type>_<Method>_<Scenario>`
- Never connect to real databases, NATS, or external services in unit tests
- When a handler publishes to JetStream, inject the publish function as a field so tests can capture data without a real NATS connection

### Table-Driven Tests
- Prefer table-driven tests when testing multiple input/output variations of the same logic
- Each test case must have a descriptive name
- Use `t.Run(name, func(t *testing.T) { ... })` for subtests

### Test Independence
- Each test must be fully independent — no shared mutable state between tests
- Never rely on test execution order
- Set up and tear down all state within each test (or subtest)

### Test Data & Fixtures
- Use `testdata/` directory within the package for test fixtures (JSON files, golden files, mock data) — the Go toolchain ignores this directory during builds
- Test fixtures stay close to the tests that use them

### Test Helpers & Utilities
- Test helpers belong in `_test.go` files only — NEVER put test helpers in production code
- Shared test utilities used by multiple packages may live in a dedicated `pkg/testutil/` package (only imported by test files)

### Test-Driven Development (TDD)
- ALL new code MUST follow the Red-Green-Refactor TDD cycle — no exceptions
- The TDD cycle for every task:
  1. **Red:** Write comprehensive tests FIRST in `*_test.go` — run them and confirm they FAIL (implementation doesn't exist yet)
  2. **Green:** Write the minimum implementation to make all tests PASS
  3. **Refactor:** Clean up the implementation while keeping tests green
  4. **Commit:** Commit with a descriptive message after tests pass
- Never write implementation code before its corresponding tests exist
- Never skip the Red phase — if tests pass before implementation, the tests are wrong or testing the wrong thing
- Tests must cover: happy path, error paths, edge cases (empty collections, boundary conditions), and invalid input
- For handler tests: test each NATS/HTTP handler method with table-driven tests covering all documented scenarios
- For store tests: integration tests with testcontainers cover store implementations

### Coverage
- **Minimum 80% code coverage** is REQUIRED for all packages — code below this threshold MUST NOT be merged
- **Target 90%+ coverage** for core business logic: handlers, store implementations, and shared `pkg/` packages
- Cover error paths and boundary conditions, not just happy paths — meaningful coverage, not vanity percentages
- Use `go test -coverprofile=coverage.out` and `go tool cover -func=coverage.out` to verify coverage percentages
- Every handler method must have tests for: valid input, invalid/malformed input, store errors, and edge cases
- Every exported function in `pkg/` must have corresponding test cases

### Integration Tests
- All integration tests use the `//go:build integration` build tag
- Test files live in the same package as the code under test (`package main` for services, `package <pkg>` for libraries) — never external `*_test` packages
- **Containers come from `pkg/testutil`** — do not start your own with `testcontainers.GenericContainer` / `natsmod.Run` / `mongodb.Run` etc. Process-shared helpers (one container, many tests, started via `sync.Once`, terminated via `TerminateAll`):
  - `testutil.MongoDB(t, prefix) *mongo.Database` — isolated DB per test
  - `testutil.CassandraKeyspace(t, prefix) (keyspace, *gocql.Session, host)` — isolated keyspace per test
  - `testutil.MinIO(t, prefix) (*minio.Client, bucket)` — isolated bucket per test
  - `testutil.Elasticsearch(t) string` — shared ES URL; pair with `testutil.ElasticsearchIndex(t, prefix)` for a per-test isolated index (DELETEd on cleanup)
  - `testutil.NATS(t) string` — shared NATS URL with JetStream enabled
  - `testutil.NATSWebSocket(t) NATSWebSocketInfo` — shared WebSocket-enabled NATS (no auth, no JetStream) for client-transport tests; container-first with a `nats-server` subprocess fallback (subprocess mode has no docker network — tests that attach sibling containers must skip when `Network == ""`)
- Valkey (cluster-mode — services use this in production):
  - `testutil.SharedValkeyCluster(t) *redis.ClusterClient` — process-shared cluster (started via `sync.Once`, reaped via `TerminateValkey`/`TerminateAll`). Per-test caller MUST register `t.Cleanup(func() { testutil.FlushValkey(t) })` so sibling tests start with a clean keyspace. Default choice.
  - `testutil.StartValkeyCluster(t) *redis.ClusterClient` — per-test cluster (each test gets its own container via `t.Cleanup`). Use ONLY when the test asserts on cluster-routing state (e.g. `CLUSTER KEYSLOT` checks) or owns a store wrapper that calls `Close()` on the underlying client.
- **Every integration test package must have a `TestMain` that drives cleanup**:
  ```go
  //go:build integration
  package mypkg

  import (
      "testing"
      "github.com/hmchangw/chat/pkg/testutil"
  )

  func TestMain(m *testing.M) { testutil.RunTests(m) }
  ```
  `testutil.RunTests` wraps `m.Run()` + `testutil.TerminateAll()` + `os.Exit(code)`. For concurrent pre-warming use `testutil.RunTestsWithPrewarm(m, testutil.EnsureElasticsearch, testutil.EnsureNATS, ...)` — runs each `EnsureXxx` concurrently and fails fast on the first error before `m.Run`. The `testutil.PrewarmFailFast(fns...)` building block is also exposed for packages that need extra cleanup between `m.Run` and `os.Exit`.
- **Ryuk is disabled repo-wide** (via `pkg/testutil/init.go`) because our CI runner can't run the reaper sidecar. `testutil.TerminateAll` is the only cleanup mechanism on clean exits. SIGKILL / Ctrl+C will leak containers locally — acceptable trade-off; flip Ryuk back on with `TESTCONTAINERS_RYUK_DISABLED=false go test ...` if debugging a leak.
- Per-test isolation is the caller's responsibility: the `MongoDB`/`Cassandra`/`MinIO` helpers already hash `t.Name()`; for ES use a per-test unique index name and DELETE on cleanup; for NATS use a per-test `*nats.Conn` pair with `Drain`/`Shutdown` cleanups; for shared Valkey call `testutil.FlushValkey(t)` in `t.Cleanup` (StartValkeyCluster's per-test mode is automatic).
- Inline `testcontainers.GenericContainer` is only acceptable when a shared testutil container can't accommodate the test (e.g. search-service CCS needs two ES nodes on a shared docker network; `pkg/roomcrypto` needs a Node container with bundled scripts). Each inline container must store its reference and register `t.Cleanup(container.Terminate)`. (`pkg/roomkeysender`'s former inline ws-NATS container has been promoted to `testutil.NATSWebSocket`.)
- New shared dependencies (a container type used by ≥2 packages) belong in `pkg/testutil` with the same shape: `Xxx(t)` + `EnsureXxx()` + `TerminateXxx()`, container ref stored at package level, and `TerminateXxx` wired into `TerminateAll`.

### Model Tests
- `pkg/model/model_test.go` verifies all domain types marshal/unmarshal correctly via a generic `roundTrip` helper

### General
- Run `make generate` before testing if store interfaces changed
- ALWAYS use the `-race` flag in testing — use `go test -race` to catch data races (the Makefile handles this)

## Section 5: Workflow Guardrails

### Before Committing
- Run `make generate` first if store interfaces were changed
- Lint and tests are enforced by a pre-commit hook — fix failures before retrying
- SAST (`gosec`, `govulncheck`, `semgrep`) is a **blocking CI gate** (the `sast` job, fail on medium+). Run `make sast` locally before pushing. Suppress only genuine false positives with a justified gosec-native comment — `// #nosec <RULE> -- reason` on the line **directly above** the statement. Note: golangci-lint's `//nolint:gosec` directive does NOT suppress standalone `gosec`; the two mechanisms are independent and a knowingly-unsafe `InsecureSkipVerify`/conversion needs both.
- Never commit `.env` files
- Never merge code directly into `master` or `main` — always create a PR for review first
- If your changes touch a client-facing handler (any handler registered with `nc.QueueSubscribe` or `natsrouter.Register` whose subject begins with `chat.user.{account}.request.…` or `chat.user.{account}.room.{roomID}.{siteID}.msg.send`, or any HTTP route in `auth-service`), update `docs/client-api.md` in the same PR to reflect the new request/response schema, error cases, and triggered events.
- `docs/reviews/` holds session-scoped multi-agent review reports (output of the `branch_review` skill). Delete every file under `docs/reviews/` from the branch just before creating the PR — these reports are working notes for the author, not shippable artifacts.

### Documenting the Client API (`docs/client-api.md`)
- Any change to a client-facing RPC (a handler whose NATS subject begins with `chat.user.`) must be reflected in `docs/client-api.md` in the same PR (see the client-facing-handler bullet above).
- Every request body and response payload is a field table (current style). Each field has an explicit type — never `object`. Compound types get their own named table (shared types in §3.0 Shared schemas, one-offs inline) and are referenced by linked name (e.g. `[Participant](#participant)`, `ChannelRef[]`, `map<emoji, UserRef[]>`).
- Every success response includes a JSON example.
- Keep edits clean: minimal prose, no redundant comments or long explanations.
- If the change also touches `docs/client-api/request-reply.md` or `docs/client-api/events.md` (the derived request/reply and events views), update the matching view(s) in the same PR — they must never drift from the canonical `docs/client-api.md`.
- Any change to a client-facing **request/reply struct or a server→client event struct** in `pkg/model/` (including `pkg/model/event.go`) — adding, removing, renaming, or retyping a field — must update `docs/client-api.md` **and** its derived views (`docs/client-api/request-reply.md`, `docs/client-api/events.md`) in the same PR, even when no handler registration changed.

### Before Editing
- Always read a file before modifying it — understand existing code before suggesting changes
- Follow existing patterns in the codebase — don't invent new conventions

### When Adding Dependencies
- Ask before adding new third-party dependencies to `go.mod`
- Prefer standard library solutions when reasonable

### When Creating Services
- Follow the flat service directory convention — new service at repo root, not under `cmd/` or `internal/`
- Include `deploy/Dockerfile`, `deploy/azure-pipelines.yml`, and `deploy/docker-compose.yml`
- Follow the per-service file organization (`main.go`, `handler.go`, `store.go`, etc.)

### When Writing Code
- Verify compilation after changes — don't leave broken code
- Keep changes minimal and focused — don't refactor unrelated code
- If unsure about scope or approach, ask before implementing

## Section 6: Project-Specific Patterns

### NATS & Messaging
- Use `github.com/nats-io/nats.go` for core and `github.com/nats-io/nats.go/jetstream` for JetStream
- Connect in `main.go` — on failure, log and exit immediately, don't retry at startup
- Use `iter.Stop()` + `wg.Wait()` + `nc.Drain()` for graceful shutdown — see "JetStream Consumer Pattern" and "Graceful Shutdown" sections
- All NATS payloads are JSON with typed structs from `pkg/model`, never `map[string]interface{}`. Codec: the message hot-path workers (`broadcast-worker`, `message-worker`, `notification-worker`, `message-gatekeeper`, `roomlist-worker`) marshal/unmarshal via `github.com/bytedance/sonic` (default config) for throughput, warmed at startup with `pkg/jsonwarm.Pretouch`; everywhere else uses `encoding/json`. sonic's default output is semantically equivalent but not byte-identical to stdlib (HTML metacharacters left unescaped, map keys unsorted), so only adopt it on a path after confirming no consumer relies on byte-identity (payload hashing, signatures, dedup keys) or marshals `map` fields — see the sonic wire-compat tests in `broadcast-worker`/`message-gatekeeper`. One exception: `message-gatekeeper/fetcher_history.go` decodes a narrow projection rather than the full `cassandra.Message`, because that type embeds the marshal-only struct-keyed `Reactions` map whose decoder sonic rejects.
- Use NATS request/reply for synchronous operations; `nc.QueueSubscribe` with service name as queue group
- Use `natsutil.ReplyJSON` for success responses; for errors return a typed `*errcode.Error` from the handler and let `errnats.Reply` / `errhttp.Write` marshal the envelope (see `docs/error-handling.md`).
- Define all stream configs in `pkg/stream/stream.go` with name pattern `<STREAM>-<siteID>`
- Use durable consumers named after the service
- Stream creation is gated by `BOOTSTRAP_STREAMS` (see below); when enabled, use `js.CreateOrUpdateStream` (it's idempotent) via the service's `bootstrapStreams` helper, never inline

### Error Handling at the NATS/HTTP Boundary
`pkg/errcode` has a broad surface, but **day-to-day handler code touches almost none of it.** Use this tiering — if you reach past Tier 1, you should know why.

- **Tier 1 — every handler (this is 90% of usage).** Return a typed error built from a named constructor, optionally tagged with a `reason`. You do NOT call the adapter, classify, or log — the plumbing does:
  - `return errcode.NotFound("room not found")` — pick the constructor whose name matches the HTTP/wire category (`BadRequest`, `NotFound`, `Forbidden`, `Conflict`, `Internal`, …).
  - `return errcode.Forbidden("only owners can do this", errcode.WithReason(errcode.RoomNotOwner))` — add `WithReason` **only** when the frontend must branch on the case. Prefer a package-level sentinel (e.g. room-service `helper.go`) over reconstructing the same error at multiple sites, so `errors.Is` matches.
  - For an infra failure, `return fmt.Errorf("get subscription: %w", err)` — a raw wrapped error collapses to `internal` at the boundary; do NOT dress it up as an errcode.
- **Tier 2 — one line per handler, written once and copied.** The adapter that turns the returned error into the wire envelope. You pick exactly one, determined by your transport, never both:
  - NATS raw handler: `errnats.Reply(ctx, m.Msg, err)`.
  - `pkg/natsrouter` handler: returned automatically by the router — you write nothing.
  - Gin handler: `errhttp.Write(ctx, c, err)`.
- **Tier 3 — specialist, you'll know when.** Don't use these in ordinary request/reply handlers:
  - `errcode.Permanent` / `IsPermanent` — JetStream **workers only**, to Ack-poison vs Nak-retry.
  - `errcode.Parse` — **cross-site consumers** decoding a remote envelope (e.g. `memberlist_client.go`).
  - `errnats.Marshal` / `MarshalQuiet` / `ReplyQuiet` — already-logged paths; the plain `Reply` already classifies-and-logs once, so `Quiet` exists only to avoid a double-log.
  - `errcode.Classify`, `WithLogger`, `WithLogValues` — boundary/observability plumbing; handlers get request-id logging for free from the router middleware.
- **Never log AND return.** `Reply`/`Write` run `Classify`, which logs once at a category-aware level. A `slog.Error(...)` before returning the same error double-logs.
- **`WithCause` wraps an infra error, never another `*errcode.Error`** (one-errcode-per-chain; it panics otherwise, and semgrep guards it). Never put a raw token/body/subject in a cause or message — it reaches the server log.
- Full guide: `docs/error-handling.md`. Wire reference for clients: `docs/client-api.md` §6.

### Event Timestamps
- Every NATS event struct in `pkg/model` must include a `Timestamp int64 \`json:"timestamp" bson:"timestamp"\`` field
- Set the timestamp at the publish site using `time.Now().UTC().UnixMilli()`
- This is the event-level timestamp (when the event was published), distinct from any domain-level timestamps in embedded structs (e.g., `Message.CreatedAt`)

### NATS Subject Naming
- Dot-delimited hierarchical subjects — use `pkg/subject` builders, never raw `fmt.Sprintf`
- User-scoped: `chat.user.{account}.…`
- Room-scoped: `chat.room.{roomID}.…`
- MESSAGES-CANONICAL: `chat.msg.canonical.{siteID}.created` (`.edited`, `.deleted` for future)
- Inbox (cross-site, remote-origin): `chat.inbox.{destSiteID}.external.{eventType}` — published directly into the destination site's INBOX
- Inbox (same-site search feed): `chat.inbox.{siteID}.internal.{eventType}`
- Outbox (origin-side federation buffer): `chat.outbox.{siteID}.{destSiteID}.{eventType}` — `room-service` (subscription-state events), `room-worker` (membership events), `message-worker` (thread-subscription events) and `broadcast-worker` (mention badges) publish one event per destination; `outbox-worker` forwards each to the destination INBOX (destination scoped so the per-peer membership FIFO consumers can filter on one site)
- Wildcards: `*` for single-token, `>` for multi-token tail — define patterns in `pkg/subject`

### JetStream Streams
- `MESSAGES-{siteID}` — User message submissions
- `MESSAGES-CANONICAL-{siteID}` — Validated messages (single source of truth for downstream workers). Consumed by `message-worker` (Cassandra persistence), `broadcast-worker` (fan-out; one buffered preview write), `roomlist-worker` (room/subscription MongoDB writes), `notification-worker` and `search-sync-worker`. `roomlist-worker` runs with `MaxDeliver=-1` so a MongoDB outage retries rather than drops. Before this split, a `SetSubscriptionMentions` failure in `broadcast-worker` returned before any publish, blocking fan-out entirely — at the repo-default `MaxDeliver=5` the message was NAKed five times and then dropped, never delivered to any client, not duplicated. `broadcast-worker` keeps exactly one MongoDB write — the room-list preview, which it buffers and drains best-effort and never awaits (`broadcast-worker/preview_writer.go`) — because sealing a preview needs the users, mention participants and attachments the fan-out already resolved and `roomlist-worker` deliberately holds none of. The room's own pointer (`lastMsgAt`/`lastMsgId`/`lastMentionAllAt`), the sender's `lastSeenAt` and the mention badges are all `roomlist-worker`'s, so no MongoDB failure can block or NAK fan-out any more. The two writers touch disjoint halves of the room document, each with its own watermark; what they give up is atomicity between `previewForMsgId` and `lastMsgId`, and a disagreement there only costs history-service's lazy walk, which then warms the room back. Both coalescers order messages with `msgbucket.NewerRow` so they cannot pick different "newest" messages at a same-millisecond tie.
- `ROOMS-{siteID}` — Member invite requests
- `INBOX-{siteID}` — Cross-site federation events, published directly by remote sites onto the `external.>` lane (no sourcing/SubjectTransform); same-site services also publish a search-only feed onto the `internal.>` lane
- `OUTBOX-{siteID}` — Origin-side federation buffer: `room-service` publishes an `OutboxEvent` here for its request/reply cross-site events, `room-worker` for its order-sensitive events (membership + `room_renamed`), `message-worker` for its thread-subscription events and `broadcast-worker` for room-level mention badges (`subscription_mention`); `outbox-worker` consumes it and forwards each event to the destination's INBOX with at-least-once retry — per remote peer (from `ALL_SITE_IDS`, `MaxDeliver=-1`), a concurrent consumer for the order-insensitive subscription-state event types plus a FIFO consumer (`MaxAckPending=1`) for the order-sensitive types (`member_added`/`member_removed`/`room_renamed`, which share one lane so they can't overtake each other); per-destination (not one shared consumer) so a down peer's parked forwards fill only its own ack-pending budget instead of stalling healthy peers. The two filter sets partition the stream and live in `pkg/outbox` (`ConcurrentEventTypes` / `OrderedEventTypes`); a new OUTBOX event type MUST be added to exactly one of them — producers publish via `outbox.Publish`, which rejects types outside the partition instead of letting them sit in the stream unconsumed. Owned by `outbox-worker`
- **Stream bootstrap is opt-in.** Services that consume from or publish to a stream MUST NOT create it in production — streams are owned by ops/IaC. Each such service's `config` includes `Bootstrap bootstrapConfig` (env prefix `BOOTSTRAP_`) with a single `Enabled` field tagged `env:"STREAMS" envDefault:"false"`. The service's `bootstrap.go` defines a `bootstrapStreams(ctx, js, siteID, enabled) error` helper that no-ops when `Enabled=false`. Local `deploy/docker-compose.yml` sets `BOOTSTRAP_STREAMS=true` so any service can stand up against a fresh NATS in dev. New services that interact with JetStream MUST follow this convention.
- **Stream bootstrap ownership.** When a service does bootstrap a stream in dev, the helper sets ONLY the stream's schema — `Name + Subjects` from `pkg/stream.<Stream>(siteID)`. Cross-site federation is direct-publish: a service at the origin site JetStream-publishes to the destination's `chat.inbox.{destSiteID}.external.>` lane, routed by the NATS supercluster/gateway topology (an ops/IaC concern that MUST NOT appear in any service's `bootstrap.go`). INBOX has a single owning service (`inbox-worker`) and OUTBOX has a single owning service (`outbox-worker`). Other services that consume from or publish to a remote INBOX (e.g., `search-sync-worker`, and the cross-site publishers room-worker/message-worker/user-service, plus `outbox-worker` which forwards the federated events) rely on `inbox-worker` to create the local stream and on ops/IaC for the routing that makes a remote publish land. `room-service` no longer publishes cross-site directly, and `room-worker` no longer publishes its order-sensitive events (membership, `room_renamed`) cross-site directly — both publish an `OutboxEvent` to the local OUTBOX and `outbox-worker` does the forwarding.

### Valkey
- **Every Valkey key is built by a `pkg/cachekeys` builder — never by a string literal at the call site.** A new cache MUST add its `Keyspace` (name, prefix, suffix, sample) plus a builder to `pkg/cachekeys`; the package tests then enforce that its pattern overlaps no existing keyspace and that its builder classifies back to its own name. This is the same rule shape as the OUTBOX event-type partition: registration is a precondition of use, so forgetting it fails loudly rather than silently.
- The registry is what makes a cache attributable. `cache-metrics-worker` walks the keyspace and exports `valkey_cache_keys` / `valkey_cache_bytes` per cache via `pkg/cachescan`; a key matching no registered keyspace lands in the `unclassified` series. An unregistered cache is therefore visible but unnamed — alert on `unclassified` growth.
- Reported bytes are attributed bytes and do NOT sum to a node's `used_memory`: `MEMORY USAGE` excludes each key's share of the dict bucket array and cluster slot index (~82% of the true per-key cost when measured on Valkey 7.2), and a node carries a fixed baseline belonging to no cache. Use the gauges for per-cache share and trend; use `used_memory` for what the node holds.
- Use the `valkeyutil.Client` interface everywhere. `valkeyutil.Cluster` unwraps the raw `*redis.ClusterClient` and exists only for commands outside that interface (keyspace scans) — do not reach for it to avoid adding a method to a store.

### MongoDB
- Never use ORMs (no GORM, no ent) — use native drivers directly
- Driver: `go.mongodb.org/mongo-driver/v2`
- Use `mongoutil.Connect` from `pkg/mongoutil`
- Collections: lowercase plural of the domain entity (e.g., `rooms`, `subscriptions`, `messages`)
- Primary keys: application-generated via `pkg/idgen`, mapped to `bson:"_id"`. Format depends on the entity:
  - **Subscriptions, RoomMembers, ThreadRooms, ThreadSubscriptions**: UUIDv7 hex without hyphens (32 chars) via `idgen.GenerateUUIDv7()` — time-ordered for B-tree locality on high-write collections
  - **Channel Rooms**: 17-char base62 via `idgen.GenerateID()` — short, human-friendly
  - **DM Rooms**: sorted concat of two `user.ID` strings (~34 chars) via `idgen.BuildDMRoomID(a, b)` — deterministic, no separate dedup needed. A DM room is **always exactly two participants** — a direct message is never among 3 people or more; any conversation of 3+ users is a channel room, never a DM
  - **Messages**: 20-char base62 via `idgen.GenerateMessageID()` for new IDs (or client-supplied for user messages). `idgen.IsValidMessageID` accepts **either 17 or 20 char** base62 — 17 is the legacy length retained for backward compatibility with messages written before the 20-char cutover (federation replays, JetStream redeliveries, historical records).
  - **SSO tokens** (`sso_tokens`): 17-char base62 via `idgen.GenerateID()` for new docs — same length as the legacy ids migrated from the old stack; migrated docs keep their original `_id`
- Check `mongo.ErrNoDocuments` explicitly when a missing record is expected
- Create indexes in the store constructor or a dedicated `EnsureIndexes` method at startup
- **No `$lookup`**: server-side joins (`$lookup` in aggregation pipelines) are forbidden unless there is a very good, documented reason — prefer separate queries or denormalized data, and justify any exception in the PR. Pre-existing `$lookup` sites are grandfathered; when you touch one, add an inline `// $lookup justification: …` comment explaining why a join is unavoidable
- **Always project precisely**: every find/aggregation MUST specify an explicit projection that selects only the fields the caller needs — never fetch whole documents when a subset suffices
- **Retired room keys.** `retired_room_keys` holds one TTL-expired document per rotated-out key version, keyed `{roomID}:{version}` (a re-archive of the same version overwrites in place — idempotent for identical bytes, last-write-wins if a version number is reused after a `Delete`) (collection name: `roomkeystore.RetiredKeysCollection`). Retention is configured per service via `ROOM_KEY_RETIRED_TTL` (default 30m) and MUST be at least twice `broadcast-worker`'s `ROOM_KEY_CACHE_TTL`, because a version can be stamped into a message at the very end of a cache entry's life and retention has to outlast that entry plus the client's `key.get` fetch and retry. All services that read or write this collection (`room-service`, `room-worker`, `bot-room-service`) MUST be configured with the same `ROOM_KEY_RETIRED_TTL`; a service configured short expires versions its peers still consider resolvable, and `key.get` then permanently fails for messages already on the wire. `broadcast-worker` reads the value only to fail fast at startup when it is under `2 × ROOM_KEY_CACHE_TTL` — it is the only service that knows both numbers.

### Cassandra
- Driver: `github.com/gocql/gocql`
- Use `cassutil.Connect` from `pkg/cassutil` — `LocalQuorum` consistency, 10-second timeout
- Cassandra is ONLY for message history (time-series) — MongoDB handles everything else
- Design tables around query patterns (partition key = room ID, clustering key = timestamp), no secondary indexes
- `docs/cassandra_message_model.md` is the single source of truth for the message schema. Any PR that touches it MUST, in the same PR, update both downstream mirrors:
  1. The Go UDT/row structs in `pkg/model/cassandra/` (keep `cql:"…"` tags aligned with the columns).
  2. The init DDL under `docker-local/cassandra/init/*.cql` that creates the types and tables.
- **Bucketed message table.** `messages_by_room` uses a composite partition key `(room_id, bucket)`. The bucket is `floor(created_at_unix_ms / windowMs) * windowMs`. The window is configured per service via `MESSAGE_BUCKET_HOURS` (default 360). All services that read or write this table MUST be configured with the same `MESSAGE_BUCKET_HOURS`; mismatches will cause writes and reads to target different partitions and silently lose data. Bucket math lives in `pkg/msgbucket`.
- **Thread reply table.** `thread_messages_by_thread` is partitioned by `thread_room_id` alone — one partition per thread. Reads slice the partition by `created_at` clustering, no bucket walk required.
- **Plaintext message creates pin their write timestamp; nothing else does.** Every *plaintext* create INSERT in `message-worker/store_cassandra.go` binds `USING TIMESTAMP ?` from the message's own `CreatedAt` (`writeTS`, microseconds). Cassandra resolves conflicts per cell by write timestamp and gocql stamps statements with the client clock at execution time (`DefaultTimestamp` defaults to true), so an unpinned create that commits, NAKs on a later step and redelivers would outrank an edit made in between and silently restore the original body — a real window, since `message-worker` carries an outage retry budget spanning roughly an hour. Pinning makes a redelivery replay the identical write — which is only true when the bound values are identical on every attempt, so it is the precondition for pinning at all, not a side benefit. A new plaintext create INSERT into these tables MUST pin; edits, deletes and derived SETs (`tcount`/`tlm`) MUST NOT, so each stays strictly above the create it supersedes. **The precondition holds for the body, not for the enrichment columns**: `message-worker`'s handler re-resolves `sender` and `mentions` on every delivery and both fail open, so a degraded attempt and a healthy retry bind different values under one timestamp, and the per-cell value comparison can keep the degraded one or mix the two. The row stays readable either way, so the pin stays; making those values deterministic from the canonical event (or writing them as a separate unpinned mutation) is the real fix and has not been done. `USING TIMESTAMP` cannot be combined with an LWT (`IF EXISTS`/`IF NOT EXISTS`) — the server rejects a custom timestamp on a conditional statement.
  - **An encrypted create MUST NOT pin, because its bytes differ on every attempt.** `atrest.Encrypt` draws a fresh random nonce per call, so a redelivery of the same message produces a different `enc_payload` *and* a different `enc_meta`. Those are separate cells, and Cassandra breaks a same-timestamp conflict per cell by comparing values — independently. Two attempts pinned to one timestamp can therefore leave the ciphertext from one paired with the nonce from the other, and AES-GCM cannot open that: the row is undecryptable permanently. Letting encrypted creates ride the client clock keeps each redelivery strictly newer, so one attempt wins both cells and the pair stays coherent. The cost is that an encrypted create redelivery can still revert an edit made in between — a recoverable content loss, traded against an unrecoverable one. Closing that properly needs the ciphertext and nonce in a single reconciled cell (or a deterministic encrypted bundle), not a pin. `message-worker/store_cassandra_writetime_test.go` pins both halves of this rule.
  - **A tombstone that must clear an older row cannot be pinned.** The encrypted create paths clear the plaintext body columns (`msg`/`attachments`/`card`/`card_action`) of any pre-at-rest row at the same key, or a leftover plaintext value sits beside the new `enc_payload` and `ApplyDecryptedFields` overwrites it with an empty field on read. A legacy row was written at execution time — *after* `CreatedAt` — so a pinned clear lands before it and clears nothing. Those clears therefore ride the client clock as separate `UPDATE`s (`stripLegacyPlaintext*`) beside the (also unpinned) encrypted INSERT, never as NULLs bound into it — keeping the requirement local, so the clear cannot be silently re-pinned along with the INSERT. Mixed timestamps within one batch are legal as long as no batch-level timestamp is set. Re-running a strip is harmless: an encrypted edit already nulls the same four columns. **`bot-message-worker` is not yet migrated**: its ten create INSERTs still take the client clock. Its exposure is much smaller — the repo-default `MaxDeliver=6` (~2.6 min, no outage retry budget) and no failure point after the Cassandra commit in its handler — but the race is the same and it should be pinned when that service is next touched.

### HTTP (Gin + Resty)
- Use Gin for all HTTP servers — never `net/http` mux directly
- Register routes in `routes.go`, not `main.go`
- Validate request bodies at handler level using Gin binding/validation
- Every HTTP service exposes `GET /healthz`
- Use Resty for all outbound HTTP calls — never `net/http` client directly
- Always set timeouts on both Gin server and Resty client

### Configuration
- All config from environment variables — no config files
- Use `caarlos0/env` to parse into a typed `Config` struct — never use `os.Getenv` directly in service code
- `SCREAMING_SNAKE_CASE` for env var names; prefix with service name for service-specific vars
- Fail fast on missing required config — log error and exit with non-zero code
- Always provide `envDefault` for non-critical config (port, database name, log level); never default secrets or connection strings — mark them `required`
- **A knob shared by more than one service is declared once, in the package that owns the thing it configures, and mounted as a named field.** `mongoutil.PoolConfig` / `mongoutil.BreakerConfig`, `valkeyutil.Config`, and each L2 tier's `TTLConfig` (`roommetacache`, `userstore`, `roomsubcache`, `atrest`, `subauthcache`, `sessioncache`, `roomtimescache`). Never re-declare the env tag and `envDefault` in a service — that is how two services reading the same Valkey key end up on different TTLs, which the tag-level default cannot prevent. A service that needs its own env prefix puts `envPrefix` on the field; the tags carry the full operator-facing name, so `Breaker mongoutil.BreakerConfig` with `envPrefix:"HISTORY_"` reads `HISTORY_MONGO_BREAKER_FAILS`.

### Docker
- Multi-stage Dockerfiles: `golang:1.25.13-alpine` builder, `alpine:3.21` runtime
- Location: `<service>/deploy/Dockerfile`
- Build context: repo root so `pkg/` and `go.mod` are accessible
- Docker Compose for local dev only — include only the dependencies the service needs
- Always enable JetStream (`--jetstream`) and HTTP monitoring (`--http_port 8222`) for NATS
- Each service also has `<service>/deploy/azure-pipelines.yml` for CI/CD

### JetStream Consumer Pattern
- Choose the pattern based on the service's throughput needs:
  - **High-throughput** (`cons.Messages()` + semaphore): Pull iterator with a channel-based semaphore (`chan struct{}`) sized by `cfg.MaxWorkers` (from `MAX_WORKERS` env var, default `100`), `PullMaxMessages(2 * cfg.MaxWorkers)`, and `sync.WaitGroup` to track in-flight goroutines
  - **Sequential** (`cons.Consume()`): Callback-based sequential processing for lower-volume streams where concurrency is unnecessary
- Match the pattern already used by the service being modified — don't mix patterns within a single consumer
- Follow existing worker services (`message-worker`, `broadcast-worker`, etc.) as reference implementations

### JetStream Redelivery Backoff

Two levers space redeliveries, and they fire on **disjoint** failure modes. Set both.

- **Consumer `BackOff`** (server-side, `pkg/stream.ConsumerSettings`) fires only when a
  message goes un-acked past `AckWait` — pod crash, OOM, hang, or a handler slower than
  `AckWait`. Derived as `AckWait * BackOffFactor^i` capped at `BackOffMax`; defaults give
  `{30s, 1m, 2m, 4m, 8m}` over `MaxDeliver=6`. `CONSUMER_BACKOFF_STEPS=0` disables it.
- **`pkg/jsretry`** (client-side `NakWithDelay`) fires when a handler catches a transient
  error. `DefaultBackoff` for non-latency-sensitive work, `LowLatencyBackoff` for
  user-visible fan-out. Equal-jittered; the server-side lever cannot jitter.

Three server rules the code must respect (`nats-io/nats-server`, `server/consumer.go`):

- **A bare `Nak()` ignores `BackOff` entirely** — it goes straight on the redeliver queue
  (`:3308-3311`), so a sub-second blip burns `MaxDeliver` in milliseconds. `NakWithDelay(0)`
  is the same thing, because nats.go only serializes a delay when it is `> 0`. **Never call
  either** — use `jsretry.Settle`, `jsretry.SettleQuiet`, or `jsretry.Nak`.
- **`BackOff[0]` overwrites `AckWait`** (`:677-682`). Never hardcode a `cc.BackOff` in a
  service; let `stream.DurableConsumerDefaults` derive it so the two cannot disagree.
- **`len(BackOff) > MaxDeliver` is a hard create/update error** (`:807`, `:2588`), except
  when MaxDeliver means unlimited — the server normalizes `0` and `< -1` to `-1` first
  (`:612-617`). `DurableConsumerDefaults` clamps and warns.

### Graceful Shutdown
- Use `pkg/shutdown.Wait` in every service's `main.go`
- JetStream workers cleanup order: `iter.Stop()` → `wg.Wait()` (with timeout) → `nc.Drain()` → disconnect databases
- HTTP services cleanup order: `nc.Drain()` → disconnect databases
- Shutdown timeout (25s) must be less than Kubernetes `terminationGracePeriodSeconds` (30s)
