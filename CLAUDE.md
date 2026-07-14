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
| Observability | OpenTelemetry (tracing), Prometheus (metrics), `log/slog` (logging) |
| Testing | `go.uber.org/mock` (mockgen), `stretchr/testify` (assertions), `testcontainers-go` (integration) |
| Containers | Docker multi-stage builds, Docker Compose |

**Event flow:** User publishes message to MESSAGES stream → `message-gatekeeper` validates and publishes to MESSAGES_CANONICAL → `message-worker` persists to Cassandra, `broadcast-worker` delivers to room members, `notification-worker` sends notifications → cross-site events are published directly into remote sites' INBOX streams.

**Multi-site federation:** Each site runs independently with its own NATS, MongoDB, and Cassandra. Cross-site events cross the NATS supercluster as a direct JetStream publish into the destination site's INBOX stream (`chat.inbox.{destSiteID}.external.{eventType}`, no sourcing/SubjectTransform). Origin-side, `room-service`'s request/reply federation and `room-worker`'s order-sensitive events (`member_added`/`member_removed`/`room_renamed`) are buffered through a local per-site `OUTBOX` stream that `outbox-worker` drains and forwards, so a failed cross-gateway publish is durably retried rather than lost. Both lanes are per remote peer (from `ALL_SITE_IDS`) so a down peer's parked forwards (`MaxDeliver=-1`, never Ack) fill only their own consumer's ack-pending budget instead of stalling delivery to healthy peers: the order-sensitive events ride per-destination FIFO lanes (`MaxAckPending=1`, so they can't overtake each other — e.g. a rename can't overtake the add that creates the subscription it renames; one in-flight probe per down peer); the order-insensitive subscription-state events ride a per-destination concurrent consumer (default budget). Other consumer-originated events (messages) publish to the remote INBOX directly. User subscriptions and room metadata are scoped by `siteID`.

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

All services follow this layout, including `message-gatekeeper` (validates messages and publishes to MESSAGES_CANONICAL).

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
| `make sast` | Run all SAST scans (`gosec`, `govulncheck`, `semgrep`); fails on medium+ |
| `make sast-gosec` / `make sast-vuln` / `make sast-semgrep` | Run a single SAST scan |

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
- Inline `testcontainers.GenericContainer` is only acceptable when a shared testutil container can't accommodate the test (e.g. search-service CCS needs two ES nodes on a shared docker network; `pkg/roomkeysender` needs NATS with WebSocket transport; `pkg/roomcrypto` needs a Node container with bundled scripts). Each inline container must store its reference and register `t.Cleanup(container.Terminate)`.
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
- All NATS payloads are JSON with typed structs from `pkg/model`, never `map[string]interface{}`. Codec: the message hot-path workers (`broadcast-worker`, `message-worker`, `notification-worker`, `message-gatekeeper`) marshal/unmarshal via `github.com/bytedance/sonic` (default config) for throughput, warmed at startup with `pkg/jsonwarm.Pretouch`; everywhere else uses `encoding/json`. sonic's default output is semantically equivalent but not byte-identical to stdlib (HTML metacharacters left unescaped, map keys unsorted), so only adopt it on a path after confirming no consumer relies on byte-identity (payload hashing, signatures, dedup keys) or marshals `map` fields — see the sonic wire-compat tests in `broadcast-worker`/`message-gatekeeper`. One exception: `message-gatekeeper/fetcher_history.go` decodes a narrow projection rather than the full `cassandra.Message`, because that type embeds the marshal-only struct-keyed `Reactions` map whose decoder sonic rejects.
- Use NATS request/reply for synchronous operations; `nc.QueueSubscribe` with service name as queue group
- Use `natsutil.ReplyJSON` for success responses; for errors return a typed `*errcode.Error` from the handler and let `errnats.Reply` / `errhttp.Write` marshal the envelope (see `docs/error-handling.md`).
- Define all stream configs in `pkg/stream/stream.go` with name pattern `<STREAM>_<siteID>`
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
- MESSAGES_CANONICAL: `chat.msg.canonical.{siteID}.created` (`.edited`, `.deleted` for future)
- Inbox (cross-site, remote-origin): `chat.inbox.{destSiteID}.external.{eventType}` — published directly into the destination site's INBOX
- Inbox (same-site search feed): `chat.inbox.{siteID}.internal.{eventType}`
- Outbox (origin-side federation buffer): `chat.outbox.{siteID}.{destSiteID}.{eventType}` — `room-service` (subscription-state events) and `room-worker` (membership events) publish one event per destination; `outbox-worker` forwards each to the destination INBOX (destination scoped so the per-peer membership FIFO consumers can filter on one site)
- Wildcards: `*` for single-token, `>` for multi-token tail — define patterns in `pkg/subject`

### JetStream Streams
- `MESSAGES_{siteID}` — User message submissions
- `MESSAGES_CANONICAL_{siteID}` — Validated messages (single source of truth for downstream workers)
- `ROOMS_{siteID}` — Member invite requests
- `INBOX_{siteID}` — Cross-site federation events, published directly by remote sites onto the `external.>` lane (no sourcing/SubjectTransform); same-site services also publish a search-only feed onto the `internal.>` lane
- `OUTBOX_{siteID}` — Origin-side federation buffer: `room-service` publishes an `OutboxEvent` here for its request/reply cross-site events, `room-worker` for its order-sensitive events (membership + `room_renamed`); `outbox-worker` consumes it and forwards each event to the destination's INBOX with at-least-once retry — per remote peer (from `ALL_SITE_IDS`, `MaxDeliver=-1`), a concurrent consumer for the order-insensitive subscription-state event types plus a FIFO consumer (`MaxAckPending=1`) for the order-sensitive types (`member_added`/`member_removed`/`room_renamed`, which share one lane so they can't overtake each other); per-destination (not one shared consumer) so a down peer's parked forwards fill only its own ack-pending budget instead of stalling healthy peers. The two filter sets partition the stream and live in `pkg/outbox` (`ConcurrentEventTypes` / `OrderedEventTypes`); a new OUTBOX event type MUST be added to exactly one of them — producers publish via `outbox.Publish`, which rejects types outside the partition instead of letting them sit in the stream unconsumed. Owned by `outbox-worker`
- **Stream bootstrap is opt-in.** Services that consume from or publish to a stream MUST NOT create it in production — streams are owned by ops/IaC. Each such service's `config` includes `Bootstrap bootstrapConfig` (env prefix `BOOTSTRAP_`) with a single `Enabled` field tagged `env:"STREAMS" envDefault:"false"`. The service's `bootstrap.go` defines a `bootstrapStreams(ctx, js, siteID, enabled) error` helper that no-ops when `Enabled=false`. Local `deploy/docker-compose.yml` sets `BOOTSTRAP_STREAMS=true` so any service can stand up against a fresh NATS in dev. New services that interact with JetStream MUST follow this convention.
- **Stream bootstrap ownership.** When a service does bootstrap a stream in dev, the helper sets ONLY the stream's schema — `Name + Subjects` from `pkg/stream.<Stream>(siteID)`. Cross-site federation is direct-publish: a service at the origin site JetStream-publishes to the destination's `chat.inbox.{destSiteID}.external.>` lane, routed by the NATS supercluster/gateway topology (an ops/IaC concern that MUST NOT appear in any service's `bootstrap.go`). INBOX has a single owning service (`inbox-worker`) and OUTBOX has a single owning service (`outbox-worker`). Other services that consume from or publish to a remote INBOX (e.g., `search-sync-worker`, and the cross-site publishers room-worker/message-worker/user-service, plus `outbox-worker` which forwards the federated events) rely on `inbox-worker` to create the local stream and on ops/IaC for the routing that makes a remote publish land. `room-service` no longer publishes cross-site directly, and `room-worker` no longer publishes its order-sensitive events (membership, `room_renamed`) cross-site directly — both publish an `OutboxEvent` to the local OUTBOX and `outbox-worker` does the forwarding.

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
- Check `mongo.ErrNoDocuments` explicitly when a missing record is expected
- Create indexes in the store constructor or a dedicated `EnsureIndexes` method at startup
- **No `$lookup`**: server-side joins (`$lookup` in aggregation pipelines) are forbidden unless there is a very good, documented reason — prefer separate queries or denormalized data, and justify any exception in the PR. Pre-existing `$lookup` sites are grandfathered; when you touch one, add an inline `// $lookup justification: …` comment explaining why a join is unavoidable
- **Always project precisely**: every find/aggregation MUST specify an explicit projection that selects only the fields the caller needs — never fetch whole documents when a subset suffices

### Cassandra
- Driver: `github.com/gocql/gocql`
- Use `cassutil.Connect` from `pkg/cassutil` — `LocalQuorum` consistency, 10-second timeout
- Cassandra is ONLY for message history (time-series) — MongoDB handles everything else
- Design tables around query patterns (partition key = room ID, clustering key = timestamp), no secondary indexes
- `docs/cassandra_message_model.md` is the single source of truth for the message schema. Any PR that touches it MUST, in the same PR, update both downstream mirrors:
  1. The Go UDT/row structs in `pkg/model/cassandra/` (keep `cql:"…"` tags aligned with the columns).
  2. The init DDL under `docker-local/cassandra/init/*.cql` that creates the types and tables.
- **Bucketed message table.** `messages_by_room` uses a composite partition key `(room_id, bucket)`. The bucket is `floor(created_at_unix_ms / windowMs) * windowMs`. The window is configured per service via `MESSAGE_BUCKET_HOURS` (default 72). All services that read or write this table MUST be configured with the same `MESSAGE_BUCKET_HOURS`; mismatches will cause writes and reads to target different partitions and silently lose data. Bucket math lives in `pkg/msgbucket`.
- **Thread reply table.** `thread_messages_by_thread` is partitioned by `thread_room_id` alone — one partition per thread. Reads slice the partition by `created_at` clustering, no bucket walk required.

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

### Docker
- Multi-stage Dockerfiles: `golang:1.25.12-alpine` builder, `alpine:3.21` runtime
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

### Graceful Shutdown
- Use `pkg/shutdown.Wait` in every service's `main.go`
- JetStream workers cleanup order: `iter.Stop()` → `wg.Wait()` (with timeout) → `nc.Drain()` → disconnect databases
- HTTP services cleanup order: `nc.Drain()` → disconnect databases
- Shutdown timeout (25s) must be less than Kubernetes `terminationGracePeriodSeconds` (30s)
