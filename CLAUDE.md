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

**Event flow:** User publishes message to MESSAGES stream → `message-gatekeeper` validates and publishes to MESSAGES_CANONICAL → `message-worker` persists to Cassandra, `broadcast-worker` delivers to room members, `notification-worker` sends notifications → cross-site events flow via OUTBOX/INBOX streams.

**Multi-site federation:** Each site runs independently with its own NATS, MongoDB, and Cassandra. Cross-site events use the Outbox/Inbox pattern — local events go to the OUTBOX stream, remote sites source from it into their INBOX stream. User subscriptions and room metadata are scoped by `siteID`.

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
- Use `model.ErrorResponse` via `natsutil.ReplyError` for all NATS reply errors
- Never compare errors by string — use `errors.Is` and `errors.As`
- Never expose raw internal errors to clients — sanitize errors at service boundaries, return user-safe messages

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
- All integration tests use `//go:build integration` build tag
- Use `testcontainers-go` with official modules (`mongodb`, `cassandra`, `nats`) for real dependencies
- Write `setup<Dep>(t *testing.T)` helpers that start a container, register `t.Cleanup`, and return a connected client
- Use `<service>_test` as database name to avoid collisions

### Model Tests
- `pkg/model/model_test.go` verifies all domain types marshal/unmarshal correctly via a generic `roundTrip` helper

### General
- Run `make generate` before testing if store interfaces changed
- ALWAYS use the `-race` flag in testing — use `go test -race` to catch data races (the Makefile handles this)

## Section 5: Workflow Guardrails

### Before Committing
- Run `make generate` first if store interfaces were changed
- Lint and tests are enforced by a pre-commit hook — fix failures before retrying
- Never commit `.env` files
- Never merge code directly into `master` or `main` — always create a PR for review first
- If your changes touch a client-facing handler (any handler registered with `nc.QueueSubscribe` or `natsrouter.Register` whose subject begins with `chat.user.{account}.request.…` or `chat.user.{account}.room.{roomID}.{siteID}.msg.send`, or any HTTP route in `auth-service`), update `docs/client-api.md` in the same PR to reflect the new request/response schema, error cases, and triggered events.

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
- All NATS payloads are JSON — use `encoding/json` with typed structs from `pkg/model`, never `map[string]interface{}`
- Use NATS request/reply for synchronous operations; `nc.QueueSubscribe` with service name as queue group
- Use `natsutil.ReplyJSON` for success responses, `natsutil.ReplyError` for errors
- Define all stream configs in `pkg/stream/stream.go` with name pattern `<STREAM>_<siteID>`
- Use durable consumers named after the service
- Stream creation is gated by `BOOTSTRAP_STREAMS` (see below); when enabled, use `js.CreateOrUpdateStream` (it's idempotent) via the service's `bootstrapStreams` helper, never inline

### Event Timestamps
- Every NATS event struct in `pkg/model` must include a `Timestamp int64 \`json:"timestamp" bson:"timestamp"\`` field
- Set the timestamp at the publish site using `time.Now().UTC().UnixMilli()`
- This is the event-level timestamp (when the event was published), distinct from any domain-level timestamps in embedded structs (e.g., `Message.CreatedAt`)

### NATS Subject Naming
- Dot-delimited hierarchical subjects — use `pkg/subject` builders, never raw `fmt.Sprintf`
- User-scoped: `chat.user.{account}.…`
- Room-scoped: `chat.room.{roomID}.…`
- MESSAGES_CANONICAL: `chat.msg.canonical.{siteID}.created` (`.edited`, `.deleted` for future)
- Outbox: `outbox.{siteID}.to.{destSiteID}.{eventType}`
- Wildcards: `*` for single-token, `>` for multi-token tail — define patterns in `pkg/subject`

### JetStream Streams
- `MESSAGES_{siteID}` — User message submissions
- `MESSAGES_CANONICAL_{siteID}` — Validated messages (single source of truth for downstream workers)
- `ROOMS_{siteID}` — Member invite requests
- `OUTBOX_{siteID}` — Cross-site outbound events
- `INBOX_{siteID}` — Cross-site inbound events (sourced from remote OUTBOX)
- **Stream bootstrap is opt-in.** Services that consume from or publish to a stream MUST NOT create it in production — streams are owned by ops/IaC. Each such service's `config` includes `Bootstrap bootstrapConfig` (env prefix `BOOTSTRAP_`) with a single `Enabled` field tagged `env:"STREAMS" envDefault:"false"`. The service's `bootstrap.go` defines a `bootstrapStreams(ctx, js, siteID, enabled) error` helper that no-ops when `Enabled=false`. Local `deploy/docker-compose.yml` sets `BOOTSTRAP_STREAMS=true` so any service can stand up against a fresh NATS in dev. New services that interact with JetStream MUST follow this convention.
- **Stream bootstrap ownership.** When a service does bootstrap a stream in dev, the helper sets ONLY the stream's schema — `Name + Subjects` from `pkg/stream.<Stream>(siteID)`. Federation config (`Sources` + `SubjectTransforms` for cross-site sourcing) is owned by ops/IaC and MUST NOT appear in any service's `bootstrap.go`. INBOX has a single owning service (`inbox-worker`); other services that consume from INBOX (e.g., `search-sync-worker`) skip it in their bootstrap loop and rely on `inbox-worker` to create the stream.

### MongoDB
- Never use ORMs (no GORM, no ent) — use native drivers directly
- Driver: `go.mongodb.org/mongo-driver/v2`
- Use `mongoutil.Connect` from `pkg/mongoutil`
- Collections: lowercase plural of the domain entity (e.g., `rooms`, `subscriptions`, `messages`)
- Primary keys: application-generated via `pkg/idgen`, mapped to `bson:"_id"`. Format depends on the entity:
  - **Subscriptions, RoomMembers, ThreadRooms, ThreadSubscriptions**: UUIDv7 hex without hyphens (32 chars) via `idgen.GenerateUUIDv7()` — time-ordered for B-tree locality on high-write collections
  - **Channel Rooms**: 17-char base62 via `idgen.GenerateID()` — short, human-friendly
  - **DM Rooms**: sorted concat of two `user.ID` strings (~34 chars) via `idgen.BuildDMRoomID(a, b)` — deterministic, no separate dedup needed
  - **Messages**: 20-char base62 via `idgen.GenerateMessageID()` for new IDs (or client-supplied for user messages). `idgen.IsValidMessageID` accepts **either 17 or 20 char** base62 — 17 is the legacy length retained for backward compatibility with messages written before the 20-char cutover (federation replays, JetStream redeliveries, historical records).
- Check `mongo.ErrNoDocuments` explicitly when a missing record is expected
- Create indexes in the store constructor or a dedicated `EnsureIndexes` method at startup

### Cassandra
- Driver: `github.com/gocql/gocql`
- Use `cassutil.Connect` from `pkg/cassutil` — `LocalQuorum` consistency, 10-second timeout
- Cassandra is ONLY for message history (time-series) — MongoDB handles everything else
- Design tables around query patterns (partition key = room ID, clustering key = timestamp), no secondary indexes
- `docs/cassandra_message_model.md` is the single source of truth for the message schema. Any PR that touches it MUST, in the same PR, update both downstream mirrors:
  1. The Go UDT/row structs in `pkg/model/cassandra/` (keep `cql:"…"` tags aligned with the columns).
  2. The init DDL under `docker-local/cassandra/init/*.cql` that creates the types and tables.

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
- Multi-stage Dockerfiles: `golang:1.25.8-alpine` builder, `alpine:3.21` runtime
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
