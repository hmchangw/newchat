# Page-Size Budget Trimming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Trim paginated client replies to fit the broker's `max_payload` and set the "more" flag, so the frontend receives a smaller page instead of `response_too_large`.

**Architecture:** A new `pkg/pagefit` owns byte arithmetic only (`Budget`, `Prefix`, `Window`). Three handlers whose next-page position is derived from returned content call it after assembling their page, then set their more-flag. A single row that alone exceeds the budget is returned blanked with `truncated:true` so pagination always advances.

**Tech Stack:** Go 1.25, `encoding/json`, `go.uber.org/mock`, testify, testcontainers-go, NATS.

**Spec:** `docs/superpowers/specs/2026-08-22-page-size-budget-trimming-design.md`

## Global Constraints

- Package names: short, lowercase, single word. NEVER `utils`/`helpers`/`common`/`base`.
- Wrap errors `fmt.Errorf("short description: %w", err)`; never bare `err`.
- Client-facing errors via `pkg/errcode`; infra failures returned raw.
- All new code TDD: Red (watch it fail) -> Green -> Refactor -> Commit.
- Minimum 80% coverage; target 90%+ for `pkg/`.
- `make fmt` / `make lint` / `make test` / `make sast` all clean before push.
- Integration tests: `//go:build integration`, containers from `pkg/testutil`, `TestMain` calls `testutil.RunTests(m)`.
- Comments: short and neat, max 2 lines, explain WHY not WHAT.
- A change to a client-facing struct in `pkg/model/` MUST update `docs/client-api.md` plus `docs/client-api/request-reply.md` and `docs/client-api/events.md` in the same PR.

---

### Task 1: `pkg/pagefit` budget arithmetic

**Files:**
- Create: `pkg/pagefit/pagefit.go`
- Test: `pkg/pagefit/pagefit_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `pagefit.Budget`, `pagefit.NewBudget(brokerMaxPayload int64, reserve int) Budget`, `(Budget).Enabled() bool`, `pagefit.Prefix[T any](items []T, b Budget, envelope int) int`, `pagefit.Window[T any](items []T, pivot int, b Budget, envelope int) (int, int)`.

- [ ] **Step 1: Write the failing tests** covering: disabled budget returns len(items); empty slice returns 0; exact fit keeps all; one byte over drops the last; every row oversize returns 1; window grows symmetrically around pivot; pivot at index 0 and at len-1.
- [ ] **Step 2: Run `make test SERVICE=pkg/pagefit`** — expect build failure (package does not exist).
- [ ] **Step 3: Implement** `Budget` (unexported `max int`), `NewBudget` (non-positive broker cap or reserve >= cap disables), `Prefix` (marshal each item once, prefix-sum with `,` separators, min 1 when non-empty), `Window` (expand alternately after/before pivot while it fits).
- [ ] **Step 4: Run `make test SERVICE=pkg/pagefit`** — expect PASS.
- [ ] **Step 5: Add `BenchmarkPrefix_100Rows`** to pin the marshal-pass cost.
- [ ] **Step 6: Commit** `feat(pagefit): byte-budget page trimming primitives`.

---

### Task 2: `truncated` flag on the message model

**Files:**
- Modify: `pkg/model/cassandra/message.go`
- Test: `pkg/model/model_test.go`

**Interfaces:**
- Produces: `cassandra.Message.Truncated bool` with `json:"truncated,omitempty" cql:"-"`.

- [ ] **Step 1: Write the failing test** asserting a `Message` with `Truncated: true` round-trips and that the field is omitted when false.
- [ ] **Step 2: Run `make test SERVICE=pkg/model`** — expect FAIL (unknown field).
- [ ] **Step 3: Add the field.** `cql:"-"` because it is computed at read time, never a column. Comment says why.
- [ ] **Step 4: Run `make test SERVICE=pkg/model`** — expect PASS.
- [ ] **Step 5: Commit** `feat(model): truncated flag for oversize message rows`.

---

### Task 3: history-service blanking helper + Load History trim

**Files:**
- Modify: `history-service/internal/service/utils.go` (add `blankOversize`)
- Modify: `history-service/internal/service/messages.go` (`LoadHistory`)
- Modify: `history-service/internal/service/service.go` (field + `WithPageBudget` Option)
- Test: `history-service/internal/service/messages_test.go`

**Interfaces:**
- Consumes: `pagefit.Prefix`, `pagefit.Budget`.
- Produces: `service.WithPageBudget(b pagefit.Budget) Option`; `blankOversize(m *models.Message)`.

- [ ] **Step 1: Write the failing tests.** (a) oversize page trims and `hasNext` becomes true; (b) a page that fits is byte-identical to today; (c) single oversize row returns exactly one row with `Truncated:true`, `Msg`/`Mentions`/`Attachments`/`DecodedAttachments`/`Card`/`CardAction`/`QuotedParentMessage`/`Reactions`/`SysMsgData` cleared and `MessageID`/`Sender`/`CreatedAt`/`Type` retained; (d) **continuity** — trim page 1, re-issue with `before` = oldest kept `createdAt`, assert page 2 starts at the first dropped row and no row repeats.
- [ ] **Step 2: Run `make test SERVICE=history-service`** — expect FAIL.
- [ ] **Step 3: Implement.** Trim after `redactUnavailableQuotes` + `setDecodedAttachments` (both change size). Preserve the existing invariant: never `hasNext` on an empty page.
- [ ] **Step 4: Run `make test SERVICE=history-service`** — expect PASS.
- [ ] **Step 5: Commit** `feat(history-service): trim Load History pages to the payload budget`.

---

### Task 4: Load Surrounding window trim

**Files:**
- Modify: `history-service/internal/service/messages.go` (`LoadSurroundingMessages`)
- Test: `history-service/internal/service/messages_test.go`

- [ ] **Step 1: Write the failing tests.** Oversize window trims both ends, keeps the central message, and sets `moreBefore`/`moreAfter` for whichever end lost rows; `timestamp` mode (no central) trims around the pivot index; a window that fits is unchanged.
- [ ] **Step 2: Run `make test SERVICE=history-service`** — expect FAIL.
- [ ] **Step 3: Implement** using `pagefit.Window` with pivot = index of the central message (or the insertion index in timestamp mode). OR the existing `HasNext` flags with "rows were dropped at this end".
- [ ] **Step 4: Run `make test SERVICE=history-service`** — expect PASS.
- [ ] **Step 5: Commit** `feat(history-service): trim Load Surrounding windows to the payload budget`.

---

### Task 5: Thread List trim (after enrichment)

**Files:**
- Modify: `user-service/service/threads.go` (`ListUserThreads`)
- Modify: `user-service/service/service.go` (field + `opts ...Option` + `WithPageBudget`)
- Test: `user-service/service/threads_test.go`

**Interfaces:**
- Produces: `service.Option`, `service.WithPageBudget(b pagefit.Budget) Option`.

- [ ] **Step 1: Write the failing tests.** (a) oversize merged page trims and `hasNext` is true; (b) **`NextCursor` is re-derived from the last kept item, not the pre-trim last item**; (c) the trim runs AFTER `enrichThreadPage` — seed rows that only exceed the budget once enriched, and assert the response still fits; (d) a page that fits is unchanged.
- [ ] **Step 2: Run `make test SERVICE=user-service`** — expect FAIL.
- [ ] **Step 3: Implement.** Move the byte-trim below `enrichThreadPage`, then derive `NextCursor` from `merged[len(merged)-1]`. Comment states why order matters.
- [ ] **Step 4: Run `make test SERVICE=user-service`** — expect PASS.
- [ ] **Step 5: Commit** `feat(user-service): trim Thread List pages to the payload budget`.

---

### Task 6: Config and startup wiring

**Files:**
- Modify: `history-service/internal/config/config.go`, `history-service/cmd/main.go`
- Modify: `user-service/config/config.go` (or equivalent), `user-service/main.go`
- Test: config unit tests in each service

**Interfaces:**
- Produces: `MaxResponseBytes int64` env `MAX_RESPONSE_BYTES` `envDefault:"0"` (0 = derive from broker).

- [ ] **Step 1: Write the failing test** asserting the resolved budget prefers a positive `MAX_RESPONSE_BYTES` and otherwise falls back to the broker cap, and that a non-positive result disables trimming.
- [ ] **Step 2: Run the service tests** — expect FAIL.
- [ ] **Step 3: Implement** the resolve helper and pass `pagefit.NewBudget(...)` via `WithPageBudget` in each `main.go`, reading `nc.NatsConn().MaxPayload()` as `room-service/main.go:318` does.
- [ ] **Step 4: Run the service tests** — expect PASS.
- [ ] **Step 5: Add `MAX_RESPONSE_BYTES` to both services' `deploy/docker-compose.yml`.**
- [ ] **Step 6: Commit** `feat(config): MAX_RESPONSE_BYTES page budget for history and user services`.

---

### Task 7: Client API documentation

**Files:**
- Modify: `docs/client-api.md`, `docs/client-api/request-reply.md`, `docs/client-api/events.md`

- [ ] **Step 1: Add `truncated`** to the Message field table with its type and meaning, plus a JSON example showing a truncated row.
- [ ] **Step 2: Document the short-page rule** — `limit` is a maximum, never a guarantee; the more-flag is authoritative; page until it clears.
- [ ] **Step 3: Note which RPCs trim** (Load History, Load Surrounding, Thread List) and which still return `response_too_large`.
- [ ] **Step 4: Mirror the changes** into both derived views.
- [ ] **Step 5: Commit** `docs(client-api): truncated rows and the short-page pagination rule`.

---

### Task 8: End-to-end integration test

**Files:**
- Create: `history-service/internal/service/pagefit_integration_test.go`

- [ ] **Step 1: Write the failing test.** Real NATS at a low `max_payload` plus real Cassandra/Mongo from `pkg/testutil`; seed a room with rows that overflow one page; walk pages via `before` until `hasNext` clears; assert the union equals the seeded set with every ID appearing exactly once, and that no reply is a `response_too_large` envelope.
- [ ] **Step 2: Run `make test-integration SERVICE=history-service`** — expect FAIL.
- [ ] **Step 3: Fix whatever the end-to-end run exposes.**
- [ ] **Step 4: Run `make test-integration SERVICE=history-service`** — expect PASS.
- [ ] **Step 5: Commit** `test(history-service): end-to-end pagination continuity across trimmed pages`.

---

## Self-Review

**Spec coverage:** `pkg/pagefit` -> T1. `truncated` field -> T2. Load History -> T3. Load Surrounding -> T4. Thread List incl. enrichment ordering -> T5. Budget derivation/config -> T6. Client-API contract -> T7. Continuity + integration -> T3 step 1(d), T8. Non-goals need no task.

**Placeholders:** none — every step names its files, its assertion, and its command.

**Type consistency:** `pagefit.Budget`, `Prefix`, `Window`, `WithPageBudget`, `blankOversize`, `Message.Truncated` are spelled identically in every task that references them.
