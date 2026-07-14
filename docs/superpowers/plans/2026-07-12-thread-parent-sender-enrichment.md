# Thread-Parent Sender Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry the thread parent's **sender account** on the canonical `MessageEvent` (the gatekeeper already fetches it alongside `createdAt`), so the two consumers that need the parent author race-free — `broadcast-worker` and `notification-worker` — use the event-carried `createdAt` + sender account when both are present and fall back to the history-service `FetchParent` only when either is absent.

**Architecture:** message-gatekeeper's `FetchQuotedParent` already reads the parent's `Sender` in the same request it uses to resolve `createdAt`; it discarded the account. Carry it best-effort on the `MessageEvent` **envelope** (`ThreadParentSenderAccount`, server-only, never persisted, never client-facing — mirrors `QuotedParentUnverified`). broadcast-worker's `channelThreadFanOut` and notification-worker's thread branch prefer the event-carried pair and fetch only when either is missing (edit/delete canonical events bypass the gatekeeper, or a gatekeeper soft-fail). This removes the per-reply round-trip on the hot create path while keeping the parent author a race-free recipient.

**Tech Stack:** Go 1.25, NATS JetStream, sonic (hot-path JSON), gomock, testify.

**Spec:** `docs/superpowers/specs/2026-07-12-thread-parent-sender-enrichment-design.md`

## Global Constraints

- All commands via `make` targets — never raw `go` commands (`make test SERVICE=<name>`, `make lint`, `make sast`).
- TDD: write the failing test first, watch it fail, then implement.
- Never edit `mock_*_test.go` manually — regenerate with `make generate SERVICE=<name>`. (No store-interface changes here, so none needed.)
- Error wrapping: `fmt.Errorf("what this function was doing: %w", err)`; bare wrapped errors → NAK at the worker boundary.
- Coverage floor 80% per package (target 90%+ for handlers); every touched package must stay above it.
- `MessageEvent` is the internal canonical event, not a client-facing / server→client struct — no `docs/client-api.md` change. `SendMessageRequest` is unchanged.
- Fallback rule everywhere: use the event pair only when `ThreadParentMessageCreatedAt != nil && ThreadParentSenderAccount != ""`; otherwise fetch (both values come from the same fetch).

---

### Task 1: pkg/model — envelope field

**Files:**
- Modify: `pkg/model/event.go` (`MessageEvent` struct)
- Test: `pkg/model/model_test.go`

**Interfaces:**
- Produces: `model.MessageEvent.ThreadParentSenderAccount string` (`json:"...,omitempty" bson:"-"`), populated best-effort by the gatekeeper (Task 2), consumed by Tasks 3 and 4.

- [x] **Step 1 (Red):** Add `TestMessageEvent_ThreadParentSenderAccount_JSON` mirroring `TestMessageEvent_QuotedParentUnverified_JSON` — round-trips when set, omitted when empty, never BSON-encoded (`bson:"-"`).
- [x] **Step 2 (Green):** Add the field to `MessageEvent`.
- [x] **Step 3:** `make test SERVICE=pkg/model`.

---

### Task 2: message-gatekeeper — resolve + carry the sender account

**Files:**
- Modify: `message-gatekeeper/handler.go` (`resolveThreadParentCreatedAt`, `processMessage`)
- Test: `message-gatekeeper/handler_test.go`

**Interfaces:**
- Consumes: existing `ParentMessageFetcher.FetchQuotedParent(...) (*cassandra.QuotedParentMessage, error)` — its snapshot already carries `Sender cassandra.Participant`.
- Produces: `evt.ThreadParentSenderAccount` set best-effort from the resolved snapshot.

- [x] **Step 1 (Red):** Extend the thread-reply harness tests — resolved-via-fetch and reused-verified-quote-snapshot cases assert `evt.ThreadParentSenderAccount` equals the snapshot's `Sender.Account`; fetch-fail and nil-snapshot cases assert it is empty.
- [x] **Step 2 (Green):** Rename `resolveThreadParentCreatedAt` → `resolveThreadParent`, returning `(*time.Time, string)` (createdAt + sender account) from the reuse and fetch branches; `("" , nil)` on non-thread / soft-fail. Set `evt.ThreadParentSenderAccount` in `processMessage`.
- [x] **Step 3:** `make test SERVICE=message-gatekeeper`.

---

### Task 3: broadcast-worker — skip fetch when the event carries both

**Files:**
- Modify: `broadcast-worker/handler.go` (`channelThreadFanOut` + its three callers)
- Test: `broadcast-worker/handler_test.go`

**Interfaces:**
- `channelThreadFanOut` gains `eventParentCreatedAt *time.Time, eventParentSenderAccount string`; the three thread handlers (`handleThreadCreated/Updated/Deleted`) pass `msg.ThreadParentMessageCreatedAt` + `evt.ThreadParentSenderAccount`.

- [x] **Step 1 (Red):** `TestHandleThreadCreated_ChannelRoom_UsesEventParent_SkipsFetch` — event carries both; `MockParentFetcher` registers no EXPECT (any `FetchParent` call fails); assert the parent author (from event) is delivered and a follower who joined after the event createdAt is gated out. `TestHandleThreadCreated_ChannelRoom_MissingSenderAccount_FallsBackToFetch` — createdAt present, account empty → `FetchParent` called.
- [x] **Step 2 (Green):** In `channelThreadFanOut`, when both event values are present use them and skip `FetchParent`; else fetch (both values) as before. Wire the three callers.
- [x] **Step 3:** `make test SERVICE=broadcast-worker`. Update/delete paths (gatekeeper bypass) keep fetching via the existing tests — no regression.

---

### Task 4: notification-worker — skip fetch when the event carries both

**Files:**
- Modify: `notification-worker/handler.go` (`HandleMessage`, `isThreadOnlyReply` branch)
- Test: `notification-worker/handler_test.go`

**Interfaces:**
- In the thread branch, prefer `msg.ThreadParentMessageCreatedAt` + `evt.ThreadParentSenderAccount`; the `Followers.Lookup` query is unchanged (always needed).

- [x] **Step 1 (Red):** `TestHandle_ThreadOnlyReply_UsesEventParent_SkipsFetch` — event carries both; `failIfCalledParent` fails the test if `FetchParent` runs; assert parent author + unrestricted follower notified, restricted follower suppressed by the event createdAt. `TestHandle_ThreadOnlyReply_MissingSenderAccount_FallsBackToFetch` — account absent → fetch fallback notifies the parent author.
- [x] **Step 2 (Green):** Add the `if createdAt != nil && account != "" { use } else { FetchParent }` branch inside `isThreadOnlyReply`.
- [x] **Step 3:** `make test SERVICE=notification-worker`.

---

### Task 5: Verify

- [x] `make lint` — 0 issues.
- [x] `make test SERVICE=<name>` for pkg/model, message-gatekeeper, broadcast-worker, notification-worker, plus the createdAt-only consumers (message-worker, search-sync-worker) to confirm the model change is compatible.
- [x] Function-level coverage of touched code: `resolveThreadParent` 100%, `channelThreadFanOut` 88%, both handler entrypoints 95%+.
- [x] `make sast` — gosec / govulncheck / semgrep all PASS.
- [x] Commit + push to the feature branch.

## Not In Scope

- message-worker / search-sync-worker code (already event-first for createdAt; neither needs the parent account — verified compile/tests only).
- Re-adding any client `SendMessageRequest` field.
- Enriching edit/delete canonical events (they bypass the gatekeeper by design).
