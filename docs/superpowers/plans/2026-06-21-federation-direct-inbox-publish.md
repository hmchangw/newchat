# Federation: Direct Inbox Publish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the OUTBOX JetStream stream and `outbox.*` subjects; publish cross-site federation events directly into the destination site's INBOX via a JetStream publish to `chat.inbox.{destSiteID}.external.{eventType}`.

**Architecture:** Two explicit INBOX lanes replace the old local/`aggregate` split — `external.{eventType}` (remote-origin, applied to the DB by inbox-worker) and `internal.{eventType}` (same-site search feed, consumed only by search-sync-worker). The origin site issues a JetStream publish straight onto the destination's `external` lane; the NATS supercluster routes it to the destination's INBOX. There is no OUTBOX, no sourcing, no SubjectTransform.

**Tech Stack:** Go 1.25, NATS JetStream (`nats.go/jetstream`, `oteljetstream`), MongoDB, `go.uber.org/mock`, `testify`, testcontainers.

## Global Constraints

- Use `make` targets only — `make test`, `make lint`, `make test-integration`, `make generate`. Never raw `go`.
- TDD: update/extend tests first (Red), then implement (Green). 80% coverage floor.
- All NATS payloads JSON via typed structs in `pkg/model`; subjects via `pkg/subject` builders, never raw `fmt.Sprintf` at call sites.
- Cross-site federation routing (NATS supercluster/gateway) is ops/IaC — never authored in any service's `bootstrap.go`.
- `make fmt` after wide renames (struct-tag/field alignment shifts when identifier lengths change).
- Spec: `docs/superpowers/specs/2026-06-21-federation-direct-inbox-publish-design.md`.

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `pkg/subject/subject.go` | Subject builders | Remove `Outbox`/`OutboxWildcard`/`*Aggregate`; add `InboxExternal`/`InboxInternal`/`InboxExternalAll`; rewrite `InboxMemberEventSubjects` |
| `pkg/stream/stream.go` | Stream schemas | Remove `Outbox`; `Inbox` → `internal.>` + `external.>` |
| `pkg/model/event.go` | Event types | `OutboxEvent`→`InboxEvent`, constants + payload types renamed |
| `pkg/natsutil/request_id.go` | Dedup helper | `OutboxDedupID`→`InboxDedupID` |
| `room-worker/handler.go` | member/room_renamed publishes | internal (same-site) + external (cross-site) |
| `room-service/handler.go` | role/read/mute/favorite/restricted | `InboxExternal(destSiteID, …)` |
| `message-worker/handler.go` | thread_subscription_upserted | `InboxExternal(ownerSiteID, …)` |
| `user-service/publisher/publisher.go` + `main.go` + `service/status.go` | status fan-out | JetStream publish + `InboxEvent` wrapper |
| `inbox-worker/main.go`, `consumer_config*`, `bootstrap.go` | consumer | filter `external.>`; membership lane on external member subjects |
| `search-sync-worker/inbox_stream.go` | INBOX consumer | both lanes via `InboxMemberEventSubjects` |
| docs | `nats-subject-naming.md`, `CLAUDE.md`, `tools/loadgen/README.md` | reflect direct-publish model |

---

## Task 1: pkg/subject + pkg/stream lane primitives

**Files:**
- Modify: `pkg/subject/subject.go`, `pkg/subject/subject_test.go`
- Modify: `pkg/stream/stream.go`, `pkg/stream/stream_test.go`

**Interfaces produced:**
- `subject.InboxExternal(siteID, eventType string) string` → `chat.inbox.{siteID}.external.{eventType}`
- `subject.InboxInternal(siteID, eventType string) string` → `chat.inbox.{siteID}.internal.{eventType}`
- `subject.InboxExternalAll(siteID string) string` → `chat.inbox.{siteID}.external.>`
- `subject.InboxMemberEventSubjects(siteID string) []string` → internal+external member_added/removed
- `stream.Inbox(siteID)` subjects → `["chat.inbox.{siteID}.internal.>", "chat.inbox.{siteID}.external.>"]`; `stream.Outbox` removed

- [ ] **Step 1: Update the table tests (Red).** In `subject_test.go` replace the `Outbox`/`InboxMember*Aggregate` cases with `InboxExternal`/`InboxInternal`/`InboxExternalAll` and the new `InboxMemberEventSubjects` expectations (`internal.member_added`, `internal.member_removed`, `external.member_added`, `external.member_removed`). In `stream_test.go` drop the `Outbox` row and set `TestInboxConfig` to `internal.>` + `external.>`.
- [ ] **Step 2: Run, confirm fail.** `make test SERVICE=pkg/subject` and `make test SERVICE=pkg/stream` → FAIL (undefined builders).
- [ ] **Step 3: Implement (Green).** In `subject.go` delete `Outbox`, `OutboxWildcard`, `InboxMemberAdded`, `InboxMemberRemoved`, `InboxMemberAddedAggregate`, `InboxMemberRemovedAggregate`, `InboxAggregateAll`; add the four builders above. In `stream.go` delete `Outbox`; set `Inbox` subjects to the two lanes.
- [ ] **Step 4: Run.** Both pkg suites PASS.
- [ ] **Step 5: Commit.** `git commit -am "pkg: replace outbox/aggregate subjects with inbox internal/external lanes"`

## Task 2: pkg/model + pkg/natsutil renames

**Files:**
- Modify: `pkg/model/event.go`, `pkg/model/model_test.go`
- Modify: `pkg/natsutil/request_id.go`

**Interfaces produced:**
- `model.InboxEvent` (fields `Type, SiteID, DestSiteID, Payload, Timestamp` unchanged), `model.InboxEventType`
- Constants `model.InboxMemberAdded` … `model.InboxUserStatusUpdated`
- `model.RoomRenamedInboxPayload`, `model.RoomRestrictedInboxPayload`
- `natsutil.InboxDedupID(ctx, destSiteID, payloadSeed string) string`

- [ ] **Step 1: Rename in model_test.go (Red).** Point the roundtrip cases at `model.InboxEvent` / `RoomRenamed*InboxPayload`.
- [ ] **Step 2: Rename across model + natsutil.** Global token swaps: `OutboxEvent`→`InboxEvent` (covers `OutboxEventType`), each `Outbox<Const>`→`Inbox<Const>`, `RoomRenamedOutboxPayload`→`RoomRenamedInboxPayload`, `RoomRestrictedOutboxPayload`→`RoomRestrictedInboxPayload`, `OutboxDedupID`→`InboxDedupID`. Update doc comments mentioning "OutboxEvent".
- [ ] **Step 3: `make fmt`** (field alignment shifts).
- [ ] **Step 4: Run.** `make test SERVICE=pkg/model` PASS.
- [ ] **Step 5: Commit.** `git commit -am "pkg/model,natsutil: rename Outbox* to Inbox*"`

## Task 3: Repoint publishers (room-worker, room-service, message-worker)

**Files:**
- Modify: `room-worker/handler.go` (+ `handler_test.go`, `integration_test.go`, `main.go`)
- Modify: `room-service/handler.go` (+ `handler_test.go`)
- Modify: `message-worker/handler.go` (+ `handler_test.go`)

**Interfaces consumed:** Task 1 builders, Task 2 types.

- [ ] **Step 1: Update handler tests (Red).** Replace `subject.Outbox(...)` expectations with `subject.InboxExternal(dest, type)`; same-site member assertions with `subject.InboxInternal(siteID, type)`; any hardcoded `"outbox.site-a.to.site-b.X"` literals with `"chat.inbox.site-b.external.X"`; subject-matching helpers that filter on `"outbox"` substring → `.external.` / `subject.InboxExternal(...)`/`subject.InboxInternal(...)`.
- [ ] **Step 2: Run, confirm fail.** `make test SERVICE=room-worker` etc. → FAIL.
- [ ] **Step 3: Implement.** At each call site: cross-site `subject.Outbox(origin, dest, type)` → `subject.InboxExternal(dest, type)`; same-site `subject.InboxMemberAdded/Removed(site)` → `subject.InboxInternal(site, model.InboxMemberAdded/Removed)`. Wrapper structs are `model.InboxEvent`; dedup `natsutil.InboxDedupID`.
- [ ] **Step 4: Run.** Three service suites PASS.
- [ ] **Step 5: Commit.** `git commit -am "room-worker,room-service,message-worker: publish to inbox lanes directly"`

## Task 4: user-service status — JetStream publish + InboxEvent wrapper

**Files:**
- Modify: `user-service/publisher/publisher.go` (core NATS → JetStream)
- Modify: `user-service/main.go` (wire `oteljetstream.New(nc)`, `publisher.New(js)`)
- Modify: `user-service/service/status.go` (wrap payload in `model.InboxEvent`)
- Modify: `user-service/service/service.go` (interface doc), `status_test.go`, `publisher/publisher_integration_test.go`

**Interfaces consumed:** Task 1/2. **Why:** the old path published a *raw* `UserStatusUpdated` over core NATS; inbox-worker dispatches on `InboxEvent.Type`, so the wrapper is required for the event to be processed, and a JetStream publish is required for it to land in the remote INBOX across the supercluster.

- [ ] **Step 1: Update tests (Red).** `status_test.go` already asserts `subject.InboxExternal("site-b", model.InboxUserStatusUpdated)`; extend one case to decode the published bytes as `model.InboxEvent` and assert `Type == model.InboxUserStatusUpdated` and a non-empty `Payload`. Rewrite `publisher_integration_test.go` to dial JetStream, create a `TEST_PUBLISHER` stream, publish via `New(js).Publish`, and assert payload + `X-Request-ID` header; closed-conn error contains `"publish inbox event"`.
- [ ] **Step 2: Run, confirm fail.**
- [ ] **Step 3: Implement.** `publisher.Publisher{ js oteljetstream.JetStream }`, `New(js)`, `Publish` → `p.js.PublishMsg(ctx, natsutil.NewMsg(ctx, subject, data))` wrapped `"publish inbox event: %w"`. In `main.go` add `js, err := oteljetstream.New(nc)` and pass `publisher.New(js)`. In `status.go` build one `model.InboxEvent{Type: model.InboxUserStatusUpdated, SiteID: s.siteID, DestSiteID: dest, Payload: <UserStatusUpdated JSON>, Timestamp: now}` per dest and marshal that.
- [ ] **Step 4: Run.** `make test SERVICE=user-service` PASS.
- [ ] **Step 5: Commit.** `git commit -am "user-service: publish status as InboxEvent over JetStream"`

## Task 5: Consumers — inbox-worker external lane, search-sync-worker both lanes

**Files:**
- Modify: `inbox-worker/main.go` (`buildConsumerConfig` → `InboxExternalAll`; `isMembershipSubject` → external member subjects; comments), `bootstrap.go` (comment), `consumer_config_test.go`, `integration_test.go`
- Modify: `search-sync-worker/inbox_stream.go` (already uses `stream.Inbox` + `InboxMemberEventSubjects`), `spotlight_test.go`, `user_room_test.go`, `inbox_integration_test.go`

**Interfaces consumed:** Task 1/2.

- [ ] **Step 1: Update tests (Red).** `consumer_config_test.go`: import `model`; membership cases use `subject.InboxExternal(siteID, model.InboxMemberAdded/Removed)`; non-membership negatives use external subscription_read/thread_read; filter assertion `subject.InboxExternalAll(siteID)`. `spotlight_test.go`/`user_room_test.go`: StreamConfig `internal.>`+`external.>`, FilterSubjects the four internal/external member subjects. Integration tests: publish to `InboxInternal`/`InboxExternal`, assert inbox-worker filter sees only external (NumPending=1).
- [ ] **Step 2: Run, confirm fail.**
- [ ] **Step 3: Implement.** `cc.FilterSubjects = []string{subject.InboxExternalAll(siteID)}`; `isMembershipSubject` compares against `subject.InboxExternal(siteID, model.InboxMemberAdded/Removed)`. search-sync-worker needs no logic change (it derives from `stream.Inbox`/`InboxMemberEventSubjects`).
- [ ] **Step 4: Run.** `make test SERVICE=inbox-worker`, `make test SERVICE=search-sync-worker` PASS.
- [ ] **Step 5: Commit.** `git commit -am "inbox-worker,search-sync-worker: consume inbox internal/external lanes"`

## Task 6: Docs + cosmetic terminology sweep

**Files:**
- Modify: `docs/nats-subject-naming.md`, `CLAUDE.md`, `tools/loadgen/README.md`
- Modify: handler/test comments, local var names, error strings still saying "outbox"

- [ ] **Step 1: Docs.** Replace the OUTBOX stream section + subject-builder/stream-list rows with the internal/external direct-publish model; update CLAUDE.md event-flow, federation, subject-naming, and JetStream-streams bullets.
- [ ] **Step 2: Sweep.** Rename remaining cosmetic `outbox` local variables, error strings (`"outbox publish to %s"` → inbox/cross-site wording), and comments; update the few tests that assert on those error substrings.
- [ ] **Step 3: Verify.** `make lint` (0 issues) and `make test` (all PASS).
- [ ] **Step 4: Commit.** `git commit -am "docs+sweep: finish outbox→inbox terminology"`

## Task 7: Integration verification

- [ ] **Step 1:** Start Docker; `make test-integration` for the federation services (`inbox-worker`, `search-sync-worker`, `room-worker`, `room-service`, `message-worker`, `user-service`).
- [ ] **Step 2:** Confirm the inbox-worker filter-scoping test proves `internal.*` publishes are unreachable to inbox-worker while `external.*` are delivered.
- [ ] **Step 3:** Full `make test-integration` green.

---

## Self-Review

- **Spec coverage:** subject scheme (T1), stream config (T1), model/dedup renames (T2), publishers (T3), user-service core→JS + wrapper (T4), consumers (T5), docs+infra (T6), integration verify (T7). All spec sections mapped.
- **Placeholders:** none — every step names exact files/builders/commands.
- **Type consistency:** `InboxExternal(siteID, eventType)`, `InboxInternal(siteID, eventType)`, `InboxExternalAll(siteID)`, `model.InboxEvent`, `natsutil.InboxDedupID` used consistently across T1–T5.
