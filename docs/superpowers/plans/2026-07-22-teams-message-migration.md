# Teams Message-History Migration — Implementation Plan (Phase 1)

**Goal:** a durable consumer on the canonical stream (filtered to `.teams.batch`)
that transforms Teams-history messages and feeds them through message-worker's own
persist pipeline with `isMigration=true` — no re-publish, no reply. Design:
`docs/superpowers/specs/2026-07-22-teams-message-migration-design.md`.

**Scope:** Phase 1 (messages) only. The forward branch is stubbed pending the
`Forwarded` model field; room + subscription replace is Phase 2.

---

## Task 1: Sender-resolution helpers (per-service)

- [x] `message-worker`-local `mongoHRIdentityStore` (employeeId-keyed reads + identity upsert).
- [x] `employeeIDFromGraphID` is a per-service copy in message-worker (the shared HR store was removed in a prior merge — no shared `pkg/hrstore`).

## Task 2: `MessageTransformer` seam + `DefaultTransformer`

- [x] Interface `Transform(ctx, raw json.RawMessage) (model.Message, error)`.
- [x] HTML→supported-markdown (unsupported markup → raw text); text bodies pass through.
- [x] Message type: user vs system (`Type` marker).
- [x] Reply(quote) shape via `QuotedParentMessage`; sender + mentions resolved.
- [x] Reaction→shortcode table (helper + test; not attached — no model slot).
- [x] Forward branch stubbed — no forward field set.
- [x] Table-tested; overridable via an injected transformer.

## Task 3: Sender resolution (per-batch cache)

- [x] `FindUserByEmployeeId` (employeeId is globally unique — no site term) + exactly-one `FindUserByDisplayName` reads (store + mongo + mock).
- [x] Resolver: employeeId read → else display-name reuse → else `UpsertUserIdentities` (create).
- [x] Per-batch cache; mentions reuse the same resolver.

## Task 4: Deterministic message id

- [x] Stable hash of the Teams id in valid `idgen` message-id format (idempotent re-run).

## Task 5: `message-worker` batch consumer

- [x] Add subject `MsgCanonicalTeamsBatch` to `pkg/subject` (+ test).
- [x] `TeamsBatchRequest` / `TeamsBatchResult` in `pkg/model`.
- [x] Consumer: decode batch → transform → set deterministic id → feed through `processMessage` (`isMigration=true`); Ack on success, Nak on an infra failure.
- [x] Per-message error isolation (logged); wire a durable consumer + iterator shutdown into `message-worker` main.

## Task 6: Tests

- [x] Unit: transformer shapes, HTML→md, reaction map, sender matrix, deterministic id, per-message status + Nak-on-infra.
- [x] Integration: batch → persist through the real pipeline + idempotent re-run (single row).

## Task 7: Docs

- [x] This plan + the design spec (the batch subject is server-only, not in `client-api.md`).

## Deferred (out of Phase 1)

- [ ] **Search indexing** of migrated messages — this path does not emit the `.created` canonical event search-sync keys on (see the design § Known limitation).
- [ ] Forward branch → `Forwarded` snapshot (after the forward feature lands).
- [ ] Phase 2: room + subscription replace RPC to `room-service`.
