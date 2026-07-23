# Teams Message-History Migration — Implementation Plan

**Goal:** a durable consumer on the canonical stream (filtered to `.teams.batch`)
that transforms Teams-history messages and writes them directly to Cassandra via
`store.SaveMessage` — no re-publish, no reply, thread/mention/quote side-effects
dropped. Design:
`docs/superpowers/specs/2026-07-22-teams-message-migration-design.md`.

**Scope:** messages only. The forward branch is stubbed pending the `Forwarded` model
field; room + subscription creation is handled by the Teams onboarding pipeline (out of
scope here).

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

## Task 3: Sender resolution (process-wide cache)

- [x] `FindUserByEmployeeId` (employeeId is globally unique — no site term) + exactly-one `FindUserByDisplayName` reads (store + mongo + mock).
- [x] Resolver: employeeId read → else display-name reuse → else `UpsertUserIdentities` (create).
- [x] Resolver + transformer built once; a process-wide `hashicorp/golang-lru/v2` sender cache shared across all batches; mentions reuse the same resolver.

## Task 4: Deterministic message id

- [x] Stable hash of the Teams message id **alone** (globally unique) in valid `idgen` message-id format (idempotent re-run).

## Task 5: `message-worker` batch consumer

- [x] Add subject `MsgCanonicalTeamsBatch` to `pkg/subject` (+ test).
- [x] `TeamsBatchRequest` / `TeamsBatchResult` in `pkg/model`.
- [x] Consumer: decode batch → transform → set deterministic id → resolve sender → write **directly to Cassandra** via `store.SaveMessage` (no `MessageEvent` marshal / `processMessage`); Ack on success, Nak on an infra failure. Thread/mention/quote side-effects intentionally dropped.
- [x] Per-message error isolation (skip logs at `Warn`); wire a durable consumer + iterator shutdown into `message-worker` main.

## Task 6: Search indexing (reuse the message-sync consumer, no Mongo)

- [x] Extract the payload types + payload-only helpers to `pkg/teamsmigrate` (shared by the persist + index paths).
- [x] `EmployeeIDFromGraphID` derives a 17-char base62 id (native-user id shape) via `idgen.DeterministicID`; `message-worker` writes it as the migrated user's `_id`, so the persisted `UserID` equals the hash the indexer derives.
- [x] The existing `message-sync` (user) collection also binds `.teams.batch` (one consumer, not a separate durable); `BuildAction` detects the batch payload by shape and fans it out — author key = `EmployeeIDFromGraphID(from.id)`, same skips as the persist path, shares the message index. No separate `message-sync-teams` consumer.

## Task 7: Tests

- [x] Unit: transformer shapes, HTML→md, reaction map, sender matrix, deterministic id, per-message status + Nak-on-infra.
- [x] Unit: merged `message-sync` `BuildAction` — teams batch → derived-id/UserID index actions; empty-id / empty-roomId / system skipped; normal `MessageEvent` still handled.
- [x] Integration: batch → direct Cassandra write + idempotent re-run (single row).

## Task 8: Docs

- [x] This plan + the design spec (the batch subject is server-only, not in `client-api.md`).

## Deferred / follow-ups

- [ ] Forward branch → `Forwarded` snapshot (after the forward feature lands).
