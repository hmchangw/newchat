# oplog-connector deployment split — message vs collection roles (Design)

> **Status:** DESIGN. Branch: `claude/dreamy-clarke-prpl3`, off `main` (the connector is already merged). Splits the single `oplog-connector` deployment into two independent deployments of the **same binary** so a fault on the low-value collection side can no longer stall the high-volume message CDC path.

---

## 1. Problem

`oplog-connector` tails a source-Mongo change stream per collection in `WATCH_COLLECTIONS` (one watcher + one checkpoint each) and publishes `model.OplogEvent`s to `MIGRATION_OPLOG_{siteID}`. Today one deployment tails **everything** — the high-traffic `rocketchat_message` collection alongside low-value operational collections (avatars, tokens, devices, rooms, subscriptions…).

A single fatal watcher error tears down the whole process: `main.go` runs each watcher in a goroutine, and the first fatal error calls `cancel()`, which stops **every** watcher and exits non-zero (operator-driven recovery). So a lost resume token or change-stream fault on `ufsTokens` today **also stops message CDC**. Messages are the highest-volume, most latency-sensitive path; they must not share a failure domain with operational collections.

## 2. Goal

Run two independent deployments of the same image, each with its own process boundary (own change streams, own fatal-exit blast radius):

| Deployment | `WATCH_COLLECTIONS` | Watches messages? | Federation `$match` |
|---|---|---|---|
| `oplog-connector-messages` | `rocketchat_message` | yes | active |
| `oplog-connector-collections` | the other 14: rooms, subscriptions, room members, thread-subs, HR org, users + the 8 direct-transfer collections (authoritative list: `deploy/docker-compose.yml`) | no | inactive (n/a) |

The **combined single-deployment mode still works** (watch everything in one deployment) — the split is opt-in via config, so this change is backward compatible.

**Non-goals:** no change to event shape, subjects, checkpoint schema, downstream transformers, or the federation-filter semantics. This is a deployment/topology change plus one validation relaxation.

## 3. Design

### 3.1 Mechanism — config-only (same binary, two deployments)

The connector is already config-driven by `WATCH_COLLECTIONS`; the two roles differ only by which collections each lists. No new binary, no role branching in code beyond the guard below.

### 3.2 The one code change — generalize the message-collection guard

`config.go` today hard-fails if `MESSAGE_COLLECTION ∉ WATCH_COLLECTIONS`, to protect the federation-origin `$match` from silently not running. The **collections** deployment legitimately does not watch `rocketchat_message`, so it would fail this validation.

The federation `$match` is already wired per-watcher by identity — `openMongoChangeSource(…, coll == cfg.MessageCollection)` in `main.go`. So the invariant **"message collection watched ⟹ filtered"** holds *by construction*: the filter is applied to whichever watched collection's name equals `MESSAGE_COLLECTION`. Given that, the change is:

- **Keep** `MESSAGE_COLLECTION` non-empty (it is the identity of the federated collection; default stays `rocketchat_message`).
- **Drop** the `slices.Contains(WATCH_COLLECTIONS, MESSAGE_COLLECTION)` hard failure.
- **Add** a computed `watchesMessages := slices.Contains(cfg.WatchCollections, cfg.MessageCollection)` used only to make the startup log truthful. `main.go` currently *always* logs `federation-origin filter active` — a lie on the collections pod. New behavior:
  - `watchesMessages == true`  → log `federation filter active` with `message_collection`.
  - `watchesMessages == false` → log `no message collection watched — federation filter inactive`.

**Safety analysis.** Two cases lose the old fail-fast:

1. *Forgot to watch the message collection* (nothing watches it anywhere): degrades to a **loud zero-message-throughput** signal — observable, not a silent double-deliver.
2. *Typo'd `MESSAGE_COLLECTION` while still watching `rocketchat_message`* (e.g. explicitly set to a non-watched misspelling): the message watcher then runs **unfiltered**, silently migrating foreign-origin messages — the exact double-deliver the old guard existed for. This case IS reopened by the relaxation. Mitigations: the env var defaults to `rocketchat_message` (the typo requires explicitly overriding a correct default); the inactive-filter startup log is emitted at **Warn** so a pod that should be filtering but isn't is conspicuous; and the ops invariant check below (each pod's startup log states its role).

The residual risk is accepted: it requires overriding a defaulted env var with a typo, and the Warn log flags it on every restart. (Mis-naming `MESSAGE_COLLECTION` to a *different watched* collection was never caught by the old guard — presence, not correctness — and is unchanged.) The `MESSAGE_COLLECTION`-non-empty check is retained so the identity is always defined.

### 3.3 Shared infrastructure (no code change; invariants that must hold)

- **Checkpoints:** per-collection docs keyed by `siteID` + `collection` in the `migration` DB. Disjoint watch sets ⟹ disjoint checkpoint docs, no collision.
- **`MIGRATION_OPLOG` stream:** shared; subjects are per-collection (`chat.migration.oplog.{site}.{coll}.{op}`). Downstream transformers filter by subject and are indifferent to which pod published.
- **Stream bootstrap:** both deployments call `CreateOrUpdateStream` (idempotent) in dev; both no-op/verify in prod. No conflict.
- **Cross-deployment invariant (ops/IaC responsibility):** the two watch sets must be **disjoint**, and **exactly one** deployment must include `rocketchat_message`. A single process cannot detect the other; the startup log makes each pod's role visible for verification. Documented, not enforced in code.

### 3.4 Deployment artifacts

- **`deploy/Dockerfile`, `deploy/azure-pipelines.yml`:** unchanged — one image, one build. The two Deployments are ops/IaC (two k8s Deployments referencing the same image with different env).
- **Local `deploy/docker-compose.yml`:** split the single `oplog-connector` service into `oplog-connector-messages` + `oplog-connector-collections`, same build, split `WATCH_COLLECTIONS`, so local dev exercises the real topology and validates the relaxed guard end-to-end. (No host ports are published — both containers keep the default `METRICS_ADDR` `:9090` inside their own network namespace.)

## 4. Testing (TDD)

- **config_test:**
  - collections-role config (`MESSAGE_COLLECTION` set but *not* in `WATCH_COLLECTIONS`) now parses successfully (previously failed).
  - message-role config (message collection present) still parses.
  - empty `MESSAGE_COLLECTION` still rejected.
  - `watchesMessages` derivation correct for both roles.
- **start/watcher (unit):** the filter flag (`coll == MessageCollection`) is true only for the message watcher; a message-less config creates no filtered watcher.
- **integration:** a disjoint-set deployment tails only its collections and publishes only their subjects (no message subjects from the collections pod, and vice versa).
- Coverage stays ≥ 80% (CLAUDE.md §4); the pipeline's existing floor check applies.

## 5. Docs to update (same PR)

- This connector design's parent spec (`docs/superpowers/specs/2026-06-08-oplog-connector-design.md`) — add the two-role split and the disjoint / exactly-one-message-collection invariant.
- `data-migration/CDC_COVERAGE.md` — note the message vs collection deployment split.

## 6. Rollout note (informational, not in scope)

Cutover is an ops sequence: stand up the two deployments with disjoint watch sets, confirm each pod's startup log shows the expected role, then retire the combined deployment. Because checkpoints are per-collection and idempotent, a brief overlap where both a combined and a split deployment tail the same collection only causes deduped replay, not loss — but the steady state must be disjoint to avoid double-publishing a collection.
