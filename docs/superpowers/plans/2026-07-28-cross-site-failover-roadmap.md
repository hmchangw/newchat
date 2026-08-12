# Cross-Site Failover — Program Decomposition & Roadmap

> **Not a task-by-task implementation plan.** The design
> (`docs/superpowers/specs/2026-07-28-cross-site-failover-design.md`) is a DR
> *architecture* with unresolved core-mechanism decisions (§11 open questions) and
> spans app code + platform/NATS + ops/IaC. Per the writing-plans Scope Check, it
> is decomposed here into independently-plannable sub-projects. **Each sub-project
> gets its own spec → plan → implement cycle**; several must resolve open design
> questions (their own brainstorm) *before* a no-placeholder plan can be written.

**Goal:** stand up single-site failover to a shared warm backup ("lifeboat"),
scoped to send/receive in existing rooms + recent history, with clean failback.

**Governing spec:** `docs/superpowers/specs/2026-07-28-cross-site-failover-design.md`

---

## Dependency graph

```
SP0  Local/global room subjects        (EXTERNAL — already designed+planned on
      (prerequisite, not owned here)     branch claude/nats-subscription-reduction)
        │
        ▼
SP1  DR feed + backup materialization   (LINCHPIN — design not yet resolved)
      ├── SP1a  message slice  (canonical → backup Cassandra)   [most concrete]
      └── SP1b  operational slice (rooms/subs/members → Mongo)  [mechanism open]
        │
        ├───────────────┬───────────────────────┐
        ▼               ▼                        ▼
SP2  Backup serving   SP3  Routing brain       SP4  Failover trigger /
     stack + identity      (portal health-           health detection
     (identity resolved)   aware override)           (feeds SP3)
        │               │                        │
        └───────┬───────┴────────────────────────┘
                ▼
SP5  Failback & reconciliation   (needs SP1+SP2 live; algorithm is spec §6)

SP6  Ops / IaC / platform        (cross-cutting runbook, threads throughout —
                                   NOT a TDD plan)
```

**Build order:** SP0 (external) → SP1 → {SP2, SP3, SP4} → SP5, with SP6 running
alongside from SP1 onward.

---

## Sub-projects

### SP0 — Local/global room subjects  *(external dependency — do not re-plan here)*
- **Owner:** `claude/nats-subscription-reduction-z0wcya` (design + plan already
  written there).
- **Why it's here:** the backup's delivery correctness (spec §7) rests on the
  `CrossSite` flag and the `chat.local.room.>` prefix. SP1b must carry `CrossSite`
  on the DR feed; SP2 must add the `chat.local.room.>` subscribe grant to the
  backup's JWTs.
- **Action:** track it as a dependency; do not duplicate. Coordinate the two
  integration points above when it lands.

### SP1 — DR feed + backup materialization  *(LINCHPIN — needs a design cycle first)*
- **Deliverable:** every site continuously ships its whole-site state to the
  backup, which materializes per-`siteID` namespaces (spec §4.1).
- **Ready to plan?** **No.** Resolve first (own brainstorm):
  - §11.1 — operational-state feed mechanism: **Mongo change-streams vs a new
    backup-directed event fan-out**. This choice gates SP1b entirely.
  - How `CrossSite` (SP0) rides the feed.
  - Canonical-stream sourcing topology into the backup (JetStream source/mirror
    over gateway vs a dedicated consumer).
  - Restore-log retention vs read-window (spec §6.5).
- **Split:**
  - **SP1a (message slice)** — source whole `MESSAGES_CANONICAL_{siteID}` → backup
    Cassandra, bucketed schema, retention split. *Most concrete;* reuses
    `message-worker` + JetStream sourcing patterns. Likely the first thing
    plannable once the sourcing topology is picked.
  - **SP1b (operational slice)** — rooms/subs/members/user-slice → backup Mongo via
    the mechanism chosen above. Blocked on §11.1.

### SP2 — Backup serving stack + identity  *(identity resolved — World 1; serving needs SP1)*
- **Deliverable:** the backup runs `auth-service` to mint displaced users' JWTs,
  and the send/receive + history-read paths serve from the materialized copy.
- **Identity — RESOLVED (World 1), spec `specs/2026-08-11-sp2-backup-identity-jwt-minting.md`.**
  Production runs **one shared NATS account**, so the backup mints in that same
  account (not impersonation, no per-site keys) and reuses `auth-service`
  **unchanged**. The earlier "key-custody / KMS" brainstorm (old §11.4) is
  **moot**. The only identity deliverables are config/ops: the shared-template
  `chat.local.room.>` grant (SP0-coupled) and deploying `auth-service` at the
  backup (SP6) — **no new minting code**.
- **Serving path — still needs SP1.** Plannable once SP1 materialization exists;
  that (not identity) is the remaining SP2 substance.

### SP3 — Routing brain: portal-service health-aware override  *(needs SP4 signal)*
- **Deliverable:** `portal-service` becomes the single source of truth for "who
  serves account X right now"; returns the backup for a down site's accounts;
  acts as the split-brain fence (spec §4.2).
- **Ready to plan?** **Resolved & building** — spec `specs/2026-08-11-sp3-portal-routing-override.md`
  (rewritten to World 1 / operator-driven SP4). SP3-core scopes the override to the
  SSO `/api/userInfo` path only.
- **Deferred, CRITICAL follow-up — bot failover.** The bot/admin `/api/v1/login`
  path is intentionally left un-rerouted in SP3-core because bots are out of
  lifeboat scope today and their login forwards to the (down) home botplatform.
  Bots ARE business-critical; re-adding them is a follow-up whose weight is in SP1
  (materialize bot/botplatform state) + SP2 (run botplatform at the backup, wire
  the backup auth-service session branch). The SP3 routing hook is then a single
  `servingURLs(...)` call in `HandleLogin`. Tracked so this is not lost.

### SP4 — Failover trigger / health detection  *(own design)*
- **Deliverable:** auto-detect a down site (+ manual operator override) and drive
  the SP3 override (spec §11.3).
- **Ready to plan?** **No.** Resolve the detection mechanism and the override
  control surface first (own brainstorm).

### SP5 — Failback & reconciliation  *(needs SP1+SP2 live)*
- **Deliverable:** replay the outage log home, verify convergence, cut over
  clients (spec §6, fully specified algorithmically).
- **Ready to plan?** **Blocked** on SP1/SP2 existing, but the *algorithm* is the
  most complete part of the spec — this becomes concrete quickly once its
  dependencies are real.

### SP6 — Ops / IaC / platform  *(cross-cutting runbook, not a TDD plan)*
- Backup deployment (HA multi-AZ); backup as supercluster gateway peer; leaf-node
  `chat.local.>` deny on the backup's leaf; canonical restore-log `MaxAge` sizing
  (spec §6.5); backup `auth-service` deploy with the **shared** account creds
  (World 1 — no per-site NKey/KMS provisioning); **per-site replication-lag
  monitoring + alerting** (spec §8 — the RPO-decay signal). Tracked as an ops
  checklist coordinated with the platform/NATS team, delivered alongside SP1–SP5.

---

## Recommended next step

**Brainstorm SP1 (the DR feed) — specifically resolve §11.1** (Mongo change-streams
vs a backup-directed event fan-out for operational state) and the canonical
sourcing topology. It is the linchpin: SP2 and SP5 cannot exist without it, and
SP1a (the message slice) is likely writable as a concrete no-placeholder plan
immediately once the sourcing topology is chosen.

Everything downstream (SP2 identity, SP4 detection) also needs its own short
design cycle before it can be planned without placeholders — this is expected for
a DR architecture at this altitude, not a gap in the spec.
