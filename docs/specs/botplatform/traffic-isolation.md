# Bot Traffic Isolation

> **Single combined design spec.** **Part I** (requirements & architecture) for product/architects; **Part II** (technical design) for implementers. Companion: **[Bot Platform NextGen Auth Migration](./auth.md)** — this spec consumes the `principal.class` field on `/v1/auth/validate` defined there.
>
> **Status:** All architecture decisions DECIDED 2026-06-16 (Option A / SUBJECT-SPLIT — see Part I §3).

---

# Part I — Architecture & Requirements


> **Part I — Architecture & Requirements.** The *why*, the *what*, the architecture decision, the rollout. **Part II — Technical Design** follows below in the same file (subject namespaces, deployment manifests, NATS / JetStream config, metrics, test plan).
>
> **Companion spec** — [Bot Platform NextGen Auth Migration](./auth.md). Traffic isolation is a follow-on to the auth migration: once bots are first-class principals with a known class label, we can route their traffic separately.
>
> **Status:** all architecture decisions DECIDED 2026-06-16. See §3 for the SUBJECT-SPLIT decision (this spec's Option A vs B vs C) and Part II §11 for the resolved stream-design and routing questions.

---

## 1. Executive summary

**What:** Separate bot traffic from human traffic in the chat backend so that bot bursts cannot degrade human SLOs, and so the two pools can be scaled, canaried, and observed independently.

**Why:**
- Today's services (`message-gatekeeper`, `message-worker`, `broadcast-worker`, …) run a **single Deployment per service** that processes *all* traffic. A bot in a 10k-member room generates 10k broadcast deliveries per send; a bot fleet running an ingest job can saturate Cassandra writes. **Human messaging SLOs are exposed to bot load shape.**
- **Independent scaling** — bot load (ingest jobs, scheduled fan-outs) and human load (interactive chat) have different daily curves, peak shapes, and resource profiles. One HPA can't optimize both.
- **Independent canary / blast radius** — rolling a worker binary should be possible against the bot pool without risking the human pool, and vice versa.

**Key constraint:** **no code fork.** Bot and human flows must continue running the same business logic, the same `store.go`/`handler.go`, the same `pkg/` shared code. The split is at the **deployment + routing layer**, not the package layer.

---

## 2. Document map

- **Part I (sections 1–11, below)** — executive summary, architecture decision, scope, user stories, constraints, security, success criteria, rollout plan.
- **Part II — Technical Design** (further below in this file) — principal classification, subject namespace builders, per-service split plan, JetStream stream design, NATS supercluster config, deployment manifests, metrics, test plan, decisions log.

---

## 3. Architecture decision — ✅ DECIDED 2026-06-16: Option A / SUBJECT-SPLIT (subject namespace split)

> **Naming note.** This spec's Option A/B/C labels refer **only** to the traffic-class routing decision below. The companion **auth migration spec** also uses Option A/B for its own (different) auth-service placement decision — that spec **decides Option B** (`DEDICATED-SERVICE`), which is unrelated to this spec's Option A. To avoid the cross-spec letter collision, both specs now pair the letter with a self-describing suffix (e.g. `Option A / SUBJECT-SPLIT` here; `Option B / DEDICATED-SERVICE` there). When citing across specs, always use the suffix.

Where does the bot vs human separation actually happen on the wire? Three options:

| Option | Mechanism | Class isolation | Operational cost |
|---|---|---|---|
| **A / SUBJECT-SPLIT** ✅ **DECIDED** | New subject space `chat.bot.…` mirroring `chat.user.…`; each Deployment subscribes to one filter | **True traffic-class isolation** — bot Deployment literally cannot receive human messages | Mirror `pkg/subject` builders; publishers pick by principal class (consumed from `/v1/auth/validate.principal.class`, auth.md Part II §9.8); supercluster permissions extended to `chat.bot.>` |
| **B / QUEUE-GROUP-ONLY** | Both Deployments subscribe to `chat.user.…`; NATS load-balances by queue group name | **Blast-radius isolation only** — each Deployment still gets a random mix; a bot burst still saturates the human Deployment in proportion to its share | Cheapest, ~zero NATS/subject changes |
| **C / HYBRID** | Supercluster routes by class subjects; in-cluster fan-out uses shared subjects + queue groups | Partial — federation isolated, in-cluster not | Two routing models to reason about |

**Why A / SUBJECT-SPLIT:**
- The whole point of the split is to keep human SLOs green when bots burst. Option B / QUEUE-GROUP-ONLY fails the canonical scenario — a 10× bot burst still adds proportional load to the human Deployment because queue-group load-balancing is round-robin, not class-aware.
- Subject namespace is the natural place to express principal class in NATS — it's how every other dimension in this codebase is already partitioned (siteID, room, account, message-class).
- Adds one bit of state (`class`) at the publish edge; everything downstream becomes deterministic.

**Why not B / QUEUE-GROUP-ONLY:** Trivial to implement, but doesn't deliver the SLO guarantee. Useful as a *secondary* layer (queue-group within a class, for HA), not as the primary class separator.

**Why not C / HYBRID:** Two routing models, two failure modes, hard to reason about. The supercluster permission surface is already class-aware in Option A / SUBJECT-SPLIT — no benefit to splitting it.

---

## 4. User stories

### US1 — Human SLOs survive a bot burst
*As an on-call engineer, when bot message traffic spikes 10× over baseline, human-message p99 end-to-end latency stays within SLO.*

- Acceptance: synthetic load test — drive the bot pool to 100% CPU, assert human pool p99 < 200 ms (target SLO TBD by ops).
- Acceptance: simulated 10k-member room broadcast on the bot lane, human broadcasts unaffected.

### US2 — Independent scaling
*As an SRE, I can scale the bot worker pool up or down without touching the human pool.*

- Each split service runs **two Kubernetes Deployments**, one per class, with separate HPAs and resource requests.
- Per-pool connection caps to shared stores (Mongo / Cassandra / Valkey) prevent one pool exhausting the other.

### US3 — Automatic routing
*As a service developer, I publish on a single principal-aware subject builder and the right Deployment picks it up — no per-call routing logic.*

- `pkg/subject` exposes `MessageCanonical(class, siteID, …)` (and the rest of the mirrored set) where `class ∈ {bot, user}`.
- Class is **set once at the edge** (gateway / WebSocket / EventConsumer) from the validated principal, threaded through `context.Context` and message envelopes, and never re-derived downstream.

### US4 — One implementation
*As a platform owner, there is no forked `bot-message-worker` or `bot-room-service` package. Both pools run the same binary.*

- One `*_service/` directory per domain. Runtime env var (`WORKER_CLASS=user|bot`) selects subject filter + queue group.
- All `store.go` / `handler.go` / `integration_test.go` cover both classes via table-driven tests.

### US5 — Cross-site federation works for both classes
*As a multi-site operator, a bot message published at site A reaches the bot pool at site B; a human message published at site A reaches the human pool at site B.*

- NATS supercluster gateway `exports` / `imports` cover `chat.bot.>` alongside `chat.user.>`.
- INBOX consumer at site B routes by class subject onto the correct local lane.

### US6 — Per-class observability
*As an on-call engineer, every relevant metric is broken down by `is_bot=true|false`.*

- `messages_sent_total`, `request_duration_seconds`, `jetstream_consumer_pending`, `broadcast_fanout_size`, `cassandra_write_duration_seconds` — all carry an `is_bot` label.
- Dashboard pre-built before any split goes live; the split decision and subsequent tuning are **data-driven**, not vibes-driven.

### US7 — Independent canary
*As a release manager, I can canary a new worker binary in the bot pool only, without risk to humans (and vice versa).*

- Two Deployments → two rollouts. Standard k8s deployment strategies (canary, blue/green) apply per pool.

### US8 — Bursts don't poison shared state
*As an architect, a saturated bot pool does not consume disproportionate Mongo/Cassandra/Valkey connections from the shared pool — neither database connection-pool exhaustion nor JetStream consumer back-pressure leaks across class boundaries.*

- Per-Deployment connection limits documented in Part II §6.
- Per-class JetStream durables (not shared) so consumer lag in one class doesn't stall the other.

### US9 — Migration is reversible
*As a release manager, I can roll back to the shared pool at any rollout phase.*

- Collapse the two Deployments into a single queue group by re-pointing both at the same filter, or by scaling the bot Deployment to 0 and bumping the human Deployment's filter to include both namespaces. Documented per-phase.

---

## 5. Scope — which services split, which don't

**Split first (the "loud pair"; see Part II §2.4):**

| Service | Why it splits | Phase |
|---|---|---|
| `message-worker` | Cassandra writes are the dominant bot cost; isolate so a bot floor-flooding incident can't back up human writes. | Phase 3 onwards |
| `broadcast-worker` (JetStream consumer) | Fan-out is the loudest bot impact (10k-member rooms × bot send rate). Isolate or human delivery WILL be hurt. | Phase 3 onwards |
| `message-gatekeeper` | Hot validate path. **Stays shared through Phase 4** — its consumer reads from `MESSAGES_{siteID}` whose subjects (`chat.user.*.room.*.{siteID}.msg.>`) don't yet carry a class token. Split at this layer requires extending the `pkg/subject` mirror to RPC subjects too. Defer until per-class metrics show shared-gatekeeper saturation. | Deferred — re-evaluated in Phase 5+ |

The two services that actually split first (`message-worker`, `broadcast-worker`) are the ones reading from `MESSAGES_CANONICAL_{siteID}` — the natural class-isolation boundary. `message-gatekeeper` derives the sender's class from `model.IsBotAccount` at publish time and routes to the correct downstream canonical stream (Part II §5.1).

**Do NOT split (shared single Deployment):**

| Service | Why not |
|---|---|
| `botplatform-service` (new) | Bot-only by definition — already isolated. |
| `auth-service` | Human-only path (SSO); bots authenticate through botplatform-service. |
| `notification-worker` | Bots don't receive notifications by product decision — naturally human-only. |
| `room-service` | Membership / room CRUD — usually cheap. Split only if metrics show bot share ≥ 20% on this service. |
| `upload-service` | Bot uploads possible but typically modest. Same threshold rule as room-service. |
| `user-presence-service` | Bot presence is sparse and cheap. Shared. |
| `inbox-worker` | Processes the merged cross-site stream; cannot meaningfully split below stream granularity. |
| `search-service` / `search-sync-worker` | Index writes are global; queries already cheap. |

The "split first" list is intentionally short — three services deliver ~95% of the scale-independence benefit. Everything else stays shared until per-class metrics (US6) justify the extra deployment footprint.

---

## 6. Critical constraints

- **Zero SDK / client changes.** Bots and human clients use the same APIs they do today. The split is invisible from the client's perspective.
- **No code fork.** One service implementation per domain (CLAUDE.md §1 per-service file organization). Runtime config picks the class.
- **Multi-site federation continues working** for both classes — supercluster permissions extended to `chat.bot.>`.
- **Principal class is set exactly once** at the edge, by the auth layer, from the validated token. Never re-derived from message content / headers / heuristics downstream.
- **No new business logic.** Bot messages and human messages run the *same* gatekeeper / persistence / fan-out logic. The only difference is which Deployment runs the code.

---

## 7. Security

- **Class is authority-attested, not client-claimed.** A client sending `X-Principal-Class: user` while holding a bot token is ignored — the value is overwritten by the gateway from the validated principal.
- **Principal class in the subject is *informational*, not authorization.** Existing per-account ACLs (`chat.user.{account}.>` JWT grants from `auth-service`) still gate access to a user's own subject space. A bot cannot publish on `chat.user.…` even if it crafted the subject — its JWT grants are scoped to its own account namespace, regardless of class.
- **Supercluster permissions** for `chat.bot.>` follow the same model as `chat.user.>` — explicit `exports` / `imports` per direction, no wildcards across sites without intent.

---

## 8. Success criteria

| # | Criterion | Measured by |
|---|---|---|
| SC1 | Human-message p99 latency stable during 10× bot burst | Synthetic load test before each rollout phase |
| SC2 | Bot pool HPA scales independently within 60s of load shift | k8s metrics + Prometheus |
| SC3 | Per-class dashboard live before split | Grafana board |
| SC4 | Cross-site bot messages traverse supercluster end-to-end | Integration test in `pkg/testutil` (federated NATS) |
| SC5 | Rollback to shared pool exercised at least once in staging | Runbook + rollback drill |

---

## 9. Rollout plan

Phased — instrument first, deploy second, route third. Each phase is reversible.

| Phase | Action | Risk | Rollback |
|---|---|---|---|
| **0 — Instrument** | Add `is_bot` label to all metrics on shared deployments. Build per-class dashboard. | None — labels only. | n/a |
| **1 — Publish dual** | Publishers emit on BOTH the old `chat.user.…` subject AND the new `chat.bot.…` / `chat.user.…` class-aware subjects. Existing consumers continue reading old subjects only. | Duplicated publishes (cheap; messages are JSON, no payload growth). | Stop publishing on new subjects. |
| **2 — Consumers dual-subscribe** | Existing single Deployments subscribe to BOTH old and new subjects (same queue group); deduplicate at the worker by Message-ID idempotency. | Worker has to handle both subject patterns. | Drop the new subject subscription. |
| **3 — Split deployments** | Create `-user` / `-bot` Deployments per split service. Both still subscribe to old subjects (transitional). Per-class queue groups + durables. | Doubles pod count for split services. | `kubectl scale --replicas=0` on the new Deployments; existing single Deployment unaffected. |
| **4 — Cut over to class subjects** | Split Deployments unsubscribe from old subjects, subscribe to class filters only. Publishers stop emitting on old subjects. | Real cutover — bot pool now genuinely only sees bot traffic. | Re-enable old-subject subscriptions + dual-publish. |
| **5 — Sunset old subjects** | Remove dual-publish code paths and old-subject support from `pkg/subject`. | Code removal only — already inactive after Phase 4. | n/a (Phase 4 is the real cutover). |

Each phase gates on dashboard metrics (US6) + SLO health (SC1). Soak times TBD with ops.

### 9.1 Coupling with the auth migration

This spec depends on the bot-platform-nextgen auth migration delivering the `class` signal at the edge (`/v1/auth/validate.principal.class`, auth.md Part II §9.8). Strict sequencing:

| Auth phase (companion spec §19) | Bot-traffic isolation phase | Why |
|---|---|---|
| 0 — deploy dark in fz1/wsp | — wait | No nextgen traffic yet |
| 1 — chat-GW canary 1% | — wait | Class signal not yet authoritative end-to-end |
| **2 — chat-GW canary 50%; validate is dual-token authority** | **0 — instrument** | `is_bot` label can be derived locally from `model.IsBotAccount(account)` at metric-emit time even before validate carries `class` end-to-end; safe to start |
| 3 — chat-GW 100%; legacy sunset | **1–2 — dual-publish + dual-subscribe** | Auth gate stable; both stacks gone |
| Auth migration complete | **3+ — split deployments, cutover, sunset** | Full nextgen ownership |

**Phase 0 is independent** (instrumentation only — pure additive labels, no behavioral change). **Phase 1+ requires the auth signal** to be live and stable. If the auth migration slips, bot-traffic isolation Phase 0 still proceeds; the data it produces informs both rollouts.

### 9.2 Scope: fz1/wsp only

The split applies only to **nextgen deployments in fz1/wsp**. Legacy services in fz2/chat are being sunset; adding bot-lane deployments there is wasted work. During the auth canary, requests routed to fz2 (legacy) run unchanged on the shared deployment; requests routed to fz1 (nextgen) run on split deployments. After auth Phase 3 (100% to fz1), fz2 deployments are removed entirely.

---

## 10. Out of scope

- Forking the codebase into `bot-*-service` packages (explicit non-goal — §6).
- Bot-specific business logic (e.g. different rate limits, different rooms model). Any of those would be a separate spec.
- Splitting `notification-worker`, `room-service`, `upload-service`, `user-presence-service`, `inbox-worker` (deferred until metrics justify — §5).
- Multi-tenancy beyond `siteID`. Class is not tenancy.

---

## 11. Architecture diagrams

- **View A — Logical traffic separation** (FigJam): https://www.figma.com/board/hScsGyDTbGhT7laIwJsVkx
  Edge classification → class-aware publisher → filtered JetStream → human lane vs bot lane → shared services → stores → supercluster.
- **View B — Per-service deployment topology** (FigJam): https://www.figma.com/board/6vkFEKMJ0WyES2VTpBVstM
  One container image, two k8s Deployments per split service, separate HPAs, queue groups, durables, Prometheus labels.


---

# Part II — Technical Design


> **Part II — Technical Design.** Builds on Part I (above). Specifies *how* — principal classification, subject builders, deployment manifests, NATS / JetStream config, metrics, test plan. Section numbering restarts at §1 within this part.
>
> **Status:** DESIGN-DRAFT — decisions resolved in §11. Companion: [Bot Platform NextGen Auth Migration](./auth.md) Part II §9.8 for the `principal.class` contract this spec consumes.

*Isolate bot traffic from human traffic in the chat backend by class-aware subject namespaces and per-class Deployments, without forking any service implementation.*

---

## 1. Goal & non-goals

### Goals
1. **Class-aware routing on the wire.** A bot message and a human message travel on disjoint NATS subject namespaces; the right Deployment subscribes to the right namespace.
2. **One binary per service, two Deployments per split service.** No code fork; runtime config picks the class.
3. **Per-class observability.** Every relevant metric carries an `is_bot` label.
4. **Cross-site federation parity.** Both classes federate through the NATS supercluster identically.

### Non-goals
- New business logic for bots vs humans (see Part I §10).
- Splitting services not on the "loud trio" list until metrics justify (Part I §5).
- Re-architecting auth, gateway, or stream topology beyond what the class split requires.

---

## 2. Current state (grounded)

Verified against the repo on `claude/bot-platform-nextgen-migration-hb6ok2` (2026-06-16).

### 2.1 `pkg/subject` (centralized — confirmed)

- All subject construction lives in `pkg/subject/subject.go` (1070 lines). No raw `fmt.Sprintf` of subjects in any of the three loud-trio services — every subject either is a literal in `pkg/subject` or a call to a `pkg/subject` builder. A class-aware mirror set added there propagates automatically.
- Existing account-token validation: `isValidAccountToken` (`pkg/subject/subject.go:738`) rejects empty + `*` + `>`. **Does NOT reject `.`** — so existing bot accounts (`xxx.bot`) pass. PR #295's planned stricter `IsValidAccountToken` is the load-bearing concern flagged earlier (bot-account-namespace fix, deferred).
- Canonical-message builders exist: `MsgCanonicalCreated`/`Updated`/`Deleted`/`Pinned`/`Unpinned`/`Reacted(siteID string)` at `pkg/subject/subject.go:180-202`. Wildcard at line 344.

### 2.2 `pkg/stream` (per-site, single canonical stream — confirmed, with a naming gotcha)

- `MessagesCanonical(siteID)` (`pkg/stream/stream.go:22`) → `{Name: "MESSAGES_CANONICAL_{siteID}", Subjects: ["chat.msg.canonical.{siteID}.>"]}`.
- `Messages(siteID)` (`pkg/stream/stream.go:15`) → `{Name: "MESSAGES_{siteID}", Subjects: ["chat.user.*.room.*.{siteID}.msg.>"]}` — this is the **upstream stream message-gatekeeper consumes from**, distinct from `MESSAGES_CANONICAL`.
- `Outbox(siteID)` still exists (`pkg/stream/stream.go:36-41`) despite the supercluster migration noted in earlier discussion — the user has confirmed OUTBOX removal is deferred; treat it as still live for now.
- `Inbox(siteID)` (`pkg/stream/stream.go:80-88`) — INBOX has two non-overlapping subject patterns (local + federated aggregate), well-documented in the file. `inbox-worker` owns it; no change needed for class isolation.

### 2.3 Canonical subject rename — ✅ DECIDED 2026-06-16: symmetric

The current canonical subject is `chat.msg.canonical.{siteID}.{event}` — second token is `msg`. With the bot-account namespace fix landing (auth.md Part II §4.4), the symmetric rename becomes the right shape: rename existing `chat.msg.canonical.…` → `chat.user.canonical.…` for humans; add `chat.bot.canonical.…` for bots. **Rationale:** the bot-account fix commits the codebase to a `chat.{class}.…` top-level convention (`chat.user.{account}.…` for humans, `chat.bot.{botToken}.…` for bots); leaving the canonical stream on the `chat.msg.…` second-token would be the lone exception, making the ontology asymmetric forever. Pay the rename cost once.

**Files touched by the rename:**
- `pkg/subject/subject.go:180-202, 344` — six canonical builders + wildcard, rename to `chat.user.canonical.…`; add `chat.bot.canonical.…` siblings (mirror builders take `class` parameter).
- `pkg/stream/stream.go:22-27` — `MessagesCanonical(siteID)` becomes `MessagesCanonicalUser(siteID)` + new `MessagesCanonicalBot(siteID)` per §6.
- `message-worker/main.go:225` — filter subject updated.
- `message-gatekeeper/handler.go:306` — publish subject picked per sender class (§5.1).
- All corresponding test files.

Rename mechanic for the stream itself uses JetStream `Mirror` to avoid downtime — see §6.3.

### 2.4 Per-service consumer setup today

| Service | Stream consumed | Durable name | Filter / queue | File:line |
|---|---|---|---|---|
| `message-gatekeeper` | `MESSAGES_{siteID}` (the upstream stream, **not** canonical) | `message-gatekeeper` | none (pulls all `chat.user.*.room.*.{siteID}.msg.>`) | `message-gatekeeper/main.go:138, 199-202` |
| `message-worker` | `MESSAGES_CANONICAL_{siteID}` | `message-worker` | `chat.msg.canonical.{siteID}.created` (`.created` only — `.updated`/`.deleted` are written synchronously by history-service) | `message-worker/main.go:152, 222-226` |
| `broadcast-worker` (JS) | `MESSAGES_CANONICAL_{siteID}` | `broadcast-worker` | none (all canonical events) | `broadcast-worker/main.go:136, 283-285` |
| `broadcast-worker` (core NATS) | n/a | n/a | queue group `broadcast-worker`, subject `chat.server.broadcast.{siteID}.>` — for fire-and-forget badge events like thread tcount | `broadcast-worker/main.go:181-185` |

**Implications for the split plan (§5):**

- `message-gatekeeper` cannot be split cleanly on the canonical-subject axis because it consumes from `MESSAGES_{siteID}` with subjects `chat.user.*.room.*.{siteID}.msg.>`. Splitting it requires either: (a) introducing a class token in the `chat.user.*.room.…` subject space too (subject mirror at the publisher level — bigger blast radius), or (b) leaving message-gatekeeper shared and only splitting downstream (message-worker + broadcast-worker), accepting that message-gatekeeper itself is not isolated. **Recommended: (b) for Phase 1**; revisit (a) only if message-gatekeeper saturation becomes a real incident.
- `broadcast-worker`'s **core-NATS queue subscription** (for `chat.server.broadcast.{siteID}.>` thread-tcount events) is class-agnostic by nature — those are server-side fire-and-forget badge events, not client-class-tagged. Leave it shared; only the JetStream consumer (canonical-stream side) splits.

### 2.5 `pkg/model` event struct (Timestamp pattern confirmed)

- `MessageEvent` (`pkg/model/event.go:20`) carries `Event EventType`, `Message Message`, `SiteID string`, `Timestamp int64`, optional `ReactionDelta`, optional `NewTCount`. Adding a `Class string \`json:"class" bson:"class"\`` field is the natural mirror of the `Timestamp` field pattern (CLAUDE.md §6 "Event Timestamps"). One-line change in `pkg/model`; publishers populate from `ctxutil.PrincipalClass(ctx)`.

### 2.6 `IsBotAccount` (the class derivation helper)

- `model.IsBotAccount(account)` (`pkg/model/account.go:11`) returns `strings.HasSuffix(account, ".bot") || strings.HasPrefix(account, "p_")`. This is the **single source of truth for bot classification** for any path that doesn't go through `/v1/auth/validate` (e.g. background workers operating on stored messages, the Phase 0 metric labels that pre-date validate's `class` field).

### 2.7 NATS supercluster

Confirmed via discussion (2026-06-16) — cross-site federation runs via NATS supercluster gateway routing; OUTBOX is being deprecated but is still present in `pkg/stream`. Treat OUTBOX as transitional. Gateway permissions are the federation surface; both class namespaces (whichever shape lands per §2.3) need explicit `exports`/`imports` entries.

### 2.8 Net for this spec

- §3 (principal classification): correct as drafted, no changes needed.
- §4 (subject mirror table): provisionally symmetric; revisit alongside the deferred bot-account-namespace fix (§2.3).
- §5 (per-service split tables): **needs update** — `message-gatekeeper`'s consumer reads from `MESSAGES_{siteID}` not `MESSAGES_CANONICAL_{siteID}`; the current spec table is misleading on this point. Apply (b) from §2.4 — message-gatekeeper stays shared in Phase 1, downstream pair splits.
- §6 (separate streams): solid — `MESSAGES_CANONICAL_USER_{siteID}` + `MESSAGES_CANONICAL_BOT_{siteID}` with rename-via-mirror, applied to the actual `chat.msg.canonical.…` subject space.
- §7 (supercluster permissions): solid — pending OUTBOX deprecation timeline.

---

## 3. Principal classification

### 3.1 Where class is set
**Exactly once, at the edge, by the auth layer**, from the validated principal. The three edge surfaces:

| Edge | Source of class | Where set |
|---|---|---|
| **ApiGW** (HTTP `/api/v2/*`) | `POST /v1/auth/validate` response — `principal.class ∈ {"bot","user","admin"}` (auth.md Part II §9.8) | ApiGW injects `X-Principal-Class` header before forwarding to `Server` (mTLS) |
| **WebSocket server** | Same `/v1/auth/validate` call at connect time | WS server stores class on the connection; tags every published message |
| **EventConsumer** (webhook delivery) | Same `/v1/auth/validate` for the bot's outbound token | Tag on published events |

The legacy chat path (human SSO via `auth-service`) sets class from the JWT — `model.IsBotAccount(user.Account)` reads `false` for SSO accounts, so `class = "user"`.

**Three classes, two lanes.** The auth `principal.class` enum returns three values (`bot`, `user`, `admin`), but the traffic-isolation routing collapses `{user, admin} → "user"` lane. Admins are rare web-UI traffic that lands on `botplatform-service` directly, not on the chat data plane; from the message-send / broadcast / persistence perspective they're indistinguishable from humans. Subject namespace stays binary: `chat.bot.…` vs `chat.user.…`. (Q-admin-class resolved 2026-06-16.)

**Legacy tokens still get `class=bot` correctly.** During the auth hybrid phase a bot might present an imported Rocket.Chat token (Part II §4.3 legacy scheme). The validate response derives `class` from the **principal**, not the token format — `model.IsBotAccount(account)` returns the same result whether the token was issued by legacy v2 or by `botplatform-service`. No special handling required. (Q-legacy-token-class resolved 2026-06-16.)

### 3.2 How class propagates
- **HTTP**: `X-Principal-Class: bot|user` header. Gateway sets it, downstream services trust it (only ever populated by the gateway under mTLS — clients can't forge).
- **NATS**: message envelope adds `class` field at the publish site (alongside the existing `timestamp` field, CLAUDE.md §6 "Event Timestamps"). Subject also encodes class (§4), so subscribers don't need to peek into the body to route — but the envelope field is the authoritative copy carried through `context.Context`.
- **`context.Context`**: `ctxutil.WithPrincipalClass(ctx, class)` set by the edge middleware (HTTP) or the NATS router (`pkg/natsrouter`). All handlers read via `ctxutil.PrincipalClass(ctx)`.

### 3.3 What class is NOT
- Not derived from message content. Not derived from `X-User-Id`. Not derived from room membership.
- Not authorization. Existing per-account JWT grants (`chat.user.{account}.>`) gate access regardless of class.
- Not tenancy. Bots and humans share rooms, share the same `siteID`, share the same Mongo/Cassandra keyspaces.

---

## 4. Subject namespace design

### 4.1 The mirror rule
For every existing `chat.user.…` subject, define a `chat.bot.…` sibling with the same suffix structure. Both share one builder API in `pkg/subject` that takes `class` as the first parameter.

### 4.2 Builder API

```go
// pkg/subject/class.go
type Class string

const (
    ClassUser Class = "user"
    ClassBot  Class = "bot"
)

func FromUser(u model.User) Class {
    if model.IsBotAccount(u.Account) {
        return ClassBot
    }
    return ClassUser
}

// AccountToken returns the subject-safe account identifier for the given class.
// Humans: identity verbatim (validated strict, no dots).
// Bots: .bot-suffix stripped — chat.bot.> namespace already encodes the class,
// so "xxx.bot" → "xxx" (see auth.md Part II §4.7 BotSubjectName).
func AccountToken(class Class, account string) string {
    if class == ClassBot {
        return BotSubjectName(account)  // strings.TrimSuffix(account, ".bot")
    }
    return account
}

// pkg/subject/subjects.go — examples (canonical-stream subjects renamed per §2.3)
func MessageCanonical(class Class, siteID, event string) string {
    return fmt.Sprintf("chat.%s.canonical.%s.%s", class, siteID, event)
}

func MessageCanonicalFilter(class Class, siteID string) string {
    return fmt.Sprintf("chat.%s.canonical.%s.>", class, siteID)
}

// UserScoped derives a class-aware account-scoped prefix. Note: the token
// position depends on class (humans = account verbatim; bots = normalized).
func UserScoped(class Class, account string) string {
    return fmt.Sprintf("chat.%s.%s", class, AccountToken(class, account))
}
```

**Cross-spec invariants:**
- `BotSubjectName` and `IsValidBotAccount` are defined in auth.md Part II §4.7. This spec consumes them; the auth-spec PR adds them.
- Every existing call site that builds a subject from an `account` and currently assumes the account is human-shaped (no dots) gets a class-aware wrapper. Audit pass: grep `pkg/subject` callers in `message-gatekeeper`, `message-worker`, `broadcast-worker`, `room-service`, `auth-service` — switch each to the class-aware variant where the principal class is known.

### 4.3 The mirror table

Account-name notation: for human accounts `{account}` is the strict-validated identifier verbatim (e.g. `alice`); for bot accounts `{account}` is the **`.bot`-stripped form** produced by `BotSubjectName` (e.g. `xxx` for account `xxx.bot`, auth.md Part II §4.7). The `chat.bot.>` namespace already encodes the class so the suffix is redundant inside it.

| Old (today) | New — user lane | New — bot lane |
|---|---|---|
| `chat.user.{account}.request.…` | `chat.user.{account}.request.…` *(unchanged)* | `chat.bot.{account}.request.…` |
| `chat.user.{account}.room.{roomID}.{siteID}.msg.send` | `chat.user.{account}.room.{roomID}.{siteID}.msg.send` *(unchanged in Phase 1 — gatekeeper shared per §5.1)* | `chat.bot.{account}.room.{roomID}.{siteID}.msg.send` *(Phase 5+ only; Phase 1 bots still publish on the human-side subject and gatekeeper classifies)* |
| `chat.msg.canonical.{siteID}.{event}` | `chat.msg.canonical.{siteID}.{event}` *(Phase 1: shared — see §4.4a)* | *(Phase 1: shared with users on `chat.msg.canonical.…`; class lives in the event payload)* |
| `chat.room.{roomID}.…` | `chat.room.{roomID}.…` *(unchanged — Q3 decided NO room-class, §11)* | (no bot variant; rooms are class-agnostic) |

JWT grants:
- Humans: `chat.user.{account}.>` (unchanged).
- Bots: **`chat.bot.{account}.>`** where `{account}` = `BotSubjectName(rawAccount)` — eliminates the ACL escape where human `xxx` would match bot `xxx.bot`'s subject space.
- Admins: **`chat.>`** (god-mode, decided 2026-06-24, see auth.md Part II §3 Key Decisions / §5.2 `kind:"admin"`).

Phase 1 ships row 1 (publisher namespace split) + row 4 (rooms class-agnostic); row 3 is **deferred** (one shared canonical stream — see §4.4a); row 2 only matters at Phase 5+ when (and if) the user-scoped RPC subjects also get classed.

### 4.4 What gets a class token

- **Canonical message stream subjects** — see §4.4a for the Phase 1 = shared-stream decision and the deferred trade-off.
- **User-scoped client RPCs** — yes (so JWT scoping naturally enforces class).
- **Room-scoped pub/sub** — open question (§11 Q3): is a room "of a class," or are bot and human messages in the same room? Most likely the latter, in which case room-scoped subjects stay unclassified and only canonical/RPC subjects get classed.
- **Cross-site subjects** — yes (supercluster permissions are class-aware, §7).
- **Internal-only subjects** (worker-to-worker, intra-service) — no, they're not class-aware.

### 4.4a Shared vs split canonical streams — DECIDED 2026-06-24 (Phase 1 = SHARED; split deferred)

The publisher namespace is split per class (`chat.user.{account}.>` / `chat.bot.{account}.>` / admin `chat.>`) — that's enforced by JWT grants and resolves the ACL-escape problem. The orthogonal question this subsection answers: **does the downstream CANONICAL stream (and the `ROOMS_{siteID}` stream) get split per class too?**

**Phase 1 decision: keep the canonical streams SHARED across classes.** One `MESSAGES_CANONICAL_{siteID}` stream carries both bot- and user-canonical events; one `ROOMS_{siteID}` stream carries both bot- and user-originated room operations. The publisher's class is preserved in the event payload (via `principal.class` already on the canonical envelope), so consumers can label metrics and demux behavior by class without needing separate streams.

**Future split is a non-breaking change** (escape hatch detailed below) — defer it until measured noisy-neighbor pressure justifies the operational overhead. Document the trade-off here so the team can revisit with data.

#### Pros of SHARED (Phase 1 choice)

| # | Property | Benefit |
|---|---|---|
| 1 | **Simpler infra** | Two streams to provision, monitor, mirror across sites, federate — not four. Smaller surface for ops to learn / back up / replicate. |
| 2 | **Single dashboards & alerts** | One stream-depth gauge per canonical type. No per-class lag confusion ("which class is backed up?"); class-cardinality lives on metric labels, not on stream names. |
| 3 | **Workers stay single-deployment** | `message-worker`, `broadcast-worker`, `notification-worker`, `search-sync-worker`, `outbox-worker` each remain ONE Deployment / one Helm chart / one HPA. Half the Kubernetes objects. |
| 4 | **No bot/user worker-pool partitioning to deploy or operate** | No artificial split where the workload doesn't yet demand it. YAGNI. |
| 5 | **Mixed-room handling stays trivial** | When a bot sends to a mixed room, broadcast-worker reads canonical, fans out by membership — no cross-stream coordination needed. |
| 6 | **Federation simpler** | One outbox-worker reading one canonical stream. Split would mean either two outbox-workers OR one worker reading two streams (more glue code). |
| 7 | **Easier to reason about ordering & idempotency** | Single stream sequence per site = single linear history per canonical type. No interleaving questions across class streams. |
| 8 | **No canonical-subject rename needed** | The §2.3 canonical-subject rename (`chat.msg.canonical.…` → `chat.user.canonical.…`/`chat.bot.canonical.…`) is **deferred until split**. Phase 1 keeps the existing `chat.msg.canonical.…` subject as-is. |

#### Cons of SHARED — what you give up (the watch-list)

| # | Property | Cost |
|---|---|---|
| 1 | **No independent worker-pool scaling** | Cannot HPA bot-workers separately from user-workers — `message-worker` is one Deployment serving both. A 10× bot traffic spike forces user-handling capacity to grow with it (and vice versa). |
| 2 | **No independent failure domain** | A poison-pill bot message that crashes `message-worker` also halts user-message processing on that pod. Split would contain blast radius to one class. (Mitigated somewhat by JetStream's at-least-once + DLQ semantics, but the worker is shared.) |
| 3 | **Noisy-neighbor risk** | Chatty bot fleet → canonical queue depth grows → human messages wait behind bot backlog. Same lag for everyone. Mitigated upstream by gatekeeper-side rate limits, but the queueing happens downstream. |
| 4 | **Metric/SLO granularity is label-based, not stream-based** | `messages_processed_total{class}` becomes a label, not a stream/consumer split. Per-class dashboards work, but per-class **alerts** on consumer lag are harder (alert calculated from label aggregation instead of directly from per-stream depth). |
| 5 | **Cross-class rate limiting lives in app code** | "Throttle bots at 100 msg/sec without affecting humans" has to live in gatekeeper or worker logic, not at the JetStream consumer-config level. Slightly more code. |
| 6 | **HPA precision is coarser** | Worker autoscaling triggers on combined depth, not per-class. May over- or under-provision per-class during traffic asymmetry. |

#### What you keep regardless (the invariants sharing doesn't break)

- ✅ **Publisher ACL isolation** — JWT grants remain per-class (`chat.user.{account}.>` / `chat.bot.{account}.>` / `chat.>` for admin). Bots can't impersonate users at the publish edge.
- ✅ **Subject-collision safety** — separate top-level namespaces at publish; human `xxx` and bot `xxx.bot` never overlap.
- ✅ **Per-class metric tagging** — via the `class` field on the canonical event payload.
- ✅ **Mixed-room correctness** — workers don't care about publisher class for fan-out; they route by room membership.
- ✅ **Federation correctness** — one outbox-worker reading one canonical stream is simpler and works for both classes.

#### Split-later escape hatch — what triggers it, how to do it

Triggers (any one suffices):
- Measured noisy-neighbor incident: bot backlog causing human-message SLO breach (consumer-lag p99 above threshold for >X minutes).
- A specific tenant requires guaranteed per-class SLO (compliance or billing tier).
- Class-specific worker logic diverges enough that one Deployment serving both gets ugly (lots of `if class == bot {…}` branches).
- Bot traffic exceeds the worker pool's ability to scale linearly with human traffic (e.g., 100× volume asymmetry).

Mechanic when triggered:
1. Add new streams `MESSAGES_CANONICAL_BOT_{siteID}` and (if needed) `ROOMS_BOT_{siteID}` with subjects `chat.bot.canonical.{siteID}.>` and `chat.room.bot.canonical.{siteID}.>`.
2. Rename the existing streams' subject scope (the §2.3 canonical-subject rename pays off here): `chat.msg.canonical.…` → `chat.user.canonical.…`. Use JetStream `Mirror` to maintain both subject patterns during transition.
3. Update gatekeeper to publish per class: bot-originated canonical events go to `chat.bot.canonical.…`; user-originated to `chat.user.canonical.…`. Workers re-subscribe accordingly.
4. Split the worker Deployments per class (`message-worker-user` / `message-worker-bot`, etc.) once the streams are split. Independent HPA from this point.

The publisher namespace already being class-split (chat.user vs chat.bot) is what makes this a **non-breaking** change — no JWT grant changes, no client-side changes, no API contract changes. Only the internal canonical-stream wiring shifts.

#### When to revisit

- After 30 days of production traffic (bot + human) at non-trivial volume on the shared stream — review consumer-lag percentiles by `{class}` label; if p99 lag per class diverges by >2× consistently, split.
- If a specific operational pain point materializes (noisy-neighbor incident, per-tenant SLO escalation).
- When adding a 6th+ canonical consumer (more consumers = more pressure on the shared stream; revisit the cost/benefit).

- **Canonical message stream subjects** — yes (the loud trio routes off these).
- **User-scoped client RPCs** — yes (so JWT scoping naturally enforces class).
- **Room-scoped pub/sub** — open question (§11 Q3): is a room "of a class," or are bot and human messages in the same room? Most likely the latter, in which case room-scoped subjects stay unclassified and only canonical/RPC subjects get classed.
- **Cross-site subjects** — yes (supercluster permissions are class-aware, §7).
- **Internal-only subjects** (worker-to-worker, intra-service) — no, they're not class-aware.

---

## 5. Per-service split plan

### 5.0 Bot publish edge — core NATS R/R, no submit stream (DECIDED 2026-06-24)

Bots use REST end-to-end from the bot SDK's POV (preserving legacy v2 REST semantics). The translation layer inside `bp-api` uses **core NATS request/reply** to talk to `message-gatekeeper` — **not** JetStream `js.Publish` — so bot publish errors return synchronously and fail-fast, matching legacy REST behavior bots already know.

**Key consequence: there is NO `MESSAGES_BOT_{siteID}` JetStream submit stream.** The user-side `MESSAGES_{siteID}` stream (`pkg/stream/stream.go:15`) carries only `chat.user.*.room.*.{siteID}.msg.>` subjects — a parallel bot-side submit stream is not created, because bots never publish into JetStream at all.

**Per-RPC transport for bot operations:**

| Bot RPC category | Transport from bp-api | Examples |
|---|---|---|
| **Query / lookup** (read-only) | **Core NATS R/R** (already today's pattern in services) | `msg.history`, `msg.thread`, `msg.get`, `member.list`, `member.statuses`, `search.messages`, `search.rooms`, `user.profile.getByName`, `presence.query.batch`, `room.app.tabs` |
| **Mutation with single sync response** (no fan-out) | **Core NATS R/R** | `member.add`, `member.remove`, `member.role-update`, `mute.toggle`, `favorite.toggle`, `room.rename`, `room.create`, `message.read`, `message.read-receipt` |
| **Message publish** (fan-out required) | **Core NATS R/R to gatekeeper; gatekeeper synchronously publishes to JetStream `MESSAGES_CANONICAL_{siteID}` and waits for PubAck before replying to bot** | `msg.send`, `msg.edit`, `msg.delete`, `msg.react`, `msg.pin`, `msg.unpin` |

**Flow for the message-publish case:**

```text
bot ──REST──▶ bp-api ──nc.Request("chat.bot.{account}.room.R.{siteID}.msg.send")──▶ message-gatekeeper
                                                                                          │
                                                                                          ▼
                                                                              validate synchronously
                                                                              (subject, IsValidBotAccount,
                                                                               membership, payload schema)
                                                                                          │
                                                                ┌─────────────────────────┴─────────────────────────┐
                                                              valid                                              invalid
                                                                │                                                    │
                                                                ▼                                                    ▼
                                                  js.Publish("chat.msg.canonical.{siteID}.created", payload)   reply errcode (fail-fast)
                                                                │                                                    │
                                                                ▼                                                    └──▶ bp-api ──REST 4xx──▶ bot
                                                  wait for PubAck (≤ ~5ms)
                                                                │
                                                                ▼
                                                  reply { messageId, ok } via core NATS R/R
                                                                │
                                                                ▼
                                                  bp-api ──REST 200──▶ bot
```

**Edge handling:**
- **Gatekeeper unreachable** → `nc.Request` times out → bp-api returns `503` → bot's REST client retries.
- **Validation rejection** → gatekeeper replies errcode on the R/R envelope → bp-api translates to REST `4xx` (`400 invalidPayload`, `403 notMember`, etc.) → bot fails fast, no retry.
- **`js.Publish` to canonical fails** (JS leader election, disk full, etc.) → gatekeeper replies `503` on the R/R envelope → bp-api returns `503` → bot retries; idempotency on `messageId` prevents duplicate canonical writes on retry.

**Why this shape vs the user-side JetStream submit path:**

| Property | User publish (JetStream `MESSAGES_{siteID}` submit) | Bot publish (core NATS R/R) |
|---|---|---|
| **Failure mode** | Durable submit — message buffers in stream until gatekeeper consumes | Fail-fast — bp-api returns error immediately if gatekeeper unreachable |
| **Optimistic UI support** | Browser shows "sending…", message persists across transient gatekeeper hiccups | N/A — bot has no UI, just needs sync yes/no |
| **Backpressure** | Stream depth absorbs bursts; gatekeeper paces consumption | Gatekeeper rejects on overload → bot retries with backoff |
| **Latency profile** | Submit-PubAck + async validate | One-shot validate + canonical-PubAck inline (~5–15ms typical) |
| **Matches legacy contract** | N/A (no legacy for chat-frontend) | YES — bot SDK already expects sync REST success/failure |

**The fan-out side is unchanged.** Once a message lands in `MESSAGES_CANONICAL_{siteID}` (regardless of whether the publisher was a user via the submit stream or a bot via core-NATS R/R), all downstream workers (`message-worker`, `broadcast-worker`, `notification-worker`, `search-sync-worker`, `outbox-worker`) consume durably with at-least-once + replay semantics. The shared canonical stream (Phase 1, §4.4a) carries both classes; consumers tag metrics by the `class` field on the canonical payload.

**Implementation impact on `message-gatekeeper` (Phase 1):**
- Existing JetStream pull-consumer on `MESSAGES_{siteID}` stream → **unchanged** (user submit path).
- NEW core-NATS queue-subscribe on `chat.bot.*.room.*.{siteID}.msg.send` (and `.edit`/`.delete`/`.react`/`.pin`/`.unpin`) → **added** for bot R/R path; handler validates synchronously and publishes to `MESSAGES_CANONICAL_{siteID}` inline before replying.
- Both paths produce the same canonical-stream event shape (with `class` set from sender lookup).

### 5.1 `message-gatekeeper` — STAYS SHARED through Phase 4, re-evaluated in Phase 5+

`message-gatekeeper` consumes from the **upstream `MESSAGES_{siteID}` stream** with subjects `chat.user.*.room.*.{siteID}.msg.>` (`pkg/stream/stream.go:15`, `message-gatekeeper/main.go:138`) for **user** submissions, AND core-NATS queue-subscribes on `chat.bot.*.room.*.{siteID}.msg.>` for **bot** submissions (§5.0). The `chat.user.…` segment here is the user-scoped RPC namespace — it's not (yet) a class token. Splitting message-gatekeeper at this layer requires either expanding the `pkg/subject` mirror to cover the `MsgSend`/`MsgSendWildcard` builders too, or rewriting publishers (the WS gateway) to route on class.

**Phase 1 decision:** message-gatekeeper **stays as a single Deployment** through Phase 4 of the rollout (Part I §9). It validates both human and bot messages, derives the sender's class from `model.IsBotAccount(account)` (`pkg/model/account.go:11`), and publishes to the **class-appropriate canonical stream** (§5.2 below). This is the cleanest seam:

- The validation hot path runs once per message — splitting message-gatekeeper isolates the validate CPU cost, not the downstream cost. The dominant bot cost is downstream (Cassandra writes + broadcast fan-out), and those ARE split.
- Subject mirroring is deferred to where it actually buys isolation — at the canonical-stream boundary, not at the user-scoped RPC boundary.
- The publish-side change is mechanical: `handler.go:306` already picks the canonical subject; it now picks per class.

```go
// message-gatekeeper/handler.go (sketch — Phase 3+)
class := subject.ClassUser
if model.IsBotAccount(senderAccount) {
    class = subject.ClassBot
}
canonicalSubj := subject.MsgCanonicalCreated(class, siteID)  // class-aware variant added in §4
```

**Phase 1 update (DECIDED 2026-06-24, §4.4a):** the canonical stream is **shared** across classes in Phase 1, not split per class. Gatekeeper publishes to the single `MESSAGES_CANONICAL_{siteID}` stream with the existing `chat.msg.canonical.{siteID}.{event}` subject for BOTH user-originated and bot-originated messages; the `principal.class` field on the canonical payload preserves class information for downstream consumers' metric labels. The per-class subject variant (`chat.user.canonical.…` / `chat.bot.canonical.…`) is **deferred** until the §4.4a escape-hatch triggers a split.

Phase 5+ (post-cutover) can re-evaluate: if shared message-gatekeeper saturation becomes a real production incident, then introduce a class-aware mirror in the `chat.user.*.room.…` namespace and split at that point. **Don't do it preemptively** — the per-class metrics dashboard (US6) tells you when it's needed.

### 5.2 `message-worker`

**Phase 1 (DECIDED 2026-06-24, §4.4a): single shared Deployment.** `message-worker` runs as ONE Deployment consuming from the shared `MESSAGES_CANONICAL_{siteID}` stream — no `-user`/`-bot` split. Metrics tag class via the `principal.class` payload field. The per-class deployment table below is the **future design** that activates when the §4.4a escape hatch triggers a canonical-stream split.

| Aspect | Phase 1 (shared, current) | Phase N+ when split (per §4.4a escape hatch) — `-user` Deployment | Phase N+ when split — `-bot` Deployment |
|---|---|---|---|
| JetStream stream | `MESSAGES_CANONICAL_{siteID}` (shared) | `MESSAGES_CANONICAL_USER_{siteID}` | `MESSAGES_CANONICAL_BOT_{siteID}` |
| JetStream consumer | durable `message-worker` on the shared stream | durable `message-worker-user` on the USER stream | durable `message-worker-bot` on the BOT stream |
| Concurrency | `MaxWorkers=100` (combined load) | `MaxWorkers=50` per pool | `MaxWorkers=200` per pool |
| Cassandra connection pool | cap 200 per pod | cap 100 per pod | cap 200 per pod |
| Env var | `WORKER_CLASS` unset/`all` | `WORKER_CLASS=user` | `WORKER_CLASS=bot` |

Phase 1 trade-off documented in §4.4a (no independent per-class scaling / failure domain — accepted for now). Per-class isolation activates only when measured noisy-neighbor pressure justifies the split.

### 5.3 `broadcast-worker`

**Phase 1 (DECIDED 2026-06-24, §4.4a): single shared Deployment.** Same reasoning as §5.2 — `broadcast-worker` is ONE Deployment consuming from the shared `MESSAGES_CANONICAL_{siteID}` stream. Per-class split is the future-design column, deferred per the §4.4a escape hatch.

| Aspect | Phase 1 (shared, current) | Phase N+ when split — `-user` Deployment | Phase N+ when split — `-bot` Deployment |
|---|---|---|---|
| JetStream stream | `MESSAGES_CANONICAL_{siteID}` (shared) | `MESSAGES_CANONICAL_USER_{siteID}` | `MESSAGES_CANONICAL_BOT_{siteID}` |
| JetStream consumer | durable `broadcast-worker` on the shared stream | durable `broadcast-worker-user` on the USER stream | durable `broadcast-worker-bot` on the BOT stream |
| Concurrency | `MaxWorkers=300` (combined fan-out load) | `MaxWorkers=100` | `MaxWorkers=400` (fan-out is the dominant bot cost) |
| Env var | `WORKER_CLASS` unset/`all` | `WORKER_CLASS=user` | `WORKER_CLASS=bot` |

Receivers (the WebSocket connections being broadcast TO) are shared regardless — a human in a room hears both human and bot messages. Even after the split, the split is on the **producer** side of the fan-out, not the consumer side.

### 5.4 What changes inside each binary

**Phase 1: nothing.** Each worker runs as a single Deployment with the existing handler; no class-aware code path required. Metrics get a `class` label sourced from the canonical payload's `principal.class` field.

**Phase N+ (when split activates per §4.4a):** the same `handler.go` / `store.go` runs in both Deployments. The only diff:

```go
// main.go (each split service — Phase N+ only)
class := subject.Class(os.Getenv("WORKER_CLASS")) // "user" | "bot"
filter := subject.MessageCanonicalFilter(class, cfg.SiteID)
queueGroup := fmt.Sprintf("%s.%s", serviceName, class)
// ... pass into subscriber setup ...
```

When the split activates, `WORKER_CLASS` becomes a required env on split services; missing or invalid value = startup error (fail fast per CLAUDE.md §3). Until then the env var is optional and ignored.

---

## 6. JetStream stream design — Phase 1 SHARED (per §4.4a); per-class split deferred

**Phase 1 decision (DECIDED 2026-06-24, supersedes the earlier "separate streams per class" recommendation):** the existing `MESSAGES_CANONICAL_{siteID}` stream (subjects `chat.msg.canonical.{siteID}.>`) is **kept as-is, shared across classes**. No `MESSAGES_CANONICAL_USER_{siteID}` / `MESSAGES_CANONICAL_BOT_{siteID}` split in Phase 1; no canonical-subject rename. Gatekeeper publishes both user- and bot-originated canonical events to the same stream; consumers tag metrics by the `principal.class` payload field.

The per-class split below is preserved as the **escape-hatch design** — what to ship when the §4.4a triggers fire (measured noisy-neighbor incident, per-tenant SLO escalation, etc.).

### 6.1 Why Phase 1 = shared (and why the per-class split is the escape hatch, not the default)

The full trade-off is in §4.4a (pros/cons of shared vs split + escape-hatch triggers + migration mechanic). Summary for cross-reference:

- **Phase 1 picks shared** for operational simplicity (half the streams to provision/monitor/mirror/federate, single-Deployment workers, trivial mixed-room handling, no canonical-subject rename) and because the per-class scaling/isolation benefits are speculative until validated with production traffic.
- **Split-later is non-breaking** (publisher namespace stays class-split throughout, JWT grants unchanged, client-side unchanged) — only internal canonical-stream wiring shifts when triggered.
- **The earlier "shared stream + filter still couples on write path" argument** below (preserved as §6.1.1 future-design reference) remains correct as a description of split-time benefits — it's just that Phase 1 accepts those couplings as YAGNI trade-offs until a measured incident justifies paying the split cost.

#### 6.1.1 Future-design reference: separate streams (escape-hatch target)

When the §4.4a escape hatch triggers, the canonical streams split to `MESSAGES_CANONICAL_USER_{siteID}` / `MESSAGES_CANONICAL_BOT_{siteID}` with the per-class subject naming. The original split rationale (preserved here for the escape-hatch implementation):

A shared stream with class-filtered consumers still couples the two classes on the **write path** and at the **stream level**, undoing the isolation thesis. Separate streams give:

| Concern | Shared stream + filter | Separate streams ✅ |
|---|---|---|
| **Stream-wide back-pressure** | Crosses class on the write path (publisher acks wait on the same stream) | Independent — one stream's surge doesn't slow the other's acks |
| **Storage budget** (`MaxBytes`, `MaxMsgs`, `MaxAge`, `Discard`) | Per-stream → bot fills budget → human messages get discarded | Per-class budget; can tune retention per class independently |
| **Stream-level incidents** (snapshot, replication, repair) | Take both classes down together | Affect one class only |
| **Consumer lag isolation** | Per-class durables on shared stream isolate consumer lag only — not write-path or storage concerns | Full isolation top to bottom |
| **Ops surface** | One stream to monitor | Two streams — mechanical, scales linearly (this codebase already runs 5+ streams) |

The "ops cost" of two streams was the only argument for the shared option; it's overstated — stream config is clone-paste YAML in `pkg/stream/stream.go`, alerting templates duplicate trivially. **Trade the small ops cost for clean isolation forever** *(applies at split-time, not Phase 1)*.

Per-class **durables** (not just filters) remain mandatory after split — they isolate consumer lag *within* a stream.

### 6.2 Stream / consumer config

```go
// pkg/stream/stream.go — class-aware constructors
func MessagesCanonicalUser(siteID string) jetstream.StreamConfig {
    return jetstream.StreamConfig{
        Name:     fmt.Sprintf("MESSAGES_CANONICAL_USER_%s", siteID),
        Subjects: []string{ fmt.Sprintf("chat.user.canonical.%s.>", siteID) },
        // ... retention/storage tuned for human load profile ...
    }
}

func MessagesCanonicalBot(siteID string) jetstream.StreamConfig {
    return jetstream.StreamConfig{
        Name:     fmt.Sprintf("MESSAGES_CANONICAL_BOT_%s", siteID),
        Subjects: []string{ fmt.Sprintf("chat.bot.canonical.%s.>", siteID) },
        // ... retention/storage tuned for bot burst profile (e.g. larger MaxBytes) ...
    }
}

// consumer config (per worker, per class) — durable + stream chosen by class
func consumerConfig(svc, class, siteID string) jetstream.ConsumerConfig {
    return jetstream.ConsumerConfig{
        Durable:       fmt.Sprintf("%s-%s", svc, class),
        // No FilterSubject needed — the stream itself is class-scoped
        // ... rest unchanged ...
    }
}
```

Stream bootstrap (CLAUDE.md §6, gated by `BOOTSTRAP_STREAMS`) creates both per-site streams. The legacy `MESSAGES_CANONICAL_{siteID}` constructor is removed.

### 6.3 Rename migration (legacy → `_USER_` + new subject namespace)

Two coupled renames land together: the stream name (`MESSAGES_CANONICAL_{siteID}` → `MESSAGES_CANONICAL_USER_{siteID}`) and the subject prefix (`chat.msg.canonical.…` → `chat.user.canonical.…`, per §2.3). JetStream handles the stream rename via Mirror; the subject rename is coordinated through dual-publish in the broader rollout (Part I §9 Phase 1).

**Sequencing the Mirror with Part I §9 phases — these MUST NOT overlap with publisher dual-publish, or the same canonical event will land in the new USER stream twice (once via Mirror's `chat.msg.canonical.…` → `chat.user.canonical.…` transform, once via the publisher's direct `chat.user.canonical.…` write):**

1. **Phase 0 (Part I §9) — Add the new streams.** Create `MESSAGES_CANONICAL_USER_{siteID}` with `Subjects: ["chat.user.canonical.{siteID}.>"]`. Add `MESSAGES_CANONICAL_BOT_{siteID}` with `Subjects: ["chat.bot.canonical.{siteID}.>"]` at the same time. Publishers still write only to `chat.msg.canonical.…` (old) → land in old stream as before. Both new streams are empty.

2. **Phase 0/1 boundary — Backfill the new USER stream via Mirror, ONE-SHOT.** Briefly enable `Mirror` + `SubjectTransform` on `MESSAGES_CANONICAL_USER_{siteID}` to map `chat.msg.canonical.{siteID}.>` → `chat.user.canonical.{siteID}.>`. Let it catch up the historical lag (within retention). **Then disable the Mirror before Phase 1 starts.** Mirror's purpose was only to seed the new stream with the existing backlog so consumers can replay from it post-cutover — it does NOT stay live during Phase 1's dual-publish.

3. **Phase 1 (dual-publish) — Publishers emit on BOTH old AND new subjects.** With Mirror disabled, dual-publish is the ONLY ingestion path for the new stream. Idempotency: every canonical event carries a stable `messageId` (existing field on the canonical struct) used as the JetStream message ID (`Nats-Msg-Id` header); JetStream's duplicate-detection window (`Duplicates` config, set to ≥ 2 × dual-publish window) drops re-publishes of the same `messageId`. Consumers MUST also be idempotent on `messageId` — they already are for replay/redelivery reasons.

4. **Phases 2-3 (consumer dual-subscribe, split deployments)** — consumers gradually move to the new streams. Old stream still receives traffic from publishers (dual-publish).

5. **Phase 4 (cutover) — Publishers stop emitting on `chat.msg.canonical.…`.** Old stream stops receiving new traffic; new USER+BOT streams are the only writes.

6. **Phase 5 (sunset) — Delete the old stream and its consumers.**

Bot stream `MESSAGES_CANONICAL_BOT_{siteID}` is created fresh in step 1 — no Mirror, no historical backfill (bot canonical events never existed under any previous subject).

**Rollback at any step before Phase 4:** publisher rollback to old-subject-only is safe because the old stream is still ingesting and old consumers are still bound to it. The new USER stream's data is replayable from `messageId` idempotency on the next forward attempt.

**Idempotency key contract:** `chat.msg.canonical.…` events already carry `messageId` (server-assigned ULID, unique per logical event); the rename plus dual-publish reuses this as the JetStream `Nats-Msg-Id` header. Any consumer (existing or new) that processes the same `messageId` twice MUST be safe to no-op — this contract is enforced by message-worker and broadcast-worker's existing replay handling.

---

## 7. NATS supercluster routing

Cross-site federation now happens via the supercluster gateways (no OUTBOX stream — confirmed 2026-06-16). Each site's gateway needs explicit permissions for both class subject spaces:

```hcl
# gateway-{siteID}.conf (illustrative)
gateway {
  name: "siteA"
  authorization {
    permissions {
      publish    { allow: ["chat.user.>", "chat.bot.>", "_INBOX.>"] }
      subscribe  { allow: ["chat.user.>", "chat.bot.>", "_INBOX.>"] }
    }
  }
}
```

**Verification before each rollout phase:** publish a message on `chat.bot.canonical.siteA.test` from siteA, assert it arrives on siteB and is consumed by `broadcast-worker-bot` at siteB (not by the human pool).

`inbox-worker` continues to consume from `INBOX_{siteID}` regardless of class — it's the stream-wide ingress and shouldn't be split.

---

## 8. Deployment manifests

One Deployment per class per split service. Shape:

Example below uses `message-worker` (one of the loud pair that actually splits in Phase 1, per §5.2). The same shape applies to `broadcast-worker` (§5.3). `message-gatekeeper` stays a single Deployment through Phase 4 (§5.1) — its split is deferred and only revisited if shared-gatekeeper saturation shows up in metrics.

```yaml
# message-worker/deploy/deployment-user.yaml (sketch)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: message-worker-user
  labels: { app: message-worker, class: user }
spec:
  replicas: 3
  selector:
    matchLabels: { app: message-worker, class: user }
  template:
    metadata:
      labels: { app: message-worker, class: user }
    spec:
      containers:
        - name: message-worker
          image: message-worker:<tag>   # SAME image as -bot
          env:
            - { name: WORKER_CLASS, value: user }
            - { name: MAX_WORKERS, value: "50" }
            - { name: MONGO_MAX_POOL_SIZE, value: "50" }
            - { name: CASS_MAX_CONNS, value: "100" }
          # ... rest unchanged ...
---
# horizontal-pod-autoscaler (per Deployment)
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: message-worker-user
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: message-worker-user
  minReplicas: 3
  maxReplicas: 50
  metrics:
    - type: Resource
      resource:
        name: cpu
        target: { type: Utilization, averageUtilization: 70 }
```

`message-worker-bot` is the same manifest with `class: bot`, `WORKER_CLASS=bot`, `MAX_WORKERS=200`, `MONGO_MAX_POOL_SIZE=80`, `CASS_MAX_CONNS=200`, `maxReplicas: 200`. Numbers are starting points — tune from per-class metrics (§9).

**Per-pool connection caps to shared stores** (Mongo / Cassandra / Valkey) — sum across both pools must stay within the store's overall limit. Document the math in `docs/deployment.md` follow-up.

---

## 9. Metrics & labels

Every existing metric on the split services gets an `is_bot` label. Examples:

| Metric | Current labels | Adds |
|---|---|---|
| `messages_sent_total` | `siteID`, `event_type` | `is_bot` |
| `request_duration_seconds` | `service`, `method` | `is_bot` |
| `jetstream_consumer_pending` | `stream`, `consumer` | (durable name already encodes class; alert on per-class durables separately) |
| `broadcast_fanout_size` | `siteID` | `is_bot` |
| `cassandra_write_duration_seconds` | `keyspace`, `table` | `is_bot` |
| `mongo_op_duration_seconds` | `collection`, `op` | `is_bot` |

Source the `is_bot` value from `ctxutil.PrincipalClass(ctx)` (§3.2) **once the auth migration has reached Phase 2+** and `principal.class` is propagated end-to-end. **In Phase 0 (Part I §9.1)** — before validate's `class` field has flowed everywhere — derive locally from `model.IsBotAccount(account)` (`pkg/model/account.go:11`) as a temporary fallback. Switch over to the context-sourced value as Phase 2 lights up; from Phase 2 onward, **never re-derive from account ID at metric-emit time**.

**Per-class dashboard** lives in `monitoring/grafana/dashboards/bot-vs-user.json`. Built and merged in Phase 0 (Part I §9) — *before* any split.

---

## 10. Test plan

### 10.1 Unit tests
- `pkg/subject/class_test.go` — class derivation from `model.User`, builder output for each class.
- Per split service: existing handler tests run twice (table-driven `class` parameter), once per class.

### 10.2 Integration tests
- `pkg/testutil` helpers already provide isolated NATS / JetStream containers (CLAUDE.md §4).
- New integration test per split service: publish a `bot` message and a `user` message, assert only the right Deployment's consumer receives.
- Cross-site federation test: publish `chat.bot.canonical.siteA.test` at siteA NATS, assert it arrives at siteB NATS, gated to the bot consumer.

### 10.3 Load tests (`tools/loadgen`)
- New scenario: `bot_burst` — generate 10× baseline bot send rate; assert human-pool p99 stays within SLO.
- New scenario: `cross_class_fanout` — bot in a 10k-member room (some humans, some bots); assert both pools receive at appropriate rates.
- Existing `botroom` scenarios extended with the `is_bot` label assertion.

### 10.4 Rollback drill
- Staged environment exercise — at Phase 4, scale `-bot` Deployments to 0 + re-enable old-subject subscription on `-user`. Verify human traffic continues, bot traffic falls through to `-user`. (US9.)

---

## 11. Resolved decisions (2026-06-16)

All questions previously open are decided. Tracked here as a permanent reference so anyone touching this design can see *why* each shape is what it is.

### Q1 — Stream design ✅ separate streams (§6)
`MESSAGES_CANONICAL_USER_{siteID}` and `MESSAGES_CANONICAL_BOT_{siteID}` — physically separate, not shared-with-filter. Decision drives §6 (Stream design), §5 (per-service tables now reference the class-specific stream by name), and §7 (federation enablement covers both stream names). **Rationale:** shared back-pressure on the write path and storage-budget coupling undermine the isolation thesis; per-class durables alone aren't enough. Trade marginal ops cost (one extra stream per site) for clean isolation top-to-bottom forever.

### Q2 — `class` field on the message envelope ✅ yes (§3.2)
`Class string \`json:"class" bson:"class"\`` on the canonical event struct in `pkg/model`. Mirrors the existing `Timestamp` pattern (CLAUDE.md §6). Authoritative — subjects can be re-published (federation, oplog-transformer); envelope `class` cannot drift.

### Q3 — Room-subject class token ✅ no (sender-side only)
Canonical and user-scoped RPC subjects get a class token. Room-scoped subjects (`chat.room.{roomID}.…`) do NOT. **Rationale:** rooms are mixed (humans + bots co-present); class is a property of the **sender**, not the room. Forcing room subjects to be class-classed would either duplicate broadcasts or make WebSocket subscribers double-subscribe — zero benefit, real complexity.

### Q4 — Threshold for splitting other services ✅ no automatic threshold
Replace any numeric threshold with a **quarterly per-class dashboard review + incident-triggered re-evaluation**. A service becomes a split candidate when ANY of:
- Bot RPS share exceeds 25% sustained over a week
- p99 latency divergence between classes exceeds 50%
- Bot-driven load causes a production incident on the shared service

Becoming a candidate triggers a 1-page "split readiness" doc and a design review. **Splitting is never automatic** — the cost (new Deployment, HPA, runbook, alerts, on-call mental model) deserves a deliberate decision.

### Q5 — Per-pool DB connection caps ✅ no fixed quotas; small per-pod pools + alerts (§8)
Hard quotas between bot/user pools waste budget on one side during the other's bursts. Instead: keep per-pod pools small (Mongo 20–30, Cassandra 50–100, Valkey 1 cluster client per pod), HPA the scaling dimension, and alert when total `connections_in_use` > 70% of cluster budget OR when bot pool consumes > 80% of total store connections. Document the math in `docs/deployment.md`.

### Q6 — Cross-site federation enablement ✅ Phase 0 (early)
Supercluster permissions for `chat.bot.>` (both stream names) added in Part I §9 Phase 0. **Rationale:** permission expansion is idempotent and inert in the absence of traffic — adding early de-risks Phase 1+ from the "messages silently dropped at federation boundary" failure mode.

### Q-admin-class — Three classes or two lanes? ✅ two lanes (§3.1)
The auth `principal.class` enum is `{bot, user, admin}` (auth.md Part II §9.8), but traffic-isolation collapses `{user, admin} → "user"` lane. Admin web-UI traffic lands on `botplatform-service` directly, not on the chat data plane; from message-send / broadcast / persistence, admins are indistinguishable from humans. Subject namespace stays binary.

### Q-legacy-token-class — Bot using a legacy token gets correct class? ✅ yes by construction (§3.1)
The validate response derives `class` from the **principal** (account roles), not the token format. A bot presenting an imported legacy RC token still resolves to `class=bot`. No special handling required.

### Q-fz1-only — Where does the split apply? ✅ fz1/wsp only (Part I §9.2)
Bot-traffic isolation applies only to nextgen deployments in fz1/wsp. Legacy services in fz2/chat are being sunset; splitting there is wasted work. After auth Phase 3 (100% to fz1), fz2 deployments are removed entirely.

### Q-codeaudit — Ground spec in repo ⏳ pending
Still to do, but documented here so it isn't lost. The auth.md Part II §2 has file:line citations against `auth-service/`, `pkg/userstore`, `pkg/model`. This spec needs the same treatment against `pkg/subject`, `pkg/stream`, `pkg/model/events*.go`, and each loud-trio service's `main.go`. Tracked as a follow-up before this spec moves out of draft.

---

## 12. Verification checklist

Before each rollout phase advances:

- [ ] Per-class dashboard healthy for both classes at current weight (SC3).
- [ ] Human-pool p99 unchanged vs prior phase (SC1).
- [ ] Bot pool HPA scaling correctly under synthetic burst (SC2).
- [ ] Cross-site integration test green for both classes (SC4).
- [ ] Rollback drill executed at least once in staging (SC5).
- [ ] `pkg/subject` mirror table reviewed — no new subject added in the prior phase missing its class sibling.
- [ ] Supercluster permissions cover both class namespaces; verified via end-to-end probe.
- [ ] Connection-pool sum across `-user` + `-bot` Deployments within store budget.

---

## 13. References
- **Part I** — Architecture decision, user stories, scope, rollout: above in this file.
- **Companion spec** — [Bot Platform NextGen Auth Migration](./auth.md) (combined Parts I+II+III).
- **Diagrams** — View A (logical traffic separation): https://www.figma.com/board/hScsGyDTbGhT7laIwJsVkx · View B (per-service deployment topology): https://www.figma.com/board/6vkFEKMJ0WyES2VTpBVstM
- **CLAUDE.md** — §1 (per-service organization), §3 (subject naming), §6 (JetStream consumer pattern, supercluster routing).
