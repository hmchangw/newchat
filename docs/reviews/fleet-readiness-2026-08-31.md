# Fleet Production-Readiness Report

**Scope:** all 35 Go services in the monorepo
**Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** the `production_readiness` skill run per service — six independent expert agents each (code quality, architecture, test coverage, maintainability, integration, performance), every finding requiring `file:line` evidence, judged against `CLAUDE.md` and industry practice for Go microservices at scale. Per-service reports live beside this one in `docs/reviews/`.

## How this run was executed

Three steps were computed **once repo-wide** rather than 35 times, because every service would otherwise re-scan an identical repo:

- **`make sast`** — `gosec` **PASS** (0 findings at `-severity medium -confidence low -tests=true`); repo-owned `semgrep` rules **0 findings** across 18 rules / 1,798 files, fixture tests 2/2. **`govulncheck` and the `semgrep` registry packs could not run** — `vuln.go.dev` and `semgrep.dev` are blocked by this environment's egress policy (403 on CONNECT). That is an environmental gap, not a code finding, and it is the one CI gate this audit could not exercise.
- **`make generate`** — produced **zero diff** repo-wide. Mocks are current everywhere. (One caveat found later: `message-worker/mock_hridentity_test.go` has no `//go:generate` directive at all, so `make generate` never visits it — a zero diff there means "not checked", not "up to date".)
- **Coverage** — one `-covermode=atomic` profile over `./...`, sliced per service.

Two `pkg/` tests failed only under the loaded parallel run and **pass in isolation** (`pkg/shutdown`, `pkg/testutil`); they are load-flaky, not broken.

## The headline number

> **34 of 35 services fall below the repo's own 80% coverage floor. 19 are below 60%.**
> **`translation-service` (82.3%) is the only service in the fleet that passes.**

`CLAUDE.md` §4 states the 80% minimum as a merge gate: "code below this threshold MUST NOT be merged." On today's `master` that gate would reject all but one service. This is not 35 independent lapses — Chapter 2 shows it is largely **one root cause repeated 35 times**.

## Fleet scorecard

Scores are the mean of the six dimensions. Coverage is statement-weighted from the shared profile.

| Service | Overall | D1 Qual | D2 Arch | D3 Test | D4 Maint | D5 Integ | D6 Perf | Coverage |
|---|---|---|---|---|---|---|---|---|
| roomlist-worker | **3.7** | 4 | 4 | 2 | 4 | 4 | 4 | 65.9% |
| broadcast-worker | **3.5** | 4 | 4 | 2 | 3 | 4 | 4 | 67.7% |
| user-service | **3.3** | 4 | 4 | 2 | 3 | 3 | 4 | 53.2% |
| room-worker | **3.3** | 4 | 4 | 2 | 2 | 4 | 4 | 62.8% |
| search-service | **3.3** | 4 | 4 | 2 | 3 | 4 | 3 | 66.9% |
| message-gatekeeper | **3.3** | 4 | 4 | 2 | 3 | 3 | 4 | 65.5% |
| media-service | **3.3** | 4 | 4 | 2 | 3 | 4 | 3 | 70.0% |
| history-service | **3.2** | 4 | 4 | 1 | 3 | 3 | 4 | 55.0% |
| room-service | **3.2** | 4 | 3 | 1 | 3 | 4 | 4 | 57.2% |
| admin-service | **3.2** | 4 | 3 | 2 | 3 | 4 | 3 | 68.9% |
| message-worker | **3.2** | 4 | 4 | 1 | 3 | 4 | 3 | 56.8% |
| search-sync-worker | **3.2** | 4 | 4 | 2 | 3 | 2 | 4 | 67.7% |
| notification-worker | **3.2** | 4 | 4 | 1 | 3 | 3 | 4 | 59.0% |
| outbox-worker | **3.2** | 4 | 4 | 1 | 3 | 4 | 3 | 36.9% |
| auth-service | **3.2** | 4 | 3 | 2 | 4 | 3 | 3 | 61.9% |
| upload-service | **3.0** | 4 | 4 | 2 | 3 | 2 | 3 | 76.5% |
| inbox-worker | **2.8** | 4 | 3 | 1 | 3 | 3 | 3 | 44.1% |
| botplatform-service | **2.8** | 3 | 3 | 1 | 4 | 3 | 3 | 56.5% |
| bot-room-service | **2.8** | 4 | 3 | 1 | 3 | 3 | 3 | 49.0% |
| user-presence-service | **2.8** | 4 | 3 | 1 | 3 | 3 | 3 | 45.1% |

**Coverage for the remaining 15** (audits completing; coverage is measured and final): translation-service **82.3%** ✅ · teams-room-verify 78.9% · client-update-service 76.8% · tcard-service 69.7% · teams-chat-sync 67.6% · teams-chat-member-sync 60.3% · portal-service 58.6% · teams-hr-sync 57.5% · teams-room-creation 55.9% · teams-user-sync 53.4% · teams-room-inspector 47.7% · bot-message-handler 40.9% · push-notification-service 26.9% · hr-sync-worker 21.1% · **bot-message-worker 13.6%** (fleet low).

### What the scores say

No service scored below 2.8 and none above 3.7 — **the fleet is uniformly mid-band**, which is itself the finding. The code *inside* functions is consistently good: D1 (code quality) averages ~3.9 and never drops below 3. What drags every service down is the same three things — **untested wiring, shared state configured per-service instead of once, and cross-service contracts asserted by comment rather than by compiler or test.**

---

## 2. Cross-cutting risk #1 — the coverage floor has one root cause, not 35

34 services miss the 80% floor. It would be easy to read that as 35 teams each being slightly lax. **The per-service evidence says otherwise: in service after service, the deficit is concentrated in one untestable `main()`.**

| Service | `main.go` coverage | Share of the service's *entire* coverage deficit |
|---|---|---|
| message-gatekeeper | **2.4%** (126 stmts) | the whole gap — excluding `main.go` the package is **~91%** |
| notification-worker | **2.0%** (196 stmts) | essentially all of it — `handler.go` is **96.7%** |
| broadcast-worker | **3.2%** (217 stmts) | `handler.go` 88.0%, `preview_writer.go` 97.1% |
| search-sync-worker | **0%** (322 lines) | **165 of 241** uncovered statements; rest of service is 80–100% |
| message-worker | **7.2%** (181 stmts) | **~46%** of the deficit; `handler.go` is **95.1%** |
| outbox-worker | **0%** | **89 of 141** uncovered statements; `handler.go`+`bootstrap.go` are **100%** |
| roomlist-worker | **0%** | **83 of 95** uncovered; excluding it the service is **93.9%** |
| auth-service | **0%** (61 stmts) | the whole gap — `handler.go` is **~93%** |
| upload-service | **0%** (70 stmts) | 14.4% of the service; excluding it and the integration-only stores, **95.3%** |
| room-service | **8.1%** | with `store_mongo.go` (3.4%, integration-only), the two are the entire gap |
| inbox-worker | **0%** | plus the **whole 30-method Mongo store inlined into `main.go`** |

**The pattern is unmistakable and the fix is one refactor, repeated.** These `main()` functions are not boilerplate — they contain real decisions that nothing tests:

- **message-gatekeeper**: the `shutdown.Wait` closure that converts a `wg.Wait()` overrun into `worker drain timed out`.
- **notification-worker**: the migrated-event **Ack-drop branch** — an inverted condition there re-notifies every migrated message and no test would fail.
- **outbox-worker**: the message-disposition closure (panic→Ack, permanent→Ack, transient→NakWithDelay) **and the only site that sets `jetstream.WithMsgID`** — deleting the cross-site idempotency guarantee would fail no test in the repo.
- **search-sync-worker**: nine `os.Exit(1)` config gates, and **the INBOX/HR non-ownership guard** — the service's most load-bearing architectural rule, enforced by two locals compared by name inside `main()`, with zero tests.
- **inbox-worker**: the two-lane dispatcher whose FIFO-vs-fan-out routing is the subject of cross-cutting risk #3 — asserted by a 20-line comment and nothing else.
- **auth-service**: every key-material guard (signing key must be account-type, OIDC issuer required when `DEV_MODE=false`).

Three services already show the fix in-repo: `broadcast-worker` extracts `guardedProcessor` (100% covered), `bot-message-worker/main.go:66` uses a `run(ctx) error` split, and several extract `buildConsumerConfig`. **The rest inline it.**

A second, smaller contributor is worth naming because it makes the numbers *look* worse than reality: **generated mocks and integration-only stores sit in the coverage denominator.** `user-service/service/mocks` alone is **306 uncovered statements — 13.3% of that service's denominator** — and no amount of real testing will ever move it. `history-service/internal/service/mocks` is another 385 statements (15%). Excluding generated code, user-service is ~72.7% rather than 53.2%, and history-service ~64.7% rather than 55.0%. **Still failing — but the gap becomes actionable rather than demoralising.**

### Fleet recommendations

- `critical` — **Adopt a `run(ctx, cfg) error` seam as a repo convention** and apply it service by service, keeping `os.Exit` only in `main`. Extract three things consistently: `validateConfig(cfg) error`, the consumer/handler wiring, and the per-message disposition closure. On the evidence above this single refactor moves most services from the 50–68% band into the high 70s–80s **without writing a single vanity test**, and it makes the genuinely dangerous branches (Ack-drop, dedup ID, drain timeout, ownership guards) testable for the first time.
- `high` — **Exclude generated mocks from the coverage denominator** repo-wide (build tag or `-coverpkg` filter), and add a `-tags=integration` coverage target so store layers that *are* tested stop reading as 0%.
- `high` — Once both are done, re-baseline and **enforce the 80% gate in CI**. It is currently a rule the whole fleet violates, which means it is not a rule.

---

## 3. Cross-cutting risk #2 — shared knobs re-declared per service

`CLAUDE.md` §6 is unusually explicit: *"A knob shared by more than one service is declared once, in the package that owns the thing it configures, and mounted as a named field… Never re-declare the env tag and `envDefault` in a service — that is how two services reading the same Valkey key end up on different TTLs, which the tag-level default cannot prevent."*

**This rule is violated by at least nine distinct knobs across the fleet**, and the experts found it independently in twelve services:

| Knob | Services re-declaring it | Owning package that should hold it | Consequence if they drift |
|---|---|---|---|
| `ROOM_KEY_RETIRED_TTL` / `ROOM_KEY_GRACE_PERIOD` | room-service, room-worker, bot-room-service, broadcast-worker | `roomkeystore` (exports no config type) | `CLAUDE.md` states it directly: a short TTL expires versions peers still consider resolvable, and **`key.get` then permanently fails for messages already on the wire** |
| `USER_CACHE_SIZE` / `USER_CACHE_TTL` | message-worker, message-gatekeeper, broadcast-worker, room-worker, notification-worker, history-service, user-presence-service (**7**) | `userstore` — **which already has the correct `TTLConfig` for the L2 tier one line below** | Two services front the same user data with different L1 lifetimes |
| `BADGE_CACHE_TTL` | inbox-worker, room-service, user-service | `badgecache` (exports no config type) | **inbox-worker writes badge state that user-service reads** — a divergent TTL is a live coherence bug |
| `ROOM_META_CACHE_TTL` | room-worker (**60 s**), broadcast-worker / message-gatekeeper / notification-worker (**2 m**) | `roommetacache` (has the L2 `TTLConfig`) | Already divergent today — per-process L1 so not a shared-key bug, but exactly the drift the rule exists to stop |
| `ADMIN_ACCT_PREFIX` | search-sync-worker, room-service, message-gatekeeper, broadcast-worker, media-service (**5**) | `pkg/model` (owns `SetPlatformAdminAccountPrefix`) | **A prefix mismatch mis-classifies platform admins** |
| `SESSIONS_MAX_PER_ACCOUNT`, `BCRYPT_COST` | admin-service, botplatform-service | `session` / `pwhash` | Both write the **same `sessions` collection**; drifting caps mean one service evicts sessions the other still honours |
| MinIO endpoint / keys / `MINIO_DOWNLOAD_TIMEOUT` | upload-service, client-update-service, media-service | `minioutil` (**has no `Config` type at all**) | Three services, one object store, three sets of defaults |
| `VALKEY_ADDRS` / `VALKEY_PASSWORD` | `user-presence-service/sync` hand-dials instead of `valkeyutil` | `valkeyutil` | The sync's Valkey traffic is **uninstrumented** and carries a different dial policy against the same keyspace — and `presencestore`'s own comment says this duplication *was already removed once* |
| `PRESENCE_STALE_THRESHOLD` / `PRESENCE_CONNS_TTL` | user-presence-service **and** its own `sync/` sibling | `presencestore` | Both feed the **same Lua script against the same keys**; an operator override moves one and not the other |

Two of these have already drawn blood in the deploy layer rather than in Go:

- **`bot-room-service/deploy/docker-compose.yml:21` hardcodes `ROOM_KEY_RETIRED_TTL=30m`** while its three peers all use `${ROOM_KEY_RETIRED_TTL:-30m}`. The Go defaults agree, so this looks fine — but **an operator raising the fleet value moves three services and silently leaves the fourth short**, producing exactly the permanent `key.get` failure `CLAUDE.md` warns about. Three separate experts flagged this independently.
- **`room-worker/deploy/teams/` is a diverged fork, not a variant**: its Dockerfile is byte-identical to the default, and its compose *drops* `ROOM_SUBJECT_MODE`, `ROOM_KEY_RETIRED_TTL`, `MONGO_KEY_READ_PREFERENCE` and the entire `ATREST_*`/`VAULT_*` block while hardcoding values the default parameterises.

The tell that this is a systemic gap rather than carelessness: **several services get it exactly right in the same struct where they get it wrong.** message-worker mounts `UserL2 userstore.TTLConfig` correctly one line below its hand-rolled `USER_CACHE_*`. inbox-worker mounts `mongoutil.PoolConfig` and `valkeyutil.Config` correctly beside its re-declared `BADGE_CACHE_TTL`. user-service mounts `Pool mongoutil.PoolConfig` on the NATS path and hand-rolls it on the HTTP path — **and that one has a live cost: the hand-rolled copy drops `ServerSelectionTimeout`, so the transport serving the client-facing sidebar keeps the driver's 30 s default, the exact hang `poolconfig.go` exists to prevent.** The justifying comment there is factually wrong about why the shared type could not be used.

### Fleet recommendations

- `high` — **Add the missing config types**: `roomkeystore.TTLConfig`, `badgecache.TTLConfig`, `userstore.CacheConfig` (L1, beside the existing L2 `TTLConfig`), `minioutil.Config`, `presencestore.TTLConfig`, `session.CapConfig`, `pwhash.Config`, and an admin-prefix config in `pkg/model`. Then delete the per-service declarations in one PR per knob. **Nine PRs, each mechanical, each closing a class of silent divergence.**
- `high` — Fix `bot-room-service/deploy/docker-compose.yml:21` to `${ROOM_KEY_RETIRED_TTL:-30m}` **now** — it is a one-character-class change guarding a documented permanent-failure mode.
- `medium` — Rebuild `room-worker/deploy/teams/` from the default deploy with only `MODE` and `OTEL_SERVICE_NAME` overridden.
- `medium` — Add a repo-owned semgrep rule (with fixture, per §2) that flags an `env:"…"` tag whose name already appears in a `pkg/` config struct. This is the mechanical guard that makes the rule self-enforcing rather than review-enforced.

---

## 4. Cross-cutting risk #3 — federation guarantees that break at the seams

The federation architecture is genuinely well designed. The OUTBOX partition is airtight: the two filter sets in `pkg/outbox` are **provably disjoint** (there is a test), they **jointly cover all 16 event types any producer emits**, `outbox.Publish` **rejects any type outside the partition** so a gap cannot go silent, and consumer `FilterSubjects` are built from the same slices so adding a type auto-creates its lane. Subjects come from `pkg/subject` builders everywhere — **zero raw `fmt.Sprintf` subject construction outside `pkg/subject` in the entire fleet.**

**The design is right. The seams are where it fails.**

### 4.1 The ordering guarantee the origin pays for is discarded at the destination

This is the single most consequential systemic finding in the audit.

`room_renamed` is in `pkg/outbox.OrderedEventTypes`. It rides the per-destination FIFO lane at `MaxAckPending=1` — deliberately capping that lane's throughput to one in-flight message — for a stated reason: *"so a `room_renamed` can't overtake the `member_added` that creates the subscription it renames."*

**At the destination, `inbox-worker`'s `isMembershipSubject` routes only `member_added` and `member_removed` to the sequential lane.** `room_renamed` goes to the `MaxWorkers` fan-out pool and is processed **concurrently with an in-flight `member_added`** (`inbox-worker/main.go:1032-1035`).

And the stranding is **permanent, not transient**: `UpdateSubscriptionNamesForRoom` is an `UpdateMany` over *existing* subscriptions, so a rename applied before the subscription exists matches zero documents; `handleMemberAdded` then writes the stale `event.RoomName`, and **no later event corrects it**.

The same chapter found a second instance of the same class: **`subscription_opened` is applied with no high-water-mark guard**, even though `pkg/outbox.ConcurrentEventTypes` justifies concurrent forwarding *on the explicit claim* that "inbox-worker applies them under high-water-mark / idempotent-upsert guards." Every sibling handler (mute, favorite, section_moved, role, rename, restrict) carries the `$lt` guard. This one does not — and `SubscriptionOpenedEvent.Timestamp` **already exists on the wire and is simply ignored**. A reordered hide→reopen pair leaves the room permanently in the wrong state.

**The lesson generalises: the origin-side partition is enforced by code (`outbox.Publish` rejects unknown types), while the destination-side lane split is a hand-maintained list of two subjects.** One side is mechanical; the other is a comment.

### 4.2 Contracts that do not exist, or do not match

Five cross-service contracts were found to be broken, undocumented, or unimplemented — each verified from both sides:

| Contract | State | Evidence |
|---|---|---|
| **notification-worker → user-presence-service** bulk presence | **No responder exists anywhere in the repo.** The worker requests `chat.presence.{siteID}.request.snapshot`; the presence service registers only `query.batch`/`query.batch.peer`. The payloads are also incompatible (`PresenceSnapshotReply` vs `PresenceQueryResponse`), **and** the batch sizes disagree (worker chunks at 512, service hard-rejects above 100). Three independent incompatibilities. Masked today only because `PRESENCE_RPC_ENABLED` defaults false — **flipping that flag yields a 2 s timeout per chunk and fail-open on every message**, so DND/in-call suppression never engages | `notification-worker/presence.go:61`; `user-presence-service/main.go:172-178` |
| **search-sync-worker bot-message lane** | **`critical`.** The collection binds `BOT-MESSAGES-CANONICAL` (subjects `chat.bot.canonical.…`) but its consumer filter is `chat.msg.canonical.…` — **not a subset of its own stream**, so `CreateOrUpdateConsumer` is rejected and the pod exits 1 in default mode; if accepted, no bot message would ever be indexed. **A unit test asserts the wrong filter**, so CI cannot catch it | `search-sync-worker/messages.go:129`, `:74-80`; `messages_test.go:622` |
| **botplatform-service member endpoints** | The code registers `POST …/members/add` and `…/members/remove`; **`docs/client-api.md` publishes `POST`/`DELETE …/members`.** Any SDK written against the doc gets a 404 on both. The derived view also drops all five bot endpoints and says "Emits: None — HTTP-only" | `botplatform-service/routes.go:69-73` vs `docs/client-api.md:8584` |
| **`BotIdentity` enrichment** | `AppID`/`AppName`/`EngName`/`ChineseName` are read downstream (`bot-room-service` persists them as room-owner identity; `bot-message-handler` derives display names) but **botplatform-service — the only producer — never populates them.** Every bot room stores an empty owner app identity | `botplatform-service/bot_forwarder.go:62` vs `bot-room-service/handler.go:199` |
| **auth-service JWT scope** | `docs/client-api.md:200` states scope "is derived server-side from the principal's roles (admin > bot > user)". **It is not** — `signNATSJWT` stamps only `account:<name>`, and bots, admins and SSO users all receive the identical `scoped_user` template. Clients are told a security property the server does not provide | `auth-service/handler.go:321`; `pkg/principal/principal.go` |

### 4.3 A third federation lane nobody documented

`broadcast-worker` publishes cross-site room-activity as a **fire-and-forget core-NATS publish to `chat.roomactivity.{destSiteID}`** — bypassing both OUTBOX and INBOX. `CLAUDE.md`'s federation model has exactly two lanes. This one has **no stream, no retry, no ack**, and if ops/IaC never exports the subject across the gateway the feature is **silently dead**: the local publish succeeds and nothing arrives. Nothing in `docs/` describes the subject.

Meanwhile the destination side of that lane writes to `remote_rooms` — a collection `inbox-worker` upserts and deletes, `pkg/model` documents inbox-worker as owning, and **which a repo-wide grep finds no reader for.** The whole activity-refresh lane (core-NATS subscriber, a 110-line cache, three store methods, its own config) currently feeds nothing.

### 4.4 Destination-axis validation is missing where event-type validation exists

`pkg/outbox.Publish` guards the **event-type** axis: publish an unpartitioned type and it is rejected. **Nothing guards the destination axis.** `broadcast-worker/federateMentions` derives `destSiteID` straight from `participant.SiteID` with no check against `cfg.AllSiteIDs` — which is already parsed and sitting unused two files away — so a stale or decommissioned site ID publishes to a subject **no `outbox-worker` consumer filters on**, and the event sits in OUTBOX until retention deletes it.

The mirror-image failure exists in `outbox-worker` itself: **an unset `ALL_SITE_IDS` creates zero consumers**, so the sole OUTBOX owner runs as a **no-op while producers keep filling the stream** — and the health check reports green, because it only checks the NATS connection. The shipped compose default collapses to exactly that case.

### Fleet recommendations

- `critical` — **Derive `inbox-worker`'s `isMembershipSubject` from `pkg/outbox.OrderedEventTypes`** rather than a hardcoded pair, and add a test asserting the two sets agree. This makes the destination lane split mechanical, like the origin partition already is.
- `critical` — Guard `UpdateSubscriptionOpen` with an `openUpdatedAt` `$lt` from the event's own `Timestamp`, matching its six sibling handlers.
- `critical` — Fix the search-sync-worker bot-lane filter **and the test that enshrines it**, then add a table test asserting **every collection's `FilterSubjects` ⊆ its own `StreamConfig.Subjects`**.
- `high` — Resolve the presence contract: implement the snapshot handler **or** repoint notification-worker at `PresenceQueryBatch`, reconcile the 512-vs-100 batch limits, and add a test that a request the worker builds is accepted by the service.
- `high` — Validate `destSiteID` against the configured peer set in `broadcast-worker`, and make an empty `ALL_SITE_IDS` in `outbox-worker` a startup failure or a red readiness probe.
- `high` — Reconcile the botplatform route/doc mismatch and the auth-service scope claim; both are published contracts that are currently wrong.
- `medium` — Either document `chat.roomactivity.{destSiteID}` in `CLAUDE.md` as an explicitly best-effort third lane with its gateway-export requirement, or move it onto a durable lane. Decide `remote_rooms`' fate in the same change.
- `medium` — Add `bot-room-service` and `admin-service` to `CLAUDE.md`'s cross-site-publisher lists; both federate today and neither is named.

---

## 5. Cross-cutting risk #4 — security

`gosec` and the 18 repo-owned `semgrep` rules are **clean across the whole repo**. Every finding below came from an expert *reading* the code, not from a scanner — which is exactly the class of defect scanners miss.

### The one to fix today

- **`critical` — Read-SSRF with credential exfiltration in `upload-service`.** `drive_host` is taken **verbatim from the client query string** and used as the upstream base URL, **with the Drive `api-token` header attached** (`upload-service/handler.go:342-358` → `pkg/drive/uploader.go:136-140`). Any authenticated room member can point it at an attacker-controlled host and receive `DRIVE_API_TOKEN` (and `LEGACY_DRIVE_API_TOKEN` via `/api/v3`); the response body is then streamed back, making it a **full read-SSRF into the cluster**. The allowlist to validate against — `cfg.Drive.BaseURLMap` — **already exists in-process and is never consulted.**

### Revocation that does not revoke

- **`high` — `admin-service` session revocation leaves the cache warm.** Three of the four revoke paths delete sessions in Mongo but never bust the Valkey session cache: admin set-password, self-service change-password, and the deactivate branch. **Four of the six experts found this independently.** `pkg/session` was explicitly redesigned to close this exact hole — its bulk deletes return IDs *because* "returning only a count is what let a revoked token keep authenticating from cache until its refresh window elapsed" — and this service's store interface **re-opens it by returning only `error`**. Compounding it: `SESSION_CACHE_TTL` defaults to 90 minutes **and slides on read**, so a revoked or deactivated token keeps authenticating **indefinitely while in active use**. The service's own test asserts the invariant for the other two paths.

### The auth boundary

`auth-service` is the callout every client connection traverses. Three findings there:

- **`high` — the auth-bypass branch ships in the production binary**, gated only by `DEV_MODE` (`envDefault:"false"`). `handleDevAuth` mints a signed, scoped NATS user JWT from a **fully client-supplied `account`** with no token and no validation. There is no build tag, no separate binary, and no startup refusal when the signing key looks production-shaped — **a single config typo is fleet-wide impersonation.**
- **`high` — a guaranteed nil-panic on the dev path**: dev mode wires a `nil` `TokenValidator`, but `HandleAuth` still routes a token-carrying request into `handleSSO`, which dereferences it. The doc comment asserts the opposite, and **the only test covering that claim injects a non-nil fake — a configuration `main.go` can never produce.** Its sibling `handleSession` has exactly the guard `handleSSO` lacks.
- **`high` — no admission control on the unauthenticated `POST /api/v1/auth`**, and **unbounded JWKS refetch**: go-oidc's `RemoteKeySet` has no minimum refresh interval, so a caller submitting valid-looking JWTs with random `kid` values drives a continuous 10 s-timeout fetch loop against the IdP, at zero attacker cost, with nothing throttling submission rate. `ginutil.MaxConcurrency` exists and `user-service` already uses it.

### Unauthenticated CPU burn

The same shape appears three times, each on a login endpoint:

- `admin-service`: `POST /v1/login` runs **unconditional bcrypt (cost 10) on every request including both denial arms**, with no limiter installed.
- `botplatform-service`: `/api/v1/login` has **no rate limiter** and full bcrypt per request; separately, its `botRateLimit` runs **after** `requireBot`, so **an invalid token is never rate-limited** and each bogus request is one uncapped Mongo `FindOne`.
- `botplatform-service` again: the idempotency middleware does `io.ReadAll(c.Request.Body)` with **no size cap**, and it runs *before* the handler — so `bindStrict`'s documented `MaxBytesReader` cap **can never reject during read**. The in-code comment claiming "body is capped so oversized requests fail during read" is **false whenever Valkey is configured, i.e. in production.**

### Data exposure

- **`medium` — `search-sync-worker` logs message bodies.** The failed-bulk-item log emits Elasticsearch's raw `error.reason`, which routinely quotes the offending field value (`Preview of field's value: '…'`) — and for the messages collection that field is `content`. This violates §3 *and* the rule the same file states 130 lines above it: *"the document body never belongs in an error that reaches the server log."* `ErrorType` and `Status` are already logged and carry the diagnosis.
- **`medium` — `inbox-worker` decodes whole `model.User` documents** — including `Services`, which carries credential material — on the sequential membership lane, to read two fields.
- **`medium` — `media-service` caches errors publicly.** `Cache-Control: public, max-age=21600` plus `ETag` are written **before** the blob fetch, so a MinIO 500 or a not-found inherits them and becomes a **shared-cache-storable error**, pinned in CDN and browser caches for six hours per key.
- **`medium` — `media-service` authorization is asymmetric**: HTTP emoji upload is admin-gated, but the NATS `emoji.delete` RPC is open to any authenticated account, guarded only by a kill-switch — for site-wide shared state the handler's own comment identifies as such.
- **`medium` — `upload-service` type filtering is bypassable.** `resolveMediaType` returns the **client-declared** Content-Type whenever it is anything but empty or `application/octet-stream`, so the byte-sniff and SVG checks never run for a lying client. A POST declaring `image/png` on SVG bytes sails past the `image/svg+xml` blacklist. **Every SVG-defence test declares `application/octet-stream`; the lying-declared case is untested.** Separately, `/upload/images` never consults the MIME filter at all.
- **`medium` — image decompression bombs**: `media-service`'s bot-avatar upload calls `image.Decode` with **no `DecodeConfig` dimension pre-check**, so a small compressed PNG declaring huge dimensions allocates the full raster. The emoji path in the same service does this correctly in two phases, with an explicit decompression-bomb comment — the avatar path has no `MAX_DIMENSION` knob at all.

### The audit gap

`govulncheck` and the `semgrep` registry packs could not run. **This is the highest-value scan for exactly the services above** — `auth-service`'s `go-oidc`/`nkeys`/`jwt/v2` tree, and the `minio-go`/`resty`/`gin` trees in the upload path. Nothing in this audit clears them.

### Fleet recommendations

- `critical` — Validate `drive_host` against `cfg.Drive.BaseURLMap` before use, or better, drop the parameter and re-derive the host from the room's `siteID` as the upload path already does.
- `high` — Return revoked session IDs from `admin-service`'s two transactional revoke methods and call `sessioncache.BustMany` at all three sites.
- `high` — Move `handleDevAuth` behind a `//go:build dev` file so it is not linked into release images; add the missing nil guard to `handleSSO`; install `ginutil.MaxConcurrency` on `/api/v1/auth`, `admin-service`'s `/v1/login` and `botplatform-service`'s `/api/v1/login`; move `botRateLimit` ahead of `requireBot`.
- `high` — Cap the idempotency middleware's body read; make `resolveMediaType` decide from bytes and extension with the declared type as a tiebreak only, and run `/upload/images` through the same filter.
- `medium` — Drop the raw ES `error.reason` from the bulk-failure log; project `FindUsersByAccounts`; move `setImageCacheHeaders` after a successful blob fetch and emit `no-store` on error paths; gate `emoji.delete` on admin; add a `DecodeConfig` dimension pre-check to the avatar upload.
- `high` — **Run `make sast` (govulncheck + registry packs) from a network-permitted runner before any of this ships.** It is the one blocking CI gate this audit could not exercise.


---

## 6. Complete fleet scorecard — all 35 services

All 35 audits are now complete. This table supersedes the partial scorecard in Chapter 1; nothing in it changed the cross-cutting findings, which were already robust across the first 20.

| Rank | Service | Overall | D1 Qual | D2 Arch | D3 Test | D4 Maint | D5 Integ | D6 Perf | Coverage | crit | high |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `roomlist-worker` | **3.7** | 4 | 4 | 2 | 4 | 4 | 4 | 65.9% | 0 | 1 |
| 2 | `translation-service` | **3.7** | 4 | 3 | 4 | 4 | 4 | 3 | **82.3%** ✅ | 0 | 3 |
| 3 | `broadcast-worker` | **3.5** | 4 | 4 | 2 | 3 | 4 | 4 | 67.7% | 0 | 5 |
| 4 | `tcard-service` | **3.5** | 4 | 4 | 2 | 4 | 4 | 3 | 69.7% | 0 | 4 |
| 5 | `teams-chat-sync` | **3.5** | 4 | 4 | 2 | 4 | 4 | 3 | 67.6% | 0 | 3 |
| 6 | `teams-room-creation` | **3.5** | 4 | 4 | 1 | 4 | 4 | 4 | 55.9% | 1 | 1 |
| 7 | `teams-room-verify` | **3.5** | 4 | 4 | 2 | 4 | 4 | 3 | 78.9% | 0 | 2 |
| 8 | `client-update-service` | **3.3** | 4 | 4 | 2 | 4 | 3 | 3 | 76.8% | 0 | 4 |
| 9 | `media-service` | **3.3** | 4 | 4 | 2 | 3 | 4 | 3 | 70.0% | 0 | 3 |
| 10 | `message-gatekeeper` | **3.3** | 4 | 4 | 2 | 3 | 3 | 4 | 65.5% | 0 | 7 |
| 11 | `room-worker` | **3.3** | 4 | 4 | 2 | 2 | 4 | 4 | 62.8% | 0 | 9 |
| 12 | `search-service` | **3.3** | 4 | 4 | 2 | 3 | 4 | 3 | 66.9% | 0 | 11 |
| 13 | `teams-room-inspector` | **3.3** | 4 | 4 | 1 | 4 | 4 | 3 | 47.7% | 1 | 1 |
| 14 | `user-service` | **3.3** | 4 | 4 | 2 | 3 | 3 | 4 | 53.2% | 1 | 6 |
| 15 | `admin-service` | **3.2** | 4 | 3 | 2 | 3 | 4 | 3 | 68.9% | 0 | 10 |
| 16 | `auth-service` | **3.2** | 4 | 3 | 2 | 4 | 3 | 3 | 61.9% | 0 | 12 |
| 17 | `bot-message-handler` | **3.2** | 4 | 4 | 1 | 4 | 3 | 3 | 40.9% | 2 | 4 |
| 18 | `history-service` | **3.2** | 4 | 4 | 1 | 3 | 3 | 4 | 55.0% | 1 | 6 |
| 19 | `message-worker` | **3.2** | 4 | 4 | 1 | 3 | 4 | 3 | 56.8% | 1 | 9 |
| 20 | `notification-worker` | **3.2** | 4 | 4 | 1 | 3 | 3 | 4 | 59.0% | 2 | 12 |
| 21 | `outbox-worker` | **3.2** | 4 | 4 | 1 | 3 | 4 | 3 | 36.9% | 1 | 8 |
| 22 | `portal-service` | **3.2** | 4 | 4 | 1 | 4 | 3 | 3 | 58.6% | 1 | 2 |
| 23 | `room-service` | **3.2** | 4 | 3 | 1 | 3 | 4 | 4 | 57.2% | 1 | 10 |
| 24 | `search-sync-worker` | **3.2** | 4 | 4 | 2 | 3 | 2 | 4 | 67.7% | 1 | 12 |
| 25 | `teams-chat-member-sync` | **3.2** | 4 | 4 | 2 | 4 | 2 | 3 | 60.3% | 0 | 4 |
| 26 | `teams-user-sync` | **3.2** | 4 | 4 | 1 | 4 | 3 | 3 | 53.4% | 1 | 5 |
| 27 | `upload-service` | **3.0** | 4 | 4 | 2 | 3 | 2 | 3 | 76.5% | 1 | 7 |
| 28 | `bot-room-service` | **2.8** | 4 | 3 | 1 | 3 | 3 | 3 | 49.0% | 1 | 12 |
| 29 | `botplatform-service` | **2.8** | 3 | 3 | 1 | 4 | 3 | 3 | 56.5% | 1 | 14 |
| 30 | `hr-sync-worker` | **2.8** | 4 | 3 | 1 | 3 | 3 | 3 | 21.1% | 1 | 6 |
| 31 | `inbox-worker` | **2.8** | 4 | 3 | 1 | 3 | 3 | 3 | 44.1% | 1 | 11 |
| 32 | `teams-hr-sync` | **2.8** | 4 | 4 | 1 | 3 | 3 | 2 | 57.5% | 2 | 5 |
| 33 | `user-presence-service` | **2.8** | 4 | 3 | 1 | 3 | 3 | 3 | 45.1% | 1 | 14 |
| 34 | `push-notification-service` | **2.7** | 3 | 2 | 1 | 4 | 3 | 3 | 26.9% | 1 | 6 |
| 35 | `bot-message-worker` | **2.5** | 3 | 3 | 1 | 3 | 2 | 3 | **13.6%** | 1 | 10 |

**Fleet mean: 3.2 / 5.** Range 2.5–3.7. Mean coverage **58.1%**.

### Finding totals across all 210 expert reports

| Severity | critical | high | medium | low | nitpick | **Total** |
|---|---|---|---|---|---|---|
| Count | **24** | **239** | 514 | 470 | 194 | **1,441** |

### What the completed picture confirms

**The band did not widen.** With all 35 in, the spread is 2.5 to 3.7 — narrower than most fleets of this size, and the shape of the distribution is the finding. Nobody is failing; nobody is excellent. Every service is competent code held back by the same three things.

**Dimension-by-dimension, the fleet is bimodal.** D1 (code quality) averages **3.9** and drops below 4 exactly three times. D2 (architecture) averages **3.6**. D4 (maintainability) **3.4**, D5 (integration) **3.4**, D6 (performance) **3.3**. And D3 (test coverage) averages **1.5** — sixteen services floored at 1, seventeen at 2, one at 4. *The inside of the functions is good. The wiring around them is untested, and the contracts between them are unenforced.*

**The three lowest scores share a shape.** `bot-message-worker` (2.5), `push-notification-service` (2.7) and `user-presence-service` (2.8) are all services where a **capability the name promises is partially or wholly unwired**: an unimplemented Cassandra write-timestamp contract, a dispatcher that logs instead of dispatching, and an RPC responder that does not exist. In each case the code that *is* there is fine. What is missing is not quality — it is completion, and in two of the three cases nothing in the service says so out loud.

**The two highest scores share the opposite shape.** `roomlist-worker` (3.7) and `translation-service` (3.7) both have small, dependency-injected, `main()`-light surfaces. `translation-service` is the fleet's only service over the coverage floor, and it is not the smallest or the simplest — it is the one whose logic is not trapped behind wiring. That is the entire argument of Chapter 2, visible as a single data point.

**Where coverage and score diverge, read the score.** `teams-room-creation` scores 3.5 at 55.9% coverage; `upload-service` scores 3.0 at 76.5%. Coverage measures how much of a service is exercised; it says nothing about whether the service is doing the right thing. `upload-service` carries the fleet's one genuinely `critical` security finding **in well-tested code**.

---

## 7. Cross-cutting risk #5 — `CLAUDE.md` and the docs have drifted from the code

`CLAUDE.md` is binding project law, and every one of the 210 expert agents read it in full before judging anything. That makes it unusually well-audited — and it turned up **wrong in places the code depends on**, alongside a broader pattern of documentation that describes a system slightly different from the one that runs.

This chapter is separate from the others because its failure mode is different. A bug is found by the person it bites. **A wrong document is found by the person who trusted it** — and by then they have already written the code.

### 7.1 `CLAUDE.md` states a risk assessment that is factually wrong

The Cassandra write-timestamp section says of `bot-message-worker`: *"Its exposure is much smaller — the repo-default `MaxDeliver=6` (~2.6 min, no outage retry budget) and no failure point after the Cassandra commit in its handler."*

Both halves are wrong, and the agent that checked verified each independently:

- **There *is* a failure point after the commit.** `countAndSetParentTcount` runs *after* `ExecuteBatch` commits and can fail on the partition scan or either UPDATE, returning a transient error that NAKs and replays an already-committed create (`bot-message-worker/store_cassandra.go:186`, `:241`, `:246-270`).
- **The window is ~12.6 minutes, not ~2.6.** The handler NAKs on `jsretry.DefaultBackoff`, not the bare `AckWait` schedule the note assumes.

This matters because that assessment is **the stated justification for leaving the service unpinned**. The document's own argument for deferring the fix rests on two facts that are not true, and the deferral has held.

### 7.2 Producer and consumer lists in `CLAUDE.md` are incomplete

`bot-room-service` is a **fifth OUTBOX producer**, publishing `member_added`/`member_removed` through `outbox.Publish` on the per-destination FIFO lane (`bot-room-service/handler.go:676`, `:690`). CLAUDE.md's JetStream Streams section and `pkg/outbox/outbox.go:2` both enumerate only room-service, room-worker, message-worker and broadcast-worker.

Its events *do* partition correctly, so nothing is stranded today — but this was found **independently by the OUTBOX owner's audit and by bot-room-service's own**, which is how federation doc drift usually surfaces: two services disagree about who is on the contract. The `chat.roomactivity` third lane in Chapter 4 is the same pattern one step further along, where the undocumented lane has no owner at all.

### 7.3 Service READMEs describe services that no longer exist

`teams-hr-sync/README.md` presents itself as the contract for *"an external persister [that] can replace this worker"*, and is materially wrong in four places:

- it points the direct-write surface at **`pkg/hrstore` — a package that does not exist** (and `write_store.go:20-21` says the opposite: "Owned by this service … there is no shared store package");
- it claims the diff is scoped `source:"teams"`, while the query is an unfiltered `bson.M{}` and `model.IEmployee` **has no source field at all**;
- it documents a `Source` field and a `transform.SourceTeams` tag that do not exist;
- it names the change-type constants wrongly.

`hr-sync-worker/README.md` — the consumer of that same feed, and likewise offered as a replaceable-implementation contract — repeats two of the four. **A replacement built from either document would filter on a field that is not there**, and silently delete rows it believed it was scoping.

### 7.4 The client API contract has drifted in eight services

Every drift below is against `docs/client-api.md` or its two derived views, which CLAUDE.md §5 requires to stay in lockstep:

| Service | Drift | Consequence |
|---|---|---|
| `auth-service` | §2.2 claims JWT scope "is derived server-side from the principal's roles (admin > bot > user)". It is not — `signNATSJWT` stamps only `account:<name>`, and `pkg/principal` records role scoping as unimplemented | **Clients are told a security property the server does not provide** |
| `botplatform-service` | Docs publish `POST`/`DELETE …/rooms/:roomID/members`; the code registers `…/members/add` and `…/members/remove` | **Any SDK written from the docs 404s on both** |
| `botplatform-service` | The derived view lists two endpoints and says "Emits: None — HTTP-only", dropping all five bot endpoints canonical §10.3–10.7 documents | Half the bot surface is invisible in the derived view |
| `message-gatekeeper` | The derived view says botDM rooms receive no `new_message` fan-out; the canonical doc and `broadcast-worker` say the opposite | **A client built from the view silently misses every bot DM** |
| `portal-service` | `§2.5` is used **twice**; the login endpoint has no TOC entry; `upstream_unavailable` is attributed to an endpoint that makes no outbound call | Colliding anchors in the derived views; wrong error attribution |
| `auth-service` | The `authToken` branch's entire response shape is undocumented | Six user fields the doc promises come back empty |
| `media-service` | `GET /api/v1/drive.members` is absent from §7 and bypasses `pkg/errcode` for a bespoke envelope | An undocumented endpoint with an error taxonomy no client library knows |
| `upload-service` | Broken internal references to the service's own section | — |

Four services also **document error reasons that no code path emits** (`portal-service`'s `site_unknown`, `botplatform-service`'s `dm_ensure_unavailable`) or **emit client-facing errors that appear in no table** (`message-gatekeeper`'s three attachment errors and the invalid-quoted-parent error; `auth-service`'s second 400 wording).

### 7.5 Comments that claim guarantees the code does not provide

These are the most dangerous class, because they are read *while* changing the code they describe:

- **`auth-service/handler.go:312-313`** documents the effective NATS grants as including `_INBOX.>` on pub and sub, and claims to be "kept in sync with `docker-local/setup.sh` and `docs/client-api.md` §2.1". **Both sources say the opposite** — the setup script states "There is no _INBOX grant" and the §2.1 table has no such row. A platform-team change made from this comment **would over-grant**, on the service that defines what every client may publish.
- **`user-presence-service/main.go:65-67`** asserts a compile-time interface check covers "including `SetExternal`". `PresenceStore` declares no `SetExternal`; the type system is not providing the guard the comment claims.
- **`message-gatekeeper/store.go:79-85`** documents `ParentMessageFetcher` errors as all soft-failed. The handler has since **tiered** them — terminal errors now *reject* the send. A second implementation written against this comment would get the failure semantics **backwards**.
- **`media-service/main.go:107-110`** explains there is deliberately "no blanket HTTP timeout … a short deadline would cancel a slow up/download mid-stream" — three lines above `srv.WriteTimeout = 30 * time.Second`.
- **`bot-room-service/sysmsg.go:385-388`** claims a dedup id that cannot dedup a retry.
- **`teams-hr-sync/store.go:12`** states "this producer never writes `hr_employee`"; direct mode writes exactly that collection through the same constant.

### 7.6 Rules that are correct but that six services must break

CLAUDE.md §6 requires `pkg/shutdown.Wait` in "every service's `main.go`". The six `teams-*` and `hr-sync` one-shot CronJob binaries all use `signal.NotifyContext` instead — **correctly**, because `shutdown.Wait` blocks waiting for a signal and is the wrong primitive for a run-to-completion job. Four separate audits independently flagged this as a rule violation *and* argued the code is right.

Similarly, the per-service file layout (`handler.go`, `routes.go`) has no sanctioned form for a job with no handler and no routes, so six services name that file `syncer.go` or `runner.go` and read as non-compliant.

**These are documentation bugs, not code bugs.** A rule that correct code must break trains readers to treat the document as advisory — which is precisely what makes §7.1–§7.5 dangerous.

### Fleet recommendations

- `high` — **Correct the `bot-message-worker` exposure note in `CLAUDE.md` §Cassandra.** It is wrong on both halves and is the stated reason a durability contract remains unimplemented. Do this before the fix itself, so the fix is argued from the real numbers.
- `high` — **Reconcile `docs/client-api.md` and both derived views against the code, service by service**, starting with `auth-service` §2.2 (a stated security property that does not exist) and `botplatform-service`'s member endpoints (documented routes that 404). Consider generating the derived views rather than maintaining them: six of the eight drifts above are view-versus-canonical, and CLAUDE.md already forbids exactly that divergence.
- `high` — **Rewrite `teams-hr-sync/README.md` and `hr-sync-worker/README.md` from the code.** Both are offered as replaceable-implementation contracts and both are wrong about the data model.
- `medium` — **Add `bot-room-service` to the OUTBOX producer list** in `CLAUDE.md` and `pkg/outbox/outbox.go`, and give `chat.roomactivity` (Chapter 4) either documentation or an owner.
- `medium` — **Fix the six guarantee-claiming comments in §7.5**, prioritising `auth-service`'s `_INBOX` grant comment — it is the one that would cause a security regression if acted on.
- `medium` — **Add a CronJob carve-out to §6** for `pkg/shutdown.Wait` and the per-service file layout, so six correct services stop reading as violations.
- `low` — When a `CLAUDE.md` claim is load-bearing enough to justify deferring work — as §7.1's was — **cite the file and line it rests on**, so the next reader can check it in seconds rather than re-deriving it.
