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

