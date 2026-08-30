# Subscription Role `member` → `user` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Status:** Shipped in [hmchangw/newchat#435](https://github.com/hmchangw/newchat/pull/435). All tasks below are checked; the plan is kept as the design record.

**Goal:** New subscriptions are created with the non-owner role `"user"` instead of `"member"`, and every response or event that carries subscription roles hands the client `"user"` even when the stored document still holds the pre-cutover `"member"`.

**Architecture:** Two halves that must not be confused.

1. **Write side** — every producer of a non-owner role writes `model.RoleUser`. That covers subscription *creation* (room-worker channel create / `member.add` / Teams migration, inbox-worker's federated create) and the two *mutation* paths that also stamp a role (`SetOwnerRole` demote, the restrict-transition roles rewrite in both room-service and inbox-worker), plus the oplog migration transformer and the dev seed tools. After this, no writer produces `"member"` again.
2. **Read side** — documents written before the cutover keep `"member"` in Mongo. Rather than sprinkling a normalizer across every response builder, `model.Role` normalizes at the **JSON boundary**: `MarshalJSON` emits `"user"` for the legacy value, `UnmarshalJSON` accepts either spelling and stores the normalized one. All client traffic (NATS request/reply, HTTP, server→client events) and all cross-site federation payloads are JSON, so one choke point covers them. BSON is deliberately left alone — storage is never rewritten behind a reader's back, and a backfill stays a separate, explicit decision.

`UnmarshalJSON` also buys input compatibility for free: a client still sending `newRole: "member"` to the role-update RPC decodes to `RoleUser`, so the handler compares against one value only.

**Tech stack:** Go 1.25, NATS + JetStream, MongoDB, `go.uber.org/mock`, `stretchr/testify`, `testcontainers-go`.

---

## Conventions

- One task = one commit. Don't squash mid-implementation.
- Commands: `make test SERVICE=<dir>` (unit, race), `make test-integration SERVICE=<dir>` (Docker), `make lint`, `make sast`.
- TDD throughout: for the write-side tasks the RED step is flipping the existing assertion to `RoleUser` and watching it fail; for the read-side tasks it is a new test that fails until the normalizer exists.
- `RoleMember` is **not** deleted. It stays as the named legacy constant so tests, migrations and the normalizer can refer to the stored value by name.

---

## Decisions taken up front

| Question | Decision |
|----------|----------|
| Which writers change? | **All of them**, not just creation. Leaving demote/restrict writing `"member"` would keep minting dirty data that the read side has to paper over forever. |
| Does the role-update RPC still accept `"member"`? | **Yes.** Both spellings are accepted, normalized to `"user"` internally. Rejecting `"member"` would break every client already in the field. |
| Where does read normalization live? | **Everything returned to a client**, via the JSON boundary — not just user-service's `subscription.list`. One client must never see two spellings depending on which endpoint answered. |
| Is stored data backfilled? | **No.** Out of scope here. The read normalizer makes a backfill optional rather than urgent; `SetOwnerRole` heals a document opportunistically when a role actually changes. |

---

## File map

| File | What changes |
|------|--------------|
| `pkg/model/subscription.go` | `RoleUser` const; `RoleMember` re-documented as legacy; `NormalizeRole` / `NormalizeRoles`; `Role.MarshalJSON` / `Role.UnmarshalJSON` |
| `pkg/model/model_test.go` | Normalizer + JSON-boundary tests; existing fixtures moved to `RoleUser` |
| `room-worker/handler.go` | `buildChannelSubs` invitees and `processAddMembers` write `RoleUser` |
| `room-worker/teamsroomcreate.go` | Migrated members write `[owner, user]` / `[user]` |
| `inbox-worker/handler.go` | `rolesForType` returns `[user]` |
| `inbox-worker/main.go` | Restrict-transition `$cond` writes `"user"` in the else branch |
| `room-service/store_mongo.go` | `SetOwnerRole` maps `"member"`→`"user"` inside the pipeline, both branches; `ApplySubscriptionRestriction` else branch writes `"user"` |
| `room-service/handler.go` | `updateRole` normalizes `req.NewRole` once, then compares against `RoleOwner`/`RoleUser` |
| `room-service/helper.go`, `store.go` | Error text and interface doc-comment |
| `data-migration/oplog-collections-transformer/subscriptions.go` | `mapSubscriptionRoles` maps non-owner → `RoleUser` |
| `tools/loadgen/*`, `tools/seed-sample-data/fixtures.go` | Seeded roles |
| `broadcast-worker/sonic_wire_test.go` | Proves sonic honors the custom `Role` marshaler |
| `user-service/service/subscriptions_test.go` | Legacy-`member`-in-Mongo → `"user"` on the wire, for `subscription.list` and `getByRoomID` |
| `docs/client-api.md` | `roles` field, `newRole` values, restrict/duty prose, all JSON examples |
| `chat-frontend/src/api/types.ts`, `api/updateMemberRole/index.ts`, `lib/constants.js` | Type unions widened; `ROLE_USER` added |

---

## Phase 1: Model + normalization

### Task 1: `RoleUser` and the JSON boundary

**Files:** `pkg/model/subscription.go`, `pkg/model/model_test.go`.

- [x] **1.1 Write failing tests** — append to `pkg/model/model_test.go`: `TestNormalizeRole` (table: member→user, user/owner/admin/unknown/empty pass through), `TestNormalizeRoles` (nil→nil, empty→empty, order preserved), `TestNormalizeRoles_DoesNotMutateInput`, `TestRoleJSON_LegacyMemberMarshalsAsUser`, `TestRoleJSON_LegacyMemberUnmarshalsAsUser`, `TestRoleJSON_RejectsNonString`, `TestRoleJSON_OtherRolesRoundTrip`, `TestSubscriptionJSON_LegacyRolesSerializeAsUser`, and `TestSubscriptionBSON_LegacyRolesStoredVerbatim`.

  The BSON test is the guard rail for the whole design: it fails loudly if someone later "helpfully" adds a `bson.Marshaler` and starts rewriting storage.

- [x] **1.2 Verify RED** — `go test ./pkg/model/` fails to build on `undefined: model.RoleUser`.

- [x] **1.3 Implement** — add `RoleUser Role = "user"`, keep `RoleMember Role = "member"` with a comment saying no writer produces it, add `NormalizeRole` / `NormalizeRoles` and the two JSON methods (`UnmarshalJSON` wraps its decode error as `decode role: %w`).

- [x] **1.4 Verify GREEN + fix fallout** — existing round-trip fixtures in `model_test.go` that used `RoleMember` now normalize; move them to `RoleUser` and extend `TestRoleValues` to assert both constants.

### Task 2: Prove sonic honors the marshaler

**Files:** `broadcast-worker/sonic_wire_test.go`.

- [x] **2.1** `TestSonic_RoleNormalizationHonored` — `sonic.Marshal` and `encoding/json.Marshal` of a `Subscription` holding `[member, owner]` must both produce `"roles":["user","owner"]`. The hot-path workers encode with sonic, so a codec that ignored `json.Marshaler` would leak the legacy value on exactly the highest-volume path.

---

## Phase 2: Write paths

Each task is RED-first by flipping the service's existing assertion to `RoleUser` and watching it fail before touching production code.

### Task 3: room-worker

**Files:** `room-worker/handler.go`, `room-worker/teamsroomcreate.go`, and their `_test.go`.

- [x] **3.1** Flip assertions in `handler_test.go` / `teamsroomcreate_test.go` to `RoleUser`; confirm `TestHandler_ProcessAddMembers`, `TestProcessCreateRoom_Channel_BuildsSubsAndMembers`, `TestActorSubscriptionIsPreRead`, and the two Teams tests fail.
- [x] **3.2** `buildChannelSubs` invitees and `processAddMembers` pass `[]model.Role{model.RoleUser}`; Teams migration passes `[owner, user]` (human) / `[user]` (bot, platform admin). Update the stale "member-only" comment.

### Task 4: inbox-worker

**Files:** `inbox-worker/handler.go`, `inbox-worker/main.go`, `_test.go`, `integration_test.go`.

- [x] **4.1** `rolesForType(channel)` returns `[user]`; the restrict `$cond` else branch writes `string(model.RoleUser)`.
- [x] **4.2** Integration expectations updated: the federated `member_added` create, and the two restrict-rewrite tests. The "roles untouched" case keeps its `RoleMember` seed — that path must not rewrite anything.

### Task 5: room-service store

**Files:** `room-service/store_mongo.go`, `store.go`, `integration_test.go`.

- [x] **5.1** Update `TestMongoStore_SetOwnerRole_Integration`: it already seeds a legacy `["member"]` document, so the new expectations are promote → `[user, owner]` and demote → `[user]`, i.e. the write heals the document instead of producing `["member","owner"]`.
- [x] **5.2** `SetOwnerRole` wraps `$roles` in a `$map` that rewrites `"member"`→`"user"` before the promote/demote `$cond`; the demote branch's "ensure the non-owner role survives" check now tests for `RoleUser`.
- [x] **5.3** `ApplySubscriptionRestriction`'s else branch writes `"user"`; update `TestMongoStore_ApplySubscriptionRestriction_*` accordingly.

### Task 6: migration + dev tools

**Files:** `data-migration/oplog-collections-transformer/subscriptions.go`, `tools/loadgen/*`, `tools/seed-sample-data/fixtures.go`.

- [x] **6.1** `mapSubscriptionRoles`: RC `"owner"` → `RoleOwner`, everything else (and the empty-roles floor) → `RoleUser`.
- [x] **6.2** Seeded roles in loadgen and seed-sample-data.

---

## Phase 3: Role-update RPC

**Files:** `room-service/handler.go`, `helper.go`, `handler_test.go`.

- [x] **7.1 Write failing tests** — `TestHandler_UpdateRole_DemoteToUser` (canonical `newRole: "user"`) and `TestHandler_UpdateRole_RejectsUnknownRole` (`"admin"`, `"moderator"`, `""`). The pre-existing `TestHandler_UpdateRole_Demote_Success` keeps sending `"member"` and becomes the backward-compatibility test.
- [x] **7.2 Implement** — `req.NewRole = model.NormalizeRole(req.NewRole)` at the top of `updateRole`, then every comparison is against `RoleOwner` / `RoleUser`. Normalizing in the handler (not relying on `UnmarshalJSON`) keeps direct in-process callers and handler unit tests on the same rules as the wire.
- [x] **7.3** Error text → `invalid role: must be owner or user`.

Note the assertions on `evt.Subscription.Roles` in the existing tests: the event is marshaled then unmarshaled, so they now read `RoleUser` — that is the wire normalizer being exercised end-to-end, not a fixture change.

---

## Phase 4: Read path coverage

**Files:** `user-service/service/subscriptions_test.go`.

- [x] **8.1** `TestListSubscriptions_LegacyMemberRoleSerializesAsUser` and `TestGetByRoomID_LegacyMemberRoleSerializesAsUser`: the mocked repository returns a subscription holding `RoleMember`, the marshaled response must contain `"roles":["user"]` and must not contain `"member"`.
- [x] **8.2** Because the implementation landed in Phase 1, verify the RED explicitly: temporarily stub `NormalizeRole` to a no-op, watch both tests fail with `"roles":["member"]` in the diff, restore.

---

## Phase 5: Docs and frontend

- [x] **9.1** `docs/client-api.md`: `roles` field description states the normalization; `newRole` documents `"owner"` / `"user"` and the accepted legacy alias; the restrict and duty-owner prose says "reset to the plain `user` role"; every `"roles": ["member"]` example becomes `["user"]`. (There is no `docs/client-api/` derived-view directory in this repo, so nothing else to keep in sync.)
- [x] **9.2** Frontend, type-level only: `Role` becomes `'owner' | 'admin' | 'user'`, `MemberRole` keeps `'member'` as the documented legacy input, `ROLE_USER` added alongside `ROLE_MEMBER`. No runtime change — the existing demote call still sends `"member"`, which the server accepts.

---

## Verification

- [x] `go test -race ./...` — clean.
- [x] `make lint` — 0 issues.
- [x] `go vet -tags integration ./...` — integration tests compile.
- [x] `make sast` — gosec PASS. govulncheck and semgrep could not run in the dev container (`vuln.go.dev` blocked by the egress proxy; semgrep not installed) — CI is the gate for those.
- [x] `pkg/model` coverage: the four new functions at 100%.
- [ ] `make test-integration` — **not run locally** (no Docker in the container). Integration assertions were updated by inspection for the four write paths that produce roles; CI is the real check.

---

## Follow-ups (deliberately not in this change)

- **Backfill.** A one-off `updateMany` rewriting stored `"member"` → `"user"` would let the JSON normalizer be retired later. Not urgent while the normalizer stands, and it wants its own migration + rollout plan.
- **Frontend runtime.** `ROLE_MEMBER` is still what `MemberRoster` sends on demote. Switching it to `ROLE_USER` is a one-line change plus its test, worth doing when the frontend is next touched.
- **Retiring `RoleMember`.** Only after the backfill lands and no producer or stored document can carry the legacy value.
