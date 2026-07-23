# Bot pipeline unification design

**Date:** 2026-07-22
**Status:** Approved, pending implementation
**Branch:** `claude/bot-implementation-8d2ctl` (PR #109)

## Context

PR #109 introduced a parallel bot messaging pipeline. Three of its fan-out services (`bot-broadcast-worker`, `bot-notification-worker`, `bot-push-notification-service`) reimplemented shrunken versions of the user pipeline's downstream services. Two consequences surfaced during review:

1. **Feature gaps vs user pipeline:** bot broadcasts skip client-facing room-key encryption; bot notifications don't filter bot recipients; bot-room-service doesn't rotate room keys on member churn or fan out keys on add. If a bot-owned room contains human members (the point of bots), these gaps break parity with user rooms.
2. **Structural duplication:** once we bring bot services to full behavioral parity with the user pipeline, the code is identical apart from which JetStream stream feeds it. Maintaining two copies is negative-value.

`search-sync-worker` already demonstrates the desired pattern: one binary, one `messageCollection` type parameterized on stream, consuming both `MESSAGES_CANONICAL` and `BOT_MESSAGES_CANONICAL`. This design extends that pattern to the three downstream services, using one-binary-two-deployments instead of one-process-two-consumers to keep failure domains isolated.

## Goals

- Delete `bot-broadcast-worker/` and `bot-notification-worker/`; rename `bot-push-notification-service/` → `push-notification-service/`.
- Parameterize `broadcast-worker`, `notification-worker`, and `push-notification-service` on stream/subject/consumer via env.
- Deploy each unified binary twice (user + bot); same image, different env overrides.
- Bot pipeline adopts existing user-pipeline wire shapes verbatim — delete `model.BotNotification`.
- Add room-key creation to bot-room-service on room-create and DM-ensure.
- Add room-key fan-out on bot add-member and rotation on bot remove-member.

## Non-goals

- Merging `bot-msg-handler` with `message-gatekeeper` (different ingress semantics — bots use req/reply with header validation; users publish direct to `MESSAGES` stream).
- Merging `bot-msg-worker` with `message-worker` (already at parity for at-rest encryption; deferrable).
- Merging `bot-room-service` with `room-service`/`room-worker` (bot ingress is HTTP-terminated through BP and has distinct auth/routing; also deferrable).
- Reworking `search-sync-worker` (already handled correctly).

## Architecture

### Single binary, two deployments per service

Each of the three unified services becomes one binary that reads its stream configuration from env at startup and wires exactly one consumer. Two Kubernetes / Azure Pipelines deployments per service:

```text
broadcast-worker/           # single binary
├── main.go                 # reads INPUT_STREAM / INPUT_SUBJECT_FILTER / CONSUMER_NAME
├── handler.go              # unchanged; processes model.MessageEvent regardless of source
├── deploy/
│   ├── user/               # existing pipeline; INPUT_STREAM=MESSAGES_CANONICAL_<site>
│   │   ├── Dockerfile
│   │   ├── docker-compose.yml
│   │   └── azure-pipelines.yml
│   └── bot/                # new; INPUT_STREAM=BOT_MESSAGES_CANONICAL_<site>
│       ├── Dockerfile
│       ├── docker-compose.yml
│       └── azure-pipelines.yml

notification-worker/        # same shape; adds OUTPUT_STREAM / OUTPUT_SUBJECT_PREFIX

push-notification-service/               # renamed from bot-push-notification-service
```

### Config surface

Env vars added to each service's `Config` struct (via `caarlos0/env`):

| binary | env var | user default | bot deployment |
|---|---|---|---|
| broadcast-worker | `INPUT_STREAM` | `MESSAGES_CANONICAL_<site>` | `BOT_MESSAGES_CANONICAL_<site>` |
| broadcast-worker | `INPUT_SUBJECT_FILTER` | `chat.msg.canonical.<site>.>` | `chat.bot.canonical.<site>.>` |
| broadcast-worker | `CONSUMER_NAME` | `broadcast-worker` | `bot-broadcast-worker` |
| notification-worker | (broadcast-worker's three) + | | |
| notification-worker | `OUTPUT_STREAM` | `PUSH_NOTIF_<site>` | `BOT_PUSH_NOTIF_<site>` |
| notification-worker | `OUTPUT_SUBJECT_PREFIX` | `chat.push.notification.<site>` | `chat.bot.notification.push.<site>` |
| push-notification-service | `INPUT_STREAM` | `PUSH_NOTIF_<site>` | `BOT_PUSH_NOTIF_<site>` |
| push-notification-service | `INPUT_SUBJECT_FILTER` | `chat.push.notification.<site>.>` | `chat.bot.notification.push.<site>.>` |
| push-notification-service | `CONSUMER_NAME` | `push-notification-service` | `bot-push-notification-service` |

`SERVICE_NAME` env is already exported to OTel resource attributes by `pkg/obs.Init`; Grafana distinguishes deployments by `service.name`. Metric names stay uniform in code.

### Wire shape

- Bot pipeline adopts `model.MessageEvent` end-to-end (already true through canonical publish).
- Bot notification-worker emits `model.PushNotificationEvent` (batched by recipient, structured Title/Body/Data). `model.BotNotification` is deleted along with `bot-notification-worker/`.
- Push-service consumes `model.PushNotificationEvent` regardless of source stream.
- `pkg/subject` retains bot-scoped subject builders for the bot deployment's stream naming; the handler code never references them.

### Bot-room-service — room key lifecycle

Bot rooms today have no room keys at all — `bot-room-service` never touches `pkg/roomkeystore`. Once the unified `broadcast-worker` encrypts bot messages via the current room key, bot rooms must have keys. Mirror the user pipeline's key lifecycle:

All four flows compose two primitives already exposed by shared packages:
- `roomkeystore.GenerateKeyPair` / `keyStore.Set` / `keyStore.Rotate` / `keyStore.SetWithVersion`
- `roomkeysender.Sender.Send(account, evt)` — one WS-scoped publish per recipient. Fan-out is a caller-owned loop over accounts (mirrors how `room-worker` composes it today).

**On room create (`bot-room-service.CreateChannel`)** — mirror `room-service.CreateChannel`:
1. `GenerateKeyPair()` → new private key
2. `keyStore.Set(ctx, roomID, pair)` → stamps v1
3. `Sender.Send(owner.Account, RoomKeyEvent{roomID, versioned})` → delivers initial key to owner

**On DM ensure (`bot-room-service.EnsureDM`)** — no key work. Mirrors user pipeline: DM/botDM rooms fan out to per-user subjects that only the recipient can subscribe to, so broadcast-worker never encrypts them and no key is stored. See `room-worker/handler.go:1447-1449` for the reference decision.

**On add member (`bot-room-service.AddMember`)** — channel rooms only (DMs never add). Mirror `room-worker.buildAndFanOutRoomKey`:
1. `keyStore.Get(ctx, roomID)` → current versioned key
2. Loop `Sender.Send(newAccount, evt)` over newly-subscribed accounts only. No rotation (matches user behavior at room-worker/handler.go:1063).

**On remove member (`bot-room-service.RemoveMember`)** — mirror `room-worker.rotateAndFanOut`:
1. Snapshot survivor accounts (post-delete)
2. `keyStore.Get` → current pair (for `predictedVersion`)
3. `GenerateKeyPair()` → v+1
4. Loop `Sender.Send(survivor, evt)` over survivors *before* commit (survivors hold new key before broadcast-worker switches)
5. `keyStore.Rotate` (or `SetWithVersion` fallback on `ErrNoCurrentKey`)

Reuses `pkg/roomkeystore` + `pkg/roomkeysender` verbatim — no new key-management code.

### Delivery-failure model (persistence-before-delivery is deliberate)

Every flow persists the key before pushing it to clients, and every `Sender.Send` call is best-effort (log-and-continue) rather than a hard error. This is intentional and mirrors the user pipeline's `room-worker` behavior — a stored-key + missing-delivery outcome is recoverable (clients re-fetch on decrypt-miss), whereas the reverse (delivered key with no server-side record) leaves ciphertext undecryptable forever and is not.

Recovery mechanisms already in the codebase and reused unchanged for bot rooms:
- **Client side**: on decrypt-miss, clients re-request the current key via the existing `room-service.getRoomKey` RPC (`chat.user.{account}.request.room.{roomID}.{siteID}.key.get`). That handler already checks membership against the shared `subscriptions` collection where bot-room-service also writes, so bot-owned channel rooms are served by the same RPC — no bot-specific key-fetch RPC needed.
- **Server side**: on the rotate-then-fan-out path, the fan-out happens *before* commit so survivors hold v+1 by the time `broadcast-worker` starts encrypting under it. If commit then fails, the fan-out is harmless (clients hold a key they can't use yet), and `Rotate`'s next successful call reconciles the version.

Introducing a durable retry queue for key-delivery events is deliberately out of scope for this refactor — it's a separate improvement that would apply equally to `room-worker` and belongs in its own design if warranted.

## Data flow (after unification)

```text
BP (HTTP) ──req/reply──> bot-msg-handler ──publish──> BOT_MESSAGES_CANONICAL
                                                           │
                                                           ├──> broadcast-worker[bot deployment]
                                                           │       ├─ encrypt via room key → WS fan-out
                                                           │       └─ per-user fan-out
                                                           ├──> notification-worker[bot deployment]
                                                           │       ├─ mute / presence / hook / mention / large-room gates
                                                           │       ├─ EligibleForPush (filters bot recipients)
                                                           │       └─ emit PushNotificationEvent → BOT_PUSH_NOTIF
                                                           ├──> bot-msg-worker
                                                           └──> search-sync-worker (existing)

                       BOT_PUSH_NOTIF ──> push-notification-service[bot deployment] ──> Dispatcher (APNs/FCM)
```

## Testing strategy

### Unit tests
- `main.go` tests assert env values wire the correct stream/subject/consumer names — one test per service, table-driven for user+bot config.
- Existing handler unit tests run unchanged — they exercise handler logic against synthetic `MessageEvent`s regardless of source stream.
- New bot-room-service tests for the room-key lifecycle: `MockKeyStore` + `MockKeySender` on create/DM-ensure/add/remove.

### Integration tests
- For each unified service (`broadcast-worker`, `notification-worker`, `push-notification-service`), one new integration test running the handler against the bot canonical stream, asserting the same handler produces expected downstream events.
- `bot-room-service`: integration test that add-member fans out the current key to new members and remove-member rotates + fans out v+1 to survivors, hitting real `pkg/roomkeystore` on Mongo.

### Compatibility spot-checks
- Confirm `notification-worker.MemberCache` handles bot subscription docs (missing `Muted` / `HistorySharedSince` fields treated as zero values). Explicit unit test.
- Confirm `notification-worker.ParentFetcher.FetchParent` accepts a bot's account for auth when bots start posting thread replies. If it doesn't, add a system-identity path.
- Confirm `notification-worker.Vetoer` (hook) handles bot senders without panicking. Existing impls are generic over `*model.Message`; should be fine.

## Migration story

No live-migration concern: bot pipeline is pre-production. Streams (`BOT_MESSAGES_CANONICAL`, `BOT_PUSH_NOTIF`) unchanged. Kill the old bot-* services, deploy the unified ones. `docker-compose` at repo root updated to spin up both deployments locally.

## Commit sequence

Each numbered slice is independently reviewable:

1. **Room-key lifecycle in bot-room-service** — create/DM-ensure key generation, add-member fan-out, remove-member rotate+fan-out. Uses existing `pkg/roomkeystore` + `pkg/roomkeysender`. Independent of unification; ships even if unification stalls.
2. **Parameterize `broadcast-worker`** on `INPUT_STREAM` / `INPUT_SUBJECT_FILTER` / `CONSUMER_NAME` env. User defaults preserve current behavior.
3. **Delete `bot-broadcast-worker/`**, add `broadcast-worker/deploy/bot/`, update root `docker-compose.yml`.
4. **Parameterize `notification-worker`** on `INPUT_STREAM` / `OUTPUT_STREAM` / `CONSUMER_NAME` env.
5. **Delete `bot-notification-worker/` + `model.BotNotification`**, add `notification-worker/deploy/bot/`.
6. **Rename `bot-push-notification-service/` → `push-notification-service/`**, parameterize on env, add `push-notification-service/deploy/{user,bot}/`.
7. **Update `docs/superpowers/specs/2026-07-22-bot-cross-site-routing-design.md`** to reflect the unified architecture; update `docs/client-api.md` if any client-observable wire changes surface (expected: none, since push wire shape is server-internal).

## Rollback

Per-slice: revert the slice's commit. Slices 1 and 2/4/6 are additive (env defaults preserve behavior). Slices 3/5 are the destructive step; if they need rolling back, restore the deleted service directory and its `deploy/` config.

## Open questions

None — Section 1/2/3 approved during brainstorming (see conversation transcript). Gotcha #2 (missing subscription fields), #3 (ParentFetcher bot-auth), #5 (docker-compose defaults) flagged as verify-during-implementation, no design decision needed.
