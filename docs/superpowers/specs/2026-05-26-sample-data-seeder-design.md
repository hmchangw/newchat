# Sample Data Seeder for Local Dev

**Date:** 2026-05-26
**Status:** Design approved, ready for plan

## Goal

After `make deps-up` brings up the local stack (NATS, MongoDB, Cassandra, Elasticsearch, Valkey, Keycloak via `docker-local/compose.deps.yaml`), `make seed` populates MongoDB and Valkey with a small, coherent, well-formed dataset that:

1. Lets a developer log in to chat-frontend as `alice` or `bob` and immediately see channels, DMs, members, and recent messages.
2. Gives every backend service a realistic baseline of users/rooms/subscriptions/messages/keys so end-to-end flows (send, broadcast, search, encryption, federation read paths) work without first having to drive the system via the UI.

The seeder is **idempotent** — re-running produces the same final state, no duplicates.

## Non-goals

- Seeding Cassandra (history). The Mongo `messages` collection is the recent-buffer and is enough for the dev playground; the Cassandra schema is verified by integration tests.
- Seeding NATS streams or replay payloads.
- Loading realistic volume (this is not loadgen — `tools/loadgen` covers that).
- Creating Keycloak users (the realm already imports `alice` + `bob` via `realm-export.json`).

## Architecture

A single Go binary at `tools/seed-sample-data` with this layout:

```text
tools/seed-sample-data/
  main.go         # CLI flag parsing, env config, connect, dispatch
  fixtures.go     # Pure-data builders: users, rooms, subscriptions, etc.
  mongo.go        # Upsert routines for each Mongo collection
  valkey.go       # Room-key + restricted-rooms-cache writes
  fixtures_test.go
  main_test.go
```

Reuses existing packages — does NOT introduce new ones:

- `pkg/model` — every record built from the typed structs (users, rooms, subscriptions, messages, etc.).
- `pkg/idgen` — for deterministic message IDs (`MessageIDFromRequestID`) and DM room IDs (`BuildDMRoomID`).
- `pkg/mongoutil` — for the connection.
- `pkg/roomkeystore` — instantiated via `NewValkeyClusterStore` so the on-wire hash format stays in lockstep with production.
- `pkg/valkeyutil` — for the search-service cache key, written with `SetJSONWithTTL` (same path the live service uses).

## CLI surface

| Flag | Default | Behavior |
|---|---|---|
| (none) | — | Idempotent populate. Safe to re-run. |
| `--reset` | `false` | Drop seed records from MongoDB collections (filtered by stable seed IDs, never `DROP DATABASE`), delete Valkey keys, then populate. |
| `--dry-run` | `false` | Print the plan (counts per collection, Valkey keys) and exit without writing. |

Environment variables (with defaults that match `compose.deps.yaml`):

| Var | Default | Purpose |
|---|---|---|
| `MONGO_URI` | `mongodb://localhost:27017` | Mongo connection string |
| `MONGO_DB` | `chat` | Mongo database name (matches every service's default) |
| `VALKEY_ADDRS` | `localhost:6379` | Comma-separated Valkey cluster seed nodes |

Exit codes: `0` on success, non-zero on connect/write failure with a clear `slog` JSON error.

## Data

### Users — 10 total (`users` collection)

| ID | Account | Site | SectID / Name | EngName | ChineseName |
|---|---|---|---|---|---|
| `u-alice` | alice | site-local | eng / Engineering | Alice Engineer | 王小愛 |
| `u-bob` | bob | site-local | eng / Engineering | Bob Developer | 陳大寶 |
| `u-carol` | carol | site-local | eng / Engineering | Carol Coder | 林小卡 |
| `u-dave` | dave | site-local | prod / Product | Dave PM | 張小達 |
| `u-eve` | eve | site-local | prod / Product | Eve Manager | 黃小夜 |
| `u-frank` | frank | site-local | design / Design | Frank Designer | 吳小法 |
| `u-grace` | grace | site-local | design / Design | Grace UX | 蔡小恩 |
| `u-heidi` | heidi | site-local | ops / Operations | Heidi Ops | 周小海 |
| `u-ivan` | ivan | site-remote | eng / Engineering (Remote) | Ivan Remote | 鄭小宜 |
| `u-judy` | judy | site-remote | prod / Product (Remote) | Judy Cross | 高小朱 |

`alice` and `bob` are the two accounts present in `auth-service/deploy/keycloak/realm-export.json` and so are the only ones a developer can actually log into via the web flow. The remaining accounts populate rooms so member lists and mention pickers look realistic.

### Rooms (`rooms` collection) — 6 total

| ID | Type | Site | Name | Members | Restricted |
|---|---|---|---|---|---|
| `r-general` | channel | site-local | general | alice, bob, carol, dave, eve, frank, grace, heidi, ivan | false |
| `r-eng` | channel | site-local | engineering | alice, bob, carol, ivan | true |
| `r-design` | channel | site-local | design | frank, grace, dave | false |
| `u-aliceu-bob` | dm | site-local | (DM label) | alice, bob | false |
| `u-carolu-eve` | dm | site-local | (DM label) | carol, eve | false |
| `r-remote-announce` | channel | site-remote | remote-announce | ivan, judy, alice | false |

DM room IDs are produced by `idgen.BuildDMRoomID(userA, userB)` — sorted concat of the two user IDs, the exact production format. Each room's `UserCount`, `LastMsgAt`, `LastMsgID` are set to match the messages inserted below.

`r-eng` carries `Restricted=true` so the search-service restricted-rooms cache write below has a real target.

### Room members (`room_members` collection)

One `RoomMember` document per (channel, user) pair across the four channels (`r-general` 9 + `r-eng` 4 + `r-design` 3 + `r-remote-announce` 3 = **19 entries total**). DMs are NOT in this collection: `room-service`'s `ListRoomMembers` falls back to the `subscriptions` collection when no `room_members` document exists for a room (see `room-service/store_mongo.go:329`), and that fallback is what DM membership relies on.

`RoomMember.ID` is `{roomID}:{userID}` (stable, idempotent). `Member.Type=individual`, `Member.Account` populated.

### Subscriptions (`subscriptions` collection)

One `Subscription` document per (user, room) the user belongs to (`r-general` 9 + `r-eng` 4 + `r-design` 3 + alice-bob DM 2 + carol-eve DM 2 + `r-remote-announce` 3 = **23 entries total**). Stable ID: `sub:{userID}:{roomID}`.

- `Roles: ["owner"]` for room creators (alice owns `r-general` and `r-eng`; frank owns `r-design`; ivan owns `r-remote-announce`); `["member"]` for everyone else.
- `JoinedAt` set to a fixed wall-clock timestamp (`2026-05-01T09:00:00Z`) so re-runs don't drift.
- `LastSeenAt` set to a per-user offset from `LastMsgAt` so the unread badge logic has signal in either direction.
- DM subscriptions encode `RoomType=dm`; the seeder emits `DMSubscription` wrappers (base Subscription + counterpart's `hrInfo`) so the wire shape matches what the live backend produces.

### Threads (`thread_rooms` + `thread_subscriptions`)

One thread under one parent message in `r-eng`:

- Parent message in `r-eng`: alice — "Should we adopt UUIDv7 for all entity IDs?"
- `thread_rooms` entry: `id = tr-uuidv7-debate`, `parentMessageId = <alice's message id>`, `replyAccounts = [bob, carol]`.
- `thread_subscriptions` entries: bob and carol each subscribed to the thread.
- Three thread replies in the `messages` collection with `ThreadParentMessageID` set: bob, carol, bob.

### Messages (`messages` collection) — 23 total

Per-room counts:

| Room | Count | Notes |
|---|---|---|
| `r-general` | 5 | Mix of users; one `@all` mention |
| `r-eng` | 4 root + 3 thread replies | One root is the thread parent above |
| `r-design` | 3 | |
| `u-aliceu-bob` | 3 | |
| `u-carolu-eve` | 2 | |
| `r-remote-announce` | 3 | Demonstrates a remote-site room with messages |

Every message ID is produced by `idgen.MessageIDFromRequestID("seed:" + roomID, "<n>")` so re-runs collide on the same ID and upsert cleanly. `CreatedAt` is a fixed timeline starting `2026-05-01T10:00:00Z` and stepping in 5-minute increments per room so the order is deterministic and `LastMsgAt` is stable.

### Valkey

**Room encryption keys** — for every channel and DM (6 rooms) write a stable 32-byte secret to `room:{<roomID>}:key`:

```text
key  = sha256("seed-room-key:" + roomID)[:32]
HSET room:{roomID}:key  priv <base64(key)>  ver 0
```

This is the exact hash format `pkg/roomkeystore` writes via `Set`, so message-worker / broadcast-worker can encrypt/decrypt the seeded messages without rotating.

**Search restricted-rooms cache** — for each member of the restricted `r-eng` room (alice, bob, carol, ivan), write the cache entry the search-service would lazy-populate after their first restricted-rooms query:

```text
SET searchservice:restrictedrooms:<account>  '{"r-eng": <unix-ms>}'  EX 300
```

5-minute TTL matches `RESTRICTED_ROOMS_CACHE_TTL`'s default in `search-service/main.go`.

## Idempotency

Every write is either an upsert (`ReplaceOne(..., SetUpsert(true))` for Mongo) or a naturally-idempotent command (`HSET` for room keys, `SET ... EX` for the search cache). All IDs are deterministic:

- User IDs: hard-coded `u-<account>`.
- Room IDs: hard-coded `r-<slug>` for channels; `idgen.BuildDMRoomID` for DMs.
- Subscription / RoomMember IDs: derived from `(userID, roomID)`.
- Message IDs: `idgen.MessageIDFromRequestID("seed:" + roomID, "<n>")`.
- Valkey keys: derived from roomID / account.

Re-running `make seed` is therefore a no-op in terms of final state and never duplicates a document.

`--reset` performs `DeleteMany({_id: {$in: [...seed IDs]}})` per collection rather than `Drop()` — so a developer who has hand-created additional records in the same dev DB does not lose them. Same idea for Valkey: `DEL` only the keys this tool owns.

## Makefile integration

```makefile
seed:
	go run ./tools/seed-sample-data

seed-reset:
	go run ./tools/seed-sample-data --reset

seed-dry-run:
	go run ./tools/seed-sample-data --dry-run
```

`make deps-up` is NOT modified to auto-seed — keeping seeding as an explicit step preserves the "containers up, no application state yet" baseline that the existing pipeline expects (e.g. integration test setup).

## Error handling

- Connect-time failures: log JSON `level=error` with the failing component and exit 1.
- Per-document write failures: log the entry, continue with the next, and exit non-zero at the end of the run if any failed. (Devs need to see the full picture of what went wrong, not just the first failure.)
- All errors wrapped per CLAUDE.md style: `fmt.Errorf("seed users: %w", err)`.

## Logging

`log/slog` JSON, single handler created in `main.go`. Per-collection summary at the end:

```json
{"level":"info","msg":"seed complete","users":10,"rooms":6,"subscriptions":23,"roomMembers":19,"messages":23,"threadRooms":1,"threadSubscriptions":2,"valkeyRoomKeys":6,"valkeyCacheEntries":4}
```

## Testing

Per `CLAUDE.md` Section 4: TDD, ≥80% coverage, unit tests in the same package.

- `fixtures_test.go` — table tests on the builder functions:
  - User count, ID format, site distribution.
  - Room member lists agree with subscription / room-member rows (no orphan subs).
  - DM room IDs match `idgen.BuildDMRoomID`.
  - Message timeline is monotonic per room.
  - Thread parent message exists in the message list.
- `main_test.go` — covers flag parsing, env defaulting, and the `--dry-run` output shape.

No integration test for the seeder itself — exercising `make deps-up && make seed && make up` IS the verification, and the data shapes are already verified end-to-end by every service's integration tests.

## Out-of-scope, future work

- A `--volume=large` flag that multiplies messages per room for soak testing. `tools/loadgen` already covers volume; revisit only if devs want a richer browse-only dataset.
- Seeding Cassandra so the history-service has data without first replaying messages through the live stack. Defer until a dev workflow actually demands it.
- A `seed` Compose profile that runs the binary in a one-shot container. Useful if/when CI starts spinning the local stack; not needed for a developer's machine.
