# roomrace/e2e

Drives the reported channel scenario against a **real stack** — `room-service`,
`room-worker`, `message-gatekeeper`, `broadcast-worker`, `message-worker`,
`history-service` on real NATS / MongoDB / Cassandra / Valkey — and prints, with
timestamps, what each client would have on screen.

```sh
./tools/roomrace/e2e/stack.sh up     # deps + seed + the six services
go run ./tools/roomrace/e2e          # baseline: today's client behaviour
./tools/roomrace/e2e/stack.sh down
```

Services run as host processes against containerised deps — much faster than
building six images, and the binaries are the real ones. Vault and room
encryption are switched off (`ATREST_ENABLED=false`, `ENCRYPTION_ENABLED=false`)
so the harness can read the payloads; nothing else is altered.

## The scenario

1. `alice` creates a channel with `bob`, and waits for the **async job result**.
2. `alice` sends the first message and waits for the `msg.send` reply.
3. `alice`'s UI "jumps into" the room: subscribe + `msg.history`.
4. `bob` subscribes on `subscription.update`, as the frontend does, then `msg.history`.

`room-worker` also emits two system messages (`room_created`, `members_added`)
into MESSAGES-CANONICAL, so the room should show three messages.

## Flags

| Flag | What it changes |
|---|---|
| *(none)* | today's behaviour |
| `-early-sub` | alice subscribes to the room subject on her own `subscription.update` instead of when she opens the room |
| `-hint` | `msg.history` carries `meta.lastMsgAt = now`, so the scan ceiling is not the stale room document |
| `-retry-history` | retry `msg.history` until the just-sent message appears |
| `-probe-room <id>` | just load history for a room and exit |

## What it found

See `../RESULTS-e2e.md`. In short: `bob` gets everything live, `alice` misses both
system messages (her async job result lands ~500 ms *after* they were broadcast),
and `msg.history` returns 2 of 3 for **both** users — the just-sent message is
excluded by a stale scan ceiling, and retrying does not help until the
history-service room cache expires. `-hint` fixes the history side; `-early-sub`
fixes alice's live side.
