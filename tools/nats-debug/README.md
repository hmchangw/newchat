# NATS Debug Tool

A browser-based debug UI for inspecting and publishing NATS messages across two independent servers.

## Features

- Connect to a **source** NATS server (for publishing) and a **dest** NATS server (for subscribing) independently
- Subscribe to **multiple subjects simultaneously** using NATS wildcards (`chat.>`, `fanout.*`, etc.)
- Each subscription is **colour-coded** so you can tell messages apart at a glance
- **Publish** arbitrary JSON payloads to any subject on the source server
- **Real-time message feed** with JSON syntax highlighting, subject filtering, copy-to-clipboard, and auto-scroll
- Quick-sample buttons pre-fill common chat subjects

## Quick Start

### Option 1 — Docker Compose (recommended)

Spins up two NATS servers and the debug UI in one command.

```bash
docker compose -f tools/nats-debug/deploy/docker-compose.yml up
```

| Service | URL |
|---------|-----|
| Debug UI | http://localhost:8090 |
| Source NATS | nats://localhost:4222 |
| Dest NATS | nats://localhost:4223 |

> **Tip:** For simple local testing, point both Source and Dest at the same NATS URL (e.g. `nats://localhost:4222`). Subscribe on dest and publish on source — messages appear in the feed immediately.

### Option 2 — Run the binary directly

Requires a NATS server already running.

```bash
# Build
make build SERVICE=tools/nats-debug

# Run (default port 8090)
./bin/tools/nats-debug

# Custom port
PORT=9000 ./bin/tools/nats-debug
```

Open http://localhost:8090.

## Usage

### 1. Connect

Enter the URLs for both servers and click **Connect**.

| Field | Description |
|-------|-------------|
| Source NATS | Server you will **publish** to |
| Dest NATS   | Server you will **subscribe** to |

### 2. Subscribe

Type a subject pattern in the Subscriptions panel and click **+**.
NATS wildcards are supported:

| Pattern | Matches |
|---------|---------|
| `chat.>` | All subjects under `chat.` |
| `fanout.*` | Single token — e.g. `fanout.site-a` |
| `chat.room.123` | Exact subject |

Add as many subscriptions as you need. Remove any with **×**.

### 3. Publish

Fill in a subject and a JSON payload, then click **Publish** (or press `Ctrl+Enter`).
Messages are sent to the **source** server.

### X-Debug headers (Publish & Request)

Both the Publish and Request panels can stamp the server's debug headers on the
message they send, so you can flag a single request and exercise the verbose
server-side logging machinery:

| Control | Header | Effect |
|---------|--------|--------|
| **X-Debug level** | `X-Debug: flow\|debug\|trace` | Opt-in per-request verbose logging on every service the message touches, joinable by `request_id` in the log aggregator. `Off` (default) sends no header. The metadata ladder is cumulative (`flow` < `debug` < `trace`) and metadata-only — never message bodies. |
| **X-Debug-Payload** | `X-Debug-Payload: 1` | Requests full request/reply **body** logging. |

> **X-Debug-Payload is inert unless the target service opts in.** A service only
> logs a body when its own config sets `DEBUG_LOG_PAYLOADS=true` (default off, so
> it does nothing in production). The header alone never causes body logging.

For fire-and-forget **Publish**, retrieve the trace from the server logs by
`request_id`. For **Request**, the reply also comes back in the panel directly.

### 4. Message Feed

Incoming messages appear in real time on the right panel:

- **Filter** by subject using the search box
- **Click** a message header to collapse/expand the payload
- **⧉** copies the raw payload to the clipboard
- **↓ Auto** toggles auto-scroll (green = enabled)
- **Clear** removes all messages from the view

### Quick Samples

Use the sidebar buttons to pre-fill common patterns:

| Button | Action |
|--------|--------|
| Subscribe chat.> | Fills subject input with `chat.>` |
| Subscribe fanout.> | Fills subject input with `fanout.>` |
| Publish sample message | Fills publish form with a sample chat message |

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `PORT`  | `8090`  | HTTP port the UI listens on |
| `NATS_CREDS_FILE` | `""` | Optional NATS user credentials file (JWT + NKey). When set, it authenticates every NATS connection the tool opens (source, dest, and request). Empty means connect without credentials. The file is checked for existence at startup — a missing path fails fast. |
| `SESSION_IDLE_TIMEOUT` | `30m` | How long a browser session may go without activity before its hub (and NATS connections) are torn down. Accepts Go durations (e.g. `15m`, `1h`). |

> **Auth:** Point `NATS_CREDS_FILE` at a mounted creds file (e.g. the shared `docker-local/backend.creds`) when the target servers require authentication. The same credentials are applied to all three connections.

## Architecture

```
Browser ──SSE──▶ nats-debug server
        ◀──HTTP─┘     │           │
                  source NATS  dest NATS
                  (publish)   (subscribe)
```

The server holds two independent NATS connections. Subscriptions are registered on the dest connection; publishes go to the source connection. Incoming NATS messages are broadcast to the owning session's browser tabs via Server-Sent Events (SSE).

### Per-session isolation

Each browser gets its **own** set of NATS connections, subscriptions, connection status, and message feed. On first request the server issues an `HttpOnly` session cookie (`ndsid`); every subsequent request and SSE stream carries it automatically (same-origin), so the frontend needs no special handling. One user connecting, disconnecting, subscribing, or publishing therefore has no effect on another user's view. Multiple tabs in the same browser intentionally share one session.

A session's hub is torn down (and its NATS connections closed) after no activity for `SESSION_IDLE_TIMEOUT` (default 30m). An open SSE stream sends periodic keep-alives that count as activity, so an actively-watched feed is never swept mid-stream.
