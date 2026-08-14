# message-worker

Consumes user message submissions from the `MESSAGES` JetStream stream, validates the sender's room subscription, persists the message, and publishes a fanout event for downstream delivery.

## Event Flow

```
User → MESSAGES-{siteID} stream
         └── message-worker
               ├── validate subscription (MongoDB)
               ├── save message (Cassandra)
               ├── update room lastMessage (MongoDB)
               ├── publish → FANOUT-{siteID} (broadcast-worker)
               └── reply → chat.user.{account}.response.{requestID}
```

## NATS Subject

Consumed subject pattern:

```
chat.user.{account}.room.{roomID}.{siteID}.msg.send
```

## Configuration

| Env Var              | Default                    | Description                     |
|----------------------|----------------------------|---------------------------------|
| `NATS_URL`           | required                   | NATS server URL                 |
| `SITE_ID`            | required                   | Site identifier                 |
| `MONGO_URI`          | required                   | MongoDB connection string       |
| `MONGO_DB`           | `chat`                     | MongoDB database name           |
| `CASSANDRA_HOSTS`    | required                   | Comma-separated Cassandra hosts |
| `CASSANDRA_KEYSPACE` | `chat`                     | Cassandra keyspace              |
| `MAX_WORKERS`        | `100`                      | Max concurrent message handlers |

## Storage

| Store     | Collection / Table | Purpose                        |
|-----------|--------------------|--------------------------------|
| MongoDB   | `subscriptions`    | Validate user is in the room   |
| MongoDB   | `rooms`            | Update `updatedAt` timestamp   |
| Cassandra | `messages`         | Persist message history        |

Cassandra table schema:

```cql
CREATE TABLE messages (
  room_id    text,
  created_at timestamp,
  id         text,
  user_id    text,
  content    text,
  PRIMARY KEY (room_id, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);
```

## Local Development

### Run unit tests (no Docker required)

```bash
make test SERVICE=message-worker
```

### Run integration tests (requires Docker)

```bash
make test-integration SERVICE=message-worker
```

Uses `testcontainers-go` to spin up real MongoDB and Cassandra containers automatically.

### Run with Docker Compose

```bash
cd message-worker/deploy
docker compose up
```

This starts NATS (with JetStream), MongoDB, Cassandra, and the worker itself.

### Run locally against Docker Compose dependencies

```bash
# Start dependencies only
cd message-worker/deploy
docker compose up nats mongodb cassandra

# Run the worker from repo root
SITE_ID=site-local go run ./message-worker
```

## Graceful Shutdown

On `SIGTERM` / `SIGINT`, the worker:

1. Stops the JetStream pull iterator
2. Waits for in-flight goroutines to finish (timeout: 25s)
3. Drains the NATS connection
4. Disconnects MongoDB and Cassandra
