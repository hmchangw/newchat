# Local Jaeger for Tracing

Run Jaeger locally to view OpenTelemetry traces from any service in the monorepo.

## Quick Start

```bash
cd tools/jaeger
docker compose up -d
```

Jaeger UI: http://localhost:16686

## Usage

1. Start Jaeger (above).
2. Run any service locally:
   ```bash
   # Example: broadcast-worker
   NATS_URL=nats://localhost:4222 MONGO_URI=mongodb://localhost:27017/?directConnection=true SITE_ID=site-local go run ./broadcast-worker/
   ```
3. Exercise the service (send messages, trigger events, etc.).
4. Open http://localhost:16686, select the service name from the **Service** dropdown, and click **Find Traces**.

No extra configuration is needed. All services use `otlptracegrpc.New(ctx)` which defaults to `localhost:4317` -- the port Jaeger is listening on.

## Custom Endpoint

To point a service at a different collector, set the environment variable before starting it:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=my-collector:4317 go run ./broadcast-worker/
```

## Ports

| Port  | Protocol  | Purpose                        |
|-------|-----------|--------------------------------|
| 4317  | OTLP gRPC | Trace ingestion (default)      |
| 4318  | OTLP HTTP | Alternative trace ingestion    |
| 16686 | HTTP      | Jaeger UI                      |

## Notes

- Uses in-memory storage -- traces are lost when the container restarts. This is expected for local development.
- Jaeger all-in-one bundles collector, query, and UI in a single container.

## Stop

```bash
docker compose down
```
