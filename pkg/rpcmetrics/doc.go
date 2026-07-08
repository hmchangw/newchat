// Package rpcmetrics — RPC observability vocabulary.
//
// Metrics (default Prometheus registry, exposed on /metrics):
//
//	rpc_server_requests_total{service, route, status}        counter
//	rpc_server_request_duration_seconds{service, route}      histogram (DefBuckets)
//
// Labels:
//
//   - service: the process's service name (same string passed to
//     natsrouter.New / InitTracer), one value per process.
//   - route:   the ROUTE PATTERN, never a live subject/URL. NATS uses the
//     natsrouter pattern with {name} placeholders; HTTP uses Gin's FullPath
//     (registered template). This is the cardinality guard.
//   - status:  the errcode Code (ok + 8 canonical codes). Anything outside the
//     pinned allowlist collapses to "internal".
//
// Emitted by pkg/natsrouter.Metrics (NATS request/reply) and
// pkg/ginutil.Metrics (Gin HTTP). Do not add per-service copies of these
// series — query by the `service` label instead.
package rpcmetrics
