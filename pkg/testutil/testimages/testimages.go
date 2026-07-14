// Package testimages pins the container images used by integration tests
// across every service and pkg. One central set of tags keeps the whole
// repo on identical versions, so a Cassandra or Mongo schema/behaviour
// drift is caught everywhere at once rather than in whichever service
// happens to have the newest tag today.
//
// Versions here track docker-local/compose.yaml (the authoritative prod
// local-dev stack) where practical. One intentional divergence:
// Cassandra tests pin 4.1.3 because cassandra:5 with the
// testcontainers-go module's default MAX_HEAP_SIZE=1024M routinely OOMs
// on standard GitHub Actions runners.
//
// This package is only imported by integration tests (files gated by
// //go:build integration). Keep it dependency-free.
package testimages

const (
	// Cassandra is the image for every CQL-backed integration test.
	// See package doc for why this diverges from prod (cassandra:5).
	Cassandra = "cassandra:4.1.3"

	// Mongo is the image for every Mongo-backed integration test.
	// Tracks the deploy stack (docker-local/compose.deps.yaml) so tests
	// exercise the same major version we ship.
	//
	// Previously pinned to 4.4.15 to catch operator-allow-list regressions
	// that newer Mongo silently accepts (e.g. $in inside
	// partialFilterExpression). That guard is retired here because prod
	// has moved to Mongo 8; if a regression of that shape recurs, the
	// replacement guard is whatever lint/validation the deploy stack
	// enforces — not an integration-test version pin.
	//
	// Patch-pinned so testcontainers can't drift across patch releases
	// between CI runs. Bump in lockstep with docker-local/compose.deps.yaml.
	Mongo = "mongo:8.2.9"

	// NATS is the image for every NATS-backed integration test
	// (core NATS + JetStream + WebSocket).
	NATS = "nats:2.11-alpine"

	// Node runs the TypeScript end-to-end clients in pkg/roomkeysender
	// and pkg/roomcrypto.
	Node = "node:20-alpine"

	// Elasticsearch is the search engine image for search-sync-worker.
	Elasticsearch = "elasticsearch:8.17.0"

	// Valkey is the Redis-compatible cache used by room-service and
	// pkg/roomkeystore. Tracks docker-local/compose.deps.yaml.
	//
	// Patch-pinned (same rationale as Mongo above).
	Valkey = "valkey/valkey:8.1.7-alpine"

	// MinIO is the image for every MinIO-backed integration test.
	MinIO = "minio/minio:RELEASE.2025-07-18T21-56-31Z"

	// Vault is the HashiCorp Vault image for pkg/atrest's KeyWrapper
	// integration tests (transit secrets engine).
	Vault = "hashicorp/vault:1.18"
)
