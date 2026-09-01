# Runtime Control API Follow-up

Status: design skeleton only. PR #271 does not implement runtime control.

## Scope

A future internal control plane may expose authenticated:

- `pause`: stop future pacing events without canceling in-flight work;
- `resume`: resume from the current time without catch-up or burst release;
- `status`: report enabled state, generation, and last change metadata.

The future control plane exposes an authenticated `GET /control/status`. An
unauthenticated status endpoint is acceptable only if it returns a separately
reviewed, non-sensitive projection.

The desired state must survive process replacement. Every accepted change
increments a generation and records actor, request ID, reason, and UTC change
time without storing credentials. Concurrent changes use generation-based
compare-and-set semantics.

## Safety boundary

Ingress is disabled by default and cluster-internal when enabled. Public
ingress is an explicit exception. It requires separate credentials supplied by
a Kubernetes Secret and must not share public application auth. The API does
not inject faults, restart dependencies, or implement an automatic safety stop.

Paused intervals are shaded. Any interval where
`loadgen_dispatch_enabled == 0` is `INCONCLUSIVE` for dispatch and absence
claims and must never be presented as `NO_IMPACT`. Resuming schedules only
future work: no accumulated deficit is emitted, and open-loop target rates
remain unchanged.
