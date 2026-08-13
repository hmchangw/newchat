package mongoutil

// WithLazyConnect skips the startup ping, so Connect returns a client without
// first proving MongoDB is reachable. The driver connects on demand instead.
//
// This exists so a long-running online service can still *start* during a
// MongoDB outage. Without it, a pod that restarts mid-outage (deploy, node
// drain, OOM kill, HPA scale-up) dies at boot and stays down for the rest of
// the outage plus the CrashLoopBackOff backoff after MongoDB returns — the
// request-path resilience work is worth nothing to a process that can't boot.
//
// The ping is correct default behaviour and stays the default. Opt in ONLY for
// long-running online services that have a useful degraded mode. Batch,
// migration and CLI jobs (data-migration/*, teams-*, hr-sync-worker, tools/*,
// seed-sample-data) must keep the ping: they have no degraded mode, and a
// silent lazy start would turn a clear startup failure into a confusing one
// deep into a run.
//
// A lazily-connected client does not defer failure indefinitely: the first
// operation still fails, bounded by the driver's server-selection timeout
// (30s by default) or the caller's context deadline, whichever is shorter.
func WithLazyConnect() Option {
	return func(c *connectConfig) { c.lazy = true }
}
