package mongoutil

// WithLazyConnect skips the startup ping, so Connect returns a client without
// first proving MongoDB is reachable. It exists so a long-running online
// service can still boot during a MongoDB outage instead of dying at startup
// and crash-looping for the rest of it.
//
// The ping stays the default. Opt in ONLY for long-running online services
// that have a useful degraded mode — a process with a health server and a
// consume/serve loop. Batch, migration and CLI jobs (data-migration/*,
// teams-*, tools/*, seed-sample-data) must keep the ping: they have no
// degraded mode, so a lazy start would turn a clear startup failure into a
// confusing one deep into a run.
//
// A lazily-connected client does not defer failure indefinitely: the first
// operation still fails, bounded by the driver's server-selection timeout
// (30s by default) or the caller's context deadline, whichever is shorter.
func WithLazyConnect() Option {
	return func(c *connectConfig) { c.lazy = true }
}
