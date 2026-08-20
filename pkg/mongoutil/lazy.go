package mongoutil

// WithLazyConnect skips the ping that Connect normally sends to check MongoDB
// is reachable, so a service starts even while MongoDB is down.
//
// Why that matters: mongo.Connect never actually talks to MongoDB — it just
// builds a client. The ping is the only thing that proves the database is
// there, and every caller quits when it fails. So a service that happens to
// restart during an outage (a deploy, a node drain, an out-of-memory kill)
// dies at startup, and Kubernetes restarts it in a loop, waiting longer each
// time — up to five minutes. Services then stay down for minutes after
// MongoDB is already back.
//
// Skipping the ping does not mean the client never connects. The driver keeps
// checking for MongoDB in the background by itself. While it is down, queries
// return an error after about 30 seconds (sooner if the caller's context runs
// out) rather than hanging. When MongoDB comes back, queries simply start
// working again — no restart, and no reconnect code of our own.
// TestConnect_LazyBootsDuringOutageThenRecovers is the proof.
//
// The ping stays on by default. Only use this for services that keep running
// and can still do something useful with MongoDB down. Batch and CLI jobs
// (data-migration/*, teams-*, tools/*, seed-sample-data) should keep it: they
// have nothing useful to do without MongoDB, so starting anyway would turn a
// clear "cannot reach MongoDB" at startup into a confusing failure later on.
func WithLazyConnect() Option {
	return func(c *connectConfig) { c.lazy = true }
}
