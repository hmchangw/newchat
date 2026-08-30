// Package jsiter keeps JetStream consumers running across the errors nats.go
// reports through Next and through Consume's error handler.
//
// nats.go reports two very different things through one error. ErrNoHeartbeat
// leaves the consumer live — it has already re-issued the pull — while
// ErrConsumerDeleted stops it for good, and Consume stops its subscription
// with no notice at all unless a ConsumeErrHandler is installed. A loop that
// returns on any error treats the first as the second: one hiccup on an
// inter-site link, the pump goroutine exits, and the service consumes nothing
// more while /readyz, which probes only the NATS connection, stays green.
//
// # The three recoverers
//
// One idea, three shapes, because the caller's loop differs each time:
//
//   - Pump wraps a pull iterator. The caller drives it: Next blocks, recovers,
//     and returns. See pump.go.
//   - Supervisor wraps a Consume subscription. nats.go drives it, so the
//     recovery has to live in a goroutine of its own. See supervisor.go.
//   - recoveringFetcher, in search-sync-worker, wraps a Fetch loop, where the
//     failure arrives through batch.Error() rather than through the call.
//
// All three use the same words: open makes a new one, cur holds it, attempt
// indexes the backoff, up feeds the readiness probe. All three follow the same
// four steps on a failure, and those steps live here, once:
//
//	Classify(err)        -> Stopped / Transient / Fatal      classify.go
//	count transients     -> escalate at TransientEscalation  (per recoverer)
//	SeedAttempt(...)     -> where to resume the backoff       backoff.go
//	SleepUntil(...)      -> park, interruptible by Stop       backoff.go
//
// # Tracing a rebuild
//
// Pump is a plain loop: Next -> Classify -> rebuild -> open -> Next.
//
// Supervisor is one goroutine too, which is the thing to hold on to: run walks
// the whole lifecycle in order and opens each round inline. The only other
// thread is nats.go's, calling observe, and observe never blocks — both its
// sends have a default arm. That single boundary is what makes a late error
// from a superseded round unable to disturb the round that replaced it. One
// consumer-deleted error travels:
//
//	observe(gen, err)      // on a nats.go goroutine; queues, never blocks
//	  -> s.failures        // or s.terminal, when the queue is full
//	     -> serve          // judge ignores it unless gen is the live round
//	        -> run         // up=false, release, SeedAttempt, sleepFn
//	           -> startRound(gen+1)  // inline: open, then take any failure
//	              -> run              // already reported against it
//	                                  // up=true, back to serve
//
// One edge there is a channel, so no editor will jump it for you: the callback
// reaches run only through s.failures or the s.terminal mailbox beside it.
//
// # Reading order
//
//	classify.go   which errors mean what
//	backoff.go    how long to wait, and where to resume
//	pump.go       the pull-iterator recoverer
//	supervisor.go the Consume recoverer
//	source.go     the constructors every service wires (Resolve/PullFrom/ConsumeFrom)
//	health.go     the readiness probe
//
// CLAUDE.md, "JetStream Consumer Recovery", carries the rules a caller must
// follow and the nats.go line references behind them.
package jsiter
