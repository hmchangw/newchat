package jsiter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmchangw/chat/pkg/health"
)

// ConsumeContext is the running-consumption handle Consume returns. Both
// jetstream's and the o11y wrapper's ConsumeContext satisfy it.
//
// Closed is part of the surface because Stop is not a barrier: it ends delivery
// and discards the buffer, but a handler already executing runs on (nats.go
// jetstream/pull.go, pullSubscription.Stop). Closed is the signal that in-flight
// work has actually finished.
type ConsumeContext interface {
	Stop()
	Closed() <-chan struct{}
}

// releaseWait bounds how long a stopped round is given to finish work already
// in a handler. Shutdown stops lanes one at a time, so this is paid per lane
// and has to stay well inside the 25s shutdown budget.
var releaseWait = 2 * time.Second

// OpenConsume starts consumption and reports later asynchronous failures through
// onError. Implementations must pass onError to jetstream.ConsumeErrHandler —
// without one, a Consume that the server stops reports nothing at all: the
// handler simply never fires again.
type OpenConsume func(ctx context.Context, onError func(error)) (ConsumeContext, error)

// Supervisor keeps a callback-style Consume running. nats.go stops the
// subscription on a fatal status and reports it only through the error handler,
// so without this the handler silently never fires again.
//
// One goroutine — run — owns every piece of mutable state and walks the whole
// lifecycle in order: start a round, hold it until it fails, back off, start
// the next. nats.go's error callback and Stop only hand it events. Rounds are
// therefore strictly sequential, which is what makes a late error from a
// superseded round structurally unable to disturb the round that replaced it.
type Supervisor struct {
	name string
	ctx  context.Context
	open OpenConsume

	// sleepFn and nowFn are fields so tests drive backoff and the escalation
	// window without real time.
	sleepFn func(context.Context, time.Duration) bool
	nowFn   func() time.Time

	// openCtx bounds the open run is executing, so Stop aborts a call to a peer
	// that is gone instead of waiting it out.
	openCtx    context.Context
	cancelOpen context.CancelFunc

	failures chan roundFailure
	// terminal holds the newest terminal failure that found no room in the
	// queue, and terminalReady wakes a reader for it. A plain slot is not
	// enough: a superseded round can occupy it, and the live round's error —
	// reported exactly once — would then be the one dropped.
	terminal      atomic.Pointer[roundFailure]
	terminalReady chan struct{}
	done          chan struct{}
	exited        chan struct{}
	once          sync.Once

	// up is written by run alone and read by the health check.
	up atomic.Bool
}

// roundFailure is a status error tagged with the round it was reported for, so
// run can tell a live round's failure from a superseded one's.
type roundFailure struct {
	gen uint64
	err error
}

// NewSupervisor starts consumption and returns the supervisor watching it. A
// failure on the first round is a startup failure — the caller decides whether
// to exit. Every later failure is retried instead, forever.
//
// ctx bounds every later restart, so pass the service context.
func NewSupervisor(ctx context.Context, name string, open OpenConsume) (*Supervisor, error) {
	s := buildSupervisor(ctx, name, open)
	if err := s.launch(); err != nil {
		s.Stop()
		return nil, err
	}
	return s, nil
}

// buildSupervisor builds a supervisor without starting it, so sleepFn and nowFn
// can be set before run reads them. Everything else goes through NewSupervisor.
func buildSupervisor(ctx context.Context, name string, open OpenConsume) *Supervisor {
	s := &Supervisor{
		name:          name,
		ctx:           ctx,
		open:          open,
		failures:      make(chan roundFailure, 16),
		terminalReady: make(chan struct{}, 1),
		done:          make(chan struct{}),
		exited:        make(chan struct{}),
	}
	s.openCtx, s.cancelOpen = context.WithCancel(ctx)
	s.sleepFn = s.sleep
	s.nowFn = time.Now
	return s
}

// launch starts the run goroutine and reports the first round's outcome.
func (s *Supervisor) launch() error {
	ready := make(chan error, 1)
	go s.run(ready)
	return <-ready
}

// IsUp reports whether consumption is running: false while restarting and
// after Stop.
func (s *Supervisor) IsUp() bool { return s.up.Load() }

// HealthCheck probes this consumer, which a live NATS connection says nothing
// about.
func (s *Supervisor) HealthCheck() health.Check { return Check(s.name, s.IsUp) }

// Stop ends supervision and the consumption it is watching. It is idempotent,
// unblocks a restart parked on backoff, and returns only once every round —
// including one still being opened — has been released, so a caller may drain
// NATS the moment it returns.
//
// Do not call it from inside an OpenConsume: Stop waits for the opener, so an
// opener waiting for Stop deadlocks, exactly as WaitGroup.Wait does.
func (s *Supervisor) Stop() {
	s.once.Do(func() {
		close(s.done)
		// Abort an open in progress rather than let it finish and briefly start
		// consuming after Stop has returned.
		s.cancelOpen()
	})
	// run opens rounds on its own goroutine, so its exit is also the moment the
	// last one is released: "Stop returned" means no subscription of ours is
	// still running, and the caller may drain NATS.
	<-s.exited
}

// run owns the supervisor's whole lifecycle. ready carries the first round's
// outcome back to NewSupervisor and is signalled exactly once.
func (s *Supervisor) run(ready chan<- error) {
	defer s.cleanup()

	var (
		gen         uint64
		attempt     int
		lastAttempt time.Time
		first       = true
	)

	for {
		gen++

		cc, err := s.startRound(gen)
		switch {
		case errors.Is(err, ErrStopped):
			signal(ready, &first, err)
			return

		case err != nil:
			// The caller of a failed NewSupervisor discards this supervisor and
			// decides whether to exit, so the first round is not retried here.
			if first {
				signal(ready, &first, err)
				return
			}
			// A round that died while starting carries its own disposition, and
			// Stopped means the same here as it does in serve: nothing can be
			// built against a closed connection, so retrying is a busy loop.
			if errors.Is(err, errRoundOverlap) || Classify(err) == Stopped {
				slog.ErrorContext(s.ctx, "jetstream consumption ended while starting",
					"consumer", s.name, "error", err)
				return
			}
			slog.ErrorContext(s.ctx, "jetstream consumption restart failed, retrying",
				"consumer", s.name, "attempt", attempt, "error", err)

		default:
			s.up.Store(true)
			signal(ready, &first, nil)
			if attempt > 0 {
				slog.InfoContext(s.ctx, "jetstream consumption restarted",
					"consumer", s.name, "attempts", attempt)
			}
			keepGoing := s.serve(gen, cc)
			s.up.Store(false)
			if !keepGoing {
				return
			}
		}

		now := s.nowFn()
		attempt = SeedAttempt(attempt, lastAttempt, now)
		lastAttempt = now
		if !s.sleepFn(s.ctx, BackoffStep(attempt)) {
			return
		}
		attempt++
	}
}

// signal reports the first round's outcome to NewSupervisor, once.
func signal(ready chan<- error, first *bool, err error) {
	if *first {
		*first = false
		ready <- err
	}
}

// startRound opens one round and hands back its handle, unless a failure was
// reported against it while it was starting.
//
// open runs here, on run's own goroutine. That is safe because observe never
// blocks — both its sends have a default arm — so there is nothing for open to
// deadlock against, and Stop cuts a slow open short by cancelling openCtx.
// Running it inline is what keeps the whole lifecycle on one goroutine.
func (s *Supervisor) startRound(gen uint64) (ConsumeContext, error) {
	cc, err := s.open(s.openCtx, func(err error) { s.observe(gen, err) })
	if err != nil {
		if s.stopping() {
			return nil, ErrStopped
		}
		return nil, fmt.Errorf("start %s consumption: %w", s.name, err)
	}
	if s.stopping() {
		s.releaseAtEnd(cc)
		return nil, ErrStopped
	}
	// A failure reported while open was still running outranks the handle it
	// invalidates: nats.go has already stopped feeding it, and installing it
	// would leave a dead subscription marked healthy.
	if failure := s.reportedFailure(gen); failure != nil {
		if !s.releaseForRestart(cc) {
			return nil, errRoundOverlap
		}
		return nil, fmt.Errorf("start %s consumption: %w", s.name, failure)
	}
	return cc, nil
}

// reportedFailure takes the failures queued for gen and returns the first that
// ends the round, or nil. Recoverable ones are logged and passed over: nats.go
// re-pulls on its own and the round is still usable.
func (s *Supervisor) reportedFailure(gen uint64) error {
	for {
		var f roundFailure
		select {
		case f = <-s.failures:
		default:
			held, ok := s.takeTerminal()
			if !ok {
				return nil
			}
			f = held
		}

		if f.gen != gen {
			continue
		}
		if Classify(f.err) == Transient {
			slog.WarnContext(s.ctx, "jetstream consumption hit a recoverable error while starting",
				"consumer", s.name, "error", f.err)
			continue
		}
		slog.ErrorContext(s.ctx, "jetstream consumption failed while starting",
			"consumer", s.name, "error", f.err)
		return f.err
	}
}

// stopping reports whether Stop has been called, so an open aborted by its own
// cancelled context is not mistaken for a startup failure.
func (s *Supervisor) stopping() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// serve holds a live round until it fails or Stop arrives, reporting whether a
// replacement should be started.
func (s *Supervisor) serve(gen uint64, cc ConsumeContext) bool {
	var recoverable transientRun

	for {
		var f roundFailure
		select {
		case f = <-s.failures:
		case <-s.terminalReady:
			held, ok := s.takeTerminal()
			if !ok {
				continue // already claimed; the wake is spurious
			}
			f = held
		case <-s.done:
			s.releaseAtEnd(cc)
			return false
		}

		switch s.judge(gen, f, &recoverable) {
		case keepServing:
			continue
		case endRound:
			s.releaseAtEnd(cc)
			slog.ErrorContext(s.ctx, "jetstream consumption ended",
				"consumer", s.name, "error", f.err)
			return false
		default: // restartRound
			if !s.releaseForRestart(cc) {
				return false
			}
			slog.ErrorContext(s.ctx, "jetstream consumption stopped, restarting",
				"consumer", s.name, "error", f.err)
			return true
		}
	}
}

// outcome is what one reported failure means for the round serve is holding.
type outcome int

const (
	keepServing outcome = iota
	restartRound
	endRound
)

// judge decides what one failure means for the live round. A failure from a
// superseded round is ignored, which is what makes a late error harmless.
func (s *Supervisor) judge(gen uint64, f roundFailure, recoverable *transientRun) outcome {
	if f.gen != gen {
		return keepServing
	}
	switch Classify(f.err) {
	case Stopped:
		return endRound
	case Transient:
		n := recoverable.record(s.nowFn())
		if n < TransientEscalation {
			slog.WarnContext(s.ctx, "jetstream consumption hit a recoverable error",
				"consumer", s.name, "attempt", n, "error", f.err)
			return keepServing
		}
		// A round that only ever reports heartbeat misses is stalled, and
		// retrying it forever is the silent stall in a new shape.
		slog.WarnContext(s.ctx, "jetstream consumption kept failing, restarting",
			"consumer", s.name, "attempts", n, "error", f.err)
		return restartRound
	default: // Fatal
		return restartRound
	}
}

// observe is the ConsumeErrHandler. It runs on a nats.go goroutine and must
// never block, so it only queues the failure for run.
func (s *Supervisor) observe(gen uint64, err error) {
	f := roundFailure{gen: gen, err: err}
	select {
	case s.failures <- f:
		return
	default:
	}

	// The queue is full and run is busy — it is parked on backoff, since a live
	// round drains this in a tight loop. A recoverable error is safe to drop
	// here: a consumer that is still failing reports again, and escalation only
	// needs enough of them to trip.
	if Classify(err) == Transient {
		return
	}
	// A terminal error is reported exactly once, so it must not be dropped. Keep
	// the newest round's: an older one is worthless once a replacement exists,
	// while losing the live one installs a dead subscription as healthy.
	for {
		held := s.terminal.Load()
		if held != nil && held.gen > f.gen {
			return
		}
		if s.terminal.CompareAndSwap(held, &f) {
			break
		}
	}
	select {
	case s.terminalReady <- struct{}{}:
	default:
	}
}

// takeTerminal claims the queued terminal failure, if one is still there. The
// wake and the claim are separate, so a reader can arrive to find it already
// taken.
func (s *Supervisor) takeTerminal() (roundFailure, bool) {
	held := s.terminal.Swap(nil)
	if held == nil {
		return roundFailure{}, false
	}
	return *held, true
}

// errRoundOverlap ends supervision when a stopped round's handler is still
// running. Starting a replacement over it is the one thing a FIFO lane must
// never do, so the lane stops visibly instead.
var errRoundOverlap = errors.New("previous round still had work in flight")

// release ends a round and waits for work already inside a handler to finish,
// reporting whether it did. Stop only ends delivery (nats.go pull.go:769); a
// callback already executing runs on.
func (s *Supervisor) release(cc ConsumeContext) bool {
	cc.Stop()

	timer := time.NewTimer(releaseWait)
	defer timer.Stop()
	select {
	case <-cc.Closed():
		return true
	case <-timer.C:
		return false
	}
}

// releaseForRestart ends a round and reports whether a replacement may start.
// It may not while the old handler is still writing: the durable's ack floor
// went with it, so the replacement is handed the very message still in flight
// and the older write can land last. On an ordered lane that is a silent
// ordering violation, which is strictly worse than a lane that stops — the stop
// shows up on /readyz, the reordering shows up in the data.
func (s *Supervisor) releaseForRestart(cc ConsumeContext) bool {
	if s.release(cc) {
		return true
	}
	slog.ErrorContext(s.ctx, "jetstream consumption still had work in flight; ending supervision rather than overlapping rounds",
		"consumer", s.name, "waited", releaseWait)
	return false
}

// releaseAtEnd ends a round nothing will replace. Shutdown has its own deadline
// and must finish, so a handler past the bound is reported, not waited out.
func (s *Supervisor) releaseAtEnd(cc ConsumeContext) {
	if !s.release(cc) {
		slog.WarnContext(s.ctx, "jetstream consumption still had work in flight when it was stopped",
			"consumer", s.name, "waited", releaseWait)
	}
}

// cleanup runs when run exits. Closing exited releases an attempt still in
// flight, which then stops its own handle — starts is unbuffered, so there is
// never one in a buffer for nobody to stop.
func (s *Supervisor) cleanup() {
	s.up.Store(false)
	// Nothing is left to receive a start, so an open still running can only
	// produce a handle for its own goroutine to release. Cancelling cuts that
	// short and releases the context whether or not Stop was the reason run
	// ended.
	s.cancelOpen()
	close(s.exited)
}

// transientRun counts consecutive recoverable errors. Consume offers no
// progress signal, so the run is bounded by time: a gap wider than
// EscalationWindow starts a fresh one.
type transientRun struct {
	n    int
	last time.Time
}

func (r *transientRun) record(now time.Time) int {
	if r.n > 0 && now.Sub(r.last) > EscalationWindow {
		r.n = 0
	}
	r.n++
	r.last = now
	return r.n
}

func (s *Supervisor) sleep(ctx context.Context, d time.Duration) bool {
	return SleepUntil(ctx, s.done, d)
}
