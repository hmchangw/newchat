// Package circuitbreaker is a small circuit breaker used to fail-fast around a
// flaky downstream (e.g. MongoDB) instead of stalling every caller on the
// downstream's own timeout. It has three states: closed (calls pass through),
// open (calls fast-fail with ErrOpen), and half-open (a single probe call is
// allowed to test recovery).
//
// The breaker itself (this file) depends only on the standard library. State
// reporting lives in metric.go, which pulls in OpenTelemetry and log/slog;
// keep that split if the core is ever vendored somewhere leaner.
package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

// ErrOpen is returned by Do when the breaker is open and the wrapped function
// is therefore not invoked.
var ErrOpen = errors.New("circuit breaker open")

// State is the breaker's current state.
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

// String renders the state as a lowercase, hyphenated label suitable for log
// fields and metric values (e.g. "closed", "half-open").
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Breaker is a concurrency-safe circuit breaker. The zero value is not usable;
// construct with New.
type Breaker struct {
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time
	probing  bool // true while a half-open probe is in flight
	// generation increments every time the breaker opens. A call captures it on
	// entry and compares on return, so a result produced before an outage can be
	// recognized as stale evidence about a downstream that has since failed.
	generation   uint64
	onTransition func(from, to State)
	isFailure    func(error) bool
}

// Option configures a Breaker at construction.
type Option func(*Breaker)

// WithClock overrides the time source (for tests).
func WithClock(now func() time.Time) Option {
	return func(b *Breaker) { b.now = now }
}

// WithFailurePredicate decides which errors count against the failure budget.
// It defaults to "every non-nil error is a failure"; supply one when the
// downstream returns errors that are healthy answers rather than signs of
// trouble — a not-found being the canonical case. An exempted error is still
// returned to the caller unchanged, it just does not move the breaker.
func WithFailurePredicate(isFailure func(error) bool) Option {
	return func(b *Breaker) { b.isFailure = isFailure }
}

// WithOnTransition registers a callback invoked on every state change, after
// the state has already been updated and the breaker's lock released — the
// callback runs outside b.mu so it can safely call back into the breaker
// (e.g. b.State()) without deadlocking. It is never invoked when the state
// does not change (e.g. a failure below threshold, or a State() call that
// doesn't advance open->half-open).
func WithOnTransition(fn func(from, to State)) Option {
	return func(b *Breaker) { b.onTransition = fn }
}

// fireTransition invokes cb with (old, new) when the state actually changed.
// Called after the breaker's lock has been released.
func fireTransition(cb func(from, to State), old, newState State) {
	if cb != nil && old != newState {
		cb(old, newState)
	}
}

// New builds a breaker that opens after threshold consecutive failures and
// stays open for cooldown before allowing a half-open probe. A non-positive
// threshold disables the breaker: calls always pass through and it never opens,
// so a service can turn the protection off by config without unwiring it. A nil
// *Breaker behaves the same way, so an optional breaker needs no nil guard at
// the call site.
func New(threshold int, cooldown time.Duration, opts ...Option) *Breaker {
	b := &Breaker{
		threshold: threshold, cooldown: cooldown, now: time.Now, state: StateClosed,
		isFailure: func(err error) bool { return err != nil },
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// State returns the breaker's current state, advancing open->half-open if the
// cooldown has elapsed.
func (b *Breaker) State() State {
	b.mu.Lock()
	old := b.state
	b.maybeHalfOpenLocked()
	newState := b.state
	cb := b.onTransition
	b.mu.Unlock()
	fireTransition(cb, old, newState)
	return newState
}

// Do runs fn unless the breaker is open. When open (and past cooldown, unless a
// probe is already in flight) it returns ErrOpen without invoking fn. The
// result of fn updates the breaker: success closes it, failure increments the
// counter and may (re)open it.
func (b *Breaker) Do(fn func() error) error {
	// A nil breaker and a non-positive threshold are the same thing — protection
	// turned off — so callers can hold an optional *Breaker without each one
	// re-inventing its own nil check.
	if b == nil || b.threshold <= 0 {
		return fn()
	}

	b.mu.Lock()
	prologueOld := b.state
	b.maybeHalfOpenLocked()
	prologueNew := b.state
	prologueCB := b.onTransition

	isProbe := false
	switch b.state {
	case StateOpen:
		b.mu.Unlock()
		fireTransition(prologueCB, prologueOld, prologueNew)
		return ErrOpen
	case StateHalfOpen:
		if b.probing {
			b.mu.Unlock()
			fireTransition(prologueCB, prologueOld, prologueNew)
			return ErrOpen
		}
		b.probing = true
		isProbe = true
	default: // StateClosed: pass through
	}
	entryGen := b.generation
	b.mu.Unlock()
	fireTransition(prologueCB, prologueOld, prologueNew)

	err := fn()

	b.mu.Lock()
	epilogueOld := b.state
	// Only the goroutine that claimed the probe slot may release it. Clearing it
	// from any other call would let the next caller start a second concurrent
	// probe against a downstream the breaker is trying to shield.
	if isProbe {
		b.probing = false
	}
	switch {
	case b.isFailure(err):
		b.failures++
		if b.state == StateHalfOpen || b.failures >= b.threshold {
			b.openLocked()
		}
	case err != nil:
		// Exempted error: a healthy answer, so it neither opens the breaker nor
		// counts as evidence of recovery.
	case b.generation != entryGen:
		// Stale success: the breaker opened while this call was in flight, so
		// the result describes a downstream that has since failed. Closing on it
		// would release the herd on evidence that predates the outage.
	default:
		b.failures = 0
		b.state = StateClosed
	}
	epilogueNew := b.state
	epilogueCB := b.onTransition
	b.mu.Unlock()
	fireTransition(epilogueCB, epilogueOld, epilogueNew)

	return err
}

// openLocked moves the breaker to open and starts a new generation, so calls
// already in flight can detect that their result is stale. Caller must hold b.mu.
func (b *Breaker) openLocked() {
	b.state = StateOpen
	b.openedAt = b.now()
	b.generation++
}

// maybeHalfOpenLocked transitions open->half-open once the cooldown elapses.
// Caller must hold b.mu.
func (b *Breaker) maybeHalfOpenLocked() {
	if b.state == StateOpen && b.now().Sub(b.openedAt) >= b.cooldown {
		b.state = StateHalfOpen
		b.probing = false
	}
}

// Do1 is Do for a function that also returns a value. It exists because the
// value-returning form is otherwise four lines of capture-and-reassign at every
// call site, repeated once per wrapped method.
//
// On failure the zero value is returned alongside the error, including when the
// breaker is open and fn never ran.
func Do1[T any](b *Breaker, fn func() (T, error)) (T, error) {
	var out T
	if err := b.Do(func() error {
		var innerErr error
		out, innerErr = fn()
		return innerErr
	}); err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}
