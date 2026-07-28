// Package circuitbreaker is a small, dependency-free circuit breaker used to
// fail-fast around a flaky downstream (e.g. MongoDB) instead of stalling every
// caller on the downstream's own timeout. It has three states: closed (calls
// pass through), open (calls fast-fail with ErrOpen), and half-open (a single
// probe call is allowed to test recovery).
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

	mu           sync.Mutex
	state        State
	failures     int
	openedAt     time.Time
	probing      bool // true while a half-open probe is in flight
	onTransition func(from, to State)
}

// Option configures a Breaker at construction.
type Option func(*Breaker)

// WithClock overrides the time source (for tests).
func WithClock(now func() time.Time) Option {
	return func(b *Breaker) { b.now = now }
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
// stays open for cooldown before allowing a half-open probe.
func New(threshold int, cooldown time.Duration, opts ...Option) *Breaker {
	b := &Breaker{threshold: threshold, cooldown: cooldown, now: time.Now, state: StateClosed}
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
	b.mu.Lock()
	prologueOld := b.state
	b.maybeHalfOpenLocked()
	prologueNew := b.state
	prologueCB := b.onTransition

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
	default: // StateClosed: pass through
	}
	b.mu.Unlock()
	fireTransition(prologueCB, prologueOld, prologueNew)

	err := fn()

	b.mu.Lock()
	epilogueOld := b.state
	b.probing = false
	if err != nil {
		b.failures++
		if b.state == StateHalfOpen || b.failures >= b.threshold {
			b.state = StateOpen
			b.openedAt = b.now()
		}
	} else {
		b.failures = 0
		b.state = StateClosed
	}
	epilogueNew := b.state
	epilogueCB := b.onTransition
	b.mu.Unlock()
	fireTransition(epilogueCB, epilogueOld, epilogueNew)

	return err
}

// maybeHalfOpenLocked transitions open->half-open once the cooldown elapses.
// Caller must hold b.mu.
func (b *Breaker) maybeHalfOpenLocked() {
	if b.state == StateOpen && b.now().Sub(b.openedAt) >= b.cooldown {
		b.state = StateHalfOpen
		b.probing = false
	}
}
