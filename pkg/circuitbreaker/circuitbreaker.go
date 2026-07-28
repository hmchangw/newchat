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
}

// Option configures a Breaker at construction.
type Option func(*Breaker)

// WithClock overrides the time source (for tests).
func WithClock(now func() time.Time) Option {
	return func(b *Breaker) { b.now = now }
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
	defer b.mu.Unlock()
	b.maybeHalfOpenLocked()
	return b.state
}

// Do runs fn unless the breaker is open. When open (and past cooldown, unless a
// probe is already in flight) it returns ErrOpen without invoking fn. The
// result of fn updates the breaker: success closes it, failure increments the
// counter and may (re)open it.
func (b *Breaker) Do(fn func() error) error {
	b.mu.Lock()
	b.maybeHalfOpenLocked()
	switch b.state {
	case StateOpen:
		b.mu.Unlock()
		return ErrOpen
	case StateHalfOpen:
		if b.probing {
			b.mu.Unlock()
			return ErrOpen
		}
		b.probing = true
	default: // StateClosed: pass through
	}
	b.mu.Unlock()

	err := fn()

	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
	if err != nil {
		b.failures++
		if b.state == StateHalfOpen || b.failures >= b.threshold {
			b.state = StateOpen
			b.openedAt = b.now()
		}
		return err
	}
	b.failures = 0
	b.state = StateClosed
	return nil
}

// maybeHalfOpenLocked transitions open->half-open once the cooldown elapses.
// Caller must hold b.mu.
func (b *Breaker) maybeHalfOpenLocked() {
	if b.state == StateOpen && b.now().Sub(b.openedAt) >= b.cooldown {
		b.state = StateHalfOpen
		b.probing = false
	}
}
