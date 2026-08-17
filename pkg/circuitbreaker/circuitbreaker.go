// Package circuitbreaker fails fast around a flaky downstream (e.g. MongoDB)
// instead of stalling every caller on the downstream's own timeout. It has three
// states: closed (calls pass through), open (calls fast-fail with ErrOpen), and
// half-open (a single probe call is allowed to test recovery).
//
// The state machine itself is github.com/sony/gobreaker/v2. This package is the
// repo's adapter over it, and exists for three things gobreaker does not provide:
//
//   - A nil *Breaker and a non-positive threshold both mean "protection turned
//     off, pass everything through", so a service can wire a breaker
//     unconditionally and disable it by config. A nil gobreaker panics.
//   - A three-way outcome. gobreaker's IsSuccessful is binary, but a downstream
//     can answer with an error that is a healthy answer — a not-found being the
//     canonical case — and that must neither open the breaker nor count as
//     evidence of recovery. IsExcluded supplies the neutral third outcome.
//   - One ErrOpen sentinel. gobreaker rejects with ErrOpenState when open and
//     ErrTooManyRequests when a probe is already in flight; callers care only
//     that the call did not run.
//
// State reporting lives in metric.go, which pulls in OpenTelemetry and log/slog.
package circuitbreaker

import (
	"errors"
	"time"

	"github.com/sony/gobreaker/v2"
)

// ErrOpen is returned by Do when the breaker is open and the wrapped function
// is therefore not invoked.
var ErrOpen = errors.New("circuit breaker open")

// State is the breaker's current state.
//
// The values are the contract of the circuit_breaker_state gauge (see metric.go)
// and deliberately do NOT match gobreaker's own iota order, which puts half-open
// at 1 and open at 2. fromGobreaker maps between them; never cast.
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

// fromGobreaker translates gobreaker's state into ours. Written as an explicit
// switch because the two iota orders differ.
func fromGobreaker(s gobreaker.State) State {
	switch s {
	case gobreaker.StateClosed:
		return StateClosed
	case gobreaker.StateOpen:
		return StateOpen
	case gobreaker.StateHalfOpen:
		return StateHalfOpen
	default:
		return StateClosed
	}
}

// Breaker is a concurrency-safe circuit breaker. The zero value is not usable;
// construct with New.
type Breaker struct {
	threshold int
	cb        *gobreaker.CircuitBreaker[any]
}

// Option configures a Breaker at construction.
type Option func(*settings)

// settings collects the options before the underlying breaker is built, since
// gobreaker takes its whole configuration at construction time.
type settings struct {
	isFailure    func(error) bool
	onTransition func(from, to State)
}

// WithFailurePredicate decides which errors count against the failure budget.
// It defaults to "every non-nil error is a failure"; supply one when the
// downstream returns errors that are healthy answers rather than signs of
// trouble — a not-found being the canonical case. An exempted error is still
// returned to the caller unchanged, it just does not move the breaker: it
// neither counts toward the trip threshold nor resets the failure count, so an
// alternating stream of timeouts and not-founds still trips.
func WithFailurePredicate(isFailure func(error) bool) Option {
	return func(s *settings) { s.isFailure = isFailure }
}

// WithOnTransition registers a callback invoked on every state change. It is
// never invoked when the state does not change.
func WithOnTransition(fn func(from, to State)) Option {
	return func(s *settings) { s.onTransition = fn }
}

// New builds a breaker that opens after threshold consecutive failures and stays
// open for cooldown before allowing a half-open probe. A non-positive threshold
// disables the breaker: calls always pass through and it never opens, so a
// service can turn the protection off by config without unwiring it. A nil
// *Breaker behaves the same way, so an optional breaker needs no nil guard at
// the call site.
func New(threshold int, cooldown time.Duration, opts ...Option) *Breaker {
	s := settings{isFailure: func(err error) bool { return err != nil }}
	for _, opt := range opts {
		opt(&s)
	}

	b := &Breaker{threshold: threshold}
	if threshold <= 0 {
		// Nothing to build: Do short-circuits to fn before touching cb.
		return b
	}

	onStateChange := func(_ string, from, to gobreaker.State) {
		if s.onTransition != nil {
			s.onTransition(fromGobreaker(from), fromGobreaker(to))
		}
	}
	b.cb = gobreaker.NewCircuitBreaker[any](gobreaker.Settings{
		MaxRequests: 1, // one half-open probe, so recovery never stampedes
		Interval:    0, // never auto-reset the failure count while closed
		Timeout:     cooldown,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// #nosec G115 -- threshold is > 0 here, and a configured failure budget never approaches 2^31
			return counts.ConsecutiveFailures >= uint32(threshold) //nolint:gosec // G115: see the #nosec note above
		},
		// Only a nil error is evidence of recovery. Everything else is either a
		// failure or excluded below — see WithFailurePredicate.
		IsSuccessful:  func(err error) bool { return err == nil },
		IsExcluded:    func(err error) bool { return err != nil && !s.isFailure(err) },
		OnStateChange: onStateChange,
	})
	return b
}

// State returns the breaker's current state, advancing open->half-open if the
// cooldown has elapsed.
func (b *Breaker) State() State {
	if b == nil || b.cb == nil {
		return StateClosed
	}
	return fromGobreaker(b.cb.State())
}

// Do runs fn unless the breaker is open. When open (or when a half-open probe is
// already in flight) it returns ErrOpen without invoking fn. The result of fn
// updates the breaker: success closes it, failure increments the counter and may
// (re)open it.
func (b *Breaker) Do(fn func() error) error {
	// A nil breaker and a non-positive threshold are the same thing — protection
	// turned off — so callers can hold an optional *Breaker without each one
	// re-inventing its own nil check.
	if b == nil || b.cb == nil {
		return fn()
	}
	_, err := b.cb.Execute(func() (any, error) { return nil, fn() })
	return translate(err)
}

// translate collapses gobreaker's two rejection errors into ErrOpen, so callers
// have one sentinel meaning "the breaker refused, fn never ran". Any other error
// is fn's own and passes through untouched.
func translate(err error) error {
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return ErrOpen
	}
	return err
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
