package natsmetrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Iterator is the context-aware pull iterator surface used by the o11y NATS
// wrapper. It is intentionally small so loop failure behavior is unit-testable.
type Iterator interface {
	Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error)
	Stop()
}

type ClassifyEvent func(jetstream.Msg) EventType
type ProcessMessage func(context.Context, *Message)

// Start registers the consumer loop with wg and then runs Consume in its own
// goroutine. Registering before returning is the point: shutdown stops the
// iterator and then waits on wg, so a loop counted only once it dispatches a
// message lets that wait pass through while a message Next already returned is
// still on its way to a worker.
func Start(ctx context.Context, iter Iterator, consumer *Consumer, maxWorkers, maxDeliver int, wg *sync.WaitGroup, classify ClassifyEvent, process ProcessMessage) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Consume(ctx, iter, consumer, maxWorkers, maxDeliver, wg, classify, process)
	}()
}

// Consume runs the existing bounded concurrent pull pattern. Callers mark the
// loop started only after iterator creation succeeds; Consume marks it stopped
// before returning from a terminal Next failure.
//
// classify runs inside the worker goroutine, not on the dispatch path: a
// classifier that parses the payload would otherwise serialize the whole
// consumer behind one message at a time.
func Consume(ctx context.Context, iter Iterator, consumer *Consumer, maxWorkers, maxDeliver int, wg *sync.WaitGroup, classify ClassifyEvent, process ProcessMessage) error {
	// A zero maxWorkers would make sem unbuffered and park the dispatch send
	// forever — the loop stops consuming with no error and no terminal metric,
	// while the loop-up gauge still reads 1. A negative one panics in make.
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	sem := make(chan struct{}, maxWorkers)
	_, err := consume(ctx, iter, consumer, maxDeliver, wg, sem, classify, process)
	return err
}

func consume(ctx context.Context, iter Iterator, consumer *Consumer, maxDeliver int, wg *sync.WaitGroup, sem chan struct{}, classify ClassifyEvent, process ProcessMessage) (bool, error) {
	received := false
	for {
		msgCtx, msg, err := iter.Next()
		if err != nil {
			if ctx.Err() != nil {
				consumer.LoopStopped(ctx)
				return received, ctx.Err()
			}
			consumer.LoopFailed(ctx, err)
			return received, err
		}
		received = true
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			consumer.LoopStopped(ctx)
			eventType := EventUnknown
			if classify != nil {
				eventType = NormalizeEventType(string(classify(msg)))
			}
			tracked := consumer.Track(msgCtx, msg, eventType, maxDeliver)
			tracked.Finish(ctx)
			return received, ctx.Err()
		}
		wg.Add(1)
		go func(msgCtx context.Context, msg jetstream.Msg) {
			eventType := EventUnknown
			if classify != nil {
				eventType = NormalizeEventType(string(classify(msg)))
			}
			tracked := consumer.Track(msgCtx, msg, eventType, maxDeliver)
			defer func() {
				tracked.Finish(msgCtx)
				<-sem
				wg.Done()
			}()
			process(tracked.Context(msgCtx), tracked)
		}(msgCtx, msg)
	}
}

const (
	defaultSupervisorMinBackoff = 250 * time.Millisecond
	defaultSupervisorMaxBackoff = 5 * time.Second
)

var ErrSupervisorRunning = errors.New("natsmetrics: supervisor already running")

type waitBackoff func(context.Context, time.Duration) error

// SupervisorConfig bounds recreation backoff. Non-positive values use the
// package defaults; MaxBackoff below MinBackoff is raised to MinBackoff.
type SupervisorConfig struct {
	MinBackoff time.Duration
	MaxBackoff time.Duration
	wait       waitBackoff
}

// IteratorFactory recreates both the durable consumer handle and its pull
// iterator. Callers keep stream, durable, and pull settings closed over in the
// factory so recovery cannot drift from startup configuration.
type IteratorFactory func(context.Context) (Iterator, error)

// Supervisor owns exactly one iterator at a time and recreates it after a
// recoverable terminal iterator failure. A deleted durable is not recoverable:
// recreating it would lose the server-side cursor and either replay or skip
// retained work depending on its delivery policy. One semaphore is shared
// across generations so recovery cannot exceed the configured worker bound
// while handlers from the previous generation are still in flight.
type Supervisor struct {
	minBackoff time.Duration
	maxBackoff time.Duration
	wait       waitBackoff
	running    atomic.Bool

	mu     sync.Mutex
	cancel context.CancelFunc
	iter   Iterator
}

func NewSupervisor(cfg SupervisorConfig) *Supervisor {
	minBackoff := cfg.MinBackoff
	if minBackoff <= 0 {
		minBackoff = defaultSupervisorMinBackoff
	}
	maxBackoff := cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultSupervisorMaxBackoff
	}
	if maxBackoff < minBackoff {
		maxBackoff = minBackoff
	}
	wait := cfg.wait
	if wait == nil {
		wait = waitForBackoff
	}
	return &Supervisor{minBackoff: minBackoff, maxBackoff: maxBackoff, wait: wait}
}

// Start creates the first consumer iterator synchronously so startup remains
// fail-fast. After that first successful creation it registers the supervisor
// loop with wg before returning. A second concurrent Start is rejected so one
// process cannot create duplicate pull iterators for the same durable through
// this supervisor.
func (s *Supervisor) Start(ctx context.Context, factory IteratorFactory, consumer *Consumer, maxWorkers, maxDeliver int, wg *sync.WaitGroup, classify ClassifyEvent, process ProcessMessage) error {
	if !s.running.CompareAndSwap(false, true) {
		return ErrSupervisorRunning
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()
	consumer.LoopStopped(runCtx)
	iter, err := factory(runCtx)
	if err != nil {
		s.abortStart(cancel)
		return fmt.Errorf("create initial consumer iterator: %w", err)
	}
	if !s.activate(runCtx, iter) {
		iter.Stop()
		s.abortStart(cancel)
		return runCtx.Err()
	}
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	sem := make(chan struct{}, maxWorkers)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer s.finish(runCtx, consumer)
		s.run(runCtx, iter, factory, consumer, maxDeliver, wg, sem, classify, process)
	}()
	return nil
}

func (s *Supervisor) run(ctx context.Context, iter Iterator, factory IteratorFactory, consumer *Consumer, maxDeliver int, wg *sync.WaitGroup, sem chan struct{}, classify ClassifyEvent, process ProcessMessage) {
	backoff := s.minBackoff
	recovered := false
	for {
		if recovered {
			consumer.RecoveryAttempt(ctx, RecoverySuccess)
		}
		consumer.LoopStarted(ctx)
		received, err := consume(ctx, iter, consumer, maxDeliver, wg, sem, classify, process)
		s.stopActive()
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, jetstream.ErrConsumerDeleted) {
			slog.ErrorContext(ctx, "jetstream consumer deleted; automatic recreation disabled to preserve delivery position", "error", err)
			return
		}
		if received {
			backoff = s.minBackoff
		} else if recovered {
			backoff = nextBackoff(backoff, s.maxBackoff)
		}
		slog.WarnContext(ctx, "jetstream consumer loop failed; recreating", "error", err, "retry_in", backoff)

		for {
			if err := s.wait(ctx, backoff); err != nil {
				return
			}
			iter, err = factory(ctx)
			if err == nil {
				if !s.activate(ctx, iter) {
					iter.Stop()
					return
				}
				recovered = true
				break
			}
			if ctx.Err() != nil {
				return
			}
			consumer.RecoveryAttempt(ctx, RecoveryFailure)
			backoff = nextBackoff(backoff, s.maxBackoff)
			slog.WarnContext(ctx, "jetstream consumer recreation failed", "error", err, "retry_in", backoff)
		}
	}
}

func (s *Supervisor) abortStart(cancel context.CancelFunc) {
	cancel()
	s.mu.Lock()
	s.cancel = nil
	s.iter = nil
	s.mu.Unlock()
	s.running.Store(false)
}

// Stop cancels retry backoff and stops the active iterator. It is idempotent;
// callers then wait on the shared WaitGroup to preserve in-flight draining.
func (s *Supervisor) Stop() {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	iter := s.iter
	s.iter = nil
	s.mu.Unlock()
	if iter != nil {
		iter.Stop()
	}
}

func (s *Supervisor) activate(ctx context.Context, iter Iterator) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	s.iter = iter
	return true
}

func (s *Supervisor) stopActive() {
	s.mu.Lock()
	iter := s.iter
	s.iter = nil
	s.mu.Unlock()
	if iter != nil {
		iter.Stop()
	}
}

func (s *Supervisor) finish(ctx context.Context, consumer *Consumer) {
	consumer.LoopStopped(ctx)
	s.mu.Lock()
	s.cancel = nil
	s.iter = nil
	s.mu.Unlock()
	s.running.Store(false)
}

func waitForBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}
