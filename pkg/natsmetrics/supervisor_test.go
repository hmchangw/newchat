package natsmetrics

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type supervisorTestIterator struct {
	next func() (context.Context, jetstream.Msg, error)
	stop func()
}

func (i *supervisorTestIterator) Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	return i.next()
}

func (i *supervisorTestIterator) Stop() {
	if i.stop != nil {
		i.stop()
	}
}

func blockingSupervisorIterator(created chan<- struct{}, active, maxActive *atomic.Int64, duplicate *atomic.Bool) *supervisorTestIterator {
	if active.Add(1) != 1 {
		duplicate.Store(true)
	}
	for {
		current := active.Load()
		previous := maxActive.Load()
		if current <= previous || maxActive.CompareAndSwap(previous, current) {
			break
		}
	}
	stopped := make(chan struct{})
	var stopOnce sync.Once
	close(created)
	return &supervisorTestIterator{
		next: func() (context.Context, jetstream.Msg, error) {
			<-stopped
			return nil, nil, jetstream.ErrConsumerDeleted
		},
		stop: func() {
			stopOnce.Do(func() {
				active.Add(-1)
				close(stopped)
			})
		},
	}
}

func failingSupervisorIterator(active, maxActive *atomic.Int64, duplicate *atomic.Bool, err error) *supervisorTestIterator {
	if active.Add(1) != 1 {
		duplicate.Store(true)
	}
	for {
		current := active.Load()
		previous := maxActive.Load()
		if current <= previous || maxActive.CompareAndSwap(previous, current) {
			break
		}
	}
	var stopOnce sync.Once
	return &supervisorTestIterator{
		next: func() (context.Context, jetstream.Msg, error) { return nil, nil, err },
		stop: func() {
			stopOnce.Do(func() { active.Add(-1) })
		},
	}
}

func TestSupervisor_RecoversAfterTerminalIteratorFailure(t *testing.T) {
	m, reader := newTestMetrics(t)
	consumer := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "site-a", Stream: "STREAM-site-a", Consumer: "durable"})
	var (
		factoryCalls atomic.Int64
		active       atomic.Int64
		maxActive    atomic.Int64
		duplicate    atomic.Bool
	)
	recovered := make(chan struct{})
	factory := func(context.Context) (Iterator, error) {
		if factoryCalls.Add(1) == 1 {
			return failingSupervisorIterator(&active, &maxActive, &duplicate, errors.New("transient iterator failure")), nil
		}
		return blockingSupervisorIterator(recovered, &active, &maxActive, &duplicate), nil
	}

	supervisor := NewSupervisor(SupervisorConfig{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		wait:       func(context.Context, time.Duration) error { return nil },
	})
	var wg sync.WaitGroup
	require.NoError(t, supervisor.Start(context.Background(), factory, consumer, 2, 5, &wg, nil, func(context.Context, *Message) {}))

	select {
	case <-recovered:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not recreate the iterator")
	}
	supervisor.Stop()
	wg.Wait()

	assert.Equal(t, int64(2), factoryCalls.Load())
	assert.Equal(t, int64(1), maxActive.Load())
	assert.False(t, duplicate.Load(), "a replacement iterator started before the failed iterator stopped")
	assert.Zero(t, active.Load())

	rm := collect(t, reader)
	terminal := metricPoints[int64](t, rm, "chat.nats.terminal.failures")
	require.Len(t, terminal, 1)
	assert.Equal(t, string(TerminalInternal), attrs(terminal[0])["reason"])
	recoveries := metricPoints[int64](t, rm, "chat.nats.consumer.recovery.attempts")
	require.Len(t, recoveries, 1)
	assert.Equal(t, string(RecoverySuccess), attrs(recoveries[0])["result"])
	loop := metricPoints[int64](t, rm, "chat.nats.consumer.loop.up")
	require.Len(t, loop, 1)
	assert.Zero(t, loop[0].Value)
}

func TestSupervisor_DeletedConsumerStopsWithoutRecreation(t *testing.T) {
	m, reader := newTestMetrics(t)
	consumer := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "site-a", Stream: "STREAM-site-a", Consumer: "durable"})
	var (
		factoryCalls atomic.Int64
		active       atomic.Int64
		maxActive    atomic.Int64
		duplicate    atomic.Bool
	)
	recreated := make(chan struct{})
	factory := func(context.Context) (Iterator, error) {
		if factoryCalls.Add(1) == 1 {
			return failingSupervisorIterator(&active, &maxActive, &duplicate, fmt.Errorf("next message: %w", jetstream.ErrConsumerDeleted)), nil
		}
		return blockingSupervisorIterator(recreated, &active, &maxActive, &duplicate), nil
	}

	supervisor := NewSupervisor(SupervisorConfig{
		MinBackoff: time.Millisecond,
		MaxBackoff: time.Millisecond,
		wait:       func(context.Context, time.Duration) error { return nil },
	})
	var wg sync.WaitGroup
	require.NoError(t, supervisor.Start(context.Background(), factory, consumer, 1, 5, &wg, nil, func(context.Context, *Message) {}))
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-recreated:
		supervisor.Stop()
		<-done
		t.Fatal("deleted durable was recreated")
	case <-time.After(2 * time.Second):
		supervisor.Stop()
		<-done
		t.Fatal("supervisor did not stop after consumer deletion")
	}

	assert.Equal(t, int64(1), factoryCalls.Load())
	assert.Equal(t, int64(1), maxActive.Load())
	assert.False(t, duplicate.Load())
	assert.Zero(t, active.Load())

	rm := collect(t, reader)
	terminal := metricPoints[int64](t, rm, "chat.nats.terminal.failures")
	require.Len(t, terminal, 1)
	assert.Equal(t, string(TerminalConsumerDeleted), attrs(terminal[0])["reason"])
	assert.Empty(t, metricPoints[int64](t, rm, "chat.nats.consumer.recovery.attempts"))
}

func TestSupervisor_RecoveryMissingConsumerStopsWithoutCreation(t *testing.T) {
	m, reader := newTestMetrics(t)
	consumer := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "site-a", Stream: "STREAM-site-a", Consumer: "durable"})
	var (
		initialCalls  atomic.Int64
		recoveryCalls atomic.Int64
		active        atomic.Int64
		maxActive     atomic.Int64
		duplicate     atomic.Bool
	)
	initialFactory := func(context.Context) (Iterator, error) {
		initialCalls.Add(1)
		return failingSupervisorIterator(&active, &maxActive, &duplicate, jetstream.ErrConnectionClosed), nil
	}
	recoveryFactory := func(context.Context) (Iterator, error) {
		recoveryCalls.Add(1)
		return nil, fmt.Errorf("lookup durable: %w", jetstream.ErrConsumerNotFound)
	}

	supervisor := NewSupervisor(SupervisorConfig{
		MinBackoff: time.Millisecond,
		MaxBackoff: time.Millisecond,
		wait:       func(context.Context, time.Duration) error { return nil },
	})
	var wg sync.WaitGroup
	require.NoError(t, supervisor.StartWithRecovery(context.Background(), initialFactory, recoveryFactory,
		consumer, 1, 5, &wg, nil, func(context.Context, *Message) {}))
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		supervisor.Stop()
		<-done
		t.Fatal("supervisor retried after recovery found the durable missing")
	}

	assert.Equal(t, int64(1), initialCalls.Load(), "recovery must not call the create-or-update startup factory")
	assert.Equal(t, int64(1), recoveryCalls.Load(), "a missing durable must stop recovery without retrying")
	assert.Equal(t, int64(1), maxActive.Load())
	assert.False(t, duplicate.Load())
	assert.Zero(t, active.Load())

	rm := collect(t, reader)
	terminal := metricPoints[int64](t, rm, "chat.nats.terminal.failures")
	require.Len(t, terminal, 2)
	reasons := map[string]int64{}
	for _, point := range terminal {
		reasons[attrs(point)["reason"]] = point.Value
	}
	assert.Equal(t, map[string]int64{
		string(TerminalStreamUnavailable): 1,
		string(TerminalConsumerDeleted):   1,
	}, reasons)
	assert.Empty(t, metricPoints[int64](t, rm, "chat.nats.consumer.recovery.attempts"))
}

func TestSupervisor_StartReturnsInitialFactoryError(t *testing.T) {
	m, reader := newTestMetrics(t)
	consumer := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "site-a", Stream: "STREAM-site-a", Consumer: "durable"})
	sentinel := errors.New("stream unavailable at startup")
	var factoryCalls atomic.Int64
	called := make(chan struct{})
	var calledOnce sync.Once
	supervisor := NewSupervisor(SupervisorConfig{})
	var wg sync.WaitGroup

	err := supervisor.Start(context.Background(), func(context.Context) (Iterator, error) {
		factoryCalls.Add(1)
		calledOnce.Do(func() { close(called) })
		return nil, sentinel
	}, consumer, 1, 5, &wg, nil, func(context.Context, *Message) {})
	if err == nil {
		<-called
		supervisor.Stop()
		wg.Wait()
	}

	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, int64(1), factoryCalls.Load())
	created := make(chan struct{})
	var active, maxActive atomic.Int64
	var duplicate atomic.Bool
	require.NoError(t, supervisor.Start(context.Background(), func(context.Context) (Iterator, error) {
		return blockingSupervisorIterator(created, &active, &maxActive, &duplicate), nil
	}, consumer, 1, 5, &wg, nil, func(context.Context, *Message) {}), "a failed startup must release single-owner state for a later retry")
	<-created
	supervisor.Stop()
	wg.Wait()
	assert.False(t, duplicate.Load())
	rm := collect(t, reader)
	assert.Empty(t, metricPoints[int64](t, rm, "chat.nats.consumer.recovery.attempts"), "startup failure is not a recovery attempt")
}

func TestSupervisor_RecreationFailuresUseCappedBackoff(t *testing.T) {
	m, reader := newTestMetrics(t)
	consumer := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "site-a", Stream: "STREAM-site-a", Consumer: "durable"})
	var (
		factoryCalls atomic.Int64
		active       atomic.Int64
		maxActive    atomic.Int64
		duplicate    atomic.Bool
		waitMu       sync.Mutex
		waits        []time.Duration
	)
	recovered := make(chan struct{})
	factory := func(context.Context) (Iterator, error) {
		call := factoryCalls.Add(1)
		switch {
		case call == 1:
			return failingSupervisorIterator(&active, &maxActive, &duplicate, errors.New("terminal iterator failure")), nil
		case call < 5:
			return nil, errors.New("consumer recreation failed")
		default:
			return blockingSupervisorIterator(recovered, &active, &maxActive, &duplicate), nil
		}
	}
	supervisor := NewSupervisor(SupervisorConfig{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 400 * time.Millisecond,
		wait: func(_ context.Context, d time.Duration) error {
			waitMu.Lock()
			waits = append(waits, d)
			waitMu.Unlock()
			return nil
		},
	})
	var wg sync.WaitGroup
	require.NoError(t, supervisor.Start(context.Background(), factory, consumer, 1, 5, &wg, nil, func(context.Context, *Message) {}))

	select {
	case <-recovered:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not recover after recreation failures")
	}
	supervisor.Stop()
	wg.Wait()

	waitMu.Lock()
	assert.Equal(t, []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 400 * time.Millisecond}, waits)
	waitMu.Unlock()
	assert.Equal(t, int64(1), maxActive.Load())
	assert.False(t, duplicate.Load())

	rm := collect(t, reader)
	points := metricPoints[int64](t, rm, "chat.nats.consumer.recovery.attempts")
	require.Len(t, points, 2)
	results := map[string]int64{}
	for _, point := range points {
		results[attrs(point)["result"]] = point.Value
	}
	assert.Equal(t, int64(3), results[string(RecoveryFailure)])
	assert.Equal(t, int64(1), results[string(RecoverySuccess)])
}

func TestSupervisor_ImmediateIteratorFailuresEscalateBackoff(t *testing.T) {
	m, _ := newTestMetrics(t)
	consumer := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "site-a", Stream: "STREAM-site-a", Consumer: "durable"})
	var (
		factoryCalls atomic.Int64
		active       atomic.Int64
		maxActive    atomic.Int64
		duplicate    atomic.Bool
		waitMu       sync.Mutex
		waits        []time.Duration
	)
	recovered := make(chan struct{})
	factory := func(context.Context) (Iterator, error) {
		if factoryCalls.Add(1) < 4 {
			return failingSupervisorIterator(&active, &maxActive, &duplicate, errors.New("iterator unavailable")), nil
		}
		return blockingSupervisorIterator(recovered, &active, &maxActive, &duplicate), nil
	}
	supervisor := NewSupervisor(SupervisorConfig{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 400 * time.Millisecond,
		wait: func(_ context.Context, d time.Duration) error {
			waitMu.Lock()
			waits = append(waits, d)
			waitMu.Unlock()
			return nil
		},
	})
	var wg sync.WaitGroup
	require.NoError(t, supervisor.Start(context.Background(), factory, consumer, 1, 5, &wg, nil, func(context.Context, *Message) {}))

	select {
	case <-recovered:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not recover after immediate iterator failures")
	}
	supervisor.Stop()
	wg.Wait()

	waitMu.Lock()
	assert.Equal(t, []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}, waits)
	waitMu.Unlock()
	assert.Equal(t, int64(1), maxActive.Load())
	assert.False(t, duplicate.Load())
}

func TestSupervisor_SuccessfulDeliveryResetsBackoff(t *testing.T) {
	m, _ := newTestMetrics(t)
	consumer := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "site-a", Stream: "STREAM-site-a", Consumer: "durable"})
	var (
		factoryCalls atomic.Int64
		active       atomic.Int64
		maxActive    atomic.Int64
		duplicate    atomic.Bool
		waitMu       sync.Mutex
		waits        []time.Duration
	)
	recovered := make(chan struct{})
	factory := func(context.Context) (Iterator, error) {
		switch factoryCalls.Add(1) {
		case 1:
			return failingSupervisorIterator(&active, &maxActive, &duplicate, errors.New("initial iterator failure")), nil
		case 2:
			if active.Add(1) != 1 {
				duplicate.Store(true)
			}
			var delivered atomic.Bool
			var stopOnce sync.Once
			return &supervisorTestIterator{
				next: func() (context.Context, jetstream.Msg, error) {
					if delivered.CompareAndSwap(false, true) {
						return context.Background(), &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}, nil
					}
					return nil, nil, errors.New("post-delivery iterator failure")
				},
				stop: func() { stopOnce.Do(func() { active.Add(-1) }) },
			}, nil
		default:
			return blockingSupervisorIterator(recovered, &active, &maxActive, &duplicate), nil
		}
	}
	supervisor := NewSupervisor(SupervisorConfig{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 400 * time.Millisecond,
		wait: func(_ context.Context, d time.Duration) error {
			waitMu.Lock()
			waits = append(waits, d)
			waitMu.Unlock()
			return nil
		},
	})
	var wg sync.WaitGroup
	require.NoError(t, supervisor.Start(context.Background(), factory, consumer, 1, 5, &wg, nil, func(_ context.Context, msg *Message) {
		assert.NoError(t, msg.Ack())
	}))

	select {
	case <-recovered:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not recover after the post-delivery failure")
	}
	supervisor.Stop()
	wg.Wait()

	waitMu.Lock()
	assert.Equal(t, []time.Duration{100 * time.Millisecond, 100 * time.Millisecond}, waits)
	waitMu.Unlock()
	assert.False(t, duplicate.Load())
}

func TestSupervisor_CancellationStopsRetries(t *testing.T) {
	m, _ := newTestMetrics(t)
	consumer := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "site-a", Stream: "STREAM-site-a", Consumer: "durable"})
	var factoryCalls atomic.Int64
	backoffStarted := make(chan struct{})
	supervisor := NewSupervisor(SupervisorConfig{
		MinBackoff: time.Second,
		MaxBackoff: time.Second,
		wait: func(ctx context.Context, _ time.Duration) error {
			close(backoffStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	})
	var wg sync.WaitGroup
	require.NoError(t, supervisor.Start(context.Background(), func(context.Context) (Iterator, error) {
		if factoryCalls.Add(1) == 1 {
			var active, maxActive atomic.Int64
			var duplicate atomic.Bool
			return failingSupervisorIterator(&active, &maxActive, &duplicate, errors.New("iterator unavailable")), nil
		}
		return nil, errors.New("consumer unavailable")
	}, consumer, 1, 5, &wg, nil, func(context.Context, *Message) {}))

	select {
	case <-backoffStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not enter backoff")
	}
	supervisor.Stop()
	wg.Wait()

	assert.Equal(t, int64(1), factoryCalls.Load(), "cancellation must prevent another recreation attempt")
}

func TestSupervisor_RejectsConcurrentStart(t *testing.T) {
	m, _ := newTestMetrics(t)
	consumer := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "site-a", Stream: "STREAM-site-a", Consumer: "durable"})
	created := make(chan struct{})
	var active, maxActive atomic.Int64
	var duplicate atomic.Bool
	supervisor := NewSupervisor(SupervisorConfig{})
	var wg sync.WaitGroup
	factory := func(context.Context) (Iterator, error) {
		return blockingSupervisorIterator(created, &active, &maxActive, &duplicate), nil
	}
	require.NoError(t, supervisor.Start(context.Background(), factory, consumer, 1, 5, &wg, nil, func(context.Context, *Message) {}))
	<-created

	err := supervisor.Start(context.Background(), factory, consumer, 1, 5, &wg, nil, func(context.Context, *Message) {})
	require.ErrorIs(t, err, ErrSupervisorRunning)
	supervisor.Stop()
	wg.Wait()

	assert.Equal(t, int64(1), maxActive.Load())
	assert.False(t, duplicate.Load())
}

func TestSupervisor_StopWaitsForInFlightHandler(t *testing.T) {
	m, reader := newTestMetrics(t)
	consumer := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "site-a", Stream: "STREAM-site-a", Consumer: "durable"})
	msg := &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}
	stopped := make(chan struct{})
	var stopOnce sync.Once
	var delivered atomic.Bool
	iter := &supervisorTestIterator{
		next: func() (context.Context, jetstream.Msg, error) {
			if delivered.CompareAndSwap(false, true) {
				return context.Background(), msg, nil
			}
			<-stopped
			return nil, nil, jetstream.ErrConsumerDeleted
		},
		stop: func() { stopOnce.Do(func() { close(stopped) }) },
	}
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handlerErr := make(chan error, 1)
	supervisor := NewSupervisor(SupervisorConfig{})
	var wg sync.WaitGroup
	require.NoError(t, supervisor.Start(context.Background(), func(context.Context) (Iterator, error) {
		return iter, nil
	}, consumer, 1, 5, &wg, func(jetstream.Msg) EventType { return EventCreated }, func(_ context.Context, tracked *Message) {
		close(handlerStarted)
		<-releaseHandler
		handlerErr <- tracked.Ack()
	}))

	<-handlerStarted
	supervisor.Stop()
	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()
	assert.Never(t, func() bool {
		select {
		case <-waited:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond, "shutdown returned before the in-flight handler completed")

	close(releaseHandler)
	select {
	case <-waited:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not finish after the handler completed")
	}
	require.NoError(t, <-handlerErr)

	rm := collect(t, reader)
	terminal := metricPoints[int64](t, rm, "chat.nats.terminal.failures")
	assert.Empty(t, terminal, "iterator stop caused by shutdown is not a terminal consumer failure")
	messages := metricPoints[int64](t, rm, "chat.nats.consumer.messages")
	require.Len(t, messages, 1)
	assert.Equal(t, string(OutcomeAck), attrs(messages[0])["outcome"])
}

// TestSupervisor_RecoverySuccessSurvivesShutdownRace pins the recreation
// counter to the recreation attempt itself. Recording success only once the
// next generation starts loses it whenever shutdown cancels the context between
// the factory returning and activate running — a window a caller can hit simply
// by stopping as soon as it observes the new iterator.
func TestSupervisor_RecoverySuccessSurvivesShutdownRace(t *testing.T) {
	m, reader := newTestMetrics(t)
	consumer := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "site-a", Stream: "STREAM-site-a", Consumer: "durable"})
	var (
		factoryCalls atomic.Int64
		active       atomic.Int64
		maxActive    atomic.Int64
		duplicate    atomic.Bool
		supervisor   *Supervisor
	)
	factory := func(context.Context) (Iterator, error) {
		if factoryCalls.Add(1) == 1 {
			return failingSupervisorIterator(&active, &maxActive, &duplicate, errors.New("terminal iterator failure")), nil
		}
		// Cancel before returning: the recreation has succeeded, but the
		// supervisor will never activate this iterator.
		supervisor.Stop()
		return failingSupervisorIterator(&active, &maxActive, &duplicate, errors.New("unused")), nil
	}
	supervisor = NewSupervisor(SupervisorConfig{
		MinBackoff: time.Millisecond,
		MaxBackoff: time.Millisecond,
		wait:       func(context.Context, time.Duration) error { return nil },
	})

	var wg sync.WaitGroup
	require.NoError(t, supervisor.Start(context.Background(), factory, consumer, 1, 5, &wg, nil, func(context.Context, *Message) {}))
	wg.Wait()

	rm := collect(t, reader)
	points := metricPoints[int64](t, rm, "chat.nats.consumer.recovery.attempts")
	results := map[string]int64{}
	for _, point := range points {
		results[attrs(point)["result"]] = point.Value
	}
	assert.Equal(t, int64(1), results[string(RecoverySuccess)],
		"a recreation that returned an iterator must be counted even if shutdown wins the activate race")
}
