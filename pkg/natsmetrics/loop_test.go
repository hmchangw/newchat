package natsmetrics

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingIterator struct{ err error }

func (i failingIterator) Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	return nil, nil, i.err
}

type oneMessageIterator struct {
	msg  jetstream.Msg
	done bool
}

func (i *oneMessageIterator) Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	if i.done {
		return nil, nil, jetstream.ErrConsumerDeleted
	}
	i.done = true
	return context.Background(), i.msg, nil
}

func TestConsume_TerminalNextFailureStopsLoop(t *testing.T) {
	m, reader := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "stream", Consumer: "consumer"})
	c.LoopStarted(context.Background())
	var wg sync.WaitGroup

	Consume(context.Background(), failingIterator{err: errors.New("iterator failed")}, c, 1, 5, &wg, nil, func(context.Context, *Message) {})

	rm := collect(t, reader)
	points := metricPoints[int64](t, rm, "chat.nats.consumer.loop.up")
	require.Len(t, points, 1)
	assert.Equal(t, int64(0), points[0].Value)
}

func TestConsume_TracksAndDisposesMessage(t *testing.T) {
	m, reader := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "stream", Consumer: "consumer"})
	c.LoopStarted(context.Background())
	msg := &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 2}}
	iter := &oneMessageIterator{msg: msg}
	var wg sync.WaitGroup

	Consume(context.Background(), iter, c, 1, 5, &wg, func(jetstream.Msg) EventType { return EventCreated }, func(ctx context.Context, tracked *Message) {
		assert.False(t, IsFinalDeliveryFromContext(ctx))
		require.NoError(t, tracked.Ack())
	})
	wg.Wait()

	rm := collect(t, reader)
	points := metricPoints[int64](t, rm, "chat.nats.consumer.messages")
	require.Len(t, points, 1)
	assert.Equal(t, int64(1), points[0].Value)
	assert.Equal(t, string(OutcomeAck), attrs(points[0])["outcome"])
	assert.Equal(t, 1, msg.acks)
}

// blockingIterator holds the loop inside Next until release is closed, then
// ends it. It lets a test observe whether the loop goroutine is registered with
// the WaitGroup while it is still running.
type blockingIterator struct {
	release chan struct{}
	done    bool
}

func (i *blockingIterator) Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	<-i.release
	if i.done {
		return nil, nil, jetstream.ErrConsumerDeleted
	}
	i.done = true
	return context.Background(), &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}, nil
}

// A zero MAX_WORKERS must not wedge the consumer. An unbuffered semaphore would
// park the dispatch send forever, leaving the loop-up gauge at 1 with no error
// and no terminal metric — a silent stall is worse than a slow consumer.
func TestConsume_NonPositiveMaxWorkersStillConsumes(t *testing.T) {
	for _, maxWorkers := range []int{0, -1} {
		t.Run(fmt.Sprintf("maxWorkers=%d", maxWorkers), func(t *testing.T) {
			m, _ := newTestMetrics(t)
			c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "stream", Consumer: "consumer"})
			c.LoopStarted(context.Background())
			iter := &oneMessageIterator{msg: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}}
			var wg sync.WaitGroup
			processed := make(chan struct{}, 1)

			done := make(chan struct{})
			go func() {
				Consume(context.Background(), iter, c, maxWorkers, 5, &wg,
					func(jetstream.Msg) EventType { return EventCreated },
					func(_ context.Context, tracked *Message) {
						processed <- struct{}{}
						require.NoError(t, tracked.Ack())
					})
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("Consume wedged with non-positive maxWorkers")
			}
			wg.Wait()
			assert.Len(t, processed, 1)
		})
	}
}

// Start must register the loop with the WaitGroup before it returns. If the
// loop is only counted once a message is dispatched, a shutdown that runs
// iter.Stop() then wg.Wait() can pass straight through while a message returned
// by Next has not been handed to a worker yet, and drain the connection under it.
func TestStart_RegistersLoopBeforeReturning(t *testing.T) {
	m, _ := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "stream", Consumer: "consumer"})
	c.LoopStarted(context.Background())
	iter := &blockingIterator{release: make(chan struct{})}
	var wg sync.WaitGroup

	Start(context.Background(), iter, c, 4, 5, &wg,
		func(jetstream.Msg) EventType { return EventCreated },
		func(_ context.Context, tracked *Message) { require.NoError(t, tracked.Ack()) },
		nil)

	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()

	assert.Never(t, func() bool {
		select {
		case <-waited:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond, "wg.Wait() returned while the consumer loop was still running")

	close(iter.release)
	assert.Eventually(t, func() bool {
		select {
		case <-waited:
			return true
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "wg.Wait() did not return after the loop exited")
}

// A terminal Next failure ends the loop for good; nothing else observes that,
// so Start must hand the cause to the caller's stopped hook, which is what
// turns a silently dead consumer into a readiness failure and a restart.
func TestStart_ReportsTerminalErrorToStoppedHook(t *testing.T) {
	m, _ := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "stream", Consumer: "consumer"})
	c.LoopStarted(context.Background())
	iterErr := errors.New("consumer deleted")
	got := make(chan error, 1)
	var wg sync.WaitGroup

	Start(context.Background(), failingIterator{err: iterErr}, c, 4, 5, &wg,
		func(jetstream.Msg) EventType { return EventCreated },
		func(context.Context, *Message) { t.Fatal("no message should be processed") },
		func(err error) { got <- err })
	wg.Wait()

	select {
	case err := <-got:
		assert.ErrorIs(t, err, iterErr)
	default:
		t.Fatal("Start must report the terminal iterator error to the stopped hook")
	}
}

// Consume returns the terminal error so a hand-rolled loop can report it too.
func TestConsume_ReturnsTerminalError(t *testing.T) {
	m, _ := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "stream", Consumer: "consumer"})
	iterErr := errors.New("consumer deleted")
	var wg sync.WaitGroup

	err := Consume(context.Background(), failingIterator{err: iterErr}, c, 4, 5, &wg, nil, nil)

	assert.ErrorIs(t, err, iterErr)
}

// heartbeatThenMessageIterator returns a missing-heartbeat error, then one
// message, then a terminal error. nats.go returns ErrNoHeartbeat from Next
// WITHOUT invalidating the iterator (jetstream/pull.go: the ReportMissingHeartbeats
// branch returns the error but does not call s.Stop()), so a loop that treats it
// as the end would abandon a live consumer.
type heartbeatThenMessageIterator struct {
	msg   jetstream.Msg
	calls int
}

func (i *heartbeatThenMessageIterator) Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	i.calls++
	switch i.calls {
	case 1:
		return nil, nil, jetstream.ErrNoHeartbeat
	case 2:
		return context.Background(), i.msg, nil
	default:
		return nil, nil, jetstream.ErrConsumerDeleted
	}
}

func TestRecoverable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"missing heartbeat", jetstream.ErrNoHeartbeat, true},
		{"wrapped missing heartbeat", fmt.Errorf("next: %w", jetstream.ErrNoHeartbeat), true},
		{"consumer deleted", jetstream.ErrConsumerDeleted, false},
		{"iterator closed", jetstream.ErrMsgIteratorClosed, false},
		{"unknown error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Recoverable(tt.err))
		})
	}
}

// TestConsume_RetriesOnMissingHeartbeat pins the rule that keeps a heartbeat
// gap from restarting the pod: the loop retries Next instead of returning, so
// the guard's restart hook never fires for it, and it still ends on the real
// terminal error that follows.
func TestConsume_RetriesOnMissingHeartbeat(t *testing.T) {
	m, _ := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "stream", Consumer: "consumer"})
	c.LoopStarted(context.Background())
	msg := &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}
	iter := &heartbeatThenMessageIterator{msg: msg}
	var wg sync.WaitGroup

	var processed int
	err := Consume(context.Background(), iter, c, 1, 5, &wg,
		func(jetstream.Msg) EventType { return EventCreated },
		func(_ context.Context, tracked *Message) {
			processed++
			require.NoError(t, tracked.Ack())
		})
	wg.Wait()

	assert.ErrorIs(t, err, jetstream.ErrConsumerDeleted, "the loop must end on the terminal error, not the heartbeat gap")
	assert.Equal(t, 1, processed, "the message after the heartbeat gap must still be processed")
	assert.Equal(t, 3, iter.calls, "the loop must call Next again after a heartbeat gap")
}

// TestStart_DoesNotReportMissingHeartbeatToStopped is the escalation guard: a
// heartbeat gap must never reach the stopped hook, because that hook raises
// SIGTERM on the process. A fleet-wide heartbeat stall would otherwise restart
// every pull-iterator worker at once.
func TestStart_DoesNotReportMissingHeartbeatToStopped(t *testing.T) {
	m, _ := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "stream", Consumer: "consumer"})
	c.LoopStarted(context.Background())
	iter := &heartbeatThenMessageIterator{msg: &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}}
	var wg sync.WaitGroup

	stoppedWith := make(chan error, 4)
	Start(context.Background(), iter, c, 1, 5, &wg, nil,
		func(_ context.Context, tracked *Message) { _ = tracked.Ack() },
		func(err error) { stoppedWith <- err })

	select {
	case err := <-stoppedWith:
		assert.NotErrorIs(t, err, jetstream.ErrNoHeartbeat, "a heartbeat gap must not be reported as the loop's death")
		assert.ErrorIs(t, err, jetstream.ErrConsumerDeleted)
	case <-time.After(2 * time.Second):
		t.Fatal("stopped hook was never called")
	}
	wg.Wait()
}

// TestLoopFailed_IgnoresRecoverableError guards the invariant from the call
// site's ordering. A heartbeat gap is not a failure, and recording it as one
// costs twice: the loop-up gauge reads 0 while the loop is still consuming,
// and because LoopFailed returns early once `up` is already false, the REAL
// terminal failure that follows then records no reason at all.
func TestLoopFailed_IgnoresRecoverableError(t *testing.T) {
	m, reader := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "stream", Consumer: "consumer"})
	c.LoopStarted(context.Background())

	c.LoopFailed(context.Background(), jetstream.ErrNoHeartbeat)

	rm := collect(t, reader)
	up := metricPoints[int64](t, rm, "chat.nats.consumer.loop.up")
	require.Len(t, up, 1)
	assert.Equal(t, int64(1), up[0].Value, "a heartbeat gap must leave the loop-up gauge alone")
	assert.Empty(t, metricPoints[int64](t, rm, "chat.nats.terminal.failures"),
		"a heartbeat gap must not record a terminal failure")

	// The death that actually ends the loop must still be recorded in full.
	c.LoopFailed(context.Background(), jetstream.ErrConsumerDeleted)

	rm = collect(t, reader)
	up = metricPoints[int64](t, rm, "chat.nats.consumer.loop.up")
	require.Len(t, up, 1)
	assert.Equal(t, int64(0), up[0].Value)
	terminal := metricPoints[int64](t, rm, "chat.nats.terminal.failures")
	require.Len(t, terminal, 1, "the real terminal failure must still record its reason")
	assert.Equal(t, string(TerminalConsumerDeleted), attrs(terminal[0])["reason"])
}
