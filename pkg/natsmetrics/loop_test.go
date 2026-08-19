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
	redeliveries := metricPoints[int64](t, rm, "chat.nats.consumer.redeliveries")
	require.Len(t, redeliveries, 1)
	assert.Equal(t, int64(1), redeliveries[0].Value)
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
		func(_ context.Context, tracked *Message) { require.NoError(t, tracked.Ack()) })

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
