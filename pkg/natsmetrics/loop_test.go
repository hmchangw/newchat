package natsmetrics

import (
	"context"
	"errors"
	"sync"
	"testing"

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
	c := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "s1", Stream: "stream", Consumer: "consumer"})
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
	c := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "s1", Stream: "stream", Consumer: "consumer"})
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
