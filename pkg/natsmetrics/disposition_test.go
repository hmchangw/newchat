package natsmetrics

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A delivery that ends without Ack/Nak/Term while the loop is still up relies
// on AckWait redelivery, which is `left_pending`. The same delivery during
// shutdown is `handler_cancelled` — the campaign has to tell those apart.
func TestFinishDistinguishesCancellationFromAckWait(t *testing.T) {
	tests := []struct {
		name   string
		loopUp bool
		ctx    func() context.Context
		want   Outcome
	}{
		{name: "loop up and live context", loopUp: true, ctx: context.Background, want: OutcomeLeftPending},
		{name: "loop already stopped", loopUp: false, ctx: context.Background, want: OutcomeHandlerCancelled},
		{
			name:   "cancelled handler context",
			loopUp: true,
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			want: OutcomeHandlerCancelled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reader := newTestMetrics(t)
			c := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "s1", Stream: "STREAM_s1", Consumer: "durable"})
			if tt.loopUp {
				c.LoopStarted(context.Background())
			}
			tracked := c.Track(context.Background(), &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}, EventCreated, 5)

			tracked.Finish(tt.ctx())

			rm := collect(t, reader)
			points := metricPoints[int64](t, rm, "chat.nats.consumer.messages")
			require.Len(t, points, 1)
			assert.Equal(t, string(tt.want), attrs(points[0])["outcome"])
		})
	}
}

// An explicit Term is a deliberate application drop, not an unexpected fault.
// Recording it as `internal` would trip the terminal-delivery alert as if the
// service had malfunctioned.
func TestTermRecordsPermanentNotInternal(t *testing.T) {
	for _, name := range []string{"Term", "TermWithReason"} {
		t.Run(name, func(t *testing.T) {
			m, reader := newTestMetrics(t)
			c := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "s1", Stream: "STREAM_s1", Consumer: "durable"})
			tracked := c.Track(context.Background(), &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}, EventCreated, 5)

			if name == "Term" {
				require.NoError(t, tracked.Term())
			} else {
				require.NoError(t, tracked.TermWithReason("caller supplied free text"))
			}

			rm := collect(t, reader)
			points := metricPoints[int64](t, rm, "chat.nats.terminal.failures")
			require.Len(t, points, 1)
			assert.Equal(t, string(TerminalPermanent), attrs(points[0])["reason"])
		})
	}
}

// A handler that already classified the drop keeps its own reason: the first
// MarkTerminal wins, so Term must not overwrite `invalid_payload`.
func TestTermDoesNotOverwriteHandlerClassification(t *testing.T) {
	m, reader := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "s1", Stream: "STREAM_s1", Consumer: "durable"})
	tracked := c.Track(context.Background(), &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}}, EventCreated, 5)

	tracked.MarkTerminal(context.Background(), TerminalInvalidPayload)
	require.NoError(t, tracked.Term())

	rm := collect(t, reader)
	points := metricPoints[int64](t, rm, "chat.nats.terminal.failures")
	require.Len(t, points, 1)
	assert.Equal(t, string(TerminalInvalidPayload), attrs(points[0])["reason"])
}

// Classification must not run on the single dispatch goroutine: a slow
// classifier would otherwise serialize the whole consumer.
func TestConsumeClassifiesOffTheDispatchPath(t *testing.T) {
	release := make(chan struct{})
	var dispatched sync.WaitGroup
	dispatched.Add(2)

	iter := &scriptedIterator{msgs: []jetstream.Msg{
		&fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}},
		&fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}},
	}}
	m, _ := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "s1", Stream: "STREAM_s1", Consumer: "durable"})
	c.LoopStarted(context.Background())

	var wg sync.WaitGroup
	go Consume(context.Background(), iter, c, 4, 5, &wg,
		func(jetstream.Msg) EventType {
			dispatched.Done()
			<-release // block every classifier until both messages are dispatched
			return EventCreated
		},
		func(context.Context, *Message) {})

	done := make(chan struct{})
	go func() { dispatched.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second message was not dispatched while the first classifier was still running")
	}
	close(release)
	wg.Wait()
}

// scriptedIterator yields msgs in order and then blocks so Consume keeps running.
type scriptedIterator struct {
	mu   sync.Mutex
	msgs []jetstream.Msg
}

func (s *scriptedIterator) Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.msgs) == 0 {
		return nil, nil, jetstream.ErrConsumerDeleted
	}
	msg := s.msgs[0]
	s.msgs = s.msgs[1:]
	return context.Background(), msg, nil
}

func (*scriptedIterator) Stop() {}
