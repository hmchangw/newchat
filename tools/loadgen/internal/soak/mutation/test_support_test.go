package mutation

import (
	"context"
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
)

type soakRPCFakeReply struct {
	data []byte
	err  error
}

type soakReadCall struct {
	subject string
	data    []byte
}

type soakReadTransport struct {
	mu      sync.Mutex
	replies []soakRPCFakeReply
	calls   []soakReadCall
}

func (t *soakReadTransport) Request(
	_ context.Context,
	subject string,
	data []byte,
	_ time.Duration,
) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = append(t.calls, soakReadCall{
		subject: subject,
		data:    append([]byte(nil), data...),
	})
	if len(t.replies) == 0 {
		return nil, assert.AnError
	}
	reply := t.replies[0]
	t.replies = t.replies[1:]
	return reply.data, reply.err
}

func (t *soakReadTransport) snapshot() []soakReadCall {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]soakReadCall(nil), t.calls...)
}

type soakRecordingSleeper struct {
	delays []time.Duration
}

func (s *soakRecordingSleeper) Sleep(
	ctx context.Context,
	delay time.Duration,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.delays = append(s.delays, delay)
	return nil
}

type fakeSoakClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeSoakClock(now time.Time) *fakeSoakClock {
	return &fakeSoakClock{now: now}
}

func (c *fakeSoakClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeSoakClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}
