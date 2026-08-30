package jsiter

import (
	"context"
	"errors"
	"testing"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeConsumer implements only the two methods the source constructors call;
// the embedded interface panics on anything else, which is the point.
type fakeConsumer struct {
	o11ynats.Consumer
	iter        o11ynats.MessagesContext
	cc          o11ynats.ConsumeContext
	err         error
	pullOpts    [][]jetstream.PullMessagesOpt
	consumeOpts [][]jetstream.PullConsumeOpt
}

func (c *fakeConsumer) Messages(_ context.Context, opts ...jetstream.PullMessagesOpt) (o11ynats.MessagesContext, error) {
	c.pullOpts = append(c.pullOpts, opts)
	if c.err != nil {
		return nil, c.err
	}
	return c.iter, nil
}

func (c *fakeConsumer) Consume(_ context.Context, _ o11ynats.JetStreamMsgHandler, opts ...jetstream.PullConsumeOpt) (o11ynats.ConsumeContext, error) {
	c.consumeOpts = append(c.consumeOpts, opts)
	if c.err != nil {
		return nil, c.err
	}
	return c.cc, nil
}

// fakeJS records what each resolve asked for and hands back one consumer.
type fakeJS struct {
	cons        *fakeConsumer
	err         error
	calls       int
	gotStreams  []string
	gotDurables []string
}

//nolint:gocritic // hugeParam: cfg by value matches the JetStream interface.
func (j *fakeJS) CreateOrUpdateConsumer(_ context.Context, stream string, cfg jetstream.ConsumerConfig) (o11ynats.Consumer, error) {
	j.calls++
	j.gotStreams = append(j.gotStreams, stream)
	j.gotDurables = append(j.gotDurables, cfg.Durable)
	if j.err != nil {
		return nil, j.err
	}
	return j.cons, nil
}

// noopConsumeContext stands in for a running subscription; Stop is real so
// Supervisor.Stop has something safe to call.
type noopConsumeContext struct{ o11ynats.ConsumeContext }

func (noopConsumeContext) Stop() {}

// Closed reports work already finished, so releasing this handle never waits.
func (noopConsumeContext) Closed() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

// drainableIter is a scriptedIter with the Drain method o11ynats.MessagesContext
// adds on top of jsiter.Iterator.
type drainableIter struct{ *scriptedIter }

func (drainableIter) Drain() {}

func TestResolve_CreatesTheDurableOnEveryCall(t *testing.T) {
	js := &fakeJS{cons: &fakeConsumer{}}
	resolve := Resolve(js, "INBOX-site-a", jetstream.ConsumerConfig{Durable: "inbox-worker"})

	for range 3 {
		cons, err := resolve(context.Background())
		require.NoError(t, err)
		assert.Same(t, js.cons, cons)
	}

	// A stopped iterator usually means the durable is gone, so a resolver that
	// cached the handle would rebuild onto a consumer the server already dropped.
	assert.Equal(t, 3, js.calls, "every rebuild must re-resolve the consumer")
	assert.Equal(t, []string{"INBOX-site-a", "INBOX-site-a", "INBOX-site-a"}, js.gotStreams)
	assert.Equal(t, []string{"inbox-worker", "inbox-worker", "inbox-worker"}, js.gotDurables)
}

func TestResolve_WrapsCreateErrorWithStreamAndDurable(t *testing.T) {
	js := &fakeJS{err: errors.New("stream not found")}
	resolve := Resolve(js, "INBOX-site-a", jetstream.ConsumerConfig{Durable: "inbox-worker"})

	cons, err := resolve(context.Background())

	require.Error(t, err)
	assert.Nil(t, cons)
	assert.ErrorIs(t, err, js.err)
	assert.Contains(t, err.Error(), "INBOX-site-a")
	assert.Contains(t, err.Error(), "inbox-worker")
}

func TestPullFrom_OpensAnIteratorWithTheGivenOptions(t *testing.T) {
	iter := drainableIter{newScriptedIter()}
	js := &fakeJS{cons: &fakeConsumer{iter: iter}}
	open := PullFrom(Resolve(js, "MESSAGES", jetstream.ConsumerConfig{}), jetstream.PullMaxMessages(7))

	got, err := open(context.Background())

	require.NoError(t, err)
	assert.Equal(t, iter, got)
	require.Len(t, js.cons.pullOpts, 1)
	assert.Len(t, js.cons.pullOpts[0], 1, "the caller's options must reach Messages")
}

func TestPullFrom_PropagatesFailures(t *testing.T) {
	resolveErr := errors.New("create failed")
	messagesErr := errors.New("iterator failed")

	for _, tc := range []struct {
		name string
		js   *fakeJS
		want error
	}{
		{"resolve fails", &fakeJS{err: resolveErr}, resolveErr},
		{"Messages fails", &fakeJS{cons: &fakeConsumer{err: messagesErr}}, messagesErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			open := PullFrom(Resolve(tc.js, "MESSAGES", jetstream.ConsumerConfig{}))

			got, err := open(context.Background())

			require.Error(t, err)
			assert.Nil(t, got)
			assert.ErrorIs(t, err, tc.want)
		})
	}
}

// errHandlers pulls the ConsumeErrHandler options out of one recorded Consume
// call. NewSupervisor's whole contract rests on exactly one being present: with
// none, a Consume the server stops reports nothing at all.
func errHandlers(opts []jetstream.PullConsumeOpt) []jetstream.ConsumeErrHandler {
	var found []jetstream.ConsumeErrHandler
	for _, o := range opts {
		if h, ok := o.(jetstream.ConsumeErrHandler); ok {
			found = append(found, h)
		}
	}
	return found
}

func TestConsumeFrom_WiresOnErrorIntoTheErrorHandler(t *testing.T) {
	js := &fakeJS{cons: &fakeConsumer{cc: noopConsumeContext{}}}
	open := ConsumeFrom(Resolve(js, "OUTBOX", jetstream.ConsumerConfig{}), func(context.Context, jetstream.Msg) {})

	var got error
	cc, err := open(context.Background(), func(e error) { got = e })
	require.NoError(t, err)
	assert.NotNil(t, cc)

	require.Len(t, js.cons.consumeOpts, 1)
	handlers := errHandlers(js.cons.consumeOpts[0])
	require.Len(t, handlers, 1, "exactly one error handler must be installed")

	handlers[0](nil, jetstream.ErrConsumerDeleted)
	assert.ErrorIs(t, got, jetstream.ErrConsumerDeleted, "the handler must forward to this round's onError")
}

// Each rebuild calls open again on the same captured opts. Appending the
// handler to that slice would stack one more on every rebuild — after N
// rebuilds a single status error would fire onError N times, and Supervisor
// would count one failure as many.
func TestConsumeFrom_DoesNotStackHandlersAcrossRebuilds(t *testing.T) {
	js := &fakeJS{cons: &fakeConsumer{cc: noopConsumeContext{}}}

	// Spare capacity is what makes append reuse the caller's array.
	callerOpts := make([]jetstream.PullConsumeOpt, 0, 4)
	callerOpts = append(callerOpts, jetstream.PullMaxMessages(5))
	spare := callerOpts[:cap(callerOpts)]

	open := ConsumeFrom(Resolve(js, "OUTBOX", jetstream.ConsumerConfig{}), func(context.Context, jetstream.Msg) {}, callerOpts...)

	var fired []int
	for round := range 3 {
		_, err := open(context.Background(), func(error) { fired = append(fired, round) })
		require.NoError(t, err)
	}

	require.Len(t, js.cons.consumeOpts, 3)
	for round, opts := range js.cons.consumeOpts {
		assert.Len(t, opts, 2, "round %d must get the caller's option plus exactly one handler", round)
		require.Len(t, errHandlers(opts), 1, "round %d stacked an extra error handler", round)
	}
	assert.Nil(t, spare[1], "the caller's spare capacity must not be written to")

	// Each round's handler must reach that round's onError, not an earlier one.
	for round, opts := range js.cons.consumeOpts {
		errHandlers(opts)[0](nil, jetstream.ErrConsumerDeleted)
		assert.Equal(t, []int{round}, fired, "round %d's handler fired the wrong callback", round)
		fired = nil
	}
}

func TestConsumeFrom_PropagatesFailures(t *testing.T) {
	resolveErr := errors.New("create failed")
	consumeErr := errors.New("consume failed")

	for _, tc := range []struct {
		name string
		js   *fakeJS
		want error
	}{
		{"resolve fails", &fakeJS{err: resolveErr}, resolveErr},
		{"Consume fails", &fakeJS{cons: &fakeConsumer{err: consumeErr}}, consumeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			open := ConsumeFrom(Resolve(tc.js, "OUTBOX", jetstream.ConsumerConfig{}), func(context.Context, jetstream.Msg) {})

			cc, err := open(context.Background(), func(error) {})

			require.Error(t, err)
			assert.Nil(t, cc)
			assert.ErrorIs(t, err, tc.want)
		})
	}
}

// The constructors exist to be handed straight to NewPump and NewSupervisor; a
// signature drift would only show up at the 16 call sites otherwise.
func TestSourceConstructors_SatisfyTheBuilderTypes(t *testing.T) {
	js := &fakeJS{cons: &fakeConsumer{iter: drainableIter{newScriptedIter()}, cc: noopConsumeContext{}}}
	resolve := Resolve(js, "MESSAGES", jetstream.ConsumerConfig{})

	pump, err := NewPump(context.Background(), "pull", PullFrom(resolve))
	require.NoError(t, err)
	t.Cleanup(pump.Stop)
	assert.True(t, pump.IsUp())

	sup, err := NewSupervisor(context.Background(), "consume", ConsumeFrom(resolve, func(context.Context, jetstream.Msg) {}))
	require.NoError(t, err)
	t.Cleanup(sup.Stop)
	assert.True(t, sup.IsUp())
}
