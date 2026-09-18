//go:build integration

package stream_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

// TestNakWithDelayHoldsAckPendingSlot is the load-bearing premise of the retry
// lane design: a message Nak'd with a delay keeps occupying the consumer's
// MaxAckPending budget for the whole backoff, so parked retries starve healthy
// traffic. With MaxAckPending=1, a single parked message must block delivery of
// the next one until its nak delay elapses.
func TestNakWithDelayHoldsAckPendingSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	const streamName = "ACKPENDING-PREMISE"
	const subj = "ackpending.premise.test"
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{subj},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), streamName) })

	for _, body := range []string{"first", "second"} {
		_, err = js.Publish(ctx, subj, []byte(body))
		require.NoError(t, err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       "premise",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    6,
		MaxAckPending: 1,
	})
	require.NoError(t, err)

	first, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
	require.NoError(t, err)
	require.Equal(t, "first", string(first.Data()))

	// Park it for longer than the probe window below.
	require.NoError(t, first.NakWithDelay(10*time.Second))

	// THE ASSERTION: with the parked message still holding the only ack-pending
	// slot, the second message must not be delivered.
	_, err = cons.Next(jetstream.FetchMaxWait(3 * time.Second))
	require.Error(t, err, "a parked nak must hold its ack-pending slot and block the next delivery")
	require.True(t, errors.Is(err, jetstream.ErrNoMessages) || errors.Is(err, nats.ErrTimeout),
		"expected no-messages/timeout, got %v", err)
}
