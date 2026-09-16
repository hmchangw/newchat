//go:build integration

package retrylane_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/retrylane"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// TestEscalationRoundTrip proves the pieces compose: a message that exhausts
// its fast budget on a hot consumer lands on the RETRY stream, addressed to
// exactly one consumer, with a byte-identical body.
func TestEscalationRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	const siteID = "rlit"
	const consumerName = "test-worker"

	hot := stream.Config{Name: "HOT-" + siteID, Subjects: []string{"hot." + siteID + ".>"}}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: hot.Name, Subjects: hot.Subjects})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), hot.Name) })

	retryCfg := stream.Retry(siteID)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: retryCfg.Name, Subjects: retryCfg.Subjects})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), retryCfg.Name) })

	body := []byte(`{"msg":"hello","n":1}`)
	_, err = js.Publish(ctx, "hot."+siteID+".created", body)
	require.NoError(t, err)

	hotCons, err := js.CreateOrUpdateConsumer(ctx, hot.Name, jetstream.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       2 * time.Second,
		MaxDeliver:    10,
		MaxAckPending: 100,
	})
	require.NoError(t, err)

	lane := &retrylane.Lane{
		Consumer:  consumerName,
		SiteID:    siteID,
		Enabled:   true,
		FastSteps: 2,
		Publish: func(ctx context.Context, subj string, data []byte, hdr nats.Header, msgID string) error {
			_, pubErr := js.PublishMsg(ctx, &nats.Msg{
				Subject: subj,
				Data:    data,
				Header:  hdr,
			}, jetstream.WithMsgID(msgID))
			return pubErr
		},
	}

	// Fail every delivery on a short backoff; deliveries 1-2 nak in place,
	// delivery 3 escalates.
	fastBackoff := []time.Duration{200 * time.Millisecond, 200 * time.Millisecond}
	handlerErr := assert.AnError

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		msg, fetchErr := hotCons.Next(jetstream.FetchMaxWait(2 * time.Second))
		if fetchErr != nil {
			continue
		}
		lane.Settle(ctx, msg, fastBackoff, handlerErr)

		retryStream, sErr := js.Stream(ctx, retryCfg.Name)
		require.NoError(t, sErr)
		info, iErr := retryStream.Info(ctx)
		require.NoError(t, iErr)
		if info.State.Msgs > 0 {
			break
		}
	}

	retryStream, err := js.Stream(ctx, retryCfg.Name)
	require.NoError(t, err)
	got, err := retryStream.GetMsg(ctx, 1)
	require.NoError(t, err, "the message must have escalated onto the RETRY stream")

	assert.Equal(t, subject.Retry(siteID, consumerName, subject.RetryTierSlow), got.Subject,
		"the escalation must be addressed to exactly one consumer")
	assert.Equal(t, body, got.Data, "the body must survive escalation byte-identically")
	assert.Equal(t, consumerName, got.Header.Get(retrylane.HeaderConsumer))
	assert.Equal(t, hot.Name, got.Header.Get(retrylane.HeaderOriginStream))
	assert.NotEmpty(t, got.Header.Get(retrylane.HeaderFirstFailedAt))
}
