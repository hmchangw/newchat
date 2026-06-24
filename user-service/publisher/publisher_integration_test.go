//go:build integration

package publisher

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/user-service/service"
)

// Compile-time assertion: `go vet -tags integration` fails if Publisher drifts from EventPublisher.
var _ service.EventPublisher = (*Publisher)(nil)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// dial returns a connected *o11ynats.Conn backed by the shared test NATS
// server. The connection is drained on test cleanup.
func dial(t *testing.T) (*o11ynats.Conn, o11ynats.JetStream) {
	t.Helper()
	nc, err := o11ynats.Connect(context.Background(), testutil.NATS(t), noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })
	js, err := nc.JetStream()
	require.NoError(t, err)
	return nc, js
}

func TestPublish_Integration(t *testing.T) {
	nc, js := dial(t)
	ctx := context.Background()

	const subj = "test.publisher.subject"
	// JetStream publish requires a stream owning the subject to ack the PubAck.
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST_PUBLISHER",
		Subjects: []string{"test.publisher.>"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(ctx, "TEST_PUBLISHER") })

	want := []byte(`{"hello":"world"}`)

	// A core subscriber on the subject receives the JetStream publish directly,
	// letting us assert the payload + propagated X-Request-ID header.
	received := make(chan *nats.Msg, 1)
	sub, err := nc.Subscribe(context.Background(), subj, func(_ context.Context, m *nats.Msg) {
		received <- m
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	// Stamp a request ID so we can assert it propagates onto the published msg.
	rctx := natsutil.WithRequestID(ctx, "22222222-2222-7222-8222-222222222222")
	err = New(js).Publish(rctx, subj, want)
	require.NoError(t, err)

	select {
	case got := <-received:
		require.Equal(t, want, got.Data)
		assert.Equal(t, "22222222-2222-7222-8222-222222222222", got.Header.Get(natsutil.RequestIDHeader),
			"request id must propagate onto the inbox event")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for published message")
	}
}

// TestPublish_ClosedConn_Integration: a closed connection must surface the
// wrapped "publish inbox event" error.
func TestPublish_ClosedConn_Integration(t *testing.T) {
	nc, js := dial(t)
	nc.Close()

	err := New(js).Publish(context.Background(), "test.publisher.closed", []byte(`{}`))
	require.Error(t, err)
	require.ErrorContains(t, err, "publish inbox event")
}
