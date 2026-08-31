package publisher

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/user-service/service"
)

// Compile-time assertion: both publishers must satisfy the consumer-defined
// interface they are wired into in main.go.
var (
	_ service.EventPublisher = (*CorePublisher)(nil)
	_ service.EventPublisher = (*Publisher)(nil)
)

const testRequestID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"

// startEmbeddedNATS boots an in-process NATS server with JetStream enabled (no
// Docker) and returns a tracing-aware connection plus its JetStream context.
// Server and connection are torn down via t.Cleanup.
func startEmbeddedNATS(t *testing.T) (*o11ynats.Conn, o11ynats.JetStream) {
	t.Helper()
	opts := &natsserver.Options{Port: -1, JetStream: true, StoreDir: t.TempDir()}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := o11ynats.Connect(context.Background(), ns.ClientURL(), noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	require.NoError(t, err)
	return nc, js
}

// subscribeOne subscribes to subject and returns a channel delivering the first
// messages received. The subscription is unsubscribed via t.Cleanup.
func subscribeOne(t *testing.T, nc *o11ynats.Conn, subject string) <-chan *nats.Msg {
	t.Helper()
	got := make(chan *nats.Msg, 4)
	sub, err := nc.Subscribe(context.Background(), subject, func(_ context.Context, m *nats.Msg) {
		got <- m
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return got
}

// awaitMsg waits for one message or fails the test.
func awaitMsg(t *testing.T, ch <-chan *nats.Msg) *nats.Msg {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for published message")
		return nil
	}
}

func TestNewCore_UsesGivenConn(t *testing.T) {
	nc, _ := startEmbeddedNATS(t)

	p := NewCore(nc)

	require.NotNil(t, p)
	assert.Same(t, nc, p.nc, "NewCore must retain the connection it was given")
}

func TestNew_UsesGivenJetStream(t *testing.T) {
	_, js := startEmbeddedNATS(t)

	p := New(js)

	require.NotNil(t, p)
	assert.Same(t, js, p.js, "New must retain the JetStream context it was given")
}

func TestCorePublisher_Publish(t *testing.T) {
	tests := []struct {
		name          string
		subject       string
		data          []byte
		requestID     string
		wantRequestID string
	}{
		{
			name:          "propagates request id from context",
			subject:       "chat.user.alice.settings.update",
			data:          []byte(`{"theme":"dark"}`),
			requestID:     testRequestID,
			wantRequestID: testRequestID,
		},
		{
			name:          "no request id in context leaves header unset",
			subject:       "chat.user.bob.settings.update",
			data:          []byte(`{"theme":"light"}`),
			requestID:     "",
			wantRequestID: "",
		},
		{
			name:          "empty payload is delivered as-is",
			subject:       "chat.user.carol.settings.update",
			data:          []byte{},
			requestID:     testRequestID,
			wantRequestID: testRequestID,
		},
		{
			name:          "binary payload survives round trip",
			subject:       "chat.user.dave.settings.update",
			data:          []byte{0x00, 0xff, 0x10, 0x7f},
			requestID:     testRequestID,
			wantRequestID: testRequestID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc, _ := startEmbeddedNATS(t)
			received := subscribeOne(t, nc, tt.subject)

			ctx := context.Background()
			if tt.requestID != "" {
				ctx = natsutil.WithRequestID(ctx, tt.requestID)
			}

			require.NoError(t, NewCore(nc).Publish(ctx, tt.subject, tt.data))

			got := awaitMsg(t, received)
			assert.Equal(t, tt.subject, got.Subject)
			assert.Equal(t, tt.data, got.Data)
			assert.Equal(t, tt.wantRequestID, got.Header.Get(natsutil.RequestIDHeader))
		})
	}
}

// A core publish is fire-and-forget: it must not be persisted by, or require,
// any stream — that is the whole point of CorePublisher over Publisher.
func TestCorePublisher_Publish_NoStreamRequired(t *testing.T) {
	nc, js := startEmbeddedNATS(t)
	received := subscribeOne(t, nc, "chat.user.erin.settings.update")

	require.NoError(t, NewCore(nc).Publish(context.Background(), "chat.user.erin.settings.update", []byte(`{}`)))
	awaitMsg(t, received)

	_, err := js.Stream(context.Background(), "ANY")
	assert.Error(t, err, "no stream exists; the core publish succeeded anyway")
}

func TestCorePublisher_Publish_ClosedConn_WrapsError(t *testing.T) {
	nc, _ := startEmbeddedNATS(t)
	nc.Close()

	err := NewCore(nc).Publish(context.Background(), "chat.user.alice.settings.update", []byte(`{}`))

	require.Error(t, err)
	assert.ErrorContains(t, err, "publish client event", "error must be wrapped with what the caller was doing")
	assert.ErrorIs(t, err, nats.ErrConnectionClosed, "the underlying cause must stay unwrapped-able")
}

func TestPublisher_Publish(t *testing.T) {
	const streamSubjects = "chat.inbox.site-b.external.>"

	tests := []struct {
		name          string
		subject       string
		data          []byte
		requestID     string
		wantRequestID string
	}{
		{
			name:          "propagates request id onto the inbox event",
			subject:       "chat.inbox.site-b.external.subscription_created",
			data:          []byte(`{"roomId":"r1"}`),
			requestID:     testRequestID,
			wantRequestID: testRequestID,
		},
		{
			name:          "no request id in context leaves header unset",
			subject:       "chat.inbox.site-b.external.subscription_removed",
			data:          []byte(`{"roomId":"r2"}`),
			requestID:     "",
			wantRequestID: "",
		},
		{
			name:          "empty payload is delivered as-is",
			subject:       "chat.inbox.site-b.external.user_status",
			data:          []byte{},
			requestID:     testRequestID,
			wantRequestID: testRequestID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc, js := startEmbeddedNATS(t)
			ctx := context.Background()

			_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
				Name:     "TEST_INBOX",
				Subjects: []string{streamSubjects},
			})
			require.NoError(t, err)

			received := subscribeOne(t, nc, tt.subject)

			pubCtx := ctx
			if tt.requestID != "" {
				pubCtx = natsutil.WithRequestID(pubCtx, tt.requestID)
			}

			require.NoError(t, New(js).Publish(pubCtx, tt.subject, tt.data))

			got := awaitMsg(t, received)
			assert.Equal(t, tt.subject, got.Subject)
			assert.Equal(t, tt.data, got.Data)
			assert.Equal(t, tt.wantRequestID, got.Header.Get(natsutil.RequestIDHeader))

			// Publish blocks on the PubAck, so by now the event is stored.
			st, err := js.Stream(ctx, "TEST_INBOX")
			require.NoError(t, err)
			info, err := st.Info(ctx)
			require.NoError(t, err)
			assert.Equal(t, uint64(1), info.State.Msgs, "the event must be persisted in the destination stream")
		})
	}
}

// No stream owns the subject, so no PubAck ever arrives — the publish must fail
// (rather than silently dropping a federation event) with a wrapped error.
func TestPublisher_Publish_NoStreamForSubject_WrapsError(t *testing.T) {
	_, js := startEmbeddedNATS(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	err := New(js).Publish(ctx, "chat.inbox.nowhere.external.user_status", []byte(`{}`))

	require.Error(t, err)
	assert.ErrorContains(t, err, "publish inbox event")
}

func TestPublisher_Publish_ClosedConn_WrapsError(t *testing.T) {
	nc, js := startEmbeddedNATS(t)
	nc.Close()

	err := New(js).Publish(context.Background(), "chat.inbox.site-b.external.user_status", []byte(`{}`))

	require.Error(t, err)
	assert.ErrorContains(t, err, "publish inbox event", "error must be wrapped with what the caller was doing")
}
