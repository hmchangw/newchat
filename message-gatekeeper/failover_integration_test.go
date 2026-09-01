//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// connectFailoverJS dials url and returns its JetStream context.
func connectFailoverJS(t *testing.T, url string) o11ynats.JetStream {
	t.Helper()
	conn, err := natsutil.Connect(context.Background(), url, "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.NoError(t, err)
	t.Cleanup(func() { conn.NatsConn().Close() })

	js, err := conn.JetStream()
	require.NoError(t, err)
	return js
}

// The gatekeeper is the hinge of the failover message path: a send that arrived
// on the failover ingress must be published to the FAILOVER canonical subject.
// Publishing to the live one would target a stream on the cluster that is down,
// so the validated message would vanish silently — the failure this asserts
// against is invisible, not loud.
func TestGatekeeper_FailoverLanePublishesToFailoverCanonical(t *testing.T) {
	db := testutil.MongoDB(t, "gk_failover")
	ctx := context.Background()

	sender := model.User{ID: "u-alice", Account: "alice", EngName: "Alice Wang"}
	seedUserAndSubscription(t, ctx, db, sender, "r1")

	handler, captured := buildHandlerWithCapture(t, db)

	req := model.SendMessageRequest{
		ID:        idgen.GenerateMessageID(),
		Content:   "hello from the failover lane",
		RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789f",
	}

	// failoverLane=true is what the failover consumer's lane binding passes
	// through to HandleJetStreamMsg.
	_, err := handler.processMessage(ctx, "alice", "r1", "site-a", &req, true)
	require.NoError(t, err)

	msg := captured()
	require.NotNil(t, msg, "a validated message must be published")
	assert.Equal(t, subject.FailoverMsgCanonicalCreated("site-a"), msg.Subject)
	assert.NotEqual(t, subject.MsgCanonicalCreated("site-a"), msg.Subject,
		"the live canonical stream is on the cluster that is down")
}

// The control: a home-lane send keeps the live canonical subject, so the failover
// wiring cannot regress steady-state routing.
func TestGatekeeper_HomeLanePublishesToLiveCanonical(t *testing.T) {
	db := testutil.MongoDB(t, "gk_failover_home")
	ctx := context.Background()

	sender := model.User{ID: "u-bob", Account: "bob", EngName: "Bob Chen"}
	seedUserAndSubscription(t, ctx, db, sender, "r2")

	handler, captured := buildHandlerWithCapture(t, db)

	req := model.SendMessageRequest{
		ID:        idgen.GenerateMessageID(),
		Content:   "hello from the home lane",
		RequestID: "01970a4f-8c2d-7c9a-abcd-e0123456789a",
	}

	_, err := handler.processMessage(ctx, "bob", "r2", "site-a", &req, false)
	require.NoError(t, err)

	msg := captured()
	require.NotNil(t, msg)
	assert.Equal(t, subject.MsgCanonicalCreated("site-a"), msg.Subject)
}

// Both standby streams the gatekeeper spans must be creatable on one cluster
// without a subject-filter collision, and neither may collide with its live
// counterpart. This is the constraint the whole chat.failover.> root exists to
// satisfy, exercised against a real server rather than by string comparison.
func TestFailoverStreams_CoexistWithLiveStreams(t *testing.T) {
	_, buddyURL := testutil.NATSPair(t)
	js := connectFailoverJS(t, buddyURL)
	ctx := context.Background()

	// Live and failover streams for the same site, on one server: the create
	// succeeds only if their subject filters are disjoint.
	for _, c := range []stream.Config{
		stream.Messages("site-a"),
		stream.MessagesCanonical("site-a"),
		stream.MessagesFailover("site-a"),
		stream.MessagesCanonicalFailover("site-a"),
		stream.PushNotification("site-a"),
		stream.PushNotificationFailover("site-a"),
		stream.Outbox("site-a"),
		stream.OutboxFailover("site-a"),
	} {
		_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: c.Name, Subjects: c.Subjects,
		})
		require.NoError(t, err, "stream %s must not collide with an already-created one", c.Name)
	}

	// A failover-lane publish must be captured by the failover stream, not the
	// live one — proof the token placement actually partitions traffic.
	ack, err := js.Publish(ctx, subject.FailoverMsgCanonicalCreated("site-a"), []byte(`{}`))
	require.NoError(t, err)
	assert.Equal(t, stream.MessagesCanonicalFailover("site-a").Name, ack.Stream)

	ack, err = js.Publish(ctx, subject.MsgCanonicalCreated("site-a"), []byte(`{}`))
	require.NoError(t, err)
	assert.Equal(t, stream.MessagesCanonical("site-a").Name, ack.Stream)

	ack, err = js.Publish(ctx, subject.FailoverMsgSend("alice", "r1", "site-a"), []byte(`{}`))
	require.NoError(t, err)
	assert.Equal(t, stream.MessagesFailover("site-a").Name, ack.Stream)

	ack, err = js.Publish(ctx, subject.MsgSend("alice", "r1", "site-a"), []byte(`{}`))
	require.NoError(t, err)
	assert.Equal(t, stream.Messages("site-a").Name, ack.Stream)
}
