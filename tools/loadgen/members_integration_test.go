//go:build integration

package main

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestMembersSustained_EndToEnd verifies the full members-sustained pipeline
// with simulated room-service and room-worker stages. Asserts the test exits
// cleanly (runMembersSustained returns 0) and the simulated stages saw
// non-zero traffic.
func TestMembersSustained_EndToEnd(t *testing.T) {
	ctx := context.Background()
	natsURL := testutil.NATS(t)
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Drain() //nolint:errcheck

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	siteID := "site-test"

	// Create the ROOMS stream so the simulated room-worker can consume from it.
	rooms := stream.Rooms(siteID)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     rooms.Name,
		Subjects: rooms.Subjects,
	})
	require.NoError(t, err)

	// Create the MESSAGES_CANONICAL stream as well — runMembersSustained tries
	// to sample the "room-worker" consumer on the ROOMS stream, but also uses
	// natsutil.Connect which may set up JetStream consumers. We create ROOMS
	// above; the MESSAGES_CANONICAL is not required for the members workload but
	// creating ROOMS is sufficient.

	// Simulated room-service: subscribes on the member-add wildcard, sends
	// a reply, and publishes the canonical event to the ROOMS stream.
	var rsCalls atomic.Int64
	rsSub, err := nc.Subscribe(subject.MemberAddWildcard(siteID), func(m *nats.Msg) {
		rsCalls.Add(1)
		var req model.AddMembersRequest
		if jsonErr := json.Unmarshal(m.Data, &req); jsonErr != nil {
			_ = m.Respond([]byte(`{"error":"bad request"}`))
			return
		}
		_ = m.Respond([]byte(`{"status":"accepted"}`))
		data, _ := json.Marshal(req)
		_, _ = js.Publish(ctx, subject.RoomCanonical(siteID, "member.add"), data)
	})
	require.NoError(t, err)
	defer rsSub.Unsubscribe() //nolint:errcheck

	// Simulated room-worker: consumes from the ROOMS stream, emits a
	// member_added broadcast event, and acks the message.
	var rwCalls atomic.Int64
	cons, err := js.CreateOrUpdateConsumer(ctx, rooms.Name, jetstream.ConsumerConfig{
		Durable:       "room-worker",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject.RoomCanonical(siteID, "member.add"),
	})
	require.NoError(t, err)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		rwCalls.Add(1)
		var req model.AddMembersRequest
		if jsonErr := json.Unmarshal(msg.Data(), &req); jsonErr != nil {
			_ = msg.Ack()
			return
		}
		evt := model.MemberAddEvent{
			Type:      "member_added",
			RoomID:    req.RoomID,
			Accounts:  req.Users,
			SiteID:    siteID,
			Timestamp: time.Now().UnixMilli(),
		}
		data, _ := json.Marshal(evt)
		_ = nc.Publish(subject.RoomMemberEvent(req.RoomID), data)
		_ = msg.Ack()
	})
	require.NoError(t, err)
	defer cc.Stop()

	// Drive runMembersSustained against the test NATS. A short duration and
	// low rate keeps the test comfortably under 60 s.
	cfg := &config{
		NatsURL:     natsURL,
		SiteID:      siteID,
		MetricsAddr: ":19099",
		MaxInFlight: 50,
	}
	args := []string{
		"--preset=members-small",
		"--duration=5s",
		"--rate=5",
		"--warmup=1s",
		"--users-per-add=2",
	}
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	code := runMembersSustained(runCtx, cfg, args)
	assert.Equal(t, 0, code, "members-sustained should exit 0")

	// Allow trailing events to settle before checking counters.
	time.Sleep(500 * time.Millisecond)

	// Both simulated stages must have seen traffic.
	assert.Greater(t, rsCalls.Load(), int64(0), "room-service was never called")
	assert.Greater(t, rwCalls.Load(), int64(0), "room-worker never consumed")
}
