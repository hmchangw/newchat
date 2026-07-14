//go:build integration

package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// startEmbeddedJetStreamNATS starts an in-process NATS server with JetStream
// enabled (file store in a temp dir) and returns a connected client, so the
// OUTBOX→INBOX relay can be exercised through a real durable consumer + publish.
func startEmbeddedJetStreamNATS(t *testing.T) *nats.Conn {
	t.Helper()
	opts := &natsserver.Options{Port: -1, JetStream: true, StoreDir: t.TempDir()}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

// createStream creates a stream the same way main.go's bootstrap does — schema
// only (Name + Subjects).
func createStream(t *testing.T, js jetstream.JetStream, cfg stream.Config) {
	t.Helper()
	_, err := js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     cfg.Name,
		Subjects: cfg.Subjects,
	})
	require.NoError(t, err)
}

// jsPublish is the same publish shape main.go wires into the handler: a
// PubAck-blocking JetStream publish carrying msgID as the Nats-Msg-Id.
func jsPublish(js jetstream.JetStream) PublishFunc {
	return func(ctx context.Context, subj string, data []byte, msgID string) error {
		msg := natsutil.NewMsg(ctx, subj, data)
		_, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID))
		return err
	}
}

// TestIntegration_OutboxRoundTrip exercises the relay end-to-end through real
// NATS JetStream: an OutboxEvent published onto the OUTBOX stream is
// consumed by the outbox-worker durable, HandleEvent forwards each target's
// pre-marshaled InboxEvent to the destination site's INBOX, and the forwarded
// bytes must equal the target's Envelope verbatim.
func TestIntegration_OutboxRoundTrip(t *testing.T) {
	ctx := context.Background()

	const (
		siteID     = "site-fed-origin"
		destSiteID = "site-fed-dest"
		roomID     = "room-fed-roundtrip"
	)

	nc := startEmbeddedJetStreamNATS(t)
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	outboxCfg := stream.Outbox(siteID)
	createStream(t, js, outboxCfg)
	// Destination INBOX so the cross-site forward lands on a real stream and the
	// JetStream publish gets a PubAck.
	createStream(t, js, stream.Inbox(destSiteID))

	// Observe the forwarded INBOX publish with a core NATS subscription on the
	// destination subject, set up before publishing the relay event.
	destSubject := subject.InboxExternal(destSiteID, model.InboxSubscriptionMuteToggled)
	type received struct {
		subject string
		data    []byte
	}
	var mu sync.Mutex
	var forwarded []received
	sub, err := nc.Subscribe(destSubject, func(msg *nats.Msg) {
		mu.Lock()
		forwarded = append(forwarded, received{subject: msg.Subject, data: append([]byte(nil), msg.Data...)})
		mu.Unlock()
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	require.NoError(t, nc.Flush())

	h := NewHandler(jsPublish(js))

	// Build the pre-marshaled InboxEvent envelope room-service would produce.
	innerPayload := []byte(`{"account":"alice","roomId":"room-fed-roundtrip"}`)
	envelope, err := json.Marshal(model.InboxEvent{
		Type:       model.InboxSubscriptionMuteToggled,
		SiteID:     siteID,
		DestSiteID: destSiteID,
		Payload:    innerPayload,
		Timestamp:  time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)

	const dedupID = "0193abcd-0193-7abc-89ab-fed000000001:site-fed-dest"
	relayEvt, err := json.Marshal(model.OutboxEvent{
		RoomID:    roomID,
		Envelope:  envelope,
		DedupID:   dedupID,
		Timestamp: time.Now().UTC().UnixMilli(),
	})
	require.NoError(t, err)

	// Create the per-destination concurrent consumer (same config as main.go) and
	// run a consume loop that drives HandleEvent, exactly like production.
	cons, err := js.CreateOrUpdateConsumer(ctx, outboxCfg.Name, buildConcurrentConsumerConfig(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 5, MaxWaiting: 512, MaxAckPending: 1000,
	}, siteID, destSiteID))
	require.NoError(t, err)

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		if err := h.HandleEvent(ctx, msg.Subject(), msg.Data()); err != nil {
			t.Errorf("HandleEvent: %v", err)
		}
		_ = msg.Ack()
	})
	require.NoError(t, err)
	t.Cleanup(cc.Stop)

	// Publish the relay event onto the OUTBOX stream's destination-scoped subject.
	_, err = js.Publish(ctx, subject.Outbox(siteID, destSiteID, model.InboxSubscriptionMuteToggled), relayEvt)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(forwarded) >= 1
	}, 5*time.Second, 20*time.Millisecond, "expected one forwarded INBOX publish on the destination subject")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, forwarded, 1, "exactly one forward per target")
	assert.Equal(t, destSubject, forwarded[0].subject)
	assert.Equal(t, envelope, forwarded[0].data,
		"forwarded bytes must equal the target's pre-marshaled Envelope verbatim")
}

// TestIntegration_ConcurrentLanePerDestinationIsolation proves finding #1's fix:
// a destination that is down (its INBOX stream missing, so forwards fail and
// retry forever under MaxDeliver=-1) must not stall subscription-state
// forwarding to a HEALTHY peer. With per-destination concurrent consumers, the
// down peer's parked events fill only its own ack-pending budget. A small
// MaxAckPending makes the down lane's saturation observable: 3 events to the
// down peer exceed its 2-slot budget, yet the healthy peer's event still
// forwards — which a single shared consumer could not guarantee.
func TestIntegration_ConcurrentLanePerDestinationIsolation(t *testing.T) {
	ctx := context.Background()

	const (
		siteID   = "site-iso-origin"
		downDest = "site-iso-down"
		upDest   = "site-iso-up"
		roomID   = "room-iso-1"
	)

	nc := startEmbeddedJetStreamNATS(t)
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	createStream(t, js, stream.Outbox(siteID))
	// Only the healthy peer's INBOX exists; the down peer's is absent, so forwards
	// to it fail (no stream → no PubAck) and retry forever.
	createStream(t, js, stream.Inbox(upDest))

	// Observe forwards landing on the healthy peer's INBOX by reading its stream
	// back (a durable land, not a raw subject delivery — a JetStream publish to
	// the down peer has no stream to land on, so it never counts here).
	var mu sync.Mutex
	upForwards := 0
	sub, err := nc.Subscribe(subject.InboxExternal(upDest, model.InboxSubscriptionMuteToggled), func(*nats.Msg) {
		mu.Lock()
		upForwards++
		mu.Unlock()
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	require.NoError(t, nc.Flush())

	h := NewHandler(jsPublish(js))

	mk := func(dest, dedupID string) []byte {
		env, err := json.Marshal(model.InboxEvent{
			Type: model.InboxSubscriptionMuteToggled, SiteID: siteID, DestSiteID: dest,
			Payload: []byte(`{}`), Timestamp: time.Now().UTC().UnixMilli(),
		})
		require.NoError(t, err)
		evt, err := json.Marshal(model.OutboxEvent{RoomID: roomID, Envelope: env, DedupID: dedupID, Timestamp: 1})
		require.NoError(t, err)
		return evt
	}

	// Saturate the down peer's lane past its 2-slot budget, then enqueue one
	// event for the healthy peer.
	for i, id := range []string{"down-1", "down-2", "down-3"} {
		_, err = js.Publish(ctx, subject.Outbox(siteID, downDest, model.InboxSubscriptionMuteToggled), mk(downDest, id))
		require.NoError(t, err, "publish down event %d", i)
	}
	_, err = js.Publish(ctx, subject.Outbox(siteID, upDest, model.InboxSubscriptionMuteToggled), mk(upDest, "up-1"))
	require.NoError(t, err)

	// One concurrent consumer per peer, small MaxAckPending so the down lane's
	// saturation is reachable with a handful of events.
	settings := stream.ConsumerSettings{AckWait: 2 * time.Second, MaxDeliver: 5, MaxWaiting: 512, MaxAckPending: 2}
	for _, dest := range []string{downDest, upDest} {
		cons, err := js.CreateOrUpdateConsumer(ctx, stream.Outbox(siteID).Name, buildConcurrentConsumerConfig(settings, siteID, dest))
		require.NoError(t, err)
		cc, err := cons.Consume(func(msg jetstream.Msg) {
			jsretry.Settle(ctx, msg, []time.Duration{50 * time.Millisecond}, h.HandleEvent(ctx, msg.Subject(), msg.Data()))
		})
		require.NoError(t, err)
		t.Cleanup(cc.Stop)
	}

	// The healthy peer's event forwards despite the down peer's forever-retrying
	// backlog — a single shared consumer could not guarantee this.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return upForwards >= 1
	}, 5*time.Second, 20*time.Millisecond, "healthy peer's event must forward while the down peer's lane is saturated")
}

// TestIntegration_OrderedLaneFIFOThroughOutage exercises the per-destination
// ordered lane's two guarantees across a simulated destination outage:
// no event is lost (MaxDeliver=-1 keeps the head retrying until the destination
// accepts it) and enqueue order is preserved (MaxAckPending=1 holds
// member_removed behind the still-failing member_added, so a recovered
// destination never sees them inverted). The outage is simulated by creating
// the destination INBOX stream only after both events are enqueued: until then
// the JetStream forward has no stream to land on and fails.
func TestIntegration_OrderedLaneFIFOThroughOutage(t *testing.T) {
	ctx := context.Background()

	const (
		siteID     = "site-fifo-origin"
		destSiteID = "site-fifo-dest"
		roomID     = "room-fifo-1"
	)

	nc := startEmbeddedJetStreamNATS(t)
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	outboxCfg := stream.Outbox(siteID)
	createStream(t, js, outboxCfg)

	h := NewHandler(jsPublish(js))

	mkEvt := func(eventType, dedupID string) []byte {
		envelope, err := json.Marshal(model.InboxEvent{
			Type: eventType, SiteID: siteID, DestSiteID: destSiteID,
			Payload: []byte(`{"account":"bob"}`), Timestamp: time.Now().UTC().UnixMilli(),
		})
		require.NoError(t, err)
		evt, err := json.Marshal(model.OutboxEvent{
			RoomID: roomID, Envelope: envelope, DedupID: dedupID,
			Timestamp: time.Now().UTC().UnixMilli(),
		})
		require.NoError(t, err)
		return evt
	}

	// Enqueue member_added then member_removed while the destination is "down"
	// (its INBOX stream does not exist yet).
	_, err = js.Publish(ctx, subject.Outbox(siteID, destSiteID, model.InboxMemberAdded),
		mkEvt(model.InboxMemberAdded, "fifo-added-1"))
	require.NoError(t, err)
	_, err = js.Publish(ctx, subject.Outbox(siteID, destSiteID, model.InboxMemberRemoved),
		mkEvt(model.InboxMemberRemoved, "fifo-removed-1"))
	require.NoError(t, err)

	// Drive the lane exactly like main.go: per-destination FIFO consumer,
	// jsretry disposition (short backoff so the test converges quickly).
	mcons, err := js.CreateOrUpdateConsumer(ctx, outboxCfg.Name, buildOrderedConsumerConfig(stream.ConsumerSettings{
		AckWait: 5 * time.Second, MaxDeliver: 5, MaxWaiting: 512, MaxAckPending: 1000,
	}, siteID, destSiteID))
	require.NoError(t, err)
	cc, err := mcons.Consume(func(msg jetstream.Msg) {
		jsretry.Settle(ctx, msg, []time.Duration{50 * time.Millisecond}, h.HandleEvent(ctx, msg.Subject(), msg.Data()))
	})
	require.NoError(t, err)
	t.Cleanup(cc.Stop)

	// While the destination is down nothing advances: the head member_added
	// keeps failing (nothing captures the forward, PubAck times out) and holds
	// member_removed behind it — the lane's ack floor stays at zero.
	time.Sleep(300 * time.Millisecond)
	info, err := mcons.Info(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 0, info.AckFloor.Consumer,
		"head must not be acked while the destination stream is missing")
	assert.EqualValues(t, 1, info.NumAckPending,
		"FIFO lane must hold exactly the head message in flight")

	// Destination recovers: its INBOX stream appears, the retried head lands,
	// then the lane drains in order.
	inboxCfg := stream.Inbox(destSiteID)
	createStream(t, js, inboxCfg)

	// "Landed" means captured by the destination INBOX stream — read it back.
	inboxStream, err := js.Stream(ctx, inboxCfg.Name)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		si, err := inboxStream.Info(ctx)
		return err == nil && si.State.Msgs == 2
	}, 10*time.Second, 20*time.Millisecond, "both ordered events must land after recovery")

	reader, err := js.OrderedConsumer(ctx, inboxCfg.Name, jetstream.OrderedConsumerConfig{})
	require.NoError(t, err)
	var arrivals []string
	for range 2 {
		msg, err := reader.Next(jetstream.FetchMaxWait(5 * time.Second))
		require.NoError(t, err)
		var env model.InboxEvent
		require.NoError(t, json.Unmarshal(msg.Data(), &env))
		arrivals = append(arrivals, env.Type)
	}
	assert.Equal(t, []string{model.InboxMemberAdded, model.InboxMemberRemoved}, arrivals,
		"events must land in enqueue order: member_removed must not overtake the retried member_added")
}

// TestIntegration_OrderedLaneKeepsRenameBehindMemberAdded is finding #2's
// regression guard: room_renamed shares the per-destination ordered lane with
// member_added, so a rename enqueued after an add cannot be delivered ahead of
// it (which would strand a new cross-site member on the old room name). If
// room_renamed were ever moved to the concurrent lane, this inverts.
func TestIntegration_OrderedLaneKeepsRenameBehindMemberAdded(t *testing.T) {
	ctx := context.Background()

	const (
		siteID     = "site-ord-origin"
		destSiteID = "site-ord-dest"
		roomID     = "room-ord-1"
	)

	nc := startEmbeddedJetStreamNATS(t)
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	createStream(t, js, stream.Outbox(siteID))
	inboxCfg := stream.Inbox(destSiteID)
	createStream(t, js, inboxCfg)

	h := NewHandler(jsPublish(js))

	mk := func(eventType, dedupID string) []byte {
		env, err := json.Marshal(model.InboxEvent{
			Type: eventType, SiteID: siteID, DestSiteID: destSiteID,
			Payload: []byte(`{}`), Timestamp: time.Now().UTC().UnixMilli(),
		})
		require.NoError(t, err)
		evt, err := json.Marshal(model.OutboxEvent{RoomID: roomID, Envelope: env, DedupID: dedupID, Timestamp: 1})
		require.NoError(t, err)
		return evt
	}

	// member_added enqueued first, then room_renamed — both on the ordered lane.
	_, err = js.Publish(ctx, subject.Outbox(siteID, destSiteID, model.InboxMemberAdded), mk(model.InboxMemberAdded, "ord-add-1"))
	require.NoError(t, err)
	_, err = js.Publish(ctx, subject.Outbox(siteID, destSiteID, model.InboxRoomRenamed), mk(model.InboxRoomRenamed, "ord-rename-1"))
	require.NoError(t, err)

	cons, err := js.CreateOrUpdateConsumer(ctx, stream.Outbox(siteID).Name, buildOrderedConsumerConfig(stream.ConsumerSettings{
		AckWait: 5 * time.Second, MaxDeliver: 5, MaxWaiting: 512, MaxAckPending: 1000,
	}, siteID, destSiteID))
	require.NoError(t, err)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, h.HandleEvent(ctx, msg.Subject(), msg.Data()))
	})
	require.NoError(t, err)
	t.Cleanup(cc.Stop)

	inboxStream, err := js.Stream(ctx, inboxCfg.Name)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		si, err := inboxStream.Info(ctx)
		return err == nil && si.State.Msgs == 2
	}, 10*time.Second, 20*time.Millisecond, "both events must land")

	reader, err := js.OrderedConsumer(ctx, inboxCfg.Name, jetstream.OrderedConsumerConfig{})
	require.NoError(t, err)
	var arrivals []string
	for range 2 {
		msg, err := reader.Next(jetstream.FetchMaxWait(5 * time.Second))
		require.NoError(t, err)
		var env model.InboxEvent
		require.NoError(t, json.Unmarshal(msg.Data(), &env))
		arrivals = append(arrivals, env.Type)
	}
	assert.Equal(t, []string{model.InboxMemberAdded, model.InboxRoomRenamed}, arrivals,
		"room_renamed must not overtake the member_added it renames")
}
