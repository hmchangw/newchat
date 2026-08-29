package main

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// embeddedCanonical is the in-process JetStream fixture for consumer-behaviour
// tests: no Docker, so these run under `make test` alongside the unit tests.
type embeddedCanonical struct {
	js         jetstream.JetStream
	iter       jetstream.MessagesContext
	cons       jetstream.Consumer
	subject    string
	maxDeliver int
}

func startEmbeddedCanonical(t *testing.T, siteID string, settings stream.ConsumerSettings) embeddedCanonical {
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

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	sc := stream.MessagesCanonical(siteID)
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{Name: sc.Name, Subjects: sc.Subjects})
	require.NoError(t, err)

	cons, err := js.CreateOrUpdateConsumer(context.Background(), sc.Name,
		buildConsumerConfig(settings, "default", siteID))
	require.NoError(t, err)

	iter, err := cons.Messages()
	require.NoError(t, err)
	t.Cleanup(iter.Stop)

	return embeddedCanonical{
		js: js, iter: iter, cons: cons,
		subject:    subject.MsgCanonicalCreated(siteID),
		maxDeliver: settings.MaxDeliver,
	}
}

// salvageStore records the thread-message writes the salvage path performs.
// Hand-written rather than gomock: these calls run on a consume goroutine,
// where a controller's t.Fatalf would be an illegal FailNow off the test
// goroutine.
type salvageStore struct {
	mu    sync.Mutex
	saved []*model.Message
}

func (s *salvageStore) savedMessages() []*model.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*model.Message, len(s.saved))
	copy(out, s.saved)
	return out
}

// GetMessageCreatedAt always misses: the parent never lands, which is the
// condition under test.
func (s *salvageStore) GetMessageCreatedAt(context.Context, string) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

// GetMessageSender matches the miss above — an absent parent has no sender row.
func (s *salvageStore) GetMessageSender(context.Context, string) (*cassParticipant, error) {
	return nil, errMessageNotFound
}

func (s *salvageStore) SaveThreadMessage(_ context.Context, msg *model.Message, _ *cassParticipant, _, _ string) (*int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *msg
	s.saved = append(s.saved, &cp)
	n := len(s.saved)
	return &n, nil
}

func (s *salvageStore) SaveMessage(context.Context, *model.Message, *cassParticipant, string) error {
	return nil
}
func (s *salvageStore) GetQuotedParentSnapshot(context.Context, string) (*cassandra.QuotedParentMessage, bool, error) {
	return nil, false, nil
}
func (s *salvageStore) UpdateParentMessageThreadRoomID(context.Context, string, string, time.Time, string) error {
	return nil
}

type salvageUsers struct{}

func (salvageUsers) FindUserByID(context.Context, string) (*model.User, error) { return nil, nil }
func (salvageUsers) FindUserByAccount(_ context.Context, account string) (*model.User, error) {
	return &model.User{ID: "u-1", Account: account, SiteID: "site-salvage", EngName: "Alice Wang"}, nil
}
func (salvageUsers) FindUsersByAccounts(context.Context, []string) ([]model.User, error) {
	return nil, nil
}

type salvageThreads struct{}

func (salvageThreads) CreateThreadRoom(context.Context, *model.ThreadRoom) error { return nil }
func (salvageThreads) GetThreadRoomByParentMessageID(context.Context, string) (*model.ThreadRoom, error) {
	return &model.ThreadRoom{ID: "tr-1"}, nil
}
func (salvageThreads) InsertThreadSubscription(context.Context, *model.ThreadSubscription) error {
	return nil
}
func (salvageThreads) UpsertThreadSubscription(context.Context, *model.ThreadSubscription) error {
	return nil
}
func (salvageThreads) MarkThreadSubscriptionMention(context.Context, *model.ThreadSubscription) error {
	return nil
}
func (salvageThreads) UpdateThreadRoomLastMessage(context.Context, string, string, []string, time.Time) error {
	return nil
}
func (salvageThreads) AddReplyAccounts(context.Context, string, []string) error { return nil }
func (salvageThreads) GetHistorySharedSince(context.Context, string, []string) (map[string]*time.Time, error) {
	return map[string]*time.Time{}, nil
}
func (salvageThreads) AdvanceThreadSubscriptionLastSeen(context.Context, string, string, time.Time) error {
	return nil
}
func (salvageThreads) AddThreadUnread(context.Context, string, string, []string) error { return nil }

// TestConsume_UnresolvableThreadParent_IsSalvagedNotAbandoned drives the
// production consume composition against a real broker.
//
// message-worker is the sole persister of message history and nothing
// dead-letters a MaxDeliver drop, so a reply whose parent never lands used to
// be destroyed: NAK on every delivery, then abandoned by the broker. It now
// retries while the parent's own write may still arrive and, on the FINAL
// delivery, persists the reply without parent linkage.
//
// This exercises what the handler unit test cannot: that the real consume loop
// actually stamps the delivery metadata IsFinalDeliveryFromContext reads. If
// main.go stopped calling tracked.Context, the unit test would still pass and
// this test would fail.
func TestConsume_UnresolvableThreadParent_IsSalvagedNotAbandoned(t *testing.T) {
	// MaxDeliver stays at the production default so the assertion below proves the
	// parent-resolution CAP (2 attempts), not a coincidence with the consumer's
	// own limit. One retry on DefaultBackoff's 1s rung, then salvage.
	const maxDeliver = 6

	e := startEmbeddedCanonical(t, "site-salvage", stream.ConsumerSettings{
		AckWait:       30 * time.Second, // beyond the test window: every redelivery must come from a NAK
		MaxDeliver:    maxDeliver,
		MaxWaiting:    512,
		MaxAckPending: 1000,
	})

	store := &salvageStore{}
	h := NewHandler(store, salvageUsers{}, salvageThreads{}, "site-salvage",
		func(context.Context, string, []byte, string) error { return nil })

	reader := sdkmetric.NewManualReader()
	consumerMetrics := natsmetrics.NewFromProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))).
		Consumer(natsmetrics.ConsumerConfig{
			Site: "site-salvage", Stream: "MESSAGES-CANONICAL-site-salvage", Consumer: "message-worker",
		})
	consumerMetrics.LoopStarted(context.Background())

	// The production per-message body for the live feed. The Track/Context/Finish
	// preamble mirrors main.go's consume goroutine — that is the wiring under test.
	process := liveProcessor(h)
	var deliveries atomic.Int32
	// lastAttempt/attemptVisible record what the handler could actually see, so a
	// failure reports the observed facts rather than asserting a cause. The salvage
	// hinges entirely on DeliveryAttemptFromContext returning (>=2, true); if the
	// loop's Track/Context wiring or the broker's metadata is not what this test
	// assumes, "attempt visible" goes false and names the real problem.
	var lastAttempt atomic.Uint64
	var attemptVisible atomic.Bool
	// A dead iterator must not look like a quiet consumer: without this the
	// goroutine returns silently and the test reports "never persisted" with no
	// hint that nothing was ever delivered.
	var iterErr atomic.Value
	go func() {
		for {
			msg, err := e.iter.Next()
			if err != nil {
				iterErr.Store(err.Error())
				return
			}
			deliveries.Add(1)
			msgCtx := context.Background()
			tracked := consumerMetrics.Track(msgCtx, msg, natsmetrics.EventTypeFromSubject(msg.Subject()), e.maxDeliver)
			msgCtx = tracked.Context(msgCtx)
			if attempt, ok := natsmetrics.DeliveryAttemptFromContext(msgCtx); ok {
				lastAttempt.Store(attempt)
				attemptVisible.Store(true)
			}
			process(msgCtx, tracked)
			tracked.Finish(msgCtx)
		}
	}()

	now := time.Now().UTC()
	evt := model.MessageEvent{
		Event: model.EventCreated, SiteID: "site-salvage", Timestamp: now.UnixMilli(),
		Message: model.Message{
			ID: "orphan-reply", RoomID: "r1", UserID: "u-1", UserAccount: "alice",
			Content: "a reply whose parent never landed", CreatedAt: now,
			ThreadParentMessageID: "never-persisted-parent",
		},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	_, err = e.js.Publish(context.Background(), e.subject, data)
	require.NoError(t, err)

	// The reply must end up in history rather than being abandoned at MaxDeliver.
	if !assert.Eventually(t, func() bool { return len(store.savedMessages()) == 1 }, 20*time.Second, 50*time.Millisecond,
		"the reply was never persisted — it is being retried to MaxDeliver and abandoned, with no dead-letter behind it") {
		// Report what was observed instead of leaving the caller to guess. Either
		// the handler never saw an exhausted budget (attempt never reached
		// parentResolveAttempts, or was never visible at all — a wiring problem),
		// or it saw it and still did not persist (a handler problem).
		// deliveries==0 means the message never reached the handler at all, which is
		// a delivery/filter problem, not a salvage problem — so report the broker's
		// own view and the subjects involved before blaming the handler.
		iterMsg := "<none>"
		if v, ok := iterErr.Load().(string); ok {
			iterMsg = v
		}
		pending, ackPending, delivered := uint64(0), uint64(0), uint64(0)
		filters := []string(nil)
		if info, ierr := e.cons.Info(context.Background()); ierr == nil {
			pending, ackPending, delivered = info.NumPending, uint64(info.NumAckPending), info.Delivered.Consumer
			filters = info.Config.FilterSubjects
			if info.Config.FilterSubject != "" {
				filters = append(filters, info.Config.FilterSubject)
			}
		}
		t.Fatalf("salvage never happened: deliveries=%d lastAttempt=%d attemptVisible=%t (need attempt >= parentResolveAttempts=%d)\n"+
			"  iterator error: %s\n"+
			"  consumer: num_pending=%d num_ack_pending=%d delivered=%d filters=%q\n"+
			"  published to: %q\n"+
			"  deliveries=0 with num_pending>0 means the message is in the stream but the consumer never got it "+
			"(filter mismatch or a dead iterator), NOT a salvage failure",
			deliveries.Load(), lastAttempt.Load(), attemptVisible.Load(), parentResolveAttempts,
			iterMsg, pending, ackPending, delivered, filters, e.subject)
	}

	saved := store.savedMessages()[0]
	assert.Equal(t, "orphan-reply", saved.ID)
	assert.Equal(t, "never-persisted-parent", saved.ThreadParentMessageID, "thread linkage on the reply is preserved")
	assert.Nil(t, saved.ThreadParentMessageCreatedAt, "no parent coords exist to stamp")

	// Salvage happens on the second delivery, not the first (the race window) and
	// not the sixth: spending the whole MaxDeliver budget would hold this message's
	// ack-pending slot for DefaultBackoff's 756s.
	assert.Equal(t, int32(parentResolveAttempts), deliveries.Load(),
		"the reply must be salvaged once the short parent-resolution budget is spent, not at MaxDeliver")

	// Settled, so the slot is released and the broker has nothing left to abandon.
	require.Eventually(t, func() bool {
		info, ierr := e.cons.Info(context.Background())
		return ierr == nil && info.NumAckPending == 0
	}, 10*time.Second, 50*time.Millisecond, "the salvaged reply must be acked, not left pending")

	assert.Never(t, func() bool { return len(store.savedMessages()) > 1 }, 2*time.Second, 200*time.Millisecond,
		"the salvaged reply must not be persisted twice")
}
