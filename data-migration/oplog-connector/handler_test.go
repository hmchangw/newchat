package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/mock/gomock"
)

// --- fakes -------------------------------------------------------------------

type fakeSource struct {
	events  []changeEvent
	idx     int
	nextErr error // returned instead of draining (e.g. history-lost)
	closed  bool
}

func (f *fakeSource) Next(context.Context) (changeEvent, error) {
	if f.nextErr != nil {
		return changeEvent{}, f.nextErr
	}
	if f.idx >= len(f.events) {
		return changeEvent{}, context.Canceled // graceful drain
	}
	ev := f.events[f.idx]
	f.idx++
	return ev, nil
}

func (f *fakeSource) Close(context.Context) error { f.closed = true; return nil }

type fakePublisher struct {
	mu        sync.Mutex
	msgs      []*nats.Msg
	failFirst int
	calls     int
	err       error
}

func (p *fakePublisher) PublishMsg(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls <= p.failFirst {
		return nil, p.err
	}
	p.msgs = append(p.msgs, msg)
	return &jetstream.PubAck{}, nil
}

// pubFunc adapts a function to the publisher interface.
type pubFunc func(context.Context, *nats.Msg) (*jetstream.PubAck, error)

func (f pubFunc) PublishMsg(ctx context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return f(ctx, msg)
}

// savedCheckpoints records the checkpoints a watcher persists. Mutex-guarded
// because the periodic flusher may call Save from a separate goroutine.
type savedCheckpoints struct {
	mu  sync.Mutex
	cps []Checkpoint
}

func (s *savedCheckpoints) all() []Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Checkpoint(nil), s.cps...)
}

func (s *savedCheckpoints) ids() []string {
	cps := s.all()
	ids := make([]string, len(cps))
	for i, cp := range cps {
		ids[i] = cp.EventID
	}
	return ids
}

// captureStore returns a generated MockCheckpointStore wired to record every
// saved checkpoint, plus the recorder.
func captureStore(t *testing.T) (*MockCheckpointStore, *savedCheckpoints) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := NewMockCheckpointStore(ctrl)
	rec := &savedCheckpoints{}
	m.EXPECT().Save(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cp *Checkpoint) error {
		rec.mu.Lock()
		rec.cps = append(rec.cps, *cp)
		rec.mu.Unlock()
		return nil
	}).AnyTimes()
	return m, rec
}

// --- helpers -----------------------------------------------------------------

func mkEvent(id, op string) changeEvent {
	tok, err := bson.Marshal(bson.M{"_data": "RT-" + id})
	if err != nil {
		panic(err)
	}
	doc, err := bson.Marshal(bson.M{"_id": id})
	if err != nil {
		panic(err)
	}
	return changeEvent{
		EventID:       id,
		ResumeToken:   tok,
		Op:            op,
		DB:            "rocketchat",
		Collection:    "rocketchat_message",
		DocumentKey:   doc,
		FullDocument:  doc,
		ClusterTimeMs: 1718100000000,
	}
}

func testWatcher(src changeSource, pub publisher, store CheckpointStore, every int) *watcher {
	// Large max-age so the periodic flusher never fires during fast unit tests;
	// tests that exercise it set checkpointMaxAge explicitly.
	w := newWatcher("site1", "rocketchat_message", src, pub, store, every, time.Hour)
	w.initialBackoff = time.Millisecond
	w.maxBackoff = 2 * time.Millisecond
	w.now = func() int64 { return 1718100000123 }
	return w
}

func msgIDs(msgs []*nats.Msg) []string {
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.Header.Get("Nats-Msg-Id")
	}
	return ids
}

// --- tests -------------------------------------------------------------------

func TestWatcher_PublishesInOrderAndCheckpointsOnDrain(t *testing.T) {
	src := &fakeSource{events: []changeEvent{
		mkEvent("E1", "insert"), mkEvent("E2", "update"), mkEvent("E3", "delete"),
	}}
	pub := &fakePublisher{}
	store, saved := captureStore(t)
	w := testWatcher(src, pub, store, 100) // no mid-run save; only final

	require.NoError(t, w.run(context.Background()))

	require.Len(t, pub.msgs, 3)
	assert.Equal(t, "chat.migration.oplog.site1.rocketchat_message.insert", pub.msgs[0].Subject)
	assert.Equal(t, "chat.migration.oplog.site1.rocketchat_message.delete", pub.msgs[2].Subject)
	assert.Equal(t, "E1", pub.msgs[0].Header.Get("Nats-Msg-Id"))
	assert.Equal(t, []string{"E1", "E2", "E3"}, msgIDs(pub.msgs), "publish order = oplog order")

	all := saved.all()
	require.NotEmpty(t, all)
	last := all[len(all)-1]
	assert.Equal(t, "E3", last.EventID, "final checkpoint is the last published event")
	assert.Equal(t, "runtime", last.Source)
	assert.True(t, src.closed, "source closed on exit")
}

func TestWatcher_CheckpointEveryNAndFinal(t *testing.T) {
	src := &fakeSource{events: []changeEvent{
		mkEvent("E1", "insert"), mkEvent("E2", "insert"), mkEvent("E3", "insert"),
	}}
	pub := &fakePublisher{}
	store, saved := captureStore(t)
	w := testWatcher(src, pub, store, 2) // save every 2 events + final

	require.NoError(t, w.run(context.Background()))

	ids := saved.ids()
	assert.Contains(t, ids, "E2", "interval checkpoint at the 2nd event")
	assert.Equal(t, "E3", ids[len(ids)-1], "final checkpoint at drain")
}

func TestWatcher_PublishRetriesUntilAck(t *testing.T) {
	src := &fakeSource{events: []changeEvent{mkEvent("E1", "insert")}}
	pub := &fakePublisher{failFirst: 2, err: errors.New("no pub-ack")}
	store, saved := captureStore(t)
	w := testWatcher(src, pub, store, 100) // only the final drain checkpoint

	require.NoError(t, w.run(context.Background()))

	assert.Equal(t, 3, pub.calls, "2 failures + 1 success")
	require.Len(t, pub.msgs, 1, "event published exactly once after ack")
	assert.Equal(t, []string{"E1"}, saved.ids(), "checkpoint only after ack")
}

func TestWatcher_NoCheckpointWhenNeverAcked(t *testing.T) {
	// Publisher fails and triggers shutdown (cancel) — the frontier must not
	// advance and no checkpoint may be written past the un-acked event.
	ctx, cancel := context.WithCancel(context.Background())
	pub := pubFunc(func(context.Context, *nats.Msg) (*jetstream.PubAck, error) {
		cancel()
		return nil, errors.New("stream down")
	})
	src := &fakeSource{events: []changeEvent{mkEvent("E1", "insert")}}
	store, saved := captureStore(t)
	w := testWatcher(src, pub, store, 1)

	require.NoError(t, w.run(ctx), "ctx-cancel during retry is a graceful stop")
	assert.Empty(t, saved.all(), "no checkpoint past an un-acked event")
}

func TestIsHistoryLost(t *testing.T) {
	assert.True(t, isHistoryLost(mongo.CommandError{Code: 286}))
	assert.False(t, isHistoryLost(mongo.CommandError{Code: 11000}), "other server errors are not history-lost")
	assert.False(t, isHistoryLost(errors.New("plain error")))
	assert.False(t, isHistoryLost(nil))
}

func TestWatcher_DegradedEventStillPublishedAndCheckpointed(t *testing.T) {
	// A field that fails to encode must NOT drop the event: it is published
	// degraded so the checkpoint advances legitimately (lossless guarantee).
	ev := mkEvent("E1", "insert")
	ev.FullDocument = bson.Raw{0x05, 0x00, 0x00, 0x00, 0x01} // invalid BSON → encode fails

	src := &fakeSource{events: []changeEvent{ev}}
	pub := &fakePublisher{}
	store, saved := captureStore(t)
	w := testWatcher(src, pub, store, 1)

	require.NoError(t, w.run(context.Background()))

	require.Len(t, pub.msgs, 1, "degraded event is still published, never dropped")
	assert.Equal(t, "E1", pub.msgs[0].Header.Get("Nats-Msg-Id"))
	assert.Equal(t, []string{"E1"}, saved.ids(), "checkpoint advances past the published degraded event")
}

func TestWatcher_PeriodicFlushCheckpointsBelowCount(t *testing.T) {
	// Few events, count threshold never reached — the time-based flusher must
	// still persist the frontier (M1: bound replay by wall-clock).
	src := &fakeSource{events: []changeEvent{mkEvent("E1", "insert"), mkEvent("E2", "insert")}}
	pub := &fakePublisher{}
	store, saved := captureStore(t)
	w := newWatcher("site1", "rocketchat_message", src, pub, store, 1000, 10*time.Millisecond)
	w.initialBackoff = time.Millisecond
	w.now = func() int64 { return 1718100000123 }

	go func() { _ = w.run(t.Context()) }()

	require.Eventually(t, func() bool {
		ids := saved.ids()
		return len(ids) > 0 && ids[len(ids)-1] == "E2"
	}, 3*time.Second, 10*time.Millisecond, "periodic flusher should persist the frontier without hitting the count threshold")
}

func TestCheckpointer_FlushDedupes(t *testing.T) {
	store, saved := captureStore(t)
	cps := &checkpointer{store: store}

	cps.record(&Checkpoint{Collection: "c", EventID: "E1"})
	require.NoError(t, cps.flush(context.Background()))
	require.NoError(t, cps.flush(context.Background())) // same frontier — no second write
	assert.Equal(t, []string{"E1"}, saved.ids())

	cps.record(&Checkpoint{Collection: "c", EventID: "E2"})
	require.NoError(t, cps.flush(context.Background()))
	assert.Equal(t, []string{"E1", "E2"}, saved.ids())
}

func TestWatcher_HistoryLostIsFatal(t *testing.T) {
	src := &fakeSource{nextErr: mongo.CommandError{Code: 286, Message: "ChangeStreamHistoryLost"}}
	pub := &fakePublisher{}
	store, saved := captureStore(t)
	w := testWatcher(src, pub, store, 1)

	err := w.run(context.Background())
	require.Error(t, err, "lost resume token must be fatal (no silent reseed)")
	assert.Contains(t, err.Error(), "history lost")
	assert.Empty(t, saved.all())
}
