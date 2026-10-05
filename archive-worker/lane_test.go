package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/loopguard"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

type scriptedFetcher struct {
	batches [][]jetstream.Msg
	i       int
	idle    sync.Once
	idleCh  chan struct{}
}

// scripted builds a fetcher over batches; its idle channel closes on the first
// Fetch after the script is spent, which means the lane has finished handling
// every scripted message and is back at the top of its loop.
func scripted(batches ...[]jetstream.Msg) *scriptedFetcher {
	return &scriptedFetcher{batches: batches, idleCh: make(chan struct{})}
}

func (f *scriptedFetcher) waitIdle(t *testing.T) {
	t.Helper()
	select {
	case <-f.idleCh:
	case <-time.After(2 * time.Second):
		t.Fatal("lane never finished the scripted batches")
	}
}

type sliceBatch struct{ msgs []jetstream.Msg }

func (b sliceBatch) Messages() <-chan o11ynats.FetchedMessage {
	ch := make(chan o11ynats.FetchedMessage, len(b.msgs))
	for _, m := range b.msgs {
		ch <- o11ynats.FetchedMessage{Ctx: context.Background(), Msg: m}
	}
	close(ch)
	return ch
}

// Fetch is only ever called from the lane's run goroutine, so i needs no lock.
// Once the script is spent it behaves like an idle consumer: an empty batch
// after a short wait, or the context error once the lane is cancelled.
func (f *scriptedFetcher) Fetch(ctx context.Context, _ int, _ ...jetstream.FetchOpt) (msgBatch, error) {
	if f.i < len(f.batches) {
		b := f.batches[f.i]
		f.i++
		return sliceBatch{msgs: b}, nil
	}
	f.idle.Do(func() { close(f.idleCh) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(20 * time.Millisecond):
		return sliceBatch{}, nil
	}
}

type errFetcher struct{ err error }

func (e errFetcher) Fetch(context.Context, int, ...jetstream.FetchOpt) (msgBatch, error) {
	return nil, e.err
}

// flakyFetcher fails with err for the first n calls, then delegates.
type flakyFetcher struct {
	next  msgFetcher
	err   error
	fails int
}

func (f *flakyFetcher) Fetch(ctx context.Context, n int, o ...jetstream.FetchOpt) (msgBatch, error) {
	if f.fails > 0 {
		f.fails--
		return nil, f.err
	}
	return f.next.Fetch(ctx, n, o...)
}

func (f *fakeObjects) putCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.puts)
}

func eventMsg(t *testing.T, seq uint64, ev model.MessageEvent) *fakeMsg { //nolint:gocritic // hugeParam: tests pass map values, which are not addressable
	t.Helper()
	data, err := json.Marshal(ev)
	require.NoError(t, err)
	return &fakeMsg{subject: "chat.msg.canonical.site-a." + string(ev.Event), data: data, seq: seq, stream: "MESSAGES-CANONICAL-site-a"}
}

func laneCfg() laneConfig {
	return laneConfig{site: "site-a", name: "events", fetchBatch: 10, fillInterval: time.Hour, now: time.Now}
}

func testCipher(t *testing.T) *auditarchive.Cipher {
	t.Helper()
	c, err := auditarchive.NewCipher(testDEK())
	require.NoError(t, err)
	return c
}

func TestLane_Run(t *testing.T) {
	c := testCipher(t)
	events := loadEvents(t)
	good1 := eventMsg(t, 1, events["created"])
	good2 := eventMsg(t, 2, events["deleted"])
	skip := eventMsg(t, 3, func() model.MessageEvent { e := events["created"]; e.Event = model.EventThreadReplyAdded; return e }())
	poison := &fakeMsg{subject: "x", data: []byte("{"), seq: 4, stream: "MESSAGES-CANONICAL-site-a"}

	obj, idx := &fakeObjects{}, &fakeIndex{}
	fl := newFlusher(obj, idx, cfgFast(), &metrics{})
	b := newBatcher(2, 1<<20, time.Hour) // count bound of 2 trips before the hour
	guard := loopguard.New("test-lane", func() {})
	l := newLane(laneCfg(), scripted([]jetstream.Msg{good1, skip, poison, good2}), buildEventItem, c, b, fl, guard)

	stop, done := make(chan struct{}), make(chan struct{})
	go l.run(context.Background(), stop, done)
	require.Eventually(t, func() bool { return obj.putCount() == 1 }, 2*time.Second, 10*time.Millisecond)
	close(stop)
	<-done

	assert.True(t, good1.acked)
	assert.True(t, good2.acked)
	assert.True(t, skip.acked)
	assert.False(t, skip.termed)
	assert.True(t, poison.termed)
	assert.False(t, poison.naked)
	assert.False(t, poison.acked)
	assert.Equal(t, 1, obj.putCount(), "both good messages share one segment")
	assert.NoError(t, guard.Check().Probe(context.Background()), "a clean stop is not a death")
}

func TestLane_DecodesZstdPayload(t *testing.T) {
	events := loadEvents(t)
	m := eventMsg(t, 1, events["created"])
	m.data = natsutil.EncodeZstd(m.data)
	m.headers = nats.Header{natsutil.HeaderNatsEncoding: []string{natsutil.EncodingZstd}}
	obj := &fakeObjects{}
	l := newLane(laneCfg(), scripted([]jetstream.Msg{m}), buildEventItem, testCipher(t), newBatcher(1, 1<<20, time.Hour),
		newFlusher(obj, &fakeIndex{}, cfgFast(), &metrics{}), loopguard.New("test-lane", func() {}))
	stop, done := make(chan struct{}), make(chan struct{})
	go l.run(context.Background(), stop, done)
	require.Eventually(t, func() bool { return obj.putCount() == 1 }, 2*time.Second, 10*time.Millisecond)
	close(stop)
	<-done
	assert.True(t, m.acked)
	assert.False(t, m.termed)
}

func TestLane_BuildErrorNaksWithDelay(t *testing.T) {
	events := loadEvents(t)
	m := eventMsg(t, 1, events["created"])
	failing := func(context.Context, string, jetstream.Msg, []byte, *auditarchive.Cipher, time.Time) (item, error) {
		return item{}, errors.New("transient build failure")
	}
	obj, sf := &fakeObjects{}, scripted([]jetstream.Msg{m})
	l := newLane(laneCfg(), sf, failing, testCipher(t), newBatcher(100, 1<<20, time.Hour),
		newFlusher(obj, &fakeIndex{}, cfgFast(), &metrics{}), loopguard.New("test-lane", func() {}))
	stop, done := make(chan struct{}), make(chan struct{})
	go l.run(context.Background(), stop, done)
	sf.waitIdle(t)
	close(stop)
	<-done
	assert.True(t, m.naked)
	assert.Greater(t, m.nakDelay, time.Duration(0), "never a bare nak")
	assert.False(t, m.acked)
	assert.False(t, m.termed)
	assert.Equal(t, 0, obj.putCount())
}

type unmarshalableDoc struct{ C chan int }

func (unmarshalableDoc) SetLocation(string, int64) {}

func TestLane_SealFailureNaksBatch(t *testing.T) {
	events := loadEvents(t)
	m := eventMsg(t, 1, events["created"])
	badDoc := func(ctx context.Context, _ string, msg jetstream.Msg, _ []byte, _ *auditarchive.Cipher, _ time.Time) (item, error) {
		return item{ctx: ctx, msg: msg, seq: 1, frame: []byte{1}, docs: []docSpec{{Index: "i", ID: "d", Doc: unmarshalableDoc{C: make(chan int)}}}}, nil
	}
	obj, sf := &fakeObjects{}, scripted([]jetstream.Msg{m})
	l := newLane(laneCfg(), sf, badDoc, testCipher(t), newBatcher(1, 1<<20, time.Hour),
		newFlusher(obj, &fakeIndex{}, cfgFast(), &metrics{}), loopguard.New("test-lane", func() {}))
	stop, done := make(chan struct{}), make(chan struct{})
	go l.run(context.Background(), stop, done)
	sf.waitIdle(t)
	close(stop)
	<-done
	assert.True(t, m.naked, "an unsealable batch is released for redelivery, not dropped")
	assert.False(t, m.acked)
	assert.Equal(t, 0, obj.putCount())
}

func TestLane_DrainOnStop(t *testing.T) {
	events := loadEvents(t)
	only := eventMsg(t, 1, events["created"])
	obj, idx := &fakeObjects{}, &fakeIndex{}
	b := newBatcher(100, 1<<20, time.Hour)
	l := newLane(laneCfg(), scripted([]jetstream.Msg{only}), buildEventItem, testCipher(t), b,
		newFlusher(obj, idx, cfgFast(), &metrics{}), loopguard.New("test-lane", func() {}))
	stop, done := make(chan struct{}), make(chan struct{})
	go l.run(context.Background(), stop, done)
	require.Eventually(t, func() bool { return b.len() == 1 }, 2*time.Second, 5*time.Millisecond, "message is buffered, no bound tripped")
	assert.Equal(t, 0, obj.putCount(), "nothing flushed before stop")
	close(stop)
	<-done
	assert.Equal(t, 1, obj.putCount(), "partial batch is flushed on shutdown")
	assert.True(t, only.acked)
}

func TestLane_FillIntervalFlushes(t *testing.T) {
	events := loadEvents(t)
	only := eventMsg(t, 1, events["created"])
	t0 := time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC)
	var clock atomic.Int64
	clock.Store(t0.UnixNano())
	cfg := laneCfg()
	cfg.fillInterval = time.Minute
	cfg.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	obj := &fakeObjects{}
	b := newBatcher(100, 1<<20, time.Minute)
	l := newLane(cfg, scripted([]jetstream.Msg{only}), buildEventItem, testCipher(t), b,
		newFlusher(obj, &fakeIndex{}, cfgFast(), &metrics{}), loopguard.New("test-lane", func() {}))
	stop, done := make(chan struct{}), make(chan struct{}, 1)
	go l.run(context.Background(), stop, done)
	require.Eventually(t, func() bool { return b.len() == 1 }, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, 0, obj.putCount(), "fill interval has not elapsed")
	clock.Store(t0.Add(2 * time.Minute).UnixNano())
	require.Eventually(t, func() bool { return obj.putCount() == 1 }, 2*time.Second, 5*time.Millisecond, "due batch flushes while the lane is still running")
	close(stop)
	<-done
	assert.True(t, only.acked)
}

func TestLane_TransientFetchErrorKeepsRunning(t *testing.T) {
	events := loadEvents(t)
	only := eventMsg(t, 1, events["created"])
	obj := &fakeObjects{}
	guard := loopguard.New("test-lane", func() {})
	f := &flakyFetcher{next: scripted([]jetstream.Msg{only}), err: jetstream.ErrNoHeartbeat, fails: 2}
	l := newLane(laneCfg(), f, buildEventItem, testCipher(t), newBatcher(1, 1<<20, time.Hour),
		newFlusher(obj, &fakeIndex{}, cfgFast(), &metrics{}), guard)
	l.fetchRetry = time.Millisecond
	stop, done := make(chan struct{}), make(chan struct{})
	go l.run(context.Background(), stop, done)
	require.Eventually(t, func() bool { return obj.putCount() == 1 }, 2*time.Second, 5*time.Millisecond)
	close(stop)
	<-done
	assert.True(t, only.acked)
	assert.NoError(t, guard.Check().Probe(context.Background()), "a missing heartbeat is not a loop death")
}

func TestLane_TerminalFetchErrorStopsGuard(t *testing.T) {
	for _, terr := range []error{jetstream.ErrConsumerNotFound, jetstream.ErrConsumerDeleted, jetstream.ErrStreamNotFound} {
		t.Run(terr.Error(), func(t *testing.T) {
			fired := make(chan struct{}, 1)
			guard := loopguard.New("test-lane", func() { fired <- struct{}{} })
			l := newLane(laneCfg(), errFetcher{err: terr}, buildEventItem, testCipher(t), newBatcher(100, 1<<20, time.Hour),
				newFlusher(&fakeObjects{}, &fakeIndex{}, cfgFast(), &metrics{}), guard)
			stop, done := make(chan struct{}), make(chan struct{})
			go l.run(context.Background(), stop, done)
			<-done
			assert.ErrorIs(t, guard.Check().Probe(context.Background()), terr, "readiness fails after a terminal fetch error")
			select {
			case <-fired:
			default:
				t.Error("unexpected-stop hook did not run")
			}
		})
	}
}

func TestLane_TerminalFetchErrorDrainsBuffered(t *testing.T) {
	events := loadEvents(t)
	only := eventMsg(t, 1, events["created"])
	obj := &fakeObjects{}
	// The first Fetch yields the message; the second reports the death.
	seq := &sequencedFetcher{first: scripted([]jetstream.Msg{only}), then: errFetcher{err: jetstream.ErrConsumerDeleted}}
	l := newLane(laneCfg(), seq, buildEventItem, testCipher(t), newBatcher(100, 1<<20, time.Hour),
		newFlusher(obj, &fakeIndex{}, cfgFast(), &metrics{}), loopguard.New("test-lane", func() {}))
	done := make(chan struct{})
	go l.run(context.Background(), make(chan struct{}), done)
	<-done
	assert.Equal(t, 1, obj.putCount(), "messages already fetched are archived before the lane gives up")
	assert.True(t, only.acked)
}

type sequencedFetcher struct {
	first, then msgFetcher
	n           int
}

func (s *sequencedFetcher) Fetch(ctx context.Context, n int, o ...jetstream.FetchOpt) (msgBatch, error) {
	s.n++
	if s.n == 1 {
		return s.first.Fetch(ctx, n, o...)
	}
	return s.then.Fetch(ctx, n, o...)
}

func TestLane_ContextCancelEndsLoop(t *testing.T) {
	events := loadEvents(t)
	only := eventMsg(t, 1, events["created"])
	obj := &fakeObjects{}
	b := newBatcher(100, 1<<20, time.Hour)
	guard := loopguard.New("test-lane", func() {})
	l := newLane(laneCfg(), scripted([]jetstream.Msg{only}), buildEventItem, testCipher(t), b,
		newFlusher(obj, &fakeIndex{}, cfgFast(), &metrics{}), guard)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go l.run(ctx, make(chan struct{}), done)
	require.Eventually(t, func() bool { return b.len() == 1 }, 2*time.Second, 5*time.Millisecond)
	cancel()
	<-done
	assert.True(t, only.acked, "buffered work is flushed even though the context is gone")
	assert.NoError(t, guard.Check().Probe(context.Background()))
}

type fakeO11yBatch struct{ msgs []o11ynats.FetchedMessage }

func (f fakeO11yBatch) Messages() <-chan o11ynats.FetchedMessage {
	ch := make(chan o11ynats.FetchedMessage, len(f.msgs))
	for _, m := range f.msgs {
		ch <- m
	}
	close(ch)
	return ch
}
func (f fakeO11yBatch) Error() error { return nil }
func (f fakeO11yBatch) Stop()        {}

type fakeO11yConsumer struct {
	o11ynats.Consumer
	batch o11ynats.MessageBatch
	err   error
}

func (f fakeO11yConsumer) Fetch(context.Context, int, ...jetstream.FetchOpt) (o11ynats.MessageBatch, error) {
	return f.batch, f.err
}

type fakeRawBatch struct{ msgs []jetstream.Msg }

func (f fakeRawBatch) Messages() <-chan jetstream.Msg {
	ch := make(chan jetstream.Msg, len(f.msgs))
	for _, m := range f.msgs {
		ch <- m
	}
	close(ch)
	return ch
}
func (f fakeRawBatch) Error() error { return nil }

type fakeRawConsumer struct {
	jetstream.Consumer
	batch jetstream.MessageBatch
	err   error
}

func (f fakeRawConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return f.batch, f.err
}

type ctxKey string

func TestConsumerAdapters(t *testing.T) {
	m1, m2 := &fakeMsg{seq: 1}, &fakeMsg{seq: 2}
	ctx := context.WithValue(context.Background(), ctxKey("id"), "caller")
	drain := func(b msgBatch) []o11ynats.FetchedMessage {
		var got []o11ynats.FetchedMessage
		for m := range b.Messages() {
			got = append(got, m)
		}
		return got
	}
	t.Run("o11y adapter passes the receive-span context through", func(t *testing.T) {
		own := context.WithValue(context.Background(), ctxKey("id"), "span")
		a := o11yConsumerAdapter{c: fakeO11yConsumer{batch: fakeO11yBatch{msgs: []o11ynats.FetchedMessage{{Ctx: own, Msg: m1}, {Ctx: own, Msg: m2}}}}}
		b, err := a.Fetch(ctx, 10)
		require.NoError(t, err)
		assert.Equal(t, []o11ynats.FetchedMessage{{Ctx: own, Msg: m1}, {Ctx: own, Msg: m2}}, drain(b))
	})
	t.Run("raw adapter stamps the fetch context on each message", func(t *testing.T) {
		a := rawConsumerAdapter{c: fakeRawConsumer{batch: fakeRawBatch{msgs: []jetstream.Msg{m1, m2}}}}
		b, err := a.Fetch(ctx, 10)
		require.NoError(t, err)
		assert.Equal(t, []o11ynats.FetchedMessage{{Ctx: ctx, Msg: m1}, {Ctx: ctx, Msg: m2}}, drain(b))
	})
	t.Run("wrapped fetch errors still classify as terminal", func(t *testing.T) {
		_, err := o11yConsumerAdapter{c: fakeO11yConsumer{err: jetstream.ErrConsumerNotFound}}.Fetch(ctx, 10)
		assert.True(t, terminalFetchErr(err))
		_, err = rawConsumerAdapter{c: fakeRawConsumer{err: jetstream.ErrConsumerDeleted}}.Fetch(ctx, 10)
		assert.True(t, terminalFetchErr(err))
		assert.False(t, terminalFetchErr(jetstream.ErrNoHeartbeat))
		assert.False(t, terminalFetchErr(errors.New("timeout")))
	})
}
