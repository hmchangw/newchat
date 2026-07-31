package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/subject"
)

// newFanoutTestHandler builds a Handler via NewHandler so future field
// additions don't silently leave the test exercising a partially-initialized
// struct. Only fanOutKey is exercised here; the store/publish/keyStore stubs
// are unused by it.
func newFanoutTestHandler(t *testing.T, keySender *roomkeysender.Sender, workers int) *Handler {
	t.Helper()
	h := NewHandler(nil, "test-site", nil, testKeyStore, keySender, subject.RouteGlobal)
	h.SetKeyFanoutWorkers(workers)
	return h
}

// barrierPublisher blocks every Publish call on a shared barrier so a test can
// observe how many goroutines reach Publish concurrently before any are
// allowed to finish.
type barrierPublisher struct {
	barrier chan struct{}
	arrived chan struct{}
	count   atomic.Int32
}

func newBarrierPublisher(buffer int) *barrierPublisher {
	return &barrierPublisher{
		barrier: make(chan struct{}),
		arrived: make(chan struct{}, buffer),
	}
}

func (b *barrierPublisher) Publish(_ string, _ []byte) error {
	b.count.Add(1)
	b.arrived <- struct{}{}
	<-b.barrier
	return nil
}

func TestFanOutKey_RespectsWorkerCap(t *testing.T) {
	const accounts = 8
	const workers = 3

	bp := newBarrierPublisher(accounts)
	h := newFanoutTestHandler(t, roomkeysender.NewSender(bp), workers)

	accts := make([]string, accounts)
	for i := range accts {
		accts[i] = fmt.Sprintf("u-%d", i)
	}
	evt := model.RoomKeyEvent{RoomID: "r", Version: 1}

	done := make(chan struct{})
	go func() {
		h.fanOutKey(context.Background(), "r", accts, &evt)
		close(done)
	}()

	// Exactly `workers` goroutines should reach the publisher before any
	// finish; the rest should be queued on the semaphore.
	for i := 0; i < workers; i++ {
		select {
		case <-bp.arrived:
		case <-time.After(time.Second):
			t.Fatalf("only %d goroutines reached Publish; fanout is not concurrent", i)
		}
	}
	select {
	case <-bp.arrived:
		t.Fatalf("more than %d goroutines reached Publish; worker cap not enforced", workers)
	case <-time.After(50 * time.Millisecond):
	}
	assert.Equal(t, int32(workers), bp.count.Load())

	close(bp.barrier)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fanout did not complete after releasing the barrier")
	}
	assert.Equal(t, int32(accounts), bp.count.Load(), "every account must have been published exactly once")
}

// recordingPublisher just records the accounts it sees, no blocking.
type recordingPublisher struct {
	mu       sync.Mutex
	subjects []string
}

func (r *recordingPublisher) Publish(subj string, _ []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subjects = append(r.subjects, subj)
	return nil
}

func (r *recordingPublisher) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.subjects))
	copy(out, r.subjects)
	return out
}

func TestFanOutKey_PublishesEveryAccount(t *testing.T) {
	const accounts = 100

	rp := &recordingPublisher{}
	h := newFanoutTestHandler(t, roomkeysender.NewSender(rp), 16)

	accts := make([]string, accounts)
	for i := range accts {
		accts[i] = fmt.Sprintf("acct-%03d", i)
	}
	evt := model.RoomKeyEvent{RoomID: "r"}
	h.fanOutKey(context.Background(), "r", accts, &evt)

	got := rp.snapshot()
	require.Len(t, got, accounts, "must publish once per account")
}

// dataRecordingPublisher records the raw payload slice of every Publish call
// WITHOUT copying, so a test can compare backing-array identity across calls.
// Safe because fanOutKey marshals once and never mutates the buffer after it is
// handed to Publish.
type dataRecordingPublisher struct {
	mu       sync.Mutex
	payloads [][]byte
}

func (d *dataRecordingPublisher) Publish(_ string, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.payloads = append(d.payloads, data)
	return nil
}

func (d *dataRecordingPublisher) snapshot() [][]byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([][]byte, len(d.payloads))
	copy(out, d.payloads)
	return out
}

// TestFanOutKey_MarshalsOnce asserts the event is serialized exactly once and
// the same buffer is fanned out to every recipient. It compares backing-array
// identity (same first-element address) rather than byte-equality: two
// json.Marshal calls always allocate distinct buffers, so a per-recipient
// re-marshal is caught deterministically even when every marshal lands in the
// same millisecond and produces identical JSON.
func TestFanOutKey_MarshalsOnce(t *testing.T) {
	const accounts = 50

	dp := &dataRecordingPublisher{}
	h := newFanoutTestHandler(t, roomkeysender.NewSender(dp), 8)

	accts := make([]string, accounts)
	for i := range accts {
		accts[i] = fmt.Sprintf("acct-%03d", i)
	}
	evt := model.RoomKeyEvent{RoomID: "r", Version: 2, PrivateKey: []byte{0xaa, 0xbb}}
	h.fanOutKey(context.Background(), "r", accts, &evt)

	payloads := dp.snapshot()
	require.Len(t, payloads, accounts)
	first := payloads[0]
	require.NotEmpty(t, first)
	for i, p := range payloads {
		require.NotEmpty(t, p, "payload %d is empty", i)
		assert.True(t, &p[0] == &first[0],
			"payload %d is a distinct allocation; event was marshaled more than once", i)
	}
}

func TestFanOutKey_NoAccountsIsNoOp(t *testing.T) {
	rp := &recordingPublisher{}
	h := newFanoutTestHandler(t, roomkeysender.NewSender(rp), 16)
	h.fanOutKey(context.Background(), "r", nil, &model.RoomKeyEvent{})
	assert.Empty(t, rp.snapshot())
}

// TestFanOutKey_WorkersDefaultWhenZero guards the defensive default inside
// fanOutKey. Existing tests in handler_test.go construct &Handler{...}
// directly with keyFanoutWorkers left at zero; the defensive default keeps
// those paths from deadlocking on an unbuffered semaphore.
func TestFanOutKey_WorkersDefaultWhenZero(t *testing.T) {
	rp := &recordingPublisher{}
	h := &Handler{
		keySender:        roomkeysender.NewSender(rp),
		keyFanoutWorkers: 0,
	}
	h.fanOutKey(context.Background(), "r", []string{"a", "b", "c"}, &model.RoomKeyEvent{})
	assert.Len(t, rp.snapshot(), 3)
}
