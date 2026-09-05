package search

import (
	"context"
	"sync"
	"time"

	"github.com/hmchangw/chat/tools/loadgen/internal/soak/read"
)

type testTransport struct {
	mu       sync.Mutex
	reply    []byte
	err      error
	subjects []string
}

func (t *testTransport) Request(
	_ context.Context,
	target string,
	_ []byte,
	_ time.Duration,
) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.subjects = append(t.subjects, target)
	return append([]byte(nil), t.reply...), t.err
}

func (t *testTransport) calls() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.subjects...)
}

type testSleeper struct{}

func (*testSleeper) Sleep(ctx context.Context, _ time.Duration) error {
	return ctx.Err()
}

type testRecorder struct {
	mu      sync.Mutex
	samples []read.Sample
}

func (r *testRecorder) Record(sample *read.Sample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, *sample)
}

func (r *testRecorder) snapshot() []read.Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]read.Sample(nil), r.samples...)
}
