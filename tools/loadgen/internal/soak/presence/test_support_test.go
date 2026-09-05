package presence

import (
	"context"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/read"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
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

type testSleeper struct{}

func (*testSleeper) Sleep(ctx context.Context, _ time.Duration) error {
	return ctx.Err()
}

type testReadRecorder struct {
	mu      sync.Mutex
	samples []read.Sample
}

func (r *testReadRecorder) Record(sample *read.Sample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, *sample)
}

type testObserver struct {
	mu          sync.Mutex
	signals     map[string]int
	checks      map[string]int
	connections map[model.PresenceStatus]int
}

func newTestObserver() *testObserver {
	return &testObserver{
		signals:     make(map[string]int),
		checks:      make(map[string]int),
		connections: make(map[model.PresenceStatus]int),
	}
}

func (o *testObserver) CountSignal(signal string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.signals[signal]++
}

func (o *testObserver) CountCheck(result string, count int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.checks[result] += count
}

func (o *testObserver) SetConnections(status model.PresenceStatus, count int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.connections[status] = count
}

func (o *testObserver) signal(signal string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.signals[signal]
}

func (o *testObserver) check(result string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.checks[result]
}

func (o *testObserver) connection(status model.PresenceStatus) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.connections[status]
}

func testTopology(candidates int) *topology.Topology {
	users := make([]model.User, 0, candidates+2)
	for i := range candidates + 2 {
		users = append(users, model.User{
			ID:      "u" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
			Account: "user-" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
		})
	}
	return &topology.Topology{ActiveUsers: users[:2]}
}
