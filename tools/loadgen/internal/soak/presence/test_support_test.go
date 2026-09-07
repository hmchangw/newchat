package presence

import (
	"context"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
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

// testTopology builds the pair every presence test runs against. It took a
// candidate count that never reached the caller — both call sites got exactly
// these two users whatever they asked for — so the knob is gone rather than
// made real: widening the population would change the fixture the assertions
// on user-a0 and user-b0 are written against.
func testTopology() *topology.Topology {
	users := make([]model.User, 0, 2)
	for i := range 2 {
		users = append(users, model.User{
			ID:      "u" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
			Account: "user-" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
		})
	}
	return &topology.Topology{ActiveUsers: users}
}
