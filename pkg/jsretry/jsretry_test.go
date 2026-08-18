package jsretry

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/errcode"
)

// fakeMsg is a jsretry.Msg test double recording ack/nak behaviour. ackErr /
// nakErr force the Ack / NakWithDelay network calls to fail.
type fakeMsg struct {
	numDelivered uint64
	metaErr      error
	ackErr       error
	nakErr       error
	acked        bool
	naked        bool
	nakDelay     time.Duration
}

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return &jetstream.MsgMetadata{NumDelivered: m.numDelivered}, nil
}
func (m *fakeMsg) Ack() error { m.acked = true; return m.ackErr }
func (m *fakeMsg) NakWithDelay(d time.Duration) error {
	m.naked = true
	m.nakDelay = d
	return m.nakErr
}

var testSchedule = []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second}

func TestSettle(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		wantAck      bool
		wantNak      bool
		wantNakDelay bool
	}{
		{name: "success — Ack", err: nil, wantAck: true},
		{
			name:    "permanent — Ack to drop poison",
			err:     errcode.Permanent(errcode.BadRequest("malformed")),
			wantAck: true,
		},
		{
			name:         "transient — Nak with backoff",
			err:          errors.New("cassandra unavailable"),
			wantNak:      true,
			wantNakDelay: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &fakeMsg{}
			Settle(context.Background(), m, testSchedule, tt.err)
			assert.Equal(t, tt.wantAck, m.acked, "acked")
			assert.Equal(t, tt.wantNak, m.naked, "naked")
			if tt.wantNakDelay {
				assert.Positive(t, m.nakDelay, "nak delay should be a positive backoff")
			}
		})
	}
}

// SettleQuiet must reach the same ack/nak decision as Settle — it differs only
// in logging (covered separately).
func TestSettleQuiet_SameDecisions(t *testing.T) {
	t.Run("permanent — Ack", func(t *testing.T) {
		m := &fakeMsg{}
		SettleQuiet(context.Background(), m, testSchedule, errcode.Permanent(errcode.BadRequest("x")))
		assert.True(t, m.acked)
		assert.False(t, m.naked)
	})
	t.Run("transient — Nak with backoff", func(t *testing.T) {
		m := &fakeMsg{}
		SettleQuiet(context.Background(), m, testSchedule, errors.New("boom"))
		assert.True(t, m.naked)
		assert.Positive(t, m.nakDelay)
	})
}

func TestSettle_BackoffSelectedByAttempt(t *testing.T) {
	m := &fakeMsg{numDelivered: 2} // second delivery -> testSchedule[1]
	Settle(context.Background(), m, testSchedule, errors.New("boom"))
	assert.Equal(t, testSchedule[1], m.nakDelay)
}

func TestBackoffFor(t *testing.T) {
	tests := []struct {
		name         string
		numDelivered uint64
		want         time.Duration
	}{
		{name: "metadata zero — first", numDelivered: 0, want: testSchedule[0]},
		{name: "first delivery — first", numDelivered: 1, want: testSchedule[0]},
		{name: "second delivery — second", numDelivered: 2, want: testSchedule[1]},
		{name: "third delivery — third", numDelivered: 3, want: testSchedule[2]},
		{name: "beyond schedule — reuses last", numDelivered: 99, want: testSchedule[2]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, backoffFor(&fakeMsg{numDelivered: tt.numDelivered}, testSchedule))
		})
	}
}

func TestBackoffFor_MetadataError(t *testing.T) {
	assert.Equal(t, testSchedule[0], backoffFor(&fakeMsg{metaErr: errors.New("no meta")}, testSchedule))
}

// countHandler records how many log records were emitted.
type countHandler struct{ n *int }

func (h countHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (h countHandler) Handle(context.Context, slog.Record) error { *h.n++; return nil }
func (h countHandler) WithAttrs([]slog.Attr) slog.Handler        { return h }
func (h countHandler) WithGroup(string) slog.Handler             { return h }

// Settle logs the business error once; SettleQuiet stays silent on a successful
// Nak (the caller already logged the failure, e.g. via errcode.Classify).
func TestLoggingContract(t *testing.T) {
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	t.Run("Settle logs transient failure", func(t *testing.T) {
		var n int
		slog.SetDefault(slog.New(countHandler{n: &n}))
		Settle(context.Background(), &fakeMsg{}, testSchedule, errors.New("boom"))
		assert.Positive(t, n, "Settle should log the business error")
	})

	t.Run("SettleQuiet does not log transient failure", func(t *testing.T) {
		var n int
		slog.SetDefault(slog.New(countHandler{n: &n}))
		SettleQuiet(context.Background(), &fakeMsg{}, testSchedule, errors.New("boom"))
		assert.Zero(t, n, "SettleQuiet must not re-log the already-logged error")
	})
}

// A failing Ack/Nak network call is always logged, even under SettleQuiet which
// otherwise suppresses the business-error log — so SettleQuiet isolates the
// network-failure log as the only record emitted, one per settle branch.
func TestSettle_NetworkErrors(t *testing.T) {
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	tests := []struct {
		name string
		err  error
		msg  *fakeMsg
	}{
		{name: "Ack fails on success", err: nil, msg: &fakeMsg{ackErr: errors.New("ack failed")}},
		{name: "Ack fails on permanent", err: errcode.Permanent(errcode.BadRequest("x")), msg: &fakeMsg{ackErr: errors.New("ack failed")}},
		{name: "Nak fails on transient", err: errors.New("boom"), msg: &fakeMsg{nakErr: errors.New("nak failed")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n int
			slog.SetDefault(slog.New(countHandler{n: &n}))
			SettleQuiet(context.Background(), tt.msg, testSchedule, tt.err)
			assert.Equal(t, 1, n, "the network-failure path should log exactly once")
		})
	}
}

func TestElapsedFor(t *testing.T) {
	// The schedule Settle applies: the delay served after delivery k is
	// backoff[k-1] while the schedule lasts, then the last entry repeats.
	sched := []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute}

	tests := []struct {
		name         string
		backoff      []time.Duration
		numDelivered uint64
		want         time.Duration
	}{
		{name: "first delivery has served no delay", backoff: sched, numDelivered: 1, want: 0},
		{name: "zero is treated as a first delivery", backoff: sched, numDelivered: 0, want: 0},
		{name: "second delivery served the first delay", backoff: sched, numDelivered: 2, want: time.Second},
		{name: "third delivery served 1s+5s", backoff: sched, numDelivered: 3, want: 6 * time.Second},
		{name: "fourth delivery served 1s+5s+30s", backoff: sched, numDelivered: 4, want: 36 * time.Second},
		{name: "fifth delivery adds the first tail wait", backoff: sched, numDelivered: 5, want: 2*time.Minute + 36*time.Second},
		{name: "tail repeats past the schedule", backoff: sched, numDelivered: 7, want: 6*time.Minute + 36*time.Second},
		// 36s + 30 tail waits = 1h36s: the first delivery at or past a 1h window.
		{name: "an hour of retrying is reached on the 34th delivery", backoff: sched, numDelivered: 34, want: time.Hour + 36*time.Second},
		{name: "the 33rd delivery is still under an hour", backoff: sched, numDelivered: 33, want: 58*time.Minute + 36*time.Second},
		{name: "empty schedule has no accumulated wait", backoff: nil, numDelivered: 99, want: 0},
		{name: "single-entry schedule repeats it", backoff: []time.Duration{time.Second}, numDelivered: 4, want: 3 * time.Second},
		// A nonsense delivery count must saturate, never wrap: a wrapped negative
		// would read as "barely retried" to a caller deciding whether to give up.
		{name: "an absurd delivery count saturates", backoff: sched, numDelivered: ^uint64(0), want: time.Duration(math.MaxInt64)},
		{name: "a zero-delay tail stops accumulating", backoff: []time.Duration{time.Second, 0}, numDelivered: 1 << 40, want: time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ElapsedFor(tt.backoff, tt.numDelivered))
		})
	}
}

func TestElapsedFor_MatchesBackoffForSchedule(t *testing.T) {
	// ElapsedFor must sum exactly the delays backoffFor hands to NakWithDelay,
	// or the drop deadline it feeds would drift from the real retry cadence.
	var want time.Duration
	for n := uint64(1); n <= 40; n++ {
		assert.Equal(t, want, ElapsedFor(DefaultBackoff, n), "numDelivered=%d", n)
		want += backoffFor(&fakeMsg{numDelivered: n}, DefaultBackoff)
	}
}
