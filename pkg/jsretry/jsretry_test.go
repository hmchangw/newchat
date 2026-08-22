package jsretry

import (
	"context"
	"errors"
	"log/slog"
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

// assertJittered asserts d is within the equal-jitter band [base/2, base].
func assertJittered(t *testing.T, base, d time.Duration) {
	t.Helper()
	assert.GreaterOrEqual(t, d, base/2, "equal jitter guarantees at least half the base delay")
	assert.LessOrEqual(t, d, base, "jitter never exceeds the base delay")
}

func TestSettle_BackoffSelectedByAttempt(t *testing.T) {
	m := &fakeMsg{numDelivered: 2} // second delivery -> testSchedule[1]
	Settle(context.Background(), m, testSchedule, errors.New("boom"))
	assertJittered(t, testSchedule[1], m.nakDelay)
}

func TestBackoffFor(t *testing.T) {
	tests := []struct {
		name         string
		numDelivered uint64
		base         time.Duration
	}{
		{name: "metadata zero — first", numDelivered: 0, base: testSchedule[0]},
		{name: "first delivery — first", numDelivered: 1, base: testSchedule[0]},
		{name: "second delivery — second", numDelivered: 2, base: testSchedule[1]},
		{name: "third delivery — third", numDelivered: 3, base: testSchedule[2]},
		{name: "beyond schedule — reuses last", numDelivered: 99, base: testSchedule[2]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertJittered(t, tt.base, backoffFor(&fakeMsg{numDelivered: tt.numDelivered}, testSchedule))
		})
	}
}

func TestBackoffFor_MetadataError(t *testing.T) {
	assertJittered(t, testSchedule[0], backoffFor(&fakeMsg{metaErr: errors.New("no meta")}, testSchedule))
}

// Jitter must actually vary, otherwise a fleet that parked during one outage
// retries in lockstep the moment the dependency recovers.
func TestBackoffFor_JitterVaries(t *testing.T) {
	seen := make(map[time.Duration]struct{})
	for range 200 {
		seen[backoffFor(&fakeMsg{numDelivered: 3}, testSchedule)] = struct{}{}
	}
	assert.Greater(t, len(seen), 1, "equal jitter should produce a spread of delays")
}

// A non-positive base must floor to minNakDelay, never pass through: a zero
// delay serializes as a bare -NAK.
func TestJitter_NonPositiveFloorsToMinimum(t *testing.T) {
	assert.Equal(t, minNakDelay, jitter(0))
	assert.Equal(t, minNakDelay, jitter(-time.Second))
	assert.Equal(t, minNakDelay, jitter(minNakDelay))
}

func TestNak_UsesJitteredBackoffForAttempt(t *testing.T) {
	m := &fakeMsg{numDelivered: 2}
	Nak(context.Background(), m, testSchedule, "downstream unavailable")
	assert.True(t, m.naked)
	assert.False(t, m.acked)
	assertJittered(t, testSchedule[1], m.nakDelay)
}

func TestNak_LogsWhenTheNakCallFails(t *testing.T) {
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	var n int
	slog.SetDefault(slog.New(countHandler{n: &n}))
	Nak(context.Background(), &fakeMsg{numDelivered: 1, nakErr: errors.New("conn closed")}, testSchedule, "transient")
	assert.Equal(t, 1, n, "a failed nak network call is logged once")
}

func TestNak_SilentOnSuccess(t *testing.T) {
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	var n int
	slog.SetDefault(slog.New(countHandler{n: &n}))
	Nak(context.Background(), &fakeMsg{numDelivered: 1}, testSchedule, "transient")
	assert.Zero(t, n, "the caller owns the business-error log; Nak must not double-log")
}

// Synadia's published four-entry schedule plus a 10m tail — 12m36s over five
// gaps, the agreed ~15 min retry budget.
func TestDefaultBackoff_Shape(t *testing.T) {
	assert.Equal(t, []time.Duration{
		1 * time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute,
	}, DefaultBackoff)
}

// Deliberately not extended: broadcast fan-out is user-visible and a
// 15-minute-late broadcast is worthless.
func TestLowLatencyBackoff_StaysShort(t *testing.T) {
	assert.Equal(t, []time.Duration{
		200 * time.Millisecond, 1 * time.Second, 5 * time.Second, 30 * time.Second,
	}, LowLatencyBackoff)
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

// jitter halves the base, so a sub-2ns entry can round to zero — and
// NakWithDelay(0) is the bare -NAK this package exists to prevent.
func TestNak_SubNanosecondScheduleNeverSendsZero(t *testing.T) {
	for range 200 {
		m := &fakeMsg{numDelivered: 1}
		Nak(context.Background(), m, []time.Duration{1}, "tiny")
		assert.Positive(t, m.nakDelay, "a nak delay of 0 is a bare -NAK")
	}
}

func TestNak_EmptyScheduleDoesNotPanic(t *testing.T) {
	m := &fakeMsg{numDelivered: 1}
	assert.NotPanics(t, func() { Nak(context.Background(), m, nil, "empty") })
}

func TestSettle_EmptyScheduleDoesNotPanic(t *testing.T) {
	m := &fakeMsg{numDelivered: 1}
	assert.NotPanics(t, func() {
		Settle(context.Background(), m, nil, errors.New("boom"))
	})
}
