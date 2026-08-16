package jobguard

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

// fakeMsg is a minimal jobguard.Message stub recording Ack calls so tests can
// assert poison-pill disposal without a NATS server.
type fakeMsg struct {
	subject  string
	acked    bool
	ackCalls int
	ackErr   error
	marked   bool
}

func (m *fakeMsg) Subject() string   { return m.subject }
func (m *fakeMsg) Ack() error        { m.acked = true; m.ackCalls++; return m.ackErr }
func (m *fakeMsg) MarkHandlerPanic() { m.marked = true }

func TestGuard_NoPanic_RunsFnAndReportsFalse(t *testing.T) {
	ran := false
	panicked := Guard("subj", func() { ran = true })
	assert.True(t, ran, "fn must run")
	assert.False(t, panicked, "no panic must report panicked=false")
}

func TestGuard_Panic_RecoversAndReportsTrue(t *testing.T) {
	var panicked bool
	assert.NotPanics(t, func() {
		panicked = Guard("subj", func() { panic("boom") })
	}, "Guard must contain the panic so the worker survives")
	assert.True(t, panicked, "a recovered panic must report panicked=true")
}

func TestRun_Panic_AcksAsPoisonDrop(t *testing.T) {
	msg := &fakeMsg{subject: "chat.msg.canonical.s.created"}
	assert.NotPanics(t, func() {
		Run(msg, func() { panic("boom: errcode option misuse") })
	}, "a panicking process must be recovered, not crash the worker")
	assert.True(t, msg.acked, "panic must Ack (poison-pill drop) — a deterministic panic would otherwise crash-loop via redelivery")
	assert.Equal(t, 1, msg.ackCalls)
	assert.True(t, msg.marked, "panic drops must expose terminal failure evidence")
}

func TestRun_NoPanic_DoesNotAck(t *testing.T) {
	msg := &fakeMsg{subject: "subj"}
	Run(msg, func() { /* process owns its own Ack/Nak on the normal path */ })
	assert.False(t, msg.acked, "Run must not Ack on the normal path — process owns disposal")
}

// captureLogs redirects the default slog logger to a buffer for the duration
// of the test so assertions can inspect emitted records.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func TestRun_Panic_LogsDistinctDropLine(t *testing.T) {
	logs := captureLogs(t)
	Run(&fakeMsg{subject: "chat.poison.subject"}, func() { panic("boom") })
	out := logs.String()
	assert.Contains(t, out, "dropped poison message", "Ack-drop must be greppably distinct from a redeliver")
	assert.Contains(t, out, "chat.poison.subject", "drop log must carry the subject")
}

func TestRun_NoPanic_DoesNotLogDrop(t *testing.T) {
	logs := captureLogs(t)
	Run(&fakeMsg{subject: "subj"}, func() {})
	assert.NotContains(t, logs.String(), "dropped poison message", "normal path must not log a drop")
}

func TestRun_PanicWithAckError_DoesNotLogDrop(t *testing.T) {
	logs := captureLogs(t)
	Run(&fakeMsg{subject: "subj", ackErr: errors.New("ack failed")}, func() { panic("boom") })
	out := logs.String()
	assert.Contains(t, out, "failed to ack after panic", "a failed Ack must be surfaced")
	assert.NotContains(t, out, "dropped poison message", "a failed Ack is not a successful drop")
}

func TestRun_PanicWithAckError_DoesNotCrash(t *testing.T) {
	msg := &fakeMsg{subject: "subj", ackErr: errors.New("ack failed")}
	assert.NotPanics(t, func() {
		Run(msg, func() { panic("boom") })
	}, "an Ack error on the panic path must be logged, not crash the worker")
	assert.True(t, msg.acked)
}
