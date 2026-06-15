package natsrouter

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// flowRecorder captures records for asserting on the on-demand FLOW lines.
type flowRecorder struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *flowRecorder) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelInfo }

//nolint:gocritic // slog.Handler mandates the Record value parameter
func (h *flowRecorder) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, r.Clone())
	return nil
}
func (h *flowRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *flowRecorder) WithGroup(string) slog.Handler      { return h }
func (h *flowRecorder) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.recs)
}
func (h *flowRecorder) hasFlow(msg string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.recs {
		if h.recs[i].Level == logctx.LevelFlow && h.recs[i].Message == msg {
			return true
		}
	}
	return false
}

// payloads returns the `payload` field of every "debug payload" record.
func (h *flowRecorder) payloads() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for i := range h.recs {
		if h.recs[i].Message != "debug payload" {
			continue
		}
		h.recs[i].Attrs(func(a slog.Attr) bool {
			if a.Key == "payload" {
				out = append(out, a.Value.String())
			}
			return true
		})
	}
	return out
}

func runLoggingChain(ctx context.Context) *flowRecorder {
	rec := &flowRecorder{}
	prev := slog.Default()
	slog.SetDefault(slog.New(logctx.NewHandler(rec)))
	defer slog.SetDefault(prev)
	c := &Context{ctx: ctx, Msg: &nats.Msg{Subject: "test.subject"}, chain: &chainState{index: -1}}
	c.chain.handlers = []HandlerFunc{Logging(), func(*Context) {}}
	c.Next()
	return rec
}

// Logging() must no longer emit an always-on INFO line; per-request visibility
// is now an on-demand FLOW breadcrumb gated by X-Debug admission.
func TestLogging_EmitsFlowOnlyWhenAdmitted(t *testing.T) {
	logctx.Configure(logctx.Config{Rate: 1e6, Burst: 1 << 20})
	t.Cleanup(func() { logctx.Configure(logctx.Config{Rate: 0, Burst: 0}) })

	t.Run("admitted flow request emits the FLOW line", func(t *testing.T) {
		admitted := logctx.Admit(context.Background(), nats.Header{natsutil.DebugHeader: []string{"flow"}})
		rec := runLoggingChain(admitted)
		assert.True(t, rec.hasFlow("nats request"), "flagged request emits a FLOW breadcrumb")
	})

	t.Run("unflagged request emits nothing", func(t *testing.T) {
		rec := runLoggingChain(context.Background())
		assert.Equal(t, 0, rec.count(), "no always-on per-request line")
	})
}

func TestHandlerTimeout_SetsDeadline(t *testing.T) {
	c := &Context{ctx: context.Background(), chain: &chainState{index: -1}}
	var observedDeadline time.Time
	var ok bool
	c.chain.handlers = []HandlerFunc{
		HandlerTimeout(50 * time.Millisecond),
		func(c *Context) {
			observedDeadline, ok = c.Deadline()
		},
	}
	c.Next()

	require.True(t, ok, "deadline must be set inside the chain")
	assert.WithinDuration(t, time.Now().Add(50*time.Millisecond), observedDeadline, 30*time.Millisecond)
}

func TestHandlerTimeout_DoneFiresAfterExpiry(t *testing.T) {
	c := &Context{ctx: context.Background(), chain: &chainState{index: -1}}
	c.chain.handlers = []HandlerFunc{
		HandlerTimeout(20 * time.Millisecond),
		func(c *Context) {
			// Generous outer budget (2s) to absorb CI scheduler jitter — the
			// 20ms timer is what we're verifying fires; the outer bound only
			// catches a totally broken implementation.
			select {
			case <-c.Done():
				// expected
			case <-time.After(2 * time.Second):
				t.Fatal("ctx.Done() did not fire within 2s after a 20ms timeout")
			}
		},
	}
	c.Next()
}

// TestHandlerTimeout_DoesNotCancelParentContext verifies that the
// timeout middleware's defer-cancel only cancels its own derived ctx,
// never the parent ctx supplied to the handler chain. The earlier
// name ("DoesNotLeakDeadlineToCallerAfterChainEnds") was misleading
// — what's actually being asserted is parent-ctx isolation.
func TestHandlerTimeout_DoesNotCancelParentContext(t *testing.T) {
	parent, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()
	c := &Context{ctx: parent, chain: &chainState{index: -1}}
	c.chain.handlers = []HandlerFunc{
		HandlerTimeout(20 * time.Millisecond),
		func(c *Context) {
			// no-op, return immediately
		},
	}
	c.Next()
	// HandlerTimeout's `defer cancel()` only cancels the derived ctx it
	// installed via SetContext. The parent supplied at acquireContext
	// time must remain unaffected — verify that here.
	select {
	case <-parent.Done():
		t.Fatal("parent context must not be cancelled by HandlerTimeout")
	default:
	}
}

func TestRequireRequestID_ValidPasses(t *testing.T) {
	const id = "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	c := &Context{
		ctx:   context.Background(),
		Msg:   &nats.Msg{Subject: "x", Header: nats.Header{natsutil.RequestIDHeader: []string{id}}},
		chain: &chainState{index: -1},
	}
	var ran bool
	c.chain.handlers = []HandlerFunc{
		RequireRequestID(),
		func(c *Context) { ran = true },
	}
	c.Next()

	require.True(t, ran, "handler must run when request ID is a valid UUID")
	got, ok := c.Get(requestIDKey)
	require.True(t, ok)
	assert.Equal(t, id, got)
	assert.Equal(t, id, natsutil.RequestIDFromContext(c))
}

func TestRequireRequestID_MissingAborts(t *testing.T) {
	c := &Context{
		ctx:   context.Background(),
		Msg:   &nats.Msg{Subject: "x", Header: nats.Header{}},
		chain: &chainState{index: -1},
	}
	var ran bool
	c.chain.handlers = []HandlerFunc{
		RequireRequestID(),
		func(c *Context) { ran = true },
	}
	c.Next()

	assert.False(t, ran, "handler must NOT run when request ID is missing")
	assert.True(t, c.IsAborted())
	_, stamped := c.Get(requestIDKey)
	assert.False(t, stamped, "request ID must not be stamped on the abort path")
}

func TestRequireRequestID_InvalidAborts(t *testing.T) {
	c := &Context{
		ctx:   context.Background(),
		Msg:   &nats.Msg{Subject: "x", Header: nats.Header{natsutil.RequestIDHeader: []string{"not-a-uuid"}}},
		chain: &chainState{index: -1},
	}
	var ran bool
	c.chain.handlers = []HandlerFunc{
		RequireRequestID(),
		func(c *Context) { ran = true },
	}
	c.Next()

	assert.False(t, ran, "handler must NOT run when request ID is malformed")
	assert.True(t, c.IsAborted())
	_, stamped := c.Get(requestIDKey)
	assert.False(t, stamped, "request ID must not be stamped on the abort path")
}

func TestRequireRequestID_NilMsgAborts(t *testing.T) {
	// NewContext-style test context leaves Msg nil; the middleware must abort
	// cleanly (no panic in errnats.Reply) rather than dereference a nil Msg.
	c := &Context{ctx: context.Background(), chain: &chainState{index: -1}}
	var ran bool
	c.chain.handlers = []HandlerFunc{
		RequireRequestID(),
		func(c *Context) { ran = true },
	}
	c.Next()

	assert.False(t, ran, "handler must NOT run when Msg (and thus request ID) is absent")
	assert.True(t, c.IsAborted())
}

// RequestID must admit the inbound X-Debug rung so it propagates onto the
// handler ctx (and thence onto every outbound NewMsg). Honored-emission gating
// is logctx's concern and tested there; here we only verify the wiring.
func TestRequestID_AdmitsDebugRung(t *testing.T) {
	const id = "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	c := &Context{
		ctx: context.Background(),
		Msg: &nats.Msg{Subject: "x", Header: nats.Header{
			natsutil.RequestIDHeader: []string{id},
			natsutil.DebugHeader:     []string{"trace"},
		}},
		chain: &chainState{index: -1},
	}
	var observed natsutil.DebugLevel
	c.chain.handlers = []HandlerFunc{
		RequestID(),
		func(c *Context) { observed = natsutil.DebugLevelFromContext(c) },
	}
	c.Next()

	assert.Equal(t, natsutil.DebugTrace, observed, "inbound X-Debug rung must reach the handler ctx")
}

func TestRequestID_NoDebugHeaderIsOff(t *testing.T) {
	c := &Context{
		ctx:   context.Background(),
		Msg:   &nats.Msg{Subject: "x", Header: nats.Header{}},
		chain: &chainState{index: -1},
	}
	observed := natsutil.DebugTrace // sentinel that must be overwritten
	c.chain.handlers = []HandlerFunc{
		RequestID(),
		func(c *Context) { observed = natsutil.DebugLevelFromContext(c) },
	}
	c.Next()

	assert.Equal(t, natsutil.DebugOff, observed, "no X-Debug header → no rung on ctx")
}
