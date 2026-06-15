package logctx

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// --- test doubles ------------------------------------------------------------

type allowAll struct{}

func (allowAll) Allow() bool { return true }

// setLimiter swaps the package limiter for the duration of a test.
//
// Tests in this package mutate the package-global `limiter` (here and via
// Configure), so they MUST NOT call t.Parallel() — concurrent mutation + read
// would trip -race. Keep this package's tests serial.
func setLimiter(t *testing.T, l allower) {
	t.Helper()
	prev := limiter
	limiter = l
	t.Cleanup(func() { limiter = prev })
}

// capture is a base slog.Handler that records every Handle'd record. Its
// Enabled mimics a production JSON handler pinned at INFO.
type capture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *capture) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelInfo }

//nolint:gocritic // slog.Handler mandates the Record value parameter
func (c *capture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}
func (c *capture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *capture) WithGroup(string) slog.Handler      { return c }
func (c *capture) levels() []slog.Level {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]slog.Level, len(c.records))
	for i := range c.records {
		out[i] = c.records[i].Level
	}
	return out
}

func headerFor(rung string) nats.Header {
	if rung == "" {
		return nil
	}
	return nats.Header{natsutil.DebugHeader: []string{rung}}
}

// --- threshold ---------------------------------------------------------------

func TestThreshold_ExhaustiveMapping(t *testing.T) {
	assert.Equal(t, slog.LevelInfo, threshold(natsutil.DebugOff))
	assert.Equal(t, LevelFlow, threshold(natsutil.DebugFlow))
	assert.Equal(t, slog.LevelDebug, threshold(natsutil.DebugBasic))
	assert.Equal(t, LevelTrace, threshold(natsutil.DebugTrace))
}

// The custom levels must straddle the stdlib ones so the cumulative ladder
// off > flow > debug > trace holds in slog's descending space.
func TestThreshold_LevelOrdering(t *testing.T) {
	assert.Greater(t, slog.LevelInfo, LevelFlow)
	assert.Greater(t, LevelFlow, slog.LevelDebug)
	assert.Greater(t, slog.LevelDebug, LevelTrace)
}

// --- RenderLevelNames --------------------------------------------------------

func TestRenderLevelNames(t *testing.T) {
	render := func(lvl slog.Level) string {
		a := RenderLevelNames(nil, slog.Attr{Key: slog.LevelKey, Value: slog.AnyValue(lvl)})
		return a.Value.String()
	}
	assert.Equal(t, "FLOW", render(LevelFlow))
	assert.Equal(t, "TRACE", render(LevelTrace))
	assert.Equal(t, slog.LevelInfo.String(), render(slog.LevelInfo))
	assert.Equal(t, slog.LevelDebug.String(), render(slog.LevelDebug))
}

func TestRenderLevelNames_LeavesNonLevelAttrsAlone(t *testing.T) {
	a := RenderLevelNames(nil, slog.String("msg", "hello"))
	assert.Equal(t, "hello", a.Value.String())
}

// --- Admit -------------------------------------------------------------------

func TestAdmit_HonorsEachRungWhenAllowed(t *testing.T) {
	setLimiter(t, allowAll{})
	cases := []struct {
		rung    string
		wantReq natsutil.DebugLevel
		wantThr slog.Level
	}{
		{"flow", natsutil.DebugFlow, LevelFlow},
		{"debug", natsutil.DebugBasic, slog.LevelDebug},
		{"trace", natsutil.DebugTrace, LevelTrace},
	}
	for _, tc := range cases {
		t.Run(tc.rung, func(t *testing.T) {
			ctx := Admit(context.Background(), headerFor(tc.rung))
			assert.Equal(t, tc.wantReq, natsutil.DebugLevelFromContext(ctx), "requested rung propagates")
			assert.Equal(t, tc.wantThr, honoredThreshold(ctx), "honored threshold set")
		})
	}
}

func TestAdmit_OffHeaderNeitherSet(t *testing.T) {
	setLimiter(t, allowAll{})
	for _, h := range []nats.Header{headerFor(""), headerFor("0"), headerFor("garbage")} {
		ctx := Admit(context.Background(), h)
		assert.Equal(t, natsutil.DebugOff, natsutil.DebugLevelFromContext(ctx))
		assert.Equal(t, slog.LevelInfo, honoredThreshold(ctx), "no honored threshold when off")
	}
}

func TestAdmit_NilHeadersSafe(t *testing.T) {
	setLimiter(t, allowAll{})
	ctx := Admit(context.Background(), nil)
	assert.Equal(t, natsutil.DebugOff, natsutil.DebugLevelFromContext(ctx))
	assert.Equal(t, slog.LevelInfo, honoredThreshold(ctx))
}

// Over budget: the requested rung still propagates downstream, but nothing is
// honored for emission on this instance.
func TestAdmit_OverBudgetPropagatesButSuppresses(t *testing.T) {
	setLimiter(t, denyAll{})
	ctx := Admit(context.Background(), headerFor("trace"))
	assert.Equal(t, natsutil.DebugTrace, natsutil.DebugLevelFromContext(ctx), "intent must still propagate")
	assert.Equal(t, slog.LevelInfo, honoredThreshold(ctx), "but emission suppressed when over budget")
}

func TestAdmit_RateLimitHonorsUpToBurstThenSuppresses(t *testing.T) {
	// Burst of 2, effectively no refill within the test window.
	prev := limiter
	t.Cleanup(func() { limiter = prev })
	Configure(Config{Rate: 0.0001, Burst: 2})

	honored := 0
	for i := 0; i < 5; i++ {
		ctx := Admit(context.Background(), headerFor("debug"))
		if honoredThreshold(ctx) == slog.LevelDebug {
			honored++
		}
		// Intent propagates regardless of the cap.
		assert.Equal(t, natsutil.DebugBasic, natsutil.DebugLevelFromContext(ctx))
	}
	assert.Equal(t, 2, honored, "exactly burst-many messages honored")
}

func TestAdmit_OffRequestDoesNotConsumeTokens(t *testing.T) {
	// A burst of 1; an off request must not spend it.
	lim := rate.NewLimiter(rate.Limit(0.0001), 1)
	setLimiter(t, lim)
	Admit(context.Background(), headerFor("off"))
	ctx := Admit(context.Background(), headerFor("debug"))
	assert.Equal(t, slog.LevelDebug, honoredThreshold(ctx), "off request must not have spent the token")
}

// Default (unconfigured) limiter honors nothing — a service that never calls
// Configure gets zero verbose output even for flagged traffic.
func TestDefaultLimiter_HonorsNothing(t *testing.T) {
	ctx := Admit(context.Background(), headerFor("trace"))
	assert.Equal(t, slog.LevelInfo, honoredThreshold(ctx))
}

// --- Handler -----------------------------------------------------------------

func TestHandler_EmissionByThreshold(t *testing.T) {
	setLimiter(t, allowAll{})
	cases := []struct {
		rung      string
		wantPass  []slog.Level // sub-INFO levels that must be emitted
		wantBlock []slog.Level
	}{
		{"", nil, []slog.Level{LevelFlow, slog.LevelDebug, LevelTrace}},
		{"flow", []slog.Level{LevelFlow}, []slog.Level{slog.LevelDebug, LevelTrace}},
		{"debug", []slog.Level{LevelFlow, slog.LevelDebug}, []slog.Level{LevelTrace}},
		{"trace", []slog.Level{LevelFlow, slog.LevelDebug, LevelTrace}, nil},
	}
	for _, tc := range cases {
		t.Run("rung="+tc.rung, func(t *testing.T) {
			base := &capture{}
			logger := slog.New(NewHandler(base))
			ctx := Admit(context.Background(), headerFor(tc.rung))

			// INFO always passes regardless of rung.
			logger.InfoContext(ctx, "info")
			for _, l := range append(append([]slog.Level{}, tc.wantPass...), tc.wantBlock...) {
				logger.Log(ctx, l, "x")
			}

			got := base.levels()
			assert.Contains(t, got, slog.LevelInfo, "INFO always emitted")
			for _, l := range tc.wantPass {
				assert.Contains(t, got, l, "expected level emitted")
			}
			for _, l := range tc.wantBlock {
				assert.NotContains(t, got, l, "expected level suppressed")
			}
		})
	}
}

// A record with no admitted ctx never emits below INFO, even though the wrapper
// bypasses the base level for admitted requests.
func TestHandler_UnadmittedContextSuppressesSubInfo(t *testing.T) {
	base := &capture{}
	logger := slog.New(NewHandler(base))
	logger.Log(context.Background(), slog.LevelDebug, "x")
	logger.Log(context.Background(), LevelTrace, "x")
	assert.Empty(t, base.levels())
}

func TestHandler_WithAttrsAndGroupPreserveGating(t *testing.T) {
	setLimiter(t, allowAll{})
	base := &capture{}
	var h slog.Handler = NewHandler(base)
	h = h.WithAttrs([]slog.Attr{slog.String("svc", "x")})
	h = h.WithGroup("g")
	_, ok := h.(*Handler)
	require.True(t, ok, "WithAttrs/WithGroup must keep the wrapper type")

	logger := slog.New(h)
	admitted := Admit(context.Background(), headerFor("flow"))
	logger.Log(admitted, LevelFlow, "kept")
	logger.Log(context.Background(), LevelFlow, "dropped") // unadmitted
	assert.Equal(t, []slog.Level{LevelFlow}, base.levels())
}

// --- payload capture --------------------------------------------------------

func TestAdmit_StampsPayloadRequestFromHeader(t *testing.T) {
	setLimiter(t, allowAll{})
	ctx := Admit(context.Background(), nats.Header{natsutil.DebugPayloadHeader: []string{"1"}})
	assert.True(t, natsutil.PayloadCaptureFromContext(ctx), "X-Debug-Payload stamps the ctx")

	// payload capture works even with no X-Debug rung present
	assert.True(t, natsutil.PayloadCaptureFromContext(
		Admit(context.Background(), nats.Header{natsutil.DebugPayloadHeader: []string{"true"}})))

	// absent → not stamped
	assert.False(t, natsutil.PayloadCaptureFromContext(Admit(context.Background(), headerFor("trace"))))
}

func capturedPayloads(rec *capture) []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var out []string
	for i := range rec.records {
		if rec.records[i].Message != "debug payload" {
			continue
		}
		rec.records[i].Attrs(func(a slog.Attr) bool {
			if a.Key == "payload" {
				out = append(out, a.Value.String())
			}
			return true
		})
	}
	return out
}

func TestCapturePayload_GatedByConfigAndRequest(t *testing.T) {
	requested := natsutil.WithPayloadCapture(context.Background())
	body := []byte(`{"q":"LoadHistory"}`)

	install := func() *capture {
		rec := &capture{}
		prev := slog.Default()
		slog.SetDefault(slog.New(rec))
		t.Cleanup(func() { slog.SetDefault(prev) })
		return rec
	}
	restorePayloads := func() { capturePayloads = false }

	t.Run("flag OFF + requested → silent (prod invariant)", func(t *testing.T) {
		rec := install()
		Configure(Config{Rate: 1e6, Burst: 1 << 20, Payloads: false})
		t.Cleanup(restorePayloads)
		CapturePayload(requested, "request", "chat.x", body)
		assert.Empty(t, capturedPayloads(rec), "no body when DEBUG_LOG_PAYLOADS is off, even with the header")
	})

	t.Run("flag ON + requested → body emitted", func(t *testing.T) {
		rec := install()
		Configure(Config{Rate: 1e6, Burst: 1 << 20, Payloads: true})
		t.Cleanup(restorePayloads)
		CapturePayload(requested, "request", "chat.x", body)
		assert.Equal(t, []string{string(body)}, capturedPayloads(rec))
	})

	t.Run("flag ON + NOT requested → silent", func(t *testing.T) {
		rec := install()
		Configure(Config{Rate: 1e6, Burst: 1 << 20, Payloads: true})
		t.Cleanup(restorePayloads)
		CapturePayload(context.Background(), "request", "chat.x", body)
		assert.Empty(t, capturedPayloads(rec))
	})
}

func TestShouldCapture(t *testing.T) {
	requested := natsutil.WithPayloadCapture(context.Background())
	t.Cleanup(func() { capturePayloads = false })

	capturePayloads = false
	assert.False(t, ShouldCapture(requested))
	capturePayloads = true
	assert.True(t, ShouldCapture(requested))
	assert.False(t, ShouldCapture(context.Background()), "not requested → false even when enabled")
}

// Enabled is the caller-side predicate that lets a hot path skip building
// expensive breadcrumb args (e.g. msg.Metadata()) when the record would be
// dropped anyway. It must mirror the Handler's admission decision.
func TestEnabled(t *testing.T) {
	// Unadmitted: only INFO+ is enabled; FLOW/TRACE are not.
	assert.False(t, Enabled(context.Background(), LevelFlow))
	assert.False(t, Enabled(context.Background(), LevelTrace))
	assert.True(t, Enabled(context.Background(), slog.LevelInfo))

	// Admitted at flow: FLOW passes, TRACE (deeper) does not.
	flow := withHonoredThreshold(context.Background(), LevelFlow)
	assert.True(t, Enabled(flow, LevelFlow))
	assert.False(t, Enabled(flow, LevelTrace))

	// Admitted at trace: both FLOW and TRACE pass.
	trace := withHonoredThreshold(context.Background(), LevelTrace)
	assert.True(t, Enabled(trace, LevelFlow))
	assert.True(t, Enabled(trace, LevelTrace))
}

// Handle is part of the public slog.Handler contract and may be invoked directly
// (handler fan-out, slog.NewLogLogger, a wrapping handler). It must mirror
// Enabled's sub-INFO gate so a FLOW/TRACE record never reaches the base handler
// for an unadmitted request, even when Enabled was bypassed.
func TestHandler_HandleGatesSubINFOWhenCalledDirectly(t *testing.T) {
	base := &capture{}
	h := NewHandler(base)
	rec := func(l slog.Level) slog.Record { return slog.NewRecord(time.Now(), l, "x", 0) }

	// Unadmitted ctx: sub-INFO must be dropped by Handle itself.
	require.NoError(t, h.Handle(context.Background(), rec(slog.LevelDebug)))
	require.NoError(t, h.Handle(context.Background(), rec(LevelTrace)))
	assert.Empty(t, base.levels(), "sub-INFO must not reach base when unadmitted")

	// INFO+ always delegates; admitted sub-INFO at/below threshold passes.
	require.NoError(t, h.Handle(context.Background(), rec(slog.LevelInfo)))
	require.NoError(t, h.Handle(withHonoredThreshold(context.Background(), LevelTrace), rec(LevelTrace)))
	assert.Equal(t, []slog.Level{slog.LevelInfo, LevelTrace}, base.levels())
}
