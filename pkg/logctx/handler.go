package logctx

import (
	"context"
	"io"
	"log/slog"
)

// SetupDefault installs, as the process-wide slog default, a JSON logger whose
// records are wrapped for per-request X-Debug emission — FLOW/TRACE render by
// name, and sub-INFO records surface only for admitted requests. Call once at
// startup, before parsing config. Centralizes the NewHandler+RenderLevelNames
// pairing so a service can't wire one without the other.
func SetupDefault(w io.Writer) {
	base := slog.NewJSONHandler(w, &slog.HandlerOptions{ReplaceAttr: RenderLevelNames})
	slog.SetDefault(slog.New(NewHandler(base)))
}

type honoredKey int

const honoredThresholdKey honoredKey = 0

// withHonoredThreshold records the minimum slog level this message may emit.
func withHonoredThreshold(ctx context.Context, l slog.Level) context.Context {
	return context.WithValue(ctx, honoredThresholdKey, l)
}

// honoredThreshold returns the admitted minimum level, or slog.LevelInfo when
// the request was not admitted for verbose logging.
func honoredThreshold(ctx context.Context) slog.Level {
	if l, ok := ctx.Value(honoredThresholdKey).(slog.Level); ok {
		return l
	}
	return slog.LevelInfo
}

// Enabled reports whether a record at level would be emitted for ctx. Mirrors
// the Handler's admission decision so a hot path can guard the construction of
// expensive breadcrumb args — e.g. `if logctx.Enabled(ctx, logctx.LevelFlow) {
// ... msg.Metadata() ... }` — and pay nothing when the rung is suppressed.
// slog.Log evaluates its variadic args in the caller before Enabled runs, so
// this predicate is the only way to skip that work entirely.
func Enabled(ctx context.Context, level slog.Level) bool {
	return level >= honoredThreshold(ctx)
}

// Handler wraps a base slog.Handler. Records at/above INFO pass through the base
// unchanged; sub-INFO records (FLOW, DEBUG, TRACE) pass ONLY when ctx was
// admitted to a threshold at or below the record's level — letting a single
// flagged request emit verbose lines even while the base handler sits at INFO.
type Handler struct {
	base slog.Handler
}

// NewHandler wraps base with per-request verbose admission.
func NewHandler(base slog.Handler) *Handler {
	return &Handler{base: base}
}

func (h *Handler) Enabled(ctx context.Context, lvl slog.Level) bool {
	if lvl >= slog.LevelInfo {
		return h.base.Enabled(ctx, lvl)
	}
	return lvl >= honoredThreshold(ctx)
}

//nolint:gocritic // slog.Handler mandates the Record value parameter
func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	// Defensive: callers normally gate via Enabled first, but Handle is part of
	// the public slog.Handler contract and may be invoked directly. Mirror
	// Enabled's sub-INFO gate so a FLOW/TRACE record never reaches the base
	// handler for an unadmitted request.
	if r.Level < slog.LevelInfo && r.Level < honoredThreshold(ctx) {
		return nil
	}
	return h.base.Handle(ctx, r)
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &Handler{base: h.base.WithAttrs(attrs)}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{base: h.base.WithGroup(name)}
}
