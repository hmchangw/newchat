// Package logctx scopes verbose-logging emission to individual requests. It pairs
// with pkg/natsutil's X-Debug propagation: natsutil carries the requested rung
// across services; logctx decides — once per message, under a per-instance rate
// cap — whether to honor it, and a context-aware slog.Handler emits the matching
// records. (Named for context-scoped logging; not a utils/helpers bucket.)
package logctx

import (
	"log/slog"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// Two custom levels straddle the stdlib ones (DEBUG=-4, INFO=0), giving the
// cumulative ladder off(INFO) > flow > debug > trace in slog's descending space.
const (
	LevelFlow  = slog.Level(-2) // cross-service path + timing breadcrumbs
	LevelTrace = slog.Level(-8) // per-item / per-recipient edges
)

// threshold is the single bridge from the ascending wire rung to the descending
// slog threshold. Nothing else open-codes this inversion.
func threshold(l natsutil.DebugLevel) slog.Level {
	switch l {
	case natsutil.DebugFlow:
		return LevelFlow
	case natsutil.DebugBasic:
		return slog.LevelDebug
	case natsutil.DebugTrace:
		return LevelTrace
	default: // DebugOff and any out-of-range value
		return slog.LevelInfo
	}
}

// RenderLevelNames is a slog HandlerOptions.ReplaceAttr that prints the custom
// levels as "FLOW"/"TRACE" (slog would otherwise render them "DEBUG-2"/"DEBUG-4").
func RenderLevelNames(_ []string, a slog.Attr) slog.Attr {
	if a.Key != slog.LevelKey {
		return a
	}
	if lvl, ok := a.Value.Any().(slog.Level); ok {
		switch lvl {
		case LevelFlow:
			a.Value = slog.StringValue("FLOW")
		case LevelTrace:
			a.Value = slog.StringValue("TRACE")
		default:
			// stdlib levels keep slog's own rendering
		}
	}
	return a
}
