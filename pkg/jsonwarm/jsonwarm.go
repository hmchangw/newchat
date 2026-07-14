// Package jsonwarm pre-compiles sonic's JIT-compiled JSON codecs at startup.
package jsonwarm

import (
	"log/slog"
	"reflect"

	"github.com/bytedance/sonic"
)

// Pretouch warms sonic's JIT-compiled encoder/decoder for each type so the first
// marshal/unmarshal after boot doesn't pay the compile cost (and to avoid
// first-hit JIT latency spikes). Compilation is recursive over nested types.
// Failures are non-fatal — a warm-up miss only means a cold first hit.
func Pretouch(types ...reflect.Type) {
	for _, t := range types {
		if err := sonic.Pretouch(t); err != nil {
			slog.Warn("sonic pretouch failed", "type", t.String(), "error", err)
		}
	}
}
