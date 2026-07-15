package obs

import (
	"log/slog"
	"os"
)

// init installs a JSON slog default at package load, before any importing
// service's main() runs. Without it, a fatal logged *before* Init() succeeds —
// a config-parse failure, or Init()'s own error — would go through Go's default
// text handler, violating the JSON logging discipline on exactly the
// startup-failure lines operators grep first. Init() replaces this with the
// SDK's trace-correlated logger on success; until then, at least it is JSON.
func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
}
