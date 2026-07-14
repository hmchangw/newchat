package health

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

// serverOptions carries optional behavior toggles for the health server.
type serverOptions struct {
	// pprof mounts the net/http/pprof handlers on the health mux when set.
	pprof bool
}

// registerPprof mounts the standard net/http/pprof handlers on mux. The Index
// handler at /debug/pprof/ also serves the named profiles (heap, goroutine,
// allocs, …), so the explicit handlers below cover only the endpoints that need
// a dedicated func. This is the same handler set tools/loadgen exposes.
func registerPprof(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

// ServeWithPprof behaves like Serve but mounts the net/http/pprof profiling
// endpoints on the same port when pprofEnabled is true. With pprofEnabled false
// it is identical to Serve — profiling is off by default so the
// operator-exposed health port never leaks profiling without an explicit
// opt-in. Intended for short-lived local profiling during load generation.
func ServeWithPprof(addr string, timeout time.Duration, pprofEnabled bool, checks ...Check) (stop func(context.Context) error, err error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("health server listen on %q: %w", addr, err)
	}
	return serveListenerWithOptions(listener, timeout, serverOptions{pprof: pprofEnabled}, checks...), nil
}
