package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	runtimepprof "runtime/pprof"
	"sort"
	"time"
)

// startSoakPProfServer serves the profiling endpoints on their own listener,
// separate from the metrics endpoint Prometheus scrapes, so turning profiling
// on never widens what the scrape target exposes. An empty address leaves it
// off. The listener is bound before returning so the caller sees the resolved
// address (port 0 in tests) rather than racing the goroutine.
func startSoakPProfServer(addr string) *http.Server {
	if addr == "" {
		return nil
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("Cassandra soak pprof listener", "addr", addr, "error", err)
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", httppprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)
	server := &http.Server{
		Addr:              listener.Addr().String(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := server.Serve(listener); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			slog.Error("Cassandra soak pprof server", "error", err)
		}
	}()
	slog.Info("Cassandra soak pprof server listening", "addr", server.Addr)
	return server
}

type soakHeapProfileConfig struct {
	Dir  string
	Keep int
}

// soakHeapProfiler writes heap profiles to disk on a timer. A pod being
// OOM-killed cannot be port-forwarded in time, so the profile has to already be
// on the volume when the process dies; Keep bounds how many survive so a long
// run cannot fill it.
type soakHeapProfiler struct {
	dir      string
	keep     int
	sequence int
}

func newSoakHeapProfiler(cfg soakHeapProfileConfig) (*soakHeapProfiler, error) {
	if cfg.Dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("create soak heap profile directory: %w", err)
	}
	return &soakHeapProfiler{dir: cfg.Dir, keep: max(1, cfg.Keep)}, nil
}

// Run writes one profile per tick and returns when the ticks stop or the
// context ends. Write failures are logged, never fatal: losing a profile must
// not take the run down.
func (p *soakHeapProfiler) Run(ctx context.Context, ticks <-chan time.Time) {
	if p == nil || ticks == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ticks:
			if !ok {
				return
			}
			if err := p.WriteProfile(); err != nil {
				slog.Error("write Cassandra soak heap profile", "error", err)
			}
		}
	}
}

// WriteProfile forces a GC first so the profile reports live memory rather than
// whatever the allocator happened to be holding.
func (p *soakHeapProfiler) WriteProfile() error {
	if p == nil {
		return nil
	}
	runtime.GC()
	p.sequence++
	path := filepath.Join(p.dir, fmt.Sprintf("heap-%06d.pprof", p.sequence))
	// #nosec G304 -- the path is built from an operator-configured directory
	// and a counter, never from request data.
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create soak heap profile %s: %w", path, err)
	}
	if err := runtimepprof.Lookup("heap").WriteTo(file, 0); err != nil {
		_ = file.Close()
		return fmt.Errorf("write soak heap profile %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close soak heap profile %s: %w", path, err)
	}
	return p.prune()
}

// prune removes the oldest profiles beyond keep. Names are zero-padded and
// monotonic, so lexical order is age order.
func (p *soakHeapProfiler) prune() error {
	matches, err := filepath.Glob(filepath.Join(p.dir, "heap-*.pprof"))
	if err != nil {
		return fmt.Errorf("list soak heap profiles: %w", err)
	}
	if len(matches) <= p.keep {
		return nil
	}
	sort.Strings(matches)
	for _, stale := range matches[:len(matches)-p.keep] {
		if err := os.Remove(stale); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale soak heap profile %s: %w", stale, err)
		}
	}
	return nil
}
