// Package health serves Kubernetes-style liveness and readiness probes over
// HTTP. Liveness reports only that the process is running; readiness runs a
// set of dependency probes so traffic is routed away while a dependency is
// unreachable.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	statusOK       = "ok"
	statusNotReady = "not ready"
)

// Probe reports whether one dependency is healthy. It must respect ctx
// cancellation so a hung dependency cannot stall the readiness response.
type Probe func(ctx context.Context) error

// Check pairs a dependency name with its probe.
type Check struct {
	Name  string
	Probe Probe
}

// response is the JSON body returned by both probes.
type response struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// LivenessHandler reports process liveness. It always returns 200 — a running
// process is live regardless of dependency state, so liveness must not probe
// dependencies (a transient outage should not trigger a pod restart).
func LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, response{Status: statusOK})
	}
}

// ReadinessHandler reports whether all dependencies are reachable. Probes run
// concurrently under timeout; any failure yields 503 so traffic is routed away
// until the dependency recovers.
func ReadinessHandler(timeout time.Duration, checks ...Check) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		results, ok := runChecks(ctx, checks)
		body := response{Status: statusOK, Checks: results}
		status := http.StatusOK
		if !ok {
			body.Status = statusNotReady
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, body)
	}
}

// runChecks runs every probe concurrently and returns each probe's result keyed
// by name plus whether all passed. A nil map is returned when there are no
// checks so the readiness body omits an empty object.
func runChecks(ctx context.Context, checks []Check) (map[string]string, bool) {
	if len(checks) == 0 {
		return nil, true
	}

	results := make(map[string]string, len(checks))
	allOK := true
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, c := range checks {
		wg.Add(1)
		go func(c Check) {
			defer wg.Done()
			err := probeWithContext(ctx, c.Probe)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				results[c.Name] = err.Error()
				allOK = false
				return
			}
			results[c.Name] = statusOK
		}(c)
	}
	wg.Wait()

	return results, allOK
}

// probeWithContext runs probe but returns as soon as ctx is done, so a probe
// that ignores cancellation cannot hold the readiness response open. The probe
// goroutine still finishes on its own; the buffered channel keeps it from
// leaking on its send.
func probeWithContext(ctx context.Context, probe Probe) error {
	done := make(chan error, 1)
	go func() { done <- probe(ctx) }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Register mounts /healthz (liveness) and /readyz (readiness) on mux, for
// services that already run their own HTTP server.
func Register(mux *http.ServeMux, timeout time.Duration, checks ...Check) {
	mux.Handle("/healthz", LivenessHandler())
	mux.Handle("/readyz", ReadinessHandler(timeout, checks...))
}

// NewServer builds a standalone health server bound to addr with hardened
// timeouts, for the NATS worker services that have no HTTP server of their own.
// The timeouts guard the operator-exposed port against hung scrapers tying up a
// goroutine indefinitely.
func NewServer(addr string, timeout time.Duration, checks ...Check) *http.Server {
	srv := newServer(timeout, serverOptions{}, checks...)
	srv.Addr = addr
	return srv
}

// newServer builds the health http.Server with hardened timeouts. The timeouts
// guard the operator-exposed port against hung scrapers tying up a goroutine
// indefinitely. When opts.pprof is set the standard net/http/pprof handlers are
// mounted alongside the probe endpoints on the same mux.
func newServer(timeout time.Duration, opts serverOptions, checks ...Check) *http.Server {
	mux := http.NewServeMux()
	Register(mux, timeout, checks...)
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	if opts.pprof {
		registerPprof(mux)
		// CPU and trace profiles stream the response for a client-chosen
		// duration that routinely exceeds the hardened write timeout
		// (e.g. /debug/pprof/profile?seconds=30), so the server would cut the
		// response off mid-profile and the capture would fail. pprof is a
		// dev/load-test-only opt-in, so drop the write timeout when it's mounted.
		srv.WriteTimeout = 0
	}
	return srv
}

// Serve binds addr and serves the health endpoints in a background goroutine.
// It binds synchronously so a port conflict fails startup loudly rather than
// surfacing in a goroutine while the service runs on with no probes. The
// returned stop func gracefully shuts the server down and is meant to be
// registered with shutdown.Wait.
func Serve(addr string, timeout time.Duration, checks ...Check) (stop func(context.Context) error, err error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("health server listen on %q: %w", addr, err)
	}
	return ServeListener(listener, timeout, checks...), nil
}

// ServeListener serves the health endpoints on an already-bound listener. It
// takes ownership of the listener; the returned stop func closes it via the
// server's graceful Shutdown.
func ServeListener(listener net.Listener, timeout time.Duration, checks ...Check) func(context.Context) error {
	return serveListenerWithOptions(listener, timeout, serverOptions{}, checks...)
}

// serveListenerWithOptions serves the health endpoints on an already-bound
// listener with the given server options. It takes ownership of the listener;
// the returned stop func closes it via the server's graceful Shutdown.
func serveListenerWithOptions(listener net.Listener, timeout time.Duration, opts serverOptions, checks ...Check) func(context.Context) error {
	srv := newServer(timeout, opts, checks...)
	go func() {
		slog.Info("health server listening", "addr", listener.Addr().String())
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("health server failed", "error", err)
		}
	}()
	return srv.Shutdown
}

func writeJSON(w http.ResponseWriter, status int, body response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// Probe responses are tiny; an encode failure means the client hung up.
		slog.Debug("health: encode response failed", "error", err)
	}
}
