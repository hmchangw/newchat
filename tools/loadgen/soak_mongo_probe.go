package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

const (
	soakMongoProbeInterval = 5 * time.Second
	soakMongoProbeTimeout  = 2 * time.Second
)

type soakMongoPinger interface {
	Ping(context.Context, *readpref.ReadPref) error
}

type soakMongoProbeOutcome string

const (
	soakMongoProbeSuccess soakMongoProbeOutcome = "success"
	soakMongoProbeError   soakMongoProbeOutcome = "error"
)

type soakMongoProbeSnapshot struct {
	Up          bool
	CompletedAt time.Time
}

type soakMongoProbe struct {
	pinger  soakMongoPinger
	timeout time.Duration
	now     func() time.Time
	metrics *Metrics
	state   atomic.Pointer[soakMongoProbeSnapshot]
}

func newSoakMongoProbe(
	pinger soakMongoPinger,
	timeout time.Duration,
	now func() time.Time,
	metrics *Metrics,
) *soakMongoProbe {
	if timeout <= 0 {
		timeout = soakMongoProbeTimeout
	}
	if now == nil {
		now = time.Now
	}
	probe := &soakMongoProbe{pinger: pinger, timeout: timeout, now: now, metrics: metrics}
	probe.state.Store(&soakMongoProbeSnapshot{})
	return probe
}

func (p *soakMongoProbe) Probe(ctx context.Context) error {
	if p == nil || p.pinger == nil {
		return fmt.Errorf("mongo probe is not configured")
	}
	probeCtx, cancel := context.WithTimeout(ctx, p.timeout)
	err := p.pinger.Ping(probeCtx, readpref.Primary())
	cancel()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("ping Mongo primary: %w", ctxErr)
	}
	previous := p.Snapshot()
	completedAt := p.now().UTC()
	p.state.Store(&soakMongoProbeSnapshot{Up: err == nil, CompletedAt: completedAt})
	if p.metrics != nil {
		outcome := soakMongoProbeSuccess
		if err != nil {
			outcome = soakMongoProbeError
		}
		p.metrics.MongoProbeAttempts.WithLabelValues(string(outcome)).Inc()
	}
	if err != nil {
		if previous.CompletedAt.IsZero() || previous.Up {
			slog.Warn("Mongo primary probe entered degraded state", "error", err)
		}
		return fmt.Errorf("ping Mongo primary: %w", err)
	}
	if !previous.CompletedAt.IsZero() && !previous.Up {
		slog.Info("Mongo primary probe recovered")
	}
	return nil
}

func (p *soakMongoProbe) Snapshot() soakMongoProbeSnapshot {
	if p == nil {
		return soakMongoProbeSnapshot{}
	}
	snapshot := p.state.Load()
	if snapshot == nil {
		return soakMongoProbeSnapshot{}
	}
	return *snapshot
}

func runSoakMongoProbe(
	ctx context.Context,
	probe *soakMongoProbe,
	ticks <-chan time.Time,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-ticks:
			if !ok {
				return nil
			}
			_ = probe.Probe(ctx)
		}
	}
}

func startSoakMongoProbe(
	ctx context.Context,
	pinger soakMongoPinger,
	metrics *Metrics,
	now func() time.Time,
) func() {
	probe := newSoakMongoProbe(pinger, soakMongoProbeTimeout, now, metrics)
	metrics.SetMongoProbeSource(probe.Snapshot)
	probeCtx, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(soakMongoProbeInterval)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ticker.Stop()
		_ = probe.Probe(probeCtx)
		_ = runSoakMongoProbe(probeCtx, probe, ticker.C)
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
}

type soakHeartbeatSnapshot struct {
	Degraded    bool
	LastSuccess time.Time
}

type soakHeartbeatStatus struct {
	metrics *Metrics
	state   atomic.Pointer[soakHeartbeatSnapshot]
}

func newSoakHeartbeatStatus(metrics *Metrics) *soakHeartbeatStatus {
	status := &soakHeartbeatStatus{metrics: metrics}
	status.state.Store(&soakHeartbeatSnapshot{})
	if metrics != nil {
		metrics.SetSoakHeartbeatSource(status.Snapshot)
	}
	return status
}

func (s *soakHeartbeatStatus) RecordHeartbeatAttempt(
	outcome soakHeartbeatOutcome,
	degraded bool,
	completedAt time.Time,
) {
	if s == nil {
		return
	}
	previous := s.Snapshot()
	next := soakHeartbeatSnapshot{
		Degraded: degraded, LastSuccess: previous.LastSuccess,
	}
	if outcome == soakHeartbeatSuccess {
		next.LastSuccess = completedAt.UTC()
	}
	s.state.Store(&next)
	if s.metrics != nil {
		s.metrics.SoakHeartbeatAttempts.WithLabelValues(string(outcome)).Inc()
	}
}

func (s *soakHeartbeatStatus) Snapshot() soakHeartbeatSnapshot {
	if s == nil {
		return soakHeartbeatSnapshot{}
	}
	snapshot := s.state.Load()
	if snapshot == nil {
		return soakHeartbeatSnapshot{}
	}
	return *snapshot
}
