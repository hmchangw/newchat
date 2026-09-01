package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ConsumerSampler polls a single durable consumer's info every interval and
// records min/peak/final samples. Start with Run(ctx); stop by cancelling ctx.
type ConsumerSampler struct {
	mu       sync.Mutex
	js       jetstream.JetStream
	stream   string
	durable  string
	metrics  *Metrics
	interval time.Duration
	sample   func(context.Context) (*jetstream.ConsumerInfo, error)
	now      func() time.Time

	hasSample        bool
	minPending       uint64
	peakPending      uint64
	finalPending     uint64
	peakAckPending   uint64
	finalRedelivered uint64
	baselinePending  uint64
	sampleErrors     uint64
	invalidate       func(string)
}

type consumerSamplerOption func(*ConsumerSampler)

func withConsumerSamplerInvalidation(invalidate func(string)) consumerSamplerOption {
	return func(sampler *ConsumerSampler) { sampler.invalidate = invalidate }
}

func withConsumerSamplerNow(now func() time.Time) consumerSamplerOption {
	return func(sampler *ConsumerSampler) { sampler.now = now }
}

// consumerDurableRegistry allowlists the durables that may appear as a
// Prometheus label. A typo has to fail at construction rather than mint an
// unbounded label, so every sampled consumer is named here explicitly.
var consumerDurableRegistry = map[string]struct{}{
	"room-worker": {}, "message-gatekeeper": {}, "message-worker": {},
	"broadcast-worker": {}, "notification-worker": {},
	"notification-worker-room-event-invalidate": {},
	"message-sync":   {},
	"spotlight-sync": {},
	"user-room-sync": {},
}

// NewConsumerSampler constructs a sampler after validating its bounded labels.
func NewConsumerSampler(
	js jetstream.JetStream,
	stream,
	durable string,
	m *Metrics,
	interval time.Duration,
	options ...consumerSamplerOption,
) (*ConsumerSampler, error) {
	if !validConsumerStream(stream) {
		return nil, fmt.Errorf("unsupported consumer stream %q", stream)
	}
	if _, known := consumerDurableRegistry[durable]; !known {
		return nil, fmt.Errorf("unsupported consumer durable %q", durable)
	}
	if interval <= 0 {
		return nil, errors.New("consumer sample interval must be greater than zero")
	}
	sampler := &ConsumerSampler{
		js: js, stream: stream, durable: durable, metrics: m, interval: interval,
		now: time.Now,
	}
	for _, option := range options {
		option(sampler)
	}
	return sampler, nil
}

func validConsumerStream(name string) bool {
	for _, prefix := range []string{
		"MESSAGES-CANONICAL-", "MESSAGES-", "ROOMS-", "INBOX-",
	} {
		if site, found := strings.CutPrefix(name, prefix); found {
			return failureRunIDPattern.MatchString(site)
		}
	}
	return false
}

// Run polls ConsumerInfo until ctx is cancelled.
func (s *ConsumerSampler) Run(ctx context.Context) {
	s.sampleOnce(ctx)
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.sampleOnce(ctx)
		}
	}
}

func (s *ConsumerSampler) sampleOnce(ctx context.Context) {
	if s.sample != nil {
		info, err := s.sample(ctx)
		if err != nil {
			s.recordSampleError("lookup", err)
			return
		}
		s.recordInfo(info)
		return
	}
	cons, err := s.js.Consumer(ctx, s.stream, s.durable)
	if err != nil {
		s.recordSampleError("lookup", err)
		return
	}
	info, err := cons.Info(ctx)
	if err != nil {
		s.recordSampleError("info", err)
		return
	}
	s.recordInfo(info)
}

func (s *ConsumerSampler) recordSampleError(reason string, err error) {
	s.mu.Lock()
	s.sampleErrors++
	invalidate := s.invalidate
	s.mu.Unlock()
	if s.metrics != nil {
		s.metrics.ConsumerSampleErrors.WithLabelValues(s.stream, s.durable, reason).Inc()
		s.metrics.ConsumerUp.WithLabelValues(s.stream, s.durable).Set(0)
		s.metrics.ConsumerAckFloorStall.DeleteLabelValues(s.stream, s.durable)
	}
	if invalidate != nil {
		invalidate(reason)
	}
	slog.Warn("consumer sample failed", "stream", s.stream, "durable", s.durable, "reason", reason, "error", err)
}

func (s *ConsumerSampler) recordInfo(info *jetstream.ConsumerInfo) {
	if info == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := info.NumPending
	ack := uint64(info.NumAckPending)
	redel := uint64(info.NumRedelivered)

	if s.metrics != nil {
		s.metrics.ConsumerPending.WithLabelValues(s.stream, s.durable).Set(float64(pending))
		s.metrics.ConsumerAckPending.WithLabelValues(s.stream, s.durable).Set(float64(ack))
		s.metrics.ConsumerRedelivered.WithLabelValues(s.stream, s.durable).Set(float64(redel))
		s.metrics.ConsumerUp.WithLabelValues(s.stream, s.durable).Set(1)
		s.metrics.ConsumerDelivered.WithLabelValues(s.stream, s.durable).Set(float64(info.Delivered.Consumer))
		s.metrics.ConsumerAckFloor.WithLabelValues(s.stream, s.durable).Set(float64(info.AckFloor.Consumer))
		s.metrics.ConsumerStreamFloor.WithLabelValues(s.stream, s.durable).Set(float64(info.AckFloor.Stream))
		s.metrics.ConsumerMaxDeliver.WithLabelValues(s.stream, s.durable).Set(float64(info.Config.MaxDeliver))
		s.metrics.ConsumerAckWait.WithLabelValues(s.stream, s.durable).Set(info.Config.AckWait.Seconds())
		if pending > 0 && info.AckFloor.Last != nil {
			stall := max(0, s.now().UTC().Sub(info.AckFloor.Last.UTC()).Seconds())
			s.metrics.ConsumerAckFloorStall.WithLabelValues(s.stream, s.durable).Set(stall)
		} else {
			s.metrics.ConsumerAckFloorStall.DeleteLabelValues(s.stream, s.durable)
		}
		if info.Delivered.Last != nil {
			s.metrics.ConsumerLastActive.WithLabelValues(s.stream, s.durable).Set(float64(info.Delivered.Last.UTC().Unix()))
		}
	}

	if !s.hasSample {
		s.hasSample = true
		s.baselinePending = pending
		s.minPending = pending
		s.peakPending = pending
		s.peakAckPending = ack
	} else {
		if pending < s.minPending {
			s.minPending = pending
		}
		if pending > s.peakPending {
			s.peakPending = pending
		}
		if ack > s.peakAckPending {
			s.peakAckPending = ack
		}
	}
	s.finalPending = pending
	s.finalRedelivered = redel
}

// Snapshot returns a concurrency-safe ConsumerStat from what has been observed so far.
func (s *ConsumerSampler) Snapshot() ConsumerStat {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ConsumerStat{
		Stream:          s.stream,
		Durable:         s.durable,
		MinPending:      s.minPending,
		PeakPending:     s.peakPending,
		FinalPending:    s.finalPending,
		PeakAckPending:  s.peakAckPending,
		Redelivered:     s.finalRedelivered,
		BaselinePending: s.baselinePending,
		HasSample:       s.hasSample,
		SampleErrors:    s.sampleErrors,
	}
}
