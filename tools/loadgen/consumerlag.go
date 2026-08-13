package main

import (
	"context"
	"log/slog"
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

	hasSample        bool
	minPending       uint64
	peakPending      uint64
	finalPending     uint64
	peakAckPending   uint64
	finalRedelivered uint64
}

// NewConsumerSampler constructs a sampler.
func NewConsumerSampler(js jetstream.JetStream, stream, durable string, m *Metrics, interval time.Duration) *ConsumerSampler {
	return &ConsumerSampler{js: js, stream: stream, durable: durable, metrics: m, interval: interval}
}

// Run polls ConsumerInfo until ctx is cancelled.
func (s *ConsumerSampler) Run(ctx context.Context) {
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
	if s.metrics != nil {
		s.metrics.ConsumerSampleErrors.WithLabelValues(s.stream, s.durable, reason).Inc()
		s.metrics.ConsumerUp.WithLabelValues(s.stream, s.durable).Set(0)
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

	s.metrics.ConsumerPending.WithLabelValues(s.stream, s.durable).Set(float64(pending))
	s.metrics.ConsumerAckPending.WithLabelValues(s.stream, s.durable).Set(float64(ack))
	s.metrics.ConsumerRedelivered.WithLabelValues(s.stream, s.durable).Set(float64(redel))
	s.metrics.ConsumerUp.WithLabelValues(s.stream, s.durable).Set(1)
	s.metrics.ConsumerDelivered.WithLabelValues(s.stream, s.durable).Set(float64(info.Delivered.Consumer))
	s.metrics.ConsumerAckFloor.WithLabelValues(s.stream, s.durable).Set(float64(info.AckFloor.Consumer))
	s.metrics.ConsumerStreamFloor.WithLabelValues(s.stream, s.durable).Set(float64(info.AckFloor.Stream))
	s.metrics.ConsumerMaxDeliver.WithLabelValues(s.stream, s.durable).Set(float64(info.Config.MaxDeliver))
	s.metrics.ConsumerAckWait.WithLabelValues(s.stream, s.durable).Set(info.Config.AckWait.Seconds())
	if info.Delivered.Last != nil {
		s.metrics.ConsumerLastActive.WithLabelValues(s.stream, s.durable).Set(float64(info.Delivered.Last.UTC().Unix()))
	}

	if !s.hasSample {
		s.hasSample = true
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

// Snapshot returns a ConsumerStat from what has been observed so far.
// Must only be called after Run has returned (i.e., after the context
// passed to Run has been cancelled and its goroutine has exited);
// concurrent calls to Snapshot while Run is still ticking are unsafe.
func (s *ConsumerSampler) Snapshot() ConsumerStat {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ConsumerStat{
		Stream:         s.stream,
		Durable:        s.durable,
		MinPending:     s.minPending,
		PeakPending:    s.peakPending,
		FinalPending:   s.finalPending,
		PeakAckPending: s.peakAckPending,
		Redelivered:    s.finalRedelivered,
	}
}
