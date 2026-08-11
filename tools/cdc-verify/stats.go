package main

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// StreamStats is one stats tick, broadcast to the UI.
type StreamStats struct {
	Stream      string            `json:"stream"`
	Msgs        uint64            `json:"msgs"`
	Bytes       uint64            `json:"bytes"`
	FirstSeq    uint64            `json:"firstSeq"`
	LastSeq     uint64            `json:"lastSeq"`
	PerSubject  map[string]uint64 `json:"perSubject"` // full subject -> count
	RatePerSec  float64           `json:"ratePerSec"` // delta(LastSeq)/delta(t), sliding window
	Consumers   []ConsumerLag     `json:"consumers"`
	WatcherLive bool              `json:"watcherLive"`
	TakenAtMs   int64             `json:"takenAtMs"`
	Error       string            `json:"error,omitempty"` // poll failure, shown in UI
}

type ConsumerLag struct {
	Name       string `json:"name"`
	NumPending uint64 `json:"numPending"`
	AckPending int    `json:"ackPending"`
	Error      string `json:"error,omitempty"`
}

// streamInfoFn abstracts the JetStream calls for unit tests.
type streamInfoFn func(ctx context.Context) (*jetstream.StreamInfo, error)
type consumerInfoFn func(ctx context.Context, name string) (*jetstream.ConsumerInfo, error)

type seqSample struct {
	t   time.Time
	seq uint64
}

type statsPoller struct {
	stream         string
	si             streamInfoFn
	ci             consumerInfoFn
	trackConsumers []string
	interval       time.Duration
	watcherLive    func() bool
	onTick         func(StreamStats)

	mu     sync.Mutex
	window []seqSample // ring, oldest first, max rateWindowSamples
	last   StreamStats
}

const rateWindowSamples = 13

func newStatsPoller(stream string, si streamInfoFn, ci consumerInfoFn,
	trackConsumers []string, interval time.Duration,
	watcherLive func() bool, onTick func(StreamStats)) *statsPoller {
	return &statsPoller{stream: stream, si: si, ci: ci, trackConsumers: trackConsumers,
		interval: interval, watcherLive: watcherLive, onTick: onTick}
}

func (p *statsPoller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	p.pollOnce(ctx, time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			p.pollOnce(ctx, now)
		}
	}
}

func (p *statsPoller) pollOnce(ctx context.Context, now time.Time) {
	stats := StreamStats{Stream: p.stream, TakenAtMs: now.UnixMilli(), WatcherLive: p.watcherLive()}

	info, err := p.si(ctx)
	if err != nil {
		stats.Error = err.Error()
	} else {
		st := info.State
		stats.Msgs, stats.Bytes = st.Msgs, st.Bytes
		stats.FirstSeq, stats.LastSeq = st.FirstSeq, st.LastSeq
		stats.PerSubject = st.Subjects
		stats.RatePerSec = p.pushAndRate(now, st.LastSeq)
	}

	if p.ci != nil {
		for _, name := range p.trackConsumers {
			lag := ConsumerLag{Name: name}
			if cinfo, cerr := p.ci(ctx, name); cerr != nil {
				lag.Error = cerr.Error()
			} else {
				lag.NumPending = cinfo.NumPending
				lag.AckPending = cinfo.NumAckPending
			}
			stats.Consumers = append(stats.Consumers, lag)
		}
	}

	p.mu.Lock()
	p.last = stats
	p.mu.Unlock()
	if p.onTick != nil {
		p.onTick(stats)
	}
}

// pushAndRate appends a sample and returns the sliding-window rate. A
// sequence that went backwards (stream purge) resets the window.
func (p *statsPoller) pushAndRate(now time.Time, lastSeq uint64) float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.window); n > 0 && lastSeq < p.window[n-1].seq {
		p.window = nil
	}
	p.window = append(p.window, seqSample{t: now, seq: lastSeq})
	if len(p.window) > rateWindowSamples {
		p.window = p.window[1:]
	}
	if len(p.window) < 2 {
		return 0
	}
	oldest, newest := p.window[0], p.window[len(p.window)-1]
	secs := newest.t.Sub(oldest.t).Seconds()
	if secs <= 0 {
		return 0
	}
	return float64(newest.seq-oldest.seq) / secs
}

func (p *statsPoller) Last() StreamStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last
}
