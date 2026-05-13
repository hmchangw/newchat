// Package msgbucket computes time-bucket boundaries for Cassandra message
// partitions. Bucket values are start-of-window unix milliseconds derived
// deterministically from a row's created_at; no shared state is required.
package msgbucket

import "time"

// Sizer computes the bucket value containing a timestamp.
type Sizer struct {
	windowMs int64
}

// New returns a Sizer for the given fixed window. Window must be positive;
// callers are expected to validate at startup (see service main.go).
func New(window time.Duration) Sizer {
	return Sizer{windowMs: window.Milliseconds()}
}

// Of returns the bucket (start-of-window unix millis) containing t.
func (s Sizer) Of(t time.Time) int64 {
	return (t.UnixMilli() / s.windowMs) * s.windowMs
}

// Prev returns the bucket immediately before b.
func (s Sizer) Prev(b int64) int64 { return b - s.windowMs }

// Next returns the bucket immediately after b.
func (s Sizer) Next(b int64) int64 { return b + s.windowMs }

// WindowMs returns the configured window in milliseconds.
func (s Sizer) WindowMs() int64 { return s.windowMs }
