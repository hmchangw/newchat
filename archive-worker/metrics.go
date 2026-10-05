package main

// metrics is a placeholder until the observability task wires real
// instruments; the methods exist so the flusher and lane compile against the
// final call shape.
type metrics struct{}

func (m *metrics) segments(int, int)   {}
func (m *metrics) events(string, int)  {}
func (m *metrics) writeFailure(string) {}
func (m *metrics) blobs(string, int64) {}
func (m *metrics) redelivered(int)     {} //nolint:unused // called by the lane in a later task
