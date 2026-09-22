package main

import (
	"context"
	"time"
)

// Whether startTicker's first tick runs immediately or only after one interval
// has elapsed.
const (
	tickOnStart       = true
	tickAfterInterval = false
)

// startTicker runs tick every `every` until the returned stop is called or ctx is
// cancelled. It is the scaffolding behind the two background pollers (the lag
// sampler and the degraded-marker refresher), which differ only in the work one
// tick does.
//
// stop is synchronous: it blocks until the goroutine has actually exited, so no
// tick can still be in flight after it returns — the termination path the
// service's graceful shutdown (and the tests asserting it) rely on.
func startTicker(ctx context.Context, every time.Duration, firstTickOnStart bool, tick func(context.Context)) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		if firstTickOnStart {
			tick(ctx)
		}
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				tick(ctx)
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}
