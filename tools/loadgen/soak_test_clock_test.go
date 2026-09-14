package main

import (
	"sync"
	"time"
)

type fakeSoakClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeSoakClock(now time.Time) *fakeSoakClock {
	return &fakeSoakClock{now: now}
}

func (c *fakeSoakClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeSoakClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}
