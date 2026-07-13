package main

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGuardedJob_SkipsWhileRunning(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	var runs atomic.Int32

	job := guardedJob(func() {
		runs.Add(1)
		if runs.Load() == 1 {
			close(started)
			<-release
		}
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		job.Run()
	}()
	<-started

	// second fire while the first is still executing returns without running
	job.Run()
	assert.Equal(t, int32(1), runs.Load(), "overlapping fire must be skipped")

	close(release)
	wg.Wait()

	// after the first run finishes, the next fire executes again
	job.Run()
	assert.Equal(t, int32(2), runs.Load(), "guard must release after completion")
}
