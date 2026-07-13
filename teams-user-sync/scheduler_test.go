package main

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/msgraph"
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

func TestRunSync_LogsAndSwallowsOutcome(t *testing.T) {
	t.Run("success path", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1"}).
			Return(map[string]struct{}{"u1": {}}, nil)
		lister := &fakeLister{pages: [][]msgraph.GraphUser{
			{{ID: "u1", UserPrincipalName: "alice@corp.example"}},
		}}

		// must not panic; outcome is logged, not returned
		runSync(NewSyncer(store, lister, 500))
	})
	t.Run("error path", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		lister := &fakeLister{err: errors.New("graph down")}

		runSync(NewSyncer(store, lister, 500))
	})
}

func TestCronSlogLogger_DoesNotPanic(t *testing.T) {
	cronSlogLogger{}.Info("wake", "now", "x")
	cronSlogLogger{}.Error(errors.New("boom"), "failed", "job", "sync")
}
