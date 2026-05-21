//go:build integration

package testutil

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

// RunTests runs m.Run, terminates shared containers, and exits.
// Usage: func TestMain(m *testing.M) { testutil.RunTests(m) }
func RunTests(m *testing.M) {
	code := m.Run()
	TerminateAll()
	os.Exit(code)
}

// PrewarmFailFast runs each Ensure* concurrently and returns the first
// error, or nil if all succeed. Intended for use in TestMain before m.Run.
func PrewarmFailFast(fns ...func() error) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(fns))
	for _, fn := range fns {
		wg.Add(1)
		go func(f func() error) {
			defer wg.Done()
			if err := f(); err != nil {
				errCh <- err
			}
		}(fn)
	}
	wg.Wait()
	close(errCh)
	if err, ok := <-errCh; ok {
		return err
	}
	return nil
}

// RunTestsWithPrewarm pre-warms via PrewarmFailFast, then RunTests.
// On prewarm failure, exits with code 1 after TerminateAll cleanup.
func RunTestsWithPrewarm(m *testing.M, prewarms ...func() error) {
	if err := PrewarmFailFast(prewarms...); err != nil {
		fmt.Fprintf(os.Stderr, "prewarm shared containers: %v\n", err)
		TerminateAll()
		os.Exit(1)
	}
	RunTests(m)
}
