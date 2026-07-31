package testutil

import "time"

// retryPollInterval is the pause between retryUntil attempts.
const retryPollInterval = 100 * time.Millisecond

// retryUntil calls fn until it succeeds or timeout elapses, returning fn's last
// error. fn always runs at least once, so a non-positive timeout still yields one
// attempt, and fn is never attempted once the deadline has passed. For container
// readiness checks where a wait strategy has fired but the service is not yet answering.
func retryUntil(timeout time.Duration, fn func() error) error {
	deadline := time.Now().Add(timeout)
	for {
		err := fn()
		if err == nil {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return err
		}
		time.Sleep(min(remaining, retryPollInterval))
	}
}
