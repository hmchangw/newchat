package main

import "time"

// nextReconcileProbe schedules the next history probe for an operation whose
// message has not landed yet.
//
// The wait is the time already spent waiting, which doubles the interval on
// every probe and needs no attempt counter: verifyAfter and the deadline are
// both persisted with the operation, so a run that recovers from its journal
// resumes the same schedule rather than restarting the walk. A flat interval
// costs deadline/interval claims for one operation — ~590 at the defaults —
// which is enough for a small fraction of late messages to demand more
// reconcile capacity than the read lane has.
//
// The last probe is pulled back inside the deadline: expiring an operation
// without one more look would report a message that did land, late, as missing.
// When even that no longer fits, the returned time falls at or past the
// deadline so the expiry sweep takes the operation instead of the reconciler
// spinning on it.
func nextReconcileProbe(now, verifyAfter, deadline time.Time, retryInterval time.Duration) time.Time {
	wait := max(now.Sub(verifyAfter), retryInterval)
	next := now.Add(wait)
	if next.Before(deadline) {
		return next
	}
	final := deadline.Add(-retryInterval)
	if final.After(now) {
		return final
	}
	return next
}
