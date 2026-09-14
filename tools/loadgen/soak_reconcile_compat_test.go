package main

import (
	"time"

	soakreconcile "github.com/hmchangw/chat/tools/loadgen/internal/soak/reconcile"
)

func nextReconcileProbe(now, verifyAfter, deadline time.Time, retryInterval time.Duration) time.Time {
	return soakreconcile.NextProbe(now, verifyAfter, deadline, retryInterval)
}
