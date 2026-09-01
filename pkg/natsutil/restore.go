package natsutil

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
)

// RestoreTracker records when a connection last came back after being lost.
//
// Room-event routing needs this: for a while after the home connection is
// restored, publishers must emit to BOTH subject roots, because clients revert
// to their home site on a slower clock than servers do (see
// subject.EffectiveRouteMode). The zero value is valid and reads as "never
// lost", which keeps a service that has had no outage out of the grace window.
// The stamp is an atomic UnixNano rather than a mutex-guarded time.Time:
// RestoredAt is read once per room-event publish, from up to MAX_WORKERS
// goroutines at once, while MarkRestored runs only on a reconnect. A lock-free
// load keeps the hot side off a contended cache line.
type RestoreTracker struct {
	nanos atomic.Int64
}

// MarkRestored stamps a restoration. A later stamp replaces an earlier one, so a
// second outage restarts the grace window rather than inheriting the first; an
// earlier stamp is ignored, so an out-of-order mark cannot cut a window short.
func (t *RestoreTracker) MarkRestored(at time.Time) {
	n := at.UnixNano()
	for {
		cur := t.nanos.Load()
		if n <= cur {
			return
		}
		if t.nanos.CompareAndSwap(cur, n) {
			return
		}
	}
}

// RestoredAt returns the last restoration, or the zero time if the connection
// has never been lost.
func (t *RestoreTracker) RestoredAt() time.Time {
	n := t.nanos.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

// TrackRestores watches conn and stamps a tracker every time it returns to
// CONNECTED after a drop.
//
// StatusChanged rather than nats.ReconnectHandler: Connect already installs a
// ReconnectHandler for logging, and same-kind nats.Options overwrite rather than
// compose, so adding one here would silently drop the other. StatusChanged is an
// independent listener registry — the same reason Drain uses it.
//
// The watcher goroutine exits when ctx is cancelled. A nil conn yields an inert
// tracker, so a caller with no connection needs no nil check of its own.
func TrackRestores(ctx context.Context, conn *o11ynats.Conn) *RestoreTracker {
	tr := &RestoreTracker{}
	if conn == nil {
		return tr
	}
	nc := conn.NatsConn()
	ch := nc.StatusChanged(nats.CONNECTED)

	go func() {
		defer nc.RemoveStatusListener(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				now := time.Now().UTC()
				tr.MarkRestored(now)
				slog.InfoContext(ctx, "nats connection restored; entering room-route grace window",
					"restored_at", now)
			}
		}
	}()
	return tr
}
