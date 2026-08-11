package natsutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
)

// Drain puts the connection into the drain state and blocks until it reaches
// CLOSED or ctx expires.
//
// nats.Conn.Drain only *starts* the drain — it spawns drainConnection on a
// goroutine and returns nil immediately — so a shutdown hook that returns
// nc.Drain() directly returns before subscriptions have drained and before the
// final publish flush, and the process exits with buffered JetStream acks and
// request/reply responses still in the write buffer.
func Drain(ctx context.Context, conn *o11ynats.Conn) error {
	if conn == nil {
		return nil
	}
	nc := conn.NatsConn()
	if nc == nil {
		return nil
	}

	// Register the listener before calling Drain so a fast close cannot land in
	// the gap between the two calls, and re-check IsClosed after, to cover a
	// close that completed before the listener was in place. Together these
	// close both sides of the race.
	ch := nc.StatusChanged(nats.CLOSED)
	defer nc.RemoveStatusListener(ch)

	if err := nc.Drain(); err != nil {
		switch {
		case errors.Is(err, nats.ErrConnectionClosed):
			return nil
		case errors.Is(err, nats.ErrConnectionReconnecting):
			// Drain hard-closed the connection. Buffered publishes are gone,
			// but there was no reachable server to flush them to — a normal
			// shutdown-during-outage, not a failure worth reporting upward.
			slog.WarnContext(ctx, "nats drain skipped: connection reconnecting")
			return nil
		default:
			return fmt.Errorf("start nats drain: %w", err)
		}
	}
	if nc.IsClosed() {
		return nil
	}

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("nats drain incomplete: %w", ctx.Err())
	}
}
