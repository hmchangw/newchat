package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/user-presence-service/presencestore"
)

// Sweeper periodically recomputes accounts whose sweep deadline has passed
// and publishes any resulting status changes.
type Sweeper struct {
	store    PresenceStore
	publish  presencestore.PublishFunc
	siteID   string
	interval time.Duration
	now      func() time.Time
}

// NewSweeper builds a Sweeper.
func NewSweeper(store PresenceStore, publish presencestore.PublishFunc, siteID string, interval time.Duration) *Sweeper {
	return &Sweeper{store: store, publish: publish, siteID: siteID, interval: interval, now: time.Now}
}

// Run ticks until ctx is cancelled.
func (s *Sweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.tick(ctx); err != nil {
				slog.Error("presence sweep failed", "error", err)
			}
		}
	}
}

// tick runs one sweep and publishes changes.
func (s *Sweeper) tick(ctx context.Context) error {
	changes, err := s.store.Sweep(ctx, s.now())
	if err != nil {
		return err
	}
	for _, ch := range changes {
		presencestore.PublishState(ctx, s.publish, s.siteID, ch.Account, ch.Effective, s.now())
	}
	return nil
}
