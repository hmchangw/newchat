package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/subject"
)

// metaReader is the slice of Store the cross-site check needs.
type metaReader interface {
	GetRoomMeta(ctx context.Context, roomID string) (roommetacache.Meta, error)
}

// remotePeers returns the sites to refresh: everything in allSiteIDs except this
// one, blanks dropped and duplicates collapsed. Empty means single-site, which
// disables the refresh.
func remotePeers(siteID string, allSiteIDs []string) []string {
	seen := make(map[string]struct{}, len(allSiteIDs))
	var peers []string
	for _, s := range allSiteIDs {
		if s == "" || s == siteID {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		peers = append(peers, s)
	}
	return peers
}

// crossSiteChecker reports whether a room has members off this site, reading the
// room-meta cache. An unclassified room (nil CrossSite) counts as cross-site:
// the cost of guessing wrong is one wasted publish, and the destination ignores
// rooms it holds no subscription for. A meta read failure skips the refresh —
// the next message in the room retries it.
func crossSiteChecker(s metaReader) func(ctx context.Context, roomID string) bool {
	return func(ctx context.Context, roomID string) bool {
		meta, err := s.GetRoomMeta(ctx, roomID)
		if err != nil {
			return false
		}
		return meta.CrossSite == nil || *meta.CrossSite
	}
}

// roomActivityPublisher emits one refresh per peer site on the core-NATS
// activity lane. One room per message keeps the payload bounded by
// construction — a batched form would grow with the number of active rooms and
// eventually exceed max_payload, silently losing a whole flush window.
func roomActivityPublisher(p Publisher, siteID string, peers []string) func(context.Context, roomActivityRefresh) error {
	return func(ctx context.Context, r roomActivityRefresh) error {
		evt := model.RoomActivityEvent{
			RoomID:    r.roomID,
			SiteID:    siteID,
			LastMsgAt: r.at.UTC().UnixMilli(),
			Timestamp: time.Now().UTC().UnixMilli(),
		}
		data, err := json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("marshal room activity event: %w", err)
		}
		for _, peer := range peers {
			if err := p.Publish(ctx, subject.RoomActivity(peer), data); err != nil {
				return fmt.Errorf("publish room activity to %s: %w", peer, err)
			}
		}
		return nil
	}
}
