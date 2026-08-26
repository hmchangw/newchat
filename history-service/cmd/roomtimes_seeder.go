package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	"github.com/hmchangw/chat/history-service/internal/service"
)

// roomTimesSeeder writes each room's createdAt into the shared room-times tier
// after an authoritative read, so a later degraded read has a walk floor.
//
// It wraps the repository BENEATH the process-local room cache, which is the
// whole point. The service used to seed from its own call site, but in
// production that call lands on readcache.RoomCache, so a hit — the common case
// for a hot room — still did a Valkey SET. Every history request without a
// usable client hint paid a network write. Down here only a genuine cache miss
// reaches the source, so seeding costs one write per room per cache TTL per pod.
//
// It also makes the tier's other rule structural rather than commented: only a
// repository read can reach this decorator, so a client-supplied hint cannot
// become another reader's walk floor.
type roomTimesSeeder struct {
	service.RoomRepository
	times service.RoomTimesCache
}

// GetRoomTimes seeds createdAt on success. The store is best-effort and never
// affects the returned values — the caller has them either way.
func (s roomTimesSeeder) GetRoomTimes(ctx context.Context, roomID string) (lastMsgAt, createdAt time.Time, err error) {
	lastMsgAt, createdAt, err = s.RoomRepository.GetRoomTimes(ctx, roomID)
	if err == nil {
		s.times.Store(ctx, roomID, createdAt)
	}
	return lastMsgAt, createdAt, err
}

// GetRoomTimesByIDs is left to the embedded repository. The batch path feeds
// the room list, which never walks Cassandra per room, so seeding from it would
// write one key per listed room on every request for no reader.
func (s roomTimesSeeder) GetRoomTimesByIDs(ctx context.Context, ids []string) (map[string]mongorepo.RoomTimes, error) {
	return s.RoomRepository.GetRoomTimesByIDs(ctx, ids)
}
